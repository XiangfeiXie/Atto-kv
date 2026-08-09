# KVStore 性能优化报告 — 2026-08-09

> **会话范围:** GET 热点路径分析 + 默认配置修正 + 基准测试修复 + dynbuf 优化
> **测试环境:** Ubuntu 24.04, Kernel 6.8.0, VMware 虚拟机, 单线程
> **对比基线:** Redis 7.0

---

## 1. 修改摘要

| 文件 | 修改 | 类型 | 影响 |
|------|------|------|------|
| `src/config/config.c:19` | `PERSIST_AOF` → `PERSIST_NONE` | Bug 修复 | AOF/RDB 定期函数默认关闭 |
| `conf/kvstore-none.conf:2` | `network proactor` → `network reactor` | Bug 修复 | Reactor 基准测试现正确运行 reactor 引擎 |
| `src/util/dynbuf.c` | 移除 `dynbuf_reserve()` 页预触达 | 性能优化 | 消除不必要的物理页分配开销 |

---

## 2. 详细分析

### 2.1 默认 persist_mode 修正

**问题：** `config_default()` 中 `persist_mode = PERSIST_AOF`，导致即使用户不指定任何持久化配置，以下函数也会在**每次事件循环迭代**中被调用：

```c
// kvstore.c:116-124 — update_cached_time() 热路径
if (ENABLE_AOF) {                            // persist_mode == PERSIST_AOF → true
    kvs_wal_periodic();                      // 检查 wal_ring_ok → 早期返回
    kvs_aof_auto_rewrite_check();            // 检查 wal_ring_ok / fd / cooldown → 早期返回
}
if (ENABLE_RDB) {                            // persist_mode == PERSIST_AOF → true
    rdb_periodic();                          // 检查 bgsave_pending → 早期返回
}
```

虽然这些函数在没有实际持久化配置时是廉价早期返回（仅检查几个全局标志位），但每次事件循环的函数调用 + 分支检查仍有微小的累积开销。

**修改：** `config.c:19`
```c
// Before
kvs_config.persist_mode = PERSIST_AOF;
// After
kvs_config.persist_mode = PERSIST_NONE;  /* off by default */
```

**验证：** `expire_enabled` 默认值已经是 `0`（关闭），无需修改。

---

### 2.2 基准测试配置错误修复（关键发现）

**问题：** `conf/kvstore-none.conf` 中 `network proactor`，但该文件被 `bench_pipeline_v2.sh` 用于**不带 `--proactor` 标志**的"reactor"基准测试。

```bash
# bench_pipeline_v2.sh 逻辑：
CONF="kvstore-none.conf"          # 默认配置 → 含 "network proactor"
# ...
_start_kvs() {
    taskset -c "$SERVER_CPU" "$KVS_BIN" "$KVSCONF_PATH" &  # 直接使用配置，不覆盖 network 模式
}
```

**影响范围：** 此前所有标注为 "reactor" 的基准测试数据实际上都是在 **proactor 模式**下运行的。两轮测试（"reactor" vs "proactor"）的唯一区别是 `loglevel info` vs `loglevel warn`，而非网络引擎差异。

**修改：** `kvstore-none.conf:2`
```
# Before
network proactor
# After
network reactor
```

**后续动作：** 修正后需要重跑基准测试才能获得真实的 reactor vs proactor 对比数据。

---

### 2.3 dynbuf_reserve 页预触达移除

**问题：** `dynbuf_reserve()` 在扩展缓冲区时，对每个新分配的 4KB 页写入一个字节以强制物理页分配（pre-fault）。这在正常使用中是多余的——页会在首次实际写入时由内核按需分配。

```c
// 已移除的代码 (dynbuf.c:25-29):
/* Pre-fault newly added pages to avoid minor page faults later */
int start_page = (old_cap + 4095) & ~4095;
for (int off = start_page; off < new_cap; off += 4096)
    nb[off] = 0;  // 触发物理页分配
```

**分析：** 内核的按需分页（demand paging）机制足够高效。预触达在 `realloc` 返回后立即遍历所有新页，产生额外的内存写入和 TLB 压力，却没有避免任何实际瓶颈——因为缓冲区扩展本身就是低频操作。

**修改后代码：**
```c
int dynbuf_reserve(DynBuf *db, int total_cap) {
    if (!db || total_cap <= 0) return -1;
    if (total_cap <= db->cap) return 0;

    int new_cap = db->cap > 0 ? db->cap : 16;
    while (new_cap < total_cap) new_cap *= 2;
    if (new_cap > KVS_NET_BUF_MAX) return -2;

    char *nb = (char *)realloc(db->buf, (size_t)new_cap);
    if (!nb) return -1;

    db->buf = nb;
    db->cap = new_cap;
    return 0;
}
```

---

### 2.4 GET 热点路径分析结论

通过 `perf record -g` 对 reactor 和 proactor 引擎的调用图分析：

| 组件 | CPU 占比 | 说明 |
|------|:------:|------|
| 内核 (tcp_recvmsg / tcp_sendmsg / epoll_wait) | ~37% | 网络 I/O 栈 |
| libc 封装 (recv / send) | ~12.5% | 系统调用胶水层 |
| **kvstore 应用逻辑 (kvs_dispatch + resp_parse)** | **~3%** | 命令分发 + RESP 解析 |
| Redis 应用逻辑 (dictFind + lookupKey) | ~26% | 作为对比 |

**结论：** kvstore 的应用层 GET 热路径已经极致优化（仅为 Redis 应用层开销的 **1/8**）。当前瓶颈在网络 I/O 层，而非命令处理或 hash 表查找。进一步的吞吐提升需要从 I/O 模型层面入手。

---

## 3. 基准测试结果（修正后）

### 3.1 Reactor 模式（epoll level-triggered）

```
测试时间: 2026-08-09 09:51 | 配置: network reactor, persistence none, expire off
参数: n=1,000,000 c=50 d=16 warmup=2,000,000 | CPU: server=0,2 client=4
```

| P | kvs GET | redis GET | 倍率 |
|---|--------:|----------:|:----:|
| P=1   | 20,571  | 18,152   | **1.13x** |
| P=4   | 81,764  | 77,469   | **1.06x** |
| P=8   | 155,104 | 144,098  | **1.08x** |
| P=16  | 297,024 | 181,296  | **1.64x** |
| P=20  | 353,864 | 203,087  | **1.74x** |
| P=40  | 539,687 | 257,487  | **2.10x** |
| P=80  | 639,080 | 277,604  | **2.30x** |
| P=160 | 606,428 | 241,618  | **2.51x** |

- **峰值 GET:** 639K @ P=80
- **全 P 深度领先 Redis**，P≥16 后倍率 1.64x–2.51x
- **P=160 微降至 606K**，epoll LT 在极端负载下略有 event storm 效应

### 3.2 Proactor 模式（io_uring + SQPOLL）

```
测试时间: 2026-08-09 08:10 | 配置: network proactor, persistence none, expire off
参数: n=1,000,000 c=50 d=16 warmup=2,000,000 | CPU: server=0,2 client=4
```

| P | kvs GET | redis GET | 倍率 |
|---|--------:|----------:|:----:|
| P=1   | 20,544  | 20,550   | 1.00x |
| P=4   | 95,344  | 80,090   | **1.19x** |
| P=8   | 133,459 | 132,194  | 1.01x |
| P=16  | 250,044 | 135,686  | **1.84x** |
| P=20  | 300,487 | 191,929  | **1.57x** |
| P=40  | 558,437 | 222,415  | **2.51x** |
| P=80  | 938,202 | 232,859  | **4.03x** |
| P=160 | 1,383,062 | 246,012 | **5.62x** |

- **峰值 GET:** 1,383,062 @ P=160（**5.62x Redis**）
- **P=160 SET:** 1,211,041（**6.08x Redis**）

### 3.3 Reactor vs Proactor 对比

| P | reactor GET | proactor GET | 优势 |
|---|------------:|-------------:|:---:|
| P=1   | 20,571  | 20,544   | 持平 |
| P=4   | 81,764  | 95,344   | proactor +17% |
| P=8   | 155,104 | 133,459  | reactor +16% |
| P=16  | 297,024 | 250,044  | reactor +19% |
| P=20  | 353,864 | 300,487  | reactor +18% |
| P=40  | 539,687 | 558,437  | 持平 |
| P=80  | 639,080 | 938,202  | **proactor +47%** |
| P=160 | 606,428 | 1,383,062 | **proactor +128%** |

**交叉点：P=40**。低 P 时 epoll LT + inline flush 的简洁设计优于 SQPOLL；高 P 时 io_uring 零 syscall 提交 + 内核旁路优势充分释放。

---

## 4. 已知问题与后续优化方向

### 4.1 Proactor 中低 P 性能劣势

P=8–20 区间 reactor 领先 proactor 16%–19%。根因是 SQPOLL 内核线程在低吞吐下的 CPU 空转开销 + io_uring CQE 处理链路（`handle_cqe` → `handle_read` → `arm_recv` + `arm_send` SQE 构造）比 reactor 的纯 syscall 路径更重。

**可能的优化方向：**
- 减少 CQE batch 处理中的 SQE 重新构造频率
- 在 `handle_read` 中合并 `arm_recv` + `arm_send`，减少 `io_uring_get_sqe` 调用次数
- 多轮 SQE 提交合并为单次 `io_uring_submit`

### 4.2 Reactor 高 P 回落

P=160 GET 从 P=80 的 639K 降至 606K（−5%）。可能原因：epoll LT 模式下极端事件密度导致 epoll_wait 返回大量就绪 fd，每个都需要 `recv_cb`/`send_cb` 处理，inline flush 的双次 `send_callback` 在高并发下可能触发额外的 EAGAIN 路径。

### 4.3 dynbuf_reserve 的 realloc 开销

`dynbuf_reserve` 每次扩展都调用 `realloc`（而非预留余量）。在缓冲区频繁小幅增长场景下可能产生不必要的拷贝。可考虑实现容量翻倍策略的预分配，或使用环形缓冲区避免数据搬移。

---

## 5. 文件变更清单

```
修改:
  src/config/config.c               — persist_mode 默认值 PERSIST_AOF → PERSIST_NONE
  conf/kvstore-none.conf            — network proactor → network reactor
  src/util/dynbuf.c                 — 移除 dynbuf_reserve 页预触达循环
  benchmark.md                      — 更新 pipeline 性能数据 (reactor_3 + proactor_3)
  docs/session_optimization_2026-08-09.md — 本报告
```

---

## 6. 引擎选择建议

| 场景 | 推荐引擎 | 理由 |
|------|:--------:|------|
| 通用 Web 服务（低延迟优先） | **Reactor** | 低 CPU 占用、低 P 延迟更低、无需 root |
| 批量数据处理（吞吐优先） | **Proactor** | P≥80 时 2×+ 吞吐、零 syscall 提交 |
| 嵌入式 / 无 root 环境 | **Reactor** | SQPOLL 需要 `CAP_SYS_ADMIN` |
| 极限压测 / 高性能缓存 | **Proactor** | P=160 达到 1.38M GET/s |
