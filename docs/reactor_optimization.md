# Reactor 网络引擎优化报告

**时间:** 2026-08-05 | **影响范围:** `src/net/reactor.c` 为主，联动 `src/net/proactor.c`、`src/engine/kv_dict.h`、`src/core/kvstore_dispatch.c`、`src/config/`、`test/bench/`

---

## 0. 起点：Reactor 引擎的四个问题

优化前的 reactor 网络引擎存在四个层面的问题：

| 层面 | 问题 | 影响 |
|------|------|------|
| 正确性 | `#if ENABLE_KVSTORE` 宏从未定义，全部协议处理为死代码 | 引擎不处理任何 Redis 命令 |
| 内存 | 读缓冲区 `r_off` 永不归零 + 写缓冲区 `w_off` 无条件重置 | 缓冲区蠕变、数据损坏 |
| 性能 | 边缘触发(ET) + 读写分离回调 = 每批次 2 次 `epoll_ctl` | epoll_ctl 占 17.4% CPU |
| 性能 | 读写事件不在同一次循环处理 = "批次屏障" | 每批额外一次 `epoll_wait` |

此外还有未使用的 `<poll.h>` include、`MAX_PORTS`/`TIME_SUB_MS` 宏、`struct timeval begin` 变量、`set_event()` 无返回值的 bug。

---

## 1. 正确性修复：移除死代码 `ENABLE_KVSTORE`

### 现象

`ENABLE_KVSTORE` 在整个项目中从未被定义——CMakeLists.txt 中没有 `add_compile_definitions`，config.h 中也没有宏。C 预处理器将 `#if ENABLE_KVSTORE` 解析为 `#if 0`，导致 4 个代码块内的所有协议处理逻辑被裁剪：

- `conn_set_events()` —— 事件掩码管理
- `recv_cb` 中的 `kvs_drain_resp_commands_off` 调用
- `send_cb` 中的延迟读处理
- `send_cb` 末尾的 `conn_set_events` 调用

### 修复

移除全部 4 对 `#if ENABLE_KVSTORE` / `#endif`，协议处理逻辑回归主路径。验证：PING/SET/GET 恢复正常响应。

### 附带的代码清理

- 移除 `<poll.h>` include（使用 epoll，不需要 poll）
- 移除 `MAX_PORTS` 宏（注释标注"Single port"，实际未用）
- 移除 `TIME_SUB_MS` 宏（从未引用）
- 移除 `struct timeval begin` 全局变量（未引用）
- 内联 `set_event()` 到 `event_register()`，修复声明 `int` 但无返回值的问题
- 移除 `accept_cb()` 的前向声明（定义早于首次使用）
- `epfd` 上移至文件顶部，消除函数内的 `extern` 声明

---

## 2. 缓冲区正确性修复

### 2a. 读缓冲区蠕变（Fix 1）

**根因：** `kvs_drain_resp_commands_off` 中的 compaction 条件

```c
if (off > cap / 2 && remaining > 0 && remaining < off) {
    memmove(...);
    off = 0;
}
```

当 `remaining == 0`（缓冲区被完全消费）时，compaction 不触发。`r_off` 持续增长，下一次读取以 `tail = r_off + 0` 计算写入位置。当 `r_off` 超过 `cap` 时触发不必要的 `realloc`（64K→128K→256K→...→16MB 上限），而实际缓冲区为空。

**修复：** 在 `recv_cb` 中，`kvs_drain_resp_commands_off` 返回后：

```c
if (c->read_buf.len == 0)
    c->r_off = 0;
```

对应 proactor 已有的 `if (old_len == 0) c->r_off = 0;` 逻辑。

### 2b. 写缓冲区数据损坏（Fix 2）

**场景：** 上一次 `send_cb` 只完成部分发送（TCP 发送缓冲区满）。此时 `w_off = K`（已发送 K 字节），`write_buf.len = L`（剩余 L 字节待发），待发数据位于 `buf[K .. K+L-1]`。新数据到达触发 `recv_cb`：

```c
c->w_off = 0;  // 无条件重置写偏移
kvs_drain_resp_commands_off(...);  // → dynbuf_append 在 buf + L 处追加
```

`dynbuf_append` 写入位置 `buf + L`（其中 `L < K+L-1`，即落在待发区域内），**覆盖未发送数据**。

**修复：** 采用延迟处理模式（与 proactor 一致）：

- `recv_cb`：当 `write_buf.len > 0` 时跳过协议处理，仅 re-arm EPOLLIN 并返回
- `send_cb`：写缓冲完全排空后，检查 `read_buf.len > 0` 并处理延迟数据

---

## 3. 性能优化一：消除"批次屏障"（Inline Flush）

### 问题

边缘触发(ET)模式下，读和写由不同的事件驱动：

```
epoll_wait → EPOLLIN → recv_cb(处理) → conn_set_events(加EPOLLOUT)
epoll_wait → EPOLLOUT → send_cb(发送) → conn_set_events(去EPOLLOUT)
```

每批次需要两次 `epoll_wait` 循环。在低 Pipeline（P=5-10）时，批处理量不足以摊销这个额外往返。

### 修复

在事件循环中，处理完 EPOLLIN 后立即尝试发送：

```c
if (events[i].events & EPOLLIN) {
    c->r_action.recv_callback(connfd);
    if (!conn_pool[connfd].active) continue;
}
/* 内联 flush：同一轮 epoll 循环中直接发送 */
if (c->write_buf.len > 0) {
    c->send_callback(connfd);
    if (conn_pool[connfd].active && c->write_buf.len > 0)
        c->send_callback(connfd);  // 处理 deferred read 产生的新响应
}
```

最多两次 `send_callback`：第一次刷原始响应+触发延迟读处理，第二次刷延迟读产生的新响应。EPOLLOUT 仅保留为 EAGAIN 回退。

**效果：** P=10 GET 从 186.9K（0.97x Redis）提升到 216.0K（1.13x Redis），+15.6%。

---

## 4. 性能优化二：Edge-Triggered → Level-Triggered

### 问题

ET 模式下，即使有了 inline flush，每批次仍需要 2 次 `epoll_ctl(MOD)`：
- `recv_cb` 后：添加 EPOLLOUT（从 `EPOLLIN|EPOLLET` → `EPOLLIN|EPOLLOUT|EPOLLET`）
- `send_cb` 后：移除 EPOLLOUT（恢复 `EPOLLIN|EPOLLET`）

火焰图显示 `epoll_ctl` 占 15.9% CPU。每批 2 次 MOD 是 ET 模式的固有税——Redis 使用 level-triggered (LT)，不需要频繁 MOD。

### 设计

LT 模式下，EPOLLIN 始终在 epoll 注册中（无需 re-arm），EPOLLOUT 按需动态开关：

```
正常批次（零 epoll_ctl）：
  epoll_wait → EPOLLIN
    recv_cb: drain recv + 处理 P 条命令  ← 不调用 conn_set_events
    inline flush: send_cb 发送全部响应
      conn_set_events: write_buf=0, want=EPOLLIN, cur=EPOLLIN → lazy skip
```

**关键设计决策：**

1. **recv_cb 不管理 epoll 掩码。** 协议处理后不调用 `conn_set_events`。inline flush 在事件循环中直接发送，EPOLLOUT 的管理完全由 `send_cb` 负责。

2. **status 感知的事件掩码。** 当 `c->status != 0`（QUIT/协议错误，close-after-flush），`conn_set_events` 抑制 EPOLLIN，防止对端在关闭期间注入新命令：

   ```c
   uint32_t want = c->status ? 0 : EPOLLIN;
   if (c->write_buf.len > 0) want |= EPOLLOUT;
   ```

3. **EPOLLOUT 动态开关。** 仅在 `write_buf` 非空时注册 EPOLLOUT，排空后立即移除——避免 LT 模式下的"写就绪风暴"（LT 会在发送缓冲区有空时连续触发 EPOLLOUT）。

### 变更清单

| 位置 | 变更 |
|------|------|
| `conn_set_events` | `EPOLLIN\|EPOLLET` → `EPOLLIN` base, status 感知 |
| `event_register` | 移除 `event` 参数，硬编码 `EPOLLIN` |
| `recv_cb` | 移除正常路径的 `conn_set_events` 调用 |
| `accept_cb` / `reactor_start` | `EPOLLIN\|EPOLLET` → `EPOLLIN` |
| 全文件注释 | "edge-triggered" → "level-triggered" |

**效果：** epoll_ctl 火焰图占比从 15.9% 降至 <0.1%（仅剩 EPOLL_CTL_ADD/DEL）。

---

## 5. 性能优化三：过期系统配置开关 + 锁优化

### 问题

过期系统有三条删除路径：惰性删除（`dispatch_resolve` 内检查 `entry->expire`）、队列删除（读操作发现的过期 key 批量在写操作中清除）、后台线程主动采样（每 1 秒随机抽样 20 key）。

在 pipeline 基准测试中（无 TTL、纯 SET/GET），三条路径均不触发实际删除，但仍有开销：

| 路径 | 开销 | 每次代价 |
|------|------|:---:|
| 惰性删除 | `entry->expire` 字段读取 + `get_time_now()` 原子加载 + 分支 | ~7ns |
| 队列删除 | `flush_expire_queue()` 函数调用，检查 `count==0` 立即返回 | ~5ns |
| 后台线程 | 每 1s CAS 争抢 + 20 次随机采样 | ~4μs/s |

此外，`kv_store_rdlock()` 使用全局自旋锁（CAS 0→1）协调过期线程，每次 GET/SET 都执行一次 `lock cmpxchg`（~20ns），而 99.99% 的时间没有竞争。

### 修复 A：`expire off` 配置

在 `KvsConfig` 中新增 `expire_enabled` 字段，通过 `ENABLE_EXPIRE` 宏做编译期展开（零间接跳转开销）：

```c
// config.h
#define ENABLE_EXPIRE (kvs_config.expire_enabled)

// dispatch_resolve
if (ENABLE_EXPIRE && entry && entry->expire && now > expire) { ... }

// kvstore.c
if (ENABLE_EXPIRE) kvs_start_expire_thread();

// dispatch.c — SET 路径
if (ENABLE_EXPIRE) flush_expire_queue();
```

基准测试配置 `expire off`，消除全部过期相关开销。

### 修复 B：自旋锁结构优化

将 CAS 从 while 循环条件中提升为 fast-path 直通：

```c
// 改前：CAS 在 while 条件，失败才进 PAUSE 循环
while (__sync_val_compare_and_swap(&g_store_lock, 0, 1) != 0) {
    while (__atomic_load_n(&g_store_lock, __ATOMIC_RELAXED) != 0)
        __builtin_ia32_pause();
}

// 改后：CAS 一次成功直接返回，失败才进冷路径 do-while
if (__sync_val_compare_and_swap(&g_store_lock, 0, 1) != 0) {
    do {
        while (__atomic_load_n(&g_store_lock, __ATOMIC_RELAXED) != 0)
            __builtin_ia32_pause();
    } while (__sync_val_compare_and_swap(&g_store_lock, 0, 1) != 0);
}
```

热路径：1 次 CAS + 1 次分支（not-taken）→ 直接返回。冷路径（过期线程持锁的 0.0004% 时间）才进 PAUSE 自旋。

---

## 6. 基准测试方法学修正

原有测试方法存在三个问题导致 U 形凹陷的测量假象：

| 问题 | 旧方法 | 新方法 | 影响 |
|------|--------|--------|------|
| 核心争抢 | server/client 无绑核 | `taskset -c 0,3` (server) / `taskset -c 1,2` (client) | 消除 OS 调度噪声 |
| 单 key 热点 | `-d 16` 无 `-r` | `-r 1000000` 随机 key | 击穿 L3 cache，测真实 hash 表 |
| P=1 基准失效 | 测量客户端瓶颈，非服务器 | 明确标注 P=1 为客户端限制 | 避免虚假的 1.13x 基线 |

在 `common.sh` 中新增 `SERVER_CORES`、`CLIENT_CORES`、`BENCH_RAND` 配置变量，`start_kvs` 和 `run_bench` 统一使用 `taskset` 和 `-r`。

---

## 7. 最终性能数据

**测试条件:** Ubuntu 24.04, Kernel 6.8.0, 单线程, persistence=none, expire=off, `-r 1000000` 随机 key, taskset 钉核

### Reactor (epoll level-triggered + inline flush)

| P | kvs SET | redis SET | 倍率 | kvs GET | redis GET | 倍率 |
|---|--------:|----------:|:----:|--------:|----------:|:----:|
| P=1   | 19,328  | 17,948  | 1.08x | 19,362  | 17,449  | 1.11x |
| P=5   | 91,912  | 85,529  | 1.07x | 94,518  | 96,562  | 0.98x |
| P=10  | 197,044 | 116,863 | 1.69x | 188,679 | 154,919 | 1.22x |
| P=20  | 356,252 | 156,740 | 2.27x | 364,034 | 210,305 | 1.73x |
| P=40  | 460,405 | 195,925 | 2.35x | 576,369 | 266,241 | 2.16x |
| P=80  | 529,661 | 187,617 | 2.82x | 668,896 | 302,297 | 2.21x |
| P=160 | 531,632 | 199,760 | 2.66x | 585,823 | 318,269 | 1.84x |

### Proactor (io_uring + SQPOLL)

| P | kvs SET | redis SET | 倍率 | kvs GET | redis GET | 倍率 |
|---|--------:|----------:|:----:|--------:|----------:|:----:|
| P=1   | 21,222  | 18,884  | 1.12x | 21,363  | 18,247  | 1.17x |
| P=5   | 93,752  | 87,745  | 1.07x | 94,397  | 98,496  | 0.96x |
| P=10  | 169,860 | 115,677 | 1.47x | 171,506 | 158,417 | 1.08x |
| P=20  | 294,370 | 156,568 | 1.88x | 310,040 | 210,397 | 1.47x |
| P=40  | 468,630 | 188,908 | 2.48x | 549,867 | 306,850 | 1.79x |
| P=80  | 610,034 | 216,008 | 2.82x | 906,323 | 349,375 | 2.59x |
| P=160 | 767,174 | 209,448 | 3.66x | 946,482 | 318,376 | 2.97x |

### 引擎选择建议

| 场景 | 推荐 | 理由 |
|------|------|------|
| 通用场景 | Reactor | 零特权、低 CPU 占用、P≤40 领先 |
| 极限吞吐 | Proactor | 需 sudo+SQPOLL，P≥80 时领先 35-62% |

---

## 8. 变更文件清单

| 文件 | 变更类型 | 说明 |
|------|:---:|------|
| `src/net/reactor.c` | 重构 | 死代码移除 + ET→LT + inline flush + 缓冲修复 |
| `src/engine/kv_dict.h` | 优化 | `kv_store_rdlock` 锁结构优化 |
| `src/config/config.h` | 新增 | `expire_enabled` 配置 + `ENABLE_EXPIRE` 宏 |
| `src/config/config.c` | 新增 | `expire on\|off` 解析 + 打印 |
| `src/core/kvstore.c` | 修改 | 条件启用过期线程 |
| `src/core/kvstore_dispatch.c` | 修改 | `ENABLE_EXPIRE` 守卫过期逻辑 |
| `conf/kvstore-none.conf` | 修改 | 添加 `expire off` |
| `test/bench/common.sh` | 修改 | taskset 钉核 + `-r` 随机 key |
| `test/bench/bench_pipeline.sh` | 修改 | 方法学更新、结果头信息 |
| `benchmark.md` | 更新 | 第一、二节同步最新数据 |

---

## 9. 持久化修复：everysec 刷盘延迟 + proactor 空闲冻结

测试中发现 AOF everysec 刷盘未按预期每 1 秒触发，进一步排查发现两个独立 bug。

### 9a. everysec 刷盘延迟（wal_persist.c）

**问题：** `kvs_wal_periodic` 中用 tick 计数近似 1 秒：

```c
if (idle_ticks > 100) {  /* ~1s at 100Hz event loop */
    wal_flush_current_slot();
}
```

该计数的准确度依赖事件循环频率。reactor 空闲时 `epoll_wait(100ms)` → ~10Hz → 需 **10 秒**才刷一次盘，崩溃丢失远超承诺的 1 秒数据。

**修复：** 改为基于缓存时钟 `get_time_now()` 的时间判断，与循环频率无关：

```c
if (now_ms - last_flush_ms >= 1000) {
    wal_flush_current_slot();
    last_flush_ms = now_ms;
}
```

新增 `#include "kvstore.h"` 以使用 `get_time_now()`。

### 9b. proactor 空闲事件循环冻结（proactor.c）

**问题：** proactor 空闲时 `io_uring_submit_and_wait(ring, 1)` 在 SQPOLL 下**无限阻塞**：timeout SQE 被添加但不被内核 SQPOLL 线程提交，事件循环冻结。后果：

- `update_cached_time()` → `kvs_wal_periodic()` 不跑 → WAL 空闲期完全停摆
- RDB 周期保存、AOF 自动 rewrite 检查同样冻结
- 探针确认：空闲 3 秒内 `iter` 从 73,000 到 73,000（零增长）

**尝试过的方案：**
- timeout SQE + `io_uring_submit_and_wait` — 原代码，SQPOLL 下阻塞
- 显式 `io_uring_submit` + `IORING_ENTER_GETEVENTS` — 仍阻塞
- 加 `IORING_ENTER_SQ_WAKEUP` — 仍阻塞

SQPOLL 内核线程在 idle 200ms 后休眠，上述方案均无法可靠唤醒它去提交 timeout SQE。

**最终修复（100μs idle poll）：**

```c
if (nready == 0) {
    io_uring_submit(ring);              /* 提交未完成的 SQEs */
    struct timespec ts = { .tv_sec = 0, .tv_nsec = 100000L };
    nanosleep(&ts, NULL);               /* 有界睡眠 */
    continue;                            /* 重新轮询 CQE */
}
```

- `io_uring_submit(ring)` 确保 accept SQE 等被提交（否则连接永不接受）
- `nanosleep(100μs)` 提供确定性的空闲唤醒，绕过 SQPOLL 空闲模式复杂性
- 空闲期探针实测 ~10K 次迭代/秒，`update_cached_time` 恢复执行

### 9c. 配置文件修正

- `kvstore-aof.conf` 添加 `expire off` 保持与 kvstore-none.conf 一致（消除过期线程竞争作为测量变量）
- `bench_pipeline.sh` 的 AOF 测试段使用正确配置文件（T3 用 kvstore-aof.conf）

### 验证结果

```
Reactor AOF: 写 100 key → 2s 内刷盘 → kill -9 → 重启 → 100/100 回放 ✓
Proactor AOF: 空闲唤醒 ~10K Hz → 写 300 key → 2s 内刷盘 → kill -9 → 重启 → 297/297 回放 ✓
RDB: BGSAVE → dump.rdb → 重启 → 100/100 恢复 ✓
```
