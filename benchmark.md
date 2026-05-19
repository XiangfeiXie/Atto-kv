# KVStore Benchmark Report

> **TODO**：项目完工后做全面测试，重新覆盖以下 TBD 表格。本文档先沉淀
> 当前已验证的硬数据，作为每次回归的基准对照组。

---

## 测试环境

| 项 | 值 |
|---|---|
| 机器 | 8 vCPU (6 P-core + 2 E-core) Intel i7-13620H, VMware VM |
| 内核 | Linux 6.8.0-110-generic |
| 内存 | 8 GB, single NUMA node |
| 编译 | `gcc 13.2`, `-O3 -DNDEBUG -fno-omit-frame-pointer` (Release) |
| 分配器 | jemalloc 5.3.0 (默认启用) |
| io_uring | liburing 2.5 (multishot accept/recv + buf_ring + DEFER_TASKRUN) |
| RDMA | libfabric 1.17.0 (verbs / tcp provider) |
| BPF | libbpf 1.3.0 |

测试工具：`redis-benchmark` (随 Redis 7.x 发行)。

---

## 1. Phase 8.4 锁定基准 (6c, 单机 loopback, redis-benchmark)

> **历史数据，2026-05-17**。`-c 400 -P 10 --threads 6 -n 2000000`，
> `persistence=none`，无 BPF。每行三次取最佳。

| `threads` | `cpu_pin` | SET (rps) | GET (rps) |
|---:|---|---:|---:|
| 1 | auto | 396k | 396k |
| 2 | auto | **492k** | **494k** |
| 4 | auto | 464k | 489k |
| 5 | off  | 492k | 477k |
| 6 | auto | 434k | 493k |

**关键观察**（来自 `assessment-day0.md`）：
- 6c loopback 上 t=2 是数学最优——bench 4 threads + KV 2 worker 正好填满 6 核
- t≥4 不再线性 scaling 是因为 bench client + KV worker 抢同一组 P-core，**不是**锁争用
- 真实瓶颈在 ~2.5 μs/op 的 per-request CPU cost，不是 lock contention

> **注**：8c VM 上 `dev_quick.sh` 默认稳定 ~390k SET/GET（不 taskset 约束）。
> 启动时偶尔出现的 790k 数字不稳定，**不写入简历**。

---

## 2. Phase 3 io_uring 进阶优化（before/after）

> **2026-05-16**。三步累积：multishot accept (3.1) → buf_ring + multishot
> recv (3.2) → SINGLE_ISSUER + DEFER_TASKRUN + COOP_TASKRUN setup flags (3.3)。

| Pipeline | OP | baseline | Phase 3 | Δ |
|---|---|---:|---:|---:|
| P=1  | SET | 18 560  | 25 654  | **+38.2%** |
| P=1  | GET | 18 471  | 22 287  | +20.7% |
| P=4  | SET | 69 348  | 81 169  | +17.0% |
| P=4  | GET | 69 881  | 100 100 | **+43.2%** |
| P=10 | SET | 164 474 | 222 222 | +35.1% |
| P=10 | GET | 163 399 | 198 413 | +21.4% |
| P=20 | GET | 285 714 | 442 478 | **+54.9%** |

**平均 +30%**。低 pipeline 收益最大，符合"syscall 占比越高、io_uring 进阶越甜"
的理论。原始数据：`test/bench/phase3-*.csv`。

---

## 3. Phase 6 单核效率优化（6.1 + 6.2 + 6.4）

> **2026-05-17**。在 Phase 8.4 基准上叠加。

| metric | Phase 8.4 baseline | + Phase 6.1 (no double dict_find) | + Phase 6.2 (rdlock split) | Δ vs baseline |
|---|---:|---:|---:|---:|
| t=1 SET | 330k | 396k | 396k | **+20%** |
| t=1 GET | 304k | 396k | 396k | **+30%** |
| t=2 SET | 491k | 491k | 492k | tie |
| t=2 GET | 491k | 492k | 494k | tie |
| t=4 SET | 434k | 438k | 488k | +12% |
| t=4 GET | 431k | 437k | 489k | +13% |

- Phase 6.1：去掉 `(void)entry;` 后的 `engine_*_raw` 二次 hash + dict_find，
  单核 +20-30%
- Phase 6.2：dispatcher 按命令类型分 rdlock/wrlock，t≥4 GET 路径 +12%
- t=2 已经被 bench client 打满，单核优化看不到，**搬大机器后每个 worker 都吃这份增益**

---

## 4. AOF 持久化损耗（vs Redis 同设置）

> **历史数据**，相同 redis-benchmark `-c 50 -P 1 -n 100000`：

| 场景 | QPS | 损耗 |
|---|---:|---:|
| kvstore 无持久化 | 341,064 | — |
| kvstore AOF + RDB 双开 | 303,582 | **-11%** |
| Redis 无持久化 | 311,915 | — |
| Redis AOF everysec | 188,644 | -40% |

kvstore 在 AOF 开启时损耗只有 Redis 的 1/4——靠的是 mmap ring buffer +
io_uring 异步落盘 + consumer 线程聚合 writev。

---

## 5. 主从同步开销

| 场景 | QPS |
|---|---:|
| 无主从同步 | 289 184 ~ 321 337 |
| WAL 增量复制开启 | 293 427 ~ 295 683 |

条件唤醒优化后（atomic seq + 只在 consumer 睡眠时 eventfd），高负载下 WAL
路径**热路径零 syscall**，影响在测试误差范围内。

---

## 6. 三层 BPF 可观测开销（sockmap 流层）

> `redis-benchmark -c 50 -t set,get`，sockmap 流层观测开 vs 关。

| Pipeline | 无 BPF (req/s) | sockmap-on (req/s) | Δ |
|---|---:|---:|---:|
| P=1 SET | 20 088 | 17 908 | -10.9% |
| P=1 GET | 19 956 | 19 164 | -4.0% |
| P=4 GET | 75 301 | 68 775 | -8.7% |
| P=10 SET | 187k | 183k | < 噪声 |
| P=10 GET | 173k | 180k | < 噪声 |

每条 verdict ~200 ns；P=1 时最显眼，pipelined 场景被摊薄到测量噪声。

---

## 7. BGSAVE 主线程 stall

> Phase 4B.4 fork-COW 点时间快照，threads=4，并发写入下：

| metric | 值 |
|---|---:|
| 全分片 wrlock acquire 耗时 | 0 ms |
| fork() + 父进程释放锁总耗时 | 2-4 ms |
| 并发 SET 吞吐（BGSAVE 期间） | 260k/s（相比稳态 ~390k 略降） |
| 主线程感知 stall | **~0 ms** |

子进程 COW 内运行 no-lock 迭代器 + io_uring 异步 writev，落盘期间主进程
请求无阻塞。

---

## 8. 多 key 并发正确性（Phase 9）

> 不是性能数字，是**正确性测量**。3 worker（cpu_pin 3,4,5），4 客户端
> 并发 4 秒。原始：`test/bench/multi_key_smoke.sh`。

| 测试 | 不变量 | 结果 |
|---|---|---|
| SMOVE A↔B | `|A| + |B| ≡ 1000` | ✅ 1000+0 |
| LMOVE L1↔L2 | `len(L1) + len(L2) ≡ 500` | ✅ 500+0 |
| RPOPLPUSH R1↔R2 | `len(R1) + len(R2) ≡ 500` | ✅ 500+0 |
| BLPOP 跨 worker 唤醒 | LPUSH 在 worker A，BLPOP 在 worker B → 收到回复 | ✅ |
| BLPOP 1s 超时回复 | 1000 ± 50 ms | ✅ 1031 ms |

**6/6 PASS**。Phase 9 之前这几个测试会概率性失元素或 race；修复后稳定。

---

## 9. Lock-free seqlock 短键热缓存（Phase 10）

`src/core/fastpath_cache.{h,c}` — 1024 buckets × 512 B = **512 KB**（L2-resident），
预格式化 RESP 回复，per-bucket 64-bit seqlock（even=stable，odd=writer），
读端最多重试 1 次，写端 CAS-on-seq 单写者。在 dispatcher GET 入口、shard
锁之前查询；SET / DEL / EXPIRE post-hook 维护一致性。

### 9.1 In-process 微基准（`test/bench_fp_inproc.c`）

**为什么不用 redis-benchmark 量这个 feature**：单 op 的 TCP/io_uring 开销
≈ 50–70 µs，把 fast-path 节省的 200 ns 完全淹没。in-process 直接调用 API
才能看清楚 CPU 路径的真实差异。

**单线程公平对比**（两条路径都产出完整 RESP 回复，64 hot keys × 64 B）：

| 路径 | ns/op | Mops/s |
|---|---:|---:|
| `pthread_rwlock_rdlock` + `dict_find` + `snprintf + memcpy(RESP)` | **375** | 2.67 |
| `fp_get`（命中后单次 `memcpy` 预格式化回复） | **200** | 4.94 |
| **加速比** | | **1.90x** |

收益来源：fast-path 在 SET 时**一次**完成 `$len\r\nval\r\n` 拼装并存进 bucket，
GET 命中只需 `memcpy` 一次到 `DynBuf`；dict 路径每次 GET 都要重新 snprintf
header + 拷贝 value。

### 9.2 多读者扩展性（`SAME_SHARD=1`，64 keys 强制压同一 shard）

模拟生产中的 hot-key 倾斜：所有热点 key 落在同一 dict shard，rwlock 成为
单点。

| readers | dict Mops/s | fp_get Mops/s | fp/dict |
|---:|---:|---:|---:|
| 1 | 2.49 | 4.78 | **1.92x** |
| 2 | 2.89 | 5.87 | **2.03x** |
| 4 | 2.88 | 6.74 | **2.34x** |
| 8 | 2.83 | 6.61 | **2.34x** |

**关键观察**：

- dict 路径 1→4 线程基本**不 scale**（2.49→2.88 Mops/s）—— rwlock cacheline
  ping-pong 把吞吐锁死在 ~2.9 Mops/s 上限。
- `fp_get` 完全 lock-free，4 线程仍能达到 6.74 Mops/s，**比 dict 路径快 134%**。
- 8 线程进一步开始受内存带宽限制；这是 seqlock 数据结构本身的上限，不是锁竞争。

### 9.3 不要过度承诺：均匀随机工况是中性

`test/bench/bench_fastpath.sh`（`redis-benchmark -t get`，`key:__rand_int__`，
200 k key 均匀分布，d=64，threads=2）：

| 场景 | baseline QPS | fastpath QPS | 变化 |
|---|---:|---:|---:|
| 100% hit | 8286 | 8305 | **+0.2%** |
| 0% hit | 8247 | 8227 | **−0.2%** |

均匀随机大工作集**没有时间局部性**，fast-path 命中前也要查一次表 → 等价于
为每个 GET 多付 1 次 L2 hit。我们把 buckets 从 4096 降到 1024（2 MB → 512 KB，
能稳进 L2）后，这部分开销几乎归零（−27% → ±0.2%）。

总结：**fast-path 是 CPU-cycle 优化，不是网络吞吐优化**。它的价值在饱和负载
下的 p99 延迟、CPU 释放给 AOF / 复制等其他工作，以及 hot-key 倾斜下的
扩展性，而不是在 redis-benchmark 上把 QPS 数字推高。

### 9.4 正确性与 TSAN

- `test/fp_cache_test.c`：sanity + hash 冲突 + 1 writer + 3 readers × 5 s 压测，
  `torn_reads = 0`，`writer_collisions = 0`。
- `test/bench/fastpath_tsan_smoke.sh`：相同压测在 TSAN 下 **0 warning**（seqlock
  payload 上的数据竞争通过 `__attribute__((no_sanitize_thread))` 标记为有意
  设计，而不是用互斥消除）。

### 9.5 复现命令

```bash
# 单元 + 多线程压测
cmake --build build --target fp_cache_test && ./build/fp_cache_test 5

# TSAN 回归门
./test/bench/fastpath_tsan_smoke.sh 5

# CPU 微基准（默认 64 keys × 64B，多线程跑到 8）
cmake --build build --target bench_fp_inproc
OPS_PER_RUN=20000000 REPEATS=3 MT_OPS=10000000 MAX_THREADS=8 \
    ./build/bench_fp_inproc
SAME_SHARD=1 MT_OPS=10000000 MAX_THREADS=8 ./build/bench_fp_inproc

# 端到端（uniform random，预期 ~neutral）
sudo BENCH_N=300000 BENCH_C=200 BENCH_P=10 WARMUP_N=200000 DATA_SIZE=64 \
    ./test/bench/bench_fastpath.sh
```

---

## 10. 测试基础设施汇总

| 套件 | 内容 | 状态 |
|---|---|---|
| `test/smoke/run_all.sh` | string / list / hash / set / zset 5 类型功能验证 | ✅ |
| `test/bench/multi_thread_smoke.sh` | threads=4 下 12 项功能（BLPOP / AOF reload / RDB BGSAVE）| ✅ 12/12 |
| `test/bench/multi_key_smoke.sh` | 3 worker 多 key 并发正确性 | ✅ 6/6 |
| `test/bench/tsan_lock_smoke.sh` | TSAN focused：rdlock readers / lazy-expire upgrade / cross-shard | ✅ 6/6 + 0 warnings |
| `build/fp_cache_test` | seqlock 单元 + 1W+3R 压测，torn=0 / collisions=0 | ✅ |
| `test/bench/fastpath_tsan_smoke.sh` | fp_cache TSAN 回归门，0 warning | ✅ |
| `build/bench_fp_inproc` | fp_get 1.9x（单线程）/ +134%（4 线程同 shard）| ✅ |
| `test/bench/baseline_capture.sh` | t={1..6} × pin={off,auto} sweep | sweep ready |
| `test/bench/soak.sh` | 10/30 min 长跑：内存 / fd / DBSIZE 漂移检查 | 10min PASS, 30min TBD |
| ASAN 构建 | full multi-thread smoke 12/12 PASS | ✅ 0 findings |
| TSAN 构建 | tsan_lock_smoke + multi-thread smoke | ✅ 0 warnings |

`smoke-all` 一键跑 Release + ASAN + TSAN：30/30 PASS。

---

## TODO（项目完工后全量测试时补齐）

- [ ] **vs Redis 头对头表**（`test/bench/vs_redis.sh`）—— P=1/4/10/20 SET/GET
- [ ] **t-sweep on 8c P-core**（`baseline_8c_pcore.sh`）—— 当前只有 6c 数据
- [ ] **2-VM 物理隔离吞吐**（KV 独占 6 P-core, bench 在另一台 VM）
- [ ] **`perf top` 抓 30s** —— 找出 ~2.5 μs/op 都花在哪（指导 Phase 6.3 是否做 gperf）
- [ ] **kill -9 重启 + AOF replay 完整性** —— 1M 条 SET 写入后强杀，重启后 DBSIZE 应一致
- [ ] **30 min soak**（已有脚本，待跑）
- [ ] **sockmap fast-path before/after**（如 Phase 9.1 实施）

---

## 数据复现

```bash
# Phase 8.4 baseline
bash test/bench/baseline_capture.sh --baseline   # 写入 baseline_<host>_<nproc>c.txt

# Phase 3 io_uring 进阶 before/after
ls test/bench/phase3-*.csv

# Phase 9 多 key 并发
bash test/bench/multi_key_smoke.sh

# 一键回归（Release + ASAN + TSAN）
cmake --build build --target smoke-all
```
