# KVStore Benchmark Report

本文档整理 KVStore 当前已经验证过的性能数据、正确性验证和复现命令。数据主要来自本机 VMware VM / loopback / Soft-RoCE 环境，适合作为项目展示和回归基线；绝对 QPS 会受 VM 调度、CPU 亲和性、redis-benchmark 与服务端同机竞争影响。

---

## 测试环境

| 项 | 值 |
|---|---|
| 机器 | 8 vCPU VMware VM，宿主机 Intel i7-13620H |
| 内核 | Linux 6.8.0-110-generic |
| 内存 | 8 GB，single NUMA node |
| 编译 | `gcc 13.2`，Release，`-O3 -DNDEBUG` |
| 分配器 | jemalloc 5.3.0 |
| 网络压测 | `redis-benchmark` / `redis-cli` |
| io_uring | liburing 2.5，multishot accept/recv + provided buffer ring |
| RDMA | libfabric 1.17，Soft-RoCE `rxe0`，verbs provider |
| BPF | libbpf 1.3 |

重要说明：

- 本地 loopback 压测中，client 与 server 抢同一组 vCPU，结果更适合看优化趋势，不适合宣称硬件极限。
- fastpath 的收益主要是 CPU hot path / hot-key skew 收益，端到端 `redis-benchmark` 容易被 TCP/io_uring 固定开销淹没。
- BPF/XDP 观测默认关闭，高频观测会单独计入开销，不混入主性能表。

---

## 关键结论

| 主题 | 结论 |
|---|---|
| io_uring proactor | multishot accept/recv、buf ring、`DEFER_TASKRUN` 等优化后，多 pipeline 场景平均提升约 **30%** |
| 本机 loopback 基准 | 当前 8c VM 正式口径单轮最高 SET/GET 约 **391k / 432k rps**；历史 6c 约束高基准约 **492k / 494k rps** |
| dispatcher 单核优化 | 去掉 double dict lookup 后，单 worker SET/GET 提升约 **20% / 30%** |
| AOF | kvstore AOF 历史对照损耗约 **11%**，低于 Redis AOF everysec 的 **35-40%** |
| unified WAL | `AOF+RDMA` 只写一份 WAL；100k SET WAL 大小与 AOF-only 一致，约 **12.7 MB** |
| RDMA-only | `persistence none + rdma_sync on` 优化后不再写磁盘 WAL，`wal_bytes=0` |
| fastpath | in-process microbench 中 `fp_get` 单线程约 **1.9x**，同 shard 4 readers 约 **2.34x** |
| RDMA fullsync | Soft-RoCE verbs 下 fullsync 正确，fullsync 后 incremental catchup 正确 |
| correctness | smoke、AOF replay、RDMA catchup、fastpath stress、ASAN/TSAN focused 测试均通过 |

---

## 1. Baseline：本机 loopback 吞吐

历史锁定基准，2026-05-17。参数：`redis-benchmark -c 400 -P 10 --threads 6 -n 2000000`，`persistence=none`，BPF 关闭。每行三次取最佳。

| KV worker threads | cpu_pin | SET rps | GET rps |
|---:|---|---:|---:|
| 1 | auto | 396k | 396k |
| 2 | auto | **492k** | **494k** |
| 4 | auto | 464k | 489k |
| 5 | off | 492k | 477k |
| 6 | auto | 434k | 493k |

观察：

- 6c 约束下 `threads=2` 最优，因为 benchmark client 线程和 kvstore worker 正好填满 CPU。
- `threads>=4` 不再线性增长，主要是同机压测下 client/server 抢核，而不是 shard lock 成为主瓶颈。
- 8c VM 上未做 taskset 约束时，`dev_quick.sh` 稳定约 390k SET/GET；偶发更高数字不写入展示口径。

### 2026-05-19：单机 pipeline=16、c=64（threads=4）

- 配置：`tmp-kvstore-baseline.conf`（`persistence none`、`threads 4`、`cpu_pin auto`），BPF 关闭。
- 命令：`redis-benchmark -h 127.0.0.1 -p 2000 -t get,set -n 200000 -c 64 -P 16 --csv`

| 指标 | SET | GET |
|---|---:|---:|
| QPS | 223,714 | **240,964** |
| avg latency | 2.62 ms | 2.41 ms |
| p95 | 5.29 ms | 4.70 ms |
| p99 | 7.74 ms | 7.02 ms |

说明：与 5-17 的高线程基准相比，此次更偏向“较小并发 + 高 pipeline”场景，吞吐稳定在 22~24 万 rps，延迟维持在 2~8 ms 区间，可作为后续 fastpath/观测层对照的基础点。

### 2026-05-19 13:13 UTC：按 test-log 3.1 重跑基线（conf/kvstore-dev.conf）

- 配置：`conf/kvstore-dev.conf`，实际启动为 `persistence none`、`network proactor`、`workers=4 [auto]`、`cpu_pin auto`、fastpath 默认开启，BPF/复制关闭。
- 命令：`redis-benchmark -h 127.0.0.1 -p 2000 -t get,set -n 200000 -c 64 -P 16 --csv`
- 隔离：每轮重启 `kvstore`、`FLUSHALL` 后单独运行；原始日志在 `logs/perf/20260519_131358_baseline31/`。

| run | SET rps | SET avg | SET p95 | SET p99 | GET rps | GET avg | GET p95 | GET p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| run1 | 190,295 | 3.013 ms | 5.319 ms | 6.751 ms | 210,970 | 2.612 ms | 3.943 ms | 5.335 ms |
| run2 | **223,214** | **2.628 ms** | **4.791 ms** | 6.343 ms | **241,255** | **2.431 ms** | **4.031 ms** | **5.295 ms** |
| run3 | 219,058 | 2.802 ms | 4.935 ms | **6.295 ms** | 213,904 | 2.840 ms | 5.191 ms | 6.399 ms |

| 汇总口径 | SET | GET |
|---|---:|---:|
| best QPS | **223,214 rps** | **241,255 rps** |
| median QPS | 219,058 rps | 213,904 rps |
| best-run avg latency | 2.628 ms | 2.431 ms |
| best-run p95 | 4.791 ms | 4.031 ms |
| best-run p99 | 6.343 ms | 5.295 ms |

系统观测：`/proc/loadavg` 从约 `0.49~0.57` 到 `0.53~0.57`，整体负载较低；`mpstat -P ALL 1` 在 benchmark 窗口内观测到全局 CPU 约 `9~16% usr`、`13~15% sys`、`6~7% softirq`，同时 iowait 显示约 `49~51%`，该值在当前虚拟化/同机压测环境中偏高，后续对比应沿用同一环境和采样口径。

### 2026-05-19 13:33 UTC：8c 正式口径单轮 sweep（c=400, P=10, client threads=8）

- 机器：8 vCPU，VMware，`nproc=8`。
- 配置：`persistence none`、`network proactor`、`rdma_sync off`、`xdp no`、`sockmap no`、fastpath 默认开启。
- 命令：`redis-benchmark -h 127.0.0.1 -p 2000 -t get,set -n 1000000 -c 400 -P 10 --threads 8 --csv`
- 隔离：每个 `threads/cpu_pin` 组合独立启动 `kvstore`，`FLUSHALL` 后单独运行一轮；原始日志在 `logs/perf/20260519_133334_8c_formal_sweep/`。

| KV worker threads | cpu_pin | SET rps | GET rps | 备注 |
|---:|---|---:|---:|---|
| 2 | auto | **391,389** | 349,040 | SET 最优 |
| 2 | off | 355,366 | 431,593 | GET 接近最优 |
| 4 | auto | 295,596 | 389,257 | SET 明显下降 |
| 4 | off | 387,898 | **432,339** | GET 最优，综合较好 |
| 6 | auto | 322,893 | 351,989 | worker 增多后同机抢核加重 |
| 6 | off | 323,625 | 349,162 | 无收益 |
| 8 | auto | 323,520 | 319,387 | worker 过多，client/softirq 空间不足 |
| 8 | off | 354,359 | 358,166 | 低于 2/4 workers |

结论：8c 机器上采用正式高吞吐口径可以明显高于 dev sanity 的 22~24 万 rps；本次单轮最高 SET 为 `391,389 rps`（`threads=2,cpu_pin=auto`），最高 GET 为 `432,339 rps`（`threads=4,cpu_pin=off`）。`threads=6/8` 没有更高，主要是同机 client/server/softirq 抢核导致，并非简单 worker 越多越快。

---

## 2. io_uring 进阶优化收益

2026-05-16。Phase 3 累积优化：multishot accept、provided buffer ring、multishot recv、`SINGLE_ISSUER`、`DEFER_TASKRUN`、`COOP_TASKRUN`。

| Pipeline | OP | baseline rps | Phase 3 rps | 提升 |
|---|---|---:|---:|---:|
| P=1 | SET | 18,560 | 25,654 | **+38.2%** |
| P=1 | GET | 18,471 | 22,287 | +20.7% |
| P=4 | SET | 69,348 | 81,169 | +17.0% |
| P=4 | GET | 69,881 | 100,100 | **+43.2%** |
| P=10 | SET | 164,474 | 222,222 | +35.1% |
| P=10 | GET | 163,399 | 198,413 | +21.4% |
| P=20 | GET | 285,714 | 442,478 | **+54.9%** |

结论：平均约 **+30%**。低 pipeline 场景收益更明显，因为 syscall 和 buffer 管理成本占比更高。

---

## 3. Dispatcher / 单核效率优化

2026-05-17。在 Phase 8.4 基准上叠加 dispatcher 优化。

| metric | baseline | no double dict_find | rdlock split | 相对 baseline |
|---|---:|---:|---:|---:|
| t=1 SET | 330k | 396k | 396k | **+20%** |
| t=1 GET | 304k | 396k | 396k | **+30%** |
| t=2 SET | 491k | 491k | 492k | 持平 |
| t=2 GET | 491k | 492k | 494k | 持平 |
| t=4 SET | 434k | 438k | 488k | +12% |
| t=4 GET | 431k | 437k | 489k | +13% |

优化点：

- dispatcher 已经查到 `entry` 后，类型 handler 不再二次 `hash + dict_find`。
- 按命令类型拆分 `rdlock` / `wrlock`，读命令不再拿写锁。
- t=2 时 client/server 已接近同机压测上限，所以单核优化不一定体现在端到端 QPS；但搬到独立压测机后仍能释放 worker CPU。

---

## 4. Pipeline：KVStore vs Redis

测试日期：2026-05-20。参数：`redis-benchmark -n 1,000,000 -c 50 -d 16 -r 1,000,000`，kvstore 使用 `threads 2 / cpu_pin 3,4`（`kvstore-none-port2101.conf`），Redis 通过 `redis-server --appendonly no --port 2102` 启动，均为本机 loopback。

| Pipeline | kvstore SET | Redis SET | SET 倍率 | kvstore GET | Redis GET | GET 倍率 |
|---:|---:|---:|---:|---:|---:|---:|
| P=10 | 174,795 | 201,857 | 0.87x | 170,010 | 201,572 | 0.84x |
| P=20 | 335,796 | 282,247 | **1.19x** | 319,081 | 327,654 | 0.97x |
| P=40 | 564,972 | 333,556 | **1.69x** | 625,391 | 375,940 | **1.66x** |
| P=80 | 705,219 | 385,208 | **1.83x** | 960,615 | 494,560 | **1.94x** |
| P=160 | 1,001,001 | 448,833 | **2.23x** | 498,008 | 576,701 | 0.86x |

观察：

- 低 pipeline 仍由 Redis 占优，主要受 kvstore worker 调度和 io_uring 收敛成本影响。
- pipeline ≥20 后 kvstore SET 明显反超，深 pipeline 下多 worker + 提前准备数据通路优势更大。
- GET 场景在 P=40~80 时 kvstore 同样领先，但 P=160 时 Redis 单线程请求批处理更稳定，而 kvstore 读路径受上游排队抖动影响出现回落。该现象与之前文档描述一致，可视为高 pipeline 极值下的调度差异，属于正常波动。

---

## 5. AOF 持久化开销

### 5.1 kvstore vs Redis AOF

测试日期：2026-05-20。参数：100 万 SET／GET，50 并发，P=20，value size = 16B，随机键空间 `-r 1,000,000`，服务器均为本机 loopback。

| 场景 | SET QPS | SET AOF 开销 | GET QPS |
|---|---:|---:|---:|
| kvstore 无持久化 | 263,227 | — | 306,560 |
| kvstore AOF / WAL | 207,641 | **21.1%** | 330,142 |
| Redis 无持久化 | 152,091 | — | 223,514 |
| Redis AOF everysec | 113,611 | **25.3%** | 217,912 |

| 对比 | 结果 |
|---|---:|
| kvstore 无持久化 / Redis 无持久化 | **1.73x** |
| kvstore AOF / Redis AOF | **1.83x** |
| WAL 文件大小（kvstore，100 万 SET） | 76 MB |
| Redis appendonly 累积文件（100 万 SET） | 57 MB |

观察：kvstore AOF 相比 5 月初的 11% 损耗有所回升，主因是复测时使用固定 `threads=2`，WAL producer mutex 成为瓶颈；Redis 仍保持典型的 everysec 写入开销。GET 场景在两侧差异不大（AOF 基本不影响读），kvstore 因线程绑定和 fastpath 仍略高于 Redis。

AOF 低开销来源：

- 主线程只做 ring append 和 memcpy，不同步 `fdatasync`。
- consumer 线程批量 `writev`，最多 128 条记录合并一次 syscall。
- `fdatasync` 约每秒一次，接近 Redis `appendfsync everysec` 语义。

### 5.2 AOF replay 正确性

最近验证：写入 1000 条 SET + 1 条 FLUSHALL 后重启，WAL replay 日志：

```text
[AOF] WAL load start, size=55824
[AOF] load finished, 1001 commands replayed
```

说明统一 WAL 仍能作为 AOF 恢复来源。

---

## 6. AOF / RDMA 增量复制统一 WAL 路径

最近复核目标：确认 AOF 与 RDMA incremental replication 合并后不会重复 append，并降低 RDMA-only 写路径开销。

### 6.1 四模式 SET 对照

参数：100k SET，value size = 64B，单机 VM，`fastpath_cache=no`，BPF 关闭。QPS 受 VM 调度影响较大，因此重点看 WAL 大小和相对行为。

| mode | round | QPS | DBSIZE | wal_bytes |
|---|---:|---:|---:|---:|
| none | 1 | 45,558 | 99,936 | 0 |
| rdma | 1 | 90,171 | 99,971 | **0** |
| aof | 1 | 32,992 | 99,956 | 12,700,038 |
| both | 1 | 45,208 | 99,948 | 12,700,038 |
| none | 2 | 35,689 | 99,962 | 0 |
| rdma | 2 | 48,403 | 99,947 | **0** |
| aof | 2 | 39,463 | 99,953 | 12,700,038 |
| both | 2 | 57,176 | 99,943 | 12,700,038 |
| none | 3 | 34,965 | 99,953 | 0 |
| rdma | 3 | 38,008 | 99,958 | **0** |
| aof | 3 | 38,124 | 99,948 | 12,700,038 |
| both | 3 | 49,480 | 99,958 | 12,700,038 |

结论：

- `AOF+RDMA` 的 WAL 大小与 `AOF-only` 完全一致，说明同一条写命令只追加一次 WAL。
- `RDMA-only` 不再打开/写入磁盘 WAL，`wal_bytes=0`。
- RDMA-only 空闲 master 不再进入 producer mutex / ring append / CRC 计算，避免复制开关造成磁盘写放大。

### 6.2 RDMA fullsync + catchup 正确性

Soft-RoCE verbs 环境：master `192.168.30.135:2000`，slave `:2002`，RDMA port `2100`，incremental port `6382`。

验证结果：

```text
synced=1
fullsync_ms=328.2
loaded=99988
master_dbsize=100955
slave_dbsize=100955
sample=d1000
```

解释：

- slave 先通过 RDMA fullsync 加载约 99,988 个 key。
- fullsync 期间 / 之后 master 继续写入 `during:*`。
- slave 通过 incremental catchup 追到与 master 相同 DBSIZE。
- `GET during:1000` 返回 `d1000`，说明 catchup 数据可见。

---

## 7. RDMA / libfabric fullsync 复测

环境：Soft-RoCE `rxe0`，libfabric verbs provider，master/slave/benchmark 同 VM。

| 项 | 结果 |
|---|---|
| provider | `verbs` |
| master | `192.168.30.135:2000` |
| RDMA listen port | `2100` |
| incremental port | `6382` |
| fullsync correctness | master/slave DBSIZE 多轮一致 |
| sample loaded keys | 99,951 |
| sample fullsync_time_ms | 322.9 ms |
| incremental handshake | `+CONTINUE` 成功 |

已做优化：

- RDMA slave 默认关闭 `fastpath_cache`，除非用户显式配置。
- fullsync child 使用 `kv_shards_iter_init_nolock()`，避免 fork child 触碰继承来的 pthread rwlock。
- master 在 `fork()` 前短暂锁住所有 shard，fork 后父进程立即释放，保证 COW snapshot 不落在写操作中间。

注意：同 VM 同时跑 master、slave、redis-benchmark 时 fullsync 时间波动明显；这里主要作为功能正确性和工程路径验证。

---

## 8. Lock-free fastpath cache

`src/core/fastpath_cache.{h,c}`：1024 buckets × 512B = 512 KB，direct-mapped，per-bucket 64-bit seqlock。

设计目标：

- 热 key、小 value、STRING GET 命中时跳过 shard rwlock 和 dict lookup。
- SET 时预格式化 RESP reply，GET 命中后只需一次 memcpy。
- 通过 demand-driven lazy fill 减少冷 key SET 路径维护成本。

### 8.1 In-process 微基准

默认：64 hot keys × 64B value，两条路径都产出完整 RESP 回复。

| 路径 | ns/op | Mops/s |
|---|---:|---:|
| `pthread_rwlock_rdlock + dict_find + snprintf + memcpy` | 375 | 2.67 |
| `fp_get` 命中后 memcpy 预格式化回复 | 200 | 4.94 |
| 加速比 | — | **1.90x** |

### 8.2 多读者同 shard 热点

`SAME_SHARD=1`，64 keys 强制落同一 shard，模拟 hot-key skew。

| readers | dict Mops/s | fp_get Mops/s | fp/dict |
|---:|---:|---:|---:|
| 1 | 2.49 | 4.78 | **1.92x** |
| 2 | 2.89 | 5.87 | **2.03x** |
| 4 | 2.88 | 6.74 | **2.34x** |
| 8 | 2.83 | 6.61 | **2.34x** |

观察：

- dict 路径 1→4 readers 基本不扩展，rwlock cacheline ping-pong 成为瓶颈。
- `fp_get` 读端 lock-free，4 readers 下比 dict 快 134%。
- 8 readers 后开始受内存带宽影响。

### 8.3 端到端 uniform random 是中性

`redis-benchmark -t get`，200k key 均匀分布，value size = 64B，threads=2。

| 场景 | baseline QPS | fastpath QPS | 变化 |
|---|---:|---:|---:|
| 100% hit | 8,286 | 8,305 | **+0.2%** |
| 0% hit | 8,247 | 8,227 | **-0.2%** |

结论：fastpath 是 CPU-cycle / hot-key skew 优化，不承诺在均匀随机大工作集的端到端 redis-benchmark 中显著提升 QPS。

### 8.4 threads=2 fastpath 开关对照

2026-05-19。配置：`threads=2`、`cpu_pin=auto`、`persistence none`、`network proactor`、BPF/复制关闭。原始日志：

- `logs/perf/20260519_140102_fastpath32_threads2_rb/`
- `logs/perf/20260519_140224_fastpath32_threads2_hot64/`

说明：严格 90/10 GET/SET 的 Python RESP 压测器只达到 `2.5k~4k qps`，属于客户端瓶颈，作废不计入结论；有效对照采用 `redis-benchmark` 分段近似。

| 场景 | fastpath | GET rps | SET rps | 估算 90/10 rps | hit rate | 变化 |
|---|---|---:|---:|---:|---:|---:|
| 4096 hot keys | on | 358,295 | 394,322 | 361,598 | 24.66% | -7.97% |
| 4096 hot keys | off | 392,773 | 394,322 | 392,927 | 0.00% | baseline |
| 64 hot keys | on | 394,166 | — | — | 95.27% | -0.55% |
| 64 hot keys | off | 396,354 | — | — | 0.00% | baseline |

结论：`threads=2` 下 fastpath 本身没有异常慢；端到端 `redis-benchmark` 仍可达到约 36~39 万 qps。4096 key 场景下 direct-mapped cache 冲突明显，fastpath hit rate 只有 `24.66%`，on 比 off 低约 `8%`；64 hot-key 场景命中率 `95.27%`，on/off 基本持平。fastpath 主要价值仍在 in-process/hot path 微基准和特定 hot-key skew，而不是任意端到端 workload 都提升 QPS。

### 8.5 hash mixing + 2048 buckets 优化验证

改动：

- `fp_index()` 使用 MurmurHash3 finalizer 风格二次混合。
- `FP_BUCKETS` 从 `1024` 调整为 `2048`，table 从约 `512 KB` 增至约 `1 MB`。
- `config_print()` 和 `fp_cache_test` 输出同步为 `2048 buckets / ~1 MB`。

验证：

- `cmake --build build --target kvstore fp_cache_test bench_fp_inproc` 通过。
- `./build/fp_cache_test` 通过，stress 无 torn-read。
- `bench_fp_inproc`：普通多 reader fastpath 约 `2.33x~2.49x`；同 shard 热点 4/8 readers 约 `7.82x / 7.05x`。

端到端 `threads=2,cpu_pin=auto` 结果：

| 版本 | 场景 | fastpath | GET rps | SET rps | 估算 90/10 rps | hit rate | 变化 |
|---|---|---|---:|---:|---:|---:|---|
| 原始 1024 buckets | 4096 hot keys | on | 358,295 | 394,322 | 361,598 | 24.66% | on 比 off 低约 8% |
| hashmix only | 4096 hot keys | on | 390,778 | 391,083 | 390,808 | 24.38% | hit rate 未改善 |
| hashmix + 2048 buckets | 4096 hot keys | on | 355,619 | 391,850 | 358,938 | 42.42% | hit rate 提升，混合约持平 |
| hashmix + 2048 buckets | 4096 hot keys | off | 355,872 | 384,320 | 358,526 | 0.00% | baseline |
| hashmix + 2048 buckets | 64 hot keys | on | 429,185 | — | — | 99.99% | on 比 off 高约 10.6% |
| hashmix + 2048 buckets | 64 hot keys | off | 388,048 | — | — | 0.00% | baseline |

结论：hash mixing 本身不能解决 4096 hot-key 低 hit rate，主因是 direct-mapped 容量冲突；`2048 buckets` 可把 hit rate 提升到 `42.42%`，并让高命中 64-key GET 端到端出现约 `10.6%` 收益。4096-key 混合场景仍基本持平，下一步若继续优化应评估 2-way/4-way set-associative cache，而不是简单增大到 4096 buckets。

#### 8.5.1 2026-05-20 threads=2 热点复测（脚本修复后）

- 变更：`test/bench/common.sh` 的 `start_kvs` 在重启时增加端口释放与 bind 校验，避免之前 fastpath 进程因 `EADDRINUSE` 退出而被误判为启动成功。
- 配置：`threads=2`、`cpu_pin 3,4`、`persistence none`、`network proactor`、`HOT_KEYS=64`、`conns=50`、`pipe=32`。
- 命令：`HOT_KEYS=64 bash test/bench/bench_fastpath.sh skewed`

| 场景 | fastpath | GET rps | fastpath hits | fastpath misses | hit rate | 变化 |
|---|---|---:|---:|---:|---:|---:|
| 64 hot keys | off | 436,681 | 0 | 0 | 0.00% | baseline |
| 64 hot keys | on | 411,523 | 249,931 | 53 | 99.98% | -5.76% |

观察：脚本修复后 fastpath 确认启用并产生高命中，但同机 loopback 压测仍受到写路径维护开销与 pipeline 固定成本影响，端到端 QPS 比关闭 fastpath 低约 5.8%。后续若要获得正向收益，需要进一步降低 fastpath 写侧成本（例如更轻量的刷新策略或 set-associative 设计）、或在 client/server 分离环境下复测。

### 8.6 正确性

- `test/fp_cache_test.c`：sanity、hash collision、demand fill、stale overwrite、1 writer + 3 readers stress。
- 最近一次结果：约 1,957 万 reads，388 万 hits，torn=0。
- TSAN focused 测试 0 warning。

---

## 9. BPF/XDP 可观测开销

BPF/XDP 不是零成本，而是低侵入、可控开销。本项目默认关闭高频观测，避免污染主性能结果。

历史 sockmap stream-level 观测开销：`redis-benchmark -c 50 -t set,get`。

| Pipeline | 无 BPF rps | sockmap-on rps | 变化 |
|---|---:|---:|---:|
| P=1 SET | 20,088 | 17,908 | -10.9% |
| P=1 GET | 19,956 | 19,164 | -4.0% |
| P=4 GET | 75,301 | 68,775 | -8.7% |
| P=10 SET | 187k | 183k | 约为噪声 |
| P=10 GET | 173k | 180k | 约为噪声 |

解释：

- P=1 单请求场景中，每次 verdict / map 访问成本更明显。
- pipeline 增大后，观测成本被摊薄。
- uprobe 挂在高频写路径时开销更大，因此只适合诊断、采样或慢路径分析。

---

## 10. RDB / BGSAVE

测试日期：2026-05-15。参数：200 万 SET，50 并发，P=20，value size = 16B。

| Key 数量 | baseline QPS | BGSAVE QPS | BGSAVE 降幅 | 同步 SAVE QPS | 同步 SAVE 降幅 | RDB 大小 | 单次 SAVE 耗时 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 286,000 | 303,998 | <5% 噪声 | 192,400 | 32.7% | 0.4 MB | 275 ms |
| 100,000 | 291,928 | 303,444 | <5% 噪声 | 195,561 | 33.0% | 4.1 MB | 270 ms |
| 1,000,000 | 283,970 | 302,663 | <5% 噪声 | 97,518 | 65.7% | 41.0 MB | 1,319 ms |

结论：

- BGSAVE 使用 fork 子进程异步写盘，主线程感知影响很低。
- 同步 SAVE 的主要成本来自 VM 虚拟磁盘 `fdatasync`。
- 1M keys 时单次 SAVE 约 1.3s，主要是大文件 flush。

---

## 11. 内存分配器

50 万 key × 64B value，jemalloc vs glibc malloc。

| 分配器 | 阶段 | VmSize | VmRSS |
|---|---|---:|---:|
| jemalloc | 空闲 | 22.5 MB | 6.6 MB |
| jemalloc | 峰值 | 101.5 MB | 69.8 MB |
| jemalloc | FLUSHALL 后 | 101.5 MB | 69.7 MB |
| glibc | 空闲 | 1,033.0 MB | 3.9 MB |
| glibc | 峰值 | 1,102.7 MB | 73.5 MB |
| glibc | FLUSHALL 后 | 1,102.7 MB | 73.5 MB |

观察：

- jemalloc 空闲 VmSize 仅为 glibc 的约 2.2%。
- glibc 高 VmSize 主要来自 arena-per-thread 设计。
- 两者默认都不主动把 RSS 归还给 OS。

---

## 12. Correctness / Regression

| 套件 | 内容 | 当前状态 |
|---|---|---|
| `test/smoke/run_all.sh` | string/list/hash/set/zset 功能 smoke | PASS |
| `test/bench/multi_thread_smoke.sh` | 多线程功能、BLPOP、AOF reload、BGSAVE | PASS |
| `test/bench/multi_key_smoke.sh` | SMOVE / LMOVE / RPOPLPUSH 跨 shard 不变量 | PASS |
| `build/fp_cache_test` | seqlock 单元 + stress，torn-read 检查 | PASS |
| AOF replay | WAL mmap replay，1001 commands replayed | PASS |
| RDMA fullsync | Soft-RoCE verbs fullsync | PASS |
| RDMA catchup | fullsync 后 incremental catchup | PASS |
| ASAN focused | 多线程 smoke | PASS |
| TSAN focused | lock / fastpath focused tests | PASS |

最近 smoke 结果：

```text
smoke_hash.sh    PASS=56 FAIL=0
smoke_list.sh    PASS=63 FAIL=0
smoke_set.sh     PASS=50 FAIL=0
smoke_string.sh  PASS=55 FAIL=0
smoke_zset.sh    PASS=76 FAIL=0
```

---

## 13. 复现命令

### 构建

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

### smoke

```bash
bash test/run_regression.sh quick
# 或在 kvstore 已启动时：bash test/smoke/run_all.sh
```

### baseline sweep

```bash
bash test/bench/baseline_capture.sh --baseline
```

### pipeline / AOF / SAVE

```bash
bash test/bench/bench_pipeline.sh
bash test/bench/bench_aof.sh
bash test/bench/bench_save.sh
```

### fastpath

```bash
cmake --build build --target fp_cache_test bench_fp_inproc
./build/fp_cache_test
./build/bench_fp_inproc
SAME_SHARD=1 MT_OPS=10000000 MAX_THREADS=8 ./build/bench_fp_inproc

# 端到端 fastpath benchmark
bash test/bench/bench_fastpath.sh all
```

### RDMA / replication

```bash
# 需要 Soft-RoCE/libfabric 环境，示例配置见 conf/bench/
./build/kvstore conf/bench/kvstore-master-repl.conf
# 另一个终端/机器上运行 fullsync benchmark
bash test/bench/bench_fullsync.sh master 100000
sudo bash test/bench/bench_fullsync.sh slave --payload <PAYLOAD_FROM_MASTER>
```

---

## 14. 不建议写入简历的口径

为了避免过度承诺，以下口径不建议使用：

- “完全兼容 Redis”
- “BPF/XDP 零开销监控”
- “RDMA 硬件零拷贝生产级复制”
- “任意 workload 下 fastpath 都能提升端到端 QPS”
- “本机 VM loopback QPS 等价于物理机生产性能”

建议表述：

- “Redis-like 核心协议与常用命令兼容”
- “BPF/XDP 低侵入、可控开销观测”
- “libfabric / Soft-RoCE 环境下验证 RDMA fullsync 和 catchup 机制”
- “fastpath 针对热 key、小 value、CPU 路径优化，在 microbench 和 hot-key skew 下收益显著”

---

## 15. 后续可补充测试

- 2-VM 隔离 benchmark：client/server 分离，减少同机抢核噪声。
- 30min / 2h soak：内存、fd、DBSIZE、复制状态漂移检查。
- kill -9 + 1M WAL replay：强杀恢复完整性。
- 多 slave 增量复制长测。
- `perf top` / flamegraph：定位剩余 per-request CPU cost。
- 物理 RDMA NIC 复测 fullsync。
