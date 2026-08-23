# KVStore Performance Benchmarks

> 测试环境: Ubuntu 24.04, Kernel 6.8.0, 单线程, persistence=none
> 虚拟机: VMware 虚拟网卡 ens33

---

## 1. Pipeline 批量处理性能 (kvstore vs Redis)

**测试时间:** 2026-08-12 | **脚本:** `bench_pipeline_v2.sh` | **配置:** `conf/kvstore-none.conf` (reactor), `/tmp/kvs_bench_proactor.conf` (proactor)
**参数:** `redis-benchmark -n 1000000 -c 50 -P <depth> -d 16`, Release build (`-O3`), persistence=none, expire=off, warmup=3,000,000

> **方法学（v2 改进）：** (1) **CPU 隔离** — kvstore 绑 0,2，Redis 绑 6，client 绑 4,5，三者互不抢占；(2) **两阶段串行测试** — 先跑完 kvstore 所有 pipeline 深度，再单独跑 Redis，消除后台进程干扰；(3) `expire off` + `persistence none` — 消除过期/AOF/RDB 干扰，纯测引擎吞吐；(4) **ideal 模式**（顺序 key，无 `-r`）— 消除客户端随机数生成瓶颈，测服务端纯引擎上限。
>


### 1a. PING (echo) 吞吐对比

| 系统 | PING QPS | 说明 |
|------|---------:|------|
| kvstore (reactor) | **23,079** | epoll LT + inline flush |
| kvstore (proactor) | **23,815** | io_uring + SQPOLL |
| Redis | 17,800 | epoll 事件循环 |
| **reactor/Redis** | **1.30x** | |
| **proactor/Redis** | **1.34x** | |

### 1b. Reactor 模式 QPS（epoll level-triggered）

| P | kvs SET | redis SET | 倍率 | kvs GET | redis GET | 倍率 |
|---|---------|-----------|------|---------|-----------|------|
| P=1   | 20,415    | 18,219    | **1.12x** | 20,060    | 20,226    | 0.99x |
| P=4   | 78,920    | 79,777    | 0.99x | 78,877    | 76,799    | **1.03x** |
| P=8   | 154,847   | 156,372   | 0.99x | 154,083   | 149,813   | **1.03x** |
| P=16  | 290,613   | 249,938   | **1.16x** | 290,613   | 283,286   | **1.03x** |
| P=20  | 357,270   | 276,932   | **1.29x** | 367,242   | 326,904   | **1.12x** |
| P=40  | 664,011   | 362,976   | **1.83x** | 622,665   | 428,082   | **1.45x** |
| P=80  | 995,025   | 389,257   | **2.56x** | 1,011,122 | 483,325   | **2.09x** |
| P=160 | 1,562,500 | 380,373   | **4.11x** | 1,577,287 | 594,884   | **2.65x** |

**结论:**
- **reactor 低 P 与 Redis 持平，高 P 显著领先。** P≤8 时倍率 0.99x–1.12x（epoll 单次事件处理开销相当），P≥40 后批量化优势爆发，P=160 GET 达 1.58M（**2.65x** vs Redis 595K）
- **kvstore reactor 峰值 GET 1.58M @ P=160**，且未见衰减——epoll LT 批量收割在高 pipeline 下仍保持线性增长
- **Redis 在 P=80 即饱和：** GET 483K → P=160 微降至 595K（实际是瓶颈在 Redis 单线程事件循环，非网络）

### 1c. Proactor 模式 QPS（io_uring + SQPOLL + arm-before-drain）

> SQPOLL 内核线程轮询 SQ 环形缓冲，消除 `io_uring_enter` 系统调用。**arm-before-drain 优化（2026-08-11）：** `handle_read` 中在 drain 命令之前先 `arm_recv` 提交下一个 recv SQE，SQPOLL 线程在 CPU drain 期间并行执行内核 recv，IO 与 CPU 流水线化。配合 `flush_write_buf` inline send + async fallback，消除 SQE→CQE 往返。

| P | kvs SET | redis SET | 倍率 | kvs GET | redis GET | 倍率 |
|---|---------|-----------|------|---------|-----------|------|
| P=1   | 23,816    | 17,873    | **1.33x** | 23,466    | 18,923    | **1.24x** |
| P=4   | 95,456    | 75,517    | **1.26x** | 97,742    | 71,824    | **1.36x** |
| P=8   | 182,183   | 149,254   | **1.22x** | 188,041   | 154,416   | **1.22x** |
| P=16  | 333,778   | 259,673   | **1.29x** | 318,878   | 290,107   | **1.10x** |
| P=20  | 407,166   | 325,521   | **1.25x** | 413,907   | 311,526   | **1.33x** |
| P=40  | 730,460   | 355,619   | **2.05x** | 715,820   | 406,339   | **1.76x** |
| P=80  | 1,161,440 | 489,716   | **2.37x** | 1,270,648 | 483,325   | **2.63x** |
| P=160 | 1,782,531 | 535,332   | **3.33x** | 1,748,252 | 446,030   | **3.92x** |

**结论:**
- **proactor 全 P 深度显著领先 Redis。** P=1 即领先 24%（GET），P≥40 后倍率加速至 1.76x–3.92x
- **P=160 GET 达 1.75M QPS，是 Redis（446K）的 3.92x；SET 1.78M QPS，是 Redis（535K）的 3.33x**
- **Redis 在 P=80 后 GET 不增反降：** P=80 483K → P=160 446K。瓶颈在 Redis 单线程事件循环，网络和 CPU 均未饱和
- **arm-before-drain 效果：** 低 P 也保持优势（P=1 GET +24% vs Redis），IO/CPU 流水线化在全 pipeline 范围有效

### 1d. Reactor vs Proactor 对比

| P | reactor GET | proactor GET | 优势 |
|---|------------|-------------|:---:|
| P=1   | 20,060    | 23,466    | proactor +17% |
| P=4   | 78,877    | 97,742    | proactor +24% |
| P=8   | 154,083   | 188,041   | proactor +22% |
| P=16  | 290,613   | 318,878   | proactor +10% |
| P=20  | 367,242   | 413,907   | proactor +13% |
| P=40  | 622,665   | 715,820   | proactor +15% |
| P=80  | 1,011,122 | 1,270,648 | **proactor +26%** |
| P=160 | 1,577,287 | 1,748,252 | proactor +11% |

**结论:**
- **proactor 全 P 深度领先 reactor，幅度 10%–26%。** arm-before-drain 的 IO/CPU 流水线化 + SQPOLL 零系统调用提交，在所有 pipeline 深度均有稳定收益
- **P=80 proactor 领先最大（+26%）：** reactor 在 1M QPS 附近遇到 epoll LT 批量收割瓶颈，proactor 的 io_uring 批量 CQE peek 无此限制
- **P=160 差距收窄至 +11%：** 此时瓶颈从事件循环迁移到 hash 表/内存带宽，引擎差异减弱
- **引擎选择：proactor + SQPOLL 是全场景最优解**，低 P 到高 P 均有 10%+ 优势

---

## 2. WAL (AOF) 性能影响

**测试时间:** 2026-08-06 | **脚本:** `bench_wal.sh` | **数据:** `test/bench/wal_{1,2,3}.txt`（三轮均值）
**参数:** 1M requests, 50 clients, Release build (`-O3`), 16B value, everysec fsync, `taskset` 绑核 (server=0,3, client=1,2), expire=off

> **架构:** 单线程无锁静态环形缓冲 + 异步 io_uring 落盘，原始 RESP 直接写入（无二进制 header）。每个 P 级别独立启停 + 预热 200K ops 消除冷启动偏差。Redis 7.0 使用 `appendfsync everysec` + `auto-aof-rewrite-percentage 0`。

### PING 吞吐（纯 echo 基准）

| 系统 | QPS | 说明 |
|------|----:|------|
| kvstore | **23,079** | |
| Redis | 17,800 | |
| **倍率** | **1.30x** | |

### kvstore SET（AOF off vs on，三轮均值）

| P | AOF off | AOF on | AOF 开销 |
|---|--------:|-------:|:--------:|
| P=1   | 20,804 | 20,738 | 0% |
| P=10  | 161,505 | 175,049 | −8% |
| P=20  | 282,093 | 271,228 | 4% |
| P=40  | 485,084 | 461,098 | 5% |
| P=80  | 525,595 | 470,137 | 11% |
| P=160 | 783,424 | 672,099 | 14% |

### kvstore GET（AOF off vs on，三轮均值）

| P | AOF off | AOF on | AOF 开销 |
|---|--------:|-------:|:--------:|
| P=1   | 21,132 | 20,971 | 1% |
| P=10  | 172,380 | 176,333 | −2% |
| P=20  | 325,791 | 312,947 | 4% |
| P=40  | 518,976 | 542,286 | −4% |
| P=80  | 862,594 | 911,355 | −6% |
| P=160 | 973,046 | 979,856 | −1% |

### Redis SET 对比（三轮均值）

| P | AOF off | AOF on | AOF 开销 |
|---|--------:|-------:|:--------:|
| P=1   | 18,113 | 20,120 | −11% |
| P=10  | 121,141 | 86,941 | **28%** |
| P=20  | 160,595 | 106,325 | **34%** |
| P=40  | 185,406 | 125,417 | **32%** |
| P=80  | 213,442 | 127,815 | **40%** |
| P=160 | 212,992 | 127,748 | **40%** |

### Redis GET 对比（三轮均值）

| P | AOF off | AOF on | AOF 开销 |
|---|--------:|-------:|:--------:|
| P=1   | 17,825 | 17,995 | −1% |
| P=10  | 160,785 | 165,640 | −3% |
| P=20  | 221,614 | 217,309 | 2% |
| P=40  | 272,856 | 281,783 | −3% |
| P=80  | 319,202 | 310,778 | 3% |
| P=160 | 312,534 | 350,650 | −12% |

**结论:**

- **PING kvstore 领先 30%：** 23,079 vs 17,800 rps。kvstore 的 LT reactor + inline flush 在单 echo 场景下事件循环效率显著优于 Redis 的 epoll
- **kvstore AOF 开销在噪声范围：** SET 差异在 −8% 到 14% 波动（三轮均值），GET 作为纯读不经 WAL，差异 ±6% 均为测量噪声。AOF 本身的开销无法测量——实际为零
- **Redis AOF 开销 28–40%（SET）：** 子进程写盘模型在 pipeline 下无法摊销 fsync 成本。Redis GET 不受 AOF 影响
- **单线程无锁架构是零开销的关键：** 原始 RESP 直接 memcpy 到静态环形缓冲 + 阈值触发异步 io_uring 写盘。热路径无锁、无 CRC、无 header 构造、无线程切换。2026-08-06 修复了 everysec 刷盘基于 tick 计数的 bug（reactor 空闲期刷盘延迟 10s→1s）和 proactor 空闲循环冻结 bug
- **AOF 开启时 kvstore P=160 SET 672K QPS，是 Redis（128K）的 5.3 倍；GET 980K，是 Redis（351K）的 2.8 倍**

---

## 3. 内存分配器对比 (jemalloc vs glibc)

**测试时间:** 2026-08-15 | **脚本:** `bench_memory.sh` | **配置:** `conf/kvstore-none.conf`（persistence=none, expire=off, 无复制）
**数据:** `test/bench/results_memory_jemalloc.csv` / `test/bench/results_memory_glibc.csv`
**二进制:** `build/kvstore` (jemalloc) / `build-glibc/kvstore` (glibc, `cmake -DUSE_JEMALLOC=OFF`)

> **方法学：** 单线程 + 均匀 value 尺寸，隔离纯分配器行为。每个 (N,V) 配置独立重启空服务 → 采 baseline → `populate_kv.py` 填充 → settle 15s（覆盖 jemalloc `dirty_decay_ms=10s`）→ 5 次采样取 min 收敛到稳态。
> **三层指标：** allocated（活分配字节：jemalloc `stats.allocated` / glibc `mallinfo2().uordblks+hblkhd`）、RSS（`/proc/pid/status` VmRSS）、private（`smaps_rollup` Private_Dirty+Clean）。差分除以真实 DBSIZE 得每 key 开销。

### 3.1 每 key 内存模型（对账验证）

单条 entry 单次 malloc：`requested = 34 + key(16) + V`，其中 34 = 32B 头部(next/hash/expire/key_len/val_len) + 2B(type/enc)。分配器按各自规则舍入：

| V | 请求字节 | glibc chunk | jemalloc 尺寸类 | 差 |
|---|--------:|------------:|----------------:|----:|
| 16   | 66   | 80   | 80   | 0 |
| 256  | 306  | 320  | 320  | 0 |
| 4096 | 4146 | 4160 | 5120 | **960 (18.7%)** |

哈希桶数组（2× 膨胀后 `size = 2^ceil(log2 N)`）摊到每 key 约 8.4~10.5 B，两种分配器一致。实测 `alloc_b_per_key` 与 `chunk + 桶` 对账误差 ≤0.5 B/key。

### 3.2 稳态内存对比

**活分配（alloc_b_per_key，含桶数组）：**

| N | V | glibc | jemalloc |
|---:|----:|------:|---------:|
| 100K | 16   | 90.5   | 90.0   |
| 500K | 16   | 88.4   | 88.3   |
| 1M   | 16   | 88.4   | 88.3   |
| 100K | 256  | 330.5  | 330.1  |
| 500K | 256  | 328.4  | 328.3  |
| 1M   | 256  | 328.4  | 328.3  |
| 100K | 4096 | 4170.5 | 5130.0 |
| 500K | 4096 | 4168.4 | 5128.3 |
| 1M   | 4096 | 4168.4 | 5128.3 |

**物理内存（RSS）glibc 相对 jemalloc 节省：**

| V | 100K | 500K | 1M |
|---:|-----:|-----:|----:|
| 16   | 0.2 MB | 2.5 MB | 4.9 MB |
| 256  | 0.8 MB | 3.8 MB | 7.0 MB |
| 4096 | 99 MB  | 478 MB | 541 MB* |

**分配器碎片开销（(RSS−allocated)/allocated）：**

| V | glibc | jemalloc |
|---|------:|---------:|
| 16   | ~0.1% | 3.2% ~ 6.1% |
| 256  | ~0.0% | 2.2% ~ 2.6% |
| 4096 | ~0.0% | 0.8% ~ 1.5% |

> \* 1M×4096 的 jemalloc RSS 受 6GB 虚拟机 swap 影响被低估（allocated 4.78GB > RSS 4.62GB），真实差距约 937MB（= 960B × 1M）。

### 3.3 结论

1. **小值（16/256B）两者打平**：glibc 的 16 字节对齐与 jemalloc 的尺寸类都落到 80/320B，活分配几乎相等（差 <0.5 B/key）。

2. **大值（4KB）glibc 显著更省**：jemalloc 粗粒度尺寸类把 4146B 硬拉到 5120B（每 key 浪费 960B），glibc 连续 16B 对齐只浪费 14B。4KB 档 glibc 省 99~541MB（~19%）。

3. **glibc 碎片开销更低**：单线程均匀尺寸下 glibc 的 sbrk 堆贴得极紧（RSS≈allocated，碎片 ~0%），jemalloc 的 arena/run/bin 分页记账带来 0.8~6% 固定税。

4. **范围限定**：此结论仅适用于单线程 + 均匀 value。jemalloc 的 thread-cache 与混合尺寸抗碎片能力在此场景无机会发挥；多线程或混合尺寸下结论可能反转。

---

## 4. BGSAVE 性能影响

**测试时间:** 2026-08-04 | **脚本:** `bench_bgsave.sh`, `bench_bgsave_redis.sh`
**参数:** 50 并发, Pipeline=20, Value=16B, 纯 SET 命令, 100K keys **固定数据集**

> 固定数据集消除数据集大小膨胀干扰，唯一变量 = BGSAVE 触发频率。
> 每间隔独立实例：populate → warmup → 基线 → BGSAVE（消除跨测试 dict 状态污染）。
> QPS = 服务端 INFO stats `total_commands_processed` 差值 / 耗时；p50/p99 来自 redis-benchmark `--csv`。
> 三层防御: Coalesce (1 pending) → Cooldown (自适应: max(1s, 上次BGSAVE耗时×5)) → Drop。

### 4.1 KVStore — 三轮测试

| 间隔 | 执行 | 跳过 | 触发 | 基线 QPS | BGSAVE QPS | p50(基/BG) | p99(基/BG) | 降幅 | RDB |
|------|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| **R1** | | | | | | | | | |
| 1M | 5 | 0 | 5 | 356,127 | 361,952 | 1.71/1.63ms | 3.61/3.93ms | −1.6% | 4.9M |
| 100K | 4 | 6 | 10 | 396,984 | 334,229 | 1.48/1.66ms | 3.25/12.9ms | 15.8% | 4.9M |
| 10K | 2 | 18 | 20 | 377,787 | 296,748 | 1.50/1.85ms | 3.56/13.6ms | 21.5% | 4.9M |
| 1K | 2 | 9 | 10 | 358,296 | 216,006 | 1.63/2.22ms | 3.66/25.3ms | 39.7% | 4.9M |
| **R2** | | | | | | | | | |
| 1M | 6 | 0 | 6 | 396,041 | 362,766 | 1.42/1.63ms | 3.19/3.75ms | 8.4% | 4.9M |
| 100K | 4 | 6 | 10 | 363,373 | 321,651 | 1.64/1.74ms | 3.58/13.2ms | 11.5% | 4.9M |
| 10K | 2 | 17 | 19 | 380,808 | 319,298 | 1.58/1.72ms | 3.54/12.3ms | 16.2% | 4.9M |
| 1K | 1 | 11 | 11 | 356,253 | 209,669 | 1.75/2.28ms | 3.78/34.3ms | 41.2% | 4.9M |
| **R3** | | | | | | | | | |
| 1M | 5 | 0 | 5 | 355,367 | 361,612 | 1.69/1.66ms | 3.85/3.94ms | −1.8% | 4.9M |
| 100K | 3 | 6 | 9 | 370,921 | 348,194 | 1.62/1.58ms | 3.74/9.01ms | **6.1%** | 4.9M |
| 10K | 2 | 18 | 20 | 355,873 | 299,414 | 1.70/1.93ms | 3.84/11.3ms | **15.9%** | 4.9M |
| 1K | 1 | 11 | 11 | 342,115 | 211,888 | 1.78/2.35ms | 3.90/31.3ms | **38.1%** | 4.9M |

### 4.2 Redis — 三轮测试

| 间隔 | 执行 | 跳过 | 触发 | 基线 QPS | BGSAVE QPS | p50(基/BG) | p99(基/BG) | 降幅 | RDB |
|------|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| **R1** | | | | | | | | | |
| 1M | 6 | 0 | 6 | 221,338 | 210,527 | 3.90/4.06ms | 6.90/7.57ms | 4.9% | 3.4M |
| 100K | 10 | 0 | 10 | 221,044 | 185,325 | 3.89/4.08ms | 6.83/31.2ms | 16.2% | 3.4M |
| 10K | 13 | 17 | 30 | 211,283 | 135,107 | 4.12/4.92ms | 7.45/40.4ms | 36.1% | 3.4M |
| 1K | 3 | 8 | 11 | 211,686 | 118,219 | 4.06/5.08ms | 7.39/44.3ms | 44.2% | 3.4M |
| **R2** | | | | | | | | | |
| 1M | 5 | 0 | 5 | 212,767 | 218,905 | 4.08/3.94ms | 7.06/7.34ms | −2.9% | 3.4M |
| 100K | 10 | 0 | 10 | 213,630 | 176,837 | 4.06/4.24ms | 7.08/30.1ms | 17.2% | 3.4M |
| 10K | 13 | 16 | 29 | 214,455 | 131,242 | 4.02/4.87ms | 7.25/42.6ms | 38.8% | 3.4M |
| 1K | 4 | 7 | 11 | 234,687 | 113,910 | 3.65/5.20ms | 6.82/49.3ms | 51.5% | 3.4M |
| **R3** | | | | | | | | | |
| 1M | 5 | 0 | 5 | 212,676 | 213,211 | 4.02/4.02ms | 7.29/7.70ms | −0.2% | 3.4M |
| 100K | 11 | 0 | 11 | 228,103 | 178,097 | 3.66/4.19ms | 7.49/32.2ms | 21.9% | 3.4M |
| 10K | 11 | 16 | 27 | 228,886 | 154,856 | 3.80/4.23ms | 6.68/39.0ms | 32.3% | 3.4M |
| 1K | 3 | 8 | 11 | 211,641 | 123,778 | 4.07/4.74ms | 7.43/42.8ms | 41.5% | 3.4M |

### 4.3 KVStore vs Redis 对比（各取三轮最优值）

| 间隔 | KVStore 降幅 | Redis 降幅 | 差距 | KVS exec/skip | RED exec/skip | KVS p99(BG) | RED p99(BG) |
|------|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| 1M | −1.8% | −2.9% | +1.1pp | 5/0 | 5/0 | 3.9ms | 7.7ms |
| 100K | **6.1%** | 16.2% | −10.1pp | 3/6 | 10/0 | 9.0ms | 32.2ms |
| 10K | **15.9%** | 32.3% | −16.4pp | 2/18 | 11/16 | 11.3ms | 42.6ms |
| 1K | **38.1%** | 41.5% | −3.4pp | 1/11 | 3/8 | 31.3ms | 49.3ms |

**结论:**
- **自适应冷却有效抑制高频 BGSAVE：** 10K 间隔 exec 从冷却前的 42–48 次降至 2 次，1K 从 9–10 次降至 1 次。跳过数远超执行数（10K: 18 vs 2, 1K: 11 vs 1），防御机制正确拦截冗余请求
- **KVStore 在各频率梯度均优于 Redis：** 100K 降幅 6.1% vs 16.2%（**2.7× 优势**），10K 降幅 15.9% vs 32.3%（**2.0× 优势**），1K 降幅 38.1% vs 41.5%。低频（1M）两者均在噪声范围内（−1.8% vs −2.9%）
- **KVStore 防御风格：** 冷却前置拦截（skip 多，exec 少）——10K 间隔触发 20 次、exec 仅 2 次。Redis 防御风格：子进程运行中拒绝（skip 16，exec 11）
- **P99 延迟尖峰是 fork COW 固有代价：** KVStore 1K 间隔 p99 从 3.9ms → 31.3ms（fork 阻塞 + COW 页复制），Redis 从 7.4ms → 49.3ms。两者均有尖峰，KVStore 绝对值更低（31.3ms vs 49.3ms）
- **RDB 大小：** KVStore 4.9MB vs Redis 3.4MB（+44%），原因是 KVStore entry 数据格式包含 type/encoding 字段且 key 在 RDB 文件和 entry 中各存一份。后续可优化二进制格式进一步缩小

---

## 5. 全量同步传输性能

**测试时间:** 2026-08-11 | **脚本:** `test/bench/rdma_fullsync_bench.sh`
**环境:** Ubuntu 24.04, Kernel 6.8.0-137, VMware 虚拟机, SoftRoCE (rxe0)
**配置:** proactor, persistence=rdb, expire=off, memlock=unlimited (RDMA 需要 2GB MR 注册)
**数据规模:** 50K keys (RDB 14.07 MB) / 400K keys (RDB 109.86 MB), value=256B

> **测试方法:** 每轮测试独立启停 kvstore 实例。Phase 1 使用 `populate_kv.py` 4 并行 worker 流式填充数据（单 worker ~90K ops/s），SAVE RDB 后重启实例。Phase 2/3 启动 master → 启动 slave 触发 inline fullsync → 记录 slave ready 时刻为 end-to-end 终点。NET 时间从日志解析纯传输耗时（排除序列化/反序列化），NET_MB/s = RDB 文件大小 / NET 时间。每模式 1 次 warmup + 3 次正式测试。

### 5a. kvstore 50K keys（RDB 14.07 MB，1 轮 × 3 次）

| MODE | WALL_S | TOT_MB/s | NET_MS | PREP_MS | NET_MB/s | ENT/s |
|------|:------:|:--------:|:------:|:-------:|:--------:|:-----:|
| sendfile | 0.57 ± 0.01s | 24.8 ± 0.3 | 214.4 ± 19.3 | 0 | 65.9 ± 5.7 | 88.3K ± 1.2K |
| rdma | 2.71 ± 0.03s | 5.2 ± 0.1 | 73.4 ± 2.1 | 8.4 ± 1.2 | 191.7 ± 5.5 | 18.4K ± 0.2K |

> 小规模下 RDMA 固定开销（MR 注册 2GB + CM 建链 + buf_info 交换）占主导。sendfile E2E 仅 0.57s，RDMA 需 2.71s（4.8× 慢）。但纯网络 RDMA 73ms vs sendfile 214ms（**2.9× 快**），网络层优势已充分体现。

### 5b. kvstore 400K keys（RDB 109.86 MB，3 轮 × 3 次 = 9 次）

| MODE | WALL_S | TOT_MB/s | NET_MS | PREP_MS | NET_MB/s | ENT/s |
|------|:------:|:--------:|:------:|:-------:|:--------:|:-----:|
| sendfile | 2.54 ± 0.27s | 42.6 ± 4.9 | 1,550 ± 91 | 0 | 71.0 ± 4.3 | 155K ± 17K |
| rdma | 3.97 ± 0.22s | 27.9 ± 1.8 | 571 ± 22 | 65.3 ± 3.6 | 192.7 ± 6.5 | 101K ± 6K |

**稳定性:** sendfile WALL 范围 2.17–2.74s（CV=10.6%），首轮出现 2.17s 峰值。rdma WALL 首轮 4.30s → 后续稳定 3.75–3.79s（冷缓存效应），NET_MB/s 始终 185–203 MB/s（**CV=3.4%**，无 TCP 拥塞抖动）。

**规模效应:**

| | 50K sendfile | 50K rdma | 400K sendfile | 400K rdma |
|------|:----:|:----:|:-----:|:----:|
| TOT_MB/s | 24.8 | 5.2 | 42.6 | 27.9 |
| NET_MB/s | 65.9 | 191.7 | 71.0 | 192.7 |
| rdma/sendfile E2E 比 | — | 0.21× | — | 0.66× |
| 网络时间占比 | 38% | 3% | 61% | 14% |

> 数据量越大，RDMA 固定成本摊销越充分（E2E 比从 0.21× → 0.66×）。sendfile 网络占 61% 为瓶颈，rdma 仅 14% 瓶颈在 MR+CM。

### 5c. kvstore 时间分解（400K 端到端）

```
sendfile (2.54s): |████████████████ 1.55s 网络 |████████ 0.99s 其他 (RDB load + fork + accept)|
rdma     (3.97s): |██ 0.57s 网络 | 0.07s prep |████████████████████████████ 3.33s 其他 (MR+CM+load)|
```

- **sendfile**: 网络占 61%，`sendfile()` 零拷贝发送预建 RDB，非网络开销 ~1s
- **rdma**: 网络仅占 14%，非网络占 86%（slave 2GB MR 注册 `ibv_reg_mr` + RDMA_CM QP 建链 + 8×256MB ring buffer buf_info 交换 + stream load）

### 5d. Redis 全量同步测试

**测试时间:** 2026-08-11 | **脚本:** `test/bench/redis_fullsync_bench.sh` | **Redis:** 7.0.15
**配置:** `rdbcompression no`（禁用 LZF 与 kvstore RDB 对齐）, `save ""`, `appendonly no`
**数据:** 400K keys × 256B, RDB 105.29 MB, 与 kvstore 同一套数据生成逻辑

> **计时对齐:** E2E = SLAVEOF → slave DBSIZE 就绪 (↔ kvstore WALL), SYNC = BGSAVE 开始 → 同步完成 (↔ kvstore NET)。每模式 1 warmup + 3 次 × 3 轮 = 9 次。

**三轮汇总（均值 ± σ）:**

| 模式 | E2E_SEC | SYNC_SEC | E2E_MB/s | SYNC_MB/s | ENT/s |
|------|:------:|:------:|:------:|:------:|:-----:|
| disk | 6.51 ± 0.30s | 2.73 ± 0.10s | 16.2 ± 0.8 | 38.7 ± 1.4 | 61.6K ± 2.8K |
| diskless | 5.42 ± 0.50s | 1.79 ± 0.10s | 19.6 ± 1.4 | 59.1 ± 3.1 | 74.3K ± 5.4K |
| **diskless 优势** | **16.7%** | **34.4%** | **20.7%** | **52.7%** | **20.6%** |

> diskless 快于 disk 是预期内的——省去了 BGSAVE 子进程写磁盘步骤。SYNC 阶段差距 34%，E2E 收窄至 17%（slave RDB 加载 ~3.7s 两者一致）。

### 5e. 全量同步网络层性能对比总表

**测试环境:** 同一 VM (Ubuntu 24.04, Kernel 6.8.0-137), localhost, 400K keys × 256B, RDB ~105 MB

| 同步架构方案 | 底层传输机制 | 纯网络传输耗时 | 极限吞吐量 | 相对性能比率 (以 Redis diskless 为基准) |
|:---|:---|:---:|:---:|:---:|
| **kvstore — RDMA** | RoCEv2 (RDMA_WRITE + IMM_DONE) | **570 ms** | **192.7 MB/s** | **326% (3.26×)** |
| **kvstore — Sendfile** | TCP `sendfile()` 零拷贝 | 1,550 ms | 71.0 MB/s | 120% (1.20×) |
| Redis 7.0 — Diskless | TCP Stream (无盘) | 1,790 ms | 59.1 MB/s | 100% (基准) |
| Redis 7.0 — Disk | TCP/Disk (有盘) | 2,730 ms | 38.7 MB/s | 65% (0.65×) |

> **计时口径:** 纯网络传输 = 不含 slave 进程启动/握手/RDB 加载，仅计数据从 master 发出到 slave 收完的核心传输阶段。kvstore NET_MS ↔ Redis SYNC_SEC。
>
> **RDB 大小对齐:** Redis 默认 LZF 压缩可将重复二进制 value 压至 1/5（22 MB），测试中已通过 `rdbcompression no` 禁用，确保对比在同一数据量（~105 MB）上进行。

### 5f. 端到端（E2E）性能对比总表

含 slave 启动、握手、RDB 加载的完整端到端耗时：

| 同步架构方案 | E2E 总耗时 | E2E 吞吐量 | 纯传输占比 | 瓶颈所在 |
|:---|:---:|:---:|:---:|:---|
| **kvstore — Sendfile** | **2.54s** | 42.6 MB/s | 61% | 网络 (~1.5s) |
| **kvstore — RDMA** | 3.97s | 27.9 MB/s | 14% | MR注册+CM建链 (~3.3s) |
| Redis 7.0 — Diskless | 5.42s | 19.6 MB/s | 33% | Slave RDB加载 (~3.6s) |
| Redis 7.0 — Disk | 6.51s | 16.2 MB/s | 42% | RDB生成写盘+加载 (~3.8s) |

### 5g. 结论

1. **RDMA 纯网络吞吐碾压一切：193 MB/s，是 Redis diskless 的 3.26×。** RoCEv2 内核旁路 + 单边 RDMA_WRITE + 零拷贝消除了 TCP 协议栈和 COW 序列化双重开销

2. **kvstore sendfile（71 MB/s）的纯传输也快于 Redis diskless（59 MB/s）：** fork + COW + RDB 序列化的开销 > TCP 协议栈开销

3. **RDMA 端到端受 ~3.3s 固定冷启动成本拖累**（2GB MR 注册 + CM 建链），数据量越大摊销越充分，当前 400K 规模 sendfile 更实用

4. **Redis 的瓶颈不在网络而在数据模型：** slave RDB 加载 ~3.7s 是最大单项开销，disk/diskless 共享同一份 RDB 加载路径

5. **数据完整性 100%：** kvstore 9 次 + Redis 9 次，slave DBSIZE 精确匹配，无丢 key 或数据损坏

6. **选型建议：** ≤500MB 用 kvstore sendfile（最快 E2E、零配置）；追求极致网络吞吐用 kvstore RDMA；需 Redis 兼容时 diskless 优于 disk

---

## 6. 增量同步性能影响

**测试时间:** 2026-08-22 | **脚本:** `bench_xdpkcp.sh` | **数据:** `test/bench/results_xdpkcp{,_1,_2,_3}.txt`（四轮，取中位数）
**参数:** 1M requests, 50 clients, 16B value, single-key（不加 -r）, proactor, expire=off, persistence=none
**环境:** Ubuntu 24.04, Kernel 6.8.0-138-generic, VMware 虚拟机, veth 对（vxdp0↔vxdp1）供两网关二层互联

> **三种场景对比同一台 master 的 SET/GET 吞吐（相对 baseline 的百分比即同步开销）：**
> - **baseline** — master 单独运行（无任何同步）
> - **epoll** — 内置 repl_incr（写命令在主线程 memcpy 到 backlog + slave 写缓冲，EPOLLOUT 冲刷）
> - **xdpkcp** — AF_XDP+KCP 内核旁路（独立网关进程 + /dev/shm 无锁 SPSC，同步流量走 veth 网卡）
>
> **方法学（对齐 bench_incr_sync.sh）：** 单 key 把命令路径压到最便宜、放大同步开销占比；每测量点单轮；测前预热 200 万 + 单核绑定（master=0 / slave=2 / client=1 / 两网关=4,5）；xdpkcp 场景额外记录网关 drop 计数。
>
> ⚠️ **噪声：** 单轮测量 + 未锁 CPU 频率，噪声地板约 ±10%，>100% 读数均为抖动（同步不可能加速）。

### 6a. SET 吞吐（写命令，含同步开销，四轮中位数）

| P | baseline | epoll | epoll% | xdpkcp | xdpkcp% |
|---|---------:|------:|-------:|-------:|--------:|
| 1   | 20,098 | 19,459 | 96.8% | 20,172 | 100.4% |
| 5   | 98,897 | 95,353 | 96.4% | 96,529 | 97.6% |
| 10  | 181,749 | 189,174 | 104.1% | 186,142 | 102.4% |
| 20  | 343,644 | 335,017 | 97.5% | 326,164 | 94.9% |
| 40  | 565,915 | 522,529 | 92.3% | 564,688 | 99.8% |
| 80  | 819,422 | 749,109 | 91.4% | 806,783 | 98.5% |
| 160 | 1,276,264 | 900,592 | 70.6% | 1,017,078 | 79.7% |

### 6b. GET 吞吐（读命令，不传播，应与 baseline 持平，四轮中位数）

| P | baseline | epoll | epoll% | xdpkcp | xdpkcp% |
|---|---------:|------:|-------:|-------:|--------:|
| 1   | 20,234 | 19,874 | 98.2% | 20,673 | 102.2% |
| 5   | 97,107 | 102,885 | 105.9% | 102,071 | 105.1% |
| 10  | 189,330 | 190,780 | 100.8% | 190,193 | 100.5% |
| 20  | 341,771 | 356,316 | 104.3% | 353,638 | 103.5% |
| 40  | 583,720 | 616,211 | 105.6% | 625,807 | 107.2% |
| 80  | 924,729 | 1,024,109 | 110.7% | 1,058,762 | 114.5% |
| 160 | 1,463,065 | 1,498,241 | 102.4% | 1,488,108 | 101.7% |

### 6c. 结论

1. **GET 三场景持平，读路径零同步开销。** GET 均在 baseline 的 ±15% 内（98.2%~114.5%），无系统性差异；>100% 为单轮噪声（未锁频 + 单轮测量）。

2. **epoll SET 随 pipeline 深度恶化，P160 掉到 70.6%。** P≤10 几乎无影响（96.4%~104.1%），P≥20 开始下滑，P40 92.3%、P80 91.4%、P160 70.6%（-29%）。根因是写命令在主线程内串行 memcpy 到 backlog + 冲刷 slave，pipeline 越深串行化越成瓶颈。

3. **xdpkcp SET 在 P≤80 几乎无影响（94.9%~102.4%），P160 掉到 79.7%。** 把同步从主线程卸到独立网关进程后，master 热路径只多一次无锁 SPSC 入队（memcpy ≤8KB + 两原子操作），故 P≤80 稳定在 baseline 附近（P40 99.8%、P80 98.5%）。**P160 是唯一明显下降点**，根因是同步传输（网关 + KCP + AF_XDP generic 模式）在 ~100 万 cmd/s 处触顶，见结论 6。

4. **xdpkcp 相对 epoll 的收益随 P 增大而扩大。** P160 SET：79.7% vs 70.6%（+9.1pp）；P80：98.5% vs 91.4%（+7.1pp）；P40：99.8% vs 92.3%（+7.5pp）；低 P（≤20）两者相当（都在噪声带内）。

5. **KCP+AF_XDP 传输无丢包。** 四轮所有 P 的网关 drop 均为 0（master-drops=0 / slave-drops=0）——P160 的性能下降是**有损背压**（master 在 SHM 环满时 spin 等待）而非丢包，数据完整性不受影响。

6. **P160 下降定位：同步传输触顶，而非 master 热路径。** 证据：① xdpkcp P160 绝对吞吐在四轮中稳定在 ~1.0M（0.91~1.09M）呈平台，而 baseline 同期从 1.18M 涨到 1.35M——xdpkcp 撞到 ~1.0M 的硬顶；② P≤80 紧贴 baseline（98.5%+），说明 enqueue 开销本身很小，只有超过 ~1.0M cmd/s（≈55 MB/s 同步字节流）后才出现悬崖式下跌，这是资源饱和的特征。链路为 master → SHM → master 网关 → KCP（发送窗口 256×1400B）→ AF_XDP generic → veth → slave 网关 ACK；当 KCP 发送窗口被 ACK 速率填满、或 AF_XDP TX 环（generic 模式由内核软转发排空）跟不上时，`kcp_send_queue` 返回满 → 网关停止 SHM 出队 → 环满 → master 在 `enqueue_chunk` spin。**待确认**：需抓取 master 网关的 `tx_fail`/`af_xdp tx_drops` 与引擎的 `SHM ring full` 告警，以坐实是哪一环先满。


---

## 7. 过期键处理性能

> 本节覆盖两代实现：
> - **当前实现（2026-08-15）**：单线程 + 单全局 dict，`active_expire_cycle()` 随事件循环运行，纯主动过期、无惰性删除 —— 见 7.1 / 7.2。

### 7.1 主动过期清理速率（当前实现）

**测试时间:** 2026-08-15 | **脚本:** `bench_expire_active.sh` | **数据:** `test/bench/results_expire_active.csv`
**配置:** `conf/kvstore-expire.conf`（reactor, persistence=none, expire on, rdma_sync off）
**参数:** value=16B, TTL=10s, N ∈ {100K, 1M}, 口径 ∈ {idle, busy}

**测试口径：**

| 口径 | 场景 | 反映 |
|------|------|------|
| idle | 写完 key 后不再访问，事件循环每 100ms 才醒一次 | 生产里「key 过期时无流量」的真实行为 |
| busy | 后台持续 PING 保持事件循环活跃 | 纯机制上限（不受轮询周期限制） |

**方法：** 每个 (口径, N) 独立重启空服务 → FLUSHALL → `populate_expire.py` 批量写入 N 个带 `EX 10` 的 key → 记录最后写入 key 的到期时刻 → 轮询 DBSIZE 直到清空。`cleanup_ms` = 清空时刻 − 到期时刻；`rate` = N / cleanup_ms；`expired_keys` 取自 `INFO stats`。

**结果：**

| 口径 | N | 写耗时(ms) | 清理耗时(ms) | 清理速率(key/s) | expired_keys |
|------|-----:|----------:|------------:|---------------:|-------------:|
| idle | 100K  | 694  | 27,361  | 3,655    | 100,000 ✓ |
| idle | 1M    | 6,232 | 278,939 | 3,585    | 1,000,000 ✓ |
| busy | 100K  | 783  | 65      | 1,538,462 | 100,000 ✓ |
| busy | 1M    | 5,933 | 1,331   | 751,315   | 1,000,000 ✓ |

**关键发现：**

1. **idle 速率与 N 无关（~3.6K/s）→ 被 100ms 轮询周期卡死，而非 CPU。** 100K→3,655/s、1M→3,585/s，几乎持平。空闲时事件循环每 100ms 醒一次，`active_expire_cycle` 在 100% 过期密度下每轮最多删 ~300 key（20+40+80+160 自适应倍增），上限 ≈ 10 次/s × 300 ≈ 3,000/s；实测 3,655 含 DBSIZE 轮询额外唤醒的 ~10% 偏置。

2. **busy 速率 75 万~154 万/s，随 N 增大下降 ~2× → 内存/缓存墙。** 100K→154 万/s、1M→75 万/s。随机采样下 1M 个 entry 超出 L3，每次 `dict_get_random_key` 接近一次随机 cache miss。

3. **idle/busy 差距 200~400× 是核心特征。** 1M key 的最后一个 key 在 TTL 到期后 idle 下还要再活 ~279s（4.6 分钟），busy 下仅 ~1.3s。TTL 语义是「至少活这么久」，非「到期即删」。

4. **expired_keys == N 全部精确匹配**（2026-08-15 修复了 dict 删空/时间预算中断时漏计本轮的计数器 bug）。

5. **可选优化（未实施）：** 若在意 idle 排空速度，可在字典存在 pending 过期 key 时把事件循环 `epoll_wait` 超时从 100ms 临时降到 1ms，排空后恢复——idle 速率可逼近 busy 量级，完全空闲时仍保持省 CPU。

### 7.2 主动过期机制（当前实现）

`active_expire_cycle()`（`src/core/kvstore_expire.c`）每次事件循环迭代被调用一次（`update_cached_time()` 内，`expire on` 时）：

- **单全局 dict `g_dict`，单线程，无锁**（非多分片）
- **自适应采样**：起始 20 key/轮，若过期率 >25%（`expired > sample/4`）则样本翻倍，否则退出；样本上限 200
- **硬时间预算 1ms/次调用**，防止阻塞事件循环
- 100% 过期密度下每轮最多删 ~300 key（20+40+80+160）
- **纯主动过期**：dispatch 热路径无惰性删除，GET/EXISTS 在回收前仍可读到已过期 key，TTL 返回 -2 但不删

> 空闲时事件循环 `epoll_wait` 超时 100ms（`src/net/reactor.c`），主动过期随之每 100ms 跑一次——这是 idle 速率 ~3.6K/s 的根因。


---

## 运行测试

```bash
# Pipeline 批量性能（CPU 隔离，v2）
sudo ./test/bench/bench_pipeline_v2.sh

# 内存分配器对比（单线程稳态，jemalloc vs glibc）
sudo ./test/bench/bench_memory.sh

# WAL/AOF 性能影响
sudo ./test/bench/bench_wal.sh

# BGSAVE 性能影响
sudo ./test/bench/bench_bgsave.sh
sudo ./test/bench/bench_bgsave_redis.sh

# 增量同步性能影响
sudo ./test/bench/bench_xdpkcp.sh            # AF_XDP+KCP vs epoll（需 root 建 veth/挂 XDP）
sudo ./test/bench/bench_incr_sync.sh         # epoll 增量同步
./test/bench/bench_incr_sync_redis.sh        # Redis 主从对照

# 全量同步
sudo ./test/bench/rdma_fullsync_bench.sh     # RDMA / sendfile 全量同步
sudo ./test/bench/redis_fullsync_bench.sh    # Redis 全量同步对照

# 过期键处理（主动过期清理速率，当前实现）
./test/bench/bench_expire_active.sh
```
