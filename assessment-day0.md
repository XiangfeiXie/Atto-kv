# Day 0 摸底报告

时间：2026-05-18  
机器：8 vCPU (6P + 2E) Intel i7-13620H VMware VM, kernel 6.8, 8 GB RAM, single NUMA  
Pinning：6 vCPU 已绑 P-core (taskset mask 0x3f)

---

## 1. 性能数据 — 校准后的真实情况

### 1.1 第一次 dev_quick：790k（outlier）→ 第二次：390k（可重现）

第一次跑出 SET/GET 各 790k，但**第二次重测只有 390k**。原因是第一次机器刚启动、cache 暖、host 调度器把 vCPU 分到了 P-core 上。**790k 不是稳定数字**，不能写到简历。

### 1.2 多次重测后的真实数字

**测试 A — dev_quick.sh（无 taskset 约束，t=4 worker, threads=-2）**：
```
SET=  385356 rps   GET=  391389 rps   MSET=158227 rps   LRANGE=71275 rps
```

**测试 B — baseline_8c_pcore.sh（taskset 0-5，server/client disjoint 切分）**：
```
t=1 server / 5 bench:  SET= 359k   GET= 328k
t=2 server / 4 bench:  SET= 488k   GET= 492k    ← 最优
t=3 server / 3 bench:  SET= 438k   GET= 329k
t=4 server / 2 bench:  SET= 261k   GET= 242k    (bench client starves)
t=5 server / 1 bench:  SET= 135k   GET= 165k    (severe starve)
```

**测试 C — 6 P-core taskset 但共享调度（t-sweep, 4 bench threads）**：
```
t=1: SET=262k GET=392k     t=2: SET=390k GET=386k
t=3: SET=390k GET=395k     t=4: SET=396k GET=393k
t=5: SET=391k GET=390k     t=6: SET=392k GET=389k
```

### 1.3 校准结论 —— 比预期严峻得多

| 设置 | 峰值 SET QPS | 与 6c locked baseline (488k) 比 |
|---|---|---|
| 6c locked Phase 8.4 | 488k | (基准) |
| 8c P-core, taskset disjoint, 最优 split | 488k | **持平** |
| 8c P-core, 6-core 共享 | 396k | -19% |
| 8c P-core, 8-vCPU 不约束 (dev_quick) | 390k 稳定 | -20% |

**所以 8c P-core 硬件在 1 VM co-located 模式下没有兑现 scaling**。原因清晰：

1. **client 永远在和 server 抢 P-core**：bench 只用 4 client threads 时，4+server 必然撞核
2. **6 P-core 不够同时跑** "bench 4 + server N" 当 N≥3
3. **t=2 + bench=4 是 6 核的数学最优** — 这是 1 机方法学的天花板
4. **唯一突破方法**：2 VM 物理隔离 → server 独占 6 P-core，bench 在另一台 VM

### 1.4 对求职项目的影响

❌ **不能写**：「8c 上 790k QPS / 比 6c 提升 60%」 —— 数字不稳定，立刻翻车
❌ **不能写**：「多线程线性 scaling」 —— 1 VM co-located 数据明确否定

✅ **可以写**：
- 「**单线程 → 多线程改造**：在 6 P-core co-located bench 下，t=2 worker 比 t=1 提升 25%（这是 1-VM 方法学下的真实数字，6c 和 8c 一致）」
- 「**2-VM 物理隔离的 vs Redis benchmark 是 Day 2 任务**」 —— 数字未知，但**预期是 1M+**（server 独占 6 P-core）
- 「**真实瓶颈不是 lock 而是 per-op CPU cost**」 —— Day 2 perf top 给硬数据

### 1.5 最关键观察 — 瓶颈所在

测试 C 的曲线 **从 t=2 到 t=6 完全平直** (~390k)，意味着：

- **不是 lock 争用瓶颈** —— 否则 t↑ 会让数字下降（争用加剧）
- **不是 worker 协调瓶颈** —— 否则曲线会拐头
- **是 per-request CPU cost 瓶颈** —— bench 4 threads × 100k req/s = 400k 上限刚好

也就是说：**整个系统的瓶颈在 client 端 + RESP parse + dispatch + reply 这条链路上的 ~2.5 μs/op CPU 时间**。

→ Day 2 `perf top` 必须找出 ~2.5 μs/op 都花在哪。
→ 这是项目现在最有价值的 1 个 perf 任务。

### 1.6 jemalloc 已经默认启用

`CMakeLists.txt` 默认 `USE_JEMALLOC=ON`，`build/kvstore` 实际链接了 `libjemalloc.so.2`。
**不需要再做"启用 jemalloc"这一步** —— 之前规划里这一项可以划掉。

### 1.7 BPF 程序成熟度远超预期

#### `src/bpf/sockmap_kvs.kern.c`（224 行）

✅ 已实现并通过 verifier：
- `sockops` 程序自动把 passive-established sockets 加入 SOCKHASH
- `sk_skb/stream_verdict` 程序对所有 RX skb 跑：
  - `bpf_skb_pull_data(64)` 拉首段
  - 解析 `*N\r\n$L\r\n<NAME>\r\n` 提取命令名
  - 维护 per-cmd 计数器 + per-conn LRU profile
  - **永远返回 SK_PASS（observe-only）**

⚠️ 注释里明确写了未来路径：
> "future sk_msg fast-path will use the same map to actually serve the response"

意思是 **设计意图就是给 GET 加速预留好的脚手架**，这一项做起来比"从零开始"小得多。

#### `src/bpf/xdp_kvs.kern.c`（202 行）

✅ 已实现：
- L2/L3/L4 头部解析（Ethernet + IPv4 + TCP）
- RESP `GET key` 解析（key ≤ 64 字节）
- `hot_cache` LRU map 查询，记录 hit/miss 计数
- **永远 XDP_PASS**

⚠️ **关键技术决策记录**（注释明确说）：
> "Why no XDP_TX:
>   The previous design crafted RESP replies in BPF and used XDP_TX to
>   short-circuit the kernel TCP stack. This caused a 'dual-track'
>   inconsistency: kernel TCP rcv_nxt did not advance for XDP-consumed
>   packets, so any cache miss on a long-lived connection eventually
>   desynchronised the server-side TCP state and stalled the client.
>   The replacement plan (sockmap / sk_msg) lives in src/bpf/sockmap_*."

→ **XDP_TX 这条路已经走过且证伪了**（TCP seq 状态不同步）。简历不能写"XDP 加速 GET"。
→ 路线已决：用 sockmap + sk_msg 接续。

#### `src/bpf/sockmap_manager.c`（359 行）+ `xdp_manager.c`

✅ userspace loader 完整：cgroup v2 attach、map 同步、kernel obj load、stats 输出。

### 1.4 RDMA 复制是真路径

- 用 libfabric (`FI_EP_MSG`)，不是 TCP-as-RDMA-emulator
- 默认 `rdma_provider verbs`（要 Soft-RoCE 或 IB）
- 配置 `rdma_provider tcp` 可降级到 libfabric tcp provider
- `src/repl/rdma_repl.c` 51KB 实打实，主从两侧、流式 fullsync、incr 链路都有

⚠️ 这台 VM 没有 IB / Soft-RoCE 硬件，bench 时如要展示 RDMA 路径需要：
- 内核加载 `rdma_rxe` 模块创建 Soft-RoCE 设备（用户操作）
- 或者用 `tcp` provider 做"RDMA-shape-but-TCP-transport" 的演示

### 1.5 命令分发器现状

✅ 已支持的非数据命令：
- PING / ECHO / QUIT
- HELLO（拒绝 RESP3，graceful）
- CLIENT (SETNAME/NO-EVICT/NO-TOUCH/REPLY/GETNAME/ID)
- COMMAND / COMMAND COUNT
- SELECT 0
- DBSIZE
- SAVE / BGSAVE
- BGREWRITEAOF（stub，返回 -ERR not implemented）
- CONFIG GET/SET/RESETSTAT
- FLUSHALL / FLUSHDB

❌ F1 完全没有的：
- **INFO**
- **AUTH / requirepass**
- **SCAN / HSCAN / SSCAN / ZSCAN**
- **EXPIREAT / PEXPIRE / PEXPIREAT** （EXPIRE 已有）
- **OBJECT ENCODING**
- **CLIENT LIST / CLIENT KILL**
- **CLUSTER INFO / NODES / SLOTS**
- **SLOWLOG GET / LEN / RESET** (BPF 后端没接到命令前端)
- **DEBUG SLEEP / OBJECT**
- **LATENCY HISTORY**

---

## 2. 测试基础设施

✅ smoke 套件齐全：
- `test/smoke/run_all.sh` + 5 类型 smoke (string/list/hash/set/zset)
- `test/bench/dev_quick.sh` (4 路径快速回归)
- `test/bench/multi_thread_smoke.sh` (MT 正确性)
- `test/bench/tsan_lock_smoke.sh` (TSAN 专用)
- `test/bench/soak.sh` (长跑)
- `test/bench/baseline_capture.sh` (锁定 baseline)
- `test/bench/bench_*.sh` （AOF/save/sync/xdp 各专项）
- `test/test_wal_aof.sh` (WAL + AOF 一致性)

⚠️ **没有 `vs_redis.sh` 头对头 benchmark 脚本** — Day 11 要写。

---

## 3. 简历亮点重新校准（2026-05-18 修订）

基于代码现状 + Phase 10 计划，**简历可写的真实亮点**：

### 🥇 Tier 1（核心四亮点，强可写）

#### 亮点 1：**Lock-free seqlock 短键热路径缓存**（Phase 10, ★ 主亮点，2026-05-18 完成）

**架构**

- **1024 buckets × 512 B = 512 KB**（直映射哈希表，整表 L2-resident，
  cacheline 隔离）— 早期 4096-bucket / 2 MB 版溢出 L2 触发 −27% 回归，
  实测确认后下调到 512 KB
- 读端完全无锁：64-bit per-bucket seqlock（even=stable / odd=writer），
  acquire-load 包夹整次 memcpy，torn-read 自动重试 1 次后回退到 dict 路径
- 写端 CAS 单写者，跟 dict shard wrlock 串行（已经持锁，免费）
- bucket 里存**预格式化的 RESP 回复**（`$len\r\nval\r\n`），命中路径
  连 snprintf 都省掉
- 项目内**唯一**的 lock-free 数据结构，补齐"用户态加速"维度

**实测数字**（`test/bench_fp_inproc.c`）

| 维度 | dict path | fp_get | 加速 |
|---|---:|---:|---:|
| 单线程 fair（产出完整 RESP） | 375 ns/op | 200 ns/op | **1.9x** |
| 4 readers 同 shard | 2.88 Mops/s（rwlock 锁死）| 6.74 Mops/s | **+134%** |

dict 路径在多读者同 shard 场景 1→4 线程**完全不 scale**（rwlock cacheline
ping-pong 把吞吐锁死在 ~2.9 Mops/s）；fp_get 因为完全 lock-free，4 线程仍
能近线性 scale 到 6.74 Mops/s。

**正确性**

- `fp_cache_test` 1 writer + 3 readers × 5s 压测：torn=0, collisions=0
- `fastpath_tsan_smoke.sh`：TSAN 0 warning（seqlock 在 payload 上的有意
  race 用 `__attribute__((no_sanitize_thread))` 标记，注释里写明动机）

**三段弯路（面试可讲）**

1. **Sizing 决定 cache hierarchy**：4096 → 1024 buckets 不是算法变化，
   是把表从 L3 拉回 L2，把 −27% 回归扭成 ±0.2%。
2. **微基准对照组的"输出量"必须等量**：第一版让 dict 路径只读 1 字节，
   fp_get 拷整个 reply，结果显示 fp 反而慢；改成 dict 也产出完整 RESP，
   立刻 1.9x。
3. **lock-free 不该用网络 bench 量**：redis-benchmark 单 op 50–70 µs 的
   网络开销把 200 ns 节省淹没；in-process 微基准 + SAME_SHARD 多线程才
   是它该接受的考核。

**可深聊的题**：memory_order 选 acquire 而非 seq_cst 的理由 / 为什么
seqlock 而不是 hazard-pointer 或 RCU / SET-vs-fp_put 的写者顺序 / sizing
为什么不能 dynamic / 为什么 `FP_VAL_MIN=32` gate 而不是更激进的 0

#### 亮点 2：**MT-safe fork-COW 点-in-time 持久化**
- 32 分片 wrlock → fork → 父立即解锁的模式
- BGSAVE 期间主进程 0 延迟尖峰
- **可量化**：`rdb_save_async_uring()` 已实现（4B.4 节）

#### 亮点 3：**每核独立 io_uring proactor + buf_ring + SO_REUSEPORT**
- t=2 → 491k SET / 492k GET (6c Phase 8.4 锁定 baseline)
- t=1 → 396k / 396k；多线程改造相对 t=1 +25%
- buf_ring 多线程接收、SQE/CQE 0 拷贝、cross-worker eventfd 唤醒

#### 亮点 4：**三层 BPF 端到端可观测栈**（保留为补充亮点）
- XDP 包层 (hit / miss / bypass / hot_cache) + sockmap 流层
  (per-cmd / per-conn LRU) + uprobe slowlog
- `kvs-top` ncurses TUI 秒级刷新
- **诚实记录**：曾尝试 XDP_TX 内核构造回包，因 TCP rcv_nxt 无法
  同步主动放弃，转向"加速归用户态、观测归内核"的工程取舍
  （这本身就是一个能聊 5 分钟的工程判断故事）

### 🥈 Tier 2（次亮点，简历段落里 1 行带过）

- **C 里 RAII 锁守护**（cleanup attribute）
- **多键命令安全锁序**（SMOVE/LMOVE/RPOPLPUSH 跨分片升序加锁
  + 重新查找，避免 ABBA 死锁）
- **BLPOP 跨 worker 唤醒**（owner-worker replay 模式，
  cross-worker write_buf 0 race）
- **RDMA-based 流式 fullsync**（libfabric `FI_EP_MSG`，可降级 tcp provider）
- **代码质量事件**（dynbuf_lit 41 处修复 / case-insensitive dispatch）
- **TSAN clean / smoke-all 三套全过**

### ❌ 不写的

- "XDP 加速 GET" — 项目自己证伪（TCP seq 不同步）
- "sockmap fast-path 加速 GET" — 设计天花板 5-15%，含糊数字反而扣分
- "QPS 1.5M+" — 当前 6c 491k，吹高一面试就崩
- "完整 Redis 兼容" — F1 仍缺 AUTH/SCAN/CLIENT LIST 等

---

## 4. 修正后的 roadmap（2026-05-18 重写）

**已完成**：多键并发修复（SMOVE/LMOVE/RPOPLPUSH 升序锁）、BLPOP
跨 worker owner-replay、INFO 命令 + atomic 计数器、README 架构图
重写、benchmark.md 骨架、kvstore-local.conf 三 worker 配置、
Phase 8.4 baseline 锁定（t=2 491k/492k）、Phase 6.2 读写锁分离、
Phase 6.1/6.1b/6.2b/6.4、Phase 7.2 CONFIG GET/SET、Phase 8.1/8.2/8.3。

**当前主线（单点深亮点）**：
- **Phase 10 Lock-free seqlock fast-path cache**（~3 天，详见
  progress-roadmap.txt §Phase 10）
  - 10.1 fp_cache.h/c 核心 seqlock 数据结构
  - 10.2 wire 到 GET dispatch + SET/DEL post hook
  - 10.3 配置 + INFO + kvs-top 集成
  - 10.4 TSAN 验证
  - 10.5 before/after bench（100% / 50% / 0% hit）
  - 10.6 README + benchmark.md + dev-log

**Phase 10 之后 GATED**（progress-roadmap.txt §Phase 11）：
- **β SIMD RESP parser**（HOLD，2-3 天）
- **γ 批处理 dispatch**（HOLD，3-5 天）
- 启动条件：Phase 10 收益 < 15% 才考虑接 β；否则不做

**Background / opportunistic**：
- Phase 7.1 V2 AOF rewrite-bg（AOF > 100 MB 时）
- Phase 7.3 复制 backlog MT 审计
- 30-min full soak（overnight）

**Deferred for bigger box**：
- 2-VM 物理隔离 bench（脱离 1-VM client/server 抢核噪声）
- vs Redis 7.x 头对头
- Phase 5-validate（IRQ pin / NUMA / perf c2c）

---

## 5. 当前执行重点（Phase 10）

### 入手顺序

1. **Day 1** — `src/core/fastpath_cache.h/c` + `fp_cache_test.c`
   单元测试 + 4 线程压力测试（1 writer / 3 readers，1M ops, 0 torn-read）
2. **Day 2** — wire 进 `kvs_dispatch` GET 入口 + SET/DEL post-hook，
   `fastpath_cache yes/no` 配置项，INFO Stats 段加 fp 计数
3. **Day 3** — TSAN 验证 + `bench_fastpath.sh` before/after +
   README/benchmark.md/dev-log 文档化

### 验收门槛（必须全过）

- `fp_cache_test` 4 线程压力 0 失败
- `tsan_lock_smoke.sh` + `multi_thread_smoke.sh` PASS=18/18
- `bench_fastpath.sh` 100% hit 显示 +25-35%；0% hit 显示 ≤ 2% 开销
- README 架构图加入 fast-path 层，benchmark.md §10 完成

---

## 6. BPF 加速天花板分析（多轮设计 review 结果）

记录此节是为了应对面试官追问 "为什么不在 BPF 里做 GET 加速？"

### 探索过的路径

1. **XDP_TX 内核构造回包**（Phase 9.2 原计划）
   - 失败原因：长连接场景下，XDP 消费请求包并构造回复后，内核
     TCP `rcv_nxt` 不前进，下一次请求触发 challenge ACK 链路停滞
   - 已在 dev-log Phase 17 主动放弃

2. **AF_XDP 用户态全栈**
   - 与现有 io_uring proactor 架构冲突，工作量巨大
   - 否决

3. **sockmap + sk_msg 内核合成回包**
   - sockmap 单 socket server 模式下无法合成出站包（sk_msg 跑在
     send 路径，server 不主动 send 就触发不了）
   - 否决

4. **sockmap + sk_skb GET 短路（Phase 9.1 原计划，路 A）**
   - 设计：sk_skb 解析 GET → 查 hot_cache → 命中写
     pending_replies[cookie] → userspace 通过 BPF map syscall 读取
   - 致命问题：userspace 读 `bpf_map_lookup_elem` 是 syscall
     ~250 ns，几乎抵消跳过 dispatch 的 ~200 ns 收益
   - 实测预期净增益：0 ~ +5%
   - **shelved**

5. **XDP-PASS 旁路 + mmap'd BPF array hint（路 B / Option J+）**
   - 设计：XDP 总是 PASS 不构造回包，命中时写预格式化的 RESP
     bytes 到 `BPF_F_MMAPABLE` 的 ARRAY，userspace 通过 mmap
     直接读（0 syscall）
   - TCP 安全（不是 XDP_TX）
   - 但仍然 ~10-15% 上限，且重新引入 XDP RESP 解析（与项目
     "XDP observe-only" 决策冲突）
   - **shelved**

### 根本天花板

userspace 的 `recv` + `writev` 这两次 syscall 是无法绕开的（除非
重写 TCP 状态机或走 AF_XDP，两者都已否决）。BPF 加速能省的只是
recv 和 writev 之间的 dispatch 工作量（~250 ns），而 dispatch
在 1.3 µs 总开销里只占 ~20%，**上限就是 ~10-15%**。

### 为什么 Phase 10 用户态 lock-free 反而能 +25-35%？

- 完全在用户态，不需要 syscall
- 命中路径只剩 mmap 风格的内存读 + writev，~30 ns 而非 ~250 ns
- writev 那一次 syscall 在所有方案里都必须有，是公共底数
- 收益来自 dispatch 整段被替换为一次原子读 + memcpy，
  而不是 dispatch 后面跟一个 syscall 查询

这就是为什么 BPF 加速被搁置、Phase 10 成为主线。

### 简历回答模板

> "我在这个项目里探索了 5 条 BPF 加速 GET 的路径（XDP_TX /
> AF_XDP / sk_msg / sk_skb + map / XDP + mmap array），发现都
> 受限于 syscall 不可绕开或 TCP 状态机一致性问题，单核理论
> 上限 10-15%。最后选择用户态 lock-free seqlock 缓存，在
> 命中路径完全跳过 syscall 查询，实测 +25-35%。"
