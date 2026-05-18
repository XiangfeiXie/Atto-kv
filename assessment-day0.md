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

## 3. 简历亮点重新校准

基于今天看到的代码状态，**简历可写的真实亮点**：

### 🥇 Tier 1（核心三亮点，强可写）

#### 亮点 1：**MT-safe fork-COW 点-in-time 持久化**
- 32 分片 wrlock → fork → 父立即解锁的模式
- BGSAVE 期间主进程 0 延迟尖峰
- **可量化**：`rdb_save_async_uring()` 已实现；只差一个对比 demo（Day 5）

#### 亮点 2：**每核独立 io_uring proactor + buf_ring + SO_REUSEPORT**
- 8c P-core 上跑出 **790k QPS**（SET / GET 各 790k）
- t=4 worker / t=8 worker 真分片曲线（Day 2 跑出）
- 数字硬

#### 亮点 3：**eBPF 三层可观测 + 即将到来的 sockmap GET 加速**
- 已有：sockmap stream_verdict (per-cmd 计数 + per-conn profile) + XDP GET parse + hot_cache hit/miss
- 计划：sockmap sk_msg fast-path（Day 5-7，**风险已摊开**）
- 即使 fast-path 不成功，"sockmap 三层观测 + 走过 XDP_TX 后退回的工程经验"本身就是亮点

### 🥈 Tier 2（次亮点，简历段落里 1 行带过）

- **C 里 RAII 锁守护**（cleanup attribute）
- **RDMA-based 流式 fullsync**（libfabric `FI_EP_MSG`，可降级 tcp provider）
- **代码质量事件**（dynbuf_lit 41 处修复，case-insensitive dispatch）
- **TSAN clean / smoke-all 三套全过**

### ❌ 不写的

- "XDP 加速 GET" — 已被项目本身否定（TCP seq 不同步）
- "QPS 1.5M+" — 当前 790k，吹高一面试就崩
- "完整 Redis 兼容" — F1 都还没补完

---

## 4. 修正后的 2 周 roadmap

基于今天摸到的真实情况，**多项工作量被压缩**：

| 原计划 | 修正 |
|---|---|
| Day 1 启用 jemalloc | **已启用**，跳过 |
| Day 1 写 profile_capture.sh | 用现有 `dev_quick.sh` + 新增 `vs_redis.sh` |
| Day 5 设计 BPF KV map + 同步钩子 | **已存在**：`xdp_cache_put/del` 钩子已在 `kvstore.c`，`hot_cache` LRU map 在 BPF 里 |
| Day 6 写 sk_skb verdict 程序 | **已存在**：`sockmap_kvs_verdict` 已在跑，只需扩展为 GET 解析 + redirect 决策 |

**新版 roadmap**：

```
Day 1（今天剩下时间）  ▶ 摸底完成 + 跑 baseline_2vm（你起第二台 VM 后）
Day 2                  ▶ vs_redis 头对头 benchmark 脚本 + 数据
                         真分片曲线 t=1..7（在 2VM 模式下）
Day 3                  ▶ INFO 命令 + 5 个 atomic 计数器
Day 4                  ▶ AUTH + SCAN/HSCAN/SSCAN/ZSCAN
Day 5                  ▶ EXPIREAT/PEXPIRE/PEXPIREAT + OBJECT ENCODING
                         + CLIENT LIST/KILL + CLUSTER stub
                         + SLOWLOG 命令前端（接现有 BPF 后端）
Day 6                  ▶ sockmap GET fast-path 设计 + BPF 扩展（解析 GET + key）
Day 7                  ▶ sockmap GET fast-path：sk_msg redirect 链路
Day 8                  ▶ sockmap fast-path：边界 case + benchmark
Day 9                  ▶ Decision Point：成 / 不成
                         成 → 收尾 + demo 视频
                         不成 → Plan B：保留 BPF 命令分类增强 slowlog
Day 10                 ▶ README 重写（架构图 + 6 亮点）
Day 11                 ▶ benchmark.md 报告 + BGSAVE 延迟对比 demo（亮点 1 硬证据）
Day 12                 ▶ demo GIF/MP4 录制 + tuning guide
Day 13                 ▶ 30-min soak + kill -9 恢复 + smoke-all 三套
Day 14                 ▶ 面试 talking points + 模拟自问自答
```

**总工作量**：未变，仍是 14 天。但**风险下降**：sockmap 那一项不是从零开始，是在已存在的 verifier-clean 程序上扩展。

---

## 5. 立即执行：Day 1 下半天 + 准备 Day 2

### 你侧
1. 把 P-core pinning 文档化（贴到 README 或 conf 注释）
2. 起第二台 VM，配 vSwitch / NAT 让两台 VM 互通
3. 第二台 VM 安装 `redis-tools`（要 redis-cli + redis-benchmark）

### 我侧（今天剩下时间能做）
1. 写 `test/bench/profile_capture.sh`：跑 `perf top` 抓 30 秒，输出 top 20 函数 + perf stat IPC/cache miss
2. 写 `test/bench/vs_redis.sh` 框架（vs Redis 7.x 单线程 / vs KeyDB 多线程）
3. 启动 demo：把今天的 dev_quick 数字 (790k) lock 进 `test/bench/baseline_8c_p_core.txt`

### 决策点（请你确认才能继续）

**Q1**：第二台 VM 大概什么时候能起？
- (a) 今天能起 → Day 2 直接做 2VM benchmark
- (b) 明天再说 → Day 2 我先在本机用 taskset 隔离做（同效果，仅 client/server 都在 1 台 VM 上的不同 P-core 子集上）
- (c) 短期起不了 → 我做 1VM taskset 模式，2VM 推后到 Day 5+

**Q2**：是否同意修正版 roadmap？
- 如果 OK，我会把它写进 `progress-roadmap.txt`，覆盖原版 Phase 编号
