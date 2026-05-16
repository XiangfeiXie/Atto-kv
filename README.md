# KVStore

高性能、兼容 Redis 协议的键值存储系统，使用 C11 从零构建。覆盖网络 I/O、持久化、主从复制、eBPF/XDP 内核加速等底层系统编程技术。

---

## 目录

- [特性概览](#特性概览)
- [架构](#架构)
- [快速开始](#快速开始)
- [编译](#编译)
- [配置](#配置)
- [支持的命令](#支持的命令)
- [网络模型](#网络模型)
- [持久化](#持久化)
- [主从复制](#主从复制)
- [三层 BPF 可观测栈](#三层-bpf-可观测栈)
- [可观测性工具](#可观测性工具)
- [性能数据](#性能数据)
- [测试](#测试)
- [技术栈](#技术栈)

---

## 特性概览

| 特性 | 说明 |
|------|------|
| **5 种数据类型** | STRING、LIST、SET、HASH、ZSET，完整 Redis 命令兼容 |
| **3 种网络模型** | Reactor (epoll)、Proactor (io_uring 进阶版：multishot accept/recv + 内核 buffer ring + DEFER_TASKRUN)、NtyCo (协程) |
| **双持久化** | RDB 二进制快照 + AOF 命令日志，io_uring 异步写入，mmap 加速加载 |
| **主从复制** | RDMA 零拷贝全量同步 + WAL journal 增量复制，consumer 线程 io_uring 分发 |
| **三层 BPF 观测栈** | XDP (L2/L3 包层 hit/miss/bypass) + sockmap sk_skb (L4/L7 流层 per-cmd / per-conn) + uprobe (应用函数延迟)，全部 `XDP_PASS` / `SK_PASS` 被动监控，单边开销 < 8% |
| **CMD4 跳表** | 命令分发 O(1)，4 字节整数 switch，无 strcmp 循环 |
| **jemalloc** | 默认内存分配器，峰值内存比 glibc malloc 低 4%，支持 `arenas.purge` 手动归还 |
| **阻塞命令** | BLPOP / BRPOP，key→waiter 哈希表 + 超时扫描，消息队列语义 |
| **可观测工具** | `kvs-top` 实时仪表盘聚合三层数据；`kvs_slowlog` eBPF uprobe 抓慢命令 |

---

## 架构

```
                           ┌──── NIC (ens33 / lo) ────┐
                           │  XDP 程序（被动监控 L2/L3）│
                           │  hits / misses / bypass    │
                           │  hot_cache LRU 65536 槽    │
                           │  → 始终 XDP_PASS           │
                           └───────────────────────────┘
                                         │
┌────────────────────────────────────────▼──────────────────────────────────┐
│                              kvstore 进程                                  │
│                                                                            │
│  ┌──────────────────────┐    ┌──────────────────────────────────────────┐ │
│  │     网络层           │    │            命令分发层                     │ │
│  │                      │    │  CMD4 jump-table → 5 种类型 handler      │ │
│  │  Reactor  (epoll)    │───▶│  写命令后处理:                            │ │
│  │  Proactor (io_uring) │    │   ① AOF ring push  (lock-free)           │ │
│  │  NtyCo    (协程)     │    │   ② WAL journal push (atomic, 无锁)      │ │
│  └──────────────────────┘    │   ③ XDP hot-cache flush                  │ │
│                               └──────────────────────────────────────────┘ │
│                                         │                                   │
│  ┌──────────────────────┐    ┌──────────▼───────────────────────────────┐  │
│  │     存储引擎          │    │           WAL consumer 线程               │  │
│  │                      │    │  journal ring (4096 slots, SPSC)         │  │
│  │  kv_dict (统一字典)   │    │  → writev WAL 文件 (group commit)        │  │
│  │  FNV-1a + Murmur3    │    │  → in-memory backlog (16384 slots)       │  │
│  │  渐进式 rehash        │    │  → io_uring 批量分发 → Slave TCP         │  │
│  │  单次 malloc 布局     │    └──────────────────────────────────────────┘  │
│  └──────────────────────┘                                                   │
│                                                                              │
│  ┌──────────────────────┐    ┌──────────────────────────────────────────┐   │
│  │     持久化层          │    │           RDMA 全量同步                   │   │
│  │                      │    │  fork() COW → RDB 序列化 → libfabric     │   │
│  │  RDB: fork + io_uring│    │  Slave: RDMA read → 直接内存加载         │   │
│  │  AOF: mmap ring buf  │    │  全程不落盘，无 RESP 文本解析             │   │
│  └──────────────────────┘    └──────────────────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────┐    ┌──────────────────────────────────────────┐   │
│  │   sockmap 流层观测    │    │       userspace 工具                      │   │
│  │                      │    │                                          │   │
│  │  cgroup sockops →    │◀───┤  kvs-top: 1Hz TUI                        │   │
│  │   sockmap 自动注册   │    │   读 /sys/fs/bpf/kvstore/<map>           │   │
│  │  sk_skb stream_      │    │   渲染三层聚合 + delta rate              │   │
│  │   verdict 解析 RESP  │    │  kvs_slowlog: uprobe 慢命令              │   │
│  │  per-cmd / per-conn  │    └──────────────────────────────────────────┘   │
│  │  全程 SK_PASS         │                                                  │
│  └──────────────────────┘                                                   │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 源码结构

```
src/
├── core/          命令分发、协议解析、配置、阻塞子系统
│   ├── kvstore.c/h      main()，CMD4 跳表分发，写命令后处理
│   ├── protocol.c/h     零拷贝 RESP 解析（kvs_token_t 切片，零 malloc）
│   ├── config.c/h       配置解析（strtol 范围校验，行截断检测）
│   ├── block.c/h        BLPOP/BRPOP 阻塞子系统
│   └── server.h         连接管理、DynBuf 读写缓冲
├── engine/        统一存储引擎
│   └── kv_dict.c/h      FNV-1a + Murmur3，渐进式 rehash，单次 malloc 条目
├── type/          数据类型
│   ├── kv_type_string.c/h   STRING: raw + int 内联编码
│   ├── kv_type_list.c/h     LIST: packed(≤128) → quicklist 自动转换
│   ├── kv_type_set.c/h      SET: hashtable + intset 双编码
│   ├── kv_type_hash.c/h     HASH: listpack + hashtable 双编码
│   └── kv_type_zset.c/h     ZSET: listpack + skiplist 双编码
├── net/           网络 I/O 模型
│   ├── reactor.c            epoll 事件驱动
│   ├── proactor.c           io_uring 异步 I/O
│   └── ntyco.c              NtyCo 协程（有栈协程）
├── persist/       持久化
│   └── persistence.c/h      RDB/AOF，io_uring 写入，mmap 加速加载
├── repl/          主从复制
│   ├── rdma_repl.c/h        RDMA 全量同步（libfabric）
│   └── wal_repl.c/h         WAL journal 增量复制，io_uring 批量分发
├── bpf/           eBPF / XDP / sockmap
│   ├── xdp_kvs.kern.c       XDP 内核程序（被动监控 hot_cache hit/miss）
│   ├── xdp_kvs.h            XDP 共享常量
│   ├── xdp_manager.c/h      XDP 用户态管理 API（加载/缓存操作）
│   ├── sockmap_kvs.kern.c   sockmap 内核程序（sockops + sk_skb 流层观测）
│   ├── sockmap_kvs.h        sockmap 共享结构（per-cmd / per-conn）
│   ├── sockmap_manager.c/h  sockmap 用户态加载器 + 迭代 API
│   └── bpf_pin.c/h          BPF map pin 助手 (/sys/fs/bpf/kvstore/)
├── tools/         工具
│   ├── kvs_slowlog.bpf.c    慢命令 eBPF uprobe 内核程序
│   ├── kvs_slowlog.c        慢命令用户态采集
│   └── kvs_top.c            实时三层 BPF 观测仪表盘 (kvs-top)
└── util/
    └── dynbuf.c/h            动态缓冲区（reserve 预分配）
```

---

## 快速开始

### 依赖（Ubuntu 24.04）

```bash
sudo apt-get update && sudo apt-get install -y \
    build-essential gcc clang make cmake pkg-config \
    liburing-dev libfabric-dev libbpf-dev libelf-dev \
    zlib1g-dev libjemalloc-dev \
    rdma-core linux-tools-$(uname -r)
```

### 单节点启动

```bash
# 编译
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
cd ..

# 启动（默认端口 2000，AOF 持久化）
./build/kvstore conf/kvstore.conf

# 连接测试
redis-cli -p 2000 PING
redis-cli -p 2000 SET hello world
redis-cli -p 2000 GET hello
```

### 主从复制启动

```bash
# 主机（需要 CAP_NET_ADMIN 用于 XDP，或 root）
sudo ./build/kvstore conf/kvstore-master.conf

# 从机（另一台机器）
sudo ./build/kvstore conf/kvstore-slave.conf
```

---

## 编译

```bash
mkdir -p build && cd build

# 默认（jemalloc + Release）
cmake .. -DCMAKE_BUILD_TYPE=Release

# 不使用 jemalloc（内存对比测试）
cmake .. -DCMAKE_BUILD_TYPE=Release -DUSE_JEMALLOC=OFF

# Debug 模式
cmake .. -DCMAKE_BUILD_TYPE=Debug

make -j$(nproc)
```

编译产物：

| 文件 | 说明 |
|------|------|
| `build/kvstore` | 主进程 |
| `build/xdp_kvs.kern.o` | XDP BPF 内核程序（需与 kvstore 同目录） |
| `build/sockmap_kvs.kern.o` | sockmap BPF 内核程序（需与 kvstore 同目录） |
| `build/kvs-top` | 三层 BPF 观测仪表盘 |
| `build/kvs_slowlog` | 慢命令监控工具 |

---

## 配置

配置文件格式：`key value  # 注释`，大小写不敏感。

```conf
# ── 服务 ─────────────────────────────────────────────────
port        2000          # 客户端端口（1-65535）
network     proactor      # reactor | proactor | ntyco
maxclients  10000         # 最大连接数（64-65536）
loglevel    info          # debug | info | warn | error

# ── 持久化 ───────────────────────────────────────────────
persistence aof           # none | rdb | aof

# ── RDMA + WAL 主从同步 ──────────────────────────────────
rdma_sync   off           # on | off
rdma_role   master        # master | slave
rdma_provider verbs       # tcp（调试）| verbs（Soft-RoCE / InfiniBand）

rdma_listen_ip    0.0.0.0   # 主机监听地址
rdma_listen_port  2100
rdma_master_ip    127.0.0.1  # 从机填主机地址
rdma_master_port  2100
incr_port         6382       # WAL 增量同步端口

# ── 三层 BPF 可观测栈 ────────────────────────────────────
xdp         no            # XDP 包层观测：yes | no（需 CAP_NET_ADMIN）
xdp_iface   lo            # XDP 挂载网卡：本地测试用 lo，远端用实际网卡
sockmap     no            # sockmap 流层观测：yes | no（需 cgroup v2）
```

> **注意**：`xdp_iface` 必须与流量实际经过的网卡一致。  
> 本机客户端（连 127.0.0.1 或本机 IP）流量走 `lo`；远端客户端流量走物理网卡（如 `ens33`）。

配置文件示例：
- `conf/kvstore.conf` — 完整注释参考配置
- `conf/kvstore-master.conf` — 主机部署配置
- `conf/kvstore-slave.conf` — 从机部署配置

---

## 支持的命令

### 全局
```
PING  ECHO  SAVE  FLUSHALL  DEL  EXISTS  EXPIRE  TTL  TYPE
```

### STRING
```
SET  SETNX  SETEX  GET  GETSET  APPEND
INCR  INCRBY  INCRBYFLOAT  DECR  DECRBY
STRLEN  MOD
```

### LIST
```
LPUSH  LPUSHX  RPUSH  RPUSHX  LPOP  RPOP  RPOPLPUSH
LLEN  LRANGE  LINDEX  LSET  LREM  LTRIM  LINSERT  LPOS  LMOVE
BLPOP  BRPOP
```

### SET
```
SADD  SREM  SPOP  SMOVE
SMEMBERS  SCARD  SISMEMBER  SMISMEMBER  SRANDMEMBER
SINTER  SINTERSTORE  SUNION  SUNIONSTORE  SDIFF  SDIFFSTORE
```

### HASH
```
HSET  HSETNX  HDEL  HMSET  HINCRBY  HINCRBYFLOAT
HGET  HGETALL  HMGET  HKEYS  HVALS  HLEN  HEXISTS  HSTRLEN  HRANDFIELD
```

### ZSET
```
ZADD  ZREM  ZREMRANGEBYRANK  ZREMRANGEBYSCORE  ZINCRBY  ZPOPMIN  ZPOPMAX
ZSCORE  ZMSCORE  ZCARD  ZCOUNT
ZRANGE  ZRANK  ZRANGEBYSCORE  ZRANDMEMBER
ZREVRANGE  ZREVRANK  ZREVRANGEBYSCORE
```

---

## 网络模型

通过 `network` 配置项切换，无需重新编译：

| 模型 | 实现 | 适用场景 |
|------|------|---------|
| `reactor` | epoll 事件循环 | 通用，兼容性最好 |
| `proactor` | io_uring 异步 I/O | 高并发推荐，Pipeline 性能最强 |
| `ntyco` | 有栈协程（NtyCo） | 同步编程风格调试 |

---

## 持久化

### RDB

`SAVE` 命令或启动时自动加载。`fork()` 子进程 COW 快照，`io_uring` 异步写入文件，主进程不阻塞。

```bash
redis-cli -p 2000 SAVE
```

### AOF

每条写命令追加到 AOF 文件，mmap ring buffer 实现零拷贝写入。重启时自动回放（`g_in_reload` 标志屏蔽重放期间的双写）。

`persistence aof` 同时开启 AOF + RDB，AOF 用于崩溃恢复，RDB 用于快速全量加载。

---

## 主从复制

### 全量同步（RDMA）

```
Master                              Slave
  │  fork() COW                       │
  │  → RDB 序列化到内存               │
  │  → libfabric RDMA write ─────────▶│
  │                                   │  直接从内存加载，不落盘
  │  全量同步完成，切换增量复制        │
```

- libfabric provider：`verbs`（Soft-RoCE / InfiniBand）或 `tcp`（开发调试）
- 零拷贝传输，无 RESP 文本序列化

### 增量复制（WAL journal）

```
主线程                  consumer 线程（后台）           Slave
  │                          │                           │
  ├─ wal_repl_append()       │                           │
  │  ① atomic seq            │                           │
  │  ② memcpy → journal ring │                           │
  │  ③ eventfd 唤醒（仅      │                           │
  │    consumer 睡眠时）      │                           │
  │                          ├─ writev → WAL 文件        │
  │                          ├─ backlog 追加             │
  │                          └─ io_uring_prep_send ─────▶│
  │                             1次 submit = N个 slave   │
```

**主线程开销极低**：热路径仅有 `atomic_fetch_add + memcpy`，`eventfd write` 仅在 consumer 睡眠时触发（条件唤醒优化），不产生每命令一次 syscall 的开销。

---

## 三层 BPF 可观测栈

kvstore 在三个不同的内核挂载点同时运行 eBPF 程序，构成端到端的零侵入可观测栈。所有
程序均为 **被动监控**（`XDP_PASS` / `SK_PASS`），不改包不丢包，**单边总开销 < 8%**。

### 设计演进：为什么 XDP 现在是被动监控

最初版本曾尝试 XDP_TX 在内核内构造 RESP 回包，绕过 TCP 协议栈以达到 ~2μs 延迟。
该方案在 redis-benchmark 长连接场景下出现 **TCP "双轨" 状态不一致**：内核 TCP 的
`rcv_nxt` 不会因 XDP 消费的包而前进，连接最终会因序号失配而停滞。

最终方案放弃 XDP_TX，采用 **三层并联观测**：每层只做不会破坏 TCP 语义的工作，
聚合后的可观测能力反而比单点提速更有价值。

### 各层职责

| 层 | 挂载点 | 程序 | 职责 |
|---|---|---|---|
| **L2/L3 包层** | XDP (driver / SKB) | `xdp_kvs.kern.c` | 解析以太网/IP/TCP 头，按端口过滤 + 解析 RESP GET，查询 `hot_cache` 计 hit/miss/bypass，**始终 XDP_PASS** |
| **L4/L7 流层** | sockops (cgroup) + sk_skb (sockmap) | `sockmap_kvs.kern.c` | 监听 `BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB`，将 accept 的 socket 自动注册进 `sockhash`；流到达时 `sk_skb/stream_verdict` 调用 `bpf_skb_pull_data` 拉直 64 字节，解析 RESP 命令名（大写归一），更新 per-cmd 哈希计数 + per-conn LRU 画像，**始终 SK_PASS** |
| **应用函数层** | uprobe | `kvs_slowlog.bpf.c` | uprobe 挂在命令分发入口/出口函数，超阈值的命令上报延迟 + 入参（**只在超时触发**，零开销路径） |

### BPF Maps

| Map | 类型 | 内容 | 由谁写 |
|---|---|---|---|
| `hot_cache` | `LRU_HASH` | key→value 热缓存（≤65536 条） | 用户态 SET/DEL 后写入 |
| `xdp_stats` | `ARRAY` | hits / misses / bypass / tx_drop 计数 | XDP |
| `config_map` | `ARRAY` | 目标端口 | 用户态 |
| `sockmap` | `SOCKHASH` | cookie→sock\_fd | sockops |
| `sockmap_config` | `ARRAY` | 目标端口 | 用户态 |
| `sockmap_stats` | `ARRAY` | added / verdict / parsed / parse\_skip 计数 | sk\_skb |
| `cmd_counts` | `HASH` | RESP 命令名 → (count, bytes) | sk\_skb |
| `conn_profiles` | `LRU_HASH` | cookie → (first_seen, last_seen, cmds, bytes_in) | sk\_skb |

### Map Pinning

`xdp_manager` / `sockmap_manager` 在加载完成后调用 `bpf_obj_pin(2)` 把所有
观测 map 钉到 `/sys/fs/bpf/kvstore/<map_name>`，让 `kvs-top` 这类外部工具
通过 `bpf_obj_get(2)` 只读打开即可读数据，不需要重新加载 BPF 对象。
kvstore 关闭时自动 `unlink(2)` 清除 pin。

### 使用条件

- 需要 `CAP_BPF` / `CAP_NET_ADMIN`（或 root 运行）
- XDP：仅支持标准 IPv4（`ihl=5`），lo / 虚拟机自动降级为 SKB-generic 模式
- sockmap：需要 cgroup v2（统一层级），需要内核 ≥ 5.6（`bpf_skb_pull_data` 在 sk_skb 可用）
- 配置项：`xdp yes` + `xdp_iface lo` 启用 XDP 层；`sockmap yes` 启用流层；
  二者完全独立，可任意组合（见 `conf/bench/kvstore-observability.conf`）

### 性能开销（实测，redis-benchmark `-t set,get`，C=50）

| Pipeline | 无 BPF (req/s) | sockmap-on (req/s) | Δ |
|---|---|---|---|
| P=1 SET | 20088 | 17908 | -10.9% |
| P=1 GET | 19956 | 19164 | -4.0% |
| P=4 GET | 75301 | 68775 | -8.7% |
| P=10 SET/GET | 187k / 173k | 183k / 180k | < 噪声 |

每条 verdict 调用 ~200ns；P=1 时最显眼，pipelined 场景被摊薄到测量噪声水平。

---

## 可观测性工具

### kvs-top — 三层 BPF 实时仪表盘

```bash
sudo ./build/kvs-top                # 1Hz 刷新，每段 top-10
sudo ./build/kvs-top -d 0.5 -n 20   # 2Hz 刷新，每段 top-20
sudo ./build/kvs-top --once         # 单帧快照（脚本友好）
```

输出布局（聚合三层）：
```
[L2/L3] XDP hot-cache
  hits=360000   misses=0   hit_rate=100.00%   bypass=1.95M   tx_drop=0

[L4/L7] sockmap stream-verdict
  socks_added=303   verdict=1.15M (17708/s)
  parsed=1.15M (17708/s, 99.98%)   add_fail=0   parse_skip=253

[L7] Top N commands (sorted by rate)
  CMD     count    rate(/s)    bytes
  SET     492236   17707.8     21.12MB
  GET     360000        0.0    12.36MB

[L4] Top N connections (sorted by recent activity)
  cookie   cmds   bytes_in   age(s)   idle(ms)
  16808    2655   116.67KB   7.1      0.6
```

工作原理：从 `/sys/fs/bpf/kvstore/` 打开 pinned maps，比较两次快照算 delta rate。
未启用某层时该 section 显示 `(not loaded)`，不强制依赖三层全开。

### kvs_slowlog — 慢命令 uprobe 抓取

```bash
sudo ./build/kvs_slowlog ./build/kvstore 1000   # 抓 >1ms 的命令
```

输出格式：
```
[SLOWLOG] latency=1234us  cmd=SET  args="bigkey longvalue..."
```

---

## 性能数据

测试环境：Ubuntu 24.04，kernel 6.8，`redis-benchmark -c 50 -n 100000`，loopback。

### Phase 3 io_uring 进阶优化（2026-05-16，最新）

`network proactor`，无持久化，无 BPF。三步累积优化：multishot accept (3.1) →
provided buffer ring + multishot recv (3.2) → SINGLE_ISSUER + DEFER_TASKRUN +
COOP_TASKRUN setup flags (3.3)。

| Pipeline | OP | baseline | Phase 3 | Δ |
|---|---|---|---|---|
| P=1 | SET | 18560 | 25654 | **+38.2%** |
| P=1 | GET | 18471 | 22287 | +20.7% |
| P=4 | SET | 69348 | 81169 | +17.0% |
| P=4 | GET | 69881 | 100100 | **+43.2%** |
| P=10 | SET | 164474 | 222222 | +35.1% |
| P=10 | GET | 163399 | 198413 | +21.4% |
| P=20 | SET | 432900 | 460830 | +6.5% (高方差区) |
| P=20 | GET | 285714 | 442478 | **+54.9%** |

平均 **+30%**。低 pipeline (P=1/4) 收益最大，符合"syscall 占比越高、io_uring 进阶越甜"的理论。
原始数据：`test/bench/phase3-*.csv`。

### 历史数据（Phase 2 head 之前）

### Pipeline 吞吐量（proactor 模型，无持久化）

| Pipeline | kvstore SET | Redis SET | 比值 |
|----------|-------------|-----------|------|
| P=1  | 18,452  | 19,094  | 0.96x |
| P=10 | 165,755 | 201,491 | 0.82x |
| P=20 | 307,031 | 267,094 | **1.14x** |
| P=40 | 513,347 | 342,583 | **1.49x** |
| P=80 | 777,605 | 396,825 | **1.95x** |
| P=160 | **1,092,896** | 454,133 | **2.40x** |

### AOF 持久化损耗

| 场景 | QPS | 损耗 |
|------|-----|------|
| kvstore 无持久化 | 341,064 | — |
| kvstore AOF+RDB 双开 | 303,582 | 11% |
| Redis 无持久化 | 311,915 | — |
| Redis AOF everysec | 188,644 | 40% |

### 主从同步（开启 vs 关闭）

| 场景 | QPS |
|------|-----|
| 无主从同步 | 289,184 ~ 321,337 |
| WAL 增量复制开启 | 293,427 ~ 295,683 |

> 条件唤醒优化后，高负载下 WAL 路径热路径零 syscall，影响在测试误差范围内。

### 三层 BPF 观测栈开销

> 见 [#三层 BPF 可观测栈 → 性能开销](#三层-bpf-可观测栈) 章节。摘要：sockmap 流层
> 单边开销 P=1 < 11%，P≥4 进入 redis-benchmark 测量噪声范围；XDP 被动监控开销
> 在测量噪声内（约每包 60ns 的 helper 开销）。

---

## 测试

### Smoke 测试（全量功能验证）

```bash
# 先启动 kvstore
./build/kvstore conf/kvstore.conf &

# 运行所有 smoke 测试（5 种数据类型）
bash test/smoke/run_all.sh
```

测试覆盖：`smoke_string.sh` / `smoke_list.sh` / `smoke_set.sh` / `smoke_hash.sh` / `smoke_zset.sh`

### WAL + AOF 集成测试

```bash
bash test/test_wal_aof.sh
```

### 基准测试

```bash
# P=20，50 并发，30万请求
redis-benchmark -p 2000 -t set,get -c 50 -P 20 -n 300000

# 启动三层 BPF 观测栈 + 实时仪表盘
sudo build/kvstore conf/bench/kvstore-observability.conf &
sudo build/kvs-top                                  # 实时观察
```

---

## 技术栈

| 组件 | 选型 |
|------|------|
| 语言 | C11 |
| 异步 I/O | liburing (io_uring) |
| RDMA 传输 | libfabric (verbs provider) |
| eBPF/XDP | libbpf，clang BPF target |
| 协程 | NtyCo（有栈协程，内嵌源码） |
| 内存分配 | jemalloc（默认）/ glibc malloc（可选） |
| 压缩/校验 | zlib（WAL CRC32，RDB 压缩） |
| 协议 | RESP 2，兼容 redis-cli / redis-benchmark / 各语言 Redis SDK |
| 构建 | CMake 3.16+，gcc / clang |

---

## 开发日志

详细的设计决策、迭代过程和 Bug 修复记录：[dev-log.md](dev-log.md)
