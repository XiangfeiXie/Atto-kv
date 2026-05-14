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
- [XDP 热路径加速](#xdp-热路径加速)
- [可观测性工具](#可观测性工具)
- [性能数据](#性能数据)
- [测试](#测试)
- [技术栈](#技术栈)

---

## 特性概览

| 特性 | 说明 |
|------|------|
| **5 种数据类型** | STRING、LIST、SET、HASH、ZSET，完整 Redis 命令兼容 |
| **3 种网络模型** | Reactor (epoll)、Proactor (io_uring)、NtyCo (协程)，配置切换 |
| **双持久化** | RDB 二进制快照 + AOF 命令日志，io_uring 异步写入，mmap 加速加载 |
| **主从复制** | RDMA 零拷贝全量同步 + WAL journal 增量复制，consumer 线程 io_uring 分发 |
| **XDP 热路径** | eBPF XDP 程序在网卡驱动层拦截 GET，热缓存命中直接 XDP_TX 回包，延迟 ~2 μs |
| **CMD4 跳表** | 命令分发 O(1)，4 字节整数 switch，无 strcmp 循环 |
| **jemalloc** | 默认内存分配器，峰值内存比 glibc malloc 低 4%，支持 `arenas.purge` 手动归还 |
| **阻塞命令** | BLPOP / BRPOP，key→waiter 哈希表 + 超时扫描，消息队列语义 |
| **慢命令监控** | `kvs_slowlog` eBPF uprobe 工具，零侵入采集超阈值命令 |

---

## 架构

```
                           ┌──── NIC (ens33 / lo) ────┐
                           │  eBPF XDP 程序             │
                           │  热缓存命中 → XDP_TX 2μs  │
                           │  未命中    → XDP_PASS ↓   │
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
├── bpf/           eBPF/XDP
│   ├── xdp_kvs.kern.c       XDP 内核程序（BPF）
│   ├── xdp_kvs.h            共享常量（BPF maps、RESP 格式）
│   └── xdp_manager.c/h      用户态管理 API（加载/挂载/缓存操作）
├── tools/         工具
│   ├── kvs_slowlog.bpf.c    慢命令 eBPF uprobe 内核程序
│   └── kvs_slowlog.c        用户态采集输出
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

# ── XDP 热路径 ───────────────────────────────────────────
xdp         no            # yes | no（需 CAP_NET_ADMIN）
xdp_iface   lo            # 挂载网卡：本地测试用 lo，远端压测用实际网卡
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

## XDP 热路径加速

### 工作原理

XDP 程序挂载在指定网卡，在驱动层（`sk_buff` 分配之前）处理数据包：

```
网卡收包
  └─▶ XDP 程序
        ├─ 解析 Eth/IPv4/TCP 头
        ├─ 匹配目标端口（kvstore port）
        ├─ 解析 RESP GET 命令
        ├─ 查询 hot_cache BPF map
        │     命中 → 构造 RESP bulk-string 回包
        │             修改 Eth/IP/TCP 头（交换 src/dst）
        │             重算 checksum（BPF helper）
        │             bpf_xdp_adjust_tail 调整包长
        │             XDP_TX 直接从网卡发出  ← ~2μs
        └─ 未命中 / 非 GET → XDP_PASS → 内核协议栈 → kvstore 进程
```

### BPF Maps

| Map | 类型 | 说明 |
|-----|------|------|
| `hot_cache` | `BPF_MAP_TYPE_HASH` | key→value 热缓存（最多 65536 条） |
| `xdp_stats` | `BPF_MAP_TYPE_ARRAY` | 命中/未命中/bypass/tx_drop 统计 |
| `resp_scratch` | `BPF_MAP_TYPE_PERCPU_ARRAY` | 每 CPU 构造回包的临时缓冲区 |
| `kvs_port` | `BPF_MAP_TYPE_ARRAY` | 目标端口配置 |

### 使用条件

- 需要 `CAP_NET_ADMIN` 权限（或 root 运行）
- 仅支持标准 IPv4（`ihl=5`），含 IP options 的包自动 fallback
- value 长度上限 **256 字节**，超长 key 自动 fallback
- 本质绕过内核 TCP 状态机（无重传/拥塞控制），适用于 demo / benchmark
- 虚拟机 / loopback 接口自动降级为 SKB（generic XDP）模式

### 缓存管理

kvstore 进程在写命令执行后自动同步 XDP 热缓存：
- `SET` / `HSET` 等写操作 → `xdp_cache_set()` 更新
- `DEL` / `EXPIRE` 等删除/过期操作 → `xdp_cache_del()` 淘汰

---

## 可观测性工具

### kvs_slowlog

eBPF uprobe 挂载在 kvstore 二进制的命令分发函数上，零侵入采集执行超过阈值的命令：

```bash
# 采集执行时间 > 1ms 的命令
sudo ./build/kvs_slowlog ./build/kvstore 1000
```

输出格式：
```
[SLOWLOG] latency=1234us  cmd=SET  args="bigkey longvalue..."
```

---

## 性能数据

> ⚠️ 以下数据来自早期测试阶段，**最新基准数据待更新**。  
> 测试环境：Ubuntu 24.04，`redis-benchmark -t set/get -c 50 -n 300000~1000000`

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

### XDP 热路径延迟

| 路径 | 延迟 |
|------|------|
| 完整内核协议栈（kvstore 进程处理） | ~15 μs |
| XDP 热缓存命中（XDP_TX） | **~2 μs** |

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

# XDP 热路径（需先开启 xdp=yes 并预热缓存）
redis-benchmark -p 2000 -t get -c 1 -n 100000
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
