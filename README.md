# KVStore

KVStore 是一个使用 **C11 从零实现的 Redis-like 高性能内存数据库**。项目重点展示网络 I/O、存储引擎、持久化、主从复制、RDMA/libfabric、BPF/XDP 可观测以及系统性能优化能力。

这个项目不是简单的 Redis 命令练习，而是围绕“高吞吐、低侵入观测、可靠持久化、可恢复复制”构建的一套完整系统工程实践。

---

## 项目亮点

| 方向 | 实现内容 |
|---|---|
| **Redis 协议兼容** | 实现 RESP 解析与回复，支持常用 Redis 命令和 STRING / LIST / HASH / SET / ZSET 五类核心数据结构 |
| **高性能网络模型** | 支持 epoll reactor、io_uring proactor、NtyCo coroutine；proactor 使用 multishot accept/recv、provided buffer ring、`DEFER_TASKRUN` 等优化 |
| **分片存储引擎** | 32-shard hash table，每个 shard 独立 `pthread_rwlock`，支持渐进式 rehash、TTL、lazy expire、多 key 命令有序加锁 |
| **RDB / AOF / WAL** | RDB fork-COW 快照；AOF 与增量复制统一 WAL；consumer 线程批量 `writev`；启动时支持 mmap replay |
| **主从复制** | 基于 libfabric / Soft-RoCE 的 RDMA fullsync；fullsync 后通过 TCP incremental replication + backlog catchup 追增量 |
| **lock-free GET fastpath** | 1024 buckets × 512B 的 seqlock direct-mapped cache，热 key 小 value GET 可跳过 shard 锁和 dict lookup |
| **BPF/XDP 可观测** | XDP packet-level passive stats、sockmap stream stats、uprobe slowlog；默认关闭，需通过 `kvs_top` 独立 attach，避免污染主 benchmark |
| **系统化验证** | smoke、ASAN、TSAN、fastpath stress、AOF replay、RDMA fullsync/catchup、redis-benchmark 性能回归 |

---

## 适合展示的能力点

- **网络系统编程**
  - `epoll`
  - `io_uring`
  - `SO_REUSEPORT`
  - 多 worker event loop
  - eventfd cross-worker wakeup

- **存储系统设计**
  - sharded hash table
  - 多数据类型编码
  - TTL 与 lazy expire
  - RDB snapshot
  - AOF/WAL recovery

- **复制与一致性**
  - RDMA fullsync
  - fork-COW snapshot
  - incremental WAL stream
  - replid / seq / backlog catchup
  - fullsync 与增量切换

- **性能优化**
  - io_uring 进阶特性
  - SET/GET 热路径拆解
  - lock-free seqlock fastpath
  - AOF/RDMA 统一 WAL 去重
  - RDMA-only 避免磁盘写放大

- **Linux kernel / eBPF**
  - XDP
  - sockmap
  - uprobe
  - pinned BPF maps
  - 低侵入 observability

---

## 架构概览

```text
client / redis-benchmark / redis-cli
        |
        v
+---------------------------+
| network layer             |
| epoll / io_uring / NtyCo  |
| multi-worker + SO_REUSEPORT
+-------------+-------------+
              |
              v
+---------------------------+
| RESP parser + dispatcher  |
| CMD4 dispatch             |
| command type classification
+-------------+-------------+
              |
              +----------------------+
              | STRING GET fastpath  |
              | lock-free seqlock    |
              +----------------------+
              |
              v
+---------------------------+
| sharded KV engine         |
| 32 shards + rwlock        |
| string/list/hash/set/zset |
| TTL / lazy expire         |
+-------------+-------------+
              |
        write command post-hook
              |
              v
+---------------------------+       +------------------------------+
| unified WAL append        | ----> | WAL consumer thread          |
| AOF + incremental repl    |       | batch writev / backlog       |
+-------------+-------------+       | io_uring dispatch to slaves  |
              |                     +------------------------------+
              |
              v
+---------------------------+
| persistence / replication |
| RDB fork-COW snapshot     |
| AOF mmap replay           |
| RDMA fullsync + catchup   |
+---------------------------+

observability:
  XDP packet stats / sockmap stream stats / uprobe slowlog
```

---

## 核心模块

| 模块 | 文件/目录 | 说明 |
|---|---|---|
| 协议与调度 | `src/core/` | RESP 解析、命令分发、配置、INFO、网络入口 |
| 存储引擎 | `src/engine/` | sharded dict、entry 管理、snapshot iterator |
| 数据类型 | `src/type/` | string/list/hash/set/zset 类型实现 |
| 持久化 | `src/persist/` | RDB、AOF/WAL replay、路径管理 |
| 复制 | `src/repl/` | WAL incremental replication、RDMA/libfabric fullsync |
| fastpath | `src/core/fastpath_cache.*` | seqlock 短键热缓存 |
| BPF/XDP | `src/bpf/`, `src/xdp/` | XDP、sockmap、uprobe 观测程序 |
| 测试 | `test/` | smoke、benchmark、ASAN/TSAN、fastpath tests |
| 配置 | `conf/` | 默认配置、bench 配置、主从配置 |

---

## 已实现功能

### Redis 协议与命令

- **协议**
  - RESP array / bulk string 解析
  - redis-cli / redis-benchmark 可直接访问

- **通用命令**
  - `PING`
  - `COMMAND`
  - `HELLO`
  - `SELECT`
  - `INFO`
  - `CONFIG`
  - `CLIENT`
  - `DBSIZE`
  - `FLUSHALL`
  - `SAVE`
  - `BGSAVE`

- **数据类型**
  - STRING: `GET`, `SET`, `SETNX`, `SETEX`, `INCR`, `DECR`, `APPEND`, `MGET`, `MSET`
  - LIST: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `BLPOP`, `BRPOP`
  - HASH: `HSET`, `HGET`, `HDEL`, `HGETALL`, `HLEN`
  - SET: `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SMOVE`
  - ZSET: `ZADD`, `ZRANGE`, `ZREM`, `ZCARD`

> 说明：项目目标是 Redis-like 核心能力实现，不宣称完整替代 Redis。Cluster、Lua、ACL、完整 SCAN 族和 Redis 全命令兼容仍属于后续扩展方向。

---

## 性能摘要

更完整的数据见 [benchmark.md](./benchmark.md)。

| 场景 | 结果摘要 |
|---|---|
| io_uring 进阶优化 | Phase 3 后多个 pipeline 场景平均提升约 **30%** |
| 本机 loopback + pipeline | `threads=2` 时 SET/GET 约 **263k / 306k rps**；pipeline=40/80 时 kvstore SET/GET 分别约 **1.7× / 1.9×** Redis |
| 单核效率优化 | 去除 double dict lookup 后单 worker SET/GET 提升约 **20% / 30%** |
| AOF 对比 | kvstore AOF 损耗约 **21%**，Redis AOF everysec 损耗约 **25%**（100 万 SET/GET，P=20） |
| fastpath microbench | 单线程 `fp_get` 约 **1.9x**；4 readers 同 shard 场景约 **2.34x** |
| AOF + RDMA WAL 合并 | `AOF+RDMA` 只写一份 WAL；100k SET WAL 大小与 AOF-only 一致，约 **12.7 MB** |
| RDMA-only 优化 | `persistence none + rdma_sync on` 不再写磁盘 WAL，`wal_bytes=0` |
| RDMA fullsync/catchup | Soft-RoCE verbs 下 fullsync 成功，catchup 后 master/slave DBSIZE 一致 |

---

## 快速开始

### 依赖

常用依赖：

- `gcc`
- `cmake`
- `make`
- `pkg-config`
- `liburing`
- `jemalloc`
- `redis-tools`

可选依赖：

- `libfabric`
- `libbpf`
- `clang`
- `llvm`
- `bpftool`

### 编译

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

### 启动

```bash
./build/kvstore conf/kvstore.conf
```

### 使用 redis-cli

```bash
redis-cli -p 2000 PING
redis-cli -p 2000 SET hello world
redis-cli -p 2000 GET hello
```

### 使用 redis-benchmark

```bash
redis-benchmark -h 127.0.0.1 -p 2000 -t set,get -n 100000 -c 50 -P 16
```

---

## 常用配置

配置文件示例见 `conf/kvstore.conf` 和 `conf/bench/`。

| 配置项 | 说明 |
|---|---|
| `port` | 服务端口 |
| `network` | 网络模型：`reactor` / `proactor` / `ntyco` |
| `threads` | worker 数量 |
| `cpu_pin` | CPU 亲和性 |
| `persistence` | `none` / `rdb` / `aof` |
| `rdma_sync` | 是否启用 RDMA fullsync / 增量复制 |
| `rdma_role` | `master` / `slave` |
| `rdma_provider` | libfabric provider，如 `verbs` / `tcp` |
| `fastpath_cache` | 是否启用 lock-free GET fastpath |
| `latency_histogram` | 是否启用延迟直方图采样（默认关闭） |

> **观察层默认不随配置启动**：XDP / sockmap 需要通过 `kvs_top` 的 `--attach-*` 参数按需挂载；`conf/bench/` 仍保留演示配置。

---

## 主从复制

KVStore 的复制由两部分组成：

- **fullsync**
  - master fork 子进程获取 COW snapshot
  - child 使用 no-lock iterator 序列化数据
  - 通过 libfabric / RDMA 发送给 slave
  - slave streaming load

- **incremental replication**
  - 写命令进入统一 WAL journal
  - consumer 线程维护 backlog
  - slave fullsync 后通过 TCP incremental channel catchup

最近优化：

- AOF 与 RDMA 增量复制统一为单次 `wal_repl_append()`
- `AOF+RDMA` 不再重复 WAL append
- `RDMA-only` 不再写磁盘 WAL
- fullsync 期间即使 slave 尚未 active，也保留 backlog，保证 catchup 不丢写

---

## BPF/XDP 可观测

项目包含三层低侵入观测能力：

| 层级 | 技术 | 用途 |
|---|---|---|
| L2/L3 | XDP | packet-level stats、passive hit/miss/bypass |
| L4/L7 | sockmap / sk_skb | stream-level command / connection stats |
| L7 | uprobe | slowlog、函数级延迟诊断 |

设计原则：

- BPF/XDP 不是零成本，而是低侵入、可控开销。
- 高频观测默认关闭，避免影响主性能 benchmark。
- 需要排障时按需开启，并通过采样、per-CPU map、阈值过滤控制成本。

### 独立观测工具 `kvs_top`

`kvs_top` 现在既是 dashboard，也负责 on-demand attach/detach：

```bash
# 以 loopback 接口加载 XDP，并把 sockmap 挂到 kvstore 进程所在 cgroup
sudo ./build/kvs-top \
     --attach-xdp --xdp-iface lo \
     --attach-sockmap --sockmap-pid $(pidof kvstore)

# 仅挂载后立即退出（可用于脚本化部署 / 清理）
sudo ./build/kvs-top --attach-xdp --xdp-iface lo \
                     --attach-sockmap --sockmap-pid $(pidof kvstore) \
                     --detach --once
```

- XDP/sockmap 的 BPF map 会被 pin 到 `/sys/fs/bpf/kvstore/`，dashboard 读取同一路径。
- 退出或 `Ctrl-C` 时会自动 detach，确保服务进程回到纯净性能口径。
- 在单机 loopback 环境中挂载观测对 redis-benchmark 会造成约 10–15% 吞吐回退（额外包路径与 map 操作开销），正常性能评测请保持默认关闭。

---

## 测试与验证

```bash
# 构建
cmake --build build -j"$(nproc)"

# 功能回归
bash test/run_regression.sh quick

# 单独运行已启动服务上的数据类型 smoke
bash test/smoke/run_all.sh

# fastpath 单元与并发压测
cmake --build build --target fp_cache_test
./build/fp_cache_test

# fastpath CPU microbenchmark
cmake --build build --target bench_fp_inproc
./build/bench_fp_inproc
```

已验证内容包括：

- string/list/hash/set/zset smoke 全通过
- fastpath torn-read stress 通过
- AOF WAL replay 通过
- RDMA fullsync / catchup 通过
- ASAN / TSAN focused 测试通过

---

## Benchmark

详细性能数据、测试环境、复现命令和测试脚本说明见：

- [benchmark.md](./benchmark.md)
- [test/test.md](./test/test.md)

---

## 项目边界与后续方向

### 已完成

- Redis-like 核心协议与常用命令
- 多线程 io_uring 网络模型
- 五类核心数据结构
- RDB / AOF / WAL
- RDMA fullsync + TCP incremental replication
- lock-free GET fastpath
- BPF/XDP 可观测
- 系统化 benchmark 与 correctness 验证

### 待完善

- 更完整 Redis command coverage
- `SCAN` / `AUTH` / ACL / Lua / Cluster
- `BGREWRITEAOF`
- 多 slave 长时间稳定性压测
- 2-VM 或物理机隔离 benchmark
- 更完整的故障注入与恢复测试

---

## 面试讲解建议

推荐重点讲这 5 个故事：

- **io_uring proactor 优化**
  - 从普通收发到 multishot accept/recv、buf ring、`DEFER_TASKRUN`，解释为什么低 pipeline 收益明显。

- **fastpath cache**
  - 为什么 direct-mapped seqlock cache 能绕过 shard lock，为什么只适合短 key 小 value 热点读。

- **统一 WAL**
  - AOF 和增量复制如何共享一条 WAL 路径，如何避免重复 append 和 RDMA-only 磁盘写放大。

- **RDMA fullsync + catchup**
  - fork-COW snapshot、no-lock iterator、libfabric 传输、fullsync 后 backlog catchup。

- **BPF/XDP 观测取舍**
  - BPF 低侵入但不是零成本，因此默认关闭高频观测，按需开启诊断。

---

## License

本项目用于系统编程、数据库内核与性能优化学习展示。
