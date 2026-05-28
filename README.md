# KVStore — 高性能 Redis 兼容键值存储

一个由 C 语言实现的高性能键值存储系统，兼容 Redis 协议，支持 RDMA 全量同步、eBPF 增量复制、AF_XDP 零拷贝网络、向量相似度搜索。

---

## 架构总览

```
                               ┌─────────────────────┐
                               │   Redis Clients      │
                               │ (redis-cli / SDK)    │
                               └──────────┬──────────┘
                                          │ RESP Protocol
                              ┌───────────▼───────────┐
                              │   Network Layer        │
                              │ ┌───────┬───────┬────┐ │
                              │ │reactor│proactor│ntco│ │
                              │ │(epoll)│iou_ring│(coro│ │
                              │ └───────┴───────┴────┘ │
                              └───────────┬───────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │                           │                           │
    ┌─────────▼─────────┐    ┌───────────▼───────────┐    ┌──────────▼──────────┐
    │  Command Dispatch  │    │   Persistence Engine   │    │   Replication        │
    │  (RESP Parser)     │    │ ┌──────┬──────┬──────┐ │    │ ┌──────────────────┐ │
    │  Type dispatch:    │    │ │ RDB  │ AOF  │ WAL  │ │    │ │ RDMA Full Sync   │ │
    │  STRING / HASH     │    │ └──────┴──────┴──────┘ │    │ │ (libfabric)      │ │
    │  VECTOR            │    └───────────┬───────────┘    │ └──────────────────┘ │
    └─────────┬─────────┘                │                 │ ┌──────────────────┐ │
              │                          │                 │ │ eBPF + KCP + XDP │ │
    ┌─────────▼───────────┐             │                 │ │ Incremental Sync │ │
    │  Storage Engine      │             │                 │ └──────────────────┘ │
    │  32-Shard Dict       │             │                 └──────────────────────┘
    │  Progressive Rehash  │             │
    │  Per-shard RWLock    │             │
    │  TTL / Active Expire │             │
    └──────────────────────┘             │
                                         │
                         ┌───────────────▼──────────────┐
                         │   Block Allocator             │
                         │   (jemalloc or glibc malloc)  │
                         └──────────────────────────────┘
```

---

## 设计亮点

### 1. 分片无锁字典引擎

存储引擎采用 **32-shard 哈希表**，每个 shard 配有独立的 `pthread_rwlock_t`，读写锁粒度从"全表锁"降低到 1/32。

**键值布局**：每个 `kv_entry_t` 是一次 `malloc` 的紧凑布局：
```
┌──────────────────────────┐
│ next (8B)        ◄── 链表指针
│ hash (8B)        ◄── 缓存哈希值（rehash 时无需重算）
│ expire (8B)      ◄── 过期时间戳 (ms)
│ key_len (4B)     ◄── key 长度
│ val_len (4B)     ◄── data[] 总大小（含 type+encoding）
├──────────────────────────┤
│ data[0] : type           │
│ data[1] : encoding       │
│ data[2..val_len-1]       │  ◄── payload
├──────────────────────────┤
│ key[key_len]             │  ◄── key 紧跟在 data 后
└──────────────────────────┘
```

**设计决策**：
- **单次 malloc 包含 header + value + key**，减少随机内存访问和分配器开销
- **渐进式 rehash**：每次写入操作迁移 8 个桶，避免长时间的全局锁持有
- **原地更新同大小 value**：`kv_type_string.c` 检测 `val_len == nd` 时直接 `memcpy`，零 malloc/free 调用
- **负载因子阈值**：扩容 `used > size`（>1.0），缩容 `used < size/10`（<0.1）
- 分片结构 128 字节对齐，避免跨核伪共享

### 2. io_uring Proactor 网络模型

支持三种网络模型：`reactor`（epoll）、`proactor`（io_uring）、`ntyco`（NtyCo 协程）。**Proactor 模式是推荐配置**。

**多 Worker 架构**：
```
                        ┌──────────┐
                        │  Listener │  (accept fd)
                        └─────┬────┘
                              │ SO_REUSEPORT / round-robin
              ┌───────────────┼───────────────┐
        ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
        │  Worker 0  │  │  Worker 1 │  │  Worker N │
        │  io_uring  │  │  io_uring  │  │  io_uring  │
        └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
              │               │               │
         ┌────▼────┐    ┌────▼────┐    ┌────▼────┐
         │ Conn Set │    │ Conn Set │    │ Conn Set │
         └─────────┘    └─────────┘    └─────────┘
```

**关键优化**：
- 每个 Worker 拥有独立的 `io_uring` 实例，避免跨核 SQ 争用（`SINGLE_ISSUER` 模型）
- 连接由首次 accept 的 Worker 持有，生命周期内不迁移
- 跨 Worker 通信（如 BLPOP 解除阻塞）通过 `io_uring_submit_and_wait` 唤醒信号实现
- Pipeline 批量处理：高 Pipeline 深度下 SET 可达 1.2M QPS（redis-benchmark 的 2.68x）

### 3. RDMA 全量同步 + eBPF/KCP 增量复制

复制系统分为两个阶段：

**全量同步（RDMA/libfabric）**：
```
Master                                    Slave
  │  ◄── RDMA 连接请求 ────────────────────│
  │  ──── 内存 RDB 快照 ───────────────────►│  (verbs/tcp provider)
  │  ──── 传输完成 ────────────────────────►│
```

**增量同步（eBPF + KCP + AF_XDP）**：
```
Client ─── SET key val ───► Master
                               │
                          ┌────▼─────┐
                          │ eBPF      │  ← sk_skb socket filter
                          │ 过滤写命令 │     只捕获 SET/DEL/HSET 等
                          └────┬─────┘     返回 SK_PASS（不影响正常请求）
                               │
                          ┌────▼─────┐
                          │  Ringbuf  │  ← 直接 skb → ringbuf 拷贝
                          └────┬─────┘     零中间缓冲区
                               │
                          ┌────▼─────┐
                          │ KCP +     │  ← 可靠 UDP 传输
                          │ AF_XDP    │     ARQ + 快速重传 + 拥塞控制
                          └────┬─────┘     AF_XDP 零拷贝发送
                               │
                     UDP (KCP 协议)
                               │
                          ┌────▼─────┐
                          │ Slave     │
                          │ 应用命令  │
                          └──────────┘
```

**性能数据**：增量同步仅产生 **4.18% QPS 影响**（16,903 → 16,197），优化前为 98%。

**优化历程**（eBPF 过滤器）：
1. 固定 4096 字节分配 → 512 字节动态分配（节省 87% 内存）
2. skb → temp_buf → ringbuf 双重拷贝 → 直接 skb → ringbuf 单次拷贝
3. 全量命令解析 → 首字节 `*` 快速过滤 + 仅解析前 64 字节
4. 512KB ringbuf → 2MB ringbuf（减少丢事件风险）

### 4. 主动过期删除

后台线程每 **100ms** 扫描一次所有 32 个分片：

```
for each shard:
    if trywrlock(&shard->lock) succeeds:
        随机采样 20 个 key
        删除过期的 key
        if 过期率 > 25%:
            递归扫描（最多 4 轮）
    else:
        跳过（不阻塞正常请求）
```

**设计决策**：
- 使用 `pthread_rwlock_trywrlock` 避免阻塞读写请求
- 采样而非全量扫描，每次扫描 O(1) 时间复杂度
- 递归扫描机制确保高过期率场景下快速收敛
- 单次最多删除：32 shards × 20 keys × 4 rounds = **2,560 keys**

**测试数据**（详见 benchmark）：

| 场景 | 线程数 | Key 数量 | 删除数量 | 清理耗时 | 删除率 | 删除速率 |
|------|--------|---------|----------|----------|--------|----------|
| 小规模 | 1 | 10K | 10,000 | 2s | 100% | 5.0K keys/s |
| 中规模 | 1 | 50K | 50,000 | 2s | 100% | 25.0K keys/s |
| 大规模 | 1 | 100K | 100,000 | 2s | 100% | 50.0K keys/s |
| 多线程 | 4 | 100K | 100,000 | 3s | 100% | 33.3K keys/s |

### 5. 延迟直方图

内置 **16 桶对数分布** 延迟直方图（ns 粒度），支持 P50/P95/P99 查询：

| 桶 | 范围 | 桶 | 范围 |
|----|------|----|------|
| B0  | < 1 µs    | B8  | 128–256 µs |
| B1  | 1–2 µs    | B9  | 256–512 µs |
| B2  | 2–4 µs    | B10 | 512–1024 µs |
| B3  | 4–8 µs    | B11 | 1–2 ms |
| B4  | 8–16 µs   | B12 | 2–4 ms |
| B5  | 16–32 µs  | B13 | 4–8 ms |
| B6  | 32–64 µs  | B14 | 8–16 ms |
| B7  | 64–128 µs | B15 | ≥ 16 ms |

**设计决策**：
- 每个 Worker 写入独立的缓存行对齐 slot（8 slots），**消除伪共享**
- 单共享 slot 时 LOCK XADD 缓存行乒乓 ≈ 1 µs/op 开销 → shard 后降至 ~10 ns/op
- TSC 快速时钟（`rdtsc`）：~5 ns/调用，vs `clock_gettime` 在 VM 上实测 73 µs
- 回放路径（AOF/WAL reload）不记录直方图，避免污染业务延迟统计

### 6. 块级 BLPOP/BRPOP

支持阻塞列表弹出操作，基于 notify callback 机制：
- 客户端阻塞在指定 key 上
- 写操作完成后通知网络层触发响应发送
- 跨 Worker 唤醒通过 `proactor_kick_worker` 实现

---

## 支持的命令

### STRING 类型
| 命令 | 状态 |
|------|------|
| `GET`, `SET`, `SETNX`, `SETEX` | ✅ |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | ✅ |
| `APPEND`, `STRLEN` | ✅ |
| `MGET`, `MSET`, `MSETNX` | ✅ |
| `GETSET` | ✅ |
| `GETRANGE`, `SETRANGE` | ✅ |

### HASH 类型
| 命令 | 状态 |
|------|------|
| `HSET`, `HMSET`, `HSETNX` | ✅ |
| `HGET`, `HMGET`, `HGETALL` | ✅ |
| `HDEL`, `HEXISTS` | ✅ |
| `HLEN`, `HKEYS`, `HVALS` | ✅ |
| `HINCRBY`, `HINCRBYFLOAT` | ✅ |
| `HSTRLEN` | ✅ |

### 通用命令
`PING`, `INFO`, `DBSIZE`, `KEYS`, `FLUSHALL`, `FLUSHDB`, `DEL`, `UNLINK`, `EXISTS`, `EXPIRE`, `TTL`, `TYPE`, `RANDOMKEY`

### 阻塞命令
`BLPOP`, `BRPOP`

### 持久化命令
`BGSAVE`, `LASTSAVE`

### 向量命令
`VEC.CREATE`, `VEC.INSERT`, `VEC.DELETE`, `VEC.GET`, `VEC.SEARCH`, `VEC.COUNT`

---

## 内存分配器选择

KVStore 支持 **jemalloc** 和 **glibc malloc** 两种分配器，通过 CMake 选项切换。

**测试结论**（32 项全面测试，详见 [benchmark.md](benchmark.md)）：

| 场景 | 推荐 | 理由 |
|------|------|------|
| **通用场景** | glibc malloc | 峰值更低（-6% 到 -18%），空闲基线更小（5.7 MB vs 8.4 MB） |
| **大 value + 频繁驱逐** | jemalloc | 4096B 场景下回收率 48.6% vs 24.6%，稳定态更低 |
| **小 value 高并发** | glibc malloc | 64B-256B 场景下峰值更低，回收率相当 |

**关键数据**：

| Value | t=4 jemalloc Peek RSS | t=4 glibc Peek RSS | glibc 优势 |
|-------|----------------------|---------------------|-----------|
| 64B   | 61.3 MB              | 57.5 MB             | **-6.2%** |
| 256B  | 109.8 MB             | 105.5 MB            | **-3.9%** |
| 1024B | 168.0 MB             | 145.2 MB            | **-13.6%** |
| 4096B | 291.0 MB             | 240.0 MB            | **-17.5%** |

> glibc 在 31/32 场景下峰值 RSS 更低（胜率 96.9%）。

---

## 编译器与依赖

### 系统要求

- **操作系统**: Linux kernel 5.10+（eBPF CO-RE）
- **CPU**: x86_64（SSE4.2）
- **内存**: 2GB+ RAM

### 依赖安装

```bash
sudo apt-get install -y \
    build-essential cmake clang llvm \
    libbpf-dev libfabric-dev liburing-dev \
    libjemalloc-dev libxdp-dev zlib1g-dev \
    redis-tools linux-headers-$(uname -r)
```

### 编译

```bash
# 标准构建（jemalloc）
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# glibc 构建（不使用 jemalloc）
cmake -S . -B build-glibc -DCMAKE_BUILD_TYPE=Release -DUSE_JEMALLOC=OFF
cmake --build build-glibc -j$(nproc)

# 设置 eBPF capabilities（避免 sudo 运行）
cmake --build build --target set-caps
```

---

## 配置参考

配置文件使用类似 Redis 的 INI 格式。完整配置项见 `conf/kvstore.conf`。

**核心配置项**：

```ini
# 服务
port 2000                  # 监听端口
network proactor           # reactor | proactor | ntyco
maxclients 10000           # 最大连接数
loglevel info              # debug | info | warn | error

# 持久化
persistence aof            # none | rdb | aof

# 线程（仅 proactor 模式）
threads 0                  # 0=自动(nproc-1), N=手动
cpu_pin auto               # auto | off | 1,2,3,4

# RDMA 全量同步
rdma_sync on
rdma_role master           # master | slave
rdma_provider verbs        # tcp | verbs | sockets
rdma_listen_port 2100

# eBPF + KCP 增量复制
xdp_repl on
xdp_repl_ifname eth0
xdp_repl_port 6483
xdp_driver_mode 0          # 0=skb 通用, 1=native 硬件加速
xdp_repl_zero_copy 1       # 0=UDP, 1=AF_XDP 零拷贝
```

### 快速启动

```bash
# 单机开发模式
./build/kvstore conf/kvstore-none.conf

# 空配置（全部默认值）
./build/kvstore /dev/null
```

### 主从复制部署

```bash
# Master (192.168.30.135)
sudo ./build/kvstore conf/kvstore-master-local.conf

# Slave (192.168.30.136)
sudo ./build/kvstore conf/kvstore-slave-local.conf

# 验证复制
redis-cli -h 192.168.30.135 -p 2000 SET foo bar
redis-cli -h 192.168.30.136 -p 2001 GET foo   # → "bar"
```

---

## 项目结构

```
kvstore/
├── src/
│   ├── core/           # 服务入口、协议解析、配置、阻塞管理
│   │   ├── kvstore.c   (主程序，命令分发，~2000 行)
│   │   ├── protocol.c  (RESP 协议解析器)
│   │   ├── config.c    (配置文件解析)
│   │   └── block.c     (BLPOP/BRPOP 阻塞系统)
│   ├── engine/          # 存储引擎核心
│   │   └── kv_dict.c   (32-shard 哈希表，渐进式 rehash)
│   ├── type/            # 数据类型实现
│   │   ├── kv_type_string.c  (STRING 命令)
│   │   └── kv_type_hash.c    (HASH 命令，支持 listpack 编码)
│   ├── net/             # 网络 I/O 层
│   │   ├── reactor.c   (epoll 事件驱动)
│   │   ├── proactor.c  (io_uring 批量提交/收割)
│   │   └── ntyco.c     (NtyCo 协程集成)
│   ├── persist/         # 持久化引擎
│   │   ├── persistence.c  (RDB 快照 + AOF/WAL)
│   │   ├── journal.c     (WAL 预写日志)
│   │   └── wal_persist.c (AOF 格式持久化)
│   ├── repl/            # 复制子系统
│   │   ├── rdma_repl.c      (RDMA/libfabric 全量同步)
│   │   ├── repl_kcp_xdp.c   (KCP + XDP 增量复制)
│   │   ├── af_xdp_socket.c  (AF_XDP 零拷贝 socket)
│   │   ├── repl_backlog.c   (复制积压缓冲区)
│   │   └── *.bpf.c          (eBPF 内核态过滤器)
│   ├── bpf/             # eBPF 管理
│   │   ├── xdp_kvs.kern.c      (XDP 内核 BPF 程序)
│   │   ├── xdp_manager.c       (XDP 用户态管理)
│   │   ├── sockmap_kvs.kern.c  (sockmap 内核 BPF)
│   │   └── sockmap_manager.c   (sockmap 用户态管理)
│   ├── vector/          # 向量计算
│   ├── storage/         # 向量存储引擎
│   ├── index/           # 向量索引（平面暴力搜索）
│   ├── tools/           # 可观测性工具
│   │   ├── kvs_top.c    (实时监控面板)
│   │   └── kvs_slowlog.c (BPF 慢查询日志)
│   └── util/            # 工具类（动态缓冲区）
├── test/
│   ├── bench/           # 性能测试套件
│   │   ├── bench_pipeline.sh     (Pipeline 批量性能)
│   │   ├── bench_wal.sh          (WAL/AOF 影响)
│   │   ├── bench_memory_v3.sh    (内存分配器对比)
│   │   ├── bench_rdma_provider.sh (RDMA 全量同步)
│   │   ├── bench_incremental_impact.sh (增量同步影响)
│   │   ├── bench_save.sh         (BGSAVE 影响)
│   │   ├── bench_expire_v2.sh    (过期键处理)
│   │   ├── common.sh             (公共函数库)
│   │   └── results_*.csv/txt     (测试结果数据)
│   └── smoke/           # 冒烟测试
├── conf/                # 配置文件
│   ├── bench/           (基准测试专用配置)
│   ├── kvstore-master-local.conf
│   └── kvstore-slave-local.conf
├── kvs-client/          # 多语言客户端示例
│   ├── go-kvstore.go, py-kvstore.py, js-kvstore.js
│   ├── rust-kvstore.rs, javakvstore.java
├── NtyCo/               # 内嵌协程库
├── third_party/kcp/     # KCP 可靠 UDP 协议
├── CMakeLists.txt       # CMake 构建（384 行）
└── benchmark.md         # 性能测试报告
```

---

## 性能测试

完整性能报告见 [benchmark.md](benchmark.md)。测试套件位于 `test/bench/`。

### 快速运行

```bash
# Pipeline 性能测试
sudo ./test/bench/bench_pipeline.sh

# 内存分配器对比测试
sudo ./test/bench/bench_memory_v3.sh

# 过期键稳定性测试
./test/bench/bench_expire_v2.sh

# 增量同步性能影响测试
./test/bench/bench_incremental_impact.sh

# WAL/AOF 性能影响测试
sudo ./test/bench/bench_wal.sh
```

### 关键性能数据

**Pipeline 批量处理**（1M req, 50 clients, threads=1）：

| Pipeline | kvs SET | redis SET | kvs/redis |
|----------|---------|-----------|-----------|
| P=1      | 18K     | 19K       | 0.95x     |
| P=20     | 304K    | 270K      | **1.13x** |
| P=40     | 579K    | 324K      | **1.79x** |
| P=80     | 912K    | 405K      | **2.26x** |
| P=160    | 1,229K  | 458K      | **2.68x** |

**增量同步性能影响**（500K req, 50 clients, 1KB value）：

| 场景 | QPS | 平均延迟 |
|------|-----|---------|
| 基线（无 slave） | 17,910 | 1.786 ms |
| 增量同步（有 slave） | 17,095 | 1.626 ms |
| **性能影响** | **-4.00%** | — |

**WAL (AOF) 性能影响**（1M req, P=160）：

| 系统 | AOF off | AOF on | 开销 |
|------|---------|--------|------|
| kvstore | 1,618K | 933K | 42.3% |
| Redis   | 396K   | 206K  | 48.0% |

---

## 故障排查

### eBPF 程序加载失败

```bash
# 检查内核版本 (需要 5.10+)
uname -r
# 安装内核头文件
sudo apt-get install linux-headers-$(uname -r)
# 手动设置 capabilities
echo "1234" | sudo -S setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep build/kvstore
```

### RDMA 连接失败

```bash
# 检查防火墙
sudo iptables -A INPUT -p tcp --dport 2100 -j ACCEPT
# 检查 Master 是否在监听
sudo ss -tlnp | grep 2100
# 验证 provider（Master 和 Slave 必须一致）
grep rdma_provider conf/kvstore-*.conf
```

### AF_XDP 不支持 loopback

`lo` 接口不支持 AF_XDP 零拷贝模式，会自动降级到 UDP。使用物理/虚拟网卡可获得完整 AF_XDP 支持。

---


