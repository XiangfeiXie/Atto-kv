# KVStore — 高性能兼容 Redis 协议的键值存储系统

KVStore 是一个用 C 语言从零构建的高性能键值存储系统，完全兼容 RESP 协议。项目深入覆盖了数据结构、网络 I/O、持久化、主从复制、RDMA 和 eBPF 等底层系统编程领域。

## 核心特性

- **统一字典引擎** — 单一 `kv_dict_t` 存储所有数据类型，渐进式 rehash，单次 malloc 条目布局
- **类型系统** — STRING（含整数内联编码）、LIST（packed + quicklist 双编码）
- **三种网络模型** — Reactor (epoll)、Proactor (io_uring)、NtyCo (协程)，可按需切换
- **持久化** — RDB 二进制快照 + AOF 命令日志，io_uring 异步写入，mmap 快速加载
- **主从复制** — RDMA 全量同步（RDB 二进制格式，零拷贝传输）+ eBPF uprobe 增量复制
- **阻塞命令** — BLPOP / BRPOP 支持消息队列语义，超时管理 + 自动唤醒
- **高性能** — 纯内存 34 万 QPS，AOF+RDB 双开仍达 30 万 QPS，Pipeline 深度 160 时突破 109 万 QPS，启动内存仅 6.8 MB

## 性能概览

测试环境：Ubuntu 24.04, redis-benchmark, `-t set -c 50 -P 20 -n 300000~1000000`, 无持久化基线

### Pipeline 批量处理性能

| Pipeline | kvs-SET | Redis-SET | 比值 | kvs-GET | Redis-GET | 比值 |
|----------|---------|-----------|------|---------|-----------|------|
| P=1 | 18,452 | 19,094 | 0.96x | 18,115 | 19,158 | 0.94x |
| P=10 | 165,755 | 201,491 | 0.82x | 162,048 | 193,761 | 0.83x |
| P=20 | **307,031** | 267,094 | 1.14x | 296,648 | 314,663 | 0.94x |
| P=40 | **513,347** | 342,583 | 1.49x | **513,347** | 432,713 | 1.18x |
| P=80 | **777,605** | 396,825 | 1.95x | **640,615** | 495,786 | 1.29x |
| P=160 | **1,092,896** | 454,133 | 2.40x | **829,187** | 536,193 | 1.54x |

> P≥20 后 kvstore 全面反超 Redis，高 Pipeline 下 proactor 异步 I/O 模型优势显著。Redis 在 P=160 时饱和于 ~500K，kvstore 突破 109 万 QPS（2.4x）。

### AOF 持久化性能

| 场景 | QPS | 持久化损耗 |
|------|-----|-----------|
| kvstore AOF 关闭 | 341,064 | — |
| kvstore AOF+RDB 双开 | 303,582 | **20.0%** |
| Redis 无持久化 | 311,915 | — |
| Redis AOF everysec | 188,644 | **40.0%** |

> mmap AOF 写入效率远高于 Redis 的 fsync 方案，损耗仅 20%（Redis 40%）。AOF 开启的 kvstore（304K）仍比无持久化 Redis（312K）接近。

### SAVE 性能影响

| Keys | 基线 QPS | 周期 SAVE QPS | 降幅 | RDB 大小 |
|------|----------|---------------|------|----------|
| 1,000 | 308,008 | 311,203 | 0% | 0 MB |
| 10,000 | 292,969 | 300,601 | 0% | 0.4 MB |
| 100,000 | 300,300 | 299,700 | 0.2% | 4.1 MB |
| 1,000,000 | 297,619 | 309,917 | 0% | 41.0 MB |

> fork + io_uring RDB 写入在子进程中执行，对主进程 SET 性能几乎无影响。

### 主从同步

| 场景 | QPS |
|------|-----|
| 无主从同步（基线） | 289,184~321,337 |
| eBPF 增量复制 | 293,427~295,683 |

> eBPF uprobe 增量复制对 Master QPS 影响极小（0~8%，在测试误差范围内）。

### 内存分配器对比 (500K keys × 64B)

| 分配器 | 空闲 RSS | 峰值 RSS | 稳定 RSS | 峰值增长 | 归还 |
|--------|---------|----------|---------|----------|------|
| jemalloc | 6.8 MB | 72.2 MB | 71.9 MB | 63.9 MB | 0.3 MB |
| glibc malloc | 4.0 MB | 75.3 MB | 75.3 MB | 69.5 MB | 0 MB |

> jemalloc 峰值内存略优（4%），启动时 glibc 更轻。FLUSHALL 后两者均不主动归还内存给 OS，但 jemalloc 可通过 `mallctl("arenas.purge")` 手动触发归还。

## 快速开始

### 依赖安装 (Ubuntu 24.04)

```bash
sudo apt-get update && sudo apt-get install -y \
    build-essential gcc make \
    liburing-dev libfabric-dev libbpf-dev libelf-dev \
    zlib1g-dev libjemalloc-dev \
    rdma-core linux-tools-$(uname -r)
```

### 编译

```bash
cd 9.1-kvstore
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# 如需 glibc malloc 版本（内存对比测试）：
# cmake .. -DCMAKE_BUILD_TYPE=Release -DUSE_JEMALLOC=OFF
```

### 启动

```bash
# 最小化启动（纯内存，无持久化，无复制）
./build/kvstore conf/kvstore.conf

# 连接测试
redis-cli -p 2000
> SET hello world
> GET hello
```

## 架构

```
src/
├── core/          命令分发、协议解析、生命周期、客户端/阻塞子系统
│   ├── kvstore.c/h      命令分发 + main()，懒加载 client_t，写命令后处理
│   ├── protocol.c/h     零拷贝 RESP 解析（kvs_token_t 切片），协议批处理
│   ├── server.h         连接管理、DynBuf 读写缓冲
│   ├── config.c/h       配置文件解析
│   ├── client.h         应用层客户端抽象
│   └── block.c/h        阻塞命令子系统（key→waiter 哈希表，超时扫描）
├── engine/        统一存储引擎
│   └── kv_dict.c/h      统一字典：FNV-1a + Murmur3 哈希，渐进式 rehash，
│                         单次 malloc 条目布局 [header][data][key]
├── type/          数据类型
│   ├── kv_type_string.c/h  STRING：raw + int 内联编码
│   └── kv_type_list.c/h    LIST：packed（≤128 元素）→ quicklist 自动转换
├── net/           网络 I/O 模型
│   ├── reactor.c          epoll 事件驱动
│   ├── proactor.c         io_uring 异步 I/O
│   └── ntyco.c            NtyCo 协程
├── persist/       持久化
│   └── persistence.c/h   RDB/AOF，io_uring 加速写入，mmap 加速加载
├── repl/          主从复制
│   ├── rdma_repl.c/h      RDMA 全量同步（RDB 二进制，纯内存）
│   └── ebpf_incr_repl.c/h eBPF uprobe 增量复制（内核态捕获写入）
└── util/          工具模块
    └── dynbuf.c/h         动态缓冲区
```

## 配置

配置文件采用键值对格式，详见 `conf/` 目录。核心配置项：

```conf
port 2000                        # 监听端口
network proactor                 # reactor | proactor | ntyco
maxclients 10000                 # 最大客户端连接数
persistence none                 # none | rdb | aof
rdma_sync_enabled false          # RDMA 全量同步开关
rdma_role master                 # master | slave
```

## 支持的命令

**全局**: PING, ECHO, SAVE, FLUSHALL

**STRING**: SET, GET, MOD, DEL, EXIST, EXPIRE

**LIST**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, BLPOP, BRPOP

## 主从复制

### 全量同步（RDMA + RDB 二进制）

Master 将整个数据集序列化为 RDB 二进制格式到内存缓冲区，通过 RDMA 零拷贝传输到 Slave，Slave 直接从内存解析加载，全程不落盘、无 RESP 文本解析。

### 增量复制（eBPF uprobe）

Master 上每个写操作触发内核态 eBPF uprobe，捕获命令原文写入 ringbuf，用户态消费者线程通过 RDMA 推送到所有 Slave。故障时自动断开所有 Slave 并强制全量重同步。

### 启动示例

```bash
# Master
sudo ./build/kvstore conf/kvstore-master.conf

# Slave
sudo ./build/kvstore conf/kvstore-slave.conf
```

## 数据类型设计

### STRING

- **KV_ENCODING_RAW** — 原始字节串，payload 直接存储在 entry 中
- **KV_ENCODING_INT** — 内联 int64_t，数字值自动识别并以 8 字节存储，GET 时格式化为字符串

### LIST

- **KV_ENCODING_PACKED** — 内联数组，适用于 ≤128 元素且 ≤4096 字节的小列表
- **KV_ENCODING_QUICKLIST** — 链表打包节点，元素个数或体积超限时自动转换
- 持久化时 quicklist 展平为 packed 格式（避免序列化原始指针）

## 性能优化 (Phase 8)

| # | 优化 | 效果 |
|---|------|------|
| 1 | AOF 重放双写消除 (`g_in_reload`) | 回放时不再重复追加 AOF |
| 2 | KVItem 临时分配消除 | SET: 3 alloc → 1 alloc, GET: 2 alloc → 0 alloc |
| 3 | 整数内联编码 | 数字值以 int64_t 存储，节省内存 |
| 4 | Quicklist 转换 O(n²)→O(n) | 顺序指针遍历替代索引查找 |
| 5 | GET 直接引用 | 返回值指针直接指向 entry 内部，零拷贝 |
| 6 | 字典扩容阈值 2.0→1.0 | 更短的哈希链，更快查找 |
| 7 | DynBuf reserve API | 预分配缓冲区，减少 realloc |
| 8 | 空表快速返回 | 空字典查询跳过 rehash 步骤 |

## 技术栈

- **语言**: C
- **网络**: epoll / io_uring (liburing) / NtyCo 协程
- **RDMA**: libfabric (verbs provider, Soft-RoCE)
- **可观测性**: eBPF (libbpf, uprobe, ringbuf)
- **内存**: jemalloc
- **协议**: RESP (兼容 redis-cli, redis-benchmark, 各语言 Redis SDK)

## 开发日志

详细的开发过程、设计决策和 bug 修复记录见 [dev-log.md](../dev-log.md)。
