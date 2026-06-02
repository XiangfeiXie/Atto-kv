# KVStore — 高性能 Redis 兼容键值存储引擎

KVStore 是一个由 C 语言从零构建的高性能键值存储系统，兼容 Redis RESP 协议。采用 **io_uring proactor** 网络模型、**单次 malloc 统一字典** 存储引擎、**eBPF + TCP 增量复制**与 **libfabric RDMA 全量同步**，在 Pipeline 高并发场景下性能可达 Redis 的 **2.7 倍**。

---

## 架构概览

```
┌────────────────────────────────────────────────────┐
│                    KVStore                          │
├────────────────────────────────────────────────────┤
│  网络层: reactor (epoll) / proactor (io_uring)     │
│           / NtyCo (协程)                            │
├────────────────────────────────────────────────────┤
│  协议层: RESP 解析器 (零拷贝 token)                  │
│          命令分发 (首字节快速路径 + TOKEN_EQ)        │
├────────────────────────────────────────────────────┤
│  类型层: String (RAW/INT)  Hash (PACKED/DICT)      │
├────────────────────────────────────────────────────┤
│  引擎层: 统一字典 (渐进式 rehash, FNV-1a+Murmur3)   │
│          读写锁并发控制 + 惰性过期队列               │
├────────────────────────────────────────────────────┤
│  持久化: WAL (AOF) + RDB, io_uring 异步写, mmap 加载│
│          CRC32C 硬件加速 (SSE4.2)                   │
├────────────────────────────────────────────────────┤
│  复制层: RDMA/libfabric 全量同步                    │
│          eBPF filter + TCP 增量同步 + 16MB backlog  │
│  阻塞层: BLPOP/BRPOP 阻塞命令支持                    │
└────────────────────────────────────────────────────┘
```

---

## 技术亮点

### 1. 三合一网络模型

| 模型 | 底层 | 适用场景 |
|------|------|---------|
| **proactor** | io_uring (SQPOLL + provided buffers) | 高并发、高吞吐（默认推荐） |
| **reactor** | epoll（边沿触发 + 懒事件掩码更新） | 低延迟、简单稳定 |
| **ntyco** | 用户态协程（NtyCo） | 协程式编程模型 |

- Provided buffer ring 实现零拷贝 multishot recv
- SQPOLL 模式让内核轮询提交队列，消除 submit 系统调用
- 连接状态 `conn_info` 打包进 `user_data`（`fd:32|event:8|gen:24`），避免额外查找

### 2. 统一字典存储引擎

- **单次 malloc**：每个 entry 一次分配 (header + type/encoding + payload + key)，避免指针追逐
- **渐进式 rehash**：双哈希表增量迁移，迭代期间暂停 rehash 保证安全
- **FNV-1a + Murmur3 finalizer**：8 字节批量 FNV-1a 后接 Murmur3 64-bit 混淆，分布均匀
- **智能编码**：
  - String：小整数自动 INT 编码，避免字符串分配
  - Hash：小哈希 PACKED 编码（field/value 内联），大数据量自动升级 DICT 编码
- **惰性过期队列**：读操作发现过期 key 不入队等待，写操作 piggyback 批量删除，消除锁竞争
- **自适应过期采样**：后台线程根据过期率动态调整采样量（20-200），过期率 < 25% 自动停止

### 3. io_uring 异步持久化

- **WAL (AOF) 异步写**：主线程零阻塞入队 → 后台线程 io_uring 批量落盘
- **RDB BGSAVE**：`fork() + COW` + io_uring 批量写，主线程无感
- **mmap 加载**：AOF/RDB 恢复使用 mmap 直接映射文件，避免 `read()` 系统调用和用户态拷贝
- **CRC32C 硬件加速**：使用 SSE4.2 `_mm_crc32_u64` 指令，比 zlib `crc32()` 快 10 倍
- 支持 `always` / `everysec` / `no` 三种 fsync 策略

### 4. 主从复制：两阶段架构

#### 阶段一：RDMA/libfabric 全量同步

```
Master: fork() → 子进程序列化 → 父进程 fi_send (1MB 分块流式传输)
Slave:  fi_cq_sread → 流式反序列化 → 加载
```

- 使用 libfabric（支持 tcp/verbs/sockets provider）
- 流式传输，无需预先生成 RDB 文件，内存友好
- 无 RDMA 硬件时 tcp provider 可达 30-50 MB/s

#### 阶段二：eBPF + TCP 增量同步

```
Client Write → eBPF filter 捕获 → ringbuf → 16MB Backlog → TCP → Slave
```

- **eBPF 智能过滤**：内核态 sockops + sk_skb 程序只捕获写命令（SET/DEL/HSET/LPUSH 等），**减少 60-80% 数据量**
- **TCP 可靠传输**：复用内核 TCP 栈的重传、流控、拥塞控制
- **16MB 复制积压缓冲区**（ring buffer）：支持断线重连增量恢复（partial resync）
- **自动重连**：断开后 3 秒自动重连

### 5. 高性能协议解析

- **零拷贝 RESP token**：`kvs_token_t` 直接指向读缓冲区，不分配内存
- **首字节快速路径**：SET/GET/HSET/HGET 第一时间字符匹配跳过 25+ 次 `TOKEN_EQ`
- **长度前缀编译期比较**：`TOKEN_EQ(t, "literal")` 展开为 `sizeof()-1 + memcmp`，常数时间
- **大小写不敏感**：命令名一次 toupper 后全链路复用，兼容 Redis 语义
- **批量 drain**：一次 `io_uring` 收割后连续解析多个 RESP 命令，减少函数调用开销

### 6. 观测性与可运维性

- **INFO 命令**：兼容 Redis INFO，输出 server/clients/memory/persistence/stats/replication/cpu/keyspace 全部段
- **延迟直方图**（Phase 10.6）：16 个 log2 桶（<1µs → ≥16ms），自动计算 p50/p99/p999
  - 8 slots × cacheline 对齐，消除多核伪共享
  - 1/64 采样率，成本 ≤ 2% CPU
- **TSC 快速时钟**（Phase 10.7）：`rdtsc` 耗时 ~5ns vs `clock_gettime` 的 ~73µs（容器/VM 回退到 syscall 时），中位数校准自动回退
- **CONFIG GET/SET**：运行时热修改 loglevel 和 maxclients，无需重启
- **jemalloc 集成**：`INFO memory` 直接读取 `stats.allocated`，精确内存统计

### 7. 命令兼容性

已实现 40+ Redis 命令：

| 类别 | 命令 |
|------|------|
| **String** | SET, GET, SETNX, SETEX, GETSET, APPEND, INCR, INCRBY, DECR, DECRBY, MOD, STRLEN, MGET, MSET, MSETNX |
| **Hash** | HSET, HMSET, HSETNX, HGET, HMGET, HGETALL, HKEYS, HVALS, HDEL, HLEN, HEXISTS, HSTRLEN, HINCRBY, HINCRBYFLOAT, HRANDFIELD |
| **Key** | DEL, UNLINK, EXISTS, EXPIRE, TTL, PTTL, PERSIST, TYPE, KEYS, DBSIZE, FLUSHALL, FLUSHDB |
| **Connection** | PING, ECHO, QUIT, HELLO, CLIENT (SETNAME/GETNAME/ID), COMMAND, SELECT |
| **Server** | INFO, CONFIG (GET/SET/RESETSTAT), SAVE, BGSAVE, BGREWRITEAOF |
| **Blocking** | BLPOP, BRPOP |

### 8. 多语言客户端 SDK

`kvs-client/` 目录提供 5 种语言的示例客户端：

| 语言 | 文件 |
|------|------|
| Python | `py-kvstore.py` |
| Go | `go-kvstore.go` |
| Java | `javakvstore.java` |
| JavaScript | `js-kvstore.js` |
| Rust | `rust-kvstore.rs` |

---

## 性能基准

> 测试环境：Ubuntu 24.04, Kernel 6.8.0, VMware 虚拟机, 单线程, persistence=none

### Pipeline 批量处理（vs Redis）

| Pipeline | KVStore SET | Redis SET | 倍率 |
|----------|------------|-----------|------|
| P=1  | 18,042 | 19,015 | 0.95x |
| P=10 | 168,606 | 189,358 | 0.89x |
| P=20 | 303,951 | 269,614 | **1.13x** |
| P=40 | 578,704 | 323,520 | **1.79x** |
| P=80 | 912,409 | 404,531 | **2.26x** |
| P=160 | 1,228,501 | 457,666 | **2.68x** |

- 低 Pipeline 深度与 Redis 持平，P≥20 开始反超
- 随机 Key 场景下优势出现更早（P=10 已达 1.48x）
- AOF 开启时 P=160 仍达 933K QPS，是 Redis (206K) 的 **4.5 倍**

### 增量同步开销

| 场景 | QPS | 影响 |
|------|-----|------|
| baseline（无 Slave） | 17,910 | - |
| with Slave（增量同步） | 17,095 | **-4.0%** |

### 内存分配器深度对比

| 场景 | jemalloc | glibc |
|------|----------|-------|
| 碎片控制 | 4.9x 内存开销 | 16.0x |
| 内存回收率 | **79.6%** | 1.9% |
| 长期膨胀 | 78% | 35% |

**推荐生产环境 `jemalloc + 调优`**：碎片控制能力为 glibc 的 3.3 倍，回收速度为 40 倍。

详细数据见 [benchmark.md](benchmark.md)。

---

## 快速开始

### 依赖安装

```bash
sudo apt-get install -y build-essential cmake pkg-config \
    liburing-dev libjemalloc-dev libfabric-dev zlib1g-dev \
    libbpf-dev clang llvm redis-tools
```

### 编译

```bash
cd 9.1-kvstore
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

可选构建类型：
- `Release`：优化全开（默认）
- `RelWithDebInfo`：优化 + 调试符号
- `Debug`：断言 + 无优化

若无 RDMA 需求可关闭 jemalloc：
```bash
cmake -DUSE_JEMALLOC=OFF ..
```

### 启动服务

```bash
# 单机运行
./build/kvstore conf/kvstore.conf

# 使用 redis-cli 测试
redis-cli -p 2000 SET key1 "hello"
redis-cli -p 2000 GET key1
# → "hello"
```

### 主从复制部署

**主机**（`conf/kvstore-master.conf`）：
```bash
sudo ./build/kvstore conf/kvstore-master.conf
```

**从机**（`conf/kvstore-slave.conf`）：
```bash
sudo ./build/kvstore conf/kvstore-slave.conf
```

验证同步：
```bash
redis-cli -p 2000 SET foo "bar"   # 写入主机
redis-cli -p 2001 GET foo          # 从从机读取
# → "bar"
```

### 本地测试（单机主从）

```bash
# 终端 1: 主机
sudo ./build/kvstore conf/kvstore-master-local.conf

# 终端 2: 从机
sudo ./build/kvstore conf/kvstore-slave-local.conf

# 终端 3: 测试
redis-cli -p 2000 SET k v && redis-cli -p 2001 GET k
```

---

## eBPF 权限配置

eBPF 程序需要内核权限。推荐使用 capabilities 而非 root：

```bash
sudo setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep build/kvstore
```

或通过构建目标自动配置：
```bash
cmake --build build --target set-caps
```

eBPF 过滤器加载失败时自动降级为 fallback 模式（无过滤，全量 TCP 复制），不影响基本功能。

---

## 运行测试

```bash
# Pipeline 批量性能
sudo ./test/bench/bench_pipeline.sh

# 内存分配器深度对比
sudo ./test/bench/bench_memory_comparison.sh

# WAL/AOF 性能影响
sudo ./test/bench/bench_wal.sh

# BGSAVE 性能影响
./test/bench/bench_save.sh

# 增量同步性能影响
./test/bench/bench_incremental_impact.sh

# RDMA 全量同步
sudo ./test/bench/bench_rdma_provider.sh

# 过期键处理
./test/bench/bench_expire_v2.sh

# Smoke 测试 (Release + ASAN + TSAN)
cmake --build build --target smoke-all
```

---

## 项目结构

```
9.1-kvstore/
├── src/
│   ├── core/          # 主程序、协议解析、命令分发、阻塞子系统、配置
│   ├── engine/        # 统一字典存储引擎（渐进式 rehash）
│   ├── type/          # 数据类型：String、Hash
│   ├── net/           # 网络模型：reactor (epoll)、proactor (io_uring)、ntyco
│   ├── persist/       # WAL + RDB 持久化、Journal 日志、CRC32C
│   ├── repl/          # 复制：RDMA 全量、eBPF+TCP 增量、backlog
│   └── util/          # 动态缓冲区
├── conf/              # 配置文件
├── test/              # 测试套件和 Benchmark 脚本
├── kvs-client/        # 多语言客户端 SDK
├── NtyCo/             # 第三方协程库
└── build/             # 构建产物
```

---

## 依赖项

| 库 | 用途 |
|----|------|
| **liburing** | io_uring proactor 网络模型 |
| **libfabric** | RDMA 全量同步（tcp/verbs provider） |
| **libbpf / clang** | eBPF 增量复制过滤器 |
| **jemalloc** (可选) | 内存分配器（碎片控制/精确统计） |
| **zlib** | RDB 压缩 |
| **NtyCo** (内嵌) | 协程网络模型 |

---

## 技术参考

本项目的设计受到以下系统启发：
- **Redis**：RESP 协议、命令语义、主从复制架构
- **Valkey**：现代化 fork 方向
- **Dragonfly**：io_uring + 多线程设计思路
- **ScyllaDB/Seastar**：shared-nothing 线程模型
