# KVStore

一个高性能的 Redis 兼容键值存储系统，支持 RDMA 全量同步和 eBPF 增量复制。

---

## 项目亮点

**核心特性：RDMA + eBPF 主从复制**

- **RDMA 全量同步**：基于 libfabric 的超高速快照传输（verbs provider 可达 100+ MB/s）
- **eBPF 增量复制**：智能内核级写命令过滤，仅 **4.18%** 性能开销（优化前为 98%）
- **零拷贝网络**：AF_XDP 支持，最小化 CPU 使用
- **高性能网络模型**：支持 epoll reactor / io_uring proactor / NtyCo coroutine
- **分片存储引擎**：32-shard 哈希表，每个 shard 独立 rwlock，支持渐进式 rehash
- **向量存储**：支持向量插入、检索和相似度搜索

---

## 性能数据

### eBPF 增量复制性能影响

| 场景 | QPS | 平均延迟 | 性能影响 |
|------|-----|----------|---------|
| **基线（无复制）** | 16,903 | 1.771 ms | - |
| **eBPF 增量复制** | 16,197 | 1.790 ms | **4.18%** ✅ |

### 优化历程

**初始实现**：98% QPS 下降（16,903 → ~200 QPS）

**问题根源**：
- 固定 4088 字节分配（即使 50 字节命令也分配 4KB）
- 双重拷贝开销（skb → temp_buf → ringbuf）
- 对每个数据包进行完整命令解析

**优化方案**：
1. 事件大小缩减至 512 字节（节省 87% 内存）
2. 直接 skb → ringbuf 拷贝（消除双重拷贝）
3. 快速首字节过滤（'*' 检查 RESP 协议）
4. 最小化解析（仅解析前 64 字节用于命令检测）
5. 增加 ringbuf 容量（512KB → 2MB）

**最终结果**：4.18% 性能影响（生产环境可接受）

### RDMA 全量同步性能

| Provider | 传输速度 | 适用场景 |
|----------|---------|---------|
| **tcp** | 40-50 MB/s | 标准以太网 |
| **verbs** | 100+ MB/s | InfiniBand/RoCE |

---

## 架构设计

### 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         Master 节点                              │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Redis 协议服务 (Port 6379)                              │   │
│  │  支持命令: GET/SET/DEL/HSET/HGET/KEYS/INFO 等           │   │
│  └──────────────────────────────────────────────────────────┘   │
│           │                    │                                 │
│           │                    │ (仅写命令)                      │
│           ▼                    ▼                                 │
│  ┌──────────────────┐  ┌──────────────────────────────────┐    │
│  │  存储引擎        │  │  eBPF 过滤器 (sk_skb/sockmap)    │    │
│  │  32-shard dict   │  │  • 捕获写命令 (SET/DEL/HSET...)  │    │
│  │  STRING/HASH     │  │  • 过滤读命令 (GET/HGET...)      │    │
│  │  渐进式 rehash   │  │  • 拷贝到 ringbuf                │    │
│  └──────────────────┘  └──────────────────────────────────┘    │
│                                   │                              │
│                                   ▼                              │
│                        ┌──────────────────────────────────┐    │
│                        │  KCP + XDP/AF_XDP 发送器         │    │
│                        │  • 可靠 UDP 传输                 │    │
│                        │  • 支持零拷贝                    │    │
│                        └──────────────────────────────────┘    │
└─────────────────────────────────────┬───────────────────────────┘
                                      │
                                      │ UDP (KCP 协议)
                                      │
┌─────────────────────────────────────▼───────────────────────────┐
│                         Slave 节点                               │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  KCP + XDP/AF_XDP 接收器                                 │   │
│  │  • 接收写命令                                            │   │
│  │  • 应用到本地存储                                        │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                   │                              │
│                                   ▼                              │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  存储引擎（副本）                                        │   │
│  │  32-shard dict                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                   │                              │
│                                   ▼                              │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Redis 协议服务 (Port 6380, 只读)                       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 复制流程

#### 1. 全量同步（RDMA）

```
Master                                    Slave
  │                                         │
  │◄────── RDMA 连接请求 ────────────────────│
  │                                         │
  │──────── 生成 RDB 快照 ──────────────────►│
  │         (libfabric verbs/tcp)           │
  │                                         │
  │──────── 传输完成 ───────────────────────►│
  │                                         │
  │         加载 RDB 到内存                  │
  │                                         │
```

#### 2. 增量同步（eBPF + KCP）

```
Client                Master (eBPF)              Slave
  │                      │                         │
  │─── SET key val ─────►│                         │
  │                      │                         │
  │                   ┌──▼──┐                      │
  │                   │ sk_skb                     │
  │                   │ filter │                   │
  │                   └──┬──┘                      │
  │                      │                         │
  │                      │ (检测到写命令)          │
  │                      │                         │
  │                   ┌──▼──────┐                  │
  │                   │ ringbuf │                  │
  │                   └──┬──────┘                  │
  │                      │                         │
  │                   ┌──▼──────┐                  │
  │                   │   KCP   │                  │
  │                   │ 发送器  │                  │
  │                   └──┬──────┘                  │
  │                      │                         │
  │                      │─── UDP (KCP) ──────────►│
  │                      │                         │
  │                      │                      ┌──▼──────┐
  │                      │                      │   KCP   │
  │                      │                      │ 接收器  │
  │                      │                      └──┬──────┘
  │                      │                         │
  │                      │                      ┌──▼──────┐
  │                      │                      │ 应用    │
  │                      │                      │ 命令    │
  │                      │                      └─────────┘
  │◄──── +OK ────────────│                         │
  │                      │                         │
```

---

## 支持的命令

### 通用命令
- `PING` - 测试连接
- `INFO` - 服务器信息
- `DBSIZE` - 键数量
- `KEYS` - 列出键
- `FLUSHALL` / `FLUSHDB` - 清空数据库
- `DEL` / `UNLINK` - 删除键

### STRING 类型
- `GET` / `SET` / `SETNX` / `SETEX`
- `INCR` / `DECR` / `INCRBY` / `DECRBY`
- `APPEND` / `STRLEN`
- `MGET` / `MSET`

### HASH 类型
- `HSET` / `HMSET` / `HSETNX`
- `HGET` / `HMGET` / `HGETALL`
- `HDEL` / `HEXISTS`
- `HLEN` / `HKEYS` / `HVALS`

### 向量命令
- `VEC.CREATE` / `VECTOR.CREATE` - 创建向量集合
- `VEC.INSERT` / `VECTOR.INSERT` - 插入向量
- `VEC.DELETE` / `VECTOR.DELETE` - 删除向量
- `VEC.GET` / `VECTOR.GET` - 获取向量
- `VEC.SEARCH` / `VECTOR.SEARCH` - 相似度搜索
- `VEC.COUNT` / `VECTOR.COUNT` - 向量数量

---

## 编译与依赖

### 系统要求

- **操作系统**: Linux kernel 5.10+ (eBPF CO-RE 支持)
- **CPU**: x86_64
- **内存**: 4GB+ RAM
- **网络**: 以太网（tcp provider）或 InfiniBand/RoCE（verbs provider）

### 依赖安装

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    clang \
    llvm \
    libbpf-dev \
    libfabric-dev \
    liburing-dev \
    libjemalloc-dev \
    redis-tools \
    linux-headers-$(uname -r)

# CentOS/RHEL
sudo yum install -y \
    gcc \
    cmake \
    clang \
    llvm \
    libbpf-devel \
    libfabric-devel \
    liburing-devel \
    jemalloc-devel \
    kernel-devel
```

### 编译

```bash
# 克隆仓库
git clone https://github.com/yourusername/kvstore.git
cd kvstore

# 编译
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

---

## 双虚拟机部署指南

本指南假设你有两台虚拟机：
- **Master VM**: 192.168.30.135
- **Slave VM**: 192.168.30.136

### 步骤 1: 网络配置

在两台虚拟机上，查看网卡接口名：

```bash
ip link show
```

常见接口名：
- `eth0`, `eth1` (传统命名)
- `ens33`, `ens160` (VMware)
- `enp0s3`, `enp0s8` (VirtualBox)

### 步骤 2: 配置 Master

编辑 `conf/kvstore-master.conf`：

```ini
# ── 服务配置 ──────────────────────────────────────────────────────
port 6379                # Redis 协议服务端口
network proactor         # 使用 proactor 网络模型
maxclients 10000         # 最大客户端连接数
loglevel info            # 日志级别

# ── 持久化配置 ────────────────────────────────────────────────────
persistence none         # 纯内存模式（不启用持久化）

# ── 线程配置 ──────────────────────────────────────────────────────
threads 1                # 工作线程数
cpu_pin auto             # CPU 亲和性：auto 自动绑定

# ── RDMA 全量同步配置 ─────────────────────────────────────────────
rdma_sync on             # 启用 RDMA 同步
rdma_role master         # 角色：master
rdma_provider verbs      # tcp（以太网）或 verbs（InfiniBand/RoCE）
rdma_listen_ip 0.0.0.0   # 监听地址：0.0.0.0 表示所有网卡
rdma_listen_port 2100    # RDMA 监听端口

# ── 增量同步配置（eBPF + KCP + XDP/AF_XDP）────────────────────────
xdp_repl on              # 启用增量同步
xdp_driver_mode skb      # XDP 模式: skb（通用）或 native（硬件加速）
xdp_repl_ifname eth0     # 网卡接口名（根据实际网卡修改）
xdp_repl_queue 0         # XDP 队列 ID
xdp_repl_udp_port 6383   # UDP 端口（用于 KCP 传输）
xdp_repl_tcp_port 6483   # TCP 端口（KCP 控制端口）
xdp_repl_batch 64        # 批处理大小
xdp_repl_mtu 1400        # MTU 大小
```

### 步骤 3: 配置 Slave

编辑 `conf/kvstore-slave.conf`：

```ini
# ── 服务配置 ──────────────────────────────────────────────────────
port 6380                # Redis 协议服务端口（与 master 不同）
network proactor
maxclients 10000
loglevel info

# ── 持久化配置 ────────────────────────────────────────────────────
persistence none         # 纯内存模式

# ── 线程配置 ──────────────────────────────────────────────────────
threads 1
cpu_pin auto

# ── RDMA 全量同步配置 ─────────────────────────────────────────────
rdma_sync on             # 启用 RDMA 同步
rdma_role slave          # 角色：slave
rdma_provider verbs      # 必须与 master 保持一致
rdma_master_ip 192.168.30.135   # Master IP 地址（修改为实际 master IP）
rdma_master_port 2100    # Master RDMA 端口

# ── 增量同步配置（eBPF + KCP + XDP/AF_XDP）────────────────────────
xdp_repl on              # 启用增量同步
xdp_driver_mode skb
xdp_repl_ifname ens33    # 网卡接口名（VMware 通常是 ens33）
xdp_repl_queue 0
xdp_repl_udp_port 6383   # UDP 端口（与 master 相同）
xdp_repl_tcp_port 6483   # TCP 端口（与 master 相同）
xdp_repl_batch 64
xdp_repl_mtu 1400
```

**重要说明**:
- `rdma_provider` 必须在 master 和 slave 上保持一致
- 两台不同机器：UDP/TCP 端口应与 master 相同（master 会自动学习 slave 地址）
- 同一台机器：端口必须不同，避免冲突
- 网卡名称：VMware 通常是 `ens33`，VirtualBox 通常是 `enp0s3`
- XDP 驱动模式：虚拟机环境使用 `skb` 模式（通用模式）
- AF_XDP 可以在 `skb` 模式下正常工作，不需要改成 native 模式

### 步骤 4: 启动 Master

在 Master VM 上：

```bash
cd kvstore
sudo ./build/kvstore conf/kvstore-master.conf
```

预期输出：
```
[INFO] Starting KVStore Master
[INFO] Redis protocol listening on 0.0.0.0:6379
[INFO] RDMA listening on 0.0.0.0:2100 (provider: verbs)
[INFO] eBPF filter attached to eth0
[INFO] XDP replication enabled (mode: skb)
[INFO] Ready to accept connections
```

### 步骤 5: 启动 Slave

在 Slave VM 上：

```bash
cd kvstore
sudo ./build/kvstore conf/kvstore-slave.conf
```

预期输出：
```
[INFO] Starting KVStore Slave
[INFO] Connecting to master at 192.168.30.135:2100
[INFO] RDMA full sync started
[INFO] Received 1000000 keys (512 MB) in 5.2s (98.5 MB/s)
[INFO] Full sync complete
[INFO] eBPF filter attached to eth0
[INFO] Incremental sync enabled
[INFO] Redis protocol listening on 0.0.0.0:6380 (read-only)
[INFO] Ready to accept connections
```

### 步骤 6: 验证复制

在 Master VM 上写入数据：

```bash
redis-cli -h 192.168.30.135 -p 6379 SET mykey "Hello World"
redis-cli -h 192.168.30.135 -p 6379 HSET user:1 name "Alice" age "30"
```

在 Slave VM 上读取数据：

```bash
redis-cli -h 192.168.30.136 -p 6380 GET mykey
# 输出: "Hello World"

redis-cli -h 192.168.30.136 -p 6380 HGETALL user:1
# 输出: 1) "name"
#       2) "Alice"
#       3) "age"
#       4) "30"
```

---

## 配置参考

### 网络 Provider

| Provider | 传输方式 | 吞吐量 | 适用场景 |
|----------|---------|--------|---------|
| `tcp` | TCP/IP over Ethernet | 40-50 MB/s | 标准网络 |
| `verbs` | RDMA over InfiniBand/RoCE | 100+ MB/s | 高性能网络 |

**重要**: Master 和 Slave 必须使用**相同的 provider**。

### XDP 驱动模式

| 模式 | 说明 | 性能 | 兼容性 |
|------|------|------|--------|
| `skb` | 通用模式（软件） | 良好 | 所有网卡 |
| `native` | 硬件卸载 | 优秀 | 需要网卡支持 |

### 端口配置

**重要说明**：Master 会从 slave 的第一个数据包中自动学习 slave 的地址和端口。

**两台不同机器部署**（推荐）：

```ini
# Master
port 6379
xdp_repl_udp_port 6383
xdp_repl_tcp_port 6483

# Slave
port 6380
xdp_repl_udp_port 6383    # 与 master 相同
xdp_repl_tcp_port 6483    # 与 master 相同
```

**同一台机器部署**（测试用）：

```ini
# Master
port 6379
xdp_repl_udp_port 6383
xdp_repl_tcp_port 6483

# Slave
port 6380
xdp_repl_udp_port 7383    # 必须不同，避免端口冲突
xdp_repl_tcp_port 7483    # 必须不同
```

---

## 性能测试

### 测试增量同步性能影响

```bash
cd test/bench
sudo ./bench_incremental_impact.sh
```

此脚本会：
1. 测试基线 QPS（无复制）
2. 提示你启动 slave
3. 测试启用复制后的 QPS
4. 生成 CSV 报告

### 测试 RDMA 全量同步

```bash
cd test/bench
sudo ./bench_fullsync.sh
```

测量指标：
- 同步时间
- 吞吐量（MB/s）
- 加载的键数量

### 手动测试

```bash
# 启动 master
sudo ./build/kvstore conf/kvstore-master.conf

# 另一个终端，启动 slave
sudo ./build/kvstore conf/kvstore-slave.conf

# 另一个终端，运行 redis-benchmark
redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 100000 -c 50 -d 1024

# 检查 slave 是否已复制数据
redis-cli -p 6380 DBSIZE
```

---

## 故障排查

### eBPF 程序加载失败

**错误**: `libbpf: failed to load BPF program`

**解决方案**:
1. 检查内核版本: `uname -r`（必须 5.10+）
2. 安装内核头文件: `sudo apt-get install linux-headers-$(uname -r)`
3. 启用 BTF: `sudo modprobe btf`
4. 检查 BPF 文件系统: `mount | grep bpf`

### RDMA 连接失败

**错误**: `Failed to connect to master`

**解决方案**:
1. 验证网络连通性: `ping <master_ip>`
2. 检查防火墙规则:
   ```bash
   sudo iptables -A INPUT -p tcp --dport 2100 -j ACCEPT
   sudo iptables -A INPUT -p udp --dport 6383 -j ACCEPT
   ```
3. 验证 master 正在监听: `sudo netstat -tulpn | grep 2100`
4. 检查 provider 兼容性: Master 和 slave 必须使用相同 provider

### AF_XDP 不支持

**警告**: `AF_XDP not supported on lo, falling back to UDP`

**说明**: Loopback 接口不支持 AF_XDP。这是预期行为且无害。使用物理/虚拟网卡可获得 AF_XDP 支持。

### 性能影响过大

**问题**: 复制导致 >10% QPS 下降

**解决方案**:
1. 验证 eBPF 优化已启用（检查 `REPL_OBSERVE_MAX_DATA = 512`）
2. 增加 `repl_observe.h` 中的 ringbuf 大小
3. 如果网卡支持，使用 `native` XDP 模式
4. 检查 CPU 亲和性: `cpu_pin auto`

### Slave 数据不同步

**问题**: Slave 数据与 master 不匹配

**解决方案**:
1. 检查 eBPF 过滤器是否已挂载: `sudo bpftool prog list`
2. 验证 KCP 接收器正在运行: `sudo netstat -ulpn | grep 7383`
3. 检查 master 日志中的复制错误
4. 重启 slave 以触发全量重新同步

---

## 技术细节

### eBPF 过滤器设计

eBPF 程序使用 `sk_skb/stream_verdict` 在 socket 层拦截 Redis 命令：

1. **快速过滤**: 检查首字节是否为 '*'（RESP 数组标记）
2. **命令解析**: 仅解析前 64 字节以识别命令类型
3. **写命令检测**: 过滤 SET、DEL、HSET、HDEL 等写命令
4. **零拷贝**: 直接从 skb 拷贝到 ringbuf（无中间缓冲区）
5. **透明**: 始终返回 `SK_PASS`（不阻塞应用程序）

### RDMA 传输协议

1. Slave 通过 libfabric 连接到 master
2. Master 在内存中生成 RDB 快照
3. Master 发送快照大小头
4. Master 通过 RDMA 流式传输 RDB 数据
5. Slave 接收并加载 RDB 到内存
6. 传输完成后关闭连接

### KCP 可靠性

KCP 在 UDP 上提供类似 TCP 的可靠性：
- **ARQ**: 自动重传请求处理丢包
- **拥塞控制**: 自适应窗口大小
- **快速重传**: 快速从丢包中恢复
- **选择性 ACK**: 高效确认

---

## 许可证

本项目用于系统编程、数据库内核与性能优化学习展示。

---

## 致谢

- **Redis**: 协议规范和灵感来源
- **libbpf**: eBPF CO-RE 支持
- **libfabric**: RDMA 抽象层
- **KCP**: 可靠 UDP 协议
