# KVStore — 高性能 Redis 兼容键值存储

一个由 C 语言实现的高性能键值存储系统，兼容 Redis 协议，支持 RDMA 全量同步、eBPF + TCP 增量复制、io_uring 零拷贝网络、向量相似度搜索。

---

## 主从复制架构

### 复制流程

主从复制分为两个阶段：

#### 阶段 1：RDMA 全量同步（启动时）

```
┌─────────┐                                    ┌─────────┐
│  Master │                                    │  Slave  │
└────┬────┘                                    └────┬────┘
     │                                              │
     │◄─────────── fi_connect (port 2100) ─────────┤
     │                                              │
     ├─────────── FI_CONNECTED ───────────────────►│
     │                                              │
     │  fork() + COW snapshot                       │
     │  ┌──────────────────┐                       │
     │  │ Child: serialize │                       │
     │  │ Parent: fi_send  │                       │
     │  └──────────────────┘                       │
     │                                              │
     ├─────────── RDB chunks (streaming) ─────────►│
     │                                              │ fi_cq_sread
     │                                              │ rdb_load_stream
     │                                              │
     ├─────────── EOF chunk ──────────────────────►│
     │                                              │
     │                                              │ Full sync done
```

**特点**：
- 使用 libfabric（tcp 或 verbs provider）
- 流式传输：1MB 分块，无需预先生成完整 RDB 文件
- fork() + COW：子进程序列化，父进程发送，互不阻塞
- 性能：tcp provider 40-50 MB/s，verbs provider 100+ MB/s

#### 阶段 2：TCP + eBPF 增量同步（运行时）

```
┌─────────┐                                    ┌─────────┐
│  Master │                                    │  Slave  │
└────┬────┘                                    └────┬────┘
     │                                              │
     │  eBPF filter (port 2000)                     │
     │  ┌─────────────────────┐                     │
     │  │ sockops: attach     │                     │
     │  │ parser: filter SET  │                     │
     │  │ verdict: to ringbuf │                     │
     │  └─────────────────────┘                     │
     │                                              │
     │◄──────── TCP connect (port 6483) ────────────┤
     │                                              │
     ├─────────── TCP ESTABLISHED ─────────────────►│
     │                                              │
     │  Client: SET key value                       │
     │  ↓                                           │
     │  eBPF captures → ringbuf                     │
     │  ↓                                           │
     │  Backlog (64MB)                              │
     │  ↓                                           │
     ├─────────── [len:4][data:N] ─────────────────►│
     │                                              │ Apply to local
     │                                              │ kvstore (port 2001)
     │                                              │
     │  Auto-reconnect every 3s if disconnected     │
     │◄─────────────────────────────────────────────┤
```

**特点**：
- eBPF 过滤器：只捕获写命令（SET/DEL/HSET 等），减少 60-80% 数据量
- TCP 可靠传输：内核自动处理重传、流控、拥塞控制
- 64MB 复制积压缓冲区：支持断线重连增量恢复
- 延迟：1-3ms（本地网络）
- 吞吐量：300-500 MB/s（取决于 TCP 调优）

### 配置说明

#### 主机配置（kvstore-master.conf）

```conf
# RDMA 全量同步
rdma_sync on
rdma_role master
rdma_provider tcp              # tcp（以太网）或 verbs（RDMA 硬件）
rdma_listen_ip 0.0.0.0
rdma_listen_port 2100

# TCP 增量同步
incr_repl on
incr_repl_port 6483            # TCP 复制端口
```

#### 从机配置（kvstore-slave.conf）

```conf
# RDMA 全量同步
rdma_sync on
rdma_role slave
rdma_provider tcp              # 必须与主机一致
rdma_master_ip 192.168.30.135  # 主机 IP
rdma_master_port 2100

# TCP 增量同步
incr_repl on
incr_repl_master_port 6483     # 主机的 TCP 复制端口
```

### Provider 选择

| Provider | 适用场景 | 性能 | 要求 |
|----------|---------|------|------|
| **tcp** | 普通以太网 | 40-50 MB/s | 无特殊要求 |
| **verbs** | RDMA 硬件 | 100+ MB/s | InfiniBand/RoCE 或 Soft-RoCE |

**推荐**：使用 `tcp` provider，兼容性好，性能足够。

---

## 快速开始

### 编译

```bash
# 安装依赖
sudo apt-get install -y build-essential cmake pkg-config \
    liburing-dev libjemalloc-dev libfabric-dev zlib1g-dev \
    libbpf-dev clang llvm redis-tools

# 编译
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### 本地测试（单机主从）

#### 1. 启动主机

```bash
sudo ./build/kvstore conf/kvstore-master-local.conf
```

#### 2. 启动从机（另一个终端）

```bash
sudo ./build/kvstore conf/kvstore-slave-local.conf
```

#### 3. 测试同步

```bash
# 向主机写入
redis-cli -p 2000 SET key1 "hello"

# 从从机读取
redis-cli -p 2001 GET key1
# 输出: "hello"
```

### 两台虚拟机部署

#### 主机虚拟机（192.168.30.135）

```bash
# 1. 开放防火墙
sudo ufw allow 2000/tcp  # Redis 服务
sudo ufw allow 2100/tcp  # RDMA 全量同步
sudo ufw allow 6483/tcp  # TCP 增量同步

# 2. 启动主机
sudo ./build/kvstore conf/kvstore-master.conf

# 3. 验证端口
sudo netstat -tuln | grep -E ":(2000|2100|6483)"
```

#### 从机虚拟机

```bash
# 1. 修改配置文件
vi conf/kvstore-slave.conf
# 确保: rdma_master_ip 192.168.30.135

# 2. 测试连通性
ping -c 3 192.168.30.135
telnet 192.168.30.135 2100
telnet 192.168.30.135 6483

# 3. 启动从机
sudo ./build/kvstore conf/kvstore-slave.conf
```

### 成功标志

**主机日志**：
```
[RDMA-REPL] master: listening for slaves
[RDMA-REPL] master: slave connected, starting streaming full sync
[RDMA-REPL] master: streaming full sync done
[REPL-TCP] Slave connected from 192.168.30.136:xxxxx
```

**从机日志**：
```
[RDMA-REPL] slave: connected, awaiting streaming full sync
[RDMA-REPL] slave: loaded 0 keys from fullsync
[REPL-TCP] Connected to 192.168.30.135:6483
```

---

## 性能优化

### 信号处理优化

**问题**：原始代码中 RDMA 调用使用无限等待（`-1` 超时），导致 Ctrl+C 无法中断程序。

**解决方案**：
- 所有 `fi_cq_sread` 和 `fi_eq_sread` 改为 1 秒超时
- 每次循环检查 `g_kvs_stop` 信号标志
- Ctrl+C 响应时间：1-2 秒

**修改位置**：
- `rdma_repl.c:rdma_wait_cq()` - CQ 等待
- `rdma_repl.c:rdma_wait_eq()` - EQ 等待
- `rdma_repl.c:rdma_master_thread()` - 主机监听循环
- `rdma_repl.c:rdma_repl_slave_run()` - 从机接收循环

### 网络优化

**TCP 参数**：
```c
// repl_tcp.c
int opt = 1;
setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));  // 禁用 Nagle

int sndbuf = 2 * 1024 * 1024;  // 2MB 发送缓冲区
setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

int rcvbuf = 2 * 1024 * 1024;  // 2MB 接收缓冲区
setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
```

---

## 常见问题

### Q1: 为什么必须用 sudo 运行？

**A**: eBPF 程序需要 `CAP_BPF` 或 root 权限才能加载到内核。

### Q2: verbs provider 初始化失败？

**A**: 你的系统没有 RDMA 硬件或 Soft-RoCE。使用 `tcp` provider 即可：

```conf
rdma_provider tcp
```

如果需要 verbs，配置 Soft-RoCE：
```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev eth0
```

### Q3: 从机连接超时？

**A**: 检查：
1. 主机是否已启动
2. 防火墙是否开放端口 2100 和 6483
3. 网络是否连通（ping 测试）
4. 配置文件中的 IP 地址是否正确

### Q4: Ctrl+C 无法停止程序？

**A**: 已修复。如果仍有问题，等待 1-2 秒让信号处理完成。

### Q5: 数据不同步？

**A**: 检查：
1. 从机日志是否显示 "Connected"
2. eBPF 过滤器是否加载成功
3. 使用 `redis-cli` 测试写入和读取

---

## 性能指标

| 指标 | 值 |
|------|---|
| **全量同步** | 40-50 MB/s (tcp), 100+ MB/s (verbs) |
| **增量同步延迟** | 1-3ms |
| **增量同步吞吐** | 300-500 MB/s |
| **eBPF 过滤效果** | 减少 60-80% 数据量 |
| **自动重连间隔** | 3 秒 |
| **复制积压缓冲区** | 64 MB |

---

## 更新日志

### Phase 9.1 (2026-05-29)

- ✅ 实现 RDMA 全量同步（libfabric）
- ✅ 实现 eBPF + TCP 增量同步
- ✅ 修复 Ctrl+C 信号处理
- ✅ 支持 tcp 和 verbs provider
- ✅ 添加 64MB 复制积压缓冲区
- ✅ 优化 TCP 参数（TCP_NODELAY, 2MB 缓冲区）
- ✅ 完善配置文件和文档
