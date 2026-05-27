# RDMA Provider 性能测试指南

## 概述

本测试用于对比不同 RDMA provider（tcp vs verbs）的全量同步性能。

## 测试架构

```
主机 (Master)                           从机 (Slave)
┌─────────────────┐                    ┌─────────────────┐
│  kvstore        │                    │  kvstore        │
│  (已填充数据)    │ ─── RDMA ───────> │  (空数据库)      │
│                 │    fullsync        │                 │
│  Provider: tcp  │                    │  Provider: tcp  │
│  或 verbs       │                    │  或 verbs       │
└─────────────────┘                    └─────────────────┘
```

## 测试流程

### 步骤 1: 准备主机配置

编辑 `conf/kvstore-master.conf`：

```conf
port 2000
rdma_sync on
rdma_role master
rdma_provider tcp        # 或 verbs
rdma_listen_ip 0.0.0.0
rdma_listen_port 2100
persistence none
```

### 步骤 2: 启动主机

```bash
# 在主机上运行
cd /home/xxf/9.1-kvstore
sudo ./build/kvstore conf/kvstore-master.conf
```

### 步骤 3: 填充测试数据

在主机上运行 `fill_data.sh` 填充数据：

```bash
# 填充 100MB 数据
./test/bench/fill_data.sh 100 127.0.0.1 2000

# 输出示例：
# [SUCCESS] Data filled successfully
# PAYLOAD_BYTES=104857600
# PAYLOAD_MB=100.00
# KEY_COUNT=102400
#
# Next step: Run on slave machine:
#   sudo ./test/bench/bench_rdma_provider.sh --payload 104857600 --master-ip 127.0.0.1
```

**参数说明：**
- 第1个参数：数据大小（MB）
- 第2个参数：主机IP（默认 127.0.0.1）
- 第3个参数：主机端口（默认 2000）

### 步骤 4: 运行从机测试

在从机上运行 `bench_rdma_provider.sh`：

```bash
# 使用 fill_data.sh 输出的 PAYLOAD_BYTES
sudo ./test/bench/bench_rdma_provider.sh \
    --payload 104857600 \
    --master-ip 192.168.1.10

# 或指定更多参数
sudo ./test/bench/bench_rdma_provider.sh \
    --payload 104857600 \
    --master-ip 192.168.1.10 \
    --master-port 2100 \
    --slave-port 2002 \
    --providers tcp,verbs \
    --timeout 120
```

**参数说明：**
- `--payload <bytes>`: 数据大小（字节），从 fill_data.sh 获取
- `--master-ip <ip>`: 主机IP地址（必需）
- `--master-port <port>`: 主机RDMA端口（默认 2100）
- `--slave-port <port>`: 从机服务端口（默认 2002）
- `--providers <list>`: 测试的provider列表，逗号分隔（默认 tcp,verbs）
- `--timeout <seconds>`: 同步超时时间（默认 120秒）
- `--output <file>`: CSV输出文件（默认自动生成）

### 步骤 5: 查看测试结果

测试完成后会生成 CSV 文件：

```bash
# 查看 CSV 文件
cat rdma_provider_benchmark_20250115_103000.csv

# 输出示例：
timestamp            provider  payload_mb  payload_bytes  sync_time_ms  throughput_mbps  keys_loaded  slave_port  master_ip
2025-01-15 10:30:00  tcp       100.00      104857600      10234.5       9.82             102400       2002        192.168.1.10
2025-01-15 10:31:00  verbs     100.00      104857600      2156.3        46.61            102400       2002        192.168.1.10
```

## CSV 字段说明

| 字段 | 说明 |
|------|------|
| timestamp | 测试时间戳 |
| provider | RDMA provider (tcp/verbs) |
| payload_mb | 数据大小（MB） |
| payload_bytes | 数据大小（字节） |
| sync_time_ms | 同步时间（毫秒） |
| throughput_mbps | 吞吐量（MB/s） |
| keys_loaded | 加载的key数量 |
| slave_port | 从机端口 |
| master_ip | 主机IP |

## 日志关键字

测试脚本通过以下日志关键字判断同步状态：

**从机连接成功：**
```
[RDMA-REPL] slave: connected, awaiting streaming full sync
```

**同步完成：**
```
[RDMA-REPL] slave: fullsync_time_ms=2156.3 payload_bytes=104857600 throughput_mbps=46.61
[RDMA-REPL] slave: loaded 102400 keys from fullsync (rdb streaming)
```

## 测试不同数据量

```bash
# 主机端：填充不同大小的数据
./test/bench/fill_data.sh 10    # 10MB
./test/bench/fill_data.sh 50    # 50MB
./test/bench/fill_data.sh 100   # 100MB
./test/bench/fill_data.sh 500   # 500MB
./test/bench/fill_data.sh 1000  # 1GB

# 从机端：每次使用对应的 PAYLOAD_BYTES
sudo ./test/bench/bench_rdma_provider.sh --payload <bytes> --master-ip <ip>
```

## 故障排查

### 问题 1: 主机不可达
```
[ERROR] Master not reachable at 192.168.1.10:2100
```

**解决方案：**
- 检查主机是否启动
- 检查网络连通性：`ping 192.168.1.10`
- 检查防火墙规则
- 确认主机监听端口：`ss -tln | grep 2100`

### 问题 2: 同步超时
```
[ERROR] Fullsync timeout after 120s
```

**解决方案：**
- 增加超时时间：`--timeout 300`
- 检查从机日志：`tail -50 /tmp/kvstore_slave_tcp_*.log`
- 检查网络带宽和延迟

### 问题 3: Provider 不支持
```
[RDMA-REPL] fi_getinfo (slave) failed: No data available
```

**解决方案：**
- 检查 libfabric 是否安装：`pkg-config --modversion libfabric`
- 检查 provider 是否可用：`fi_info -p verbs`
- 对于 verbs：确保安装了 RDMA 驱动和 libibverbs

### 问题 4: 权限不足
```
[ERROR] This script requires root privileges
```

**解决方案：**
- 使用 sudo 运行：`sudo ./test/bench/bench_rdma_provider.sh ...`

## 性能预期

| Provider | 网络类型 | 预期吞吐量 | 预期延迟 |
|----------|---------|-----------|---------|
| tcp | 1Gbps 以太网 | 100-120 MB/s | 高 |
| tcp | 10Gbps 以太网 | 800-1000 MB/s | 中 |
| verbs | InfiniBand | 2000-5000 MB/s | 低 |
| verbs | RoCE (10Gbps) | 1000-1200 MB/s | 低 |

## 示例：完整测试流程

```bash
# === 主机端 ===
# 1. 启动主机
sudo ./build/kvstore conf/kvstore-master.conf

# 2. 填充 100MB 数据
./test/bench/fill_data.sh 100 192.168.1.10 2000
# 输出: PAYLOAD_BYTES=104857600

# === 从机端 ===
# 3. 运行测试
sudo ./test/bench/bench_rdma_provider.sh \
    --payload 104857600 \
    --master-ip 192.168.1.10

# 4. 查看结果
cat rdma_provider_benchmark_*.csv
```

## 相关文件

- `test/bench/fill_data.sh` - 主机数据填充脚本
- `test/bench/bench_rdma_provider.sh` - 从机性能测试脚本
- `conf/kvstore-master.conf` - 主机配置模板
- `conf/kvstore-slave.conf` - 从机配置模板
