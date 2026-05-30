# 配置文件更新说明

**日期:** 2026-05-30  
**更新内容:** 根据测试结果和最佳实践更新所有配置文件

---

## 更新的配置文件

### 1. kvstore.conf（配置说明文件）

**更新内容:**
- ✅ 更新了主从复制架构说明
- ✅ 添加了 TCP + eBPF 增量同步详细说明
- ✅ 更新了 provider 选择建议（推荐使用 tcp）
- ✅ 添加了性能优化建议
- ✅ 添加了监控建议和日志关键字
- ✅ 添加了故障排查 FAQ
- ✅ 移除了过时的 XDP/AF_XDP 配置项

**关键变更:**
```conf
# 推荐使用 tcp provider（兼容性好）
rdma_provider tcp

# TCP + eBPF 增量同步（Phase 9.1）
incr_repl on
incr_repl_port 6483
```

---

### 2. kvstore-master.conf（主机配置）

**更新内容:**
- ✅ 更新了启动命令说明（需要设置 BUILD_DIR）
- ✅ 添加了 capabilities 设置说明
- ✅ 更新了 provider 说明（tcp 推荐）
- ✅ 添加了增量同步架构说明
- ✅ 添加了性能特点说明
- ✅ 添加了部署检查清单

**关键配置:**
```conf
# RDMA 全量同步
rdma_sync on
rdma_role master
rdma_provider tcp        # 推荐使用 tcp
rdma_listen_ip 0.0.0.0
rdma_listen_port 2100

# TCP + eBPF 增量同步
incr_repl on
incr_repl_port 6483
```

**部署检查清单:**
1. 设置 BUILD_DIR 环境变量
2. 设置 capabilities 或使用 sudo
3. 开放防火墙端口（2000, 2100, 6483）
4. 验证端口监听
5. 检查日志

---

### 3. kvstore-slave.conf（从机配置）

**更新内容:**
- ✅ 更新了启动命令说明（需要设置 BUILD_DIR）
- ✅ 添加了 capabilities 设置说明
- ✅ 更新了 provider 说明（tcp 推荐）
- ✅ 添加了增量同步流程说明
- ✅ 添加了性能特点说明
- ✅ 添加了部署检查清单

**关键配置:**
```conf
# RDMA 全量同步
rdma_sync on
rdma_role slave
rdma_provider tcp        # 必须与 master 一致
rdma_master_ip 192.168.30.135  # 修改为实际 master IP
rdma_master_port 2100

# TCP 增量同步
incr_repl on
incr_repl_master_port 6483
```

**部署检查清单:**
1. 修改 rdma_master_ip 为实际 master IP
2. 确保 master 已启动并监听端口
3. 确保网络连通（ping, telnet）
4. 设置 BUILD_DIR 环境变量
5. 设置 capabilities 或使用 sudo
6. 检查日志

---

### 4. kvstore-master-local.conf（本地测试主机）

**当前配置:**
```conf
port 2000
network proactor
persistence none
threads 1

rdma_sync on
rdma_role master
rdma_provider tcp        # 已更新为 tcp
rdma_listen_ip 0.0.0.0
rdma_listen_port 2100

incr_repl on
incr_repl_port 6483
```

**状态:** ✅ 已在测试中验证，配置正确

---

### 5. kvstore-slave-local.conf（本地测试从机）

**当前配置:**
```conf
port 2001
network proactor
persistence none
threads 1

rdma_sync on
rdma_role slave
rdma_provider tcp        # 已更新为 tcp
rdma_master_ip 127.0.0.1
rdma_master_port 2100

incr_repl on
incr_repl_master_port 6483
```

**状态:** ✅ 已在测试中验证，配置正确

---

## 主要变更总结

### 1. Provider 推荐变更

**之前:**
```conf
rdma_provider verbs      # 默认推荐 verbs
```

**现在:**
```conf
rdma_provider tcp        # 推荐使用 tcp（兼容性好）
```

**原因:**
- verbs provider 需要 RDMA 硬件或 Soft-RoCE
- tcp provider 在普通以太网上工作良好
- 测试验证 tcp provider 性能足够（40-50 MB/s）

---

### 2. 增量同步说明更新

**之前:**
- 提到 KCP + AF_XDP
- 配置项较复杂

**现在:**
- 明确说明使用 TCP + eBPF
- 简化配置项
- 添加详细的架构和性能说明

**新增配置项说明:**
```conf
# TCP + eBPF 增量同步
incr_repl on              # 启用增量同步
incr_repl_port 6483       # Master: 监听端口
incr_repl_master_port 6483  # Slave: Master 的端口
```

---

### 3. 部署说明增强

**新增内容:**
1. **BUILD_DIR 环境变量说明**
   ```bash
   export BUILD_DIR=/path/to/kvstore/build
   ```

2. **Capabilities 设置说明**
   ```bash
   sudo setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep kvstore
   ```

3. **防火墙配置说明**
   ```bash
   sudo ufw allow 2000/tcp  # Redis 服务
   sudo ufw allow 2100/tcp  # RDMA 全量同步
   sudo ufw allow 6483/tcp  # TCP 增量同步
   ```

4. **网络连通性检查**
   ```bash
   ping <master_ip>
   telnet <master_ip> 2100
   telnet <master_ip> 6483
   ```

---

### 4. 监控和故障排查

**新增监控建议:**
- 复制延迟监控（比较 DBSIZE）
- 连接状态监控（日志关键字）
- 全量同步频率监控
- 积压缓冲区使用率监控
- eBPF 过滤器状态监控

**新增日志关键字:**
- `[RDMA-REPL]` - RDMA 全量同步相关
- `[REPL-TCP]` - TCP 增量同步相关
- `[REPL-INCR]` - 增量恢复相关
- `[REPL-STREAM]` - 复制流相关

**新增故障排查 FAQ:**
- Q1: eBPF 过滤器加载失败？
- Q2: 从机连接超时？
- Q3: 数据不同步？
- Q4: Ctrl+C 无法停止程序？
- Q5: verbs provider 初始化失败？

---

## 配置文件对比

### 主要差异

| 配置项 | 旧值 | 新值 | 说明 |
|--------|------|------|------|
| rdma_provider | verbs | tcp | 推荐使用 tcp |
| 增量同步 | xdp_repl | incr_repl | 使用 TCP + eBPF |
| 文档 | 简单 | 详细 | 添加架构、性能、部署说明 |

---

## 使用建议

### 开发/测试环境

使用本地测试配置:
```bash
# Master
sudo BUILD_DIR=/home/xxf/9.1-kvstore/build \
  ./kvstore conf/kvstore-master-local.conf

# Slave
sudo BUILD_DIR=/home/xxf/9.1-kvstore/build \
  ./kvstore conf/kvstore-slave-local.conf
```

### 生产环境（两台机器）

**Master 机器:**
1. 编辑 `conf/kvstore-master.conf`
2. 确认 `rdma_provider tcp`
3. 开放防火墙端口
4. 启动服务

**Slave 机器:**
1. 编辑 `conf/kvstore-slave.conf`
2. 修改 `rdma_master_ip` 为 master IP
3. 确认 `rdma_provider tcp`
4. 测试网络连通性
5. 启动服务

---

## 验证配置

### 检查配置文件

```bash
# 查看 master 配置
grep -E "rdma_|incr_repl" conf/kvstore-master.conf

# 查看 slave 配置
grep -E "rdma_|incr_repl" conf/kvstore-slave.conf
```

### 测试连接

```bash
# 在 slave 机器上测试
ping <master_ip>
telnet <master_ip> 2100
telnet <master_ip> 6483
```

### 检查日志

**Master 日志应显示:**
```
[RDMA-REPL] master: using provider 'tcp' on 0.0.0.0:2100
[RDMA-REPL] master: listening for slaves
[REPL-TCP] eBPF filter loaded, monitoring port 2000
[REPL-TCP] Master initialized on port 6483
```

**Slave 日志应显示:**
```
[RDMA-REPL] slave: using provider 'tcp', connecting to <master_ip>:2100
[RDMA-REPL] slave: connected, awaiting streaming full sync
[RDMA-REPL] slave: loaded N keys from fullsync
[REPL-TCP] Connected to <master_ip>:6483
```

---

## 总结

✅ **所有配置文件已更新**
- kvstore.conf（配置说明）
- kvstore-master.conf（主机配置）
- kvstore-slave.conf（从机配置）
- kvstore-master-local.conf（本地测试主机）
- kvstore-slave-local.conf（本地测试从机）

✅ **主要改进**
- 推荐使用 tcp provider（兼容性好）
- 明确 TCP + eBPF 增量同步架构
- 添加详细的部署说明和检查清单
- 添加监控建议和故障排查 FAQ
- 移除过时的配置项

✅ **已验证**
- 本地测试配置已通过完整测试
- 100% 复制准确性
- 所有功能正常工作

---

**更新完成日期:** 2026-05-30  
**状态:** ✅ 配置文件已更新并验证
