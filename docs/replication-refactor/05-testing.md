# 测试方案和验证步骤

## 🧪 测试环境准备

### 1. 清理环境
```bash
cd /home/xxf/9.1-kvstore
pkill -f kvstore
rm -f /tmp/repl_offset /tmp/master.log /tmp/slave.log
```

### 2. 编译
```bash
cd build
make clean
cmake ..
make -j$(nproc)
```

检查编译是否成功：
```bash
ls -lh kvstore
# 应该看到可执行文件
```

---

## 📋 测试用例

### 测试 1：基本全量同步

**步骤**：
```bash
# 1. 启动 master
./build/kvstore conf/kvstore-master-local.conf > /tmp/master.log 2>&1 &
MASTER_PID=$!
sleep 2

# 2. 写入测试数据
for i in {1..100}; do
    redis-cli -p 2000 SET key$i value$i > /dev/null
done

# 3. 启动 slave（触发全量同步）
./build/kvstore conf/kvstore-slave-local.conf > /tmp/slave.log 2>&1 &
SLAVE_PID=$!

# 4. 等待同步完成
sleep 10

# 5. 验证数据
redis-cli -p 2001 GET key50
# 应返回 "value50"

redis-cli -p 2001 DBSIZE
# 应返回 100
```

**预期日志**：

Master:
```
[REPL-TCP] Slave requests sync from offset 0
[REPL-TCP] Offset 0 too old, triggering full sync (base_seq=12345)
[RDMA-REPL] Registered child session: pid=7890
[REPL-TCP] Sent fullsync response (base_seq=12345, rdma_port=2101)
[CHILD] Fullsync process started (pid=7890, base_seq=12345)
[CHILD] Listening on port 2101
[CHILD] Slave connected
[CHILD] Total entries to send: 100
[CHILD] Fullsync complete: 100 entries, 5120 bytes, 10.5 ms, 487.6 KB/s
[REPL-TCP] Slave completed fullsync (base_seq=12345, entries=100, batches=1)
```

Slave:
```
[REPL-TCP] Connecting to master 127.0.0.1:6483...
[REPL-TCP] Connected to 127.0.0.1:6483
[SLAVE] Starting RDMA fullsync to 127.0.0.1:2101
[SLAVE] Connected to master child process
[SLAVE] Pre-posted 4 receive buffers
[SLAVE] Header: key_count=100, base_seq=12345
[SLAVE] Batch 1: 100 entries, 5120 bytes
[SLAVE] Connection closed, fullsync done
[SLAVE] Total: 1 batches, 100 entries
[REPL-TCP] Sent FULLSYNC_DONE (base_seq=12345)
```

---

### 测试 2：增量同步

**步骤**：
```bash
# 基于测试 1 的环境，slave 已经运行

# 1. 在 master 写入新数据
redis-cli -p 2000 SET newkey1 newvalue1
redis-cli -p 2000 SET newkey2 newvalue2

# 2. 等待增量同步
sleep 2

# 3. 验证 slave 收到增量
redis-cli -p 2001 GET newkey1
# 应返回 "newvalue1"
```

**预期日志**：

Master:
```
[REPL-TCP] Master published: SET newkey1 newvalue1
```

Slave:
```
[REPL-TCP] Received incremental command: SET newkey1 newvalue1
```

---

### 测试 3：断线重连 + 增量追赶

**步骤**：
```bash
# 1. 停止 slave
kill $SLAVE_PID
sleep 2

# 2. 在 master 写入数据（slave 离线期间）
for i in {101..110}; do
    redis-cli -p 2000 SET key$i value$i
done

# 3. 重启 slave
./build/kvstore conf/kvstore-slave-local.conf > /tmp/slave.log 2>&1 &
SLAVE_PID=$!

# 4. 等待重连和追赶
sleep 5

# 5. 验证数据
redis-cli -p 2001 GET key105
# 应返回 "value105"
```

**预期日志**：

Master:
```
[REPL-TCP] Slave requests sync from offset 12345
[REPL-TCP] Offset 12345 available in backlog
[REPL-TCP] Sent 512 bytes of catchup data
```

Slave:
```
[REPL-TCP] Sent SYNC_REQUEST(offset=12345)
[REPL-TCP] Received incremental sync response
[REPL-TCP] Applying 512 bytes catchup data
[REPL-TCP] Catchup complete, entering realtime mode
```

---

### 测试 4：断线重连 + offset 过期（重新全量）

**步骤**：
```bash
# 1. 停止 slave
kill $SLAVE_PID

# 2. 在 master 写入大量数据（超过 64MB backlog）
for i in {1..100000}; do
    redis-cli -p 2000 SET bigkey$i $(head -c 1000 /dev/urandom | base64)
done

# 3. 重启 slave
./build/kvstore conf/kvstore-slave-local.conf > /tmp/slave.log 2>&1 &

# 4. 等待全量同步
sleep 30

# 5. 验证
redis-cli -p 2001 DBSIZE
```

**预期日志**：

Master:
```
[REPL-TCP] Slave requests sync from offset 12345
[REPL-TCP] Offset 12345 too old, triggering full sync
[CHILD] Fullsync process started
```

---

## 🔍 调试工具

### 查看进程
```bash
ps aux | grep kvstore
```

### 查看日志
```bash
# Master 日志
tail -f /tmp/master.log | grep -E "(REPL|RDMA|CHILD)"

# Slave 日志
tail -f /tmp/slave.log | grep -E "(REPL|RDMA|SLAVE)"
```

### 监控 backlog
```bash
# 在 master 执行
redis-cli -p 2000 INFO replication
```

### 检查 offset 文件
```bash
cat /tmp/repl_offset
```

---

## ✅ 验证清单

- [ ] 全量同步成功（100 keys）
- [ ] 增量同步成功（实时命令）
- [ ] 断线重连 + 增量追赶成功
- [ ] 断线重连 + offset 过期触发全量
- [ ] 心跳保活（15s 超时）
- [ ] 子进程自动回收（SIGCHLD）
- [ ] 多 slave 并发全量（启动 2 个 slave）

---

## 🐛 常见问题

### 1. 编译错误
```bash
# 检查缺失的头文件
grep -r "rdma_repl.h" src/
```

### 2. 连接失败
```bash
# 检查端口占用
netstat -tuln | grep -E "(2101|6483)"
```

### 3. 数据不一致
```bash
# 对比 master 和 slave
redis-cli -p 2000 DBSIZE
redis-cli -p 2001 DBSIZE
```

---

## 📊 性能基准

### 小数据集（100MB）
- 预期耗时：< 100ms
- 吞吐量：> 1 GB/s

### 中数据集（1GB）
- 预期耗时：< 1s
- 吞吐量：> 1 GB/s

### 大数据集（10GB）
- 预期耗时：< 10s
- 吞吐量：> 1 GB/s

---

**完成！** 所有实现指南已创建，可以开始按文档实现代码。
