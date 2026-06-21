# Slave RDMA 接收逻辑实现指南

## 📍 文件位置

`src/repl/rdma_repl_slave.c`

---

## 🎯 修改目标

简化 slave 接收逻辑，去掉两阶段连接，改为单阶段直连。

---

## 🔄 架构变化

### 旧架构
```
1. Connect master:2100 (RDMA parent)
2. Recv HANDSHAKE(child_port=2101)
3. Disconnect, Connect child:2101
4. Send HANDSHAKE, Recv data with ctrl messages
```

### 新架构
```
1. repl_tcp recv SYNC_RESPONSE(rdma_port=2101)
2. rdma_repl_slave_fullsync("127.0.0.1", 2101)
3. Direct connect, recv data until FI_SHUTDOWN
4. Return to repl_tcp, send FULLSYNC_DONE
```

---

## 📝 函数签名

```c
/* 新增函数 */
int rdma_repl_slave_fullsync(const char *master_ip, uint16_t rdma_port);
```

---

## 🔑 关键实现

### 1. 单阶段连接
直接连接 child RDMA 端口，无需父进程握手。

### 2. 连接关闭检测
```c
if (err.err == FI_ESHUTDOWN || err.err == FI_ENOTCONN) {
    // 正常结束
    break;
}
```

### 3. 无控制消息
纯数据流，连接关闭 = EOF。

---

## 📋 完整代码

请参考完整实现（约 200 行），包含：
- libfabric 连接
- 4 x 1GB receive buffer 预分配
- 接收循环直到 FI_SHUTDOWN
- Header 解析 + entry 解析
- rdb_rebuild_all_encodings()
- repl_incr_save_offset(base_seq)

---

## 📝 repl_tcp.c 调用

```c
if (status == SYNC_STATUS_FULLSYNC_REQUIRED) {
    rdma_repl_slave_fullsync(master_ip, rdma_port);
    send(FULLSYNC_DONE);
    recv(catchup_data);
}
```

---

**下一步**：阅读 `05-testing.md` 了解测试方案
