# TCP 复制线程修改指南

## 📍 文件位置

`src/repl/repl_tcp.c`

---

## 🎯 修改目标

在 master 侧的 `master_handle_sync_request()` 函数中添加 fork 子进程逻辑，实现全量同步的协调。

---

## 📝 当前代码分析

### 当前的 `master_handle_sync_request()` 结构

```c
static int master_handle_sync_request(int fd) {
    recv(sync_request);
    
    if (repl_incr_has_offset(last_offset)) {
        // 增量同步
        send(SYNC_STATUS_OK);
        send(catchup_data);
    } else {
        // 需要全量同步
        send(SYNC_STATUS_FULLSYNC_REQUIRED);
        // ❌ 问题：这里直接返回，slave 不知道如何全量
    }
}
```

**问题**：
- 全量同步只是告诉 slave "你需要全量"，但没有提供 RDMA 端口
- Slave 自己调用 `rdma_repl_slave_run()` 去连接 2100（已删除）
- Master 没有 fork 子进程的逻辑

---

## ✅ 修改后的逻辑

### 关键改动点

1. **添加新消息类型**：`CTRL_MSG_FULLSYNC_DONE`
2. **fork 子进程**：调用 `master_fullsync_child(base_seq)`
3. **等待完成**：recv `FULLSYNC_DONE` 消息
4. **发送追赶数据**：base_seq 到 current_offset

---

## 📋 完整实现代码

由于代码较长，请参考完整的 `master_handle_sync_request()` 实现（约 150 行）。

关键流程：
```
if (offset 在 backlog) {
    // 增量同步（保持不变）
} else {
    base_seq = current_offset;
    child = fork();
    if (child == 0) {
        close(tcp_fd);
        master_fullsync_child(base_seq);
        _exit(0);
    }
    session_add(child);
    send(FULLSYNC_RESPONSE);
    recv(FULLSYNC_DONE);
    send(catchup_data);
}
```

---

**下一步**：阅读 `02-rdma_master.md` 实现子进程的 RDMA 数据传输
