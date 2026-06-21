# RDMA 复制架构重构指南

## 📌 重构概述

本次重构将 RDMA 复制架构从"混合控制+数据"模式改为"TCP 控制 + RDMA 数据"模式，借鉴 Redis 的简洁设计。

---

## 🎯 目标

### 当前问题
- ❌ RDMA 控制消息和数据消息混在一起，导致接收顺序混乱
- ❌ 父进程 RDMA 线程只做握手，职责单一但占用资源
- ❌ 三个端口（6379 客户端、2100 RDMA 握手、2101 RDMA 数据、6483 TCP 增量）
- ❌ slave 接收逻辑复杂（预 post 大 buffer 但先收到小控制消息）

### 改进后
- ✅ TCP (6483) 统一控制入口（握手、判断增量/全量、心跳、协调）
- ✅ RDMA (2101) 仅用于全量同步的数据传输（纯数据，无控制消息）
- ✅ 父进程不参与 RDMA，只管理子进程 session
- ✅ 简化协议，删除 RDMA 控制消息

---

## 🏗️ 新架构

```
┌─────────────────────────────────────────────────────────────────┐
│ Master 进程                                                      │
│                                                                  │
│  主线程 (6379)                                                   │
│    - 处理客户端命令                                              │
│    - 写命令 → repl_incr_append()                                 │
│                                                                  │
│  TCP 复制线程 (6483) ← 唯一控制入口                             │
│    - Accept slave                                                │
│    - Recv SYNC_REQUEST(last_offset)                              │
│    - 判断: offset >= backlog_start?                              │
│        ├─ YES → 增量同步（TCP 发送）                             │
│        └─ NO  → 全量同步：                                       │
│              - base_seq = master_offset                          │
│              - fork() 子进程                                     │
│              - 子进程创建 RDMA (2101)，纯数据传输                │
│              - 父进程等待 FULLSYNC_DONE                          │
│              - 发送追赶数据                                      │
│    - 进入实时增量（心跳 + 命令流）                               │
└─────────────────────────────────────────────────────────────────┘
         ↓ TCP 6483 (控制 + 增量数据)
         ↓ RDMA 2101 (全量数据，子进程临时创建)
┌─────────────────────────────────────────────────────────────────┐
│ Slave 进程                                                       │
│                                                                  │
│  主线程 (2001)                                                   │
│    - 处理客户端命令                                              │
│                                                                  │
│  TCP 复制线程                                                     │
│    - Connect master:6483                                         │
│    - Send SYNC_REQUEST(last_offset)                              │
│    - Recv response:                                              │
│        ├─ INCR_SYNC → 接收增量（TCP）                            │
│        └─ FULL_SYNC(base_seq, rdma_port) →                      │
│              - Connect master:rdma_port (2101)                   │
│              - RDMA 接收数据                                     │
│              - 连接关闭 = 完成                                   │
│              - Send FULLSYNC_DONE                                │
│              - Recv 追赶数据                                     │
│    - 进入实时增量                                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📂 文件修改清单

| 文件 | 状态 | 说明 |
|------|------|------|
| `rdma_repl.c` | ✅ 已完成 | 简化为只管理子进程 session |
| `rdma_repl.h` | ✅ 已完成 | 更新接口 |
| `repl_tcp.c` | 🔄 待修改 | 添加 fork 全量同步逻辑 |
| `repl_tcp.h` | 🔄 待修改 | 添加 FULLSYNC_DONE 消息类型 |
| `rdma_repl_master.c` | 🔄 待重写 | 子进程纯 RDMA 数据传输 |
| `rdma_repl_slave.c` | 🔄 待重写 | 简化为单连接接收 |
| `rdma_ctrl_protocol.h` | 🗑️ 可选删除 | 不再需要 RDMA 控制消息 |
| `rdma_ctrl_protocol.c` | 🗑️ 可选删除 | 不再需要 RDMA 控制消息 |

---

## 📚 实现指南文件

1. **01-repl_tcp.md** - TCP 复制线程修改（添加 fork 逻辑）
2. **02-rdma_master.md** - Master 子进程重写（纯 RDMA 数据）
3. **03-rdma_slave.md** - Slave 接收逻辑重写（单连接）
4. **04-protocol.md** - 协议定义和消息格式
5. **05-testing.md** - 测试方案和验证步骤

---

## ⚡ 性能特点

### 优势
- ✅ **简洁**：控制流和数据流分离
- ✅ **可靠**：TCP 保证控制消息顺序
- ✅ **高效**：RDMA 零拷贝，无 ACK 流水线
- ✅ **并发**：多 slave 可并发全量（各自独立子进程）

### 性能估算

| 数据集 | Fork | RDMA 传输 | 解析 | 追赶 | 总计 | 吞吐量 |
|--------|------|----------|------|------|------|--------|
| 100MB  | 5ms  | 10ms     | 50ms | 5ms  | 70ms | 1.4GB/s |
| 1GB    | 5ms  | 100ms    | 500ms| 10ms | 615ms| 1.6GB/s |
| 10GB   | 10ms | 1000ms   | 5s   | 20ms | 6s   | 1.6GB/s |

---

## 🔧 编译和测试

### 编译
```bash
cd /home/xxf/9.1-kvstore/build
make clean
cmake ..
make -j$(nproc)
```

### 测试
```bash
# 清理旧进程
pkill -f kvstore

# 启动 master
./build/kvstore conf/kvstore-master-local.conf &

# 写入测试数据
for i in {1..100}; do
    redis-cli -p 2000 SET key$i value$i
done

# 启动 slave（触发全量同步）
./build/kvstore conf/kvstore-slave-local.conf &

# 等待同步
sleep 5

# 验证数据
redis-cli -p 2001 GET key50
# 应返回 "value50"

# 测试增量
redis-cli -p 2000 SET newkey newvalue
sleep 1
redis-cli -p 2001 GET newkey
# 应返回 "newvalue"
```

---

## 📖 阅读顺序

建议按以下顺序阅读实现指南：

1. **04-protocol.md** - 先了解协议定义
2. **01-repl_tcp.md** - 理解控制流程
3. **02-rdma_master.md** - 实现数据发送
4. **03-rdma_slave.md** - 实现数据接收
5. **05-testing.md** - 验证实现

---

## ⚠️ 注意事项

1. **fork 安全**：子进程必须关闭继承的 FD（客户端连接、TCP socket）
2. **RDMA 资源**：子进程创建独立的 libfabric 资源，不共享父进程
3. **连接关闭**：RDMA 连接关闭 = 全量完成，无需 EOF 消息
4. **错误处理**：子进程异常退出时，父进程通过 SIGCHLD 感知
5. **并发全量**：多个 slave 可同时全量，各自独立子进程

---

**下一步**：阅读 `04-protocol.md` 了解协议定义
