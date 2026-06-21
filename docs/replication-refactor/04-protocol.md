# 协议定义和消息格式

## 📡 通信协议

### 协议分层

```
┌─────────────────────────────────────────────────────────┐
│ 控制平面 (TCP 6483)                                      │
│  - 同步请求/响应                                         │
│  - 心跳保活                                              │
│  - 全量同步协调                                          │
│  - 增量数据传输                                          │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ 数据平面 (RDMA 2101)                                     │
│  - 仅用于全量同步                                        │
│  - 纯数据传输，无控制消息                                │
│  - 子进程临时创建，使用完关闭                            │
└─────────────────────────────────────────────────────────┘
```

---

## 📦 TCP 控制消息

### 消息头结构

```c
/* 所有 TCP 控制消息的通用头部 */
struct ctrl_msg_header {
    uint32_t magic;          /* 0x4B565343 "KVSC" */
    uint32_t type;           /* ctrl_msg_type_t */
    uint64_t timestamp;      /* Unix timestamp (ms) */
} __attribute__((packed));
```

### 消息类型

```c
typedef enum {
    CTRL_MSG_SYNC_REQUEST = 1,    /* Slave → Master: 请求同步 */
    CTRL_MSG_SYNC_RESPONSE = 2,   /* Master → Slave: 同步响应 */
    CTRL_MSG_PING = 3,            /* Master → Slave: 心跳 */
    CTRL_MSG_PONG = 4,            /* Slave → Master: 心跳响应 */
    CTRL_MSG_DATA_FRAME = 5,      /* Master → Slave: 增量数据帧 */
    CTRL_MSG_FULLSYNC_DONE = 6,   /* Slave → Master: 全量完成 */
} ctrl_msg_type_t;
```

---

## 🔄 同步流程消息

### 1. SYNC_REQUEST (Slave → Master)

```c
struct sync_request_msg {
    struct ctrl_msg_header hdr;  /* type = CTRL_MSG_SYNC_REQUEST */
    uint64_t last_offset;        /* Slave 的最后 offset */
} __attribute__((packed));
```

**语义**：
- `last_offset = 0`：首次连接，需要全量
- `last_offset > 0`：断线重连，请求从该 offset 继续

---

### 2. SYNC_RESPONSE (Master → Slave)

```c
#define SYNC_STATUS_INCR 0        /* 增量同步 */
#define SYNC_STATUS_FULL 1        /* 全量同步 */

struct sync_response_msg {
    struct ctrl_msg_header hdr;  /* type = CTRL_MSG_SYNC_RESPONSE */
    uint32_t status;             /* SYNC_STATUS_INCR / SYNC_STATUS_FULL */
    uint64_t start_offset;       /* 起始 offset（增量时有效） */
    uint64_t data_len;           /* 追赶数据长度（增量时有效） */
    
    /* 全量同步时额外字段 */
    uint64_t base_seq;           /* COW 快照的 offset */
    uint16_t rdma_port;          /* RDMA 数据端口（通常 2101） */
    uint16_t padding;
} __attribute__((packed));
```

---

## 🚀 RDMA 数据格式

### 全量同步数据流

```
┌──────────────────────────────────────────────────────────┐
│ Header (24 字节，第一个 batch 的开头)                    │
├──────────────────────────────────────────────────────────┤
│ magic (4B)      : 0x52444232 "RDB2"                      │
│ version (4B)    : 1                                      │
│ db_num (4B)     : 0                                      │
│ key_count (4B)  : 总 entry 数量                          │
│ base_seq (8B)   : COW 快照的 offset                      │
└──────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────┐
│ Entry 1                                                  │
├──────────────────────────────────────────────────────────┤
│ key_len (4B)    : 网络字节序                             │
│ key (N bytes)   : 原始字节                               │
│ val_len (4B)    : 网络字节序                             │
│ value (M bytes) : 原始字节                               │
│ expire (8B)     : Unix timestamp (ms)，网络字节序        │
└──────────────────────────────────────────────────────────┘
```

**下一步**：阅读 `01-repl_tcp.md` 了解 TCP 复制线程的实现
