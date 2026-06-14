# KVStore RDMA vs Redis 全量同步对比与优化建议

## 一、性能异常根因：FI_MSG vs FI_RMA

### 1.1 当前实现问题

**代码证据**：
```c
hints->caps = FI_MSG;  // 使用消息传输，而非RDMA Write/Read
hints->ep_attr->type = FI_EP_MSG;

// 发送方式：类似send/recv的双边操作
fi_send(ep, s, wire_len, fi_mr_desc(mr), 0, &send_ctx[send_idx]);
fi_recv(ep, recv_buf[i], CHUNK_WIRE_SIZE, fi_mr_desc(mr_recv[i]), 0, &recv_ctx[i]);
```

**为什么SoftRoCE比TCP慢**：
- **FI_MSG需要CPU参与**：接收端必须预先post recv buffer，发送时CPU触发，类似传统socket
- **SoftRoCE额外开销**：
  - 用户态→内核态切换（libfabric → verbs → rxe驱动）
  - 内存注册/反注册开销（每次MR操作）
  - UDP封装（RoCE v2协议栈）
  - 模拟RDMA可靠传输（ACK/NACK/重传）
- **TCP直接路径**：libfabric tcp provider直接映射到内核TCP栈，零开销

**正确做法应该是FI_RMA**：
```c
hints->caps = FI_RMA | FI_WRITE;  // 单边RDMA Write
// Slave预先暴露远程内存地址
// Master直接DMA写入，CPU零参与
fi_write(ep, local_buf, len, desc, 0, remote_addr, remote_key, ctx);
```

### 1.2 为什么没用FI_RMA？

从代码看，选择FI_MSG的原因：
1. **流式传输需求**：不知道总大小，无法预分配固定remote buffer
2. **接收端状态机**：需要根据chunk_type（HEADER/DATA/EOF）动态处理
3. **开发简单**：FI_MSG类似socket编程，FI_RMA需要复杂的内存管理

---

## 二、Redis 全量同步可借鉴的优化点

### 2.1 Redis RDB全量同步流程（psync2）

```
Master:
  1. fork() 子进程
  2. 子进程 rdbSave() → 生成完整RDB文件到磁盘
  3. 父进程开启 replication buffer（客户端输出缓冲区）捕获写命令
  4. 子进程完成后，父进程：
     - socket send RDB文件内容（可能数GB）
     - 切换到增量复制，发送 replication buffer
  5. Slave 接收RDB → 清空旧数据 → rdbLoad() → 进入增量模式

优化（Redis 4.0+ Psync2）:
  - Partial resync：断线重连时用 replication backlog 增量恢复
  - Diskless replication：子进程直接序列化到socket，跳过磁盘
```

### 2.2 当前KVStore实现的优势

✅ **已经超越Redis的点**：

1. **流式传输，无磁盘落地**
   - Redis diskless模式仍需等子进程完成才开始发送
   - KVStore父子进程并行：序列化→发送同时进行，延迟更低

2. **RDMA零拷贝潜力**
   - Redis只能用TCP send()，CPU必须拷贝
   - KVStore有RDMA硬件时可实现真DMA

3. **增量+全量双通道**
   - eBPF独立捕获增量，不依赖客户端输出缓冲区
   - 16MB backlog支持partial resync

### 2.3 当前缺失的Redis优化

❌ **应该借鉴的点**：

#### **A. Replication Buffer/Backlog机制不完善**

**Redis做法**：
```c
// 全量同步期间的写命令处理
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    // 1. 写入replication backlog（环形缓冲区，partial resync用）
    feedReplicationBacklog(buf, len);
    
    // 2. 对每个slave：
    for (slave in slaves) {
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            // 全量同步未完成，积累到输出缓冲区
            addReplyToBuffer(slave, buf, len);
        } else {
            // 增量模式，直接发送
            sendToClient(slave);
        }
    }
}
```

**KVStore当前问题**：
```c
#define REWRITE_BUF_SIZE 4096  // 只能缓存4096条命令！

int rdma_repl_capture_write(const kvs_journal_record_t *rec) {
    if (g_rewrite_buf.count >= REWRITE_BUF_SIZE) {
        pthread_mutex_unlock(&g_rewrite_buf.mutex);
        return -1;  // 满了直接失败，数据会丢失！
    }
    // malloc每条记录，碎片化严重
    kvs_journal_record_t *copy = malloc(sizeof(*copy) + rec->len);
    g_rewrite_buf.records[g_rewrite_buf.count++] = copy;
}
```

**改进建议**：
- 改用环形缓冲区（类似Redis backlog），避免malloc
- 容量应该动态增长或关联全量同步速度
- 满了应该阻塞写入或降级（Redis的client-output-buffer-limit机制）

#### **B. 没有"diskless-sync-delay"优化**

**Redis做法**：
```ini
# 多个slave同时请求全量同步时，延迟N秒等待更多slave到达
# 然后一次fork()服务所有slave，节省内存
repl-diskless-sync-delay 5
```

**KVStore当前**：
每个slave连接都独立fork()，并发3个slave = fork()3次

**改进建议**：
```c
// 伪代码
static pthread_mutex_t sync_batch_mtx = PTHREAD_MUTEX_INITIALIZER;
static struct {
    int pending_count;
    time_t first_request_time;
    slave_info_t *slaves[MAX_BATCH];
} sync_batch;

void rdma_master_serve_one(...) {
    pthread_mutex_lock(&sync_batch_mtx);
    sync_batch.slaves[sync_batch.pending_count++] = client_info;
    
    if (sync_batch.pending_count == 1) {
        // 第一个slave，启动延迟定时器
        schedule_timer(DELAY_MS, batch_sync_all);
    } else if (sync_batch.pending_count >= MAX_BATCH) {
        // 达到批量上限，立即执行
        batch_sync_all();
    }
    pthread_mutex_unlock(&sync_batch_mtx);
}

void batch_sync_all() {
    fork();  // 一次fork服务所有slaves
    for (each slave) {
        fi_send(slave->ep, shm_data, ...);  // 多播
    }
}
```

#### **C. RDB格式效率**

**Redis优势**：
- RDB是紧凑的二进制格式，有LZF压缩
- 针对不同类型优化：ziplist/quicklist/intset等压缩编码

**KVStore当前**：
```c
// 序列化格式简单
[klen:4][key][dlen:4][data][expire:8]
```

**改进建议**：
- 借鉴Redis的编码优化：小整数/短字符串内联
- 考虑LZ4/zstd流式压缩（硬件RDMA可能不值得，网络快）
- 类型信息压缩：bitfield编码多个标志位

#### **D. 主从切换时的Partial Resync**

**Redis PSYNC2**（Redis 4.0+）：
- 每个实例维护 `replid` + `repl_offset`
- Slave升级为Master后保留旧replid
- 新Slave可以从新Master继续partial resync，无需全量

**KVStore当前**：
```c
// 注释显示：断线重连时"intentionally re-run full sync"
// On reconnect we intentionally re-run full sync first
```

**改进建议**：
- 实现replid机制，支持主从切换后增量恢复
- 扩大backlog（16MB可能不够高延迟网络）

#### **E. 无损复制选项（wait命令）**

**Redis WAIT**：
```redis
SET key value
WAIT 2 1000  # 等待至少2个slave确认，超时1秒
```

**KVStore当前**：
异步复制，主机不知道slave是否收到

**改进建议**：
```c
// 在RDMA CQ completion后更新确认偏移
typedef struct {
    uint64_t ack_offset;  // slave已确认的偏移量
    pthread_cond_t ack_cond;
} slave_state_t;

// WAIT命令实现
int cmd_wait(int numslaves, int timeout_ms) {
    uint64_t target_offset = current_repl_offset;
    int acked = 0;
    for (each slave) {
        if (slave->ack_offset >= target_offset) acked++;
    }
    if (acked >= numslaves) return acked;
    
    // 阻塞等待
    pthread_cond_timedwait(&repl_ack_cond, timeout_ms);
    // 重新统计
}
```

---

## 三、针对性能瓶颈的架构优化

### 3.1 升级到FI_RMA单边RDMA

**方案一：预分配固定大小remote buffer**
```c
// Slave预分配4GB接收缓冲区
char *remote_buf = malloc(4GB);
fi_mr_reg(domain, remote_buf, 4GB, FI_REMOTE_WRITE, &mr);
uint64_t remote_key = fi_mr_key(mr);

// 通过带外通道告知Master（如TCP控制连接）
send_to_master(&remote_addr, &remote_key);

// Master端：循环RDMA Write
size_t offset = 0;
while (has_data) {
    fi_write(ep, local_chunk, chunk_size, desc, 0,
             remote_addr + offset, remote_key, &ctx);
    offset += chunk_size;
}

// Slave端：轮询内存标志位检测完成
volatile uint32_t *done_flag = (uint32_t*)(remote_buf + 4GB - 4);
while (*done_flag != MAGIC_DONE) { cpu_relax(); }
```

**优势**：
- Master CPU只需发起DMA，不等待
- Slave CPU可以同时处理其他任务
- SoftRoCE也能受益（减少协议开销）

**劣势**：
- 需要预估buffer大小
- 流式解析变复杂（需要独立线程轮询）

**方案二：混合模式**
- 控制通道用FI_MSG（HEADER/EOF）
- 数据传输用FI_RMA（DATA chunks）
- 兼顾灵活性和性能

### 3.2 去掉fork()，改用RCU快照

**Redis为什么必须fork()**：
- C语言无GC，COW是最简单的快照方式
- 全局字典无版本控制

**KVStore可以的替代方案**：
```c
// 基于epoch的快照
typedef struct {
    uint64_t epoch;
    kv_entry_t *entry;
} versioned_entry_t;

// 全量同步时
uint64_t snap_epoch = atomic_fetch_add(&global_epoch, 1);
pthread_t tid;
pthread_create(&tid, NULL, serialize_with_epoch, (void*)snap_epoch);

// 序列化线程
void* serialize_with_epoch(void *arg) {
    uint64_t my_epoch = (uint64_t)arg;
    for (each entry in dict) {
        if (entry->epoch <= my_epoch) {
            // 这个版本对我可见
            serialize(entry);
        }
    }
}
```

**优势**：
- 无fork开销（页表拷贝）
- 无eventfd/epoll同步开销
- 可以复用io_uring的worker线程

**劣势**：
- 需要MVCC机制支持
- 代码改动大

### 3.3 多路复用：一次fork服务多个slave

```c
struct sync_session {
    struct fid_ep *eps[MAX_SLAVES];
    int slave_count;
    pthread_t send_threads[MAX_SLAVES];
};

// 父进程spawn N个发送线程，共享同一shm ring
for (int i = 0; i < slave_count; i++) {
    pthread_create(&threads[i], NULL, send_worker, 
                   &(struct send_arg){.ep = eps[i], .shm = shm});
}
```

---

## 四、实测性能预估

### 4.1 如果改用FI_RMA

**理论分析**：
- 当前FI_MSG：28.58 MB/s（虚拟机TCP）
- SoftRoCE开销消除：预计 **40-50 MB/s**（接近物理网卡极限）
- 真实RDMA硬件（100Gbps）：**>1 GB/s**

### 4.2 如果去掉fork()

**瓶颈转移**：
- 当前：进程同步 + 网络
- 优化后：纯网络（序列化可并行）
- **延迟降低50%**（无fork时间）

### 4.3 综合优化后

| 优化项 | 当前 | 优化后 | 提升 |
|--------|------|--------|------|
| 吞吐量（虚拟机） | 28.58 MB/s | **50 MB/s** | 1.75x |
| 吞吐量（RDMA硬件） | N/A | **1+ GB/s** | 35x+ |
| 延迟（RTT） | fork时间+传输 | 纯传输 | -50% |
| CPU占用（Master） | 高（epoll轮询） | 极低（DMA） | -80% |
| 并发slave能力 | 1次fork/slave | 批量fork | 3-5x |

---

## 五、优先级建议

### 🔴 高优先级（性价比高，立即可做）

#### **1. 修复rewrite buffer溢出bug**
```c
// 当前：固定4096条，满了返回-1（数据丢失）
#define REWRITE_BUF_SIZE 4096

// 改进：环形缓冲区
typedef struct {
    char *data;
    size_t capacity;  // 初始16MB，可动态扩展
    size_t write_pos;
    size_t read_pos;
    pthread_mutex_t lock;
} ring_buffer_t;

int rewrite_buffer_append(const char *cmd, size_t len) {
    if (ring_full(&g_rewrite_ring)) {
        // 选项1：阻塞写入（等待全量同步完成）
        // 选项2：动态扩容（最多扩到256MB）
        // 选项3：记录溢出，全量同步后强制重新同步
        if (!ring_try_expand(&g_rewrite_ring)) {
            log_warn("Rewrite buffer overflow");
            mark_slave_need_fullresync();
            return -1;
        }
    }
    ring_write(&g_rewrite_ring, cmd, len);
    return 0;
}
```

**影响**：防止高写入负载下的数据不一致，代码改动小（~200行）

#### **2. 实现批量同步（diskless-sync-delay）**

一次fork()服务多个slave，节省内存66%+，代码改动中等（~500行）

#### **3. 扩大共享环形缓冲区**
```c
// 当前：16 slots × 1MB = 16MB
#define SHM_RING_SIZE 16

// 改进：64 slots × 1MB = 64MB（可配置）
#define SHM_RING_SIZE 64
```

**影响**：适配真实RDMA硬件，避免管道阻塞

### 🟡 中优先级（架构改进）

**4. 升级到FI_RMA混合模式**
- 虚拟机1.5-1.75x，真实硬件30x+
- 代码改动大（~2000行）

**5. 增强partial resync（replid机制）**
- 支持主从切换后增量恢复
- 代码改动中（~800行）

**6. 实现WAIT命令**
- 同步复制语义
- 代码改动中（~600行）

### 🟢 低优先级

**7. 去fork化（MVCC）** - 需引擎层支持，改动巨大

**8. 压缩传输** - 仅对慢速网络有意义

---

## 六、总结

### 核心发现

**为什么verbs比TCP慢3.3倍？**
- ❌ 使用FI_MSG双边操作，CPU全程参与
- ❌ SoftRoCE模拟开销 > 原生TCP
- ✅ 解决：升级到FI_RMA单边RDMA Write

**Redis最值得借鉴的3点**：
1. ⭐⭐⭐⭐⭐ **Rewrite buffer机制** - 环形缓冲区（必做，防数据丢失）
2. ⭐⭐⭐⭐ **批量同步** - 多slave合并fork（性价比极高）
3. ⭐⭐⭐ **FI_RMA升级** - 真正利用RDMA硬件（最大收益）

**当前优势保持**：
- ✅ 流式传输（无磁盘）
- ✅ eBPF增量捕获
- ✅ fork COW快照简单可靠

### 性能提升路线图

**短期（1-2周）**：修复buffer溢出 + 扩大ring + 批量同步 → 稳定性 + 内存节省

**中期（1-2月）**：FI_RMA升级 → 虚拟机1.75x，真实硬件30x+

**长期（3-6月）**：MVCC快照 → 延迟-50%

### 投入产出比排序

1. 🏆 Rewrite buffer修复（1天，必做）
2. 🥈 批量同步（3天，性价比最高）
3. 🥉 扩大ring buffer（0.5天，简单有效）
4. FI_RMA升级（2周，最大收益）
5. Partial resync（1周，可选）
6. WAIT命令（1周，可选）

**建议：先修复bug和低成本优化，再投入FI_RMA架构升级。**
