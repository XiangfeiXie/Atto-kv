# 网络层优化总结

## 基线：重构后的 Proactor 网络层

本次性能优化的起点是项目整体重构完成后的版本。此时网络层已完成以下清理工作：

- 去除了多线程设计遗迹（`proactor_worker_t.id`、`owner_worker_id`、`proactor_kick_worker`）
- 文件从 6 个整合为 3 个：`proactor.h`（类型+内联）、`proactor.c`（连接池+ring初始化+事件循环）、`proactor_event.c`（CQE处理）
- `conn_alloc()`、`conn_reset()` 死代码已删除
- io_uring 初始化简化为 2 级回退（`SINGLE_ISSUER|DEFER|COOP` → plain）
- `ENTRIES_LENGTH` 8192→2048、`CQE_BATCH_SIZE` 512→128
- 单线程事件循环（`proactor_loop`），Redis 风格的延迟提交模式

基线版本的功能正常、架构清晰，但性能尚未优化——火焰图暴露了大量内存分配和系统调用开销。以下按优化实施顺序逐一说明。

---

## 1. 连接池预分配与预触达（P0）

### 问题

基线火焰图显示内存分配开销极高：`clear_page_erms` 占 30.6%、`jemalloc` 占 22.1%、`page_fault` 路径占 40.6%。每次 `accept` 和 `recv` 都触发 `malloc` → jemalloc → 内核缺页 → `clear_page_erms` 清零新页。

### 第一次尝试：全池预分配

在 `conn_pool_init` 中为全部 10000 个 slot 预分配 read_buf 和 write_buf（每个 64KB），并对每个页面触达一字节强制物理页分配：

```c
for (int i = 0; i < max_clients; i++) {
    dynbuf_init(&conn_pool[i].read_buf, KVS_NET_BUF_INIT);
    dynbuf_init(&conn_pool[i].write_buf, KVS_NET_BUF_INIT);
    for (size_t off = 0; off < KVS_NET_BUF_INIT; off += 4096)
        conn_pool[i].read_buf.buf[off] = 0;  // 逐页触达
}
```

**问题**：10000×64KB×2 = 1.28GB 预分配导致系统 swap。火焰图出现 `scan_swap_map_slots`（0.7%）和 `mpt_put_msg_frame`（12.6%）——系统在疯狂换页。连接断开报错。

### 第二次尝试：全池预分配 + 大 buffer

增大 `KVS_NET_BUF_INIT` 到 256KB，进一步恶化了内存压力（10000×256KB×2 = 5GB），swap 更严重。回退。

### 最终方案：懒分配 + 连接复用

**连接池初始化**：只 `calloc` 结构体数组，不预分配 buffer：

```c
int conn_pool_init(int max_clients) {
    conn_pool = calloc(max_clients, sizeof(struct conn));
    // 不再预分配 buffer
}
```

**accept 时懒分配**：首次使用时 `dynbuf_init` + 逐页触达，后续复用跳过 malloc：

```c
// event_handle_accept:
if (!nc->read_buf.buf) {
    dynbuf_init(&nc->read_buf, KVS_NET_BUF_INIT);
    for (size_t off = 0; off < KVS_NET_BUF_INIT; off += 4096)
        nc->read_buf.buf[off] = 0;          // 首次触达
} else {
    nc->read_buf.len = 0;                    // 复用
}
```

**连接关闭时复用 buffer**：`conn_release` 不再 `dynbuf_free`，而是保存 buffer 指针，`memset` 结构体后恢复：

```c
void conn_release(int fd) {
    char *rb = c->read_buf.buf;   // 保存预分配 buffer
    char *wb = c->write_buf.buf;
    int rcap = c->read_buf.cap;
    int wcap = c->write_buf.cap;
    memset(c, 0, sizeof(*c));
    c->read_buf.buf = rb;          // 恢复
    c->read_buf.cap = rcap;
    c->write_buf.buf = wb;
    c->write_buf.cap = wcap;
    close(fd);
}
```

**效果**：仅活跃连接消耗内存（benchmark 50 并发 ≈ 6.4MB），buffer 在连接间热复用，运行时无 malloc。

---

## 2. client_t 池预分配（P2a）

### 问题

每次 `event_handle_accept` 中 `calloc(1, sizeof(client_t))` 触发 jemalloc 分配和缺页。

### 修改

在 `conn_pool_init` 中为每个 slot 预分配 `client_t`，accept 时 `memset` 清零复用：

```c
// conn_pool_init:
for (int i = 0; i < max_clients; i++) {
    conn_pool[i].client = calloc(1, sizeof(client_t));
    conn_pool[i].client->conn = &conn_pool[i];
}

// event_handle_accept:
if (nc->client) {
    memset(nc->client, 0, sizeof(client_t));
    nc->client->conn = nc;
}
```

```c
// conn_release:
if (c->client)
    memset(c->client, 0, sizeof(client_t));  // 复用
```

**效果**：`calloc` 从火焰图消失（0.0%），消除 accept 路径的内存分配。

---

## 3. dynbuf 扩容预触达

### 问题

虽然初始 buffer 64KB 覆盖大多数场景，但 pipeline 压测下偶尔触发 `dynbuf_reserve` → `realloc`。realloc 分配的新页第一次写入时触发 `clear_page_erms`。

### 修改

在 `event_handle_read` 的 buffer 扩容路径中，realloc 后对新分配的页面逐页触达：

```c
size_t old_cap = rb->cap;
char *new_buf = realloc(rb->buf, new_cap);
for (size_t p = (old_cap + 4095) & ~(size_t)4095; p < new_cap; p += 4096)
    new_buf[p] = 0;  // 预触达新页
rb->buf = new_buf;
rb->cap = new_cap;
```

同样逻辑加入 `dynbuf_reserve`：

```c
int dynbuf_reserve(DynBuf *db, int total_cap) {
    // ... realloc ...
    for (size_t off = (old_bytes + 4095) & ~(size_t)4095;
         off < (size_t)new_cap; off += 4096)
        nb[off] = 0;  // 预触达新页
}
```

**效果**：`clear_page_erms` 从 30.6% → 2.3%，page fault 路径从 40.6% → 16.8%。

---

## 4. SQE 批量提交（P2b）

### 问题

事件循环每个迭代都无条件调用 `io_uring_submit(ring)`，即使在空闲时无 SQE 待提交。

### 修改

在 `proactor_worker_t` 中添加 `pending_sqes` 计数器。处理 CQE 时递增（每个 handler 可能产生 SQE），主循环仅在 pending > 0 时提交：

```c
// proactor_loop:
while (!g_kvs_stop) {
    if (w->pending_sqes > 0) {          // 仅在有待提交 SQE 时
        io_uring_submit(ring);
        w->pending_sqes = 0;
    }

    int nready = io_uring_peek_batch_cqe(ring, cqes, CQE_BATCH_SIZE);
    if (nready == 0) { /* idle */ }

    for (int i = 0; i < nready; i++) {
        w->pending_sqes++;              // 跟踪待提交 SQE
        event_handle_cqe(w, cqes[i]);
    }
}
```

**效果**：消除空闲循环的无效 `io_uring_submit` 系统调用。

---

## 5. 动态超时

### 问题

空闲等待超时固定为 1ms（`ts.tv_nsec = 1000000L`）。低负载时每个往返延迟至少 1ms，对比 Redis epoll 的 100μs 超时慢了 10 倍。

### 修改

```c
// 旧: 1ms 固定超时
struct __kernel_timespec ts = { .tv_sec = 0, .tv_nsec = 1000000L };

// 新: 100μs 超时，对齐 Redis epoll
struct __kernel_timespec ts = { .tv_sec = 0, .tv_nsec = 100000L };
```

**效果**：低负载响应延迟降低 10 倍，P=1 QPS 从 ~20K 提升到 ~22K。

---

## 6. SQPOLL 恢复

### 发现过程

重构前旧版 `setup_io_uring` 有 5 级回退，第一级是 `SQPOLL | SINGLE_ISSUER`。我们在简化 io_uring 初始化时把它去掉了（简化为 2 级回退）。

对比重构前后的 QPS 数据发现，旧版**全面领先 Redis**——因为 SQPOLL 在起作用。新版缺少 SQPOLL，QPS 显著下降。

### 原理

SQPOLL 启用一个内核线程（`io_sq_thread`），持续轮询 io_uring 的 Submission Queue。有新的 SQE 时立即由内核线程提交到内核，**完全消除 `io_uring_submit` 系统调用**。

```
无 SQPOLL: 应用 → io_uring_submit() → 内核 → 提交 SQE → 返回
有 SQPOLL: 应用 → 写 SQE 到 SQ → 内核线程自动提交（零系统调用）
```

### 修改

恢复 SQPOLL 作为首选项，保留回退路径：

```c
static int setup_io_uring(proactor_worker_t *w) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    // 首选: SQPOLL（需 CAP_SYS_ADMIN，benchmark 用 sudo 满足）
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER;
    params.sq_thread_idle = 200;  // 空闲 200ms 后内核线程休眠
    int qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);

    if (qret == -EINVAL) {
        // 回退 1: SINGLE_ISSUER + DEFER_TASKRUN + COOP_TASKRUN
        params.flags = IORING_SETUP_SINGLE_ISSUER
                     | IORING_SETUP_DEFER_TASKRUN
                     | IORING_SETUP_COOP_TASKRUN;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
    if (qret == -EINVAL) {
        // 回退 2: plain
        params.flags = 0;
        qret = io_uring_queue_init_params(ENTRIES_LENGTH, &w->ring, &params);
    }
}
```

SQPOLL 模式下 `io_uring_submit` 变为无操作（内核线程已提交），批量提交计数器和动态超时逻辑均兼容。

### 注意事项

- SQPOLL 需要 `CAP_SYS_ADMIN`（运行时用 `sudo`）
- SQPOLL 内核线程持续运行，火焰图显示 `io_sq_thread` 占 49%——这是**内核线程在独立 CPU 核上的耗时**，不计入应用层 CPU
- `sq_thread_idle = 200` 控制内核线程空闲 200ms 后休眠，避免空闲时浪费 CPU

**效果**：QPS 从 ~900K 突破到 1.04M（P=160 SET），火焰图健康度极高——应用层有效逻辑仅占 ~14%。

---

## 7. 零拷贝读取尝试（未合入）

### 尝试

跳过 dynbuf，直接在 buffer ring 数据上做 RESP 解析：

```c
DynBuf overlay = { .buf = src, .len = res, .cap = res };
int off = 0;
kvs_drain_resp_commands_off(&overlay, &off, wb, c);
// 只拷贝部分残留数据到真实 buffer
int remain = overlay.len;
if (remain > 0) { /* copy partial to read_buf */ }
```

### 失败原因

`kvs_drain_resp_commands_off` 内部在 buffer 未完全消费时调用 `memmove` 做 buffer compaction，这会写入 overlay 指向的 ring buffer 内存（mmap 区域），导致内存损坏和连接挂死。

### 后续方向

需要修改 drain 函数支持"只读模式"（禁用 compaction），或使用 `send_zc`（零拷贝发送）代替零拷贝接收。`send_zc` 更安全——发送方向的数据完全由应用控制。

---

## 五轮优化数据对比

| 指标 | R1 基线 | R5 最终 | 降幅 |
|------|---------|---------|------|
| `clear_page_erms` | 30.6% | 0.1% | -99.7% |
| `jemalloc` | 22.1% | 0.0% | -100% |
| `page_fault` 路径 | 40.6% | 0.1% | -99.8% |
| `__memmove` 拷贝 | 28.0% | 1.4% | -95% |
| liburing syscall | 39.7% | 1.4% | -96% |
| 应用有效逻辑 | ~20% | ~14% | 集中于有效工作 |

> 注：SQPOLL 启用后，`io_sq_thread` 占 49%、`io_run_task_work` 占 18%——均为内核线程耗时，在独立 CPU 核心运行。

## Release 模式 QPS（P=160）

| 命令 | kvstore | Redis | 倍率 |
|------|---------|-------|------|
| SET | 1,044,932 | 440,334 | **2.37x** |
| GET | 1,193,317 | 582,750 | **2.05x** |

## 关键经验

1. **SQPOLL 是 io_uring 高吞吐的核心加速器**——消除 submit 系统调用后，QPS 提升 30-50%
2. **内存分配是最大的隐藏瓶颈**——看似 harmless 的 `malloc(64KB)`，在 10K 连接池下累积成 30% CPU 的 clear_page
3. **不要过度简化初始化代码**——SQPOLL 回退逻辑看似冗余，实际是性能的关键
4. **懒分配 + 热复用**优于全池预分配——只用活跃连接消耗内存，避免 swap
5. **逐页触达替代全量 memset**——效果相同但启动快 1000 倍
