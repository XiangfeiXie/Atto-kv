# KVStore 主从同步全流程详解

本文档按时间顺序梳理主机与从机从启动、建立连接、全量同步（RDMA/libfabric）到增量同步（eBPF + TCP）的完整代码执行路径。

---

## 一、总体流程概览

```
时间线 ──────────────────────────────────────────────────────────────────────►

主机 main()
  │
  ├─ 加载持久化数据 (engine_reload)
  ├─ 启动 repl_stream (TCP+eBPF 增量同步基础设施，后台线程监听 6483)
  ├─ 启动 rdma_repl_master (RDMA 全量同步监听器，后台线程监听 2100)
  ├─ 启动 expire 线程
  └─ 进入主事件循环 (proactor/reactor/ntyco 处理客户端请求)
        │
        │  ← eBPF 开始捕获写命令 → 加入 backlog → 等 slave 连上来
        │
从机 main()
  │
  ├─ 加载持久化数据
  ├─ rdma_repl_slave_run() ─────── fi_connect ──────► 主机收到 FI_CONNREQ
  │                                                        │
  │                                                   fork() + COW
  │                                                   ┌─────────────┐
  │                                                   │ 子: 序列化  │
  │   ◄──────── fi_recv (1MB chunks streaming) ───────│ 父: RDMA发送 │
  │                                                   └─────────────┘
  │   收到 EOF → rdb_rebuild_all_encodings()
  │   xdp_slave_incremental_ready(base_seq)
  │     └─ repl_stream_start_slave() → TCP connect 6483
  │                                         │
  │   发送 SYNC request(last_offset) ──────►│ master_handle_sync_request()
  │                                         │   → partial resync 或 FULLSYNC
  │   ◄─── 接收 catchup data ──────────────│
  │                                         │
  │   ◄─── 持续接收 [len:4][data] 帧 ──────│ eBPF ringbuf → backlog → TCP send
  │   send 到本地 kvstore                   │
  │                                         │
  └─ 进入主事件循环 (port 2001 对外服务)    └─ 主机持续服务 (port 2000)
```

---

## 二、主机启动阶段

### 2.1 `main()` 初始化顺序

**文件**: [src/core/kvstore.c](src/core/kvstore.c), 函数 `main()`

```
main():
  1. signal(SIGPIPE, SIG_IGN)                  # 忽略 SIGPIPE
  2. sigaction(SIGTERM/SIGINT, sig_shutdown)   # 注册退出信号
  3. setlinebuf(stdout/stderr)                 # 行缓冲输出
  4. g_kvs_start_time_ms = get_time_now_real() # 记录启动时间
  5. kvs_tsc_calibrate()                       # TSC 时钟校准
  6. persist_set_basedir(argv[0])              # 设置持久化根目录
  7. config_load(conf_path)                    # 加载配置文件
  8. config_print()                            # 打印配置信息
  9. init_kvengine()                           # 初始化字典引擎 + 阻塞子系统
 10. engine_reload()                           # 加载 AOF/RDB 持久化数据
 11. kvs_journal_init()                        # 初始化 Journal
 12-13. 启动持久化 (WAL + AOF async)
 14. repl_stream_start_master()  ◄── 启动增量同步
 15. rdma_repl_master_start()    ◄── 启动全量同步监听器
 16. kvs_start_expire_thread()                 # 启动后台过期线程
 17. proactor_start()                          # 进入主事件循环
```

### 2.2 增量同步基础设施初始化

**调用链**: `main()` → `repl_stream_start_master()` → `repl_tcp_init(MASTER)`

**文件**: [src/repl/repl_stream.c](src/repl/repl_stream.c):21-56

```c
repl_stream_start_master():
    repl_incr_init()                          // 分配 64MB 环形缓冲区
        → malloc(64MB)
        → master_offset = 0, slave_offset = 0
        → pthread_mutex_init()

    repl_tcp_init(REPL_ROLE_MASTER):
        tcp_listen(incr_repl_port=6483)       // 创建 TCP 监听 socket
            → socket() → setsockopt(SO_REUSEADDR) → bind() → listen(5)
        master_load_bpf()                     // 加载 eBPF 过滤器
            → bpf_object__open_file("repl_filter.bpf.o")
            → bpf_object__load()
            → 配置 repl_config map (写入监听端口)
            → bpf_prog_attach(sockops, cgroup_fd, BPF_CGROUP_SOCK_OPS)
            → bpf_prog_attach(parser, sockmap_fd, BPF_SK_SKB_STREAM_PARSER)
            → bpf_prog_attach(verdict, sockmap_fd, BPF_SK_SKB_STREAM_VERDICT)
            → ring_buffer__new(ringbuf_fd, master_on_data, ...)
        pthread_create(master_thread_func)    // 启动后台主线程
```

**此时主机状态**:
- TCP 端口 6483 处于 LISTEN 状态
- eBPF 过滤器已 attach 到 cgroup socket 层，开始监控目标端口的写命令
- 64MB backlog ring buffer 已就绪

### 2.3 RDMA 全量同步监听器初始化

**调用链**: `main()` → `rdma_repl_master_start()` → `rdma_master_thread()`

**文件**: [src/repl/rdma_repl.c](src/repl/rdma_repl.c):853-950

```c
rdma_repl_master_start():
    pthread_create(rdma_master_thread)         // detach 线程
    pthread_detach()

rdma_master_thread():
    rdma_build_hints()                         // FI_MSG, FI_EP_MSG
    fi_getinfo(rdma_listen_ip, rdma_listen_port, FI_SOURCE, hints)
    fi_fabric() → fi_eq_open() → fi_passive_ep()
    fi_pep_bind() → fi_listen()

    // 主 accept 循环
    for (;;) {
        fi_eq_sread(eq, &event, entry, 1000)  // 1秒超时，可被信号中断
        if (event == FI_CONNREQ):
            rdma_master_serve_one(fabric, eq, client_info)
    }
```

**此时主机状态**:
- RDMA passive endpoint 在端口 2100 监听
- 等待从机发起 `fi_connect()`

---

## 三、从机启动阶段

### 3.1 `main()` 初始化顺序

从机的 `main()` 流程与主机相同，但在步骤 15 调用 `rdma_repl_slave_run()`：

**文件**: [src/core/kvstore.c](src/core/kvstore.c):1662-1681

```c
main():
    // ... 步骤 1-13 与主机相同 ...
    // 步骤 14: 从机不启动 repl_stream_start_master
    // 步骤 15: RDMA 全量同步
    if (rdma_sync_enabled && rdma_role == ROLE_SLAVE) {
        for (fullsync_attempt = 0; fullsync_attempt < 5; fullsync_attempt++) {
            rdma_repl_slave_run()              // 全量同步
            if (!rdma_repl_slave_need_fullresync())
                break;                         // 成功，退出循环
        }
        // 随后进入步骤 16-17 (expire线程 + 主事件循环)
    }
```

### 3.2 从机发起 RDMA 全量同步连接

**文件**: [src/repl/rdma_repl.c](src/repl/rdma_repl.c):991-1253

```c
rdma_repl_slave_run():
    // Phase 1: 建立 libfabric 连接
    rdma_build_hints()
    fi_getinfo(rdma_master_ip, rdma_master_port, 0, hints)  // 查询主机地址
    fi_fabric() → fi_eq_open() → fi_domain()
    fi_cq_open()                              // 完成队列
    fi_endpoint() → fi_ep_bind(eq) → fi_ep_bind(cq) → fi_enable()

    // Phase 2: 预分配接收缓冲区 (4 × 1MB+5B)
    for (i = 0; i < 4; i++):
        recv_buf[i] = malloc(CHUNK_WIRE_SIZE)  // 1MB + 5B header
        rdma_mr_reg(domain, recv_buf[i], FI_RECV)  // 注册 MR
        fi_recv(ep, recv_buf[i], CHUNK_WIRE_SIZE)  // 预提交 recv

    // Phase 3: 发起连接
    fi_connect(ep, dest_addr)                 // 连接到主机 2100
    rdma_wait_eq(eq, &event)                  // 等待 FI_CONNECTED
```

---

## 四、全量同步：主机侧

### 4.1 连接建立

**文件**: [src/repl/rdma_repl.c](src/repl/rdma_repl.c):648-847, `rdma_master_serve_one()`

当 `rdma_master_thread` 的 `fi_eq_sread()` 收到 `FI_CONNREQ` 事件后：

```c
rdma_master_serve_one(fabric, eq, client_info):
    // Step 1: 构建 libfabric 资源
    fi_domain(fabric, client_info, &domain)
    fi_cq_open(domain, &cq)
    fi_endpoint(domain, client_info, &ep)
    fi_ep_bind(ep, &eq->fid, 0)              // 绑定事件队列
    fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV)  // 绑定完成队列
    fi_enable(ep)

    // Step 2: 接受连接
    fi_accept(ep, NULL, 0)                    // 接受 RDMA 连接
    rdma_wait_eq(eq, &event, &entry)          // 等待 FI_CONNECTED
```

### 4.2 创建共享内存与 fork

```c
    // Step 3: 开启 rewrite buffer
    rewrite_buffer_enable()
    // 全量同步期间，主机收到的写命令被捕获到 rewrite buffer (最多4096条)
    // 由 kvs_write_journal_append() → rdma_repl_capture_write() 写入

    // Step 4: 创建共享内存环 (memfd + mmap)
    shm_fd = memfd_create("kvstore_fullsync")
    ftruncate(shm_fd, sizeof(shm_ring_t))     // 16 slots × ~1MB
    shm = mmap(MAP_SHARED)                    // 父子进程共享
    memset(shm, 0)                             // 所有 slot state = SLOT_EMPTY

    // Step 5: 注册内存区域 (必须在 fork 之前)
    rdma_mr_reg(domain, shm, shm_size, FI_SEND, &mr_shm)

    // Step 6: 创建 eventfd 对
    child_evt_fd  = eventfd(0, EFD_NONBLOCK)  // 子写父读：通知有新数据
    parent_evt_fd = eventfd(0, 0)             // 父写子读：通知子进程退出

    // Step 7: fork()
    kv_store_lock_for_snapshot()               // 获取快照一致性锁
    child_pid = fork()
    kv_store_unlock_snapshot()
```

### 4.3 子进程：序列化数据

```c
    if (child_pid == 0) {
        // ===================== 子进程 =====================
        child_close_extra_fds(child_evt_fd, parent_evt_fd)
        // 关闭从父进程继承的所有 libfabric FD，子进程只做序列化

        child_serialize_loop(shm, child_evt_fd, parent_evt_fd):
            // 等待父进程写入 base_seq
            eventfd_read(parent_evt_fd)        // 阻塞等待
            base_seq = shm->base_seq

            // Step A: 获取第一个 slot，写入 Header(24B)
            s = child_acquire_slot(shm)         // 等待 SLOT_EMPTY
            // 写入: [magic:4][version:4][db_num:4][key_count:4][base_seq:8]
            wp = s->data + 24

            // Step B: 遍历所有 key，序列化到环
            kv_store_iter_init(&it)
            while (entry = kv_store_iter_next(&it)):
                n = serialize_entry_inline(wp, room, entry)
                // 格式: [klen:4][key][dlen:4][data][expire:8]
                if (n < 0):  // 剩余空间不足
                    s->chunk_len = wp - s->data
                    s->chunk_type = CHUNK_TYPE_DATA
                    s->state = SLOT_READY
                    eventfd_write(child_evt_fd, 1)   // 通知父进程
                    s = child_acquire_slot(shm)       // 获取新 slot
                    wp = s->data
                    // 重试序列化
                wp += n

            // Step C: 发送最后一个有数据的 slot
            if (wp > s->data):
                s->state = SLOT_READY
                eventfd_write(child_evt_fd, 1)

            // Step D: 发送 EOF chunk
            eof_s = child_acquire_slot(shm)
            eof_s->chunk_len = 0
            eof_s->chunk_type = CHUNK_TYPE_EOF
            eof_s->state = SLOT_READY
            eventfd_write(child_evt_fd, 1)

            // Step E: 等待父进程通知退出
            while (!shm->child_should_exit):
                usleep(1000)
            _exit(0)
    }
```

### 4.4 父进程：RDMA 发送线程

```c
    // ===================== 父进程 =====================
    session_add(child_pid)

    // Step A: 写入 base_seq 并通知子进程
    post_fork_seq = 0
    shm->base_seq = post_fork_seq
    eventfd_write(parent_evt_fd, 1)            // 唤醒子进程开始序列化

    // Step B: 创建发送线程
    arg = { ep, cq, mr_shm, shm, child_evt_fd, parent_evt_fd }
    pthread_create(&tid, parent_send_thread, &arg)

    parent_send_thread():
        epfd = epoll_create1()
        epoll_ctl(epfd, EPOLL_CTL_ADD, child_evt_fd)  // 监听子进程信号

        while (1):
            // 1. epoll_wait 监听子进程信号
            epoll_wait(epfd, timeout = inflight>0 ? 1ms : -1)
            eventfd_read(child_evt_fd)           // 消费事件

            // 2. 扫描环，发送 READY slots
            for (round = 0; round < 16; round++):
                s = &shm->slots[send_idx]
                if (s->state == SLOT_READY):
                    wire_len = 5 + s->chunk_len  // [len:4][type:1][data:N]
                    fi_send(ep, s, wire_len, fi_mr_desc(mr_shm), 0, ctx)
                    // 零拷贝发送：直接发送共享内存中的数据
                    s->state = SLOT_INFLIGHT
                    inflight++
                    if (s->chunk_type == CHUNK_TYPE_EOF):
                        eof_sent = 1
                send_idx = (send_idx + 1) % 16

            // 3. 收割 CQ 完成事件
            while (fi_cq_read(cq, &ce, 1) > 0):
                inflight--
                通过 ce.op_context 找到对应 slot
                shm->slots[slot_idx].state = SLOT_EMPTY  // 归还给子进程

            // 4. 检查是否完成
            if (eof_sent && inflight == 0 && 所有slot都是EMPTY):
                break

        // 5. 全量同步完成，处理 rewrite buffer
        rewrite_buffer_disable()
        rewrite_buffer_replay()
        // 将全量同步期间捕获的增量写命令回放到本地引擎
        // 通过 kvs_dispatch() 逐条执行

        // 6. 通知子进程退出
        shm->child_should_exit = 1
        eventfd_write(parent_evt_fd, 1)

    // Step C: 等待发送线程结束
    pthread_join(tid)

    // Step D: 回收子进程
    waitpid(child_pid)
    session_remove(child_pid)

    // Step E: 释放 RDMA 连接
    fi_shutdown(ep)
    fi_close(ep), fi_close(cq), fi_close(domain)
```

**关键设计要点**:

| 组件 | 作用 |
|------|------|
| `memfd + MAP_SHARED` | 父子进程共享的环形缓冲区，避免管道拷贝 |
| `eventfd` | 轻量级进程间通知（子→父：数据就绪；父→子：退出信号） |
| `fork() before MR registration` | MR 在 fork 前注册，MAP_SHARED 保证物理页不变 |
| `COW snapshot` | fork() 时刻子进程获得引擎的 COW 快照，不需拷贝全量数据 |
| `rewrite buffer` | 全量同步期间捕获增量写，完成后回放，保证不丢数据 |
| 零拷贝发送 | `fi_send` 直接从 `mmap` 共享内存发送，无额外拷贝 |

---

## 五、全量同步：从机侧

### 5.1 接收流式数据

**文件**: [src/repl/rdma_repl.c](src/repl/rdma_repl.c):1083-1253

```c
    // 连接已建立，4 个 recv buffer 已预提交
    // 进入接收循环
    while (!eof_received && pending > 0):
        // Step 1: 等待 RDMA 数据到达
        fi_cq_sread(cq, &ce, 1000)            // 1秒超时，可被信号中断
        pending--

        // Step 2: 通过 op_context 找到对应的 recv buffer
        buf_idx = find_buf_by_context(ce.op_context)
        buf     = recv_buf[buf_idx]
        msg_len = ce.len

        // Step 3: 解析 chunk header
        chunk_len  = *(uint32_t*)buf           // 前 4 字节
        chunk_type = buf[4]                    // 第 5 字节
        payload    = buf + 5                   // 数据部分

        switch (chunk_type):
            case CHUNK_TYPE_DATA:
                if (!header_parsed):
                    // 第一个数据块包含 24 字节 header + entries
                    key_count = rdb_load_stream_begin(payload, 16)
                        // 解析 [magic][version][db_num][key_count]
                        // 清空引擎，准备加载
                    base_seq = *(uint64_t*)(payload + 16)
                    g_slave_base_seq = base_seq

                    // 加载 header 之后的 entry 数据
                    if (chunk_len > 24):
                        n = rdb_load_stream_append(payload + 24, chunk_len - 24)
                        total_keys += n
                    header_parsed = 1
                else:
                    // 后续 chunk 全是 entry 数据
                    n = rdb_load_stream_append(payload, chunk_len)
                    total_keys += n
                break

            case CHUNK_TYPE_EOF:
                eof_received = 1
                rdb_rebuild_all_encodings()    // 重建 PACKED→DICT 编码
                break

        // Step 4: 重新提交 recv buffer
        fi_recv(ep, buf, CHUNK_WIRE_SIZE)      // 复用同一 buffer
        pending++
```

### 5.2 流式反序列化

**文件**: [src/persist/persistence.c] 中的 `rdb_load_stream_begin()` 和 `rdb_load_stream_append()`

```c
rdb_load_stream_begin(buf, len):
    // 解析 header: [magic:4][version:4][db_num:4][key_count:4]
    // 验证 magic = 0x524442 ("RDB2")
    g_in_reload = 1                           // 标记 reload 模式
    kvs_flush_all()                           // 清空现有数据
    return key_count

rdb_load_stream_append(buf, len):
    // 逐个解析 entry:
    //   [klen:4][key][dlen:4][data][expire:8]
    while (offset < len):
        klen = *(uint32_t*)(buf + offset)
        key  = buf + offset + 4
        dlen = *(uint32_t*)(buf + offset + 4 + klen)
        data = buf + offset + 4 + klen + 4
        expire = *(int64_t*)(buf + offset + 4 + klen + 4 + dlen)

        // 插入字典
        hash = kv_hash(key, klen)
        entry = dict_add(dict, key, klen, data, dlen, hash)
        if (entry): entry->expire = expire

        offset += 4 + klen + 4 + dlen + 8
    return entry_count
```

### 5.3 切换到增量同步

```c
    // RDMA 连接关闭，全量同步完成
    fi_shutdown(ep)
    释放所有 libfabric 资源

    // 启动 XDP/AF_XDP 增量流
    xdp_slave_incremental_ready(session_id=0, epoch=0, base_seq):
        g_slave_base_seq = base_seq
        repl_stream_start_slave(base_seq)
```

---

## 六、增量同步：基础设施初始化

### 6.1 从机启动 TCP 增量同步

**调用链**: `xdp_slave_incremental_ready()` → `repl_stream_start_slave()` → `repl_tcp_init(SLAVE)`

**文件**: [src/repl/repl_stream.c](src/repl/repl_stream.c):58-96

```c
repl_stream_start_slave(base_seq):
    repl_incr_init()                           // 分配 64MB 环形缓冲区
    repl_tcp_init(REPL_ROLE_SLAVE):
        // 设置 TCP 参数
        cfg.enable_nodelay = 1                 // 禁用 Nagle
        cfg.sndbuf_size = 256KB
        cfg.rcvbuf_size = 256KB
        cfg.master_ip = rdma_master_ip
        cfg.repl_port = incr_repl_master_port  // 6483

        pthread_create(slave_thread_func)      // 启动从机 TCP 线程
```

### 6.2 从机 TCP 线程主循环

**文件**: [src/repl/repl_tcp.c](src/repl/repl_tcp.c):772-880

```c
slave_thread_func():
    last_saved_offset = repl_incr_load_offset()  // 从 /tmp/repl_offset 读取

    while (g_repl.running):
        // ===== 连接阶段 =====
        if (peer_fd < 0):
            tcp_connect(master_ip, repl_port=6483)
                → socket() → tcp_set_options(TCP_NODELAY, 256KB buf)
                → connect()
            // TCP 连接已建立

            // 发送同步请求
            sync_result = slave_send_sync_request(peer_fd, last_saved_offset)
                // 发送:
                //   sync_request {
                //     magic: "SYNC" (0x53594E43)
                //     last_offset: <上次保存的复制偏移>
                //   }
                //
                // 接收 sync_response:
                //   status: SYNC_STATUS_OK or FULLSYNC_REQUIRED
                //   start_offset, data_len
                //
                // 如果 OK 且有 catchup 数据:
                //   接收 catchup 数据
                //   逐帧解析 [len:4][data:N]
                //   slave_apply_command() → send 到本地 kvstore

            if (sync_result == -2):            // FULLSYNC_REQUIRED
                close(peer_fd)
                // 需要重新 RDMA 全量同步，由外层循环处理
                sleep(10)
                continue
            else if (sync_result != 0):
                sleep(RECONNECT_INTERVAL=3)    // 重连前等 3 秒
                continue

        // ===== 正常接收阶段 =====
        // 1. 接收帧头 (4 字节长度)
        tcp_recv_exact(peer_fd, &frame_len, 4)
        frame_len = ntohl(frame_len)

        // 2. 接收帧数据
        tcp_recv_exact(peer_fd, frame_buf, frame_len)

        // 3. 应用到本地 kvstore
        slave_apply_command(frame_buf, frame_len)
            → connect(localhost, kv_port)      // 连接到本机 kvstore
            → send(local_fd, data, len)        // 把 RESP 命令发给本地服务
            // 本地 kvstore 解析执行，同时触发 kvs_write_journal_append

        // 4. 定期确认 offset
        if (frames_recv % 100 == 0):
            repl_incr_ack_offset(slave_offset) // 更新内存中的 slave_offset

        if (frames_recv % 1000 == 0):
            repl_incr_save_offset(slave_offset)// 持久化到 /tmp/repl_offset
```

---

## 七、增量同步：主机侧运行时

### 7.1 eBPF 数据捕获

**文件**: [src/repl/repl_filter.bpf.c](src/repl/repl_filter.bpf.c) (内核态) + [src/repl/repl_tcp.c](src/repl/repl_tcp.c):375-435 (用户态)

**内核态 — eBPF sockops 程序** (`repl_filter_sockops`):
```
当客户端连接到被监控端口 (2000) 时:
    BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB:
        cookie = bpf_get_socket_cookie(skops)
        bpf_sock_hash_update(skops, &repl_sockmap, &cookie, BPF_ANY)
        // 将被动建立的连接 cookie 加入 sockmap
```

**内核态 — eBPF stream_verdict 程序** (`repl_filter_verdict`):
```
每次 TCP 报文到达:
    1. bpf_skb_load_bytes(skb, 0, &first_byte, 1)
    2. if (first_byte != '*'): return SK_PASS     // 非 RESP 协议，跳过
    3. bpf_skb_load_bytes(skb, 0, data, min(64, len))
    4. is_write_command(data, len):
        检查前 3 个字符:
          SET, DEL → 是
          LPUSH, LPOP, RPUSH, RPOP → 是
          HSET, HDEL, SADD, SREM, ZADD, ZREM → 是
          INCR, DECR, APPEND, MSET, FLUSHALL → 是
          GET, HGET, LRANGE... → 否（读命令，不复制）
    5. 如果是写命令:
        e = bpf_ringbuf_reserve(&repl_ringbuf)
        bpf_skb_load_bytes(skb, 0, e->data, len)
        // 将完整的 RESP 命令数据写入 ringbuf
        bpf_ringbuf_submit(e, 0)
    6. return SK_PASS  // 始终放行，不影响正常业务
```

**用户态 — ringbuf 消费者** (`master_on_data`):
```c
master_on_data(ctx, data, len):
    e = (repl_observe_event *)data

    // 1. 获取/创建该连接的 TCP 重组上下文
    rctx = get_or_create_ctx(e->sock_cookie)

    // 2. 追加数据到重组缓冲区
    memcpy(rctx->buf + rctx->buf_len, e->data, e->len)
    rctx->buf_len += e->len

    // 3. 提取完整的 RESP 命令
    while (rctx->buf_len > 0):
        cmd_len = find_complete_resp_command(rctx->buf, rctx->buf_len)
        // 解析 *N\r\n$L\r\n... 格式，计算完整命令长度
        if (cmd_len <= 0):
            break                              // 数据不完整，等待更多 TCP 段

        // 4. 追加到 backlog (为增量恢复做准备)
        repl_incr_append(rctx->buf, cmd_len)

        // 5. 如果从机已连接，直接发送
        if (peer_fd >= 0):
            tcp_send_frame(peer_fd, rctx->buf, cmd_len)
            // 发送格式: [frame_len:4(BE)][RESP_data:N]

        // 6. 从缓冲区移除已处理的命令
        memmove(rctx->buf, rctx->buf + cmd_len, rctx->buf_len - cmd_len)
        rctx->buf_len -= cmd_len
```

### 7.2 主机 TCP 主线程主循环

**文件**: [src/repl/repl_tcp.c](src/repl/repl_tcp.c):635-690

```c
master_thread_func():
    fcntl(listen_fd, O_NONBLOCK)              // 非阻塞 accept

    while (g_repl.running):
        // 1. 轮询 eBPF ringbuf (1ms 超时)
        ring_buffer__poll(g_repl.ringbuf, 1)
        // 这会触发 master_on_data 回调，消费 eBPF 捕获的写命令

        // 2. 接受新的从机连接 (非阻塞)
        if (peer_fd < 0):
            fd = accept(listen_fd, &slave_addr)
            if (fd >= 0):
                tcp_set_options(fd)            // TCP_NODELAY, 256KB buf
                master_handle_sync_request(fd) // 处理同步握手

        // 3. 每 10 秒清理过期的 TCP 重组上下文
        if (++cleanup_counter >= 10000):
            cleanup_stale_contexts()

        usleep(1000)                           // 1ms 循环间隔
```

### 7.3 同步握手：增量恢复还是全量

**文件**: [src/repl/repl_tcp.c](src/repl/repl_tcp.c):556-633

```c
master_handle_sync_request(fd):
    // Step 1: 接收 sync_request
    tcp_recv_exact(fd, &req, sizeof(sync_request))
    验证 magic == 0x53594E43

    last_offset = be64toh(req.last_offset)

    // Step 2: 检查 offset 是否仍在 backlog 中
    if (repl_incr_has_offset(last_offset)):
        // ===== 增量恢复 (Partial Resync) =====
        resp.status = SYNC_STATUS_OK
        resp.start_offset = htobe64(last_offset)

        // 从 backlog 读取所有缺失数据
        repl_incr_get_range(last_offset, catchup_buf, max_len, &catchup_len)
        resp.data_len = htobe64(catchup_len)

        // 发送 response header
        send(fd, &resp, sizeof(resp))
        // 发送 catchup 数据
        if (catchup_len > 0):
            send(fd, catchup_buf, catchup_len)
    else:
        // ===== 需要全量同步 =====
        resp.status = SYNC_STATUS_FULLSYNC_REQUIRED
        resp.start_offset = 0
        resp.data_len = 0
        send(fd, &resp, sizeof(resp))
        // 从机收到此状态后应重新触发 RDMA 全量同步
```

---

## 八、数据流追踪：一条 SET 命令的旅程

以客户端写入 `SET key1 "hello"` 为例，追踪数据在系统中的完整路径：

```
1. redis-cli → TCP → 主机 port 2000
      │
2.    proactor/reactor 接收 RESP: "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$5\r\nhello\r\n"
      │
3.    kvs_protocol_batch() → resp_parse_slices() → kvs_dispatch()
      │  ├─ 执行: string_engine_set("key1", "hello")
      │  └─ 成功后: kvs_write_journal_append(msg_raw, msg_len)
      │
4.    kvs_write_journal_append_kind():
      ├─ kvs_journal_append() → 写入内存 Journal
      ├─ kvs_wal_persist_append_raw() → 追加到 WAL (AOF)
      └─ (全量同步期间) rdma_repl_capture_write() → rewrite buffer
      │
5.    【eBPF 路径】内核态 stream_verdict 程序:
        bpf_skb_load_bytes()                // 读取 TCP 报文
        is_write_command("*3\r\n$3\r\nSET...")
          → 首3字符 "SET" → 是写命令
        bpf_ringbuf_reserve()               // 分配 ringbuf 空间
        bpf_skb_load_bytes()                // 拷贝完整 RESP
        bpf_ringbuf_submit()                // 提交到 ringbuf
      │
6.    【用户态消费】master_on_data():
        rctx = get_or_create_ctx(cookie)     // 根据 cookie 找重组上下文
        memcpy(rctx->buf, e->data, e->len)   // 追加到重组缓冲区
        cmd_len = find_complete_resp_command()
        repl_incr_append(buf, cmd_len)       // → 64MB ring buffer
        tcp_send_frame(peer_fd, buf, cmd_len)// → [len:4][data:N] over TCP
      │
7.    【从机接收】slave_thread_func():
        tcp_recv_exact(&frame_len, 4)        // 读取 4 字节长度
        tcp_recv_exact(frame_buf, frame_len) // 读取帧数据
        slave_apply_command(frame_buf, frame_len)
          → send(kv_fd, data, len)           // 发送到本地 kvstore port 2001
      │
8.    【从机执行】本地 kvstore port 2001:
        kvs_dispatch()                       // 解析执行 SET
        string_engine_set("key1", "hello")   // 插入字典
        // g_in_reload 标记抑制了 AOF 追加和 eBPF 探测
        // (从机执行的复制命令不产生新的 WAL，避免循环)
```

---

## 九、异常处理与恢复

### 9.1 全量同步期间主机收到写命令

```
问题: fork() 后，子进程持有 COW 快照。在此期间主机又收到了新的写命令
解决: rewrite buffer
  1. rewrite_buffer_enable() 在 fork 前调用
  2. kvs_write_journal_append() 在 g_rewrite_buf.enabled 时将写命令追加到 buffer
  3. 全量同步完成后，parent_send_thread 调用 rewrite_buffer_replay()
     → 逐条 kvs_dispatch() 回放这些增量写命令
  4. 回放完成后，子进程的 COW 快照 + 父进程的 rewrite buffer = 完整数据
```

### 9.2 从机断开重连

```
情况 A: 从机 offset 仍在主机 64MB backlog 内
  从机 → sync_request(last_offset=1000)
  主机 → repl_incr_has_offset(1000) = true
  主机 → repl_incr_get_range(1000, buf, ...) → 返回缺失数据
  主机 → send sync_response(OK) + catchup_data
  从机 → 接收并应用缺失数据 → 回到增量同步
  → 无需全量同步

情况 B: 从机 offset 太旧，已滚出 backlog
  从机 → sync_request(last_offset=1000)
  主机 → repl_incr_has_offset(1000) = false
  主机 → send sync_response(FULLSYNC_REQUIRED)
  从机 → 返回 -2，进入 sleep(10)
  外层 main() 检测到 need_fullresync → 重新调用 rdma_repl_slave_run()
  → 再次走全量同步流程
```

### 9.3 eBPF 过滤器加载失败

```c
// repl_tcp.c:920-922
if (master_load_bpf() != 0) {
    fprintf(stderr, "[REPL-TCP] Warning: eBPF filter failed, using fallback mode\n");
    // 继续运行，但不会有 eBPF 级别的过滤
    // 增量同步将无法正常工作（无 ringbuf 数据源）
}
```

### 9.4 信号处理

```c
// 所有 RDMA 等待函数使用 1 秒超时 + 信号检查
rdma_wait_cq():
    for (;;):
        if (g_kvs_stop): return -1
        fi_cq_sread(cq, &ce, 1, NULL, 1000)  // 1秒超时而非无限等待
        if (r == -FI_EAGAIN): continue        // 超时，重新检查 g_kvs_stop

// 优雅退出流程 (main 函数末尾):
kvs_stop_expire_thread()
aof_async_shutdown()
repl_stream_stop()           // 停止 TCP 增量同步线程
kvs_wal_persist_stop()
kvs_journal_shutdown()
dest_kvengine()
```

---

## 十、关键数据结构速查

| 结构体 | 文件 | 大小 | 用途 |
|--------|------|------|------|
| `shm_ring_t` | `rdma_repl.c` | 16 × ~1MB | 父子进程共享的序列化数据环 |
| `shm_slot_t` | `rdma_repl.c` | 1MB + 5B | 单个 chunk (header + data + state) |
| `rewrite_buffer_t` | `rdma_repl.c` | 4096 entries | 全量同步期间缓冲增量写 |
| `repl_backlog_t` | `repl_incr.c` | 64MB | 增量复制的环形积压缓冲区 |
| `reassembly_ctx` | `repl_tcp.c` | 16KB per conn | TCP 层 RESP 命令重组上下文 |
| `sync_request` | `repl_tcp.c` | 12B | 从机同步请求 (magic + offset) |
| `sync_response` | `repl_tcp.c` | 16B | 主机同步响应 (status + offset + len) |
| `repl_observe_event` | `repl_filter.bpf.c` | 4KB | eBPF ringbuf 事件 (cookie + len + data) |

---

## 十一、端口与线程一览

| 组件 | 线程 | 端口 | 说明 |
|------|------|------|------|
| RDMA 全量同步 (主机) | `rdma_master_thread` | 2100 | 监听从机连接，每次 accept 创建 libfabric EP |
| RDMA 发送 (主机) | `parent_send_thread` | — | 每从机一个，epoll + `fi_send` 流水线 |
| TCP 增量同步 (主机) | `master_thread_func` | 6483 | eBPF ringbuf 消费 + accept 从机 + 帧发送 |
| TCP 增量同步 (从机) | `slave_thread_func` | → 6483 | 连接主机 + 接收帧 + 应用到本地 kvstore |
| 过期键清理 | `expire_thread_main` | — | 每秒采样删过期键 |
| WAL 异步写 | AOF 后台线程 | — | io_uring 批量写 incr.wal |
| 客户端服务 | 主线程 | 2000/2001 | proactor/reactor/ntyco 事件循环 |
