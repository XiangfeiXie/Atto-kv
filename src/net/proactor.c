#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <liburing.h>

#include "kvstore.h"
#include "config.h"
#include "protocol.h"

/*=============================================================================
 *  常量定义
 *============================================================================*/

#define ENTRIES_LENGTH   2048    /* io_uring 队列深度，需大于最大并发连接数 */

/*=============================================================================
 *  核心设计：串行（半双工）Proactor 模型
 *
 *  关键特性：
 *  1. 同一连接上 recv 与 send 不同时在飞（串行模型）
 *  2. 处理完输入：有回复先发，send 完成后再 arm 下一次读
 *  3. 语义与 epoll 单事件模型一致，结构简单
 *  4. event 掩码分别记录 recv/send 在飞（仅 master 流式推送路径两者并存）
 *============================================================================*/

/* CQE user_data 编码：高 32 位 = 完成类型，低 32 位 = fd
 * 显式类型区分 accept/recv/send，无需再与监听 fd 比对
 * 0 保留：io_uring 超时 CQE 的 user_data 为 0，落在 fd 0 空槽被 inactive 拦下 */
enum {
    TYPE_RECV   = 1,   /* 读操作完成 */
    TYPE_SEND   = 2,   /* 写操作完成 */
    TYPE_ACCEPT = 3    /* accept 完成 */
};

/* conn_t.event 在飞掩码：recv 与 send 可并存
 * （仅 master 流式推送路径：recv 常驻做断开检测 + send 推送并存；
 *   其余路径——含 slave 收养——均严格串行，event 恒为单 bit） */
enum {
    EV_RECV = 1,    /* recv SQE 在飞 */
    EV_SEND = 2     /* send SQE 在飞 */
};

/*=============================================================================
 *  连接状态结构体
 *  使用 fd 即索引的池化设计，热路径零哈希查找
 *============================================================================*/
typedef struct {
    /* 基础信息 */
    int     fd;                 /* socket 文件描述符，同时也是池索引 */
    int     active;             /* 连接是否活跃（用于幂等释放） */
    
    /* 读写缓冲（Slab 风格复用，热路径零分配） */
    DynBuf  read_buf;           /* 读缓冲：协议解析的原始数据 */
    DynBuf  write_buf;          /* 写缓冲：待发送的响应数据 */
    int     r_off;              /* 读缓冲已消费偏移（未压缩） */
    int     w_off;              /* 写缓冲已发送偏移（未压缩） */
    
    /* 状态控制 */
    int     status;             /* 标记需要关闭（QUIT / 协议错误），发完再关 */
    uint8_t event;              /* 在飞 I/O 掩码：EV_RECV/EV_SEND（可并存） */
    
    /* 业务处理器：支持普通客户端和 Replication 连接动态切换 */
    kvs_drain_fn on_data;       /* 数据处理函数，由业务层注入 */
} conn_t;

/*=============================================================================
 *  全局状态（单线程，无 worker 抽象）
 *============================================================================*/

static conn_t           *conn_pool      = NULL;  /* 连接池，大小 = maxclients */
static int               conn_pool_size = 0;     /* 连接池容量 */
static struct io_uring   g_ring;                /* io_uring 实例 */
static int               g_listen_fd    = -1;   /* 监听 socket */
static int               g_accept_armed = 0;    /* accept SQE 是否已在飞 */

/* 默认数据处理函数，由 proactor_start(port, handler) 注入
 * 普通客户端连接沿用该默认，repl 连接按需覆盖 */
static kvs_drain_fn      g_default_handler = NULL;

/*=============================================================================
 *  连接池管理
 *  设计要点：Slab 风格复用，保留缓冲内存避免热路径 malloc
 *============================================================================*/

/**
 * 初始化连接池
 * @param max_clients 最大客户端连接数
 * @return 0 成功，-1 失败
 */
static int conn_pool_init(int max_clients) {
    conn_pool_size = max_clients;
    conn_pool = calloc((size_t)max_clients, sizeof(conn_t));
    if (!conn_pool) {
        fprintf(stderr, "[proactor] failed to allocate conn pool\n");
        return -1;
    }
    printf("[proactor] conn pool: %d slots (%zu MB)\n",
           max_clients,
           (sizeof(conn_t) * (size_t)max_clients) / (1024 * 1024));
    return 0;
}

/**
 * 释放连接
 * 幂等设计：repl 交接时可能重复释放，通过 active 标记保护
 * @param fd 连接文件描述符
 */
static void conn_release(int fd) {
    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];
    if (!c->active) return;              /* 幂等：已释放则直接返回 */

    /* 先通知 repl 清理该连接的状态（普通客户端是 no-op，内部按角色判断） */
    repl_on_close(fd);

    /* 复位槽位并关闭 socket（保留 read/write 缓冲底层内存，避免 accept 热路径反复 malloc） */
    char *rb = c->read_buf.buf, *wb = c->write_buf.buf;
    int   rc = c->read_buf.cap,  wc = c->write_buf.cap;
    memset(c, 0, sizeof(*c));
    c->read_buf.buf  = rb;  c->read_buf.cap  = rc;
    c->write_buf.buf = wb;  c->write_buf.cap = wc;
    close(fd);
}

/**
 * 销毁连接池（程序退出时调用）
 */
static void conn_pool_destroy(void) {
    if (!conn_pool) return;
    for (int i = 0; i < conn_pool_size; i++) {
        if (conn_pool[i].active)
            conn_release(i);
        /* 释放缓冲内存 */
        dynbuf_free(&conn_pool[i].read_buf);
        dynbuf_free(&conn_pool[i].write_buf);
    }
    free(conn_pool);
    conn_pool = NULL;
    conn_pool_size = 0;
}

/*=============================================================================
 *  SQE 辅助函数（提交 I/O 请求）
 *============================================================================*/

/**
 * 提交 recv SQE（非阻塞）
 * 先确保读缓冲有可用空间（已满则翻倍扩容，与 reactor 一致）；
 * 读缓冲不压缩已消费前缀，命令完整消费后由 handle_read 归零 r_off，
 * 慢速滴灌下由 KVS_NET_BUF_MAX 上限兜底
 * @param c 连接对象
 * @return 0 成功，-1 失败（缓冲超限/无 SQE）
 */
static int arm_recv(conn_t *c) {
    /* 前置检查：连接活跃、未在飞、未标记关闭 */
    if (!c->active || (c->event & EV_RECV) || c->status)
        return 0;

    /* 读缓冲当前写入位置；空间不足则翻倍扩容 */
    DynBuf *rb = &c->read_buf;
    size_t tail = (size_t)(c->r_off + rb->len);
    if (tail >= (size_t)rb->cap) {
        size_t new_cap = rb->cap ? (size_t)rb->cap * 2 : KVS_NET_BUF_INIT;
        if (new_cap > KVS_NET_BUF_MAX) return -1;
        char *new_buf = realloc(rb->buf, new_cap);
        if (!new_buf) return -1;
        rb->buf = new_buf;
        rb->cap = (int)new_cap;
    }
    size_t avail = (size_t)rb->cap - tail;

    /* 获取 SQE 并提交 recv 请求 */
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) return -1;

    io_uring_prep_recv(sqe, c->fd, rb->buf + tail, avail, 0);
    sqe->user_data = ((uint64_t)TYPE_RECV << 32) | (uint32_t)c->fd;
    c->event |= EV_RECV; /* 标记在飞 */
    return 0;
}

/**
 * 提交 send SQE（非阻塞）
 * @param c 连接对象
 * @return 0 成功，-1 失败
 */
static int arm_send(conn_t *c) {
    /* 前置检查：连接活跃、未在飞、有待发数据 */
    if (!c->active || (c->event & EV_SEND) || c->write_buf.len <= 0)
        return 0;

    /* 获取 SQE 并提交 send 请求 */
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) return -1;

    io_uring_prep_send(sqe, c->fd,
                       c->write_buf.buf + c->w_off,
                       (size_t)c->write_buf.len, MSG_NOSIGNAL);
    sqe->user_data = ((uint64_t)TYPE_SEND << 32) | (uint32_t)c->fd;
    c->event |= EV_SEND; /* 标记在飞 */
    return 0;
}

/**
 * 提交 accept SQE（非阻塞）
 * @return 0 成功，-1 失败
 */
static int arm_accept(void) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) return -1;
    io_uring_prep_accept(sqe, g_listen_fd, NULL, NULL, SOCK_NONBLOCK);
    sqe->user_data = ((uint64_t)TYPE_ACCEPT << 32) | (uint32_t)g_listen_fd;
    return 0;
}

/*=============================================================================
 *  CQE 处理器（I/O 完成回调）
 *  设计原则：总是立即处理，不延迟，处理器内重新 arm 后续工作
 *============================================================================*/

/**
 * 初始化新连接（accept 和 repl 收养共用）
 * 使用 Slab 风格：首次分配缓冲，重用时直接复用
 * @param fd 已建立连接的 socket fd
 * @param arm_read 1=立即 arm 第一个 recv；0=暂不 arm（repl 收养串行路径先发请求）
 * @return 0 成功，-1 失败
 */
static int conn_init_fd(int fd, int arm_read) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    if (conn_pool[fd].active) return 0;    /* 已初始化则直接返回 */

    /* 设置非阻塞和 Nagle 禁用 */
    fcntl(fd, F_SETFL, O_NONBLOCK);
    int nd = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));

    conn_t *c = &conn_pool[fd];

    /* 首次使用时惰性分配读/写缓冲（热路径零 malloc） */
    if (!c->read_buf.buf)
        dynbuf_init(&c->read_buf, KVS_NET_BUF_INIT);
    if (!c->write_buf.buf)
        dynbuf_init(&c->write_buf, KVS_NET_BUF_INIT);

    /* 重置连接状态（保留缓冲内存） */
    c->fd            = fd;
    c->active        = 1;
    c->read_buf.len  = 0;
    c->write_buf.len = 0;
    c->r_off         = 0;
    c->w_off         = 0;
    c->status        = 0;
    c->event         = 0;
    c->on_data       = g_default_handler;  /* 默认使用普通客户端处理器 */

    /* 默认立即 arm 第一个 recv；repl 收养路径传 0（slave 先发 PSYNC，
     * send 完成后由 handle_write 串行 arm recv） */
    if (!arm_read) return 0;
    return arm_recv(c);
}

/**
 * 处理 accept 完成事件
 */
static void handle_accept(struct io_uring_cqe *cqe) {
    g_accept_armed = 0;    /* 标记 accept SQE 已消费，后续重新 arm */

    int connfd = cqe->res;
    if (connfd < 0) return;
    
    if (conn_init_fd(connfd, 1) != 0) {
        close(connfd);
        return;
    }
}

/**
 * 处理读完成事件
 * 核心逻辑：
 * 1. 数据追加到读缓冲
 * 2. 调用业务处理器解析命令、生成回复
 * 3. 串行模型：有回复先 arm send，send 完成后再 arm 下一次读
 * 4. 同一连接上 recv 与 send 不同时在飞
 */
static void handle_read(struct io_uring_cqe *cqe, conn_t *c) {
    int res = cqe->res;
    c->event &= ~EV_RECV;   /* 清除 recv 在飞标记 */

    DynBuf *rb = &c->read_buf;
    DynBuf *wb = &c->write_buf;

    /* 追加数据到读缓冲 */
    if (res > 0)
        rb->len += res;

    /* 处理对端关闭或错误 */
    if (res <= 0) {
        /* 排掉剩余字节，异步冲刷待发回复后释放 */
        if (rb->len > 0) {
            c->on_data(c->fd, rb, &c->r_off, wb, NULL);
            if (!c->active) return;   /* handler 可能已释放连接 */
        }
        if (wb->len > 0) {
            /* 有待发数据：先发完再释放 */
            if (arm_send(c) != 0)
                conn_release(c->fd);
            else
                c->status = 1;   /* 标记发送完成后再释放 */
        } else {
            conn_release(c->fd);
        }
        return;
    }

    /* 正常读完成：调用业务处理器解析数据 */
    int want_close = 0;
    c->on_data(c->fd, rb, &c->r_off, wb, &want_close);
    if (want_close) c->status = 1;    /* 业务层要求关闭（如 QUIT 命令） */
    if (!c->active) return;           /* handler 可能已释放连接（如 repl） */

    /* 清理已完全消费的读缓冲 */
    if (rb->len == 0)
        c->r_off = 0;

    /* 串行（半双工）模型：有回复先发，send 完成后再 arm 下一次读 */
    if (wb->len > 0) {
        if (arm_send(c) != 0) {
            conn_release(c->fd);
            return;
        }
        return;               /* 待发回复（含 +FULLRESYNC）发完再继续读 */
    }

    /* 无待发数据：需要关闭则直接释放，否则立即 arm 下一次读 */
    if (c->status) {
        conn_release(c->fd);
        return;
    }
    if (arm_recv(c) != 0)
        conn_release(c->fd);
}

/**
 * 处理写完成事件
 * 支持部分发送重试，保证数据完整性
 */
static void handle_write(conn_t *c, int res) {
    c->event &= ~EV_SEND;   /* 清除 send 在飞标记 */
    DynBuf *wb = &c->write_buf;

    /* 发送失败：直接释放连接 */
    if (res <= 0) {
        conn_release(c->fd);
        return;
    }

    /* 更新发送进度 */
    c->w_off += res;
    wb->len  -= res;

    if (wb->len > 0) {
        /* 部分发送：压缩缓冲并继续发送 */
        if (c->w_off > 0) {
            memmove(wb->buf, wb->buf + c->w_off, (size_t)wb->len);
            c->w_off = 0;
        }
        if (arm_send(c) != 0)
            conn_release(c->fd);
    } else {
        /* 全部发完：清空缓冲，串行模型下 recv 不在飞，重新 arm 读 */
        c->w_off = 0;
        dynbuf_clear(wb);

        if (c->status) {
            conn_release(c->fd);
        } else if (!(c->event & EV_RECV) && arm_recv(c) != 0) {
            /* master 流式推送路径 recv+send 可能在飞，此时不重复 arm recv */
            conn_release(c->fd);
        }
    }
}

/**
 * CQE 分发器：根据 user_data 区分 accept/recv/send
 */
static void handle_cqe(struct io_uring_cqe *cqe) {
    /* 解码：高 32 位类型，低 32 位 fd */
    uint64_t ud   = cqe->user_data;
    uint32_t type = (uint32_t)(ud >> 32);
    int      fd   = (int)(uint32_t)ud;

    /* accept 完成（低 32 位为监听 fd，此处不使用） */
    if (type == TYPE_ACCEPT) {
        handle_accept(cqe);
        return;
    }

    /* 普通连接：校验 fd 后按类型分发 */
    if (fd < 0 || fd >= conn_pool_size) return;
    conn_t *c = &conn_pool[fd];
    if (!c->active) return;

    if (type == TYPE_SEND)
        handle_write(c, cqe->res);
    else
        handle_read(cqe, c);
}

/*=============================================================================
 *  Replication ↔ Engine 访问接口
 *  引擎将这些函数实例赋给 kvstore.h 中声明的全局指针
 *  repl 模块只依赖这三个全局指针，不 include 本文件
 *============================================================================*/

/**
 * 注册一个连接为 repl 连接，切换到专用 handler
 * @param fd 连接 fd
 * @param handler repl 专用数据处理函数
 * @return 0 成功，-1 失败
 */
static int eng_repl_register(int fd, kvs_drain_fn handler) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    conn_t *c = &conn_pool[fd];
    if (!c->active) {
        /* 新 fd：完整初始化，但不 arm recv——串行收养路径先发请求
         * （slave_adopt 发 PSYNC），send 完成由 handle_write 再 arm recv */
        if (conn_init_fd(fd, 0) != 0) return -1;
        c = &conn_pool[fd];
    }
    c->on_data = handler;    /* 切换处理器 */
    return 0;
}

/**
 * 向 repl 连接追加待发送数据（如 +FULLRESYNC 响应）
 * @param fd 连接 fd
 * @param data 数据指针
 * @param len 数据长度
 * @return 0 成功，-1 失败
 */
static int eng_repl_append(int fd, const char *data, int len) {
    if (fd < 0 || fd >= conn_pool_size) return -1;
    conn_t *c = &conn_pool[fd];
    if (!c->active) return -1;
    DynBuf *wb = &c->write_buf;

    /* 计算追加位置 = 已发送前缀 + 待发长度（w_off 与 len 分离） */
    size_t tail = (size_t)(c->w_off + wb->len);
    if (tail + (size_t)len > (size_t)wb->cap) {
        /* 缓冲不足：扩容（额外 +4096 减少频繁 realloc） */
        size_t need = tail + (size_t)len + 4096;
        char *nb = realloc(wb->buf, need);
        if (!nb) return -1;
        wb->buf = nb;
        wb->cap = (int)need;
    }
    memcpy(wb->buf + tail, data, (size_t)len);
    wb->len += len;

    /* 追加后立即挂 send SQE */
    return arm_send(c);
}

/**
 * 释放 repl 连接（通过引擎的统一释放路径）
 */
static void eng_repl_release(int fd) {
    conn_release(fd);
}

/*=============================================================================
 *  主事件循环
 *  设计要点：
 *  1. 非 SQPOLL：应用侧显式 io_uring_enter 提交，每次 submit/wait 都进内核
 *  2. 忙轮询 + 阻塞混合：有 CQE 时忙处理，空闲时 submit_and_wait 定时休眠
 *     （100ms 超时：无 CQE 也周期性唤醒，驱动 repl tick 的主从同步推进）
 *  3. 批量收割 CQE：peek_batch 一次取多个，减少 acquire 屏障开销
 *  4. 每轮调用 repl_tick 用于主从复制状态推进
 *============================================================================*/

static void proactor_loop(void) {
    /* 初始 arm accept */
    if (arm_accept() != 0) {
        fprintf(stderr, "[proactor] failed to arm accept\n");
        return;
    }
    g_accept_armed = 1;

    printf("[proactor] io_uring event loop started\n");

    while (!g_kvs_stop) {
        /* 步骤 1：非阻塞批量收割已就绪的 CQE
         * 非 SQPOLL 下 CQE 由最近一次 io_uring_enter 产生，直接读共享 CQ 环 */
        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&g_ring, cqes, 128);

        if (nready == 0) {
            /* 步骤 2：无就绪 CQE，提交 SQE 并阻塞等待
             * 100ms 超时：空闲时也能周期性醒来跑 repl tick
             * （slave 收 +FULLRESYNC 后无 CQE 时全量交接仍要推进） */
            struct __kernel_timespec ts = { .tv_sec = 0, .tv_nsec = 100000000 };
            struct io_uring_cqe *tick_cqe = NULL;
            int ret = io_uring_submit_and_wait_timeout(&g_ring, &tick_cqe, 1,
                                                       &ts, NULL);
            if (ret < 0) {
                if (ret == -EINTR || ret == -ETIME) continue;  /* 中断/超时：继续 */
                fprintf(stderr, "[proactor] submit_and_wait: %s\n", strerror(-ret));
                break;
            }
            /* 阻塞返回后重新收割（可能只有超时 CQE，也可能有真实事件） */
            nready = io_uring_peek_batch_cqe(&g_ring, cqes, 128);
        }

        /* 步骤 3：每轮更新缓存时间（减少 gettimeofday 系统调用） */
        update_cached_time();

        /* 步骤 4：复制状态 tick（用于 slave 断线重连/全量交接推进） */
        repl_tick();

        /* 步骤 5：批量处理所有就绪的 CQE
         * peek_batch 只 load 一次 ktail 并批量拷贝指针
         * 省去 for_each 逐条 acquire 屏障 */
        for (int i = 0; i < nready; i++)
            handle_cqe(cqes[i]);
        io_uring_cq_advance(&g_ring, nready);

        /* 步骤 6：保持一个 accept SQE 在队列中
         * 让下一次 submit_and_wait 有活可干，避免空闲时无事可做 */
        if (!g_accept_armed && arm_accept() == 0)
            g_accept_armed = 1;

        /* 步骤 7：提交本轮 arm 的 SQE（非 SQPOLL 下进入内核处理）
         * 下一轮 peek/等待直接看到这批 I/O 的完成 */
        io_uring_submit(&g_ring);
    }

    fprintf(stderr, "[proactor] exit stop=%d\n", (int)g_kvs_stop);
}

/*=============================================================================
 *  初始化和销毁
 *============================================================================*/

/**
 * 初始化 io_uring 实例
 * 非 SQPOLL 模式：应用侧每个 submit/wait 直接进内核（io_uring_enter）。
 * 低 pipeline 下没有 SQPOLL 内核线程的唤醒握手延迟，单请求延迟更稳定；
 * 高吞吐靠批量收割 CQE 与批量 submit 摊薄系统调用开销。
 */
static int setup_io_uring(void) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    /* 非 SQPOLL：不设 IORING_SETUP_SQPOLL，由应用侧显式提交 */
    int ret = io_uring_queue_init_params(ENTRIES_LENGTH, &g_ring, &params);
    if (ret < 0) {
        fprintf(stderr, "[proactor] io_uring init: %s\n", strerror(-ret));
        return -1;
    }

    printf("[proactor] io_uring ready (entries=%d)\n", ENTRIES_LENGTH);
    return 0;
}

/**
 * 创建并绑定监听 socket
 */
static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (sockfd < 0) return -1;

    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port);

    if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("[proactor] bind");
        close(sockfd);
        return -1;
    }
    listen(sockfd, SOMAXCONN);
    return sockfd;
}

/**
 * 清理资源
 */
static void worker_destroy(void) {
    io_uring_queue_exit(&g_ring);
    if (g_listen_fd >= 0) {
        close(g_listen_fd);
        g_listen_fd = -1;
    }
}

/*=============================================================================
 *  对外入口
 *============================================================================*/

/**
 * 启动 Proactor 引擎
 * @param port 监听端口
 * @param handler 默认业务处理器（由 protocol.c 注入）
 * @return 0 成功，-1 失败
 */
int proactor_start(unsigned short port, kvs_drain_fn handler) {
    /* 设置默认处理器 */
    g_default_handler = handler;

    /* 注册 repl 引擎接口（供 replication 模块使用） */
    repl_engine_register = eng_repl_register;
    repl_engine_append   = eng_repl_append;
    repl_engine_release  = eng_repl_release;

    /* 初始化连接池 */
    if (conn_pool_init(kvs_config.maxclients) < 0) return -1;

    /* 初始化监听 socket */
    g_listen_fd = init_listen_socket(port);
    if (g_listen_fd < 0) {
        conn_pool_destroy();
        return -1;
    }

    /* 初始化 io_uring */
    if (setup_io_uring() < 0) {
        close(g_listen_fd);
        conn_pool_destroy();
        return -1;
    }

    printf("[proactor] single-threaded io_uring on port %u\n", port);

    /* 进入主事件循环（阻塞直到 g_kvs_stop 被置位） */
    proactor_loop();

    /* 清理资源 */
    worker_destroy();
    conn_pool_destroy();
    return 0;
}