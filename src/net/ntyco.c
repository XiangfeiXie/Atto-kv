/*=============================================================================
 *  ntyco.c — NtyCo 协程网络引擎
 *
 *  与 reactor(epoll)/proactor(io_uring) 平级的第三种网络模型：每连接一个
 *  协程（NtyCo 协程库），阻塞点在 nty_recv/nty_send/nty_accept 内让出调度器，
 *  fd 就绪后恢复继续——代码按同步顺序写，运行时由协程调度器异步推进。
 *
 *  本引擎接入 rbuf 后与其余引擎读缓冲统一（2026-09）：
 *  - 读缓冲 = rbuf 节点队列（rbuf_t，节点 RBUF_NODE_SIZE 全引擎一致）；
 *  - 数据入口 = 注入的 kvs_rbuf_drain_fn（三引擎统一 kvs_drain_rbuf_commands）；
 *  - 写缓冲沿用 DynBuf + w_off（半包发送记账），冲刷归本文件。
 *
 *  本引擎不参与复制：
 *  - ntyco 启动时 repl_engine_* 恒 NULL（仅 reactor/proactor 赋值）→
 *    kvs_dispatch 的 PSYNC 门控短路，控制命令按普通命令处理；
 *  - kvstore.c 对 network=ntyco + repl_role!=off 组合启动即拒（fail-fast）。
 *
 *  语义保证：
 *  - 单协程/连接 ⇒ server_reader 内无抢占点 ⇒ drain 期间无调度切换，
 *    g_kvs_drain_fd 与 rbuf reserve/commit 配对语义可靠；
 *  - 每轮"读 → drain → 整段冲刷"⇒ drain 入口 w_off==0（wb 追加点 = 物理
 *    尾部，与 reactor/proactor 同一不变量，大块回复可直接追加）；
 *  - nty_recv 是单缓冲形态：只灌 reserve 返回的 iov[0]，未用段由 rbuf_commit
 *    自动回池（ntyco 读吞吐非目标，节点容量仍与全引擎统一）。
 *============================================================================*/

#include "nty_coroutine.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "protocol.h"   /* kvs_rbuf_drain_fn（typedef 在协议头，间接含 dynbuf.h） */
#include "rbuf.h"       /* rbuf_t / RBUF_NODE_SIZE / RBUF_MAX_IOV */

/* 默认解析回调入口（kvs_rbuf_drain_fn）：ntyco_start 时由上层统一注入
 * kvs_drain_rbuf_commands。与 reactor/proactor 的 g_default_handler 同名对齐：
 * accept 的新连接把它拷进 client->on_data，之后 server_reader 一律走
 * client->on_data —— 三引擎"每连接解析回调 = on_data"命名统一 */
static kvs_rbuf_drain_fn g_default_handler = NULL;

/* 协程客户端结构体（堆分配，每连接独立） */
typedef struct {
    int fd;
    rbuf_t read_rb;    /* rbuf 节点队列读缓冲（与 reactor/proactor 统一） */
    DynBuf write_buf;  /* 动态写缓冲（批量回复核心） */
    int w_off;         /* 已发送前缀（半包发送记账） */
    kvs_rbuf_drain_fn on_data;  /* 解析回调入口：与 reactor/proactor 的 conn.on_data
                                   同名对齐；ntyco 无 repl 角色切换，恒为默认入口 */
} NtyClient;

/**
 * @brief 客户端协程处理函数：异步读 -> 协议解析 -> 异步发送
 * @param arg 堆分配的 NtyClient 结构体
 */
void server_reader(void *arg) {
    NtyClient *client = (NtyClient *)arg;
    int fd = client->fd;

    while (1) {
        /* 1. 取可写段并异步读入（reserve → nty_recv → commit 须配对） */
        struct iovec iov[RBUF_MAX_IOV];
        int nseg = rbuf_reserve(&client->read_rb, iov, RBUF_MAX_IOV);
        if (nseg <= 0) {   /* 空闲池空且新节点分配失败：OOM，断连 */
            fprintf(stderr, "[ntyco] fd %d rbuf reserve failed\n", fd);
            break;
        }

        /* nty_recv 单缓冲：只灌第一段；半包由调度器就绪等待补齐 */
        ssize_t ret = nty_recv(fd, iov[0].iov_base, iov[0].iov_len, 0);
        if (ret <= 0) {    /* 对端关闭(ret=0)/连接错误(ret<0) */
            rbuf_commit(&client->read_rb, 0);   /* 归还未用段 */
            break;
        }
        rbuf_commit(&client->read_rb, (int)ret);

        /* 2. 16MB 护栏：单连接已收未消费逻辑字节上限（同 reactor/proactor） */
        if (rbuf_len(&client->read_rb) > 16u * 1024u * 1024u) {
            fprintf(stderr, "[ntyco] fd %d read buffer overflow\n", fd);
            break;
        }

        /* 3. 协议解析：统一入口 kvs_drain_rbuf_commands（rbuf 形态，accept 时
         *    存入 client->on_data）want_close：QUIT/协议错误置位，冲刷后由本函数
         *    关闭连接 */
        int want_close = 0;
        client->on_data(fd, &client->read_rb, &client->write_buf, &want_close);

        /* 4. 冲刷回复（nty_send 内部就绪等待；保留半包记账兼容部分发送） */
        while (client->write_buf.len > 0) {
            ssize_t n = nty_send(fd, client->write_buf.buf + client->w_off,
                                 client->write_buf.len, MSG_NOSIGNAL);
            if (n <= 0) {   /* 发送失败：断连 */
                goto cleanup;
            }
            client->w_off += (int)n;
            client->write_buf.len -= (int)n;
        }
        client->w_off = 0;
        dynbuf_clear(&client->write_buf);   /* 仅发空后清，保持 w_off==0 不变量 */

        if (want_close)
            break;                          /* QUIT / 协议错误：冲刷完成，关闭 */
    }

cleanup:
    /* 资源释放（读缓冲节点 + 写缓冲 + fd + 协程私有结构） */
    rbuf_destroy(&client->read_rb);
    dynbuf_free(&client->write_buf);
    close(fd);
    free(client);
}

/**
 * @brief 设置文件描述符为非阻塞模式
 * @param fd 文件描述符
 */
static void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * @brief 监听协程：异步 accept 客户端连接
 * @param arg 监听端口
 */
void server(void *arg) {
    unsigned short port = *(unsigned short *)arg;

    /* 1. 创建 TCP 套接字 */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket failed");
        return;
    }

    /* 2. 端口复用 + 非阻塞 */
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nonblock(listen_fd);

    /* 3. 绑定地址 */
    struct sockaddr_in local = {0};
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (struct sockaddr *)&local, sizeof(local)) < 0) {
        perror("bind failed");
        close(listen_fd);
        return;
    }

    /* 4. 监听连接 */
    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen failed");
        close(listen_fd);
        return;
    }
    printf("✅ NtyCo协程模型启动 | 监听端口：%d | 高并发就绪\n", port);

    /* 5. 异步 accept 循环（NtyCo 协程模式） */
    while (1) {
        struct sockaddr_in remote = {0};
        socklen_t len = sizeof(remote);
        int cli_fd = nty_accept(listen_fd, (struct sockaddr *)&remote, &len);

        if (cli_fd < 0) {
            continue;
        }
        /* 客户端 socket 设置非阻塞 */
        set_nonblock(cli_fd);

        /* 6. 堆分配客户端结构体（避免栈溢出，多连接安全） */
        NtyClient *client = (NtyClient *)malloc(sizeof(NtyClient));
        if (!client) {
            close(cli_fd);
            continue;
        }

        /* 7. 初始化客户端状态（读缓冲 rbuf + 写缓冲 DynBuf，与 reactor/proactor 一致） */
        client->fd = cli_fd;
        rbuf_init(&client->read_rb, RBUF_NODE_SIZE);
        dynbuf_init(&client->write_buf, KVS_NET_BUF_INIT);
        client->w_off = 0;
        client->on_data = g_default_handler;  /* 解析回调入口（accept 时拷入，见 ntyco_start） */

        /* 8. 创建协程处理客户端（一对一协程模型） */
        nty_coroutine *read_co;
        if (nty_coroutine_create(&read_co, server_reader, client) != 0) {
            /* 协程创建失败，释放资源 */
            rbuf_destroy(&client->read_rb);
            dynbuf_free(&client->write_buf);
            free(client);
            close(cli_fd);
        }
    }
}

/**
 * @brief NtyCo 协程模型启动入口
 * @param port 监听端口
 * @param handler 命令解析处理函数（kvs_rbuf_drain_fn，与 reactor/proactor 统一）
 * @return 0
 */
int ntyco_start(unsigned short port, kvs_rbuf_drain_fn handler) {
    g_default_handler = handler;

    /* 创建监听协程 */
    nty_coroutine *listen_co = NULL;
    nty_coroutine_create(&listen_co, server, &port);

    /* 启动 NtyCo 协程调度器。说明：调度器只在不持有任何协程时返回，而监听协程
     * 常驻等待 ⇒ 本函数阻塞到进程退出；SIGTERM 不回读 g_kvs_stop（停机需 kill，
     * 见 run_ntyco.sh 注释）。NtyCo 为第三方模块不可改，优雅停机待其提供接口 */
    nty_schedule_run();
    return 0;
}
