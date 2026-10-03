/*=============================================================================
 *  rdma.c — RDMA 全量同步（verbs 数据面 + 业务控制层）
 *
 *  对标 Redis diskless 全量同步：RDMA 只是本项目的一种传输方式。与 sendfile
 *  路径同构——都是"fork 子进程 COW 快照 → 数据面子进程发送 → slave 就地加载"，
 *  差异只在数据面字节如何送达：
 *    - 服务端（rdma_server_run）：整份 RDB 无盘流序列化进发送暂存块，数据面用
 *      一个 WR 整块直写对端
 *    - 客户端（rdma_client_run）：等尾部 IMM 到达后就地一次性解析加载
 *
 *  文件内分段（曾是 rdma.c + rdma_transport.{h,c} 三个文件，合并后以段隔开）：
 *    ① 内部结构   ② 小工具   ③ 数据面-服务端   ④ 数据面-客户端   ⑤ 控制层入口
 *
 *  线上格式（单块，上限 = RDMA_BUF_SIZE）：
 *   [流头 token 8B][RDB 无盘流: key_count 4B + entries...]
 *  无盘流与磁盘 dump.rdb 条目字节序完全一致（⑤ 用的是同一个 ③ 加载端解析，
 *  见 persist_rdb.c），唯一差别是不带 crc 槽：无盘传输不落盘、无防篡改需求，
 *  完整性靠 RC 可靠有序 + key_count 数量核对。
 *
 *  为什么只需一个 WR：收发缓冲是两端自持的固定一块（上限 RDMA_BUF_SIZE），
 *  接收端不可能溢出 ⇒ 无需分块、无需滑动窗口、无需 ACK —— RC 可靠有序 +
 *  缓冲固定，顺序与完整性天然成立，整块一个 WR 即可。WRITE_WITH_IMM 挂在
 *  这唯一的 WR 上作完成通知，imm_data 留 0：全程只有这一个 IMM，它的值不携带
 *  任何信息，接收端也不读，设常量只会看着像校验。
 *
 *  为什么服务端要两阶段（conn_open 必须先于序列化）：监听必须在对端连接之前
 *  就绪。大快照序列化耗时数百 ms，若先序列化再监听，对端 REQ 落在监听就绪
 *  之前会被丢弃、rdma_connect 永远等不到事件，双方挂死。故 conn_open（监听+
 *  接受+收凭证，监听立即就绪）先行，再序列化，最后 conn_send。
 *
 *  两条实测经验固化在实现里（勿删）：
 *   1. rdma_connect/accept 必须显式 memset+赋值 conn_param（NULL 时字段内容
 *      不定，某些进程状态下会被对端 REJ_INVALID_RETRY_COUNT）
 *   2. 服务端发完必须等到那个唯一的 send CQE 才能断连销毁，否则在途的 WR
 *      会被丢弃、对端永远等不到尾部 IMM
 *
 *  计时归属：数据面只**测量**、不打印（conn_send 把 post→CQE 区间与注册耗时
 *  回传给调用方），唯一的汇总结论行由控制层（⑤）统一排版 —— 那条线还要拼上
 *  连接建立/序列化两段，只有控制层知道。速率一律以发送端为准：接收端的墙钟
 *  里必然混着对端的序列化时间，测不出纯传输（服务端是"先连后序列化"，客户端
 *  从 post 完 dummy recv 起就在等）。
 *============================================================================*/

#define _GNU_SOURCE
#include "rdma.h"
#include "config.h"       /* kvs_config */
#include "kv_engine.h"    /* dict_reserve / dict_clear */
#include "persistence.h"  /* rdb_serialize_* / rdb_load_stream_* */
#include "kvstore.h"      /* now_monotonic_ns / xfer_rate_mbs */

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <endian.h>      /* htobe64 / be64toh */

/*=============================================================================
 *  ① 内部结构
 *============================================================================*/

/* RDB 无盘流头长度（RDMA 全量）：仅 key_count[4]，无 crc 槽
 * 磁盘 dump.rdb 头是 RDB_FILE_HDR_LEN=8（key_count[4] + crc 槽[4]），那是
 * SAVE 落盘路径的事，与 RDMA 无关。两格式条目字节序一致（同一 ③ 加载端
 * 解析，见 persist_rdb.c），只差这个 crc 槽 */
#define RDB_STREAM_HDR_LEN  4

/** 对端缓冲凭证（谁接收数据，谁把自己的 addr/rkey 发给对方） */
struct buf_info {
    uint64_t addr;
    uint32_t rkey;
} __attribute__((packed));

/** 服务端连接句柄：conn_open 填充，conn_send 使用后销毁。
 *  调用方栈上自持（conn_open 会先 memset），不是堆上的不透明句柄 */
struct rdma_conn {
    struct rdma_cm_id *listen_id;
    struct rdma_cm_id *id;
    struct ibv_mr     *info_mr;
    struct buf_info    peer;   /* 对端（客户端）接收缓冲凭证 */
};

/*=============================================================================
 *  ② 小工具
 *============================================================================*/

/** 阻塞轮询一条完成（纯轮询占用 CPU 直至到达） */
static int poll_completion(struct ibv_cq *cq, const char *what)
{
    struct ibv_wc wc;
    int n;
    do {
        n = ibv_poll_cq(cq, 1, &wc);
    } while (n == 0);

    if (n < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[rdma] %s CQ error status=%s\n", what,
                n < 0 ? "poll-failed" : ibv_wc_status_str(wc.status));
        return -1;
    }
    return 0;
}

/* 双端一致的 QP 属性：单 SGE + 全部信号化。
 * 深度写死 1——整块一个 WR 的模型下，每个方向实际各只用掉 1 个：
 * 服务端 = 1 个收凭证 recv + 1 个发数据 send，客户端 = 1 个发凭证 send
 * + 1 个收 IMM 的 dummy recv。（某设备若拒绝深度 1，提到 2 即可，不影响结构） */
static void qp_attr(struct ibv_qp_init_attr *qa)
{
    memset(qa, 0, sizeof(*qa));
    qa->cap.max_send_wr  = 1;
    qa->cap.max_recv_wr  = 1;
    qa->cap.max_send_sge = 1;
    qa->cap.max_recv_sge = 1;
    /* 不设 sq_sig_all：两处发送各自显式带 IBV_SEND_SIGNALED，铺开只会多一个
     * 与显式标志重复的开关 */
}

/* 双端一致的连接参数：显式填满四个字段，勿用 NULL（字段内容不定，某些进程
 * 状态下会被对端 REJ_INVALID_RETRY_COUNT）。两端共用同一份，不再各填一半 */
static void conn_param(struct rdma_conn_param *cp)
{
    memset(cp, 0, sizeof(*cp));
    cp->responder_resources = 1;
    cp->initiator_depth     = 1;
    cp->retry_count         = 7;
    cp->rnr_retry_count     = 7;
}

/*=============================================================================
 *  ③ 数据面 — 服务端
 *    conn_open  → 监听 + 接受 + 收凭证（监听尽早就绪，对端不会提前连接被丢）
 *    conn_send  → 整块直写 + 等完成 + 销毁连接
 *    conn_close → 主动放弃时的错误路径（conn_open 失败时也复用它）
 *============================================================================*/

/**
 * 主动关闭连接并释放资源（conn_open 成功后、conn_send 之前放弃时的错误路径）。
 * 成功发送后由 conn_send 内部自行销毁，无需（也不应）再调本函数。
 * 自带 NULL 检查，句柄全 NULL（刚 memset 过）时是空操作。
 */
static void conn_close(struct rdma_conn *c)
{
    if (!c)
        return;
    if (c->info_mr)
        rdma_dereg_mr(c->info_mr);
    if (c->id) {
        if (c->id->qp)
            rdma_disconnect(c->id);
        rdma_destroy_ep(c->id);
    }
    if (c->listen_id)
        rdma_destroy_ep(c->listen_id);
}

/**
 * 阶段 1：监听 → 接受 → 收对端缓冲凭证。
 * 连接就绪即返回；调用方可在其后任意时刻准备数据（如序列化快照），
 * 再调 conn_send 直写。
 *
 * c 由调用方自持（栈上即可）：函数先 memset，失败时内部清理干净并返回 -1
 * （调用方无需再调 conn_close）。
 *
 * @return 0 成功，-1 失败
 */
static int conn_open(const char *ip, int port, struct rdma_conn *c)
{
    struct rdma_addrinfo hints, *res = NULL;
    char port_str[16];

    memset(c, 0, sizeof(*c));   /* 失败路径 conn_close 依赖指针全 NULL */

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags      = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (rdma_getaddrinfo(ip, port_str, &hints, &res)) {
        perror("[rdma] getaddrinfo");
        return -1;
    }

    struct ibv_qp_init_attr qa;
    qp_attr(&qa);

    if (rdma_create_ep(&c->listen_id, res, NULL, &qa)) {
        perror("[rdma] create_ep");
        goto fail;
    }
    rdma_freeaddrinfo(res);
    res = NULL;

    if (rdma_listen(c->listen_id, 1)) {
        perror("[rdma] listen");
        goto fail;
    }

    if (rdma_get_request(c->listen_id, &c->id)) {
        perror("[rdma] get_request");
        goto fail;
    }

    /* 先注册并投递缓冲凭证接收，再 accept —— 客户端连接建立后会立刻
     * 发送凭证，若此刻 RQ 为空会触发 RNR NAK 直至重试耗尽毁链 */
    c->info_mr = rdma_reg_msgs(c->id, &c->peer, sizeof(c->peer));
    if (!c->info_mr) {
        perror("[rdma] reg info mr");
        goto fail;
    }
    if (rdma_post_recv(c->id, NULL, &c->peer, sizeof(c->peer), c->info_mr)) {
        perror("[rdma] post recv info");
        goto fail;
    }

    struct rdma_conn_param ap;
    conn_param(&ap);
    if (rdma_accept(c->id, &ap)) {
        perror("[rdma] accept");
        goto fail;
    }

    /* 收对端缓冲凭证（slave 的接收缓冲）；连接已就绪，可先于数据准备 */
    if (poll_completion(c->id->recv_cq, "buf_info")) {
        fprintf(stderr, "[rdma] no buf_info from client\n");
        goto fail;
    }

    return 0;

fail:
    if (res)
        rdma_freeaddrinfo(res);
    /* 失败路径的收尾与主动关闭完全同构（指针由 memset 初始全 NULL，
     * conn_close 自带 NULL 检查），直接复用它 */
    conn_close(c);
    return -1;
}

/**
 * 阶段 2：把 [buf, len) 整块直写客户端缓冲（一个 WR 推送 → 等完成 → 销毁连接）。
 * 调用后 conn 被销毁（成败皆然），不能再复用。
 *
 * 内部实测的两段回传给调用方拼汇总结论行，本层不打印：
 *   reg  —— 发送缓冲注册为本端 MR（大缓冲时百毫秒级）
 *   xfer —— 纯传输：ibv_post_send → send CQE
 * RDMA_WRITE 的 send CQE 语义是"对端已 ACK"，即数据确已写入对端内存 ——
 * 与 sendfile 侧"字节进了本端发送缓冲"相比是 真·端到端。
 *
 * @param out_reg_ms / out_xfer_ms 可为 NULL
 * @return 0 成功，-1 失败
 */
static int conn_send(struct rdma_conn *c, const void *buf, size_t len,
                     double *out_reg_ms, double *out_xfer_ms)
{
    struct ibv_mr *mr = NULL;
    int rc = -1;

    if (out_reg_ms)  *out_reg_ms  = 0.0;
    if (out_xfer_ms) *out_xfer_ms = 0.0;

    /* 参数不合法也统一走 out：本函数的契约是"调用后 conn 被销毁"，
     * 各错误路径行为一致（conn_close 自带 NULL 检查） */
    if (!c || !c->id || !buf || len == 0)
        goto out;

    if (len > UINT32_MAX) {   /* ibv_sge.length 是 uint32_t */
        fprintf(stderr, "[rdma] len %zu exceeds single-SGE limit\n", len);
        goto out;
    }

    /* 发送缓冲（调用方自持）：注册为本端 MR。
     * 注册耗时回传给调用方单列（大缓冲时是百毫秒级），不计入纯传输速率 */
    uint64_t t_reg = now_monotonic_ns();
    mr = rdma_reg_msgs(c->id, (void *)buf, len);
    if (!mr) {
        perror("[rdma] reg_msgs send buf");
        goto out;
    }
    uint64_t reg_ns = now_monotonic_ns() - t_reg;

    /* 整块一个 WR 直写：接收缓冲固定且有上限 ⇒ 无需分块、无需滑动窗口 */
    struct ibv_sge sge = {
        .addr   = (uintptr_t)buf,
        .length = (uint32_t)len,
        .lkey   = mr->lkey,
    };
    struct ibv_send_wr wr = { 0 }, *bad = NULL;
    wr.sg_list    = &sge;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = c->peer.addr;
    wr.wr.rdma.rkey        = c->peer.rkey;

    /* 纯传输计时起点：post 之前（注册已完成，不计入速率） */
    uint64_t t_xfer = now_monotonic_ns();
    if (ibv_post_send(c->id->qp, &wr, &bad)) {
        perror("[rdma] ibv_post_send");
        goto out;
    }

    /* 唯一一个 send CQE：RC 下它意味着对端已 ACK（数据确已写入对端内存），
     * 故这段区间就是真·端到端传输耗时 */
    if (poll_completion(c->id->send_cq, "send") < 0)
        goto out;

    if (out_reg_ms)  *out_reg_ms  = (double)reg_ns / 1e6;
    if (out_xfer_ms) *out_xfer_ms = (double)(now_monotonic_ns() - t_xfer) / 1e6;
    rc = 0;

out:
    if (mr)
        rdma_dereg_mr(mr);
    /* 断连销毁前必须已等到那个 send CQE：
     * 否则在途的 WR 会被丢弃、对端永远等不到尾部 IMM */
    conn_close(c);
    return rc;
}

/*=============================================================================
 *  ④ 数据面 — 客户端：连接 → 发凭证 → 等尾部 IMM → 清理（一步完成）
 *============================================================================*/

/**
 * 接收整块内存。
 * 返回时整份数据已在 buf 里（实际字节数由应用层流头自描述）。
 *
 * @param buf 接收缓冲（调用方自持，远程写目标，须 ≥ cap 字节）
 * @param cap 接收缓冲容量
 * @return 0 成功，-1 失败
 */
static int conn_recv(const char *ip, int port, void *buf, size_t cap)
{
    int rc = -1;
    struct rdma_cm_id *id = NULL;
    struct ibv_mr *mr = NULL, *info_mr = NULL, *dmr = NULL;
    struct rdma_addrinfo hints, *res = NULL;
    char port_str[16];

    if (!buf || cap == 0)
        return -1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (rdma_getaddrinfo(ip, port_str, &hints, &res)) {
        perror("[rdma] getaddrinfo");
        goto out;
    }

    struct ibv_qp_init_attr qa;
    qp_attr(&qa);

    if (rdma_create_ep(&id, res, NULL, &qa)) {
        perror("[rdma] create_ep");
        goto out;
    }
    rdma_freeaddrinfo(res);
    res = NULL;

    /* 接收缓冲（远程写目标）：调用方自持的单块大缓冲 */
    mr = rdma_reg_write(id, buf, cap);
    if (!mr) {
        perror("[rdma] reg_write buf");
        goto out;
    }

    /* 预投递一个 dummy recv：捕获尾部 WRITE_WITH_IMM。
     * 数据不经过它（IMM 不带 payload，HCA 不往里 DMA），它只是接完成事件的
     * 一个"坑位"。因此紧跟 post 就 dereg 是安全的——前提是这块内存本身不能
     * 消失：dummy 必须是 static（WQE 里留的是注册时的 lkey，注销后内核不再
     * 跟踪，但 static 内存永远有效）。别照搬到栈/堆变量上。 */
    {
        static char dummy;
        dmr = rdma_reg_msgs(id, &dummy, 1);
        if (!dmr) {
            perror("[rdma] reg dummy mr");
            goto out;
        }
        if (rdma_post_recv(id, NULL, &dummy, 1, dmr)) {
            perror("[rdma] post recv dummy");
            goto out;
        }
        rdma_dereg_mr(dmr);
        dmr = NULL;
    }

    struct rdma_conn_param cp;
    conn_param(&cp);
    if (rdma_connect(id, &cp)) {
        perror("[rdma] connect");
        goto out;
    }

    /* 把本端缓冲凭证发给服务端（服务端据此直写本缓冲） */
    {
        struct buf_info bi = {
            .addr = (uintptr_t)buf,
            .rkey = mr->rkey,
        };
        info_mr = rdma_reg_msgs(id, &bi, sizeof(bi));
        if (!info_mr) {
            perror("[rdma] reg info mr");
            goto out;
        }
        if (rdma_post_send(id, NULL, &bi, sizeof(bi), info_mr,
                           IBV_SEND_SIGNALED)) {
            perror("[rdma] post send buf_info");
            goto out;
        }
        if (poll_completion(id->send_cq, "buf_info-send")) {
            fprintf(stderr, "[rdma] send buf_info failed\n");
            goto out;
        }
        rdma_dereg_mr(info_mr);
        info_mr = NULL;
    }

    /* 唯一的 IMM：见到即整块落地；实际长度由应用层协议头自描述 */
    if (poll_completion(id->recv_cq, "imm-done")) {
        fprintf(stderr, "[rdma] no imm-done\n");
        goto out;
    }

    rc = 0;

out:
    if (mr)      rdma_dereg_mr(mr);
    if (info_mr) rdma_dereg_mr(info_mr);
    if (id) {
        if (id->qp)
            rdma_disconnect(id);
        rdma_destroy_ep(id);
    }
    if (res)
        rdma_freeaddrinfo(res);
    return rc;
}

/*=============================================================================
 *  ⑤ 控制层入口
 *============================================================================*/

/**
 * RDMA 服务端入口（master 全量同步传输子进程，fork 由 repl_fullsync.c 触发）
 *
 * 流程：
 * 1. conn_open：监听 + 接受 + 收对端接收缓冲凭证。
 *    必须先于序列化（理由见文件头"为什么服务端要两阶段"）
 * 2. 整个 RDB 无盘流（key_count + entries，无 crc 槽）序列化进暂存块
 *    （persist_rdb.c 的 ④ 序列化器；装不下 RDMA_BUF_SIZE 即失败，不留半截快照）
 * 3. conn_send：一个 WR 整块直写对端（内部等唯一 send CQE，尾部 IMM 通知）
 *
 * 打印：本层拼唯一的汇总结论行 [rdma] xfer-end（形状与 sendfile 侧一致），
 * conn/ser/reg 三段准备成本收在其后，不做单独的 prep-end 行
 *
 * @param token 快照 offset（会话元数据，随流头直传对端续传）
 * @return 0 成功，-1 失败
 */
int rdma_server_run(uint64_t token)
{
    setlinebuf(stdout); setlinebuf(stderr);
    const char *ip = kvs_config.fullsync_listen_ip;
    if (!strcmp(ip, "0.0.0.0"))
        ip = NULL;

    /* 先建连接：监听必须在对端连接之前就绪 */
    struct rdma_conn conn;
    uint64_t t0 = now_monotonic_ns();   /* 连接建立计时起点 */
    if (conn_open(ip, kvs_config.fullsync_port, &conn) < 0)
        return -1;
    uint64_t t1 = now_monotonic_ns();   /* 连接就绪：xfer 从此起算（不含连接建立） */

    /* 发送暂存块（调用方自持，整份快照序列化目标） */
    char *buf = malloc(RDMA_BUF_SIZE);
    if (!buf) {
        fprintf(stderr, "[rdma] malloc(%zu) failed\n", (size_t)RDMA_BUF_SIZE);
        conn_close(&conn);
        return -1;
    }

    /* 快照流头：仅 token（快照 offset），数据长度由 RDB 流头 key_count 自描述 */
    uint64_t tok_be = htobe64(token);
    memcpy(buf, &tok_be, RDMA_TOKEN_LEN);

    /* 整份 RDB 无盘流（key_count + entries，无 crc 槽）一次性序列化进暂存块。
     * 一整块装满、一个 WR 推走，所以没有分批接口：装不下就是失败，
     * 不留半截快照（对端按 key_count 解析，半截必然数量对不上） */
    uint64_t s1 = now_monotonic_ns();
    size_t off = RDMA_TOKEN_LEN
               + rdb_serialize_all(buf + RDMA_TOKEN_LEN,
                                   RDMA_BUF_SIZE - RDMA_TOKEN_LEN);
    uint64_t prep_ns = now_monotonic_ns() - s1;
    if (off == RDMA_TOKEN_LEN) {
        fprintf(stderr,
                "[rdma] snapshot exceeds transport buffer (%zu B), aborting\n",
                (size_t)RDMA_BUF_SIZE);
        free(buf);
        conn_close(&conn);
        return -1;
    }

    double conn_ms = (t1 > t0) ? (double)(t1 - t0) / 1e6 : 0.0;
    double ser_ms  = (double)prep_ns / 1e6;

    /* 传输阶段：一个 WR 整块直写（连接已就绪；内含等 send CQE + 销毁连接）。
     * 数据面两段耗时由它回传，与本层这两段拼成一行 */
    double reg_ms = 0.0, xfer_ms = 0.0;
    int rc = conn_send(&conn, buf, off, &reg_ms, &xfer_ms);
    free(buf);
    if (rc < 0)
        return -1;

    /* 汇总结论行：形状与 [sendfile] xfer-end 完全一致 —— 前三个字段对齐、
     * 速率同列，准备成本收在 " | prep:" 之后（sendfile 是 save，本线是
     * conn/ser/reg）。rate 与 sendfile 同走 xfer_rate_mbs（十进制 MB/s） */
    char tb[24];
    printf("[rdma] xfer-end: bytes=%zu xfer=%.1fms rate=%.2fMB/s | prep: "
           "conn=%.1fms ser=%.1fms reg=%.1fms | at=%s\n",
           off, xfer_ms, xfer_rate_mbs(off, xfer_ms),
           conn_ms, ser_ms, reg_ms,
           xfer_wall_str(get_time_now_real(), tb, sizeof(tb)));

    return 0;
}

/**
 * RDMA 客户端入口（slave 端全量同步：+FULLRESYNC 握手后由 repl_fullsync.c 编排）
 *
 * 流程：
 * 1. conn_recv：连接 + 注册接收缓冲 + 发缓冲凭证 + 等尾部 IMM（整块落地）
 * 2. 解出流头 token → 按 RDB 流头 key_count 限条数就地解析加载
 *    （无盘流无 crc 槽：完整性 = RC 可靠有序 + loaded==expected 数量核对）
 *
 * 长度语义：真实数据字节数无法先验（条目变长），故 append_n 喂的是"流头后
 * 的整块尾部缓冲"，以 limit=key_count 在第 key_count 条处截停；尾部未用缓冲
 * 是 calloc 的空白，不会被误解析。
 *
 * @param out_token 输出：从流头解出的 token（快照 offset，供增量续传对齐）
 * @return 0 成功，-1 失败
 */
int rdma_client_run(uint64_t *out_token)
{
    setlinebuf(stdout); setlinebuf(stderr);

    /* 接收缓冲（调用方自持，远程写目标） */
    char *buf = calloc(1, RDMA_BUF_SIZE);
    if (!buf) {
        fprintf(stderr, "[rdma] malloc(%zu) failed\n", (size_t)RDMA_BUF_SIZE);
        return -1;
    }

    int rc = conn_recv(kvs_config.fullsync_master_ip,
                       kvs_config.fullsync_port, buf, RDMA_BUF_SIZE);
    if (rc < 0) {
        fprintf(stderr, "[rdma] recv failed\n");
        free(buf);
        return -1;
    }

    /* 快照流头：token（快照 offset）+ RDB 无盘流（key_count + entries） */
    uint64_t tok_be;
    memcpy(&tok_be, buf, RDMA_TOKEN_LEN);
    uint64_t snap = be64toh(tok_be);
    if (out_token)
        *out_token = snap;

    /* 数据长度由 RDB 流头 key_count 自描述：
     * 只解析 key_count 条，数量不一致默认加载失败（无 magic 校验） */
    int status = 0;
    const char *stream = buf + RDMA_TOKEN_LEN;        /* 跳过流头 token */
    int expected = rdb_load_stream_begin(stream);
    if (expected >= 0) {
        if (expected > 0)
            dict_reserve((uint32_t)expected);
        long loaded = rdb_load_stream_append_n(stream + RDB_STREAM_HDR_LEN,
                                               RDMA_BUF_SIZE - RDMA_TOKEN_LEN
                                               - RDB_STREAM_HDR_LEN,
                                               expected);
        if (loaded != expected) {
            fprintf(stderr, "[rdma] key count mismatch: expected %d, got %ld\n",
                    expected, loaded);
            dict_clear();
            status = -1;                      /* 疑似传输截断 */
        }
    } else {
        status = -1;
    }
    free(buf);
    return status;
}
