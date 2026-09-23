/*=============================================================================
 *  rdma.c — RDMA 全量同步的控制层
 *
 *  分层定位（自上而下，每层只依赖下一层）：
 *    repl_fullsync.c    全量同步编排 + sendfile/RDMA 按配置分派（fork 传输子进程）
 *    rdma.c（本文件）   业务控制：RDB 无盘流 序列化 / 加载 / 性能统计，不触碰 verbs
 *    rdma_transport.c   verbs 数据面：连接 / 整块单 WR / IMM 通知
 *
 *  本文件对标 Redis diskless 全量同步：RDMA 只是本项目的一种传输方式。与
 *  sendfile 路径（fork 子进程落盘 dump.rdb 后转送）同构——都是"fork 子进程
 *  COW 快照 → 数据面子进程发送 → slave 就地加载"，差异只在数据面字节如何送达：
 *    - 服务端（rdma_server_run）：整份 RDB 无盘流序列化进发送暂存块，
 *      rdma_xport 用一个 WR 整块直写对端
 *    - 客户端（rdma_client_run）：等尾部 IMM 到达后就地一次性解析加载
 *
 *  线上格式（单块，大小上限 = RDMA_BUF_SIZE）：
 *   [流头 token 8B][RDB 无盘流: key_count 4B + entries...]
 *   RDB 无盘流与磁盘 dump.rdb 字节序完全一致（③ 加载端同一解析，见
 *   persist_rdb.c），唯一差别是不带 crc 槽：无盘传输不落盘、无防篡改需求，
 *   完整性靠 RC 可靠有序 + key_count 数量核对。数据长度由 RDB 流头
 *   key_count 自描述（slave 只解析 key_count 条），数量不一致即加载失败，
 *   无需 magic/total_len 等额外校验字段。
 *============================================================================*/

#define _GNU_SOURCE
#include "rdma.h"
#include "rdma_transport.h"
#include "config.h"       /* kvs_config */
#include "kv_engine.h"      /* dict_reserve / dict_clear */
#include "persistence.h"  /* rdb_serialize_* / rdb_load_stream_* */
#include "kvstore.h"      /* now_monotonic_ns */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <endian.h>      /* htobe64 / be64toh */

/*=============================================================================
 *  工具
 *============================================================================*/

/* RDB 无盘流头长度（RDMA 全量）：仅 key_count[4]，无 crc 槽
 * 磁盘 dump.rdb 头是 RDB_FILE_HDR_LEN=8（key_count[4] + crc 槽[4]），那是
 * SAVE 落盘路径的事，与 RDMA 无关。两格式条目字节序一致（同一 ③ 加载端
 * 解析，见 persist_rdb.c），只差这个 crc 槽 */
#define RDB_STREAM_HDR_LEN  4

/*=============================================================================
 *  入口
 *============================================================================*/

/**
 * RDMA 服务端入口（master 全量同步传输子进程，fork 由 repl_fullsync.c 触发）
 *
 * 流程：
 * 1. rdma_xport_conn_open：监听 + 接受 + 收对端接收缓冲凭证。
 *    必须先于序列化（见调用点注释：对端 REQ 早到会被丢、双方挂死）
 * 2. 整个 RDB 无盘流（key_count + entries，无 crc 槽）序列化进暂存块
 *    （④ 序列化器，persist_rdb.c；装不下 RDMA_BUF_SIZE 即失败，不留半截快照）
 * 3. rdma_xport_conn_send：一个 WR 整块直写对端（内部等唯一 send CQE，尾部 IMM 通知）
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

    /* 先建连接：监听必须在对端连接之前就绪。大快照序列化耗时数百 ms,
     * 若后起监听, 对端 REQ 落在监听就绪前会被丢弃、rdma_connect 永远
     * 等不到事件, 双方挂死。因此 conn_open 先行, 再序列化, 最后直写 */
    rdma_xport_conn_t *conn = NULL;
    uint64_t t0 = now_monotonic_ns();   /* 连接建立计时起点 */
    if (rdma_xport_conn_open(ip, kvs_config.fullsync_port, &conn) < 0)
        return -1;
    uint64_t t1 = now_monotonic_ns();   /* 连接就绪: xfer 从此起算(序列化+直写, 不含连接建立) */

    /* 发送暂存块（调用方自持，整份快照序列化目标） */
    char *buf = malloc(RDMA_BUF_SIZE);
    if (!buf) {
        fprintf(stderr, "[rdma] malloc(%zu) failed\n", (size_t)RDMA_BUF_SIZE);
        rdma_xport_conn_close(conn);
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
        rdma_xport_conn_close(conn);
        return -1;
    }

    double conn_ms = (t1 > t0) ? (double)(t1 - t0) / 1e6 : 0.0;
    double ser_ms  = (double)prep_ns / 1e6;

    /* 传输阶段：一个 WR 整块直写（连接已就绪；内含等 send CQE + 断连清理）。
     * 数据面三段（bytes/reg/xfer）由传输层回传，与本层这两段拼成一行 */
    rdma_xport_stat_t st;
    int rc = rdma_xport_conn_send(conn, buf, off, &st);
    free(buf);
    if (rc < 0)
        return -1;

    /* 汇总结论行：形状与 [sendfile] xfer-end 完全一致 —— 前三个字段对齐、
     * 速率同列，准备成本收在 " | prep:" 之后（sendfile 是 save，本线是
     * conn/ser/reg）。rate 与 sendfile 同走 xfer_rate_mbs（十进制 MB/s） */
    printf("[rdma] xfer-end: bytes=%zu xfer=%.1fms rate=%.2fMB/s | prep: "
           "conn=%.1fms ser=%.1fms reg=%.1fms\n",
           st.bytes, st.xfer_ms, xfer_rate_mbs(st.bytes, st.xfer_ms),
           conn_ms, ser_ms, st.reg_ms);

    return 0;
}

/**
 * RDMA 客户端入口（slave 端全量同步：+FULLRESYNC 握手后由 repl_fullsync.c 编排）
 *
 * 流程：
 * 1. rdma_xport_recv：连接 + 注册接收缓冲 + 发缓冲凭证 + 等尾部 IMM（整块落地）
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

    int rc = rdma_xport_recv(kvs_config.fullsync_master_ip,
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
