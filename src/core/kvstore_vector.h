/*=============================================================================
 *  kvstore_vector.h — 向量命令的内部接口
 *
 *  向量在本项目里不是新引擎、也不是新节点类型：它就是一条**payload 带固定头
 *  的普通 KV 记录**。于是 dict_* / entry_* / TTL / RDB / AOF / 复制全部沿用
 *  现有路径，本模块只负责：
 *    1. payload 头的编解码（x86 原生小端，memcpy 按字节读写，不 casting）
 *    2. 写入时的 L2 归一化
 *    3. VSEARCH 的单线程暴力 top-k
 *
 *  payload 布局（与 kvstore-vector/README.md §3.1 一致）：
 *    [0..3]   magic   "AIV1"
 *    [4]      version 1
 *    [5]      dtype   0 = float32
 *    [6]      metric  0 = cosine（向量已归一化）
 *    [7]      flags   保留，0
 *    [8..9]   dim     little-endian uint16
 *    [10..11] reserved
 *    [12..15] meta_len little-endian uint32
 *    [16..]   meta    原样字节
 *    [..]     vector  dim 个小端 float32，已 L2 归一化
 *============================================================================*/

#ifndef __KVSTORE_VECTOR_H__
#define __KVSTORE_VECTOR_H__

#include "kvstore.h"   /* kvs_token_t / DynBuf / dispatch_result_t */

#define KV_VEC_MAGIC_LEN    4
#define KV_VEC_HDR_LEN      16
#define KV_VEC_VERSION      1
#define KV_VEC_DTYPE_F32    0
#define KV_VEC_METRIC_COS   0

#define KV_VEC_MAX_DIM      4096                    /* 单条向量维度上限 */
#define KV_VEC_MAX_META     (64u * 1024u)           /* meta 字节上限 */
#define KV_VEC_MAX_TOPK     1000                    /* VSEARCH 返回条数上限 */

/* 四条向量命令的处理入口（由 kvstore_dispatch.c 调用） */
dispatch_result_t vec_cmd_vset(kvs_token_t *tokens, int argc,
                               DynBuf *wb, int *journaled);
dispatch_result_t vec_cmd_vget(kvs_token_t *tokens, int argc,
                               DynBuf *wb, int *journaled);
dispatch_result_t vec_cmd_vdel(kvs_token_t *tokens, int argc,
                               DynBuf *wb, int *journaled);
dispatch_result_t vec_cmd_vsearch(kvs_token_t *tokens, int argc,
                                  DynBuf *wb, int *journaled);

#endif /* __KVSTORE_VECTOR_H__ */
