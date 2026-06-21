# Master 子进程 RDMA 数据传输实现指南

## 📍 文件位置

`src/repl/rdma_repl_master.c`

---

## 🎯 修改目标

将当前的 `master_fullsync_fork()` 改为 `master_fullsync_child()`，实现纯 RDMA 数据传输，无控制消息。

---

## 🔄 架构变化

### 旧架构（当前）
```
父进程 accept RDMA (2100) → fork → 
子进程创建 RDMA (2101) →
发送 BATCH_ACK 控制消息 → 发送数据 → 等待 ACK
```

### 新架构（目标）
```
repl_tcp fork → 
子进程创建 RDMA (2101) →
纯数据流式发送 → fi_shutdown()
```

---

## 📝 函数签名

```c
/* 删除旧函数 */
// int master_fullsync_fork(struct fid_domain *domain, struct fid_ep *ep, struct fid_cq *cq);

/* 新增函数 */
int master_fullsync_child(uint64_t base_seq);
```

---

## 🛠️ 完整实现

### 步骤 1：删除旧代码

删除以下内容：
- `master_fullsync_fork()` 函数
- 所有 `ctrl_msg_send/recv` 调用
- BATCH_ACK 等待逻辑

### 步骤 2：实现新函数

```c
/* src/repl/rdma_repl_master.c */

#include "rdma_repl.h"
#include "kvstore.h"
#include "kv_dict.h"
#include "repl_incr.h"

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>

/* Child process RDMA data port */
#define CHILD_RDMA_PORT  2101

/* Batch size for streaming */
#define STREAM_BATCH_SIZE  (512 * 1024 * 1024)  /* 512MB */

/* ── Helper: Wait for CQ completion ───────────────────────────────── */

static int wait_cq(struct fid_cq *cq) {
    struct fi_cq_data_entry e;
    for (;;) {
        ssize_t r = fi_cq_read(cq, &e, 1);
        if (r > 0) return 0;
        if (r < 0 && r != -FI_EAGAIN) {
            struct fi_cq_err_entry err;
            fi_cq_readerr(cq, &err, 0);
            fprintf(stderr, "[CHILD] CQ error: %s (err=%d, prov_errno=%d)\n",
                    fi_strerror((int)-r), err.err, err.prov_errno);
            return -1;
        }
        usleep(100);
    }
}

/* ── Helper: Serialize one entry ──────────────────────────────────── */

static int serialize_entry(char *buf, size_t room, kv_entry_t *e) {
    /* Format: [klen:4][key][dlen:4][data][expire:8] */
    size_t need = 4 + e->key_len + 4 + e->val_len + 8;
    if (need > room) return -1;

    char *p = buf;
    *(uint32_t *)p = htonl(e->key_len); p += 4;
    memcpy(p, entry_key(e), e->key_len); p += e->key_len;
    *(uint32_t *)p = htonl(e->val_len); p += 4;
    memcpy(p, e->data, e->val_len); p += e->val_len;
    *(uint64_t *)p = htobe64((uint64_t)e->expire);

    return (int)need;
}

/* ── Child Process: Fullsync Main ─────────────────────────────────── */

int master_fullsync_child(uint64_t base_seq) {
    printf("[CHILD] Fullsync process started (pid=%d, base_seq=%lu)\n",
           getpid(), (unsigned long)base_seq);

    /* Step 1: Create independent libfabric resources */
    struct fi_info *hints = fi_allocinfo();
    if (!hints) {
        fprintf(stderr, "[CHILD] fi_allocinfo failed\n");
        return -1;
    }
    hints->caps = FI_MSG;
    hints->ep_attr->type = FI_EP_MSG;
    hints->mode = 0;
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED;

    struct fi_info *info = NULL;
    char service[16];
    snprintf(service, sizeof(service), "%d", CHILD_RDMA_PORT);

    int rc = fi_getinfo(FI_VERSION(1, 17), "0.0.0.0", service, FI_SOURCE, hints, &info);
    fi_freeinfo(hints);
    if (rc) {
        fprintf(stderr, "[CHILD] fi_getinfo: %s\n", fi_strerror(-rc));
        return -1;
    }

    struct fid_fabric *fabric = NULL;
    struct fid_eq *eq = NULL;
    struct fid_pep *pep = NULL;

    if (fi_fabric(info->fabric_attr, &fabric, NULL) ||
        fi_eq_open(fabric, &(struct fi_eq_attr){.wait_obj = FI_WAIT_UNSPEC}, &eq, NULL) ||
        fi_passive_ep(fabric, info, &pep, NULL) ||
        fi_pep_bind(pep, &eq->fid, 0) ||
        fi_listen(pep)) {
        fprintf(stderr, "[CHILD] Failed to setup passive EP\n");
        return -1;
    }

    printf("[CHILD] Listening on port %d\n", CHILD_RDMA_PORT);

    /* Step 2: Wait for slave connection */
    struct fi_eq_cm_entry entry;
    uint32_t event;
    ssize_t r = fi_eq_sread(eq, &event, &entry, sizeof(entry), 60000, 0);
    if (r < 0 || event != FI_CONNREQ) {
        fprintf(stderr, "[CHILD] No connection received\n");
        return -1;
    }

    struct fid_domain *domain = NULL;
    struct fid_ep *ep = NULL;
    struct fid_cq *cq = NULL;

    if (fi_domain(fabric, entry.info, &domain, NULL) ||
        fi_cq_open(domain, &(struct fi_cq_attr){
            .format = FI_CQ_FORMAT_DATA, .wait_obj = FI_WAIT_UNSPEC
        }, &cq, NULL) ||
        fi_endpoint(domain, entry.info, &ep, NULL) ||
        fi_ep_bind(ep, &eq->fid, 0) ||
        fi_ep_bind(ep, &cq->fid, FI_SEND | FI_RECV) ||
        fi_accept(ep, NULL, 0) ||
        fi_enable(ep)) {
        fprintf(stderr, "[CHILD] Failed to accept connection\n");
        return -1;
    }

    r = fi_eq_sread(eq, &event, &entry, sizeof(entry), 10000, 0);
    if (r < 0 || event != FI_CONNECTED) {
        fprintf(stderr, "[CHILD] Connection failed\n");
        return -1;
    }

    printf("[CHILD] Slave connected\n");

    /* Step 3: Allocate batch buffer */
    char *batch_buf = (char *)malloc(STREAM_BATCH_SIZE);
    if (!batch_buf) {
        fprintf(stderr, "[CHILD] malloc failed\n");
        return -1;
    }
    size_t batch_offset = 0;
    uint32_t total_entries = 0;
    uint64_t total_bytes = 0;

    struct timespec t1, t2;
    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Step 4: Count entries and write header */
    uint32_t entry_count = kv_store_count();
    printf("[CHILD] Total entries to send: %u\n", entry_count);

    /* Header: [magic:4][version:4][db_num:4][key_count:4][base_seq:8] = 24 bytes */
    uint32_t magic = htonl(0x52444232);  /* "RDB2" */
    uint32_t version = htonl(1);
    uint32_t db_num = htonl(0);
    uint32_t key_count = htonl(entry_count);
    uint64_t seq = htobe64(base_seq);

    memcpy(batch_buf + batch_offset, &magic, 4); batch_offset += 4;
    memcpy(batch_buf + batch_offset, &version, 4); batch_offset += 4;
    memcpy(batch_buf + batch_offset, &db_num, 4); batch_offset += 4;
    memcpy(batch_buf + batch_offset, &key_count, 4); batch_offset += 4;
    memcpy(batch_buf + batch_offset, &seq, 8); batch_offset += 8;

    /* Step 5: Iterate KV store and stream batches */
    kv_store_iter_t it;
    kv_store_iter_init(&it);
    kv_entry_t *e;

    while ((e = kv_store_iter_next(&it)) != NULL) {
        int n = serialize_entry(batch_buf + batch_offset,
                                STREAM_BATCH_SIZE - batch_offset, e);

        if (n < 0) {
            /* Batch full, send it */
            if (fi_send(ep, batch_buf, batch_offset, NULL, 0, NULL) < 0) {
                fprintf(stderr, "[CHILD] fi_send failed\n");
                free(batch_buf);
                return -1;
            }
            if (wait_cq(cq) < 0) {
                free(batch_buf);
                return -1;
            }

            total_bytes += batch_offset;
            batch_offset = 0;

            /* Retry serialization */
            n = serialize_entry(batch_buf + batch_offset,
                               STREAM_BATCH_SIZE - batch_offset, e);
            if (n < 0) {
                fprintf(stderr, "[CHILD] Entry too large\n");
                free(batch_buf);
                return -1;
            }
        }

        batch_offset += n;
        total_entries++;
    }
    kv_store_iter_free(&it);

    /* Step 6: Send last batch if any */
    if (batch_offset > 0) {
        if (fi_send(ep, batch_buf, batch_offset, NULL, 0, NULL) < 0) {
            fprintf(stderr, "[CHILD] fi_send (last batch) failed\n");
            free(batch_buf);
            return -1;
        }
        if (wait_cq(cq) < 0) {
            free(batch_buf);
            return -1;
        }
        total_bytes += batch_offset;
    }

    /* Step 7: Shutdown connection (EOF signal) */
    fi_shutdown(ep, FI_WRITE);
    
    clock_gettime(CLOCK_MONOTONIC, &t2);
    double ms = (t2.tv_sec - t1.tv_sec) * 1000.0 + (t2.tv_nsec - t1.tv_nsec) / 1e6;
    double mbps = (ms > 0) ? (total_bytes / (ms / 1000.0) / (1024.0 * 1024.0)) : 0;

    printf("[CHILD] Fullsync complete: %u entries, %lu bytes, %.1f ms, %.2f MB/s\n",
           total_entries, (unsigned long)total_bytes, ms, mbps);

    /* Step 8: Cleanup */
    free(batch_buf);
    fi_close(&ep->fid);
    fi_close(&cq->fid);
    fi_close(&domain->fid);
    fi_close(&pep->fid);
    fi_close(&eq->fid);
    fi_close(&fabric->fid);
    fi_freeinfo(info);

    return 0;
}
```

---

## 🔑 关键改进

### 1. 无控制消息
- ❌ 删除：`ctrl_msg_send(BATCH_ACK)`
- ❌ 删除：`ctrl_msg_recv(ACK)`
- ✅ 纯数据：连续 `fi_send()` 数据 batch

### 2. 连接关闭作为 EOF
- ❌ 删除：`ctrl_msg_send(EOF)`
- ✅ 使用：`fi_shutdown(ep, FI_WRITE)`

### 3. 简化流程
```
旧: send_header → send_data → wait_ack → repeat
新: send_data → send_data → ... → shutdown
```

---

## 📊 性能优化

### 动态 Batch 大小
```c
size_t batch_size;
size_t total_size = kv_store_size();

if (total_size < 100 * 1024 * 1024) {
    batch_size = 16 * 1024 * 1024;   /* 16MB */
} else if (total_size < 1024 * 1024 * 1024) {
    batch_size = 128 * 1024 * 1024;  /* 128MB */
} else {
    batch_size = 512 * 1024 * 1024;  /* 512MB */
}
```

---

**下一步**：阅读 `03-rdma_slave.md` 实现 Slave 的接收逻辑
