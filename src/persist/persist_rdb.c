/* =========================================================
 * persist_rdb.c — RDB save (fork + io_uring) and load (mmap).
 *
 * Depends on persistence.c for path utils, store_scan,
 * rdb_write_uring, mread, and global BGSAVE state.
 * ========================================================= */

#include "persistence.h"
#include "persist_internal.h"
#include "kvstore.h"
#include "kv_type_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <sys/mman.h>
#include <sys/stat.h>

/* ── RDB entry payload accessor ── */
int rdb_entry_payload(kv_entry_t *e, const char **data, int *dlen) {
    if (!e) return -1;
    if (entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT)
        return -1;
    *data = e->data;
    *dlen = (int)e->val_len;
    return 0;
}

/* ── Serialization callbacks ── */

/* Forward decl — defined after rdb_save_sync section */
static int *page_map_get(rdb_buf_t *b, uintptr_t page);

/* Serialize one STRING dict entry to RDB binary format (rdb_buf_t). */
static void rdb_save_callback_buf(kv_entry_t *e, void *priv) {
    char *owned = NULL;
    const char *data_ptr = NULL;
    int dlen = 0;
    uint32_t owned_len = 0;
    if (e && entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT) {
        if (hash_flatten_to_packed(e, &owned, &owned_len) != 0) return;
        data_ptr = owned;
        dlen = (int)owned_len;
    } else if (rdb_entry_payload(e, &data_ptr, &dlen) != 0) {
        return;
    }

    rdb_buf_t *b = (rdb_buf_t *)priv;
    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    long long expire = e->expire;

    /* P1: single reserve + direct memcpy instead of 5 rdb_buf_put calls */
    size_t total = sizeof(int) + (size_t)klen + sizeof(int) + (size_t)dlen + sizeof(long long);
    rdb_buf_reserve(b, total);
    char *dst = b->buf + b->len;
    memcpy(dst, &klen, sizeof(int)); dst += sizeof(int);
    memcpy(dst, key, (size_t)klen);  dst += klen;
    memcpy(dst, &dlen, sizeof(int)); dst += sizeof(int);
    memcpy(dst, data_ptr, (size_t)dlen); dst += dlen;
    memcpy(dst, &expire, sizeof(long long));
    b->len += total;

    /* P0: decrement page refcount, madvise when last entry on page done */
    uintptr_t page = ((uintptr_t)e) & ~(uintptr_t)0xFFF;
    int *cnt = page_map_get(b, page);
    if (cnt && --(*cnt) == 0)
        madvise((void *)page, 4096, MADV_DONTNEED);

    free(owned);
}

void serialize_entry_to_buf(DynBuf *out, kv_entry_t *e) {
    char *owned = NULL;
    const char *data_ptr = NULL;
    int dlen = 0;
    uint32_t owned_len = 0;
    if (e && entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT) {
        if (hash_flatten_to_packed(e, &owned, &owned_len) != 0) return;
        data_ptr = owned;
        dlen = (int)owned_len;
    } else if (rdb_entry_payload(e, &data_ptr, &dlen) != 0) {
        return;
    }

    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    long long expire = e->expire;

    dynbuf_append(out, (const char *)&klen, sizeof(int));
    dynbuf_append(out, key, klen);
    dynbuf_append(out, (const char *)&dlen, sizeof(int));
    dynbuf_append(out, data_ptr, dlen);
    dynbuf_append(out, (const char *)&expire, sizeof(long long));
    free(owned);
}

int serialize_entry_inline(char *dst, size_t max_space, kv_entry_t *e) {
    char *owned = NULL;
    const char *data_ptr = NULL;
    int dlen = 0;
    uint32_t owned_len = 0;
    if (e && entry_type(e) == KV_TYPE_HASH && entry_encoding(e) == KV_ENCODING_DICT) {
        if (hash_flatten_to_packed(e, &owned, &owned_len) != 0) return 0;
        data_ptr = owned;
        dlen = (int)owned_len;
    } else if (rdb_entry_payload(e, &data_ptr, &dlen) != 0) {
        return 0;
    }

    const char *key = entry_key(e);
    int klen = (int)e->key_len;
    long long expire = e->expire;
    int total = (int)(sizeof(int) + klen + sizeof(int) + dlen + sizeof(long long));
    if (total > (int)max_space) { free(owned); return -1; }

    char *q = dst;
    memcpy(q, &klen, sizeof(int)); q += sizeof(int);
    memcpy(q, key, klen);          q += klen;
    memcpy(q, &dlen, sizeof(int)); q += sizeof(int);
    memcpy(q, data_ptr, dlen);     q += dlen;
    memcpy(q, &expire, sizeof(long long));
    free(owned);
    return total;
}

/* DynBuf serialization callback (for RDMA fullsync). */
static void rdb_serialize_cb_dynbuf(kv_entry_t *e, void *priv) {
    if (!e) return;
    serialize_entry_to_buf((DynBuf *)priv, e);
}

/* Set to 1 inside the BGSAVE fork child (currently unused in single-dict mode) */
int g_rdb_in_fork_child = 0;

void rdb_serialize_to_buf(DynBuf *out) {
    int magic = 0x524442;
    int version = 2;
    int db_num = 0;
    int key_count = (int)kv_store_count();

    dynbuf_reserve(out, 16 + key_count * 64);

    dynbuf_append(out, (const char *)&magic,     sizeof(int));
    dynbuf_append(out, (const char *)&version,   sizeof(int));
    dynbuf_append(out, (const char *)&db_num,    sizeof(int));
    dynbuf_append(out, (const char *)&key_count, sizeof(int));

    store_scan(rdb_serialize_cb_dynbuf, out);
}


/* =============================================================================
 * RDB synchronous save (called from fork child or fallback path)
 * ============================================================================= */

#define PAGE_MAP_INIT_SIZE  16384   /* power of 2, ~17K pages for 1M entries */

/* Find or create a slot for |page| in the open-addressing hash map.
 * Returns pointer to the count field. */
static int *page_map_find_or_create(rdb_buf_t *b, uintptr_t page) {
    size_t mask = b->page_map_mask;
    size_t slot = (size_t)((page >> 12) * 11400714819323198549ULL) & mask;

    for (;;) {
        if (b->page_map[slot].page == page)
            return &b->page_map[slot].count;
        if (b->page_map[slot].page == 0)
            break;
        slot = (slot + 1) & mask;
    }

    /* check load factor, grow if >= 75% */
    size_t used = 0;
    for (size_t i = 0; i < b->page_map_size; i++)
        if (b->page_map[i].page) used++;

    if ((used + 1) * 4 >= b->page_map_size * 3 && b->page_map_size < (1 << 20)) {
        size_t old_sz = b->page_map_size;
        size_t new_sz = old_sz * 2;
        size_t new_mask = new_sz - 1;
        typeof(b->page_map) old = b->page_map;

        b->page_map = calloc(new_sz, sizeof(*b->page_map));
        if (b->page_map) {
            b->page_map_size = new_sz;
            b->page_map_mask = new_mask;
            for (size_t i = 0; i < old_sz; i++) {
                if (old[i].page) {
                    int *c = page_map_find_or_create(b, old[i].page);
                    *c = old[i].count;
                }
            }
        }
        free(old);
        if (!b->page_map) return NULL;

        /* re-find slot in new table */
        slot = (size_t)((page >> 12) * 11400714819323198549ULL) & b->page_map_mask;
        for (;;) {
            if (b->page_map[slot].page == page && b->page_map[slot].page)
                return &b->page_map[slot].count;
            if (b->page_map[slot].page == 0) break;
            slot = (slot + 1) & b->page_map_mask;
        }
    }

    b->page_map[slot].page = page;
    b->page_map[slot].count = 0;
    return &b->page_map[slot].count;
}

/* Look up count for |page| (must exist). Returns NULL if not found. */
static int *page_map_get(rdb_buf_t *b, uintptr_t page) {
    size_t mask = b->page_map_mask;
    size_t slot = (size_t)((page >> 12) * 11400714819323198549ULL) & mask;

    for (;;) {
        if (b->page_map[slot].page == page)
            return &b->page_map[slot].count;
        if (b->page_map[slot].page == 0)
            return NULL;
        slot = (slot + 1) & mask;
    }
}

/* Phase-1 callback: count entries per page (no allocation) */
static void rdb_pre_scan_cb(kv_entry_t *e, void *priv) {
    if (!e) return;
    rdb_buf_t *b = (rdb_buf_t *)priv;
    uintptr_t page = ((uintptr_t)e) & ~(uintptr_t)0xFFF;
    int *cnt = page_map_find_or_create(b, page);
    if (cnt) (*cnt)++;
}

void rdb_save_sync(void) {
    rdb_buf_t b = {0};

    int key_count = (int)kv_store_count();

    /* P0 Phase 1: pre-scan — count entries per page */
    b.page_map = calloc(PAGE_MAP_INIT_SIZE, sizeof(*b.page_map));
    if (b.page_map) {
        b.page_map_size = PAGE_MAP_INIT_SIZE;
        b.page_map_mask = PAGE_MAP_INIT_SIZE - 1;
        store_scan(rdb_pre_scan_cb, &b);
    }

    rdb_buf_reserve(&b, 16 + (size_t)key_count * 64);

    int magic = 0x524442;
    int version = 2;
    int db_num = 0;
    rdb_buf_put(&b, &magic, sizeof(int));
    rdb_buf_put(&b, &version, sizeof(int));
    rdb_buf_put(&b, &db_num, sizeof(int));
    rdb_buf_put(&b, &key_count, sizeof(int));

    /* P0 Phase 2: serialize with incremental MADV_DONTNEED */
    store_scan(rdb_save_callback_buf, &b);
    free(b.page_map);

    /* persist_path() returns a static buffer; copy before 2nd call */
    char tmp_path[1024], dst_path[1024];
    strncpy(tmp_path, persist_path(RDB_TMP), sizeof(tmp_path) - 1);
    tmp_path[sizeof(tmp_path) - 1] = '\0';
    strncpy(dst_path, persist_path(RDB_REL), sizeof(dst_path) - 1);
    dst_path[sizeof(dst_path) - 1] = '\0';
    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "[RDB] open tmp failed: %s\n", strerror(errno));
        free(b.buf);
        return;
    }

    rdb_write_uring(fd, b.buf, b.len);
    close(fd);
    rename(tmp_path, dst_path);

    free(b.buf);
    printf("[RDB] saved via io_uring, %zu bytes\n", b.len);
}


/* =============================================================================
 * RDB asynchronous save (fork + child writes via io_uring)
 * ============================================================================= */

/* Track the currently running BGSAVE child */
volatile pid_t g_rdb_child_pid = 0;

void rdb_save_async_uring(void) {
    static int installed = 0;
    if (!installed) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = rdb_sigchld;
        sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
        sigaction(SIGCHLD, &sa, NULL);
        installed = 1;
    }

    if (g_rdb_child_pid > 0) {
        printf("[RDB] BGSAVE already in progress (pid=%d), skipping\n",
               (int)g_rdb_child_pid);
        return;
    }

    long long t_lock = get_time_now_real();
    kv_store_lock_for_snapshot();
    long long t_locked = get_time_now_real();

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "[RDB] fork failed (%s), falling back to sync save\n",
                strerror(errno));
        kv_store_unlock_snapshot();
        rdb_save_sync();
        return;
    }
    if (pid == 0) {
        g_rdb_in_fork_child = 1;
        rdb_save_sync();
        fflush(stdout);
        fflush(stderr);
        _exit(0);
    }
    kv_store_unlock_snapshot();
    long long t_done = get_time_now_real();
    g_rdb_child_pid = pid;
    printf("[RDB] BGSAVE started, child pid=%d (snapshot stall: %lld ms acquire, %lld ms total)\n",
           (int)pid, t_locked - t_lock, t_done - t_lock);
}


/* =============================================================================
 * RDB load (mmap + streaming)
 * ============================================================================= */

int rdb_load_stream_begin(const char *buf, size_t len) {
    const char *p   = buf;
    const char *end = buf + len;

    int magic = 0, version = 0, db_num = 0, key_count = 0;
    if (!(p = mread(p, end, &magic,     sizeof(int)))) return -1;
    if (!(p = mread(p, end, &version,   sizeof(int)))) return -1;
    if (!(p = mread(p, end, &db_num,    sizeof(int)))) return -1;
    if (!(p = mread(p, end, &key_count, sizeof(int)))) return -1;

    if (magic != 0x524442) {
        printf("[RDB] stream load: magic mismatch (0x%x)\n", magic);
        return -1;
    }

    kvs_flush_all();
    printf("[RDB] stream begin: version=%d, expected keys=%d\n", version, key_count);
    return key_count;
}

int rdb_load_stream_append(const char *buf, size_t len) {
    const char *p   = buf;
    const char *end = buf + len;
    int loaded = 0;

    while (p < end) {
        int klen = 0, dlen = 0;
        long long expire = 0;

        if (!(p = mread(p, end, &klen, sizeof(int)))) break;
        if (p + klen > end) break;
        const char *ks = p;
        p += klen;

        if (!(p = mread(p, end, &dlen, sizeof(int)))) break;
        if (p + dlen > end) break;

        uint64_t h = kv_hash((const uint8_t *)ks, (uint32_t)klen);
        kv_entry_t *e = dict_add(kv_dict_for(h), ks, (uint32_t)klen, p, (uint32_t)dlen, h);
        p += dlen;

        if (!(p = mread(p, end, &expire, sizeof(long long)))) break;
        if (e && expire) e->expire = expire;
        if (e) hash_rebuild_encoding(e);
        loaded++;
    }

    return loaded;
}

void rdb_rebuild_all_encodings(void) {
    kv_store_iter_t it;
    kv_store_iter_init(&it);
    kv_entry_t *e;
    while ((e = kv_store_iter_next(&it)) != NULL)
        hash_rebuild_encoding(e);
    kv_store_iter_free(&it);
}

int rdb_load_from_buf(const char *buf, size_t len) {
    if (len < 16) return -1;

    int key_count = rdb_load_stream_begin(buf, 16);
    if (key_count < 0) return -1;

    int loaded = rdb_load_stream_append(buf + 16, len - 16);
    if (loaded < 0) { kv_store_clear(); return -1; }

    rdb_rebuild_all_encodings();

    printf("[RDB] buf load: %d keys, %zu bytes\n", loaded, len);
    return loaded;
}

void rdb_load_mmap(void) {
    int fd = open(persist_path(RDB_REL), O_RDONLY);
    if (fd < 0) {
        printf("[RDB] open failed\n");
        return;
    }
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) {
        close(fd);
        return;
    }
    size_t fsz = (size_t)st.st_size;
    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);

    if (base == MAP_FAILED) {
        printf("[RDB] mmap failed: %s, falling back to read()\n", strerror(errno));

        char *buf = malloc(fsz);
        if (!buf) { close(fd); return; }

        lseek(fd, 0, SEEK_SET);
        size_t total_read = 0;
        while (total_read < fsz) {
            ssize_t n = read(fd, buf + total_read, fsz - total_read);
            if (n <= 0) break;
            total_read += n;
        }
        close(fd);

        if (total_read == fsz) {
            rdb_load_from_buf(buf, fsz);
        } else {
            printf("[RDB] read failed: only read %zu/%zu bytes\n", total_read, fsz);
        }
        free(buf);
        return;
    }

    close(fd);
    madvise(base, fsz, MADV_SEQUENTIAL);

    rdb_load_from_buf(base, fsz);

    munmap(base, fsz);
}
