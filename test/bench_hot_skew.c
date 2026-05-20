/*
 * bench_hot_skew.c — End-to-end hot-skew benchmark for fastpath cache.
 *
 * Generates keys that all hash to the same shard, pre-populates them,
 * warms up the fastpath cache, then runs pipelined GET/SET benchmarks
 * at high throughput.  Compares fp=ON vs fp=OFF and reports the ratio.
 *
 * Build:  gcc -O2 -o build/bench_hot_skew test/bench_hot_skew.c src/engine/kv_dict.c -I src/engine -lpthread
 * Run:    ./build/bench_hot_skew [port]
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <stdint.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/* ---- kv_hash (copied from src/engine/kv_dict.c to avoid linking issues) ---- */
static uint64_t kv_hash_local(const uint8_t *key, uint32_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (uint32_t i = 0; i < len; i++) {
        h ^= key[i];
        h *= 1099511628211ULL;
    }
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

#define SHARD_MASK 31u
static inline uint32_t shard_of(uint64_t h) { return (uint32_t)((h >> 32) & SHARD_MASK); }

/* ---- tiny dynamic buffer ---- */
struct buf {
    char *data;
    int   len;
    int   cap;
};

static void buf_init(struct buf *b, int cap) {
    b->data = malloc((size_t)cap);
    b->len  = 0;
    b->cap  = cap;
}

static void buf_reserve(struct buf *b, int need) {
    if (b->len + need <= b->cap) return;
    while (b->len + need > b->cap) b->cap *= 2;
    b->data = realloc(b->data, (size_t)b->cap);
}

static void buf_cat(struct buf *b, const char *s, int n) {
    buf_reserve(b, n);
    memcpy(b->data + b->len, s, (size_t)n);
    b->len += n;
}

static void buf_reset(struct buf *b) { b->len = 0; }
static void buf_free(struct buf *b) { free(b->data); }

/* ---- key set ---- */
#define MAX_KEYS   128
#define MAX_KLEN    32
#define VALUE_BYTES 64

static char  g_keys[MAX_KEYS][MAX_KLEN];
static int   g_klens[MAX_KEYS];
static int    g_nkeys;
static int    g_target_shard;

static void build_same_shard_keys(int nkeys) {
    g_nkeys = nkeys;
    int collected = 0;
    int probe = 0;

    /* pick shard of first key */
    while (collected < nkeys) {
        char buf[32];
        int  blen = snprintf(buf, sizeof(buf), "hk:%08d", probe++);
        uint64_t h = kv_hash_local((const uint8_t *)buf, (uint32_t)blen);
        uint32_t shard = shard_of(h);
        if (collected == 0) g_target_shard = (int)shard;
        if ((int)shard != g_target_shard) continue;
        memcpy(g_keys[collected], buf, (size_t)blen + 1);
        g_klens[collected] = blen;
        collected++;
    }
    printf("  Generated %d keys on shard %d (probed %d candidates)\n",
           nkeys, g_target_shard, probe);
}

/* ---- socket helpers ---- */

static int tcp_connect(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        perror("inet_pton"); close(fd); return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect"); close(fd); return -1;
    }
    return fd;
}

/* Read exactly n bytes (blocking). */
static int read_exact(int fd, char *buf, int n) {
    int off = 0;
    while (off < n) {
        int r = (int)read(fd, buf + off, (size_t)(n - off));
        if (r <= 0) return -1;
        off += r;
    }
    return 0;
}

/* Read one RESP line up to \r\n, return number of bytes including the \r\n. */
static int read_line(int fd, char *buf, int cap) {
    int off = 0;
    while (off < cap - 1) {
        if (read_exact(fd, buf + off, 1) < 0) return -1;
        if (off >= 1 && buf[off-1] == '\r' && buf[off] == '\n') return off + 1;
        off++;
    }
    return -1;
}

/* ---- RESP helpers ---- */

static int skip_resp_reply(int fd, int count) {
    for (int i = 0; i < count; i++) {
        char line[256];
        int n = read_line(fd, line, sizeof(line));
        if (n < 2) return -1;
        if (line[0] == '$') {
            int blen = atoi(line + 1);
            if (blen < 0) continue;
            char tmp[1024];
            char crlf[2];
            if (read_exact(fd, tmp, blen) < 0) return -1;
            if (read_exact(fd, crlf, 2) < 0) return -1;
        }
    }
    return 0;
}

/* ---- pipelining helpers ---- */

/* Send all buffered data */
static int send_all(int fd, const char *data, int len) {
    int off = 0;
    while (off < len) {
        int w = (int)write(fd, data + off, (size_t)(len - off));
        if (w <= 0) return -1;
        off += w;
    }
    return 0;
}

/* Build pipeline: N "GET <key>\r\n" commands */
static void build_get_pipeline(struct buf *b, int count) {
    buf_reset(b);
    for (int i = 0; i < count; i++) {
        int idx = i % g_nkeys;
        buf_cat(b, "*2\r\n$3\r\nGET\r\n", 14);
        char slen[16];
        int  sl = snprintf(slen, sizeof(slen), "$%d\r\n", g_klens[idx]);
        buf_cat(b, slen, sl);
        buf_cat(b, g_keys[idx], g_klens[idx]);
        buf_cat(b, "\r\n", 2);
    }
}

/* Build pipeline: N "SET <key> <value>\r\n" commands */
static void build_set_pipeline(struct buf *b, int count, const char *val, int vlen) {
    buf_reset(b);
    char vhdr[32];
    int  vh = snprintf(vhdr, sizeof(vhdr), "$%d\r\n", vlen);
    for (int i = 0; i < count; i++) {
        int idx = i % g_nkeys;
        buf_cat(b, "*3\r\n$3\r\nSET\r\n", 14);
        char slen[16];
        int  sl = snprintf(slen, sizeof(slen), "$%d\r\n", g_klens[idx]);
        buf_cat(b, slen, sl);
        buf_cat(b, g_keys[idx], g_klens[idx]);
        buf_cat(b, "\r\n", 2);
        buf_cat(b, vhdr, vh);
        buf_cat(b, val, vlen);
        buf_cat(b, "\r\n", 2);
    }
}

/* Write a single SET command to populate one key */
static void send_set(int fd, const char *key, int klen, const char *val, int vlen) {
    char buf[512];
    int off = 0;

    #define APPEND(s, n) do { memcpy(buf+off, s, n); off += n; } while(0)

    APPEND("*3\r\n$3\r\nSET\r\n", 14);
    int hl = snprintf(buf + off, sizeof(buf) - off, "$%d\r\n", klen);
    off += hl;
    APPEND(key, klen);
    APPEND("\r\n", 2);
    hl = snprintf(buf + off, sizeof(buf) - off, "$%d\r\n", vlen);
    off += hl;
    APPEND(val, vlen);
    APPEND("\r\n", 2);

    #undef APPEND

    send_all(fd, buf, off);
}

/* ---- INFO parser: extract fastpath stats ---- */

static uint64_t extract_stat(const char *resp, const char *key) {
    const char *p = strstr(resp, key);
    if (!p) return 0;
    p += strlen(key);
    while (*p == ':' || *p == ' ') p++;
    return (uint64_t)atoll(p);
}

/* ---- Main benchmark ---- */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

typedef struct {
    double qps;
    double wall_sec;
    int    total_ops;
    uint64_t fp_hits;
    uint64_t fp_misses;
    double fp_hit_pct;
} result_t;

/* Run GET benchmark */
static result_t bench_get(int fd, int pipeline_size, int total_ops) {
    struct buf req_buf;
    buf_init(&req_buf, 1024*1024);

    /* warmup: one round of pipeline */
    build_get_pipeline(&req_buf, pipeline_size);
    send_all(fd, req_buf.data, req_buf.len);
    if (skip_resp_reply(fd, pipeline_size) < 0) {
        fprintf(stderr, "  warmup failed\n");
        result_t r = {0}; return r;
    }

    /* timed runs */
    double t0 = now_sec();
    for (int sent = 0; sent < total_ops; sent += pipeline_size) {
        send_all(fd, req_buf.data, req_buf.len);
        if (skip_resp_reply(fd, pipeline_size) < 0) {
            fprintf(stderr, "  bench failed at %d ops\n", sent);
            break;
        }
    }
    double t1 = now_sec();

    buf_free(&req_buf);

    result_t r;
    r.wall_sec  = t1 - t0;
    r.total_ops = total_ops;
    r.qps       = (double)total_ops / r.wall_sec;
    return r;
}

/* Run SET benchmark */
static result_t bench_set(int fd, int pipeline_size, int total_ops, const char *val, int vlen) {
    struct buf req_buf;
    buf_init(&req_buf, 1024*1024);

    build_set_pipeline(&req_buf, pipeline_size, val, vlen);

    /* warmup: one round */
    send_all(fd, req_buf.data, req_buf.len);
    if (skip_resp_reply(fd, pipeline_size) < 0) {
        fprintf(stderr, "  warmup failed\n");
        result_t r = {0}; return r;
    }

    double t0 = now_sec();
    for (int sent = 0; sent < total_ops; sent += pipeline_size) {
        send_all(fd, req_buf.data, req_buf.len);
        if (skip_resp_reply(fd, pipeline_size) < 0) {
            fprintf(stderr, "  bench failed at %d ops\n", sent);
            break;
        }
    }
    double t1 = now_sec();

    buf_free(&req_buf);

    result_t r;
    r.wall_sec  = t1 - t0;
    r.total_ops = total_ops;
    r.qps       = (double)total_ops / r.wall_sec;
    return r;
}

/* Get INFO fastpath stats */
static void query_fp_stats(int fd, uint64_t *hits, uint64_t *misses) {
    /* Send INFO command */
    const char *cmd = "*1\r\n$4\r\nINFO\r\n";
    if (send_all(fd, cmd, (int)strlen(cmd)) < 0) return;

    /* RESP: $<len>\r\n<data>\r\n */
    char line[256];
    int n = read_line(fd, line, sizeof(line));
    if (n < 2 || line[0] != '$') return;

    int blen = atoi(line + 1);
    if (blen <= 0 || blen > 65536) return;

    char *info_resp = malloc((size_t)blen + 1);
    if (read_exact(fd, info_resp, blen) < 0) { free(info_resp); return; }
    info_resp[blen] = '\0';

    /* skip trailing \r\n */
    char crlf[2];
    read_exact(fd, crlf, 2);

    *hits   = extract_stat(info_resp, "fastpath_cache_hits");
    *misses = extract_stat(info_resp, "fastpath_cache_misses");

    free(info_resp);
}

/* ---- main ---- */

int main(int argc, char **argv) {
    int port        = argc > 1 ? atoi(argv[1]) : 2000;
    int nkeys       = argc > 2 ? atoi(argv[2]) : 64;
    int pipeline    = argc > 3 ? atoi(argv[3]) : 256;
    int total_ops   = argc > 4 ? atoi(argv[4]) : 500000;

    if (nkeys < 1 || nkeys > MAX_KEYS) nkeys = 64;
    if (pipeline < 1 || pipeline > 10000) pipeline = 256;
    if (total_ops < pipeline) total_ops = pipeline * 4;

    printf("== bench_hot_skew ==\n");
    printf("  port         = %d\n", port);
    printf("  nkeys        = %d\n", nkeys);
    printf("  pipeline     = %d\n", pipeline);
    printf("  total_ops    = %d\n", total_ops);
    printf("  value_bytes  = %d\n", VALUE_BYTES);

    build_same_shard_keys(nkeys);

    /* Generate value */
    char *value = malloc((size_t)VALUE_BYTES);
    for (int i = 0; i < VALUE_BYTES; i++) value[i] = (char)('a' + (i % 26));

    /* ---- Populate ---- */
    printf("\n--- Population ---\n");
    int pop_fd = tcp_connect("127.0.0.1", port);
    if (pop_fd < 0) {
        fprintf(stderr, "Cannot connect to server at port %d. Is it running?\n", port);
        free(value);
        return 1;
    }

    struct buf pop_pipe;
    buf_init(&pop_pipe, 256*1024);
    for (int i = 0; i < nkeys; i++) {
        send_set(pop_fd, g_keys[i], g_klens[i], value, VALUE_BYTES);
    }
    /* Read replies */
    for (int i = 0; i < nkeys; i++) {
        char line[256];
        if (read_line(pop_fd, line, sizeof(line)) < 2) {
            fprintf(stderr, "  Populate SET %d failed\n", i);
            close(pop_fd);
            goto cleanup;
        }
    }
    printf("  Populated %d keys\n", nkeys);

    /* Warm up fastpath: several rounds of GET */
    printf("\n--- Fastpath Warmup ---\n");
    buf_init(&pop_pipe, 256*1024);
    build_get_pipeline(&pop_pipe, nkeys);
    for (int w = 0; w < 4; w++) {
        send_all(pop_fd, pop_pipe.data, pop_pipe.len);
        skip_resp_reply(pop_fd, nkeys);
    }
    printf("  Warmup done (4 rounds of %d GETs)\n", nkeys);

    /* Check fastpath status */
    uint64_t fp_h, fp_m;
    query_fp_stats(pop_fd, &fp_h, &fp_m);
    printf("  After warmup — fp_hits=%lu fp_misses=%lu (%.1f%% hit)\n",
           (unsigned long)fp_h, (unsigned long)fp_m,
           (fp_h + fp_m) ? 100.0 * (double)fp_h / (double)(fp_h + fp_m) : 0.0);

    close(pop_fd);

    /* ---- Benchmark: GET ---- */
    printf("\n--- GET Benchmark (pipeline=%d, %d total ops) ---\n",
           pipeline, total_ops);

    int bench_fd = tcp_connect("127.0.0.1", port);
    if (bench_fd < 0) {
        fprintf(stderr, "Cannot connect for benchmark\n");
        goto cleanup;
    }

    result_t r_get = bench_get(bench_fd, pipeline, total_ops);
    printf("  GET: %.2f QPS  (%.3f s for %d ops)\n",
           r_get.qps, r_get.wall_sec, r_get.total_ops);

    /* Check final stats */
    query_fp_stats(bench_fd, &fp_h, &fp_m);
    printf("  fp_hits=%lu  fp_misses=%lu  hit_rate=%.1f%%\n",
           (unsigned long)fp_h, (unsigned long)fp_m,
           (fp_h + fp_m) ? 100.0 * (double)fp_h / (double)(fp_h + fp_m) : 0.0);

    close(bench_fd);

    /* ---- Benchmark: SET ---- */
    printf("\n--- SET Benchmark (pipeline=%d, %d total ops) ---\n",
           pipeline, total_ops);

    bench_fd = tcp_connect("127.0.0.1", port);
    if (bench_fd < 0) {
        fprintf(stderr, "Cannot connect for SET benchmark\n");
        goto cleanup;
    }

    result_t r_set = bench_set(bench_fd, pipeline, total_ops, value, VALUE_BYTES);
    printf("  SET: %.2f QPS  (%.3f s for %d ops)\n",
           r_set.qps, r_set.wall_sec, r_set.total_ops);

    close(bench_fd);

    /* ---- Summary ---- */
    printf("\n== Summary (all keys on shard %d) ==\n", g_target_shard);
    printf("  GET: %10.2f QPS\n", r_get.qps);
    printf("  SET: %10.2f QPS\n", r_set.qps);

cleanup:
    buf_free(&pop_pipe);
    free(value);
    return 0;
}
