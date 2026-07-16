/* test_xdp_reasm.c — XDP reassembly unit + integration tests
 *
 * Tests the full pipeline: seq dedup/ordering → RESP extract → write filter → callback.
 *
 * Coverage:
 *   1. In-order delivery      6.  Pipeline commands     11. Multi-conn
 *   2. Out-of-order + drain   7.  Large value (multi-pkt) 12. RESP partial
 *   3. Duplicate discard      8.  Mid-stream join         13. Read filter
 *   4. Overlapping trim       9.  SYN/FIN/RST             14. Conn table full
 *   5. Gap + late fill       10.  TCP Fast Open           15. Complex OOO (5 segs)
 *
 * Build: gcc -O2 -g -o build/test_xdp_reasm test/test_xdp_reasm.c
 *             -Isrc/repl -I/usr/include -lbpf -lelf -lz -lpthread
 */

#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <linux/if_link.h>

/* ── Types from repl_xdp_common.h / repl_xdp_reasm.h ─────────────── */

#define XDP_MAX_PAYLOAD   4096
#define XDP_RINGBUF_SZ    (8 * 1024 * 1024)
#define XDP_FLAG_SYN      0x01
#define XDP_FLAG_FIN      0x02
#define XDP_FLAG_RST      0x04

struct xdp_repl_event {
    uint32_t saddr, daddr;
    uint16_t sport, dport;
    uint32_t seq;
    uint16_t data_len;
    uint8_t  flags, pad;
    uint8_t  data[XDP_MAX_PAYLOAD];
};

typedef void (*xdp_reasm_write_cb)(const uint8_t *cmd, uint32_t cmd_len);

struct xdp_reasm_stats {
    uint64_t events_total, events_data, events_syn, events_fin_rst;
    uint64_t bytes_ordered, cmds_extracted, cmds_written;
    uint32_t conns_active;
    uint64_t ooo_segments, dup_discarded;
};

typedef struct {
    int             use_netlink;
    struct bpf_link *link;
    int             ifindex, prog_fd;
} xdp_attach_t;

/* Declared in repl_tcp_internal.h, defined below */
static int find_complete_resp_command(const uint8_t *data, size_t len);
static int is_write_command(const uint8_t *data, size_t len);

/* ── Include actual source ───────────────────────────────────────── */

#define REPL_XDP_COMMON_H
#define REPL_XDP_REASM_H
#define REPL_TCP_INTERNAL_H
#include "../src/repl/repl_xdp_reasm.c"

/* ── RESP parser + write filter ───────────────────────────────────── */

#define MAX_RESP_CMD_LEN  (4 * 1024 * 1024)

static int find_complete_resp_command(const uint8_t *data, size_t len) {
    if (len < 4 || data[0] != '*') return -1;
    size_t i = 1; int argc = 0;
    while (i < len && data[i] >= '0' && data[i] <= '9') {
        argc = argc * 10 + (data[i] - '0');
        if (argc > 64) return -1;
        i++;
    }
    if (i == 1) return -1;
    if (i + 1 >= len) return 0;
    if (data[i] != '\r' || data[i + 1] != '\n') return -1;
    i += 2;
    for (int arg = 0; arg < argc; arg++) {
        if (i >= len) return 0;
        if (data[i] != '$') return -1;
        i++;
        int arg_len = 0;
        while (i < len && data[i] >= '0' && data[i] <= '9') {
            arg_len = arg_len * 10 + (data[i] - '0');
            if (arg_len > MAX_RESP_CMD_LEN) return -1;
            i++;
        }
        if (i + 1 >= len) return 0;
        if (data[i] != '\r' || data[i + 1] != '\n') return -1;
        i += 2;
        if (i + (size_t)arg_len + 2 > len) return 0;
        i += (size_t)arg_len + 2;
    }
    return (int)i;
}

static int is_write_command(const uint8_t *data, size_t len) {
    if (!data || len < 12 || data[0] != '*') return 0;
    size_t i = 1;
    while (i < len && data[i] >= '0' && data[i] <= '9') i++;
    if (i + 1 >= len || data[i] != '\r' || data[i + 1] != '\n') return 0;
    i += 2;
    if (i >= len || data[i] != '$') return 0; i++;
    while (i < len && data[i] >= '0' && data[i] <= '9') i++;
    if (i + 1 >= len || data[i] != '\r' || data[i + 1] != '\n') return 0;
    i += 2;
    if (i + 2 >= len) return 0;
    char c0 = (char)data[i], c1 = (char)data[i+1], c2 = (char)data[i+2];
    if (c0 >= 'a' && c0 <= 'z') c0 -= 32;
    if (c1 >= 'a' && c1 <= 'z') c1 -= 32;
    if (c2 >= 'a' && c2 <= 'z') c2 -= 32;
    if (c0 == 'S' && c1 == 'E' && c2 == 'T') return 1;
    if (c0 == 'D' && c1 == 'E' && c2 == 'L') return 1;
    if (c0 == 'H' && (c1 == 'S' || c1 == 'D')) return 1;
    if (c0 == 'L' && c1 == 'P') return 1;
    if (c0 == 'I' && c1 == 'N' && c2 == 'C') return 1;
    if (c0 == 'M' && c1 == 'S' && c2 == 'E') return 1;
    if (c0 == 'F' && c1 == 'L' && c2 == 'U') return 1;
    return 0;
}

/* ── Test harness ─────────────────────────────────────────────────── */

static int   g_ok  = 0, g_fail = 0;
static int   g_cb_n = 0, g_cb_len = 0;
static char  g_cb_buf[65536];

#define TASSERT(cond, fmt, ...) do { \
    if (!(cond)) { \
        g_fail++; \
        fprintf(stderr, "  FAIL L%d: " fmt "\n", __LINE__, ##__VA_ARGS__); \
    } else g_ok++; \
} while(0)

static void test_cb(const uint8_t *cmd, uint32_t clen) {
    if (g_cb_len + clen < (int)sizeof(g_cb_buf)) {
        memcpy(g_cb_buf + g_cb_len, cmd, clen);
        g_cb_len += clen;
    }
    g_cb_n++;
}

static void reset(void) {
    xdp_reasm_shutdown();
    memset(g_cb_buf, 0, sizeof(g_cb_buf));
    g_cb_n = g_cb_len = 0;
    xdp_reasm_init(test_cb);
}

#define MKADDR 0x7f000001
#define MKPORT 19999

/* Compound-literal macros (lvalues, can take address) */
#define EV(sa,sp,sq,d,len,fl) \
    (&(struct xdp_repl_event){ \
        .saddr = htonl(sa), .daddr = htonl(MKADDR), \
        .sport = htons(sp), .dport = htons(MKPORT), \
        .seq = htonl(sq), .data_len = (len), .flags = (fl), \
        .data = "" /* will be memcpy'd below */ \
    })

/* Pump: copy data into a local struct, then feed to reasm */
static void pump(uint32_t sa, uint16_t sp, uint32_t seq,
                 const char *d, int len, uint8_t flags) {
    struct xdp_repl_event e = {0};
    e.saddr    = htonl(sa);
    e.daddr    = htonl(MKADDR);
    e.sport    = htons(sp);
    e.dport    = htons(MKPORT);
    e.seq      = htonl(seq);
    e.data_len = (uint16_t)len;
    e.flags    = flags;
    if (d && len > 0 && len <= XDP_MAX_PAYLOAD) memcpy(e.data, d, len);
    xdp_reasm_on_event(NULL, &e, sizeof(e));
}

#define PS(sa,sp,sq,s,fl)   pump(sa,sp,sq,s,strlen(s),fl)
#define PB(sa,sp,sq,d,l,fl) pump(sa,sp,sq,d,l,fl)

/* Pump a pre-built event struct (for complex tests) */
static void pump_raw(struct xdp_repl_event *e) {
    xdp_reasm_on_event(NULL, e, sizeof(*e));
}

/* Standard SET command = 33 bytes */
static const char *CMD_SET =
    "*3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n";

/* ── TESTS ────────────────────────────────────────────────────────── */

/* T1: Basic in-order */
static void t1_inorder(void) {
    reset();
    PS(0x7f000001, 10001, 1001, CMD_SET, 0);
    TASSERT(g_cb_n == 1, "expected 1 cb, got %d", g_cb_n);
    printf("  %s in-order\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T2: Duplicate discard */
static void t2_dup(void) {
    reset();
    PS(0x7f000001, 10002, 1001, CMD_SET, 0);
    PS(0x7f000001, 10002, 1001, CMD_SET, 0);
    TASSERT(g_cb_n == 1, "dup: exp 1, got %d", g_cb_n);
    printf("  %s duplicate\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T3: SYN ISN + data */
static void t3_syn(void) {
    reset();
    PB(0x7f000001, 10003, 5000, NULL, 0, XDP_FLAG_SYN);
    PS(0x7f000001, 10003, 5001, CMD_SET, 0);
    TASSERT(g_cb_n == 1, "syn: exp 1, got %d", g_cb_n);
    printf("  %s SYN ISN\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T4: SYN with data (TCP Fast Open) */
static void t4_tfo(void) {
    reset();
    PS(0x7f000001, 10004, 5000, CMD_SET, XDP_FLAG_SYN);
    /* ISN=5000, rcv_nxt=5001. Payload at seq 5001 → in-order */
    TASSERT(g_cb_n == 1, "tfo: exp 1, got %d", g_cb_n);
    printf("  %s TCP Fast Open\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T5: Out-of-order 2 parts — part 2 arrives first */
static void t5_ooo(void) {
    reset();
    uint32_t sa = 0x7f000001; uint16_t sp = 10005;
    int n = strlen(CMD_SET), mid = n / 2;

    PB(sa, sp, 1000, NULL, 0, XDP_FLAG_SYN); /* ISN=1000 */

    /* part2 first: seq 1001+mid, OOO */
    PB(sa, sp, 1001 + mid,
                                   CMD_SET + mid, n - mid, 0);
    TASSERT(g_cb_n == 0, "ooo: p2 alone no cb, got %d", g_cb_n);

    /* part1: seq 1001, fills gap, triggers drain */
    PB(sa, sp, 1001,
                                   CMD_SET, mid, 0);
    TASSERT(g_cb_n == 1, "ooo: after fill, exp 1, got %d", g_cb_n);
    printf("  %s out-of-order 2-parts\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T6: Overlap — partial overlap trimmed */
static void t6_overlap(void) {
    reset();
    uint32_t sa = 0x7f000001; uint16_t sp = 10006;

    PB(sa, sp, 1000, NULL, 0, XDP_FLAG_SYN);
    PB(sa, sp, 1001, CMD_SET, 20, 0);   /* 20 bytes */
    TASSERT(g_cb_n == 0, "overlap: incomplete, got %d", g_cb_n);

    /* Bytes 12-32 (21B), seq 1013 — overlaps bytes 12-19 (8B trimmed) */
    PB(sa, sp, 1013, CMD_SET + 12, 21, 0);
    TASSERT(g_cb_n == 1, "overlap: complete, exp 1, got %d", g_cb_n);
    printf("  %s overlap trim\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T7: Pipeline — 3 commands in one segment */
static void t7_pipeline(void) {
    reset();
    const char *cmds =
        "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
        "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
        "*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n";
    PB(0x7f000001, 10007, 1001, cmds, strlen(cmds), 0);
    TASSERT(g_cb_n == 3, "pipeline: exp 3, got %d", g_cb_n);
    printf("  %s pipeline (3-in-1)\n", g_cb_n == 3 ? "PASS:" : "FAIL:");
}

/* T8: Large value spanning 3 packets, delivered OOO: 3,2,1 */
static void t8_large(void) {
    reset();
    uint32_t sa = 0x7f000001; uint16_t sp = 10008;
    PB(sa, sp, 1000, NULL, 0, XDP_FLAG_SYN);

    static char cmd[7000];
    int hl = snprintf(cmd, sizeof(cmd),
        "*3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$6000\r\n");
    for (int i = 0; i < 6000; i++) cmd[hl++] = 'A' + (i % 26);
    cmd[hl++] = '\r'; cmd[hl++] = '\n';
    int total = hl;

    int c1 = 2100, c2 = 2100, c3 = total - c1 - c2;

    /* Build events in advance, deliver OOO: 3, 2, 1 */
    struct xdp_repl_event e1 = {
        .saddr = htonl(sa), .daddr = htonl(0x7f000001),
        .sport = htons(sp), .dport = htons(19999),
        .seq   = htonl(1001), .data_len = c1, .flags = 0
    };
    struct xdp_repl_event e2 = e1;
    e2.seq = htonl(1001 + c1); e2.data_len = c2;
    struct xdp_repl_event e3 = e1;
    e3.seq = htonl(1001 + c1 + c2); e3.data_len = c3;
    memcpy(e1.data, cmd, c1);
    memcpy(e2.data, cmd + c1, c2);
    memcpy(e3.data, cmd + c1 + c2, c3);

    pump_raw(&e3); pump_raw(&e2);
    TASSERT(g_cb_n == 0, "large: ooo no cb yet, got %d", g_cb_n);
    pump_raw(&e1);
    TASSERT(g_cb_n == 1, "large: exp 1 cb, got %d", g_cb_n);
    printf("  %s large value OOO (%dB, 3pkts)\n",
           g_cb_n == 1 ? "PASS:" : "FAIL:", total);
}

/* T9: Mid-stream join — no SYN, estimate ISN */
static void t9_midstream(void) {
    reset();
    PS(0x7f000001, 10009, 5001, CMD_SET, 0);
    TASSERT(g_cb_n == 1, "midstream: exp 1, got %d", g_cb_n);
    printf("  %s mid-stream join\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T10: FIN with trailing data */
static void t10_fin(void) {
    reset();
    PB(0x7f000001, 10010, 1000, NULL, 0, XDP_FLAG_SYN);
    PS(0x7f000001, 10010, 1001, CMD_SET, 0);
    TASSERT(g_cb_n == 1, "fin: first cmd, exp 1, got %d", g_cb_n);

    /* FIN seq=1001+33=1034, with trailing DEL command */
    const char *del = "*2\r\n$3\r\nDEL\r\n$3\r\nKEY\r\n";
    int dellen = strlen(del);
    /* FIN byte = 1 seq number, then data */
    PB(0x7f000001, 10010, 1034, del, dellen, XDP_FLAG_FIN);
    TASSERT(g_cb_n == 2, "fin: +DEL cmd, exp 2 total, got %d", g_cb_n);
    printf("  %s FIN + data\n", g_cb_n == 2 ? "PASS:" : "FAIL:");
}

/* T11: Multi-connection isolation */
static void t11_multicon(void) {
    reset();
    PS(0x7f000001, 20001, 1001, CMD_SET, 0);
    PS(0x7f000001, 20002, 2001, CMD_SET, 0);
    TASSERT(g_cb_n == 2, "multi: exp 2, got %d", g_cb_n);

    /* conn A: second cmd at seq 1001+33=1034 */
    PS(0x7f000001, 20001, 1034,
        "*2\r\n$3\r\nDEL\r\n$3\r\nKEY\r\n", 0);
    TASSERT(g_cb_n == 3, "multi: A2, exp 3, got %d", g_cb_n);
    printf("  %s multi-connection\n", g_cb_n == 3 ? "PASS:" : "FAIL:");
}

/* T12: Connection table at capacity */
static void t12_tablefull(void) {
    reset();
    int i;
    for (i = 0; i < 65; i++)
        PS(0x7f000001, (uint16_t)(30000 + i), 1001, CMD_SET, 0);
    TASSERT(g_cb_n >= 64, "full: exp >=64, got %d", g_cb_n);
    printf("  %s conn table full (%d/65)\n",
           g_cb_n >= 64 ? "PASS:" : "FAIL:", g_cb_n);
}

/* T13: RESP partial → wait → complete */
static void t13_partial(void) {
    reset();
    PB(0x7f000001, 10013, 1001, CMD_SET, 15, 0);
    TASSERT(g_cb_n == 0, "partial: no cb yet, got %d", g_cb_n);
    PB(0x7f000001, 10013, 1016, CMD_SET + 15, 18, 0);
    TASSERT(g_cb_n == 1, "partial: complete, exp 1, got %d", g_cb_n);
    printf("  %s RESP partial\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T14: Read command filtered */
static void t14_readfilter(void) {
    reset();
    PS(0x7f000001, 10014, 1001,
        "*2\r\n$3\r\nGET\r\n$3\r\nKEY\r\n", 0);
    TASSERT(g_cb_n == 0, "read: GET filtered, got %d", g_cb_n);
    printf("  %s read filter\n", g_cb_n == 0 ? "PASS:" : "FAIL:");
}

/* T15: RST flag */
static void t15_rst(void) {
    reset();
    PB(0x7f000001, 10015, 1001, NULL, 0, XDP_FLAG_RST);
    PB(0x7f000001, 10015, 1001, NULL, 0, XDP_FLAG_RST);
    printf("  PASS: RST no-crash\n");
}

/* T16: Empty payload + GC no-crash */
static void t16_empty_gc(void) {
    reset();
    PB(0x7f000001, 10016, 1001, NULL, 0, 0);
    xdp_reasm_gc();
    printf("  PASS: empty + GC no-crash\n");
}

/* T17: Complex OOO — 5 segments, reversed order */
static void t17_complex_ooo(void) {
    reset();
    uint32_t sa = 0x7f000001; uint16_t sp = 10017;

    PB(sa, sp, 1000, NULL, 0, XDP_FLAG_SYN);

    /* Build 5 events covering CMD_SET (33B), split [7,7,7,6,6] */
    int sz[5] = {7, 7, 7, 6, 6};
    uint32_t off = 0;
    struct xdp_repl_event se[5];
    for (int i = 0; i < 5; i++) {
        memset(&se[i], 0, sizeof(se[i]));
        se[i].saddr    = htonl(sa);
        se[i].daddr    = htonl(0x7f000001);
        se[i].sport    = htons(sp);
        se[i].dport    = htons(19999);
        se[i].seq      = htonl(1001 + off);
        se[i].data_len = (uint16_t)sz[i];
        se[i].flags    = 0;
        memcpy(se[i].data, CMD_SET + off, sz[i]);
        off += sz[i];
    }

    /* Deliver: 5,3,4,2,1 */
    pump_raw(&se[4]); pump_raw(&se[2]); pump_raw(&se[3]);
    pump_raw(&se[1]); pump_raw(&se[0]);

    TASSERT(g_cb_n == 1, "complex: exp 1, got %d", g_cb_n);
    printf("  %s complex OOO (5 segs)\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* T18: Exact duplicate with SYN */
static void t18_dup_syn(void) {
    reset();
    PB(0x7f000001, 10018, 5000, NULL, 0, XDP_FLAG_SYN);
    PS(0x7f000001, 10018, 5001, CMD_SET, 0);
    /* Duplicate SYN: already known, shouldn't reset rcv_nxt */
    PB(0x7f000001, 10018, 5000, NULL, 0, XDP_FLAG_SYN);
    /* Duplicate data */
    PS(0x7f000001, 10018, 5001, CMD_SET, 0);
    TASSERT(g_cb_n == 1, "dup-syn: exp 1, got %d", g_cb_n);
    printf("  %s duplicate SYN+data\n", g_cb_n == 1 ? "PASS:" : "FAIL:");
}

/* ── Main ─────────────────────────────────────────────────────────── */

int main(void) {
    printf("=== XDP Reassembly Tests ===\n\n");

    t1_inorder();
    t2_dup();
    t3_syn();
    t4_tfo();
    t5_ooo();
    t6_overlap();
    t7_pipeline();
    t8_large();
    t9_midstream();
    t10_fin();
    t11_multicon();
    t12_tablefull();
    t13_partial();
    t14_readfilter();
    t15_rst();
    t16_empty_gc();
    t17_complex_ooo();
    t18_dup_syn();

    xdp_reasm_shutdown();

    int total = g_ok + g_fail;
    printf("\n=== %d/%d passed ===\n", g_ok, total);
    return g_fail > 0 ? 1 : 0;
}
