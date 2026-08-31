/*=============================================================================
 *  testcase.c — kvstore 功能测试客户端
 *
 *  极简设计（参考 inazuma-kv/tests/testcase.c）：
 *  - 命令统一走 RESP 报文（resp_cmd 构造），收完整 RESP 回复，strcmp 整串比较
 *  - exec_cmd / exec_cmd_prefix：发一条命令并校验回复（单命令场景）
 *  - expect_reply / expect_reply_prefix：只校验已收到的一条回复（批处理场景）
 *  - 每个测试函数内部自带循环，循环参数独立可调
 *
 *  用法：
 *    ./build/testcase <ip> <port> basic    [loop]
 *    ./build/testcase <ip> <port> pipeline [loop] [P1 P2 ...]
 *    ./build/testcase <ip> <port> fill     <start> <count>
 *    ./build/testcase <ip> <port> verify   <start> <count>
 *      basic    — 基本命令 CRUD 闭环，循环 loop 次（默认 10000）
 *      pipeline — 打包发送 P 条命令，批量读回并校验回复；
 *                 对每个 P 值循环 loop 次（默认 10000），P 默认 1 4 16 32 64 128
 *      fill     — 填充 count 个 key（key=fill:%012d，value=value:%012d），
 *                 用于持久化/主从同步测试的数据准备
 *      verify   — 校验 fill 填充的数据：GET 每个 key，value 必须精确匹配；
 *                 配合 fill 做持久化/主从同步后的数据一致性检查
 *      expire   — 填充 count 个带 ttl 秒过期的 key（key=expire:%012d），
 *                 填充完成后每秒发 DBSIZE 并打印，观察 key 随过期衰减；
 *                 服务器须以 'expire on' 启动（默认 off 时不会主动清理）
 *
 *  编译：gcc -O2 -o build/testcase test/testcase.c
 *  退出码：0 全部通过；非 0 测试失败
 *============================================================================*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>

#define MAX_MSG_LENGTH   4096
#define MAX_BATCH        (512 * 1024)
#define MAX_PIPES        32
#define DEFAULT_LOOP     10000

/*=============================================================================
 *  收发（带读缓冲：批量读回复时减少系统调用，并精确读 bulk body）
 *============================================================================*/

typedef struct {
    int  fd;
    char buf[65536];
    int  pos, len;
} rbuf_t;

static rbuf_t g_rb;

/** 完整发送（处理部分发送） */
static void send_all(int fd, const char *msg, int length) {
    int sent = 0;
    while (sent < length) {
        int n = (int)send(fd, msg + sent, length - sent, 0);
        if (n <= 0) { perror("send"); exit(1); }
        sent += n;
    }
}

/** 填充读缓冲（前一次内容已消费完） */
static int rbuf_fill(rbuf_t *b) {
    b->pos = 0; b->len = 0;
    int n = (int)recv(b->fd, b->buf, sizeof b->buf, 0);
    if (n <= 0) return -1;
    b->len = n;
    return n;
}

/** 从缓冲读一个字节 */
static int rbuf_getc(rbuf_t *b) {
    if (b->pos >= b->len)
        if (rbuf_fill(b) <= 0) return -1;
    return (unsigned char)b->buf[b->pos++];
}

/**
 * 读一条完整 RESP 回复到 out（含 CRLF；$bulk 带 body）
 * @return 字节数；-1 连接断开/超时
 */
static int recv_reply(rbuf_t *b, char *out, int cap) {
    int n = 0;
    while (1) {                              /* 第一行 */
        int c = rbuf_getc(b);
        if (c < 0 || n >= cap - 1) return -1;
        out[n++] = (char)c;
        if (c == '\n') break;
    }
    out[n] = '\0';
    if (out[0] == '$' && out[1] != '-') {    /* bulk body */
        long blen = atol(out + 1);
        int need = (int)blen + 2;            /* body + CRLF */
        while (need > 0) {
            int c = rbuf_getc(b);
            if (c < 0 || n >= cap - 1) return -1;
            out[n++] = (char)c;
            need--;
        }
        out[n] = '\0';
    }
    return n;
}

/*=============================================================================
 *  命令构造 — 统一走 RESP 报文
 *  kvstore 的 AOF 重放和主从传播只认原始 RESP 字节（msg_raw 原样存），
 *  内联命令不会被日志化，因此所有命令统一用 RESP 构造
 *============================================================================*/

/** 构造一条 RESP 命令（*N\r\n$len\r\narg\r\n...），返回总长度；失败 -1 */
static int resp_cmd(char *buf, int cap, int argc, ...) {
    va_list ap;
    int n = snprintf(buf, cap, "*%d\r\n", argc);
    if (n < 0 || n >= cap) return -1;
    va_start(ap, argc);
    for (int i = 0; i < argc; i++) {
        const char *arg = va_arg(ap, const char *);
        int w = snprintf(buf + n, cap - n, "$%zu\r\n%s\r\n", strlen(arg), arg);
        if (w < 0 || n + w >= cap) { va_end(ap); return -1; }
        n += w;
    }
    va_end(ap);
    return n;
}

/*=============================================================================
 *  校验 — 单命令（发+校验）与批处理（只校验）统一入口
 *============================================================================*/

/** 从 g_rb 读一条回复，与期望串精确比较；不符即失败退出 */
static void expect_reply(const char *expect, const char *what) {
    char rep[MAX_MSG_LENGTH];
    if (recv_reply(&g_rb, rep, sizeof rep) < 0) {
        fprintf(stderr, "==> FAILED -> %s: 读取回复失败\n", what);
        exit(1);
    }
    if (strcmp(rep, expect) != 0) {
        printf("==> FAILED -> %s, EXPECTED: '%s' BUT GOT: '%s'\n",
               what, expect, rep);
        exit(1);
    }
}

/** 同 expect_reply，但只比前缀（动态值/错误文本） */
static void expect_reply_prefix(const char *prefix, const char *what) {
    char rep[MAX_MSG_LENGTH];
    if (recv_reply(&g_rb, rep, sizeof rep) < 0) {
        fprintf(stderr, "==> FAILED -> %s: 读取回复失败\n", what);
        exit(1);
    }
    if (strncmp(rep, prefix, strlen(prefix)) != 0) {
        printf("==> FAILED -> %s, EXPECTED prefix: '%s' BUT GOT: '%s'\n",
               what, prefix, rep);
        exit(1);
    }
}

/** 发送一条 RESP 命令并校验回复（精确匹配） */
static void exec_cmd(int fd, const char *cmd, const char *expect, const char *what) {
    send_all(fd, cmd, (int)strlen(cmd));
    expect_reply(expect, what);
}

/** 发送一条 RESP 命令并校验回复（前缀匹配） */
static void exec_cmd_prefix(int fd, const char *cmd, const char *prefix,
                            const char *what) {
    send_all(fd, cmd, (int)strlen(cmd));
    expect_reply_prefix(prefix, what);
}

/*=============================================================================
 *  连接
 *============================================================================*/

static int connect_tcpserver(const char *ip, unsigned short port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = inet_addr(ip);
    sa.sin_port = htons(port);
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        perror("connect");
        return -1;
    }
    struct timeval tv = { .tv_sec = 10, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    return fd;
}

/*=============================================================================
 *  basic_cmds — 基本命令 CRUD 闭环，内部循环 loop 次
 *============================================================================*/

void basic_cmds(int fd, int loop) {
    char cmd[MAX_MSG_LENGTH];
    for (int i = 0; i < loop; i++) {
        resp_cmd(cmd, sizeof cmd, 3, "SET", "basic", "key");
        exec_cmd(fd, cmd, "+OK\r\n", "SET");
        resp_cmd(cmd, sizeof cmd, 2, "GET", "basic");
        exec_cmd(fd, cmd, "$3\r\nkey\r\n", "GET");
        resp_cmd(cmd, sizeof cmd, 3, "SET", "basic", "val");
        exec_cmd(fd, cmd, "+OK\r\n", "SET-overwrite");
        resp_cmd(cmd, sizeof cmd, 2, "GET", "basic");
        exec_cmd(fd, cmd, "$3\r\nval\r\n", "GET-overwrite");
        resp_cmd(cmd, sizeof cmd, 3, "EXPIRE", "basic", "100");
        exec_cmd(fd, cmd, ":1\r\n", "EXPIRE");
        resp_cmd(cmd, sizeof cmd, 2, "TTL", "basic");
        exec_cmd_prefix(fd, cmd, ":", "TTL");    /* 动态秒数 */
        resp_cmd(cmd, sizeof cmd, 2, "DEL", "basic");
        exec_cmd(fd, cmd, ":1\r\n", "DEL");
        resp_cmd(cmd, sizeof cmd, 2, "GET", "basic");
        exec_cmd(fd, cmd, "$-1\r\n", "GET-nil");
        resp_cmd(cmd, sizeof cmd, 2, "DEL", "basic");
        exec_cmd(fd, cmd, ":0\r\n", "DEL-nil");
        resp_cmd(cmd, sizeof cmd, 1, "PING");
        exec_cmd(fd, cmd, "+PONG\r\n", "PING");
        resp_cmd(cmd, sizeof cmd, 2, "ECHO", "hello");
        exec_cmd(fd, cmd, "$5\r\nhello\r\n", "ECHO");
        resp_cmd(cmd, sizeof cmd, 1, "NOSUCHCMD");
        exec_cmd_prefix(fd, cmd, "-ERR", "unknown-cmd");
        resp_cmd(cmd, sizeof cmd, 2, "SET", "onlykey");
        exec_cmd_prefix(fd, cmd, "-ERR", "SET-wrong-args");
    }
    printf("OK: basic_cmds %d 轮全部通过\n", loop);
}

/*=============================================================================
 *  pipeline_cmds — 打包发送 P 条命令，批量读回并校验回复
 *  对每个 P ∈ pipes[] 循环 loop 次
 *============================================================================*/

void pipeline_cmds(int fd, int loop, const int *pipes, int npipes) {
    char batch[MAX_BATCH];
    for (int pi = 0; pi < npipes; pi++) {
        int P = pipes[pi];
        for (int i = 0; i < loop; i++) {
            /* 一次性打包 P 条 SET 命令（RESP） */
            size_t len = 0;
            char key[32];
            for (int j = 0; j < P; j++) {
                snprintf(key, sizeof key, "pipe:%d:%d", P, j);
                int w = resp_cmd(batch + len, (int)(sizeof batch - len),
                                 3, "SET", key, "v");
                if (w < 0 || len + (size_t)w >= sizeof batch) exit(1);
                len += (size_t)w;
            }
            send_all(fd, batch, (int)len);

            /* 读回 P 条回复，逐条校验 +OK */
            for (int j = 0; j < P; j++) {
                char what[64];
                snprintf(what, sizeof what, "pipeline P=%d SET[%d]", P, j);
                expect_reply("+OK\r\n", what);
            }
        }
        printf("  [OK] pipeline P=%-3d × %d 轮\n", P, loop);
    }

    /* 清理测试 key（批量 DEL，RESP） */
    size_t len = 0;
    char key[32];
    for (int pi = 0; pi < npipes; pi++)
        for (int j = 0; j < pipes[pi]; j++) {
            snprintf(key, sizeof key, "pipe:%d:%d", pipes[pi], j);
            int w = resp_cmd(batch + len, (int)(sizeof batch - len),
                             2, "DEL", key);
            if (w < 0 || len + (size_t)w >= sizeof batch) exit(1);
            len += (size_t)w;
        }
    send_all(fd, batch, (int)len);
    for (int pi = 0; pi < npipes; pi++)
        for (int j = 0; j < pipes[pi]; j++) {
            char what[64];
            snprintf(what, sizeof what, "pipeline P=%d DEL", pipes[pi]);
            expect_reply(":1\r\n", what);
        }
    printf("OK: pipeline 全部通过 (loop=%d, pipes=%d 档)\n", loop, npipes);
}

/*=============================================================================
 *  fill_cmds / verify_cmds — 数据填充与校验
 *  用于自行测试 RDB/AOF 持久化和主从同步（两端独立可重现，无需传数据）
 *  范围由命令行指定：fill <start> <count>
 *  key = fill:%012d，value = value:%012d
 *
 *  命令统一走 RESP（resp_cmd）：kvstore 的 AOF 重放和主从传播只认原始
 *  RESP 字节（msg_raw 原样存），内联命令不会被日志化，写路径必须 RESP
 *============================================================================*/

/** 测试数据集 key/value 构造（fill/verify/expire 共用，保证两端一致）
 *  key = <prefix>:%012d，value = value:%012d */
static void pair(int i, const char *prefix, char *key, int kcap, char *val, int vcap) {
    snprintf(key, kcap, "%s:%012d", prefix, i);
    snprintf(val, vcap, "value:%012d", i);
}

void fill_cmds(int fd, int start, int count) {
    char batch[MAX_BATCH];
    int filled = 0;
    while (filled < count) {
        size_t len = 0;
        int nthis = 0;
        while (filled < count) {
            char key[24], val[24];
            pair(start + filled, "fill", key, sizeof key, val, sizeof val);
            int w = resp_cmd(batch + len, (int)(sizeof batch - len),
                             3, "SET", key, val);
            if (w < 0 || len + (size_t)w >= sizeof batch) break;   /* 批满 */
            len += (size_t)w;
            nthis++;
            filled++;
        }
        if (nthis == 0) break;
        send_all(fd, batch, (int)len);
        for (int k = 0; k < nthis; k++) {
            char what[64];
            snprintf(what, sizeof what, "fill[%d]",
                     start + filled - nthis + k);
            expect_reply("+OK\r\n", what);
        }
    }
    printf("OK: fill %d 个 key (start=%d)\n", count, start);
}

void verify_cmds(int fd, int start, int count) {
    char batch[MAX_BATCH];
    int verified = 0;
    while (verified < count) {
        size_t len = 0;
        int nthis = 0;
        while (verified < count) {
            char key[24], val[24];
            pair(start + verified, "fill", key, sizeof key, val, sizeof val);
            int w = resp_cmd(batch + len, (int)(sizeof batch - len),
                             2, "GET", key);
            if (w < 0 || len + (size_t)w >= sizeof batch) break;   /* 批满 */
            len += (size_t)w;
            nthis++;
            verified++;
        }
        if (nthis == 0) break;
        send_all(fd, batch, (int)len);
        for (int k = 0; k < nthis; k++) {
            char key[24], val[24], expect[64];
            pair(start + verified - nthis + k,
                 "fill", key, sizeof key, val, sizeof val);
            snprintf(expect, sizeof expect, "$%zu\r\n%s\r\n",
                     strlen(val), val);
            expect_reply(expect, "verify");
        }
    }
    printf("OK: verify %d 个 key 全部正确 (start=%d)\n", count, start);
}

/*=============================================================================
 *  expire_cmds — 过期功能测试
 *  1. 一次性填充 count 个带过期时间的 key（SET + EXPIRE 成对打包发送）
 *  2. 填充完成后每隔一秒发 DBSIZE 并打印，观察 key 随过期被抽样清理的衰减
 *
 *  依赖：服务器须以 'expire on' 启动（active_expire_cycle 抽样删除，
 *  expire off 时已过期 key 不会被主动清理，DBSIZE 不会下降）。
 *  key = expire:%012d，value = value:%012d；范围由命令行指定：<start> <count> <ttl>
 *============================================================================*/

/** 发送 DBSIZE 并返回当前键数量（整数回复 :N\r\n） */
static int get_dbsize(int fd) {
    char cmd[32], rep[MAX_MSG_LENGTH];
    int n = resp_cmd(cmd, sizeof cmd, 1, "DBSIZE");
    if (n < 0) {
        fprintf(stderr, "==> FAILED -> DBSIZE: 命令构造失败\n");
        exit(1);
    }
    send_all(fd, cmd, n);
    if (recv_reply(&g_rb, rep, sizeof rep) < 0) {
        fprintf(stderr, "==> FAILED -> DBSIZE: 读取回复失败\n");
        exit(1);
    }
    return atoi(rep + 1);   /* ":N\r\n" 跳过 ':' */
}

void expire_cmds(int fd, int start, int count, int ttl) {
    /* 基线：填充前的 dbsize（剔除服务器其它数据的干扰） */
    int base = get_dbsize(fd);

    /* 阶段 1：一次性填充（SET + EXPIRE 成对打包，逻辑同 fill_cmds） */
    char batch[MAX_BATCH];
    int filled = 0;
    while (filled < count) {
        size_t len = 0;
        int nthis = 0;
        while (filled < count) {
            char key[24], val[24], ttlstr[16];
            pair(start + filled, "expire", key, sizeof key, val, sizeof val);
            snprintf(ttlstr, sizeof ttlstr, "%d", ttl);
            /* SET 与 EXPIRE 成对提交：两者都能容纳才发，避免半条命令 */
            int w1 = resp_cmd(batch + len, (int)(sizeof batch - len),
                              3, "SET", key, val);
            int w2 = (w1 > 0) ? resp_cmd(batch + len + w1,
                                         (int)(sizeof batch - len - w1),
                                         3, "EXPIRE", key, ttlstr) : -1;
            if (w1 <= 0 || w2 <= 0 ||
                len + (size_t)w1 + (size_t)w2 >= sizeof batch)
                break;                          /* 批满：整对都不发，下轮重试 */
            len += (size_t)w1 + (size_t)w2;
            nthis++;
            filled++;
        }
        if (nthis == 0) break;
        send_all(fd, batch, (int)len);
        for (int k = 0; k < nthis; k++) {
            char what[64];
            snprintf(what, sizeof what, "expire[%d]-SET",
                     start + filled - nthis + k);
            expect_reply("+OK\r\n", what);
            snprintf(what, sizeof what, "expire[%d]-EXPIRE",
                     start + filled - nthis + k);
            expect_reply(":1\r\n", what);
        }
    }
    int after = get_dbsize(fd);
    printf("OK: expire 填充 %d 个 key (start=%d, ttl=%ds)\n", count, start, ttl);
    printf("expire: dbsize=%d（基线 %d，新增 %d）\n", after, base, after - base);

    /* 阶段 2：每秒轮询 DBSIZE 打印，一直循环直到新增的测试 key 全部过期
     * （dbsize 回到填充前基线；空库启动时基线为 0，即 dbsize 打到 0）。
     * 主动过期是抽样清理（实测约几百 key/秒），大量 key 的衰减是渐进过程，
     * 需要数十秒甚至更久，因此不设时间上限，由观察者按需 Ctrl-C 中断。
     * 若 ttl 已过 dbsize 仍未衰减，打印一次提示（服务器未以 expire on 启动
     * 时不会主动清理），但循环继续，直到衰减完成。 */
    int saw_decay = 0, hint_shown = 0;
    for (int t = 1;; t++) {
        sleep(1);
        int cur = get_dbsize(fd);
        printf("expire: %ds dbsize=%d\n", t, cur);
        if (cur < after) saw_decay = 1;
        if (cur <= base) {
            printf("expire: 全部 %d 个测试 key 已过期\n", count);
            return;
        }
        if (!hint_shown && t > ttl && !saw_decay) {
            printf("expire: 提示——已过 %d 秒 dbsize 仍未衰减，"
                   "服务器需以 'expire on' 启动\n", ttl);
            hint_shown = 1;
        }
    }
}

/*=============================================================================
 *  入口
 *============================================================================*/

static void usage(const char *prog) {
    printf("用法:\n");
    printf("  %s <ip> <port> basic    [loop]\n", prog);
    printf("  %s <ip> <port> pipeline [loop] [P1 P2 ...]\n", prog);
    printf("  %s <ip> <port> fill     <start> <count>\n", prog);
    printf("  %s <ip> <port> verify   <start> <count>\n", prog);
    printf("  %s <ip> <port> expire   <start> <count> <ttl>\n", prog);
}

int main(int argc, char *argv[]) {
    if (argc < 4) { usage(argv[0]); return 2; }
    const char *ip   = argv[1];
    int         port = atoi(argv[2]);
    const char *test = argv[3];
    int         loop = (argc >= 5) ? atoi(argv[4]) : DEFAULT_LOOP;

    /* pipeline 的 P 列表：命令行可选，默认 1 4 16 32 64 128 */
    int pipes[MAX_PIPES], npipes = 0;
    if (strcmp(test, "pipeline") == 0) {
        if (argc > 5) {
            for (int i = 5; i < argc && npipes < MAX_PIPES; i++) {
                int p = atoi(argv[i]);
                if (p > 0) pipes[npipes++] = p;
            }
        } else {
            static const int def[] = { 1, 4, 16, 32, 64, 128 };
            npipes = 6;
            for (int i = 0; i < 6; i++) pipes[i] = def[i];
        }
    }

    /* fill/verify/expire 的范围参数：<start> <count> [<ttl>] */
    int fstart = 0, fcount = 0, fttl = 0;
    if (strcmp(test, "fill") == 0 || strcmp(test, "verify") == 0) {
        if (argc < 6) { usage(argv[0]); return 2; }
        fstart = atoi(argv[4]);
        fcount = atoi(argv[5]);
        if (fcount <= 0) {
            fprintf(stderr, "count 必须 > 0\n");
            return 2;
        }
    } else if (strcmp(test, "expire") == 0) {
        if (argc < 7) { usage(argv[0]); return 2; }
        fstart = atoi(argv[4]);
        fcount = atoi(argv[5]);
        fttl   = atoi(argv[6]);
        if (fcount <= 0 || fttl <= 0) {
            fprintf(stderr, "count/ttl 必须 > 0\n");
            return 2;
        }
    }

    g_rb.fd = connect_tcpserver(ip, (unsigned short)port);
    if (g_rb.fd < 0) return 2;
    g_rb.pos = g_rb.len = 0;

    if (strcmp(test, "basic") == 0) {
        basic_cmds(g_rb.fd, loop);
    } else if (strcmp(test, "pipeline") == 0) {
        pipeline_cmds(g_rb.fd, loop, pipes, npipes);
    } else if (strcmp(test, "fill") == 0) {
        fill_cmds(g_rb.fd, fstart, fcount);
    } else if (strcmp(test, "verify") == 0) {
        verify_cmds(g_rb.fd, fstart, fcount);
    } else if (strcmp(test, "expire") == 0) {
        expire_cmds(g_rb.fd, fstart, fcount, fttl);
    } else {
        fprintf(stderr, "未知测试: %s\n", test);
        usage(argv[0]);
        return 2;
    }

    close(g_rb.fd);
    return 0;
}
