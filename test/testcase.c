/*=============================================================================
 *  testcase.c — kvstore 功能测试客户端
 *
 *  极简设计（参考 inazuma-kv/tests/testcase.c）：
 *  - 发内联命令（自动补 \r\n），收完整 RESP 回复，strcmp 整串比较
 *  - testcase() 精确匹配；testcase_prefix() 前缀匹配（动态值/错误文本）
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
 *
 *  编译：gcc -O2 -o build/testcase test/testcase.c
 *  退出码：0 全部通过；非 0 测试失败
 *============================================================================*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
static void send_msg(int connfd, const char *msg, int length) {
    int sent = 0;
    while (sent < length) {
        int n = (int)send(connfd, msg + sent, length - sent, 0);
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

/** 发命令并读一条完整回复（自动补 \r\n） */
static void send_and_recv(int connfd, const char *msg, char *result, int cap) {
    char cmd[MAX_MSG_LENGTH];
    snprintf(cmd, sizeof cmd, "%s\r\n", msg);
    send_msg(connfd, cmd, (int)strlen(cmd));
    if (recv_reply(&g_rb, result, cap) < 0) {
        fprintf(stderr, "==> FAILED -> 读取回复失败: %s\n", msg);
        exit(1);
    }
}

/*=============================================================================
 *  断言
 *============================================================================*/

static void testcase(int connfd, const char *msg, const char *pattern,
                     const char *casename) {
    char result[MAX_MSG_LENGTH] = {0};
    send_and_recv(connfd, msg, result, sizeof result);
    if (strcmp(result, pattern) != 0) {
        printf("==> FAILED -> %s, EXPECTED: '%s' BUT GOT: '%s'\n",
               casename, pattern, result);
        exit(1);
    }
}

static void testcase_prefix(int connfd, const char *msg, const char *prefix,
                            const char *casename) {
    char result[MAX_MSG_LENGTH] = {0};
    send_and_recv(connfd, msg, result, sizeof result);
    if (strncmp(result, prefix, strlen(prefix)) != 0) {
        printf("==> FAILED -> %s, EXPECTED prefix: '%s' BUT GOT: '%s'\n",
               casename, prefix, result);
        exit(1);
    }
}

/*=============================================================================
 *  连接
 *============================================================================*/

static int connect_tcpserver(const char *ip, unsigned short port) {
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof server_addr);
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip);
    server_addr.sin_port = htons(port);
    if (connect(connfd, (struct sockaddr *)&server_addr, sizeof server_addr) != 0) {
        perror("connect");
        return -1;
    }
    struct timeval tv = { .tv_sec = 10, .tv_usec = 0 };
    setsockopt(connfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    return connfd;
}

/*=============================================================================
 *  basic_cmds — 基本命令 CRUD 闭环，内部循环 loop 次
 *============================================================================*/

void basic_cmds(int connfd, int loop) {
    for (int i = 0; i < loop; i++) {
        testcase(connfd, "SET basic key", "+OK\r\n", "SET");
        testcase(connfd, "GET basic", "$3\r\nkey\r\n", "GET");
        testcase(connfd, "SET basic val", "+OK\r\n", "SET-overwrite");
        testcase(connfd, "GET basic", "$3\r\nval\r\n", "GET-overwrite");
        testcase(connfd, "EXPIRE basic 100", ":1\r\n", "EXPIRE");
        testcase_prefix(connfd, "TTL basic", ":", "TTL");    /* 动态秒数 */
        testcase(connfd, "DEL basic", ":1\r\n", "DEL");
        testcase(connfd, "GET basic", "$-1\r\n", "GET-nil");
        testcase(connfd, "DEL basic", ":0\r\n", "DEL-nil");
        testcase(connfd, "PING", "+PONG\r\n", "PING");
        testcase(connfd, "ECHO hello", "$5\r\nhello\r\n", "ECHO");
        testcase_prefix(connfd, "NOSUCHCMD", "-ERR", "unknown-cmd");
        testcase_prefix(connfd, "SET onlykey", "-ERR", "SET-wrong-args");
    }
    printf("OK: basic_cmds %d 轮全部通过\n", loop);
}

/*=============================================================================
 *  pipeline_cmds — 打包发送 P 条命令，批量读回并校验回复
 *  对每个 P ∈ pipes[] 循环 loop 次
 *============================================================================*/

void pipeline_cmds(int connfd, int loop, const int *pipes, int npipes) {
    char batch[MAX_BATCH];
    for (int pi = 0; pi < npipes; pi++) {
        int P = pipes[pi];
        for (int i = 0; i < loop; i++) {
            /* 一次性打包 P 条 SET 命令 */
            size_t len = 0;
            for (int j = 0; j < P; j++) {
                int w = snprintf(batch + len, sizeof batch - len,
                                 "SET pipe:%d:%d v\r\n", P, j);
                if (w < 0 || len + (size_t)w >= sizeof batch) exit(1);
                len += (size_t)w;
            }
            send_msg(connfd, batch, (int)len);

            /* 读回 P 个回复，逐个校验 +OK */
            for (int j = 0; j < P; j++) {
                char rep[MAX_MSG_LENGTH];
                if (recv_reply(&g_rb, rep, sizeof rep) < 0) {
                    fprintf(stderr, "==> FAILED -> pipeline P=%d 回复中断 (第 %d/%d)\n",
                            P, j, P);
                    exit(1);
                }
                if (strcmp(rep, "+OK\r\n") != 0) {
                    printf("==> FAILED -> pipeline P=%d reply[%d]: '%s'\n", P, j, rep);
                    exit(1);
                }
            }
        }
        printf("  [OK] pipeline P=%-3d × %d 轮\n", P, loop);
    }

    /* 清理测试 key（批量 DEL） */
    size_t len = 0;
    for (int pi = 0; pi < npipes; pi++)
        for (int j = 0; j < pipes[pi]; j++) {
            int w = snprintf(batch + len, sizeof batch - len,
                             "DEL pipe:%d:%d\r\n", pipes[pi], j);
            if (w < 0 || len + (size_t)w >= sizeof batch) exit(1);
            len += (size_t)w;
        }
    send_msg(connfd, batch, (int)len);
    for (int pi = 0; pi < npipes; pi++)
        for (int j = 0; j < pipes[pi]; j++) {
            char rep[MAX_MSG_LENGTH];
            if (recv_reply(&g_rb, rep, sizeof rep) < 0 ||
                strcmp(rep, ":1\r\n") != 0) {
                fprintf(stderr, "==> FAILED -> pipeline 清理 key pipe:%d:%d\n",
                        pipes[pi], j);
                exit(1);
            }
        }
    printf("OK: pipeline 全部通过 (loop=%d, pipes=%d 档)\n", loop, npipes);
}

/*=============================================================================
 *  fill_cmds / verify_cmds — 数据填充与校验
 *  用于自行测试 RDB/AOF 持久化和主从同步（两端独立可重现，无需传数据）
 *  范围由命令行指定：fill <start> <count>
 *  key = fill:%012d，value = value:%012d
 *
 *  注意：fill 必须发 RESP 命令（不能发内联命令）
 *  - kvstore 的 AOF 重放和主从传播都只认原始 RESP 字节（msg_raw 原样存）
 *  - 若发内联 SET，AOF 里存的是内联文本，重启后 0 条重放（数据丢）
 *  verify 只发只读 GET，不会被日志/传播，内联即可
 *============================================================================*/

/** 构造一条 RESP 格式的 SET 命令，返回长度 */
static int resp_set(char *buf, int cap, int i) {
    char key[24], val[24];
    snprintf(key, sizeof key, "fill:%012d", i);
    snprintf(val, sizeof val, "value:%012d", i);
    return snprintf(buf, cap, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                    (int)strlen(key), key, (int)strlen(val), val);
}

void fill_cmds(int connfd, int start, int count) {
    char batch[MAX_BATCH];
    int filled = 0;
    while (filled < count) {
        size_t len = 0;
        int nthis = 0;
        while (filled < count) {
            int i = start + filled;
            int w = resp_set(batch + len, (int)(sizeof batch - len), i);
            if (w < 0 || len + (size_t)w >= sizeof batch) break;   /* 批满 */
            len += (size_t)w;
            nthis++;
            filled++;
        }
        if (nthis == 0) break;
        send_msg(connfd, batch, (int)len);
        for (int k = 0; k < nthis; k++) {
            char rep[MAX_MSG_LENGTH];
            if (recv_reply(&g_rb, rep, sizeof rep) < 0 ||
                strcmp(rep, "+OK\r\n") != 0) {
                fprintf(stderr, "==> FAILED -> fill 第 %d 条回复异常: '%s'\n",
                        start + filled - nthis + k, rep);
                exit(1);
            }
        }
    }
    printf("OK: fill %d 个 key (start=%d)\n", count, start);
}

void verify_cmds(int connfd, int start, int count) {
    char batch[MAX_BATCH];
    int verified = 0;
    while (verified < count) {
        size_t len = 0;
        int nthis = 0;
        while (verified < count) {
            int i = start + verified;
            int w = snprintf(batch + len, sizeof batch - len,
                             "GET fill:%012d\r\n", i);
            if (w < 0 || len + (size_t)w >= sizeof batch) break;   /* 批满 */
            len += (size_t)w;
            nthis++;
            verified++;
        }
        if (nthis == 0) break;
        send_msg(connfd, batch, (int)len);
        for (int k = 0; k < nthis; k++) {
            int i = start + verified - nthis + k;
            char expect[64], rep[MAX_MSG_LENGTH];
            snprintf(expect, sizeof expect, "$18\r\nvalue:%012d\r\n", i);
            if (recv_reply(&g_rb, rep, sizeof rep) < 0 ||
                strcmp(rep, expect) != 0) {
                printf("==> FAILED -> verify key %d, EXPECTED: '%s' BUT GOT: '%s'\n",
                       i, expect, rep);
                exit(1);
            }
        }
    }
    printf("OK: verify %d 个 key 全部正确 (start=%d)\n", count, start);
}

/*=============================================================================
 *  入口
 *============================================================================*/

static void usage(void) {
    printf("用法:\n");
    printf("  %s <ip> <port> basic    [loop]\n", "testcase");
    printf("  %s <ip> <port> pipeline [loop] [P1 P2 ...]\n", "testcase");
    printf("  %s <ip> <port> fill     <start> <count>\n", "testcase");
    printf("  %s <ip> <port> verify   <start> <count>\n", "testcase");
}

int main(int argc, char *argv[]) {
    if (argc < 4) { usage(); return 2; }
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

    /* fill/verify 的范围参数：<start> <count> */
    int fstart = 0, fcount = 0;
    if (strcmp(test, "fill") == 0 || strcmp(test, "verify") == 0) {
        if (argc < 6) { usage(); return 2; }
        fstart = atoi(argv[4]);
        fcount = atoi(argv[5]);
        if (fcount <= 0) {
            fprintf(stderr, "count 必须 > 0\n");
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
    } else {
        fprintf(stderr, "未知测试: %s\n", test);
        usage();
        return 2;
    }

    close(g_rb.fd);
    return 0;
}
