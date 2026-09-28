/*=============================================================================
 *  testcase.c — kvstore 功能测试客户端
 *
 *  极简设计（参考 inazuma-kv/tests/testcase.c）：
 *  - 命令统一走 RESP 报文（resp_cmd 构造），收完整 RESP 回复，strcmp 整串比较
 *  - exec_cmd / exec_cmd_prefix：发一条命令并校验回复（单命令场景）
 *  - expect_reply / expect_reply_prefix：只校验已收到的一条回复（批处理场景）
 *  - 每个测试函数结尾统一 report() 打印「命令条数 + 耗时 + 折算速率」
 *
 *  被测的写语义（见 src/core/kvstore_dispatch.c）：
 *    SET key value [ttl秒]  —— upsert：键不存在即新增、已存在即覆盖，都回 +OK
 *    MOD key value [ttl秒]  —— 只修改，键不存在回 nil（不新增）
 *    过期时间只在写入时给定；无 EXPIRE 命令，TTL 为只读查询。
 *    省略 ttl = 永不过期；不带 ttl 的 SET / MOD = 清除旧过期。
 *
 *  用法：
 *    ./build/testcase <ip> <port> basic    [loop]
 *    ./build/testcase <ip> <port> pipeline [loop] [P1 P2 ...]
 *    ./build/testcase <ip> <port> fill     <start> <count>
 *    ./build/testcase <ip> <port> verify   <start> <count>
 *    ./build/testcase <ip> <port> expire   <start> <count> <ttl>
 *    ./build/testcase <ip> <port> dbsize   [loop]
 *    ./build/testcase <ip> <port> savebench <per_round> <rounds>
 *      basic    — SET/MOD/DEL/TTL 的 CRUD 闭环（含冲突与 ttl 分支），循环 loop 次
 *      pipeline — 打包发送 P 条 SET，批量读回并校验；对每个 P 循环 loop 次，
 *                 每个 P 档之间 FLUSHALL 清库
 *      fill     — 填充 count 个 key（key=fill:%012d，value=value:%012d），
 *                 用于持久化/主从同步测试的数据准备。SET 是 upsert，
 *                 目标区间已有数据会被直接覆盖，不再要求区间为空
 *      verify   — 校验 fill 填充的数据：GET 每个 key，value 精确匹配
 *      expire   — 填充 count 个带 ttl 秒过期的 key（key=expire:%012d），
 *                 随后每秒打印 DBSIZE，观察 key 随主动过期衰减；
 *                 服务器须以 'expire on' 启动
 *      dbsize   — DBSIZE 计数正确性闭环，循环 loop 次：
 *                 每轮 FLUSHALL 归零 → 新增 / 等长改写 / 变长改写 /
 *                 冲突 / 未命中 / DEL / 重复 DEL / DEL 不存在，逐步断言。
 *                 注意：会清空实例数据，只能对测试实例运行
 *      savebench— 周期性 BGSAVE 对写入 QPS/延迟的影响：每写 per_round 条 SET
 *                 发一次 BGSAVE，共 rounds 轮，最后报 QPS / P50 / P99。
 *                 服务端由调用方自行启动；key 单调递增、全程不重复。
 *
 *  编译：gcc -O2 -o build/testcase test/testcase.c
 *  退出码：0 全部通过；非 0 测试失败
 *============================================================================*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

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

/* 已发出的命令条数。批处理按「条」累加（一次 send 可能装 P 条），
 * 每个测试函数在入口取快照，收尾交给 report() 算出本函数的净条数 */
static long long g_cmds;

/** 单调递增的毫秒时钟（只用来量本进程内的耗时差） */
static double now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

/** 单调递增的微秒时钟（savebench 逐条记延迟用）。
 *  不用 now_ms()：gettimeofday 只到毫秒，本机回环亚毫秒的往返会被整个抹平，
 *  P50 会退化成一堆 0 和 1。CLOCK_MONOTONIC 走 vDSO，一次约 20ns */
static double now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

/**
 * 每个测试函数的统一收尾：打印命令条数 + 耗时 + 折算速率
 * @param cmds0 进入测试时的 g_cmds 快照
 * @param t0    进入测试时的 now_ms() 快照
 * @param fmt   测试名（可带参数），如 "basic loop=%d"
 */
static void report(long long cmds0, double t0, const char *fmt, ...) {
    char name[96];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(name, sizeof name, fmt, ap);
    va_end(ap);

    long long n  = g_cmds - cmds0;
    double    ms = now_ms() - t0;
    printf("OK: %-26s %9lld 条命令  %8.3f s  %9.0f 条/s\n",
           name, n, ms / 1000.0, ms > 0 ? n * 1000.0 / ms : 0.0);
    fflush(stdout);
}

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
    g_cmds++;
    expect_reply(expect, what);
}

/** 发送一条 RESP 命令并校验回复（前缀匹配） */
static void exec_cmd_prefix(int fd, const char *cmd, const char *prefix,
                            const char *what) {
    send_all(fd, cmd, (int)strlen(cmd));
    g_cmds++;
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
    /* 关 Nagle：本客户端是「发一条等一条」，与 redis-benchmark 同口径。
     * 不关的话回复若被拆成两个小段，Nagle + 对端延迟 ACK 会每轮多塞 40ms */
    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof nodelay);
    struct timeval tv = { .tv_sec = 10, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    return fd;
}

/*=============================================================================
 *  basic_cmds — 现有命令集的 CRUD 闭环，内部循环 loop 次
 *
 *  每轮的 21 条命令按「语义分组」排列，组内顺序即断言顺序：
 *    SET upsert      SET 新键 → GET 命中 → SET 同名覆盖回 +OK → GET 新值
 *    MOD 只修改      MOD 命中 → GET 新值 → MOD 未命中回 nil → GET 仍为空
 *    ttl 跟随写入    TTL 无过期=-1 → SET 覆盖带 ttl（长度变了走重建）
 *                    → TTL 动态秒数 → MOD 带 ttl → TTL 动态秒数
 *                    → MOD 不带 ttl 清过期 → TTL 回到 -1
 *                    → SET 新键带 ttl → TTL 动态 → DEL
 *    DEL             DEL 命中 :1 → GET 空 → DEL 重复 :0
 *
 *  严格「一条在飞」，耗时完全由 RTT 决定：本机回环健康时 ~55us/条
 *  （1 万轮约 9s），被宿主机抢占时能到 ~700us/条（同一万轮 ~110s）。
 *  所以按 10% 报进度，免得「慢」被误读成「挂住」。
 *============================================================================*/

void basic_cmds(int fd, int loop) {
    char cmd[MAX_MSG_LENGTH];
    long long c0 = g_cmds;
    double    t0 = now_ms();

    int step = loop / 10;
    if (step < 1) step = 1;

    for (int i = 0; i < loop; i++) {
        /* --- SET upsert：新键建、同名覆盖，两种情况都回 +OK --- */
        resp_cmd(cmd, sizeof cmd, 3, "SET", "basic", "key");
        exec_cmd(fd, cmd, "+OK\r\n", "SET 新增");
        resp_cmd(cmd, sizeof cmd, 2, "GET", "basic");
        exec_cmd(fd, cmd, "$3\r\nkey\r\n", "SET 后 GET");
        resp_cmd(cmd, sizeof cmd, 3, "SET", "basic", "val");
        exec_cmd(fd, cmd, "+OK\r\n", "SET 同名覆盖回 OK");
        resp_cmd(cmd, sizeof cmd, 2, "GET", "basic");
        exec_cmd(fd, cmd, "$3\r\nval\r\n", "SET 覆盖生效（等长原地写）");

        /* --- MOD 只修改：未命中回 nil，且不新建 --- */
        resp_cmd(cmd, sizeof cmd, 3, "MOD", "basic", "val");
        exec_cmd(fd, cmd, "+OK\r\n", "MOD 改写");
        resp_cmd(cmd, sizeof cmd, 2, "GET", "basic");
        exec_cmd(fd, cmd, "$3\r\nval\r\n", "MOD 后 GET");
        resp_cmd(cmd, sizeof cmd, 3, "MOD", "nosuch", "v");
        exec_cmd(fd, cmd, "$-1\r\n", "MOD 未命中回 nil");
        resp_cmd(cmd, sizeof cmd, 2, "GET", "nosuch");
        exec_cmd(fd, cmd, "$-1\r\n", "MOD 未命中不新建");

        /* --- 过期时间只在写入时给定（无 EXPIRE 命令） --- */
        resp_cmd(cmd, sizeof cmd, 2, "TTL", "basic");
        exec_cmd(fd, cmd, ":-1\r\n", "TTL 无过期");
        resp_cmd(cmd, sizeof cmd, 4, "SET", "basic", "x", "100");
        exec_cmd(fd, cmd, "+OK\r\n", "SET 覆盖带 ttl");
        resp_cmd(cmd, sizeof cmd, 2, "TTL", "basic");
        exec_cmd_prefix(fd, cmd, ":", "SET 覆盖 ttl 已生效");
        resp_cmd(cmd, sizeof cmd, 4, "MOD", "basic", "val", "100");
        exec_cmd(fd, cmd, "+OK\r\n", "MOD 带 ttl");
        resp_cmd(cmd, sizeof cmd, 2, "TTL", "basic");
        exec_cmd_prefix(fd, cmd, ":", "TTL 读回动态秒数");
        resp_cmd(cmd, sizeof cmd, 3, "MOD", "basic", "val");
        exec_cmd(fd, cmd, "+OK\r\n", "MOD 不带 ttl");
        resp_cmd(cmd, sizeof cmd, 2, "TTL", "basic");
        exec_cmd(fd, cmd, ":-1\r\n", "MOD 不带 ttl 即清过期");
        resp_cmd(cmd, sizeof cmd, 4, "SET", "tkey", "v", "100");
        exec_cmd(fd, cmd, "+OK\r\n", "SET 新键带 ttl");
        resp_cmd(cmd, sizeof cmd, 2, "TTL", "tkey");
        exec_cmd_prefix(fd, cmd, ":", "新键 TTL 动态秒数");
        resp_cmd(cmd, sizeof cmd, 2, "DEL", "tkey");
        exec_cmd(fd, cmd, ":1\r\n", "DEL 带 ttl 的键");

        /* --- DEL --- */
        resp_cmd(cmd, sizeof cmd, 2, "DEL", "basic");
        exec_cmd(fd, cmd, ":1\r\n", "DEL 命中");
        resp_cmd(cmd, sizeof cmd, 2, "GET", "basic");
        exec_cmd(fd, cmd, "$-1\r\n", "DEL 后 GET");
        resp_cmd(cmd, sizeof cmd, 2, "DEL", "basic");
        exec_cmd(fd, cmd, ":0\r\n", "DEL 重复删除");

        if ((i + 1) % step == 0) {
            printf("  ... basic %d/%d 轮\n", i + 1, loop);
            fflush(stdout);   /* 输出重定向到文件时是全缓冲，不刷看不见 */
        }
    }
    report(c0, t0, "basic loop=%d", loop);
}

/*=============================================================================
 *  pipeline_cmds — 打包发送 P 条命令，批量读回并校验回复
 *  对每个 P ∈ pipes[] 循环 loop 次，每个 P 单独计时
 *
 *  键名必须带轮次（pipe:<P>:<全局序号>）：SET 如今是 upsert，键名固定也不会
 *  回 nil，但那样第 2 轮起每批都变成纯覆盖，走不到新增路径（也就压不到
 *  扩容/再哈希）。要测的是 pipeline 深度下的写入，不是覆盖。
 *
 *  每个 P 档跑完发一条 FLUSHALL 清库：不清理的话键数 = loop × P，默认参数
 *  下能堆到百万级，库越大后面的档越吃亏，测的就不是 pipeline 深度了。
 *  这条 FLUSHALL 在该档 report 之后发，不混进该档的命令数/耗时。
 *============================================================================*/

void pipeline_cmds(int fd, int loop, const int *pipes, int npipes) {
    char batch[MAX_BATCH];
    char name[32];

    for (int pi = 0; pi < npipes; pi++) {
        int P = pipes[pi];
        long long c0 = g_cmds;
        double    t0 = now_ms();

        for (int i = 0; i < loop; i++) {
            /* 一次性打包 P 条 SET（RESP） */
            size_t len = 0;
            char key[32];
            for (int j = 0; j < P; j++) {
                snprintf(key, sizeof key, "pipe:%d:%d", P, i * P + j);
                int w = resp_cmd(batch + len, (int)(sizeof batch - len),
                                 3, "SET", key, "v");
                if (w < 0 || len + (size_t)w >= sizeof batch) {
                    fprintf(stderr, "==> FAILED -> pipeline P=%d: 批次缓冲溢出\n", P);
                    exit(1);
                }
                len += (size_t)w;
            }
            send_all(fd, batch, (int)len);
            g_cmds += P;

            /* 读回 P 条回复，逐条校验 +OK */
            for (int j = 0; j < P; j++) {
                char what[64];
                snprintf(what, sizeof what, "pipeline P=%d SET[%d]", P, j);
                expect_reply("+OK\r\n", what);
            }
        }
        snprintf(name, sizeof name, "pipeline P=%d", P);
        report(c0, t0, "%s", name);

        /* 清掉本档写下的 loop×P 个键，别让库规模拖累后面的档 */
        resp_cmd(batch, sizeof batch, 1, "FLUSHALL");
        exec_cmd(fd, batch, "+OK\r\n", "pipeline FLUSHALL");
    }
}

/*=============================================================================
 *  fill_cmds / verify_cmds — 数据填充与校验
 *  用于自行测试 RDB/AOF 持久化和主从同步（两端独立可重现，无需传数据）
 *  范围由命令行指定：fill <start> <count>
 *  key = fill:%012d，value = value:%012d
 *
 *  命令统一走 RESP（resp_cmd）：kvstore 的 AOF 重放和主从传播只认原始
 *  RESP 字节（msg_raw 原样存），内联命令不会被日志化，写路径必须 RESP
 *
 *  管道深度：命令攒满 batch[MAX_BATCH]（512KB）才 send 一次 —— 每条 SET
 *  62 字节（RESP 开销 27 + key 17 + value 18），故一次 send ≈ 8456 条 /
 *  ≈512KB，单次 write 极大。repl-xdp 镜像取的是**单个 TCP 段的负载**：
 *  回环（MSS 65483）上这条 write 就是一个 ~64KB 的段，超过 MAX_REC 即整段
 *  被丢（slave 一个 key 都收不到）；MTU 1500 的网卡/veth 会先切成 ≤1460B
 *  的段，故不受影响。redis-benchmark 路径不涉及（单次 write 仅 P×45 字节）。
 *============================================================================*/

/** 测试数据集 key/value 构造（fill/verify/expire 共用，保证两端一致）
 *  key = <prefix>:%012d，value = value:%012d */
static void pair(int i, const char *prefix, char *key, int kcap, char *val, int vcap) {
    snprintf(key, kcap, "%s:%012d", prefix, i);
    snprintf(val, vcap, "value:%012d", i);
}

void fill_cmds(int fd, int start, int count) {
    char batch[MAX_BATCH];
    long long c0 = g_cmds;
    double    t0 = now_ms();

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
        g_cmds += nthis;
        for (int k = 0; k < nthis; k++) {
            char what[64];
            snprintf(what, sizeof what, "fill[%d]",
                     start + filled - nthis + k);
            expect_reply("+OK\r\n", what);
        }
    }
    report(c0, t0, "fill start=%d count=%d", start, count);
}

void verify_cmds(int fd, int start, int count) {
    char batch[MAX_BATCH];
    long long c0 = g_cmds;
    double    t0 = now_ms();

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
        g_cmds += nthis;
        for (int k = 0; k < nthis; k++) {
            char key[24], val[24], expect[64];
            pair(start + verified - nthis + k,
                 "fill", key, sizeof key, val, sizeof val);
            snprintf(expect, sizeof expect, "$%zu\r\n%s\r\n",
                     strlen(val), val);
            expect_reply(expect, "verify");
        }
    }
    report(c0, t0, "verify start=%d count=%d", start, count);
}

/*=============================================================================
 *  expire_cmds — 过期功能测试
 *  1. 一次性填充 count 个带过期时间的 key（SET key val <ttl>，一条命令搞定）
 *  2. 填充完成后每隔一秒发 DBSIZE 并打印，观察 key 随过期被抽样清理的衰减
 *
 *  依赖：服务器须以 'expire on' 启动（active_expire_cycle 抽样删除，
 *  expire off 时已过期 key 不会被主动清理，DBSIZE 不会下降）。
 *  key = expire:%012d，value = value:%012d；范围由命令行指定：<start> <count> <ttl>
 *
 *  计时只覆盖阶段 1：阶段 2 是观测，耗时由 ttl 和抽样清理速度决定，
 *  与客户端能力无关，报出来只会误导。
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
    g_cmds++;
    if (recv_reply(&g_rb, rep, sizeof rep) < 0) {
        fprintf(stderr, "==> FAILED -> DBSIZE: 读取回复失败\n");
        exit(1);
    }
    return atoi(rep + 1);   /* ":N\r\n" 跳过 ':' */
}

void expire_cmds(int fd, int start, int count, int ttl) {
    /* 基线：填充前的 dbsize（剔除服务器其它数据的干扰） */
    int base = get_dbsize(fd);

    /* 阶段 1：一次性填充（打包发送，逻辑同 fill_cmds，只是多带一个 ttl 参数） */
    char batch[MAX_BATCH];
    char ttlstr[16];
    snprintf(ttlstr, sizeof ttlstr, "%d", ttl);

    long long c0 = g_cmds;
    double    t0 = now_ms();

    int filled = 0;
    while (filled < count) {
        size_t len = 0;
        int nthis = 0;
        while (filled < count) {
            char key[24], val[24];
            pair(start + filled, "expire", key, sizeof key, val, sizeof val);
            int w = resp_cmd(batch + len, (int)(sizeof batch - len),
                             4, "SET", key, val, ttlstr);
            if (w < 0 || len + (size_t)w >= sizeof batch) break;   /* 批满 */
            len += (size_t)w;
            nthis++;
            filled++;
        }
        if (nthis == 0) break;
        send_all(fd, batch, (int)len);
        g_cmds += nthis;
        for (int k = 0; k < nthis; k++) {
            char what[64];
            snprintf(what, sizeof what, "expire[%d]",
                     start + filled - nthis + k);
            expect_reply("+OK\r\n", what);
        }
    }
    report(c0, t0, "expire fill ttl=%ds", ttl);

    int after = get_dbsize(fd);
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
 *  dbsize_cmds — DBSIZE 计数正确性闭环
 *
 *  每轮以 FLUSHALL 归零（空库下 DBSIZE 必须是 0，绝对断言，无需基线），
 *  再依次走下面各步，每一步之后断言 DBSIZE。整体循环 loop 次，设计同 basic_cmds。
 *
 *  真正要守的不变量：**记录数只在「键不存在 → 走到 add」时自增**。
 *  改写走两条分支（src/core/kvstore_dispatch.c 的 SET 覆盖 / MOD）：
 *    · 等长改写 → memcpy 原地写
 *    · 变长改写 → dict_resize_entry 重建条目
 *  两条都不许改记录数；SET 覆盖虽进了写路径但没 add，MOD 未命中则根本不进
 *  引擎写路径，更不该动记录数。各引擎的 xxx_add 是无条件自增的，一旦哪条
 *  路径误入，这里就会漏出来。
 *
 *  注意：本测试会 FLUSHALL 清空实例数据，只能对测试实例运行。
 *============================================================================*/

/** 发一条 DBSIZE 并断言等于 want；不符即失败退出 */
static void expect_dbsize(int fd, int want, const char *what) {
    int got = get_dbsize(fd);
    if (got != want) {
        printf("==> FAILED -> %s, EXPECTED DBSIZE: %d BUT GOT: %d\n",
               what, want, got);
        exit(1);
    }
}

void dbsize_cmds(int fd, int loop) {
    char cmd[256];
    long long c0 = g_cmds;
    double    t0 = now_ms();

    for (int i = 0; i < loop; i++) {
        /* 1. 归零：FLUSHALL 后必须是 0 */
        resp_cmd(cmd, sizeof cmd, 1, "FLUSHALL");
        exec_cmd(fd, cmd, "+OK\r\n", "FLUSHALL");
        expect_dbsize(fd, 0, "FLUSHALL 后归零");

        /* 2. 新增 3 个新 key → 3 */
        for (int j = 0; j < 3; j++) {
            char k[32];
            snprintf(k, sizeof k, "db:%d", j);
            resp_cmd(cmd, sizeof cmd, 3, "SET", k, "v");
            exec_cmd(fd, cmd, "+OK\r\n", "SET 新增");
        }
        expect_dbsize(fd, 3, "新增 3 个 key");

        /* 3. SET 撞已存在的键 → 覆盖（进写路径但没 add）→ 不增 */
        resp_cmd(cmd, sizeof cmd, 3, "SET", "db:0", "w");
        exec_cmd(fd, cmd, "+OK\r\n", "SET 覆盖已存在的键");
        expect_dbsize(fd, 3, "SET 覆盖后不增");

        /* 4. 等长改写（v → w）→ 不增 */
        resp_cmd(cmd, sizeof cmd, 3, "MOD", "db:0", "w");
        exec_cmd(fd, cmd, "+OK\r\n", "MOD 等长改写");
        expect_dbsize(fd, 3, "等长改写后不增");

        /* 5. 变长改写（v → a-longer-value）→ 走 resize_entry，仍不增 */
        resp_cmd(cmd, sizeof cmd, 3, "MOD", "db:1", "a-longer-value");
        exec_cmd(fd, cmd, "+OK\r\n", "MOD 变长改写");
        expect_dbsize(fd, 3, "变长改写后不增");

        /* 6. MOD 未命中 → nil，且不新建 → 不增 */
        resp_cmd(cmd, sizeof cmd, 3, "MOD", "db:never", "v");
        exec_cmd(fd, cmd, "$-1\r\n", "MOD 未命中");
        expect_dbsize(fd, 3, "MOD 未命中不新建");

        /* 7. 删除存在的 key → :1 且 -1 */
        resp_cmd(cmd, sizeof cmd, 2, "DEL", "db:0");
        exec_cmd(fd, cmd, ":1\r\n", "DEL 存在的 key");
        expect_dbsize(fd, 2, "DEL 后 -1");

        /* 8. 重复删除同一个 key → :0 且不再减 */
        resp_cmd(cmd, sizeof cmd, 2, "DEL", "db:0");
        exec_cmd(fd, cmd, ":0\r\n", "DEL 重复删除");
        expect_dbsize(fd, 2, "重复 DEL 不减");

        /* 9. 删除从未写过的 key → :0 且不变 */
        resp_cmd(cmd, sizeof cmd, 2, "DEL", "db:never");
        exec_cmd(fd, cmd, ":0\r\n", "DEL 不存在的 key");
        expect_dbsize(fd, 2, "DEL 不存在不减");
    }
    report(c0, t0, "dbsize loop=%d", loop);
}

/*=============================================================================
 *  savebench_cmds — 周期性 BGSAVE 对写入 QPS / 延迟的影响
 *
 *  结构：每写 per_round 条 SET，发一次 BGSAVE，如此 rounds 轮。
 *  key 用全局单调递增计数（key:0、key:1…），全程不重复。
 *
 *  与 shell 版 bench_save.sh 的关键差别：BGSAVE 和 SET 走**同一条连接、
 *  同一个循环**。shell 版每段起一个 redis-benchmark、用 redis-cli 另起进程发
 *  SAVE，中间那 ~170ms 空隙正是 fork 停顿掉落的地方 —— 那个停顿从来进不了
 *  任何测量窗口。这里没有空隙，服务端每一刻都在被测。
 *
 *  口径：
 *   · QPS 的墙钟只累加 SET 段，BGSAVE 自身的往返夹在两段之间、不计入。
 *     否则轮数多的低档位会因为多跑几次 BGSAVE 而吃亏，档与档就不可比。
 *   · 子进程落盘的 I/O 竞争落在**下一轮**的 SET 段里，会被如实计入 ——
 *     那才是「SAVE 对写入的影响」里持续的那部分。
 *   · BGSAVE 回 -ERR（上一次还没落完）是正常行为：只发和读，不判成败、
 *     不计数。所以「每 per_round 条一次 BGSAVE」的准确含义是「尝试一次」。
 *
 *  循环内零分配、零 I/O、零 printf —— 记录本身不能污染测量。延迟数组在循环
 *  外一次分配，跑完才排序。每条命令严格「一条在飞」：发一条、收完回复、记
 *  时间，即 c=1 P=1，与 redis-benchmark 同口径。
 *============================================================================*/

#define SB_VAL_LEN 16          /* value 定长 16B，与 bench_save.sh 的 VSIZE 一致 */

static int cmp_u32(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

void savebench_cmds(int fd, long long per_round, int rounds) {
    if (per_round <= 0 || rounds <= 0) {
        fprintf(stderr, "per_round/rounds 必须 > 0\n");
        exit(2);
    }

    long long total = per_round * rounds;
    uint32_t *lat = malloc(sizeof(uint32_t) * (size_t)total);
    if (lat == NULL) { perror("malloc"); exit(1); }

    char val[SB_VAL_LEN + 1];
    memset(val, 'x', SB_VAL_LEN);
    val[SB_VAL_LEN] = '\0';

    char cmd[MAX_MSG_LENGTH], rep[MAX_MSG_LENGTH];
    long long idx = 0;
    double work_us = 0.0;

    for (int r = 0; r < rounds; r++) {
        double ts = now_us();

        for (long long i = 0; i < per_round; i++) {
            char key[32];
            snprintf(key, sizeof key, "key:%lld", idx);

            int len = resp_cmd(cmd, sizeof cmd, 3, "SET", key, val);
            if (len < 0) { fprintf(stderr, "resp_cmd 失败\n"); exit(1); }

            double t0 = now_us();
            send_all(fd, cmd, len);
            recv_reply(&g_rb, rep, sizeof rep);   /* 读掉即扔，不比较内容 */
            lat[idx] = (uint32_t)(now_us() - t0);
            idx++;
            g_cmds++;
        }

        work_us += now_us() - ts;                 /* 只累加 SET 段 */

        resp_cmd(cmd, sizeof cmd, 1, "BGSAVE");   /* 发了就继续，不计时、不看回复 */
        send_all(fd, cmd, (int)strlen(cmd));
        recv_reply(&g_rb, rep, sizeof rep);
        g_cmds++;
    }

    qsort(lat, (size_t)total, sizeof(uint32_t), cmp_u32);

    /* 百分位按下标取（不插值）：排序后第 total*P/100 个 */
    long long i50 = total * 50 / 100;
    long long i99 = total * 99 / 100;

    printf("savebench 每轮 %lld 条 × %d 轮 = %lld 条\n", per_round, rounds, total);
    printf("QPS %lld   P50 %.3f ms   P99 %.3f ms\n",
           work_us > 0.0 ? (long long)(total / (work_us / 1e6)) : 0,
           (double)lat[i50] / 1000.0,
           (double)lat[i99] / 1000.0);
    fflush(stdout);

    free(lat);
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
    printf("  %s <ip> <port> dbsize   [loop]\n", prog);
    printf("  %s <ip> <port> savebench <per_round> <rounds>\n", prog);
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

    /* savebench 的 <每轮条数> <轮数> */
    long long sb_per = 0;
    int       sb_rounds = 0;
    if (strcmp(test, "savebench") == 0) {
        if (argc < 6) { usage(argv[0]); return 2; }
        sb_per    = atoll(argv[4]);
        sb_rounds = atoi(argv[5]);
        if (sb_per <= 0 || sb_rounds <= 0) {
            fprintf(stderr, "per_round/rounds 必须 > 0\n");
            return 2;
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
    } else if (strcmp(test, "dbsize") == 0) {
        dbsize_cmds(g_rb.fd, loop);
    } else if (strcmp(test, "savebench") == 0) {
        savebench_cmds(g_rb.fd, sb_per, sb_rounds);
    } else {
        fprintf(stderr, "未知测试: %s\n", test);
        usage(argv[0]);
        return 2;
    }

    close(g_rb.fd);
    return 0;
}
