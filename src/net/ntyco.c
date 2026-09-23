/*=============================================================================
 *  ntyco.c — NtyCo 协程网络引擎
 *
 *  与 reactor(epoll)/proactor(io_uring) 平级的第三种网络模型：每连接一个
 *  协程（NtyCo 协程库），阻塞点在让出调度器处（nty_wait_readable / nty_send /
 *  nty_accept），fd 就绪后恢复继续——代码按同步顺序写，运行时由协程调度器异步推进。
 *
 *  本引擎接入 rbuf 后与其余引擎读缓冲统一（2026-09）：
 *  - 读缓冲 = rbuf 节点队列（rbuf_t，节点 RBUF_NODE_SIZE 全引擎一致）；
 *  - 数据入口 = 注入的 kvs_rbuf_drain_fn（三引擎统一 kvs_drain_rbuf_commands）；
 *  - 写缓冲沿用 DynBuf + w_off（半包发送记账），冲刷归本文件。
 *
 *  本引擎参与复制（2026-09 接入，见下方 "repl 接线" 段）：
 *  - ntyco_start 赋值 repl_engine_register_rbuf / repl_engine_append，
 *    引擎每轮 tick 经常驻 ticker 协程调 repl_tick；
 *  - 全量同步不经过本引擎（master fork 子进程走数据面端口，slave 内联阻塞
 *    接收，见 repl_fullsync.c），本引擎只承载控制面那一条 PSYNC 链路。
 *
 *  语义保证：
 *  - 单协程/连接 ⇒ server_reader 内无抢占点 ⇒ drain 期间无调度切换，
 *    g_kvs_drain_fd 与 rbuf reserve/commit 配对语义可靠；
 *  - 读路径「先直读、读空才挂起」（2026-09-17）：每轮先 nty_recv_nowait
 *    （syscall 直读，不登记 epoll、不让出），只有真读空（EAGAIN）才挂起等就绪。
 *    挂起走的是**本文件自己的 nty_wait_readable**，不用库的 nty_recv —— 那函数
 *    是"无条件先登记 → 换栈让出 → 唤醒 → 注销 → 才读"（nty_socket.c:76 的
 *    nty_poll_inner 里那次 yield 没有前置判断），内核缓冲里有数据也要先停/醒
 *    一轮，而一次读最多吃满一个 rbuf 节点（RBUF_NODE_SIZE = 1024B），P 一深
 *    一批就要读好几次，停/醒随之翻倍。实测（-c 1，P=160）：改前每批约 100 次
 *    系统调用（epoll_ctl 16.1 + rt_sigprocmask 16.9 + epoll_wait 8.1），
 *    reactor 约 16 次；服务端 CPU 2.969µs 每命令 vs reactor 0.969µs。
 *    而两边**真正的 recv/send 次数完全一样**（每次读 4.0 次/批）——
 *    多出来的全是包裹在读写外面的协程停/醒。
 *
 *    （2026-09-18 补）上面这段只解决了"一批要读好几次"的场景，**PING / P=1 一点
 *    没改善**——那两档每往返只有一条命令，直读探测必然落空，唯一那次读永远走
 *    挂起路径。查下去发现 nty_recv 是**两轮**停/醒不是一轮：它体内那句 recv()
 *    被同编译单元里 COROUTINE_HOOK 的强符号 recv() 抢走（hook 版 recv 在
 *    nty_socket.c:416，无条件再 nty_poll_inner 一次）。单轮停/醒后每往返系统
 *    调用 13 → 8，PING 从 reactor 的 85% 回到 100~107%。详见第 2 步的注释。
 *
 *    为什么是"本文件自己复刻一轮"而不是"改 NtyCo 源码"：NtyCo 是 git 子模块
 *    （上游 72ab5fd），改了父仓库工作区就是脏的、SHA 仍钉在上游，而
 *    git submodule update --init / 重新 clone 会**静默丢掉**这些改动。
 *    改 nty_coroutine.h:325 的 `#define COROUTINE_HOOK` 关 hook 也不行：那个
 *    define 是无条件的（编译期 -U 挡不住），要关同样得改子模块；且关掉会连带
 *    失去 socket()/accept() hook 顺手设的 O_NONBLOCK，裸 recv 会把整个调度线程
 *    阻塞住（是挂死不是变慢）。所以走本文件自持：只用 nty_coroutine.h 的公开
 *    接口，子模块保持上游原样。细节与不变量见 nty_wait_readable 的注释。
 *  - 回复「攒到读空再发」：写合并成一次 sendto（reactor 由主循环 send_cb 同效），
 *    代价是 EOF / want_close 这些收尾路径必须显式补一次冲刷，否则丢掉已攒回复；
 *  - drain 入口要求 w_off==0（wb 追加点 = 物理尾部，与 reactor/proactor 同一
 *    不变量，大块回复可直接追加）。普通连接有 nty_flush_reply 发空后清零保底；
 *    唯一的例外是 repl 链路（只走不让出的 raw send，可能残留 w_off），
 *    故循环开头补了一次逻辑尾对齐（同 reactor recv_cb 顶部）；
 *  - 单缓冲形态（底层 recv，NtyCo/core/nty_socket.c:211）：一次 reserve
 *    取一段即可，与 rbuf 的单段暴露语义一致（三引擎同此一条路）。
 *  - EAGAIN 与真错误分开处理：直读与醒来后那次读都原样透传内核的 errno，
 *    EAGAIN 是"暂时无数据"，重试而非断连。
 *
 *  repl 接线（本文件最需要小心的一块）：
 *  协程模型没有 reactor 那种"非抢占事件循环"来兜底异步发送。engine 的
 *  repl_engine_append 会被**任意连接的协程**调用（master 每执行一条写命令，
 *  repl_incr_propagate 就往从库链路推一条），所以发送必须由引擎自己负责。
 *  为此确立一条不变量：
 *
 *    repl 链路的出站字节流只存在于该连接自己的 write_buf 里，
 *    且只由**不让出**的上下文冲刷（不让出的裸 send，见 nty_repl_flush）。
 *
 *  为什么不能沿用 server_reader 那条会 yield 的 nty_send 冲刷：PSYNC 命中
 *  backlog 窗口时 master_enter_streaming 会把 "+CONTINUE" + 最多 1MB 回补
 *  写进连接 wb，nty_send 发不全时会 nty_poll_inner 让出；让出期间别的协程
 *  append 的字节若被直发，就会插到尚未发完的回补字节之前 —— 从库把回补
 *  字节当命令流解析，静默错乱。一个缓冲 + 一个不让出的写者，顺序才平凡成立。
 *
 *  ⚠ 本引擎里"不让出"不能用 libc 的 send/write/recv 来实现 ——
 *  NtyCo/core/nty_socket.c 有一套 COROUTINE_HOOK，用**全局强符号**重定义了
 *  send / write / recv / recvfrom / sendto / socket / accept / connect，
 *  覆盖 libc，全进程生效（close 是唯一的例外，它只是透传）。
 *  其中 send/write 发不全时都会 nty_poll_inner 让出协程，所以本项目里
 *  "裸 send" 其实是会 yield 的。要真正不让出只有两条路：syscall(SYS_sendto)
 *  （nty_repl_flush 走这条），或直接调 hook 里保存的 send_f 函数指针。
 *============================================================================*/

#include "nty_coroutine.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>   /* TCP_NODELAY（accept 后设，见该处注释） */
#include <sys/syscall.h>   /* SYS_sendto：绕开 NtyCo 对 send 的符号劫持，见 nty_repl_flush */
#include <unistd.h>        /* syscall() */
#include <fcntl.h>
#include <errno.h>      /* EAGAIN/EWOULDBLOCK：区分"暂时无数据"与真错误 */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "protocol.h"   /* kvs_rbuf_drain_fn（typedef 在协议头，间接含 dynbuf.h） */
#include "rbuf.h"       /* rbuf_t / RBUF_NODE_SIZE */
#include "kvstore.h"    /* repl_engine_* 访问点 / kvs_config（ticker 起停判据） */
#include "repl.h"       /* repl_on_close / repl_tick / repl_incr_master_streaming_fd */

/* 默认解析回调入口（kvs_rbuf_drain_fn）：ntyco_start 时由上层统一注入
 * kvs_drain_rbuf_commands。与 reactor/proactor 的 g_default_handler 同名对齐：
 * accept 的新连接把它拷进 client->on_data，之后 server_reader 一律走
 * client->on_data —— 三引擎"每连接解析回调 = on_data"命名统一 */
static kvs_rbuf_drain_fn g_default_handler = NULL;

/* 协程客户端结构体（堆分配，每连接独立） */
typedef struct {
    int fd;
    rbuf_t read_rb;    /* rbuf 节点队列读缓冲（与 reactor/proactor 统一） */
    DynBuf write_buf;  /* 动态写缓冲（批量回复核心） */
    int w_off;         /* 已发送前缀（半包发送记账） */
    kvs_rbuf_drain_fn on_data;  /* 解析回调入口：与 reactor/proactor 的 conn.on_data
                                   同名对齐；普通连接为默认入口，slave 出站链路
                                   由 eng_repl_register_rbuf 换成 repl 专用 handler */
    int repl_link;     /* 1 = repl 链路：出站由 ticker 独占冲刷（禁走会 yield 的
                          nty_send，理由见文件头 "repl 接线"） */
} NtyClient;

/*-------------------------------------------------------------------------
 *  repl 接线（引擎 ↔ 复制模块）
 *
 *  一主一从（repl 侧 g_slave_fd / g_repl_fd 都是单例），故引擎侧同样只维护
 *  一个"当前 repl 链路"指针，不引入 reactor 那样的 fd→conn 连接表。
 *-------------------------------------------------------------------------*/

static NtyClient *g_repl_client = NULL;   /* 当前 repl 链路；NULL = 无 */

/* 本段位置在两者的定义之前，先声明（定义分别见文件下部）：
 * - server_reader：eng_repl_register_rbuf 复用它承载 repl 链路
 * - set_nonblock：收养 fd 时必须置非阻塞（理由见 eng_repl_register_rbuf） */
static void server_reader(void *arg);
static void set_nonblock(int fd);

/**
 * 冲刷 repl 链路待发数据：裸 send，**永不让出协程**
 *
 * 不让出是这条链路的正确性前提（见文件头）：只有一个写者、且写者不被切走，
 * 顺序才天然成立，也不会出现"nty_send 持有 buf 跨 yield、期间 append 触发
 * realloc"的悬垂指针。
 *
 * 调用者：ticker 协程（周期补发残量）+ repl 链路自己的 server_reader（当轮即发）。
 * @param c repl 链路 client
 */
static void nty_repl_flush(NtyClient *c) {
    while (c->write_buf.len > 0) {
        /* 必须走 syscall(SYS_sendto)，不能写 send()：NtyCo/core/nty_socket.c 里
         * 用**全局符号**重定义了 send/write/recv/accept/connect（那套 COROUTINE_HOOK），
         * 它们对 libc 的强符号覆盖是全进程生效的。其中 send() 在发不全时会
         * nty_poll_inner → 让出协程——那本函数的"永不让出"就是假的：实测 ticker
         * 里一次半包 send 就让本协程带着"fd 已被链路协程占用"的键插进 waiting
         * 红黑树，触发 NtyCo 的 assert(co_tmp == NULL) 直接 abort。
         * syscall() 绕开符号解析，拿到的是内核的 sendto，不经过任何 hook。 */
        ssize_t n = (ssize_t)syscall(SYS_sendto, c->fd,
                                     c->write_buf.buf + c->w_off,
                                     (size_t)c->write_buf.len,
                                     MSG_NOSIGNAL, NULL, (size_t)0);
        if (n > 0) {
            c->w_off         += (int)n;
            c->write_buf.len -= (int)n;
            continue;
        }
        if (n == 0)
            return;                                  /* 不该发生：当作发不动，下跳再试 */
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;                                  /* 发不全：下一跳（≤100ms）再试 */
        /* 真错误（EPIPE/ECONNRESET）：只关读方向，让链路协程自己观察到 EOF 走
         * cleanup。绝不能在这里 close/free —— 本函数从 ticker 调用，越俎代庖会
         * 让链路协程永远挂在调度器上，且 fd 被 accept 复用时 epoll 注册与调度器
         * 等待树都会冲突（epoll_ctl ADD EEXIST / RB_INSERT 断言）。 */
        shutdown(c->fd, SHUT_RDWR);
        return;
    }
    c->w_off = 0;
    dynbuf_clear(&c->write_buf);   /* 仅发空后清，保持 w_off==0 不变量 */
}

/**
 * 让出协程，msecs 毫秒后由调度器唤醒（msecs 必须 > 0）
 *
 * **不能只调 nty_coroutine_sleep**：看 NtyCo/core/nty_schedule.c:87 的实现，
 * msecs>0 那条路只把本协程插进 sleeping 红黑树 + 置 SLEEPING 标志就返回了，
 * 函数末尾那行 `//yield` 是句空注释 —— 它不让出。只调它 = 一个不让出的死循环，
 * 会饿死整个调度器（实测：master 起了 ticker 之后连 redis-cli DBSIZE 都不回，
 * 而 repl_role off 时一切正常）。而 msecs==0 那条路是"插 ready 队列 + yield"，
 * 会让 ticker 满速空转，也不行。
 *
 * 故此处补一次显式 yield —— nty_coroutine_sleep / nty_coroutine_yield /
 * nty_coroutine_get_sched 三个都是 nty_coroutine.h 里的公开 API，NtyCo 本身不动。
 */
static void nty_sleep_yield(uint64_t msecs) {
    nty_coroutine_sleep(msecs);                                     /* 登记到 sleeping 树 */
    nty_coroutine_yield(nty_coroutine_get_sched()->curr_thread);    /* 真正让出 */
}

/**
 * ticker 协程：引擎侧的"每轮事件循环"
 *
 * 协程模型没有可挂接的主循环钩子（nty_schedule_run 在 NtyCo 内部转，不可改），
 * 故用一个常驻协程以 100ms 周期充当 reactor/proactor 主循环里
 * update_cached_time + repl_tick 的角色，外加 repl 链路冲刷。
 * 顺序：先 tick（slave 侧推进连接/重连，会把 PSYNC 塞进 wb）再 flush，
 * 这样刚收养的新链路同一轮就把 PSYNC 发出去。
 * 另两个引擎的主循环同样是无条件每轮跑这两件事，本协程与 repl_role 无关。
 */
static void nty_ticker(void *arg) {
    (void)arg;
    while (1) {
        /* 优雅停机：本协程是 ntyco 唯一的"每轮"，所以也是唯一的检查点。
         *
         * reactor / proactor 的主循环是 `while (!g_kvs_stop)`，退出后控制权落回
         * main 的清理段；ntyco 没有这条回路 —— ntyco_start() 进的是
         * nty_schedule_run()，它的退出条件是四个协程队列（ready/waiting/
         * sleeping/busy）全空，而监听协程与本体 ticker 协程常驻，永不满足，
         * 也不会因信号中断而返回（nty_schedule_epoll 对 EINTR 是 continue 重进
         * epoll_wait，见 NtyCo/core/nty_schedule.c）。于是 main 的清理段永远到不了。
         *
         * 由本协程替 main 把这段走完再 exit(0)：kvs_shutdown 与 main 第 6 段是
         * 同一个函数，两条路径的停机行为一致。放在循环首行是为了让
         * "收到信号到真正退出"最多再等一个 100ms 周期（下面那次 sleep）。
         */
        if (g_kvs_stop) {
            printf("[ntyco] shutting down gracefully\n");   /* 与 reactor.c 同名同措辞 */
            kvs_shutdown();
            exit(0);
        }
        /* 刷新缓存时钟：reactor/proactor 在各自事件循环里每轮调一次
         * （reactor.c / proactor.c 的 update_cached_time），而 ntyco 的
         * 调度循环在 NtyCo 里、改不得 —— 本 ticker 就是本引擎的"每轮"。
         * 不刷的话 get_time_now() 恒返回 0（g_cached_ms 是零初始化的
         * static，没有任何地方给它初值），slave 的退避门槛
         * `now < g_next_retry` 会永久为真：连一次失败后再也不重连。 */
        update_cached_time();
        repl_tick();                                    /* slave：连接/重连推进；master：no-op */
        if (g_repl_client)
            nty_repl_flush(g_repl_client);              /* 补发残量 / 探测断链 */
        nty_sleep_yield(100);
    }
}

/**
 * 全量同步的传输子进程入口（repl_engine_postfork_child 的实现）
 *
 * 把本进程从 NtyCo 调度器里摘出去：清掉 TSD 里的 sched 后，
 * nty_coroutine_get_sched() 返回 NULL，COROUTINE_HOOK 的每个强符号
 * （socket/accept/connect/recv/send/write）都走各自的透传分支直接调 libc，
 * 于是子进程变回一个普通的阻塞式进程 —— 不再有人往 epoll 里塞 fd、
 * 也不再有人 yield 进调度循环。
 *
 * **不这么做会崩 master 主进程**：epoll fd 跨 fork 共享同一个内核对象，
 * 子进程里被劫持的 accept() 会无条件 nty_poll_inner → EPOLL_CTL_ADD 把子进程的
 * 数据面监听 fd 加进**父进程的 epoll**；slave 连上该端口后父进程 epoll_wait
 * 收到这个 fd 的事件，而父进程等待树里没有任何协程等它 →
 * nty_schedule_search_wait 的 assert(co != NULL) 直接 SIGSEGV。
 *
 * 只清 TSD 不做别的：子进程紧接着 _exit()，既有协程结构体一概不用再碰。
 */
static void ntyco_postfork_child(void) {
    pthread_setspecific(global_sched_key, NULL);
}

/**
 * 追加待发送数据到 repl 链路（repl_engine_append 的实现）
 *
 * 由 repl 从**任意连接**的协程里调用（master 每执行一条写命令推一次）。
 * 本函数只入队不发送：发送由 nty_repl_flush 在不让出的上下文里完成。
 *
 * @param fd 目标连接 fd（只可能是 streaming 从库链路）
 * @return 0 成功，-1 失败（无链路 / fd 不匹配 / OOM）
 */
static int eng_repl_append(int fd, const char *data, int len) {
    NtyClient *c = g_repl_client;
    if (!c || c->fd != fd) return -1;

    /* 逻辑尾追加（w_off + len 处），形状与 reactor.c 的 eng_repl_append 一致。
     * 必须是这个形状、不能用 dynbuf_cat：断点续发时 w_off > 0，dynbuf_cat 写
     * buf+len 会落到已发前缀区造成乱序。扩容也不做几何增长——实测 20 万 key
     * （12MB 积压）的 SIGSTOP 用例下与 reactor 的线性 +4096 耗时无差别，
     * 单缓冲区不值得为"理论上的 O(n²)"多留一段分歧。 */
    size_t tail = (size_t)(c->w_off + c->write_buf.len);
    if (tail + (size_t)len > (size_t)c->write_buf.cap) {
        size_t need = tail + (size_t)len + 4096;   /* 额外余量，少 realloc */
        char *nb = (char *)realloc(c->write_buf.buf, need);
        if (!nb) return -1;
        c->write_buf.buf = nb;
        c->write_buf.cap = (int)need;
    }
    memcpy(c->write_buf.buf + tail, data, (size_t)len);
    c->write_buf.len += len;
    return 0;
}

/**
 * 收养一个已连接的出站 fd 作为 repl 链路（repl_engine_register_rbuf 的实现）
 *
 * 仅 slave 侧调用（repl_incr_slave.c 的 slave_adopt）：fd 由 slave_connect_once
 * 建好并连上，本函数把它包成 NtyClient 并起一个 server_reader 协程跑给定
 * handler —— server_reader 本来就走 client->on_data，换 handler 即换角色。
 *
 * @param fd 已连接的 fd
 * @param handler repl 链路解析入口（repl_incr_slave_link_rbuf_handler）
 * @return 0 成功，-1 失败
 */
static int eng_repl_register_rbuf(int fd, kvs_rbuf_drain_fn handler) {
    /* 收养契约：slave_connect_once 注释明写"引擎收养后会把 fd 置为非阻塞"。
     * 这不是装饰——下面 ticker 会做裸 send，fd 若仍是阻塞的，一次 send 就能
     * 卡死整个调度器。 */
    set_nonblock(fd);

    NtyClient *c = (NtyClient *)malloc(sizeof(NtyClient));
    if (!c) goto fail;

    c->fd        = fd;
    rbuf_init(&c->read_rb, RBUF_NODE_SIZE);
    dynbuf_init(&c->write_buf, KVS_NET_BUF_INIT);
    c->w_off     = 0;
    c->on_data   = handler;
    c->repl_link = 1;

    nty_coroutine *co = NULL;
    if (nty_coroutine_create(&co, server_reader, c) != 0) {
        rbuf_destroy(&c->read_rb);
        dynbuf_free(&c->write_buf);
        free(c);
        goto fail;
    }

    g_repl_client = c;
    return 0;

fail:
    /* 必须通知 repl 侧复位 g_repl_fd：slave_adopt 先置 g_repl_fd = fd 再调本函数
     * 且忽略返回值，失败若不上报，repl_incr_slave_tick 会因 g_repl_fd >= 0
     * 永久早退——从库再也不会重连。 */
    close(fd);
    repl_on_close(fd);
    return -1;
}

/**
 * 直读一段：**不登记 epoll、不让出协程**
 *
 * 为什么不能用 nty_recv / 裸 recv()：见 NtyCo/core/nty_socket.c:211（nty_recv）
 * —— "先 nty_poll_inner（epoll_ctl ADD + 挂 waiting 树 + 换栈让出），被唤醒之后
 * 才真正 recv"。内核缓冲里明明有数据，也要先停/醒一轮。一次读最多吃满一个 rbuf
 * 节点（RBUF_NODE_SIZE = 1024B），pipeline 一深一批要读好几次，停/醒随之翻倍。
 *（2026-09-18 记：nty_recv 还是**两轮**停/醒——它体内那句 recv() 被同编译单元的
 *  hook 抢走——所以读路径干脆连它也不用了，改走本文件自己的 nty_wait_readable。）
 *
 * 走 syscall() 绕开符号解析，拿到的是内核的 recvfrom，不经过任何 hook ——
 * 与 nty_repl_flush 用 syscall(SYS_sendto) 是同一个手法、同一个理由。
 * 对已连接的 TCP socket，带 NULL 地址的 recvfrom 等价于 recv。
 *
 * @return >0 读到的字节数；0 对端关闭；-1 出错（errno 已置，EAGAIN = 暂无数据）
 */
static ssize_t nty_recv_nowait(int fd, void *buf, size_t len) {
    return (ssize_t)syscall(SYS_recvfrom, fd, buf, len, 0, NULL, (size_t)0);
}

/**
 * 等 fd 可读：**自己登记 + 自己让出**，一次等待只停/醒一轮
 *
 * 为什么不直接调库里的 nty_recv（NtyCo/core/nty_socket.c:211）：它体内那句
 * recv() 被同编译单元的 hook 抢走（hook 版 recv 在 nty_socket.c:416，无条件先
 * nty_poll_inner 再 recv_f 真读），于是**一次调用停/醒两轮**。而 nty_poll_inner
 * 是 static、库外面调不到，所以这里照它的时序自己做一遍 —— 只停一轮，
 * 且**不依赖 NtyCo 侧的任何补丁**（子模块保持上游原样，见文件头）。
 *
 * 时序必须与 nty_poll_inner 逐步一致，这是**正确性**要求不是风格问题：
 * 调度器唤醒时走 nty_schedule_search_wait(fd)（nty_schedule.c:121）从 waiting
 * 红黑树里按 fd 找人，而那个函数**没有 NULL 检查**（RB_FIND 落空后直接
 * `co->status = 0`）。也就是说库的隐含前提是
 *     「epoll 里有这个 fd  ⇔  waiting 树里有协程」
 * 下面五步就是维持这个不变量的最小序列，顺序不能动。
 *
 * 用到的全是 nty_coroutine.h 的公开接口（ntyco.c:79 已 include），没有一处
 * 碰 NtyCo 的内部符号：
 *   nty_coroutine_get_sched()    nty_coroutine.h:270（static inline）
 *   sched->poller_fd / curr_thread  nty_coroutine.h:165,168
 *   nty_schedule_sched_wait()    nty_coroutine.h:297
 *   nty_coroutine_yield()        nty_coroutine.h:306
 *   nty_schedule_desched_wait()  nty_coroutine.h:296
 *
 * 两个参数细节：
 *  - events 传 POLLIN|POLLERR|POLLHUP（不是 EPOLLIN）：sched_wait 靠
 *    `events & POLLIN` 判方向来置 WAIT_READ，照抄 nty_poll_inner 的传法；
 *  - timeout 传 1 是库里的"纯事件等待"魔法值：sched_wait 内部
 *    `if (timeout == 1) return;`（nty_schedule.c:175）跳过挂睡眠树，于是不等
 *    超时、只等 fd 就绪 —— 正是我们要的（挂了 epoll 就不会丢唤醒，无需超时兜底）。
 *
 * @return 0 已就绪（醒后仍需自己真读一次，仍可能 EAGAIN）；
 *         -1 不在协程上下文（本引擎的调用点必然在协程里，返回 -1 仅作兜底）
 */
static int nty_wait_readable(int fd) {
    nty_schedule *sched = nty_coroutine_get_sched();
    if (sched == NULL)
        return -1;

    nty_coroutine *co = sched->curr_thread;

    /* 1. 登记：调度器只读 ev.data.fd（nty_schedule.c:349），谁登记的它不管 */
    struct epoll_event ev;
    ev.events  = EPOLLIN | EPOLLERR | EPOLLHUP;
    ev.data.fd = fd;
    epoll_ctl(sched->poller_fd, EPOLL_CTL_ADD, fd, &ev);

    /* 2. 挂 waiting 树（此刻起 fd 与协程同进同出）+ 3. 换栈让出 */
    nty_schedule_sched_wait(co, fd, POLLIN | POLLERR | POLLHUP, 1);
    nty_coroutine_yield(co);

    /* 4. 注销 + 5. 摘 waiting 树。次序同 nty_poll_inner：先 epoll 后树 */
    epoll_ctl(sched->poller_fd, EPOLL_CTL_DEL, fd, &ev);
    nty_schedule_desched_wait(fd);
    return 0;
}

/**
 * 冲刷普通连接的待发回复（nty_send 发不全时会登记就绪等待并让出）
 *
 * 与 nty_repl_flush 的分工：普通连接的出站只由本协程自己写，让出是安全的；
 * repl 链路必须用那个"永不让出"的版本，理由见文件头 "repl 接线"。
 *
 * @return 0 发空；-1 连接已不可写（调用方走 cleanup）
 */
static int nty_flush_reply(NtyClient *c) {
    while (c->write_buf.len > 0) {
        ssize_t n = nty_send(c->fd, c->write_buf.buf + c->w_off,
                             c->write_buf.len, MSG_NOSIGNAL);
        if (n <= 0)                      /* 发送失败：断连 */
            return -1;
        c->w_off         += (int)n;
        c->write_buf.len -= (int)n;
    }
    c->w_off = 0;
    dynbuf_clear(&c->write_buf);         /* 仅发空后清，保持 w_off==0 不变量 */
    return 0;
}

/**
 * @brief 客户端协程处理函数：异步读 -> 协议解析 -> 异步发送
 * @param arg 堆分配的 NtyClient 结构体
 */
static void server_reader(void *arg) {
    NtyClient *client = (NtyClient *)arg;
    int fd = client->fd;

    while (1) {
        /* 0. 逻辑尾对齐（同 reactor recv_cb 顶部那 5 行）。普通连接恒 w_off==0
         *    （nty_send 发空后清零），此分支不触发；repl 链路只走裸 send，发不全
         *    会残留已发前缀，而下面 on_data 里的 dynbuf_cat 要求"wb 物理尾部 ==
         *    逻辑尾部"（repl_incr_master.c 的 master_enter_streaming 明写了该前置）。 */
        if (client->write_buf.len > 0 && client->w_off > 0) {
            memmove(client->write_buf.buf,
                    client->write_buf.buf + client->w_off,
                    (size_t)client->write_buf.len);
            client->w_off = 0;
        }

        /* 1. 取一段可写空间，**先直读试一把**（reserve → 读 → commit 须配对）。
         *    直读不走调度器：库的 nty_recv（以及被 hook 覆盖的 recv()）是无条件
         *    "登记 EPOLLIN → 换栈让出 → 被唤醒 → 注销 → 才 recv"的，内核缓冲里
         *    明明有数据也要先停/醒一轮。而一次读最多吃满一个 rbuf 节点
         *    （RBUF_NODE_SIZE = 1024B，见 rbuf_reserve 的单段语义），P=160 的一批
         *    4000B 要读 4 次 —— 走那条路就是每批 4 轮停/醒。
         *    实测（-c 1，P=160）：改前每批约 100 次系统调用（epoll_ctl 16.1 /
         *    rt_sigprocmask 16.9 / epoll_wait 8.1），reactor 约 16 次；服务端
         *    CPU 2.969µs 每命令 vs reactor 0.969µs。而两边**真正的 recv/send
         *    次数完全一样**（recvfrom 4.0/批），多出来的全是这层协程停/醒。 */
        char    *base;
        uint32_t cap;
        if (rbuf_reserve(&client->read_rb, &base, &cap) != 1) {
            /* 空闲池空且新节点分配失败：OOM，断连 */
            fprintf(stderr, "[ntyco] fd %d rbuf reserve failed\n", fd);
            break;
        }

        ssize_t ret = nty_recv_nowait(fd, base, cap);   /* 不让出的裸读 */
        int err = (ret < 0) ? errno : 0;

        /* 2. 直读落空 = 这批请求读到头了：先把攒下的回复一次发出去，再让出等下一批。
         *    顺序不能反 —— 客户端正等整批回复，不放行它就不会发下一批，先让出就是
         *    死等（NODELAY 之前踩的是同一类"两边互等"的坑，见 accept 处的注释）。
         *    这一步同时就是写合并：整批回复攒到这里，合并成一次 sendto
         *    （reactor 由主循环的 send_cb 干这件事，效果相同）。 */
        if (ret < 0 && (err == EAGAIN || err == EWOULDBLOCK)) {
            if (!client->repl_link && nty_flush_reply(client) < 0) {
                rbuf_commit(&client->read_rb, 0);   /* 配对收尾在途 reserve 再走 */
                goto cleanup;                       /* 连接已不可写 */
            }
            /* 真空了才挂起等下一批：nty_wait_readable 自己登记 + 让出，只停一轮。
             * 醒来后必须自己再真读一次（库的 nty_recv 是"停完顺手帮你读"，我们
             * 拆成了两步），所以下面紧跟一句 nty_recv_nowait。
             *
             * 为什么不用库的 nty_recv —— 它一次调用停/醒**两轮**：
             *
             *   nty_recv (nty_socket.c:211)
             *     nty_poll_inner(...)        ← 停/醒一轮：登记 + 让出 + 唤醒 + 注销
             *     recv(fd, buf, len, flags)  ← 这句被自己的 hook 抢走
             *       = nty_socket.c:416 的强符号 recv（COROUTINE_HOOK 在
             *         nty_coroutine.h:325 打开），无条件又 nty_poll_inner 一轮，
             *         然后才 recv_f 真读
             *
             * 作者本意显然是"上面刚停过，下面只想真读"，被自己的 hook 劫持了。
             * 曾直接改 NtyCo 源码把那句换成 recv_f 修掉，但 NtyCo 是子模块，
             * git submodule update --init 会把补丁静默冲掉。故改为**本文件自己
             * 复刻一轮停/醒**（nty_wait_readable），子模块保持上游原样。
             *
             * 收益（-c 1，PING）：每往返 epoll_ctl 4.01→2.00、
             * rt_sigprocmask 4.17→2.11、epoll_wait 2.02→1.03，系统调用合计
             * 13→8；交替 A/B 三轮：ntyco/reactor 0.848 → 0.982 / 1.073，
             * ntyco/redis 0.904 → 1.104 / 1.100。
             * 为什么偏偏 PING/P=1 收益最大：这两档每往返只一条命令，直读探测必然
             * 落空（读到那 14 字节后的下一次探测就是 EAGAIN），唯一那次读永远落在
             * 挂起路径上，两轮停/醒全砸在关键路径；P=160 直读吃掉了 4 次读里前 3 次，
             * 停/醒被摊薄，所以那两档改前改后纹丝不动。
             * 剩下的 1 轮是协程引擎的底线（总得在某处阻塞等下一批请求），
             * ntyco 与 reactor 的每往返差距已回到个位数 µs。
             *
             * 顺带：nty_poll_inner 里 struct epoll_event ev 没清零，
             * ev.data.fd 只写低 4 字节，strace 打出的 u64 高 4 字节是栈垃圾
             * （曾据此误判成"同一 fd 有两处注册点"）。对 epoll 本身无害 ——
             * 我们自己的 nty_wait_readable 里那个 ev 是完整赋值的，无此问题。 */
            nty_wait_readable(fd);                  /* 真空了才登记 + 让出等就绪 */
            ret = nty_recv_nowait(fd, base, cap);   /* 醒了直接真读，不再停第二轮 */
            err = (ret < 0) ? errno : 0;
        }

        rbuf_commit(&client->read_rb, (int)ret);   /* 负值内部按 0 处理，配对必达 */

        if (ret == 0) {
            /* 对端关闭。回复现在攒到"读空"才发，EOF 可能先到 —— 不补这一下，
             * 本轮已解析命令的回复就随着下面的 cleanup 丢了。 */
            if (!client->repl_link)
                nty_flush_reply(client);
            break;
        }
        if (ret < 0) {
            /* 两次读（直读的那次、nty_wait_readable 醒后的那次）都走
             * nty_recv_nowait，原样透传内核的 errno。EAGAIN 是"暂时无数据"而非
             * 连接故障 —— 重试即可：下一轮照样先直读，直读落空会去
             * nty_wait_readable 挂起等就绪，不会空转。真错误才断连。
             * 被唤醒后仍读到 EAGAIN 是可能的（例如 EPOLLHUP 唤醒但数据已在别处
             * 消费掉），这条 continue 就是它的兜底：回到循环头重来一遍。 */
            if (err == EAGAIN || err == EWOULDBLOCK)
                continue;
            break;
        }

        /* 3. 16MB 护栏：单连接已收未消费逻辑字节上限（同 reactor/proactor） */
        if (rbuf_len(&client->read_rb) > 16u * 1024u * 1024u) {
            fprintf(stderr, "[ntyco] fd %d read buffer overflow\n", fd);
            break;
        }

        /* 4. 协议解析：统一入口 kvs_drain_rbuf_commands（rbuf 形态，accept 时
         *    存入 client->on_data）want_close：QUIT/协议错误置位，冲刷后由本函数
         *    关闭连接 */
        int want_close = 0;
        client->on_data(fd, &client->read_rb, &client->write_buf, &want_close);

        /* 5. repl 链路角色标记（本段无 yield 点，标记是原子的）
         *  - 本连接刚被 master_enter_streaming 记成 streaming 从库 → 收归冲刷权，
         *    此后 append 进来的字节由不让出的 nty_repl_flush 发，而非会 yield 的
         *    nty_send（理由见文件头 "repl 接线"）；
         *  - 反向：本轮 want_close（QUIT / 协议错）时先交还冲刷权，让下面那次末轮
         *    冲刷把最后一段回复（+FULLRESYNC 或错误）同步送完再关。 */
        if (!client->repl_link && repl_incr_master_streaming_fd() == fd) {
            client->repl_link = 1;
            g_repl_client = client;
        }
        if (client->repl_link && want_close) {
            client->repl_link = 0;
            if (g_repl_client == client) g_repl_client = NULL;
        }

        /* 6. 冲刷回复
         *  - repl 链路：出站权在 ticker，每轮都冲（不让出的裸 send，理由见文件头）
         *  - 普通连接：**不在这里冲**。本轮读满说明后面还有字节，回复先攒着，
         *    等第 2 步"直读落空"或下面的收尾路径合并成一次 sendto —— 见那个分支
         *    的注释。 */
        if (client->repl_link)
            nty_repl_flush(client);    /* 发不全留给下轮 / ticker */

        if (want_close) {
            /* QUIT / 协议错误：回复必须同步发完再关。上面那个反向分支已经把
             * repl_link 摘掉了（连带摘出 g_repl_client），这里走普通冲刷。 */
            if (nty_flush_reply(client) < 0)
                goto cleanup;
            break;
        }
    }

cleanup:
    /* repl 链路先摘链：ticker 协程每 100ms 会碰 g_repl_client，本协程 free 之后
     * 它必须已经看不见这个 client（否则 use-after-free）。本段无 yield 点。 */
    if (g_repl_client == client) g_repl_client = NULL;
    /* 通知 repl 侧复位（master：清 streaming 从库；slave：安排 1s 后重连）。
     * 对普通连接是 no-op —— 两个 handler 都只做整数比较，无条件调用安全。 */
    repl_on_close(fd);

    /* 资源释放（读缓冲节点 + 写缓冲 + fd + 协程私有结构） */
    rbuf_destroy(&client->read_rb);
    dynbuf_free(&client->write_buf);
    close(fd);
    free(client);
}

/**
 * @brief 设置文件描述符为非阻塞模式
 * @param fd 文件描述符
 */
static void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * @brief 监听协程：异步 accept 客户端连接
 * @param arg 监听端口
 */
void server(void *arg) {
    unsigned short port = *(unsigned short *)arg;

    /* 1. 创建 TCP 套接字 */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket failed");
        return;
    }

    /* 2. 端口复用 + 非阻塞 */
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nonblock(listen_fd);

    /* 3. 绑定地址 */
    struct sockaddr_in local = {0};
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (struct sockaddr *)&local, sizeof(local)) < 0) {
        perror("bind failed");
        close(listen_fd);
        return;
    }

    /* 4. 监听连接 */
    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen failed");
        close(listen_fd);
        return;
    }
    printf("✅ NtyCo协程模型启动 | 监听端口：%d | 高并发就绪\n", port);

    /* 5. 异步 accept 循环（NtyCo 协程模式） */
    while (1) {
        struct sockaddr_in remote = {0};
        socklen_t len = sizeof(remote);
        int cli_fd = nty_accept(listen_fd, (struct sockaddr *)&remote, &len);

        if (cli_fd < 0) {
            continue;
        }
        /* 客户端 socket 设置非阻塞 */
        set_nonblock(cli_fd);

        /* 禁用 Nagle（reactor/proactor 都在 accept 后设了，本引擎原先漏了）。
         * 必须设，否则 pipline 深了会塌：
         *   回复是「一次 recv 收多少就 flush 多少」——一次 recv 最多收满一个
         *   rbuf 节点（RBUF_NODE_SIZE = 1024B），所以一批请求只要超过 1024B
         *   （echo hello 25B/条 → P ≥ 41）就会分成 ≥2 次 recv、≥2 次 send。
         *   第 2 次 send 时第 1 次的数据还没被 ACK → Nagle 把它扣在发送队列里
         *   （几百字节是 sub-MSS 小段）；而客户端正等整批回复才发下一批，不会
         *   带出 ACK → 只能干等 delayed ACK 超时，每批固定 +40ms。
         * 实测（-c 1）：P ≤ 40 每批 0.9ms，P ≥ 41 起每批恒定 41.0ms（P=41/80/160
         * 的 ms/批 全是 41.0），QPS 只随 P 线性爬；strace 服务端 sendto 可见
         * 批间间隔清一色 39~41ms。加上本行后 P=160 由 3.9k 回到 20 万级。
         * 注意 setsockopt 不在 NtyCo 的 COROUTINE_HOOK 劫持名单里（那份名单只有
         * send/write/recv/recvfrom/sendto/socket/accept/connect），可直接调。 */
        int nodelay = 1;
        setsockopt(cli_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        /* 6. 堆分配客户端结构体（避免栈溢出，多连接安全） */
        NtyClient *client = (NtyClient *)malloc(sizeof(NtyClient));
        if (!client) {
            close(cli_fd);
            continue;
        }

        /* 7. 初始化客户端状态（读缓冲 rbuf + 写缓冲 DynBuf，与 reactor/proactor 一致） */
        client->fd = cli_fd;
        rbuf_init(&client->read_rb, RBUF_NODE_SIZE);
        dynbuf_init(&client->write_buf, KVS_NET_BUF_INIT);
        client->w_off = 0;
        client->on_data = g_default_handler;  /* 解析回调入口（accept 时拷入，见 ntyco_start） */
        client->repl_link = 0;                /* 入站连接恒非 repl 链路（那条走 eng_repl_register_rbuf） */

        /* 8. 创建协程处理客户端（一对一协程模型） */
        nty_coroutine *read_co;
        if (nty_coroutine_create(&read_co, server_reader, client) != 0) {
            /* 协程创建失败，释放资源 */
            rbuf_destroy(&client->read_rb);
            dynbuf_free(&client->write_buf);
            free(client);
            close(cli_fd);
        }
    }
}

/**
 * @brief NtyCo 协程模型启动入口
 * @param port 监听端口
 * @param handler 命令解析处理函数（kvs_rbuf_drain_fn，与 reactor/proactor 统一）
 * @return 0
 */
int ntyco_start(unsigned short port, kvs_rbuf_drain_fn handler) {
    g_default_handler = handler;

    /* repl 接线：引擎侧三个函数指针（repl → 引擎）。repl_on_close / repl_tick 是
     * 直接函数不需注册。非复制场景也照常赋值 —— 赋值本身无副作用，真正决定
     * "跑不跑"的是 dispatch 层的门控（repl_role，见 kvstore_dispatch.c）。 */
    repl_engine_register_rbuf = eng_repl_register_rbuf;
    repl_engine_append        = eng_repl_append;
    repl_engine_postfork_child = ntyco_postfork_child;

    /* 创建监听协程 */
    nty_coroutine *listen_co = NULL;
    nty_coroutine_create(&listen_co, server, &port);

    /* 常驻 ticker 协程：协程模型没有可挂接的主循环钩子，故用它以 100ms 周期
     * 充当 reactor/proactor 主循环里 update_cached_time + repl_tick 的角色
     * （见 nty_ticker）。无条件起 —— 另外两个引擎的主循环也是每轮无条件跑这
     * 两件事（reactor.c:540 / proactor.c:526 一带，超时同为 100ms），不按
     * repl_role 分支；缓存时钟尤其依赖它（否则 get_time_now 恒 0）。 */
    nty_coroutine *tick_co = NULL;
    nty_coroutine_create(&tick_co, nty_ticker, NULL);

    /* 启动 NtyCo 协程调度器。说明：调度器只在不持有任何协程时返回，而监听协程
     * 与 ticker 协程都常驻 ⇒ 本函数不返回，下面的 return 到不了，main 的清理段
     * 也到不了。停机由 ticker 协程代劳：它每轮读 g_kvs_stop，命中就调
     * kvs_shutdown()（与 main 第 6 段同一个函数）再 exit(0) —— 于是 SIGINT/
     * SIGTERM 能优雅停机，NtyCo 本身仍一行不改。详见 nty_ticker 的说明。 */
    nty_schedule_run();
    return 0;
}
