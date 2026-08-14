/**
 * rdma_repl.c — RDMA 全量同步会话管理
 *
 * fork 子进程生命周期管理：SIGCHLD 回收、PID 追踪、清理。
 */

#include "rdma_repl.h"
#include "tcp_repl.h"
#include "kvstore.h"
#include "repl_incr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netdb.h>
#include <pthread.h>

#define MAX_CHILD_SESSIONS  16

static struct {
    pid_t pids[MAX_CHILD_SESSIONS];
    int count;
    pthread_mutex_t lock;
} g_sessions = {.lock = PTHREAD_MUTEX_INITIALIZER};

static void sigchld_handler(int sig) {
    (void)sig;
    int status; pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_sessions.lock);
        for (int i = 0; i < g_sessions.count; i++)
            if (g_sessions.pids[i] == pid) {
                g_sessions.pids[i] = g_sessions.pids[--g_sessions.count];
                break;
            }
        pthread_mutex_unlock(&g_sessions.lock);
        if (WIFEXITED(status))
            printf("[RDMA-REPL] child %d exit=%d\n", pid, WEXITSTATUS(status));
        else if (WIFSIGNALED(status))
            fprintf(stderr, "[RDMA-REPL] child %d signal=%d\n", pid, WTERMSIG(status));
    }
}

void session_add(pid_t pid) {
    pthread_mutex_lock(&g_sessions.lock);
    if (g_sessions.count < MAX_CHILD_SESSIONS)
        g_sessions.pids[g_sessions.count++] = pid;
    else
        fprintf(stderr, "[RDMA-REPL] session table full\n");
    pthread_mutex_unlock(&g_sessions.lock);
}

void session_kill_all(void) {
    pthread_mutex_lock(&g_sessions.lock);
    for (int i = 0; i < g_sessions.count; i++) {
        if (g_sessions.pids[i] > 0) {
            kill(g_sessions.pids[i], SIGTERM);
            usleep(50000);
            if (kill(g_sessions.pids[i], 0) == 0) {
                kill(g_sessions.pids[i], SIGKILL);
                usleep(50000);
            }
        }
    }
    g_sessions.count = 0;
    pthread_mutex_unlock(&g_sessions.lock);
}

static void session_init(void) {
    struct sigaction sa = {0};
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
}

/* ── Master ──────────────────────────────────────────────────────── */

int rdma_repl_master_start(void) {
    session_init();
    return 0;
}

/* 收到 Slave 的 FULLSYNC 命令后 fork 子进程监听推送 */
static int is_tcp(void) { return strcmp(kvs_config.fullsync_transport, "sendfile") == 0; }

int rdma_repl_master_trigger_sync(void) {
    session_kill_all();

    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return -1; }

    pid_t pid = fork();
    if (pid < 0) { perror("fork"); close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid == 0) {
        close(pipefd[0]);
        if (is_tcp())
            _exit(tcp_master_fullsync_child(repl_backlog_offset(), pipefd[1]));
        else
            master_fullsync_child(repl_backlog_offset(), pipefd[1]);
        _exit(0);
    }
    close(pipefd[1]);
    char ready;
    if (read(pipefd[0], &ready, 1) < 0) perror("read pipe");
    close(pipefd[0]);

    session_add(pid);
    return 0;
}

/* ── Slave ───────────────────────────────────────────────────────── */

/* 通过 TCP 向 Master Redis 端口发送 FULLSYNC 命令，等 +OK 响应 */
static int slave_send_fullsync_cmd(void) {
    const char *ip   = kvs_config.rdma_master_ip;
    int         port = kvs_config.master_port;        /* Master TCP service port */
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    struct addrinfo hints = {0}, *ai;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(ip, port_str, &hints, &ai) != 0) {
        perror("[slave] getaddrinfo"); return -1;
    }

    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) { perror("[slave] socket"); freeaddrinfo(ai); return -1; }

    if (connect(fd, ai->ai_addr, ai->ai_addrlen) < 0) {
        perror("[slave] connect"); close(fd); freeaddrinfo(ai); return -1;
    }
    freeaddrinfo(ai);

    const char *cmd = "FULLSYNC\r\n";
    if (send(fd, cmd, strlen(cmd), 0) < 0) {
        perror("[slave] send FULLSYNC"); close(fd); return -1;
    }

    char resp[16] = {0};
    ssize_t n = recv(fd, resp, sizeof(resp) - 1, 0);
    close(fd);
    if (n < 0) { perror("[slave] recv FULLSYNC response"); return -1; }

    if (strncmp(resp, "+OK", 3) != 0) {
        fprintf(stderr, "[slave] FULLSYNC rejected: %.*s\n", (int)n, resp);
        return -1;
    }
    printf("[slave] FULLSYNC accepted by master\n");
    return 0;
}

int rdma_repl_slave_start(void) {
    session_init();

    /* Run fullsync inline — fork + COW would lose the loaded data in the
     * parent process since g_dict modifications don't cross fork boundary.
     * TODO: restore async fork with shared memory or post-fork reload. */
    if (slave_send_fullsync_cmd() < 0)
        return -1;
    if (is_tcp())
        return tcp_slave_fullsync();
    else
        return rdma_repl_slave_fullsync();
}
