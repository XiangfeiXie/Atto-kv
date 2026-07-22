/* ===================================================================
 * rdma_repl.c — RDMA replication session management
 *
 * Architecture:
 *   - TCP (6483): Control plane (handled by repl_tcp.c)
 *   - RDMA (2101): Data plane (fullsync child process only)
 *   - This file only manages child process lifecycle (fork/wait)
 * =================================================================== */

#include "rdma_repl.h"
#include "kvstore.h"
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/wait.h>
#include <pthread.h>

/* ── Child process session management ─────────────────────────────── */

#define MAX_CHILD_SESSIONS  16

static struct {
    pid_t pids[MAX_CHILD_SESSIONS];
    int count;
    pthread_mutex_t lock;
} g_sessions = {.lock = PTHREAD_MUTEX_INITIALIZER};

static void sigchld_handler(int sig) {
    (void)sig;
    int status;
    pid_t pid;
    /* Reap all terminated children */
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_sessions.lock);
        for (int i = 0; i < g_sessions.count; i++) {
            if (g_sessions.pids[i] == pid) {
                /* Remove from array */
                g_sessions.pids[i] = g_sessions.pids[g_sessions.count - 1];
                g_sessions.count--;
                break;
            }
        }
        pthread_mutex_unlock(&g_sessions.lock);

        if (WIFEXITED(status)) {
            printf("[RDMA-REPL] Child %d exited with status %d\n",
                   pid, WEXITSTATUS(status));
        } else if (WIFSIGNALED(status)) {
            fprintf(stderr, "[RDMA-REPL] Child %d killed by signal %d\n",
                    pid, WTERMSIG(status));
        }
    }
}

void session_add(pid_t pid) {
    pthread_mutex_lock(&g_sessions.lock);
    if (g_sessions.count < MAX_CHILD_SESSIONS) {
        g_sessions.pids[g_sessions.count++] = pid;
        printf("[RDMA-REPL] Registered child session: pid=%d\n", pid);
    } else {
        fprintf(stderr, "[RDMA-REPL] Session table full, cannot add pid=%d\n", pid);
    }
    pthread_mutex_unlock(&g_sessions.lock);
}

/* Kill all tracked child processes before forking a new one.
 * Prevents fi_getinfo failure caused by a previous child still
 * holding the RDMA_CM listener on the fullsync port. */
void session_kill_all(void) {
    pthread_mutex_lock(&g_sessions.lock);
    for (int i = 0; i < g_sessions.count; i++) {
        pid_t pid = g_sessions.pids[i];
        if (pid > 0) {
            printf("[RDMA-REPL] Killing old child pid=%d\n", pid);
            kill(pid, SIGTERM);
            /* Give it a moment to exit and release the port */
            usleep(50000);
            /* Force kill if still alive */
            if (kill(pid, 0) == 0) {
                kill(pid, SIGKILL);
                usleep(50000);
            }
        }
    }
    g_sessions.count = 0;
    pthread_mutex_unlock(&g_sessions.lock);
}

static void session_init(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
}

/* ── Master: session management only ───────────────────────────── */

int rdma_repl_master_start(void) {
    printf("[RDMA-REPL] Initializing session management\n");

    /* Initialize child process session management */
    session_init();

    printf("[RDMA-REPL] Session management initialized\n");
    return 0;
}

/* ── Slave: entry point (called by repl_tcp.c) ────────────────── */

int rdma_repl_slave_run(void) {
    /* This function is now implemented in rdma_repl_slave.c */
    extern int rdma_repl_slave_fullsync(const char *master_ip, uint16_t rdma_port);
    extern KvsConfig kvs_config;

    return rdma_repl_slave_fullsync(kvs_config.rdma_master_ip, 2101);
}
