#ifndef __RDMA_REPL_H__
#define __RDMA_REPL_H__

#include <stdint.h>
#include <sys/types.h>

/* ===================================================================
 * RDMA replication - Simplified architecture
 *
 * Architecture:
 *   - TCP (6483): All control (sync request, heartbeat, coordination)
 *   - RDMA (2101): Data only (fullsync streaming)
 *   - Master: rdma_repl_master_start() only manages child sessions
 *   - Slave: rdma_repl_slave_run() called by repl_tcp when fullsync needed
 * =================================================================== */

/* Master: Initialize session management (SIGCHLD handler) */
int rdma_repl_master_start(void);

/* Slave: Perform full sync via RDMA (called by repl_tcp.c) */
int rdma_repl_slave_run(void);

/* Session management: Add child process PID for tracking */
void session_add(pid_t pid);

/* Kill all tracked child processes (clean up before re-fork). */
void session_kill_all(void);

/* Master child process: Pure RDMA data streaming (called after fork).
 * ready_fd >= 0: write a byte after fi_listen() so parent knows we're ready. */
int master_fullsync_child(uint64_t base_seq, int ready_fd);

/* Slave: Direct RDMA fullsync connection (called by repl_tcp.c) */
int rdma_repl_slave_fullsync(const char *master_ip, uint16_t rdma_port);

#endif /* __RDMA_REPL_H__ */
