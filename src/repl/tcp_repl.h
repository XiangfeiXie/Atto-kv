#ifndef __TCP_REPL_H__
#define __TCP_REPL_H__

#include <stdint.h>

/* TCP sendfile 全量同步入口（fork 子进程调用） */

int tcp_master_fullsync_child(uint64_t base_seq, int ready_fd);
int tcp_slave_fullsync(void);

#endif
