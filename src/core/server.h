#ifndef __SERVER_H__
#define __SERVER_H__

#include <stdint.h>
#include <signal.h>
#include "dynbuf.h"
#include "net_common.h"    /* conn_kind_t, buffer constants */

/* ── Global graceful-shutdown flag ── */
extern volatile sig_atomic_t g_kvs_stop;

/* ── Callback types ── */
typedef int (*RCALLBACK)(int fd);
typedef int (*msg_handler)(char *msg, int len, DynBuf *write_buf);

#endif
