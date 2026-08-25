#ifndef NET_COMMON_H
#define NET_COMMON_H

/* ── Connection role — lets a network engine dispatch an fd to the right
 *   handler (client vs replication) without a hardcoded fd list. ── */
typedef enum {
    CONN_KIND_CLIENT = 0,     /* normal client connection */
    CONN_KIND_REPL_LISTEN,    /* master's incremental-replication listen socket */
    CONN_KIND_REPL_SLAVE,     /* master-side accepted slave link */
    CONN_KIND_REPL_MASTER,    /* slave-side outbound link to master */
} conn_kind_t;

/* ── Network buffer defaults ── */
#define KVS_NET_BUF_INIT  65536     /* 64 KB */
#ifndef KVS_NET_BUF_MAX
#define KVS_NET_BUF_MAX   (16 * 1024 * 1024)  /* 16 MB */
#endif

#endif /* NET_COMMON_H */
