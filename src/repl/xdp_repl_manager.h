#ifndef KVS_XDP_REPL_MANAGER_H
#define KVS_XDP_REPL_MANAGER_H

#include "repl_frame.h"

#include <stdint.h>

int  xdp_repl_manager_init(const char *ifname, int queue_id, int udp_port,
                           int zero_copy_required);
int  xdp_repl_send_fragment(const kvs_repl_frame_hdr_t *hdr,
                            const char *payload, uint32_t len);
int  xdp_repl_poll(void);
void xdp_repl_manager_stop(void);

#endif
