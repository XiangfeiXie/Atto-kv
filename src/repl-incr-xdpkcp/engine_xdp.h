/* engine_xdp.h — engine-side integration for the AF_XDP+KCP gateway.
 *
 * Compiled into the kvstore binary.  The engine is the SHM creator; the
 * gateway attaches.  Which function is active depends on rdma_role:
 *
 *   master: xdp_engine_enqueue() is called from the write-command hook to
 *           hand each command to the gateway.
 *   slave : xdp_engine_poll() is called from the event loop to drain commands
 *           the gateway delivered and replay them via kvs_dispatch().
 */

#ifndef ENGINE_XDP_H
#define ENGINE_XDP_H

/* Master: push one write command (raw RESP bytes) to the gateway.  Called on
 * the write hot path; no-ops if the SHM region is absent. */
void xdp_engine_enqueue(const char *data, int len);

/* Master: flush the pending batch of write commands to the gateway.  Called
 * from the event loop so a partial batch drains promptly instead of waiting
 * for the buffer to fill.  no-ops if the batch is empty. */
void xdp_engine_flush(void);

/* Slave: drain gateway-delivered bytes and replay complete commands.  Called
 * from the event loop; no-ops unless the slave SHM region is active. */
void xdp_engine_poll(void);

int  xdp_engine_master_init(void);   /* create SHM region (0 = ok) */
int  xdp_engine_slave_init(void);    /* create SHM region + replay buffer */
int  xdp_engine_slave_active(void);  /* 1 if slave XDP path is live */
void xdp_engine_shutdown(void);      /* unlink SHM region */

#endif /* ENGINE_XDP_H */
