#ifndef __BLOCK_H__
#define __BLOCK_H__

#include <stdint.h>
#include "client.h"

/* ── Notify callback — network layer registers this to trigger send on unblock ── */
typedef void (*block_notify_cb)(client_t *c);
void block_set_notify_cb(block_notify_cb cb);

/* ── Initialize the blocking subsystem ── */
void block_init(void);

/* ── Register a client as blocked on a key ── */
void block_add(const char *key, uint32_t key_len, client_t *c);

/* ── Wake the first client waiting on a key (caller re-dispatches) ── */
client_t *block_wake_one(const char *key, uint32_t key_len);

/* ── Signal the network layer to send on a woken client's connection ── */
void block_notify_woken(client_t *c);

/* ── Remove a client from all keys it was waiting on ── */
void block_remove_client(client_t *c);

/* ── Return ms until nearest timeout, or -1 if none blocked ── */
int block_nearest_timeout(void);

/* ── Wake all clients whose timeout has expired ── */
void block_check_timeouts(void);

#endif
