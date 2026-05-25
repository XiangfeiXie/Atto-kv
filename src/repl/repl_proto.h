/* repl_proto.h — Replication protocol definitions.
 *
 * TCP stream framing:
 *   [data_len:4B (network byte order)][data:data_len]
 *
 * Master repl-agent:
 *   - attaches eBPF to observe kvstore traffic
 *   - reads data from BPF ring buffer
 *   - sends length-prefixed frames to slave via TCP
 *
 * Slave repl-agent:
 *   - receives frames from master via TCP
 *   - writes raw data to local kvstore
 *
 * Does NOT parse RESP protocol. Only guarantees byte-identical delivery.
 * TCP provides reliable ordered transport — no seq/ACK/retransmit needed.
 */
#ifndef REPL_PROTO_H
#define REPL_PROTO_H

#include <stdint.h>

#define REPL_PORT         6483   /* TCP port for master-slave replication */

#endif
