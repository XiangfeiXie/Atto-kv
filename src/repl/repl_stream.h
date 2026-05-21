#ifndef KVS_REPL_STREAM_H
#define KVS_REPL_STREAM_H

#include "journal.h"

#include <stdint.h>

int  repl_stream_start_master(void);
int  repl_stream_start_slave(uint64_t start_seq);
int  repl_stream_publish(const kvs_journal_record_t *rec);
void repl_stream_ack(uint64_t seq);
void repl_stream_request_fullsync(void);
void repl_stream_stop(void);
uint64_t repl_stream_last_ack(void);

#endif
