#ifndef KVS_VECTOR_STORE_H
#define KVS_VECTOR_STORE_H

#include "protocol.h"
#include "dynbuf.h"
#include "repl_frame.h"

#include <stdint.h>

typedef struct {
    char *buf;
    uint32_t len;
} vector_mutation_buf_t;

void vector_store_init(void);
void vector_store_destroy(void);
void vector_store_clear(void);
uint64_t vector_store_count(void);

int vector_cmd_is_write(kvs_token_t cmd);
void vector_mutation_buf_free(vector_mutation_buf_t *m);
int vector_mutation_encode_from_cmd(kvs_token_t cmd,
                                    kvs_token_t *tokens,
                                    int argc,
                                    vector_mutation_buf_t *out);
int vector_mutation_apply(const char *buf, uint32_t len);

dispatch_result_t vector_cmd(kvs_token_t cmd,
                             kvs_token_t *tokens,
                             int argc,
                             DynBuf *wb);

#endif
