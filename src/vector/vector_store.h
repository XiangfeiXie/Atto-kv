#ifndef KVS_VECTOR_STORE_H
#define KVS_VECTOR_STORE_H

#include <stdint.h>

void vector_store_init(void);
void vector_store_destroy(void);
void vector_store_clear(void);
uint64_t vector_store_count(void);

#endif
