#include "vector_store.h"
#include "vec_engine.h"

void vector_store_init(void) { vec_engine_init(); }
void vector_store_clear(void) { vec_engine_clear(); }
void vector_store_destroy(void) { vec_engine_shutdown(); }
uint64_t vector_store_count(void) { return vec_engine_total_count(); }
