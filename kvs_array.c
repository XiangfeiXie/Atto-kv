

#include "kvstore.h"


// singleton

kvs_array_t global_array = {0};

int kvs_array_create(kvs_array_t *inst) {

	if (!inst) return -1;
	if (inst->table) {
		printf("table has alloc\n");
		return -1;
	}	
	inst->table = kvs_malloc(KVS_ARRAY_SIZE * sizeof(kvs_array_item_t));
	if (!inst->table) {
		return -1;
	}

	memset(inst->table, 0, KVS_ARRAY_SIZE * sizeof(kvs_array_item_t));
	inst->total = 0;

	return 0;
}

void kvs_array_destory(kvs_array_t *inst) {

	if (!inst) return ;

	if (inst->table) {
		for(int i=0;i< inst -> total ;i++){
			if(inst -> table[i].key){
				kvs_free(inst -> table[i].key);
				kvs_free(inst -> table[i].item->value);
				kvs_free(inst -> table[i].item);
			}
		}
		kvs_free(inst->table);
	}

}



int kvs_array_set(kvs_array_t *inst, const char *key,const KVItem*item) {

	if (!inst || !key || !item) return -1;
	if (inst->total == KVS_ARRAY_SIZE) return -1;

	//检查是否存在
	for(int i=0;i< inst -> total;i++){
		if(inst -> table[i].key && strcmp( inst -> table[i].key,key)==0)
		return 1;
	}

	//复制key
	char *kcopy = kvs_malloc(strlen(key)+1);
	strcpy(kcopy,key);

	//复制value
	char* vcopy = kvs_malloc(item -> len +1);
	memcpy(vcopy,item -> value ,item->len);
	vcopy[item -> len ] = 0;

	int pos =-1 ;
	for (int i = 0;i < KVS_ARRAY_SIZE; i++){
		if(inst -> table[i].key == NULL){
			pos = i;
			break;
		}
	}
	if(pos == -1){
		return -1;
	}

	inst -> table[pos].item = (KVItem*)kvs_malloc(sizeof(KVItem));
	inst -> table[pos].key = kcopy;
    inst->table[pos].item->value = vcopy;
    inst->table[pos].item->len = item->len;
	inst->table[pos].item->expire = item->expire;

    

	inst -> total++;
	return 0;
}

KVItem* kvs_array_get(kvs_array_t *inst,const  char *key) {

	if(!inst || ! key){
		return NULL;
	}

	long long now = get_time_now();

	for(int i=0;i < KVS_ARRAY_SIZE; i++){
		kvs_array_item_t *it = &inst -> table[i];
		if(!it -> key )continue;

		if(strcmp(it -> key , key)==0){
			//惰性删除
			if(it->item->expire != 0&& now > it->item->expire){
				kvs_free(it -> key);
				kvs_free(it -> item -> value);
				kvs_free(it -> item);
				it -> key =NULL;
				inst ->total --;
				return NULL;
			}
			return it->item;
		}
	}
	return NULL;
}



int kvs_array_del(kvs_array_t *inst,const  char *key) {
    if (!inst || !key) return -1;

    for (int i = 0; i < KVS_ARRAY_SIZE; i++) {
        kvs_array_item_t *it = &inst->table[i];
        if (!it->key) continue;

        if (strcmp(it->key, key) == 0) {
            free(it->key);
            free(it->item -> value);
            it->key = NULL;
            inst->total--;
            return 0;
        }
    }
    return 1;

}



int kvs_array_mod(kvs_array_t *inst, const char *key, const KVItem*item) {

    if (!inst || !key || !item) return -1;

    KVItem* old = kvs_array_get(inst, key);
    if (!old) return 1;

    // 释放旧值
    kvs_free(old->value);

    // 复制新值
    char* vcopy = malloc(item->len + 1);
    memcpy(vcopy, item->value, item->len);
    vcopy[item->len] = 0;

    old->value = vcopy;
    old->len = item->len;

    // 只有新item带expire才更新
    if (item->expire != 0) {
        old->expire = item->expire;
    }

    return 0;
}



int kvs_array_exist(kvs_array_t *inst,const  char *key) {
	return kvs_array_get(inst, key) ? 0 : 1;
}


