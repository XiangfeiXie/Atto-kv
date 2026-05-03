#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "kvstore.h"

kvs_hash_t global_hash;

static int _hash(const char *key, int size) {
	if (!key) return -1;
	int sum = 0;
	for (int i = 0; key[i]; i++) sum += key[i];
	return sum % size;
}

// 安全创建节点
static hashnode_t *_create_node(const char *key, const KVItem *item) {
	hashnode_t *node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
	if (!node) return NULL;

	node->key = kvs_malloc(strlen(key) + 1);
	strcpy(node->key, key);

	KVItem *it = (KVItem*)kvs_malloc(sizeof(KVItem));
	it->value = kvs_malloc(item->len + 1);
	memcpy(it->value, item->value, item->len);
	it->value[item->len] = 0;
	it->len = item->len;
	it->expire = item->expire;

	node->value = it;
	node->next = NULL;
	return node;
}

int kvs_hash_create(kvs_hash_t *hash) {
	if (!hash) return -1;
	hash->nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * MAX_TABLE_SIZE);
	hash->max_slots = MAX_TABLE_SIZE;
	hash->count = 0;
	return 0;
}

// 安全销毁（无野指针）
void kvs_hash_destory(kvs_hash_t *hash) {
	if (!hash) return;
	for (int i = 0; i < hash->max_slots; i++) {
		hashnode_t *node = hash->nodes[i];
		while (node) {
			hashnode_t *tmp = node;
			node = node->next;

			kvs_free(tmp->key);
			kvs_free(tmp->value->value);
			kvs_free(tmp->value);
			kvs_free(tmp);
		}
	}
	kvs_free(hash->nodes);
}

int kvs_hash_set(kvs_hash_t *hash, const char *key, const KVItem *item) {
	if (!hash || !key || !item) return -1;
	int idx = _hash(key, hash->max_slots);

	hashnode_t *node = hash->nodes[idx];
	while (node) {
		if (strcmp(node->key, key) == 0) return 1;
		node = node->next;
	}

	hashnode_t *new_node = _create_node(key, item);
	new_node->next = hash->nodes[idx];
	hash->nodes[idx] = new_node;
	hash->count++;
	return 0;
}

// GET + 惰性删除（安全）
KVItem* kvs_hash_get(kvs_hash_t *hash, const char *key) {
	if (!hash || !key) return NULL;
	int idx = _hash(key, hash->max_slots);

	hashnode_t *node = hash->nodes[idx];
	while (node) {
		if (strcmp(node->key, key) == 0) {
			KVItem *it = node->value;
			long long now = get_time_now();
			if (it->expire != 0 && now > it->expire) {
				kvs_hash_del(hash, key);
				return NULL;
			}
			return it;
		}
		node = node->next;
	}
	return NULL;
}

int kvs_hash_mod(kvs_hash_t *hash, const char *key, const KVItem *item) {
	if (!hash || !key || !item) return -1;
	KVItem *old = kvs_hash_get(hash, key);
	if (!old) return 1;

	kvs_free(old->value);
	old->value = kvs_malloc(item->len + 1);
	memcpy(old->value, item->value, item->len);
	old->value[item->len] = 0;
	old->len = item->len;
	if (item->expire != 0) old->expire = item->expire;
	return 0;
}

int kvs_hash_del(kvs_hash_t *hash, const char *key) {
	if (!hash || !key) return -1;
	int idx = _hash(key, hash->max_slots);
	hashnode_t *head = hash->nodes[idx];
	if (!head) return 1;

	if (strcmp(head->key, key) == 0) {
		hash->nodes[idx] = head->next;
		kvs_free(head->key);
		kvs_free(head->value->value);
		kvs_free(head->value);
		kvs_free(head);
		hash->count--;
		return 0;
	}

	hashnode_t *cur = head;
	while (cur->next) {
		if (strcmp(cur->next->key, key) == 0) break;
		cur = cur->next;
	}
	if (!cur->next) return 1;

	hashnode_t *tmp = cur->next;
	cur->next = tmp->next;
	kvs_free(tmp->key);
	kvs_free(tmp->value->value);
	kvs_free(tmp->value);
	kvs_free(tmp);
	hash->count--;
	return 0;
}

int kvs_hash_exist(kvs_hash_t *hash, const char *key) {
	return kvs_hash_get(hash, key) ? 0 : 1;
}

// RDB 遍历
void kvs_hash_scan(kvs_hash_t *db, kv_scan_cb cb, void *priv) {
	if (!db || !cb) return;
	for (int i = 0; i < db->max_slots; i++) {
		hashnode_t *node = db->nodes[i];
		while (node) {
			cb(node->key, node->value, priv);
			node = node->next;
		}
	}
}

// 清空（RDB 加载用）
void kvs_hash_clear(kvs_hash_t *db) {
	if (!db) return;
	kvs_hash_destory(db);
	kvs_hash_create(db);
}