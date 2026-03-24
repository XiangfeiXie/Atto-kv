#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "kvstore.h"

// #define MAX_LEVEL 6

// typedef struct Node {
// 	int key;
// 	char value[128];
// 	struct Node **forward;
// } Node;

// typedef struct SkipList {
// 	int level;
// 	Node *header;
// } SkipList;

// struct kvs_skiptable {
// 	SkipList *skl;
// };
typedef struct kvs_skiptable kvs_skiptable_t;

kvs_skiptable_t global_skiptable;

static Node *createNode(int level, int key, char *value) {
	Node *newNode = (Node *)kvs_malloc(sizeof(Node));
	newNode->key = key;
	strcpy(newNode->value, value);
	newNode->forward = (Node **)kvs_malloc((level + 1) * sizeof(Node *));

	for (int i = 0; i <= level; i++) {
		newNode->forward[i] = NULL;
	}
	return newNode;
}

int kvs_skiptable_create(kvs_skiptable_t *skiptable) {
	skiptable->skl = (SkipList *)kvs_malloc(sizeof(SkipList));
	skiptable->skl->level = 0;
	skiptable->skl->header = createNode(MAX_LEVEL, -1, "");

	for (int i = 0; i <= MAX_LEVEL; ++i) {
		skiptable->skl->header->forward[i] = NULL;
	}
	return 0;
}

static int randomLevel(void) {
	int level = 0;
	while (rand() < RAND_MAX / 2 && level < MAX_LEVEL)
		level++;
	return level;
}

int kvs_skiptable_set(kvs_skiptable_t *skiptable,char *key_str, char *value) {
	int key = atoi(key_str);
	SkipList *skipList = skiptable->skl;

	Node *update[MAX_LEVEL + 1] = {0};
	Node *current = skipList->header;

	for (int i = skipList->level; i >= 0; --i) {
		while (current->forward[i] != NULL && current->forward[i]->key < key)
			current = current->forward[i];
		update[i] = current;
	}

	current = current->forward[0];
	if (current != NULL && current->key == key) {
		return 1;
	}

	int level = randomLevel();
	if (level > skipList->level) {
		for (int i = skipList->level + 1; i <= level; ++i)
			update[i] = skipList->header;
		skipList->level = level;
	}

	Node *newNode = createNode(level, key, value);
	for (int i = 0; i <= level; ++i) {
		newNode->forward[i] = update[i]->forward[i];
		update[i]->forward[i] = newNode;
	}
	return 0;
}

char *kvs_skiptable_get(kvs_skiptable_t *skiptable,char *key_str) {
	int key = atoi(key_str);
	SkipList *skipList = skiptable->skl;
	Node *current = skipList->header;

	for (int i = skipList->level; i >= 0; --i) {
		while (current->forward[i] != NULL && current->forward[i]->key < key)
			current = current->forward[i];
	}

	current = current->forward[0];
	if (current && current->key == key) {
		return current->value;
	}
	return NULL;
}

int kvs_skiptable_del(kvs_skiptable_t *skiptable,char *key_str) {
	int key = atoi(key_str);
	SkipList *skipList = skiptable->skl;
	Node *update[MAX_LEVEL + 1] = {0};
	Node *current = skipList->header;

	for (int i = skipList->level; i >= 0; --i) {
		while (current->forward[i] != NULL && current->forward[i]->key < key)
			current = current->forward[i];
		update[i] = current;
	}

	current = current->forward[0];
	if (current == NULL || current->key != key) {
		return 1;
	}

	for (int i = 0; i <= skipList->level; ++i) {
		if (update[i]->forward[i] != current) break;
		update[i]->forward[i] = current->forward[i];
	}

	kvs_free(current->forward);
	kvs_free(current);

	while (skipList->level > 0 && skipList->header->forward[skipList->level] == NULL) {
		skipList->level--;
	}
	return 0;
}

int kvs_skiptable_mod(kvs_skiptable_t *skiptable, char *key_str, char *value) {
	char *old = kvs_skiptable_get(skiptable, key_str);
	if (!old) return 1;
	strcpy(old, value);
	return 0;
}

int kvs_skiptable_exist(kvs_skiptable_t *skiptable,char *key_str) {
	char *val = kvs_skiptable_get(skiptable, key_str);
	return val ? 0 : 1;
}

void kvs_skiptable_destroy(kvs_skiptable_t *skiptable) {
	if (!skiptable || !skiptable->skl) return;
	SkipList *skl = skiptable->skl;
	Node *cur = skl->header->forward[0];

	while (cur) {
		Node *tmp = cur;
		cur = cur->forward[0];
		kvs_free(tmp->forward);
		kvs_free(tmp);
	}

	kvs_free(skl->header->forward);
	kvs_free(skl->header);
	kvs_free(skl);
}