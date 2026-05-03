#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "kvstore.h"
#include "server.h"

kvs_skiptable_t global_skiptable;

// 安全创建节点：字符串key + KVItem
static Node *createNode(int level, const char *key, const KVItem *item) {
    Node *newNode = (Node *)kvs_malloc(sizeof(Node));
    if (!newNode) return NULL;

    // 1. 复制key（支持中文/任意字符）
    newNode->key = kvs_malloc(strlen(key) + 1);
    if (!newNode->key) {
        kvs_free(newNode);
        return NULL;
    }
    strcpy(newNode->key, key);

    // 2. 复制KVItem（安全内存分配）
    newNode->value = (KVItem*)kvs_malloc(sizeof(KVItem));
    if (!newNode->value) {
        kvs_free(newNode->key);
        kvs_free(newNode);
        return NULL;
    }
    newNode->value->value = kvs_malloc(item->len + 1);
    if (!newNode->value->value) {
        kvs_free(newNode->value);
        kvs_free(newNode->key);
        kvs_free(newNode);
        return NULL;
    }
    memcpy(newNode->value->value, item->value, item->len);
    newNode->value->value[item->len] = 0;
    newNode->value->len = item->len;
    newNode->value->expire = item->expire;

    // 3. 初始化forward
    newNode->forward = (Node **)kvs_malloc((level + 1) * sizeof(Node *));
    if (!newNode->forward) {
        kvs_free(newNode->value->value);
        kvs_free(newNode->value);
        kvs_free(newNode->key);
        kvs_free(newNode);
        return NULL;
    }
    memset(newNode->forward, 0, (level + 1) * sizeof(Node *));

    return newNode;
}

// 创建跳表：header节点用空字符串
int kvs_skiptable_create(kvs_skiptable_t *skiptable) {
    if (!skiptable) return -1;
    skiptable->skl = (SkipList *)kvs_malloc(sizeof(SkipList));
    if (!skiptable->skl) return -1;

    skiptable->skl->level = 0;
    skiptable->skl->total = 0;
    // header节点用空字符串，value用空KVItem
    KVItem empty_item = {0};
    skiptable->skl->header = createNode(MAX_LEVEL, "", &empty_item);
    if (!skiptable->skl->header) {
        kvs_free(skiptable->skl);
        return -1;
    }
    return 0;
}

// 保留你原来的随机level逻辑
static int randomLevel(void) {
    int level = 0;
    while ((rand() & 0x7FFF) < (0x5 * 0x7FFF) && level < MAX_LEVEL)
        level++;
    return level;
}

// SET：字符串key比较，彻底解决乱码
int kvs_skiptable_set(kvs_skiptable_t *skiptable, const char *key, const KVItem *item) {
    if (!skiptable || !key || !item) return -1;
    SkipList *skipList = skiptable->skl;

    Node *update[MAX_LEVEL + 1] = {0};
    Node *current = skipList->header;

    // 用strcmp比较字符串key，支持中文
    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
        update[i] = current;
    }

    current = current->forward[0];
    if (current != NULL && strcmp(current->key, key) == 0) {
        return 1; // 已存在，返回1
    }

    int level = randomLevel();
    if (level > skipList->level) {
        for (int i = skipList->level + 1; i <= level; ++i)
            update[i] = skipList->header;
        skipList->level = level;
    }

    Node *newNode = createNode(level, key, item);
    if (!newNode) return -2;

    for (int i = 0; i <= level; ++i) {
        newNode->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = newNode;
    }

    skipList->total++;
    return 0;
}

// GET：字符串key + 惰性删除
KVItem* kvs_skiptable_get(kvs_skiptable_t *skiptable, const char *key) {
    if (!skiptable || !key) return NULL;
    SkipList *skipList = skiptable->skl;
    Node *current = skipList->header;

    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
    }

    current = current->forward[0];
    if (current && strcmp(current->key, key) == 0) {
        KVItem *it = current->value;
        long long now = get_time_now();
        // 惰性删除过期key
        if (it->expire != 0 && now > it->expire) {
            kvs_skiptable_del(skiptable, key);
            return NULL;
        }
        return it;
    }
    return NULL;
}

// DEL：安全释放，彻底解决段错误
int kvs_skiptable_del(kvs_skiptable_t *skiptable, const char *key) {
    if (!skiptable || !key) return -1;
    SkipList *skipList = skiptable->skl;
    Node *update[MAX_LEVEL + 1] = {0};
    Node *current = skipList->header;

    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
        update[i] = current;
    }

    current = current->forward[0];
    if (current == NULL || strcmp(current->key, key) != 0) {
        return 1; // 不存在
    }

    // 调整指针
    for (int i = 0; i <= skipList->level; ++i) {
        if (update[i]->forward[i] != current) break;
        update[i]->forward[i] = current->forward[i];
    }

    // 安全释放所有内存（无野指针）
    kvs_free(current->key);
    kvs_free(current->value->value);
    kvs_free(current->value);
    kvs_free(current->forward);
    kvs_free(current);

    // 收缩跳表层级
    while (skipList->level > 0 && skipList->header->forward[skipList->level] == NULL) {
        skipList->level--;
    }

    skipList->total--;
    return 0;
}

// MOD：安全修改，无段错误
int kvs_skiptable_mod(kvs_skiptable_t *skiptable, const char *key, const KVItem *item) {
    if (!skiptable || !key || !item) return -1;
    KVItem *old = kvs_skiptable_get(skiptable, key);
    if (!old) return 1;

    // 释放旧值，分配新值
    kvs_free(old->value);
    old->value = kvs_malloc(item->len + 1);
    if (!old->value) return -2;
    memcpy(old->value, item->value, item->len);
    old->value[item->len] = 0;
    old->len = item->len;
    if (item->expire != 0)
        old->expire = item->expire;

    return 0;
}

// EXIST：统一接口
int kvs_skiptable_exist(kvs_skiptable_t *skiptable, const char *key) {
    return kvs_skiptable_get(skiptable, key) ? 0 : 1;
}

// 销毁：彻底安全，无重复释放
void kvs_skiptable_destroy(kvs_skiptable_t *skiptable) {
    if (!skiptable || !skiptable->skl) return;
    SkipList *skl = skiptable->skl;
    Node *cur = skl->header;

    while (cur) {
        Node *tmp = cur;
        cur = cur->forward[0];

        // 安全释放所有内存
        if (tmp->key) kvs_free(tmp->key);
        if (tmp->value) {
            if (tmp->value->value) kvs_free(tmp->value->value);
            kvs_free(tmp->value);
        }
        if (tmp->forward) kvs_free(tmp->forward);
        kvs_free(tmp);
    }

    kvs_free(skl);
    skiptable->skl = NULL;
}

// RDB遍历：和其他引擎统一
void kvs_skiptable_scan(kvs_skiptable_t *db, kv_scan_cb cb, void *priv) {
    if (!db || !cb || !db->skl) return;
    Node *cur = db->skl->header->forward[0];
    while (cur) {
        cb(cur->key, cur->value, priv);
        cur = cur->forward[0];
    }
}

// 清空：RDB加载用
void kvs_skiptable_clear(kvs_skiptable_t *db) {
    if (!db) return;
    kvs_skiptable_destroy(db);
    kvs_skiptable_create(db);
}