#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kvstore.h"

// 内部工具函数
static rbtree_node *rbtree_mini(rbtree *T, rbtree_node *x) {
	while (x->left != T->nil) x = x->left;
	return x;
}

static rbtree_node *rbtree_successor(rbtree *T, rbtree_node *x) {
	rbtree_node *y = x->parent;
	if (x->right != T->nil) return rbtree_mini(T, x->right);
	while ((y != T->nil) && (x == y->right)) {
		x = y;
		y = y->parent;
	}
	return y;
}

static void rbtree_left_rotate(rbtree *T, rbtree_node *x) {
	rbtree_node *y = x->right;
	x->right = y->left;
	if (y->left != T->nil) y->left->parent = x;
	y->parent = x->parent;
	if (x->parent == T->nil) T->root = y;
	else if (x == x->parent->left) x->parent->left = y;
	else x->parent->right = y;
	y->left = x;
	x->parent = y;
}

static void rbtree_right_rotate(rbtree *T, rbtree_node *y) {
	rbtree_node *x = y->left;
	y->left = x->right;
	if (x->right != T->nil) x->right->parent = y;
	x->parent = y->parent;
	if (y->parent == T->nil) T->root = x;
	else if (y == y->parent->right) y->parent->right = x;
	else y->parent->left = x;
	x->right = y;
	y->parent = x;
}

static void rbtree_insert_fixup(rbtree *T, rbtree_node *z) {
	while (z->parent->color == RED) {
		if (z->parent == z->parent->parent->left) {
			rbtree_node *y = z->parent->parent->right;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;
				z = z->parent->parent;
			} else {
				if (z == z->parent->right) {
					z = z->parent;
					rbtree_left_rotate(T, z);
				}
				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_right_rotate(T, z->parent->parent);
			}
		} else {
			rbtree_node *y = z->parent->parent->left;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;
				z = z->parent->parent;
			} else {
				if (z == z->parent->left) {
					z = z->parent;
					rbtree_right_rotate(T, z);
				}
				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_left_rotate(T, z->parent->parent);
			}
		}
	}
	T->root->color = BLACK;
}

static void rbtree_insert(rbtree *T, rbtree_node *z) {
	rbtree_node *y = T->nil;
	rbtree_node *x = T->root;
	while (x != T->nil) {
		y = x;
		if (strcmp(z->key, x->key) < 0) x = x->left;
		else if (strcmp(z->key, x->key) > 0) x = x->right;
		else return;
	}
	z->parent = y;
	if (y == T->nil) T->root = z;
	else if (strcmp(z->key, y->key) < 0) y->left = z;
	else y->right = z;
	z->left = T->nil;
	z->right = T->nil;
	z->color = RED;
	rbtree_insert_fixup(T, z);
}

static void rbtree_delete_fixup(rbtree *T, rbtree_node *x) {
	while ((x != T->root) && (x->color == BLACK)) {
		if (x == x->parent->left) {
			rbtree_node *w = x->parent->right;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;
				rbtree_left_rotate(T, x->parent);
				w = x->parent->right;
			}
			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {
				if (w->right->color == BLACK) {
					w->left->color = BLACK;
					w->color = RED;
					rbtree_right_rotate(T, w);
					w = x->parent->right;
				}
				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->right->color = BLACK;
				rbtree_left_rotate(T, x->parent);
				x = T->root;
			}
		} else {
			rbtree_node *w = x->parent->left;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;
				rbtree_right_rotate(T, x->parent);
				w = x->parent->left;
			}
			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {
				if (w->left->color == BLACK) {
					w->right->color = BLACK;
					w->color = RED;
					rbtree_left_rotate(T, w);
					w = x->parent->left;
				}
				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->left->color = BLACK;
				rbtree_right_rotate(T, x->parent);
				x = T->root;
			}
		}
	}
	x->color = BLACK;
}

static rbtree_node *rbtree_delete(rbtree *T, rbtree_node *z) {
	rbtree_node *y = T->nil;
	rbtree_node *x = T->nil;
	if ((z->left == T->nil) || (z->right == T->nil)) y = z;
	else y = rbtree_successor(T, z);
	if (y->left != T->nil) x = y->left;
	else x = y->right;
	x->parent = y->parent;
	if (y->parent == T->nil) T->root = x;
	else if (y == y->parent->left) y->parent->left = x;
	else y->parent->right = x;
	if (y != z) {
		char *tmpk = z->key;
		z->key = y->key;
		y->key = tmpk;
		void *tmpv = z->value;
		z->value = y->value;
		y->value = tmpv;
	}
	if (y->color == BLACK) rbtree_delete_fixup(T, x);
	return y;
}

static rbtree_node *rbtree_search(rbtree *T, KEY_TYPE key) {
	rbtree_node *node = T->root;
	while (node != T->nil) {
		if (strcmp(key, node->key) < 0) node = node->left;
		else if (strcmp(key, node->key) > 0) node = node->right;
		else return node;
	}
	return T->nil;
}

// ------------------------------
// 对外 KV 引擎接口（安全版）
// ------------------------------

kvs_rbtree_t global_rbtree = {0};

int kvs_rbtree_create(kvs_rbtree_t *inst) {
	if (!inst) return -1;
	inst->nil = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
	inst->nil->color = BLACK;
	inst->root = inst->nil;
	inst->total = 0;
	return 0;
}

// 递归安全释放
static void rbtree_free_nodes(rbtree *T, rbtree_node *node) {
	if (node == T->nil) return;
	rbtree_free_nodes(T, node->left);
	rbtree_free_nodes(T, node->right);
	kvs_free(node->key);
	kvs_free(((KVItem*)node->value)->value);
	kvs_free(node->value);
	kvs_free(node);
}

void kvs_rbtree_destory(kvs_rbtree_t *inst) {
	if (!inst) return;
	rbtree_free_nodes(inst, inst->root);
	kvs_free(inst->nil);
}

int kvs_rbtree_set(kvs_rbtree_t *inst, const char *key, const KVItem *item) {
	if (!inst || !key || !item) return -1;
	rbtree_node *n = rbtree_search(inst, (char*)key);
	if (n != inst->nil) return 1;

	rbtree_node *node = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
	node->key = kvs_malloc(strlen(key)+1);
	strcpy(node->key, key);

	KVItem *it = (KVItem*)kvs_malloc(sizeof(KVItem));
	it->value = kvs_malloc(item->len + 1);
	memcpy(it->value, item->value, item->len);
	it->value[item->len] = 0;
	it->len = item->len;
	it->expire = item->expire;
	node->value = it;

	rbtree_insert(inst, node);
	inst->total++;
	return 0;
}

KVItem* kvs_rbtree_get(kvs_rbtree_t *inst, const char *key) {
	if (!inst || !key) return NULL;
	rbtree_node *node = rbtree_search(inst, (char*)key);
	if (node == inst->nil) return NULL;

	KVItem *it = (KVItem*)node->value;
	long long now = get_time_now();
	if (it->expire != 0 && now > it->expire) {
		rbtree_node *cur = rbtree_delete(inst, node);
		kvs_free(cur->key);
		kvs_free(((KVItem*)cur->value)->value);
		kvs_free(cur->value);
		kvs_free(cur);
		inst->total--;
		return NULL;
	}
	return it;
}

int kvs_rbtree_del(kvs_rbtree_t *inst, const char *key) {
	if (!inst || !key) return -1;
	rbtree_node *node = rbtree_search(inst, (char*)key);
	if (node == inst->nil) return 1;

	rbtree_node *cur = rbtree_delete(inst, node);
	kvs_free(cur->key);
	kvs_free(((KVItem*)cur->value)->value);
	kvs_free(cur->value);
	kvs_free(cur);
	inst->total--;
	return 0;
}

int kvs_rbtree_mod(kvs_rbtree_t *inst, const char *key, const KVItem *item) {
	if (!inst || !key || !item) return -1;
	KVItem *old = kvs_rbtree_get(inst, key);
	if (!old) return 1;

	kvs_free(old->value);
	old->value = kvs_malloc(item->len + 1);
	memcpy(old->value, item->value, item->len);
	old->value[item->len] = 0;
	old->len = item->len;
	if (item->expire != 0) old->expire = item->expire;
	return 0;
}

int kvs_rbtree_exist(kvs_rbtree_t *inst, const char *key) {
	return kvs_rbtree_get(inst, key) ? 0 : 1;
}

// 中序遍历（给 RDB 用）
static void scan_inorder(rbtree *T, rbtree_node *n, kv_scan_cb cb, void *priv) {
	if (n == T->nil) return;
	scan_inorder(T, n->left, cb, priv);
	cb(n->key, (KVItem*)n->value, priv);
	scan_inorder(T, n->right, cb, priv);
}

void kvs_rbtree_scan(kvs_rbtree_t *db, kv_scan_cb cb, void *priv) {
	if (!db || !cb) return;
	scan_inorder(db, db->root, cb, priv);
}

void kvs_rbtree_clear(kvs_rbtree_t *inst) {
	if (!inst) return;
	rbtree_free_nodes(inst, inst->root);
	inst->root = inst->nil;
	inst->total = 0;
}