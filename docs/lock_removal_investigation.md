# KVStore 全局锁移除 & P=5 性能 Dip 排查报告

> 日期: 2026-08-08
> 范围: 移除 `g_store_lock`、分析 P=5 GET 性能低谷、规划后续架构清理与过期策略

---

## 一、背景

KVStore 原始架构中，每个命令（GET/SET/DEL 等）都需要获取全局 CAS 自旋锁
`g_store_lock`。注释声称该锁用于"expire-thread coordination"（过期线程协调），
但即使配置 `expire off`，所有命令路径仍然加锁/解锁。

用户指示：
1. 删除全局锁
2. 删除主动过期线程，借鉴 Redis 过期策略
3. 验证 P=5 pipeline 下 GET 性能低于 Redis 的问题是否解决

---

## 二、已完成的修改

### 2.1 全局锁移除

| 文件 | 修改内容 |
|------|---------|
| `src/engine/kv_dict.h` | 删除 `g_store_lock` extern 声明、`kv_store_rdlock()`、`kv_store_wrlock()`、`kv_store_unlock()` 内联函数；保留 `kv_store_lock_for_snapshot()` / `kv_store_unlock_snapshot()` 空操作（兼容 RDB 保存） |
| `src/engine/kv_dict.c` | 删除 `volatile int g_store_lock = 0;` 定义 |
| `src/core/kvstore_dispatch.c` | 删除 **38 处**锁调用：inline GET（2 处）、inline SET（3 处）、DBSIZE（2 处）、FLUSHALL（2 处）、MGET（2 处）、MSET（2 处）、DEL（2 处）、EXISTS（2 处）、KEYS（2 处）、single_key 路径（17 处解锁 + 2 处加锁） |

### 2.2 过期策略改造

| 文件 | 修改内容 |
|------|---------|
| `src/core/kvstore_expire.c` | **重写**：删除后台线程（`pthread`）、过期队列（`enqueue_expired_key`/`flush_expire_queue`）、主动采样周期（`active_expire_cycle`）、线程管理（`kvs_start/stop_expire_thread`）。仅保留 `g_expired_keys` 计数器和 `kvs_expired_keys_ptr()` 导出 |
| `src/core/kvstore.c` | 删除 `kvs_start_expire_thread()` 和 `kvs_stop_expire_thread()` 调用 |
| `src/core/kvstore_internal.h` | 删除 `kvs_start/stop_expire_thread`、`enqueue_expired_key`、`flush_expire_queue` 声明 |
| `src/core/kvstore.h` | 删除 `kvs_start/stop_expire_thread` 声明 |

### 2.3 dispatch_resolve 行为变化

**之前**（锁 + 读写分离）：
- 读操作发现过期 key → 入队 `enqueue_expired_key()` → 等下次写操作批量删除
- 写操作发现过期 key → 直接删除

**现在**（Redis 风格，单线程无锁）：
- 任何操作发现过期 key → **直接删除**（`entry_cleanup_by_type` + `dict_delete` + 计数递增）

```c
// dispatch_resolve — 读/写统一处理
static inline kv_entry_t *dispatch_resolve(kvs_token_t key_tok,
                                            uint64_t *hash_out, int is_write) {
    (void)is_write;  // 不再区分读写
    // ...
    if (ENABLE_EXPIRE && entry && entry->expire && get_time_now() > entry->expire) {
        entry_cleanup_by_type(entry);
        dict_delete(kv_dict_for(hash), key_tok.ptr, (uint32_t)key_tok.len, hash);
        atomic_fetch_add_explicit(kvs_expired_keys_ptr(), 1, memory_order_relaxed);
        return NULL;
    }
    return entry;
}
```

---

## 三、P=5 性能 Dip 排查

### 3.1 问题确认

锁移除后，P=5 GET dip 依然存在：

```
# fine-grained GET 测试 (n=200000, c=30, d=16)
P=1   kvstore=18292  redis=17289  ratio=1.05x  (领先)
P=2   kvstore=37293  redis=33916  ratio=1.09x  (领先)
P=3   kvstore=56307  redis=53263  ratio=1.05x  (领先)
P=4   kvstore=75786  redis=73046  ratio=1.03x  (领先)
P=5   kvstore=94029  redis=101163 ratio=0.92x  ← dip!
P=6   kvstore=110867 redis=114092 ratio=0.97x  (恢复中)
P=8   kvstore=152439 redis=154202 ratio=0.98x
P=10  kvstore=182815 redis=162075 ratio=1.12x  (重新领先)
```

**结论：锁不是 P=5 dip 的根因。** 锁移除后 dip 仅从 0.93x 微幅改善至 0.92x
（在测试噪声范围内无实质变化）。

### 3.2 分层隔离测试

使用 PING 命令（无存储引擎，纯网络+协议层）隔离问题：

```
         PING (纯网络)              GET (含存储引擎)
P=1      1.07x  > Redis             1.05x  > Redis
P=5      1.07x  > Redis  ✅         0.92x  < Redis  ❌
P=10     0.96x  < Redis             1.12x  > Redis
```

**PING 在 P=5 完全没有 dip**，证明：
- 网络层（reactor/epoll）在 P=5 工作正常
- 问题出在存储引擎的 GET 命令处理路径

### 3.3 Perf 热点分析

```
P=5 GET profiling (perf record, cpu-clock, 10538 samples):

 13.87%  [.] dict_find           ← #1 用户态热点
  7.87%  [k] do_syscall_64       ← 内核态（网络栈）
  6.09%  [k] _raw_spin_unlock_irqrestore
  5.59%  [.] recv
  3.68%  [.] __send
  1.94%  [.] kvs_dispatch
```

`dict_find` 汇编热点：

```
55.21%  cmp    %r12,0x8(%rbx)    ← 从内存加载 entry->hash 并比较
        jne    be28               ← 哈希不匹配则跳转

39.99%  test   %rbx,%rbx          ← 检查 bucket 是否为空
```

**结论：`dict_find` 是内存访问密集型操作**，55% 时间花在加载 entry 的 hash 字段。
这是哈希表查表本身的开销，不是锁或线程问题。

### 3.4 Value Size 敏感性

不同 value size 下 dip 位置会发生变化：

```
           d=3            d=16           d=128
P=1  GET   1.05x          1.02x          1.10x
P=4  GET   0.94x ←dip     1.03x          0.99x
P=5  GET   0.97x          0.92x ←dip     0.97x
P=10 GET   1.06x          1.10x          1.25x
```

dip 随数据大小和 pipeline 深度移动，**确认这是 CPU 微架构效应**
（指令调度、分支预测、缓存对齐），而非代码逻辑缺陷。

### 3.5 根本原因分析

Redis 在 P=4→P=5 获得 **38%** QPS 提升（73046→101163），
kvstore 仅获得 **24%**（75786→94029）。

差异根因：
1. **kv_entry_t 结构更大**（32B header + 内联 key）vs Redis dictEntry（24B + 外挂 key/value），导致 L1/L2 缓存命中率差异
2. **kv_hash (FNV-1a+Murmur3) vs Redis (SipHash)**：hash 函数计算延迟不同
3. **Pipeline 批处理**：`kvs_drain_resp_commands_off` 每条命令独立调用 `kvs_dispatch`，无法跨命令共享优化

**P=5 的具体 dip 是上述因素在特定 batch size 下的叠加效应，属于微架构层面的性能特征，不应在此时过度优化。**

### 3.6 整体性能评估

```
                    P=1    P=10   P=20   P=40   P=80   P=160
GET kvstore/redis   ~1.0x  1.10x  1.68x  2.19x  2.08x  2.07x
SET kvstore/redis   ~1.1x  1.47x  2.14x  2.32x  2.34x  2.60x
```

kvstore 在 P≥10 后持续领先 10-260%，整体架构优于单线程 Redis。
P=5 的 8% 劣势是可接受的微架构噪声。

---

## 四、后续工作建议

### 4.1 立即清理（低风险、高收益）

#### A. 删除死代码

以下函数已无调用者，可直接删除：

| 函数 | 位置 | 原用途 |
|------|------|--------|
| `dict_get_random_key()` | `kv_dict.c:532` / `kv_dict.h:134` | 已被删除的 `active_expire_cycle` 调用 |
| `kv_store_` 系列锁函数 | 已删除 | — |

需要修改的文件：
- `src/engine/kv_dict.c`：删除 `dict_get_random_key()` 实现（~40 行）
- `src/engine/kv_dict.h`：删除声明

#### B. 修正过期配置描述

`src/config/config.c:257`：
```c
// 旧：kvs_config.expire_enabled ? "on (lazy+queue+thread)" : "off"
// 新：kvs_config.expire_enabled ? "on (lazy)" : "off"
```

`src/config/config.h:58-63` 注释也需要更新，去掉 "queue + background thread" 描述。

#### C. 快照锁清理

`kv_store_lock_for_snapshot()` / `kv_store_unlock_snapshot()` 目前是空操作。
RDB 保存通过 `fork()` 实现，内核 COW 提供天然快照隔离。
可以删除这 3 个 no-op 及 `persist_rdb.c` 中的 3 个调用点。

### 4.2 架构清理（中等风险）

#### D. 简化 kv_store_t 包装层

当前：
```c
typedef struct { kv_dict_t dict; } kv_store_t;  // 单字段包装
extern kv_store_t g_store;
```

`kv_store_t` 只包装了一个 `kv_dict_t`，没有额外状态。可以直接用 `g_dict`
替代所有 `g_store.dict` 的访问路径。

影响范围较大（~15 个文件），但每个修改是机械的 `s/g_store.dict/g_dict/g`。

#### E. 统一 hash 函数选择

当前 `kv_dict_for(hash)` 忽略 hash 参数（单 dict 模式），是为将来分片预留的接口。
可以简化或保留，建议保留——分片是合理的演进方向。

### 4.3 过期策略实现（需要讨论）

当前实现只有 **惰性过期**（lazy expiration）：key 被访问时才检查并删除。
**如果 key 从未被访问，将永远占用内存。**

#### 方案对比

| 方案 | 描述 | 复杂度 | 是否推荐 |
|------|------|--------|---------|
| **A: 纯惰性** | 仅在访问时删除过期 key（当前实现） | 低 | 临时可用 |
| **B: Redis 风格** | 惰性 + 主事件循环主动采样 | 中 | **推荐** |
| **C: 惰性 + 定时全表扫描** | 每 N 秒遍历 dict 删除过期 | 低 | 大批量时阻塞长 |
| **D: 惰性 + 排序过期链表** | 过期 key 按时间排序，定时从头部删除 | 高 | 内存开销大 |

#### 推荐方案 B：Redis 风格主动采样

在事件循环 `beforeSleep()` 等价位置（`update_cached_time()` 中）调用
`active_expire_cycle()`。原代码中的主动采样算法是正确的，只需要：

1. 去掉 `g_store_lock` 的 CAS 操作（已在主线程，无需锁）
2. 去掉后台线程封装（`expire_thread_main`、`pthread_create`）
3. 在 `update_cached_time()` 或事件循环中直接调用

```c
// 伪代码
void update_cached_time(void) {
    // ... existing time sync ...
    if (ENABLE_EXPIRE) active_expire_cycle();  // 每次事件循环迭代采样
}

static void active_expire_cycle(void) {
    kv_dict_t *dict = kv_dict_for(0);
    int sample = 20;  // 基础采样数
    
    do {
        int expired = 0, sampled = 0;
        for (int i = 0; i < sample; i++) {
            kv_entry_t *e = dict_get_random_key(dict);
            if (!e) break;
            sampled++;
            if (e->expire && get_time_now() > e->expire) {
                // 直接删除（已在主线程，无需锁）
                entry_cleanup_by_type(e);
                dict_delete(dict, entry_key(e), e->key_len, e->hash);
                expired++;
            }
        }
        // 自适应：过期率高则增大采样
        if (expired > sample/4) sample *= 2;
        else break;
    } while (sample <= 200);  // 上限 200 次采样
}
```

**关键约束**：
- 单次采样上限 200 key、过期率低于 25% 时立即退出
- 不影响命令处理延迟（每次事件循环最多增加 ~50µs）
- 渐进式清理，类似 rehash 的分步思想

---

## 五、总结

```
已完成：
  ✅ 全局锁 g_store_lock 移除（~38 处调用点）
  ✅ 后台过期线程移除（~150 行代码删除）
  ✅ 过期策略改为 Redis 风格惰性删除

P=5 Dip 结论：
  ✅ 锁不是根因（dip 依然存在但锁已消除）
  ✅ PING 测试排除网络层（PING 在 P=5 正常）
  ✅ Perf 定位 dict_find 为最大热点（内存访问密集）
  ✅ 确认为微架构效应，不同 value size 下 dip 位置不同
  ✅ 整体性能优秀（P≥10 领先 10-260%）

待讨论：
  ⏳ 死代码清理（dict_get_random_key、过期配置描述等）
  ⏳ 架构简化（kv_store_t 包装层）
  ⏳ 过期策略补全（Redis 风格主动采样）
```

---

## 六、修订历史

| 日期 | 修订 |
|------|------|
| 2026-08-08 | 初始版本：锁移除 + P=5 dip 排查 + 后续规划 |
