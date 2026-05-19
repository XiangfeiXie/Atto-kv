# Interview Talking Points — KVStore (Phase 10 lock-free fast-path)

简历主亮点完整准备稿。每段后面有"会被怎么问 / 怎么答 / 别说什么"。

---

## 一句话简历

> 在 Redis 兼容键值存储里实现了一个 lock-free seqlock 短键热路径缓存：
> 1024 buckets × 512 B = 512 KB（L2-resident）的直映射表，预格式化 RESP
> 回复 + per-bucket 64-bit 原子序列号；in-process 微基准下 GET 路径加速
> **1.9x**（375 ns → 200 ns），多读者同 shard 工况下吞吐**比 rwlock 路径
> 高 134%**（4 线程 6.74 vs 2.88 Mops/s）；TSAN 1 writer + 3 readers × 5 s
> 压测 0 torn read / 0 warning。

---

## 30 秒电梯版

> "我们的 dict 是 32 分片 + per-shard rwlock，GET 命中也得排队过 reader
> lock，在热点 key 倾斜场景 rwlock 的 cacheline ping-pong 是真瓶颈。
> 我加了一层 lock-free 直映射表挡在 dict 前面，bucket 里存预格式化好的
> RESP 回复，读端用 64-bit seqlock 包夹整次 memcpy 验 torn-read，写端
> 跟 dict shard wrlock 串行。单线程拿 1.9x，多读者同 shard 4 线程 +134%，
> TSAN 干净。中间还踩了 sizing 把表搞成 2 MB 溢 L2 触发 −27% 回归这种坑，
> 在 dev-log 里完整记录了。"

---

## 三段弯路（这是面试官最想听的）

### 弯路 1：Sizing 把 L2 命中变成 L3 来回
- **症状**：4096 buckets × 512 B = 2 MB 表上线后，redis-benchmark
  uniform-random GET 从 11825 QPS 跌到 8602 QPS，−27% 回归。
- **排查**：`perf stat` 看 LLC-load-misses 飙升。意识到 200 k key 工作集
  下，每个 GET 都要先 acquire-load 远端 bucket 的 seq + key（基本不命中
  也要查），2 MB 表稳定**溢出 L2**，等于给每个 GET 多付一次 L3 round-trip。
- **修法**：1024 × 512 B = 512 KB，**稳进 L2**。回归从 −27% 拉回 ±0.2%。
- **教训**：**lock-free 数据结构第一个设计参数是尺寸，不是算法**。
  1024 vs 4096 算法等价，但落在 L2 vs L3 上的性能天差地别。

### 弯路 2：微基准对照组"输出量"不对等
- **症状**：第一版微基准跑出 dict_find = 116 ns，fp_get = 200 ns，
  fp_get 反而**慢**。完全反直觉。
- **排查**：dict 路径只读了 `entry_payload(e)[0]` 1 个字节，fp_get 强制
  memcpy 整个 74 B 预格式化 reply。**比较的输出量不一样**。
- **修法**：让 dict 路径也产出完整 RESP（snprintf header + memcpy val
  + "\r\n"）。重测 dict 375 ns vs fp 200 ns，**1.9x**。
- **教训**：**对比基准最容易在"输出形式"上偷换概念**。fast-path 的卖点
  本来就是"把 SET 时一次格式化的成本摊销到所有 GET 上"，bench 必须
  把 dict 路径每次 GET 的格式化成本算进去。

### 弯路 3：用错了量尺
- **症状**：sizing 修对之后端到端 redis-benchmark 仍然只 ±0.2%，跟
  in-process 微基准的 1.9x 对不上。
- **排查**：算了一下，loopback + io_uring + RESP 解析单 op ≈ 50–70 µs，
  fast-path 节省的 200 ns 只占 0.3%。**网络开销把 CPU 优化彻底淹没**。
- **修法**：换工具。写 in-process 微基准 + 加 SAME_SHARD=1 多读者扩展性
  测试，让锁竞争暴露出来，数据立刻说话（4 线程 +134%）。
- **教训**：**lock-free / cache-hot 这类优化的真实价值是 CPU 周期 + 锁
  竞争扩展性 + 饱和延迟，不是 wall-clock QPS**。强行用 redis-benchmark
  量只会给自己挖坑。

---

## 可能被深问的 6 个问题

### Q1: 为什么用 seqlock，不用 RCU 或 hazard pointer？

- **数据 size**：bucket 是 512 B 固定大小，没有指针所有权问题，不需要
  RCU 的延迟回收
- **写频率**：写者已经有 dict shard wrlock 串行（fast-path put 是 dict
  set 的 post-hook），seqlock 写端 CAS 单写者免费就拿到了
- **读端 cost**：seqlock 读 = 2 次 acquire-load + 一次比较，0 atomic RMW；
  hazard pointer 至少要写一次 hp 槽 + memory fence
- **退路**：seqlock torn-read 检测后回退到 dict 路径，不影响正确性，
  比 RCU 的"必须 free 路径全部正确"风险低
- **代码量**：seqlock 整个 fastpath_cache.c < 200 行，RCU 要带 epoch 管理

### Q2: memory_order 怎么选的？

- 写端：`seq |= 1`（acquire-release，作为临界区开始）→ 写 payload →
  `seq += 2`（release-store，把所有写发布给 reader）
- 读端：load `seq` (acquire) → 读 payload → load `seq` (acquire) 校验
- **关键不变量**：reader 的第一次 load 的 acquire 跟 writer 的最后那次
  release 配对——它保证了"如果 reader 看到偶数 seq，那么 reader 后续读
  到的 payload 至少是这个 seq 对应的一致状态"
- 没用 `seq_cst` 是因为 fast-path 跨 bucket 之间没有顺序依赖，全 store
  buffer flush 是浪费

### Q3: ABA 问题怎么处理？

- seq 是 64-bit 单调递增，writer 每次 += 2，**理论上 2^63 次写后才会回卷**
- 即使 hot bucket 1 GHz 写也要 2920 年才一圈，工程上等于不会发生
- 真正的 ABA 防护其实在 **key 校验**：reader 读完 payload 后比较
  `key_hash + key_len + memcmp(key)`，命中的同 bucket 不同 key 会被
  这一步识破

### Q4: SET 之后立即 GET 的一致性？

- SET 持有 dict shard wrlock 全程；fp_put 在 wrlock 内部 commit 之后调用
- 同 worker 的下一个 GET 来到 fp_get 时，writer 的 release-store 已经发生，
  reader 看到偶数 seq 时 payload 一定是新版本（acquire/release 配对）
- 跨 worker 的 GET 因为也要先经过自己 worker 的 io_uring CQE epoch，
  之间隐含 fence，看见旧版本的窗口不影响 RESP 协议正确性（GET 是
  read-after-write，最差就是看见前一版，下一次 GET 一定看见新版）

### Q5: 为什么不让 fast-path 写端也 lock-free？

- 写端要做 hash 校验 + memcmp(key) + memcpy(reply) + 序列号管理，
  跟 dict 写者天然就要互斥（同一个 key 不能两个 SET 并发）
- 而 dict shard wrlock **已经在那儿了**——SET 命令本来就持有它
- 利用现成的串行点是工程优雅；非要把写端做 lock-free 反而要引入 retry
  loop + ABA 防护，复杂度爆炸

### Q6: 为什么 FP_VAL_MIN=32？为什么不缓存所有 value？

- `< 32 B` 的 value 在 dict 路径上已经极快（entry 整个就在 L1），
  fp_cache 多一次 bucket 探查反而是净开销
- 而 fp_cache 资源有限（1024 buckets），让小 value 占住 bucket =
  挤掉真正能受益的中等 value
- 32 是经验阈值，没有严格证明是最优；做得更精细可以做 admission policy
  （类似 W-TinyLFU）但 ROI 不高

---

## 可能被深问的「测试与验证」侧

### Q7: TSAN 真的"干净"吗？seqlock 不是天生有 race？

- 是的，**payload 上的 race 是有意设计**，TSAN 看到会报。我们用
  `__attribute__((no_sanitize_thread))` 标记 fp_get / fp_put / fp_del /
  fp_compare_key 4 个函数，**注释里写清楚为什么 correct**：
  - reader 的两次 seq acquire-load 自带 torn-read 检测
  - writer 的 CAS-on-seq 保证单写者
  - 即使 reader 读到 torn payload，第二次 seq 校验会失败，重试或回退
- 这是**业界标准做法**（Linux kernel 的 seqlock 也用同款 annotate）。
  与"用互斥关 TSAN warning"完全不同——后者是把 race 消掉、性能也消掉。

### Q8: 怎么证明没有真的 torn read？

- `fp_cache_test` 里 1 writer + 3 readers × 5 s 压测，每次读完 payload
  做 `key_hash + key_len + reply[0]` 一致性校验，**任何 torn read 都会
  导致这三个字段对不上**。运行 5 s, ~5M reads, **torn=0**。
- 还有 `writer_collisions=0`：writer 永远独占 bucket（受 dict wrlock
  保护），CAS-on-seq 从来没失败过。

---

## 不要说什么

- ❌ "比 Redis 快 1.9x" — 假的，1.9x 是 in-process 微基准对比内部 dict 路径
- ❌ "QPS 提升 50%" — redis-benchmark 实测 ±0.2%，不要吹这个数字
- ❌ "完全消除了锁" — 写端还是用 dict shard wrlock，串行写者
- ❌ "RCU 实现的 lock-free" — 是 seqlock，不是 RCU，差别很大
- ❌ "永远 100% 命中" — 64 keys 在 1024 buckets 里 hash 仍有 ~6% 碰撞，
  collision 的另一个 key 会 miss 回退到 dict

## 必要时的诚实退路

- 如果面试官问"端到端 QPS 提升多少"：直说 redis-benchmark 看是中性，
  原因是网络 50-70 µs 把 200 ns 优化淹没；这是 CPU 优化不是吞吐优化，
  价值在饱和工况 / 锁竞争 / 释放 CPU 给 AOF 复制。这个**坦诚就是加分项**。
- 如果面试官问"为什么不做 Phase 11 SIMD parser"：直说优先级判断——
  Phase 10 已经超额完成，再深挖单核 5% 的边际收益不如花时间打磨观测/
  正确性，这是工程取舍。
