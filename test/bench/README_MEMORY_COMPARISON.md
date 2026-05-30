# jemalloc vs glibc 内存分配器深度对比测试

## 概述

这个测试脚本通过 4 个针对性的压力场景，深度对比 jemalloc 和 glibc 内存分配器的优缺点。

## 测试场景

### 场景 B: 混合大小分配（碎片炸弹）
**目标**: 测试混合大小对象分配后的碎片控制能力

**测试步骤**:
1. 写入 100K × 64B 小对象
2. 写入 100K × 4KB 大对象（交错存储）
3. 删除所有 4KB 对象
4. 等待内存稳定，测量碎片率

**关键指标**:
- 碎片率 = (实际RSS - 理论内存) / 实际RSS × 100%
- 内存开销 = 实际RSS / 理论内存

**预期结果**:
- jemalloc: 碎片率 < 20%，内存开销 < 1.5x
- glibc: 碎片率 > 30%，内存开销 > 2.0x

---

### 场景 C: 内存回收时间曲线
**目标**: 测试 FLUSHALL 后的内存回收速度

**测试步骤**:
1. 写入 500K × 256B
2. FLUSHALL 清空所有数据
3. 每 5 秒采样一次 RSS，持续 120 秒
4. 计算回收率

**关键指标**:
- 回收率 = (峰值RSS - 最终RSS) / (峰值RSS - 空闲RSS) × 100%
- 回收时间曲线

**预期结果**:
- jemalloc (调优): 30 秒内回收 90%
- glibc: 60 秒后仍保留 > 50% 内存

---

### 场景 D: 长时间运行稳定性
**目标**: 检测长期运行的内存膨胀问题

**测试步骤**:
1. 循环 10 轮：写入 200K × 256B → FLUSHALL → 等待稳定
2. 记录每轮的稳定态 RSS
3. 计算膨胀率

**关键指标**:
- 膨胀率 = (第10轮RSS - 第1轮RSS) / 第1轮RSS × 100%

**预期结果**:
- jemalloc: 膨胀率 < 10%
- glibc: 膨胀率 > 30%

---

### 场景 E: Churn 工作负载（流失模拟）
**目标**: 模拟真实缓存场景（不断有新旧数据替换）

**测试步骤**:
1. 初始写入 200K keys
2. 循环 20 轮：删除 50K 随机 keys + 写入 50K 新 keys
3. 记录每轮的 RSS
4. 计算增长率

**关键指标**:
- 增长率 = (最终RSS - 初始RSS) / 初始RSS × 100%

**预期结果**:
- jemalloc: RSS 稳定在初始峰值的 105-110%
- glibc: RSS 持续增长到初始峰值的 130-150%

---

## 使用方法

### 基本用法
```bash
# 使用默认配置（jemalloc 调优已启用）
sudo ./test/bench/bench_memory_comparison.sh
```

### 禁用 jemalloc 调优
```bash
# 测试 jemalloc 默认配置
sudo JEMALLOC_TUNE=0 ./test/bench/bench_memory_comparison.sh
```

### 前置条件
1. 需要 root 权限（用于精确的内存测量）
2. 需要编译两个版本的 kvstore：
   - `build/kvstore` (jemalloc 版本)
   - `build-glibc/kvstore` (glibc 版本)

### 编译两个版本
```bash
# 编译 jemalloc 版本
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc) kvstore

# 编译 glibc 版本
mkdir -p build-glibc && cd build-glibc
cmake .. -DCMAKE_BUILD_TYPE=Release -DUSE_JEMALLOC=OFF
make -j$(nproc) kvstore
```

---

## 测试配置

### 系统配置
- **线程数**: 1 (单线程，避免多线程干扰)
- **持久化**: 关闭
- **网络模型**: proactor
- **端口**: 2000

### jemalloc 调优参数
```bash
MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000,background_thread:true
```

**参数说明**:
- `dirty_decay_ms:1000`: 脏页保留 1 秒后回收（默认 10 秒）
- `muzzy_decay_ms:1000`: 模糊页保留 1 秒后回收（默认 10 秒）
- `background_thread:true`: 启用后台线程主动回收内存

---

## 输出结果

### 文件位置
- **文本报告**: `test/bench/results_memory_comparison.txt`
- **CSV 数据**: `test/bench/results_memory_comparison.csv`

### 报告内容
1. **场景 B**: 碎片率对比表
2. **场景 C**: 内存回收曲线数据
3. **场景 D**: 10 轮循环的膨胀率
4. **场景 E**: Churn 工作负载的增长率
5. **综合结论**: 自动统计各场景胜负，给出推荐

---

## 关键改进（相比初版）

### 1. 修复了碎片率计算错误
- **问题**: 初版计算出 97% 的碎片率（不合理）
- **修复**: 正确计算理论内存，并显示内存开销倍数

### 2. 修复了删除操作
- **问题**: 初版删除操作使用错误的 RESP 格式
- **修复**: 使用 `redis-cli --pipe` 正确删除 key，并验证删除数量

### 3. 启用 jemalloc 调优
- **问题**: 初版默认关闭调优，导致 jemalloc 回收率仅 0.1%
- **修复**: 默认启用 `dirty_decay_ms:1000`，加速内存回收

### 4. 增加等待时间
- **问题**: 初版等待 45 秒不够，内存未充分稳定
- **修复**: 场景 B/D 增加到 90 秒

### 5. 改为单线程测试
- **问题**: 4 线程会导致 jemalloc 创建多个 arena，增加碎片
- **修复**: 使用 1 线程，更纯粹地测试分配器本身

### 6. 智能结论生成
- **问题**: 初版结论是硬编码的，不反映实际测试结果
- **修复**: 根据实际数据自动判断胜负，生成客观结论

---

## 预期测试结果

### 启用 jemalloc 调优后
```
场景 B（碎片炸弹）:  jemalloc < glibc  ✓
场景 C（回收曲线）:  jemalloc > glibc  ✓
场景 D（长期稳定）:  jemalloc < glibc  ✓
场景 E（Churn）:     jemalloc < glibc  ✓

总体: jemalloc 胜出 4/4 场景
```

### 禁用 jemalloc 调优
```
场景 B（碎片炸弹）:  jemalloc ≈ glibc  ~
场景 C（回收曲线）:  jemalloc < glibc  ✗
场景 D（长期稳定）:  jemalloc ≈ glibc  ~
场景 E（Churn）:     jemalloc < glibc  ✓

总体: 表现相当，需要调优
```

---

## 故障排查

### 问题 1: 碎片率仍然很高（> 80%）
**可能原因**:
- 删除操作未生效
- key 格式不匹配

**排查方法**:
```bash
# 查看日志中的 "删除了 X 个 key"
grep "删除了" test/bench/results_memory_comparison.txt

# 应该显示: 删除了 100000 个 key
```

### 问题 2: jemalloc 回收率很低（< 5%）
**可能原因**:
- 调优参数未生效
- 等待时间不够

**排查方法**:
```bash
# 确认调优已启用
grep "jemalloc 调优" test/bench/results_memory_comparison.txt

# 应该显示: jemalloc 调优: enabled
```

### 问题 3: 内存膨胀率异常高（> 200%）
**可能原因**:
- 内存未充分回收
- 存在内存泄漏

**排查方法**:
```bash
# 查看每轮的稳定 RSS
grep "long_running.*stable_rss" test/bench/results_memory_comparison.csv

# 应该看到逐渐稳定的趋势，而不是持续增长
```

---

## 扩展测试

### 测试不同的 decay 时间
```bash
# 测试更激进的回收（500ms）
sudo MALLOC_CONF=dirty_decay_ms:500,muzzy_decay_ms:500,background_thread:true \
  ./test/bench/bench_memory_comparison.sh

# 测试更保守的回收（5000ms）
sudo MALLOC_CONF=dirty_decay_ms:5000,muzzy_decay_ms:5000,background_thread:true \
  ./test/bench/bench_memory_comparison.sh
```

### 对比多线程场景
修改脚本中的 `threads 1` 为 `threads 4`，观察多线程对碎片率的影响。
