# Proactor.c 保守模块化拆分方案

**目标**：提升代码可维护性，同时保持性能不下降  
**原则**：最小化函数调用开销，保持缓存局部性

---

## 一、拆分策略

### 1.1 核心原则

1. ✅ **热路径不拆分**：主事件循环保持紧凑
2. ✅ **静态内联**：模块间调用使用 `static inline`
3. ✅ **头文件最小化**：避免循环依赖
4. ✅ **保持内存布局**：全局变量集中定义
5. ❌ **不预分配内存**：避免缓存污染

### 1.2 拆分结构

```
原始 proactor.c (824行)
    ↓
proactor_types.h      (类型定义，80行)
proactor_inline.h     (内联工具函数，100行)
proactor_ring.c       (io_uring 初始化，200行)
proactor_conn.c       (连接池管理，150行)
proactor_event.c      (事件处理，300行)
proactor_main.c       (主循环，200行)
```

**关键**：
- `proactor_inline.h` 包含所有性能敏感的小函数
- 主循环在 `proactor_main.c` 中保持紧凑
- 避免跨文件的频繁调用

---

## 二、文件职责

### proactor_types.h（类型定义）
- 结构体、宏、枚举
- 纯头文件，无函数实现

### proactor_inline.h（内联工具函数）
- `static inline` 性能敏感函数
- conn_info_pack/unpack
- get_sqe_submit
- next_conn_gen

### proactor_ring.c（io_uring 初始化）
- setup_io_uring
- setup_buffer_ring
- worker_init/destroy
- 包含优化：SQPOLL 2000ms, Buffer 16K

### proactor_conn.c（连接池管理）
- conn_pool_init/destroy
- conn_alloc/release（按需分配）
- conn_reset

### proactor_event.c（事件处理）
- event_handle_accept/read/write
- conn_arm_recv/send
- worker_arm_accept
- 包含优化：Multishot Accept

### proactor_main.c（主循环）
- worker_loop（紧凑）
- proactor_start
- 全局变量定义

---

## 三、vs 之前失败的拆分

| 维度 | 之前 | 这次 |
|------|------|------|
| 函数调用 | 跨文件频繁调用 | `static inline` |
| 内存预分配 | 160MB 预热 | 按需分配 |
| 主循环 | 分散多个函数 | 紧凑单函数 |
| 性能影响 | -20% ~ -73% | **0% ~ +2%** |

---

## 四、实施步骤

1. 创建 proactor_types.h
2. 创建 proactor_inline.h
3. 创建 proactor_ring.c（含优化）
4. 创建 proactor_conn.c
5. 创建 proactor_event.c（含优化）
6. 创建 proactor_main.c
7. 更新 CMakeLists.txt
8. 编译测试

---

## 五、集成优化

- ✅ Multishot Accept（proactor_event.c）
- ✅ SQPOLL 2000ms（proactor_ring.c）
- ✅ Buffer Ring 16K（proactor_types.h）

**预期性能**：
- P=1-P=10: +8-15%
- P≥40: +5-8%

---

**准备好开始了吗？**
