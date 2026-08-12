# XDP 增量同步组件 — 批量命令丢失修复报告

> **日期:** 2026-08-12
> **组件:** `src/repl-incr-xdptracepoint/` — XDP 增量同步 daemon
> **问题:** 大批量 RESP 命令复制时部分命令丢失（100 个 SET 仅 87 个到达 slave）
> **根因:** `XDP_MAX_PAYLOAD` 仅 4096 字节，loopback TSO/GSO 巨型 segment 被静默截断

---

## 1. 修改摘要

| 文件 | 修改 | 类型 | 影响 |
|------|------|------|------|
| `src/repl-incr-xdptracepoint/xdp_common.h` | `XDP_MAX_PAYLOAD`: 4096 → 65536 | Bug 修复 | 消除大型 TCP segment 的静默截断 |
| `src/repl-incr-xdptracepoint/xdp_common.h` | `XDP_RINGBUF_SZ`: 8MB → 16MB | 配套调整 | 适应 16× 更大的 ringbuf event |
| `src/repl-incr-xdptracepoint/xdp_reasm.c` | 更新过时注释 | 文档 | 移除硬编码的 4096 描述 |
| `test/smoke/test_incr_sync.sh` | 移除误导性 SKIP 标签 | 测试 | 原标签将 bug 归因于 "kvstore pipeline load limitation" |

### 1.1 新增调试基础设施

本次调试过程中为组件新增了关键诊断能力，保留用于未来排障：

| 文件 | 新增内容 | 用途 |
|------|----------|------|
| `xdp_reasm.h` | `buf_resets`、`parse_errors` 计数器 | 用户态数据路径诊断 |
| `xdp_filter.bpf.c` | `repl_stats` percpu map + `XDP_CNT_*` 计数器 | BPF 端 ringbuf 满/截断诊断 |
| `xdp_loader.c` | `xdp_print_bpf_stats()` | 优雅退出时打印 BPF 统计 |
| `xdp_common.h` | `XDP_CNT_*` 枚举 | BPF/用户态共享计数器索引 |
| `main.c` | `FINAL` 统计行 + BPF 统计输出 | 退出时输出完整诊断快照 |

---

## 2. 问题表现

烟雾测试 `test/smoke/test_incr_sync.sh` 的批量管道测试稳定失败：

```
PASS  RESP 100 SETs in one send (master)
SKIP  Slave DBSIZE exact match — got 87/100
SKIP  Spot-check bulk keys — got 4/5
```

每次都是精确的 **87/100**，不是随机丢失。

直接向 kvstore slave 发送 5000 个管道命令（不读响应）全部正确，排除了 kvstore 自身的管道解析 bug。

---

## 3. 调试过程

### 3.1 第一轮：用户态计数器

在 `xdp_reasm_stats` 中添加 `buf_resets` 和 `parse_errors` 计数器：

```c
// xdp_reasm.h — 新增字段
uint64_t buf_resets;         /* circ_write failure → buffer reset */
uint64_t parse_errors;       /* resp_scan returned -1 (non-RESP) */
```

**结果：** `buf_reset=0, parse_err=0` — 数据路径内没有发生丢弃。

### 3.2 第二轮：BPF 端计数器

问题定位到 XDP ringbuf 层。在 BPF 程序中添加 percpu 计数器 map：

```c
// xdp_filter.bpf.c — 关键诊断点
if (!e) {
    /* Ringbuf full — data silently dropped! */
    __u32 cnt_key = XDP_CNT_RINGBUF_FULL;
    __u64 *cnt = bpf_map_lookup_elem(&repl_stats, &cnt_key);
    if (cnt) (*cnt)++;
    return XDP_PASS;
}
```

**结果：**
```
[XDP-BPF] total=4 submitted=4 ringbuf_full=0 probe_fail=0
[MAIN]  FINAL: events=4 written=148 conns=4 ooo=0 wb=0 send_drop=0 buf_reset=0 parse_err=0
```

- `ringbuf_full=0` — ringbuf **从未满**，不是在 BPF 层丢的
- `events=4` — 仅 4 个 TCP segment 被捕获
- 每个 segment 最多 4096 字节 → 关键线索

### 3.3 根因确认：数据大小计算

```python
# 100 个 bulk SET 命令的精确大小
Total size of 100 bulk SETs: 4700 bytes
Commands fitting in 4096 bytes: 87        # ← 与 slave DBSIZE 完美吻合
Phase1 (20 SET+20 INCR):   1120 bytes
Phase2 (10 SET+10 INCR):    570 bytes
Total all phases:          6390 bytes
```

**数字闭环：**
- 4700 字节 ＞ 4096 字节 `XDP_MAX_PAYLOAD` → 截断
- 4096 ÷ (4700÷100) ≈ 87 个命令到达 slave
- Slave DBSIZE = 87 ✓
- `written = 40(Phase1) + 20(Phase2) + 1(FLUSHALL) + 87(截断后) = 148` ✓

---

## 4. 根因分析

### 4.1 截断路径

[`xdp_filter.bpf.c:103-104`](src/repl-incr-xdptracepoint/xdp_filter.bpf.c#L103-L104)：

```c
if (payload_len > XDP_MAX_PAYLOAD)
    payload_len = XDP_MAX_PAYLOAD;  // 静默截断到 4096 字节
```

在 loopback 接口上，内核使用 **TSO/GSO**（TCP Segmentation Offload / Generic Segmentation Offload）将多个 `send()` 调用聚合成一个巨型 TCP segment（最大 64KB）。

**数据流：**

```
Python sendall(4700 bytes)
  → 内核 TSO/GSO 聚合为一个巨型 TCP segment (4700 bytes)
    → XDP BPF 捕获该 segment
      → payload_len = min(4700, 4096) = 4096
        → 后 604 字节（13 个命令）被截断
          → ringbuf → 用户态 daemon → 转发 87/100 → slave
```

整个过程**没有任何错误日志或计数器**，被截断的数据静默丢失。

### 4.2 为什么不是 ringbuf 溢出

8MB ringbuf × 8KB 数据 → 不可能溢出。`ringbuf_full=0` 已证明。

### 4.3 为什么是精确的 87 而不是随机值

100 个 SET 命令使用统一的 key 格式 `bulk_k{i:04d}` + `bulk_v{i:04d}`（每个 47 字节）。TCP segment 边界由 `sendall()` 的缓冲区大小决定，每次测试的 segment 边界完全相同。

---

## 5. 修复方案

### 5.1 核心修复

```c
// xdp_common.h（修复前）
#define XDP_MAX_PAYLOAD     4096

// xdp_common.h（修复后）
#define XDP_MAX_PAYLOAD     65536   // 覆盖 loopback 最大 TSO segment (64KB)
```

### 5.2 配套调整

| 调整项 | 修复前 | 修复后 | 原因 |
|--------|--------|--------|------|
| `XDP_EVENT_SIZE` | 4116 (20+4096) | 65556 (20+65536) | event 结构体大小增加 16× |
| `XDP_RINGBUF_SZ` | 8 MB | 16 MB | 保持 ~250 events 容量 |
| OOO buffer pool | 256×4KB = 1MB | 256×64KB = 16MB | 每个预分配 buffer 匹配 event 大小 |

### 5.3 总内存影响

- Ringbuf: 8MB → 16MB（**+8MB**）
- OOO buffer pool: 1MB → 16MB（**+15MB**）
- **总计: +23MB**，仅影响 daemon 进程

### 5.4 适用场景

| 部署场景 | TSO/GSO/GRO | 巨型 segment | 修复覆盖 |
|----------|-------------|-------------|----------|
| 单机 loopback | TSO/GSO | ≤ 64KB | ✅ |
| 物理网卡 (master→slave) | GRO (接收侧) | ≤ 64KB | ✅ |
| 跨主机 (客户端→master) | 发送端 TSO + 接收端 GRO | ≤ 64KB | ✅ |

---

## 6. 验证结果

### 6.1 烟雾测试

```
修复前: SKIP  Slave DBSIZE exact match — got 87/100
修复后: PASS  Slave DBSIZE matches master (100)

修复前: SKIP  Spot-check bulk keys — got 4/5
修复后: PASS  Slave: spot-check 5 bulk keys

最终: 39/40 passed, 1 skipped (仅剩信息性 SKIP)
```

唯一的 SKIP 是 "Read-command filtering check — all events were write commands"，这是因为测试中未发送读命令，无法验证读过滤功能，属正常情况。

### 6.2 Daemon 统计

```
[MAIN] FINAL: events=43 written=181 conns=5 ooo=0 wb=0 send_drop=0 buf_reset=0 parse_err=0
```

- `events=43` — 完整捕获所有 TCP segment（不再是 4）
- `written=181` — 所有命令被转发（不再是 148）
- `send_drop=0, buf_reset=0, parse_err=0` — 无任何数据丢失

---

## 7. 经验教训

1. **XDP 数据路径的静默截断是危险的。** 应在截断点添加至少一个计数器（本次修复中在 BPF 端新增了 `repl_stats` map 和 `XDP_CNT_*` 计数器体系，可用于未来排障）。

2. **loopback 测试不能假设小 MTU。** TSO/GSO 使 loopback 上的 TCP segment 可以达到 64KB，远远超过物理网卡的 1500 字节 MTU。

3. **"管道负载限制"的早期假设是错误的。** kvstore 的管道处理完全没有问题（5000 命令验证通过）。问题始终在 XDP→ringbuf→用户态这条数据采集路径上。

4. **诊断基础设施应前置。** 本次调试中新增的 BPF 和用户态计数器应该作为标准组件的一部分，避免未来遇到类似问题时需要反复加日志重编译。
