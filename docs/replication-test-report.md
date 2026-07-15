# 主从同步测试报告

**日期**: 2026-07-15  
**测试对象**: KVStore 10.0 主从复制（TCP + eBPF 增量同步）  
**配置**: Master `conf/kvstore-master-local.conf` / Slave `conf/kvstore-slave-local.conf`

---

## 一、测试环境

| 项目 | 值 |
|------|-----|
| 内核版本 | 6.8.0-124-generic |
| eBPF 支持 | `CONFIG_BPF_STREAM_PARSER=y` |
| 网络模型 | Proactor (io_uring + SQPOLL) |
| Master 端口 | 2000 (Redis) / 2100 (RDMA) / 6483 (TCP 复制) |
| Slave 端口 | 2001 (Redis) |

---

## 二、测试结论总览

| 类别 | 测试项 | Proactor 结果 | Reactor 结果 |
|------|--------|:------------:|:------------:|
| 基本 | SET / GET / DEL | ✅ PASS | ✅ PASS |
| 基本 | 批量 100 key 逐条写入 | ✅ PASS | ✅ PASS |
| Pipeline | 单 TCP 段 pipeline (≤1460B, 20 条命令) | ✅ PASS | ✅ PASS |
| Pipeline | 多 TCP 段 pipeline (~8180B, 200 条命令) | ❌ **FAIL** | ✅ PASS |
| Pipeline | 大 pipeline (~42KB, 1000 条命令) | ❌ **FAIL** | ✅ PASS |
| 长命令 | 100KB SET value | ❌ **FAIL** | 未测 |
| 读写混合 | SET+GET+DEL 混合 pipeline | ✅ PASS | ✅ PASS |

---

## 三、缺陷详情

### 缺陷 #1 (严重): Proactor + eBPF 多 TCP 段数据丢失

**影响范围**: Proactor 网络模型下，所有超过单个 TCP MSS (~1460 字节) 的客户端请求无法被 eBPF 增量复制捕获。

**复现步骤**:

```bash
# 1. 启动 Master (proactor)
build/kvstore conf/kvstore-master-local.conf

# 2. 启动 Slave
build/kvstore conf/kvstore-slave-local.conf

# 3. 向 Master 发送 pipeline
python3 -c "
import socket
def resp(*a):
    return '*' + str(len(a)) + '\r\n' + ''.join(f'\${len(x)}\r\n{x}\r\n' for x in a)
s = socket.socket(); s.connect(('127.0.0.1', 2000))
# 200 条 SET，约 8180 字节，跨 6 个 TCP 段
s.sendall(''.join(resp('SET', f'k{i}', f'v{i}') for i in range(200)).encode())
" 

# 4. Slave 端查询 — 所有 key 返回 $-1
redis-cli -p 2001 GET k0   # → (nil)
redis-cli -p 2001 GET k199 # → (nil)
```

**诊断过程**:

1. **定位数据流断裂点**: 在 `master_on_data()` 添加调试日志，200-pipe 测试时回调**从未被调用**，确认 ringbuf 无事件。

2. **排除用户态问题**: `bpftool map dump` 确认 `repl_config` 配置正确 (port=2000)，`repl_filter.bpf.o` 加载成功。`bpftrace` 跟踪 `sk_psock_strp_data_ready` / `sk_psock_verdict_recv` 均为 0，证明 strparser 未被触发。

3. **定位 io_uring 交互**: 切换 `network reactor` 后，相同测试**全部通过**。确认问题出在 Proactor 的 `io_uring_prep_recv_multishot` + `IORING_SETUP_SQPOLL` 与内核 sockmap/strparser 的交互。

**根因分析**:

```
正常路径 (Reactor/epoll):
  数据到达 → sk_data_ready → sk_psock_strp_data_ready
  → strp_parse → BPF stream_parser → BPF stream_verdict
  → ringbuf → SK_PASS → 正常收包队列 → epoll 通知用户态

异常路径 (Proactor/io_uring SQPOLL):
  第 1 段到达 → sk_data_ready → strparser → ringbuf ✅
  io_uring SQPOLL 开始主动轮询 socket
  第 2~N 段到达 → SQPOLL 内核线程直接通过
    tcp_read_sock / tcp_recvmsg_locked 读取数据
    → 不触发 sk_data_ready → strparser 跳过 ❌
  → ringbuf 只有第 1 段数据，流重组中断
```

**临时绕过方案**: 使用 `network reactor` 替代 `network proactor`。

**建议修复方向**（按优先级）:

1. 去掉 `IORING_SETUP_SQPOLL`，使用 `IORING_SETUP_DEFER_TASKRUN` + `IORING_SETUP_SINGLE_ISSUER`（`proactor_ring.c:51-53` 已有 fallback，可设为默认路径）
2. 改用独立 tracepoint/kprobe 方式捕获写命令，而非依赖 sockmap/strparser
3. 检查内核 6.8+ 是否有相关补丁，确认是否需升级内核

### 缺陷 #2 (轻微): 未设置 capabilities 时 eBPF 静默退化

**影响范围**: 编译后未手动执行 `setcap` 时，eBPF 过滤器静默失败，日志仅输出一行 Warning，增量复制完全不可用，容易在调试中被忽略。

**日志示例**:
```
[REPL-TCP] Warning: eBPF filter failed, using fallback mode
```

但实际并无 fallback — `repl_stream_publish()` 为空操作，所有增量复制依赖 eBPF。

**建议**: eBPF 加载失败时打印更显眼的错误信息，并考虑在主循环中周期性重试加载。

---

## 四、测试覆盖的代码路径

| 代码 | 文件 | 测试结果 |
|------|------|:------:|
| RESP 解析 (`find_complete_resp_command`) | `repl_tcp_master.c:86-125` | ✅ 正确 |
| 写命令过滤 (`is_write_command`) | `repl_tcp_master.c:35-77` | ✅ 正确 |
| 流重组 + 多命令提取 | `repl_tcp_master.c:269-291` | ✅ 逻辑正确 |
| 从机帧接收 (`slave_thread_func`) | `repl_tcp_slave.c:235-260` | ✅ 正确 |
| 从机命令应用 (`slave_apply_command`) | `repl_tcp_slave.c:12-70` | ✅ 正确 |
| eBPF sockops 端口监听 | `repl_filter.bpf.c:40-56` | ✅ 字节序正确 |
| eBPF stream_verdict 数据拷贝 | `repl_filter.bpf.c:68-91` | ✅ 逻辑正确 |
| **eBPF + io_uring SQPOLL 交互** | **内核** | ❌ **多段丢失** |

---

## 五、附录：关键命令速查

```bash
# 启动 Master
build/kvstore conf/kvstore-master-local.conf

# 启动 Slave
build/kvstore conf/kvstore-slave-local.conf

# 设置 capabilities（编译后需执行一次）
echo "1234" | sudo -S setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep build/kvstore

# 测试单条复制
redis-cli -p 2000 SET k v && redis-cli -p 2001 GET k

# 测试 pipeline 复制（单段）
python3 -c "
import socket
s=socket.socket();s.connect(('127.0.0.1',2000))
s.sendall('*3\r\n\$3\r\nSET\r\n\$1\r\na\r\n\$1\r\nb\r\n*3\r\n\$3\r\nSET\r\n\$1\r\nc\r\n\$1\r\nd\r\n'.encode())
"
# 检查 Slave: redis-cli -p 2001 GET a && redis-cli -p 2001 GET c

# 查看 BPF 状态
sudo bpftool prog list | grep repl_filter
sudo bpftool map dump id $(sudo bpftool map list | grep repl_sockmap | awk -F: '{print $1}')
```
