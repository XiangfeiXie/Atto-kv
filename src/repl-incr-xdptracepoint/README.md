# kvstore-incr-sync

基于 XDP 的 Redis 无感知增量同步守护进程。通过在内核态旁路抓包，用户态完成 TCP 重组和 RESP 协议解析，过滤写命令后转发到从节点，实现主从数据同步。

## 架构

```
┌─────────────────────────────────────────────────────┐
│ XDP BPF (内核态)                                     │
│   纯旁路拷贝 — 始终返回 XDP_PASS，不影响原有流量       │
│   按端口过滤 IPv4/TCP，拷贝载荷到 ringbuf              │
└──────────────────────┬──────────────────────────────┘
                       │ ringbuf (8MB)
┌──────────────────────▼──────────────────────────────┐
│ 用户态单线程 (无锁)                                    │
│   ring_buffer__poll() → xdp_reasm_on_event()         │
│     → 分桶哈希表 (256 桶, 锁无关)                     │
│     → TCP seq 去重/排序 (OOO 链表)                   │
│     → 环形缓冲区 (摊销 O(1) 写入)                     │
│     → RESP 协议解析 + 写命令过滤                      │
│     → on_write() → 直接追加 slave 写缓冲              │
│   poll 返回后 → send(slave_fd, MSG_DONTWAIT)         │
└──────────────────────┬──────────────────────────────┘
                       │ TCP (非阻塞 send)
                   Slave Redis
```

## 数据流

1. **XDP 抓包**: 客户端→master 的 TCP 包在网卡 XDP hook 被拷贝，payload + 四元组 + seq 提交到 ringbuf
2. **TCP 重组**: 单线程无锁处理，按 `(saddr, sport)` 建立连接状态，seq 去重、乱序排队，保序写入环形缓冲区
3. **RESP 解析**: 从环形缓冲区扫描完整 RESP 命令
4. **写命令过滤**: 识别 SET/DEL/INCR/LPUSH/SADD/ZADD/HSET 等 13 类写命令，忽略只读命令
5. **发送**: 写命令直接追加到 slave 写缓冲区，poll 返回后非阻塞 send 到 slave

## 文件结构

```
repl-incr-xdptracepoint/
├── config.h           # 所有配置宏 + likely/unlikely 分支预测提示
├── xdp_common.h       # BPF/用户态共享结构体 (ringbuf event)
├── xdp_filter.bpf.c   # XDP BPF 内核程序 (纯旁路抓包, XDP_PASS)
├── xdp_loader.h       # XDP BPF 加载/卸载 API
├── xdp_loader.c       # XDP BPF 加载实现 (bpf_link + netlink 双模式)
├── xdp_reasm.h        # TCP 重组 + RESP 解析 API (无锁)
├── xdp_reasm.c        # 核心实现: 哈希表、环形缓冲、RESP解析、OOO池
├── main.c             # 主入口: slave连接管理、非阻塞发送、主循环
└── CMakeLists.txt      # 构建配置
```

## 编译

```bash
mkdir build && cd build
cmake ..
make
```

依赖: `libbpf >= 1.0`, `clang` (编译 BPF), `cmake >= 3.16`

## 配置

所有配置通过 `config.h` 中的宏定义，支持编译期覆盖：

```bash
# 自定义端口和网卡
cmake .. -DCMAKE_C_FLAGS="\
  -DKVS_FILTER_PORT=6379 \
  -DSLAVE_PORT=6380 \
  -DXDP_IFACE=\\\"eth0\\\" \
  -DCONN_MAX=8192 \
  -DSLAVE_WB_HIGH_WATER=8388608"
make
```

### 主要配置项

| 宏 | 默认值 | 说明 |
|----|--------|------|
| `SLAVE_IP` | 127.0.0.1 | 从节点地址 |
| `SLAVE_PORT` | 6379 | 从节点端口 |
| `KVS_FILTER_PORT` | 6379 | XDP 抓包过滤端口 |
| `XDP_IFACE` | lo | XDP 挂载网卡 |
| `CONN_HASH_BITS` | 8 | 哈希桶位数 (2^8=256) |
| `CONN_MAX` | 4096 | 最大连接数 |
| `CONN_GC_TIMEOUT` | 30 | 连接空闲超时 (秒) |
| `OOO_MAX_SEGMENTS` | 64 | 每连接最大乱序段数 |
| `CMD_BUF_CAP_MAX` | 4MB | 单连接缓冲区上限 |
| `SLAVE_WB_HIGH_WATER` | 4MB | Slave 写缓冲高水位 (触发丢弃) |
| `SLAVE_WB_LOW_WATER` | 1MB | Slave 写缓冲低水位 (恢复) |
| `SLAVE_RECONNECT_S` | 3 | 重连退避间隔 (秒) |
| `SLAVE_POLL_FAST_MS` | 5 | 有积压时 poll 超时 (ms) |
| `SLAVE_POLL_IDLE_MS` | 100 | 空闲时 poll 超时 (ms) |

## 运行

```bash
# generic 模式 (SKB, 所有 NIC 通用)
sudo XDP_MODE=generic ./kvstore-incr-sync

# native 模式 (需 NIC 驱动支持)
sudo XDP_MODE=native ./kvstore-incr-sync

# auto 模式 (BPF link API 自动选择)
sudo ./kvstore-incr-sync
```

也可通过 `setcap` 避免每次 sudo:

```bash
sudo setcap cap_bpf,cap_net_admin,cap_sys_admin+ep ./kvstore-incr-sync
XDP_MODE=generic ./kvstore-incr-sync
```

## 设计要点

### 无锁分桶哈希表
256 个哈希桶，每条 TCP 连接按 `(src_ip, src_port)` 哈希到固定桶。单线程运行，无需任何锁。保留分桶结构以便将来扩展到多线程（libbpf ringbuf 多消费者模式）。

### 环形缓冲区
替代线性 buffer + memmove。读写指针单调递增，取模访问。读完命令后只移动读指针，无需 memmove。空闲时自动缩容，大块空闲内存及时回收。

### 直接反压路径
Slave 写缓冲达到高水位 (SLAVE_WB_HIGH_WATER, 4MB) → `on_write` 返回 -1 → 丢包（`send_dropped` 计数）→ BPF ringbuf 继续被消费 → BPF Ringbuf 满 → `bpf_ringbuf_reserve` 返回 NULL → BPF 静默丢弃。整条链路无阻塞、无锁。

### OOO 数据缓冲池
预分配 256 个 4KB 数据缓冲，OOO 段插入时从池中获取，释放时归还。热路径零 malloc：数据缓冲复用，仅 24 字节 `ooo_seg_t` 结构体需 malloc。合并场景自动降级为 heap 分配，池缓冲归还。

### 写命令查表
`is_write_command_buf` 用 `switch ((c0<<8)|c1)` 替代 12 个链式 if-else。编译器生成密集跳转表（jump table），O(1) 常数时间匹配，无需逐条比较。

### 分支预测提示
热路径使用 `likely()`/`unlikely()` 宏（`__builtin_expect`）提示 CPU 分支预测器：顺序到达包、命令完整解析、写缓冲不超水位判为 likely；SYN/FIN/RST、协议错误、缓冲满、连接断开判为 unlikely。减少流水线冲刷。

### 自适应 poll
有未发送数据时用 5ms 快速超时重试 send，空闲时用 100ms 超时省 CPU。

### TCP 流还原
- 纯被动观测，只处理客户端→master 单向流量
- SYN 提取 ISN，seq 相对化后去重排序
- 中途观测 (miss SYN) 自动推测 ISN 建立连接
- FIN/RST 后清空缓冲区并标记可回收
- 连接 GC：30s 空闲 或 关闭且无待处理数据

### 乱序处理
排序链表存储 OOO 段，插入时自动合并相邻/重叠段达到硬上限后清空重置。

### 可靠性边界
- **纯旁路不修改原流量** — 转发失败不影响 master
- 写缓冲满时 **丢弃而非阻塞** — 反压到 BPF 层
- 断线 **非阻塞自动重连** — 3 秒退避
- 关闭时 **阻塞冲刷** — 确保已解析的命令发送到 slave
- 性能瓶颈在用户态 **TCP 重组和 RESP 解析**，而非 XDP 传输层

## XDP 模式

| 模式 | 命令 | 说明 |
|------|------|------|
| auto | 默认 | BPF link API，内核自动选 native/generic |
| generic | `XDP_MODE=generic` | SKB 模式，所有 NIC 支持，适合测试 |
| native | `XDP_MODE=native` | 驱动层零拷贝，需 NIC 驱动支持 |

注意: loopback (`lo`) 的 XDP generic 模式在部分内核版本可能不触发，生产环境建议使用物理网卡。

## 测试

测试脚本需要创建隔离网络环境：

```bash
# 1. 创建 veth pair 和 network namespace
sudo ip link add veth0 type veth peer name veth1
sudo ip addr add 10.0.0.1/24 dev veth0
sudo ip link set veth0 up

sudo ip netns add testns
sudo ip link set veth1 netns testns
sudo ip netns exec testns ip addr add 10.0.0.2/24 dev veth1
sudo ip netns exec testns ip link set veth1 up
sudo ip netns exec testns ip route add default via 10.0.0.1

# 2. 启动 Redis
redis-server --port 6380 --bind 10.0.0.1 --protected-mode no --daemonize yes
redis-server --port 6381 --bind 127.0.0.1 --protected-mode no --daemonize yes

# 3. 启动同步守护进程
sudo XDP_MODE=generic ./kvstore-incr-sync

# 4. 从隔离命名空间发送流量测试
sudo ip netns exec testns redis-cli -h 10.0.0.1 -p 6380 SET key value

# 5. 验证同步
redis-cli -h 127.0.0.1 -p 6381 GET key
```

## License

GPL-2.0 (BPF 程序要求)
