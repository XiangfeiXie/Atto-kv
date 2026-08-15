# kvstore-incr-sync

基于 XDP 的 Redis **无感知**增量同步守护进程。在内核态旁路镜像客户端→master 的 TCP 字节流（始终 `XDP_PASS`，不影响原流量），用户态按 TCP 序列号重建有序字节流（空洞检测），再以 1:1 连接把字节流**原样（verbatim）回放**到从库。

不解析 RESP、不过滤命令、不做乱序缓冲。

## 架构

```
客户端 ──TCP──▶ master kvstore (KVS_FILTER_PORT)
                   │  主库网卡入向 (XDP_PASS + 拷贝，不修改原流量)
                   ▼
            XDP BPF (内核态, xdp_filter.bpf.c)
               按 dport == KVS_FILTER_PORT 过滤 IPv4/TCP
               拷贝载荷 + 四元组 + seq 提交 ringbuf (16MB)
                   │  ringbuf
                   ▼
            用户态单线程 epoll (main.c)
              ring_buffer__consume() ─▶ stream.c (seq 重建 + 空洞检测)
                                           │ 有序字节段
                                           ▼
                                      replay.c (1:1 verbatim 回放 + 应答排空)
                                           │ 非阻塞 send
                                           ▼
                              从库 kvstore (SLAVE_IP:SLAVE_PORT)
```

## 数据流

1. **XDP 抓包**：客户端→master 的 TCP 包在主库网卡 XDP hook 被拷贝，payload + `(saddr,sport)` + seq 提交 ringbuf；返回 `XDP_PASS` 放行，内核协议栈照常处理。
2. **流重建**（`stream.c`）：按 `(saddr, sport)` 建立连接状态，以 `rcv_nxt` 严格有序推进；seq 超前 → 记空洞并设 `HOLE_TIMEOUT_MS` 截止时间；重复段丢弃；SYN 提取 ISN、FIN/RST 收尾、空闲 GC。
3. **verbatim 回放**（`replay.c`）：每条源连接 1:1 映射到一条从库 TCP 连接；有序字节追加写缓冲，epoll 非阻塞 `send`；从库应答（读命令仍会执行）`recv` 后丢弃；断线非阻塞重连。
4. **统计**：每 `STATS_INTERVAL_S` 打印 `[STATS]` 与 `[XDP-BPF]` 计数。

## 文件结构

```
repl-incr-xdptracepoint/
├── config.h          # 所有配置宏
├── xdp_common.h      # BPF/用户态共享结构体 (event、reserve 常量、flags)
├── xdp_filter.bpf.c  # XDP BPF 程序 (纯旁路抓包, XDP_PASS)
├── xdp_loader.h/.c   # BPF 加载 / 挂载 / 卸载 (auto = native→generic 回退)
├── stream.h/.c       # TCP 流重建 (空洞检测，无 OOO 缓冲)
├── replay.h/.c       # verbatim 回放 (1:1 映射 + 应答排空)
├── main.c            # 单线程 epoll 主循环
└── CMakeLists.txt    # 构建配置
```

## 编译

```bash
mkdir build && cd build
cmake ..
make
```

依赖：`libbpf >= 1.0`、`clang`（编译 BPF）、`cmake >= 3.16`。

> ⚠️ **残留缓存坑**：测试脚本会通过 `-DCMAKE_C_FLAGS="-DSLAVE_PORT=… -DKVS_FILTER_PORT=…"` 覆盖 config.h，这些值会留在 CMake 缓存里，之后重编译仍用旧端口。改完 config.h 后若端口不生效，先清掉再编译：
> ```bash
> cmake .. -U CMAKE_C_FLAGS && make
> ```

### 可选：缩小 ringbuf 每事件保留字节

默认 `XDP_RESERVE_PAYLOAD = XDP_MAX_PAYLOAD = 65536`（保证 generic+GRO 的 64KB 聚合包不截断）。在 **native / 1500-MTU** 部署下，XDP 看到的线缆段 ≤ 1460，可缩小到 4096 以容纳约 8 倍事件：

```bash
cmake .. -DXDP_RESERVE_PAYLOAD=4096
```

## 配置

所有配置通过 `config.h` 宏定义，可用 `-D` 编译期覆盖。当前默认值（两机部署）：

| 宏 | 默认值 | 说明 |
|----|--------|------|
| `SLAVE_IP` | 192.168.30.136 | 从库地址 |
| `SLAVE_PORT` | 2000 | 从库**主端口**（即从库的 `port`，不是 incr_repl_port） |
| `KVS_FILTER_PORT` | 2000 | XDP 抓包过滤端口（= master 的 `port`） |
| `XDP_IFACE` | ens33 | XDP 挂载网卡（master 的接收网卡） |
| `CONN_HASH_BITS` | 8 | 哈希桶位数（2^8=256） |
| `CONN_MAX` | 4096 | 最大连接数 |
| `CONN_GC_TIMEOUT_MS` | 30000 | 空闲连接 GC（ms） |
| `HOLE_TIMEOUT_MS` | 2000 | 空洞超时→判 corrupt（ms） |
| `REPLAY_BUF_INIT` | 4096 | 每条回放连接写缓冲初始大小 |
| `REPLAY_BUF_MAX` | 16MB | 写缓冲硬上限→判 corrupt |
| `REPLAY_RECONNECT_MS` | 1000 | 从库重连退避（ms） |
| `REPLAY_DRAIN_MS` | 5000 | FIN 后冲刷排空上限（ms） |
| `REPLAY_EPOLL_MAXEVENTS` | 64 | epoll_wait 批量大小 |
| `STATS_INTERVAL_S` | 10 | 统计打印间隔（s） |
| `TIMER_TICK_MS` | 100 | 定时 tick（ms） |

`xdp_common.h` 中的抓包常量：

| 宏 | 默认值 | 说明 |
|----|--------|------|
| `XDP_MAX_PAYLOAD` | 65536 | 载荷上限（覆盖 loopback 64KB MTU 与 IPv4 GRO 64KB） |
| `XDP_RESERVE_PAYLOAD` | = MAX | ringbuf 每事件保留字节（决定 ringbuf 容量） |
| `XDP_CAPTURE_PAYLOAD` | min(MAX, RESERVE) | 实际截断上限 |
| `XDP_RINGBUF_SZ` | 16MB | ringbuf 总大小 |

## 运行

```bash
sudo ./kvstore-incr-sync                             # auto：先 native，失败回退 generic
sudo env XDP_MODE=generic ./kvstore-incr-sync       # 强制 generic(SKB)
sudo env XDP_MODE=native ./kvstore-incr-sync        # 强制 native(DRV)
```

> `sudo` 会重置环境变量，`XDP_MODE` 等需用 `sudo env VAR=… ./bin`（或 `sudo -E`），否则不生效。

启动后关键日志：

```
[MAIN] slave=192.168.30.136:2000 filter_port=2000 iface=ens33
[XDP-LOAD] attached 'ens33' (ifindex=2, generic(SKB)), port 2000
[REPLAY] connected to 192.168.30.136:2000
[STATS] events=… written=… ooo=… send_drop=… conns=… holes=… dup=… corrupt=…
```

## XDP 模式

| 模式 | 命令 | 说明 |
|------|------|------|
| auto | 默认 | 先 native(DRV)，失败回退 generic(SKB)，启动打印实际模式 |
| native | `XDP_MODE=native` | 驱动层（GRO 之前），需驱动实现 `ndo_bpf`；`e1000` 等虚拟网卡会报 `EOPNOTSUPP` |
| generic | `XDP_MODE=generic` | SKB 层（GRO 之后），所有网卡支持 |

### 段大小与 RESERVE 的关系

XDP 实际看到的段大小取决于模式与 GRO：

| 场景 | XDP 看到的段大小 | `RESERVE=4096` |
|------|------------------|----------------|
| native（vmxnet3 / 物理网卡） | ≤ 1460（GRO 之前） | ✅ 安全 |
| generic + GRO on | ≤ 64KB（GRO 之后聚合） | ❌ 截断→corrupt |
| generic + GRO off | ≤ 1460 | ✅ 安全（仍有 SKB 开销） |

native 模式在 GRO 之前运行，天然看到线缆帧，无需关 GRO；generic 模式要缩小 RESERVE 必须先 `ethtool -K <iface> gro off`。

## 两机部署

```
客户端(第三台机器) ──▶ master 192.168.30.137:2000 ──[ens33 上挂 XDP]──▶ slave 192.168.30.136:2000
```

步骤：

1. **master 机器**：启动 master kvstore（`port 2000`），再 `sudo ./kvstore-incr-sync`（XDP 挂 ens33，filter 2000）。
2. **slave 机器**：启动 slave kvstore（`port 2000`），并**关闭其自带复制**，否则写命令会被执行两次（`INCR` 等非幂等命令直接出错）：

   ```
   rdma_sync off
   incr_sync off
   persistence none
   ```

3. **客户端**：从第三台机器连 `192.168.30.137:2000` 发写命令。

注意事项：

- XDP 只抓**真正过网卡**的流量。客户端在 master 本机用 `127.0.0.1` 连会走 `lo` 回环，ens33 抓不到（`events=0`）。
- 构建前确认 config.h 的端口与 master/slave 的 `port` 一致，且没有残留 `-D` 端口覆盖。
- 单机冒烟测试（见下）与两机部署的 `XDP_IFACE` / `SLAVE_IP` 不同，需分别配置。

## 设计要点

### 纯旁路观察（零感知）
BPF 程序对命中端口的数据包 `bpf_ringbuf_submit` 后固定返回 `XDP_PASS`，master 完全无感知，不修改、不丢弃任何流量。

### 空洞检测（无 OOO 缓冲）
不保留乱序链表、不排序、不合并。seq 超前只记一个空洞并设截止时间：`HOLE_TIMEOUT_MS` 内没补上 → 判 corrupt（观察者丢包不可恢复——客户端只会重传给 master，不会重传给观察者）。

### verbatim 回放（无 RESP 解析 / 不过滤）
字节流原样回放，不解析 RESP、不区分读写命令。读命令也会被回放，从库执行后其应答被 `recv` 丢弃（应答排空），避免从库输出缓冲阻塞。

### 1:1 连接映射
每条源连接（客户端↔master）严格映射到一条从库连接，天然保持各连接内部的命令顺序；多客户端并发各走各的连接。

### 单线程 epoll
ringbuf（`ring_buffer__epoll_fd`）、timerfd、各从库 socket 统一进 epoll，事件驱动低 CPU；`replay_sync_interest()` 每轮批量后重算 EPOLLOUT。

### ringbuf 固定 reserve（内核约束）
`bpf_ringbuf_reserve()` 要求编译期常量大小（`ARG_CONST_ALLOC_SIZE_OR_ZERO`），故每个事件固定保留 `sizeof(header)+XDP_RESERVE_PAYLOAD`，只填 `data_len` 字节，消费端按 `data_len` 跳过尾部填充。

## 可靠性边界

- **纯旁路不修改原流量**：回放失败不影响 master。
- **观察者丢包不可恢复**：空洞检测发现后判 corrupt，交由后续 resync 层补齐（当前未实现）。
- **写缓冲超限判 corrupt**：从库持续阻塞时 `REPLAY_BUF_MAX` 触顶，该连接停止回放而非无限堆积。
- **非阻塞重连**：从库断线 `REPLAY_RECONNECT_MS` 退避重连。
- 无全量同步 / 无复制控制面，仅覆盖增量（预留后续扩展）。

## 测试

单机冒烟测试（`lo` 回环，master/slave 同机不同端口）：

```bash
cd ../../test/smoke
./test_incr_sync.sh            # 默认 MASTER_PORT=5555 SLAVE_PORT=5556
```

覆盖：写命令复制、管道/批量命令、并发连接、重连、统计与一致性校验。

> 注意：冒烟测试假设 `XDP_IFACE=lo`、`SLAVE_IP=127.0.0.1`（master/slave 同机），与上面两机部署的 `ens33` + 真实 IP 不同，运行前需按场景改 config.h 或用 `-D` 覆盖。

## License

GPL-2.0（BPF 程序要求）
