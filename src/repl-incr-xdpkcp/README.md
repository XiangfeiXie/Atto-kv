# repl-incr-xdpkcp — AF_XDP + KCP 内核旁路增量同步

把增量同步的底层传输从 TCP 换成 **AF_XDP（内核旁路）+ KCP（可靠 UDP）**，

**范围：仅热路径**（写命令 → KCP → AF_XDP → 从机回放），不含 PSYNC / backlog 补发。

## 架构

两个二进制 + 一个 `/dev/shm` 共享内存通道（每台机器上引擎是 SHM 创建者，网关是挂载者）：

```
【Master 机器】                          【Slave 机器】
kvstore (master)                         kvstore (slave)
  │ 写命令成功 → kvs_repl_incr_hook        │ 事件循环 xdp_engine_poll()
  │ xdp_engine_enqueue()                  │ SHM 出队 → resp_parse_slices
  ▼                                        ▲  → kvs_dispatch() 回放 (g_in_reload=1)
SHM tx (无锁 SPSC)                        SHM rx (无锁 SPSC)
  ▲                                        │
  │ spsc_dequeue                           │ spsc_enqueue
kvstore-xdp-gateway --master             kvstore-xdp-gateway --slave
  │ ikcp_send → ikcp_update → output     │ ikcp_input → ikcp_recv(有序字节流)
  │   → AF_XDP Tx 发 UDP:8888            │   → 写 SHM；ikcp_update → ACK
  │ AF_XDP Rx 收 ACK → ikcp_input        │   → AF_XDP Tx 回 ACK
  ▼                                        ▲
        ──────── UDP:8888（内核旁路）───────
```

- **KCP 是双向的**（ACK 必须回流），master / slave 网关都同时有 TX 与 RX，
  两边的网卡都要挂 XDP 程序（`xdp_kcp.bpf.c`）把 `UDP:8888` 重定向进 XSK。
- **单条 SPSC 队列**，方向由角色决定：master 上引擎生产、网关消费；slave 上
  网关生产、引擎消费。

## 文件

| 文件 | 职责 |
|------|------|
| `shm_ipc.h/.c` | 无锁 SPSC 环形队列（`/dev/shm`，默认 `kvstore_xdpkcp_shm`）。同时编入两个二进制。 |
| `xdp_kcp.bpf.c` | XDP 程序：`bpf_redirect_map(&xsks_map, rx_queue, XDP_PASS)` 把 `UDP:8888` 重定向进 XSK。 |
| `af_xdp.h/.c` | raw AF_XDP（`linux/if_xdp.h`，不依赖 libxdp）：UMEM + Fill/Comp/Rx/Tx 四环 + XDP 加载挂载。 |
| `kcp_transport.h/.c` | KCP 胶水：`ikcp_output` 回调 → 组 Eth+IPv4+UDP 帧 → AF_XDP Tx；RX → `ikcp_input`。 |
| `gateway.c` | 网关进程 `main()`：busy-poll 主循环（idle 让出 `usleep(1)` + `_mm_pause()`）。 |
| `engine_xdp.h/.c` | 引擎侧（编入 kvstore）：`xdp_engine_enqueue()`（master）/ `xdp_engine_poll()`（slave）。 |
| `ikcp.h/.c` | vendored KCP（skywind3000/kcp）。 |

## 编译

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target kvstore kvstore-xdp-gateway
# 产物：build/kvstore、build/kvstore-xdp-gateway、build/xdp_kcp.bpf.o
```

依赖：`libbpf >= 1.0`、`clang`（编译 BPF）、`linux/if_xdp.h`（内核头，已有）。

## 运行

### 两机部署

Master 机器：

```bash
# kvstore.conf 里加：xdp_sync on   + rdma_role master
sudo ./build/kvstore conf/kvstore.conf
sudo ./build/kvstore-xdp-gateway --master \
    --iface ens33 --bpf build/xdp_kcp.bpf.o \
    --local-ip 192.168.30.137 --peer-ip 192.168.30.136 \
    --peer-mac 00:0c:29:xx:xx:xx
```

Slave 机器：

```bash
# kvstore.conf 里加：xdp_sync on   + rdma_role slave
sudo ./build/kvstore conf/kvstore.conf
sudo ./build/kvstore-xdp-gateway --slave \
    --iface ens33 --bpf build/xdp_kcp.bpf.o \
    --local-ip 192.168.30.136 --peer-ip 192.168.30.137 \
    --peer-mac 00:0c:29:yy:yy:yy
```

客户端连 master 发 `SET/INCR`，在 slave 侧 `GET` 校验一致性。

### 单机 veth 冒烟（无 `lo`）

AF_XDP 不能跑在 `lo` 上（回环无真实网卡 RX 队列），单机需用 veth 对：

```bash
sudo ip link add veth0 type veth peer name veth1
sudo ip link set veth0 up; sudo ip link set veth1 up
sudo ip addr add 10.0.0.1/24 dev veth0
sudo ip addr add 10.0.0.2/24 dev veth1
# 两组 kvstore + 网关各绑一个 veth，用 XDPKCP_SHM_NAME 区分两个 SHM：
XDPKCP_SHM_NAME=/kv_0 ./build/kvstore conf/master.conf &   # rdma_role master, port A
XDPKCP_SHM_NAME=/kv_1 ./build/kvstore conf/slave.conf  &   # rdma_role slave, port B
sudo env XDPKCP_SHM_NAME=/kv_0 ./build/kvstore-xdp-gateway --master --iface veth0 ...
sudo env XDPKCP_SHM_NAME=/kv_1 ./build/kvstore-xdp-gateway --slave  --iface veth1 ...
```

## 关键参数

- KCP 会话：固定 `conv=0x11223344`；`ikcp_nodelay(kcp, 1, 10, 2, 1)`（低延迟，关拥塞窗）；
  `ikcp_wndsize(kcp, 256, 256)`；`ikcp_setmtu(kcp, 1400)`；**stream 模式**（字节流）。
- 同步端口：`--port`（默认 `8888`），需与 `xdp_kcp.bpf.c` 的 `KCP_SYNC_PORT` 一致
  （改端口用 `-DKCP_SYNC_PORT=<n>` 重编 BPF）。
- SHM 槽：`SHM_MAX_PAYLOAD=8192`、`SHM_RING_SIZE=1024`（见 `shm_ipc.h`）。
- AF_XDP：copy 模式（`XDP_COPY`，兼容所有驱动）+ `XDP_USE_NEED_WAKEUP`；
  UMEM 4096 帧 × 4KB，Rx/Tx 各 2048 帧独立池。

## 可靠性边界

- **AF_XDP 依赖真实网卡驱动支持 XDP**（e1000/vmxnet3 等虚拟网卡可能报
  `EOPNOTSUPP`，需 generic 模式回退或换网卡）；**`lo` 不支持**。
- **无 backlog / PSYNC**：网关断线期间的命令会丢（热路径范围，后续补 resync 层）。
- **大命令（>8KB）在 master 引擎侧丢弃**并计数（benchmark 写命令普遍 <8KB）。
- **UDP 校验和暂为 0**（同二层同机房可接受，跨网关需补）。
- 网关在引擎之后启动（引擎创建 SHM）；引擎崩溃重启需一起重启网关。
- 单机多实例用 `XDPKCP_SHM_NAME` 区分 SHM 名（默认 `/kvstore_xdpkcp_shm`）。
