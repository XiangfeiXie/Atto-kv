# repl-xdp — XDP 旁路镜像增量同步（挂载到 kvstore）

把 kvstore **master 的客户端写流量在网卡层原样镜像一份给 slave**，使 slave 与
master 保持增量一致。挂在 master 侧的**旁路进程**，与 kvstore **零耦合**：
XDP 只采样，内核 TCP 栈负责可靠传输，master 业务侧无任何改动、无额外开销。

**实测（挂到 kvstore 上）：** 累计 40 万 SET + 跨机 DEL + FLUSHALL，master/slave
DBSIZE 始终一致、逐 value 校验全对、BPF ring 丢采样 **0 条**；master 性能无影响。

## 原理

- **XDP（master 客户端流量所经网卡入方向）只采样，绝不拦截**：客户端 → master
  端口的包，把 TCP 负载复制进 BPF ring buffer，原包一律 `XDP_PASS`。
- **用户态（旁路进程）三件事**：挂 XDP、把 ring 里采到的字节 `write()` 到从机、
  读掉从机 `+OK` 应答（不读会撑满 rcvbuf → 窗口 0 → 从机停摆）。
- **可靠传输全部交给内核 TCP 栈**（seq / ACK / 重传 / 窗口 / 背压），用户态
  不做手动 TCP 组包。
- **顺序保证**：BPF ring 是 per-CPU 的，veth 多 RX 队列会跨 CPU 乱序。每条采样
  带**首个负载字节的 TCP seq + 客户端源端口**，用户态按 seq 重组（顺序段直发、
  乱序段暂存、重复段丢弃）；每个连接 SYN 发锚点（seq=ISN+1）处理连接切换。
- **背压**：socket 非阻塞 + 用户态待发缓冲 `g_pend`，ring 永远即时消费；从机
  暂时跟不上时 `g_pend` 增长，跟上了自动排空。ring 满才丢采样（有 `drops` 计数）。

## 与 raw socket 版（早期 repl-xdp.c）的区别

| | raw socket 版（已删） | 本组件（repl-xdp-sock） |
|---|---|---|
| 发送方 | 用户态重建 TCP 段：手动 seq/校验和/RTO/窗口/缓存 | **内核 TCP 栈**，write() 即完成 |
| 发送可靠 | 用户态 RTO 重传 + 缓存 | 内核 RTO/重传 |
| 复杂度 | 约 900 行 | 约 500 行 |
| 从机应答 | 用户态解析 ACK 推窗口 | 内核自动回 ACK，用户态读掉 +OK |

## 挂载到 kvstore 上的使用方法

> 默认参数已对齐 kvstore 主从端口：`MASTER_PORT=2000`、`SLAVE_PORT=2001`、
> `IFACE=veth0`、`SLAVE_IP=10.0.1.2`。需换端口/网卡用 `-D` 覆盖编译（见文末）。

### 1. 编译（组件与 kvstore 一起编译）

```bash
cd /home/xxf/9.1-kvstore/build
cmake .. && make -j
# 产物：build/kvstore  build/repl-xdp  build/repl-xdp.bpf.o
```

### 2. 网络环境（master / slave 两个 netns + veth）

```bash
sudo ip netns add red && sudo ip netns add slv
sudo ip link add veth0 type veth peer name veth1
sudo ip link set veth1 netns slv && sudo ip link set veth0 netns red
sudo ip netns exec red ip addr add 10.0.1.1/24 dev veth0
sudo ip netns exec slv ip addr add 10.0.1.2/24 dev veth1
sudo ip netns exec red ip link set veth0 up
sudo ip netns exec slv ip link set veth1 up
sudo ip netns exec red ip link set lo up
sudo ip netns exec slv ip link set lo up
```

### 3. 启动 kvstore master + slave

```bash
sudo ip netns exec red bash -c 'prlimit --pid $$ --memlock=unlimited; nohup \
  /home/xxf/9.1-kvstore/build/kvstore \
  /home/xxf/9.1-kvstore/conf/kvstore-xdp-master.conf > /tmp/kvs-master.log 2>&1 &'
sudo ip netns exec slv bash -c 'prlimit --pid $$ --memlock=unlimited; nohup \
  /home/xxf/9.1-kvstore/build/kvstore \
  /home/xxf/9.1-kvstore/conf/kvstore-xdp-slave.conf > /tmp/kvs-slave.log 2>&1 &'
```

`kvstore-xdp-master.conf` / `kvstore-xdp-slave.conf`：master 2000、slave 2001，
`repl_role off` —— **增量同步完全由 XDP 镜像完成**，不启用 PSYNC 双写。

### 4. 挂载 repl-xdp（master netns，需 root + memlock）

```bash
sudo ip netns exec red bash -c 'prlimit --pid $$ --memlock=unlimited; nohup \
  /home/xxf/9.1-kvstore/build/repl-xdp \
  /home/xxf/9.1-kvstore/build/repl-xdp.bpf.o > /tmp/repl-xdp.log 2>&1 &'
```

日志出现 `[xdp] 已挂 veth0` + `[sock] 已连从机 10.0.1.2:2001` 即挂载成功。

### 5. 写入并校验增量同步

> **测试客户端必须从 slave netns（或任何跨 veth 的地方）发起**：客户端若在
> master netns 内连本机 IP，走本地回环路径不经 veth0，XDP 采样不到。
> （把 `IFACE` 改挂 `lo` 想直接镜像本机填充同样采不到——回环大块写的 frag 问题，
> 见下文「已知问题」。）

```bash
# 向 master 写 10 万 key（跨 veth → master 10.0.1.1:2000）
sudo ip netns exec slv /home/xxf/9.1-kvstore/build/testcase 10.0.1.1 2000 fill 0 100000
# slave 上逐 value 校验（数据一致性 = 增量同步成功）
sudo ip netns exec slv /home/xxf/9.1-kvstore/build/testcase 10.0.1.2 2001 verify 0 100000
# DBSIZE 两端一致
sudo ip netns exec red  redis-cli -p 2000 DBSIZE
sudo ip netns exec slv redis-cli -p 2001 DBSIZE
# DEL / FLUSHALL 同样跨 veth 传播（增删改全量镜像）
sudo ip netns exec slv redis-cli -p 2000 -h 10.0.1.1 FLUSHALL
```

### 6. 退出与清理

```bash
sudo pkill -INT -x repl-xdp     # 优雅退出：打印镜像字节/待发峰值/ring 丢采样
sudo pkill -x kvstore
```

## 实测结果（挂载到 kvstore 上）

| 场景 | 结果 |
|---|---|
| fill 10 万 × 3 轮（30 万）+ 末轮 10 万 | master=slave 每轮一致，逐 value 校验全对 |
| 跨 veth DEL 3 个 key | slave 同步删除，DBSIZE 同步 -3 |
| 跨 veth FLUSHALL | 两端同时归零 |
| BPF ring 丢采样 | **0 条**（累计镜像约 4300 万字节） |
| master 性能（镜像开/关 fill 10 万） | ~261ms / ~243ms，差异 ~7% 在抖动内，无影响 |
| 干净 SIGINT 退出 | 无 stack smashing，统计正常，XDP 正常卸载 |

## 已知边界

- **单客户端流**：只保证一条客户端流的 RESP 命令边界；多客户端合流会破坏命令
  边界（kvstore slave 按 RESP 解析，边界错会断连重试，但不在本组件保证范围）。
- **干净链路前提**：ring 永不溢出的前提下 seq 重组无空洞。若 ring 满丢采样
  （4MB ring 在本规模不触发），有 `drops` 计数 + 待发记录数上限兜底退出，不
  会默默错位。
- **待发缓冲无上限**：从机长期跟不上时 `g_pend` 无限增长（本规模实测峰值
  ~500KB 且自动排空）。
- **客户端重传**：同一包被客户端重传时 BPF 采到同 seq 重复段，用户态按
  `seq < g_next_seq` 丢弃，不影响正确性。

## 已知问题：IFACE=lo 镜像本机填充采不到数据（未修复）

**场景**：把 `IFACE` 从网卡改挂 `lo`，想镜像「master 本机向 127.0.0.1 填充」的流量。

**症状**：repl-xdp 启动正常（`[xdp] 已挂 lo`、已连从机、开始镜像），但客户端
`fill` 大块写入期间，退出统计为 `镜像 0 字节`。抓 BPF ring 发现**只采到连接 SYN，
DATA 记录一条没有**（`drops` 计数为 0，排除 ring 溢出）。

**根因**（2026-08-31 实测定位，未修复）：回环路径上，**大块 `send()` 的 TCP 负载
挂在 skb 的 page frags（非线形）里**。generic（SKB 模式）XDP 的 `ctx->data ~
data_end` 只覆盖 skb 线性头；payload 在 frag 中时，repl-xdp-sock.bpf.c 里
`payload < data_end` 不成立，DATA 分支被静默跳过 → ring 里只有 SYN 锚点。

| 客户端发送方式 | lo 上 XDP 能否采到 DATA |
|---|---|
| 大块 send（testcase fill 512KB 一次） | ✗ 只有 SYN |
| 单条小 send（62B） | ✓ |
| 单连接逐条小 send（27B×500 + TCP_NODELAY） | ✓ |

**排除项**：不是 GSO/GRO——`ethtool -K lo gro off gso off tso off` 后依旧丢。
lo 专属：`IFACE=ens33` 的跨机镜像正常（网卡驱动 RX 重建线形 skb，payload 可见）；
lo 不走驱动，发送端 frag skb 原样进 RX，XDP 只能看到线性头。

**修复方向（未实施）**：
- **最小**：testcase fill 改小分块 send + `TCP_NODELAY`，让每个 send 独立成小线性段。
  只对本 demo 的客户端生效。
- **通用**：改用 TC eBPF 挂 lo，用 `bpf_skb_load_bytes()` 穿透 frag 读 payload。
  注意 lo 的 MSS=65483（单段可达 64KB）> `MAX_REC=2048`，需处理大段切块或压 MTU。

## 配置项（`-D` 覆盖，改后需重新编译）

| 宏 | 默认值 | 含义 |
|---|---|---|
| `MASTER_PORT` | `2000` | 主服务端口（BPF 采样识别客户端负载） |
| `SLAVE_IP` | `"10.0.1.2"` | 从机 IP |
| `SLAVE_PORT` | `2001` | 从机端口 |
| `IFACE` | `"veth0"` | master 侧客户端流量所经网卡 |

```bash
# 例：换端口后重新编译（REPL_XDP_DEFINES 同时作用于 BPF 与用户态）
cd /home/xxf/9.1-kvstore/build
cmake .. -DREPL_XDP_DEFINES="-DMASTER_PORT=6380 -DSLAVE_PORT=6381" && make repl-xdp
```
