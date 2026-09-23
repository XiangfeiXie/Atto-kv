# repl-xdp — XDP 旁路镜像增量同步

把 kvstore **master 的客户端写流量在网卡层原样镜像一份给 slave**，使 slave 与
master 保持增量一致。挂在 master 侧的**旁路进程**，与 kvstore **零耦合**：
XDP 只采样，内核 TCP 栈负责可靠传输，master 业务侧无任何改动、无额外开销。

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
  因用户态做了重组，**不要求 master 钉单核**（这一点与 repl-tcp_recvmsg 相反）。
- **背压**：socket 非阻塞 + 用户态待发缓冲 `g_pend`，ring 永远即时消费；从机
  暂时跟不上时 `g_pend` 增长，跟上了自动排空。ring 满才丢采样（有 `drops` 计数）。

## 使用方法

> 默认参数：`MASTER_PORT=2000`、`SLAVE_PORT=2001`、`IFACE=veth0`、
> `SLAVE_IP=10.0.1.2`。需换端口/网卡见文末「配置项」。

### 1. 编译

```bash
cd /home/xxf/9.1-kvstore/build
cmake .. && cmake --build . --target repl-xdp repl-xdp-bpf -j"$(nproc)"
# 产物：build/repl-xdp  build/repl-xdp.bpf.o
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

两个实例都 `repl_role off` —— **增量同步完全由 XDP 镜像完成**，不启用 PSYNC 双写。

```
# master.conf                # slave.conf
port 2000                    port 2001
network proactor             network proactor
maxclients 1000              maxclients 1000
persistence none             persistence none
engine hash                  engine hash
repl_role off                repl_role off
expire off                   expire off
```

```bash
sudo ip netns exec red bash -c 'prlimit --pid $$ --memlock=unlimited; nohup \
  /home/xxf/9.1-kvstore/build/kvstore /tmp/xdp/master.conf > /tmp/kvs-master.log 2>&1 &'
sudo ip netns exec slv bash -c 'prlimit --pid $$ --memlock=unlimited; nohup \
  /home/xxf/9.1-kvstore/build/kvstore /tmp/xdp/slave.conf > /tmp/kvs-slave.log 2>&1 &'
```

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

```bash
# 向 master 写 10 万 key（跨 veth → master 10.0.1.1:2000）
sudo ip netns exec slv /home/xxf/9.1-kvstore/build/testcase 10.0.1.1 2000 fill 0 100000
# slave 上逐 value 校验（数据一致性 = 增量同步成功）
sudo ip netns exec slv /home/xxf/9.1-kvstore/build/testcase 10.0.1.2 2001 verify 0 100000
# DBSIZE 两端一致
sudo ip netns exec red redis-cli -p 2000 DBSIZE
sudo ip netns exec slv redis-cli -p 2001 DBSIZE
# DEL / FLUSHALL 同样跨 veth 传播（增删改全量镜像）
sudo ip netns exec slv redis-cli -p 2000 -h 10.0.1.1 FLUSHALL
```

### 6. 退出与清理

```bash
sudo pkill -INT -x repl-xdp     # 优雅退出：打印镜像字节/待发峰值/ring 丢采样
sudo pkill -x kvstore
```

## 配置项（`-D` 覆盖，改后需重新编译）

| 宏 | 默认值 | 含义 |
|---|---|---|
| `MASTER_PORT` | `2000` | 主服务端口（BPF 采样识别客户端负载） |
| `SLAVE_IP` | `"10.0.1.2"` | 从机 IP |
| `SLAVE_PORT` | `2001` | 从机端口 |
| `IFACE` | `"veth0"` | master 侧客户端流量所经网卡 |

```bash
cd /home/xxf/9.1-kvstore/build
cmake -DREPL_XDP_DEFINES="-DMASTER_PORT=6380 -DSLAVE_PORT=6381" .. && make -j
```

> 只有数字宏能经 `REPL_XDP_DEFINES` 安全传递。字符串宏（`SLAVE_IP` / `IFACE`）
> 覆盖会粘参，要改就直接改 `.c` 与 `.bpf.c` 里的默认值。

## 已知问题：`IFACE=lo` 镜像本机填充采不到数据（未修复）

把 `IFACE` 改挂 `lo`、想镜像「master 本机向 127.0.0.1 填充」的流量时，组件启动
一切正常，但**只采到连接 SYN，DATA 记录一条都没有**（`drops` 为 0，排除 ring 溢出）。

**根因**：回环路径上大块 `send()` 的 TCP 负载挂在 skb 的 page frags（非线形）里。
SKB 模式 XDP 的 `ctx->data ~ data_end` 只覆盖 skb 线性头，`payload < data_end`
不成立，DATA 分支被静默跳过。**不是 GSO/GRO**（关掉依旧丢）；网卡（如 ens33）不
受影响——驱动 RX 会重建线形 skb。

**规避**：本组件挂网卡用（`IFACE=veth0` 或真实网卡），客户端从对侧 netns / 另一
台机发起。要镜像 lo 得改 TC eBPF + `bpf_skb_load_bytes()` 穿透 frag，且 lo 的
MSS=65483 超过 `MAX_REC=2048`，还需处理大段切块。
