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

> 默认参数：`MASTER_PORT=2000`、`SLAVE_PORT=2000`、`IFACE=lo`、
> `SLAVE_IP=192.168.30.136`（本机主从 + 压测客户端也在 master 本机走回环那套
> 拓扑）。**下面第 2–5 节的 netns+veth demo 与默认值不同**，要先按第 4 节把
> `IFACE`/`SLAVE_IP`/`SLAVE_PORT` 改成 `veth0`/`10.0.1.2`/`2001` 再编译。
> 端口/网卡见文末「配置项」。

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

> 本 demo 要求 `IFACE=veth0`、`SLAVE_IP=10.0.1.2`、`SLAVE_PORT=2001`，与组件
> 默认值（`lo` / `192.168.30.136` / `2000`）**不同**，须先改 `.c` 里的这三个
> 字符串宏再编译，否则组件会挂到 netns 的 `lo` 上，采样恒为 0。

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
| `SLAVE_IP` | `"192.168.30.136"` | 从机 IP |
| `SLAVE_PORT` | `2000` | 从机端口 |
| `IFACE` | `"lo"` | master 侧客户端流量所经网卡 |
| `MAX_REC` | `16384` | 单条采样上限 = **单个 TCP 段的负载**上限，见文末「已知问题」 |

```bash
cd /home/xxf/9.1-kvstore/build
cmake -DREPL_XDP_DEFINES="-DMASTER_PORT=6380 -DSLAVE_PORT=6381" .. && make -j
```

> 只有数字宏能经 `REPL_XDP_DEFINES` 安全传递。字符串宏（`SLAVE_IP` / `IFACE`）
> 覆盖会粘参，要改就直接改 `.c` 与 `.bpf.c` 里的默认值。
>
> **改了宏一定要强制重编 `.bpf.o`**：它的自定义命令只依赖源文件、不依赖宏值，
> `make` 会一路 `Built target repl-xdp-bpf` 认为它是最新的，宏改了个寂寞。
> 稳妥做法 `rm -f build/repl-xdp.bpf.o build/repl-xdp` 再 `make repl-xdp`。
> 验法：`bpftool btf dump file build/repl-xdp.bpf.o | grep scratch_rec`，
> 看 size 是否 = 16 + `MAX_REC`（16384 → `size=16400`）。

## 已知问题：单条 TCP 段超过 `MAX_REC` 会被静默丢弃

### 症状

把 `IFACE` 挂 `lo`、镜像「master 本机向 127.0.0.1 发命令」的流量时，组件启动
一切正常（`[xdp] 已挂 lo` + `[sock] 已连从机`），但**只采到连接 SYN，DATA 记录
一条都没有**（`drops` 为 0），从机上那些写全都不存在。

### 根因：不在内核，在组件自己的入口判界

`push_rec()` 第一件事就是判长度：

```c
if (len < 0 || len > MAX_REC)
    return;                    /* 静默返回：不计数、不告警 */
```

而调用方传进来的 `len` 是**整段 TCP 负载**，不是"一条命令"：

```c
push_rec(REC_DATA, sport, bpf_ntohl(tcp->seq),
         (char *)data_end - (char *)payload, payload);
```

于是**任何单个 TCP 段的负载超过 `MAX_REC`，这一段就整段被丢掉** —— 不是截断，
是全丢。判据是"段"，不是"命令"、不是"write"、也不是"包"。

### 为什么回环上必挂、挂网卡却看不出来

关键在 MSS：**客户端一次 `write()` 会按 MSS 切成若干个 TCP 段**。

| 挂载点 | MTU / MSS | 一次 7200B 的 write | 结果 |
|---|---|---|---|
| `lo` | 65536 / 65483 | **就是一整段 7200B** | 段负载 7200 > `MAX_REC` → 整段丢 |
| `ens33` / `veth0` | 1500 / 1460 | 切成 5 段，每段 ≤1460B | 每段都合格 → 正常 |

所以网卡上"没问题"**不是因为它更正确，只是因为它的段更小**。回环 MSS 65483
意味着 `MAX_REC` 在 lo 上同时就是"客户端单次 write 的上限"。旧默认值 2048 配
回环，redis-benchmark 只要 P≥46（P×45 字节 > 2048）就一个字节都采不到；
`testcase fill` 一次 `send()` 约 512KB（见 `test/fill_cmds` 注释），更是必挂。

### 为什么 `drops=0` 是个假线索

`drops` 计数器**只在 `bpf_ringbuf_output()` 失败（ring 满）那一条路径自增**，
走上面那个 `return` 根本不计数。所以"`drops` 为 0"只能排除 ring 溢出，
**排除不了长度判界** —— 早先据此推出的"回环大 `send()` 的负载挂在 skb page
frags 里、SKB 模式 XDP 的 `data_end` 只覆盖线性头、所以穿透不了"是**错的**：
通用（SKB）模式 XDP 在跑程序之前会把 skb 线性化，`data_end` 是覆盖到负载尾的，
**数据一直都在，是组件自己扔了**。那次归因被 `drops=0` 带偏了。

### 判据与实测边界

逐条 SET 二分（段负载 = value + 24 + key 长 + value 位数）：

- `MAX_REC=16384` 时**精确钉死**：段负载 **16384B 通 / 16385B 丢**
- `MAX_REC=2048` 时只测到区间：负载 2035B 通 / 2235B 丢（与 2048 一致，未逐字节逼近）

与 DRV/SKB 挂载模式**无关**（`lo` 上是 SKB 回退，`veth0` 上是原生 DRV，两者的
差别只是段大段小）。

### 当前取值与它的上限

`MAX_REC` 默认已调到 **16384**，覆盖 ≤16KB 的单次 write（redis-benchmark 约
P≤360；实测 P=1 / 40 / 160 三档主从 DBSIZE 完全一致）。

**不能再往上加**：`scratch` 是 `BPF_MAP_TYPE_PERCPU_ARRAY`，value 超过约 32KB
创建会 `E2BIG`（同 `repl-tcp_recvmsg/README.md` 那条限制），所以 65536 是不可行的。
它也只能**编译期**改，改完记得强制重编 `.bpf.o`（见「配置项」）。

### 尚未覆盖的

单次 write > 16384 字节的客户端（如 `testcase fill`，一次约 512KB）在 `lo` 上
**仍会被整段丢弃**。要覆盖得改成**大段按 seq 切成多条 ≤`MAX_REC` 的记录**：
用户态本来就按 seq 重组（顺序段直发、乱序段暂存、重复段丢弃），所以只要 BPF 侧
把一条大段拆成 `seq, seq+N, seq+2N…` 的连续记录，**用户态一行都不用改**。
目前没做——回环压测走的是 redis-benchmark，单次 write 只有 P×45 字节，够用了。
挂网卡用则完全不受这条限制（段 ≤1460B）。
