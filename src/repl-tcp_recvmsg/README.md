# repl-tcp_recvmsg — kprobe 读侧镜像增量同步（挂载到 kvstore）

把 kvstore **master 进程读到的客户端命令字节**镜像一份给 slave，使 slave 与
master 保持增量一致。挂在 master 侧的**旁路进程**，与 kvstore **零耦合**：
kprobe 只采样，内核 TCP 栈负责可靠传输，master 业务侧无任何改动。

**实测（挂到 kvstore 上，reactor 引擎，全程 127.0.0.1）：** 十万 → 五十万 → 一百
万 SET 增量填充，master/slave DBSIZE 始终一致、value 校验全过、BPF ring 丢采样
**0 条**、produced==consumed。累计镜像 127MB 端到端零丢失。详见下方大流量实测。

### 大流量实测数据

master 钉单核（`taskset -c 5`），`testcase fill` 连续 SET 灌数据，从机当普通
客户端应用镜像命令：

| 轮次 | 灌入量 | 主从 DBSIZE | 校验 | BPF 丢采样 | 突发窗口速率 |
|---|---|---|---|---|---|
| 10 万 | 6 MB | 100000 == 100000 | 全量逐 value ✓ | 0 | 峰值 4.43 MB/s① |
| 50 万 | 30 MB | 500000 == 500000 | 全量逐 value ✓ | 0 | 峰值 15.54 MB/s |
| 100 万 | 60 MB（fill 3.479s） | 1000000 == 1000000 | 抽查 90万~99万 ✓ | 0 | **~17 MB/s 持续**（17.44/16.03/17.70，recs 17~18k/s）② |

> ① 单轮仅 ~2 个 stat 窗口、含爬坡/收尾，速率被 1s 窗口摊薄；② 3.5s 完整突发、
> 连续 3 个窗口一致 → 最干净的稳态值。

**消费是 work-conserving**：每个 [stat] 窗口 prod==cons、backlog 恒 0，镜像消费
完全跟得上生产——速率上限在 master 单核 reactor 写路径（~17MB/s），不在镜像/消费
侧。镜像 SIGINT 退出累计汇总（含上述三轮全部流量）：

```
[mirror] 退出：镜像 125100100 字节，待发峰值 524272 字节，BPF ring 丢采样 0 条
[mirror] 统计：生产总计 127055172 字节，消费总计 127055172 字节（差 = ring 残留存量）
```

突发时从机 socket 瞬时追不上，待发缓冲峰值仅 512KB，随后自动排空，全程无采样丢失。

## 与 repl-xdp / repl-recv 的定位差异

| | repl-xdp | repl-recv | 本组件（repl-tcp_recvmsg） |
|---|---|---|---|
| 采样点 | 网卡入方向（XDP） | 用户态 libc `recv`（uprobe） | 内核 `tcp_recvmsg_locked`（kprobe） |
| 适用引擎 | 需 veth/netns | 仅 reactor（proactor 不经过 libc） | **reactor + proactor 通吃** |
| 网络环境 | veth + netns + XDP | 无特殊要求 | 无特殊要求（lo 即可） |
| 采样内容 | TCP 负载（需按 seq 重组） | 应用读到的字节 | 应用读到的字节（天然按序） |

`tcp_recvmsg_locked` 是 `read/recv/recvmsg` 与 `io_uring_recvmsg` 的共同收口，
所以本组件同时覆盖 reactor（`recvmsg`）与 proactor（`io_uring_prep_recvmsg`）。

## 原理

- **挂载点**：kprobe 挂 `tcp_recvmsg_locked` 入口、kretprobe 挂出口。
- **按 tgid 过滤目标进程**：BPF 对全系统 TCP 读都触发，但只镜像 argv 传入的
  master PID（写进 `cfg` map）。master 作为服务器只读客户端连接 → 采到的就是
  客户端写入的命令字节流。kprobe 无法按 pid 限定 attach 范围，必须在程序里过滤。
- **入口/出口暂存（pending hash）**：kretprobe 拿不到入口参数，且
  `tcp_recvmsg_locked` 执行中会推进 `msg_iter`。所以入口把本次读要灌的
  `msg_iter` 快照存进按 tid 索引的 `pending`，出口取用后删除。
- **多段 gather 逐段镜像**：kvstore 引擎 `recvmsg` 一次最多读
  `RBUF_MAX_IOV=16` 段、每段 ≤ 1KB 的 rbuf 节点（`msg_iter` 恒 ITER_IOVEC）。
  出口按返回值逐段把用户内存拷进 ring，**一段 = 一条记录**，消费侧按记录序拼接
  即得完整字节流。记录头带全局单调 `seq`，用户态按序核对——乱序即报错退出
  （fail-closed），不静默错序。
- **返回值为 int 的符号坑（关键）**：`tcp_recvmsg_locked` 返回 `int`。x86-64
  写 eax 会清零高 32 位，kretprobe 若按 64 位无符号读 RAX，会把 `-EAGAIN(-11)`
  误读成 `4294967285`，绕过 `ret<=0` 守卫、把没数据的段当 16KB 垃圾拷进 ring
  （实测首条读必现）。必须 `(long)(int)PT_REGS_RC(ctx)` 截断到有符号 int 再扩展。
- **scratch 中转缓冲的尺寸坑（关键）**：用户内存 → ring 要经 per-CPU map 中转。
  per-CPU map 的 value 过大（>~32KB）创建会 `E2BIG`。因每段 ≤ 1KB，`MAX_REC`
  定为 1024 即可（对齐 `rbuf.h` 的 `RBUF_NODE_SIZE`）；若日后 node_cap 上调须同步。
- **背压**：socket 非阻塞 + 用户态待发缓冲 `g_pend`，ring 永远即时消费；从机
  跟不上时 `g_pend` 增长，跟上了自动排空。ring 满才丢采样（有 `drops` 计数）。

## 使用方法

> 换从机地址/端口改 `repl-tcp_recvmsg.h` 的 `SLAVE_IP/SLAVE_PORT`（字符串宏
> 经 CMake `-D` 会粘参，与 repl-xdp 同坑，覆盖须改源码默认值）。

### 1. 编译

```bash
cd /home/xxf/9.1-kvstore/build
cmake .. && cmake --build . --target repl-tcp_recvmsg repl-tcp_recvmsg-bpf -j"$(nproc)"
# 产物：build/repl-tcp_recvmsg  build/repl-tcp_recvmsg.bpf.o
```

### 2. 启动 kvstore master + slave（同机 127.0.0.1 演示）

```bash
# master 必须 taskset 钉单 CPU：ring 按 per-CPU 产出，固定后消费必按序
taskset -c 5 ./kvstore ../conf/kvstore-xdp-master.conf &   # 监听 :2000
./kvstore ../conf/kvstore-xdp-slave.conf &                 # 监听 :2001
# 记录 master PID
```

### 3. 启动镜像（root 加载 eBPF，memlock 解除）

```bash
sudo prlimit --pid $$ --memlock=unlimited
sudo ./repl-tcp_recvmsg <master-pid> repl-tcp_recvmsg.bpf.o
# Ctrl-C 退出并打印汇总（镜像字节 / drops / produced / consumed）
```

### 4. 验证主从一致

```bash
./testcase 127.0.0.1 2000 fill 0 20000      # 写 master 2 万个 key
redis-cli -p 2000 DBSIZE; redis-cli -p 2001 DBSIZE   # 两端应同为 20000
./testcase 127.0.0.1 2000 verify 0 20000    # master 逐 value 校验
./testcase 127.0.0.1 2001 verify 0 20000    # slave 逐 value 校验（镜像端到端）
# 镜像进程每秒打 [stat]：prod/cons 速率、backlog、drops、recs/s
```

## 设计取舍备忘

- **只镜像 ITER_IOVEC，不镜像 ITER_UBUF**：目标进程（kvstore master）读客户端
  命令恒经 `import_iovec` → IOVEC；`read()/recv()` 单缓冲属 UBUF 且不用。砍掉
  UBUF 分支既贴合现实，也避开其 verifier 失败路径（clang 会把未符号扩展的 RAX
  当拷贝长度传 `bpf_probe_read_user`，verifier 无法证明非负）。
- **失败-关闭**：`seq` 不连续（跨 CPU 乱序 / 丢采样）直接报错退出，不静默错序。
  生产同 repl-xdp 的方案：`taskset` 固定 master 到单 CPU 保顺序。
