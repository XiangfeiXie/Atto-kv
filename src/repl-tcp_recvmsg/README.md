# repl-tcp_recvmsg — kprobe 读侧镜像增量同步

把 kvstore **master 进程读到的客户端命令字节**镜像一份给 slave，使 slave 与
master 保持增量一致。挂在 master 侧的**旁路进程**，与 kvstore **零耦合**：
kprobe 只采样，内核 TCP 栈负责可靠传输，master 业务侧无任何改动。

组件不带任何参数，PID 和 `.bpf.o` 都自动定位，运行期不打印任何东西。
**唯一的外部要求：master 必须 `taskset` 钉单核**（理由见「顺序保证」）。

## 快速上手

```bash
# 0. 编译（产物在 build/ 下，用户态与 .o 同目录）
cd /home/xxf/9.1-kvstore/build
cmake .. && cmake --build . --target repl-tcp_recvmsg repl-tcp_recvmsg-bpf -j"$(nproc)"

# 1. master 必须钉单核；两个实例都 repl_role off ——
#    本组件走「抄客户端命令流」这条路，不经过项目内置的 PSYNC 复制。
taskset -c 5 ./kvstore /tmp/demo/master.conf &    # port 2000
./kvstore /tmp/demo/slave.conf &                  # port 2001，普通实例即可

# 2. 镜像：零参数启动，需 root
sudo ./repl-tcp_recvmsg

# 3. 验证
./testcase 127.0.0.1 2000 fill 0 50000
redis-cli -p 2000 DBSIZE; redis-cli -p 2001 DBSIZE   # 两端同为 50000
./testcase 127.0.0.1 2001 verify 0 50000             # 从库逐 value 校验
# Ctrl-C 退出，打一行汇总
```

demo 用的两个 conf：

```
# master.conf                # slave.conf
port 2000                    port 2001
network reactor              network proactor
maxclients 200                 maxclients 200
persistence none             persistence none
repl_role off                repl_role off
expire off                   expire off
```

> 端口和从机地址都写在 `repl-tcp_recvmsg.h` 里（`MASTER_PORT` / `SLAVE_IP` /
> `SLAVE_PORT`），当前默认值就是同机演示用的 `127.0.0.1` / `2001`。字符串宏经
> CMake `-D` 会粘参（与 repl-xdp 同坑），要改成跨机地址就直接改源码默认值。

### 命令行

```
repl-tcp_recvmsg [repl-tcp_recvmsg.bpf.o]
```

两个东西的定位方式：

- **master 进程**：按固定端口 `MASTER_PORT` 反查 —— 扫 `/proc/net/tcp{,6}` 拿监听
  该端口的 socket inode，再扫 `/proc/*/fd` 找持有它的进程。读别人的 fd 目录、加载
  BPF 都要 root，所以组件本身就必须 `sudo` 起。端口写死一个宏就够，不解析 conf。
- **`.bpf.o`**：取可执行文件同目录（`/proc/self/exe` 推导）——CMake 把两者产在
  同一个 `build/` 下，这个关系挪目录、换机器都成立。给个参数可以覆盖（调试用）。

## 原理

- **挂载点**：kprobe 挂 `tcp_recvmsg_locked` 入口、kretprobe 挂出口。
  `tcp_recvmsg_locked` 是 `read/recv/recvmsg` 与 io_uring 读的**共同收口**，所以
  本组件同时覆盖 reactor（`recv`）与 proactor（`io_uring_prep_recv`）。
- **一次 recv = 一条记录**：三引擎现在都是「`rbuf_reserve` 取一段连续空间 →
  单缓冲 recv」，`msg_iter` 恒为 `ITER_UBUF`。出口只需把那一段连续用户内存整个
  拷进 ring，没有多段 gather 可走。
- **按 tgid 过滤**：BPF 对全系统 TCP 读都触发（入口一次数组查找即退），只镜像
  `cfg` map 里那个 master 进程。master 作为服务器只读客户端连接 → 采到的就是
  客户端写入的命令字节流。
- **入口暂存（pending hash）**：kretprobe 拿不到入口参数，且 `msg_iter.ubuf` 在
  `tcp_recvmsg_locked` 执行中会被 `iov_iter_advance` 推进（出口读到的是
  base + ret）。所以入口把基址快照进按 tid 索引的 `pending`，出口取用后删除。
- **scratch 中转缓冲**：用户内存 → ring 必须经 per-CPU map 中转，因为
  `bpf_ringbuf_reserve` 的 size 必须是**编译期常量**（传运行期长度被验证器拒：
  `R2 is not a known constant`）。`MAX_REC=1024` 对齐 `RBUF_NODE_SIZE`；
  per-CPU map 的 value 超过约 32KB 创建会 `E2BIG`，所以不能靠加大 `MAX_REC` 买保险。
- **背压**：socket 非阻塞 + 用户态待发缓冲 `g_pend`，ring 永远即时消费。每轮先
  尽力 flush 再从机应答，**只发不读会把本端 rcvbuf 撑满 → 窗口 0 → 从机写不出
  应答就停摆**。退出前同样要边发边读地收尾（最多 2s）。

## 顺序保证（本组件的核心约束）

**master 必须 `taskset` 钉到单核。**

ring 是 per-CPU 的，libbpf 按 subring 下标顺序排空。master 单线程又被钉在一个核
上 → 所有记录只进那一个 subring → 到达序 == 排空序，顺序是**结构性**成立的，不是
概率。一旦跨核迁移，"先到的记录进了 CPU3 的 subring、后到的进了 CPU0"就会出现，
而排空是 0 排在 3 前面 —— 顺序就翻了，从库跟着错位。

BPF 给每条成功进 ring 的记录打**全局单调 seq**，用户态逐条核对，对不上立刻报错
退出并提示钉核。seq 不连续只有两种成因，且都意味着镜像流已不可信：

1. `drops` 计数 > 0 —— ring 满或记录越界，那一条永久没进 ring；
2. `drops` 计数 = 0 —— master 跨了 CPU。

**不做用户态有界重排**：重排只在"master 恰好在两次 poll 之间跨了核"时才走到，
是条几乎测不到的路径，等于往"一个字节都不许错"的数据面上放一段没被测过的代码；
而 `taskset -c N` 是零代码的。真到必须在多核上跑的那天再加重排，那时有真实触发
场景可以验证。

## 设计取舍

- **只放行 `ITER_UBUF`**：三引擎读路径已统一为单缓冲 `recv` / `io_uring_prep_recv`，
  UBUF 是唯一实际形态。gather/pvec/pipe 等形态在本项目里不出现，砍掉既贴合现实
  也少一层分支。`ITER_UBUF` 的值显式写成 `#define`（0），防止依赖头文件漂移。
- **失败-关闭**：`seq` 不连续、收到 `len=0` 的毒丸记录，都直接报错退出，不静默错序。
  `len=0` 只有 `bpf_probe_read_user` 失败才会发出 —— 正常路径 `ret > 0` 保证
  `len` 恒 `> 0`，所以它是可靠的"这条读没采到"信号。不这么做的话那条记录会凭空
  消失而 seq 照常递增，从库静默错位，是最坏的一种。
- **`on_event` 不判记录长度**：BPF 侧 `bpf_ringbuf_output` 预留的就是
  `16 + n`，libbpf 回调原样把 reserve 的长度当 `size` 传下来，故 `size` 恒等于
  `sizeof(*rec) + rec->len`，判界是恒假分支。真出事（长度错）只可能来自 `.bpf.o`
  与用户态 `struct tcp_rec` 不一致，那是 rec 头里 `seq` 会对不上号、被顺序核对
  当场拦下，判界挡不住也测不出。
- **不做周期打印**：附属组件不需要常驻输出，只在退出时打一行汇总（镜像字节与
  条数、待发峰值、退出时未送出的字节、丢采样）。

## 与 repl-xdp 的定位差异

| | repl-xdp | 本组件（repl-tcp_recvmsg） |
|---|---|---|
| 采样点 | 网卡入方向（XDP） | 内核 `tcp_recvmsg_locked`（kprobe） |
| 适用引擎 | 需 veth/netns | **reactor + proactor 通吃** |
| 网络环境 | veth + netns + XDP | 无特殊要求（lo 即可） |
| 采样内容 | TCP 负载（需按 seq 重组） | 应用读到的字节（天然按序） |
| 对 master 的要求 | 无（用户态做重组） | **必须 `taskset` 钉单核** |
