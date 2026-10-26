# repl-tcp_recvmsg — kprobe 读侧镜像增量同步

把 kvstore **master 进程读到的客户端命令字节**镜像一份给 slave，使 slave 与
master 保持增量一致。挂在 master 侧的**旁路进程**，与 kvstore **零耦合**：
kprobe 只采样，内核 TCP 栈负责可靠传输，master 业务侧无任何改动。

组件不带任何参数，PID 和 `.bpf.o` 都自动定位，运行期不打印任何东西。
**唯一的外部要求：master 必须 `taskset` 钉单核**（理由见「顺序保证」）。
注意钉的是 **master**，不是本组件 —— 本组件自己钉不钉核不影响正确性（钉了只是
少抢 master 的核）；把组件钉到某个核**不能**替代钉 master。

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
> `SLAVE_PORT`），**当前默认值是跨机用的 `192.168.30.136` / `2000`**。上面那段
> 同机演示要把 `SLAVE_IP` / `SLAVE_PORT` 改回 `127.0.0.1` / `2001`；跨机时从机
> 在 `.136:2000` 起一个普通实例（`repl_role off`）即可。字符串宏经 CMake `-D`
> 会粘参（与 repl-xdp 同坑），要改就直接改源码默认值。

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
- **入口暂存（pending，per-CPU 单槽）**：kretprobe 拿不到入口参数，且
  `msg_iter.ubuf` 在 `tcp_recvmsg_locked` 执行中会被 `iov_iter_advance` 推进
  （出口读到的是 base + ret）。所以入口把基址写进 per-CPU 的 `pending` 槽，出口
  取用后清 `valid`。简化前是按 tid 索引的哈希（跨核也能对上，但每次读要
  update/lookup/delete）；现在省掉了哈希，代价是**入口/出口必须落在同一 CPU**，
  见「顺序保证」。
- **直接 reserve，无 scratch**：`bpf_ringbuf_reserve` 的 size 必须是**编译期
  常量**（传运行期长度被验证器拒：`R2 is not a known constant`），所以按
  `sizeof(struct tcp_rec) + MAX_REC` 预留一条记录，用户内存只拷一次进 ring。
  简化前是「用户内存 → per-CPU scratch → `bpf_ringbuf_output`」的两次拷贝。
  `MAX_REC=1024` 对齐 `RBUF_NODE_SIZE`（一次 recv 最多吃一个节点）；每条记录
  固定占 1040B，4MB ring 约可缓存 4000 条。
- **背压**：socket 非阻塞 + 用户态待发缓冲 `g_pend`，ring 永远即时消费。每轮先
  尽力 flush 再从机应答，**只发不读会把本端 rcvbuf 撑满 → 窗口 0 → 从机写不出
  应答就停摆**。退出前同样要边发边读地收尾（最多 2s）。

## 顺序保证（本组件的核心约束）

**master 必须 `taskset` 钉到单核；本组件进程不需要绑核。**

ring 是 per-CPU 的，libbpf 按 subring 下标顺序排空。master 单线程又被钉在一个核
上 → 所有记录只进那一个 subring → 到达序 == 排空序，顺序是**结构性**成立的，不是
概率。一旦跨核迁移，"先到的记录进了 CPU3 的 subring、后到的进了 CPU0"就会出现，
而排空是 0 排在 3 前面 —— 顺序就翻了，从库跟着错位。

简化后 seq 是 **per-CPU 自增**（去掉全局原子操作）。钉核时 per-CPU 值就是全局值；
跨核时新 CPU 上的计数器从 0 开始，用户态会看到
`seq 不连续：期望 N 实际 0/1/2…` 并 fail-closed 退出。此时 `drops=0` 说明不是
ring 满，而是 master 换了 CPU。

还有一条更隐蔽的：pending 是 **per-CPU 单槽**。一次 recv 若在 CPU A 进入、在 CPU B
返回，出口查的是 B 的槽：

- B 槽 `valid=0` → 这次读**静默不镜像，seq 也不递增**，用户态的顺序核对发现不了；
- B 槽残留上一条 `valid=1` → 出口会拿旧 `ub` 去读，镜像出错字节，而 seq 依然连续。

这两种静默错误**无法由组件自检发现**，只能靠钉核从结构上排除。所以「不绑核也能
跑」是错觉：没迁移时侥幸正确，迁移时可能报错，也可能悄悄错数据。

此外，per-CPU 单槽还隐含「**同一时刻只有一个目标 recv 在飞**」：当前三个引擎都是
单线程事件循环（AOF 线程不读客户端 socket），成立；若以后 master 改成多线程收
命令，即使钉在同一核上，多个线程也会互相覆盖这个槽。

**不做用户态有界重排**：重排只在"master 恰好在两次 poll 之间跨了核"时才走到，
是条几乎测不到的路径，等于往"一个字节都不许错"的数据面上放一段没被测过的代码；
而 `taskset -c N` 是零代码的。真到必须在多核上跑的那天再加重排，那时有真实触发
场景可以验证。

### 看到 `seq 不连续` 时先查谁没绑核

`drops=0` 且实际 seq 从 0 起，基本就是 master 跨了 CPU。确认钉的是 master：

```bash
grep Cpus_allowed_list /proc/$(pgrep -x kvstore)/status   # 应只有一个 CPU 编号
```

`taskset -c 3 ./repl-tcp_recvmsg` 钉的是组件，**不能**替代钉 master。

## 设计取舍

- **只放行 `ITER_UBUF`**：三引擎读路径已统一为单缓冲 `recv` / `io_uring_prep_recv`，
  UBUF 是唯一实际形态。gather/pvec/pipe 等形态在本项目里不出现，砍掉既贴合现实
  也少一层分支。`ITER_UBUF` 的值显式写成 `#define`（0），防止依赖头文件漂移。
- **失败-关闭**：`seq` 不连续、收到 `len=0` 的毒丸记录，都直接报错退出，不静默错序。
  `len=0` 只有 `bpf_probe_read_user` 失败才会发出 —— 正常路径 `ret > 0` 保证
  `len` 恒 `> 0`，所以它是可靠的"这条读没采到"信号。不这么做的话那条记录会凭空
  消失而 seq 照常递增，从库静默错位，是最坏的一种。它覆盖的是"记录到了但对不上
  号"和"记录里没数据"；per-CPU pending 跨核时的静默漏采不在自检范围内（见
  「顺序保证」），所以钉核是前提，不是优化。
- **`on_event` 的长度判界是恒假兜底**：BPF 侧固定 reserve
  `sizeof(*rec) + MAX_REC`（1040B），libbpf 回调把 reserve 长度原样作为 `size`
  传下来，故 `size` 恒为 1040；而 `rec->len ≤ MAX_REC`，所以
  `rec->len > size - sizeof(*rec)` 永不成立。保留这行只是为了 `.bpf.o` 与用户态
  `struct tcp_rec` 布局不一致时不至于越界读；镜像流本身的一致性仍由 `seq` 核对
  负责。
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
| 管道深度（P）上限 | **有**：单段负载 > `MAX_REC`(16384) 整段丢；回环上 `P×45B ≤ 16KB`（约 P≤360） | **无**：采样点在应用读之后，一次 recv ≤ `RBUF_NODE_SIZE`(1024) |

> 本组件没有 P 上限：客户端一次 `write` 再大，内核也只会按 master 的
> `RBUF_NODE_SIZE=1024` 一段段喂给 `recv`，组件按 recv 切片镜像，从库拼回原流。
> repl-xdp 的上限来自"按 TCP 段采样"，两者别混。
