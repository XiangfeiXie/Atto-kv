# xfer — 传输速率测试组件（sendfile / rdma）

两条传输路各一个**自包含**的 `.c`，各出一个可执行文件，喂**同一个文件**，
只量"传输这一段能跑多快"：

- `build/xfer-sendfile` — TCP `sendfile(2)` 零拷贝
- `build/xfer-rdma` — RDMA 单 WR 整块直写

**定位**：这是做实验的地方，不是 kvstore 的一部分。不 include 项目任何头文件、
不链 jemalloc / io_uring / 引擎 / ntyco，也不生成 RDB。结论先在组件上拿到，
再决定要不要搬回 kvstore —— 所以这里可以比项目里跑得更纯、更极端。

## 原理

### xfer-sendfile.c

服务端 `open` 文件 → `accept` → `sendfile()` 循环把整个文件推完 → `shutdown`
等对端收完 → 关连接。客户端一直 `recv` 到 EOF（对端关闭即结束），默认落到 `/tmp`。
**没有握手、没有协议头**，协议就是"裸字节 + 关闭"。

### xfer-rdma.c

客户端注册一块接收缓冲，把 `{addr, rkey}` 发给服务端；服务端把整个文件作为
**一个 WR**（`IBV_WR_RDMA_WRITE_WITH_IMM`，`imm_data` = 字节数）直写过去；
客户端等到那条 IMM 就结束。

没有分块、没有滑动窗口、没有 ACK —— RC 可靠有序 + 收发缓冲都是固定的一大块，
本来就不需要。收发缓冲固定 512 MiB（`RDMA_BUF_SIZE`），也就是可传文件的上限。

**顺序要点**：连接就绪（accept + 收到凭证）之后才读文件。读 158 MB 是百毫秒级，
若先读后监听，对端的 REQ 落在监听就绪之前会被丢、双方挂死。

### 计时口径：三条线统一到"对端已经拿到"

| 线 | 停表在哪 | 含义 |
|---|---|---|
| `xfer-rdma` | `post_send` → send CQE | 对端已 ACK，数据**确已写进对端内存** |
| `xfer-sendfile` | `sendfile()` 循环 → `shutdown(SHUT_WR)` → 等到对端 FIN | 对端**已把数据全部读走** |
| `iperf3` | 接收端 read 计数 / 墙钟 | 对端已读走 |

sendfile 这一侧需要特别处理：阻塞 socket 上 `sendfile()` 返回只表示字节进了
**本端**发送缓冲，末段还在途。不等它就停表，量到的是"本端交出去有多快"，比
iperf3 的接收端口径虚高 —— 本机实测虚高 2~5%（`tail=` 字段就是这个修正量，
≈ 收发缓冲之和 ÷ 速率，是个与文件大小无关的常数，文件越大占比越小）。

做法是循环结束后 `shutdown(SHUT_WR)`：FIN 排在这批数据之后，客户端收到 EOF 就
`close`，FIN 回来时本端 `recv` 返回 0 = 数据确实被读完了。**协议一个字没改。**

两侧现在测的是同一件事，可以互相校验：实测两端数字相差 0.1~1.2%。

> 仍有的一层固有差别（不该抹平）：RDMA 的 CQE 只到"对端网卡写进内存"，不需要
> 对端 CPU 参与；sendfile/iperf3 到"对端应用把数据从 socket 缓冲读走"，含对端
> 一次拷贝和系统调用。这是 TCP 与 RDMA 的本质差别。

## 使用方法

### 1. 编译

```bash
cd /home/xxf/9.1-kvstore/build
cmake .. && make xfer-sendfile xfer-rdma -j"$(nproc)"
```

### 2. 备一份文件

```bash
head -c 158400008 /dev/urandom > /tmp/x.rdb   # 与 kvstore 150000×1024B 快照同尺寸
```

内容是无关的（纯传输计时与内容无关），但**别用稀疏文件** —— `dd seek=` 造出来
的是空洞，sendfile 会走快路，数不可信。用 `du` 对一下 apparent size 与实占。

### 3. 跑 sendfile

```bash
./build/xfer-sendfile server /tmp/x.rdb 2100 &
./build/xfer-sendfile client 192.168.30.137 2100              # 落到 /tmp/xfer_<pid>.rdb
./build/xfer-sendfile client 192.168.30.137 2100 --discard    # 关键对照：只收不落盘
```

### 4. 跑 rdma

```bash
./build/xfer-rdma server /tmp/x.rdb 2200 &
./build/xfer-rdma client 192.168.30.137 2200 --discard
```

两个组件都监听端口，**串行跑，不要同时起**。

### 5. 读数

```
[sendfile] xfer-end: bytes=... xfer=...ms rate=...MB/s | tail=...ms
[sendfile] recv-end: bytes=... wait=...ms rate=...MB/s [ (discard)]
[rdma]     xfer-end: bytes=... xfer=...ms rate=...MB/s | prep: conn=...ms read=...ms
[rdma]     recv-end: bytes=... wait=...ms [ (discard)]
```

| 字段 | 含义 |
|---|---|
| `xfer` / `rate` | **纯传输区间**，口径见上表。十进制 MB/s |
| `tail` | 尾部同步那一段（把 sendfile 的口径从"发送缓冲"抬到"对端收完"的修正量） |
| `conn` | 从"可以监听"到"连接可用且对端凭证到手"。**含等对端来连的那段时间**（服务端先起、客户端迟到多久它就有多大）⇒ 不是传输成本 |
| `read` | 服务端把文件读进内存（rdma 要整块发，sendfile 不需要，故只有 rdma 有） |
| `wait` | 客户端墙钟。sendfile 侧与 `xfer` 同口径，用来互相校验；**rdma 侧不是** —— 服务端是"先连后读文件"，客户端的等待里夹着对端读文件的时间，只作参考 |

`--discard` **只**摘掉客户端的落盘，不动计时。

## 选项

| 选项 | 端 | 说明 |
|---|---|---|
| `--discard` | 客户端 | 收到即丢、不落盘。把"从端落盘"从测量里摘出去的**唯一旋钮** |

## 回环实测（158400008 B 同一份文件，三轮交替）

`192.168.30.137` 在本机是 `dev lo`，不出网卡。iperf3 一律**内存模式（不加
`-F`）**，`-t 5`。

| 轮次 | iperf3 | iperf3 `-Z` | sendfile 落盘 | sendfile `--discard` | rdma |
|---|---|---|---|---|---|
| 1 | 11.7 Gbit/s (1462 MB/s) | 20.0 (2500) | 228.42 MB/s | 1600.57 | 225.17 |
| 2 | 11.3 (1412) | 20.6 (2575) | 399.77 | 2017.69 | 203.36 |
| 3 | 11.1 (1387) | 20.6 (2575) | 440.85 | 1999.50 | 185.07 |
| （kvstore 里实测） | — | — | 48.65 | — | 206.86 |

**读法**：

1. **`-Z` 就是答案。** iperf3 加 `-Z`（MSG_ZEROCOPY，省掉"用户态→内核"那一次
   拷贝）从 1.4 GB/s 跳到 **2.5 GB/s，+80%**。sendfile 走的正是零拷贝路径
   （内核把 page cache 的页直接挂进 skb），所以它高于 iperf3 是**真的**，
   不是计时口径造成的。口径修正只有 2~5%（`tail=` 那一段）。
2. **口径统一后，sendfile 两端数字对上了**（1813.93 vs 1833.93、396.2 vs 395.9
   ……差 0.1~1.2%），说明这个数就是"对端收完"的真实速率。
3. **sendfile 的纯传输 1.6~2.0 GB/s ≈ 回环上限量级**（`-Z` 能到 2.5），落盘一加
   就掉到 228~441 且抖动大 ⇒ 瓶颈在**接收端落盘**，不在主端 `sendfile()`。
   kvstore 里那个 48.65 就是落在更慢的窗口里测的，不是代码性质。
4. **rdma 稳在 185~225 MB/s**，与 `ib_write_bw -R` 的 207.97 打平 ⇒ 顶到 `rxe0`
   （soft-RoCE）的软件上限，与 kvstore 里那个 206.86 一致。**比 sendfile 慢一个
   数量级**，但这是软件 rxe 的天花板，不代表真硬件 RDMA 的水平。

## 与 kvstore 的关系

这两个文件是**独立实现**，不是 `src/transport/{sendfile,rdma}.c` 的拷贝
—— 组件里刻意去掉了握手、协议头、落盘、加载这些实验无关的东西，所以数字更纯，
但也**不能**直接回头去改项目的传输代码。项目侧一行没动。

结论往项目搬的时候要重新对上两处：项目里 sendfile 服务端传输前要
`rdb_save_sync()`（158 MB 落盘，实测 2.8 s 量级），从端收完还要
`rdb_load_from_buf()`。这两段都在传输区间之外。

（试过把"服务端刚写完 158 MB、脏页还在途"这个条件也搬进来 —— `cp` 一份新文件
后立刻开传，实测 376/396 MB/s，**没有**变慢，所以项目那个 48.65 不是被 SAVE
的脏页拖的。这条存疑，先记在这。）
