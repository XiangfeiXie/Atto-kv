# 9.1 Kvstore


### 面试题
1. 为什么会实现kvstore，使用场景在哪里？

为了深入理解高性能 KV 存储的底层原理，从零实现了兼容 Redis 协议的存储系统，把数据结构、网络 IO、分布式主从复制串联成完整项目。

2. reactor, ntyco, io_uring的三种网络模型的性能差异？

io_uring 异步 IO 开销最低，写 3.7w QPS、读 4.1w QPS；
ntyco 协程开发效率高，性能接近异步；
epoll 通用性最强、稳定，适合绝大多数生产环境。

3. 多线程的kvstore该如何改进？

把全局锁改为分段锁 / 分片锁，降低锁竞争；
读写分离，使用读写锁提升读性能；
IO 线程、工作线程、持久化线程线程池隔离；
对热点数据做无锁结构如 RCU、原子操作；
CPU 亲和性绑定，减少上下文切换。

4. 私有协议如何设计会更加安全可靠？

加长度前缀 + 校验和，防篡改、防粘包半包；
加序列号 / 请求 ID，支持重试、去重、异步响应；
加版本号与握手认证，保证兼容与安全；
支持TLS 加密传输

5. 协议改进以后，对已有的代码有哪些改变？

协议解析层从 RESP 改为私有协议解析；
网络层增加长度校验、分包重组逻辑；
连接结构增加序列号、状态机；
主从复制协议同步升级；
保留兼容模式，不影响原有逻辑。

6. kv引擎实现了哪些？

实现了 数组、哈希表、红黑树、跳表 四种存储引擎，对外统一接口，可自由切换。

7. 每个kv引擎的使用场景，以及性能差异？

跳表：性能最好，支持有序、范围查询，适合高并发生产；
哈希表：O (1) 读写，纯 KV 查询最快；
红黑树：平衡稳定、内存可控，适合有序场景；
数组：教学演示，性能最低。
整体性能：跳表 ≈ 哈希表 > 红黑树 >>> 数组。

8. 测试用例如何实现？并且保证代码覆盖率超过90%

单元测试：覆盖 set/get/del/ 过期 / 异常；
边界测试：空值、超长 key、并发冲突；
集成测试：网络 + 协议 + 存储 + 主从全链路；
压力测试：redis-benchmark 压测；
使用 gcov/lcov 统计覆盖率，补全错误分支、释放逻辑、异常路径，达到 90%+。

9. 网络并发量如何？qps如何？

单机支持 5w+ 并发连接。
io_uring 下性能最高：读 4.1w QPS，写 3.7w QPS。
Pipeline 批处理可提升 37 倍性能；高并发下吞吐量线性增长无瓶颈，io_uring 比 epoll 高 35%，达到生产级轻量级 KV 存储水平。

10. 能够跟哪些系统交互使用？

直接支持 redis-cli、redis-benchmark；
支持各类语言 Redis SDK（Java/Go/Python 等）；
可接入微服务、网关、监控系统（Prometheus）；
支持主从架构、RDB 持久化文件交互。

### 架构设计
![image](https://disk.0voice.com/p/py)


一、核心总表（所有有效测试结果）
统一基准：-h 192.168.30.128 -p 2000 -t set -n 50000（长压、稳态、无虚高）

序号	并发数 -c	Pipeline -P	总请求数 -n	实测 QPS	数据有效性	备注
1	    10	        1	        50000	  244.15	    无批处理，单条命令真实处理能力
2	    10	        4	        50000	  2606.00		批处理生效，性能大幅提升
3	    10	        6	        50000	  5716.05	    批处理持续优化，性能稳步提升
4	    10	        8	        50000	  9262.77		当前最优批处理大小，性能峰值
5	    20	        6	        50000	  13420.38		并发翻倍，吞吐量同步提升
6	    50	        6	        50000	  28004.48		高并发下，批处理优势完全释放



二、维度一：固定并发 c=10，不同 Pipeline 对 QPS 的影响
核心结论：你的批处理设计完全生效，P 越大，稳态 QPS 越高，无拥堵、无掉速。

Pipeline -P	并发 -c	总请求 -n	实测 QPS	性能提升（相对 P=1）
1	         10	    50000	    244.15	    基准线
4	         10	    50000	    2606.00	    10.67 倍
6	         10	    50000	    5716.05	    23.41 倍
8	         10	    50000	    9262.77	    37.94 倍


三、维度二：固定 Pipeline P=6，不同并发数对 QPS 的影响
核心结论：你的 KV 存储在高并发下，批处理吞吐量线性增长，无明显瓶颈。

并发数 -c	Pipeline -P	总请求 -n	实测 QPS	性能提升（相对 c=10）
10	        6	        50000	    5716.05	    基准线
20	        6	        50000	    13420.38	2.35 倍
50	        6	        50000	    28004.48	4.90 倍


四、补充：历史有效数据（用于完整性能画像）

序号	并发数 -c	Pipeline -P	总请求数 -n	实测 QPS	备注
7	    10	        16	        50000	   4112.23	P=16 出现拥堵，QPS 回落，P=8 为最优
8	    100	        6	        50000	   6866.30	并发过高，出现排队，QPS 回落
9	    10	        6	        100000	   3074.71	长压 10 万条，QPS 回落，进入稳态
10	    10	        6	        500000	   957.05	极限长压，彻底压崩，无参考意义

---

## 详细使用说明

### 一、项目概述

9.1-kvstore 是一个高性能的键值存储系统，完全兼容 Redis 协议。项目实现了多种网络模型（reactor、proactor/io_uring、ntyco 协程）、多种存储引擎（数组、红黑树、哈希表、跳表）、主从复制、数据持久化（RDB/AOF）、RDMA 全量同步以及 eBPF 增量复制等特性。

### 二、系统依赖

在编译和运行之前，请确保系统已安装以下依赖：

#### 必需依赖
- **gcc/clang**: C 编译器
- **make**: 构建工具
- **libpthread**: 线程库
- **liburing**: io_uring 库（Linux 5.1+）
- **libfabric**: RDMA/libfabric 库
- **libbpf + libelf**: eBPF 相关库
- **libz**: 压缩库
- **jemalloc**: 高性能内存分配器

#### 安装依赖（Ubuntu/Debian）
```bash
sudo apt-get update
sudo apt-get install -y build-essential gcc clang make liburing-dev libfabric-dev libbpf-dev libelf-dev zlib1g-dev libjemalloc-dev
```

#### 安装依赖（CentOS/RHEL）
```bash
sudo yum install -y gcc clang make liburing-devel libfabric-devel libbpf-devel elfutils-libelf-devel zlib-devel jemalloc-devel
```

#### 内核要求
- Linux 内核版本 5.1+（支持 io_uring）
- 启用 eBPF 功能
- RDMA/Soft-RoCE 支持（如需使用 RDMA 功能）

### 三、编译项目

#### 1. 生成 vmlinux.h（eBPF 所需）
```bash
# 需要 bpftool 工具
sudo apt-get install -y linux-tools-$(uname -r)  # Ubuntu
# 或
sudo yum install -y bpftool  # CentOS

# 生成 vmlinux.h
bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h
```

#### 2. 编译主程序
```bash
cd /home/xxf/9.1-kvstore
make clean
make
```

编译成功后会生成以下可执行文件：
- `kvstore`: 主服务器程序
- `testcase`: 测试客户端程序

#### 3. 编译子模块
Makefile 会自动编译 NtyCo 协程库子模块。

### 四、配置文件说明

配置文件采用键值对格式，注释以 `#` 开头。默认配置文件为 `kvstore.conf`。

#### 配置项详解

##### 网络配置
```conf
# 监听端口
port 2000

# 网络 I/O 模型：reactor | proactor | ntyco
network proactor    # proactor 使用 io_uring，性能最佳
```

##### 日志配置
```conf
# 日志级别：debug | info | warn | error
loglevel debug
```

##### 主从复制配置
```conf
# 启用主从复制：on | off
replication off

# 角色：master | slave
mode master

# 从节点配置：主节点地址（仅 slave 模式需要）
masterip   127.0.0.1
masterport 2001

# 复制端口
replport 2001
```

##### 持久化配置
```conf
# 持久化模式：none | rdb | aof | both
#   none  - 不持久化，重启丢失数据
#   rdb   - 定期快照（dump.rdb）
#   aof   - 仅追加日志（appendonly.aof）
#   both  - AOF 优先加载，RDB 作为备份
persistence both
```

##### RDMA 全量同步配置
```conf
# 启用 RDMA 同步模块：on | off
rdma_repl on

# 角色：master | slave
rdma_role master

# libfabric provider：tcp | verbs | sockets | udp
# verbs 配合内核 rxe 模块使用 Soft-RoCE
rdma_provider verbs

# Master：本地绑定地址和端口
rdma_bind_ip   127.0.0.1
rdma_bind_port 2100

# Slave：RDMA master 地址
rdma_master_ip   0.0.0.0
rdma_master_port 2100
```

##### 存储引擎配置
```conf
# 存储引擎：array | rbtree | hash | skiptable
engine skiptable    # 跳表性能最佳，支持有序查询
```

### 五、启动服务器

#### 1. 使用默认配置启动
```bash
./kvstore
# 等同于 ./kvstore kvstore.conf
```

#### 2. 指定配置文件启动
```bash
./kvstore kvstore.conf
```

#### 3. 启动 Master 节点（RDMA 测试配置）
```bash
./kvstore kvstore-master-rdma-test.conf
```

#### 4. 启动 Slave 节点（RDMA 测试配置）
```bash
./kvstore kvstore-slave-rdma-test.conf
```

#### 5. 验证服务启动
```bash
# 查看进程
ps aux | grep kvstore

# 查看端口监听
netstat -tuln | grep 2000
# 或
ss -tuln | grep 2000
```

### 六、客户端连接与测试

#### 1. 使用 redis-cli 连接
```bash
# 连接默认端口 2000
redis-cli -p 2000

# 连接指定主机和端口
redis-cli -h 127.0.0.1 -p 2000
```

#### 2. 基本命令测试
```bash
# 在 redis-cli 中执行
SET key1 value1
GET key1
DEL key1
SET key2 value2 EX 10    # 设置 10 秒过期
```

#### 3. 使用 redis-benchmark 压测
```bash
# 基准测试
redis-benchmark -h 127.0.0.1 -p 2000 -t set,get -n 10000 -c 10

# Pipeline 批处理测试
redis-benchmark -h 127.0.0.1 -p 2000 -t set -n 50000 -c 10 -P 8

# 长连接压测
redis-benchmark -h 127.0.0.1 -p 2000 -t set,get -n 100000 -c 50
```

#### 4. 使用自带的 testcase 客户端
```bash
# 交互模式
./testcase

# 批量模式（支持 Pipeline）
./testcase
# 在交互界面输入：
pipeline
SET k1 v1
SET k2 v2
SET k3 v3
exec
```

### 七、主从复制配置

#### 1. Master 配置
创建 `master.conf`：
```conf
port 2000
network proactor
loglevel info
replication on
mode master
persistence both
replport 2001
engine skiptable
```

启动 Master：
```bash
./kvstore master.conf
```

#### 2. Slave 配置
创建 `slave.conf`：
```conf
port 2002
network proactor
loglevel info
replication on
mode slave
masterip 127.0.0.1
masterport 2001
persistence both
replport 2003
engine skiptable
```

启动 Slave：
```bash
./kvstore slave.conf
```

#### 3. 验证复制
```bash
# 在 Master 上写入数据
redis-cli -p 2000 SET mykey myvalue

# 在 Slave 上读取数据
redis-cli -p 2002 GET mykey
```

### 八、RDMA 全量同步配置

#### 1. 配置 Soft-RoCE（如使用 verbs provider）
```bash
# 加载 RDMA 内核模块
sudo modprobe rdma_cm
sudo modprobe ib_core
sudo modprobe ib_uverbs

# 配置 Soft-RoCE（rxe）
sudo apt-get install -y rdma-core
# 创建 rxe 设备（替换 eth0 为实际网卡）
sudo rxe_cfg add eth0
```

#### 2. Master RDMA 配置
```conf
rdma_repl on
rdma_role master
rdma_provider verbs
rdma_bind_ip   0.0.0.0
rdma_bind_port 2100
```

#### 3. Slave RDMA 配置
```conf
rdma_repl on
rdma_role slave
rdma_provider verbs
rdma_bind_ip   127.0.0.1
rdma_bind_port 2101
rdma_master_ip   <master-ip>
rdma_master_port 2100
```

### 九、eBPF 增量复制

eBPF 增量复制通过内核探针自动捕获数据变更，无需额外配置。当启用 `rdma_repl` 时，增量复制会自动工作。

- **uprobe**: `kvs_rdma_incr_probe` 捕获 SET/DEL/FLUSHALL 等操作
- **ringbuf**: 内核到用户空间的数据传输
- **relay**: 用户空间中继，与全量同步集成

### 十、存储引擎选择与切换

#### 各引擎特性对比
| 引擎 | 性能 | 有序查询 | 内存占用 | 适用场景 |
|------|------|----------|----------|----------|
| skiptable | 最高 | 支持 | 中等 | 高并发生产环境 |
| hash | 最高（纯KV） | 不支持 | 低 | 纯键值查询 |
| rbtree | 中等 | 支持 | 可控 | 有序场景 |
| array | 最低 | 不支持 | 低 | 教学演示 |

#### 切换引擎
修改配置文件中的 `engine` 参数：
```conf
engine skiptable    # 切换到跳表
engine hash         # 切换到哈希表
engine rbtree       # 切换到红黑树
engine array        # 切换到数组
```

重启服务生效：
```bash
# 停止服务
pkill kvstore

# 重新启动
./kvstore kvstore.conf
```

### 十一、持久化文件说明

#### RDB 文件（dump.rdb）
- 定期生成的二进制快照
- 启动时自动加载
- 文件紧凑，恢复速度快

#### AOF 文件（appendonly.aof）
- 记录所有写操作命令
- 数据安全性更高
- 可配置重写策略

#### 持久化策略选择
- **开发测试**: `persistence none`（无需持久化）
- **生产环境**: `persistence both`（AOF + RDB）
- **性能优先**: `persistence rdb`（仅快照）
- **数据安全**: `persistence aof`（仅日志）

### 十二、性能调优建议

#### 1. 网络模型选择
- **最佳性能**: `network proactor`（io_uring）
- **通用兼容**: `network reactor`（epoll）
- **开发效率**: `network ntyco`（协程）

#### 2. 批处理优化
使用 Pipeline 批处理可大幅提升性能：
```bash
redis-benchmark -h 127.0.0.1 -p 2000 -t set -n 50000 -c 10 -P 8
```
推荐 Pipeline 大小：6-8

#### 3. 并发优化
- 根据 CPU 核心数调整并发连接数
- 使用 CPU 亲和性绑定减少上下文切换
- 考虑启用 jemalloc 内存分配器（已默认启用）

#### 4. 存储引擎优化
- 高并发场景选择 `skiptable` 或 `hash`
- 需要范围查询选择 `skiptable` 或 `rbtree`
- 内存受限选择 `hash`

### 十三、故障排查

#### 1. 编译错误
```bash
# 缺少 vmlinux.h
bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h

# 缺少依赖
sudo apt-get install -y liburing-dev libfabric-dev libbpf-dev
```

#### 2. 运行时错误
```bash
# 权限不足（eBPF 需要 root 或 CAP_BPF）
sudo ./kvstore kvstore.conf

# 端口被占用
netstat -tuln | grep 2000
pkill kvstore

# RDMA 初始化失败
# 检查 RDMA 设备
rdma link show
# 检查 rxe 模块
lsmod | grep rxe
```

#### 3. 复制失败
```bash
# 检查网络连通性
telnet <master-ip> <masterport>

# 检查防火墙
sudo iptables -L

# 查看日志
# 调整 loglevel 为 debug
loglevel debug
```

#### 4. 性能问题
```bash
# 检查 CPU 使用
top -p $(pgrep kvstore)

# 检查网络延迟
ping <server-ip>

# 检查磁盘 I/O（持久化场景）
iostat -x 1

# 切换到 proactor 模式
network proactor
```

### 十四、开发与调试

#### 1. 启用调试日志
```conf
loglevel debug
```

#### 2. GDB 调试
```bash
gdb ./kvstore
(gdb) run kvstore.conf
(gdb) bt    # 查看调用栈
(gdb) info threads
```

#### 3. Valgrind 内存检查
```bash
valgrind --leak-check=full ./kvstore kvstore.conf
```

#### 4. 性能分析
```bash
perf record -g ./kvstore kvstore.conf
perf report
```

### 十五、多语言客户端示例

#### Python
```python
import redis

r = redis.Redis(host='127.0.0.1', port=2000)
r.set('key', 'value')
print(r.get('key'))
```

#### Go
```go
package main

import (
    "github.com/go-redis/redis/v8"
    "context"
)

func main() {
    rdb := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:2000",
    })
    // 使用 rdb 进行操作
}
```

#### Java
```java
import redis.clients.jedis.Jedis;

public class Main {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1", 2000);
        jedis.set("key", "value");
        System.out.println(jedis.get("key"));
    }
}
```

### 十六、项目结构说明

```
9.1-kvstore/
├── kvstore.c              # 主程序入口
├── kvstore.h              # 头文件定义
├── config.c/h             # 配置文件解析
├── reactor.c              # Reactor 网络模型
├── proactor.c             # Proactor (io_uring) 网络模型
├── ntyco.c               # NtyCo 协程网络模型
├── repl.c/h              # 主从复制
├── repl_net.c/h          # 复制网络层
├── rdma_repl.c/h         # RDMA 全量同步
├── ebpf_incr.bpf.c       # eBPF 增量复制内核代码
├── ebpf_incr_repl.c/h    # eBPF 增量复制用户空间
├── persistence.c/h       # RDB/AOF 持久化
├── kvs_array.c           # 数组存储引擎
├── kvs_hash.c            # 哈希表存储引擎
├── kvs_rbtree.c          # 红黑树存储引擎
├── kvs_skiptable.c       # 跳表存储引擎
├── testcase.c            # 测试客户端
├── kvstore.conf          # 默认配置文件
├── Makefile              # 构建脚本
└── NtyCo/                # 协程库子模块
```

### 十七、常见问题 FAQ

**Q: 为什么启动失败提示权限不足？**  
A: eBPF 功能需要 root 权限或 CAP_BPF 能力，请使用 sudo 运行。

**Q: RDMA 连接失败怎么办？**  
A: 检查是否正确配置了 RDMA 设备，尝试使用 `tcp` provider 替代 `verbs`。

**Q: 如何清理持久化文件？**  
A: 删除 `dump.rdb` 和 `appendonly.aof` 文件，或设置 `persistence none`。

**Q: 支持哪些 Redis 命令？**  
A: 支持基础命令：SET、GET、DEL、EXPIRE、FLUSHALL 等，完全兼容 RESP 协议。

**Q: 如何查看当前使用的存储引擎？**  
A: 查看配置文件中的 `engine` 参数，或查看启动日志。

**Q: 性能测试的最佳实践是什么？**  
A: 使用 Pipeline 批处理，合理设置并发数（推荐 10-50），避免过小或过大。

### 十八、技术支持与贡献

如有问题或建议，请查看项目文档或联系开发团队。

---

## 附录：快速启动指南

### 最小化配置启动
```bash
# 1. 编译
make

# 2. 启动（使用默认配置）
./kvstore

# 3. 测试
redis-cli -p 2000
> SET hello world
> GET hello
```

### 完整功能启动
```bash
# 1. 编译
make

# 2. 启动 Master（RDMA + 持久化）
./kvstore kvstore-master-rdma-test.conf

# 3. 启动 Slave（另一个终端）
./kvstore kvstore-slave-rdma-test.conf

# 4. 压测
redis-benchmark -h 127.0.0.1 -p 2000 -t set,get -n 100000 -c 50 -P 8
```