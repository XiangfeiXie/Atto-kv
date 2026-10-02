#!/bin/bash
# =============================================================================
# bench_xdp.sh — repl-xdp 组件的 SET QPS
#
# 在项目根目录执行（主机 192.168.30.137；从机 192.168.30.136 需自己先起好）：
#   ./test/bench/bench_xdp.sh 40          # P=40
#
# 和 bench_tcp_recvmsg.sh 是同一套流程、同一套清场/等从机/收尾，只差组件那
# 一条命令：
#   repl-tcp_recvmsg  挂 kprobe，采 master 的 socket 读
#   repl-xdp          挂 XDP，采 master 网卡入方向的包
# 所以组件多带一个参数（BPF 目标文件），其余一字不改。
#
# 主从都用 conf/kvstore-none.conf：repl_role off → 不挂复制回调、不走 PSYNC
# 双写，增量同步完全由 XDP 镜像完成。全程只有组件这一条增量同步。
#
# ⚠ 一处本质差别，别踩坑：XDP 挂在 ens33 上，只看得见**从 ens33 进来的**包。
#   本脚本这一行 redis-benchmark 在本机连 127.0.0.1:2000，走的是回环，压根
#   不过 ens33 —— XDP 采样为 0，这一轮等于白跑。判据在组件退出汇总那行：
#   会打"镜像 0 字节"，看到 0 就是没采到，不是组件坏了。
#   要真测，压测客户端必须从对侧发起，二选一：
#     · 在从机 192.168.30.136 上跑 redis-benchmark -h 192.168.30.137 -p 2000 …
#     · 或按 src/repl-xdp/README.md 起 netns + veth 那套，客户端从对侧 netns 发起
#   先按本机回环这么放着，按你的实际拓扑改那一行。
#
# 脚本顺序：
#   1. 清场：杀掉残留的 kvstore 与 repl-xdp
#   2. 等从机就绪：轮询 PING，通了才往下走（从机由你手动起）
#   3. 起 master（绑核 1）
#   4. 起组件（绑核 0）
#   5. 跑 redis-benchmark（绑核 3）
#   6. 把起过的东西杀掉
#
# 命令数 n = 100000 × P，一次只测一个 P；多轮由你自己重复调用本脚本。
#
# sudo 一律走 `echo $SPW | sudo -S`（-S 从 stdin 读密码），不用 askpass 脚本
# —— 脚本之后要挪到从机 192.168.30.136 上跑，那边没有 /tmp/askpass.sh。
# `-p ''` 是压掉 "[sudo] password for ..." 那行，免得混进压测输出。密码写在
# 下面的 SPW，按目标机器的实际密码改。密码错会响亮报错并中止，不会静默跑空。
#
# 全部 sudo 起：组件的 XDP 加载需要 root（CAP_BPF + CAP_NET_ADMIN）。
# 注意 root 起的 master 会把 dump.rdb 落在项目根目录且属主是 root，之后非
# sudo 的实例就写不动它了。
#
# 组件不用 prlimit 抬 memlock：kernel 6.8 起 BPF 内存走 memcg 记账、不占
# RLIMIT_MEMLOCK，且 root 的 memlock 上限本来就有 483MB，4MB 的 ring 绰绰有余。
#
# 杀组件用 `pkill -x repl-xdp`：8 个字符，没到内核 comm 的 15 字符截断线，
# 不用像 repl-tcp_recvmsg 那样去掉尾巴的 g。
# =============================================================================

P=$1
n=$((100000 * P))

PORT=2000
SLAVE_IP=192.168.30.136
WAIT=180                        # 等从机就绪的最长秒数
SPW=1234                        # sudo 密码

echo "=== P=$P  n=$n ==="

echo "$SPW" | sudo -S -p '' pkill -x kvstore
echo "$SPW" | sudo -S -p '' pkill -x repl-xdp          # 上轮没杀干净会污染这轮
sleep 0.5

# 等从机就绪。放在起 master 之前，好让 master 从启动到开压的间隔固定
# （下面 1s + 1s），不因为你手起从机的快慢而飘。
# 从机只起一次、跨多轮一直在，所以除了第一轮，这里都是瞬间通过。
echo "等待从机 $SLAVE_IP:$PORT（先在那边用 kvstore-none.conf 起，最多 ${WAIT}s）..."
for _ in $(seq "$WAIT"); do
    redis-cli -h "$SLAVE_IP" -p "$PORT" PING >/dev/null 2>&1 && break
    sleep 1
done
redis-cli -h "$SLAVE_IP" -p "$PORT" PING >/dev/null 2>&1 || {
    echo "等不到从机 $SLAVE_IP:$PORT，中止"
    exit 1
}
echo "从机就绪"

echo "$SPW" | taskset -c 1 sudo -S -p '' build/kvstore conf/kvstore-none.conf &
sleep 1

# 零参数启动：.bpf.o 由组件按 /proc/self/exe 取可执行文件同目录（与 repl-tcp_recvmsg 一致）。
# 不吞 stderr：组件连不上从机会立刻退出，报错就在这里，看得见才查得动。
echo "$SPW" | taskset -c 0 sudo -S -p '' build/repl-xdp &
sleep 1
pgrep -x repl-xdp >/dev/null || {
    echo "组件没起来，中止"
    echo "$SPW" | sudo -S -p '' pkill -x kvstore
    exit 1
}

taskset -c 3 redis-benchmark -p "$PORT" -c 1 -n "$n" -P "$P" -t set

echo "$SPW" | sudo -S -p '' pkill -x kvstore
echo "$SPW" | sudo -S -p '' pkill -INT -x repl-xdp     # SIGINT 才走收尾
# 等组件把收尾跑完再让脚本退出：它收到 SIGINT 后要先"边发边读"地把待发缓冲
# 冲干净才打那行退出汇总。脚本一退，这段就被打断，汇总也就没了 —— 那行是
# 这次压测里"组件到底镜像了多少、丢没丢采样"的唯一证据。
sleep 3
