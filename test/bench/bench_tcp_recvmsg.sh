#!/bin/bash
# =============================================================================
# bench_tcp_recvmsg.sh — repl-tcp_recvmsg 组件的 SET QPS
#
# 在项目根目录执行（主机 192.168.30.137；从机 192.168.30.136 需自己先起好）：
#   ./test/bench/bench_tcp_recvmsg.sh 40          # P=40
#
# 主从都用 conf/kvstore-none.conf：repl_role off → 不挂复制回调、不 fork、
# 不监听复制端口，主循环零复制开销。全程只有组件这一条增量同步。
#
# 脚本顺序：
#   1. 清场：杀掉残留的 kvstore 与 repl-tcp_recvmsg
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
# 全部 sudo 起：组件的 eBPF 加载与 /proc/<pid>/fd 反查 master 都需要 root，
# master 跟着一起 sudo 只为两边权限一致。注意 root 起的 master 会把 dump.rdb
# 落在项目根目录且属主是 root，之后非 sudo 的实例就写不动它了。
#
# 杀组件用 `pkill -x repl-tcp_recvms`（少个 g），不是笔误：pkill -x 比的是
# /proc/<pid>/comm，内核把它截到 15 字符，而 "repl-tcp_recvmsg" 是 16 个字符
# —— 写全名 pkill 会警告"pattern longer than 15 characters"并匹配到 0 个进程，
# 静默什么都杀不掉。
# =============================================================================

P=$1
n=$((100000 * P))

PORT=2000
SLAVE_IP=192.168.30.136
WAIT=180                        # 等从机就绪的最长秒数
SPW=1234                        # sudo 密码

echo "=== P=$P  n=$n ==="

echo "$SPW" | sudo -S -p '' pkill -x kvstore
echo "$SPW" | sudo -S -p '' pkill -x repl-tcp_recvms   # 上轮没杀干净会污染这轮
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

# 不吞 stderr：组件连不上从机会立刻退出，报错就在这里，看得见才查得动
echo "$SPW" | taskset -c 0 sudo -S -p '' build/repl-tcp_recvmsg &
sleep 1
pgrep -x repl-tcp_recvms >/dev/null || {
    echo "组件没起来，中止"
    echo "$SPW" | sudo -S -p '' pkill -x kvstore
    exit 1
}

taskset -c 3 redis-benchmark -p "$PORT" -c 1 -n "$n" -P "$P" -t set

echo "$SPW" | sudo -S -p '' pkill -x kvstore
echo "$SPW" | sudo -S -p '' pkill -INT -x repl-tcp_recvms   # SIGINT 才走收尾
# 等组件把收尾跑完再让脚本退出：它收到 SIGINT 后要先"边发边读"地把待发缓冲
# 冲干净（最多 2s）才打那行退出汇总。脚本一退，这段就被打断，汇总也就没了
# —— 那行是这次压测里"组件到底镜像了多少、丢没丢采样"的唯一证据。
sleep 3
