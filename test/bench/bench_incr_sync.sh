#!/bin/bash
# =============================================================================
# bench_incr_sync.sh — 项目自带增量同步（PSYNC）的 SET QPS
#
# 在项目根目录执行（主机 192.168.30.137；从机 192.168.30.136 需自己先起好）：
#   ./test/bench/bench_incr_sync.sh 40          # P=40
#
# 和 bench_tcp_recvmsg.sh 是同一套清场/等从机/收尾，唯一的区别是**没有组件
# 可起**：增量同步就在 kvstore 自己进程里，靠 repl_role master + incr_sync on
# 打开。所以主从两边都得用带复制的 conf，不能再像组件那两个脚本一样用
# kvstore-none.conf：
#   主机  conf/kvstore-master.conf
#   从机  conf/kvstore-slave.conf        （你手动起）
# 两个 conf 的 incr_sync 都得是 on，否则从机收完一次全量就终结、之后 master
# 的写根本不传播，测的就不是增量同步。脚本开头会查，不是 on 直接中止。
#
# 折损从哪来（跟组件那套是两种账，预期形状也不一样）：
#   传播在 master 的**命令路径上同步发生** —— kvs_journal_effect
#   （src/core/kvstore_dispatch.c:33）→ repl_backlog_append（memcpy 进 1MB 积压
#   环）+ repl_incr_propagate → repl_engine_append（塞进从库连接的发送缓冲，
#   真正的 write 由引擎事件循环做）。
#   即代价是**每条命令**付一次，不是"每次 recv"付一次。所以预期折损率随 P
#   **基本走平**，不像 repl-tcp_recvmsg / repl-xdp 那样被 pipeline 摊薄 ——
#   这是个可以拿数据检验的预期，真测出来明显随 P 下降就说明还有别的成本项。
#
# 脚本顺序：
#   1. 查两个 conf 的 incr_sync
#   2. 清场：杀掉残留的 kvstore、清掉 dump.rdb
#   3. 等从机就绪：轮询 PING，通了才往下走（从机由你手动起）
#   4. 起 master（绑核 1）
#   5. 跑 redis-benchmark（绑核 3）
#   6. 杀掉 master
#
# 命令数 n = 100000 × P，一次只测一个 P；多轮由你自己重复调用本脚本。
#
# 清 dump.rdb 是必须的：master 的 conf 是 persistence rdb，不清就会加载上一轮
# 的数据，启动耗时和 backlog 起点都不是你以为的那份。root 起的 master 写的
# dump.rdb 属主是 root，所以删它也要 sudo。
#
# master 的 stdout 不重定向、留在终端 —— 跑完要自己看那几行 [INCR-MASTER]：
# 有 `PSYNC …` 后面跟着 `fd=… streaming (backfill … -> …)`，才说明这一轮从机
# 进了流式、测的是增量同步。只有 `PSYNC` 没有 `streaming`，说明它走了全量，
# 数据不算数。这和另两个脚本打印组件退出汇总是同一个作用：唯一证据。
# （不重定向还有个实际原因：重定向到文件后 stdout 变块缓冲，最后 pkill 的
#   SIGTERM 可能让它来不及 flush，日志就没了。）
#
# sudo 一律走 `echo $SPW | sudo -S`（-S 从 stdin 读密码），不用 askpass 脚本。
# `-p ''` 是压掉 "[sudo] password for ..." 那行，免得混进压测输出。密码写在
# 下面的 SPW，按目标机器的实际密码改。密码错会响亮报错并中止，不会静默跑空。
# =============================================================================

P=$1
n=$((100000 * P))

PORT=2000
SLAVE_IP=192.168.30.136
WAIT=180                        # 等从机就绪的最长秒数
SPW=1234                        # sudo 密码
MASTER_CONF=conf/kvstore-master.conf
SLAVE_CONF=conf/kvstore-slave.conf

echo "=== P=$P  n=$n ==="

# incr_sync 不是 on 的话，从机收完一次全量就终结，master 之后的写不传播，
# 测出来会是一条"几乎没折损"的假曲线。宁可中止。
for c in "$MASTER_CONF" "$SLAVE_CONF"; do
    grep -q '^incr_sync on' "$c" || {
        echo "$c 里不是 'incr_sync on'，测的不会是增量同步，中止"
        exit 1
    }
done

echo "$SPW" | sudo -S -p '' pkill -x kvstore
sleep 0.5
echo "$SPW" | sudo -S -p '' rm -f dump.rdb

# 等从机就绪。从机只起一次、跨多轮一直在，所以除了第一轮，这里都是瞬间通过。
echo "等待从机 $SLAVE_IP:$PORT（先在那边用 conf/kvstore-slave.conf 起，最多 ${WAIT}s）..."
for _ in $(seq "$WAIT"); do
    redis-cli -h "$SLAVE_IP" -p "$PORT" PING >/dev/null 2>&1 && break
    sleep 1
done
redis-cli -h "$SLAVE_IP" -p "$PORT" PING >/dev/null 2>&1 || {
    echo "等不到从机 $SLAVE_IP:$PORT，中止"
    exit 1
}
echo "从机就绪"

echo "$SPW" | taskset -c 1 sudo -S -p '' build/kvstore "$MASTER_CONF" &
# 覆盖从机连上来发 PSYNC、master 回 +CONTINUE 进 streaming 这段。空库且
# offset=0 在 backlog 窗口内，走不到全量（全量要 fork 传输子进程，那就不止
# 这点时间了）；真走了全量，看日志里的 FULLRESYNC 自己把这里加长。
sleep 3

taskset -c 3 redis-benchmark -p "$PORT" -c 1 -n "$n" -P "$P" -t set

echo "$SPW" | sudo -S -p '' pkill -x kvstore
