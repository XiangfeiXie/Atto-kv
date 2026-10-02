#!/bin/bash
# =============================================================================
# bench_fullsync.sh — 全量同步前，往 master 灌数据
#
# 在项目根目录执行：
#   ./test/bench/bench_fullsync.sh
#
# 脚本只做一件事：灌 KEYS 个 key。传输速率不在这里测 —— kvstore 自己会打印，
# master 的 stdout 上会出现这一行（rate 就是传输速率，自己读）：
#   [sendfile] xfer-end: bytes=... xfer=...ms rate=...MB/s | prep: save=...ms
#   [rdma]     xfer-end: bytes=... xfer=...ms rate=...MB/s | prep: conn=...ms ser=...ms reg=...ms
# 两条线形状一样：前三个字段对齐，| 之后是传输前必须付的准备成本。
# 所以起 master 时 stdout 别丢进 /dev/null。
#
# 流程（顺序不能反）：
#   1. rm -f build/dump.rdb      不清会加载上一轮的数据，快照就不是你以为的那份
#   2. 起 master（用自己的 conf）
#   3. 跑本脚本灌数据
#   4. 再起 slave                早了它 PSYNC 到空库，之后 incr_sync off 的写
#                                根本不传播，填充的数据永远到不了从库
#
# 常量按需改。
# =============================================================================

PORT=2000
#1GB=1016803
#150000=151.1 MiB
KEYS=1016803
VSIZE=1024

# ---- 单条 key 的字节数（见 persist_rdb.c 条目布局）----
#   [klen:4][key:16][dlen:4][data:VSIZE][expire:8] = 32 + VSIZE
#   key 恒为 16 字符（下面的 "key:%012d"）
SNAPSHOT_BYTES=$((KEYS * (32 + VSIZE) + 8))

# 填充：awk 造 RESP 流 → redis-cli --pipe（不依赖 python）。
# 退出码不可信：--pipe 收尾那条 inline ECHO 会被 kvstore 拒掉（回 ERR invalid
# RESP protocol），redis-cli 因此返回非零，但数据是全部落进去了的，所以吞掉。
awk -v n="$KEYS" -v vs="$VSIZE" '
    BEGIN {
        for (j = 0; j < vs; j++) v = v sprintf("%c", 65 + (j % 26));
        for (i = 0; i < n; i++) {
            k = sprintf("key:%012d", i);
            printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, vs, v;
        }
    }' | timeout 300 redis-cli -p "$PORT" --pipe >/dev/null || true

got=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)

echo
echo "  填充完成    DBSIZE=$got / 期望 $KEYS"
echo "  预期快照    $KEYS×$((32 + VSIZE)) + 8 = $SNAPSHOT_BYTES B ≈ $(awk -v b="$SNAPSHOT_BYTES" 'BEGIN{printf "%.1f", b/1048576}') MiB"
echo "              拿这个数对 master 打的 bytes=，对不上说明传的不是这份数据"
