#!/bin/bash
# =============================================================================
# fullsync_bench.sh — 全量同步速率基准测试
#
# 用法:
#   主机: sudo ./build/kvstore conf/kvstore-master.conf
#   从机: sudo ./build/kvstore conf/kvstore-slave.conf
#   本机: ./tools/fullsync_bench.sh -m 192.168.30.135 -s 192.168.30.136 \\
#              -k 100000 -v 512
#
# 速率计算:
#   Master send  = total_bytes / 发送耗时 (序列化 + fi_send + fi_shutdown)
#   Slave  recv  = total_bytes / 接收耗时 (header解析→接收循环→rebuild)
#   Slave  e2e   = total_bytes / 端到端耗时 (函数入口→rebuild，含连接建立)
#
#   整体传输速率建议取 Master send，它测量纯数据平面的序列化+发送时间，
#   不包含 RDMA CM 握手等一次性开销。
# =============================================================================

set -euo pipefail

# 默认参数
MASTER_HOST="127.0.0.1"
SLAVE_HOST="127.0.0.1"
NKEYS=100000
VSIZE=512
MASTER_PORT=2000
SLAVE_PORT=2000

usage() {
    echo "Usage: $0 -m MASTER_IP [-s SLAVE_IP] [-k NKEYS] [-v VSIZE]"
    echo "  -m  Master IP (required)"
    echo "  -s  Slave IP  (default: same as master)"
    echo "  -k  Key count (default: 100000)"
    echo "  -v  Value size in bytes (default: 512)"
    exit 1
}

while getopts "m:s:k:v:h" opt; do
    case $opt in
        m) MASTER_HOST="$OPTARG" ;;
        s) SLAVE_HOST="$OPTARG" ;;
        k) NKEYS="$OPTARG" ;;
        v) VSIZE="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

echo "============================================"
echo " Full Sync Benchmark"
echo "============================================"
echo " Master       : $MASTER_HOST:$MASTER_PORT"
echo " Slave        : $SLAVE_HOST:$SLAVE_PORT"
echo " Keys         : $NKEYS"
echo " Value size   : $VSIZE bytes"
echo " Est. data    : ~$(( NKEYS * (VSIZE + 30) / 1024 / 1024 )) MB"
echo "============================================"

# ── Step 1: Populate master ──────────────────────────────────────────
echo ""
echo "── Step 1: Populating master ──"

VAL=$(python3 -c "print('x'*${VSIZE})")
# 生成 RESP 到临时文件
RESP_FILE=$(mktemp)
for i in $(seq 1 $NKEYS); do
    printf '*3\r\n$3\r\nSET\r\n$7\r\nk%05d\r\n$%d\r\n%s\r\n' "$i" "$VSIZE" "$VAL"
done > "$RESP_FILE"

RESP_SIZE=$(wc -c < "$RESP_FILE")
echo "  RESP file: $(( RESP_SIZE / 1024 / 1024 )) MB"

START=$(date +%s%3N)
cat "$RESP_FILE" | redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" --pipe 2>/dev/null
END=$(date +%s%3N)
POP_MS=$(( END - START ))
echo "  Populated in ${POP_MS}ms ($(( RESP_SIZE * 1000 / POP_MS / 1024 / 1024 )) MB/s RESP)"

rm -f "$RESP_FILE"

MC=$(redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" DBSIZE 2>/dev/null | tail -1 | tr -d '\r')
echo "  Master DBSIZE: ${MC:-0}"

# ── Step 2: Restart slave (触发全量同步) ─────────────────────────────
echo ""
echo "── Step 2: Restarting slave ──"

# 杀旧 slave
ssh "$SLAVE_HOST" "sudo pkill -9 kvstore 2>/dev/null; sleep 1" || true

# 清理旧日志
ssh "$SLAVE_HOST" "sudo rm -f /tmp/slave_fs_bench.log"

# 启动 slave，日志重定向
ssh "$SLAVE_HOST" "cd ~/9.1-kvstore && sudo ./build/kvstore conf/kvstore-slave.conf > /tmp/slave_fs_bench.log 2>&1 &"

echo "  Slave started, waiting for full sync..."

# ── Step 3: 轮询等待同步完成 ────────────────────────────────────────
TIMEOUT=300
POLL_START=$(date +%s)
while true; do
    SC=$(redis-cli -h "$SLAVE_HOST" -p "$SLAVE_PORT" DBSIZE 2>/dev/null | tail -1 | tr -d '\r' || echo "0")
    if [ -n "$SC" ] && [ "$SC" -gt 0 ] 2>/dev/null; then
        ELAPSED=$(($(date +%s) - POLL_START))
        echo "  Done! Slave has $SC keys (waited ${ELAPSED}s)"
        break
    fi
    if [ $(($(date +%s) - POLL_START)) -gt $TIMEOUT ]; then
        echo "  Timeout after ${TIMEOUT}s"
        break
    fi
    sleep 3
done

# ── Step 4: 收集日志并分析 ───────────────────────────────────────────
echo ""
echo "── Step 4: Results ──"
echo ""

# Master 侧: 需要从主机拉日志
# 简化: 直接从本机 master 日志提取
MASTER_LOG=$(ssh "$MASTER_HOST" "sudo cat /tmp/*.log 2>/dev/null | grep 'CHILD.*complete' | tail -1" 2>/dev/null || echo "")

# Slave 侧: 从从机拉日志
SLAVE_LOG=$(ssh "$SLAVE_HOST" "sudo cat /tmp/slave_fs_bench.log" 2>/dev/null || echo "")

echo "====== Master (send) ======"
if [ -n "$MASTER_LOG" ]; then
    echo "$MASTER_LOG" | while read line; do echo "  $line"; done
else
    echo "  (could not retrieve master log)"
fi

echo ""
echo "====== Slave (recv) ======"
echo "$SLAVE_LOG" | grep -E 'Recv|End-to-end|Header:|Total:' | while read line; do
    echo "  $line"
done

echo ""
echo "====== Rate Summary ======"
echo "  Master send  : 序列化+网络发送速率 (纯数据平面)"
echo "  Slave  recv  : 接收+解析+dict_add速率 (纯数据路径)"
echo "  Slave  e2e   : 从connect到rebuild全程速率 (含连接建立)"
echo ""
echo "  整体传输速率 → 取 Master send 的 MB/s"
echo "  它测量的是实际数据传输(序列化+网络)时间,"
echo "  不含一次性开销(RDMA CM握手/连接建立/rebuild_encodings)。"
