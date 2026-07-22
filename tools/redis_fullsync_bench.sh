#!/bin/bash
# =============================================================================
# redis_fullsync_bench.sh — Redis 原生全量同步速率测试
#
# 用法: ./redis_fullsync_bench.sh [-k 50000] [-v 512]
#
# 输出: RDB 文件大小、同步耗时、传输速率 (MB/s)
#
# 原理:
#   1. Master 写入 N 条 key → 数据落内存
#   2. 测量 RDB 文件大小 (BGSAVE 生成)
#   3. 启动 Replica → Master fork 子进程写 RDB → 发送给 Replica → 加载
#   4. 轮询 replica INFO replication 的 master_sync_in_progress
#   5. 计算: 速率 = RDB 文件大小 / (同步完成时间 - Replica 启动时间)
# =============================================================================
set -euo pipefail

NKEYS=50000
VSIZE=512
MASTER_PORT=6379
REPLICA_PORT=6380
TIMEOUT=120

usage() {
    echo "Usage: $0 [-k NKEYS] [-v VSIZE]"
    echo "  -k  Key count (default: 50000)"
    echo "  -v  Value size in bytes (default: 512)"
    exit 1
}
while getopts "k:v:h" opt; do
    case $opt in k) NKEYS="$OPTARG" ;; v) VSIZE="$OPTARG" ;; h) usage ;; esac
done

cleanup() {
    if [ -n "${MPID:-}" ]; then kill $MPID 2>/dev/null; fi
    if [ -n "${RPID:-}" ]; then kill $RPID 2>/dev/null; fi
    rm -f /tmp/redis_master.conf /tmp/redis_replica.conf /tmp/redis_rdb.rdb /tmp/redis_resp.txt
}
trap cleanup EXIT

echo "============================================"
echo " Redis Fullsync Benchmark"
echo "============================================"
echo " Keys:      $NKEYS"
echo " Value:     $VSIZE bytes"
echo " Est.data:  ~$(( NKEYS * (VSIZE + 30) / 1024 / 1024 )) MB"
echo "============================================"

cd /tmp

# ── 0. Cleanup from previous run ──────────────────────────────────────
pkill -9 redis-server 2>/dev/null || true
sleep 1
rm -f /tmp/dump.rdb /tmp/redis_master.log /tmp/redis_replica.log /tmp/redis_resp.txt

# ── 1. Configs ─────────────────────────────────────────────────────────
cat > redis_master.conf << EOF
port $MASTER_PORT
bind 127.0.0.1
daemonize no
loglevel notice
logfile /tmp/redis_master.log
save ""
repl-diskless-sync no
EOF

cat > redis_replica.conf << EOF
port $REPLICA_PORT
bind 127.0.0.1
daemonize no
loglevel notice
logfile /tmp/redis_replica.log
save ""
replicaof 127.0.0.1 $MASTER_PORT
repl-diskless-sync no
EOF

# ── 2. Start master + populate ────────────────────────────────────────
echo ""
echo "── 1. Starting Redis master, populating $NKEYS keys ──"

redis-server redis_master.conf > /dev/null 2>&1 &
MPID=$!
sleep 1
redis-cli -p $MASTER_PORT PING > /dev/null || { echo "Master failed"; exit 1; }

echo "  Generating RESP file..."
START=$(date +%s%3N)
python3 -c "
val = b'x' * $VSIZE
with open('/tmp/redis_resp.txt', 'wb') as f:
    for i in range(1, $NKEYS + 1):
        k = f'k{i:05d}'.encode()
        f.write(b'*3\r\n\$3\r\nSET\r\n\$%d\r\n%s\r\n\$%d\r\n%s\r\n' % (len(k), k, $VSIZE, val))
" 2>&1
GEN_MS=$(($(date +%s%3N) - START))
echo "  RESP generated in ${GEN_MS}ms ($(du -h /tmp/redis_resp.txt | cut -f1))"

echo "  Piping to redis..."
cat /tmp/redis_resp.txt | redis-cli -p $MASTER_PORT --pipe 2>/dev/null
POP_END=$(date +%s%3N)
POP_MS=$(( POP_END - START ))
MC=$(redis-cli -p $MASTER_PORT DBSIZE | tr -d '\r')
echo "  Master: $MC keys (total ${POP_MS}ms)"

# ── 3. BGSAVE → get RDB size ──────────────────────────────────────────
echo ""
echo "── 2. BGSAVE → measure RDB file size ──"

redis-cli -p $MASTER_PORT BGSAVE > /dev/null
for i in $(seq 1 60); do
    IN_PROGRESS=$(redis-cli -p $MASTER_PORT INFO persistence 2>/dev/null | grep 'rdb_bgsave_in_progress' | cut -d: -f2 | tr -d '\r ')
    if [ "$IN_PROGRESS" = "0" ]; then break; fi
    sleep 0.5
done

RDB_DIR=$(redis-cli -p $MASTER_PORT CONFIG GET dir 2>/dev/null | tail -1 | tr -d '\r')
RDB_NAME=$(redis-cli -p $MASTER_PORT CONFIG GET dbfilename 2>/dev/null | tail -1 | tr -d '\r')
RDB_FILE="${RDB_DIR}/${RDB_NAME}"
ls -la "$RDB_FILE" 2>/dev/null
RDB_SIZE=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo "0")
if [ "$RDB_SIZE" -eq 0 ]; then
    # Fallback: find any dump.rdb
    RDB_FILE=$(find /tmp -name 'dump*.rdb' -newer /tmp/redis_master.conf 2>/dev/null | head -1)
    RDB_SIZE=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo "0")
fi
echo "  RDB: $RDB_FILE  →  $(( RDB_SIZE / 1024 / 1024 )) MB ($RDB_SIZE bytes)"

# ── 4. Start replica, time the fullsync ────────────────────────────────
echo ""
echo "── 3. Starting Redis replica ──"

# Record start time
REPL_START=$(date +%s%3N)

redis-server redis_replica.conf > /dev/null 2>&1 &
RPID=$!
sleep 1

# Poll until sync complete
SYNC_DONE=0
for i in $(seq 1 $TIMEOUT); do
    INFO=$(redis-cli -p $REPLICA_PORT INFO replication 2>/dev/null || echo "")
    SYNCING=$(echo "$INFO" | grep 'master_sync_in_progress' | cut -d: -f2 | tr -d '\r ')
    if [ "$SYNCING" = "0" ]; then
        REPL_END=$(date +%s%3N)
        SYNC_DONE=1
        break
    fi
    sleep 1
done

if [ "$SYNC_DONE" -eq 0 ]; then
    echo "  Timeout after ${TIMEOUT}s"
    exit 1
fi

SYNC_MS=$(( REPL_END - REPL_START ))
SC=$(redis-cli -p $REPLICA_PORT DBSIZE | tr -d '\r')

# Try multiple sources for RDB size, in order:
# 1. stat the actual file
# 2. INFO persistence rdb_last_save_size
# 3. BGSAVE log "bytes: N"
TOTAL_BYTES=$RDB_SIZE
if [ "${TOTAL_BYTES:-0}" -eq 0 ]; then
    TOTAL_BYTES=$(redis-cli -p $MASTER_PORT INFO persistence 2>/dev/null | grep 'rdb_last_save_size' | cut -d: -f2 | tr -d '\r ')
fi
if [ "${TOTAL_BYTES:-0}" -eq 0 ]; then
    TOTAL_BYTES=$(grep -oP '(?<=bytes: )\d+' /tmp/redis_master.log 2>/dev/null | tail -1 || echo "0")
fi

# ── 5. Results ────────────────────────────────────────────────────────
SYNC_SEC=$(python3 -c "print($SYNC_MS / 1000.0)")
MBPS=$(python3 -c "print(round($TOTAL_BYTES / $SYNC_SEC / 1024 / 1024, 2))")

echo ""
echo "── 4. Results ──"
echo ""
echo "  RDB size:      $TOTAL_BYTES bytes ($(( TOTAL_BYTES / 1024 / 1024 )) MB)"
echo "  Sync time:     ${SYNC_MS}ms (${SYNC_SEC}s)"
echo "  Throughput:    ${MBPS} MB/s"
echo "  Replica keys:  $SC (master: $MC)"
echo ""

# Redis log summary
echo "── Redis master log (sync events) ──"
grep -i 'sync\|replica\|bgsave\|rdb' /tmp/redis_master.log 2>/dev/null | tail -5 || echo "  (no log)"
echo ""
echo "── Redis replica log ──"
grep -i 'sync\|MASTER\|RDB\|loading' /tmp/redis_replica.log 2>/dev/null | tail -5 || echo "  (no log)"

echo ""
HR=$(python3 -c "
b = $TOTAL_BYTES
if   b >= 1<<30: print(f'{b/(1<<30):.1f} GB')
elif b >= 1<<20: print(f'{b/(1<<20):.1f} MB')
else:            print(f'{b/1024:.0f} KB')
")
echo "============================================"
printf "  %-20s  %s\n"   "Metric"          "Value"
printf "  %-20s  %s\n"   "--------------------" "-----"
printf "  %-20s  %s (%s)\n" "RDB size" "$TOTAL_BYTES" "$HR"
printf "  %-20s  %s ms\n" "Sync time"  "$SYNC_MS"
printf "  %-20s  %s sec\n" "Sync time" "$SYNC_SEC"
printf "  %-20s  %s MB/s\n" "Throughput" "$MBPS"
printf "  %-20s  %s keys\n" "Replica keys" "$SC"
echo "============================================"
