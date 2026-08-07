#!/bin/bash
# redis_fullsync_bench.sh — Redis 全量同步速率对比（无盘 vs 有盘）
#
# 设计:
#   1. 两轮复用同一 RDB：有盘生成 → 无盘自动加载，避免重复填充
#   2. 计时 = "Starting BGSAVE for SYNC" → "Synchronization succeeded"
#   3. 输出一行简洁结果，方便多轮测试
#
# 用法: ./test/bench/redis_fullsync_bench.sh [--keys N] [--vsize B]
set -u

LOG_DIR="/tmp/redis_fs_modes_$$"; mkdir -p "$LOG_DIR"
KEYS=120000; VSIZE=4096; MPORT=8400; SPORT=8401; TIMEOUT=300

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keys)  KEYS="$2";  shift 2 ;;
        --vsize) VSIZE="$2"; shift 2 ;;
        --port)  MPORT="$2"; SPORT=$((MPORT+1)); shift 2 ;;
        -h|--help) echo "用法: $0 [--keys N] [--vsize B] [--port P]"; exit 0 ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

for t in redis-server redis-cli python3 bc; do
    command -v "$t" >/dev/null 2>&1 || { echo "缺少工具: $t"; exit 1; }
done

cleanup() {
    redis-cli -p "$MPORT" SHUTDOWN NOSAVE 2>/dev/null || true
    redis-cli -p "$SPORT" SHUTDOWN NOSAVE 2>/dev/null || true
    sleep 1
}

await() {
    local log=$1 rx=$2 i=0 max=$((TIMEOUT * 10))
    while [ $i -lt $max ]; do
        grep -qE "$rx" "$log" 2>/dev/null && { date +%s.%N; return 0; }
        i=$((i+1)); sleep 0.1
    done
    return 1
}

# ── 主流程 ────────────────────────────────────────────────────────────────

cleanup
rm -rf "${LOG_DIR}/disk_m" "${LOG_DIR}/diskless_m"

# --- 有盘 ---
mdir="${LOG_DIR}/disk_m" sdir="${LOG_DIR}/disk_s"; mkdir -p "$mdir" "$sdir"
mlog="${LOG_DIR}/disk_master.log" slog="${LOG_DIR}/disk_slave.log"

>&2 echo -n "[disk] populate + save ... "
redis-server --port "$MPORT" --daemonize yes --dir "$mdir" --dbfilename dump.rdb \
    --logfile "$mlog" --save "" --appendonly no \
    --repl-diskless-sync no >/dev/null 2>&1
sleep 1

python3 -c "
import sys, os; n=$KEYS; pad=os.urandom($VSIZE); pl=len(pad); o=sys.stdout.buffer
for i in range(1,n+1):
    k=f'key:{i:012d}'.encode(); kl=len(k)
    o.write(f'*3\r\n\$3\r\nSET\r\n\${kl}\r\n'.encode()); o.write(k)
    o.write(f'\r\n\${pl}\r\n'.encode()); o.write(pad); o.write(b'\r\n')
" | redis-cli -p "$MPORT" --pipe >/dev/null 2>&1
redis-cli -p "$MPORT" SAVE >/dev/null 2>&1

disk_keys=$(redis-cli -p "$MPORT" DBSIZE 2>/dev/null || echo 0)
disk_rdb=$(stat -c %s "$mdir/dump.rdb" 2>/dev/null || echo 0)
rdb_mb=$(awk "BEGIN { printf \"%.2f\", $disk_rdb/1048576 }")
>&2 echo "$disk_keys keys, $rdb_mb MB RDB"

redis-server --port "$SPORT" --daemonize yes --dir "$sdir" --dbfilename dump.rdb \
    --logfile "$slog" --save "" --appendonly no >/dev/null 2>&1
sleep 1
redis-cli -p "$SPORT" SLAVEOF 127.0.0.1 "$MPORT" >/dev/null 2>&1

ts=$(await "$mlog" "Starting BGSAVE for SYNC") || { echo "FAIL"; cleanup; exit 1; }
te=$(await "$mlog" "Synchronization with replica .* succeeded") || { echo "FAIL"; cleanup; exit 1; }
dt_disk=$(echo "$te - $ts" | bc)
rate_disk=$(echo "scale=2; $disk_rdb / $dt_disk / 1048576" | bc)

cleanup

# --- 无盘 ---
mdir="${LOG_DIR}/diskless_m" sdir="${LOG_DIR}/diskless_s"; mkdir -p "$mdir" "$sdir"
cp "${LOG_DIR}/disk_m/dump.rdb" "$mdir/dump.rdb"
mlog="${LOG_DIR}/diskless_master.log" slog="${LOG_DIR}/diskless_slave.log"

>&2 echo -n "[diskless] load RDB ... "
redis-server --port "$MPORT" --daemonize yes --dir "$mdir" --dbfilename dump.rdb \
    --logfile "$mlog" --save "" --appendonly no \
    --repl-diskless-sync yes --repl-diskless-sync-delay 0 >/dev/null 2>&1
sleep 2
>&2 echo "$(redis-cli -p "$MPORT" DBSIZE 2>/dev/null) keys"

redis-server --port "$SPORT" --daemonize yes --dir "$sdir" --dbfilename dump.rdb \
    --logfile "$slog" --save "" --appendonly no >/dev/null 2>&1
sleep 1
redis-cli -p "$SPORT" SLAVEOF 127.0.0.1 "$MPORT" >/dev/null 2>&1

ts=$(await "$mlog" "Starting BGSAVE for SYNC") || { echo "FAIL"; cleanup; exit 1; }
te=$(await "$mlog" "Synchronization with replica .* succeeded") || { echo "FAIL"; cleanup; exit 1; }
dt_dl=$(echo "$te - $ts" | bc)
rate_dl=$(echo "scale=2; $disk_rdb / $dt_dl / 1048576" | bc)

cleanup

# ── 结果 ──
echo ""
printf "  KEYS=%s  VSIZE=%s B  RDB=%s MB (%s bytes)\n\n" \
    "$disk_keys" "$VSIZE" "$rdb_mb" "$disk_rdb"
printf "  %-12s %10s %12s %10s\n" "" "TIME(s)" "DATA_B" "MB/s"
printf "  %-12s %10.3f %12s %10.2f\n" "disk"     "$dt_disk" "$disk_rdb" "$rate_disk"
printf "  %-12s %10.3f %12s %10.2f\n" "diskless" "$dt_dl"   "$disk_rdb" "$rate_dl"
echo ""
