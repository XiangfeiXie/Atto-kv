#!/bin/bash
# =============================================================================
# bench_memory.sh — KVStore 内存占用测量脚本
#
# 目标：在 kvstore-none.conf（不开启任何功能）下，测量纯 KV 数据的内存占用，
#       并同时报告三个层面的"内存"：
#         · allocated   — INFO memory 的 used_memory（存活分配字节的精确和）
#                         jemalloc: stats.allocated；glibc: mallinfo2().uordblks
#         · rss         — /proc/<pid>/status 的 VmRSS（真实物理占用）
#         · private     — /proc/<pid>/smaps_rollup 的 Private_Dirty+Private_Clean
#                         剔除共享库/代码页，最干净的 KV 占用
#
# 方法：每个 (N,V) 配置独立重启空服务 → 采 baseline → 填充 → settle → 采稳态，
#       用差分 (after-baseline) 除以真实 DBSIZE 得到每 key 开销。
#
# 用法：
#   ./test/bench/bench_memory.sh                          # 默认二维小扫描
#   ./test/bench/bench_memory.sh --n "100000 1000000" --v "16 256 4096"
#   ./test/bench/bench_memory.sh --n 50000 --v 64 --settle 8
#   # jemalloc vs glibc 对比（各跑一次，输出到不同 CSV）
#   ./test/bench/bench_memory.sh --bin build/kvstore        --out test/bench/results_memory_jemalloc.csv
#   ./test/bench/bench_memory.sh --bin build-glibc/kvstore  --out test/bench/results_memory_glibc.csv
#
# 关键参数：
#   --n      key 数量列表        (默认 100000 1000000)
#   --v      值大小列表(字节)    (默认 16 256 4096)
#   --settle 填充后静置秒数      (默认 15，覆盖 jemalloc dirty_decay=10s)
#   --samples 稳态采样次数       (默认 5，间隔 1s，取 min)
#   --conf   配置文件            (默认 kvstore-none.conf)
#   --bin    kvstore 二进制      (默认 build/kvstore；glibc 用 build-glibc/kvstore)
#   --out    结果 CSV 输出路径   (默认 test/bench/results_memory.csv)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 复用公共函数库 (start_kvs / stop_kvs / wait_for_port / log / 颜色)
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

# ---- 默认参数 ----
CONF="${CONF:-kvstore-none.conf}"
N_LIST="100000 1000000"
V_LIST="16 256 4096"
SETTLE_SEC=15
SAMPLES=5
BASELINE_SETTLE_SEC=2
HOST="127.0.0.1"
RESULT_CSV="${PROJECT_DIR}/test/bench/results_memory.csv"

# ---- 解析 CLI ----
while [ $# -gt 0 ]; do
    case "$1" in
        --n)        N_LIST="$2";       shift 2 ;;
        --v)        V_LIST="$2";       shift 2 ;;
        --settle)   SETTLE_SEC="$2";   shift 2 ;;
        --samples)  SAMPLES="$2";      shift 2 ;;
        --conf)     CONF="$2";         shift 2 ;;
        --bin)      KVS_BIN="$2";      shift 2 ;;
        --out)      RESULT_CSV="$2";   shift 2 ;;
        *) echo "未知参数: $1" >&2; exit 1 ;;
    esac
done

# ---- 预检 ----
require_tools_py() {
    for tool in redis-cli python3 bc; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            err "缺少工具: $tool"
            exit 1
        fi
    done
    if [ ! -x "$KVS_BIN" ]; then
        err "kvstore 未编译: $KVS_BIN"
        exit 1
    fi
}
require_tools_py

CONF_PATH="${CONF_DIR}/${CONF}"
if [ ! -f "$CONF_PATH" ]; then
    err "配置文件不存在: $CONF_PATH"
    exit 1
fi
PORT=$(grep '^port ' "$CONF_PATH" | awk '{print $2}')
PORT=${PORT:-2000}

# ---- 采样函数 ----
# sample_once <pid> <port> → 输出 7 个数字 (kB)：
#   alloc rss vmsize hwm priv_dirty priv_clean pss
sample_once() {
    local pid=$1 port=$2

    # 逻辑分配量 (INFO memory used_memory, 字节)
    #   jemalloc = stats.allocated；glibc = mallinfo2().uordblks
    local alloc_b=0
    local info
    if info=$(redis-cli -h "$HOST" -p "$port" INFO memory 2>/dev/null); then
        # RESP 行尾带 \r，剥离后再取数值，否则 $(( )) 会报 syntax error
        alloc_b=$(printf '%s\n' "$info" | tr -d '\r' | awk -F: '$1=="used_memory"{print $2; exit}')
        alloc_b=${alloc_b:-0}
    fi
    local alloc_kb=$((alloc_b / 1024))

    local status="/proc/$pid/status"
    local rss=0 vmsize=0 hwm=0
    if [ -r "$status" ]; then
        rss=$(awk '/^VmRSS:/{print $2}' "$status");   rss=${rss:-0}
        vmsize=$(awk '/^VmSize:/{print $2}' "$status"); vmsize=${vmsize:-0}
        hwm=$(awk '/^VmHWM:/{print $2}' "$status");   hwm=${hwm:-0}
    fi

    local pd=0 pc=0 pss=0
    local sm="/proc/$pid/smaps_rollup"
    if [ -r "$sm" ]; then
        pd=$(awk '/^Private_Dirty:/{print $2}' "$sm"); pd=${pd:-0}
        pc=$(awk '/^Private_Clean:/{print $2}' "$sm"); pc=${pc:-0}
        pss=$(awk '/^Pss:/{print $2}' "$sm");           pss=${pss:-0}
    fi

    echo "$alloc_kb $rss $vmsize $hwm $pd $pc $pss"
}

# sample_stable <pid> <port> <samples> <interval> → 同 sample_once，但
#   RSS/private 取多次采样的最小值（空闲时只降不升，min 才是收敛值），
#   allocated/vmsize/hwm/pss 取最后一次（稳定量）。
sample_stable() {
    local pid=$1 port=$2 samples=${3:-5} interval=${4:-1}
    local a r v h pd pc ps
    local min_rss=999999999 min_pd=999999999 min_pc=999999999
    local la=0 lv=0 lh=0 lps=0
    for ((i=0; i<samples; i++)); do
        read -r a r v h pd pc ps <<< "$(sample_once "$pid" "$port")"
        la=$a; lv=$v; lh=$h; lps=$ps
        [ "$r" -lt "$min_rss" ] && min_rss=$r
        [ "$pd" -lt "$min_pd" ] && min_pd=$pd
        [ "$pc" -lt "$min_pc" ] && min_pc=$pc
        [ $i -lt $((samples-1)) ] && sleep "$interval"
    done
    echo "$la $min_rss $lv $lh $min_pd $min_pc $lps"
}

# ---- 单配置测量 ----
measure_one() {
    local n=$1 vsize=$2

    step "重启空服务 (conf=$CONF)"
    start_kvs "$CONF"
    local pid
    pid=$(cat "${LOG_DIR}/kvstore.pid")
    sleep "$BASELINE_SETTLE_SEC"

    local base
    base=$(sample_stable "$pid" "$PORT" 3 1)
    read -r ba br bv bh bpd bpc bps <<< "$base"
    log "baseline: alloc=${ba}kB rss=${br}kB hwm=${bh}kB priv=$((bpd+bpc))kB"

    step "填充 n=$n vsize=$vsize"
    local sent
    sent=$(python3 "${SCRIPT_DIR}/populate_kv.py" "$HOST" "$PORT" 1 "$n" "$vsize" 2>/dev/null)
    sent=${sent:-0}
    log "填充完成 sent=$sent"

    step "settle ${SETTLE_SEC}s 后稳态采样"
    sleep "$SETTLE_SEC"
    local after
    after=$(sample_stable "$pid" "$PORT" "$SAMPLES" 1)
    read -r aa ar av ah apd apc aps <<< "$after"
    log "after:    alloc=${aa}kB rss=${ar}kB hwm=${ah}kB priv=$((apd+apc))kB"

    # 校验实际落盘数
    local dbsize=0
    dbsize=$(redis-cli -h "$HOST" -p "$PORT" DBSIZE 2>/dev/null | tr -d '\r')
    dbsize=${dbsize:-0}
    if [ "$dbsize" -ne "$n" ]; then
        warn "DBSIZE=$dbsize != 期望 $n，按实际值计算"
    fi

    # 差分 + 每 key 开销 (字节)
    local d_alloc=$((aa - ba)) d_rss=$((ar - br)) d_hwm=$((ah - bh))
    local d_priv=$(( (apd + apc) - (bpd + bpc) ))

    awk -v da="$d_alloc" -v dr="$d_rss" -v dh="$d_hwm" -v dp="$d_priv" -v n="$dbsize" \
        -v N="$n" -v V="$vsize" \
        'BEGIN {
            if (n <= 0) n = 1;
            ov = (da > 0 && dp > da) ? (dp - da) * 100.0 / da : 0.0;
            printf "%d,%d,%d,%d,%d,%d,%d,%.1f,%.1f,%.1f,%.1f\n",
                N, V, n,
                da, dr, dh, dp,
                (da * 1024) / n, (dr * 1024) / n, (dp * 1024) / n, ov;
        }'
}

# ---- 主流程 ----
main() {
    # 打印 CSV 头到 stdout + 结果文件
    local header="N,V,dbsize,alloc_kb,rss_kb,rss_peak_kb,private_kb,alloc_b_per_key,rss_b_per_key,private_b_per_key,overhead_pct"
    echo "$header"
    echo "$header" > "$RESULT_CSV"
    log "内存测量开始: conf=$CONF port=$PORT N={$N_LIST} V={$V_LIST} settle=${SETTLE_SEC}s"

    for vsize in $V_LIST; do
        for n in $N_LIST; do
            step "=== 测量 N=$n V=$vsize ==="
            local row
            row=$(measure_one "$n" "$vsize")
            echo "$row"
            echo "$row" >> "$RESULT_CSV"
            log "记录: $row"
        done
    done

    stop_kvs
    log "完成。结果已写入 $RESULT_CSV"
}

main "$@"
