#!/bin/bash
# =============================================================================
# bench_memory.sh — malloc(glibc) vs jemalloc 对 kvstore 的内存生命周期对比
#
# 三阶段测量（对应 kvstore 真实关心的三个点）:
#   1) 启动   — 空库 RSS 基线
#   2) 峰值   — 填充 N×V 后读物理峰值 VmHWM + allocator 自报 used_memory
#               峰值/key = 同量数据占多少物理内存 (容量); 峰值−used = 分片浪费
#   3) 释放   — FLUSHALL 后等 RELEASE_WAIT 秒再读 RSS
#               返回率 = 删除后内存还给系统的比例 (对应 TTL/淘汰/清库后的表现)
#
# 固定测试点: N=1000000 key × V=16 字节 (小 value = 海量小分配, allocator 差异最明显)。
#
# jemalloc 版通过环境变量注入 MALLOC_CONF="dirty_decay_ms:0": 释放的脏页立即
# MADV_DONTNEED 还给 OS, 否则默认 MADV_FREE 成 muzzy 态 RSS 不降, 测不出返回率。
# start_kvs 在 common.sh 共用不能改, 本脚本用 start_kvs_mem 包装注入。
#
# 用法（项目根目录执行）:
#   sudo ./test/bench/bench_memory.sh                      # jemalloc vs glibc
#   sudo ./test/bench/bench_memory.sh --bin build/kvstore  # 只测单个
#
# 输出: 终端对比表 + test/bench/results_memory.txt
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# 固定参数
N=1000000                # key 数量
V=16                     # value 字节
BASE_SETTLE=2            # 启动后静置
RELEASE_WAIT=15          # FLUSHALL 后静置 (让 jemalloc dirty_decay≈10s 真实生效)
HOST=127.0.0.1
RESULT_FILE="test/bench/results_memory.txt"

# 默认对比两个 allocator 的二进制
BINS=("build/kvstore" "build-glibc/kvstore")
BIN_LABELS=("jemalloc" "glibc")

# ── 参数解析 (仅 --bin) ─────────────────────────────────────────────────
help() {  # 只打印头部注释块 (到第一个非 # 行即停)
    awk '!/^#/ {exit} NR>=2 {sub(/^# */, ""); print}' "$0"
    exit 0
}
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bin)      BINS=("$2"); BIN_LABELS=("$(basename "$2")"); shift 2 ;;
        -h|--help)  help ;;
        *)          echo "未知参数: $1" >&2; exit 1 ;;
    esac
done

require_tools

WORK="$(mktemp -d /tmp/membench.XXXXXX)"
CONF_PATH="${WORK}/mem.conf"
write_kvs_conf "$CONF_PATH" "$KVS_PORT" proactor rdb

cleanup() { stop_all; rm -rf "$WORK"; }
trap cleanup EXIT

# ── 一次读数: "used_kB rss_kB hwm_kB" (INFO used_memory + /proc VmRSS/VmHWM) ──
read_mem() { # $1=port  $2=pid
    local port=$1 pid=$2 used_b rss hwm
    used_b=$(redis-cli -h "$HOST" -p "$port" INFO memory 2>/dev/null \
        | tr -d '\r' | awk -F: '$1=="used_memory"{print $2; exit}')
    used_b=${used_b:-0}
    rss=$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null); rss=${rss:-0}
    hwm=$(awk '/^VmHWM:/{print $2}' "/proc/$pid/status" 2>/dev/null); hwm=${hwm:-0}
    echo "$((used_b / 1024)) $rss $hwm"
}

# ── 启动 kvstore（本脚本专用包装, 不动 common.sh 的 start_kvs）──────────
# 链接了 jemalloc 的二进制注入 MALLOC_CONF="dirty_decay_ms:0":
# kvs_flush_all 里 arenas.purge 才对脏页走 MADV_DONTNEED, FLUSHALL 后 RSS 真的回落
# (实测: 默认 muzzy 态 RSS 不降, dirty_decay_ms:0 立即降). export 的变量被子进程继承,
# jemalloc 进程启动读库时生效. glibc 版不注入(它不读 MALLOC_CONF, 也无需此配置).
start_kvs_mem() { # $1=conf  $2=port  $3=bin
    local conf=$1 port=$2 bin=$3
    if ldd "$bin" 2>/dev/null | grep -q jemalloc; then
        export MALLOC_CONF="dirty_decay_ms:0"
    else
        unset MALLOC_CONF
    fi
    start_kvs "$conf" "$port"
}

# ── 测一个 allocator → "基线rss 峰值hwm used_kB 峰值/key(B) 释放rss 返回率%" ──
measure_one() { # $1=bin  $2=label
    local bin=$1 label=$2 pid base_rss hwm used release_rss
    stop_all
    KVS_BIN="$bin"
    start_kvs_mem "$CONF_PATH" "$KVS_PORT" "$bin" >&2   # log 走 stderr, 防污染 $(...) 捕获
    pid=$(pgrep -x kvstore | head -1)
    [ -n "$pid" ] || { err "$label 进程未找到"; return 1; }

    sleep "$BASE_SETTLE"
    read -r _ base_rss _ <<< "$(read_mem "$KVS_PORT" "$pid")"
    log "  $label 启动: rss=${base_rss}kB" >&2

    # 填充后立即记实际条数——必须在 FLUSHALL 之前, 否则 DBSIZE 恒为 0
    pipe_populate "$HOST" "$KVS_PORT" "$N" "$V"
    local dbsize denom peak_key ret
    dbsize=$(redis-cli -h "$HOST" -p "$KVS_PORT" DBSIZE 2>/dev/null | tr -d '\r')
    dbsize=${dbsize:-0}
    [ "$dbsize" -eq "$N" ] || warn "$label 实际填充 $dbsize (期望 $N), 按实际计算"

    read -r used _ hwm <<< "$(read_mem "$KVS_PORT" "$pid")"
    log "  $label 峰值: hwm=${hwm}kB used=${used}kB" >&2

    redis-cli -h "$HOST" -p "$KVS_PORT" FLUSHALL >/dev/null 2>&1 || true
    sleep "$RELEASE_WAIT"
    read -r _ release_rss _ <<< "$(read_mem "$KVS_PORT" "$pid")"
    log "  $label 释放: rss=${release_rss}kB" >&2

    denom=$dbsize; [ "$denom" -eq 0 ] && denom=1
    peak_key=$(awk "BEGIN{printf \"%.1f\", ($hwm - $base_rss)*1024/$denom}")
    if [ "$hwm" -gt "$base_rss" ]; then
        ret=$(awk "BEGIN{v=($hwm - $release_rss)*100/($hwm - $base_rss); if(v<0)v=0; if(v>100)v=100; printf \"%.0f\", v}")
    else
        ret="N/A"
    fi
    echo "$base_rss $hwm $used $peak_key $release_rss $ret"
}

# ── 主流程 ──────────────────────────────────────────────────────────────
> "$RESULT_FILE"
{
    echo "bench_memory — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "N=$N keys  V=$V B  release_wait=${RELEASE_WAIT}s  (峰值=填充后 VmHWM; 返回率=FLUSHALL 后内存归还比例)"
    echo ""
    printf "  %-8s | %9s | %9s | %9s | %11s | %10s | %7s\n" \
        "allocator" "基线(kB)" "峰值(kB)" "used(kB)" "峰值/key(B)" "释放后(kB)" "返回率%"
} >> "$RESULT_FILE"

declare -A MEMS
for i in "${!BINS[@]}"; do
    bin=${BINS[$i]}; label=${BIN_LABELS[$i]}
    [ -x "$bin" ] || { err "二进制不存在: $bin"; continue; }
    res=$(measure_one "$bin" "$label") || { err "$label 测量失败"; continue; }
    read -r base_rss hwm used peak_key release_rss ret <<< "$res"
    MEMS["$label"]="$peak_key $ret"
    line="$(printf "  %-8s | %9s | %9s | %9s | %11s | %10s | %7s" \
        "$label" "$base_rss" "$hwm" "$used" "$peak_key" "$release_rss" "$ret")"
    echo "$line"
    echo "$line" >> "$RESULT_FILE"
done
stop_all

# 结论: jemalloc 相对 glibc 的峰值/key 下降 + 返回率对比
if [ -n "${MEMS[jemalloc]:-}" ] && [ -n "${MEMS[glibc]:-}" ]; then
    read -r jkey jret <<< "${MEMS[jemalloc]}"
    read -r gkey gret <<< "${MEMS[glibc]}"
    drop=$(awk "BEGIN{printf \"%.1f\", (1 - $jkey/$gkey)*100}")
    echo ""
    echo "结论: jemalloc 峰值/key 比 glibc 低 ${drop}%  (同数据占更少物理内存)"
    echo "结论: jemalloc 返回率 ${jret}% vs glibc ${gret}%  (删除后归还系统的比例)"
    {
        echo ""
        echo "结论: jemalloc 峰值/key 比 glibc 低 ${drop}%; 返回率 ${jret}% vs ${gret}%"
    } >> "$RESULT_FILE"
fi
echo ""
echo "结果已写入: $RESULT_FILE"
