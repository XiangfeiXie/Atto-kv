#!/bin/bash
# =============================================================================
# bench_pipeline_v2.sh — Pipeline 性能对比 (kvstore vs Redis)
#
# 用法:
#   sudo ./test/bench/bench_pipeline_v2.sh                    # 全深度 (1 5 10 20 40 80 160)
#   sudo ./test/bench/bench_pipeline_v2.sh 20 40 80            # 指定 P
#   sudo ./test/bench/bench_pipeline_v2.sh --small             # 快速验证 (10万请求)
#   sudo ./test/bench/bench_pipeline_v2.sh --proactor          # 强制 proactor 模式
#
# 输出: test/bench/results.txt
# =============================================================================

set -euo pipefail

readonly PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
readonly KVS_BIN="${PROJECT_DIR}/build/kvstore"
readonly CONF_DIR="${PROJECT_DIR}/conf"
readonly RESULT_FILE="${PROJECT_DIR}/test/bench/results.txt"
readonly KVS_PORT=2000
readonly REDIS_PORT=6379

readonly RED='\033[0;31m'  GRN='\033[0;32m'
readonly YLW='\033[1;33m'  CYN='\033[0;36m'
readonly BLD='\033[1m'     NC='\033[0m'

# ── 默认参数 ────────────────────────────────────────────────────────────
CONF="kvstore-none.conf"
PROACTOR=0
BENCH_N=1000000        # 压测请求数
BENCH_C=50             # 并发连接
WARMUP_MULT=2          # 预热倍数: warmup = BENCH_N * WARMUP_MULT（确保 GET 命中率）
DATA_SIZE=16           # value 字节
SERVER_CPU="0,2"
CLIENT_CPU="4"
TIMEOUT=180            # 单次压测超时秒

PIPELINES=(1 4 8 16 20 40 80 160)

# ── 参数解析 ────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --small)    BENCH_N=100000; BENCH_C=30; WARMUP_MULT=1;
                    PIPELINES=(1 4 8 20 80); shift ;;
        --proactor) PROACTOR=1; shift ;;
        --n)        BENCH_N="$2"; shift 2 ;;
        --c)        BENCH_C="$2"; shift 2 ;;
        --d)        DATA_SIZE="$2"; shift 2 ;;
        *)          PIPELINES=("$@"); break ;;
    esac
done

WARMUP_N=$((BENCH_N * WARMUP_MULT))

# ── 准备 proactor 临时配置 ──────────────────────────────────────────────
if [ "$PROACTOR" -eq 1 ]; then
    CONF="/tmp/kvs_bench_proactor.conf"
    cat > "$CONF" <<-'KCONF'
port 2000
network proactor
persistence none
loglevel warn
rdma_sync off
expire off
KCONF
fi

readonly TMPDIR="$(mktemp -d -t pipeline.XXXXXX)"
# 解析最终配置文件路径
if [[ "$CONF" == */* ]]; then
    readonly KVSCONF_PATH="$CONF"
else
    readonly KVSCONF_PATH="${CONF_DIR}/${CONF}"
fi

# ── 清理 ────────────────────────────────────────────────────────────────
cleanup() {
    pkill -x kvstore 2>/dev/null || true
    pkill -x redis-server 2>/dev/null || true
    rm -rf "$TMPDIR"
    sleep 0.5
}
trap cleanup EXIT

# ═══════════════════════════════════════════════════════════════════════════

die()  { echo -e "${RED}[FATAL]${NC} $(date +%H:%M:%S) $*" >&2; exit 1; }
info() { echo -e "${GRN}[INFO]${NC}  $(date +%H:%M:%S) $*" >&2; }
step() { echo -e "${CYN}[STEP]${NC}  $(date +%H:%M:%S) $*" >&2; }

_wait_port() {
    local host=$1 port=$2 max=${3:-10}
    for ((i = 0; i < max * 10; i++)); do
        ss -tlnp 2>/dev/null | grep -q ":${port} " && return 0
        sleep 0.1
    done
    return 1
}

_ratio() {
    local a=$1 b=$2
    if [[ "$a" =~ ^[0-9]+(\.[0-9]+)?$ && "$b" =~ ^[0-9]+(\.[0-9]+)?$ ]] && \
       [ "$(echo "$b > 0" | bc -l 2>/dev/null)" = "1" ]; then
        printf "%.2fx" "$(echo "scale=4; $a / $b" | bc -l 2>/dev/null)"
    else
        echo "  N/A "
    fi
}

_preflight() {
    [ "$(id -u)" -eq 0 ] || die "需要 root 权限，请使用 sudo 运行"
    for tool in redis-cli redis-benchmark bc ss taskset; do
        command -v "$tool" >/dev/null 2>&1 || die "缺少工具: $tool"
    done
    [ -x "$KVS_BIN" ] || die "kvstore 未编译: $KVS_BIN"
    [ -f "$KVSCONF_PATH" ] || die "配置文件不存在: $KVSCONF_PATH"
    mkdir -p "$(dirname "$RESULT_FILE")"
}

# ═══════════════════════════════════════════════════════════════════════════

_stop_all() {
    pkill -x kvstore 2>/dev/null || true
    for ((i = 0; i < 40; i++)); do
        ss -tlnp 2>/dev/null | grep -q ":${KVS_PORT} " || break
        sleep 0.1
    done
    pkill -x redis-server 2>/dev/null || true
    sleep 0.5
}

_start_kvs() {
    local port
    port=$(grep '^port ' "$KVSCONF_PATH" 2>/dev/null | awk '{print $2}')
    port=${port:-$KVS_PORT}

    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/dump.rdb.tmp" \
          "${PROJECT_DIR}/build/appendonly.aof" "${PROJECT_DIR}/build/incr.wal"

    taskset -c "$SERVER_CPU" "$KVS_BIN" "$KVSCONF_PATH" \
        > "${TMPDIR}/kvstore.log" 2>&1 &
    echo $! > "${TMPDIR}/kvstore.pid"

    _wait_port 127.0.0.1 "$port" 15 || {
        tail -20 "${TMPDIR}/kvstore.log" >&2
        die "kvstore 启动超时 (port=$port)"
    }
    info "kvstore  启动 (pid=$(cat ${TMPDIR}/kvstore.pid), port=$port)"
}

_start_redis() {
    redis-server --daemonize yes --port $REDIS_PORT || die "Redis 启动失败"
    sleep 0.5
    redis-cli -p $REDIS_PORT PING >/dev/null 2>&1 || die "Redis 无响应"

    local rpid
    rpid=$(pgrep -x redis-server | head -1)
    [ -n "$rpid" ] && taskset -pc "$SERVER_CPU" "$rpid" >/dev/null 2>&1 || true

    redis-cli -p $REDIS_PORT CONFIG SET save ""          >/dev/null 2>&1
    redis-cli -p $REDIS_PORT CONFIG SET appendonly no     >/dev/null 2>&1
    redis-cli -p $REDIS_PORT FLUSHALL                     >/dev/null 2>&1
    info "Redis    启动 (port=$REDIS_PORT, 持久化=off)"
}

# ═══════════════════════════════════════════════════════════════════════════
# 压测引擎
# ═══════════════════════════════════════════════════════════════════════════

_run_bench() {
    # $1=host $2=port $3=test(set/get) $4=P
    # stdout: QPS (数字)
    local host=$1 port=$2 test=$3 P=$4
    local qps="0" line retry=0

    while [ $retry -lt 2 ]; do
        # redis-benchmark --csv 输出:
        #   "test","rps","avg_latency_ms",...
        #   "SET","12345.67",...
        # tail -1 取数据行 (跳过 header)
        line=$(timeout "$TIMEOUT" \
            taskset -c "$CLIENT_CPU" \
            redis-benchmark -h "$host" -p "$port" -t "$test" \
                -n "$BENCH_N" -c "$BENCH_C" -P "$P" -d "$DATA_SIZE" \
                -r "$BENCH_N" --csv 2>/dev/null \
            | tail -1)

        if [ -n "$line" ]; then
            # 从 CSV 中提取第2列 (rps)
            qps=$(echo "$line" | awk -F',' '{print $2}' | tr -d '"')
            if [[ "$qps" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                echo "$qps"
                return 0
            fi
        fi
        retry=$((retry + 1))
        [ $retry -lt 2 ] && sleep 0.5
    done
    echo "0"
}

# ═══════════════════════════════════════════════════════════════════════════
# 结果输出
# ═══════════════════════════════════════════════════════════════════════════

declare -A K_SET R_SET K_GET R_GET

_print_header() {
    local netmode="reactor"
    [ "$PROACTOR" -eq 1 ] && netmode="proactor"
    cat > "$RESULT_FILE" <<-HEADER
# Pipeline 性能对比 — kvstore vs Redis
# 时间: $(date '+%Y-%m-%d %H:%M:%S')
# 网络模型: $netmode
# 参数: n=$BENCH_N  c=$BENCH_C  d=$DATA_SIZE  warmup=$WARMUP_N
# CPU:  server=$SERVER_CPU  client=$CLIENT_CPU
# Pipeline: ${PIPELINES[*]}

HEADER
}

_append_raw() { echo "$@" >> "$RESULT_FILE"; }

_print_table() {
    local netmode="reactor"
    [ "$PROACTOR" -eq 1 ] && netmode="proactor"
    local title="Pipeline 性能 [$netmode]"

    local div hdr sep
    div=$(printf '+%s+%s+%s+%s+%s+%s+%s+' \
        '------' '------------' '------------' '--------' '------------' '------------' '--------')
    hdr=$(printf '| %-4s | %12s | %12s | %6s | %12s | %12s | %6s |' \
        'P'   'kvs-SET'     'redis-SET'    'S倍率'    'kvs-GET'     'redis-GET'    'G倍率')
    sep=$(printf '| %-4s | %12s | %12s | %6s | %12s | %12s | %6s |' \
        '----' '------------' '------------' '------' '------------' '------------' '------')

    echo ""
    echo -e "  ${BLD}${YLW}${title}${NC}"
    echo "$div"
    echo "$hdr"
    echo "$sep"

    _append_raw ""; _append_raw "$title"
    _append_raw "$div"; _append_raw "$hdr"; _append_raw "$sep"

    local best_k=0 best_r=0
    for P in "${PIPELINES[@]}"; do
        local ks=${K_SET[$P]:-0}  rs=${R_SET[$P]:-0}
        local kg=${K_GET[$P]:-0}  rg=${R_GET[$P]:-0}
        local sr gr
        sr=$(_ratio "$ks" "$rs")
        gr=$(_ratio "$kg" "$rg")

        local row
        row=$(printf '| %-4s | %12s | %12s | %6s | %12s | %12s | %6s |' \
            "P$P" "$ks" "$rs" "$sr" "$kg" "$rg" "$gr")
        echo "$row"
        _append_raw "$row"

        [ "${kg%.*}" -gt "$best_k" ] 2>/dev/null && best_k=${kg%.*}
        [ "${rg%.*}" -gt "$best_r" ] 2>/dev/null && best_r=${rg%.*}
    done

    echo "$div"
    _append_raw "$div"

    echo ""
    echo -e "  ${GRN}结果已保存: $RESULT_FILE${NC}"

    if [ "$best_k" -gt 0 ] 2>/dev/null; then
        local br; br=$(_ratio "$best_k" "$best_r")
        local note="峰值GET: kvstore=${best_k}  redis=${best_r}  峰值比=${br}"
        _append_raw ""; _append_raw "$note"
        echo -e "  ${YLW}${note}${NC}"
    fi
    echo ""
}

# ═══════════════════════════════════════════════════════════════════════════

main() {
    _preflight
    _stop_all

    info "Pipeline 压测 — n=$BENCH_N c=$BENCH_C d=$DATA_SIZE"
    info "Pipeline 深度: ${PIPELINES[*]}"
    info "网络模型: $( [ "$PROACTOR" -eq 1 ] && echo proactor || echo reactor)"
    info "预热: $WARMUP_N keys (${WARMUP_MULT}x benchn)"
    _print_header

    for P in "${PIPELINES[@]}"; do
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        step "Pipeline=$P  (n=$BENCH_N c=$BENCH_C)"

        _stop_all
        _start_kvs
        _start_redis

        # ── SET ───────────────────────────────────
        printf "  %-20s" "SET → kvstore ..."
        K_SET[$P]=$(_run_bench 127.0.0.1 "$KVS_PORT"  set "$P")
        echo -e "${GRN}$(printf '%12s' "${K_SET[$P]}") req/s${NC}"

        printf "  %-20s" "SET → Redis ..."
        R_SET[$P]=$(_run_bench 127.0.0.1 "$REDIS_PORT" set "$P")
        echo -e "${GRN}$(printf '%12s' "${R_SET[$P]}") req/s${NC}"

        local rs; rs=$(_ratio "${K_SET[$P]}" "${R_SET[$P]}")
        echo -e "  ${YLW}SET 倍率: $rs${NC}"

        # ── 预热 (kvstore + redis 并行) ────────
        printf "  %-20s" "预热 keys ..."
        taskset -c "$CLIENT_CPU" redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" \
            -t set -n "$WARMUP_N" -c 50 -P 100 -d "$DATA_SIZE" \
            -r "$WARMUP_N" -q >/dev/null 2>&1 &
        local wp1=$!
        taskset -c "$CLIENT_CPU" redis-benchmark -h 127.0.0.1 -p "$REDIS_PORT" \
            -t set -n "$WARMUP_N" -c 50 -P 100 -d "$DATA_SIZE" \
            -r "$WARMUP_N" -q >/dev/null 2>&1 &
        local wp2=$!
        wait $wp1 $wp2 2>/dev/null || true
        echo "done  ($WARMUP_N keys)"

        # ── GET ───────────────────────────────────
        printf "  %-20s" "GET → kvstore ..."
        K_GET[$P]=$(_run_bench 127.0.0.1 "$KVS_PORT"  get "$P")
        echo -e "${GRN}$(printf '%12s' "${K_GET[$P]}") req/s${NC}"

        printf "  %-20s" "GET → Redis ..."
        R_GET[$P]=$(_run_bench 127.0.0.1 "$REDIS_PORT" get "$P")
        echo -e "${GRN}$(printf '%12s' "${R_GET[$P]}") req/s${NC}"

        local rg; rg=$(_ratio "${K_GET[$P]}" "${R_GET[$P]}")
        echo -e "  ${YLW}GET 倍率: $rg${NC}"

        _append_raw "P=$P  SET  kvstore=${K_SET[$P]}  redis=${R_SET[$P]}  ratio=$rs"
        _append_raw "P=$P  GET  kvstore=${K_GET[$P]}  redis=${R_GET[$P]}  ratio=$rg"
    done

    _print_table
}

main "$@"
