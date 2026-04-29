#!/bin/bash
# =============================================================================
# common.sh  —  KVStore benchmark shared functions
#
# Source this in other benchmark scripts:
#   source "$(dirname "$0")/common.sh"
#
# Memory: uses kernel-tracked VmPeak / VmHWM from /proc — no background poll.
# =============================================================================

set -euo pipefail

PROJ_DIR="$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")"
HOST="${KVSTORE_HOST:-127.0.0.1}"
SERVER_BIN="$PROJ_DIR/kvstore"

# redis-benchmark defaults (override via env)
#   e.g. RB_REQUESTS=50000 RB_PIPELINE=6 ./bench_engine.sh
RB="${REDIS_BENCHMARK:-redis-benchmark}"
RB_CLIENTS="${RB_CLIENTS:-50}"
RB_PIPELINE="${RB_PIPELINE:-20}"
RB_REQUESTS="${RB_REQUESTS:-50000}"
RB_KEYSPACE="${RB_KEYSPACE:-100000}"
RB_VALSIZE="${RB_VALSIZE:-4}"

RESULT_DIR="$PROJ_DIR/test/results"
SERVER_PID=""

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
C_RED='\033[0;31m' C_GREEN='\033[0;32m' C_YELLOW='\033[1;33m'
C_BLUE='\033[0;34m' C_CYAN='\033[0;36m' C_BOLD='\033[1m' C_NC='\033[0m'

banner() {
    echo -e "${C_BOLD}============================================================${C_NC}"
    echo -e "${C_BOLD}  $1${C_NC}"
    echo -e "${C_BOLD}============================================================${C_NC}"
    echo ""
}

section() { echo -e "\n${C_CYAN}--- $1 ---${C_NC}\n"; }

# ---------------------------------------------------------------------------
# Config generator
# ---------------------------------------------------------------------------

write_config() {
    local path="$1" port="$2" engine="$3" network="$4" persist="$5"
    shift 5
    {
        echo "port $port"
        echo "engine $engine"
        echo "network $network"
        echo "persistence $persist"
        echo "loglevel error"
        echo "replication off"
        echo "rdma_sync off"
        echo "rdma_repl off"
        for kv in "$@"; do echo "$kv"; done
    } > "$path"
}

# ---------------------------------------------------------------------------
# Memory helpers — VmPeak / VmHWM are kernel-tracked, no polling needed
# ---------------------------------------------------------------------------

_mem_field() { awk "/^${2}:/{print \$2}" /proc/"$1"/status 2>/dev/null || echo 0; }

# snapshot: VmSize VmRSS VmPeak VmHWM
mem_snap() {
    local pid="$1"
    echo "$(_mem_field "$pid" VmSize) $(_mem_field "$pid" VmRSS) $(_mem_field "$pid" VmPeak) $(_mem_field "$pid" VmHWM)"
}

# ---------------------------------------------------------------------------
# Server start / stop
# ---------------------------------------------------------------------------

server_start() {
    local cfg="$1" port="$2"
    "$SERVER_BIN" "$cfg" > /dev/null 2>&1 &
    SERVER_PID=$!

    local deadline=$((SECONDS + 15))
    while (( SECONDS < deadline )); do
        if timeout 1 bash -c "echo >/dev/tcp/$HOST/$port" 2>/dev/null; then
            sleep 0.3
            return 0
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo -e "${C_RED}[FATAL] Server exited early (port=$port)${C_NC}" >&2
            return 1
        fi
        sleep 0.2
    done
    echo -e "${C_RED}[FATAL] Server did not start in 15s (port=$port)${C_NC}" >&2
    return 1
}

server_stop() {
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
    rm -f "$PROJ_DIR/dump.rdb" "$PROJ_DIR/appendonly.aof"
}

# ---------------------------------------------------------------------------
# redis-benchmark runner
# ---------------------------------------------------------------------------

# Run benchmark and extract QPS
# -q uses \r to overwrite progress; tr converts them to newlines
run_bench() {
    local port="$1" op="$2"
    "$RB" \
        -h "$HOST" -p "$port" \
        -t "$op" \
        -c "$RB_CLIENTS" \
        -P "$RB_PIPELINE" \
        -n "$RB_REQUESTS" \
        -r "$RB_KEYSPACE" \
        -d "$RB_VALSIZE" \
        -q 2>/dev/null | tr '\r' '\n' | grep "requests per second" | awk '{print $2}'
}

# ---------------------------------------------------------------------------
# Full benchmark: SET (populates DB) → GET (reads back), memory tracked
#
# No preload — SET 本身就是写入，顺便把数据填好了给 GET 用。
# 空库直接 SET 才是真实的写入性能。
# ---------------------------------------------------------------------------

# Usage:  run_full_bench <name> <port> <engine> <network> <persist> [extra_kv...]
# Sets globals: R_NAME R_SET_QPS R_GET_QPS
#               R_VM_START R_RSS_START R_VM_PEAK R_RSS_PEAK R_VM_END R_RSS_END
run_full_bench() {
    local name="$1" port="$2" engine="$3" network="$4" persist="$5"
    shift 5

    local cfg
    cfg="$(mktemp /tmp/kvbench_XXXXXX.conf)"
    write_config "$cfg" "$port" "$engine" "$network" "$persist" "$@"

    section "Starting $name (engine=$engine network=$network persist=$persist)"
    if ! server_start "$cfg" "$port"; then
        rm -f "$cfg"
        return 1
    fi

    # ---- Initial memory ----
    local vm0 rss0
    read -r vm0 rss0 _ _ <<< "$(mem_snap "$SERVER_PID")"

    # ---- SET benchmark (also fills DB for GET) ----
    echo "  Running SET benchmark..."
    local set_qps
    set_qps="$(run_bench "$port" "set")"
    echo "    SET QPS: $set_qps"

    # ---- Post-SET memory (capture VmPeak/VmHWM) ----
    local vm_set rss_set
    read -r _ _ vm_set rss_set <<< "$(mem_snap "$SERVER_PID")"

    # ---- GET benchmark (reads keys written by SET) ----
    echo "  Running GET benchmark..."
    local get_qps
    get_qps="$(run_bench "$port" "get")"
    echo "    GET QPS: $get_qps"

    # ---- Final memory ----
    local vm2 rss2
    read -r vm2 rss2 _ _ <<< "$(mem_snap "$SERVER_PID")"

    # ---- Shutdown ----
    server_stop
    rm -f "$cfg"

    # ---- Results ----
    R_NAME="$name"
    R_SET_QPS="$set_qps"
    R_GET_QPS="$get_qps"
    R_VM_START="$vm0"
    R_RSS_START="$rss0"
    R_VM_PEAK="$vm_set"
    R_RSS_PEAK="$rss_set"
    R_VM_END="$vm2"
    R_RSS_END="$rss2"

    return 0
}

# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

fmt_kb() {
    local kb=${1:-0}
    if (( kb >= 1048576 )); then
        awk "BEGIN { printf \"%.2fGB\", $kb / 1048576 }"
    elif (( kb >= 1024 )); then
        awk "BEGIN { printf \"%.1fMB\", $kb / 1024 }"
    else
        echo "${kb}KB"
    fi
}

print_result_row() {
    printf "%-26s %-10s %-8s %-7s %12s %12s %10s %10s %10s %10s\n" \
        "$R_NAME" "$1" "$2" "$3" \
        "$R_SET_QPS" "$R_GET_QPS" \
        "$(fmt_kb "$R_VM_START")" "$(fmt_kb "$R_VM_PEAK")" "$(fmt_kb "$R_VM_END")" \
        "$(fmt_kb "$R_RSS_START")" "$(fmt_kb "$R_RSS_PEAK")" "$(fmt_kb "$R_RSS_END")"
}

print_header() {
    printf "%-26s %-10s %-8s %-7s %12s %12s %10s %10s %10s %10s\n" \
        "Test" "Engine" "Net" "Persist" \
        "SET QPS" "GET QPS" \
        "VmStart" "VmPeak" "VmEnd" \
        "RSSStart" "RSSPeak" "RSSEnd"
    printf "%s\n" "$(printf '%0.s-' {1..130})"
}

# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

mkdir -p "$RESULT_DIR"

echo -e "${C_GREEN}KVStore Benchmark  (redis-benchmark)${C_NC}"
echo "  Host:     $HOST"
echo "  Clients:  $RB_CLIENTS"
echo "  Pipeline: $RB_PIPELINE"
echo "  Requests: $RB_REQUESTS"
echo "  Keyspace: $RB_KEYSPACE"
echo "  Valsize:  $RB_VALSIZE bytes"
echo ""
