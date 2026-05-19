#!/bin/bash
set -euo pipefail
script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

MODE="${1:-all}"
BENCH_N=${BENCH_N:-1000000}
BENCH_C=${BENCH_C:-400}
BENCH_P=${BENCH_P:-10}
DATA_SIZE=${DATA_SIZE:-16}
WARMUP_N=${WARMUP_N:-200000}
HOT_KEYS=${HOT_KEYS:-64}
VALUE_SIZE=${VALUE_SIZE:-64}
TOTAL_OPS=${TOTAL_OPS:-200000}
PIPE=${PIPE:-32}
CONNS=${CONNS:-50}
OUT_TXT="$PROJECT_DIR/test/bench/fastpath_results.txt"

usage() {
  cat <<EOF
Usage: $0 [uniform|skewed|all]

uniform  Compare fastpath_cache=no/yes for redis-benchmark 100% hit and 0% hit GET.
skewed   Compare no/yes for a small hot-key GET workload.
all      Run both uniform and skewed cases.
EOF
}

case "$MODE" in
  uniform|skewed|all) ;;
  -h|--help) usage; exit 0 ;;
  *) echo "unknown mode: $MODE" >&2; usage; exit 1 ;;
esac

require_tools
cmake --build "$PROJECT_DIR/build" --target kvstore >/dev/null
mkdir -p "$LOG_DIR"
: > "$OUT_TXT"

ratio() {
  awk -v a="$1" -v b="$2" 'BEGIN { if (b > 0) printf "%.2f", (a-b)*100.0/b; else printf "nan" }'
}

info_fp() {
  redis-cli -p "$KVS_PORT" INFO stats | grep -E 'fastpath_cache_(enabled|hits|misses|hit_rate|writer_collisions)' || true
}

run_get_uniform() {
  run_bench 127.0.0.1 "$KVS_PORT" get "$BENCH_N" "$BENCH_C" "$BENCH_P" "$DATA_SIZE"
}

warmup_uniform() {
  redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t set     -n "$WARMUP_N" -c "$BENCH_C" -P "$BENCH_P" -d "$DATA_SIZE" -q >/dev/null
}

run_uniform_case() {
  local label="$1" conf="$2" do_warm="$3"
  stop_kvs
  start_kvs "$conf"
  redis-cli -p "$KVS_PORT" FLUSHALL >/dev/null
  [ "$do_warm" = "yes" ] && warmup_uniform
  sleep 0.2
  local qps
  qps=$(run_get_uniform)
  echo "--- $label / $conf ---" | tee -a "$OUT_TXT" >&2
  echo "GET_QPS=$qps" | tee -a "$OUT_TXT" >&2
  info_fp | tee -a "$OUT_TXT" >&2
  echo | tee -a "$OUT_TXT" >&2
  echo "$qps"
}

run_uniform() {
  {
    echo "=== fastpath uniform redis-benchmark ==="
    echo "date: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "params: n=$BENCH_N c=$BENCH_C P=$BENCH_P d=$DATA_SIZE warmup=$WARMUP_N"
    echo
  } | tee -a "$OUT_TXT"
  local base_hit fp_hit base_miss fp_miss hit_gain miss_gain
  base_hit=$(run_uniform_case "100% hit baseline" "kvstore-fastpath-off.conf" yes | tail -1)
  fp_hit=$(run_uniform_case "100% hit fastpath" "kvstore-fastpath.conf" yes | tail -1)
  base_miss=$(run_uniform_case "0% hit baseline" "kvstore-fastpath-off.conf" no | tail -1)
  fp_miss=$(run_uniform_case "0% hit fastpath" "kvstore-fastpath.conf" no | tail -1)
  hit_gain=$(ratio "$fp_hit" "$base_hit")
  miss_gain=$(ratio "$fp_miss" "$base_miss")
  {
    echo "=== uniform summary ==="
    printf "%-18s %12s %12s %10s
" "scenario" "baseline" "fastpath" "gain"
    printf "%-18s %12s %12s %9s%%
" "100% hit" "$base_hit" "$fp_hit" "$hit_gain"
    printf "%-18s %12s %12s %9s%%
" "0% hit" "$base_miss" "$fp_miss" "$miss_gain"
    echo
  } | tee -a "$OUT_TXT"
}

run_get_skewed() {
  redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t set       -n $((HOT_KEYS * 200)) -r "$HOT_KEYS" -c 50 -P 10       -d "$VALUE_SIZE" -q >/dev/null 2>&1
  redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t get       -n 50000 -r "$HOT_KEYS" -c "$CONNS" -P "$PIPE" -d "$VALUE_SIZE" -q       >/dev/null 2>&1
  redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t get       -n "$TOTAL_OPS" -r "$HOT_KEYS" -c "$CONNS" -P "$PIPE" -d "$VALUE_SIZE"       --csv 2>/dev/null | awk -F',' '/^"GET"/ { gsub(/"/, "", $2); print $2; exit }'
}

run_skewed_case() {
  local label="$1" conf="$2"
  stop_kvs
  start_kvs "$conf"
  redis-cli -p "$KVS_PORT" FLUSHALL >/dev/null
  sleep 0.2
  local qps
  qps=$(run_get_skewed)
  echo "--- $label / $conf ---" | tee -a "$OUT_TXT" >&2
  echo "GET_QPS=$qps" | tee -a "$OUT_TXT" >&2
  info_fp | tee -a "$OUT_TXT" >&2
  echo | tee -a "$OUT_TXT" >&2
  echo "$qps"
}

run_skewed() {
  {
    echo "=== fastpath skewed hot-key benchmark ==="
    echo "date: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "params: hot_keys=$HOT_KEYS value_size=$VALUE_SIZE total_ops=$TOTAL_OPS conns=$CONNS pipe=$PIPE"
    echo
  } | tee -a "$OUT_TXT"
  local base fp gain
  base=$(run_skewed_case "skewed baseline" "kvstore-fastpath-off.conf" | tail -1)
  fp=$(run_skewed_case "skewed fastpath" "kvstore-fastpath.conf" | tail -1)
  gain=$(ratio "$fp" "$base")
  {
    echo "=== skewed summary ==="
    printf "%-22s %12s %12s %10s
" "scenario" "baseline" "fastpath" "gain"
    printf "%-22s %12s %12s %9s%%
" "skewed (${HOT_KEYS} hot keys)" "$base" "$fp" "$gain"
    echo
  } | tee -a "$OUT_TXT"
}

case "$MODE" in
  uniform) run_uniform ;;
  skewed) run_skewed ;;
  all) run_uniform; run_skewed ;;
esac

stop_kvs
