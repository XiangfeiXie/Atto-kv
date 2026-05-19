#!/bin/bash
# =============================================================================
# bench_fastpath.sh — Phase 10 lock-free seqlock fast-path before/after
#
# Measures GET throughput with fastpath_cache=no vs yes using redis-benchmark
# conditions aligned with Phase 8.4 local baseline:
#   - threads=2, cpu_pin 3,4
#   - c=400, P=10, n=1,000,000, d=16
#
# Scenarios:
#   100% hit: prewarm exactly redis-benchmark GET key pattern -> cache hits
#   0% hit:   no warmup -> fp misses then dict miss/nil; overhead check only
#
# NOTE: 50% hit mixed workload is harder with stock redis-benchmark because GET
# chooses its key distribution internally.  Keep it as a follow-up if needed
# with a custom pipeline generator; 100% and 0% cover the important upper bound
# and miss-overhead bound.
# =============================================================================

set -euo pipefail
script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

BENCH_N=${BENCH_N:-1000000}
BENCH_C=${BENCH_C:-400}
BENCH_P=${BENCH_P:-10}
DATA_SIZE=${DATA_SIZE:-16}
WARMUP_N=${WARMUP_N:-200000}

run_get() {
  run_bench 127.0.0.1 "$KVS_PORT" get "$BENCH_N" "$BENCH_C" "$BENCH_P" "$DATA_SIZE"
}

warmup_set() {
  redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t set \
    -n "$WARMUP_N" -c "$BENCH_C" -P "$BENCH_P" -d "$DATA_SIZE" -q >/dev/null
}

info_fp() {
  redis-cli -p "$KVS_PORT" INFO stats | grep -E 'fastpath_cache_(enabled|hits|misses|hit_rate)' || true
}

ratio() {
  awk -v a="$1" -v b="$2" 'BEGIN { if (b > 0) printf "%.2f", (a-b)*100.0/b; else printf "nan" }'
}

require_tools
cmake --build "$PROJECT_DIR/build" --target kvstore >/dev/null
mkdir -p "$LOG_DIR"

OUT_TXT="$PROJECT_DIR/test/bench/fastpath_results.txt"
: > "$OUT_TXT"

{
  echo "=== Phase 10 fastpath benchmark ==="
  echo "date: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "params: n=$BENCH_N c=$BENCH_C P=$BENCH_P d=$DATA_SIZE warmup=$WARMUP_N"
  echo "configs: kvstore-fastpath-off.conf vs kvstore-fastpath.conf"
  echo
} | tee -a "$OUT_TXT"

run_case() {
  local label="$1"
  local conf="$2"
  local do_warm="$3"

  stop_kvs
  start_kvs "$conf"
  redis-cli -p "$KVS_PORT" FLUSHALL >/dev/null
  if [ "$do_warm" = "yes" ]; then
    warmup_set
  fi
  sleep 0.2
  local qps
  qps=$(run_get)
  echo "--- $label / $conf ---" | tee -a "$OUT_TXT" >&2
  echo "GET_QPS=$qps" | tee -a "$OUT_TXT" >&2
  info_fp | tee -a "$OUT_TXT" >&2
  echo | tee -a "$OUT_TXT" >&2
  echo "$qps"
}

base_hit=$(run_case "100% hit baseline" "kvstore-fastpath-off.conf" yes | tail -1)
fp_hit=$(run_case "100% hit fastpath" "kvstore-fastpath.conf" yes | tail -1)
base_miss=$(run_case "0% hit baseline" "kvstore-fastpath-off.conf" no | tail -1)
fp_miss=$(run_case "0% hit fastpath" "kvstore-fastpath.conf" no | tail -1)

hit_gain=$(ratio "$fp_hit" "$base_hit")
miss_gain=$(ratio "$fp_miss" "$base_miss")

{
  echo "=== summary ==="
  printf "%-18s %12s %12s %10s\n" "scenario" "baseline" "fastpath" "gain"
  printf "%-18s %12s %12s %9s%%\n" "100% hit" "$base_hit" "$fp_hit" "$hit_gain"
  printf "%-18s %12s %12s %9s%%\n" "0% hit" "$base_miss" "$fp_miss" "$miss_gain"
  echo
  echo "PASS gates: target +25~35% at 100% hit, <=2% overhead at 0% hit."
} | tee -a "$OUT_TXT"

stop_kvs
