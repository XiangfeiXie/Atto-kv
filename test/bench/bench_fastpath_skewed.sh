#!/bin/bash
# =============================================================================
# bench_fastpath_skewed.sh — Phase 10 fast-path under a hot-key workload
#
# Why this exists:
#   redis-benchmark's `key:__rand_int__` distributes GETs uniformly over a
#   large key range — the worst case for any hot-cache (no temporal locality).
#   Real production traffic almost always shows Pareto/Zipf skew where ~5%
#   of keys serve ~95% of GETs.  This bench targets the latter shape.
#
# Workload:
#   - HOT_KEYS keys (default 64), each holding a VALUE_SIZE-byte payload
#   - Pipelined GETs cycling through the same hot key set
#   - All keys eligible for the fast-path (klen ≤ 64, vlen ≥ FP_VAL_MIN=32,
#     vlen ≤ FP_VAL_MAX=256)
#
# This is a microbenchmark.  The bound it measures is the GET hit path
# itself (parser → fp_get → dynbuf_cat → wire).  No claim is made about
# random-access workloads — those are covered by bench_fastpath.sh and
# are documented as ~neutral.
# =============================================================================

set -euo pipefail
script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

HOT_KEYS=${HOT_KEYS:-64}
VALUE_SIZE=${VALUE_SIZE:-64}
TOTAL_OPS=${TOTAL_OPS:-200000}
PIPE=${PIPE:-32}
CONNS=${CONNS:-50}

require_tools

# Use redis-benchmark with -r <HOT_KEYS>: it picks key names from the
# range [0, HOT_KEYS) at random — exactly the small-hot-set shape we
# want (working set fits comfortably inside the L1/L2 footprint of the
# 512 KB fast-path table).  This avoids the redis-cli --pipe reply-
# parsing issues we hit with a custom RESP generator.
run_get_bench() {
  # 1) Populate the hot keys with VALUE_SIZE-byte values.
  #    -r HOT_KEYS *randomly* picks key indices, so we need many more
  #    SETs than keys to be sure every index is covered (otherwise GETs
  #    on unset keys permanently miss the fp_cache and dilute hit rate).
  redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t set \
      -n $((HOT_KEYS * 200)) -r "$HOT_KEYS" -c 50 -P 10 \
      -d "$VALUE_SIZE" -q >/dev/null 2>&1
  # 2) Pre-warm one short GET burst so the fast-path table is filled
  #    before measurement starts.
  redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t get \
      -n 50000 -r "$HOT_KEYS" -c "$CONNS" -P "$PIPE" -d "$VALUE_SIZE" -q \
      >/dev/null 2>&1
  # 3) Measured GET burst — print just the GET rps line.
  redis-benchmark -h 127.0.0.1 -p "$KVS_PORT" -t get \
      -n "$TOTAL_OPS" -r "$HOT_KEYS" -c "$CONNS" -P "$PIPE" -d "$VALUE_SIZE" \
      --csv 2>/dev/null \
      | awk -F',' '/^"GET"/ { gsub(/"/, "", $2); print $2; exit }'
}

info_fp() {
  redis-cli -p "$KVS_PORT" INFO stats | grep -E 'fastpath_cache_(enabled|hits|misses|hit_rate|writer_collisions)' || true
}

run_case() {
  local label="$1" conf="$2"
  stop_kvs
  start_kvs "$conf"
  redis-cli -p "$KVS_PORT" FLUSHALL >/dev/null
  sleep 0.2
  local qps
  qps=$(run_get_bench)
  echo "--- $label / $conf ---" >&2
  echo "GET_QPS=$qps" >&2
  info_fp >&2
  echo >&2
  echo "$qps"
}

ratio() {
  awk -v a="$1" -v b="$2" \
      'BEGIN { if (b > 0) printf "%.2f", (a-b)*100.0/b; else printf "nan" }'
}

OUT_TXT="$PROJECT_DIR/test/bench/fastpath_skewed_results.txt"
{
  echo "=== Phase 10 fastpath skewed-workload benchmark ==="
  echo "date: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "params: hot_keys=$HOT_KEYS value_size=$VALUE_SIZE total_ops=$TOTAL_OPS"
  echo
} | tee "$OUT_TXT"

base=$(run_case "skewed baseline" "kvstore-fastpath-off.conf" 2>>"$OUT_TXT")
fp=$(run_case   "skewed fastpath" "kvstore-fastpath.conf"     2>>"$OUT_TXT")
gain=$(ratio "$fp" "$base")

{
  echo "=== summary ==="
  printf "%-22s %12s %12s %10s\n" "scenario" "baseline" "fastpath" "gain"
  printf "%-22s %12s %12s %9s%%\n" "skewed (${HOT_KEYS} hot keys)" "$base" "$fp" "$gain"
} | tee -a "$OUT_TXT"

stop_kvs
