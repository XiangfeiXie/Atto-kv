#!/bin/bash
# =============================================================================
# bench_engine.sh  —  Storage engine QPS & memory comparison
#
# Tests: rbtree, hash, skiptable
# Fixed: reactor network, no persistence
#
# Usage:
#   ./bench_engine.sh                  # default params
#   RB_CLIENTS=100 ./bench_engine.sh   # custom concurrency
#   RB_VALSIZE=256 ./bench_engine.sh   # custom value size
#
# Output:
#   prints summary table
#   saves CSV -> results/engine_<ts>.csv
# =============================================================================

cd "$(dirname "$0")"
source ./common.sh

TS="$(date +%Y%m%d_%H%M%S)"
CSV="$RESULT_DIR/engine_${TS}.csv"

ENGINES=("rbtree" "hash" "skiptable")
TAGS=("rbtree" "hash" "skiptable")

BASE_PORT=21100

banner "Storage Engine Benchmark"

echo "  Value size:  ${RB_VALSIZE}B  |  Keyspace: $RB_KEYSPACE  |  Pipeline: $RB_PIPELINE"
echo ""

print_header

# CSV header
echo "name,engine,network,persist,set_qps,get_qps,vm_start_kb,vm_peak_kb,vm_end_kb,rss_start_kb,rss_peak_kb,rss_end_kb" > "$CSV"

for i in "${!ENGINES[@]}"; do
    eng="${ENGINES[$i]}"
    tag="${TAGS[$i]}"
    port=$((BASE_PORT + i))

    echo -e "${C_BOLD}► engine-$tag${C_NC}"

    run_full_bench "engine-$tag" "$port" "$eng" "reactor" "none"

    print_result_row "$eng" "reactor" "none"

    # Save to CSV
    echo "$R_NAME,$eng,reactor,none,$R_SET_QPS,$R_GET_QPS,$R_VM_START,$R_VM_PEAK,$R_VM_END,$R_RSS_START,$R_RSS_PEAK,$R_RSS_END" >> "$CSV"

    echo ""
done

echo ""
echo -e "${C_GREEN}Results saved: $CSV${C_NC}"
echo ""
