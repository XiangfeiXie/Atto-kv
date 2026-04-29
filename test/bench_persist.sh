#!/bin/bash
# =============================================================================
# bench_persist.sh  —  Persistence mode QPS & memory comparison
#
# Tests: none, rdb, aof, both
# Fixed: skiptable engine, reactor network
#
# Note: persistence files (dump.rdb, appendonly.aof) are cleaned between runs.
#
# Usage:
#   ./bench_persist.sh
#   RB_VALSIZE=256 ./bench_persist.sh
#
# Output:
#   prints summary table
#   saves CSV -> results/persist_<ts>.csv
# =============================================================================

cd "$(dirname "$0")"
source ./common.sh

TS="$(date +%Y%m%d_%H%M%S)"
CSV="$RESULT_DIR/persist_${TS}.csv"

PERSISTS=("none" "rdb" "aof" "both")
TAGS=("none" "rdb" "aof" "both")

BASE_PORT=21300

banner "Persistence Mode Benchmark"

echo "  Value size:  ${RB_VALSIZE}B  |  Keyspace: $RB_KEYSPACE  |  Pipeline: $RB_PIPELINE"
echo ""

print_header

# CSV header
echo "name,engine,network,persist,set_qps,get_qps,vm_start_kb,vm_peak_kb,vm_end_kb,rss_start_kb,rss_peak_kb,rss_end_kb" > "$CSV"

for i in "${!PERSISTS[@]}"; do
    p="${PERSISTS[$i]}"
    tag="${TAGS[$i]}"
    port=$((BASE_PORT + i))

    echo -e "${C_BOLD}► persist-$tag${C_NC}"

    run_full_bench "persist-$tag" "$port" "skiptable" "reactor" "$p"

    print_result_row "skiptable" "reactor" "$p"

    echo "$R_NAME,skiptable,reactor,$p,$R_SET_QPS,$R_GET_QPS,$R_VM_START,$R_VM_PEAK,$R_VM_END,$R_RSS_START,$R_RSS_PEAK,$R_RSS_END" >> "$CSV"

    echo ""
done

echo ""
echo -e "${C_GREEN}Results saved: $CSV${C_NC}"
echo ""
