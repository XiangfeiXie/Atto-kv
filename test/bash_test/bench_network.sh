#!/bin/bash
# =============================================================================
# bench_network.sh  —  Network engine QPS & memory comparison
#
# Tests: reactor, proactor, ntyco
# Fixed: skiptable engine, no persistence
#
# Usage:
#   ./bench_network.sh
#   RB_CLIENTS=100 ./bench_network.sh
#   RB_VALSIZE=256 ./bench_network.sh
#
# Output:
#   prints summary table
#   saves CSV -> results/network_<ts>.csv
# =============================================================================

cd "$(dirname "$0")"
source ./common.sh

TS="$(date +%Y%m%d_%H%M%S)"
CSV="$RESULT_DIR/network_${TS}.csv"

NETS=("reactor" "proactor" "ntyco")
TAGS=("reactor" "proactor" "ntyco")

BASE_PORT=21200

banner "Network Engine Benchmark"

echo "  Value size:  ${RB_VALSIZE}B  |  Keyspace: $RB_KEYSPACE  |  Pipeline: $RB_PIPELINE"
echo ""

print_header

# CSV header
echo "name,engine,network,persist,set_qps,get_qps,vm_start_kb,vm_peak_kb,vm_end_kb,rss_start_kb,rss_peak_kb,rss_end_kb" > "$CSV"

for i in "${!NETS[@]}"; do
    net="${NETS[$i]}"
    tag="${TAGS[$i]}"
    port=$((BASE_PORT + i))

    echo -e "${C_BOLD}► network-$tag${C_NC}"

    run_full_bench "network-$tag" "$port" "skiptable" "$net" "none"

    print_result_row "skiptable" "$net" "none"

    echo "$R_NAME,skiptable,$net,none,$R_SET_QPS,$R_GET_QPS,$R_VM_START,$R_VM_PEAK,$R_VM_END,$R_RSS_START,$R_RSS_PEAK,$R_RSS_END" >> "$CSV"

    echo ""
done

echo ""
echo -e "${C_GREEN}Results saved: $CSV${C_NC}"
echo ""
