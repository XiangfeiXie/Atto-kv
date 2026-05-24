#!/bin/bash
# =============================================================================
# test_simple_perf.sh — Simple performance test without persistence/replication
#
# Usage:
#   ./test_simple_perf.sh
# =============================================================================

set -e

KVS_PORT=7379
KVS_BIN="./build/kvstore"
KVS_CONF="test_perf_simple.conf"
KVS_PID=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

# Check if kvstore binary exists
if [ ! -f "$KVS_BIN" ]; then
    error "kvstore binary not found at $KVS_BIN"
    error "Please run: make clean && make"
    exit 1
fi

# Check if config exists
if [ ! -f "$KVS_CONF" ]; then
    error "Config file not found at $KVS_CONF"
    exit 1
fi

# Check if redis-benchmark is available
if ! command -v redis-benchmark &> /dev/null; then
    error "redis-benchmark not found. Please install redis-tools"
    exit 1
fi

# Stop any existing kvstore process
stop_kvs() {
    log "Stopping any existing kvstore processes..."
    pkill -9 kvstore 2>/dev/null || true
    sleep 1
}

# Start kvstore
start_kvs() {
    log "Starting kvstore with config: $KVS_CONF"
    $KVS_BIN $KVS_CONF &
    KVS_PID=$!
    sleep 2

    if ! kill -0 $KVS_PID 2>/dev/null; then
        error "Failed to start kvstore"
        exit 1
    fi

    log "kvstore started with PID: $KVS_PID"
}

# Run benchmark
run_bench() {
    local test_name=$1
    local cmd=$2
    local n=$3
    local c=$4
    local p=$5
    local d=$6

    log "Running: $test_name (n=$n, c=$c, P=$p, d=$d)"

    local result=$(redis-benchmark -h 127.0.0.1 -p $KVS_PORT -t $cmd \
        -n $n -c $c -P $p -d $d -q 2>&1 | grep -oP '\d+\.\d+ requests per second' | grep -oP '\d+\.\d+' || echo "0")

    if [ "$result" = "0" ]; then
        warn "Benchmark failed or returned 0"
        return 1
    fi

    echo "$result"
}

# Cleanup on exit
cleanup() {
    log "Cleaning up..."
    if [ -n "$KVS_PID" ]; then
        kill -TERM $KVS_PID 2>/dev/null || true
        sleep 1
        kill -9 $KVS_PID 2>/dev/null || true
    fi
    stop_kvs
}

trap cleanup EXIT INT TERM

# Main test
main() {
    echo ""
    echo "========================================================================"
    echo "  KVStore Simple Performance Test"
    echo "  Config: threads=1, no persistence, no replication"
    echo "========================================================================"
    echo ""

    stop_kvs
    start_kvs

    # Test parameters
    BENCH_N=100000
    BENCH_C=50
    PIPELINES=(1 10 50 100)
    DATA_SIZE=16

    echo ""
    log "Test parameters:"
    log "  Requests: $BENCH_N"
    log "  Connections: $BENCH_C"
    log "  Pipeline depths: ${PIPELINES[*]}"
    log "  Data size: $DATA_SIZE bytes"
    echo ""

    # Results storage
    declare -A RESULTS_SET
    declare -A RESULTS_GET

    # Run tests for each pipeline depth
    for P in "${PIPELINES[@]}"; do
        echo "────────────────────────────────────────────────────────────────────"
        log "Testing with Pipeline depth = $P"
        echo "────────────────────────────────────────────────────────────────────"

        # SET test
        set_qps=$(run_bench "SET" "set" $BENCH_N $BENCH_C $P $DATA_SIZE)
        RESULTS_SET[$P]=$set_qps
        log "SET QPS: $set_qps"

        # Warmup for GET test
        log "Warming up for GET test..."
        redis-benchmark -h 127.0.0.1 -p $KVS_PORT -t set \
            -n 10000 -c $BENCH_C -P $P -d $DATA_SIZE -q >/dev/null 2>&1 || true

        # GET test
        get_qps=$(run_bench "GET" "get" $BENCH_N $BENCH_C $P $DATA_SIZE)
        RESULTS_GET[$P]=$get_qps
        log "GET QPS: $get_qps"

        echo ""
    done

    # Summary table
    echo ""
    echo "========================================================================"
    echo "  Performance Summary"
    echo "========================================================================"
    echo ""
    printf "  %-10s | %15s | %15s\n" "Pipeline" "SET (req/s)" "GET (req/s)"
    printf "  %-10s | %15s | %15s\n" "----------" "---------------" "---------------"

    for P in "${PIPELINES[@]}"; do
        set_qps=${RESULTS_SET[$P]:-0}
        get_qps=${RESULTS_GET[$P]:-0}
        printf "  P=%-8s | %15s | %15s\n" "$P" "$set_qps" "$get_qps"
    done

    echo ""
    echo "========================================================================"
    echo ""

    log "Test completed successfully"
}

main
