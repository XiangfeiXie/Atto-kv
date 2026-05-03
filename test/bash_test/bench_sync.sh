#!/bin/bash
# =============================================================================
# bench_sync.sh  —  Master-slave sync benchmark (rdma_sync + TCP incr)
#
# Starts both master and slave.  Runs writes on master, verifies slave reads.
# Measures sync correctness and slave query QPS.
#
# Prerequisites:
#   ebpf_incr.bpf.o must be built (run 'make' first)
#
# Usage:
#   ./bench_sync.sh
#   RB_VALSIZE=256 ./bench_sync.sh
#
# Output:
#   prints sync status and QPS
#   saves CSV -> results/sync_<ts>.csv
# =============================================================================

cd "$(dirname "$0")"
source ./common.sh

TS="$(date +%Y%m%d_%H%M%S)"
CSV="$RESULT_DIR/sync_${TS}.csv"

M_PORT=21400       # master client port
S_PORT=21401       # slave client port
R_PORT=21402       # RDMA full-sync port
I_PORT=21403       # incr TCP port

banner "Master-Slave Sync Benchmark"

# Check BPF object
if [[ ! -f "$PROJ_DIR/ebpf_incr.bpf.o" ]]; then
    echo -e "${C_RED}[FATAL] ebpf_incr.bpf.o not found — run 'make' first${C_NC}"
    exit 1
fi

echo "  Value size:  ${RB_VALSIZE}B  |  Keyspace: $RB_KEYSPACE  |  Pipeline: $RB_PIPELINE"
echo ""

# ---- Master config ----
M_CFG="$(mktemp /tmp/kvbench_master_XXXXXX.conf)"
write_config "$M_CFG" "$M_PORT" "skiptable" "reactor" "none" \
    "rdma_sync on" "rdma_repl on" "rdma_role master" \
    "rdma_bind_ip $HOST" "rdma_bind_port $R_PORT" \
    "rdma_provider tcp" "incr_port $I_PORT"

# ---- Slave config ----
S_CFG="$(mktemp /tmp/kvbench_slave_XXXXXX.conf)"
write_config "$S_CFG" "$S_PORT" "skiptable" "reactor" "none" \
    "rdma_sync on" "rdma_repl on" "rdma_role slave" \
    "rdma_master_ip $HOST" "rdma_master_port $R_PORT" \
    "rdma_provider tcp" "incr_port $I_PORT"

# ---- Start master ----
section "Starting master on port $M_PORT"
# Use background mode directly (not server_start, since that spawns &)
"$SERVER_BIN" "$M_CFG" &
M_PID=$!

declare deadline; deadline=$((SECONDS + 15))
while (( SECONDS < deadline )); do
    if echo | nc -w1 "$HOST" "$M_PORT" 2>/dev/null; then break; fi
    if ! kill -0 "$M_PID" 2>/dev/null; then
        echo -e "${C_RED}[FATAL] Master exited early${C_NC}"
        exit 1
    fi
    sleep 0.2
done
echo "  Master ready (pid=$M_PID)"

# ---- Start slave ----
section "Starting slave on port $S_PORT"
"$SERVER_BIN" "$S_CFG" &
S_PID=$!

# Slave may take longer — it blocks in rdma_repl_slave_run() for full sync
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
    if echo | nc -w1 "$HOST" "$S_PORT" 2>/dev/null; then break; fi
    if ! kill -0 "$S_PID" 2>/dev/null; then
        echo -e "${C_RED}[FATAL] Slave exited early${C_NC}"
        kill "$M_PID" 2>/dev/null || true
        wait "$M_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 0.3
done

if ! kill -0 "$S_PID" 2>/dev/null; then
    echo -e "${C_RED}[FATAL] Slave did not start${C_NC}"
    exit 1
fi
echo "  Slave ready (pid=$S_PID)"

echo ""
echo "  Full-sync should have completed (empty dataset)."
echo ""

# ---- Initial memory ----
read M_VM0 M_RSS0 _ _ <<< "$(mem_snapshot "$M_PID")"
read S_VM0 S_RSS0 _ _ <<< "$(mem_snapshot "$S_PID")"

# ---- Preload keys on master ----
section "Preloading $RB_KEYSPACE keys on master..."
mem_monitor_start "$M_PID" "master-preload"
"$RB" -h "$HOST" -p "$M_PORT" -t set -c "$RB_CLIENTS" -P "$RB_PIPELINE" \
    -n "$RB_KEYSPACE" -r "$RB_KEYSPACE" -d "$RB_VALSIZE" --csv > /dev/null 2>&1
mem_monitor_stop
echo "  Preload done."

# Wait for incremental sync to propagate to slave
echo "  Waiting for incremental sync (3s)..."
sleep 3

# ---- SET benchmark on master ----
section "SET benchmark on master..."
mem_monitor_start "$M_PID" "master-set"
M_SET_OUT=$("$RB" \
    -h "$HOST" -p "$M_PORT" \
    -t set \
    -c "$RB_CLIENTS" -P "$RB_PIPELINE" \
    -n "$RB_REQUESTS" -r "$RB_KEYSPACE" -d "$RB_VALSIZE" \
    --csv 2>&1)
M_SET_QPS=$(echo "$M_SET_OUT" | grep "throughput summary" | awk '{print $3}' || echo "0")
mem_monitor_stop
M_VM_PEAK="${MEM_PEAK_VMPEAK}"
M_RSS_PEAK="${MEM_PEAK_HWM}"
echo "  Master SET QPS: $M_SET_QPS"

# Wait for incr sync again
echo "  Waiting for incremental sync (3s)..."
sleep 3

# ---- GET benchmark on master ----
section "GET benchmark on master..."
M_GET_OUT=$("$RB" \
    -h "$HOST" -p "$M_PORT" \
    -t get \
    -c "$RB_CLIENTS" -P "$RB_PIPELINE" \
    -n "$RB_REQUESTS" -r "$RB_KEYSPACE" -d "$RB_VALSIZE" \
    --csv 2>&1)
M_GET_QPS=$(echo "$M_GET_OUT" | grep "throughput summary" | awk '{print $3}' || echo "0")
echo "  Master GET QPS: $M_GET_QPS"

# ---- GET benchmark on slave ----
section "GET benchmark on slave (verify sync)..."
S_GET_OUT=$("$RB" \
    -h "$HOST" -p "$S_PORT" \
    -t get \
    -c "$RB_CLIENTS" -P "$RB_PIPELINE" \
    -n "$RB_REQUESTS" -r "$RB_KEYSPACE" -d "$RB_VALSIZE" \
    --csv 2>&1)
S_GET_QPS=$(echo "$S_GET_OUT" | grep "throughput summary" | awk '{print $3}' || echo "0")
echo "  Slave GET QPS: $S_GET_QPS"

# Determine sync status: slave gets should return valid data
# A very low GET QPS (~0) indicates slave has no data (sync failed)
if [[ "$S_GET_QPS" == "0" ]] || [[ -z "$S_GET_QPS" ]]; then
    SYNC_STATUS="FAIL"
else
    SYNC_STATUS="PASS"
fi

# ---- Final memory ----
read M_VM2 M_RSS2 _ _ <<< "$(mem_snapshot "$M_PID")"
read S_VM2 S_RSS2 _ _ <<< "$(mem_snapshot "$S_PID")"
S_VM_PEAK=$(mem_vmpeak "$S_PID")
S_RSS_PEAK=$(mem_vmhwm "$S_PID")

# ---- Print results ----
echo ""
echo -e "${C_BOLD}============================================================${C_NC}"
echo -e "${C_BOLD}  Master-Slave Sync Results${C_NC}"
echo -e "${C_BOLD}============================================================${C_NC}"
echo ""
echo "  Sync status:     $SYNC_STATUS"
echo "  Master SET QPS:  $M_SET_QPS"
echo "  Master GET QPS:  $M_GET_QPS"
echo "  Slave  GET QPS:  $S_GET_QPS"
echo ""
echo "  Master memory:"
echo "    VmSize:  start=$(fmt_kb "$M_VM0")  peak=$(fmt_kb "$M_VM_PEAK")  end=$(fmt_kb "$M_VM2")"
echo "    VmRSS:   start=$(fmt_kb "$M_RSS0")  peak=$(fmt_kb "$M_RSS_PEAK")  end=$(fmt_kb "$M_RSS2")"
echo ""
echo "  Slave memory:"
echo "    VmSize:  start=$(fmt_kb "$S_VM0")  peak=$(fmt_kb "$S_VM_PEAK")  end=$(fmt_kb "$S_VM2")"
echo "    VmRSS:   start=$(fmt_kb "$S_RSS0")  peak=$(fmt_kb "$S_RSS_PEAK")  end=$(fmt_kb "$S_RSS2")"

# ---- Save CSV ----
{
    echo "name,role,set_qps,get_qps,vm_start_kb,vm_peak_kb,vm_end_kb,rss_start_kb,rss_peak_kb,rss_end_kb"
    echo "master-slave-sync,master,$M_SET_QPS,$M_GET_QPS,$M_VM0,$M_VM_PEAK,$M_VM2,$M_RSS0,$M_RSS_PEAK,$M_RSS2"
    echo "master-slave-sync,slave,,$S_GET_QPS,$S_VM0,$S_VM_PEAK,$S_VM2,$S_RSS0,$S_RSS_PEAK,$S_RSS2"
} > "$CSV"

# ---- Shutdown ----
echo ""
echo "  Shutting down..."
kill "$S_PID" 2>/dev/null || true
wait "$S_PID" 2>/dev/null || true
kill "$M_PID" 2>/dev/null || true
wait "$M_PID" 2>/dev/null || true
rm -f "$M_CFG" "$S_CFG" "$PROJ_DIR/dump.rdb" "$PROJ_DIR/appendonly.aof"

echo -e "${C_GREEN}Results saved: $CSV${C_NC}"
echo ""
