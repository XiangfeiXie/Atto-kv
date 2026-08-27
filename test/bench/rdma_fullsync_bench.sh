#!/bin/bash
# =============================================================================
# rdma_fullsync_bench.sh — kvstore fullsync benchmark (sendfile vs RDMA)
#
# Usage:
#   echo "passwd" | sudo -S true && bash test/bench/rdma_fullsync_bench.sh
#   bash test/bench/rdma_fullsync_bench.sh --mode sendfile
#   bash test/bench/rdma_fullsync_bench.sh --keys 100000 --iterations 5
# =============================================================================
set -u; shopt -s nullglob

# ── Defaults ───────────────────────────────────────────────────────────
KEYS=400000; VSIZE=256; ITERATIONS=3; PORT=2000
MODE=both; RDMA_IP=""; TIMEOUT=120
SLAVE_PORT=$((PORT + 2)); FPORT=$((PORT + 1))
PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="${PROJECT_DIR}/build/kvstore"
RDB_PATH="${PROJECT_DIR}/build/dump.rdb"
POP_RDB_PATH="${PROJECT_DIR}/build/populate.rdb"
LOG_DIR="/tmp/kvs_fsync_bench_$$"; mkdir -p "$LOG_DIR"

# ── Helpers ─────────────────────────────────────────────────────────────
C_BLD=$'\033[1m'; C_DIM=$'\033[2m'
C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_CYN=$'\033[36m'; C_RST=$'\033[0m'
die()  { echo "${C_RED}[FATAL]${C_RST} $*" >&2; exit 1; }
info() { echo "${C_GRN}[INFO]${C_RST}  $*" >&2; }
warn() { echo "${C_YEL}[WARN]${C_RST}  $*" >&2; }
step() { echo "${C_CYN}[STEP]${C_RST}  $*" >&2; }

_fmt() { local v=$1; [ "${v:0:1}" = "." ] && echo "0$v" || echo "$v"; }
calc_mbps() {
    if [ "$(echo "$2 > 0" | bc 2>/dev/null)" = 1 ]; then
        _fmt "$(echo "scale=2; $1 / $2 / 1048576" | bc)"
    else echo "0.00"; fi
}
calc_entps() {
    if [ "$(echo "$2 > 0" | bc 2>/dev/null)" = 1 ]; then
        echo "scale=0; $1 / $2" | bc
    else echo "0"; fi
}

usage() {
    cat <<EOF
Usage: $0 [options]

kvstore fullsync benchmark — sendfile (TCP) vs RDMA verbs

Options:
  --keys N         Key count (default: $KEYS, ~140MB RDB)
  --vsize B        Value size in bytes (default: $VSIZE)
  --iterations N   Runs per mode (default: $ITERATIONS)
  --port P         Master base port (default: $PORT)
  --mode MODE      sendfile | rdma | both (default: $MODE)
  --rdma-ip IP     RDMA device IP (auto-detect if unset)
  -h, --help       Show this help

Prerequisites for RDMA mode:
  - Soft-RoCE (rxe) or hardware RDMA device
  - memlock=unlimited (2 GB needed for slave ring buffers)
    Run with:  echo "passwd" | sudo -S prlimit --pid \$\$ --memlock=unlimited
    Then:       bash test/bench/rdma_fullsync_bench.sh
EOF
    exit 0
}

# ── Arg parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --keys)       KEYS="$2";       shift 2 ;;
        --vsize)      VSIZE="$2";      shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --port)       PORT="$2";       SLAVE_PORT=$((PORT + 2)); FPORT=$((PORT + 1)); shift 2 ;;
        --mode)       MODE="$2";       shift 2 ;;
        --rdma-ip)    RDMA_IP="$2";    shift 2 ;;
        -h|--help)    usage ;;
        *) die "Unknown option: $1 (use -h for help)" ;;
    esac
done

[[ "$MODE" =~ ^(sendfile|rdma|both)$ ]] || die "Invalid mode: $MODE"
[[ "$KEYS" -gt 0 ]] 2>/dev/null || die "Invalid keys: $KEYS"

# ── Tool checks ──────────────────────────────────────────────────────────
for tool in redis-cli python3 bc; do
    command -v "$tool" >/dev/null 2>&1 || die "Missing: $tool"
done
[ -x "$KVS" ] || die "kvstore not built: $KVS"

# ── RDMA device detection ────────────────────────────────────────────────
detect_rdma_ip() {
    [ -n "$RDMA_IP" ] && { info "RDMA IP (user): $RDMA_IP"; return 0; }
    local ip
    ip=$(ip -4 addr show rxe0 2>/dev/null | grep -oP 'inet \K[\d.]+' | head -1)
    [ -n "$ip" ] && { RDMA_IP="$ip"; info "RDMA IP (rxe0): $RDMA_IP"; return 0; }
    ip=$(hostname -I 2>/dev/null | awk '{print $1}')
    RDMA_IP="${ip:-127.0.0.1}"
    info "RDMA IP (fallback): $RDMA_IP"
}
detect_rdma_ip

# ── Memlock check & raise (CRITICAL for RDMA) ────────────────────────────
ensure_memlock() {
    local cur
    cur=$(ulimit -l 2>/dev/null || echo 0)
    if [ "$cur" = "unlimited" ]; then
        info "memlock: unlimited ✓"
        return 0
    fi
    # Need ~2 GB for 8×256MB slave ring buffers
    if [ "$cur" -ge 2097152 ]; then
        info "memlock: ${cur} KB (≥2 GB) ✓"
        return 0
    fi
    warn "memlock=${cur} KB — need ≥2 GB for RDMA buffer registration"
    warn "Attempting to raise..."

    # Try passwordless sudo first
    if command -v sudo &>/dev/null && sudo -n true 2>/dev/null; then
        sudo prlimit --pid $$ --memlock=unlimited 2>/dev/null && {
            info "memlock raised to unlimited ✓"; return 0;
        }
    fi
    # Try direct prlimit (needs CAP_SYS_RESOURCE)
    prlimit --pid $$ --memlock=unlimited 2>/dev/null && {
        info "memlock raised to unlimited ✓"; return 0;
    }
    # Prompt user
    warn "Cannot raise memlock automatically."
    warn "Run this before executing the script:"
    warn "  echo 'password' | sudo -S prlimit --pid \$\$ --memlock=unlimited"
    warn "  Then: bash test/bench/rdma_fullsync_bench.sh [options]"
    return 1
}

if [ "$MODE" = "rdma" ] || [ "$MODE" = "both" ]; then
    if [ ! -d /sys/class/infiniband/rxe0 ] && [ -z "$(ls -A /sys/class/infiniband 2>/dev/null)" ]; then
        die "No RDMA device found. RDMA mode requires rxe (Soft-RoCE) or hardware RDMA."
    fi
    ensure_memlock || die "memlock insufficient for RDMA mode"
fi

# ── Process/port management ──────────────────────────────────────────────
kvskill() {
    local i pid
    { pkill -9 -x kvstore 2>/dev/null; wait 2>/dev/null; } 2>/dev/null || true
    for pid in $(pgrep -x kvstore 2>/dev/null || true); do
        { kill -9 "$pid" 2>/dev/null; wait 2>/dev/null; } 2>/dev/null || true
    done
    for ((i=0; i<20; i++)); do
        if ! ss -tlnp 2>/dev/null | grep -qE ":($PORT|$SLAVE_PORT|$((PORT+1)))\b"; then
            break
        fi
        sleep 0.1
    done
    sleep 0.5
}

cleanup() {
    kvskill
    wait 2>/dev/null || true
    rm -f "$RDB_PATH" "${RDB_PATH}.tmp" /tmp/kvstore_fs_*.rdb 2>/dev/null
    rm -f "${LOG_DIR}"/*.conf "${LOG_DIR}"/*.log 2>/dev/null
}
trap cleanup EXIT

# wait_port port timeout [pid] — if pid given, fails fast when process dies
wait_port() {
    local p=$1 timeout=${2:-30} pid=${3:-0} i
    for ((i=0; i<timeout*2; i++)); do
        timeout 0.5 bash -c "exec 3<>/dev/tcp/127.0.0.1/$p" 2>/dev/null && return 0
        if [ "$pid" -gt 0 ] && ! kill -0 "$pid" 2>/dev/null; then
            return 1  # process died → fail fast
        fi
        sleep 0.5
    done
    return 1
}

# ── Config generation ────────────────────────────────────────────────────
write_configs() {
    local transport=$1 mconf=$2 sconf=$3
    # Master
    cat > "$mconf" <<EOF
port ${PORT}
network proactor
maxclients 100
persistence rdb
repl_role master
fullsync_listen_ip ${RDMA_IP}
fullsync_port ${FPORT}
fullsync_transport ${transport}
expire off
EOF
    # Slave
    cat > "$sconf" <<EOF
port ${SLAVE_PORT}
network proactor
maxclients 100
persistence none
repl_role slave
master_port ${PORT}
fullsync_master_ip ${RDMA_IP}
fullsync_port ${FPORT}
fullsync_transport ${transport}
expire off
EOF
}

# ── Phase 1: Data population ─────────────────────────────────────────────
# Uses populate_kv.py: streaming single-TCP-connection non-blocking pipeline,
# dramatically faster than spawning python3 per-batch (was ~200× spawns for 400K)
populate_data() {
    step "Phase 1: Populating $KEYS keys × ${VSIZE}B..."

    kvskill; rm -f "$RDB_PATH"

    cat > "${LOG_DIR}/populate.conf" <<EOF
port $PORT
network reactor
maxclients 1000
persistence rdb
expire off
EOF

    "$KVS" "${LOG_DIR}/populate.conf" > "${LOG_DIR}/populate.log" 2>&1 &
    local kvs_pid=$!

    if ! wait_port "$PORT" 30 "$kvs_pid"; then
        die "Populate instance failed to start (check ${LOG_DIR}/populate.log)"
    fi

    local POPULATE_PY="${PROJECT_DIR}/test/bench/populate_kv.py"
    [ -f "$POPULATE_PY" ] || die "populate_kv.py not found: $POPULATE_PY"

    local keys retries=0 WORKERS=4
    while true; do
        info "  Writing $KEYS keys (streaming, ${WORKERS} parallel connections)..."

        # Split key range across parallel workers for max throughput
        local chunk=$((KEYS / WORKERS)) w
        local -a worker_pids=()
        for ((w=0; w<WORKERS; w++)); do
            local w_start=$((w * chunk))
            local w_end=$((w_start + chunk - 1))
            [ $w -eq $((WORKERS - 1)) ] && w_end=$((KEYS - 1))
            python3 "$POPULATE_PY" 127.0.0.1 "$PORT" "$w_start" "$w_end" "$VSIZE" \
                > "${LOG_DIR}/pop_worker_${w}.out" \
                2> "${LOG_DIR}/pop_worker_${w}.err" &
            worker_pids+=($!)
        done

        # Wait for all workers; show tail of each on failure
        local failed=0
        for ((w=0; w<WORKERS; w++)); do
            wait "${worker_pids[w]}" 2>/dev/null || {
                warn "  Worker $w exited with error:"
                tail -3 "${LOG_DIR}/pop_worker_${w}.err" >&2
                failed=$((failed + 1))
            }
        done
        [ "$failed" -gt 0 ] && warn "  $failed/$WORKERS workers reported errors (may be harmless)"

        keys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
        if [ "$keys" -ge "$KEYS" ]; then break; fi

        retries=$((retries + 1))
        if [ "$retries" -ge 3 ]; then
            warn "Population incomplete: expected $KEYS, got $keys (after $retries retries)"
            kvskill; return 1
        fi
        warn "  Incomplete ($keys/$KEYS), retry $retries..."
    done

    # SAVE + record RDB size
    redis-cli -p "$PORT" SAVE >/dev/null 2>&1
    for ((i=0; i<120; i++)); do
        [ -f "$RDB_PATH" ] && [ -s "$RDB_PATH" ] && break
        sleep 0.5
    done

    POP_RDB_SIZE=$(stat -c %s "$RDB_PATH" 2>/dev/null || echo 0)
    [ "$POP_RDB_SIZE" -eq 0 ] && die "RDB file empty — SAVE failed"

    cp "$RDB_PATH" "$POP_RDB_PATH"
    POP_RDB_MB=$(awk "BEGIN { printf \"%.2f\", ${POP_RDB_SIZE}/1048576 }")
    info "Population done: $keys keys, RDB = $POP_RDB_MB MB ($POP_RDB_SIZE bytes)"
    kvskill; rm -f "$RDB_PATH"
    return 0
}

# ── Phase 2/3: Single test run ───────────────────────────────────────────
run_one() {
    local mode=$1 iteration=$2
    local mlog="${LOG_DIR}/${mode}_m_${iteration}.log"
    local slog="${LOG_DIR}/${mode}_s_${iteration}.log"
    local mconf="${LOG_DIR}/${mode}_master.conf"
    local sconf="${LOG_DIR}/${mode}_slave.conf"

    # Restore populated RDB
    rm -f "$RDB_PATH"; cp "$POP_RDB_PATH" "$RDB_PATH"

    write_configs "$mode" "$mconf" "$sconf"

    # Start master
    "$KVS" "$mconf" > "$mlog" 2>&1 &
    local mpid=$!
    if ! wait_port "$PORT" 20; then
        warn "[$mode #$iteration] Master startup failed"
        kvskill; return 1
    fi
    sleep 0.5

    # Verify master data
    local mkeys
    mkeys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
    if [ "$mkeys" -ne "$KEYS" ]; then
        warn "[$mode #$iteration] Master data: expected $KEYS, got $mkeys"
        kvskill; return 1
    fi

    # Start slave — this triggers inline fullsync
    local t0 t1 total_sec
    t0=$(date +%s.%N)

    timeout "$TIMEOUT" "$KVS" "$sconf" > "$slog" 2>&1 &
    local spid=$!

    # Wait for slave port — BUT check process liveness to fail fast
    if ! wait_port "$SLAVE_PORT" "$TIMEOUT" "$spid"; then
        if ! kill -0 "$spid" 2>/dev/null; then
            local ec; wait "$spid" 2>/dev/null; ec=$?
            warn "[$mode #$iteration] Slave died (exit=$ec) — check memlock!"
            echo "  Slave stderr:" >&2
            grep -E 'rdma_reg_write|Cannot allocate|error|Error|die' "$slog" 2>/dev/null | head -5 >&2 || true
        else
            warn "[$mode #$iteration] Slave startup timeout (${TIMEOUT}s)"
        fi
        kvskill; return 1
    fi

    t1=$(date +%s.%N)
    total_sec=$(echo "$t1 - $t0" | bc)

    # Verify slave data (with retries)
    local skeys=0 i
    for ((i=0; i<5; i++)); do
        skeys=$(redis-cli -p "$SLAVE_PORT" DBSIZE 2>/dev/null || echo 0)
        [[ "$skeys" =~ ^[0-9]+$ ]] && [ "$skeys" -gt 0 ] && break
        sleep 0.2
    done
    if [ "$skeys" -ne "$KEYS" ]; then
        warn "[$mode #$iteration] Slave data: expected $KEYS, got $skeys"
    fi

    # ── Parse results from logs ──────────────────────────────────────────
    local xfer_bytes xfer_ms xfer_mbps prep_ms batches ent_sec
    if [ "$mode" = "sendfile" ]; then
        local xfer_line
        xfer_line=$(grep '\[sendfile\] xfer-end:' "$mlog" 2>/dev/null | tail -1)
        [ -z "$xfer_line" ] && xfer_line=$(grep '\[sendfile\] xfer-end:' "$slog" 2>/dev/null | tail -1)
        xfer_bytes=$(echo "$xfer_line" | grep -oP 'bytes=\K[0-9]+' || echo "0")
        xfer_ms=$(echo "$xfer_line" | grep -oP 'ms=\K[0-9.]+' || echo "0")
        prep_ms="0"; batches="0"
        TOTAL_MBPS=$(calc_mbps "$POP_RDB_SIZE" "$total_sec")
        XFER_MBPS_CALC=$(calc_mbps "$POP_RDB_SIZE" "$(echo "scale=6; $xfer_ms / 1000" | bc)")
        ENT_SEC=$(calc_entps "$KEYS" "$total_sec")
        echo "$mode $iteration $KEYS $POP_RDB_SIZE $POP_RDB_MB $total_sec $TOTAL_MBPS $xfer_ms $prep_ms $XFER_MBPS_CALC $ENT_SEC $skeys $batches"
    else
        local xfer_line
        xfer_line=$(grep '\[rdma\] xfer-end:' "$mlog" 2>/dev/null | tail -1)
        xfer_bytes=$(echo "$xfer_line" | grep -oP 'bytes=\K[0-9]+' || echo "0")
        xfer_ms=$(echo "$xfer_line" | grep -oP 'xfer=\K[0-9.]+' || echo "0")
        prep_ms=$(echo "$xfer_line" | grep -oP 'prep=\K[0-9.]+' || echo "0")
        batches=$(echo "$xfer_line" | grep -oP 'batches=\K[0-9]+' || echo "0")
        TOTAL_MBPS=$(calc_mbps "$POP_RDB_SIZE" "$total_sec")
        XFER_MBPS_CALC=$(calc_mbps "$POP_RDB_SIZE" "$(echo "scale=6; $xfer_ms / 1000" | bc)")
        ENT_SEC=$(calc_entps "$KEYS" "$total_sec")
        echo "$mode $iteration $KEYS $POP_RDB_SIZE $POP_RDB_MB $total_sec $TOTAL_MBPS $xfer_ms $prep_ms $XFER_MBPS_CALC $ENT_SEC $batches"
    fi
}

# ── Stats ────────────────────────────────────────────────────────────────
calc_stats() {
    local mode=$1
    local total_sec_sum=0 xfer_ms_sum=0 total_mbps_sum=0 xfer_mbps_sum=0 ent_sec_sum=0
    local count=0 min_total_mbps="" max_total_mbps="" min_xfer_ms="" max_xfer_ms=""
    local keys="" rdb_mb=""

    while IFS=' ' read -r _mode _iter _keys _rdb_sz _rdb_mb _total_sec _total_mbps _xfer_ms _prep_ms _xfer_mbps _ent_sec _extra; do
        [ "$_mode" != "$mode" ] && continue
        count=$((count + 1)); keys=$_keys; rdb_mb=$_rdb_mb
        total_sec_sum=$(echo "$total_sec_sum + $_total_sec" | bc)
        xfer_ms_sum=$(echo "$xfer_ms_sum + $_xfer_ms" | bc)
        total_mbps_sum=$(echo "$total_mbps_sum + $_total_mbps" | bc)
        xfer_mbps_sum=$(echo "$xfer_mbps_sum + $_xfer_mbps" | bc)
        ent_sec_sum=$(echo "$ent_sec_sum + $_ent_sec" | bc)
        [ -z "$min_total_mbps" ] || [ "$(echo "$_total_mbps < $min_total_mbps" | bc)" = 1 ] && min_total_mbps=$_total_mbps
        [ -z "$max_total_mbps" ] || [ "$(echo "$_total_mbps > $max_total_mbps" | bc)" = 1 ] && max_total_mbps=$_total_mbps
        [ -z "$min_xfer_ms" ] || [ "$(echo "$_xfer_ms < $min_xfer_ms" | bc)" = 1 ] && min_xfer_ms=$_xfer_ms
        [ -z "$max_xfer_ms" ] || [ "$(echo "$_xfer_ms > $max_xfer_ms" | bc)" = 1 ] && max_xfer_ms=$_xfer_ms
    done < "${LOG_DIR}/results.txt"

    [ "$count" -eq 0 ] && { echo "0 0 0 0 0 0 0 0 0 0 0 0"; return; }
    local avg_total_sec avg_xfer_ms avg_total_mbps avg_xfer_mbps avg_ent_sec
    avg_total_sec=$(_fmt "$(echo "scale=3; $total_sec_sum / $count" | bc)")
    avg_xfer_ms=$(_fmt "$(echo "scale=1; $xfer_ms_sum / $count" | bc)")
    avg_total_mbps=$(_fmt "$(echo "scale=2; $total_mbps_sum / $count" | bc)")
    avg_xfer_mbps=$(_fmt "$(echo "scale=2; $xfer_mbps_sum / $count" | bc)")
    avg_ent_sec=$(echo "scale=0; $ent_sec_sum / $count" | bc)
    echo "$keys $rdb_mb $count $avg_total_sec $min_total_mbps $avg_total_mbps $max_total_mbps $avg_xfer_ms $min_xfer_ms $avg_xfer_mbps $max_xfer_ms $avg_ent_sec"
}

# ── Report ────────────────────────────────────────────────────────────────
print_report() {
    echo ""
    echo "══════════════════════════════════════════════════════════════════════"
    echo "  ${C_BLD}kvstore Fullsync Benchmark${C_RST} — KEYS=$KEYS  VSIZE=$VSIZE B"
    echo "  RDMA IP: $RDMA_IP   RDB Size: $POP_RDB_MB MB"
    echo "══════════════════════════════════════════════════════════════════════"
    echo ""
    printf "  %-10s %6s %8s %8s %8s %11s %10s %11s %8s\n" \
        "MODE" "KEYS" "RDB_MB" "WALL_S" "TOT_MB/s" "NET_MS" "PREP_MS" "NET_MB/s" "ENT/s"

    for mode in sendfile rdma; do
        [ "$MODE" != "both" ] && [ "$MODE" != "$mode" ] && continue

        while IFS=' ' read -r _m _i keys rdb_sz rdb_mb total_sec tmbps xms pms xmbps es _x; do
            [ "$_m" != "$mode" ] && continue
            printf "  %-10s %6s %8s %8.2f %8s %11s %10s %11s %8s\n" \
                "$_m" "$keys" "$rdb_mb" "$total_sec" "$tmbps" "$xms" "$pms" "$xmbps" "$es"
        done < "${LOG_DIR}/results.txt"

        local stats; stats=$(calc_stats "$mode")
        read keys rdb_mb cnt avg_ts min_tmbps avg_tmbps max_tmbps avg_xms min_xms avg_xmbps max_xms avg_es <<<"$stats"
        [ "$cnt" -gt 0 ] && printf "  ${C_CYN}%-10s${C_RST} ${C_DIM}(avg n=$cnt)${C_RST} %8.2f ${C_BLD}%8s${C_RST} %11s %10s ${C_BLD}%11s${C_RST} %8s\n" \
            "$mode" "$avg_ts" "$avg_tmbps" "$avg_xms" "" "$avg_xmbps" "$avg_es"
    done

    echo ""
    echo "  ${C_DIM}Wall = slave startup → ready.  NET = pure transfer (excl. serialize).${C_RST}"
    echo "  ${C_DIM}Throughput = RDB file size / transfer time / 1048576${C_RST}"
    echo ""
}

# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════
echo "${C_BLD}kvstore Fullsync Benchmark${C_RST}" >&2
echo "  keys=$KEYS  vsize=$VSIZE  iterations=$ITERATIONS  mode=$MODE" >&2
echo "  port=$PORT  slave_port=$SLAVE_PORT  rdma_ip=$RDMA_IP" >&2
echo "  memlock=$(ulimit -l)" >&2

cleanup

# Phase 1: Populate
populate_data || die "Data population failed"

# Phase 2-3: Warmup + test runs
> "${LOG_DIR}/results.txt"

for mode in sendfile rdma; do
    [ "$MODE" != "both" ] && [ "$MODE" != "$mode" ] && continue

    step "Phase: $mode ($ITERATIONS iterations + warmup)"

    # Warmup
    info "  warmup ..."
    run_one "$mode" 0 >/dev/null 2>&1 || warn "  warmup failed, continuing"
    cleanup

    # Test iterations
    ok=0; fail=0
    for ((i=1; i<=ITERATIONS; i++)); do
        info "  run $i/$ITERATIONS ..."
        if result=$(run_one "$mode" "$i"); then
            echo "$result" >> "${LOG_DIR}/results.txt"
            ok=$((ok + 1))
            read _m _i keys rdb_sz rdb_mb total_sec tmbps xms pms xmbps es _x <<<"$result"
            printf "    ${C_DIM}wall=%.1fs  net=%sms  prep=%sms  net_mb/s=%s  ent/s=%s${C_RST}\n" \
                "$total_sec" "$xms" "$pms" "$xmbps" "$es"
        else
            fail=$((fail + 1))
            warn "  run $i FAILED"
        fi
        cleanup
    done
    info "  $mode: $ok passed, $fail failed"
done

# Phase 4: Report
print_report

# Cleanup temp files
rm -f "${LOG_DIR}"/*.conf "${LOG_DIR}"/*.log "$POP_RDB_PATH" 2>/dev/null
[ "$(ls -A "$LOG_DIR" 2>/dev/null)" ] || rmdir "$LOG_DIR" 2>/dev/null
info "Done. Logs: ${LOG_DIR}/results.txt"
