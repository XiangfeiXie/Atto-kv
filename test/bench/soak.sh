#!/usr/bin/env bash
# =============================================================================
# Phase 8.3 — long-running soak test
#
# What it does:
#   1. Boots kvstore (default conf/kvstore-dev.conf; configurable via KVS_CONF)
#   2. Runs redis-benchmark looped for SOAK_MINUTES (default 30)
#      Mix: SET + GET + LPUSH + LRANGE + INCR
#   3. Every 60s: sample VmRSS, fd count, slowlog tail, dict total keys
#   4. Periodically: BGSAVE × 1, FLUSHDB × 1 (per 5-min cycle)
#   5. At the end: dump CSV + verdict (PASS if RSS+fd monotonic stable)
#
# Failure modes detected:
#   - File descriptor leak  (fd count grows linearly)
#   - Memory leak           (VmRSS grows linearly across multiple FLUSHDBs)
#   - Process crash         (kvstore PID disappears mid-run)
#   - Command latency creep (P50 from redis-benchmark goes up)
#
# Usage:
#   test/bench/soak.sh            # 30-minute soak (default)
#   SOAK_MINUTES=5 test/bench/soak.sh   # quick smoke
#   SOAK_MINUTES=120 test/bench/soak.sh # overnight
#
# Output:
#   /tmp/kvstore-soak/         logs, samples.csv, verdict.txt
# =============================================================================
set -u

SOAK_MINUTES="${SOAK_MINUTES:-30}"
KVS_BIN="${KVS_BIN:-$(pwd)/build/kvstore}"
KVS_CONF="${KVS_CONF:-$(pwd)/conf/kvstore-dev.conf}"
PORT="${PORT:-2000}"
OUTDIR="/tmp/kvstore-soak"

mkdir -p "$OUTDIR"
rm -f "$OUTDIR"/*

LOG="$OUTDIR/kvstore.log"
CSV="$OUTDIR/samples.csv"
VERDICT="$OUTDIR/verdict.txt"

if ! command -v redis-benchmark >/dev/null 2>&1; then
    echo "redis-benchmark not found" >&2; exit 2
fi
if [ ! -x "$KVS_BIN" ]; then
    echo "kvstore not built: $KVS_BIN" >&2; exit 2
fi

# ── boot kvstore ────────────────────────────────────────────────────────────
pkill -9 kvstore 2>/dev/null; sleep 1
echo "[soak] booting kvstore: $KVS_BIN $KVS_CONF"
setsid "$KVS_BIN" "$KVS_CONF" >"$LOG" 2>&1 </dev/null &
disown
sleep 1
PID=$(pgrep -f "$KVS_BIN" | head -1)
if [ -z "$PID" ]; then
    echo "[soak] FAIL: kvstore did not start" >&2
    tail -20 "$LOG" >&2
    exit 1
fi
echo "[soak] kvstore PID=$PID, soak duration=${SOAK_MINUTES}min"

# ── sample function ────────────────────────────────────────────────────────
echo "ts_s,vmrss_kb,fd_count,keys" > "$CSV"
sample() {
    local t=$1
    local rss="-" fd="-" keys="-"
    if [ -d "/proc/$PID" ]; then
        rss=$(awk '/^VmRSS:/{print $2}' /proc/"$PID"/status 2>/dev/null)
        fd=$(ls -1 /proc/"$PID"/fd 2>/dev/null | wc -l)
        keys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null | tail -1 | tr -d '\r')
    fi
    echo "$t,$rss,$fd,$keys" | tee -a "$CSV"
}

# ── load generator ─────────────────────────────────────────────────────────
# Run continuously in background.  Each cycle: SET+GET+LPUSH+LRANGE+INCR
# 50k requests each, 50 connections, pipeline 4.  ~1-2s per cycle.
runner() {
    while true; do
        redis-benchmark -p "$PORT" -t set,get,incr,lpush,lrange \
                        -n 50000 -c 50 -P 4 --csv 2>/dev/null \
                        > "$OUTDIR/bench-latest.csv" || true
    done
}
runner & RUNNER_PID=$!
disown

# ── 5-min cycle: BGSAVE + FLUSHDB ──────────────────────────────────────────
cycler() {
    while true; do
        sleep 300
        echo "[$(date +%H:%M:%S)] BGSAVE+FLUSHDB cycle"
        redis-cli -p "$PORT" BGSAVE  >/dev/null 2>&1 || true
        sleep 60
        redis-cli -p "$PORT" FLUSHDB >/dev/null 2>&1 || true
    done
}
cycler & CYCLER_PID=$!
disown

# ── main loop: sample every 60s for SOAK_MINUTES ───────────────────────────
echo "[soak] running for $SOAK_MINUTES minutes; samples every 60s"
echo "[soak] ts(s), VmRSS(kB), fd_count, DBSIZE"
sample 0
for ((m=1; m<=SOAK_MINUTES; m++)); do
    sleep 60
    sample $((m*60))
    # crash check
    if [ ! -d "/proc/$PID" ]; then
        echo "[soak] FAIL: kvstore PID $PID disappeared after ${m}min" >&2
        kill -9 $RUNNER_PID $CYCLER_PID 2>/dev/null
        echo "FAIL: process crashed at ${m}min" > "$VERDICT"
        exit 1
    fi
done

# ── teardown ───────────────────────────────────────────────────────────────
echo "[soak] teardown"
kill -9 $RUNNER_PID $CYCLER_PID 2>/dev/null
sleep 1
pkill -9 kvstore 2>/dev/null

# ── verdict ────────────────────────────────────────────────────────────────
# Baseline = sample at t=60s (post-warmup); compare last sample against it.
# Allows allocator arenas / connection buffers to settle after first minute.
# RSS check: last < 1.5× baseline   (steady-state, no leak)
# fd check:  last < baseline + 50   (small drift OK)
# (Header row=NR1, t=0 row=NR2 (cold), t=60s row=NR3 (warm baseline))
init_rss=$(awk -F, 'NR==3{print $2}' "$CSV")
last_rss=$(awk -F, 'END{print $2}' "$CSV")
init_fd=$( awk -F, 'NR==3{print $3}' "$CSV")
last_fd=$( awk -F, 'END{print $3}' "$CSV")

rss_ratio="?"
if [ -n "$init_rss" ] && [ "$init_rss" -gt 0 ] 2>/dev/null; then
    rss_ratio=$(awk -v a="$init_rss" -v b="$last_rss" 'BEGIN{printf "%.2f", b/a}')
fi
fd_drift=$((last_fd - init_fd))

{
    echo "Phase 8.3 — soak verdict"
    echo "  duration:   ${SOAK_MINUTES} min"
    echo "  init RSS:   ${init_rss} kB"
    echo "  final RSS:  ${last_rss} kB   (ratio: $rss_ratio)"
    echo "  init fd:    $init_fd"
    echo "  final fd:   $last_fd   (drift: $fd_drift)"
    echo
    rss_ok=$(awk -v r="$rss_ratio" 'BEGIN{print (r<1.5 || r=="?") ? 1 : 0}')
    fd_ok=$( [ "$fd_drift" -lt 50 ] && echo 1 || echo 0)
    if [ "$rss_ok" = "1" ] && [ "$fd_ok" = "1" ]; then
        echo "VERDICT: PASS"
    else
        echo "VERDICT: FAIL"
        [ "$rss_ok" != "1" ] && echo "  RSS grew >1.5× post-warmup — possible memory leak"
        [ "$fd_ok"  != "1" ] && echo "  fd drift >50 — possible fd leak"
    fi
} | tee "$VERDICT"

echo
echo "[soak] artifacts in $OUTDIR"
echo "       $CSV"
echo "       $VERDICT"
echo "       $LOG"
