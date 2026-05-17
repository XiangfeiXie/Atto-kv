#!/usr/bin/env bash
# Phase 4 multi-worker scaling bench.
#
# Stops any running kvstore, starts a fresh instance for each thread count,
# runs redis-benchmark with a multi-threaded client, captures RPS.
#
# Usage:  ./test/bench/bench_phase4.sh [out.csv]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/build/kvstore"
OUT="${1:-$ROOT/test/bench/phase4-mt.csv}"
PORT=2000
N=2000000
P=10
CLIENTS=400
CLIENT_THR=6

[ -x "$BIN" ] || { echo "build kvstore first"; exit 1; }
command -v redis-benchmark >/dev/null || { echo "redis-benchmark required"; exit 1; }

mkconf() {
    local threads="$1"
    cat <<EOF
port $PORT
network proactor
maxclients 10000
loglevel error
persistence none
rdma_sync off
xdp no
sockmap no
threads $threads
EOF
}

run_one() {
    local threads="$1"
    pkill -9 kvstore 2>/dev/null || true
    sleep 1
    local conf="/tmp/kv-mt-$threads.conf"
    mkconf "$threads" >"$conf"
    setsid "$BIN" "$conf" >/tmp/kvstore-mt.log 2>&1 </dev/null &
    disown
    sleep 1
    local pid
    pid=$(pgrep -f "kv-mt-$threads.conf" | head -1)
    if [ -z "$pid" ]; then
        echo "[bench] kvstore failed to start with threads=$threads"
        cat /tmp/kvstore-mt.log
        return 1
    fi
    for op in set get; do
        local rps
        rps=$(redis-benchmark -p $PORT -t "$op" -c $CLIENTS -n $N -P $P \
                              --threads $CLIENT_THR -q 2>/dev/null \
              | tr '\r' '\n' \
              | awk -v op="${op^^}" '
                    $0 ~ op": *[0-9.]+ requests" {
                        for (i = 1; i <= NF; i++) if ($i == "requests") { print $(i-1); exit }
                    }')
        echo "$threads,$op,$rps"
    done
    pkill -9 kvstore 2>/dev/null || true
    sleep 1
}

echo "threads,op,rps" >"$OUT"
for t in 1 2 4 8; do
    run_one "$t" >>"$OUT"
done

echo
echo "=== $OUT ==="
cat "$OUT"
