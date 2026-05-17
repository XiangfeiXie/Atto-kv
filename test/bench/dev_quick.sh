#!/bin/bash
# Dev-quick bench — 30s sanity check after touching lock / dispatch code.
#
# Fixed knobs (do not parametrize; the point is a stable yardstick):
#   threads=4, cpu_pin=auto, persistence=none, rdma_sync=off, xdp=no
#   bench: -c 200 --threads 4 -P 10 -n 200000
#
# Cover the four code paths that matter for Phase 6.2 lock refactor:
#   SET     — single-key wrlock
#   GET     — single-key rdlock (the one we just enabled)
#   MSET    — multi-key wrlock loop
#   LRANGE  — single-key rdlock with non-trivial payload (rules out
#             "rdlock only wins on tiny reads" suspicion)
#
# Usage:
#   test/bench/dev_quick.sh          # human-readable to stdout
#   test/bench/dev_quick.sh --save   # also append to test/bench/dev_quick_history.tsv

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="$ROOT/build/kvstore"
CONF="$ROOT/conf/kvstore-dev.conf"
HIST="$ROOT/test/bench/dev_quick_history.tsv"
SAVE=0
[ "${1:-}" = "--save" ] && SAVE=1

[ -x "$KVS" ] || { echo "build/kvstore not found; run cmake --build build first" >&2; exit 1; }
[ -f "$CONF" ] || { echo "$CONF missing" >&2; exit 1; }
command -v redis-benchmark >/dev/null || { echo "redis-benchmark required" >&2; exit 1; }

pkill -9 kvstore 2>/dev/null; sleep 1
setsid "$KVS" "$CONF" >/tmp/kvs_devquick.log 2>&1 </dev/null &
disown
sleep 1

# Pre-load a key for LRANGE (1000 elements).
# NOTE: dispatcher is currently case-sensitive (pre-existing, unrelated to
# Phase 6.2), so commands must be sent in UPPERCASE.
redis-cli -p 2000 DEL mylist >/dev/null 2>&1
for i in $(seq 1 200); do
  redis-cli -p 2000 RPUSH mylist "v$i" "v$i" "v$i" "v$i" "v$i" >/dev/null
done

run() {
  local label="$1"; shift
  local qps p50
  local out
  # -q MUST come before positional command args (otherwise it's consumed as
  # a payload).  Filter out the "WARNING: Could not fetch server CONFIG"
  # line that redis-benchmark emits because we don't implement CONFIG GET.
  # redis-benchmark -q emits live progress samples separated by bare \r on
  # the same TTY line; convert CR→NL so we can grep for the final summary
  # line ("X requests per second, p50=..."), which is always last.
  out=$(redis-benchmark -p 2000 -q -c 200 --threads 4 -P 10 -n 200000 "$@" 2>/dev/null \
        | tr '\r' '\n' | grep 'requests per second' | tail -1)
  # Tolerate both old ("SET: 15427.34 requests per second, p50=...") and
  # new ("SET: rps=15427.3 (...) avg_msec=...") -q output formats.
  qps=$(echo "$out" | grep -oE 'rps=[0-9.]+|[0-9]+\.[0-9]+ requests per second' \
        | head -1 | grep -oE '[0-9.]+' | head -1)
  p50=$(echo "$out" | grep -oE 'p50=[0-9.]+|p50_msec=[0-9.]+' | head -1 | cut -d= -f2)
  printf "  %-10s  %10s rps   p50=%s ms\n" "$label" "$qps" "${p50:-?}"
  if [ "$SAVE" = "1" ]; then
    printf "%s\t%s\t%s\t%s\n" "$(date -Iseconds)" "$label" "$qps" "${p50:-?}" >> "$HIST"
  fi
}

GIT=$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "?")
echo "──────────────────────────────────────────────────"
echo "  dev_quick  threads=4  c=200×4  P=10  n=200k  git=$GIT"
echo "──────────────────────────────────────────────────"
run "SET"    -t set
run "GET"    -t get
run "MSET"   -t mset
run "LRANGE" LRANGE mylist 0 99

pkill -9 kvstore 2>/dev/null
exit 0
