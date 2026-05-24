#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-quick}"
USER_KVS="${KVS:-}"
KVS="${KVS:-$ROOT/build/kvstore}"
PORT="${PORT:-2000}"
PASS=0
FAIL=0

usage() {
  cat <<EOF
Usage: $0 [quick|full|tsan|fastpath]

quick     Start kvstore and run data-type smoke tests.
full      quick + config smoke + multi-thread smoke + multi-key smoke + WAL/AOF smoke.
tsan      Run TSAN lock smoke. Requires build-tsan/kvstore or KVS=... override.
fastpath  Run fp_cache_test and in-process fastpath microbenchmark when built.
EOF
}

ok() { echo "[PASS] $1"; PASS=$((PASS+1)); }
bad() { echo "[FAIL] $1"; FAIL=$((FAIL+1)); }

need_bin() {
  [ -x "$KVS" ] || { echo "missing kvstore binary: $KVS" >&2; exit 1; }
}

start_kvs() {
  need_bin
  pkill -9 kvstore 2>/dev/null || true
  sleep 1
  cat > /tmp/kvstore_regression.conf <<EOF
port $PORT
network proactor
maxclients 10000
loglevel error
persistence none
rdma_sync off
xdp no
sockmap no
threads 4
cpu_pin off
EOF
  setsid "$KVS" /tmp/kvstore_regression.conf >/tmp/kvstore_regression.log 2>&1 </dev/null &
  disown
  sleep 1
}

stop_kvs() {
  pkill -9 kvstore 2>/dev/null || true
  sleep 1
}

run_step() {
  local name="$1"; shift
  echo
  echo "==== $name ===="
  if "$@"; then ok "$name"; else bad "$name"; fi
}

run_quick() {
  trap stop_kvs EXIT
  start_kvs
  run_step "basic/string/hash smoke" timeout 90s bash "$ROOT/test/smoke/run_all.sh" 127.0.0.1 "$PORT"
  stop_kvs
  trap - EXIT
}

run_full() {
  run_quick
  run_step "CONFIG smoke" bash "$ROOT/test/bench/config_smoke.sh"
  run_step "multi-thread smoke" bash "$ROOT/test/bench/multi_thread_smoke.sh"
  run_step "multi-key smoke" bash "$ROOT/test/bench/multi_key_smoke.sh"
  run_step "WAL/AOF smoke" bash "$ROOT/test/test_wal_aof.sh"
}

run_tsan() {
  if [ -z "$USER_KVS" ]; then
    KVS="$ROOT/build-tsan/kvstore"
  fi
  export KVS
  run_step "TSAN lock smoke" bash "$ROOT/test/bench/tsan_lock_smoke.sh"
}

run_fastpath() {
  [ -x "$ROOT/build/fp_cache_test" ] && run_step "fp_cache_test" "$ROOT/build/fp_cache_test" || bad "fp_cache_test missing"
  [ -x "$ROOT/build/bench_fp_inproc" ] && run_step "bench_fp_inproc" "$ROOT/build/bench_fp_inproc" || bad "bench_fp_inproc missing"
}

case "$MODE" in
  quick) run_quick ;;
  full) run_full ;;
  tsan) run_tsan ;;
  fastpath) run_fastpath ;;
  -h|--help) usage; exit 0 ;;
  *) echo "unknown mode: $MODE" >&2; usage; exit 1 ;;
esac

echo
echo "Regression summary: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
