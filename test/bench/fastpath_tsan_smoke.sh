#!/bin/bash
# Phase 10.4 — TSAN smoke for lock-free seqlock fast-path cache.
#
# Scope:
#   - Builds a standalone fp_cache_test binary under ThreadSanitizer.
#   - Runs 1 writer + 3 readers stress for N seconds.
#   - Requires 0 functional torn-read failures AND 0 TSAN reports.
#
# Why standalone instead of full kvstore first:
#   The correctness risk is inside fp_get/fp_put/fp_del seqlock protocol.
#   Isolating the data structure gives much faster signal and avoids unrelated
#   conn_pool close→accept TSAN suppressions.
#
# Usage:
#   ./test/bench/fastpath_tsan_smoke.sh        # 10s default
#   ./test/bench/fastpath_tsan_smoke.sh 30     # 30s stress

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SECS="${1:-10}"
BIN=/tmp/fp_cache_test_tsan
OUT=/tmp/fp_cache_tsan.stdout
TSAN_LOG=/tmp/fp_cache_tsan.log

rm -f "$BIN" "$OUT" "$TSAN_LOG".*

cc -fsanitize=thread -g -O1 -fno-omit-frame-pointer \
   -I"$ROOT/src/core" \
   "$ROOT/test/fp_cache_test.c" "$ROOT/src/core/fastpath_cache.c" \
   -lpthread -o "$BIN" || exit 1

# Some Linux/VM combinations need ASLR disabled for libtsan.  setarch may fail
# under restricted containers; fall back to normal execution in that case.
RUNNER=("$BIN" "$SECS")
if setarch "$(uname -m)" -R true >/dev/null 2>&1; then
  RUNNER=(setarch "$(uname -m)" -R "$BIN" "$SECS")
fi

TSAN_OPTIONS="halt_on_error=0:history_size=4:log_path=$TSAN_LOG" \
  "${RUNNER[@]}" > "$OUT" 2>&1
RC=$?
cat "$OUT"

WARN=0
if ls "$TSAN_LOG".* >/dev/null 2>&1; then
  WARN=$(cat "$TSAN_LOG".* | grep -c "WARNING: ThreadSanitizer" || true)
fi

if [ "$RC" -ne 0 ]; then
  echo "FAIL: fp_cache_test exited with rc=$RC"
  [ -f "$OUT" ] && cat "$OUT"
  exit 1
fi

if [ "$WARN" != "0" ]; then
  echo "FAIL: ThreadSanitizer warnings=$WARN"
  cat "$TSAN_LOG".* | head -120
  exit 1
fi

if ! grep -q "== ALL PASS ==" "$OUT"; then
  echo "FAIL: fp_cache_test did not report ALL PASS"
  exit 1
fi

echo "PASS: fp_cache TSAN clean (${SECS}s, 0 warnings)"
