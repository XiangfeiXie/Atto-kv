#!/usr/bin/env bash
# Run smoke tests against a running kvstore.
#
# Usage:  ./run_all.sh [host] [port]
#         HOST=127.0.0.1 PORT=2000 ./run_all.sh
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
export HOST="${1:-${HOST:-127.0.0.1}}"
export PORT="${2:-${PORT:-2000}}"

RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; RST=$'\033[0m'

if ! redis-cli -h "$HOST" -p "$PORT" PING >/dev/null 2>&1; then
    echo "${RED}ERROR${RST}: kvstore not reachable at $HOST:$PORT" >&2
    exit 2
fi

TYPES=(basic string hash)
declare -A RESULTS
overall=0

for t in "${TYPES[@]}"; do
    echo
    echo "${YEL}========================================${RST}"
    echo "${YEL}  Running smoke: $t${RST}"
    echo "${YEL}========================================${RST}"
    if bash "${DIR}/smoke_${t}.sh"; then
        RESULTS[$t]="${GRN}PASS${RST}"
    else
        RESULTS[$t]="${RED}FAIL${RST}"
        overall=1
    fi
done

echo
echo "${YEL}========================================${RST}"
echo "${YEL}  Overall smoke-test summary${RST}"
echo "${YEL}========================================${RST}"
for t in "${TYPES[@]}"; do
    printf "  %-8s  %s\n" "$t" "${RESULTS[$t]}"
done
echo
if [ "$overall" -eq 0 ]; then
    echo "${GRN}ALL SUITES PASSED${RST}"
else
    echo "${RED}SOME SUITES FAILED${RST}"
fi
exit "$overall"
