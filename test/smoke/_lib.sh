# shellcheck shell=bash
# Common helpers for kvstore smoke tests.
# Sources cleanly; expects HOST/PORT to be exported (defaults below).

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-2000}"
REDIS_CLI_TIMEOUT="${REDIS_CLI_TIMEOUT:-3s}"
RCLI="timeout ${REDIS_CLI_TIMEOUT} redis-cli -h ${HOST} -p ${PORT}"

PASS=0
FAIL=0
FAILED_NAMES=()

RED=$'\033[31m'
GRN=$'\033[32m'
YEL=$'\033[33m'
RST=$'\033[0m'

ok()   { PASS=$((PASS+1)); printf "  ${GRN}PASS${RST}  %s\n" "$1"; }
bad()  { FAIL=$((FAIL+1)); FAILED_NAMES+=("$1");
         printf "  ${RED}FAIL${RST}  %s\n        expected: %s\n        actual:   %s\n" "$1" "$2" "$3"; }

# chk NAME EXPECTED ACTUAL — exact string match
chk() {
    if [ "$3" = "$2" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi
}

# section TITLE — print a header
section() { printf "\n${YEL}── %s ──${RST}\n" "$1"; }

# summary EXIT_NAME — print suite totals and set the script exit accordingly
summary() {
    local total=$((PASS + FAIL))
    printf "\n────────────────────────────────────────\n"
    printf "%s suite: %d/%d passed" "$1" "$PASS" "$total"
    if [ "$FAIL" -gt 0 ]; then
        printf "  (${RED}%d failed${RST})\n" "$FAIL"
        for n in "${FAILED_NAMES[@]}"; do printf "    • %s\n" "$n"; done
        return 1
    fi
    printf "  ${GRN}ALL PASS${RST}\n"
    return 0
}

# Verify server is up before doing anything destructive
require_server() {
    if ! $RCLI PING >/dev/null 2>&1; then
        printf "${RED}ERROR${RST}: cannot reach kvstore at %s:%s — start it first.\n" "$HOST" "$PORT" >&2
        exit 2
    fi
    $RCLI FLUSHALL >/dev/null
}
