#!/usr/bin/env bash
# =============================================================================
# sync_data.sh — 跨机数据同步验证 (fill / verify 两模式, 薄壳, 转发给 kv_resp.py)
#
# 用于在两台机器间验证数据同步是否成功, 两端分别启动:
#   master 端: ./sync_data.sh fill    [kv_resp.py 参数...]   → 填充 N 条数据
#   slave  端: ./sync_data.sh verify  [kv_resp.py 参数...]   → 轮询校验
#
# 两端只需约定相同的 --n/--start/--prefix, 即可独立生成相同的 key/value:
#   key:  {prefix}:{i:012d}
#   value: value:{i:012d}        (i ∈ [start, start+n))
#
# 常用示例:
#   ./sync_data.sh fill   --n 12000 --host 127.0.0.1 --port 2000
#   ./sync_data.sh verify --n 12000 --host 127.0.0.1 --port 2000 --wait 60
#
# 常用参数 (默认值见 kv_resp.py):
#   --n 12000    --host 127.0.0.1 --port 2000
#   --start 0    --prefix key
#   verify 专有: --once (单次) | --wait 秒 (轮询超时, 默认 60) | --expect-dbsize N
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KV_RESP="${SCRIPT_DIR}/kv_resp.py"

usage() {
    sed -n '3,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 2
}

MODE="${1:-}"
[ -n "$MODE" ] || usage
case "$MODE" in
    fill|verify) ;;
    *) usage ;;
esac
shift

echo "== sync_data.sh $MODE $* =="
exec python3 "$KV_RESP" "$MODE" "$@"
