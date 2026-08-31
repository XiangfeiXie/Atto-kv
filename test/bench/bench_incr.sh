#!/bin/bash
# =============================================================================
# bench_incr.sh — 增量同步对 master 写吞吐的影响
#
# 协作方式: 主机(master)与从机由你手工启动, 确认主从连接成功后, 再运行本脚本。
#   本脚本不启动、不停止任何服务器, 只在 master 端口测 SET QPS 并打印数据。
#
# 指标: 只测 SET 的聚合 QPS（NCLIENTS 个 redis-benchmark 进程求和）。
#
# 用法（项目根目录执行）:
#   sudo ./test/bench/bench_incr.sh               # kvstore master 2000
#   sudo ./test/bench/bench_incr.sh 2001          # redis master 2001
#   sudo ./test/bench/bench_incr.sh 2000 1 4 16 160
#
# 输出: 终端（直接打印, 不写文件）
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

PORT=${1:-$KVS_PORT}                 # 默认 kvstore master 2000; redis 用 2001
PIPELINES=(1 4 8 16 20 40 80 160)

if [ $# -gt 0 ]; then
    shift
    [ $# -gt 0 ] && PIPELINES=("$@")
fi

require_root
require_tools

if ! wait_for_port 127.0.0.1 "$PORT" 3; then
    err "master 端口未就绪: $PORT（请先启动主机并确认主从连接成功）"
    exit 1
fi

declare -A RAWS

echo "# bench_incr — $(date '+%Y-%m-%d %H:%M:%S')"
echo "# master port=$PORT  test=set  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
echo "# pipelines: ${PIPELINES[*]}  指标: master SET QPS（从机在线, 增量同步）"
echo ""

warmup_server "$PORT"
for P in "${PIPELINES[@]}"; do
    q=$(bench_qps "$PORT" set "$P")
    RAWS["$P"]="$q"
    echo "P=$P set qps=$q"
done

# ── 汇总表（直接打印, 不写文件）─────────────────────────────────────────
echo ""
echo "bench_incr — $(date '+%Y-%m-%d %H:%M:%S')"
echo "master port=$PORT  test=set  nclients=$NCLIENTS  conn=$CONN  ops=$OPS  data=$DATA"
echo "pipelines: ${PIPELINES[*]}   (单位: SET QPS, 从机在线)"
echo ""
printf "  %-6s | %12s\n" "P" "SET QPS"
for P in "${PIPELINES[@]}"; do
    printf "  P%-4s | %12s\n" "$P" "${RAWS[$P]:-N/A}"
done
