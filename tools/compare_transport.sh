#!/bin/bash
# =============================================================================
# compare_transport.sh — 同数据量下 libfabric TCP vs 原生 TCP 对比
#
# 用法: ./compare_transport.sh [size1 size2 ...]
#       ./compare_transport.sh 1M 10M 50M 100M
#
# 输出: 表格显示两种传输方式的 MB/s 和差异
#
# 依赖: tools/libfabric_bw.c  tools/tcp_raw_bw.c (需先编译)
#   cd tools && gcc -O2 -o libfabric_bw libfabric_bw.c $(pkg-config --libs --cflags libfabric)
#   cd tools && gcc -O2 -o tcp_raw_bw tcp_raw_bw.c
# =============================================================================

TOOLS_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=12200
SIZES=("${@:-1M 10M 50M 100M 500M}")

echo "============================================"
echo " libfabric TCP  vs  raw TCP socket"
echo "============================================"
echo ""
printf "  %-8s  %12s  %12s  %s\n" "Size" "libfabric" "raw TCP" "Δ"
printf "  %-8s  %12s  %12s  %s\n" "--------" "------------" "------------" "------"

for sz in "${SIZES[@]}"; do
    PORT=$((PORT + 1))

    # ── libfabric tcp ─────────────────────────────────────────────
    "$TOOLS_DIR/libfabric_bw" --server --provider tcp --port $PORT > /dev/null 2>&1 &
    SRV=$!
    sleep 0.5
    LF_OUT=$("$TOOLS_DIR/libfabric_bw" --client 127.0.0.1 --provider tcp \
             --port $PORT --size "$sz" 2>&1)
    kill $SRV 2>/dev/null; wait $SRV 2>/dev/null
    LF_MBPS=$(echo "$LF_OUT" | grep -oP '[\d.]+(?= MB/s)' || echo "0")

    # ── raw TCP ───────────────────────────────────────────────────
    PORT=$((PORT + 1))
    "$TOOLS_DIR/tcp_raw_bw" --server --port $PORT > /dev/null 2>&1 &
    SRV=$!
    sleep 0.5
    RAW_OUT=$("$TOOLS_DIR/tcp_raw_bw" --client 127.0.0.1 --port $PORT \
              --size "$sz" 2>&1)
    kill $SRV 2>/dev/null; wait $SRV 2>/dev/null
    RAW_MBPS=$(echo "$RAW_OUT" | grep -oP '[\d.]+(?= MB/s)' || echo "0")

    # ── compare ───────────────────────────────────────────────────
    DELTA=$(python3 -c "print(round($LF_MBPS - $RAW_MBPS, 1))" 2>/dev/null || echo "-")
    printf "  %-8s  %10s  %10s  %+6s MB/s\n" \
           "$sz" "$LF_MBPS MB/s" "$RAW_MBPS MB/s" "$DELTA"
done

echo ""
echo "============================================"
echo " kvstore 全量同步 (参考): ~20 MB/s  (见 fullsync_bench.sh)"
echo "============================================"
