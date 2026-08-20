#!/usr/bin/env bash
# =============================================================================
# test_persist_aof.sh — AOF 持久化重启恢复功能测试
#
# 流程: 启动 → 填 N 条数据 → SIGTERM 优雅停 (flush+fsync WAL) → 重启 → 校验
# 用法: ./test/smoke/test_persist_aof.sh [N]        (N 默认 12000，应 > 10000)
# =============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/persist_common.sh"

CONF="kvstore-aof.conf"
N="${1:-$DEFAULT_N}"

require_kvs

echo "==================================================================="
step "AOF 持久化恢复测试 (N=$N, conf=$CONF)"
[ "$N" -le 10000 ] && warn "N=$N 未超过 10000，建议使用更大数据量"

# 1. 干净起点
clean_persist_files

# 2. 启动
start_kvs "$CONF"

# 3. 填充数据 (RESP 格式，写入 WAL 日志)
step "填充 $N 条数据 (RESP)..."
if ! FILL_OUT="$(populate_resp "$PORT" "$N")"; then
    err "填充失败: $FILL_OUT"
    stop_kvs_graceful || true
    exit 1
fi
log "填充完成: $FILL_OUT"

# 4. 优雅停 → 触发 kvs_wal_persist_stop() flush + fsync
step "SIGTERM 优雅停 (flush WAL)..."
stop_kvs_graceful || { err "AOF 测试失败: 无法优雅停止"; exit 1; }

# 5. 重启 (保留 WAL 文件，触发 aof_reload_mmap 重放)
step "重启 kvstore (WAL 重放)..."
start_kvs "$CONF"

# 6. 校验
step "校验恢复数据..."
if ! VERIFY_OUT="$(verify_resp "$PORT" "$N")"; then
    err "AOF 恢复校验失败: $VERIFY_OUT"
    stop_kvs_graceful || true
    exit 1
fi
log "校验通过: $VERIFY_OUT"

# 7. 清理
stop_kvs_graceful || true

echo ""
echo -e "${GREEN}AOF 持久化恢复测试 PASS${NC} (N=$N)"
exit 0
