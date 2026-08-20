#!/usr/bin/env bash
# =============================================================================
# test_persist_rdb.sh — RDB 持久化重启恢复功能测试
#
# 流程: 启动 → 填 N 条数据 → BGSAVE 落盘 → 等待 dump.rdb → 优雅停 → 重启 → 校验
# 用法: ./test/smoke/test_persist_rdb.sh [N]        (N 默认 12000，应 > 10000)
# =============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/persist_common.sh"

CONF="kvstore-rdb.conf"
N="${1:-$DEFAULT_N}"

require_kvs

echo "==================================================================="
step "RDB 持久化恢复测试 (N=$N, conf=$CONF)"
[ "$N" -le 10000 ] && warn "N=$N 未超过 10000，建议使用更大数据量"

# 1. 干净起点
clean_persist_files

# 2. 启动
start_kvs "$CONF"

# 3. 填充数据 (仅内存，尚未落盘)
step "填充 $N 条数据 (RESP)..."
if ! FILL_OUT="$(populate_resp "$PORT" "$N")"; then
    err "填充失败: $FILL_OUT"
    stop_kvs_graceful || true
    exit 1
fi
log "填充完成: $FILL_OUT"

# 4. BGSAVE 落盘
step "发起 BGSAVE..."
BG_OUT="$(bgsave || true)"
log "BGSAVE 回复: $BG_OUT"

step "等待 dump.rdb 生成 (落盘完成)..."
wait_rdb_file 30 || { err "RDB 测试失败: dump.rdb 未生成"; stop_kvs_graceful || true; exit 1; }
# 给 SIGCHLD 回收留出时间 (rename 已完成，仅剩子进程收尾)
sleep 0.5
log "dump.rdb 已生成: $(stat -c %s "$RDB_FILE" 2>/dev/null || echo '?') bytes"

# 5. 优雅停
step "SIGTERM 优雅停..."
stop_kvs_graceful || { err "RDB 测试失败: 无法优雅停止"; exit 1; }

# 6. 重启 (触发 rdb_load_mmap 加载)
step "重启 kvstore (RDB 加载)..."
start_kvs "$CONF"

# 7. 校验
step "校验恢复数据..."
if ! VERIFY_OUT="$(verify_resp "$PORT" "$N")"; then
    err "RDB 恢复校验失败: $VERIFY_OUT"
    stop_kvs_graceful || true
    exit 1
fi
log "校验通过: $VERIFY_OUT"

# 8. 清理
stop_kvs_graceful || true

echo ""
echo -e "${GREEN}RDB 持久化恢复测试 PASS${NC} (N=$N)"
exit 0
