#!/bin/bash
# Phase 5.2 — Pin NIC IRQs and softirq processing to cores that the kvstore
# proactor workers are NOT using, so workers get exclusive cycles on their
# cores and softirq doesn't preempt them.
#
# Default policy (matches `cpu_pin auto` in kvstore.conf):
#   kvstore workers   →  cores 1 .. (threads)        (auto-pin in proactor.c)
#   IRQ + RPS + RFS   →  core 0                       (this script)
#   bench / client    →  cores (threads+1) .. nproc-1 (roams)
#
# Usage:
#   sudo test/setup_affinity.sh                # default: pin all NIC IRQs to core 0
#   sudo test/setup_affinity.sh --iface ens33  # specific interface
#   sudo test/setup_affinity.sh --cores 0,5    # use cores 0 and 5 for IRQs
#   sudo test/setup_affinity.sh --restore      # restore irqbalance / default
#
# Idempotent: re-running won't compound state.

set -u

IRQ_CORES="0"
IFACE=""
RESTORE=0

usage() {
  sed -n '2,21p' "$0"
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --cores)    IRQ_CORES="$2"; shift 2 ;;
    --iface)    IFACE="$2"; shift 2 ;;
    --restore)  RESTORE=1; shift ;;
    -h|--help)  usage ;;
    *)          echo "unknown arg: $1"; usage ;;
  esac
done

if [ "$EUID" -ne 0 ]; then
  echo "error: must run as root (need /proc/irq write access)"
  exit 1
fi

# ── Build the bitmask for /proc/irq/N/smp_affinity (hex, comma-grouped) ────
mask_from_cores() {
  local cores="$1"
  python3 - <<PY
cores = [int(c) for c in "$cores".split(",") if c]
m = 0
for c in cores: m |= (1 << c)
# /proc/irq/.../smp_affinity wants comma-separated 32-bit groups, LSB first
groups = []
while m > 0 or not groups:
    groups.append("%x" % (m & 0xffffffff))
    m >>= 32
print(",".join(reversed(groups)))
PY
}

if [ "$RESTORE" -eq 1 ]; then
  echo "[setup_affinity] restoring default IRQ affinity (all CPUs)"
  if pgrep irqbalance >/dev/null; then
    echo "  irqbalance already running"
  else
    systemctl start irqbalance 2>/dev/null || service irqbalance start 2>/dev/null
  fi
  exit 0
fi

# Stop irqbalance so it doesn't undo our pins.
if pgrep irqbalance >/dev/null; then
  echo "[setup_affinity] stopping irqbalance"
  systemctl stop irqbalance 2>/dev/null || service irqbalance stop 2>/dev/null
fi

MASK=$(mask_from_cores "$IRQ_CORES")
echo "[setup_affinity] pinning IRQs to cores [$IRQ_CORES] (mask=$MASK)"

# ── Pin NIC IRQs ────────────────────────────────────────────────────────────
PINNED=0
SKIPPED=0
for irqdir in /proc/irq/*/; do
  irq=$(basename "$irqdir")
  # Only digit-named entries are real IRQs
  case "$irq" in
    [0-9]*) ;;
    *) continue ;;
  esac
  # Filter by interface name if provided
  if [ -n "$IFACE" ]; then
    if ! ls "$irqdir" 2>/dev/null | grep -q "$IFACE"; then
      continue
    fi
  fi
  # Try writing the mask; skip silently for IRQs that don't accept user pinning
  if echo "$MASK" > "$irqdir/smp_affinity" 2>/dev/null; then
    PINNED=$((PINNED+1))
  else
    SKIPPED=$((SKIPPED+1))
  fi
done
echo "[setup_affinity] IRQs pinned: $PINNED (skipped: $SKIPPED)"

# ── Pin RPS (Receive Packet Steering) for loopback ─────────────────────────
# RPS lets the kernel choose which CPU runs the receive softirq; on loopback
# this is where most of our work lives (no NIC).
RPS_MASK=$(mask_from_cores "$IRQ_CORES")
for rps in /sys/class/net/*/queues/rx-*/rps_cpus; do
  if [ -w "$rps" ]; then
    echo "$RPS_MASK" > "$rps" 2>/dev/null && true
  fi
done
echo "[setup_affinity] RPS rps_cpus set to mask=$RPS_MASK"

echo "[setup_affinity] done.  Verify with: cat /proc/interrupts | head"
