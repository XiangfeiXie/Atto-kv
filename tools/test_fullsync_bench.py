#!/usr/bin/env python3
"""
全量同步速率测试脚本

用法:
  # 主机 (192.168.30.135)
  sudo ./build/kvstore conf/kvstore-master.conf

  # 从机
  sudo ./build/kvstore conf/kvstore-slave.conf

  # 本脚本（可在任意一台能连上 master/slave 的机器运行）
  python3 test_fullsync_bench.py --master 192.168.30.135 \\
       --slave 192.168.30.136 --keys 100000 --size 512

速率计算原理:
  脚本输出三个视角的速率：
  1. Master send  — 从 master 子进程日志提取，含序列化+发送时间
  2. Slave  recv  — 从 slave 日志提取，纯数据路径（header→rebuild）
  3. Slave  e2e   — 从 slave 日志提取，端到端（connect→rebuild）

  整体传输速率取 Master send 的 MB/s 最准确，
  因为它测量的是实际数据平面传输时间（序列化+网络发送）。
"""

import argparse, subprocess, socket, time, os, sys, re

# ── 配置 ──────────────────────────────────────────────────────────────
MASTER_PORT = 2000
SLAVE_PORT  = 2000   # 跨 VM 可与 master 相同

# ── RESP 构建 ─────────────────────────────────────────────────────────
def build_set_resp(key: str, val: bytes) -> bytes:
    k = key.encode()
    return b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (
        len(k), k, len(val), val)

def populate(host: str, port: int, nkeys: int, vsize: int):
    """Pipeline 批量写入，报告速率"""
    val = b"x" * vsize
    batch_size = 2000
    s = socket.socket()
    s.settimeout(30)
    s.connect((host, port))

    t0 = time.time()
    written = 0
    buf = bytearray()
    for i in range(1, nkeys + 1):
        buf += build_set_resp(f"k{i:08d}", val)
        written += 1
        if written % batch_size == 0:
            s.sendall(buf)
            # drain replies
            s.recv(4096 * batch_size)
            buf = bytearray()
    if buf:
        s.sendall(buf)
        s.recv(4096 * (written % batch_size or batch_size))

    elapsed = time.time() - t0
    s.close()
    total_mb = (nkeys * (8 + vsize + 20)) / (1024 * 1024)  # ~RESP overhead
    print(f"  Populated {nkeys} keys in {elapsed:.1f}s "
          f"(~{total_mb/elapsed:.1f} MB/s RESP)")
    return nkeys

def query(host: str, port: int, cmd: str) -> str:
    """单次 redis 查询"""
    s = socket.socket()
    s.settimeout(5)
    s.connect((host, port))
    s.sendall(f"*1\r\n${len(cmd)}\r\n{cmd}\r\n".encode())
    resp = s.recv(4096).decode().strip()
    s.close()
    return resp

# ── 解析 kvstore 日志 ─────────────────────────────────────────────────
def parse_master_send(log_path: str) -> dict:
    """从 master 日志提取 CHILD Fullsync complete 行"""
    pat = re.compile(
        r"\[CHILD\] Fullsync complete: (\d+) entries, (\d+) bytes, "
        r"([\d.]+) ms, ([\d.]+) MB/s")
    with open(log_path) as f:
        for line in f:
            m = pat.search(line)
            if m:
                return {
                    "entries": int(m.group(1)),
                    "bytes":   int(m.group(2)),
                    "ms":      float(m.group(3)),
                    "mbps":    float(m.group(4)),
                }
    return {}

def parse_slave_metrics(log_path: str) -> dict:
    """从 slave 日志提取 Recv 和 End-to-end 行"""
    recv_pat = re.compile(
        r"\[SLAVE\] Recv throughput: (\d+) bytes, (\d+) entries, "
        r"([\d.]+) ms, ([\d.]+) MB/s, ([\d.]+) entries/s")
    e2e_pat = re.compile(
        r"\[SLAVE\] End-to-end: (\d+) bytes, (\d+) entries, "
        r"([\d.]+) ms, ([\d.]+) MB/s")
    result = {}
    with open(log_path) as f:
        for line in f:
            m = recv_pat.search(line)
            if m:
                result["recv"] = {
                    "bytes":   int(m.group(1)),
                    "entries": int(m.group(2)),
                    "ms":      float(m.group(3)),
                    "mbps":    float(m.group(4)),
                    "eps":     float(m.group(5)),
                }
            m = e2e_pat.search(line)
            if m:
                result["e2e"] = {
                    "bytes":   int(m.group(1)),
                    "entries": int(m.group(2)),
                    "ms":      float(m.group(3)),
                    "mbps":    float(m.group(4)),
                }
    return result

# ── remote SSH helpers ─────────────────────────────────────────────────
def ssh_cmd(host: str, cmd: str, sudo_pass: str = "") -> str:
    """在远程执行命令，返回 stdout"""
    if sudo_pass:
        cmd = f"echo '{sudo_pass}' | sudo -S {cmd}"
    try:
        r = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=5", host, cmd],
            capture_output=True, text=True, timeout=120)
        return r.stdout + r.stderr
    except Exception as e:
        return f"[SSH ERROR: {e}]"

# ── 主流程 ────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="全量同步速率基准测试")
    ap.add_argument("--master", required=True, help="Master IP")
    ap.add_argument("--slave",  required=True, help="Slave IP")
    ap.add_argument("--keys",   type=int, default=100000,
                    help="Key 数量 (default: 100000)")
    ap.add_argument("--size",   type=int, default=512,
                    help="Value 大小字节 (default: 512)")
    ap.add_argument("--no-populate", action="store_true",
                    help="不灌数据，直接用 master 现有数据测试")
    ap.add_argument("--sudo-pass", default="1234",
                    help="sudo 密码 (default: 1234)")
    args = ap.parse_args()

    MP = args.master; SP = args.slave; PW = args.sudo_pass
    NK = args.keys;   VS = args.size
    print(f"Master={MP}  Slave={SP}  Keys={NK}  Value={VS}B")

    # ── Step 1: 灌数据 ────────────────────────────────────────────────
    if not args.no_populate:
        print("\n── Step 1: Populating master ──")
        populate(MP, MASTER_PORT, NK, VS)

    # ── Step 2: 验证 master ────────────────────────────────────────────
    mc = query(MP, MASTER_PORT, "DBSIZE").splitlines()[-1]
    print(f"\n── Step 2: Master has {mc} keys ──")

    # ── Step 3: 在从机启动 slave ──────────────────────────────────────
    print(f"\n── Step 3: Starting slave on {SP} ──")
    ssh_cmd(SP, "pkill -9 kvstore 2>/dev/null; sleep 1", PW)
    ssh_cmd(SP, f"cd ~/9.1-kvstore && ./build/kvstore conf/kvstore-slave.conf "
                f"> /tmp/slave_bench.log 2>&1 &", PW)
    time.sleep(3)

    # ── Step 4: 等待同步完成 ──────────────────────────────────────────
    print(f"\n── Step 4: Waiting for full sync ──")
    start_wait = time.time()
    while time.time() - start_wait < 120:
        sc = query(SP, SLAVE_PORT, "DBSIZE").splitlines()[-1]
        if sc and sc.isdigit() and int(sc) > 0:
            elapsed = time.time() - start_wait
            print(f"  Done in {elapsed:.1f}s, slave has {sc} keys")
            break
        time.sleep(2)
    else:
        print("  Timeout!")

    # ── Step 5: 拉取日志分析 ──────────────────────────────────────────
    print(f"\n── Step 5: Parsing metrics ──")
    master_log = ssh_cmd(MP, "cat /tmp/*.log 2>/dev/null | grep -E 'CHILD.*complete' | tail -1", PW)
    slave_log  = ssh_cmd(SP, "cat /tmp/slave_bench.log", PW)

    print("\n========== Master (send) ==========")
    for line in master_log.strip().split("\n"):
        print(f"  {line}")

    print("\n========== Slave (recv) ==========")
    ms = parse_slave_metrics_from_str(slave_log)
    for k, v in ms.items():
        print(f"  [{k}] {v['ms']:.0f}ms  {v['mbps']:.1f} MB/s  {v['entries']} entries  {v['bytes']/1024/1024:.1f} MB")

    print("\n========== 速率对照 ==========")
    print("  Master send  : 序列化 + 网络发送的纯数据平面速率")
    print("  Slave  recv  : 接收 + 解析 + dict_add 速率 (不含连接建立)")
    print("  Slave  e2e   : 从连接到 rebuild 全程速率 (含 RDMA CM + rebuild)")
    print("  整体速率取 Master send 最准确 — 它测的是实际数据传输时间。")

def parse_slave_metrics_from_str(text: str) -> dict:
    """从日志文本直接解析"""
    recv_pat = re.compile(
        r"\[SLAVE\] Recv throughput: (\d+) bytes, (\d+) entries, "
        r"([\d.]+) ms, ([\d.]+) MB/s, ([\d.]+) entries/s")
    e2e_pat = re.compile(
        r"\[SLAVE\] End-to-end: (\d+) bytes, (\d+) entries, "
        r"([\d.]+) ms, ([\d.]+) MB/s")
    result = {}
    for line in text.split("\n"):
        m = recv_pat.search(line)
        if m:
            result["recv"] = {"bytes": int(m.group(1)), "entries": int(m.group(2)),
                              "ms": float(m.group(3)), "mbps": float(m.group(4)),
                              "eps": float(m.group(5))}
        m = e2e_pat.search(line)
        if m:
            result["e2e"] = {"bytes": int(m.group(1)), "entries": int(m.group(2)),
                             "ms": float(m.group(3)), "mbps": float(m.group(4))}
    return result

if __name__ == "__main__":
    main()
