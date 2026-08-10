#!/usr/bin/env python3
"""
populate_kv.py — 可靠的批量填充脚本 (直连 TCP socket, 无 redis-cli 依赖)

用法: python3 populate_kv.py <host> <port> <start> <end> <vsize>

- 边生成边发送，流式流水线，不会 OOM
- 显式读取所有 +OK 响应，确保不丢数据
- 进度每 5 秒输出到 stderr；最终插入数打印到 stdout
"""

import socket
import struct
import sys
import time


def main():
    host = sys.argv[1]
    port = int(sys.argv[2])
    start = int(sys.argv[3])
    end = int(sys.argv[4])
    vsize = int(sys.argv[5])

    total = end - start + 1
    t0 = time.time()
    last_report = t0

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(900)  # 15 min
    sock.connect((host, port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    sent_count = 0
    resp_buf = bytearray()
    pending = 0          # 已发送但未收到响应的命令数

    # 每 SEND_BATCH 条 flush 一次
    SEND_BATCH = 500

    i = start
    while i <= end:
        batch_end = min(i + SEND_BATCH - 1, end)
        cmd = bytearray()
        for j in range(i, batch_end + 1):
            k = f'key:{j:012d}'.encode()
            unit = struct.pack('>Q', j)
            rep = (vsize + 7) // 8
            v = (unit * rep)[:vsize]
            # RESP: *3\r\n$3\r\nSET\r\n$<klen>\r\n<k>\r\n$<vlen>\r\n<v>\r\n
            cmd += f'*3\r\n$3\r\nSET\r\n${len(k)}\r\n'.encode()
            cmd += k
            cmd += f'\r\n${len(v)}\r\n'.encode()
            cmd += v
            cmd += b'\r\n'

        # sendall — 大块数据可能需要多次 send
        mv = memoryview(cmd)
        while mv:
            n = sock.send(mv)
            if n <= 0:
                raise OSError("send failed")
            mv = mv[n:]

        batch_count = batch_end - i + 1
        pending += batch_count

        # ── 读取响应（尽可能读，不阻塞）──
        sock.setblocking(False)
        while pending > 0:
            try:
                chunk = sock.recv(65536)
                if not chunk:
                    raise OSError("connection closed")
                resp_buf.extend(chunk)
            except BlockingIOError:
                # 没有更多数据可读，下个 send batch 后再试
                pass

            # 解析尽可能多的完整行
            while True:
                idx = resp_buf.find(b'\r\n')
                if idx < 0:
                    break
                line = resp_buf[:idx]
                del resp_buf[:idx + 2]
                pending -= 1
                if line == b'+OK':
                    sent_count += 1
                # kvstore SET 永远返回 +OK (或断开连接)

            if pending > SEND_BATCH * 2:
                # 响应落后太多，短暂阻塞等待
                sock.setblocking(True)
                try:
                    chunk = sock.recv(65536)
                    if chunk:
                        resp_buf.extend(chunk)
                except socket.timeout:
                    pass
                sock.setblocking(False)
            else:
                break  # 回到外层继续 send 更多命令

        sock.setblocking(True)
        i = batch_end + 1

        # 进度
        now = time.time()
        if now - last_report >= 5:
            elapsed = now - t0
            rate = sent_count / elapsed if elapsed > 0 else 0
            pct = (i - start) * 100.0 / total
            print(f"  [{pct:.0f}%] {i-1}/{end}  sent={sent_count}  "
                  f"pending={pending}  rate={rate:.0f}/s",
                  file=sys.stderr, end='\r')
            last_report = now

    # ── 发送完毕，排空所有剩余响应 ──
    sock.settimeout(60)
    drain_deadline = time.time() + 60
    while pending > 0 and time.time() < drain_deadline:
        try:
            chunk = sock.recv(65536)
            if not chunk:
                break
            resp_buf.extend(chunk)
        except socket.timeout:
            break
        while True:
            idx = resp_buf.find(b'\r\n')
            if idx < 0:
                break
            line = resp_buf[:idx]
            del resp_buf[:idx + 2]
            pending -= 1
            if line == b'+OK':
                sent_count += 1

    sock.close()
    elapsed = time.time() - t0
    rate = sent_count / elapsed if elapsed > 0 else 0

    if pending > 0:
        print(f"  [WARN] {pending} responses never received", file=sys.stderr)

    print(f"  [100%] {end}/{end}  sent={sent_count}  "
          f"rate={rate:.0f}/s  time={elapsed:.1f}s",
          file=sys.stderr)

    # 输出成功插入数到 stdout (供 bash 脚本读取)
    print(sent_count)


if __name__ == '__main__':
    main()
