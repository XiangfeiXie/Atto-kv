#!/usr/bin/env python3
"""
populate_expire.py — 批量写入带 TTL 的 key（直连 TCP RESP 流水线）

用法: python3 populate_expire.py <host> <port> <start> <end> <vsize> <ttl>

- 每条命令: SET key:<i> <value> EX <ttl>
- 流式生成 + 边发边读 +OK，不会 OOM
- 进度输出到 stderr；最终插入数打印到 stdout（供 bash 读取）
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
    ttl = int(sys.argv[6])

    total = end - start + 1
    t0 = time.time()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(900)
    sock.connect((host, port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    # 固定 value（二进制，非整数，强制 RAW 编码）
    unit = struct.pack('>Q', 0)
    rep = (vsize + 7) // 8
    val = (unit * rep)[:vsize]
    vlen = str(len(val)).encode()
    ex = str(ttl).encode()
    exlen = str(len(ex)).encode()

    sent_count = 0
    resp_buf = bytearray()
    pending = 0
    SEND_BATCH = 500

    i = start
    while i <= end:
        batch_end = min(i + SEND_BATCH - 1, end)
        cmd = bytearray()
        for j in range(i, batch_end + 1):
            k = f'key:{j:012d}'.encode()
            klen = str(len(k)).encode()
            # *5 SET key val EX ttl
            cmd += b'*5\r\n$3\r\nSET\r\n$' + klen + b'\r\n' + k + b'\r\n'
            cmd += b'$' + vlen + b'\r\n' + val + b'\r\n'
            cmd += b'$2\r\nEX\r\n$' + exlen + b'\r\n' + ex + b'\r\n'

        mv = memoryview(cmd)
        while mv:
            n = sock.send(mv)
            if n <= 0:
                raise OSError("send failed")
            mv = mv[n:]

        batch_count = batch_end - i + 1
        pending += batch_count

        # 尽量非阻塞排空响应，避免发送缓冲/响应积压
        sock.setblocking(False)
        while pending > 0:
            try:
                chunk = sock.recv(65536)
                if not chunk:
                    raise OSError("connection closed")
                resp_buf.extend(chunk)
            except BlockingIOError:
                pass

            while True:
                idx = resp_buf.find(b'\r\n')
                if idx < 0:
                    break
                line = resp_buf[:idx]
                del resp_buf[:idx + 2]
                pending -= 1
                if line == b'+OK':
                    sent_count += 1

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
                break

        sock.setblocking(True)
        i = batch_end + 1

        now = time.time()
        if now - t0 >= 5:
            elapsed = now - t0
            rate = sent_count / elapsed if elapsed > 0 else 0
            pct = (i - start) * 100.0 / total
            print(f"  [{pct:.0f}%] {i - 1}/{end}  sent={sent_count}  "
                  f"pending={pending}  rate={rate:.0f}/s",
                  file=sys.stderr, end='\r')

    # 发送完毕，排空剩余响应
    sock.settimeout(60)
    deadline = time.time() + 60
    while pending > 0 and time.time() < deadline:
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

    print(f"  [100%] sent={sent_count}  rate={rate:.0f}/s  time={elapsed:.1f}s",
          file=sys.stderr)
    print(sent_count)


if __name__ == '__main__':
    main()
