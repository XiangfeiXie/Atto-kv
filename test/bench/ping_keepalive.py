#!/usr/bin/env python3
"""
ping_keepalive.py — 保持事件循环活跃的轻负载（持续 PING）

用法: python3 ping_keepalive.py <host> <port> <duration_seconds>

打开一条持久连接，持续发送 PING 并读取 +PONG，持续 duration 秒。
用于「轻负载」口径：让 reactor 的 epoll_wait 立即返回，使 active_expire_cycle
按事件循环真实迭代率运行，而非空闲时每 100ms 才醒一次。
"""
import socket
import sys
import time


def main():
    host = sys.argv[1]
    port = int(sys.argv[2])
    duration = float(sys.argv[3])

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    s.connect((host, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    deadline = time.time() + duration
    n = 0
    while time.time() < deadline:
        try:
            s.sendall(b'*1\r\n$4\r\nPING\r\n')
            n += 1
            # 每发一条就读掉响应，避免发送缓冲积压导致 server 侧 send 阻塞
            try:
                chunk = s.recv(64)  # +PONG\r\n
                if not chunk:
                    break
            except socket.timeout:
                pass
        except (OSError, BrokenPipeError):
            break

    s.close()
    print(n, file=sys.stderr)


if __name__ == '__main__':
    main()
