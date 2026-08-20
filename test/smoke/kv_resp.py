#!/usr/bin/env python3
"""
kv_resp.py — 共享 RESP 客户端: fill (填充) / verify (校验) 两个子命令。

用于持久化重启恢复测试与跨机同步验证。两端只需约定相同的
--n / --start / --prefix, 即可独立生成相同的 key/value, 无需传输数据。

key 规则:   {prefix}:{i:012d}
value 规则: value:{i:012d}      (与 key 同 i, 定长零填充, 纯 ASCII)

用法:
  kv_resp.py fill   --n N [--host H] [--port P] [--start S] [--prefix P]
  kv_resp.py verify --n N [--host H] [--port P] [--start S] [--prefix P]
                    [--once | --wait SEC] [--expect-dbsize N]

退出码: 0 = 成功, 1 = 填充/校验失败, 2 = 参数错误

注意: fill 一次性发送全部命令, N 建议 ≤ 1e6 (否则内存占用偏大)。
"""

import argparse
import socket
import sys
import time


def key_of(i, prefix):
    return f'{prefix}:{i:012d}'


def value_of(i):
    return f'value:{i:012d}'


def enc_set(i, prefix):
    k = key_of(i, prefix)
    v = value_of(i)
    return (f'*3\r\n$3\r\nSET\r\n${len(k)}\r\n{k}\r\n'
            f'${len(v)}\r\n{v}\r\n')


def enc_get(i, prefix):
    k = key_of(i, prefix)
    return f'*2\r\n$3\r\nGET\r\n${len(k)}\r\n{k}\r\n'


class Client:
    """极简 RESP 客户端 (只处理 SET/GET/DBSIZE 所需的回复类型)。"""

    def __init__(self, host, port, recv_timeout=60):
        self.sock = socket.create_connection((host, port), timeout=10)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(recv_timeout)
        self.buf = bytearray()

    def send(self, data):
        if isinstance(data, str):
            data = data.encode()
        self.sock.sendall(data)

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise OSError('connection closed')
        self.buf.extend(chunk)

    def read_line(self):
        while True:
            idx = self.buf.find(b'\r\n')
            if idx >= 0:
                line = bytes(self.buf[:idx])
                del self.buf[:idx + 2]
                return line
            self._fill()

    def read_reply(self):
        line = self.read_line()
        if line[:1] == b':':
            return int(line[1:])
        if line[:1] == b'$':
            length = int(line[1:])
            if length == -1:
                return None
            while len(self.buf) < length + 2:
                self._fill()
            data = bytes(self.buf[:length])
            del self.buf[:length + 2]
            return data
        if line[:1] == b'+':
            return line[1:]
        if line[:1] == b'-':
            return line
        return line

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


def cmd_fill(args):
    c = Client(args.host, args.port)
    data = ''.join(enc_set(i, args.prefix)
                   for i in range(args.start, args.start + args.n))
    c.send(data)

    ok = 0
    pending = args.n
    while pending > 0:
        try:
            line = c.read_line()
        except OSError:
            print(f'FAIL connection closed early ({ok}/{args.n})')
            return 1
        pending -= 1
        if line == b'+OK':
            ok += 1
        else:
            print(f'FAIL unexpected reply: {line!r}')
            return 1
    c.close()

    if ok != args.n:
        print(f'FAIL ok={ok} != n={args.n}')
        return 1
    print(f'OK {ok}')
    return 0


def verify_once(args):
    """单次校验, 返回 (matched, missing, mismatched, dbsize)。"""
    c = Client(args.host, args.port)

    c.send(b'*1\r\n$6\r\nDBSIZE\r\n')
    dbsize = c.read_reply()

    for i in range(args.start, args.start + args.n):
        c.send(enc_get(i, args.prefix))

    matched = missing = mismatched = 0
    for i in range(args.start, args.start + args.n):
        got = c.read_reply()
        exp = value_of(i).encode()
        if got is None:
            missing += 1
        elif got != exp:
            mismatched += 1
        else:
            matched += 1

    c.close()
    return matched, missing, mismatched, dbsize


def cmd_verify(args):
    deadline = time.time() + args.wait
    while True:
        try:
            matched, missing, mismatched, dbsize = verify_once(args)
        except OSError:
            matched = missing = mismatched = 0
            dbsize = -1

        dbsize_ok = (args.expect_dbsize is None) or (dbsize == args.expect_dbsize)

        if matched == args.n and dbsize_ok:
            print(f'OK verified={args.n}')
            return 0

        print(f'  matched={matched} missing={missing} mismatched={mismatched} '
              f'dbsize={dbsize}', file=sys.stderr)

        if args.once or time.time() >= deadline:
            print(f'FAIL matched={matched} missing={missing} '
                  f'mismatched={mismatched} dbsize={dbsize}')
            return 1

        time.sleep(1.0)


def main():
    p = argparse.ArgumentParser(description='RESP fill/verify client for kvstore')
    sub = p.add_subparsers(dest='mode', required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--host', default='127.0.0.1')
    common.add_argument('--port', type=int, default=2000)
    common.add_argument('--n', type=int, default=12000)
    common.add_argument('--start', type=int, default=0)
    common.add_argument('--prefix', default='key')

    pf = sub.add_parser('fill', parents=[common])
    pf.set_defaults(func=cmd_fill)

    pv = sub.add_parser('verify', parents=[common])
    g = pv.add_mutually_exclusive_group()
    g.add_argument('--once', action='store_true', help='单次检查, 不轮询')
    g.add_argument('--wait', type=float, default=60.0, help='轮询超时秒数 (默认 60)')
    pv.add_argument('--expect-dbsize', type=int, default=None,
                    help='额外要求 DBSIZE 精确等于该值')
    pv.set_defaults(func=cmd_verify)

    args = p.parse_args()
    sys.exit(args.func(args))


if __name__ == '__main__':
    main()
