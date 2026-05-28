#!/usr/bin/env python3
"""Generate RESP SET commands with TTL for redis-cli --pipe.

Usage:
  gen_expire_data.py <nkeys> <value_size> <ttl_seconds> [--redis-benchmark-keys]
"""
import sys

def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <nkeys> <value_size> <ttl_seconds> [--redis-benchmark-keys]",
              file=sys.stderr)
        sys.exit(1)

    nkeys = int(sys.argv[1])
    vsize = int(sys.argv[2])
    ttl = int(sys.argv[3])
    use_rb_fmt = "--redis-benchmark-keys" in sys.argv

    val = 'a' * vsize
    out = sys.stdout.buffer

    for idx in range(1, nkeys + 1):
        if use_rb_fmt:
            key = f"key:{idx:012d}"
        else:
            key = f"key{idx:06d}"

        # SET key value EX ttl
        out.write(
            f"*5\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${vsize}\r\n{val}\r\n$2\r\nEX\r\n${len(str(ttl))}\r\n{ttl}\r\n".encode()
        )

if __name__ == "__main__":
    main()
