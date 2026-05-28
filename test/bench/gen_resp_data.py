#!/usr/bin/env python3
"""Generate RESP SET commands for redis-cli --pipe.

Usage:
  gen_resp_data.py <nkeys> <value_size> [--redis-benchmark-keys] [--random <seed>]

Modes:
  --redis-benchmark-keys  Use "key:XXXXXXXXXXXX" format matching redis-benchmark -r
                          (13 zero-padded digits)
  --random <seed>         Random permutation of key indices (for update workload)
"""
import sys
import random


def main():
    args = sys.argv[1:]

    if len(args) < 2:
        print(f"Usage: {sys.argv[0]} <nkeys> <value_size> [--redis-benchmark-keys] [--random <seed>]",
              file=sys.stderr)
        sys.exit(1)

    nkeys = int(args[0])
    vsize = int(args[1])

    use_rb_fmt = "--redis-benchmark-keys" in args
    rng_seed = None
    for i, a in enumerate(args):
        if a == "--random" and i + 1 < len(args):
            rng_seed = int(args[i + 1])

    val = 'a' * vsize

    # Build index list (possibly randomized)
    indices = list(range(1, nkeys + 1))
    if rng_seed is not None:
        rng = random.Random(rng_seed)
        rng.shuffle(indices)

    out = sys.stdout.buffer

    if use_rb_fmt:
        # redis-benchmark format: key:XXXXXXXXXXXX (12 zero-padded digits, 16 chars total)
        for idx in indices:
            key = f"key:{idx:012d}"
            out.write(
                f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${vsize}\r\n{val}\r\n".encode()
            )
    else:
        for idx in indices:
            key = f"key{idx:06d}"
            out.write(
                f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${vsize}\r\n{val}\r\n".encode()
            )


if __name__ == "__main__":
    main()
