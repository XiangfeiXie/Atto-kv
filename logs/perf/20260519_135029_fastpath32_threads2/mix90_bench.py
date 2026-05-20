import argparse, socket, time, threading, random, statistics

def enc(*parts):
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        if isinstance(p, str): p = p.encode()
        out.append(f"${len(p)}\r\n".encode()); out.append(p); out.append(b"\r\n")
    return b"".join(out)

def read_line(sock):
    data = bytearray()
    while True:
        c = sock.recv(1)
        if not c: raise EOFError
        data += c
        if len(data) >= 2 and data[-2:] == b"\r\n":
            return bytes(data[:-2])

def read_resp(sock):
    p = sock.recv(1)
    if not p: raise EOFError
    if p in (b'+', b'-', b':'):
        return read_line(sock)
    if p == b'$':
        n = int(read_line(sock))
        if n < 0: return None
        buf = bytearray()
        need = n + 2
        while len(buf) < need:
            chunk = sock.recv(need - len(buf))
            if not chunk: raise EOFError
            buf += chunk
        return bytes(buf[:n])
    if p == b'*':
        n = int(read_line(sock))
        return [read_resp(sock) for _ in range(n)]
    raise RuntimeError(f"bad RESP prefix {p!r}")

def one(cmds, host, port, pipeline):
    s = socket.create_connection((host, port), timeout=5)
    s.settimeout(20)
    done = 0
    for i in range(0, len(cmds), pipeline):
        batch = cmds[i:i+pipeline]
        s.sendall(b''.join(batch))
        for _ in batch:
            read_resp(s)
        done += len(batch)
    s.close()
    return done

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=2000)
    ap.add_argument('--ops', type=int, default=600000)
    ap.add_argument('--clients', type=int, default=64)
    ap.add_argument('--pipeline', type=int, default=16)
    ap.add_argument('--hot-keys', type=int, default=4096)
    ap.add_argument('--get-ratio', type=float, default=0.90)
    ap.add_argument('--value-bytes', type=int, default=64)
    ap.add_argument('--seed', type=int, default=1)
    args = ap.parse_args()
    value = b'v' * args.value_bytes
    per = args.ops // args.clients
    remain = args.ops - per * args.clients
    results = []
    def worker(cid, nops):
        rnd = random.Random(args.seed + cid)
        cmds = []
        for _ in range(nops):
            k = f"fp:{rnd.randrange(args.hot_keys)}"
            if rnd.random() < args.get_ratio:
                cmds.append(enc('GET', k))
            else:
                cmds.append(enc('SET', k, value))
        t0 = time.perf_counter()
        done = one(cmds, args.host, args.port, args.pipeline)
        t1 = time.perf_counter()
        results.append((done, t1 - t0))
    ths=[]
    t0=time.perf_counter()
    for c in range(args.clients):
        n = per + (1 if c < remain else 0)
        t = threading.Thread(target=worker, args=(c,n))
        t.start(); ths.append(t)
    for t in ths: t.join()
    t1=time.perf_counter()
    total=sum(x[0] for x in results)
    wall=t1-t0
    print(f"ops={total}")
    print(f"seconds={wall:.6f}")
    print(f"qps={total/wall:.2f}")
    print(f"clients={args.clients}")
    print(f"pipeline={args.pipeline}")
    print(f"hot_keys={args.hot_keys}")
    print(f"get_ratio={args.get_ratio}")
if __name__ == '__main__': main()
