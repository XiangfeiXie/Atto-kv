# KVStore Development Log

## 2026-05-05 — Phase 1 & 2: Unified Dict Engine + Type System

### Overview

Replaced four fragmented storage engines (array, rbtree, hash, skiplist) with a single unified Dict engine featuring progressive rehashing. Built a compile-time type system framework on top, with STRING as the first type implementation.

### Phase 1: Unified Dict Engine

**New files:**
- `src/engine/kv_dict.h` — Dict API with progressive rehash, single-malloc entry layout
- `src/engine/kv_dict.c` — ~400 lines: hash function, rehash step, auto expand/shrink, iteration

**Removed files:**
- `src/engine/kvs_array.c`
- `src/engine/kvs_rbtree.c`
- `src/engine/kvs_hash.c`
- `src/engine/kvs_skiptable.c`

**Design decisions:**
- **Single malloc per entry**: `[kv_entry header][type:1][encoding:1][payload...][key bytes]` layout eliminates 4–5 separate allocations per SET operation.
- **Progressive rehashing**: Two hash tables (ht[0], ht[1]), bucket-by-bucket migration amortized O(1). Iterators pause rehash to keep scans consistent.
- **Hash function**: FNV-1a 64-bit offset basis + Murmur3 finalizer for avalanche on power-of-two table sizes.
- **Auto-resize**: Expand when load factor > 2.0, shrink when < 0.1. Max 16M buckets (2^24).
- **Memory**: ~70% reduction for typical entry (352B → 104B for 8B key + 16B value).

### Phase 2: Type System

**New files:**
- `src/type/kv_type_string.h` — STRING type handler interface
- `src/type/kv_type_string.c` — ~250 lines: SET/GET/DEL/MOD/EXIST with lazy expiry

**Type tags (data[0]):** STRING=0, LIST=1, SET=2, ZSET=3, HASH=4
**Encoding tags (data[1]):** RAW=0, PACKED=1, QUICKLIST=2, DICT=3, SKIPLIST=4

**Command dispatch flow:**
1. `resp_parse_slices()` — zero-copy, single-pass RESP parser
2. `kvs_dispatch()` — route global commands (PING/ECHO/SAVE/FLUSHALL) first, then key-based commands by entry type
3. `string_cmd()` — first-byte switch + TOKEN_EQ for sub-command matching
4. Write commands auto-trigger AOF append + eBPF probe

### Modified files

- `src/core/kvstore.h` — Removed 4 engine type definitions (~200 lines), added `extern kv_dict_t g_dict`
- `src/core/kvstore.c` — Rewrote command dispatch with two-level routing, added EXPIRE support
- `src/core/config.h` — Removed engine enum and field from KvsConfig
- `src/core/config.c` — Removed engine parsing, version → "KVStore 10.0"
- `src/persist/persistence.c` — Switched from per-engine scan to `dict_iter`-based scan
- `src/repl/rdma_repl.c` — Removed 4 extern engine variable declarations
- `CMakeLists.txt` — Replaced 4 old engine sources with `kv_dict.c`, added type sources
- `conf/kvstore.conf` — Removed engine section
- `conf/kvstore-master.conf` — Removed engine line
- `conf/kvstore-slave.conf` — Removed engine line

### Bugs fixed

- **dict_resize_entry key corruption**: When data area grows, `realloc` copies old memory but the key remained at the old offset (`data + old_val_len`) while `entry_key()` reads from `data + new_val_len`. Fixed by adding `memmove` to relocate key bytes after resize. Also fixed the shrink case (key needs to move backward when `new_data_len < old_val_len`).

### Verified

- SET / GET / DEL / MOD / EXIST / EXPIRE / PING / ECHO / SAVE / FLUSHALL
- MOD with value grow, shrink, and same-length replacement
- EXPIRE with lazy expiration (GET returns nil after TTL)
- Pipeline (multiple RESP commands in one TCP segment)
- RDB save/load compatibility
- AOF persistence compatibility

### Pending (Phase 3+)

- LIST / SET / ZSET / HASH type implementations
- TTL command
- KEYS / SCAN commands
- Blocking commands (BLPOP, etc.)
- Lua scripting support

---

## 2026-05-05 — Refactoring: Code Reorganization

### Motivation

`kvstore.c` had grown to 412 lines spanning 6 responsibilities (dynbuf, RESP parser, dispatch, drain loop, lifecycle, main). `server.h` mixed network-layer types with KV protocol declarations. As the project prepares for Phase 3 (LIST/SET/ZSET/HASH types), cleaner module boundaries were needed.

### Changes

**New files — utility layer:**
- `src/util/dynbuf.h` + `src/util/dynbuf.c` — `DynBuf` struct + `dynbuf_init/append/cat/clear/free`, moved from `server.h` and `kvstore.c`

**New files — protocol layer:**
- `src/core/protocol.h` + `src/core/protocol.c` — `kvs_token_t`, `TOKEN_EQ`, `KVS_MAX_TOKENS`, `resp_parse_slices()`, RESP static fragments, `kvs_drain_resp_commands_off()`, `kvs_protocol()`, `kvs_protocol_batch()`, all moved from `kvstore.h` and `kvstore.c`

**Cleaned up:**
- `src/core/server.h` — Now network-only: `conn` struct, `RCALLBACK`, `msg_handler`, `KVS_NET_BUF_INIT`. Includes `dynbuf.h` for `DynBuf`.
- `src/core/kvstore.h` — Now API surface only: `kvs_dispatch`, `g_dict`, `KVItem`, engine lifecycle declarations. Includes `protocol.h`.
- `src/core/kvstore.c` — Reduced from 412 → ~220 lines: only `main()`, `kvs_dispatch()`, `init/destroy/flush/reload`, `KVItem` helpers.
- `src/net/proactor.c` — Added `#include "protocol.h"`
- `src/repl/rdma_repl.c` — Removed redundant `extern kvs_protocol` declaration
- `CMakeLists.txt` — Added `src/util` to sources and include paths

### Result

```
src/
├── util/    dynbuf.h/c      ← 动态缓冲区（零依赖工具模块）
├── core/
│   ├── server.h             ← 连接管理（纯网络层）
│   ├── protocol.h/c         ← RESP 解析 + 帧切分
│   ├── config.h/c           ← 配置加载
│   └── kvstore.h/c          ← 命令分发 + 生命周期 + main
├── engine/  kv_dict.h/c     ← 统一存储引擎
├── type/    kv_type_string.* ← STRING 类型
├── net/     reactor/proactor/ntyco
├── persist/ persistence.*
└── repl/   rdma_repl.* + ebpf_incr_repl.*
```

Each module has a single, well-defined responsibility. Zero behavioral changes — all functional regression tests pass.
