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

---

## 2026-05-05 — Phase 3: LIST Type Implementation (Packed + Quicklist Dual Encoding)

### Overview

Implemented the LIST type — a double-ended sequence of strings — with dual encoding: inline packed arrays for small lists and a linked list of packed nodes (quicklist) for large lists. This is the first collection type beyond STRING, enabling message queue use cases.

### New files

- `src/type/kv_type_list.h` — LIST type interface: `list_cmd()`, `list_cleanup_entry()`, `list_flatten_to_packed()`
- `src/type/kv_type_list.c` — ~1000 lines: packed encoding, quicklist node management, quicklist header, conversion, LRANGE response, command dispatch

### Design

**Dual encoding:**
- **Packed (KV_ENCODING_PACKED)**: Inline array in entry data[], format: `[count:4][elem1_len:4][elem1_data...]...`. Used for lists ≤ 128 elements and ≤ 4096 bytes. Single memmove for front insertions.
- **Quicklist (KV_ENCODING_QUICKLIST)**: Linked list of packed nodes. Entry data[] stores a 30-byte header: `[type:1][encoding:1][total_count:4][node_max:4][head:8][tail:8][node_count:4]`. Node pointers stored as uint64_t raw addresses in data[], safe across realloc because realloc copies byte values. Node capacity: 64 elements or 4096 bytes per node.
- **Automatic conversion**: Exceeding 128 elements or 4096 bytes triggers `list_convert_to_quicklist()`, which saves all elements to temporary buffers, shrinks the entry to QL_HEADER_SIZE (30 bytes), and rebuilds as quicklist nodes.

**Commands implemented:**
- LPUSH / RPUSH — prepend/append with auto-encoding conversion
- LPOP / RPOP — single-element mode returns bulk string, count mode returns array
- LLEN — list length (0 for non-existent key, Redis-compatible)
- LRANGE — range query with negative index support
- LINDEX — single element lookup
- DEL / EXIST — key lifecycle ops (type-agnostic, dispatched by entry type)

**Persistence:**
- `list_cleanup_entry()` — frees quicklist nodes before dict_delete/dict_clear
- `list_flatten_to_packed()` — serializes quicklist to packed format for RDB save (avoids storing raw pointers)
- RDB version bumped from 1 to 2: now saves full data[] including type+encoding bytes, enabling multi-type persistence

**Key invariants maintained:**
- Single malloc per entry (dict layout preserved for both encodings)
- Raw pointer fixup after realloc in ql_node_push_left/right (neighbor prev/next)
- pk_pop_front/back copy element data to caller buffer before memmove (zero-copy pointer would be invalidated)
- Conversion saves all elements to malloc'd buffers before dict_resize_entry (memmove during shrink corrupts in-situ data)

### Modified files

- `src/core/kvstore.c` — Added LIST dispatch routing (`case KV_TYPE_LIST`), LIST commands in new-key routing (LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE/LINDEX), FLUSHALL iterates to call `list_cleanup_entry()`, replaced `string_is_write_cmd()` with inline TOKEN_EQ checks
- `src/persist/persistence.c` — RDB save/load now handles both types via full data[] serialization, quicklist entries flattened before save, bumped RDB version
- `CMakeLists.txt` — Added `src/type/kv_type_list.c` to TYPE_SRCS

### Bugs fixed during implementation

- **QL_HEADER_SIZE=26 corrected to 30**: Header fields total 30 bytes (type+encoding+count+node_max+head+tail+node_count). With 26, node_count overlapped with key position after dict_resize_entry, corrupting lookups.
- **Dangling first pointer after node realloc**: In construction loop, `ql_node_push_right` may realloc the node, invalidating `first`. Fixed by reassigning `first = last` when `!last->prev`.
- **Neighbor pointers not updated after realloc**: `ql_node_push_left/right` now fix up `nn->prev->next` and `nn->next->prev` after realloc.
- **pk_pop_front use-after-free**: Zero-copy pointer to element data became invalid after memmove. Fixed by copying data to caller buffer before memmove.
- **LPOP/RPOP return format**: Single-element pop now returns bulk string; only multi-element (count argument) returns array (Redis-compatible).

### Verified (49/49 tests pass)

- STRING regression: SET/GET/DEL/EXIST/ECHO/PING (no regressions)
- Small packed LIST: RPUSH/LPUSH/LPOP/RPOP/LLEN/LRANGE/LINDEX on inline lists
- 200-element quicklist: automatic conversion, correct elements at all positions, LPOP/RPOP correctness
- Type safety: SET on LIST key returns error, EXIST on LIST key works correctly
- AOF persistence: STRING + LIST data survives restart correctly
- DEL/LLEN on deleted keys: correct behavior (LLEN returns 0 for non-existent)

---

## 2026-05-05 — Phase 0: Network Layer Optimization

### Overview

Optimized the network layer across all three I/O models (reactor, proactor, ntyco) to prepare for blocking commands (Phase 7) and production MQ workloads. Zero behavioral changes — all 49 Phase 3 tests pass on both reactor and proactor.

### Changes

**config.h/c:**
- Added `maxclients` field to KvsConfig (default 10000, range 64–65536)
- Printed in config banner

**server.h — struct conn:**
- `active` — slot-in-use flag for dynamic pool management
- `cur_events` — cached epoll event mask (enables lazy epoll_ctl)
- `blocked` / `block_until_ms` — Phase 7 reserved fields (zero-init for now)

**reactor.c (~30 lines changed):**
- Dynamic connection pool: `calloc(maxclients, sizeof(struct conn))` replaces `conn_list[1024]`
- Slot management: `conn_release()` with bounds checking, frees buffers + DEL from epoll + close + memset
- Lazy epoll_ctl: `conn_set_events()` compares against `cur_events`, skips `epoll_ctl(MOD)` when mask unchanged — eliminates 2 unnecessary syscalls per request
- Timeout-aware epoll_wait: calls `block_nearest_timeout()` stub (returns -1 now, Phase 7 will scan blocked connections)
- EOF-safe recv: on peer FIN, process buffered data before releasing connection (fixes response loss when client closes write side immediately after sending)
- Guard against `r_init_server()` failure (returns -1 on bind error)

**proactor.c (~20 lines changed):**
- Dynamic connection pool: same as reactor
- Slot management: `conn_release()` with bounds checking
- Blocked-connection awareness: skip recv submission when `c->blocked` (Phase 7)
- EOF-safe recv: flush buffered data on recv=0 before releasing
- Added `#include "config.h"` for `kvs_config.maxclients`

**ntyo.c:**
- No changes needed (already heap-allocates per-connection NtyClient structs)

**Config files:**
- `kvstore.conf`, `kvstore-master.conf`, `kvstore-slave.conf`: added `maxclients 10000`

### Test script fix

- Changed `nc -q0` to `nc -w2` in `test/test_list.sh` — OpenBSD netcat `-q0` doesn't wait for response on current system

### Design notes

- **Why not slot-based indexing**: fd-indexed arrays are simpler and safe because Linux fd allocation is monotonic; `conn_release()` zeros the slot so fd reuse won't leak state
- **Why 3 network models are kept**: Proactor for production performance, reactor for compatibility (no io_uring dependency), ntyco for complex state-machine scenarios (blocking commands, transactions, Lua) where coroutine semantics reduce code complexity
- **NtyCo performance ceiling**: ~40-60% of Redis due to rbtree scheduler + 128KB stack memcpy per context switch. io_uring's zero-context-switch model is physically impossible to match with coroutines

### Verified

- Reactor: 49/49 tests pass
- Proactor: 49/49 tests pass
- Dynamic pool startup: correct slot count and memory footprint printed
- Config parsing: maxclients honored, bounds enforced (64–65536)
- Zero warnings, clean build
