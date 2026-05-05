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
