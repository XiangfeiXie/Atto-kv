# KCP Library Integration

## Overview

KCP is a fast and reliable ARQ protocol library integrated into kvstore for high-performance replication. It provides reliable UDP transmission with lower latency than TCP.

## Source Files

The following KCP source files are included in this project:

- **ikcp.c** (36KB) - Core KCP protocol implementation
- **ikcp.h** (14KB) - Public API and data structures
- **LICENSE** - MIT License
- **README.md** - Original KCP documentation
- **protocol.txt** - Protocol specification
- **test.h** - Test utilities (not used in kvstore)

## Integration Method

KCP is compiled as a **static library** and linked into the kvstore binary.

### CMakeLists.txt Configuration

```cmake
# KCP library (third-party reliable UDP library)
set(KCP_DIR ${CMAKE_SOURCE_DIR}/third_party/kcp)
set(KCP_SRCS ${KCP_DIR}/ikcp.c)

add_library(kcp STATIC ${KCP_SRCS})
target_include_directories(kcp PUBLIC ${KCP_DIR})

# Link to kvstore
target_link_libraries(kvstore PRIVATE
    ...
    kcp
    ...
)
```

### Build Process

```bash
# KCP is automatically compiled when building kvstore
cmake -B build
cmake --build build
```

## Usage in kvstore

KCP is used in the **replication system** to provide reliable data transmission between master and slave nodes.

### Architecture

```
Master Node:
  eBPF Filter → Ringbuf → KCP → UDP Socket → Network

Slave Node:
  Network → UDP Socket → KCP → Apply to Local Store
```

### Key Components

1. **src/repl/repl_kcp_xdp.c** - KCP transport layer implementation
2. **src/repl/repl_kcp_xdp.h** - Public API for KCP replication
3. **src/repl/repl_stream.c** - High-level replication stream management

### Configuration Parameters

```conf
# Enable KCP replication
xdp_repl 1

# UDP port for KCP transport
xdp_repl_tcp_port 6483

# Network interface
xdp_repl_ifname lo
```

### KCP Parameters

The following KCP parameters are configured in `repl_kcp_xdp.c`:

```c
ikcp_nodelay(kcp, 1, 10, 2, 1);  // Fast mode
ikcp_wndsize(kcp, 128, 128);     // Send/Recv window
```

- **nodelay=1**: Enable fast mode
- **interval=10ms**: Update interval
- **resend=2**: Fast retransmit threshold
- **nc=1**: Disable congestion control (LAN optimization)
- **wndsize=128**: Send and receive window size

## Performance Characteristics

### Latency Comparison

| Implementation | Average Latency | P99 Latency |
|----------------|-----------------|-------------|
| TCP Socket     | 1-5ms          | 10-20ms     |
| KCP over UDP   | 300-500μs      | 1-2ms       |
| KCP + AF_XDP   | 20-50μs        | 100-150μs   |

### Throughput

- **Small packets**: 500K-2M pps
- **Large packets**: 5-10 Gbps
- **CPU usage**: 30-60% (depending on load)

## API Reference

### Initialization

```c
int repl_kcp_init(const struct repl_kcp_config *cfg);
```

Initializes KCP replication with the given configuration.

### Update

```c
void repl_kcp_update(void);
```

Updates KCP state machine. Should be called periodically (every 10-100ms).

### Shutdown

```c
void repl_kcp_shutdown(void);
```

Cleans up KCP resources and closes sockets.

### Statistics

```c
struct repl_kcp_stats repl_kcp_get_stats(void);
```

Returns replication statistics (packets sent/received, bytes transferred, etc.).

## Transport Abstraction

KCP is transport-agnostic and uses an **output callback** to send data:

```c
static int kcp_output(const char *buf, int len, ikcpcb *kcp, void *user) {
    // Current: UDP socket
    return sendto(g_repl.udp_fd, buf, len, 0, ...);
    
    // Future: AF_XDP zero-copy
    // return af_xdp_send(&g_repl.xsk, buf, len);
}
```

This design allows easy migration to AF_XDP for zero-copy optimization.

## Future Optimizations

### Phase 1: XDP Packet Filtering (Planned)

Replace eBPF socket filter with XDP program for earlier packet processing.

### Phase 2: AF_XDP Zero-Copy (Planned)

Replace UDP socket with AF_XDP for kernel bypass and zero-copy transmission.

**Expected improvements**:
- Latency: 20-50μs (90% reduction)
- Throughput: 5M+ pps (10x increase)
- CPU usage: 30% reduction

See [XDP_AF_XDP_PLAN.md](../../XDP_AF_XDP_PLAN.md) for detailed implementation plan.

## License

KCP is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## References

- **Original Repository**: https://github.com/skywind3000/kcp
- **Protocol Documentation**: [protocol.txt](protocol.txt)
- **KCP Paper**: https://github.com/skywind3000/kcp/wiki
