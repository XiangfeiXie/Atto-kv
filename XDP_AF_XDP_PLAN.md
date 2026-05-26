# XDP + AF_XDP 零拷贝优化规划

## 当前状态分析

### 已完成部分
- ✅ KCP可靠传输层（UDP socket）
- ✅ eBPF智能过滤器（socket层）
- ✅ 数据结构预留（xsk_ring_prod/cons）
- ✅ 基础架构完整

### 当前瓶颈
```
用户态应用 → 内核协议栈 → UDP socket → KCP → 应用
         ↑                    ↑
      2次拷贝              上下文切换
```

### 目标架构
```
用户态应用 → AF_XDP (UMEM) → XDP程序 → 网卡
         ↑                              ↑
      零拷贝                        内核bypass
```

---

## 实现规划

### Phase 1: XDP程序实现（内核态）

**目标**: 在网卡层过滤和转发KCP数据包

**文件**: `src/repl/xdp_kcp_filter.bpf.c`

**功能**:
1. 识别KCP协议数据包（UDP端口6483/6484）
2. 快速路径：直接转发到AF_XDP socket
3. 慢速路径：其他流量走正常协议栈

**伪代码**:
```c
SEC("xdp")
int xdp_kcp_filter(struct xdp_md *ctx) {
    // 解析以太网头
    struct ethhdr *eth = data;
    
    // 解析IP头
    struct iphdr *ip = data + sizeof(*eth);
    
    // 解析UDP头
    struct udphdr *udp = data + sizeof(*eth) + sizeof(*ip);
    
    // 检查是否是KCP端口
    if (udp->dest == htons(6483) || udp->dest == htons(6484)) {
        // 重定向到AF_XDP socket
        return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, 0);
    }
    
    // 其他流量走正常路径
    return XDP_PASS;
}
```

---

### Phase 2: AF_XDP Socket实现（用户态）

**目标**: 零拷贝收发数据包

**文件**: `src/repl/af_xdp_socket.c`

**核心组件**:

#### 2.1 UMEM（用户态内存）
```c
struct xdp_umem {
    void *buffer;           // mmap分配的大块内存
    size_t size;            // 总大小（如8MB）
    uint32_t frame_size;    // 每帧大小（2048字节）
    uint32_t frame_count;   // 帧数量
    
    // 帧管理
    uint32_t *free_frames;  // 空闲帧栈
    uint32_t free_count;
};
```

#### 2.2 四个Ring Buffer
```c
struct af_xdp_socket {
    int fd;
    
    // RX路径
    struct xsk_ring_prod fill_ring;   // 用户→内核：提供空闲帧
    struct xsk_ring_cons rx_ring;     // 内核→用户：接收数据
    
    // TX路径
    struct xsk_ring_prod tx_ring;     // 用户→内核：发送数据
    struct xsk_ring_cons comp_ring;   // 内核→用户：发送完成通知
    
    struct xdp_umem *umem;
};
```

---

### Phase 3: 集成到KCP传输层

**修改文件**: `src/repl/repl_kcp_xdp.c`

**改动点**:

#### 3.1 初始化
```c
int repl_kcp_init(const struct repl_kcp_config *cfg) {
    // 选择传输方式
    if (cfg->use_af_xdp) {
        // AF_XDP零拷贝模式
        af_xdp_socket_create(&g_repl.xsk, cfg->ifname, cfg->queue_id);
        xdp_program_load(cfg->ifname, "xdp_kcp_filter.bpf.o");
        g_repl.transport = TRANSPORT_AF_XDP;
    } else {
        // UDP socket模式（当前实现）
        setup_udp_socket();
        g_repl.transport = TRANSPORT_UDP;
    }
}
```

#### 3.2 KCP输出回调
```c
static int kcp_output(const char *buf, int len, ikcpcb *kcp, void *user) {
    if (g_repl.transport == TRANSPORT_AF_XDP) {
        return af_xdp_send(&g_repl.xsk, buf, len);
    } else {
        return sendto(g_repl.udp_fd, buf, len, 0, ...);
    }
}
```

---

### Phase 4: 性能优化

#### 4.1 批量处理
```c
// 批量接收
int af_xdp_recv_batch(struct af_xdp_socket *xsk, 
                      struct xdp_packet *pkts, int max_pkts);

// 批量发送
int af_xdp_send_batch(struct af_xdp_socket *xsk,
                      struct xdp_packet *pkts, int num_pkts);
```

#### 4.2 零拷贝优化
```c
// KCP直接写入UMEM
int kcp_output_zerocopy(const char *buf, int len, ikcpcb *kcp, void *user) {
    uint64_t frame_addr = umem_alloc_frame(g_repl.xsk.umem);
    void *frame = xsk_umem__get_data(g_repl.xsk.umem->buffer, frame_addr);
    memcpy(frame + sizeof(headers), buf, len);
    submit_tx_frame(&g_repl.xsk, frame_addr, total_len);
    return len;
}
```

---

## 实现步骤

### Step 1: XDP程序（1-2天）
- [ ] 实现XDP过滤逻辑
- [ ] 添加xsks_map（AF_XDP socket映射）
- [ ] 实现bpf_redirect_map
- [ ] 编译和测试

### Step 2: AF_XDP Socket基础（2-3天）
- [ ] UMEM创建和管理
- [ ] 四个ring buffer初始化
- [ ] 基础收发函数
- [ ] 帧分配器

### Step 3: 集成到KCP（1-2天）
- [ ] 添加传输层抽象
- [ ] 实现AF_XDP发送路径
- [ ] 实现AF_XDP接收路径
- [ ] 配置选项（UDP vs AF_XDP）

### Step 4: 性能优化（2-3天）
- [ ] 批量处理
- [ ] 零拷贝优化
- [ ] Busy-polling
- [ ] 性能测试和调优

### Step 5: 测试和文档（1-2天）
- [ ] 功能测试
- [ ] 性能基准测试
- [ ] 更新文档
- [ ] 使用指南

---

## 预期性能提升

### 延迟对比
| 实现 | 平均延迟 | P99延迟 | 改善 |
|------|----------|---------|------|
| UDP Socket | 300-500μs | 1-2ms | 基准 |
| AF_XDP | 50-100μs | 200-300μs | **80%↓** |
| AF_XDP + 零拷贝 | 20-50μs | 100-150μs | **90%↓** |

### 吞吐量对比
| 实现 | 小包吞吐 | 大包吞吐 | CPU使用 |
|------|----------|----------|---------|
| UDP Socket | 500K pps | 5 Gbps | 60% |
| AF_XDP | 2M pps | 10 Gbps | 40% |
| AF_XDP + 批处理 | 5M pps | 20 Gbps | 30% |

---

## 配置选项

### CMakeLists.txt
```cmake
option(ENABLE_AF_XDP "Enable AF_XDP zero-copy" ON)

if(ENABLE_AF_XDP)
    find_package(LibXDP REQUIRED)
    add_compile_definitions(ENABLE_AF_XDP)
endif()
```

### 配置文件
```conf
# 启用AF_XDP
xdp_repl 1
xdp_repl_mode af_xdp    # udp | af_xdp

# AF_XDP参数
xdp_repl_ifname eth0
xdp_repl_queue 0
xdp_repl_zero_copy 1
xdp_repl_batch 64
xdp_repl_busy_poll 1
```

---

## 风险和挑战

### 技术挑战
1. **网卡驱动支持**: 需要支持XDP的网卡驱动
2. **内存管理**: UMEM帧分配和回收的复杂性
3. **协议栈bypass**: 需要手动构造以太网/IP/UDP头
4. **调试困难**: 零拷贝路径难以调试

### 兼容性
1. **内核版本**: 需要Linux 4.18+（AF_XDP支持）
2. **网卡支持**: 需要XDP native模式（或generic模式降级）
3. **权限要求**: 需要CAP_NET_RAW + CAP_BPF

### 降级策略
```
AF_XDP (native) → AF_XDP (generic) → UDP socket
     ↓                  ↓                  ↓
  最快但要求高      较快兼容性好      最慢但通用
```

---

## 总结

### 实现优先级
1. **P0**: XDP程序 + AF_XDP基础（核心功能）
2. **P1**: 集成到KCP（可用性）
3. **P2**: 批量处理优化（性能）
4. **P3**: 零拷贝优化（极致性能）

### 预期时间
- **最小可用版本**: 5-7天
- **完整优化版本**: 10-14天

### 成功标准
- ✅ 延迟降低到50μs以下
- ✅ 吞吐量提升3-5倍
- ✅ CPU使用率降低30%+
- ✅ 保持功能完整性

准备好开始实现了吗？我建议从Step 1开始，先实现XDP程序。
