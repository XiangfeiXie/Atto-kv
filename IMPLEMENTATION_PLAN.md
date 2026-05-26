# KCP + AF_XDP 复制系统实施计划

## 环境配置清单

### ✅ 已完成
1. **系统环境**
   - Ubuntu 24.04 LTS
   - Kernel 6.8.0-110-generic
   - 支持 eBPF 和 XDP

2. **已安装依赖**
   - libbpf-dev 1.3.0
   - libxdp-dev 1.4.2
   - clang 18
   - llvm 18
   - bpftool v7.4.0

3. **KCP 库**
   - 已下载到 third_party/kcp/
   - 包含 ikcp.c 和 ikcp.h

4. **代码备份**
   - 旧代码已备份到 backup_old_repl/
   - 创建新分支 feature/kcp-xdp-replication

### 📋 待配置项

#### 1. 网络接口配置
```bash
# 查看可用网络接口
ip link show

# 如果使用 loopback 测试
sudo ip link set lo up

# 如果使用虚拟网卡对（推荐用于测试）
sudo ip link add vethm type veth peer name veths
sudo ip link set vethm up
sudo ip link set veths up
sudo ip addr add 192.168.100.1/24 dev vethm
sudo ip addr add 192.168.100.2/24 dev veths
```

#### 2. eBPF 权限配置
```bash
# 检查是否需要 root 权限
sudo sysctl kernel.unprivileged_bpf_disabled

# 如果返回 1，需要以 root 运行或配置 CAP_BPF
# 临时允许非特权用户加载 eBPF（仅用于开发）
sudo sysctl -w kernel.unprivileged_bpf_disabled=0

# 或者使用 capabilities（推荐）
sudo setcap cap_bpf,cap_net_admin,cap_sys_admin+ep ./build/kvstore
```

#### 3. 内存锁定限制
```bash
# AF_XDP 需要锁定大量内存
ulimit -l unlimited

# 永久配置（添加到 /etc/security/limits.conf）
echo "* soft memlock unlimited" | sudo tee -a /etc/security/limits.conf
echo "* hard memlock unlimited" | sudo tee -a /etc/security/limits.conf
```

## 实施步骤

### 阶段 1: 基础架构搭建（当前）
- [x] 创建功能分支
- [x] 下载 KCP 库
- [x] 备份旧代码
- [ ] 修改 CMakeLists.txt 集成 KCP
- [ ] 创建新的复制模块目录结构

### 阶段 2: eBPF 智能过滤层
- [ ] 实现 repl_filter.bpf.c（过滤只读命令）
- [ ] 编译 eBPF 程序
- [ ] 测试 eBPF 加载和卸载

### 阶段 3: KCP + UDP 基础版本
- [ ] 实现 repl_kcp.c（KCP + UDP）
- [ ] 集成到 kvstore 主进程
- [ ] 功能测试

### 阶段 4: KCP + AF_XDP 优化版本
- [ ] 实现 repl_kcp_xdp.c（KCP + AF_XDP）
- [ ] 性能测试和调优
- [ ] 可靠性测试

### 阶段 5: 测试和文档
- [ ] 编写测试脚本
- [ ] 性能基准测试
- [ ] 编写使用文档

## 下一步行动

1. 修改 CMakeLists.txt 添加 KCP 编译
2. 创建新的复制模块文件
3. 实现 eBPF 智能过滤层
