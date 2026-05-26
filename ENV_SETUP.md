# 环境配置总结

## ✅ 已验证的环境

### 1. 系统环境
- **操作系统**: Ubuntu 24.04 LTS
- **内核版本**: 6.8.0-110-generic
- **架构**: x86_64

### 2. 已安装依赖
```
libbpf-dev:     1.3.0-2build2
libxdp-dev:     1.4.2-1ubuntu4
clang:          1:18.0-59~exp2
llvm:           1:18.0-59~exp2
bpftool:        v7.4.0
```

### 3. 网络接口
- **lo**: loopback (可用于本地测试)
- **ens33**: 物理网卡
- **vethm/veths**: 虚拟网卡对 (已存在，已加载 XDP 程序)

### 4. 内存限制
- **当前 memlock**: 1010436 KB (~987 MB)
- **状态**: 足够用于 AF_XDP (需要 ~64MB UMEM)

### 5. KCP 库
- **位置**: third_party/kcp/
- **文件**: ikcp.c, ikcp.h
- **状态**: 已下载，待集成

## ⚠️ 需要注意的事项

### 1. eBPF 权限
由于无法检查 `kernel.unprivileged_bpf_disabled`，需要：
- 方案 A: 使用 root 权限运行 kvstore
- 方案 B: 使用 capabilities (推荐)
  ```bash
  sudo setcap cap_bpf,cap_net_admin,cap_sys_admin+ep ./build/kvstore
  ```

### 2. 虚拟网卡已有 XDP 程序
vethm 和 veths 已经加载了 XDP 程序 (id:262, id:271)
- 需要先卸载旧程序或使用其他接口
- 或者使用 loopback (lo) 进行测试

### 3. 网络配置
虚拟网卡对没有配置 IP 地址，需要：
```bash
sudo ip addr add 192.168.100.1/24 dev vethm
sudo ip addr add 192.168.100.2/24 dev veths
```

## 📋 下一步配置命令

### 选项 1: 使用 loopback (最简单)
```bash
# loopback 已启用，无需额外配置
# 直接使用 127.0.0.1 进行测试
```

### 选项 2: 配置虚拟网卡对
```bash
# 卸载旧的 XDP 程序
sudo ip link set vethm xdp off
sudo ip link set veths xdp off

# 配置 IP 地址
sudo ip addr add 192.168.100.1/24 dev vethm
sudo ip addr add 192.168.100.2/24 dev veths

# 验证
ping -c 3 192.168.100.2
```

### 选项 3: 创建新的虚拟网卡对
```bash
# 创建新的虚拟网卡对
sudo ip link add vethm2 type veth peer name veths2
sudo ip link set vethm2 up
sudo ip link set veths2 up
sudo ip addr add 192.168.101.1/24 dev vethm2
sudo ip addr add 192.168.101.2/24 dev veths2
```

## ✅ 环境就绪

所有必需的依赖都已安装，可以开始实现代码！

推荐使用 **loopback (lo)** 进行初期开发和测试，因为：
- 无需额外配置
- 不会与现有 XDP 程序冲突
- 性能足够用于功能验证
