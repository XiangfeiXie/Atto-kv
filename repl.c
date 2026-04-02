#include "repl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <sys/wait.h>

// 全局复制变量
int repl_role = REPL_MASTER;
char repl_master_ip[64] = {0};
int repl_master_port = 0;
int repl_master_fd = -1;

char repl_backlog[REPL_BACKLOG_SIZE];
long long repl_offset = 0;
long long backlog_start_offset = 0;
int backlog_len = 0;

int slave_fds[MAX_SLAVES];
int slave_count = 0;
int g_current_client_fd = -1;

static pthread_t slave_thread;

// 初始化复制模块
void repl_init(void) {
    memset(repl_backlog, 0, REPL_BACKLOG_SIZE);
    memset(slave_fds, -1, sizeof(slave_fds));
    repl_offset = 0;
    backlog_start_offset = 0;
    backlog_len = 0;
    slave_count = 0;
}

// 主机：添加从机（去重）
void repl_add_slave(int fd) {
    if (fd < 0 || slave_count >= MAX_SLAVES) return;
    
    for (int i = 0; i < slave_count; i++) {
        if (slave_fds[i] == fd) return;
    }
    slave_fds[slave_count++] = fd;
    printf("✅ 主机新增从机：fd=%d 总数=%d\n", fd, slave_count);
}

// 主机：写入命令到环形缓冲区
void repl_feed_backlog(const char *cmd, int len) {
    if (backlog_len + len > REPL_BACKLOG_SIZE) {
        int remove = backlog_len + len - REPL_BACKLOG_SIZE;
        backlog_start_offset += remove;
        backlog_len -= remove;
        memmove(repl_backlog, repl_backlog + remove, backlog_len);
    }
    memcpy(repl_backlog + backlog_len, cmd, len);
    backlog_len += len;
    repl_offset += len;
}

// 主机：群发命令给所有从机
void repl_propagate(const char *cmd, int len) {
    for (int i = 0; i < slave_count; i++) {
        int fd = slave_fds[i];
        if (fd >= 0) send(fd, cmd, len, 0);
    }
}

// ==================== 【核心修复】从机：解析RDB + 全量同步 + 增量同步 ====================
void* repl_slave_sync_thread(void *arg) {
    char buf[4096];
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);

    // 第一步：接收并解析 RDB 协议头（$长度\r\n）
    int header_idx = 0;
    char rdb_header[64] = {0};
    while (1) {
        ssize_t n = read(repl_master_fd, rdb_header + header_idx, 1);
        if (n <= 0) goto disconnect;

        header_idx++;
        // 读到 \r\n 代表头结束
        if (header_idx >= 2 && rdb_header[header_idx-1] == '\n' && rdb_header[header_idx-2] == '\r') {
            break;
        }
    }

    // 解析 RDB 文件总长度
    long long rdb_total_len = atoll(rdb_header + 1);
    printf("✅ 从机开始接收 RDB 文件，大小：%lld 字节\n", rdb_total_len);

    // 第二步：接收完整 RDB 二进制数据，写入文件
    FILE *rdb_fp = fopen("dump.rdb", "wb");
    long long recv_len = 0;
    while (recv_len < rdb_total_len) {
        ssize_t n = read(repl_master_fd, buf, (recv_len + sizeof(buf) > rdb_total_len) ? (rdb_total_len - recv_len) : sizeof(buf));
        if (n <= 0) break;

        fwrite(buf, 1, n, rdb_fp);
        recv_len += n;
    }
    fclose(rdb_fp);

    // 第三步：加载 RDB 恢复数据（全量同步完成）
    printf("✅ RDB 接收完成，开始加载恢复数据...\n");
    kvs_flush_all();
    rdb_load();
    printf("✅ 从机全量同步成功！进入增量命令同步模式\n");

    // 第四步：循环处理主机发送的增量命令（原有逻辑，保持不变）
    while (1) {
        ssize_t n = read(repl_master_fd, buf, sizeof(buf)-1);
        if (n <= 0) {
            disconnect:
            printf("❌ 从机与主机断开连接\n");
            close(repl_master_fd);
            repl_master_fd = -1;
            break;
        }

        // 执行主机同步的写命令
        kvs_protocol(buf, n, &wb);
        dynbuf_clear(&wb);
    }

    dynbuf_free(&wb);
    return NULL;
}

// 从机：连接主机
int repl_slave_connect(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    repl_master_fd = fd;
    // 发送PSYNC请求同步
    const char *psync = "*2\r\n$5\r\nPSYNC\r\n$1\r\n0\r\n";
    write(fd, psync, strlen(psync));
    // 启动同步线程
    pthread_create(&slave_thread, NULL, repl_slave_sync_thread, NULL);
    return 0;
}

// 处理主从命令：PSYNC / REPLCONF / PING
int repl_handle_command(char **tokens, int count, DynBuf *wb) {
    if (strcmp(tokens[0], "PSYNC") == 0) {
        if (g_current_client_fd >= 0) {
            repl_add_slave(g_current_client_fd);
        }

        rdb_save_async();
        waitpid(-1, NULL, WNOHANG);

        FILE *fp = fopen("dump.rdb", "rb");
        if (!fp) {
            dynbuf_append(wb, "-ERR rdb fail\r\n", 13);
            return -1;
        }
        fseek(fp, 0, SEEK_END);
        long rdb_len = ftell(fp);
        fseek(fp, 0, SEEK_SET);
        char *rdb_buf = malloc(rdb_len);
        fread(rdb_buf, 1, rdb_len, fp);
        fclose(fp);

        char head[64];
        sprintf(head, "$%ld\r\n", rdb_len);
        dynbuf_append(wb, head, strlen(head));
        dynbuf_append(wb, rdb_buf, rdb_len);
        free(rdb_buf);
        printf("✅ 主机发送全量RDB成功，从机fd=%d\n", g_current_client_fd);
        return 0;
    }
    else if (strcmp(tokens[0], "REPLCONF") == 0) {
        dynbuf_append(wb, "+OK\r\n", 5);
        return 0;
    }
    else if (strcmp(tokens[0], "PING") == 0) {
        dynbuf_append(wb, "+PONG\r\n", 7);
        return 0;
    }
    return -1;
}

// 切换为主机
void repl_switch_to_master(void) {
    if (repl_role == REPL_MASTER) return;
    
    if (repl_master_fd >= 0) {
        close(repl_master_fd);
        repl_master_fd = -1;
    }
    repl_role = REPL_MASTER;
    printf("🔄 已切换为【主机】\n");
}

// 切换为从机
void repl_switch_to_slave(const char *ip, int port) {
    if (repl_role == REPL_SLAVE) return;

    strncpy(repl_master_ip, ip, sizeof(repl_master_ip)-1);
    repl_master_port = port;
    repl_role = REPL_SLAVE;

    if (repl_slave_connect(ip, port) == 0) {
        printf("🔄 已切换为【从机】，连接主机：%s:%d\n", ip, port);
    } else {
        printf("❌ 从机连接主机失败\n");
        repl_switch_to_master();
    }
}