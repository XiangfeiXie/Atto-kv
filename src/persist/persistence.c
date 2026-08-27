/*=============================================================================
 *  persistence.c — 持久化基础工具
 *  
 *  功能：
 *  1. 项目根目录解析（从可执行文件路径推导）
 *  2. 文件完整读取（mmap 优先，read fallback）
 *  
 *  其他持久化 I/O 逻辑在子模块中：
 *  - persist_aof.c  — AOF 写入 + 重写 + 重载
 *  - persist_rdb.c  — RDB 保存（fork + io_uring）和加载（mmap）
 *============================================================================*/

#include "persistence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>

/*=============================================================================
 *  项目根目录管理
 *  由 persist_set_basedir 设置一次，后续只读
 *============================================================================*/

static char  g_basedir[1024] = ".";    /* 项目根目录路径 */
static int   g_basedir_set = 0;        /* 是否已设置标志 */

/**
 * 获取持久化文件的绝对路径
 * 将相对路径拼接到项目根目录后
 * 
 * @param rel 相对路径（如 "build/dump.rdb"）
 * @return 绝对路径字符串（静态缓冲区，不可多线程共享）
 * 
 * 注意：返回指向静态缓冲区的指针，调用者不可 free
 */
const char *persist_path(const char *rel) {
    static char buf[1024];
    int n = snprintf(buf, sizeof(buf), "%s/%s", g_basedir, rel);
    if (n < 0 || (size_t)n >= sizeof(buf)) return rel;  /* 缓冲区不足，返回原路径 */
    return buf;
}

/**
 * 从可执行文件路径推导项目根目录
 * 
 * 解析策略：
 * 1. 获取可执行文件的真实路径（通过 /proc/self/exe 或 realpath）
 * 2. 提取二进制文件所在目录
 * 3. 如果二进制位于 <root>/build/，剥掉 /build 得到根目录
 * 4. 否则使用二进制所在目录作为根目录
 * 
 * @param argv0 main 函数的 argv[0]
 * 
 * 示例：
 *   二进制路径：/home/user/project/build/kvstore
 *   根目录：    /home/user/project
 */
void persist_set_basedir(const char *argv0) {
    if (g_basedir_set) return;  /* 已设置，幂等 */

    /*-------------------------------------------------------------------------
     *  步骤 1：解析可执行文件的真实路径
     *-------------------------------------------------------------------------*/
    char binpath[1024] = {0};
    
    if (argv0 && argv0[0] == '/') {
        /* 已经是绝对路径 */
        strncpy(binpath, argv0, sizeof(binpath) - 1);
    } else if (argv0) {
        /* 相对路径：尝试 /proc/self/exe（Linux 特有），否则使用 realpath */
        ssize_t n = readlink("/proc/self/exe", binpath, sizeof(binpath) - 1);
        if (n <= 0 && realpath(argv0, binpath) == NULL) {
            /* 都失败：使用当前工作目录 + argv0 */
            if (getcwd(binpath, sizeof(binpath) - 1)) {
                size_t blen = strlen(binpath);
                snprintf(binpath + blen, sizeof(binpath) - blen - 1, "/%s", argv0);
            }
        }
    } else {
        /* argv0 为空：使用 /proc/self/exe */
        ssize_t n = readlink("/proc/self/exe", binpath, sizeof(binpath) - 1);
        if (n > 0) binpath[n] = '\0';
    }

    /*-------------------------------------------------------------------------
     *  步骤 2：提取二进制所在目录
     *  dirname() 会就地修改 binpath，需注意
     *-------------------------------------------------------------------------*/
    char *dir = dirname(binpath);

    /*-------------------------------------------------------------------------
     *  步骤 3：如果二进制在 <root>/build/ 中，剥掉 /build 得到根目录
     *-------------------------------------------------------------------------*/
    char *slash = strrchr(dir, '/');
    char *base  = slash ? slash + 1 : dir;
    if (strcmp(base, "build") == 0 && slash)
        *slash = '\0';   /* 截断掉 /build 部分 */

    /* 保存结果 */
    strncpy(g_basedir, dir, sizeof(g_basedir) - 1);
    g_basedir[sizeof(g_basedir) - 1] = '\0';
    g_basedir_set = 1;
}

/*=============================================================================
 *  文件完整读取工具
 *  
 *  策略：优先使用 mmap（高效），失败时 fallback 到 read
 *  
 *  优点：
 *  - mmap 避免用户态到内核态的数据拷贝
 *  - 支持大文件（4GB+）
 *  - 调用者统一接口，无需关心底层实现
 *  
 *  内存管理：
 *  - is_mmap == 1：调用者必须 munmap(*buf, *size)
 *  - is_mmap == 0：调用者必须 free(*buf)
 *  
 *  tag 参数：用于日志前缀（如 "AOF" / "RDB"）
 *============================================================================*/

/**
 * 读取整个文件到内存
 * 
 * @param path 文件路径
 * @param buf 输出：文件内容缓冲区指针
 * @param size 输出：文件大小
 * @param is_mmap 输出：1=mmap 映射，0=malloc+read
 * @param tag 日志标签（用于错误信息）
 * @return FILE_READ_OK 成功，FILE_READ_OPEN_FAIL 打开失败，
 *         FILE_READ_EMPTY 文件为空或读取失败
 */
int file_read_full(const char *path, char **buf, size_t *size,
                   int *is_mmap, const char *tag) {
    *buf = NULL; *size = 0; *is_mmap = 0;

    /* 打开文件 */
    int fd = open(path, O_RDONLY);
    if (fd < 0) return FILE_READ_OPEN_FAIL;

    /* 获取文件大小 */
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size <= 0) {
        close(fd);
        return FILE_READ_EMPTY;
    }
    size_t fsz = (size_t)st.st_size;

    /*-------------------------------------------------------------------------
     *  尝试 mmap（首选方案）
     *  性能优势：避免数据拷贝，内核直接映射
     *-------------------------------------------------------------------------*/
    char *base = (char *)mmap(NULL, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (base != MAP_FAILED) {
        close(fd);
        madvise(base, fsz, MADV_SEQUENTIAL);  /* 顺序访问提示，优化预读 */
        *buf = base; *size = fsz; *is_mmap = 1;
        return FILE_READ_OK;
    }

    /* mmap 失败：记录警告，fallback 到 read */
    printf("[%s] mmap failed: %s, falling back to read()\n", tag, strerror(errno));

    /*-------------------------------------------------------------------------
     *  read fallback（分配堆内存）
     *-------------------------------------------------------------------------*/
    base = malloc(fsz);
    if (!base) { 
        close(fd); 
        return FILE_READ_EMPTY; 
    }

    /* 重新定位到文件开头（mmap 尝试可能改变了文件偏移） */
    lseek(fd, 0, SEEK_SET);
    
    size_t total_read = 0;
    while (total_read < fsz) {
        ssize_t n = read(fd, base + total_read, fsz - total_read);
        if (n <= 0) break;
        total_read += n;
    }
    close(fd);

    /* 验证读取完整性 */
    if (total_read != fsz) {
        printf("[%s] read failed: only read %zu/%zu bytes\n",
               tag, total_read, fsz);
        free(base);
        return FILE_READ_EMPTY;
    }

    *buf = base; *size = fsz; *is_mmap = 0;
    return FILE_READ_OK;
}