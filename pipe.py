# 生成 10000 条 SET 命令的 RESP 格式文件
with open("batch_commands.txt", "wb") as f:
    for i in range(10000):
        # 命令：SET key{i} value{i}
        # RESP 格式：*3\r\n$3\r\nSET\r\n$4\r\nkey0\r\n$6\r\nvalue0\r\n
        key = f"key{i}".encode()
        value = f"value{i}".encode()
        
        # 拼 RESP
        f.write(b"*3\r\n")
        f.write(b"$3\r\nSET\r\n")
        f.write(f"${len(key)}\r\n".encode())
        f.write(key + b"\r\n")
        f.write(f"${len(value)}\r\n".encode())
        f.write(value + b"\r\n")

print("✅ 生成完成：batch_commands.txt (10000 条 SET 命令)")