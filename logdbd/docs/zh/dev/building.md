# 开发指南

## 环境

- Rust 1.85+（edition 2024）
- protoc（protobuf 编译器，Ubuntu: `apt install protobuf-compiler`）

## 编译

```bash
# 编译整个 workspace（4 个 crate）
cargo build

# 单独编译
cargo build -p logdb          # 嵌入式库
cargo build -p logdbd         # gRPC 服务
cargo build -p logdb-exporter # CDC 导出
cargo build -p logdb-client   # Rust SDK

# Release 构建
cargo build --release
```

## 测试

```bash
# 全 workspace 测试
cargo test --workspace

# 单独模块测试
cargo test -p logdb            # 库测试（~180 个）
cargo test -p logdbd           # 服务测试（59 个）
cargo test -p logdbd --lib     # 仅单元
cargo test -p logdbd --test integration  # 仅集成
cargo test -p logdb-exporter   # 导出器测试
cargo test -p logdb-client     # SDK 测试
```

## 项目结构

```
lv-logdb/                     # Cargo workspace
├── logdb/                    # 嵌入式日志库
├── logdbd/                   # gRPC 集群服务
│   ├── proto/logdbd.proto    # protobuf 定义（17 个 RPC）
│   ├── src/
│   │   ├── main.rs           # 服务入口
│   │   ├── bin/logdbd-admin.rs  # 管理 CLI
│   │   ├── config.rs         # YAML 配置
│   │   ├── node.rs           # 节点身份 + 进程锁
│   │   ├── catalog.rs        # namespace/stream 名称→ID
│   │   ├── consumer.rs       # consumer group offset 追踪
│   │   ├── record.rs         # Record binary format
│   │   ├── storage.rs        # logdb::LogDb 封装
│   │   ├── service.rs        # gRPC 17 个 RPC handler
│   │   ├── replication.rs    # 主备复制引擎
│   │   └── snapshot.rs       # Snapshot 全量传输
│   └── tests/                # 20 个集成测试
├── logdb-exporter/           # CDC 导出进程
│   └── src/sink/             # stdout / clickhouse
└── logdb-client/             # Rust SDK
    └── src/client.rs         # Client + TailStream
```

## 数据流

### 写入路径

```
gRPC Append → service.rs → catalog.resolve(name → id)
                          → storage.append()
                              → record.encode()     # header + user_content
                              → logdb.append()      # ring → committer → fsync
                              → seq_map.insert()    # seq → gid 映射
                          → AppendResponse{seq, gid}
```

### 读取路径

```
gRPC Read → service.rs → catalog.resolve(name → id)
                        → storage.read(stream_id, seq)
                            → seq_map → gid
                            → logdb.read(gid)
                            → record.decode()
                        → ReadResponse{record}
```

### 复制路径

```
Primary run_primary_sync()
  → logdb.scan(durable) → ReplicationRecord{gid, ts, raw_bytes}
  → gRPC Sync RPC → Standby

Standby ReplicationServiceImpl::sync()
  → storage.replicate(gid, ts, content)
      → logdb.replicate()    # 写入
      → record.decode()      # 解码 header
      → seq_map.insert()     # 重建 seq→gid 映射
```

## 添加新的 RPC

1. 在 `proto/logdbd.proto` 中定义 message 和 RPC
2. 在 `service.rs` 中实现 `LogDbService` trait 方法
3. 添加集成测试

## 代码风格

- Rust edition 2024
- `cargo fmt` 格式化
- `cargo clippy` 静态检查
- 公开 API 使用 `pub(crate)` 控制可见性

## CI

参见 `.github/workflows/ci.yml`：

| Job | 内容 |
|-----|------|
| `fmt` | `cargo fmt --check` |
| `logdb` | build + test（多 feature 矩阵） |
| `msrv` | MSRV 1.85 构建验证 |
| `logdbd` | build + test（需要 protoc） |
| `clippy` | workspace clippy |
| `coverage` | llvm-cov 覆盖率 |
| `licenses` | cargo-deny 许可证检查 |
