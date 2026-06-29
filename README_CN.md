# logdb

嵌入式、追加写、可崩溃恢复、可选防篡改的本地日志数据库。Rust 实现。

## 文档

完整文档位于 [`docs/`](docs/) 目录下。参见[使用指南](docs/zh/README.md)与[二次开发指南](docs/zh/dev/README.md)。

API 参考：运行 `cargo doc --open`（发布后亦可在 docs.rs 查看）。

## 快速开始

```rust
use logdb::{Config, LogDb};

let db = LogDb::open(Config::default())?;
let seq = db.append(b"hello")?;          // 写入
db.flush()?;                             // 持久化
let record = db.read(seq)?.unwrap();     // 读取
assert_eq!(record.content, b"hello");
```

## 核心特性

- **无锁写入**：多生产者 CAS 环形缓冲区，p50 < 60ns
- **三种持久化模式**：
  - `Sync`：每次提交后 fdatasync
  - `Batch`：达到字节数/条数/时间阈值后 fdatasync
  - `Async`：仅显式 `flush()` 时 fdatasync，吞吐最高
- **哈希链**（`hash-chain` feature）：BLAKE3 keyed 模式，防篡改。每个数据库生成一次 hash_init，并持久化到段头中，重启时恢复以重新校验哈希链
- **流式压缩**（`compression` feature）：每批 zstd 压缩为一个帧，读写透明
- **加密**（`encryption` feature）：AES-256-GCM 逐帧加密，随机 nonce
- **WAL checkpoint**：持久化到 `checkpoint.dat`，崩溃后自动恢复
- **原子批量写入**：`append_batch()` 整批写入一个帧，要么全在要么全不在
- **崩溃恢复**：torn write 检测 + 截断 + 哈希链验证
- **分片**：多 Ring 支持高核心数线性扩展
- **远程推送**（`remote-push` feature）：异步推送持久化记录到远端，不反压本地写入
- **段预分配**：80% 容量时提前创建下一个 segment，滚动零阻塞

## 作为数据库 WAL 使用

```rust
// ── 正常写入 ──
db.append_batch(&[redo1, undo, redo2])?; // 原子批量
db.flush()?;
db.checkpoint(db.durable_cursor());      // 持久化 checkpoint

// ── 崩溃后恢复 ──
let report = db.recovery_report();
// report: { from_sequence: 50000, to_sequence: 100000, count: 50000 }
for rec in db.replay_from(report.from_sequence)? {
    apply_to_primary_store(rec?);         // 重放到主存储
}

// ── 运维监控 ──
let (used, total) = db.wal_usage();      // WAL 空间使用量
```

完整示例：`cargo run --example wal`

## Feature 开关

| Feature | 默认 | 功能 |
|---------|------|------|
| `hash-chain` | 关闭 | BLAKE3 keyed 哈希链，提供防篡改完整性校验 |
| `compression` | 关闭 | 流式 zstd 压缩，磁盘空间节省 |
| `encryption` | 关闭 | AES-256-GCM 加密，审计场景刚需 |
| `remote-push` | 关闭 | 异步推送持久化记录到远端 |

```toml
[dependencies]
logdb = { features = ["compression", "encryption"] }
```

## 架构

```
多个 Producer 线程
     │ append(content)
     ▼
┌─────────────────────────┐
│  Ring (可选 sharded)     │  ← CAS 无锁 claim，inline ≤ 256B 零分配
│  Slot: { 内容, hash }   │
└──────────┬──────────────┘
           │
    (可选) Sealer 线程     ← 计算 BLAKE3 keyed 哈希链
           │
    Committer 线程          ← 批量序列化 + pwrite + fdatasync
           │
    ┌──────┴──────┐
    │ Segment 文件 │         ← 追加写，满即滚动，checkpoint 截断
    └─────────────┘
           │
    Reader / Pusher         ← 读取查询 / 远程推送
```

## 测试

```bash
cargo test                          # 103 个单元测试
cargo test --features compression   # 含压缩测试
cargo test --test fuzz              # 基于属性的随机测试 (proptest)
cargo +nightly fuzz run <target>    # libfuzzer + AddressSanitizer
```

## 性能基线（SATA SSD，8 vCPU 云 VM）

| 指标 | 数值 |
|------|------|
| append(64B) p50 | 54ns |
| append(256B) p50 | 57ns |
| append(256B) p99 | 230ns |
| append(256B) 单线程吞吐 | 3.79M rec/s |
| append(256B) 4 线程吞吐 | 4.48M rec/s |
| 端到端持久化延迟 p99 | 10.4ms (NVMe 预期 <2ms) |
| 段滚动停顿 | 0ms（预分配 + idle drain） |

## 许可证

MIT OR Apache-2.0
