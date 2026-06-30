# 快速开始

一个最小化的上手示例：将 logdb 引入项目，打开数据库，写入一条记录，刷盘，然后读回来。

## 目录

- [前置条件](#前置条件)
- [添加依赖](#添加依赖)
- [最小示例](#最小示例)
- [数据目录里会出现什么](#数据目录里会出现什么)
- [下一步](#下一步)

## 前置条件

logdb 基于 **Rust 2021 edition**。请通过 [rustup](https://rustup.rs/) 安装较新版本的 stable 工具链（建议 1.70 或更新）：

```bash
rustup default stable
rustc --version
```

logdb 是一个以 Linux 为首要平台的内嵌数据库。它使用了 Linux 系统调用（`fdatasync`、`syncfs`、`clock_realtime_coarse`），并在 Linux 上开发与测试。macOS 通常可用于开发；Windows/WSL2 的 fdatasync 行为可能较慢，不作为生产目标平台。

## 添加依赖

在你的 `Cargo.toml` 中加入 logdb：

```toml
[dependencies]
logdb = { version = "0.2", path = "…" }   # 当前可用 path 或 git 源
```

logdb **默认不开启任何特性**（`default = []`）。按需开启所需能力：

| 特性            | 启用内容                                  | 适用场景                                   |
|-----------------|------------------------------------------|--------------------------------------------|
| `hash-chain`    | SHA-256 / BLAKE3 前向链式防篡改           | 需要防篡改证据 / 审计追溯时。               |
| `compression`   | 段文件的 zstd 帧压缩                      | 愿意用 CPU 换取更低的磁盘占用时。           |
| `encryption`    | AES-256-GCM 静态加密                      | 存储敏感数据、需要保密性时。                |
| `remote-push`   | 为远程推送预留的标志位（无额外依赖）       | 为将来的远程复制预留。                      |

例如，同时启用 hash-chain 与 compression：

```toml
[dependencies]
logdb = { version = "0.2", features = ["hash-chain", "compression"] }
```

## 最小示例

本示例在一个临时目录中打开数据库，写入一条记录，刷盘到持久化存储，然后再读回来。它对应 `tests/integration.rs` 中测试的 `open → append → flush → read` 生命周期，以及 `src/lib.rs` 中 `LogDb` 模块文档的描述。

```rust
use std::time::Duration;
use std::path::PathBuf;

use logdb::config::{Config, DurabilityMode};
use logdb::LogDb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 构造配置。在你的应用里请把 data_dir 指向真实路径。
    let mut config = Config::default();
    config.data_dir = PathBuf::from("/tmp/logdb-getting-started");
    config.durability_mode = DurabilityMode::Async; // 或 Sync 以获得最强保证
    config.flush_timeout = Duration::from_secs(5);

    // 2. 打开（若目录与第一个段不存在则会创建）。
    let db = LogDb::open(config)?;

    // 3. 写入一条记录。返回它的全局 record id。
    let id = db.append(b"hello logdb")?;
    println!("appended record id = {}", id);

    // 4. 强制把所有已写入记录刷到持久化（已 fsync）存储。
    db.flush()?;

    // 5. 读回记录。只有已 fsync 的记录对读者可见。
    let record = db.read(id)?.expect("record should exist after flush");
    assert_eq!(record.id.sequence, id);
    assert_eq!(record.content, b"hello logdb");
    println!("read: {:?}", record);

    // 6. 排空在途写入并优雅关闭。
    db.shutdown(Duration::from_secs(5))?;
    Ok(())
}
```

上面用到的关键 API 签名（来自 `src/lib.rs`）：

```rust
impl LogDb {
    pub fn open(config: Config) -> Result<Self, String>;
    pub fn append(&self, content: &[u8]) -> Result<u64, AppendError>;
    pub fn flush(&self) -> Result<(), FlushError>;
    pub fn read(&self, record_id: u64) -> Result<Option<Record>, ReadError>;
}
```

注意事项：

- `append` 返回**全局 record id**（`u64`）。在默认的单分区场景下，它就是你回传给 `read` 的值，也等于 `record.id.sequence`。
- 当记录不存在**或尚未被 fsync** 时，`read` 返回 `Ok(None)` —— 读者只能看到 `durable_cursor()` 之下的数据（参见[核心概念](concepts.md)）。
- `flush` 会阻塞，直到 `durable_cursor` 越过你写入的记录为止，因此它是写者与读者之间天然的同步点。

## 数据目录里会出现什么

在第一次 `append`/`flush` 之后，你的 `data_dir` 大致如下：

```
/tmp/logdb-getting-started/
├── segment-00000001.log   # 仅追加段文件（记录 + 头部）
├── segment-00000001.idx   # 用于快速定位的稀疏索引（仅原始段）
└── checkpoint.dat         # WAL 检查点：序号低于此值的记录可被截断
```

- **`segment-NNNNNNNN.log`** —— 仅追加的段文件，当达到 `segment_size`（默认 256 MiB）时自动滚动。下一个段会在当前段到达 80% 容量时**预创建**，从而把滚动期的阻塞降到一次 `fdatasync`。
- **`segment-NNNNNNNN.idx`** —— 稀疏索引，使 `read()` 能够定位到目标记录附近，而非从头扫描（仅未压缩/未加密的原始段才有）。
- **`checkpoint.dat`** —— 持久化的检查点序号，原子写入（临时文件 + `fdatasync` + rename）。崩溃恢复用它来限定 WAL 重放范围。

完全低于检查点的旧段会在下次滚动时被截断，并受你的保留策略约束。

## 下一步

- [核心概念](concepts.md) —— 核心模型：记录、`RecordId`、段、环形缓冲与游标语义。
- [实践手册](cookbook.md) —— 常见任务配方（批处理、扫描、保留策略等）。

## 相关链接

- [使用指南总览](README.md)
- [核心概念](concepts.md)
- [配置](configuration.md)

> logdb 0.2.0
