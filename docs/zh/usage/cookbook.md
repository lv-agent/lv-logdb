# 实践手册

可直接复制粘贴的最常见 logdb 负载配方：数据库 WAL、仅追加事件/日志存储、防篡改且机密的审计日志、崩溃安全的消费者管线、备节点摄入，以及长驻服务内的优雅关闭。每段示例都只使用真实的公共 API —— 没有虚构方法。

## 目录

- [配方：数据库 WAL](#配方数据库-wal)
- [配方：仅追加事件 / 日志存储](#配方仅追加事件--日志存储)
- [配方：防篡改且机密的审计日志](#配方防篡改且机密的审计日志)
- [配方：崩溃安全的消费者管线](#配方崩溃安全的消费者管线)
- [配方：备节点摄入](#配方备节点摄入)
- [配方：服务内的优雅关闭](#配方服务内的优雅关闭)
- [相关链接](#相关链接)

## 配方：数据库 WAL

把 logdb 用作数据库的预写日志（WAL）：追加每次变更、在提交边界 `flush`、待应用吸收记录后推进 `checkpoint`、重启时从 checkpoint `replay_from`。这正是 `examples/wal.rs` 所做的 —— 完整可运行示例请阅读该文件。

五个构建块及其真实签名：

```rust
impl LogDb {
    /// 追加一条记录；返回其全局 record_id。
    pub fn append(&self, content: &[u8]) -> Result<u64, AppendError>;

    /// 原子地追加多条记录（相对崩溃是全有或全无）。
    pub fn append_batch(&self, contents: &[&[u8]]) -> Result<u64, AppendError>;

    /// 阻塞直到持久化游标越过所有已追加记录。
    pub fn flush(&self) -> Result<(), FlushError>;

    /// 将 sequence 标记为 WAL checkpoint：< sequence 的记录可被截断。
    pub fn checkpoint(&self, sequence: u64);

    /// 迭代 [sequence, 日志末尾) 的记录，用于 open 时重建状态。
    pub fn replay_from(&self, sequence: u64) -> Result<RecordIter, ReadError>;
}
```

骨架（`Async` 模式，因为应用在提交边界自行 `flush`）：

```rust
use std::time::Duration;
use logdb::config::{Config, DurabilityMode};
use logdb::LogDb;

struct KvStore {
    db: LogDb,
    // replay_from 是本次会话恢复时使用的 checkpoint。本会话写入的记录
    // （sequence >= replay_from）仍可恢复。
    replay_from: u64,
}

impl KvStore {
    fn open(data_dir: &str, replay_checkpoint: u64) -> Self {
        let mut config = Config::default();
        config.data_dir = data_dir.into();
        config.durability_mode = DurabilityMode::Async; // 在提交点显式 flush()
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        // 从上一个 checkpoint 重放以重建内存状态。
        for result in db.replay_from(replay_checkpoint).unwrap() {
            let record = result.unwrap();
            // ...应用 record.content 中编码的变更...
        }
        Self { db, replay_from: replay_checkpoint }
    }

    fn put(&mut self, key: &str, value: &str) {
        let wal = format!("PUT {} {}", key, value);
        self.db.append(wal.as_bytes()).unwrap();
        self.db.flush().unwrap(); // 提交边界：先持久化再应用
        // ...应用到内存状态...
    }

    fn checkpoint(&self) {
        // checkpoint 我们恢复时的稳定点，而不是实时的持久化尾。把 checkpoint
        // 设在 durable_cursor 会覆盖你刚写的记录，使恢复时无任何内容可重放。
        self.db.checkpoint(self.replay_from);
    }

    fn close(self) {
        let _ = self.db.shutdown(Duration::from_secs(5)).unwrap();
    }
}
```

WAL 模式依赖的两个正确性要点：

1. **先 flush 再应用。** 写 WAL 条目，调用 `flush`（让持久化游标越过它），然后再变更内存状态。`flush` 之后崩溃会重放该条目；`flush` 之前崩溃则不会。
2. **checkpoint 恢复点，而非尾端。** `checkpoint(sequence)` 告诉 logdb“`sequence` 之前的记录可被截断”。若 checkpoint 活动的 `durable_cursor()`，你就覆盖了刚写的记录，下次 `recovery_report().count` 为 `0`。见[恢复：checkpoint](recovery.md#checkpoint) 与 `examples/wal.rs` 中的注释。

崩溃重放契约见[恢复：WAL 模式](recovery.md#wal-模式)。

## 配方：仅追加事件 / 日志存储

对写密集的事件或应用日志存储（遥测、访问日志、审计事件），你需要高吞吐追加和有序重放（从头或任意 offset）。用默认 `Batch` 持久化，用 `scan` / `replay_from` 取历史。

```rust
use logdb::Config;
use logdb::LogDb;

let mut config = Config::default();
config.data_dir = "./event-store".into();
let db = LogDb::open(config).unwrap();

// 写入方追加事件。Batch 模式跨多条记录摊销 fsync，
// 吞吐高且有界的数据风险窗口。
let id = db.append(b"{\"level\":\"info\",\"msg\":\"hello\"}").unwrap();

// 历史重放：从头迭代所有内容。
for result in db.replay_from(0).unwrap() {
    let record = result.unwrap();
    println!("seq={} ts={} {}", record.record_id, record.timestamp_ns,
             String::from_utf8_lossy(&record.content));
}

// 范围扫描：迭代 [from_id, to_id)。
let iter = db.scan(100, 200).unwrap();
for result in iter {
    let record = result.unwrap();
    // ...
}
```

对**实时消费者**（跟随日志尾、带各自崩溃安全游标），请用 tailer 而非轮询 `scan` —— 见下面[配方：崩溃安全的消费者管线](#配方崩溃安全的消费者管线)与[尾读（Tailers）](tailers.md)。

## 配方：防篡改且机密的审计日志

组合 `hash-chain` 与 `encryption` 特性，使日志既**防篡改**（sealed 段的事后篡改可在读取时检测）又**机密**（记录用 AES-256-GCM 加密落盘）。两者读取时透明 —— 同样的 `read` / `scan` / `replay_from` API 透明地校验链并解密，调用方无需改动。

```toml
# Cargo.toml —— 同时启用两个特性。
[dependencies]
logdb = { version = "0.2.0", features = ["hash-chain", "encryption"] }
```

```rust
use logdb::Config;
use logdb::LogDb;

// 32 字节 AES-256 密钥 —— 在带外生成并管理（KMS / vault / 信封加密）。
// 丢失密钥不可恢复：用丢失密钥加密的记录无法解密。
let key: [u8; 32] = /* 你的密钥 */;

let mut config = Config::default();
config.data_dir = "./audit-log".into();
config.hash_enabled = true;        // hash-chain：防篡改
config.encryption_key = Some(key); // encryption：落盘机密
// hash-chain 要求 shards == 1（Sealer 一次只 seal 一个分片）。
config.shards = 1;
let db = LogDb::open(config).unwrap();

// 每条记录都被封入 BLAKE3 keyed 链 AND 用 GCM 加密。
db.append(b"2026-06-30T12:00:00Z user=alice action=login").unwrap();
db.flush().unwrap();
```

各特性的贡献：

- **`hash-chain`** 以每库一把 BLAKE3 密钥（`hash_init`，持久化在每个段头）为种子，把每条记录的哈希与前一条链接，因此 sealed 段中任意字节的改动都会破坏其后所有记录的验证。Sealer 后台线程仅在 `hash_enabled` 且 `shards == 1` 时运行。见[特性：hash-chain](features.md#hash-chain)。
- **`encryption`** 用 AES-256-GCM 加密每个 frame，使用逐 frame 的随机 nonce，GCM 的认证标签在读取时检测篡改（像 CRC 失败那样上报）。见[特性：encryption](features.md#encryption)。

> hash 链检测的是*未同时重建链*的损坏/篡改；encryption 通过隐藏明文和检测 GCM 标签失败把门槛抬高。单独任一都不是针对“既能改字节又能重算链”攻击者的完整安全边界 —— 该威胁模型请叠加外部控制。注意事项见[特性](features.md#hash-chain)。

## 配方：崩溃安全的消费者管线

以至少一次（at-least-once）投递和崩溃安全游标，把记录投递到下游系统（副本、搜索索引、消息 broker）。用 `new_tailer` + `next_batch` + `commit`。

```rust
use std::time::Duration;
use logdb::LogDb;

fn run_pipeline(db: &LogDb) -> Result<(), Box<dyn std::error::Error>> {
    // new_tailer 返回 Tailer，不是 Result。若 tailer_indexer.dat 存在，
    // 恢复保存的位置；否则从 0 开始。
    let mut t = db.new_tailer("indexer");

    loop {
        match t.next_batch(500)? {
            Some(batch) => {
                // 1. 先投递副作用 —— 在推进游标前使其持久化。
                deliver_to_downstream(&batch)?;

                // 2. 然后才提交进度。next_batch 与 commit 之间崩溃会重放该批
                //    （至少一次）；commit 之后崩溃则已越过它。
                t.commit()?;
            }
            None => {
                // 日志尾已追上；短暂退避后再轮询。
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
}
```

此配方强制执行的两个不变式（见[尾读：进度仅在 commit() 时持久化](tailers.md#进度仅在-commit时持久化)）：

1. **先投递，后提交。** 副作用必须在游标推进前持久化，使崩溃永远不会丢失一条既未投递又未留待处理的记录。
2. **幂等投递。** 由于第 1 点的重放保证，跨重启时 `deliver_to_downstream` 可能两次看到同一批。使其幂等（按 record id upsert，或用幂等键去重）。

要更高吞吐，把批开大（`next_batch(10_000)`）；要更低延迟，更频繁轮询。`None` 分支是你的反压信号 —— 没有新内容可读。见[尾读：消费者循环配方](tailers.md#一个消费者循环配方)。

## 配方：备节点摄入

在主/备部署中，备节点按**主节点自己的 sequence 号**摄入从主节点收到的记录，保留全局 offset 空间，使消费者能在主 → 备之间 failover 而无需重映射 offset。这正是 `LogDb::replicate` 的用途。

```rust
use logdb::Config;
use logdb::LogDb;

// replicate 要求 shards == 1（它是 onto shard 0 的线性流）。
let mut config = Config::default();
config.data_dir = "./standby-data".into();
config.shards = 1;
let db = LogDb::open(config).unwrap();

// 摄入一条经你的 Sync RPC 从主节点收到的记录。
let sequence = 1234u64;       // 该记录在主节点的 record_id
let timestamp_ns = 1_700_000_000_000_000_000u64;
let content: &[u8] = b"replicated payload";
db.replicate(sequence, timestamp_ns, content).unwrap();
```

`replicate` 的契约（`src/lib.rs:326-391`，见[特性：remote-push](features.md#remote-push)）：

- **单分片。** `shards` 必须为 `1`；否则 `AppendError::Io("replicate requires shards=1")`。
- **按序。** `sequence` 必须等于当前生产者游标；出现间隔返回 `AppendError::Io("replicate out of order: expected {cur}, got {sequence}")`，因此调用方重试同一 sequence 直到落地。
- **幂等。** 已复制的 `sequence`（游标之下）是空操作 `Ok(())`，因此重复或重放的 Sync RPC 是安全的。
- **反压。** 拒绝覆盖在途（未提交）的 slot，经与 `append` 相同的水位线门返回 `AppendError::QueueFull`。

v1.1 在主节点侧**没有**一行式的 `db.push(...)` API —— Pusher / `RemoteSink` 管道是守护进程级别且私有的。该缺口与守护进程集成模式见[特性：remote-push](features.md#remote-push)。

## 配方：服务内的优雅关闭

在长驻服务内，`LogDb` 是共享的（通常是 `Arc<LogDb>`），因此不能调用 `shutdown(self, timeout)`（它消费唯一的强引用）。请改用 `drain(&self, timeout)`：它把一切刷到持久化存储，既不消费 handle 也不 join 线程，因此可与 `Arc<LogDb>` 一起工作。

```rust
use std::sync::Arc;
use std::time::Duration;
use logdb::LogDb;

// 服务内 handle 是共享的：
let db: Arc<LogDb> = /* ... */;

// 收到 SIGTERM / drain 信号时：排空在途追加并把到生产者游标为止的数据 fsync。
// 取 &self，因此可与 Arc<LogDb> 一起用。
match db.drain(Duration::from_secs(10)) {
    Ok(report) => {
        // report 是 ShutdownReport：Clean | PartialDurable | TimedOut。
        // Clean => 调用前追加的每条记录现在都已持久化。
        println!("drain: {:?}", report);
    }
    Err(e) => {
        // FlushError::Timeout | FlushError::Aborted。
        eprintln!("drain failed: {:?}", e);
    }
}

// drain 返回 Ok(Clean) 后，新追加返回 AppendError::ShuttingDown。
// 后台线程继续运行；进程随后可退出（线程在 drop 时无害中止，因为数据已持久化）。
```

为什么这里用 `drain` 而非 `shutdown`：

- **`drain(&self, timeout)`** 进入 drain 阶段（新追加返回 `AppendError::ShuttingDown`），等待在途追加 publish，并把到生产者游标为止的数据 fsync。后台线程继续运行。返回 `Result<ShutdownReport, FlushError>`。
- **`shutdown(self, timeout)`** 消费 `self`，drain，*然后* join 后台线程。它要求 handle 是*唯一*的强引用，因此当存在其他 `Arc<LogDb>` 克隆时无法使用。

在 WSL2 上，`fdatasync` 延迟可能导致 `Clean` 的 drain 被保守地报为 `PartialDurable` —— 分类偏向悲观。见[持久化：优雅关闭](durability.md#优雅关闭)与[错误处理：ShutdownReport](errors.md#shutdownreport)。

## 相关链接

- [使用指南](README.md)
- [写入](writing.md) —— `append`、`append_batch`、反压。
- [读取](reading.md) —— `read`、`scan`、`replay_from`。
- [持久化](durability.md) —— `flush`、`drain`、`shutdown`、`ShutdownReport`。
- [恢复](recovery.md) —— WAL 模式、checkpoint、崩溃重放。
- [尾读（Tailers）](tailers.md) —— `new_tailer`、`next_batch`、`commit`。
- [特性](features.md) —— `hash-chain`、`encryption`、`remote-push` / `replicate`。

> logdb 0.2.0
