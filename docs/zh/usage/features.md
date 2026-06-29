# 特性

logdb 提供四个可选的 Cargo 特性——**默认全部关闭**。每个特性会引入可选依赖，并解锁 [`Config`](configuration.md#字段参考) 上对应的字段。本页给出完整的特性矩阵、各特性的作用，以及开启它们的运维影响。

## 目录

- [特性矩阵](#特性矩阵)
- [开启特性](#开启特性)
- [hash-chain](#hash-chain)
- [compression](#compression)
- [encryption](#encryption)
- [remote-push](#remote-push)
- [相关链接](#相关链接)

## 特性矩阵

特性表（`Cargo.toml`，`default = []`）：

| 特性 | 可选依赖 | 解锁 | 备注 |
|---------|----------------------|---------|-------|
| `hash-chain` | `sha2`、`blake3` | `Config.hash_enabled` | BLAKE3 keyed 哈希链，用于防篡改。**仅单分片**（`shards == 1`）；否则在 `open` 时被拒绝。 |
| `compression` | `zstd`（不启用默认特性） | `Config.compression_enabled` | 流式、逐帧 zstd 压缩。读取时透明。 |
| `encryption` | `aes-gcm`（带 `aes`、`alloc`）、`getrandom` | `Config.encryption_key: Option<[u8;32]>` | 每帧 AES-256-GCM，使用随机 nonce。**密钥丢失不可恢复。** |
| `remote-push` | —（仅标志位） | 通过 `LogDb::replicate` 的备节点写入 | 由特性门控的模块；见 [remote-push](#remote-push)。 |

各特性彼此独立，可任意组合，唯一例外是 `hash-chain` 隐含 `shards == 1`（见 [hash-chain](#hash-chain)）。

## 开启特性

在你的 `Cargo.toml` 中开启特性：

```toml
[dependencies]
logdb = { version = "0.2.0", features = ["hash-chain", "compression"] }

# 或全部开启：
# logdb = { version = "0.2.0", features = ["hash-chain", "compression", "encryption", "remote-push"] }
```

然后在 `Config` 中打开对应字段：

```rust
use logdb::Config;

let config = Config {
    hash_enabled: true,           // 需要特性 "hash-chain"
    compression_enabled: true,    // 需要特性 "compression"
    encryption_key: Some(/* [u8; 32] */), // 需要特性 "encryption"
    ..Config::default()
};
let db = logdb::LogDb::open(config)?;
```

在未启用对应特性门控的情况下设置 `Config` 字段，会在**编译期**失败，而非运行期——`validate()` 不检查特性门控。上述四个旋钮是仅有的由特性门控的 `Config` 字段。

## hash-chain

`hash-chain`（`Config.hash_enabled`）在日志上附加一条防篡改的哈希链，使得对已封存 segment 的事后篡改都能在读取时被发现。它使用 **keyed 模式**的 BLAKE3：哈希链以一个保密的 `hash_init` 作为种子，每条记录的哈希把前一条的哈希与记录正文链接起来，因此链上任意一字节被改动都会破坏其后所有记录的校验。

**`hash_init` 由熵生成，且从不写盘。** 新建数据库时，它在 `open` 阶段由 `generate_hash_init`（`src/lib.rs:685-699`）生成，仅在内存中保存给 Sealer 线程使用。由此带来的影响：

- 读取时**无需密钥即可校验**——读取方重新计算哈希链即可发现篡改。
- 该密钥**并非可恢复的秘密**。进程重启丢失内存中的 `hash_init` 并不会破坏校验（密钥仅用于初始化链首）；只要盘上哈希完整，链条依旧自洽可查。把它排除在磁盘之外的意义在于：读取了文件的攻击者若没有原始密钥，就无法重新跑 keyed BLAKE3 来伪造一条一致的链。
- 哈希链由 **Sealer** 后台线程构建，该线程仅在 `hash_enabled` 且 `shards == 1` 时运行。

**单分片约束。** Sealer 一次只封存一个分片，而跨分片的全局哈希链需要 v1.1 尚未提供的全局合并顺序。当 `hash-chain` 启用且 `shards > 1` 时，`LogDb::open` 返回这条确切的错误（`src/lib.rs:176-181`）：

> hash-chain is not supported with shards > 1 in v1.1. Use shards=1 with hash-chain, or shards>1 without hash.

多分片哈希链推迟到 v1.2。该取舍见 [Sharding](sharding.md)。

## compression

`compression`（`Config.compression_enabled`）对流式 **zstd** 逐帧压缩 segment。每帧独立压缩，因此读取方可以即时解压，无需 seek 到全局字典。该依赖以 `default-features = false` 引入，以保持构建精简。

压缩在读取时**完全透明**：同样的 `LogDb::read` / scan API 会自动解码压缩 segment，调用方无需任何改动。不存在单独的"压缩读取"路径。

运维提示：

- 压缩与稀疏索引有交互：`index_stride` 只影响**原始** segment——压缩 segment 是基于帧的，没有逐记录的稀疏索引，因此该旋钮在此是空操作（见 [Configuration: index_stride](configuration.md#为延迟敏感点读调低-index_stride)）。
- 不存在逐记录的压缩开关；选择在 `Config` 时按数据库级别做出。

## encryption

`encryption`（`Config.encryption_key: Option<[u8;32]>`）用 **AES-256-GCM** 认证加密来加密 segment 帧：

- **每帧随机 nonce。** 每帧从 `getrandom` 取全新 nonce，因此相同的明文记录会被加密成不同的密文。
- **256 位密钥。** 密钥即你通过 `Config.encryption_key` 传入的 32 字节数组。`None` 表示明文（不加密）。
- **带认证。** GCM 每帧附带认证标签，因此读取时能像 CRC 失败一样检出篡改。

```rust
// 32 字节密钥——请通过外部手段生成并管理。
let key: [u8; 32] = /* 你的密钥，例如来自 KMS / vault */;
let config = Config {
    encryption_key: Some(key),
    ..Config::default()
};
```

**密钥管理是你的责任，密钥丢失不可恢复。** 记录按写入时生效的密钥加密；若该密钥丢失，这些记录将无法解密。logdb **不会**存储密钥、不会自动轮换密钥、也不会在盘上包裹密钥——`Config.encryption_key` 就是你传入的那串字节。把它当作任何其它根秘密对待：从 KMS、密封保险库或信封加密方案中获取，且绝不打印日志。

## remote-push

`remote-push` 是一个**仅标志位**的特性：它门控 `pusher` 模块和 `LogDb::replicate` API，但**不**引入任何额外依赖。v1.1 的远程能力被有意拆成两半：

**公共 API——`LogDb::replicate(sequence, timestamp_ns, content)`。** 这是 `LogDb` 上**唯一**与远程相关的方法。它是 `logdbd` 备节点用于按主节点原始序写入的备节点写入路径，保留全局偏移空间，使消费方可以在主→备之间故障切换而无需重映射偏移。备节点契约（`src/lib.rs:305-391`）：

- **单分片。** 复制是把线性流映射到 shard 0，因此 `shards` 必须为 `1`。
- **按序。** `sequence` 必须等于当前 producer 游标；缺口返回错误以便调用方重试。
- **幂等。** 已复制过（低于游标）的 `sequence` 是空操作，因此重复或重放的 Sync RPC 是安全的。
- **带背压。** 拒绝覆盖尚未提交的活动槽，通过和 `claim` 相同的水印门返回 `QueueFull`。

**守护进程级管线——Pusher / `RemoteSink` trait / `run_pusher`。** 这些**不**通过 `LogDb` 暴露。`pusher` 模块是私有的（`src/lib.rs:37` 的 `mod pusher;`——注意：不是 `pub mod`），Pusher 设计为由嵌入它的守护进程（如 `logdbd`）驱动，由该宿主守护进程掌管自己的线程、进度文件与退避策略。**没有一行式的 `db.push(...)` API**。

这是 v1.1 的一个**已知缺口**：库暴露了备节点写入（`replicate`），却没暴露主节点侧的推送驱动。公开的推送 API 需要独立的设计变更记录。守护进程级的集成模式见 [Extending logdb](../dev/extending.md)（`RemoteSink` trait 及宿主守护进程如何把记录送往远端）。

## 相关链接

- [使用指南](README.md)
- [配置](configuration.md)——每个特性解锁的 `Config` 字段。
- [Sharding](sharding.md)——`hash-chain` 为何只能单分片，以及吞吐/延迟取舍。
- [持久化](durability.md)——与四个特性均正交。
- [恢复](recovery.md)——恢复过程中哈希链校验、压缩与解密的行为。

> logdb 0.2.0
