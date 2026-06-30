# Tailer

具名的、拥有独立且持久化读取进度的消费者——构建复制、下游投递、索引管线以及任何“跟随日志”工作负载时的基础抽象。

## 目录

- [Tailer 是什么](#tailer-是什么)
- [创建 tailer](#创建-tailer)
- [Tailer API](#tailer-api)
- [进度仅在 `commit()` 时持久化](#进度仅在-commit-时持久化)
- [彼此独立的 tailer](#彼此独立的-tailer)
- [一个消费者循环示例](#一个消费者循环示例)
- [seek 与重放](#seek-与重放)
- [相关链接](#相关链接)

## Tailer 是什么

**Tailer** 是日志的具名消费者，维护自己独立的读取位置，与其它所有 tailer 以及临时的 [`read`](reading.md)/[`scan`](reading.md#范围扫描) 调用互不干扰。每个 tailer 由字符串名字标识，并由磁盘上的进度文件（`tailer_<name>.dat`）支撑，因此进程重启后能从上次断点精确续读。

需要 tailer 的典型场景：

- **复制**日志到下游系统（另一个 logdb、关系数据库、搜索引擎、消息中间件）。
- **构建派生视图**（投影、物化聚合），按序应用每条记录。
- 将记录**流式**推送给按自己节奏消费的读取者，独立于写入者与其它消费者。

如果只是做一次性历史扫描、不需要持久化进度，直接用 [`replay_from`](reading.md#从某个序列重放) 即可。需要一个能抗崩溃的*游标*时，才用 tailer。

## 创建 tailer

`LogDb::new_tailer` 打开（或创建）一个具名 tailer，并恢复其保存的位置：

```rust
impl LogDb {
    /// Create a named tailer (consumer) with independent read progress.
    /// Progress is persisted to `tailer_<name>.dat` via `commit()`.
    pub fn new_tailer(&self, name: &str) -> Tailer;
}
```

`new_tailer` 直接返回 `Tailer`——**不**是 `Result`。它不会失败：若 `tailer_<name>.dat` 存在且格式正确，则恢复保存的序列号；否则 tailer 从序列 `0` 起步。

```rust
let mut t = db.new_tailer("replicator");
assert_eq!(t.position(), 0); // 全新的 tailer
```

用同一名字重新打开会从上次 [`commit()`](#进度仅在-commit-时持久化) 的位置续读：

```rust
// 第一个进程：读 100 条记录，持久化进度。
let mut t = db.new_tailer("replicator");
let _ = t.next_batch(100).unwrap();
t.commit().unwrap();
assert_eq!(t.position(), 100);

// 后续进程 / 重启：同名从 100 续读。
let mut t2 = db.new_tailer("replicator");
assert_eq!(t2.position(), 100);
```

> 在 `shards > 1` 时，tailer 维护**每分片进度**（每个分片一个本地序列，类似 Kafka 的每分区 offset），`next_batch` 把所有分片新持久化的记录按全局 id 升序合并成一个批次。停滞的分片不会阻塞其它分片；跨批次顺序是尽力而为（停滞分片中较小的全局 id 记录可能出现在更晚的批次里）。`name` 必须是文件系统安全的标识符——它会直接被插入进度文件名 `tailer_<name>.dat`。进度文件在 `shards == 1` 时沿用 12 字节旧格式，在 `shards > 1` 时存储每分片向量（`count + seqs + crc32c`）。`position()` 返回各分片进度的最小值（粗略进度指标）；`positions()` 返回完整的每分片向量。

## Tailer API

完整接口（`src/tailer.rs`）：

| 方法 | 签名 | 说明 |
|------|------|------|
| `position` | `(&self) -> u64` | 当前读取位置——下一个待读序列号。 |
| `next_batch` | `(&mut self, max_count: usize) -> Result<Option<Vec<Record>>, String>` | 读取至多 `max_count` 条**已持久化**记录；无可用记录时返回 `Ok(None)`。 |
| `commit` | `(&self) -> std::io::Result<()>` | 将当前位置持久化到 `tailer_<name>.dat`。 |
| `seek` | `(&mut self, seq: u64)` | 把位置移到 `seq`（仅在内存中；`commit` 之前不会落盘）。 |
| `reset` | `(&mut self) -> std::io::Result<()>` | 把位置置为 `0` 并删除进度文件。 |

### `next_batch` —— 仅读已持久化记录

`next_batch` 遵循与 [`read`](reading.md#可见性与-durable-游标) 相同的持久性规则：只读到 `durable_cursor()` 为止。已写入但尚未 flush 到磁盘的记录对它不可见——不存在“现在能读到、崩溃后却丢失”的窗口。

- 返回 `Ok(Some(records))`，含从 `position()` 起至多 `max_count` 条记录。位置会越过最后返回的记录。
- 当当前位置及之后没有新的已持久化记录（已追上尾部），或该区间没有记录时，返回 `Ok(None)`。
- 发生 I/O 或解码失败时返回 `Err(String)`。

`next_batch` 是消费者循环的主力：

```rust
match t.next_batch(500)? {
    Some(batch) => {
        deliver(&batch);
        t.commit()?;
    }
    None => std::thread::sleep(Duration::from_millis(10)),
}
```

### `commit` —— 持久化进度

`commit()` 原子地把 `position()` 写入 `tailer_<name>.dat`（写临时文件 → `fdatasync` → 重命名 → 目录 sync）。它是**唯一**会推进磁盘游标的操作——见[下文](#进度仅在-commit-时持久化)。

### `seek` —— 移动游标

`seek(seq)` 把内存中的位置跳到任意序列号——适合**从已知点重放**（下游重建后重读历史，或向前跳过）。它不做 I/O，也不会落盘；若希望新位置在重启后仍然保留，请随后调用 `commit()`。

### `reset` —— 回到起点

`reset()` 把 `position` 置为 `0` 并删除进度文件。用于彻底回退一个消费者（例如修复下游 bug 后想重处理整条日志）。

## 进度仅在 `commit()` 时持久化

这是 tailer 最重要的规则，也是 at-least-once 投递的基础：

> 磁盘上的进度文件**仅**在你调用 `commit()` 时更新。`next_batch` 的读取只会推进*内存中*的位置；不 `commit`，这次推进在下次打开时就会丢失。

具体而言，进度文件（`tailer_<name>.dat`，12 字节：8 字节小端序序列号 + 4 字节 CRC32C，`src/tailer.rs:16-51`）由 `save_progress` 写入，而 `save_progress` 仅被 `Tailer::commit` 调用（`src/tailer.rs:96-140`）。`next_batch`、`seek`、甚至 `reset` 都只改动内存状态（`reset` 还会删除文件，属例外）。

**崩溃语义**——这带来的保证：

- **若在 `next_batch` 之后、`commit` 之前崩溃：** 已读取的记录*未*被标记为已消费。重新打开时，`new_tailer` 恢复上次已提交的位置，因此你会**重读**这些记录。这就是 at-least-once 投递——请把 `deliver()` 设计成幂等，或下游按 record id 去重。
- **若在 `commit` 之后崩溃：** 位置已持久化（临时文件 + `fdatasync` + 重命名 + 目录 sync）。重新打开时严格从上次提交的批次之后续读——不重读、不跳号。
- **进度写撕裂会被检出：** 文件末尾的 CRC32C 覆盖那 8 字节序列号。若 CRC 校验失败（重命名只写了一半、位腐烂），`load_progress` 回退到 `0`（`src/tailer.rs:24-34`）——即 tailer 宁可从头重放，也不信任损坏的游标。这是保守且安全的。

因此推荐的模式是：在你所保护的副作用同一关键段内，**先处理 → 后 commit**：

```rust
while let Some(batch) = t.next_batch(500)? {
    deliver_to_downstream(&batch)?; // 先把副作用做持久
    t.commit()?;                     // 再推进游标
}
```

顺序很关键：先 deliver 后 commit。若先 commit 后 deliver 再崩溃，那些记录就永久丢失了。参见[消费者循环示例](#一个消费者循环示例)。

## 彼此独立的 tailer

tailer 完全独立：每个名字有自己的进度文件和内存游标，彼此之间、与写入者之间互不干扰。两个 tailer 可以以非常不同的节奏读同一条日志——快的可以套圈的慢的，但互不影响。

```rust
let mut fast = db.new_tailer("fast-forwarder");
let mut slow = db.new_tailer("slow-indexer");

// 各自独立推进。
let _ = fast.next_batch(10_000).unwrap();
let _ = slow.next_batch(50).unwrap();
assert_eq!(fast.position(), 10_000);
assert_eq!(slow.position(), 50);

// fast 跑到前面并不会移动 slow 的游标。
let _ = fast.next_batch(10_000).unwrap();
assert_eq!(slow.position(), 50);
```

这正是扇出投递（fan-out）模型：每个下游系统（复制器、搜索索引、指标聚合器）一个 tailer，各自享有独立的持久化保证与背压。

## 一个消费者循环示例

一个正确且抗崩溃的消费者，向下游系统（副本、sink、队列）投递，且每投递一批恰好持久化一次：

```rust
use std::time::Duration;

fn run_replicator(db: &LogDb) -> Result<(), Box<dyn std::error::Error>> {
    // new_tailer 返回 Tailer，不是 Result——这里没有 `?`。
    let mut t = db.new_tailer("replicator");

    loop {
        match t.next_batch(500)? {
            Some(batch) => {
                // 1. 先投递副作用。
                send_to_replica(&batch)?;

                // 2. 然后才持久化进度——这样这行之前的崩溃
                //    会重放本批次，而不是丢掉它。
                t.commit()?;
            }
            None => {
                // 已追上尾部；短暂退避后再轮询。
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
}
```

此示例强制的两个正确性不变量：

1. **先 deliver 后 commit**——副作用（`send_to_replica`）在游标推进之前先持久化。`next_batch` 与 `commit` 之间的崩溃会重放该批次；`commit` 之后的崩溃已经越过它。不存在某条记录“既未投递、又不再待处理”的窗口。
2. **幂等投递**——因为第 1 点“崩溃即重放”的保证，跨重启时 `send_to_replica` 可能两次看到同一批次。请把它做成幂等（按 record id upsert，或用幂等键去重）。

想要更高吞吐，就把批开大些（`next_batch(10_000)`）；想要更低延迟，就更频繁地轮询或缩短睡眠。`None` 分支就是背压信号——没有新数据可读。

## seek 与重放

`seek(seq)` 是带外管理游标的逃生口：

- **从 checkpoint 重放：** 若你有“下游已消费到序列 N”这一外部事实，`t.seek(N); t.commit()?;` 即可把 tailer 直接对齐，无需读取中间记录。
- **重读某个区间：** `t.seek(N)` 后 `next_batch`，重新处理你已经提交越过但想重读的记录（例如修复下游 bug 后）。
- **向前跳：** `t.seek(future_seq)` 越过你刻意想丢弃的记录。

因为 `seek` 仅在内存中生效，在 `commit` 之前磁盘游标不受影响。若想回退*且*忘记所有已提交进度，请改用 `reset()`——它同时把位置清零并删除进度文件。

## 相关链接

- [使用指南](README.md)
- [读取](reading.md)——点读、范围扫描，以及 tailer 所继承的、由持久性界定的可见性规则。
- [持久性](durability.md)——记录何时对 `next_batch` 可见。
- [恢复](recovery.md)——崩溃之后日志（以及你的 tailer 进度文件）会怎样。
- [配置](configuration.md)——`flush_timeout`、`retention` 等影响 tailer 推进速度的旋钮。

> logdb 0.2.0
