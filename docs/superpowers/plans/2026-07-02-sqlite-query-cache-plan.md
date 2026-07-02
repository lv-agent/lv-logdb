# SQLite 查询缓存实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 logdbd 中内置 SQLite 作为查询缓存，Agent 应用可通过 SQL 直接查询日志数据。

**Architecture:** 新增 `cache/` 模块——Indexer 独立线程从 Segment 读取 committed 记录写入 `<stream>.db`，Query RPC 在 SQLite 上执行 SELECT。Segment 是真相源，SQLite 是缓存，按 stream 隔离，最终一致。

**Tech Stack:** Rust, rusqlite (bundled), tonic gRPC, 现有 logdb ScanIter/replay_from API

**设计文档:** `docs/superpowers/specs/2026-07-02-sqlite-query-cache-design.md`

---

## 文件结构

```
logdbd/
├── Cargo.toml                      # + rusqlite
├── src/
│   ├── lib.rs                      # + pub mod cache
│   ├── config.rs                   # + CacheConfig
│   ├── main.rs                     # 启动时初始化 cache::Indexer，关闭时 drain
│   ├── service.rs                  # + Query RPC 实现
│   └── cache/
│       ├── mod.rs                  # 模块入口
│       ├── config.rs               # CacheConfig
│       ├── indexer.rs              # Indexer 线程 + Schema 管理
│       ├── snapshot.rs             # 快照创建/恢复/清理
│       └── query.rs                # SQL 校验与执行
logdbd-proto/
├── Cargo.toml                      # 不变
└── proto/
    └── logdbd.proto                # + Query RPC
logdbd/tests/
└── integration.rs                  # + cache 集成测试
```

---

### Task 1: 添加依赖和配置

**Files:**
- Modify: `logdbd/Cargo.toml`
- Modify: `logdbd/src/config.rs`
- Modify: `logdbd/src/lib.rs`

- [ ] **Step 1: 添加 rusqlite 依赖**

在 `logdbd/Cargo.toml` 的 `[dependencies]` 中添加：

```toml
rusqlite = { version = "0.31", features = ["bundled"] }
```

- [ ] **Step 2: 运行 cargo check 确认依赖解析**

```bash
cargo check -p logdbd
```

Expected: 依赖下载成功，编译通过（可能有 unused import 警告）。

- [ ] **Step 3: 添加 CacheConfig 到 config.rs**

在 `logdbd/src/config.rs` 末尾的 `// ── Tests ──` 之前添加以下内容：

首先在 `Config` 结构体添加 `cache` 字段。找到 `pub struct Config {`，在最后一个字段后添加：

```rust
    #[serde(default)]
    pub cache: CacheConfig,
```

在文件末尾 `// ── Tests ──` 之前添加：

```rust
// ── Cache Config ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(default = "default_cache_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_cache_flush_interval_secs")]
    pub flush_interval_secs: u64,
    #[serde(default = "default_cache_snapshot_min_interval_secs")]
    pub snapshot_min_interval_secs: u64,
    #[serde(default = "default_cache_snapshot_retain")]
    pub snapshot_retain: usize,
    #[serde(default)]
    pub indexes: Vec<StreamIndexConfig>,
}

fn default_cache_dir() -> PathBuf {
    PathBuf::from("/var/lib/logdbd/cache")
}

fn default_cache_flush_interval_secs() -> u64 {
    30
}

fn default_cache_snapshot_min_interval_secs() -> u64 {
    300
}

fn default_cache_snapshot_retain() -> usize {
    5
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dir: default_cache_dir(),
            flush_interval_secs: 30,
            snapshot_min_interval_secs: 300,
            snapshot_retain: 5,
            indexes: vec![],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamIndexConfig {
    pub stream: String,
    pub fields: Vec<String>,
}
```

在 `Config` 的 `Default` impl 中添加 `cache: CacheConfig::default()`。找到 `impl Default for Config {` 中的结构体构造，在最后一个字段后添加：

```rust
            cache: CacheConfig::default(),
```

- [ ] **Step 4: 添加 `pub mod cache` 到 lib.rs**

在 `logdbd/src/lib.rs` 中添加：

```rust
pub mod cache;
```

- [ ] **Step 5: 运行测试确认回归**

```bash
cargo test -p logdbd
```

Expected: 所有现有测试通过。

- [ ] **Step 6: 提交**

```bash
git add logdbd/Cargo.toml logdbd/src/config.rs logdbd/src/lib.rs
git commit -m "feat: add rusqlite dependency and CacheConfig"
```

---

### Task 2: 创建 cache 模块骨架 + Schema 管理

**Files:**
- Create: `logdbd/src/cache/mod.rs`
- Create: `logdbd/src/cache/config.rs`
- Create: `logdbd/src/cache/indexer.rs`

- [ ] **Step 1: 创建 cache/config.rs**

```rust
//! Cache configuration, re-exported from the parent config module.

pub use crate::config::{CacheConfig, StreamIndexConfig};
```

- [ ] **Step 2: 创建 cache/indexer.rs — 基础结构和 Schema 管理**

```rust
//! Indexer: watches committed cursor, writes records to per-stream SQLite databases.
//!
//! Each stream gets its own `<ns>.<stream>.db` file in cache_dir.
//! The Indexer runs in a background thread, polling the committed cursor
//! and replaying new records into SQLite via Storage::scan().

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::Connection;

use crate::record::DecodedRecord;

/// Per-stream SQLite handle.
struct StreamCache {
    conn: Connection,
    /// Last seq written to this cache (stream seq).
    last_seq: u64,
}

/// Build a safe filename from namespace and stream name.
/// Replaces '/' with '_' to avoid directory traversal.
fn db_filename(ns: &str, stream: &str) -> String {
    let safe_ns = ns.replace(['/', '\\'], "_");
    let safe_stream = stream.replace(['/', '\\'], "_");
    format!("{}.{}.db", safe_ns, safe_stream)
}

/// Build a snapshot filename from namespace, stream, and timestamp.
fn snapshot_filename(ns: &str, stream: &str, timestamp: &str) -> String {
    let safe_ns = ns.replace(['/', '\\'], "_");
    let safe_stream = stream.replace(['/', '\\'], "_");
    format!("{}.{}.snap_{}.db", safe_ns, safe_stream, timestamp)
}

/// Parse the stream key (ns, stream) from a db filename.
fn parse_db_filename(name: &str) -> Option<(String, String)> {
    let name = name.strip_suffix(".db")?;
    // Ignore snapshot files
    if name.contains(".snap_") {
        return None;
    }
    let (ns, stream) = name.split_once('.')?;
    Some((ns.to_string(), stream.to_string()))
}

/// Create the records table and default indexes in a new/opened SQLite database.
pub fn create_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS records (
            seq            INTEGER PRIMARY KEY,
            gid            INTEGER NOT NULL,
            ts_ns          INTEGER NOT NULL,
            event_type     TEXT NOT NULL,
            content_type   TEXT NOT NULL DEFAULT 'application/json',
            metadata_json  TEXT NOT NULL DEFAULT '{}',
            content        BLOB,
            deleted        INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_records_event_type ON records (event_type);
        CREATE INDEX IF NOT EXISTS idx_records_ts ON records (ts_ns);"
    )?;
    Ok(())
}

/// Create extra indexes for configured metadata fields on a stream.
pub fn create_metadata_indexes(
    conn: &Connection,
    fields: &[String],
) -> Result<(), rusqlite::Error> {
    for field in fields {
        // SQLite JSON extraction: metadata_json -> '$.field'
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_records_meta_{field} \
             ON records (json_extract(metadata_json, '$.{field}'))",
            field = field.replace('\'', "''")
        );
        conn.execute(&sql, [])?;
    }
    Ok(())
}

/// Insert a decoded record into the records table.
pub fn insert_record(conn: &Connection, rec: &DecodedRecord) -> Result<(), rusqlite::Error> {
    let meta_json = serde_json::to_string(&rec.metadata).unwrap_or_else(|_| "{}".into());
    conn.execute(
        "INSERT OR IGNORE INTO records (seq, gid, ts_ns, event_type, content_type, metadata_json, content, deleted)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
        rusqlite::params![
            rec.seq,
            rec.gid,  // Wait, DecodedRecord doesn't have gid field!
            rec.timestamp_ns,
            rec.event_type,
            rec.content_type,
            meta_json,
            rec.user_content,
        ],
    )?;
    Ok(())
}
```

Wait — `DecodedRecord` doesn't have a `gid` field. Looking at the record.rs, it has `namespace_id`, `stream_id`, `seq`, `event_type`, `content_type`, `metadata`, `timestamp_ns`, `user_content`. No `gid`.

The gid is the logdb global id, which is used for ordering. The Indexer needs to track both the stream seq and the gid. Let me check how Storage tracks it...

In storage.rs, `append` returns both `gid` and `stream_seq`. The `seq_map` maps `stream_id → (stream_seq → gid)`.

For the Indexer, the simplest approach is to read from segment via the existing `Storage::scan()` which decodes records and returns `DecodedRecord`. But `DecodedRecord` doesn't have gid.

Actually, looking at the design: The Indexer reads from segment files directly via scan or replay. The Indexer needs gid for ordering (to know the global order). But actually, for the SQLite cache, we just need the per-stream seq for the primary key.

Wait, let me reconsider. The Indexer works like a Tailer - it reads everything committed, across all streams. It needs to:
1. Read committed records (using scan from last known position)
2. Decode them to get stream_id, seq, etc.
3. Insert into the correct `<stream>.db`

For tracking position, the Indexer can use the global gid. The scan API returns records with their gid (the Record struct from logdb has `id.sequence` which is the gid).

But `DecodedRecord` (from record.rs) doesn't have gid. So the Indexer should use the raw `Storage::scan()` which returns `Vec<DecodedRecord>` but doesn't give gid...

Actually, let me look at Storage::scan more carefully. It returns decoded records. The gid is lost during decoding. 

So the Indexer should work directly with logdb's API to get (gid, raw_content) pairs, then decode the raw content. Or better, it should use `Storage::scan()` for a range and get decoded records, tracking progress by the count of records per stream.

Simplest approach: 
- The Indexer maintains `last_scanned_gid: u64`
- Each iteration: `storage.scan(last_scanned_gid, u64::MAX)` to get decoded records
- The records come back in gid order (because scan orders by gid)
- After processing, advance `last_scanned_gid` to `max(processed gids) + 1`

But scan returns `Vec<DecodedRecord>` which doesn't have gid. We need to track gid somehow.

Alternative: The Indexer uses `db.scan()` directly (logdb's raw API), which returns `Record` with `id.sequence` (the gid), then decodes the content.

Actually wait - the simplest approach for the MVP: the Indexer doesn't need to track individual gids. It just needs to know which committed records it has already indexed. Since the Indexer chases the committed cursor, it can:

1. Keep `last_processed_gid`
2. Call `storage.db_arc().scan(last_processed_gid, committed_gid)` to get raw logdb Records
3. For each raw Record, decode using `record::decode_record(&rec.content)`
4. Route to the correct `<stream>.db`
5. Advance `last_processed_gid`

But Storage exposes a nice API... Let me just add a method or use the raw approach.

Actually, for simplicity in the plan, let me have the Indexer use `Storage` directly but add a simple gid tracking workaround. The Indexer can use `storage.scan(from_gid, to_gid)` which returns `Vec<DecodedRecord>`. It knows `from_gid` is the start. The records are in gid order. Since each stream's seq is monotonically increasing, we can track per-stream last_seq.

For the gid tracking: we can just track the range we scanned. But wait - scan doesn't return gids, so we can't know the exact last gid. 

Best approach for the plan: the Indexer directly uses logdb's raw scan API to get (gid, raw_bytes) pairs, then decodes.

Let me restructure the plan around this.

Actually, I think the cleanest way is to add a small helper to Storage that returns (gid, DecodedRecord) pairs, or just pass the raw logdb API to the Indexer.

For the plan, let me keep it simple: Indexer holds `Arc<Storage>` and uses `storage.scan()`. It tracks `last_gid` by taking the max gid returned. But scan doesn't return gid...

OK, simplest fix: just use `logdb::LogDb::scan()` directly from the Arc that Storage exposes. The Indexer will call:
```
storage.db_arc().scan(last_gid, committed_gid)
```
This returns `ScanIter` yielding `Result<logdb::Record, ReadError>`. Each logdb Record has `id.sequence` (the gid) and `content` (raw bytes).

Then Indexer decodes with `record::decode_record(&rec.content)` and inserts.

This is clean. The Indexer doesn't need Storage at all - just `Arc<LogDb>` + the Catalog for resolving stream_id → (ns, stream) names.

Let me rewrite the plan accordingly.

For the gid, the logdb Record type has:
```rust
pub struct Record {
    pub id: RecordId,
    pub timestamp_ns: u64,
    pub content: Vec<u8>,
}
pub struct RecordId {
    pub sequence: u64,  // this is the gid
}
```

So the raw scan gives us gid. The Indexer also needs to resolve stream_id to namespace/stream names for the db filename. This requires access to the Catalog.

Let me revise the approach:
- Indexer receives: `Arc<LogDb>`, `Arc<Catalog>`, cache_dir, config
- Indexer polls: scan from `last_gid` to `db.durable_cursor()`
- For each raw record: decode → get stream_id → resolve stream name from Catalog → insert into correct db

OK let me rewrite the plan. I've spent too long on this thinking. Let me just write the complete plan with correct code now.<｜end▁of▁thinking｜>

<｜｜DSML｜｜tool_calls>
<｜｜DSML｜｜invoke name="Write">
<｜｜DSML｜｜parameter name="content" string="true"># SQLite 查询缓存实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 logdbd 中内置 SQLite 作为查询缓存，Agent 应用可通过 SQL 直接查询日志数据。

**Architecture:** 新增 `cache/` 模块——Indexer 独立线程通过 logdb `scan()` 读取 Segment 的 committed 记录，解码后写入按 stream 隔离的 `<ns>.<stream>.db` 文件。Query RPC 解析 namespace+stream，在对应 SQLite 文件上执行 SELECT。Segment 是真相源，SQLite 是缓存，最终一致。

**Tech Stack:** Rust, rusqlite (bundled), tonic gRPC, logdb ScanIter API, serde_json

**设计文档:** `docs/superpowers/specs/2026-07-02-sqlite-query-cache-design.md`

---

## 文件结构

```
logdbd/
├── Cargo.toml                      # + rusqlite
├── src/
│   ├── lib.rs                      # + pub mod cache
│   ├── config.rs                   # + CacheConfig
│   ├── main.rs                     # 启动时初始化 cache::Indexer，关闭时 drain
│   ├── service.rs                  # + Query RPC handler
│   └── cache/
│       ├── mod.rs                  # 模块入口，pub use
│       ├── indexer.rs              # Indexer 线程
│       ├── snapshot.rs             # 快照创建/恢复/清理
│       └── query.rs                # SQL 校验与执行
logdbd-proto/proto/
└── logdbd.proto                    # + Query RPC
logdbd/tests/
└── integration.rs                  # + cache 集成测试
```

---

### Task 1: 添加依赖和 CacheConfig

**Files:**
- Modify: `logdbd/Cargo.toml`
- Modify: `logdbd/src/config.rs`
- Modify: `logdbd/src/lib.rs`

- [ ] **Step 1: 添加依赖**

在 `logdbd/Cargo.toml` 的 `[dependencies]` 末尾添加：

```toml
rusqlite = { version = "0.31", features = ["bundled"] }
```

- [ ] **Step 2: cargo check 确认编译**

```bash
cargo check -p logdbd
```

Expected: 编译通过。

- [ ] **Step 3: 添加 CacheConfig**

在 `logdbd/src/config.rs` 顶部 `Config` 结构体的 `observability` 字段后面添加 `cache` 字段：

```rust
pub struct Config {
    // ... 已有字段 ...
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub cache: CacheConfig,
}
```

在 `ObservabilityConfig` 的 `Default` impl 之后（约第 549 行），添加 CacheConfig 定义：

```rust
// ── Cache Config ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(default = "default_cache_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_cache_flush_interval_secs")]
    pub flush_interval_secs: u64,
    #[serde(default = "default_cache_snapshot_min_interval_secs")]
    pub snapshot_min_interval_secs: u64,
    #[serde(default = "default_cache_snapshot_retain")]
    pub snapshot_retain: usize,
}

fn default_cache_dir() -> PathBuf {
    PathBuf::from("/var/lib/logdbd/cache")
}

fn default_cache_flush_interval_secs() -> u64 {
    30
}

fn default_cache_snapshot_min_interval_secs() -> u64 {
    300
}

fn default_cache_retain() -> usize {
    5
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dir: default_cache_dir(),
            flush_interval_secs: 30,
            snapshot_min_interval_secs: 300,
            snapshot_retain: 5,
        }
    }
}
```

在 `Config::default()` 方法末尾添加：

```rust
            cache: CacheConfig::default(),
```

- [ ] **Step 4: 添加 `pub mod cache`**

在 `logdbd/src/lib.rs` 末尾添加：

```rust
pub mod cache;
```

- [ ] **Step 5: 运行测试确认**

```bash
cargo test -p logdbd
```

Expected: 所有已有测试通过。

- [ ] **Step 6: 提交**

```bash
git add logdbd/Cargo.toml logdbd/src/config.rs logdbd/src/lib.rs
git commit -m "feat(cache): add rusqlite dependency and CacheConfig struct"
```

---

### Task 2: 创建 cache/mod.rs 和 cache/indexer.rs

**Files:**
- Create: `logdbd/src/cache/mod.rs`
- Create: `logdbd/src/cache/indexer.rs`

- [ ] **Step 1: 创建 cache/mod.rs**

```rust
//! SQLite query cache for logdbd.
//!
//! Each stream gets its own SQLite database in `cache_dir`.
//! The Indexer chases the logdb committed cursor, inserts decoded
//! records into the per-stream SQLite files.
//! Query API executes read-only SQL against the appropriate db.

mod indexer;
mod query;
mod snapshot;

pub use indexer::Indexer;
pub use query::execute_query;
pub use snapshot::{create_snapshot, recover_or_create, cleanup_snapshots};
```

- [ ] **Step 2: 创建 cache/indexer.rs**

```rust
//! Indexer — watches logdb committed cursor, writes decoded records
//! into per-stream SQLite databases in cache_dir.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::Connection;

use crate::catalog::Catalog;
use crate::config::CacheConfig;
use crate::record;

/// Build a safe filename from namespace and stream name.
pub fn db_filename(ns: &str, stream: &str) -> String {
    let safe = |s: &str| s.replace(['/', '\\'], "_");
    format!("{}.{}.db", safe(ns), safe(stream))
}

/// Create the records table and default indexes.
fn create_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS records (
            seq            INTEGER PRIMARY KEY,
            gid            INTEGER NOT NULL,
            ts_ns          INTEGER NOT NULL,
            event_type     TEXT NOT NULL,
            content_type   TEXT NOT NULL DEFAULT 'application/json',
            metadata_json  TEXT NOT NULL DEFAULT '{}',
            content        BLOB,
            deleted        INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_records_event_type ON records (event_type);
        CREATE INDEX IF NOT EXISTS idx_records_ts ON records (ts_ns);"
    )?;
    Ok(())
}

/// Insert a decoded record into the records table.
fn insert_record(conn: &Connection, gid: u64, rec: &record::DecodedRecord) -> Result<(), rusqlite::Error> {
    let meta_json = serde_json::to_string(&rec.metadata).unwrap_or_else(|_| "{}".into());
    conn.execute(
        "INSERT OR IGNORE INTO records (seq, gid, ts_ns, event_type, content_type, metadata_json, content, deleted)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
        rusqlite::params![
            rec.seq as i64,
            gid as i64,
            rec.timestamp_ns as i64,
            rec.event_type,
            rec.content_type,
            meta_json,
            rec.user_content,
        ],
    )?;
    Ok(())
}

/// Insert a tombstone — mark the target record as deleted.
fn insert_tombstone(conn: &Connection, gid: u64, target_seq: u64) -> Result<(), rusqlite::Error> {
    // Insert the tombstone itself
    conn.execute(
        "INSERT OR IGNORE INTO records (seq, gid, ts_ns, event_type, content_type, metadata_json, content, deleted)
         VALUES (?1, ?2, 0, 'logdb.tombstone', 'application/json', '{}', X'', 0)",
        rusqlite::params![i64::MAX - target_seq as i64, gid as i64],
    )?;
    // Mark target as deleted
    conn.execute(
        "UPDATE records SET deleted = 1 WHERE seq = ?1",
        rusqlite::params![target_seq as i64],
    )?;
    Ok(())
}

/// Per-stream open SQLite connection cached in memory.
struct StreamDb {
    conn: Connection,
    last_seq: u64,
}

/// The Indexer runs in a background thread.
pub struct Indexer {
    db: Arc<logdb::LogDb>,
    catalog: Arc<Catalog>,
    cache_dir: PathBuf,
    /// Latest gid that has been processed.
    last_gid: AtomicU64,
    /// Per-stream open connections: stream_id → StreamDb.
    /// The Indexer thread is the sole writer; reads happen via query.rs.
    /// We use parking_lot or std::sync::Mutex for simplicity.
    streams: Mutex<HashMap<u64, StreamDb>>,
    running: AtomicBool,
    flush_interval: Duration,
}

impl Indexer {
    /// Create a new Indexer. Does not start the background thread yet.
    pub fn new(
        db: Arc<logdb::LogDb>,
        catalog: Arc<Catalog>,
        cache_dir: PathBuf,
        config: &CacheConfig,
    ) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();
        Self {
            db,
            catalog,
            cache_dir,
            last_gid: AtomicU64::new(0),
            streams: Mutex::new(HashMap::new()),
            running: AtomicBool::new(false),
            flush_interval: Duration::from_secs(config.flush_interval_secs),
        }
    }

    /// Start the Indexer background thread.
    pub fn start(self: Arc<Self>) {
        self.running.store(true, Ordering::Release);
        let this = Arc::clone(&self);
        std::thread::Builder::new()
            .name("logdbd-cache-indexer".into())
            .spawn(move || { this.run(); })
            .expect("spawn cache indexer thread");
    }

    /// Main loop: poll committed cursor, replay new records, flush periodically.
    fn run(&self) {
        let mut last_flush = Instant::now();

        while self.running.load(Ordering::Acquire) {
            let committed = self.db.durable_cursor();  // use durable as our visibility bound
            let current = self.last_gid.load(Ordering::Acquire);

            if current < committed {
                match self.db.scan(current, committed) {
                    Ok(iter) => {
                        let mut max_gid = current;
                        for result in iter {
                            let rec = match result {
                                Ok(r) => r,
                                Err(e) => {
                                    tracing::warn!(error = %e, "cache indexer scan error");
                                    break;
                                }
                            };
                            let gid = rec.id.sequence;
                            max_gid = max_gid.max(gid + 1);
                            self.index_record(gid, &rec.content);
                        }
                        self.last_gid.store(max_gid, Ordering::Release);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "cache indexer scan failed");
                    }
                }
            }

            // Periodic WAL checkpoint
            if last_flush.elapsed() >= self.flush_interval {
                self.flush_all();
                last_flush = Instant::now();
            }

            // Sleep a bit to avoid busy-waiting
            std::thread::sleep(Duration::from_millis(10));
        }

        // Final flush before exit
        self.flush_all();
    }

    /// Decode and route one record to the correct stream's SQLite db.
    fn index_record(&self, gid: u64, raw: &[u8]) {
        let decoded = match record::decode_record(raw) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(gid = gid, error = %e, "cache indexer decode failed");
                return;
            }
        };

        let stream_id = decoded.stream_id;

        // Resolve stream name from catalog (for the db filename)
        let stream_name = match self.catalog.stream_info_by_id(stream_id) {
            Some((_ns_id, name)) => name,
            None => {
                tracing::warn!(stream_id = stream_id, "cache indexer: unknown stream_id");
                return;
            }
        };
        let ns_name = self.catalog.namespace_name(decoded.namespace_id).unwrap_or_default();
        let db_name = db_filename(&ns_name, &stream_name);

        let mut streams = self.streams.lock().unwrap_or_else(|e| e.into_inner());

        let entry = streams.entry(stream_id).or_insert_with(|| {
            let db_path = self.cache_dir.join(&db_name);
            let conn = Connection::open(&db_path).expect("open stream cache db");
            create_schema(&conn).expect("create cache schema");
            StreamDb { conn, last_seq: 0 }
        });

        // Handle tombstone
        if decoded.event_type == "logdb.tombstone" {
            if let Some(target) = decoded.metadata.get("target_seq") {
                if let Ok(target_seq) = target.parse::<u64>() {
                    if let Err(e) = insert_tombstone(&entry.conn, gid, target_seq) {
                        tracing::warn!(error = %e, "cache indexer tombstone insert failed");
                    }
                }
            }
            return;
        }

        if let Err(e) = insert_record(&entry.conn, gid, &decoded) {
            tracing::warn!(gid = gid, stream_id = stream_id, error = %e, "cache indexer insert failed");
        }
        entry.last_seq = entry.last_seq.max(decoded.seq);
    }

    /// WAL checkpoint + fsync all open connections.
    fn flush_all(&self) {
        let streams = self.streams.lock().unwrap_or_else(|e| e.into_inner());
        for (_, s) in streams.iter() {
            if let Err(e) = s.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                tracing::warn!(error = %e, "cache indexer WAL checkpoint failed");
            }
        }
    }

    /// Shut down the Indexer thread.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Get the current progress (last processed gid).
    pub fn last_gid(&self) -> u64 {
        self.last_gid.load(Ordering::Acquire)
    }
}
```

- [ ] **Step 3: cargo check 确认编译**

```bash
cargo check -p logdbd 2>&1
```

Expected: 无编译错误（仅 unused import 警告）。

- [ ] **Step 4: 提交**

```bash
git add logdbd/src/cache/mod.rs logdbd/src/cache/indexer.rs
git commit -m "feat(cache): add Indexer — background thread that populates per-stream SQLite from segment"
```

---

### Task 3: 创建 cache/query.rs

**Files:**
- Create: `logdbd/src/cache/query.rs`

- [ ] **Step 1: 创建 cache/query.rs**

```rust
//! Query execution — validates and runs read-only SQL against a stream's SQLite db.

use std::path::Path;

use rusqlite::Connection;

/// Error returned by query validation or execution.
#[derive(Debug)]
pub enum QueryError {
    NotSelect,
    Sqlite(rusqlite::Error),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSelect => write!(f, "only SELECT statements are allowed"),
            Self::Sqlite(e) => write!(f, "sqlite error: {}", e),
        }
    }
}

impl std::error::Error for QueryError {}

/// Validate that `sql` is a read-only SELECT statement.
/// Simple check: trims whitespace, rejects anything not starting with "SELECT".
fn validate_sql(sql: &str) -> Result<(), QueryError> {
    let trimmed = sql.trim();
    if trimmed.len() < 6 {
        return Err(QueryError::NotSelect);
    }
    let prefix = &trimmed[..6].to_uppercase();
    if prefix != "SELECT" {
        return Err(QueryError::NotSelect);
    }
    Ok(())
}

/// Execute a validated SELECT statement against a db file.
/// Returns rows as JSON strings.
pub fn execute_query(db_path: &Path, sql: &str) -> Result<Vec<String>, QueryError> {
    validate_sql(sql)?;

    let conn = Connection::open(db_path).map_err(QueryError::Sqlite)?;

    let mut stmt = conn.prepare(sql).map_err(QueryError::Sqlite)?;

    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
        .collect();

    let rows = stmt
        .query_map([], |row| {
            let mut obj = serde_json::Map::new();
            for (i, name) in col_names.iter().enumerate() {
                let val: serde_json::Value = match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Null) => serde_json::Value::Null,
                    Ok(rusqlite::types::ValueRef::Integer(n)) => serde_json::json!(n),
                    Ok(rusqlite::types::ValueRef::Real(f)) => serde_json::json!(f),
                    Ok(rusqlite::types::ValueRef::Text(s)) => {
                        serde_json::Value::String(String::from_utf8_lossy(s).into_owned())
                    }
                    Ok(rusqlite::types::ValueRef::Blob(b)) => {
                        // Return blob as base64 string for JSON compatibility
                        use std::fmt::Write;
                        let mut encoded = String::with_capacity(b.len() * 2);
                        for byte in b {
                            write!(&mut encoded, "{:02x}", byte).unwrap();
                        }
                        serde_json::json!(encoded)
                    }
                    Err(_) => serde_json::Value::Null,
                };
                obj.insert(name.clone(), val);
            }
            Ok(serde_json::Value::Object(obj).to_string())
        })
        .map_err(QueryError::Sqlite)?;

    let mut results = Vec::new();
    for row in rows {
        results.push(row.map_err(QueryError::Sqlite)?);
    }
    Ok(results)
}
```

- [ ] **Step 2: cargo check 确认**

```bash
cargo check -p logdbd 2>&1
```

Expected: 编译通过（serde_json 已在依赖中）。

- [ ] **Step 3: 提交**

```bash
git add logdbd/src/cache/query.rs
git commit -m "feat(cache): add query execution — validates SELECT and runs against stream SQLite db"
```

---

### Task 4: 创建 cache/snapshot.rs

**Files:**
- Create: `logdbd/src/cache/snapshot.rs`

- [ ] **Step 1: 创建 cache/snapshot.rs**

```rust
//! Snapshot management for per-stream SQLite cache files.
//!
//! Snapshot lifecycle:
//!   recover_or_create → active .db file
//!   create_snapshot   → copy active .db → snap_{ts}.db
//!   cleanup_snapshots → delete expired snap_{ts}.db files

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Timestamp string for snapshot filenames.
fn timestamp() -> String {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", dur.as_secs())
}

/// Build a snapshot filename from namespace and stream name.
pub fn snapshot_path(
    cache_dir: &Path,
    ns: &str,
    stream: &str,
    ts: &str,
) -> PathBuf {
    let safe = |s: &str| s.replace(['/', '\\'], "_");
    let fname = format!("{}.{}.snap_{}.db", safe(ns), safe(stream), ts);
    cache_dir.join(fname)
}

/// Active db path for a stream.
pub fn active_path(cache_dir: &Path, ns: &str, stream: &str) -> PathBuf {
    let safe = |s: &str| s.replace(['/', '\\'], "_");
    cache_dir.join(format!("{}.{}.db", safe(ns), safe(stream)))
}

/// List all snapshot files matching a given (ns, stream) prefix.
pub fn list_snapshots(
    cache_dir: &Path,
    ns: &str,
    stream: &str,
) -> Vec<(PathBuf, u64)> {
    let safe = |s: &str| s.replace(['/', '\\'], "_");
    let prefix = format!("{}.{}.snap_", safe(ns), safe(stream));

    let mut results = Vec::new();
    if let Ok(entries) = fs::read_dir(cache_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(&prefix) && name_str.ends_with(".db") {
                // Extract timestamp from filename
                let ts_part = name_str
                    .strip_prefix(&prefix)
                    .and_then(|s| s.strip_suffix(".db"))
                    .unwrap_or("0");
                let ts: u64 = ts_part.parse().unwrap_or(0);
                results.push((entry.path(), ts));
            }
        }
    }
    results.sort_by_key(|(_, ts)| std::cmp::Reverse(*ts)); // newest first
    results
}

/// Recover the newest snapshot for (ns, stream), or return the active db path.
/// If no active db and no snapshot exist, create a fresh database.
pub fn recover_or_create(
    cache_dir: &Path,
    ns: &str,
    stream: &str,
) -> PathBuf {
    let active = active_path(cache_dir, ns, stream);
    if active.exists() {
        return active;
    }

    // Look for newest snapshot
    let snapshots = list_snapshots(cache_dir, ns, stream);
    if let Some((snap_path, _ts)) = snapshots.first() {
        tracing::info!(
            ns = ns,
            stream = stream,
            snapshot = %snap_path.display(),
            "recovering cache from snapshot"
        );
        if let Err(e) = fs::copy(snap_path, &active) {
            tracing::warn!(
                error = %e,
                "failed to copy snapshot, creating fresh cache"
            );
        } else {
            return active;
        }
    }

    // Fresh database — create with schema
    tracing::info!(ns = ns, stream = stream, "creating fresh cache db");
    // The Indexer will create the file with proper schema on first write,
    // but we can create it here if we want to ensure it exists.
    active
}

/// Create a snapshot of the active db for (ns, stream).
/// Returns the snapshot path if successful.
pub fn create_snapshot(
    cache_dir: &Path,
    ns: &str,
    stream: &str,
) -> Option<PathBuf> {
    let active = active_path(cache_dir, ns, stream);
    if !active.exists() {
        return None;
    }

    let ts = timestamp();
    let snap = snapshot_path(cache_dir, ns, stream, &ts);

    match fs::copy(&active, &snap) {
        Ok(_) => {
            tracing::info!(
                ns = ns, stream = stream,
                snapshot = %snap.display(),
                "created cache snapshot"
            );
            Some(snap)
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to create cache snapshot");
            None
        }
    }
}

/// Delete old snapshots, retaining at most `retain` newest.
pub fn cleanup_snapshots(cache_dir: &Path, retain: usize) {
    // Group snapshots by (ns, stream) prefix
    let mut groups: std::collections::HashMap<String, Vec<(PathBuf, u64)>> =
        std::collections::HashMap::new();

    if let Ok(entries) = fs::read_dir(cache_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(rest) = name_str.strip_suffix(".db") {
                if let Some((prefix, _ts)) = rest.rsplit_once(".snap_") {
                    groups
                        .entry(prefix.to_string())
                        .or_default()
                        .push((entry.path(), 0));
                }
            }
        }
    }

    for (_prefix, mut snaps) in groups {
        snaps.sort_by_key(|(_, ts)| std::cmp::Reverse(*ts));
        for (path, _) in snaps.iter().skip(retain) {
            if let Err(e) = fs::remove_file(path) {
                tracing::warn!(path = %path.display(), error = %e, "failed to clean up old snapshot");
            }
        }
    }
}
```

- [ ] **Step 2: cargo check 确认**

```bash
cargo check -p logdbd 2>&1
```

Expected: 编译通过。

- [ ] **Step 3: 提交**

```bash
git add logdbd/src/cache/snapshot.rs
git commit -m "feat(cache): add snapshot management — recover, create, cleanup"
```

---

### Task 5: 添加 Query gRPC 接口

**Files:**
- Modify: `logdbd-proto/proto/logdbd.proto`
- Modify: `logdbd/src/service.rs`

- [ ] **Step 1: 在 proto 文件中添加 Query RPC**

在 `logdbd-proto/proto/logdbd.proto` 的 `service LogDbService` 末尾（`ListStreams` 之后）添加：

```protobuf
  // Query — SQL SELECT against the stream's query cache
  rpc Query(QueryRequest) returns (QueryResponse);
```

在文件末尾添加消息定义：

```protobuf
// ─── Query (SQL cache) ────────────────────────────────────────────────────

message QueryRequest {
  string namespace = 1;
  string stream    = 2;
  string sql       = 3;  // SELECT statement only
}

message QueryResponse {
  repeated string rows = 1;  // each row as JSON string
}
```

- [ ] **Step 2: 重新生成 protobuf 代码**

```bash
cd logdbd-proto && cargo build
```

Expected: 生成新的 Rust 类型 `QueryRequest` 和 `QueryResponse`。

- [ ] **Step 3: 在 service.rs 中实现 Query RPC handler**

在 `logdbd/src/service.rs` 的 `impl LogDbService for LogDbServiceImpl` 块末尾（最后一个 RPC 方法之后，闭合 `}` 之前）添加：

```rust
    async fn query(
        &self,
        req: Request<pb::QueryRequest>,
    ) -> Result<Response<pb::QueryResponse>, Status> {
        let r = req.get_ref();

        // Resolve namespace + stream → db path
        let (_ns_id, _stream_id) = self.resolve(&r.namespace, &r.stream)?;

        let cache_dir = std::path::PathBuf::from("/var/lib/logdbd/cache"); // TODO: from config
        let db_path = crate::cache::indexer::db_filename(&r.namespace, &r.stream);
        let db_path = cache_dir.join(db_path);

        if !db_path.exists() {
            return Ok(Response::new(pb::QueryResponse { rows: vec![] }));
        }

        match crate::cache::execute_query(&db_path, &r.sql) {
            Ok(rows) => Ok(Response::new(pb::QueryResponse { rows })),
            Err(e) => Err(Status::invalid_argument(e.to_string())),
        }
    }
```

- [ ] **Step 4: 将 cache_dir 传入 service**

`LogDbServiceImpl` 需要知道 `cache_dir`。修改 `LogDbServiceImpl` 结构体，添加 `cache_dir: PathBuf`：

```rust
pub struct LogDbServiceImpl {
    storage: Arc<Storage>,
    catalog: Arc<Catalog>,
    consumer_tracker: Arc<ConsumerTracker>,
    hostname: String,
    role: String,
    cache_dir: PathBuf,  // NEW
}
```

修改 `LogDbServiceImpl::new()`，添加 `cache_dir: PathBuf` 参数。

修改 `query` handler 直接使用 `self.cache_dir`。

- [ ] **Step 5: 更新 main.rs 中 LogDbServiceImpl 的构造**

在 `logdbd/src/main.rs` 中，将 `cache_dir` 传入 `LogDbServiceImpl::new()`：

```rust
    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage),
        Arc::clone(&catalog),
        Arc::new(ConsumerTracker::new()),
        hostname,
        role_str,
        config.cache.dir.clone(),
    );
```

- [ ] **Step 6: cargo check + test**

```bash
cargo check -p logdbd -p logdbd-proto
cargo test -p logdbd
```

Expected: 编译通过，已有测试通过。

- [ ] **Step 7: 提交**

```bash
git add logdbd-proto/proto/logdbd.proto logdbd/src/service.rs logdbd/src/main.rs
git commit -m "feat(cache): add Query gRPC RPC — SQL SELECT against stream cache"
```

---

### Task 6: 在 main.rs 中集成 Indexer 启动和关闭

**Files:**
- Modify: `logdbd/src/main.rs`

- [ ] **Step 1: 在 main.rs 中启动 Indexer**

在 `main.rs` 中，创建 Storage 之后（约第 148 行），添加 Indexer 初始化：

```rust
    // Cache — per-stream SQLite query cache (Indexer background thread)
    let cache_indexer = {
        let indexer = Arc::new(logdbd::cache::Indexer::new(
            storage.db_arc(),
            Arc::clone(&catalog),
            config.cache.dir.clone(),
            &config.cache,
        ));
        indexer.start();
        indexer
    };
```

- [ ] **Step 2: 在 shutdown 路径中 drain Indexer**

在 `server.serve_with_shutdown` 的 shutdown 闭包中，`db_for_drain.drain()` 之前添加 Indexer 停止逻辑：

```rust
    let db_for_drain = storage.db_arc();
    let idx_for_drain = Arc::clone(&cache_indexer);
    server
        .serve_with_shutdown(listen, async move {
            shutdown_signal().await;
            tracing::info!("shutdown signal received; stopping cache indexer");
            idx_for_drain.stop();
            // Give Indexer a moment to flush
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            tracing::info!("draining logdb (flush in-flight to durable, up to 30s)");
            // ... existing drain code ...
        })
        .await?;
```

- [ ] **Step 3: cargo check 确认**

```bash
cargo check -p logdbd
```

Expected: 编译通过。

- [ ] **Step 4: 提交**

```bash
git add logdbd/src/main.rs
git commit -m "feat(cache): integrate Indexer startup and graceful shutdown into main"
```

---

### Task 7: 集成测试

**Files:**
- Modify: `logdbd/tests/integration.rs`

- [ ] **Step 1: 添加缓存集成测试**

查看现有集成测试的结构，添加一个测试用例：

```rust
#[tokio::test]
async fn cache_query_after_append() {
    // Start logdbd with cache enabled
    // Append a record
    // Wait for Indexer to catch up
    // Query via SQL
    // Assert result
}
```

具体实现需要查看现有测试结构。先用 `cargo test -p logdbd --test integration` 了解测试模式。

```bash
# 先看现有测试结构
head -100 logdbd/tests/integration.rs
```

- [ ] **Step 2: 编写测试并运行**

根据现有测试模式编写 cache 集成测试。

```bash
cargo test -p logdbd --test integration
```

Expected: 新测试通过。

- [ ] **Step 3: 提交**

```bash
git add logdbd/tests/integration.rs
git commit -m "test(cache): add integration test for query-after-append"
```

---

### Task 8: 端到端验证

- [ ] **Step 1: 编译 release 版本**

```bash
cargo build --release -p logdbd
```

Expected: 编译成功。

- [ ] **Step 2: 启动 logdbd 并测试 Query RPC**

用 grpcurl 或编写一个快速客户端脚本测试：

```bash
# Terminal 1: 启动 logdbd
cargo run --release -p logdbd -- --config logdbd/logdbd.yaml

# Terminal 2: 先 append 一条记录，再查询
grpcurl -plaintext -d '{"namespace":"test","stream":"s1","event_type":"user.input","content":"aGVsbG8="}' \
  127.0.0.1:50051 logdbd.LogDbService/Append

grpcurl -plaintext -d '{"namespace":"test","stream":"s1","sql":"SELECT * FROM records"}' \
  127.0.0.1:50051 logdbd.LogDbService/Query
```

Expected: Query 返回刚 append 的记录。

- [ ] **Step 3: 提交最终确认**

```bash
git add -A
git status
git commit -m "chore(cache): finalize SQLite query cache implementation"
```

---

## 实现顺序

```
Task 1 (依赖+配置) → Task 2 (Indexer) → Task 3 (Query) → Task 4 (Snapshot)
    ↓                                              ↓
Task 5 (gRPC 集成) ←────────────────────────────────┘
    ↓
Task 6 (main 集成) → Task 7 (测试) → Task 8 (验证)
```
