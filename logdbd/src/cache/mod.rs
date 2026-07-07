//! SQLite query cache for logdbd.
//!
//! Each stream gets its own SQLite database in `cache_dir`.
//! The Indexer chases the logdb committed cursor, inserts decoded
//! records into the per-stream SQLite files.
//! Query API executes read-only SQL against the appropriate db.

pub mod indexer;
pub mod query;
pub mod snapshot;

pub use indexer::Indexer;
// TODO(cr-027 phase 5): `replay_records`/`ReplayRecord` have zero callers after
// phase 4 rewired Subscribe onto the segment; delete alongside the Indexer.
pub use query::{execute_query, replay_records};
pub use snapshot::{cleanup_snapshots, create_snapshot, recover_or_create};
