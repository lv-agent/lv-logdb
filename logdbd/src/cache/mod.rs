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
pub use query::execute_query;
pub use snapshot::{cleanup_snapshots, create_snapshot, recover_or_create};
