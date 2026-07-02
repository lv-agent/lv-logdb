//! # logdb-client — Rust SDK for logdbd
//!
//! Ergonomic async client for the logdbd append-only audit log database.
//!
//! ## Quick start
//!
//! ```no_run
//! use logdb_client::Client;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::connect("127.0.0.1:50051").await?;
//!
//! // Append
//! let seq = client.append("my-app", "main", "test.event", b"hello").await?;
//!
//! // Read
//! let rec = client.read("my-app", "main", 1).await?;
//! assert!(rec.is_some());
//!
//! // Scan
//! let records = client.scan_all("my-app", "main", 0).await?;
//! for r in &records {
//!     println!("seq={} event_type={}", r.seq, r.event_type);
//! }
//!
//! // Tail with consumer group
//! use logdb_client::TailOptions;
//! let mut stream = client.tail("my-app", "main")
//!     .consumer_group("workers", "worker-1")
//!     .start()
//!     .await?;
//! while let Some(rec) = stream.next().await? {
//!     process(&rec);
//!     stream.commit().await?;
//! }
//! # Ok(()) }
//! # fn process(_: &logdbd::pb::Record) {}
//! ```

mod client;
mod record;

pub use client::{Client, ClientBuilder, TailOptions};
pub use record::RecordExt;

use logdbd::pb;

/// A decoded record from logdbd.
pub type Record = pb::Record;
/// Append response (seq, gid, etc.)
pub type AppendResult = pb::AppendResponse;
