//! # logdb-client — Rust SDK for logdbd
//!
//! Ergonomic async client for the logdbd append-only audit log database.
//!
//! ## Quick start
//!
//! ```ignore
//! // Requires a running logdbd server
//! use logdb_client::Client;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut client = Client::connect("127.0.0.1:50051").await?;
//! let seq = client.append("my-app", "main", "test.event", b"hello").await?;
//! let rec = client.read("my-app", "main", 1).await?;
//! # Ok(()) }
//! ```

mod client;
mod record;

pub use client::{Client, ClientBuilder, TailOptions};
pub use record::RecordExt;

use logdbd_proto::pb;

/// A decoded record from logdbd.
pub type Record = pb::Record;
/// Append response (seq, gid, etc.)
pub type AppendResult = pb::AppendResponse;
/// Structured query request (predicates + result shape). See `Client::query`.
pub type QueryRequest = pb::QueryRequest;
/// Structured query response (typed `result` oneof). See `Client::query`.
pub type QueryResponse = pb::QueryResponse;
/// Result-shape selector for `QueryRequest` (RECORDS, COUNT, EXISTS, ...).
pub type QueryResult = pb::QueryResult;
/// Oneof variants of `QueryResponse::result` (matched by callers).
pub use pb::query_response;
