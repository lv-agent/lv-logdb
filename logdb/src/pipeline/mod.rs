//! Background pipeline threads.
//!
//! The pipeline consists of:
//! - **Sealer** (optional): computes SHA-256 hash chain over published slots
//! - **Committer**: serializes records from slots to segment files, advances
//!   committed_cursor and durable_cursor
//! - **Pusher** (optional): pushes durable records to a remote endpoint

pub mod committer;
#[cfg(feature = "hash-chain")]
pub mod sealer;
pub mod signal;
pub mod trigger;
