//! Encryption key providers (cr-032 Phase 2).
//!
//! See [`provider`] for the port/adapter design. The core [`logdb::KeyRing`] is
//! pure data; provider logic (file, and out-of-tree KMS adapters) lives here and
//! never enters the `logdb` core dependency graph.

pub mod provider;
pub use provider::{build_provider, FileKeyProvider, KeyError, KeyProvider};
