//! logdb-broker — Kafka-style consumer-group coordinator for logdbd (cr-037).
//!
//! The broker is a stateless-by-reconstruction coordinator: it owns group
//! membership + shard assignment, and (Phase 3+) forwards records it Tails from
//! logdbd to the consumers that own each shard. Consumers talk ONLY to the
//! broker (symmetric data path).
//!
//! Phase 2 surface: group membership + round-robin shard assignment
//! ([`coordinator`]) exposed over gRPC ([`service`]).

pub mod config;
pub mod coordinator;
pub mod forwarder;
pub mod persistence;
pub mod service;
pub mod sessions;

pub use coordinator::{CoordinatorRegistry, GroupKey, GroupSnapshot};
pub use forwarder::Forwarder;
pub use persistence::{OffsetRecord, Persistence};
pub use service::BrokerServiceImpl;
