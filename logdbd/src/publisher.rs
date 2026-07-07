//! Durable-cursor poller that feeds the SubscribeHub. Replaces the Indexer as
//! the hub's publisher (cr-027 phase 4). The Indexer still writes SQLite; it is
//! deleted in phase 5.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use crate::storage::Storage;
use crate::subscribe::SubscribeHub;

/// How often the publisher polls the durable cursor.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Background publisher: chases `Storage::durable_gid()`, scans the new range,
/// and fans each decoded record out to the `SubscribeHub` by `stream_id`.
///
/// Latency is bounded by `POLL_INTERVAL` (≤10 ms), identical to the legacy
/// Indexer-driven push. Non-blocking sends — records stay in the segment.
pub struct SubscribePublisher {
    storage: Arc<Storage>,
    hub: Arc<SubscribeHub>,
    last_gid: AtomicU64,
    running: AtomicBool,
}

impl SubscribePublisher {
    pub fn new(storage: Arc<Storage>, hub: Arc<SubscribeHub>) -> Self {
        Self {
            storage,
            hub,
            last_gid: AtomicU64::new(0),
            running: AtomicBool::new(false),
        }
    }

    /// Spawn the background thread. Idempotent if called once.
    pub fn start(self: Arc<Self>) {
        self.running.store(true, Ordering::Release);
        let this = Arc::clone(&self);
        thread::Builder::new()
            .name("logdbd-subscribe-publisher".into())
            .spawn(move || this.run())
            .expect("spawn subscribe publisher thread");
    }

    /// Stop the background thread (it exits after the next poll).
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    fn run(&self) {
        while self.running.load(Ordering::Acquire) {
            let durable = self.storage.durable_gid();
            let last = self.last_gid.load(Ordering::Acquire);
            if durable > last {
                match self.storage.scan(last, durable) {
                    Ok(records) => {
                        for rec in &records {
                            self.hub.publish(rec.stream_id, rec);
                        }
                        // Half-open [last, durable): everything below durable is done.
                        self.last_gid.store(durable, Ordering::Release);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "subscribe publisher scan failed");
                    }
                }
            }
            thread::sleep(POLL_INTERVAL);
        }
        tracing::info!(
            last_gid = self.last_gid.load(Ordering::Acquire),
            "subscribe publisher stopped"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    fn test_storage() -> (Storage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = logdb::Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 256;
        config.durability_mode = logdb::DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        config.shards = 1;
        let db = logdb::LogDb::open(config).unwrap();
        (Storage::new(db, 1), dir)
    }

    #[tokio::test]
    async fn publisher_pushes_durable_records_to_hub() {
        let (st, _dir) = test_storage();
        let storage = Arc::new(st);
        let hub = Arc::new(SubscribeHub::new());

        // Subscribe BEFORE appending so the broadcast receiver exists.
        let ets: HashSet<String> = ["tool.call".into()].into();
        let mut handle = hub.subscribe(1, ets);

        let publisher = Arc::new(SubscribePublisher::new(
            Arc::clone(&storage),
            Arc::clone(&hub),
        ));
        publisher.clone().start();

        // Append + flush so it becomes durable.
        storage
            .append(
                1,
                1,
                "tool.call",
                "text/plain",
                &BTreeMap::new(),
                1,
                b"hello",
            )
            .unwrap();
        storage.flush().unwrap();

        // The publisher polls every ≤10 ms; allow generous headroom on a slow CI.
        let rec = tokio::time::timeout(Duration::from_secs(2), handle.next_matching())
            .await
            .expect("publisher did not push within 2s")
            .expect("hub closed");
        assert_eq!(rec.event_type, "tool.call");
        assert_eq!(rec.stream_id, 1);
        assert_eq!(rec.user_content, b"hello");

        publisher.stop();
    }
}
