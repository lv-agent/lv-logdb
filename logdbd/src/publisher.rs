//! Durable-cursor poller that feeds the SubscribeHub — the hub's publisher
//! (cr-027 phase 4).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use tokio::sync::watch;

use crate::storage::Storage;
use crate::subscribe::SubscribeHub;

/// How often the publisher polls the durable cursor.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Background publisher: chases `Storage::durable_gid()`, scans the new range,
/// and fans each decoded record out to the `SubscribeHub` by `stream_id`. Also
/// wakes blocked Tail tasks (long-poll, cr-037 A) when `durable_gid` advances.
///
/// Latency is bounded by `POLL_INTERVAL` (≤10 ms). Non-blocking sends —
/// records stay in the segment.
pub struct SubscribePublisher {
    storage: Arc<Storage>,
    subscribe_hub: Arc<SubscribeHub>,
    tombstone_tracker: Arc<crate::tombstone::TombstoneTracker>,
    last_gid: AtomicU64,
    running: AtomicBool,
    /// Wakes blocked Tail handlers when new durable data is available.
    tail_notify: watch::Sender<u64>,
}

impl SubscribePublisher {
    pub fn new(
        storage: Arc<Storage>,
        hub: Arc<SubscribeHub>,
        tombstone_tracker: Arc<crate::tombstone::TombstoneTracker>,
        tail_notify: watch::Sender<u64>,
    ) -> Self {
        Self {
            storage,
            subscribe_hub: hub,
            tombstone_tracker,
            last_gid: AtomicU64::new(0),
            running: AtomicBool::new(false),
            tail_notify,
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
        // Last-seen per-shard durable cursors. The global durable_gid is the
        // MIN across shards; in an imbalanced multi-shard workload a lagging
        // shard stalls it, so waking Tails on the min alone would miss records
        // that are already per-shard durable in other shards (Tails read
        // per-shard via read_batch). Wake whenever ANY shard advances.
        let mut last_per_shard = self.storage.durable_cursors();
        while self.running.load(Ordering::Acquire) {
            let durable = self.storage.durable_gid();
            let last = self.last_gid.load(Ordering::Acquire);
            if durable > last {
                match self.storage.scan(last, durable) {
                    Ok(records) => {
                        for rec in &records {
                            if rec.event_type == crate::tombstone::STREAM_DELETED_EVENT {
                                // Track the cutoff (mainly for replicated tombstones on
                                // standbys); never push tombstones to subscribers.
                                self.tombstone_tracker.record(rec.stream_id, rec.seq);
                            } else if self.tombstone_tracker.is_live(rec.stream_id, rec.seq) {
                                self.subscribe_hub.publish(rec.stream_id, rec);
                            }
                        }
                        // Half-open [last, durable): everything below durable is done.
                        self.last_gid.store(durable, Ordering::Release);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "subscribe publisher scan failed");
                    }
                }
            }
            // Wake blocked Tail handlers when ANY per-shard durable cursor
            // advanced. The watch value is a monotonic wake token (sum of
            // per-shard cursors), not a literal gid — it strictly increases on
            // any shard's progress so `watch` always notifies waiters.
            let now = self.storage.durable_cursors();
            if now != last_per_shard {
                let wake_token: u64 = now.iter().copied().sum();
                last_per_shard = now;
                let _ = self.tail_notify.send(wake_token);
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
            Arc::new(crate::tombstone::TombstoneTracker::new()),
                tokio::sync::watch::channel(0).0,
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
                None,
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

        // Non-blocking stop: the poller exits within one POLL_INTERVAL (10 ms);
        // we don't join the thread.
        publisher.stop();
    }

    #[tokio::test]
    async fn publisher_tracks_tombstone_and_skips_pushing_it() {
        let (st, _dir) = test_storage();
        let storage = Arc::new(st);
        let hub = Arc::new(SubscribeHub::new());
        let tombstones = Arc::new(crate::tombstone::TombstoneTracker::new());

        // Subscribe BEFORE appending so the broadcast receiver exists. Match the
        // tombstone event type too so we can prove it is never pushed.
        let ets: HashSet<String> = ["tool.call".into(), crate::tombstone::STREAM_DELETED_EVENT.into()].into();
        let mut handle = hub.subscribe(1, ets);

        let publisher = Arc::new(SubscribePublisher::new(
            Arc::clone(&storage),
            Arc::clone(&hub),
            Arc::clone(&tombstones),
                tokio::sync::watch::channel(0).0,
        ));
        publisher.clone().start();

        // 1) A normal record is appended + flushed ⇒ it must be pushed.
        storage
            .append(
                1,
                1,
                "tool.call",
                "text/plain",
                &BTreeMap::new(),
                1,
                b"pre-delete",
                None,
            )
            .unwrap();
        storage.flush().unwrap();

        let rec = tokio::time::timeout(Duration::from_secs(2), handle.next_matching())
            .await
            .expect("normal record not pushed within 2s")
            .expect("hub closed");
        assert_eq!(rec.event_type, "tool.call");
        assert_eq!(rec.user_content, b"pre-delete");

        // 2) Append + flush a tombstone. The publisher must track it (cutoff ==
        // its seq) and must NOT push it to subscribers.
        storage
            .append(
                1,
                1,
                crate::tombstone::STREAM_DELETED_EVENT,
                "application/json",
                &BTreeMap::new(),
                2,
                &[],
                None,
            )
            .unwrap();
        storage.flush().unwrap();

        // Give the poller a few cycles to observe the tombstone.
        let mut cutoff = 0u64;
        for _ in 0..50 {
            cutoff = tombstones.cutoff(1);
            if cutoff == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(cutoff, 2, "publisher must track the tombstone cutoff");

        // The tombstone must NOT be pushed: next_matching should time out.
        let pushed = tokio::time::timeout(Duration::from_millis(150), handle.next_matching()).await;
        assert!(
            pushed.is_err(),
            "tombstone record must not be pushed to subscribers"
        );

        publisher.stop();
    }
}
