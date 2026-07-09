//! Active consumer-session registry (cr-037 Phase 5).
//!
//! While a consumer's `Consume` stream is open, the broker holds a
//! [`SessionHandle`] per `(group, consumer_id)` — the record channel + the
//! current forward task. On a membership change (rebalance) the orchestrator
//! iterates a group's sessions, swaps each session's forward task to the new
//! shards, and pushes rebalance/assignment frames on the shared channel.
//!
//! Sessions are removed when the consumer disconnects (detected by the forward
//! task's channel send failing). A session whose forward is aborted (mid
//! rebalance) does NOT self-remove — the rebalance reuses it with a new task.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use logdb_broker_proto::pb::ConsumeResponse;
use tokio::sync::mpsc;
use tonic::Status;

use crate::coordinator::GroupKey;

/// Monotonic-ish milliseconds since epoch (used for heartbeat liveness).
fn now_ms() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// One open consumer session: the shared record/signal channel + the swappable
/// forward task that pumps records onto it.
pub struct SessionHandle {
    pub tx: mpsc::Sender<Result<ConsumeResponse, Status>>,
    forward: Mutex<Option<tokio::task::JoinHandle<()>>>,
    last_heartbeat: std::sync::atomic::AtomicU64,
}

impl SessionHandle {
    pub fn new(tx: mpsc::Sender<Result<ConsumeResponse, Status>>) -> Arc<Self> {
        Arc::new(Self {
            tx,
            forward: Mutex::new(None),
            last_heartbeat: std::sync::atomic::AtomicU64::new(now_ms()),
        })
    }

    /// Record a heartbeat — the consumer is still alive.
    pub fn touch(&self) {
        self.last_heartbeat
            .store(now_ms(), std::sync::atomic::Ordering::Release);
    }

    /// Milliseconds since the last heartbeat.
    pub fn ms_since_last_heartbeat(&self) -> u64 {
        let seen = self
            .last_heartbeat
            .load(std::sync::atomic::Ordering::Acquire);
        now_ms().saturating_sub(seen)
    }

    /// Install a forward task, aborting any previous one. Called on consume
    /// start and on each rebalance (swap to the new shards).
    pub fn set_forward(&self, handle: tokio::task::JoinHandle<()>) {
        let mut g = self.forward.lock().expect("forward lock poisoned");
        if let Some(old) = g.take() {
            old.abort();
        }
        *g = Some(handle);
    }

    /// Drop the forward task without replacing it (session teardown).
    pub fn abort_forward(&self) {
        let mut g = self.forward.lock().expect("forward lock poisoned");
        if let Some(old) = g.take() {
            old.abort();
        }
    }
}

/// Per-group map of active consumer sessions.
#[derive(Default)]
pub struct Sessions {
    by_group: RwLock<HashMap<GroupKey, HashMap<String, Arc<SessionHandle>>>>,
}

impl Sessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a session under `(key, consumer_id)`. Replaces any existing
    /// session for that consumer (e.g. a reconnect). The caller must abort the
    /// old session's forward task first — otherwise its eventual deregistration
    /// would remove the new session (same key).
    pub fn insert(&self, key: GroupKey, consumer_id: String, handle: Arc<SessionHandle>) {
        let mut g = self.by_group.write().expect("sessions lock poisoned");
        // Abort the OLD forward just in case the caller forgot — the old task
        // holds a clone of the old Arc and would deregister this key on exit.
        if let Some(old) = g.entry(key.clone()).or_default().insert(consumer_id, handle) {
            old.abort_forward();
        }
    }

    /// Look up a session without removing it.
    pub fn get(&self, key: &GroupKey, consumer_id: &str) -> Option<Arc<SessionHandle>> {
        let g = self.by_group.read().expect("sessions lock poisoned");
        g.get(key).and_then(|m| m.get(consumer_id).cloned())
    }

    /// Remove a session (consumer disconnected). No-op if absent.
    pub fn remove(&self, key: &GroupKey, consumer_id: &str) {
        let mut g = self.by_group.write().expect("sessions lock poisoned");
        if let Some(group) = g.get_mut(key) {
            if let Some(handle) = group.remove(consumer_id) {
                handle.abort_forward();
            }
            if group.is_empty() {
                g.remove(key);
            }
        }
    }

    /// Snapshot of a group's active sessions: `(consumer_id, handle)` pairs.
    /// Used by the rebalance orchestrator.
    pub fn get_group(&self, key: &GroupKey) -> Vec<(String, Arc<SessionHandle>)> {
        let g = self.by_group.read().expect("sessions lock poisoned");
        g.get(key)
            .map(|m| m.iter().map(|(id, h)| (id.clone(), Arc::clone(h))).collect())
            .unwrap_or_default()
    }

    /// Return all `(GroupKey, consumer_id)` for sessions whose last heartbeat
    /// exceeds `timeout_ms`. The caller should evict them and trigger rebalance.
    pub fn stale_consumers(&self, timeout_ms: u64) -> Vec<(GroupKey, String)> {
        let g = self.by_group.read().expect("sessions lock poisoned");
        let mut out = Vec::new();
        for (key, members) in g.iter() {
            for (cid, h) in members {
                if h.ms_since_last_heartbeat() > timeout_ms {
                    out.push((key.clone(), cid.clone()));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle() -> Arc<SessionHandle> {
        let (tx, _rx) = mpsc::channel(4);
        SessionHandle::new(tx)
    }

    fn key(g: &str) -> GroupKey {
        GroupKey {
            namespace: "ns".into(),
            stream: "s".into(),
            group: g.into(),
        }
    }

    #[test]
    fn insert_and_get_group() {
        let s = Sessions::new();
        s.insert(key("g1"), "c1".into(), handle());
        s.insert(key("g1"), "c2".into(), handle());
        let got = s.get_group(&key("g1"));
        let ids: Vec<&str> = got.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"c1") && ids.contains(&"c2"));
    }

    #[test]
    fn remove_drops_a_session() {
        let s = Sessions::new();
        s.insert(key("g1"), "c1".into(), handle());
        s.insert(key("g1"), "c2".into(), handle());
        s.remove(&key("g1"), "c1");
        let got = s.get_group(&key("g1"));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "c2");
    }

    #[test]
    fn groups_are_independent() {
        let s = Sessions::new();
        s.insert(key("g1"), "c1".into(), handle());
        s.insert(key("g2"), "c1".into(), handle());
        assert_eq!(s.get_group(&key("g1")).len(), 1);
        assert_eq!(s.get_group(&key("g2")).len(), 1);
    }

    #[test]
    fn remove_unknown_is_noop() {
        let s = Sessions::new();
        s.remove(&key("nope"), "c1"); // must not panic
    }

    #[test]
    fn empty_group_is_evicted() {
        let s = Sessions::new();
        s.insert(key("g1"), "c1".into(), handle());
        s.remove(&key("g1"), "c1");
        assert!(s.get_group(&key("g1")).is_empty());
    }
}
