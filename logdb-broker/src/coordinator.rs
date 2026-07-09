//! Group coordination: membership + round-robin shard assignment.
//!
//! A [`GroupState`] owns one consumer group: its members (in stable join
//! order), the current assignment generation, and a round-robin mapping of
//! shards → members. Round-robin keeps assignment simple and predictable:
//! `shard i → members[i % n]`, so the join order fully determines the split.
//! A [`CoordinatorRegistry`] holds one [`GroupState`] per `(ns, stream, group)`.

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

/// `(namespace, stream, group)` — the key for one consumer group.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    pub namespace: String,
    pub stream: String,
    pub group: String,
}

impl GroupKey {
    pub fn new(namespace: &str, stream: &str, group: &str) -> Self {
        Self {
            namespace: namespace.into(),
            stream: stream.into(),
            group: group.into(),
        }
    }
}

/// Result of a successful [`GroupState::join`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinResult {
    /// Generation after this join.
    pub generation: u32,
    /// Shards now assigned to the joining consumer (ascending).
    pub assigned_shards: Vec<u32>,
}

/// One consumer group's mutable coordination state.
///
/// Sticky cooperative rebalance (cr-037 perf): consumers keep their shards
/// across rebalances unless ownership actually changes (member joins/leaves).
/// Only the affected consumers have their forward tasks restarted.
#[derive(Debug)]
pub struct GroupState {
    generation: u32,
    /// Member consumer_ids in stable join order.
    members: Vec<String>,
    /// consumer_id → assigned shards (mirrors `members`, derived by round-robin).
    assignments: HashMap<String, Vec<u32>>,
    /// Per-shard last-processed seq (0 = none). Bound to group+shard, NOT to a
    /// consumer, so it survives rebalances and (persisted) broker restarts.
    /// Membership is transient — NOT persisted; consumers rejoin after restart.
    shard_offsets: HashMap<u32, u64>,
    /// Consumer_ids whose assignment changed in the last rebalance. The
    /// orchestrator only swaps forward tasks for these members.
    last_changed: std::collections::HashSet<String>,
    num_shards: u32,
}

impl GroupState {
    /// Create an empty group (generation 0) over `num_shards` shards.
    pub fn new(num_shards: u32) -> Self {
        Self {
            generation: 0,
            members: Vec::new(),
            assignments: HashMap::new(),
            shard_offsets: HashMap::new(),
            last_changed: std::collections::HashSet::new(),
            num_shards,
        }
    }

    /// Current generation (bumped on every membership change).
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Number of current members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Member consumer_ids in join order.
    pub fn member_ids(&self) -> &[String] {
        &self.members
    }

    /// Shards assigned to `consumer_id`, or `None` if not a member.
    pub fn assigned(&self, consumer_id: &str) -> Option<&[u32]> {
        self.assignments.get(consumer_id).map(Vec::as_slice)
    }

    /// Join (or rejoin) a consumer. A new member triggers a rebalance (generation
    /// bump); a returning member is a no-op that returns its current assignment.
    /// Uses sticky assignment beyond the first member.
    pub fn join(&mut self, consumer_id: &str) -> JoinResult {
        if !self.members.contains(&consumer_id.to_string()) {
            let first = self.members.is_empty();
            self.members.push(consumer_id.to_string());
            self.generation = self.generation.saturating_add(1);
            if first {
                self.recompute_round_robin();
                self.last_changed = self.members.iter().cloned().collect();
            } else {
                self.last_changed = self.recompute_sticky();
            }
        }
        JoinResult {
            generation: self.generation,
            assigned_shards: self
                .assignments
                .get(consumer_id)
                .cloned()
                .unwrap_or_default(),
        }
    }

    /// Leave a consumer. Returns `true` if the member was present (and a
    /// rebalance + generation bump happened), `false` if it was already gone.
    pub fn leave(&mut self, consumer_id: &str) -> bool {
        if let Some(pos) = self.members.iter().position(|m| m == consumer_id) {
            self.members.remove(pos);
            self.assignments.remove(consumer_id);
            self.generation = self.generation.saturating_add(1);
            self.last_changed = self.recompute_sticky();
            true
        } else {
            false
        }
    }

    /// Consumer_ids whose shard set changed in the last rebalance (the
    /// orchestrator only needs to swap forward tasks for these members).
    pub fn last_changed(&self) -> &std::collections::HashSet<String> {
        &self.last_changed
    }

    /// Recompute round-robin assignments (non-sticky). Used on first join.
    fn recompute_round_robin(&mut self) {
        self.assignments.clear();
        let n = self.members.len();
        if n == 0 {
            return;
        }
        for shard in 0..self.num_shards {
            let owner = &self.members[(shard as usize) % n];
            self.assignments
                .entry(owner.clone())
                .or_default()
                .push(shard);
        }
    }

    /// Sticky rebalance: keep existing shard assignments wherever possible, only
    /// move what is necessary for fairness. Returns the set of consumer_ids
    /// whose shard set actually changed — the orchestrator only needs to swap
    /// the forward task for those members (the rest stay put).
    fn recompute_sticky(&mut self) -> std::collections::HashSet<String> {
        let old = std::mem::take(&mut self.assignments);
        let n = self.members.len();
        if n == 0 {
            return old.keys().cloned().collect(); // all departed
        }

        // 1. Start from old assignments for members still present.
        for id in &self.members {
            self.assignments
                .entry(id.clone())
                .or_default()
                .extend(old.get(id).cloned().unwrap_or_default());
        }

        // 2. Collect shards not assigned to any current member (from departures).
        let mut free: Vec<u32> = (0..self.num_shards)
            .filter(|s| {
                !self
                    .assignments
                    .values()
                    .any(|v| v.contains(s))
            })
            .collect();

        // 3. Fair share: each member gets at least `floor(num_shards / n)`,
        //    first `num_shards % n` get one extra.
        let target_base = self.num_shards / n as u32;
        let extra = (self.num_shards as usize) % n;

        // 4. Shed excess shards: prefer shedding "foreign" shards
        //    (where shard % n != member index) first — they naturally
        //    belong to other members in round-robin. Only shed "ours"
        //    if we are still over target after foreign ones are gone.
        for (idx, id) in self.members.iter().enumerate() {
            let target = target_base + if idx < extra { 1 } else { 0 };
            let shards = self.assignments.get_mut(id).unwrap();
            let mut to_shed = Vec::new();
            let surplus = (shards.len() as u32).saturating_sub(target);
            // Foreign-first: shed shards where shard % n != index.
            shards.retain(|s| {
                if to_shed.len() as u32 >= surplus {
                    return true;
                }
                let is_ours = (*s as usize) % n == idx;
                if is_ours {
                    true
                } else {
                    to_shed.push(*s);
                    false
                }
            });
            // If still over target, shed from the end (our own shards)
            while shards.len() as u32 > target {
                if let Some(s) = shards.pop() {
                    to_shed.push(s);
                }
            }
            free.extend(to_shed);
        }

        // 5. Fill under-assigned members from the free pool.
        for (idx, id) in self.members.iter().enumerate() {
            let target = target_base + if idx < extra { 1 } else { 0 };
            let shards = self.assignments.get_mut(id).unwrap();
            while (shards.len() as u32) < target {
                if let Some(s) = free.pop() {
                    shards.push(s);
                } else {
                    break;
                }
            }
        }

        // 6. Sort for determinism.
        for v in self.assignments.values_mut() {
            v.sort_unstable();
        }

        // 7. Diff: which consumers had their shard set change?
        let mut changed = std::collections::HashSet::new();
        for id in &self.members {
            let new = self.assignments.get(id).map(|v| v.as_slice());
            let old_s = old.get(id).map(|v| v.as_slice());
            if new != old_s {
                changed.insert(id.clone());
            }
        }
        for id in old.keys() {
            if !self.members.contains(id) {
                changed.insert(id.clone()); // departed
            }
        }
        changed
    }

    /// Record that `shard` was processed up to `seq` (last-processed seq).
    /// Monotonic: a lower `seq` is ignored. Returns `true` if it advanced.
    pub fn commit_offset(&mut self, shard: u32, seq: u64) -> bool {
        let entry = self.shard_offsets.entry(shard).or_insert(0);
        if seq > *entry {
            *entry = seq;
            true
        } else {
            false
        }
    }

    /// Last-processed seq committed for `shard` (0 = none).
    pub fn shard_offset(&self, shard: u32) -> u64 {
        self.shard_offsets.get(&shard).copied().unwrap_or(0)
    }

    /// All committed `(shard, seq)` offsets (for persistence / forwarding).
    pub fn shard_offsets(&self) -> &HashMap<u32, u64> {
        &self.shard_offsets
    }
}

/// Read-only snapshot of one group (for ListMembers / logging).
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub generation: u32,
    /// `(consumer_id, assigned_shards)` in join order.
    pub members: Vec<(String, Vec<u32>)>,
}

/// Holds one [`GroupState`] per `(namespace, stream, group)`, keyed by
/// [`GroupKey`]. A broker owns one registry; the gRPC handlers delegate to it.
///
/// Groups are created on first join and **retained when empty** so that a
/// group's generation stays monotonic across leave/rejoin cycles (a recreated
/// group would reset generation, which consumers track).
#[derive(Debug)]
pub struct CoordinatorRegistry {
    num_shards: u32,
    groups: RwLock<HashMap<GroupKey, GroupState>>,
}

impl CoordinatorRegistry {
    pub fn new(num_shards: u32) -> Self {
        Self {
            num_shards,
            groups: RwLock::new(HashMap::new()),
        }
    }

    /// Join `consumer_id` into `(ns, stream, group)`, creating the group on
    /// first join. Returns the joiner's new generation + assignment.
    pub fn join(
        &self,
        namespace: &str,
        stream: &str,
        group: &str,
        consumer_id: &str,
    ) -> JoinResult {
        let key = GroupKey::new(namespace, stream, group);
        let mut groups = self.groups.write().unwrap_or_else(PoisonError::into_inner);
        let state = groups
            .entry(key)
            .or_insert_with(|| GroupState::new(self.num_shards));
        state.join(consumer_id)
    }

    /// Remove `consumer_id` from `(ns, stream, group)`. Returns `true` if the
    /// member was present. The (now possibly empty) group is retained.
    pub fn leave(
        &self,
        namespace: &str,
        stream: &str,
        group: &str,
        consumer_id: &str,
    ) -> bool {
        let key = GroupKey::new(namespace, stream, group);
        let mut groups = self.groups.write().unwrap_or_else(PoisonError::into_inner);
        match groups.get_mut(&key) {
            Some(state) => state.leave(consumer_id),
            None => false,
        }
    }

    /// Consumer_ids whose assignment changed in the last rebalance.
    /// Empty or absent if the group has never been changed.
    pub fn last_changed(
        &self,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> std::collections::HashSet<String> {
        let key = GroupKey::new(namespace, stream, group);
        let groups = self.groups.read().unwrap_or_else(PoisonError::into_inner);
        groups
            .get(&key)
            .map(|s| s.last_changed().clone())
            .unwrap_or_default()
    }

    /// Snapshot of `(ns, stream, group)`, or `None` if the group was never
    /// created.
    pub fn group_snapshot(
        &self,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> Option<GroupSnapshot> {
        let key = GroupKey::new(namespace, stream, group);
        let groups = self.groups.read().unwrap_or_else(PoisonError::into_inner);
        groups.get(&key).map(|state| GroupSnapshot {
            generation: state.generation(),
            members: state
                .member_ids()
                .iter()
                .map(|id| (id.clone(), state.assigned(id).unwrap_or(&[]).to_vec()))
                .collect(),
        })
    }

    /// Commit `seq` as last-processed for `shard` in `(ns, stream, group)`,
    /// creating the (possibly member-less) group if needed — offsets are bound
    /// to group+shard and outlive membership. Returns `true` if it advanced.
    pub fn commit_offset(
        &self,
        namespace: &str,
        stream: &str,
        group: &str,
        shard: u32,
        seq: u64,
    ) -> bool {
        let key = GroupKey::new(namespace, stream, group);
        let mut groups = self.groups.write().unwrap_or_else(PoisonError::into_inner);
        let state = groups
            .entry(key)
            .or_insert_with(|| GroupState::new(self.num_shards));
        state.commit_offset(shard, seq)
    }

    /// Last-processed seq committed for `shard` in a group (0 if none / unknown).
    pub fn shard_offset(
        &self,
        namespace: &str,
        stream: &str,
        group: &str,
        shard: u32,
    ) -> u64 {
        let key = GroupKey::new(namespace, stream, group);
        let groups = self.groups.read().unwrap_or_else(PoisonError::into_inner);
        groups
            .get(&key)
            .map(|s| s.shard_offset(shard))
            .unwrap_or(0)
    }

    /// Total shards the broker assigns (from config; matches logdbd's `shards`).
    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shards_of(state: &GroupState, id: &str) -> Vec<u32> {
        let mut v = state.assigned(id).unwrap_or(&[]).to_vec();
        v.sort();
        v
    }

    #[test]
    fn first_member_gets_all_shards() {
        let mut s = GroupState::new(4);
        let r = s.join("c1");
        assert_eq!(r.generation, 1);
        assert_eq!(shards_of(&s, "c1"), vec![0, 1, 2, 3]);
    }

    #[test]
    fn two_members_split_round_robin() {
        let mut s = GroupState::new(4);
        s.join("c1");
        let r2 = s.join("c2");
        assert_eq!(r2.generation, 2);
        // round-robin shard i → member[i % 2]: c1(idx 0) → {0,2}, c2(idx 1) → {1,3}
        assert_eq!(shards_of(&s, "c1"), vec![0, 2]);
        assert_eq!(shards_of(&s, "c2"), vec![1, 3]);
    }

    #[test]
    fn four_members_one_shard_each() {
        let mut s = GroupState::new(4);
        for id in ["c1", "c2", "c3", "c4"] {
            s.join(id);
        }
        // Sticky may preserve old shard ownership, so the exact mapping
        // differs from pure round-robin. But every member gets exactly 1
        // shard, all 4 shards are covered, disjoint.
        let mut seen = std::collections::HashSet::new();
        for id in ["c1", "c2", "c3", "c4"] {
            let got = shards_of(&s, id);
            assert_eq!(got.len(), 1, "{id} must own exactly 1 shard");
            assert!(seen.insert(got[0]), "shard {} assigned twice", got[0]);
        }
        assert_eq!(seen.len(), 4);
    }

    #[test]
    fn more_members_than_shards_leaves_some_idle() {
        let mut s = GroupState::new(2);
        s.join("c1");
        s.join("c2");
        s.join("c3");
        assert_eq!(shards_of(&s, "c1"), vec![0]);
        assert_eq!(shards_of(&s, "c2"), vec![1]);
        assert!(shards_of(&s, "c3").is_empty(), "surplus member gets no shards");
    }

    #[test]
    fn leave_rebalances_and_bumps_generation() {
        let mut s = GroupState::new(4);
        s.join("c1");
        s.join("c2");
        assert!(s.leave("c2"));
        assert_eq!(s.generation(), 3); // join c1→1, join c2→2, leave→3
        assert_eq!(shards_of(&s, "c1"), vec![0, 1, 2, 3]);
        assert!(s.assigned("c2").is_none(), "leaving member has no assignment");
    }

    #[test]
    fn leave_unknown_is_noop_no_generation_bump() {
        let mut s = GroupState::new(4);
        s.join("c1");
        let gen_before = s.generation();
        assert!(!s.leave("nobody"));
        assert_eq!(s.generation(), gen_before);
    }

    #[test]
    fn rejoin_existing_member_is_noop() {
        let mut s = GroupState::new(4);
        let r1 = s.join("c1");
        let r2 = s.join("c1");
        assert_eq!(r2.generation, r1.generation, "rejoin must not bump generation");
        assert_eq!(r2.assigned_shards, r1.assigned_shards);
        assert_eq!(s.member_count(), 1);
    }

    #[test]
    fn leave_shifts_assignment_of_remaining() {
        let mut s = GroupState::new(2);
        s.join("a");
        s.join("b");
        // a(idx0)→{0}, b(idx1)→{1}
        s.leave("a");
        // now only b → b→{0,1}
        assert_eq!(shards_of(&s, "b"), vec![0, 1]);
    }

    // ── CoordinatorRegistry (multi-group routing) ───────────────────────────

    #[test]
    fn registry_join_creates_group_with_full_assignment() {
        let reg = CoordinatorRegistry::new(4);
        let r = reg.join("ns", "s", "g", "c1");
        assert_eq!(r.generation, 1);
        assert_eq!(r.assigned_shards.len(), 4);
    }

    #[test]
    fn registry_independent_groups_do_not_interfere() {
        let reg = CoordinatorRegistry::new(4);
        // Same consumer id in two different groups → both get all shards,
        // independent generations.
        let ra = reg.join("ns", "s", "gA", "c1");
        let rb = reg.join("ns", "s", "gB", "c1");
        assert_eq!(ra.assigned_shards.len(), 4);
        assert_eq!(rb.assigned_shards.len(), 4);
        // Adding a second member to gA must NOT change gB's single-member state.
        reg.join("ns", "s", "gA", "c2");
        let snap_b = reg.group_snapshot("ns", "s", "gB").unwrap();
        assert_eq!(snap_b.members.len(), 1);
    }

    #[test]
    fn registry_group_keyed_by_stream_too() {
        let reg = CoordinatorRegistry::new(2);
        reg.join("ns", "stream-a", "g", "c1");
        reg.join("ns", "stream-b", "g", "c1");
        // Different stream ⇒ different group ⇒ c1 owns all shards in each.
        assert_eq!(
            reg.group_snapshot("ns", "stream-a", "g")
                .unwrap()
                .members
                .len(),
            1
        );
        assert_eq!(
            reg.group_snapshot("ns", "stream-b", "g")
                .unwrap()
                .members
                .len(),
            1
        );
    }

    #[test]
    fn registry_snapshot_reflects_join_and_leave() {
        let reg = CoordinatorRegistry::new(4);
        reg.join("ns", "s", "g", "c1");
        reg.join("ns", "s", "g", "c2");
        let snap = reg.group_snapshot("ns", "s", "g").unwrap();
        assert_eq!(snap.generation, 2);
        assert_eq!(snap.members.len(), 2);

        reg.leave("ns", "s", "g", "c1");
        let snap2 = reg.group_snapshot("ns", "s", "g").unwrap();
        assert_eq!(snap2.generation, 3);
        assert_eq!(snap2.members.len(), 1);
        // Generation stays monotonic: the empty group is retained (not reset).
        assert_eq!(snap2.members[0].0, "c2");
    }

    #[test]
    fn registry_snapshot_unknown_group_is_none() {
        let reg = CoordinatorRegistry::new(4);
        assert!(reg.group_snapshot("ns", "s", "nope").is_none());
    }

    #[test]
    fn registry_leave_unknown_group_is_false() {
        let reg = CoordinatorRegistry::new(4);
        assert!(!reg.leave("ns", "s", "nope", "c1"));
    }

    // ── offset tracking (Phase 6) ───────────────────────────────────────────

    #[test]
    fn commit_offset_stores_and_only_advances() {
        let mut s = GroupState::new(4);
        assert_eq!(s.shard_offset(1), 0, "unset shard offset defaults to 0");
        assert!(s.commit_offset(1, 5), "first commit must advance");
        assert_eq!(s.shard_offset(1), 5);
        assert!(s.commit_offset(1, 8), "higher seq advances");
        assert_eq!(s.shard_offset(1), 8);
        assert!(
            !s.commit_offset(1, 3),
            "lower seq must NOT regress the offset"
        );
        assert_eq!(s.shard_offset(1), 8);
    }

    #[test]
    fn commit_offset_is_independent_per_shard() {
        let mut s = GroupState::new(4);
        s.commit_offset(1, 5);
        s.commit_offset(2, 9);
        assert_eq!(s.shard_offset(1), 5);
        assert_eq!(s.shard_offset(2), 9);
        assert_eq!(s.shard_offset(3), 0);
    }

    #[test]
    fn registry_commit_offset_creates_group_and_persists_across_rejoin() {
        let reg = CoordinatorRegistry::new(4);
        // Committing an offset creates the group (even with no members) so the
        // offset survives a broker restart that consumers rejoin into.
        assert!(reg.commit_offset("ns", "s", "g", 2, 11));
        // The offset is retained even when a member joins/leaves (it is bound
        // to the group+shard, not the consumer).
        reg.join("ns", "s", "g", "c1");
        reg.leave("ns", "s", "g", "c1");
        assert_eq!(reg.shard_offset("ns", "s", "g", 2), 11);
    }
}
