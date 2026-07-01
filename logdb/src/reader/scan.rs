//! Cross-shard, cross-segment scan iterators.
//!
//! `ShardScanner` yields one shard's records in ascending global-id order,
//! transparently crossing segment boundaries. For `shards > 1`, [`ScanIter`]
//! k-way-merges the per-shard scanners by global id (a total order, since the
//! shard id occupies the low bits of every record id). `shards == 1` skips the
//! merge heap entirely.
//!
//! Both scanners reuse the single-segment `RecordIter` via
//! `iter_for_segment`, so raw/compressed/encrypted framing is handled
//! identically to point reads.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use crate::error::ReadError;
use crate::record::Record;

use super::iter::RecordIter;
use super::{iter_for_segment, ManifestEntry, SegmentManifest};

/// Single-shard, cross-segment ascending iterator over `[from_id, to_id)`.
struct ShardScanner {
    key: Option<[u8; 32]>,
    from_id: u64,
    to_id: u64,
    /// Candidate segments, from the one containing `from_id` onward. Fetched
    /// up front from the manifest; the manifest is not needed after this.
    segments: Vec<ManifestEntry>,
    seg_idx: usize,
    /// Current single-segment iterator (`None` before first load / after the
    /// last segment is exhausted).
    cur: Option<RecordIter>,
}

impl ShardScanner {
    fn new(
        manifest: Arc<Mutex<SegmentManifest>>,
        key: Option<[u8; 32]>,
        from_id: u64,
        to_id: u64,
    ) -> Result<Self, ReadError> {
        let segments = manifest.lock().unwrap().segments_from(from_id)?;
        Ok(Self {
            key,
            from_id,
            to_id,
            segments,
            seg_idx: 0,
            cur: None,
        })
    }

    /// Advance to the next segment that may contain records in range. Returns
    /// false when no candidate remains (end of manifest, or the next segment's
    /// base is already `>= to_id`).
    fn load_next_segment(&mut self) -> bool {
        while self.seg_idx < self.segments.len() {
            let entry = &self.segments[self.seg_idx];
            self.seg_idx += 1;
            // Segments are sorted by base_sequence; once we reach one whose
            // first record is >= to_id, nothing further can contribute.
            if entry.base_sequence() >= self.to_id {
                self.cur = None;
                return false;
            }
            match iter_for_segment(entry, self.from_id, self.to_id, self.key) {
                Ok(it) => {
                    self.cur = Some(it);
                    return true;
                }
                Err(_) => continue, // skip an unreadable segment, try the next
            }
        }
        self.cur = None;
        false
    }
}

impl Iterator for ShardScanner {
    type Item = Result<Record, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(it) = &mut self.cur {
                if let Some(r) = it.next() {
                    return Some(r);
                }
            }
            // Current segment exhausted — advance to the next candidate.
            if !self.load_next_segment() {
                return None;
            }
        }
    }
}

/// k-way min-heap merge of [`ShardScanner`]s by global record id.
struct MergeIter {
    scanners: Vec<ShardScanner>,
    /// Current peeked record per scanner (only `Ok` records are held).
    peeked: Vec<Option<Record>>,
    /// First error encountered; surfaced once the heap drains. Mirrors
    /// `RecordIter`'s contract: I/O errors propagate, bad records are skipped.
    pending_err: Option<ReadError>,
    /// Min-heap of `(global_id, scanner_idx)` for scanners with an `Ok` peek.
    /// Global ids are unique across shards (shard id in the low bits), so the
    /// tie-breaker index never actually decides.
    heap: BinaryHeap<Reverse<(u64, usize)>>,
}

impl MergeIter {
    fn new(scanners: Vec<ShardScanner>) -> Result<Self, ReadError> {
        let n = scanners.len();
        let mut merge = Self {
            scanners,
            peeked: vec![None; n],
            pending_err: None,
            heap: BinaryHeap::with_capacity(n),
        };
        // Prime: peek each scanner once.
        for i in 0..n {
            merge.refill(i);
        }
        Ok(merge)
    }

    /// Pull the next record from scanner `i`; update `peeked[i]` and the heap.
    /// The first error wins and is stored in `pending_err`; the scanner is
    /// then treated as exhausted.
    fn refill(&mut self, i: usize) {
        match self.scanners[i].next() {
            Some(Ok(r)) => {
                let gid = r.id.sequence;
                self.peeked[i] = Some(r);
                self.heap.push(Reverse((gid, i)));
            }
            Some(Err(e)) => {
                if self.pending_err.is_none() {
                    self.pending_err = Some(e);
                }
                self.peeked[i] = None;
            }
            None => {
                self.peeked[i] = None;
            }
        }
    }
}

impl Iterator for MergeIter {
    type Item = Result<Record, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(Reverse((_, i))) = self.heap.pop() {
            let r = self.peeked[i]
                .take()
                .expect("heap entry must have a peeked record");
            self.refill(i);
            return Some(Ok(r));
        }
        // Heap empty — surface a deferred error, if any.
        self.pending_err.take().map(Err)
    }
}

/// Public scan iterator returned by [`LogDb::scan`](crate::LogDb::scan) and
/// [`LogDb::replay_from`](crate::LogDb::replay_from).
///
/// `shards == 1` runs a single cross-segment stream (no merge heap); `shards >
/// 1` k-way-merges the per-shard streams by ascending global id. Records are
/// yielded in ascending global-id order. An empty range produces an empty
/// iterator (no error). Construction is crate-internal (see `ScanIter::build`).
pub struct ScanIter {
    // `+ Send` so the iterator can be moved across threads / held across an
    // `.await` in a spawned task (e.g. by async consumers like a gRPC server).
    // The concrete scanners (ShardScanner / MergeIter / RecordIter) are already
    // Send; this bound just preserves it through the trait object.
    inner: Box<dyn Iterator<Item = Result<Record, ReadError>> + Send>,
}

impl ScanIter {
    /// Build a scan iterator over one shard per manifest. One manifest yields a
    /// single stream; several yield a k-way merge by global id.
    pub(crate) fn build(
        manifests: Vec<Arc<Mutex<SegmentManifest>>>,
        key: Option<[u8; 32]>,
        from_id: u64,
        to_id: u64,
    ) -> Result<Self, ReadError> {
        let mut scanners: Vec<ShardScanner> = Vec::with_capacity(manifests.len());
        for m in manifests {
            scanners.push(ShardScanner::new(m, key, from_id, to_id)?);
        }
        Ok(match scanners.len() {
            1 => ScanIter {
                inner: Box::new(scanners.pop().expect("exactly one scanner")),
            },
            _ => ScanIter {
                inner: Box::new(MergeIter::new(scanners)?),
            },
        })
    }
}

impl Iterator for ScanIter {
    type Item = Result<Record, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
