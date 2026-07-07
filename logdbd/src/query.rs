//! Native structured query engine — filter + aggregation + anti-join over a
//! single stream's records. Reads the log directly (Phase 3 wires this onto the
//! Query RPC, replacing the SQLite cache).

use crate::record::DecodedRecord;
use std::collections::HashSet;

/// A structured query over one stream's records.
#[derive(Debug, Clone, Default)]
pub struct Query {
    /// IN filter on event_type. Empty = all event types.
    pub event_types: Vec<String>,
    /// seq >= (None = no lower bound).
    pub from_seq: Option<u64>,
    /// seq <= (None = no upper bound).
    pub to_seq: Option<u64>,
    /// Equality predicates on metadata fields (all must match; AND).
    pub metadata: Vec<MetadataFilter>,
    /// What to compute from the filtered set.
    pub result: QueryResult,
    /// Metadata field for CountDistinct/Min/Max/DistinctValues. None = use `seq`.
    pub aggregate_field: Option<String>,
    /// Optional anti-join: drop filtered records whose `join_key` value also
    /// appears on a record matching `peer_event_types`.
    pub absent: Option<AbsentMatch>,
    /// Max records/values to return. 0 = unlimited.
    pub limit: usize,
    /// Order records/values by seq descending (default ascending).
    pub descending: bool,
}

#[derive(Debug, Clone, Default)]
pub struct MetadataFilter {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Default)]
pub struct AbsentMatch {
    /// Event types that "complete" a group (e.g. turn_completed).
    pub peer_event_types: Vec<String>,
    /// Metadata field joining candidates to peers (e.g. "turn_id").
    pub join_key: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryResult {
    #[default]
    Records,
    Count,
    Exists,
    CountDistinct,
    Min,
    Max,
    DistinctValues,
}

/// Result of executing a query.
#[derive(Debug, Clone)]
pub enum ResultSet {
    Records(Vec<DecodedRecord>),
    Count(u64),
    Exists(bool),
    CountDistinct(u64),
    /// 0 when no record has the field (mirrors SQL `COALESCE(MAX(..), 0)`).
    Min(u64),
    Max(u64),
    DistinctValues(Vec<String>),
}

pub fn execute(query: &Query, records: &[DecodedRecord]) -> ResultSet {
    // 1. Predicate filter.
    let filtered: Vec<&DecodedRecord> = records
        .iter()
        .filter(|r| matches_event_types(r, &query.event_types))
        .filter(|r| matches_seq_range(r, query.from_seq, query.to_seq))
        .filter(|r| matches_metadata(r, &query.metadata))
        .collect();

    // 2. Anti-join (NOT EXISTS): drop candidates whose join_key has a matching peer.
    let filtered = apply_absent(&filtered, records, query.absent.as_ref());

    // 3. Compute result.
    match query.result {
        QueryResult::Records => {
            let mut rows: Vec<DecodedRecord> = filtered.into_iter().cloned().collect();
            sort_and_limit(&mut rows, query.descending, query.limit);
            ResultSet::Records(rows)
        }
        QueryResult::Count => ResultSet::Count(filtered.len() as u64),
        QueryResult::Exists => ResultSet::Exists(!filtered.is_empty()),
        QueryResult::CountDistinct => {
            let n = distinct_field_values(&filtered, query.aggregate_field.as_deref()).len();
            ResultSet::CountDistinct(n as u64)
        }
        QueryResult::DistinctValues => {
            let mut vals = distinct_field_values(&filtered, query.aggregate_field.as_deref());
            if query.descending {
                vals.sort_by(|a, b| b.cmp(a));
            } else {
                vals.sort();
            }
            if query.limit > 0 && vals.len() > query.limit {
                vals.truncate(query.limit);
            }
            ResultSet::DistinctValues(vals)
        }
        QueryResult::Min => ResultSet::Min(numeric_extremum(
            &filtered,
            query.aggregate_field.as_deref(),
            true,
        )),
        QueryResult::Max => ResultSet::Max(numeric_extremum(
            &filtered,
            query.aggregate_field.as_deref(),
            false,
        )),
    }
}

fn matches_event_types(r: &DecodedRecord, event_types: &[String]) -> bool {
    event_types.is_empty() || event_types.iter().any(|e| e == &r.event_type)
}

fn matches_seq_range(r: &DecodedRecord, from: Option<u64>, to: Option<u64>) -> bool {
    from.is_none_or(|f| r.seq >= f) && to.is_none_or(|t| r.seq <= t)
}

fn matches_metadata(r: &DecodedRecord, filters: &[MetadataFilter]) -> bool {
    filters
        .iter()
        .all(|f| r.metadata.get(&f.key).is_some_and(|v| v == &f.value))
}

fn sort_and_limit(rows: &mut Vec<DecodedRecord>, descending: bool, limit: usize) {
    if descending {
        rows.sort_by_key(|r| std::cmp::Reverse(r.seq));
    } else {
        rows.sort_by_key(|r| r.seq);
    }
    if limit > 0 && rows.len() > limit {
        rows.truncate(limit);
    }
}

/// Distinct string values of `field` across `records`. If `field` is None, uses
/// `seq` (stringified). Records lacking the field are skipped.
fn distinct_field_values(records: &[&DecodedRecord], field: Option<&str>) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for r in records {
        let v = match field {
            Some(f) => match r.metadata.get(f) {
                Some(val) => val.clone(),
                None => continue,
            },
            None => r.seq.to_string(),
        };
        if seen.insert(v.clone()) {
            out.push(v);
        }
    }
    out
}

/// Min (is_min=true) or Max of a numeric field. If `field` is None, uses `seq`.
/// Records lacking the field or with a non-numeric value are skipped.
/// Returns 0 if none qualify (mirrors SQL `COALESCE(MIN/MAX(..), 0)`).
fn numeric_extremum(records: &[&DecodedRecord], field: Option<&str>, is_min: bool) -> u64 {
    let mut best: Option<u64> = None;
    for r in records {
        let n = match field {
            Some(f) => match r.metadata.get(f) {
                Some(v) => match v.parse::<u64>() {
                    Ok(n) => n,
                    Err(_) => continue,
                },
                None => continue,
            },
            None => r.seq,
        };
        best = Some(match best {
            None => n,
            Some(b) => {
                if is_min {
                    b.min(n)
                } else {
                    b.max(n)
                }
            }
        });
    }
    best.unwrap_or(0)
}

/// Anti-join (NOT EXISTS): keep `candidates` whose `join_key` value does NOT
/// appear on any record in `all_records` matching `peer_event_types`.
///
/// A candidate lacking the `join_key` is kept (no peer can match a missing key).
fn apply_absent<'a>(
    candidates: &[&'a DecodedRecord],
    all_records: &[DecodedRecord],
    absent: Option<&AbsentMatch>,
) -> Vec<&'a DecodedRecord> {
    let Some(absent) = absent else {
        return candidates.to_vec();
    };
    // Set of join_key values that have at least one matching peer.
    let peer_keys: HashSet<String> = all_records
        .iter()
        .filter(|r| absent.peer_event_types.iter().any(|e| e == &r.event_type))
        .filter_map(|r| r.metadata.get(&absent.join_key).cloned())
        .collect();
    candidates
        .iter()
        .copied()
        .filter(|r| match r.metadata.get(&absent.join_key) {
            Some(val) => !peer_keys.contains(val),
            None => true,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Build a DecodedRecord with given seq, event_type, and metadata pairs.
    fn rec(seq: u64, event_type: &str, meta: &[(&str, &str)]) -> DecodedRecord {
        let mut metadata = BTreeMap::new();
        for (k, v) in meta {
            metadata.insert((*k).to_string(), (*v).to_string());
        }
        DecodedRecord {
            namespace_id: 1,
            stream_id: 1,
            seq,
            event_type: event_type.to_string(),
            content_type: "application/json".to_string(),
            metadata,
            timestamp_ns: seq * 1_000_000_000,
            user_content: format!("c-{}", seq).into_bytes(),
        }
    }

    fn records() -> Vec<DecodedRecord> {
        vec![
            rec(1, "turn_started", &[("turn_id", "10")]),
            rec(2, "llm_invoked", &[("turn_id", "10"), ("step_id", "s1")]),
            rec(3, "turn_started", &[("turn_id", "20")]),
            rec(4, "tool_invoked", &[("turn_id", "20"), ("step_id", "s2")]),
        ]
    }

    #[test]
    fn filter_by_event_type() {
        let q = Query {
            event_types: vec!["turn_started".into()],
            result: QueryResult::Records,
            ..Default::default()
        };
        match execute(&q, &records()) {
            ResultSet::Records(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].seq, 1);
                assert_eq!(rows[1].seq, 3);
            }
            _ => panic!("expected Records"),
        }
    }

    #[test]
    fn filter_by_metadata_equality() {
        let q = Query {
            metadata: vec![MetadataFilter {
                key: "turn_id".into(),
                value: "20".into(),
            }],
            result: QueryResult::Records,
            ..Default::default()
        };
        match execute(&q, &records()) {
            ResultSet::Records(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![3, 4]);
            }
            _ => panic!("expected Records"),
        }
    }

    #[test]
    fn filter_by_seq_range() {
        let q = Query {
            from_seq: Some(2),
            to_seq: Some(3),
            result: QueryResult::Records,
            ..Default::default()
        };
        match execute(&q, &records()) {
            ResultSet::Records(rows) => assert_eq!(rows.len(), 2),
            _ => panic!("expected Records"),
        }
    }

    #[test]
    fn records_default_ascending_with_limit() {
        let q = Query {
            limit: 2,
            result: QueryResult::Records,
            ..Default::default()
        };
        match execute(&q, &records()) {
            ResultSet::Records(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].seq, 1); // ascending
            }
            _ => panic!("expected Records"),
        }
    }

    #[test]
    fn records_descending() {
        let q = Query {
            descending: true,
            result: QueryResult::Records,
            ..Default::default()
        };
        match execute(&q, &records()) {
            ResultSet::Records(rows) => assert_eq!(rows[0].seq, 4),
            _ => panic!("expected Records"),
        }
    }

    #[test]
    fn count_filtered() {
        let q = Query {
            event_types: vec!["turn_started".into()],
            result: QueryResult::Count,
            ..Default::default()
        };
        assert!(matches!(execute(&q, &records()), ResultSet::Count(2)));
    }

    #[test]
    fn exists_true_and_false() {
        let yes = Query {
            event_types: vec!["tool_invoked".into()],
            result: QueryResult::Exists,
            ..Default::default()
        };
        assert!(matches!(execute(&yes, &records()), ResultSet::Exists(true)));
        let no = Query {
            event_types: vec!["nope".into()],
            result: QueryResult::Exists,
            ..Default::default()
        };
        assert!(matches!(execute(&no, &records()), ResultSet::Exists(false)));
    }

    #[test]
    fn count_distinct_metadata_field() {
        // turn_id values among turn_started records: "10", "20" → 2 distinct
        let q = Query {
            event_types: vec!["turn_started".into()],
            result: QueryResult::CountDistinct,
            aggregate_field: Some("turn_id".into()),
            ..Default::default()
        };
        assert!(matches!(
            execute(&q, &records()),
            ResultSet::CountDistinct(2)
        ));
    }

    #[test]
    fn count_distinct_skips_records_lacking_field() {
        // distinct step_id across ALL records: "s1","s2" (turn_started lack it) → 2
        let q = Query {
            result: QueryResult::CountDistinct,
            aggregate_field: Some("step_id".into()),
            ..Default::default()
        };
        assert!(matches!(
            execute(&q, &records()),
            ResultSet::CountDistinct(2)
        ));
    }

    #[test]
    fn max_of_numeric_metadata_field() {
        let q = Query {
            result: QueryResult::Max,
            aggregate_field: Some("turn_id".into()),
            ..Default::default()
        };
        assert!(matches!(execute(&q, &records()), ResultSet::Max(20)));
    }

    #[test]
    fn min_of_numeric_metadata_field() {
        let q = Query {
            result: QueryResult::Min,
            aggregate_field: Some("turn_id".into()),
            ..Default::default()
        };
        assert!(matches!(execute(&q, &records()), ResultSet::Min(10)));
    }

    #[test]
    fn max_with_no_matching_field_is_zero() {
        let q = Query {
            result: QueryResult::Max,
            aggregate_field: Some("nonexistent".into()),
            ..Default::default()
        };
        assert!(matches!(execute(&q, &records()), ResultSet::Max(0)));
    }

    #[test]
    fn max_of_seq_when_no_aggregate_field() {
        let q = Query {
            result: QueryResult::Max,
            ..Default::default()
        };
        assert!(matches!(execute(&q, &records()), ResultSet::Max(4)));
    }

    #[test]
    fn distinct_values_metadata_field() {
        let q = Query {
            result: QueryResult::DistinctValues,
            aggregate_field: Some("turn_id".into()),
            ..Default::default()
        };
        match execute(&q, &records()) {
            ResultSet::DistinctValues(vals) => {
                assert_eq!(vals, vec!["10".to_string(), "20".to_string()])
            }
            _ => panic!("expected DistinctValues"),
        }
    }

    #[test]
    fn absent_match_returns_records_without_a_peer() {
        // turn_started records whose turn_id has NO turn_completed/failed/...
        // Here NO records are "completed", so BOTH turn_started (turn_id 10, 20) qualify.
        let q = Query {
            event_types: vec!["turn_started".into()],
            result: QueryResult::Records,
            absent: Some(AbsentMatch {
                peer_event_types: vec!["turn_completed".into(), "turn_failed".into()],
                join_key: "turn_id".into(),
            }),
            ..Default::default()
        };
        match execute(&q, &records()) {
            ResultSet::Records(rows) => {
                let mut seqs: Vec<u64> = rows.iter().map(|r| r.seq).collect();
                seqs.sort();
                assert_eq!(seqs, vec![1, 3]);
            }
            _ => panic!("expected Records"),
        }
    }

    #[test]
    fn absent_match_excludes_records_with_a_peer() {
        // Add a turn_completed for turn_id=10 → seq-1 turn_started is now complete.
        let mut recs = records();
        recs.push(rec(5, "turn_completed", &[("turn_id", "10")]));
        let q = Query {
            event_types: vec!["turn_started".into()],
            result: QueryResult::Records,
            absent: Some(AbsentMatch {
                peer_event_types: vec!["turn_completed".into()],
                join_key: "turn_id".into(),
            }),
            ..Default::default()
        };
        match execute(&q, &recs) {
            ResultSet::Records(rows) => {
                // Only turn_id=20 (seq 3) remains; turn_id=10 (seq 1) has a peer.
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].seq, 3);
            }
            _ => panic!("expected Records"),
        }
    }

    #[test]
    fn absent_match_count_incomplete() {
        let q = Query {
            event_types: vec!["turn_started".into()],
            result: QueryResult::Count,
            absent: Some(AbsentMatch {
                peer_event_types: vec!["turn_completed".into()],
                join_key: "turn_id".into(),
            }),
            ..Default::default()
        };
        // No completions → both turn_started are incomplete.
        assert!(matches!(execute(&q, &records()), ResultSet::Count(2)));
    }

    #[test]
    fn absent_match_records_lacking_join_key_are_kept() {
        // A turn_started with no turn_id at all is kept (no peer can match its key).
        let mut recs = records();
        recs.push(rec(6, "turn_started", &[])); // no turn_id
        let q = Query {
            event_types: vec!["turn_started".into()],
            result: QueryResult::Count,
            absent: Some(AbsentMatch {
                peer_event_types: vec!["turn_completed".into()],
                join_key: "turn_id".into(),
            }),
            ..Default::default()
        };
        assert!(matches!(execute(&q, &recs), ResultSet::Count(3)));
    }
}
