//! Native structured query engine — filter + aggregation + anti-join over a
//! single stream's records. Reads the log directly (Phase 3 wires this onto the
//! Query RPC, replacing the SQLite cache).

use crate::record::DecodedRecord;

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

    // 2. Anti-join (Task 3 fills this in; for now a no-op pass-through).
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
        // Aggregations land in Task 2.
        QueryResult::CountDistinct
        | QueryResult::Min
        | QueryResult::Max
        | QueryResult::DistinctValues => {
            unimplemented!("aggregations added in Task 2")
        }
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

/// Anti-join. Task 1 returns the candidates unchanged when `absent` is None;
/// Task 3 implements the real NOT EXISTS semantics.
fn apply_absent<'a>(
    candidates: &[&'a DecodedRecord],
    _all_records: &[DecodedRecord],
    absent: Option<&AbsentMatch>,
) -> Vec<&'a DecodedRecord> {
    match absent {
        Some(_) => unimplemented!("anti-join added in Task 3"),
        None => candidates.to_vec(),
    }
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
}
