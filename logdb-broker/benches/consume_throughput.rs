//! Micro-benchmarks for the broker's record-forwarding overhead.
//!
//! Benchmarks `forward_stream` (the pure core — logdbd Record → broker Record
//! mapping + shard-id stamp + channel send) using a fake stream of
//! `TailResponse` batches. Real per-shard Tail + gRPC serialisation add a
//! roughly-constant factor on top; this bench captures the broker-internal
//! per-record cost.

use std::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use logdb_broker::forwarder::forward_stream;
use logdb_broker_proto::pb::ConsumeResponse;
use logdbd_proto::pb::{Record, TailResponse};
use tonic::Status;

fn rec(seq: u64) -> Record {
    Record {
        namespace_id: 1,
        stream_id: 1,
        seq,
        event_type: "bench.event".into(),
        timestamp_ns: seq * 1_000_000_000,
        content_type: "application/json".into(),
        metadata: Default::default(),
        content: format!("record-{seq:0>8}").into_bytes(),
    }
}

fn tail_batch(records: Vec<Record>) -> Result<TailResponse, Status> {
    Ok(TailResponse {
        records,
        durable_seq: 0,
        heartbeat: false,
    })
}

/// Feed N records in batches of B through `forward_stream` and drain the
/// output channel. Returns the wall-clock time.
fn forward_and_drain(rt: &Runtime, total: u64, batch: u64) -> Duration {
    let (tx, mut rx) = mpsc::channel::<Result<ConsumeResponse, Status>>(256);
    let shard_id = 3u32; // constant — realistic (one per-shard Tail)

    // Build the input stream synchronously (overhead excluded from bench).
    let batches: Vec<_> = (0..total)
        .step_by(batch as usize)
        .map(|start| {
            let end = (start + batch).min(total);
            tail_batch((start..end).map(rec).collect())
        })
        .collect();

    let start = std::time::Instant::now();
    rt.block_on(async {
        let stream = tokio_stream::iter(batches);
        forward_stream(stream, shard_id, tx).await;
    });
    // Drain the output channel (sync, after forward_stream finished).
    while let Ok(msg) = rx.try_recv() {
        // discard — the benchmark is measuring forward time, not drain time.
        let _ = black_box(msg);
    }
    start.elapsed()
}

fn bench_forward_stream(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("forward_stream");
    for &(total, batch) in &[(10_000u64, 100u64), (100_000, 100), (100_000, 500)] {
        let desc = format!("{total}rec,batch{batch}");
        group.throughput(Throughput::Elements(total));
        group.bench_function(desc, |b| {
            b.iter_with_setup(
                || (),
                |_| {
                    forward_and_drain(&rt, black_box(total), black_box(batch));
                },
            )
        });
    }
    group.finish();
}

criterion_group!(benches, bench_forward_stream);
criterion_main!(benches);
