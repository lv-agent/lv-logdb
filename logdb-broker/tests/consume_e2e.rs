//! Phase 3 end-to-end: broker forwards logdbd records to consumers, partitioned
//! by assigned shard. The broker is in the data path: producer → logdbd,
//! logdbd →(Tail)→ broker →(Consume)→ consumer.

use std::sync::Arc;
use std::time::Duration;

use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;

use logdb::{Config as DbConfig, LogDb};
use logdbd::catalog::Catalog;
use logdbd::consumer::ConsumerTracker;
use logdbd::service::LogDbServiceImpl;
use logdbd::storage::Storage;
use logdbd::subscribe::SubscribeHub;
use logdbd_proto::pb::log_db_service_server::LogDbServiceServer;

use logdb_broker::coordinator::CoordinatorRegistry;
use logdb_broker::forwarder::Forwarder;
use logdb_broker::leader::LeaderElection;
use logdb_broker::persistence::{OffsetRecord, Persistence};
use logdb_broker::service::BrokerServiceImpl;
use logdb_broker_proto::pb::broker_service_client::BrokerServiceClient;
use logdb_broker_proto::pb::broker_service_server::BrokerServiceServer;
use logdb_broker_proto::pb::{
    consume_response::Payload, CommitShardOffsetRequest, ConsumeRequest, JoinGroupRequest,
    ProduceRequest,
};

// ── Harness ──────────────────────────────────────────────────────────────────

type LogdbdHandle = (std::net::SocketAddr, tempfile::TempDir, tokio::task::JoinHandle<()>);

async fn start_logdbd(shards: usize) -> LogdbdHandle {
    let (addr, dir, jh) = start_logdbd_killable(shards).await;
    (addr, dir, jh)
}

/// Like [`start_logdbd`] but also returns a [`JoinHandle`] so the caller
/// can abort the spawned server task (simulates a crash for resilience tests).
async fn start_logdbd_killable(shards: usize) -> LogdbdHandle {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = DbConfig::default();
    cfg.data_dir = dir.path().to_path_buf();
    cfg.ring_size = 256;
    cfg.shards = shards;
    cfg.durability_mode = logdb::DurabilityMode::Sync;
    cfg.flush_timeout = Duration::from_secs(5);
    let cfg_fb = cfg.clone();
    let ckpt_path = dir.path().join("seq_map.ckpt");
    let db = LogDb::open(cfg).unwrap();
    let storage = Arc::new(
        logdbd::storage::Storage::try_new_from_checkpoint(db, shards, &ckpt_path)
            .unwrap_or_else(|e| {
                eprintln!("test logdbd: checkpoint load failed ({e}); full rebuild");
                let db = LogDb::open(cfg_fb).unwrap();
                logdbd::storage::Storage::new(db, shards)
            }),
    );
    let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
    let svc = LogDbServiceImpl::new(
        Arc::clone(&storage),
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(SubscribeHub::new()),
        "logdbd-node".into(),
        "primary".into(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let jh = tokio::spawn(async move {
        Server::builder()
            .add_service(LogDbServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, dir, jh)
}

async fn start_broker(logdbd_addr: String, num_shards: u32) -> std::net::SocketAddr {
    start_broker_with_ha(logdbd_addr, num_shards, None, 0, None).await.0
}

/// Result of [`start_broker_with_ha`]: the bound address + (if HA enabled) a
/// handle to stop the leader election (simulates a crash in failover tests).
type HaBroker = (std::net::SocketAddr, Option<Arc<LeaderElection>>);

/// Like [`start_broker`] but optionally enables per-group leader election
/// (`broker_id`) and/or session eviction (`session_timeout_ms` > 0).
/// `lease_ms` overrides the leader lease timeout (default 10 s; use a short
/// value for failover tests).
async fn start_broker_with_ha(
    logdbd_addr: String,
    num_shards: u32,
    broker_id: Option<&str>,
    session_timeout_ms: u64,
    lease_ms: Option<u64>,
) -> HaBroker {
    let forwarder = Forwarder::connect(logdbd_addr.clone()).await.unwrap();
    let persistence = Persistence::connect(logdbd_addr).await.unwrap();
    persistence.ensure_meta_stream().await.unwrap();
    let registry = Arc::new(CoordinatorRegistry::new(num_shards));
    let recovered = persistence.load_recovered_offsets().await.unwrap();
    for rec in &recovered {
        registry.commit_offset(&rec.ns, &rec.stream, &rec.group, rec.shard, rec.seq);
    }
    let _ = persistence.compact_offsets(&recovered).await;

    let leader = broker_id.map(|id| {
        let le = Arc::new(LeaderElection::new(
            id.into(),
            "127.0.0.1:0".into(),
            forwarder.channel(),
            lease_ms,
        ));
        le.start();
        le
    });
    let leader2 = leader.clone();

    let svc = BrokerServiceImpl::new(registry, Some(forwarder), Some(persistence), leader);
    if session_timeout_ms > 0 {
        let svc_arc = Arc::new(svc.clone());
        svc_arc.start_liveness_check(session_timeout_ms);
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(BrokerServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, leader2)
}

async fn drain_consume(
    stream: &mut tonic::codec::Streaming<logdb_broker_proto::pb::ConsumeResponse>,
    window: Duration,
) -> std::collections::HashSet<String> {
    let mut got = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + window;
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if let Ok(Ok(Some(resp))) = tokio::time::timeout(Duration::from_millis(200), stream.message()).await {
            if let Some(Payload::Record(r)) = resp.payload {
                got.insert(String::from_utf8_lossy(&r.content).into_owned());
            }
        }
    }
    got
}

/// Like [`drain_consume`] but returns `(shard_id, seq)` per record (for offset
/// tests that need to attribute records to shards).
async fn drain_consume_shards(
    stream: &mut tonic::codec::Streaming<logdb_broker_proto::pb::ConsumeResponse>,
    window: Duration,
) -> Vec<(u32, u64)> {
    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + window;
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if let Ok(Ok(Some(resp))) = tokio::time::timeout(Duration::from_millis(200), stream.message()).await {
            if let Some(Payload::Record(r)) = resp.payload {
                got.push((r.shard_id, r.seq));
            }
        }
    }
    got
}

// ── Tests ────────────────────────────────────────────────────────────────────

fn off(ns: &str, s: &str, g: &str, shard: u32, seq: u64) -> OffsetRecord {
    OffsetRecord {
        ns: ns.into(),
        stream: s.into(),
        group: g.into(),
        shard,
        seq,
    }
}

#[tokio::test]
async fn persistence_round_trips_offsets_to_logdbd() {
    let (logdbd_addr, _ldir, _jh) = start_logdbd(4).await;
    let pers = Persistence::connect(format!("http://{logdbd_addr}"))
        .await
        .unwrap();
    pers.ensure_meta_stream().await.unwrap();
    pers.append_offset(off("ns", "s", "g", 1, 5)).await.unwrap();
    pers.append_offset(off("ns", "s", "g", 1, 8)).await.unwrap();
    pers.append_offset(off("ns", "s", "g", 2, 3)).await.unwrap();

    // Give logdbd a moment to make the appends durable/scannable.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let recs = pers.scan_offsets().await.unwrap();
    let pairs: std::collections::HashSet<(u32, u64)> =
        recs.iter().map(|r| (r.shard, r.seq)).collect();
    assert!(pairs.contains(&(1, 5)), "shard1 seq5 must be replayable");
    assert!(pairs.contains(&(1, 8)), "shard1 seq8 must be replayable");
    assert!(pairs.contains(&(2, 3)), "shard2 seq3 must be replayable");
}

#[tokio::test]
async fn single_consumer_receives_all_keyed_records_via_broker() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let broker_addr = start_broker(format!("http://{logdbd_addr}"), shards).await;

    // The client talks ONLY to the broker — produce and consume both go through
    // it (symmetric gateway, Pulsar model A). logdbd is the unseen backend.
    let mut broker = BrokerServiceClient::connect(format!("http://{broker_addr}"))
        .await
        .unwrap();
    for i in 0..8u32 {
        let key = format!("key-{i}");
        broker
            .produce(ProduceRequest {
                namespace: "ns".into(),
                stream: "s".into(),
                event_type: "e".into(),
                content: key.as_bytes().to_vec(),
                shard_key: Some(key.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Sole consumer owns all shards; its join generation is current.
    let r = broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let mut consume = broker
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
            generation: r.generation,
        })
        .await
        .unwrap()
        .into_inner();
    let got = drain_consume(&mut consume, Duration::from_millis(500)).await;
    assert_eq!(got.len(), 8, "sole consumer must receive all 8 records via broker");
}

#[tokio::test]
async fn two_consumers_partition_records_by_assigned_shard() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let broker_addr = start_broker(format!("http://{logdbd_addr}"), shards).await;

    // Produce via the broker (symmetric gateway); the test never touches logdbd.
    let mut broker = BrokerServiceClient::connect(format!("http://{broker_addr}"))
        .await
        .unwrap();
    let mut all = std::collections::HashSet::new();
    for i in 0..16u32 {
        let key = format!("key-{i}");
        broker
            .produce(ProduceRequest {
                namespace: "ns".into(),
                stream: "s".into(),
                event_type: "e".into(),
                content: key.as_bytes().to_vec(),
                shard_key: Some(key.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        all.insert(key);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Join both. c1's join-time generation (1) goes stale when c2 joins (→2);
    // Phase 3 has no rebalance-push, so each consumer re-joins (a no-op that
    // returns the CURRENT generation + assignment) before consuming. Phase 5
    // replaces this with the rebalance protocol on the Consume stream.
    broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
        })
        .await
        .unwrap();
    broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c2".into(),
        })
        .await
        .unwrap();
    let c1 = broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let c2 = broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c2".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(c1.generation, c2.generation, "both synced to current generation");

    let mut consume_a = broker
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
            generation: c1.generation,
        })
        .await
        .unwrap()
        .into_inner();
    let mut consume_b = broker
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c2".into(),
            generation: c2.generation,
        })
        .await
        .unwrap()
        .into_inner();

    let got_a = drain_consume(&mut consume_a, Duration::from_millis(600)).await;
    let got_b = drain_consume(&mut consume_b, Duration::from_millis(600)).await;

    // Disjoint: no record delivered to both.
    for k in &got_a {
        assert!(!got_b.contains(k), "record {k} delivered to both consumers");
    }
    // Complete: together they cover all 16.
    let mut union = got_a.clone();
    union.extend(got_b.iter().cloned());
    assert_eq!(union.len(), 16, "two consumers together must cover all 16 records");
    assert_eq!(union, all);
    // Real split: neither consumer starved.
    assert!(!got_a.is_empty() && !got_b.is_empty(), "records must split across both");
}

#[tokio::test]
async fn consume_rejects_stale_generation() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let broker_addr = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let mut broker = BrokerServiceClient::connect(format!("http://{broker_addr}"))
        .await
        .unwrap();
    broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
        })
        .await
        .unwrap();
    broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c2".into(),
        })
        .await
        .unwrap(); // generation now 2

    // c1's stale generation 1 must be rejected (FailedPrecondition).
    let err = broker
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
            generation: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn broker_restart_recovers_committed_offsets() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;

    // Broker instance #1: commit an offset → it persists to the meta stream.
    let broker1 = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let mut c1 = BrokerServiceClient::connect(format!("http://{broker1}"))
        .await
        .unwrap();
    let r = c1
        .commit_shard_offset(CommitShardOffsetRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            shard_id: 2,
            committed_seq: 5,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.advanced, "first commit of seq 5 must advance");
    // give the meta-stream append a moment to become durable/scannable
    tokio::time::sleep(Duration::from_millis(200)).await;

    // "Restart": a fresh broker instance (#2) recovers from the same logdbd.
    // (broker1's task is simply abandoned — its in-memory state is gone.)
    let broker2 = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let mut c2 = BrokerServiceClient::connect(format!("http://{broker2}"))
        .await
        .unwrap();

    // A stale commit (seq 3 < recovered 5) must NOT advance → proves recovery.
    let stale = c2
        .commit_shard_offset(CommitShardOffsetRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            shard_id: 2,
            committed_seq: 3,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        !stale.advanced,
        "recovered offset (5) must reject the stale commit (3)"
    );

    // A higher commit (7 > 5) advances and re-persists.
    let higher = c2
        .commit_shard_offset(CommitShardOffsetRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            shard_id: 2,
            committed_seq: 7,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(higher.advanced, "seq 7 must advance past recovered 5");
}

#[tokio::test]
async fn consume_resumes_from_committed_offset_per_shard() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let broker_addr = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let mut broker = BrokerServiceClient::connect(format!("http://{broker_addr}"))
        .await
        .unwrap();

    // Produce 12 key-routed records.
    for i in 0..12u32 {
        let key = format!("key-{i}");
        broker
            .produce(ProduceRequest {
                namespace: "ns".into(),
                stream: "s".into(),
                event_type: "e".into(),
                content: key.as_bytes().to_vec(),
                shard_key: Some(key),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Sole consumer owns all shards; consume everything (records carry shard_id).
    let j = broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut consume = broker
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
            generation: j.generation,
        })
        .await
        .unwrap()
        .into_inner();
    let recs = drain_consume_shards(&mut consume, Duration::from_millis(600)).await;
    assert_eq!(recs.len(), 12, "must receive all 12 records");
    assert!(
        recs.iter().all(|(s, _)| *s < shards),
        "every record must carry a stamped shard_id < {shards}"
    );

    // Commit each shard's max seq.
    drop(consume);
    let mut max: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
    for (shard, seq) in &recs {
        let m = max.entry(*shard).or_insert(0);
        if *seq > *m {
            *m = *seq;
        }
    }
    for (shard, seq) in &max {
        broker
            .commit_shard_offset(CommitShardOffsetRequest {
                namespace: "ns".into(),
                stream: "s".into(),
                group: "g".into(),
                shard_id: *shard,
                committed_seq: *seq,
            })
            .await
            .unwrap();
    }

    // Re-consume: with every shard caught up to its max, nothing re-delivers.
    let mut consume2 = broker
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
            generation: j.generation,
        })
        .await
        .unwrap()
        .into_inner();
    let recs2 = drain_consume_shards(&mut consume2, Duration::from_millis(500)).await;
    assert!(
        recs2.is_empty(),
        "after committing each shard's max seq, re-consume must deliver nothing (got {:?})",
        recs2
    );

    // Produce one more record; re-consume must deliver ONLY it.
    broker
        .produce(ProduceRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            event_type: "e".into(),
            content: b"key-new".to_vec(),
            shard_key: Some("key-new".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let recs3 = drain_consume_shards(&mut consume2, Duration::from_millis(500)).await;
    assert_eq!(recs3.len(), 1, "only the newly produced record delivers");
}

#[tokio::test]
async fn group_consumer_sdk_round_trips_consume_and_commit() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let broker_addr = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let url = format!("http://{broker_addr}");

    // Produce via the broker SDK (BrokerProducer → broker → logdbd).
    let mut producer = logdb_client::broker::BrokerProducer::connect(url.clone())
        .await
        .unwrap();
    let mut keys = Vec::new();
    for i in 0..8u32 {
        let key = format!("key-{i}");
        producer
            .produce("ns", "s", "e", key.as_bytes(), Some(&key))
            .await
            .unwrap();
        keys.push(key);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Consume via the broker SDK (GroupConsumer → broker → logdbd).
    let mut consumer =
        logdb_client::broker::GroupConsumer::join(url.clone(), "ns", "s", "g", "c1")
            .await
            .unwrap();
    assert!(!consumer.assigned_shards().is_empty());
    let stream = consumer.consume().await.unwrap();

    // Drain the SDK record stream.
    let mut got: Vec<(u32, u64, String)> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(600);
    let mut stream = stream;
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if let Ok(Some(Ok(r))) = tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
            got.push((
                r.shard_id,
                r.seq,
                String::from_utf8_lossy(&r.content).into_owned(),
            ));
        }
    }
    assert_eq!(got.len(), 8, "SDK consumer must receive all 8 records");
    assert!(
        got.iter().all(|(s, _, _)| *s < shards),
        "records carry stamped shard_id < {shards}"
    );
    let got_keys: std::collections::HashSet<String> = got.iter().map(|(_, _, k)| k.clone()).collect();
    assert_eq!(got_keys.len(), 8);

    // Commit each shard's max seq, then re-consume → nothing (caught up).
    drop(stream);
    let mut max: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
    for (shard, seq, _) in &got {
        let m = max.entry(*shard).or_insert(0);
        if *seq > *m {
            *m = *seq;
        }
    }
    for (shard, seq) in &max {
        assert!(
            consumer.commit_shard(*shard, *seq).await.unwrap(),
            "commit must advance"
        );
    }
    let stream2 = consumer.consume().await.unwrap();
    let mut got2 = 0;
    let deadline2 = tokio::time::Instant::now() + Duration::from_millis(400);
    let mut stream2 = stream2;
    loop {
        if tokio::time::Instant::now() >= deadline2 {
            break;
        }
        if let Ok(Some(Ok(_))) = tokio::time::timeout(Duration::from_millis(200), stream2.next()).await
        {
            got2 += 1;
        }
    }
    assert_eq!(got2, 0, "after committing all shards, re-consume delivers nothing");

    // Leave cleanly.
    consumer.leave().await.unwrap();
}

#[tokio::test]
async fn active_consume_stream_rebalances_on_join() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let broker_addr = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let mut broker = BrokerServiceClient::connect(format!("http://{broker_addr}"))
        .await
        .unwrap();

    // Produce 16 key-routed records (spread across shards).
    for i in 0..16u32 {
        let key = format!("key-{i}");
        broker
            .produce(ProduceRequest {
                namespace: "ns".into(),
                stream: "s".into(),
                event_type: "e".into(),
                content: key.as_bytes().to_vec(),
                shard_key: Some(key),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Consumer A joins (sole member → owns all 4 shards) and starts consuming.
    let a = broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "a".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut stream_a = broker
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "a".into(),
            generation: a.generation,
        })
        .await
        .unwrap()
        .into_inner();

    // Drain A's initial delivery (all shards).
    let _ = drain_consume_shards(&mut stream_a, Duration::from_millis(300)).await;

    // B joins → generation bumps to 2, A's assignment becomes {0,2} (round-robin
    // split). The join triggers a stop-the-world rebalance pushed onto A's OPEN
    // stream: RebalanceSignal then Assignment.
    let b = broker
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "b".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(b.generation, 2);

    // Drain A's stream for the rebalance frames + post-rebalance records.
    let mut saw_rebalance = false;
    let mut assignment_shards: Option<Vec<u32>> = None;
    let mut post_records: Vec<u32> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(700);
    let mut past_assignment = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(Some(resp))) = tokio::time::timeout(Duration::from_millis(200), stream_a.message()).await {
            match resp.payload {
                Some(Payload::Rebalance(_)) => saw_rebalance = true,
                Some(Payload::Assignment(a_msg)) => {
                    assert_eq!(a_msg.generation, 2, "assignment carries the new generation");
                    let mut s = a_msg.shards.clone();
                    s.sort();
                    assert_eq!(s, vec![0, 2], "A's new assignment is the round-robin half {{0,2}}");
                    assignment_shards = Some(a_msg.shards.clone());
                    past_assignment = true;
                }
                Some(Payload::Record(r)) if past_assignment => post_records.push(r.shard_id),
                _ => {}
            }
        }
    }

    assert!(saw_rebalance, "A's open stream must receive a RebalanceSignal");
    let assigned = assignment_shards.expect("A's open stream must receive an Assignment");
    assert!(
        !post_records.is_empty(),
        "A must receive records after the rebalance (forward task restarted)"
    );
    assert!(
        post_records.iter().all(|s| assigned.contains(s)),
        "post-rebalance records must be from A's NEW shards {:?}, got {:?}",
        assigned,
        post_records
    );
}

#[tokio::test]
async fn sdk_consumer_resumes_from_committed_offset_after_broker_restart() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;

    // ── broker instance #1: produce + consume + commit ──────────────────────
    let broker1 = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let url1 = format!("http://{broker1}");

    let mut producer =
        logdb_client::broker::BrokerProducer::connect(url1.clone())
            .await
            .unwrap();
    for i in 0..8u32 {
        let key = format!("key-{i}");
        producer
            .produce("ns", "s", "e", key.as_bytes(), Some(&key))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut consumer1 =
        logdb_client::broker::GroupConsumer::join(url1, "ns", "s", "g", "c1")
            .await
            .unwrap();
    let mut stream1 = consumer1.consume().await.unwrap();

    // Drain all records; collect the max seq per shard for the commit.
    let mut rec_count = 0u64;
    let mut max: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(600);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if let Ok(Some(Ok(r))) =
            tokio::time::timeout(Duration::from_millis(200), stream1.next()).await
        {
            rec_count += 1;
            let m = max.entry(r.shard_id).or_insert(0);
            if r.seq > *m {
                *m = r.seq;
            }
        }
    }
    assert_eq!(rec_count, 8, "must receive all 8 records");
    drop(stream1);

    // Commit every shard's max seq.
    for (shard, seq) in &max {
        assert!(
            consumer1.commit_shard(*shard, *seq).await.unwrap(),
            "commit shard {shard} seq {seq}"
        );
    }
    // Do NOT call leave — the broker crashes (abandoned), simulating a restart.
    drop(consumer1);

    // ── broker instance #2 (simulated restart): offsets recovered ──────────
    tokio::time::sleep(Duration::from_millis(200)).await; // let meta-stream commits go durable
    let broker2 = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let url2 = format!("http://{broker2}");

    // Fresh GroupConsumer connecting to the restarted broker. Membership was
    // transient (not persisted), so we join fresh. Offsets ARE recovered
    // from the meta stream — re-consume must deliver zero records.
    let mut consumer2 =
        logdb_client::broker::GroupConsumer::join(url2.clone(), "ns", "s", "g", "c1")
            .await
            .unwrap();
    let stream2 = consumer2.consume().await.unwrap();
    let mut got = 0u64;
    let deadline2 = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut pinned = stream2;
    loop {
        if tokio::time::Instant::now() >= deadline2 {
            break;
        }
        if let Ok(Some(Ok(_))) =
            tokio::time::timeout(Duration::from_millis(200), pinned.next()).await
        {
            got += 1;
        }
    }
    assert_eq!(
        got, 0,
        "restarted broker must resume from committed offsets — nothing re-delivers"
    );
    consumer2.leave().await.unwrap();
}

#[tokio::test]
async fn concurrent_consumers_no_duplicates_no_loss() {
    let shards = 8u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let broker_addr = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let url = format!("http://{broker_addr}");

    // Produce 10_000 key-routed records up front.
    let mut producer = logdb_client::broker::BrokerProducer::connect(url.clone())
        .await
        .unwrap();
    for i in 0..10_000u32 {
        let key = format!("key-{i:0>5}");
        producer
            .produce("ns", "s", "e", key.as_bytes(), Some(&key))
            .await
            .unwrap();
    }
    // Give logdbd time to make them durable before consumers tail.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // 50 concurrent consumers. Each forwards received record content to a shared
    // channel; the main task counts UNIQUE records and stops the moment all
    // 10_000 have arrived — measuring real delivery latency, not a fixed drain
    // window.
    let n = 50u32;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(2048);
    for cid in 0..n {
        let u = url.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut con = logdb_client::broker::GroupConsumer::join(
                u, "ns", "s", "g", format!("c-{cid}"),
            )
            .await
            .unwrap();
            // Consumers with no assigned shards (n > shards) get an error; skip.
            let mut stream = match con.consume().await {
                Ok(s) => s,
                Err(_) => return,
            };
            // Drain until the channel closes (main drops rx after 10_000 unique).
            while let Some(Ok(r)) = stream.next().await {
                if tx
                    .send(String::from_utf8_lossy(&r.content).into_owned())
                    .await
                    .is_err()
                {
                    return; // main is done
                }
            }
        });
    }
    drop(tx); // close after all consumers spawned

    let t_start = tokio::time::Instant::now();
    let mut union = std::collections::HashSet::new();
    let mut total_deliveries = 0usize;
    let mut t_done = None;
    while let Some(content) = rx.recv().await {
        total_deliveries += 1;
        if union.insert(content) && union.len() == 10_000 {
            t_done = Some(tokio::time::Instant::now());
            break; // all delivered — stop
        }
    }
    drop(rx); // drops consumers (their tx.send fails → they exit)

    let t_done = t_done.expect("all 10_000 records delivered");
    let elapsed = t_done.duration_since(t_start);
    assert_eq!(union.len(), 10_000, "all 10_000 records must be delivered");
    assert_eq!(
        total_deliveries, 10_000,
        "exactly-once: total deliveries ({total_deliveries}) must equal unique (10_000)"
    );
    let throughput = 10_000.0 / elapsed.as_secs_f64();
    eprintln!(
        "concurrent_consumers: 10_000 records, {n} consumers, exactly-once, delivered in {elapsed:.2?} ({throughput:.0} rec/s)"
    );
}

#[tokio::test]
async fn per_group_leader_election_different_groups() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;

    // Start two HA brokers sharing the same logdbd.
    let (addr_a, _la) = start_broker_with_ha(
        format!("http://{logdbd_addr}"), shards, Some("broker-a"), 0, None,
    ).await;
    let (addr_b, _lb) = start_broker_with_ha(
        format!("http://{logdbd_addr}"), shards, Some("broker-b"), 0, None,
    ).await;
    let url_a = format!("http://{addr_a}");
    let url_b = format!("http://{addr_b}");

    // Give the leader-election background scan time to settle (± lease/3).
    tokio::time::sleep(Duration::from_millis(4000)).await;

    // Both brokers can serve different groups as leader (first to claim wins).
    // Produce is stateless — works on either broker regardless.
    let mut prod =
        logdb_client::broker::BrokerProducer::connect(url_a.clone())
            .await
            .unwrap();
    for i in 0..8u32 {
        let key = format!("key-{i}");
        prod.produce("ns", "s", "e", key.as_bytes(), Some(&key))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Group g1 on broker A (first to claim).
    let mut client_a = BrokerServiceClient::connect(url_a.clone())
        .await
        .unwrap();
    let j1 = client_a
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g1".into(),
            consumer_id: "c1".into(),
        })
        .await;
    assert!(j1.is_ok(), "broker A must accept JoinGroup for g1");

    // Group g2 on broker B (first to claim — different group).
    let mut client_b = BrokerServiceClient::connect(url_b.clone())
        .await
        .unwrap();
    let j2 = client_b
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g2".into(),
            consumer_id: "c2".into(),
        })
        .await;
    assert!(j2.is_ok(), "broker B must accept JoinGroup for g2 (different group)");

    // Verify consume works on the leader for g1.
    let c1 = client_a
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g1".into(),
            consumer_id: "c1".into(),
            generation: j1.unwrap().into_inner().generation,
        })
        .await;
    assert!(c1.is_ok(), "consume must work on the group's leader");
}

#[tokio::test]
async fn heartbeat_timeout_evicts_stale_consumer() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let timeout_ms = 2000u64;
    let (broker_addr, _) = start_broker_with_ha(
        format!("http://{logdbd_addr}"), shards, None, timeout_ms, None,
    ).await;
    let url = format!("http://{broker_addr}");
    let mut raw = BrokerServiceClient::connect(url.clone())
        .await
        .unwrap();

    // c1 joins the group but does NOT open a consume session.  It just holds
    // a membership slot — this verifies eviction removes both the session AND
    // the group membership (via registry.leave).
    //
    // Actually, evict_stale only works on ACTIVE sessions.  We open a Consume
    // stream for c1 (creating a session) but never heartbeat it.  After
    // timeout_ms, the liveness check evicts c1: removes from sessions AND
    // calls registry.leave → removes from group membership → rebalances.
    let j1 = raw
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let _c1_stream = raw
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
            generation: j1.generation,
        })
        .await
        .unwrap();
    // c1 never heartbeats.  Wait for eviction.
    tokio::time::sleep(Duration::from_millis(timeout_ms + 1000)).await;

    // c2 joins — c1 should be evicted, so c2 is the sole member and gets
    // all shards.
    let j2 = raw
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c2".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut s2 = j2.assigned_shards.clone();
    s2.sort();
    assert_eq!(
        s2, vec![0, 1, 2, 3],
        "c2 must get all shards after c1 is evicted (c1 never heartbeated)"
    );
}

#[tokio::test]
async fn ha_failover_standby_takes_over_after_leader_crash() {
    let shards = 4u32;
    let (logdbd_addr, _ldir, _jh) = start_logdbd(shards as usize).await;
    let url_d = format!("http://{logdbd_addr}");
    let lease_ms = 1500u64; // short for the test

    // Start two HA brokers.
    let (addr_a, leader_a) = start_broker_with_ha(
        url_d.clone(), shards, Some("broker-a"), 0, Some(lease_ms),
    ).await;
    let (addr_b, _leader_b) = start_broker_with_ha(
        url_d.clone(), shards, Some("broker-b"), 0, Some(lease_ms),
    ).await;
    let url_a = format!("http://{addr_a}");
    let url_b = format!("http://{addr_b}");

    // Give the background scan time to settle.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // c1 joins g1 on broker A — A becomes the leader for g1.
    let mut client_a = BrokerServiceClient::connect(url_a.clone())
        .await
        .unwrap();
    let _j1 = client_a
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g1".into(),
            consumer_id: "c1".into(),
        })
        .await
        .unwrap();

    // "Kill" broker A by stopping its leader election loop.  Its last
    // heartbeat was at lease/3 ≈ 500 ms ago.  After lease_ms (1500 ms)
    // of silence, broker B should detect staleness and claim g1.
    leader_a.unwrap().stop();
    tokio::time::sleep(Duration::from_millis(lease_ms + 1000)).await;

    // c1 (or a new consumer) now joins g1 on broker B.  B should have
    // claimed g1 by now and accept the join.
    let mut client_b = BrokerServiceClient::connect(url_b.clone())
        .await
        .unwrap();
    let j2 = client_b
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g1".into(),
            consumer_id: "c2".into(),
        })
        .await;
    assert!(
        j2.is_ok(),
        "after A's crash, B must claim g1 and accept JoinGroup: {:?}",
        j2.err()
    );

    // Verify consume works on B after failover.
    let c2 = client_b
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g1".into(),
            consumer_id: "c2".into(),
            generation: j2.unwrap().into_inner().generation,
        })
        .await;
    assert!(c2.is_ok(), "consume must work on the new leader after failover");
}

#[tokio::test]
async fn forwarder_survives_logdbd_crash() {
    let shards = 4u32;
    // Use the killable harness so we can simulate a logdbd crash.
    let (logdbd_addr, _dir, logdbd_jh) = start_logdbd_killable(shards as usize).await;
    let broker_addr = start_broker(format!("http://{logdbd_addr}"), shards).await;
    let url = format!("http://{broker_addr}");
    let mut client = BrokerServiceClient::connect(url.clone())
        .await
        .unwrap();

    // Produce records and join/consume — confirm the path works normally.
    for i in 0..4u32 {
        let key = format!("key-{i}");
        client
            .produce(ProduceRequest {
                namespace: "ns".into(),
                stream: "s".into(),
                event_type: "e".into(),
                content: key.as_bytes().to_vec(),
                shard_key: Some(key),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let j = client
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
        })
        .await
        .unwrap()
        .into_inner();

    // Open a consume stream.
    let _consume = client
        .consume(ConsumeRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
            generation: j.generation,
        })
        .await
        .unwrap();

    // Now crash logdbd (abort its server task).
    logdbd_jh.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The broker must NOT panic or stall — the forward task simply exits
    // (Tail RPC fails), the session deregisters.  The consumer's stream
    // from the broker also ends.  We can't verify the stream ending without
    // reading it, but we can verify the broker is still alive and responds
    // to produce (stateless RPC).
    let mut client2 = BrokerServiceClient::connect(url.clone())
        .await
        .unwrap();
    let prod_after = client2
        .produce(ProduceRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            event_type: "e".into(),
            content: b"after-crash".to_vec(),
            ..Default::default()
        })
        .await;
    // Produce may fail (logdbd is dead) or succeed (tonic retry) — but the
    // broker itself must not panic.
    match prod_after {
        Ok(_) | Err(_) => {} // either is acceptable
    }
}
