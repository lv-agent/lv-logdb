//! Integration tests for logdb-client SDK against a real logdbd server.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use logdb_client::Client;
use logdbd::catalog::Catalog;
use logdbd::consumer::ConsumerTracker;
use logdbd::pb::log_db_service_server::LogDbServiceServer;
use logdbd::service::LogDbServiceImpl;
use logdbd::storage::Storage;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn test_storage(dir: &std::path::Path) -> Storage {
    let mut db_config = logdb::Config::default();
    db_config.data_dir = dir.to_path_buf();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = logdb::LogDb::open(db_config).unwrap();
    Storage::new(db, 1)
}

async fn start_server() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
    let svc = LogDbServiceImpl::new(
        storage, catalog, Arc::new(ConsumerTracker::new()),
        "test-node".into(), "primary".into(),
    );
    let svc = LogDbServiceServer::new(svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, dir)
}

async fn wait_durable(client: &mut Client, ns: &str, stream: &str, min: u64) {
    for _ in 0..50 {
        if let Ok(wm) = client.watermark(ns, stream).await {
            if wm.durable_seq >= min { break; }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

const NS: &str = "test";
const STREAM: &str = "main";

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn append_and_read() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    let seq = client.append(NS, STREAM, "test.event", b"hello-world").await.unwrap();
    assert_eq!(seq, 1);
    wait_durable(&mut client, NS, STREAM, 1).await;

    let rec = client.read(NS, STREAM, 1).await.unwrap().unwrap();
    assert_eq!(rec.seq, 1);
    assert_eq!(rec.event_type, "test.event");
    assert_eq!(rec.content, b"hello-world");
}

#[tokio::test]
async fn append_with_metadata() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    let mut meta = HashMap::new();
    meta.insert("model".into(), "claude-sonnet-5".into());
    meta.insert("provider".into(), "anthropic".into());

    let resp = client.append_full(NS, STREAM, "llm.call", "application/json", &meta, 1_000_000, b"{}").await.unwrap();
    assert_eq!(resp.seq, 1);
    wait_durable(&mut client, NS, STREAM, 1).await;

    let rec = client.read(NS, STREAM, 1).await.unwrap().unwrap();
    assert_eq!(rec.event_type, "llm.call");
    assert_eq!(rec.metadata.get("model").unwrap(), "claude-sonnet-5");
}

#[tokio::test]
async fn read_not_found() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    let rec = client.read(NS, STREAM, 999).await.unwrap();
    assert!(rec.is_none());
}

#[tokio::test]
async fn scan_all() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    for i in 0..10u64 {
        client.append(NS, STREAM, "test", format!("rec-{}", i).as_bytes()).await.unwrap();
    }
    wait_durable(&mut client, NS, STREAM, 10).await;

    let all = client.scan_all(NS, STREAM, 0).await.unwrap();
    assert_eq!(all.len(), 10);
    assert_eq!(all[0].seq, 1);
    assert_eq!(all[9].seq, 10);
}

#[tokio::test]
async fn tail_subscription() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    // Write some records first
    for i in 0..5u64 {
        client.append(NS, STREAM, "test", format!("r-{}", i).as_bytes()).await.unwrap();
    }
    wait_durable(&mut client, NS, STREAM, 5).await;

    // Subscribe from seq 1 — use timeout because Tail is a long-lived stream
    let mut stream = client.tail(NS, STREAM).from_seq(1).batch_size(10).start(&mut client).await.unwrap();
    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while count < 5 {
        match tokio::time::timeout_at(deadline, stream.next_batch()).await {
            Ok(Ok(Some(batch))) => {
                count += batch.len();
                for rec in &batch {
                    assert!(rec.seq >= 1);
                }
            }
            _ => break,
        }
    }
    assert_eq!(count, 5);
}

#[tokio::test]
async fn consumer_group_commit_and_resume() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    // Write records
    for i in 0..5u64 {
        client.append(NS, STREAM, "test", format!("r-{}", i).as_bytes()).await.unwrap();
    }
    wait_durable(&mut client, NS, STREAM, 5).await;

    // Commit offset 3
    client.commit_offset(NS, STREAM, "workers", "w1", 3).await.unwrap();

    // Read back committed offset
    let offset = client.committed_offset(NS, STREAM, "workers", "w1").await.unwrap();
    assert_eq!(offset, 3);

    // Tail with consumer group — should auto-resume from 4 (offset + 1) when from_seq=0
    let mut stream = client.tail(NS, STREAM)
        .consumer_group("workers", "w1")
        .from_seq(0)  // auto-resume
        .batch_size(10)
        .start(&mut client).await.unwrap();

    let mut first_seq = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    match tokio::time::timeout_at(deadline, stream.next()).await {
        Ok(Ok(Some(rec))) => first_seq = Some(rec.seq),
        _ => {}
    }
    // With from_seq=0 and committed offset=3, auto-resume starts at 4
    assert_eq!(first_seq, Some(4));
}

#[tokio::test]
async fn list_namespaces_and_streams() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    // Write to create namespace + stream
    client.append("ns-a", "s1", "test", b"x").await.unwrap();
    client.append("ns-a", "s2", "test", b"x").await.unwrap();
    client.append("ns-b", "s1", "test", b"x").await.unwrap();

    let ns_list = client.list_namespaces().await.unwrap();
    assert_eq!(ns_list.len(), 2);
    assert!(ns_list.iter().any(|n| n.name == "ns-a"));
    assert!(ns_list.iter().any(|n| n.name == "ns-b"));

    let streams = client.list_streams("ns-a").await.unwrap();
    assert_eq!(streams.len(), 2);
    assert!(streams.iter().any(|s| s.name == "s1"));
    assert!(streams.iter().any(|s| s.name == "s2"));
}

#[tokio::test]
async fn status() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    let s = client.status().await.unwrap();
    assert_eq!(s.node_id, "test-node");
    assert_eq!(s.role, "primary");
}

#[tokio::test]
async fn verify_chain() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    for i in 0..5u64 {
        client.append(NS, STREAM, "test", format!("r-{}", i).as_bytes()).await.unwrap();
    }
    wait_durable(&mut client, NS, STREAM, 5).await;

    let result = client.verify_chain(NS, STREAM, 1, 0).await.unwrap();
    assert!(result.ok);
    assert_eq!(result.verified_from, 1);
    assert_eq!(result.verified_to, 5);
}

#[tokio::test]
async fn batch_append() {
    use logdbd::pb::AppendRequest;
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    let resp = client.append_batch(vec![
        AppendRequest { namespace: NS.into(), stream: STREAM.into(), event_type: "batch".into(), content: b"a".to_vec(), ..Default::default() },
        AppendRequest { namespace: NS.into(), stream: STREAM.into(), event_type: "batch".into(), content: b"b".to_vec(), ..Default::default() },
        AppendRequest { namespace: NS.into(), stream: STREAM.into(), event_type: "batch".into(), content: b"c".to_vec(), ..Default::default() },
    ]).await.unwrap();

    assert!(resp.error.is_none());
    assert_eq!(resp.records.len(), 3);
    assert_eq!(resp.records[0].seq, 1);
    assert_eq!(resp.records[1].seq, 2);
    assert_eq!(resp.records[2].seq, 3);
}

#[tokio::test]
async fn multi_stream_per_stream_seq() {
    let (addr, _dir) = start_server().await;
    let mut client = Client::connect(&addr).await.unwrap();

    // Stream A: 3 records, Stream B: 5 records
    for i in 0..3u64 {
        client.append("iso", "stream-a", "test", format!("a-{}", i).as_bytes()).await.unwrap();
    }
    for i in 0..5u64 {
        client.append("iso", "stream-b", "test", format!("b-{}", i).as_bytes()).await.unwrap();
    }
    wait_durable(&mut client, "iso", "stream-a", 3).await;
    wait_durable(&mut client, "iso", "stream-b", 5).await;

    let a = client.scan_all("iso", "stream-a", 0).await.unwrap();
    assert_eq!(a.len(), 3);
    assert_eq!(a[0].seq, 1);
    assert_eq!(a[2].seq, 3);

    let b = client.scan_all("iso", "stream-b", 0).await.unwrap();
    assert_eq!(b.len(), 5);
    assert_eq!(b[0].seq, 1);
    assert_eq!(b[0].content, b"b-0");
}
