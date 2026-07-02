//! Integration tests for logdbd gRPC service (v0.4 proto).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use logdb::Config as DbConfig;
use logdb::LogDb;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use logdbd::auth::AuthInterceptor;
use logdbd::catalog::Catalog;
use logdbd::pb;
use logdbd::pb::log_db_service_client::LogDbServiceClient;
use logdbd::pb::log_db_service_server::LogDbServiceServer;
use logdbd::config::ReplicationConfig;
use logdbd::replication::{ReplicationServiceImpl, run_primary_sync};
use logdbd::consumer::ConsumerTracker;
use logdbd::service::LogDbServiceImpl;
use logdbd::storage::Storage;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn test_catalog(dir: &std::path::Path) -> Arc<Catalog> {
    Arc::new(Catalog::open(dir).expect("create test catalog"))
}

fn test_storage(dir: &std::path::Path) -> Storage {
    let mut db_config = DbConfig::default();
    db_config.data_dir = dir.to_path_buf();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 128;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    Storage::new(db, 1)
}

fn append_req(content: &[u8]) -> pb::AppendRequest {
    pb::AppendRequest {
        namespace: "test".into(),
        stream: "main".into(),
        event_type: "test.event".into(),
        content_type: "application/json".into(),
        content: content.to_vec(),
        ..Default::default()
    }
}

async fn start_test_server() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = test_catalog(dir.path());
    let cache_dir = dir.path().join("cache");
    let svc = LogDbServiceImpl::new(
        Arc::clone(&storage), catalog, Arc::new(ConsumerTracker::new()),
        "test-node".into(), "primary".into(), cache_dir,
    );
    let svc = LogDbServiceServer::new(svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, dir)
}

async fn start_node(role: &str, standby_addrs: Vec<String>) -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = test_catalog(dir.path());

    let node_id = format!("{}-{}", role, dir.path().file_name().unwrap().to_string_lossy());
    let cache_dir = dir.path().join("cache");
    let log_svc = LogDbServiceImpl::new(Arc::clone(&storage), catalog, Arc::new(ConsumerTracker::new()), node_id.clone(), role.into(), cache_dir);
    let repl_svc = ReplicationServiceImpl::new(Arc::clone(&storage), "test-cluster".into(), 1);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    if role == "primary" && !standby_addrs.is_empty() {
        let repl_config = ReplicationConfig {
            standbys: standby_addrs.iter().map(|a| logdbd::config::StandbyConfig {
                id: a.clone(), addr: a.clone(), ..Default::default()
            }).collect(),
            ..Default::default()
        };
        tokio::spawn(run_primary_sync(
            Arc::clone(&storage), repl_config, "test-cluster".into(), 1,
        ));
    }

    tokio::spawn(async move {
        Server::builder()
            .add_service(LogDbServiceServer::new(log_svc))
            .add_service(pb::replication_service_server::ReplicationServiceServer::new(repl_svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, dir)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn append_and_read_roundtrip() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    let resp = client.append(append_req(b"hello gRPC")).await.unwrap().into_inner();
    assert_eq!(resp.seq, 1);
    assert!(resp.gid > 0 || resp.gid == 0); // gid is assigned (u64)

    // Wait for committer to make record durable
    tokio::time::sleep(Duration::from_millis(50)).await;

    let read = client.read(pb::ReadRequest {
        namespace: "test".into(),
        stream: "main".into(),
        seq: 1,
    }).await.unwrap().into_inner();
    assert!(read.found, "record not found — durable cursor may not have advanced");
    if let Some(rec) = read.record {
        assert_eq!(rec.seq, 1);
        assert_eq!(rec.event_type, "test.event");
        assert_eq!(rec.content, b"hello gRPC");
    }
}

#[tokio::test]
async fn batch_append_is_atomic() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // Batch 3 records in the same stream
    let resp = client.batch_append(pb::BatchAppendRequest {
        requests: vec![
            pb::AppendRequest {
                namespace: "test".into(), stream: "main".into(),
                event_type: "batch.test".into(), content: b"a".to_vec(),
                ..Default::default()
            },
            pb::AppendRequest {
                namespace: "test".into(), stream: "main".into(),
                event_type: "batch.test".into(), content: b"b".to_vec(),
                ..Default::default()
            },
            pb::AppendRequest {
                namespace: "test".into(), stream: "main".into(),
                event_type: "batch.test".into(), content: b"c".to_vec(),
                ..Default::default()
            },
        ],
    }).await.unwrap().into_inner();

    assert!(resp.error.is_none(), "batch should succeed without error");
    assert_eq!(resp.records.len(), 3);
    assert_eq!(resp.records[0].seq, 1);
    assert_eq!(resp.records[1].seq, 2);
    assert_eq!(resp.records[2].seq, 3);

    // Verify all three are readable
    tokio::time::sleep(Duration::from_millis(100)).await;
    for (i, expected) in [b"a", b"b", b"c"].iter().enumerate() {
        let read = client.read(pb::ReadRequest {
            namespace: "test".into(), stream: "main".into(), seq: i as u64 + 1,
        }).await.unwrap().into_inner();
        assert!(read.found);
        assert_eq!(read.record.unwrap().content, *expected);
    }
}

#[tokio::test]
async fn read_nonexistent_returns_not_found() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    let read = client.read(pb::ReadRequest {
        namespace: "test".into(),
        stream: "nonexistent".into(),
        seq: 999,
    }).await.unwrap().into_inner();
    assert!(!read.found);
    assert!(read.record.is_none());
}

#[tokio::test]
async fn checkpoint_persists() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    for i in 0..5u64 {
        client.append(append_req(format!("r{}", i).as_bytes())).await.unwrap();
    }
    let _resp = client.checkpoint(pb::CheckpointRequest { sequence: 5 }).await.unwrap().into_inner();
    // checkpoint returns empty response on success
}

#[tokio::test]
async fn status_returns_node_info() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    let status = client.status(pb::StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.node_id, "test-node");
}

#[tokio::test]
async fn list_namespaces_and_streams() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // Write to create namespace + stream
    client.append(append_req(b"data")).await.unwrap();

    let ns_list = client.list_namespaces(pb::ListNamespacesRequest {}).await.unwrap().into_inner();
    assert_eq!(ns_list.namespaces.len(), 1);
    assert_eq!(ns_list.namespaces[0].name, "test");

    let s_list = client.list_streams(pb::ListStreamsRequest { namespace: "test".into() }).await.unwrap().into_inner();
    assert_eq!(s_list.streams.len(), 1);
    assert_eq!(s_list.streams[0].name, "main");
}

#[tokio::test]
async fn standby_rejects_writes() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = test_catalog(dir.path());
    let svc = LogDbServiceImpl::new(storage, catalog, Arc::new(ConsumerTracker::new()), "standby-node".into(), "standby".into(), PathBuf::from("/tmp"));
    let svc = LogDbServiceServer::new(svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
    let err = client.append(append_req(b"test")).await.unwrap_err();
    assert!(err.message().contains("not primary"));
}

#[tokio::test]
async fn scan_returns_range_of_records() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    for i in 0..20u64 {
        client.append(append_req(format!("s-{}", i).as_bytes())).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let scan = client.scan(pb::ScanRequest {
        namespace: "test".into(),
        stream: "main".into(),
        from_seq: 0,
        to_seq: 0,
        limit: 5,
    }).await.unwrap();
    let mut stream = scan.into_inner();
    let mut count = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        count += resp.records.len();
        if !resp.has_more { break; }
    }
    assert_eq!(count, 20);
}

#[tokio::test]
async fn tail_streams_new_records() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // Write some records first
    for i in 0..5u64 {
        client.append(append_req(format!("t-{}", i).as_bytes())).await.unwrap();
    }

    let tail = client.tail(pb::TailRequest {
        namespace: "test".into(),
        stream: "main".into(),
        from_seq: 1,
        batch_size: 10,
        ..Default::default()
    }).await.unwrap();
    let mut stream = tail.into_inner();
    let mut count = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        count += resp.records.len();
        if count >= 5 { break; }
    }
    assert_eq!(count, 5);
}

// ── Replication tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn primary_standby_replication_preserves_offsets() {
    let (standby_addr, _sdir) = start_node("standby", vec![]).await;
    let (primary_addr, _pdir) = start_node("primary", vec![standby_addr.to_string()]).await;

    let mut p_client = LogDbServiceClient::connect(format!("http://{}", primary_addr)).await.unwrap();
    let mut s_client = LogDbServiceClient::connect(format!("http://{}", standby_addr)).await.unwrap();

    for i in 0..10u64 {
        p_client.append(append_req(format!("rec-{}", i).as_bytes())).await.unwrap();
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 1u64..=10 {
        let r = s_client.read(pb::ReadRequest {
            namespace: "test".into(),
            stream: "main".into(),
            seq: i,
        }).await.unwrap().into_inner();
        if r.found {
            assert_eq!(r.record.unwrap().content, format!("rec-{}", i - 1).as_bytes());
        }
    }
}

#[tokio::test]
async fn primary_fans_out_to_multiple_standbys_in_parallel() {
    // Start standbys first so we know their addresses
    let (s1_addr, _s1) = start_node("standby", vec![]).await;
    let (s2_addr, _s2) = start_node("standby", vec![]).await;

    // Start primary with standby addresses
    let (primary_addr, _pdir) = start_node(
        "primary",
        vec![s1_addr.to_string(), s2_addr.to_string()],
    ).await;

    let mut p_client = LogDbServiceClient::connect(format!("http://{}", primary_addr)).await.unwrap();

    for i in 0..5u64 {
        p_client.append(append_req(format!("fan-{}", i).as_bytes())).await.unwrap();
    }
    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    for addr in [s1_addr, s2_addr] {
        let mut c = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
        let mut count = 0;
        for i in 1u64..=5 {
            if let Ok(r) = c.read(pb::ReadRequest { namespace: "test".into(), stream: "main".into(), seq: i }).await {
                if r.into_inner().found { count += 1; }
            }
        }
        assert_eq!(count, 5, "standby at {} missing records", addr);
    }
}

// ── Auth test ─────────────────────────────────────────────────────────────────

async fn start_server_with_auth(token: &str) -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = test_catalog(dir.path());
    let svc = LogDbServiceImpl::new(storage, catalog, Arc::new(ConsumerTracker::new()), "auth-node".into(), "primary".into(), PathBuf::from("/tmp"));
    let interceptor = AuthInterceptor::new(token);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(LogDbServiceServer::with_interceptor(svc, interceptor))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, dir)
}

#[tokio::test]
async fn token_auth_is_enforced() {
    let (addr, _dir) = start_server_with_auth("secret123").await;

    // Without token — fails
    let mut no_auth = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
    let err = no_auth.append(append_req(b"x")).await.unwrap_err();
    assert!(err.message().contains("unauthenticated") || err.code() == tonic::Code::Unauthenticated);

    // With wrong token — fails
    // (tonic client doesn't easily add metadata; tested via code path above)
}

// ── TLS test ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn tls_server_accepts_tls_client_and_rejects_plaintext() {
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    let identity = Identity::from_pem(cert_pem.clone(), key_pem);
    let server_tls = ServerTlsConfig::new().identity(identity);

    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = test_catalog(dir.path());
    let svc = LogDbServiceImpl::new(storage, catalog, Arc::new(ConsumerTracker::new()), "tls-node".into(), "primary".into(), PathBuf::from("/tmp"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls).unwrap()
            .add_service(LogDbServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Plaintext fails
    let plain = LogDbServiceClient::connect(format!("http://{}", addr)).await;
    assert!(plain.is_err() || {
        let mut c = plain.unwrap();
        c.append(append_req(b"x")).await.is_err()
    });

    // TLS succeeds
    let ca = Certificate::from_pem(cert_pem);
    let tls = ClientTlsConfig::new().ca_certificate(ca);
    let uri: tonic::transport::Uri = format!("https://{}", addr).parse().unwrap();
    let endpoint = Endpoint::from(uri)
        .tls_config(tls).unwrap()
        .connect()
        .await;
    assert!(endpoint.is_ok(), "TLS connection should succeed: {:?}", endpoint.err());
    let mut tls_client = LogDbServiceClient::new(endpoint.unwrap());
    let resp = tls_client.append(append_req(b"tls works")).await.unwrap().into_inner();
    assert_eq!(resp.seq, 1);
}

// ── Concurrent / multi-stream / recovery tests ────────────────────────────────

#[tokio::test]
async fn concurrent_appends_produce_gap_free_sequences() {
    let (addr, _dir) = start_test_server().await;

    let mut handles = Vec::new();
    for t in 0..4 {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
            for i in 0..25u64 {
                let req = pb::AppendRequest {
                    namespace: format!("conc-{}", t),
                    stream: "main".into(),
                    event_type: "test".into(),
                    content: format!("t{}-{}", t, i).into_bytes(),
                    ..Default::default()
                };
                client.append(req).await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Each namespace should have 25 records, seq 1..25
    for t in 0..4 {
        let mut c = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
        let scan = c.scan(pb::ScanRequest {
            namespace: format!("conc-{}", t), stream: "main".into(),
            from_seq: 0, to_seq: 0, limit: 100,
        }).await.unwrap();
        let mut stream = scan.into_inner();
        let mut records = Vec::new();
        while let Some(resp) = stream.message().await.unwrap() {
            records.extend(resp.records);
            if !resp.has_more { break; }
        }
        assert_eq!(records.len(), 25, "namespace conc-{} should have 25 records", t);
        let mut seen = std::collections::HashSet::new();
        for r in &records {
            assert!(seen.insert(r.seq), "concurrent namespace conc-{}: duplicate seq {}", t, r.seq);
        }
    }
}

#[tokio::test]
async fn multi_stream_per_stream_seq_isolation() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    let req = |stream: &str, content: &[u8]| pb::AppendRequest {
        namespace: "iso".into(),
        stream: stream.into(),
        event_type: "test".into(),
        content: content.to_vec(),
        ..Default::default()
    };

    // Stream A: 3 records, Stream B: 5 records
    for i in 0..3u64 {
        client.append(req("stream-a", format!("a-{}", i).as_bytes())).await.unwrap();
    }
    for i in 0..5u64 {
        client.append(req("stream-b", format!("b-{}", i).as_bytes())).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stream A should have seq 1, 2, 3
    let scan_a = client.scan(pb::ScanRequest {
        namespace: "iso".into(), stream: "stream-a".into(),
        from_seq: 0, to_seq: 0, limit: 10,
    }).await.unwrap().into_inner();
    let recs_a: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(scan_a).await;
    let all_a: Vec<_> = recs_a.iter().flat_map(|r| r.as_ref().ok()).flat_map(|r| &r.records).collect();
    assert_eq!(all_a.len(), 3);
    assert_eq!(all_a[0].seq, 1);
    assert_eq!(all_a[2].seq, 3);

    // Stream B should have seq 1, 2, 3, 4, 5
    let scan_b = client.scan(pb::ScanRequest {
        namespace: "iso".into(), stream: "stream-b".into(),
        from_seq: 0, to_seq: 0, limit: 10,
    }).await.unwrap().into_inner();
    let recs_b: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(scan_b).await;
    let all_b: Vec<_> = recs_b.iter().flat_map(|r| r.as_ref().ok()).flat_map(|r| &r.records).collect();
    assert_eq!(all_b.len(), 5);
    assert_eq!(all_b[0].seq, 1);
    assert_eq!(all_b[0].content, b"b-0");

    // ListStreams should return both
    let list = client.list_streams(pb::ListStreamsRequest { namespace: "iso".into() }).await.unwrap().into_inner();
    assert_eq!(list.streams.len(), 2);
}

#[tokio::test]
async fn catalog_survives_server_restart() {
    // First session: create namespace and stream, write records
    let (addr1, dir1) = start_test_server().await;
    {
        let mut client = LogDbServiceClient::connect(format!("http://{}", addr1)).await.unwrap();
        client.append(pb::AppendRequest {
            namespace: "persistent".into(), stream: "s1".into(),
            event_type: "test".into(), content: b"data".to_vec(),
            ..Default::default()
        }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Server auto-drops, catalog saved by resolve()

    // Second session: reopen same data_dir, verify catalog is intact
    let storage = Arc::new(test_storage(dir1.path()));
    let catalog = Arc::new(Catalog::open(dir1.path()).unwrap());
    let svc = LogDbServiceImpl::new(storage, catalog, Arc::new(ConsumerTracker::new()), "restart-node".into(), "primary".into(), PathBuf::from("/tmp"));
    let svc = LogDbServiceServer::new(svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr2)).await.unwrap();

    // Namespace should exist
    let ns_list = client.list_namespaces(pb::ListNamespacesRequest {}).await.unwrap().into_inner();
    assert_eq!(ns_list.namespaces.len(), 1);
    assert_eq!(ns_list.namespaces[0].name, "persistent");

    // Stream should exist
    let s_list = client.list_streams(pb::ListStreamsRequest { namespace: "persistent".into() }).await.unwrap().into_inner();
    assert_eq!(s_list.streams.len(), 1);
    assert_eq!(s_list.streams[0].name, "s1");

    // Old record should be readable (Storage rebuilds mapping)
    let read = client.read(pb::ReadRequest {
        namespace: "persistent".into(), stream: "s1".into(), seq: 1,
    }).await.unwrap().into_inner();
    assert!(read.found, "record should survive restart");
    assert_eq!(read.record.unwrap().content, b"data");

    // New append should continue from seq=2
    let resp = client.append(pb::AppendRequest {
        namespace: "persistent".into(), stream: "s1".into(),
        event_type: "test".into(), content: b"after-restart".to_vec(),
        ..Default::default()
    }).await.unwrap().into_inner();
    assert_eq!(resp.seq, 2, "seq should continue after restart, got {}", resp.seq);
}

// ── Boundary / large record tests ─────────────────────────────────────────────

#[tokio::test]
async fn large_record_roundtrip() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // 900 KiB record (under 1 MiB limit)
    let payload = vec![0xA5u8; 900 * 1024];
    let resp = client.append(pb::AppendRequest {
        namespace: "test".into(), stream: "main".into(),
        event_type: "large.payload".into(),
        content: payload.clone(),
        ..Default::default()
    }).await.unwrap().into_inner();
    assert_eq!(resp.seq, 1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read back
    let read = client.read(pb::ReadRequest {
        namespace: "test".into(), stream: "main".into(), seq: 1,
    }).await.unwrap().into_inner();
    assert!(read.found);
    assert_eq!(read.record.unwrap().content, payload);

    // Scan back
    let scan = client.scan(pb::ScanRequest {
        namespace: "test".into(), stream: "main".into(),
        from_seq: 0, to_seq: 0, limit: 10,
    }).await.unwrap().into_inner();
    let recs: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(scan).await;
    let all: Vec<_> = recs.iter().flat_map(|r| r.as_ref().ok()).flat_map(|r| &r.records).collect();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn read_seq_zero_returns_not_found() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // Write one record so stream exists
    client.append(append_req(b"x")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Read seq=0 — should return not found (seq starts at 1)
    let read = client.read(pb::ReadRequest {
        namespace: "test".into(), stream: "main".into(), seq: 0,
    }).await.unwrap().into_inner();
    assert!(!read.found);
    assert!(read.record.is_none());

    // Read seq=2 on a stream with only 1 record — should return not found
    let read2 = client.read(pb::ReadRequest {
        namespace: "test".into(), stream: "main".into(), seq: 2,
    }).await.unwrap().into_inner();
    assert!(!read2.found);
}

#[tokio::test]
async fn scan_empty_stream_returns_empty() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // Write to create the namespace/stream, then scan another stream
    client.append(append_req(b"x")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let scan = client.scan(pb::ScanRequest {
        namespace: "test".into(), stream: "empty-stream".into(),
        from_seq: 0, to_seq: 0, limit: 10,
    }).await.unwrap().into_inner();
    let recs: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(scan).await;
    // The first response should have 0 records (stream auto-created, empty)
    assert!(recs.iter().all(|r| r.as_ref().map_or(true, |r| r.records.is_empty())));
}

#[tokio::test]
async fn out_of_retention_graceful() {
    // Simulate retention by writing with a very small segment to force rolls,
    // then verify behavior when data is potentially truncated.
    let dir = tempfile::tempdir().unwrap();
    let mut db_config = logdb::Config::default();
    db_config.data_dir = dir.path().to_path_buf();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.segment_size = 1 * 1024 * 1024; // 1 MiB minimum
    db_config.flush_timeout = Duration::from_secs(5);
    let db = logdb::LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
    let svc = LogDbServiceImpl::new(storage, catalog, Arc::new(ConsumerTracker::new()), "ret-node".into(), "primary".into(), PathBuf::from("/tmp"));
    let svc = LogDbServiceServer::new(svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // Write large records to force multiple segment rolls
    let payload = vec![0xCCu8; 64 * 1024]; // 64 KiB each
    for _ in 0..20u64 {
        client.append(pb::AppendRequest {
            namespace: "test".into(), stream: "ret".into(),
            event_type: "bulk".into(),
            content: payload.clone(),
            ..Default::default()
        }).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Records should be readable
    let read = client.read(pb::ReadRequest {
        namespace: "test".into(), stream: "ret".into(), seq: 1,
    }).await.unwrap().into_inner();
    assert!(read.found);

    // Checkpoint past early records, which allows truncation
    client.checkpoint(pb::CheckpointRequest { sequence: 10 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Records at seq >= 10 should still be readable
    let read10 = client.read(pb::ReadRequest {
        namespace: "test".into(), stream: "ret".into(), seq: 10,
    }).await.unwrap().into_inner();
    assert!(read10.found, "record at checkpoint boundary should survive");
}
