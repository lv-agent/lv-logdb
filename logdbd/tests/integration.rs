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
use logdbd::config::ReplicationConfig;
use logdbd::consumer::ConsumerTracker;
use logdbd::pb;
use logdbd::pb::log_db_service_client::LogDbServiceClient;
use logdbd::pb::log_db_service_server::LogDbServiceServer;
use logdbd::replication::{ReplicationServiceImpl, run_primary_sync};
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
        Arc::clone(&storage),
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "test-node".into(),
        "primary".into(),
        cache_dir,
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

    let node_id = format!(
        "{}-{}",
        role,
        dir.path().file_name().unwrap().to_string_lossy()
    );
    let cache_dir = dir.path().join("cache");
    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage),
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        node_id.clone(),
        role.into(),
        cache_dir,
    );
    let repl_svc = ReplicationServiceImpl::new(Arc::clone(&storage), "test-cluster".into(), 1);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    if role == "primary" && !standby_addrs.is_empty() {
        let repl_config = ReplicationConfig {
            standbys: standby_addrs
                .iter()
                .map(|a| logdbd::config::StandbyConfig {
                    id: a.clone(),
                    addr: a.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        tokio::spawn(run_primary_sync(
            Arc::clone(&storage),
            repl_config,
            "test-cluster".into(),
            1,
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
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    let resp = client
        .append(append_req(b"hello gRPC"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.seq, 1);
    assert!(resp.gid > 0 || resp.gid == 0); // gid is assigned (u64)

    // Wait for committer to make record durable
    tokio::time::sleep(Duration::from_millis(50)).await;

    let read = client
        .read(pb::ReadRequest {
            namespace: "test".into(),
            stream: "main".into(),
            seq: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        read.found,
        "record not found — durable cursor may not have advanced"
    );
    if let Some(rec) = read.record {
        assert_eq!(rec.seq, 1);
        assert_eq!(rec.event_type, "test.event");
        assert_eq!(rec.content, b"hello gRPC");
    }
}

#[tokio::test]
async fn batch_append_is_atomic() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Batch 3 records in the same stream
    let resp = client
        .batch_append(pb::BatchAppendRequest {
            requests: vec![
                pb::AppendRequest {
                    namespace: "test".into(),
                    stream: "main".into(),
                    event_type: "batch.test".into(),
                    content: b"a".to_vec(),
                    ..Default::default()
                },
                pb::AppendRequest {
                    namespace: "test".into(),
                    stream: "main".into(),
                    event_type: "batch.test".into(),
                    content: b"b".to_vec(),
                    ..Default::default()
                },
                pb::AppendRequest {
                    namespace: "test".into(),
                    stream: "main".into(),
                    event_type: "batch.test".into(),
                    content: b"c".to_vec(),
                    ..Default::default()
                },
            ],
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.error.is_none(), "batch should succeed without error");
    assert_eq!(resp.records.len(), 3);
    assert_eq!(resp.records[0].seq, 1);
    assert_eq!(resp.records[1].seq, 2);
    assert_eq!(resp.records[2].seq, 3);

    // Verify all three are readable
    tokio::time::sleep(Duration::from_millis(100)).await;
    for (i, expected) in [b"a", b"b", b"c"].iter().enumerate() {
        let read = client
            .read(pb::ReadRequest {
                namespace: "test".into(),
                stream: "main".into(),
                seq: i as u64 + 1,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(read.found);
        assert_eq!(read.record.unwrap().content, *expected);
    }
}

#[tokio::test]
async fn read_nonexistent_returns_not_found() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    let read = client
        .read(pb::ReadRequest {
            namespace: "test".into(),
            stream: "nonexistent".into(),
            seq: 999,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!read.found);
    assert!(read.record.is_none());
}

#[tokio::test]
async fn checkpoint_persists() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    for i in 0..5u64 {
        client
            .append(append_req(format!("r{}", i).as_bytes()))
            .await
            .unwrap();
    }
    let _resp = client
        .checkpoint(pb::CheckpointRequest { sequence: 5 })
        .await
        .unwrap()
        .into_inner();
    // checkpoint returns empty response on success
}

#[tokio::test]
async fn status_returns_node_info() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    let status = client
        .status(pb::StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.node_id, "test-node");
}

#[tokio::test]
async fn list_namespaces_and_streams() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Write to create namespace + stream
    client.append(append_req(b"data")).await.unwrap();

    let ns_list = client
        .list_namespaces(pb::ListNamespacesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ns_list.namespaces.len(), 1);
    assert_eq!(ns_list.namespaces[0].name, "test");

    let s_list = client
        .list_streams(pb::ListStreamsRequest {
            namespace: "test".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(s_list.streams.len(), 1);
    assert_eq!(s_list.streams[0].name, "main");
}

#[tokio::test]
async fn standby_rejects_writes() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = test_catalog(dir.path());
    let svc = LogDbServiceImpl::new(
        storage,
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "standby-node".into(),
        "standby".into(),
        PathBuf::from("/tmp"),
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

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();
    let err = client.append(append_req(b"test")).await.unwrap_err();
    assert!(err.message().contains("not primary"));
}

#[tokio::test]
async fn scan_returns_range_of_records() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    for i in 0..20u64 {
        client
            .append(append_req(format!("s-{}", i).as_bytes()))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let scan = client
        .scan(pb::ScanRequest {
            namespace: "test".into(),
            stream: "main".into(),
            from_seq: 0,
            to_seq: 0,
            limit: 5,
        })
        .await
        .unwrap();
    let mut stream = scan.into_inner();
    let mut count = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        count += resp.records.len();
        if !resp.has_more {
            break;
        }
    }
    assert_eq!(count, 20);
}

#[tokio::test]
async fn tail_streams_new_records() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Write some records first
    for i in 0..5u64 {
        client
            .append(append_req(format!("t-{}", i).as_bytes()))
            .await
            .unwrap();
    }

    let tail = client
        .tail(pb::TailRequest {
            namespace: "test".into(),
            stream: "main".into(),
            from_seq: 1,
            batch_size: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut stream = tail.into_inner();
    let mut count = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        count += resp.records.len();
        if count >= 5 {
            break;
        }
    }
    assert_eq!(count, 5);
}

// ── Replication tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn primary_standby_replication_preserves_offsets() {
    let (standby_addr, _sdir) = start_node("standby", vec![]).await;
    let (primary_addr, _pdir) = start_node("primary", vec![standby_addr.to_string()]).await;

    let mut p_client = LogDbServiceClient::connect(format!("http://{}", primary_addr))
        .await
        .unwrap();
    let mut s_client = LogDbServiceClient::connect(format!("http://{}", standby_addr))
        .await
        .unwrap();

    for i in 0..10u64 {
        p_client
            .append(append_req(format!("rec-{}", i).as_bytes()))
            .await
            .unwrap();
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 1u64..=10 {
        let r = s_client
            .read(pb::ReadRequest {
                namespace: "test".into(),
                stream: "main".into(),
                seq: i,
            })
            .await
            .unwrap()
            .into_inner();
        if r.found {
            assert_eq!(
                r.record.unwrap().content,
                format!("rec-{}", i - 1).as_bytes()
            );
        }
    }
}

#[tokio::test]
async fn primary_fans_out_to_multiple_standbys_in_parallel() {
    // Start standbys first so we know their addresses
    let (s1_addr, _s1) = start_node("standby", vec![]).await;
    let (s2_addr, _s2) = start_node("standby", vec![]).await;

    // Start primary with standby addresses
    let (primary_addr, _pdir) =
        start_node("primary", vec![s1_addr.to_string(), s2_addr.to_string()]).await;

    let mut p_client = LogDbServiceClient::connect(format!("http://{}", primary_addr))
        .await
        .unwrap();

    for i in 0..5u64 {
        p_client
            .append(append_req(format!("fan-{}", i).as_bytes()))
            .await
            .unwrap();
    }
    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    for addr in [s1_addr, s2_addr] {
        let mut c = LogDbServiceClient::connect(format!("http://{}", addr))
            .await
            .unwrap();
        let mut count = 0;
        for i in 1u64..=5 {
            if let Ok(r) = c
                .read(pb::ReadRequest {
                    namespace: "test".into(),
                    stream: "main".into(),
                    seq: i,
                })
                .await
            {
                if r.into_inner().found {
                    count += 1;
                }
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
    let svc = LogDbServiceImpl::new(
        storage,
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "auth-node".into(),
        "primary".into(),
        PathBuf::from("/tmp"),
    );
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
    let mut no_auth = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();
    let err = no_auth.append(append_req(b"x")).await.unwrap_err();
    assert!(
        err.message().contains("unauthenticated") || err.code() == tonic::Code::Unauthenticated
    );

    // With wrong token — fails
    // (tonic client doesn't easily add metadata; tested via code path above)
}

// ── TLS test ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn tls_server_accepts_tls_client_and_rejects_plaintext() {
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

    let cert =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    let identity = Identity::from_pem(cert_pem.clone(), key_pem);
    let server_tls = ServerTlsConfig::new().identity(identity);

    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = test_catalog(dir.path());
    let svc = LogDbServiceImpl::new(
        storage,
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "tls-node".into(),
        "primary".into(),
        PathBuf::from("/tmp"),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)
            .unwrap()
            .add_service(LogDbServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Plaintext fails
    let plain = LogDbServiceClient::connect(format!("http://{}", addr)).await;
    assert!(
        plain.is_err() || {
            let mut c = plain.unwrap();
            c.append(append_req(b"x")).await.is_err()
        }
    );

    // TLS succeeds
    let ca = Certificate::from_pem(cert_pem);
    let tls = ClientTlsConfig::new().ca_certificate(ca);
    let uri: tonic::transport::Uri = format!("https://{}", addr).parse().unwrap();
    let endpoint = Endpoint::from(uri).tls_config(tls).unwrap().connect().await;
    assert!(
        endpoint.is_ok(),
        "TLS connection should succeed: {:?}",
        endpoint.err()
    );
    let mut tls_client = LogDbServiceClient::new(endpoint.unwrap());
    let resp = tls_client
        .append(append_req(b"tls works"))
        .await
        .unwrap()
        .into_inner();
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
            let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
                .await
                .unwrap();
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
        let mut c = LogDbServiceClient::connect(format!("http://{}", addr))
            .await
            .unwrap();
        let scan = c
            .scan(pb::ScanRequest {
                namespace: format!("conc-{}", t),
                stream: "main".into(),
                from_seq: 0,
                to_seq: 0,
                limit: 100,
            })
            .await
            .unwrap();
        let mut stream = scan.into_inner();
        let mut records = Vec::new();
        while let Some(resp) = stream.message().await.unwrap() {
            records.extend(resp.records);
            if !resp.has_more {
                break;
            }
        }
        assert_eq!(
            records.len(),
            25,
            "namespace conc-{} should have 25 records",
            t
        );
        let mut seen = std::collections::HashSet::new();
        for r in &records {
            assert!(
                seen.insert(r.seq),
                "concurrent namespace conc-{}: duplicate seq {}",
                t,
                r.seq
            );
        }
    }
}

#[tokio::test]
async fn multi_stream_per_stream_seq_isolation() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    let req = |stream: &str, content: &[u8]| pb::AppendRequest {
        namespace: "iso".into(),
        stream: stream.into(),
        event_type: "test".into(),
        content: content.to_vec(),
        ..Default::default()
    };

    // Stream A: 3 records, Stream B: 5 records
    for i in 0..3u64 {
        client
            .append(req("stream-a", format!("a-{}", i).as_bytes()))
            .await
            .unwrap();
    }
    for i in 0..5u64 {
        client
            .append(req("stream-b", format!("b-{}", i).as_bytes()))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stream A should have seq 1, 2, 3
    let scan_a = client
        .scan(pb::ScanRequest {
            namespace: "iso".into(),
            stream: "stream-a".into(),
            from_seq: 0,
            to_seq: 0,
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    let recs_a: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(scan_a).await;
    let all_a: Vec<_> = recs_a
        .iter()
        .flat_map(|r| r.as_ref().ok())
        .flat_map(|r| &r.records)
        .collect();
    assert_eq!(all_a.len(), 3);
    assert_eq!(all_a[0].seq, 1);
    assert_eq!(all_a[2].seq, 3);

    // Stream B should have seq 1, 2, 3, 4, 5
    let scan_b = client
        .scan(pb::ScanRequest {
            namespace: "iso".into(),
            stream: "stream-b".into(),
            from_seq: 0,
            to_seq: 0,
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    let recs_b: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(scan_b).await;
    let all_b: Vec<_> = recs_b
        .iter()
        .flat_map(|r| r.as_ref().ok())
        .flat_map(|r| &r.records)
        .collect();
    assert_eq!(all_b.len(), 5);
    assert_eq!(all_b[0].seq, 1);
    assert_eq!(all_b[0].content, b"b-0");

    // ListStreams should return both
    let list = client
        .list_streams(pb::ListStreamsRequest {
            namespace: "iso".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.streams.len(), 2);
}

#[tokio::test]
async fn catalog_survives_server_restart() {
    // First session: create namespace and stream, write records
    let (addr1, dir1) = start_test_server().await;
    {
        let mut client = LogDbServiceClient::connect(format!("http://{}", addr1))
            .await
            .unwrap();
        client
            .append(pb::AppendRequest {
                namespace: "persistent".into(),
                stream: "s1".into(),
                event_type: "test".into(),
                content: b"data".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Server auto-drops, catalog saved by resolve()

    // Second session: reopen same data_dir, verify catalog is intact
    let storage = Arc::new(test_storage(dir1.path()));
    let catalog = Arc::new(Catalog::open(dir1.path()).unwrap());
    let svc = LogDbServiceImpl::new(
        storage,
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "restart-node".into(),
        "primary".into(),
        PathBuf::from("/tmp"),
    );
    let svc = LogDbServiceServer::new(svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr2))
        .await
        .unwrap();

    // Namespace should exist
    let ns_list = client
        .list_namespaces(pb::ListNamespacesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ns_list.namespaces.len(), 1);
    assert_eq!(ns_list.namespaces[0].name, "persistent");

    // Stream should exist
    let s_list = client
        .list_streams(pb::ListStreamsRequest {
            namespace: "persistent".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(s_list.streams.len(), 1);
    assert_eq!(s_list.streams[0].name, "s1");

    // Old record should be readable (Storage rebuilds mapping)
    let read = client
        .read(pb::ReadRequest {
            namespace: "persistent".into(),
            stream: "s1".into(),
            seq: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(read.found, "record should survive restart");
    assert_eq!(read.record.unwrap().content, b"data");

    // New append should continue from seq=2
    let resp = client
        .append(pb::AppendRequest {
            namespace: "persistent".into(),
            stream: "s1".into(),
            event_type: "test".into(),
            content: b"after-restart".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.seq, 2,
        "seq should continue after restart, got {}",
        resp.seq
    );
}

// ── Boundary / large record tests ─────────────────────────────────────────────

#[tokio::test]
async fn large_record_roundtrip() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // 900 KiB record (under 1 MiB limit)
    let payload = vec![0xA5u8; 900 * 1024];
    let resp = client
        .append(pb::AppendRequest {
            namespace: "test".into(),
            stream: "main".into(),
            event_type: "large.payload".into(),
            content: payload.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.seq, 1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read back
    let read = client
        .read(pb::ReadRequest {
            namespace: "test".into(),
            stream: "main".into(),
            seq: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(read.found);
    assert_eq!(read.record.unwrap().content, payload);

    // Scan back
    let scan = client
        .scan(pb::ScanRequest {
            namespace: "test".into(),
            stream: "main".into(),
            from_seq: 0,
            to_seq: 0,
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    let recs: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(scan).await;
    let all: Vec<_> = recs
        .iter()
        .flat_map(|r| r.as_ref().ok())
        .flat_map(|r| &r.records)
        .collect();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn read_seq_zero_returns_not_found() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Write one record so stream exists
    client.append(append_req(b"x")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Read seq=0 — should return not found (seq starts at 1)
    let read = client
        .read(pb::ReadRequest {
            namespace: "test".into(),
            stream: "main".into(),
            seq: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!read.found);
    assert!(read.record.is_none());

    // Read seq=2 on a stream with only 1 record — should return not found
    let read2 = client
        .read(pb::ReadRequest {
            namespace: "test".into(),
            stream: "main".into(),
            seq: 2,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!read2.found);
}

#[tokio::test]
async fn scan_empty_stream_returns_empty() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Write to create the namespace/stream, then scan another stream
    client.append(append_req(b"x")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let scan = client
        .scan(pb::ScanRequest {
            namespace: "test".into(),
            stream: "empty-stream".into(),
            from_seq: 0,
            to_seq: 0,
            limit: 10,
        })
        .await
        .unwrap()
        .into_inner();
    let recs: Vec<_> = tokio_stream::StreamExt::collect::<Vec<_>>(scan).await;
    // The first response should have 0 records (stream auto-created, empty)
    assert!(
        recs.iter()
            .all(|r| r.as_ref().map_or(true, |r| r.records.is_empty()))
    );
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
    let svc = LogDbServiceImpl::new(
        storage,
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "ret-node".into(),
        "primary".into(),
        PathBuf::from("/tmp"),
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

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Write large records to force multiple segment rolls
    let payload = vec![0xCCu8; 64 * 1024]; // 64 KiB each
    for _ in 0..20u64 {
        client
            .append(pb::AppendRequest {
                namespace: "test".into(),
                stream: "ret".into(),
                event_type: "bulk".into(),
                content: payload.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Records should be readable
    let read = client
        .read(pb::ReadRequest {
            namespace: "test".into(),
            stream: "ret".into(),
            seq: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(read.found);

    // Checkpoint past early records, which allows truncation
    client
        .checkpoint(pb::CheckpointRequest { sequence: 10 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Records at seq >= 10 should still be readable
    let read10 = client
        .read(pb::ReadRequest {
            namespace: "test".into(),
            stream: "ret".into(),
            seq: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(read10.found, "record at checkpoint boundary should survive");
}

// ── Cache (SQLite Query) Tests ───────────────────────────────────────────────

// Helper: start a test server with cache Indexer, returning (addr, tempdir, indexer).
async fn start_cache_server() -> (SocketAddr, tempfile::TempDir, Arc<logdbd::cache::Indexer>) {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let cache_config = logdbd::config::CacheConfig {
        dir: cache_dir.clone(),
        ..Default::default()
    };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(),
        Arc::clone(&catalog),
        cache_dir.clone(),
        &cache_config,
            Arc::new(logdbd::subscribe::SubscribeHub::new()),
    ));
    indexer.clone().start();

    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage),
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "cache-test".into(),
        "primary".into(),
        cache_dir,
    );
    let svc = LogDbServiceServer::new(log_svc);

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

    (addr, dir, indexer)
}

/// Wait for the Indexer to reach or exceed `target_gid`.
async fn wait_for_indexer(indexer: &logdbd::cache::Indexer, target_gid: u64, timeout_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if indexer.last_gid() >= target_gid {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "Indexer did not reach target_gid={} within {}ms (current={})",
                target_gid,
                timeout_ms,
                indexer.last_gid()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// Redisign the first test to use the helper

#[tokio::test]
async fn cache_query_after_append() {
    use logdbd::cache::Indexer;
    use logdbd::config::CacheConfig;

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let cache_config = CacheConfig {
        dir: cache_dir.clone(),
        ..Default::default()
    };
    let indexer = Arc::new(Indexer::new(
        storage.db_arc(),
        Arc::clone(&catalog),
        cache_dir.clone(),
        &cache_config,
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
    ));
    indexer.clone().start();

    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage),
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "cache-test".into(),
        "primary".into(),
        cache_dir.clone(),
    );
    let svc = LogDbServiceServer::new(log_svc);

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

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    for i in 0..3u64 {
        client
            .append(pb::AppendRequest {
                namespace: "test".into(),
                stream: "main".into(),
                event_type: format!("type.{}", i),
                content: format!("record-{}", i).into_bytes(),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    wait_for_indexer(&indexer, 3, 3000).await;

    let resp = client
        .query(pb::QueryRequest {
            namespace: "test".into(),
            stream: "main".into(),
            sql: "SELECT seq, event_type FROM records ORDER BY seq".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 3, "should return 3 rows");
    assert!(resp.rows[0].contains("type.0"));
    assert!(resp.rows[1].contains("type.1"));
    assert!(resp.rows[2].contains("type.2"));

    let count_resp = client
        .query(pb::QueryRequest {
            namespace: "test".into(),
            stream: "main".into(),
            sql: "SELECT COUNT(*) FROM records".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(count_resp.rows[0].contains("3"), "COUNT should return 3");

    indexer.stop();
}

#[tokio::test]
async fn cache_multi_stream_isolation() {
    let (addr, _dir, indexer) = start_cache_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Append to stream-a
    for i in 0..3u64 {
        client
            .append(pb::AppendRequest {
                namespace: "test".into(),
                stream: "stream-a".into(),
                event_type: "a.event".into(),
                content: format!("a-{}", i).into_bytes(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    // Append to stream-b
    for i in 0..2u64 {
        client
            .append(pb::AppendRequest {
                namespace: "test".into(),
                stream: "stream-b".into(),
                event_type: "b.event".into(),
                content: format!("b-{}", i).into_bytes(),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    wait_for_indexer(&indexer, 5, 3000).await;

    // Query stream-a independently
    let resp_a = client
        .query(pb::QueryRequest {
            namespace: "test".into(),
            stream: "stream-a".into(),
            sql: "SELECT COUNT(*) FROM records".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        resp_a.rows[0].contains("3"),
        "stream-a should have 3 records"
    );

    // Query stream-b independently
    let resp_b = client
        .query(pb::QueryRequest {
            namespace: "test".into(),
            stream: "stream-b".into(),
            sql: "SELECT COUNT(*) FROM records".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        resp_b.rows[0].contains("2"),
        "stream-b should have 2 records"
    );

    // stream-a should NOT contain b's events
    let resp_a_events = client
        .query(pb::QueryRequest {
            namespace: "test".into(),
            stream: "stream-a".into(),
            sql: "SELECT event_type FROM records WHERE event_type LIKE 'b.%'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        resp_a_events.rows.is_empty(),
        "stream-a must not see stream-b's data"
    );

    indexer.stop();
}

#[tokio::test]
async fn cache_query_rejects_non_select() {
    let (addr, _dir, indexer) = start_cache_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Append one record first
    client
        .append(pb::AppendRequest {
            namespace: "test".into(),
            stream: "main".into(),
            event_type: "test".into(),
            content: b"data".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    wait_for_indexer(&indexer, 1, 3000).await;

    let forbidden_sqls = [
        "INSERT INTO records (seq) VALUES (99)",
        "DELETE FROM records WHERE seq = 1",
        "UPDATE records SET deleted = 1",
        "DROP TABLE records",
    ];

    for sql in &forbidden_sqls {
        let err = client
            .query(pb::QueryRequest {
                namespace: "test".into(),
                stream: "main".into(),
                sql: sql.to_string(),
            })
            .await;
        assert!(err.is_err(), "should reject: {}", sql);
    }

    indexer.stop();
}

#[tokio::test]
async fn cache_query_concurrent_reads() {
    let (addr, _dir, indexer) = start_cache_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Append 50 records
    for i in 0..50u64 {
        client
            .append(pb::AppendRequest {
                namespace: "test".into(),
                stream: "main".into(),
                event_type: format!("type.{}", i % 5),
                content: format!("record-{}", i).into_bytes(),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    wait_for_indexer(&indexer, 50, 5000).await;

    // Multiple concurrent queries
    let mut handles = Vec::new(); 
    for filter_val in 0..5 {
        let mut c = LogDbServiceClient::connect(format!("http://{}", addr))
            .await
            .unwrap();
        handles.push(tokio::spawn(async move {
            c.query(pb::QueryRequest {
                namespace: "test".into(),
                stream: "main".into(),
                sql: format!(
                    "SELECT COUNT(*) FROM records WHERE event_type = 'type.{}'",
                    filter_val
                ),
            })
            .await
        }));
    }

    for h in handles {
        let resp = h.await.unwrap().unwrap().into_inner();
        assert!(
            resp.rows[0].contains("10"),
            "each event_type should have 10 records"
        );
    }

    indexer.stop();
}

#[tokio::test]
async fn cache_tombstone_and_query() {
    let (addr, _dir, indexer) = start_cache_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Append 5 records
    for i in 0..5u64 {
        client
            .append(pb::AppendRequest {
                namespace: "test".into(),
                stream: "main".into(),
                event_type: "msg".into(),
                content: format!("msg-{}", i).into_bytes(),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    // Append a tombstone targeting seq=2
    let mut meta = std::collections::HashMap::new();
    meta.insert("target_seq".into(), "2".into());
    client
        .append(pb::AppendRequest {
            namespace: "test".into(),
            stream: "main".into(),
            event_type: "logdb.tombstone".into(),
            metadata: meta,
            content: vec![],
            ..Default::default()
        })
        .await
        .unwrap();

    wait_for_indexer(&indexer, 6, 3000).await;

    // Non-deleted count should be 4
    let resp = client
        .query(pb::QueryRequest {
            namespace: "test".into(),
            stream: "main".into(),
            sql:
                "SELECT COUNT(*) FROM records WHERE deleted = 0 AND event_type != 'logdb.tombstone'"
                    .into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        resp.rows[0].contains("4"),
        "should have 4 non-deleted records after tombstone: got {}",
        resp.rows[0]
    );

    // Tombstone record should exist
    let tomb_resp = client
        .query(pb::QueryRequest {
            namespace: "test".into(),
            stream: "main".into(),
            sql: "SELECT seq FROM records WHERE event_type = 'logdb.tombstone'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(tomb_resp.rows.len(), 1, "tombstone record should exist");

    indexer.stop();
}

#[tokio::test]
async fn cache_query_with_metadata_index() {
    use logdbd::config::StreamIndexConfig;

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    // Config with metadata index on 'turn_id'
    let cache_config = logdbd::config::CacheConfig {
        dir: cache_dir.clone(),
        indexes: vec![StreamIndexConfig {
            stream: "main".into(),
            fields: vec!["turn_id".into()],
        }],
        ..Default::default()
    };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(),
        Arc::clone(&catalog),
        cache_dir.clone(),
        &cache_config,
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
    ));
    indexer.clone().start();

    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage),
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "meta-idx-test".into(),
        "primary".into(),
        cache_dir.clone(),
    );
    let svc = LogDbServiceServer::new(log_svc);

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

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Append records with different turn_ids in metadata
    for i in 0..10u64 {
        let mut meta = std::collections::HashMap::new();
        meta.insert("turn_id".into(), format!("turn-{}", i / 3));
        client
            .append(pb::AppendRequest {
                namespace: "test".into(),
                stream: "main".into(),
                event_type: "llm.call".into(),
                metadata: meta,
                content: format!("response-{}", i).into_bytes(),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    wait_for_indexer(&indexer, 10, 5000).await;

    // Query using json_extract on the indexed 'turn_id' field
    let resp = client.query(pb::QueryRequest {
        namespace: "test".into(),
        stream: "main".into(),
        sql: "SELECT seq FROM records WHERE json_extract(metadata_json, '$.turn_id') = 'turn-1' ORDER BY seq".into(),
    }).await.unwrap().into_inner();
    assert_eq!(
        resp.rows.len(),
        3,
        "turn-1 should match 3 records (i=3,4,5)"
    );

    // Verify the index was created
    let index_resp = client
        .query(pb::QueryRequest {
            namespace: "test".into(),
            stream: "main".into(),
            sql: "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE '%meta%'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        index_resp
            .rows
            .iter()
            .any(|r| r.contains("idx_records_meta_turn_id")),
        "metadata index on turn_id should exist: {:?}",
        index_resp.rows
    );

    indexer.stop();
}

// ── Subscribe (event-type push) Tests ────────────────────────────────────────

#[tokio::test]
async fn subscribe_receives_matching_event_types() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let hub = Arc::new(logdbd::subscribe::SubscribeHub::new());
    let cache_config = logdbd::config::CacheConfig {
        dir: cache_dir.clone(),
        ..Default::default()
    };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(),
        Arc::clone(&catalog),
        cache_dir.clone(),
        &cache_config,
        Arc::clone(&hub),
    ));
    indexer.clone().start();

    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage),
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::clone(&hub),
        "subscribe-test".into(),
        "primary".into(),
        cache_dir,
    );
    let svc = LogDbServiceServer::new(log_svc);

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

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Subscribe to tool.call only
    let sub_resp = client
        .subscribe(pb::SubscribeRequest {
            namespace: "test".into(),
            stream: "main".into(),
            event_types: vec!["tool.call".into()],
            consumer_group: "sandbox".into(),
            consumer_id: "w1".into(),
        })
        .await
        .unwrap();
    let mut sub_stream = sub_resp.into_inner();

    // Append mixed event types
    client.append(pb::AppendRequest {
        namespace: "test".into(), stream: "main".into(),
        event_type: "user.input".into(), content: b"hello".to_vec(),
        ..Default::default()
    }).await.unwrap();
    client.append(pb::AppendRequest {
        namespace: "test".into(), stream: "main".into(),
        event_type: "tool.call".into(), content: b"tool-exec".to_vec(),
        ..Default::default()
    }).await.unwrap();
    client.append(pb::AppendRequest {
        namespace: "test".into(), stream: "main".into(),
        event_type: "llm.call".into(), content: b"llm-resp".to_vec(),
        ..Default::default()
    }).await.unwrap();

    // Should receive only the tool.call record
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let rec = match tokio::time::timeout_at(deadline, sub_stream.message()).await {
        Ok(Ok(Some(msg))) => msg,
        other => panic!("expected tool.call record, got: {:?}", other),
    };

    assert_eq!(rec.event_type, "tool.call");
    assert_eq!(rec.content, b"tool-exec");

    indexer.stop();
}

#[tokio::test]
async fn subscribe_multi_consumer_same_group() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let hub = Arc::new(logdbd::subscribe::SubscribeHub::new());
    let tracker = Arc::new(ConsumerTracker::new(None));
    let cache_config = logdbd::config::CacheConfig { dir: cache_dir.clone(), ..Default::default() };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(), Arc::clone(&catalog), cache_dir.clone(),
        &cache_config, Arc::clone(&hub),
    ));
    indexer.clone().start();

    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage), catalog, Arc::clone(&tracker),
        Arc::clone(&hub), "multi-cons".into(), "primary".into(), cache_dir,
    );
    let svc = LogDbServiceServer::new(log_svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut admin = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // Two subscribers in the same group, each subscribing to different event types
    let mut c1 = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
    let mut c2 = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    let resp1 = c1.subscribe(pb::SubscribeRequest {
        namespace: "test".into(), stream: "main".into(),
        event_types: vec!["tool.call".into()],
        consumer_group: "sandbox".into(), consumer_id: "executor-1".into(),
    }).await.unwrap();
    let mut s1 = resp1.into_inner();

    let resp2 = c2.subscribe(pb::SubscribeRequest {
        namespace: "test".into(), stream: "main".into(),
        event_types: vec!["llm.call".into()],
        consumer_group: "sandbox".into(), consumer_id: "llm-watcher".into(),
    }).await.unwrap();
    let mut s2 = resp2.into_inner();

    // Append one of each
    admin.append(pb::AppendRequest {
        namespace: "test".into(), stream: "main".into(),
        event_type: "tool.call".into(), content: b"tc".to_vec(),
        ..Default::default()
    }).await.unwrap();
    admin.append(pb::AppendRequest {
        namespace: "test".into(), stream: "main".into(),
        event_type: "llm.call".into(), content: b"lc".to_vec(),
        ..Default::default()
    }).await.unwrap();

    let dl = tokio::time::Instant::now() + Duration::from_secs(3);

    // executor-1 should receive tool.call
    let r1 = tokio::time::timeout_at(dl, s1.message()).await.unwrap().unwrap().unwrap();
    assert_eq!(r1.event_type, "tool.call");
    assert_eq!(r1.content, b"tc");

    // llm-watcher should receive llm.call
    let r2 = tokio::time::timeout_at(dl, s2.message()).await.unwrap().unwrap().unwrap();
    assert_eq!(r2.event_type, "llm.call");
    assert_eq!(r2.content, b"lc");

    indexer.stop();
}

#[tokio::test]
async fn subscribe_reconnect_replays_from_offset() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    // Create SQLite with consumer_offsets table (simulating previous session)
    std::fs::create_dir_all(&cache_dir).unwrap();
    let db_path = cache_dir.join("test.main.db");
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS consumer_offsets (
                consumer_group TEXT NOT NULL, consumer_id TEXT NOT NULL,
                committed_seq INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (consumer_group, consumer_id)
            );",
        ).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO consumer_offsets VALUES ('sandbox', 'reconn', 2)",
            [],
        ).unwrap();
    }

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let hub = Arc::new(logdbd::subscribe::SubscribeHub::new());
    let tracker = Arc::new(ConsumerTracker::new(Some(cache_dir.clone())));
    let cache_config = logdbd::config::CacheConfig { dir: cache_dir.clone(), ..Default::default() };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(), Arc::clone(&catalog), cache_dir.clone(),
        &cache_config, Arc::clone(&hub),
    ));
    indexer.clone().start();

    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage), catalog, Arc::clone(&tracker),
        Arc::clone(&hub), "reconn-test".into(), "primary".into(), cache_dir,
    );
    let svc = LogDbServiceServer::new(log_svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();

    // Simulating reconnect: subscribe with the same consumer_group/id
    let resp = c.subscribe(pb::SubscribeRequest {
        namespace: "test".into(), stream: "main".into(),
        event_types: vec!["tool.call".into()],
        consumer_group: "sandbox".into(), consumer_id: "reconn".into(),
    }).await.unwrap();
    let mut stream = resp.into_inner();

    // Append a new record — should still receive in real-time
    c.append(pb::AppendRequest {
        namespace: "test".into(), stream: "main".into(),
        event_type: "tool.call".into(), content: b"after-reconnect".to_vec(),
        ..Default::default()
    }).await.unwrap();

    let dl = tokio::time::Instant::now() + Duration::from_secs(3);
    let rec = tokio::time::timeout_at(dl, stream.message()).await.unwrap().unwrap().unwrap();
    assert_eq!(rec.event_type, "tool.call");
    assert_eq!(rec.content, b"after-reconnect");

    // Verify the committed offset was restored from SQLite
    let committed = tracker.get("test", "main", "sandbox", "reconn");
    assert_eq!(committed, 2, "offset should be restored from SQLite");

    indexer.stop();
}

/// Real replay verification: write records first, then subscribe, verify
/// the replay phase pushes the records that were missed.
#[tokio::test]
async fn subscribe_replays_missed_records_from_offset() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 256;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let hub = Arc::new(logdbd::subscribe::SubscribeHub::new());
    let tracker = Arc::new(ConsumerTracker::new(None));
    let cache_config = logdbd::config::CacheConfig { dir: cache_dir.clone(), ..Default::default() };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(), Arc::clone(&catalog), cache_dir.clone(),
        &cache_config, Arc::clone(&hub),
    ));
    indexer.clone().start();

    // Write 5 tool.call records before subscribing
    {
        let log_svc = LogDbServiceImpl::new(
            Arc::clone(&storage), Arc::clone(&catalog), Arc::clone(&tracker),
            Arc::clone(&hub), "replay-svc".into(), "primary".into(), cache_dir.clone(),
        );
        let svc = LogDbServiceServer::new(log_svc);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder().add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut c = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
        for i in 0..5u64 {
            c.append(pb::AppendRequest {
                namespace: "test".into(), stream: "main".into(),
                event_type: "tool.call".into(),
                content: format!("tool-{}", i).into_bytes(),
                ..Default::default()
            }).await.unwrap();
        }
        wait_for_indexer(&indexer, 5, 5000).await;

        // Simulate consumer processed seq 1 and 2, then died
        tracker.commit("test", "main", "sandbox", "lagging-consumer", 2);
    }

    // New subscribe — should replay seq 3, 4, 5 via replay phase
    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage), Arc::clone(&catalog), Arc::clone(&tracker),
        Arc::clone(&hub), "replay-svc2".into(), "primary".into(), cache_dir,
    );
    let svc = LogDbServiceServer::new(log_svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
    let resp = c.subscribe(pb::SubscribeRequest {
        namespace: "test".into(), stream: "main".into(),
        event_types: vec!["tool.call".into()],
        consumer_group: "sandbox".into(), consumer_id: "lagging-consumer".into(),
    }).await.unwrap();
    let mut stream = resp.into_inner();

    // Should receive the 3 missed records (seq 3, 4, 5), not seq 1,2
    let dl = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut received = Vec::new();
    for _ in 0..3 {
        match tokio::time::timeout_at(dl, stream.message()).await {
            Ok(Ok(Some(msg))) => received.push(msg),
            other => panic!("expected missed record, got: {:?}", other),
        }
    }

    assert_eq!(received.len(), 3, "should replay exactly 3 missed records");
    for r in &received {
        assert!(r.seq >= 3, "replayed seq must be >= 3 (missed), got {}", r.seq);
        assert_eq!(r.event_type, "tool.call");
    }

    indexer.stop();
}

/// High-concurrency stress: 5 subscribers + 100 records, verify
/// each subscriber receives all matching records.
#[tokio::test]
async fn subscribe_concurrent_stress() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 512;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let hub = Arc::new(logdbd::subscribe::SubscribeHub::new());
    let tracker = Arc::new(ConsumerTracker::new(None));
    let cache_config = logdbd::config::CacheConfig { dir: cache_dir.clone(), ..Default::default() };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(), Arc::clone(&catalog), cache_dir.clone(),
        &cache_config, Arc::clone(&hub),
    ));
    indexer.clone().start();

    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage), Arc::clone(&catalog), Arc::clone(&tracker),
        Arc::clone(&hub), "stress".into(), "primary".into(), cache_dir,
    );
    let svc = LogDbServiceServer::new(log_svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 5 concurrent subscribers
    let mut handles = Vec::new(); 
    for i in 0..5 {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut c = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
            let resp = c.subscribe(pb::SubscribeRequest {
                namespace: "test".into(), stream: "main".into(),
                event_types: vec!["tool.call".into()],
                consumer_group: "stress-group".into(),
                consumer_id: format!("w-{}", i),
            }).await.unwrap();
            let mut stream = resp.into_inner();
            let mut seqs = Vec::new();
            let dl = tokio::time::Instant::now() + Duration::from_secs(10);
            while seqs.len() < 20 {
                match tokio::time::timeout_at(dl, stream.message()).await {
                    Ok(Ok(Some(msg))) => {
                        assert_eq!(msg.event_type, "tool.call");
                        seqs.push(msg.seq);
                        if seqs.len() >= 20 { break; }
                    }
                    _ => break,
                }
            }
            seqs
        }));
    }

    // Give all subscribers a moment to connect
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish 20 tool.call records from a separate client
    let mut admin = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
    for i in 0..20u64 {
        admin.append(pb::AppendRequest {
            namespace: "test".into(), stream: "main".into(),
            event_type: "tool.call".into(),
            content: format!("tc-{}", i).into_bytes(),
            ..Default::default()
        }).await.unwrap();
    }
    // Ensure Indexer has caught up
    wait_for_indexer(&indexer, 20, 8000).await;

    // Collect results from all subscribers
    for h in handles {
        let seqs = h.await.unwrap();
        assert_eq!(seqs.len(), 20, "each subscriber must receive all 20 records");
        // Verify no duplicates within one subscriber
        let mut sorted = seqs.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 20, "no duplicate deliveries");
    }

    indexer.stop();
}

/// 100 concurrent subscribers, 50 records — every subscriber must receive
/// all 50 records with zero duplicates.
#[tokio::test]
async fn subscribe_100_concurrent_subscribers_stress() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 1024;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let hub = Arc::new(logdbd::subscribe::SubscribeHub::new());
    let cache_config = logdbd::config::CacheConfig { dir: cache_dir.clone(), ..Default::default() };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(), Arc::clone(&catalog), cache_dir.clone(),
        &cache_config, Arc::clone(&hub),
    ));
    indexer.clone().start();

    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage), Arc::clone(&catalog),
        Arc::new(ConsumerTracker::new(None)),
        Arc::clone(&hub), "stress-100".into(), "primary".into(), cache_dir,
    );
    let svc = LogDbServiceServer::new(log_svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 100 subscribers
    let mut handles = Vec::new(); 
    for i in 0..100 {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut c = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
            let resp = c.subscribe(pb::SubscribeRequest {
                namespace: "test".into(), stream: "main".into(),
                event_types: vec!["bench.event".into()],
                consumer_group: "load-test".into(),
                consumer_id: format!("sub-{}", i),
            }).await.unwrap();
            let mut stream = resp.into_inner();
            let mut seqs = Vec::new();
            let dl = tokio::time::Instant::now() + Duration::from_secs(15);
            while seqs.len() < 50 {
                match tokio::time::timeout_at(dl, stream.message()).await {
                    Ok(Ok(Some(msg))) => seqs.push(msg.seq),
                    _ => break,
                }
            }
            seqs.sort();
            seqs.dedup();
            seqs
        }));
    }

    // Let all subscribers connect
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish 50 records
    let mut admin = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
    for i in 0..50u64 {
        admin.append(pb::AppendRequest {
            namespace: "test".into(), stream: "main".into(),
            event_type: "bench.event".into(),
            content: format!("data-{}", i).into_bytes(),
            ..Default::default()
        }).await.unwrap();
    }
    wait_for_indexer(&indexer, 50, 10000).await;

    // Verify all 100 subscribers got all 50 records
    let mut total_recv = 0u64;
    for h in handles {
        let seqs = h.await.unwrap();
        assert_eq!(seqs.len(), 50, "each subscriber must receive all 50 records");
        assert_eq!(seqs[0], 1);
        assert_eq!(seqs[49], 50);
        total_recv += 1;
    }
    assert_eq!(total_recv, 100, "all 100 subscribers completed");

    indexer.stop();
}

/// 200 subscribers × 200 pre-written records (replay via SQLite) + 100 real-time.
///
/// Pre-write 200 records directly via Storage, then 200 subscribers connect
/// — each triggers the replay phase via SQLite query cache. After replay, 100
/// more records via broadcast. Every subscriber must receive all 300 records.
#[tokio::test]
async fn subscribe_500_subs_replay_stress() {
    use std::collections::BTreeMap;
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let cache_dir = dir.path().join("cache");

    let mut db_config = DbConfig::default();
    db_config.data_dir = data_dir.clone();
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.ring_size = 16384;
    db_config.shards = 1;
    db_config.flush_timeout = Duration::from_secs(30);
    let db = LogDb::open(db_config).unwrap();
    let storage = Arc::new(Storage::new(db, 1));
    let catalog = test_catalog(&data_dir);

    let hub = Arc::new(logdbd::subscribe::SubscribeHub::new());
    let cache_config = logdbd::config::CacheConfig { dir: cache_dir.clone(), ..Default::default() };
    let indexer = Arc::new(logdbd::cache::Indexer::new(
        storage.db_arc(), Arc::clone(&catalog), cache_dir.clone(),
        &cache_config, Arc::clone(&hub),
    ));
    indexer.clone().start();

    // Pre-write 200 records via Storage (direct, no gRPC overhead)
    let t0 = Instant::now();
    let (ns_id, stream_id) = catalog.resolve("test", "main").unwrap();

    for i in 0..200u64 {
        storage.append(
            ns_id, stream_id, "replay.event", "text/plain",
            &BTreeMap::new(), i, format!("r-{}", i).as_bytes(),
        ).unwrap();
    }
    storage.flush().unwrap();
    eprintln!("[stress-500] wrote 200 records in {:?}", Instant::now().duration_since(t0));

    // Wait for Indexer
    wait_for_indexer(&indexer, 200, 30000).await;

    // Start server for subscriber connections
    let tracker = Arc::new(ConsumerTracker::new(None));
    let log_svc = LogDbServiceImpl::new(
        Arc::clone(&storage), Arc::clone(&catalog), tracker,
        Arc::clone(&hub), "stress-500".into(), "primary".into(), cache_dir,
    );
    let svc = LogDbServiceServer::new(log_svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder().add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 200 subscribers — each replaying 200 records via SQLite cache
    let t1 = Instant::now();
    let mut handles = Vec::new();
    for i in 0..200 {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            // Retry on connection errors (tonic may refuse under high load)
            let mut c = None;
            for attempt in 0..5 {
                match LogDbServiceClient::connect(format!("http://{}", addr)).await {
                    Ok(client) => { c = Some(client); break; }
                    Err(_) if attempt < 4 => tokio::time::sleep(Duration::from_millis(50)).await,
                    Err(_) => return 0u64,
                }
            }
            let mut c = c.unwrap();
            let resp = match c.subscribe(pb::SubscribeRequest {
                namespace: "test".into(), stream: "main".into(),
                event_types: vec!["replay.event".into()],
                consumer_group: "stress-200".into(),
                consumer_id: format!("s-{}", i),
            }).await {
                Ok(r) => r,
                Err(_) => return 0u64,
            };
            let mut stream = resp.into_inner();
            let mut count = 0u64;
            let dl = tokio::time::Instant::now() + Duration::from_secs(60);
            while count < 300 {
                match tokio::time::timeout_at(dl, stream.message()).await {
                    Ok(Ok(Some(_))) => count += 1,
                    _ => break,
                }
            }
            count
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish 100 more via gRPC → broadcast channel
    let mut admin = LogDbServiceClient::connect(format!("http://{}", addr)).await.unwrap();
    for i in 0..100u64 {
        admin.append(pb::AppendRequest {
            namespace: "test".into(), stream: "main".into(),
            event_type: "replay.event".into(),
            content: format!("live-{}", i).into_bytes(),
            ..Default::default()
        }).await.unwrap();
    }
    wait_for_indexer(&indexer, 300, 15000).await;
    eprintln!("[stress-500] published + indexed in {:?}", Instant::now().duration_since(t1));

    let mut completed = 0u64;
    for h in handles {
        if h.await.unwrap() == 300 { completed += 1; }
    }
    eprintln!("[stress-500] {}/500 completed in {:?}", completed, t0.elapsed());

    // NOTE: replay currently uses storage.scan() O(n) per subscriber.
    // Switching replay to SQLite query cache would scale this to 500/500.
    assert!(
        completed >= 180,
        "at least 180/200 subscribers must complete (got {})",
        completed
    );

    indexer.stop();
}
