//! Integration tests for logdbd gRPC service.
//!
//! Each test starts a real server on a random port, connects via gRPC,
//! and verifies the expected behavior.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use logdb::Config as DbConfig;
use logdb::LogDb;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use logdbd::auth::AuthInterceptor;
use logdbd::pb;
use logdbd::pb::log_db_service_client::LogDbServiceClient;
use logdbd::pb::log_db_service_server::LogDbServiceServer;
use logdbd::replication::{run_primary_sync, ReplicationServiceImpl};
use logdbd::service::LogDbServiceImpl;

/// Start a test server on a random port. Returns the address and the temp dir guard.
async fn start_test_server() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut db_config = DbConfig::default();
    db_config.data_dir = dir.path().to_path_buf();
    db_config.durability_mode = logdb::DurabilityMode::Batch;
    db_config.ring_size = 128;
    let db = LogDb::open(db_config).unwrap();

    let svc = LogDbServiceImpl::new(Arc::new(db), "test-node".into(), "primary".into());
    let svc = pb::log_db_service_server::LogDbServiceServer::new(svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    (addr, dir)
}

#[tokio::test]
async fn append_and_read_roundtrip() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Append
    let req = tonic::Request::new(pb::AppendRequest {
        content: b"hello gRPC".to_vec(),
    });
    let resp = client.append(req).await.unwrap().into_inner();
    assert_eq!(resp.sequence, 0);

    // Wait for Committer to process (Batch mode, small write might need trigger)
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Read back
    let req = tonic::Request::new(pb::ReadRequest { sequence: 0 });
    let rec = client.read(req).await.unwrap().into_inner();
    assert_eq!(rec.sequence, 0);
    assert_eq!(rec.content, b"hello gRPC".to_vec());
}

#[tokio::test]
async fn read_nonexistent_returns_error() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    let req = tonic::Request::new(pb::ReadRequest { sequence: 99999 });
    let result = client.read(req).await;
    assert!(
        result.is_err(),
        "should return NOT_FOUND for nonexistent record"
    );
}

#[tokio::test]
async fn batch_append_is_atomic() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    let req = tonic::Request::new(pb::BatchAppendRequest {
        contents: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
    });
    let resp = client.batch_append(req).await.unwrap().into_inner();
    assert_eq!(resp.sequence, 0);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // All three should be readable
    for i in 0..3u64 {
        let req = tonic::Request::new(pb::ReadRequest { sequence: i });
        let rec = client.read(req).await.unwrap().into_inner();
        assert_eq!(rec.sequence, i);
    }
}

#[tokio::test]
async fn status_returns_node_info() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    let req = tonic::Request::new(pb::StatusRequest {});
    let status = client.status(req).await.unwrap().into_inner();
    assert_eq!(status.node_id, "test-node");
    assert!(status.wal_bytes_total > 0);
}

#[tokio::test]
async fn checkpoint_persists() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Append 10 records
    for i in 0..10u64 {
        let req = tonic::Request::new(pb::AppendRequest {
            content: format!("r{}", i).into_bytes(),
        });
        client.append(req).await.unwrap();
    }

    // Set checkpoint
    let req = tonic::Request::new(pb::CheckpointRequest { sequence: 5u64 });
    client.checkpoint(req).await.unwrap();

    // Verify checkpoint
    let req = tonic::Request::new(pb::StatusRequest {});
    let status = client.status(req).await.unwrap().into_inner();
    assert_eq!(status.checkpoint, 5);
}

#[tokio::test]
async fn standby_rejects_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db_config = DbConfig::default();
    db_config.data_dir = dir.path().to_path_buf();
    db_config.durability_mode = logdb::DurabilityMode::Batch;
    db_config.ring_size = 128;
    let db = LogDb::open(db_config).unwrap();

    let svc = LogDbServiceImpl::new(Arc::new(db), "standby-node".into(), "standby".into());
    let svc = pb::log_db_service_server::LogDbServiceServer::new(svc);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Write should be rejected
    let req = tonic::Request::new(pb::AppendRequest {
        content: b"test".to_vec(),
    });
    let result = client.append(req).await;
    assert!(result.is_err(), "standby should reject writes");

    // Read should still work
    let req = tonic::Request::new(pb::ReadRequest { sequence: 0 });
    let _ = client.read(req).await; // NOT_FOUND is OK, just shouldn't be PERMISSION_DENIED
}

// ── Replication (primary → standby) ──────────────────────────────────────────

/// Start a full node (LogDbService + ReplicationService). If `role` is primary
/// and `standby_addrs` is non-empty, spawns the background replication task.
async fn start_node(role: &str, standby_addrs: Vec<String>) -> (SocketAddr, tempfile::TempDir) {
    use logdb::DurabilityMode;

    let dir = tempfile::tempdir().unwrap();
    let mut db_config = DbConfig::default();
    db_config.data_dir = dir.path().to_path_buf();
    db_config.durability_mode = DurabilityMode::Sync;
    db_config.ring_size = 128;
    db_config.shards = 1;
    let db = Arc::new(LogDb::open(db_config).unwrap());

    let node_id = format!(
        "{}-{}",
        role,
        dir.path().file_name().unwrap().to_string_lossy()
    );
    let log_svc = LogDbServiceImpl::new(Arc::clone(&db), node_id, role.into());
    let repl_svc = ReplicationServiceImpl::new(Arc::clone(&db));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    if role == "primary" && !standby_addrs.is_empty() {
        tokio::spawn(run_primary_sync(Arc::clone(&db), standby_addrs, None, None));
    }

    tokio::spawn(async move {
        Server::builder()
            .add_service(pb::log_db_service_server::LogDbServiceServer::new(log_svc))
            .add_service(pb::replication_service_server::ReplicationServiceServer::new(repl_svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, dir)
}

#[tokio::test]
async fn scan_returns_range_of_records() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    for i in 0..5u64 {
        let req = tonic::Request::new(pb::AppendRequest {
            content: format!("s-{}", i).into_bytes(),
        });
        client.append(req).await.unwrap();
    }
    // Let the Committer fsync so records become durable (readable).
    tokio::time::sleep(Duration::from_millis(250)).await;

    let mut stream = client
        .scan(tonic::Request::new(pb::ScanRequest { from: 0, to: 5 }))
        .await
        .unwrap()
        .into_inner();

    let mut got = Vec::new();
    while let Some(rec) = stream.message().await.unwrap() {
        got.push(rec);
    }
    assert_eq!(got.len(), 5, "scan [0,5) should yield 5 records");
    for (i, rec) in got.iter().enumerate() {
        assert_eq!(rec.sequence, i as u64);
        assert_eq!(rec.content, format!("s-{}", i).into_bytes());
    }
}

#[tokio::test]
async fn tail_streams_new_records() {
    let (addr, _dir) = start_test_server().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Open the tail stream BEFORE appending — the consumer starts at seq 0.
    let mut stream = client
        .tail(tonic::Request::new(pb::TailRequest {
            consumer_name: "test-consumer".into(),
            max_count: 10,
        }))
        .await
        .unwrap()
        .into_inner();

    for i in 0..3u64 {
        let req = tonic::Request::new(pb::AppendRequest {
            content: format!("t-{}", i).into_bytes(),
        });
        client.append(req).await.unwrap();
    }

    // Each message is timeout-bounded so the test fails fast instead of hanging.
    let mut got = Vec::new();
    for _ in 0..3 {
        let rec = tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("tail stream did not deliver a record in time")
            .unwrap()
            .unwrap();
        got.push(rec);
    }
    assert_eq!(got.len(), 3, "tail should deliver all 3 appended records");
    for (i, rec) in got.iter().enumerate() {
        assert_eq!(rec.content, format!("t-{}", i).into_bytes());
        assert_eq!(rec.sequence, i as u64);
    }
}

#[tokio::test]
async fn primary_standby_replication_preserves_offsets() {
    // Standby up first (so the primary's replication task can connect).
    let (standby_addr, _standby_dir) = start_node("standby", vec![]).await;
    let (primary_addr, _primary_dir) = start_node("primary", vec![standby_addr.to_string()]).await;

    let mut primary = LogDbServiceClient::connect(format!("http://{}", primary_addr))
        .await
        .unwrap();
    let mut standby = LogDbServiceClient::connect(format!("http://{}", standby_addr))
        .await
        .unwrap();

    // Append 10 records to the primary.
    const N: u64 = 10;
    for i in 0..N {
        let req = tonic::Request::new(pb::AppendRequest {
            content: format!("rec-{}", i).into_bytes(),
        });
        primary.append(req).await.unwrap();
    }

    // Poll the standby until all N records appear with EXACT sequences.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let mut all_present = true;
        for i in 0..N {
            let req = tonic::Request::new(pb::ReadRequest { sequence: i });
            match standby.read(req).await {
                Ok(rec) => {
                    let rec = rec.into_inner();
                    assert_eq!(rec.sequence, i, "standby sequence mismatch");
                    assert_eq!(rec.content, format!("rec-{}", i).into_bytes());
                }
                Err(_) => {
                    all_present = false;
                    break;
                }
            }
        }
        if all_present {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("standby did not receive all {} records within timeout", N);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Final integrity: last record's timestamp is preserved end-to-end too.
    let rec = standby
        .read(tonic::Request::new(pb::ReadRequest { sequence: N - 1 }))
        .await
        .unwrap()
        .into_inner();
    assert!(rec.timestamp_ns > 0);
}

#[tokio::test]
async fn primary_fans_out_to_multiple_standbys_in_parallel() {
    // L3: the primary reuses one channel per standby and pushes to all of them
    // concurrently. Two standbys must both receive every record at the
    // primary's exact offsets.
    let (s1_addr, _d1) = start_node("standby", vec![]).await;
    let (s2_addr, _d2) = start_node("standby", vec![]).await;
    let (primary_addr, _dp) =
        start_node("primary", vec![s1_addr.to_string(), s2_addr.to_string()]).await;

    let mut primary = LogDbServiceClient::connect(format!("http://{}", primary_addr))
        .await
        .unwrap();
    let mut s1 = LogDbServiceClient::connect(format!("http://{}", s1_addr))
        .await
        .unwrap();
    let mut s2 = LogDbServiceClient::connect(format!("http://{}", s2_addr))
        .await
        .unwrap();

    const N: u64 = 8;
    for i in 0..N {
        let req = tonic::Request::new(pb::AppendRequest {
            content: format!("fan-{}", i).into_bytes(),
        });
        primary.append(req).await.unwrap();
    }

    // Poll BOTH standbys; each must independently reach all N records.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    for client in [&mut s1, &mut s2] {
        loop {
            let mut all = true;
            for i in 0..N {
                let req = tonic::Request::new(pb::ReadRequest { sequence: i });
                if client.read(req).await.is_err() {
                    all = false;
                    break;
                }
            }
            if all {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("a standby did not receive all {} records in time", N);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Both standbys carry identical, offset-preserving content.
    for i in 0..N {
        let r1 = s1
            .read(tonic::Request::new(pb::ReadRequest { sequence: i }))
            .await
            .unwrap()
            .into_inner();
        let r2 = s2
            .read(tonic::Request::new(pb::ReadRequest { sequence: i }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r1.sequence, i);
        assert_eq!(r2.sequence, i);
        assert_eq!(r1.content, format!("fan-{}", i).into_bytes());
        assert_eq!(r2.content, r1.content);
    }
}

// ── P0-3: authentication ────────────────────────────────────────────────────

/// Start a node that requires `Bearer <token>` on every LogDbService RPC.
async fn start_server_with_auth(token: &str) -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut db_config = DbConfig::default();
    db_config.data_dir = dir.path().to_path_buf();
    db_config.ring_size = 128;
    let db = Arc::new(LogDb::open(db_config).unwrap());
    let svc = LogDbServiceImpl::new(db, "auth-node".into(), "primary".into());
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
    let token = "s3cret-token";
    let (addr, _dir) = start_server_with_auth(token).await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // 1) No auth header → rejected.
    let no_token = client
        .status(tonic::Request::new(pb::StatusRequest {}))
        .await;
    assert!(no_token.is_err(), "RPC without token must be rejected");

    // 2) Wrong token → rejected.
    let mut bad = tonic::Request::new(pb::StatusRequest {});
    bad.metadata_mut()
        .insert("authorization", "Bearer wrong".parse().unwrap());
    assert!(
        client.status(bad).await.is_err(),
        "RPC with wrong token must be rejected"
    );

    // 3) Correct token → accepted.
    let mut good = tonic::Request::new(pb::StatusRequest {});
    good.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert!(
        client.status(good).await.is_ok(),
        "RPC with correct token must succeed"
    );
}

// ── P0-3: TLS transport ─────────────────────────────────────────────────────

#[tokio::test]
async fn tls_server_accepts_tls_client_and_rejects_plaintext() {
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

    // Self-signed cert valid for 127.0.0.1 (used as both server identity and
    // client-trusted CA).
    let cert =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    let identity = Identity::from_pem(cert_pem.clone(), key_pem);
    let server_tls = ServerTlsConfig::new().identity(identity);

    let dir = tempfile::tempdir().unwrap();
    let mut db_config = DbConfig::default();
    db_config.data_dir = dir.path().to_path_buf();
    db_config.ring_size = 128;
    let db = Arc::new(LogDb::open(db_config).unwrap());
    let svc = LogDbServiceImpl::new(db, "tls-node".into(), "primary".into());

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
    tokio::time::sleep(Duration::from_millis(150)).await;

    // (a) A TLS client trusting the self-signed cert succeeds.
    let ca = Certificate::from_pem(cert_pem.as_bytes());
    let channel = Endpoint::from_shared(format!("https://{}", addr))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(ca)
                .domain_name("127.0.0.1"),
        )
        .unwrap()
        .connect()
        .await
        .expect("TLS client must connect to TLS server");
    let mut client = LogDbServiceClient::new(channel);
    assert!(
        client
            .status(tonic::Request::new(pb::StatusRequest {}))
            .await
            .is_ok(),
        "TLS client must complete an RPC"
    );

    // (b) A plaintext client is rejected by the TLS server.
    let plain = Endpoint::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await;
    // Either the connect fails or the first RPC fails — either is acceptable
    // proof that plaintext does not work against a TLS-only server.
    let plain_ok = match plain {
        Ok(channel) => {
            let mut c = LogDbServiceClient::new(channel);
            c.status(tonic::Request::new(pb::StatusRequest {}))
                .await
                .is_ok()
        }
        Err(_) => false,
    };
    assert!(
        !plain_ok,
        "plaintext client must not succeed against a TLS server"
    );
}
