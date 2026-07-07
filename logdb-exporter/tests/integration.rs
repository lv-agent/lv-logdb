//! Integration test: logdbd + exporter end-to-end.
//!
//! Starts a logdbd server, writes records, runs exporter in scan+tail mode,
//! verifies all records are exported.

use std::sync::Arc;
use std::time::Duration;

use logdbd::catalog::Catalog;
use logdbd::consumer::ConsumerTracker;
use logdbd::pb::AppendRequest;
use logdbd::pb::log_db_service_client::LogDbServiceClient;
use logdbd::pb::log_db_service_server::LogDbServiceServer;
use logdbd::service::LogDbServiceImpl;
use logdbd::storage::Storage;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

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

fn append_req(content: &[u8]) -> AppendRequest {
    AppendRequest {
        namespace: "test".into(),
        stream: "main".into(),
        event_type: "test".into(),
        content_type: "application/json".into(),
        content: content.to_vec(),
        ..Default::default()
    }
}

async fn start_logdbd() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(test_storage(dir.path()));
    let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
    let svc = LogDbServiceImpl::new(
        storage,
        catalog,
        Arc::new(ConsumerTracker::new(None)),
        Arc::new(logdbd::subscribe::SubscribeHub::new()),
        "test-node".into(),
        "primary".into(),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        Server::builder()
            .add_service(LogDbServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, dir)
}

#[tokio::test]
async fn exporter_scans_existing_records() {
    let (addr, _dir) = start_logdbd().await;
    let mut client = LogDbServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Write 5 records
    for i in 0..5u64 {
        client
            .append(append_req(format!("rec-{}", i).as_bytes()))
            .await
            .unwrap();
    }
    // Wait for Committer
    for _ in 0..100 {
        let wm = client
            .get_watermark(logdbd::pb::GetWatermarkRequest {
                namespace: "test".into(),
                stream: "main".into(),
            })
            .await
            .unwrap()
            .into_inner();
        if wm.durable_seq >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Use exporter's source to scan
    let tls_config = logdb_exporter::config::TlsConfig::default();
    let mut source = logdb_exporter::source::Source::connect(&[addr.clone()], tls_config)
        .await
        .unwrap();
    let chunks = source.scan("test", "main", 0, 100).await.unwrap();
    let total: Vec<_> = chunks.iter().flat_map(|c| &c.records).collect();
    assert_eq!(total.len(), 5);
    assert_eq!(total[0].seq, 1);
    assert_eq!(total[4].seq, 5);
}

#[tokio::test]
async fn exporter_progress_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("progress.dat");

    let mut prog = logdb_exporter::progress::Progress::new(
        "test-cluster".into(),
        1,
        "test".into(),
        "main".into(),
        p.clone(),
    );
    prog.last_seq = 42;
    prog.save().unwrap();

    let loaded = logdb_exporter::progress::Progress::load(&p)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.last_seq, 42);
    assert_eq!(loaded.namespace, "test");
}
