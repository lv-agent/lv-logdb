# logdb-client — Rust SDK for logdbd

## Install

```toml
[dependencies]
logdb-client = "0.1"
```

## Quick Start

```rust
use logdb_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("127.0.0.1:50051").await?;

    // Append
    let seq = client.append("my-app", "main", "test.event", b"hello").await?;
    println!("appended at seq {}", seq);

    // Read
    if let Some(rec) = client.read("my-app", "main", 1).await? {
        println!("[{}] {}: {:?}", rec.seq, rec.event_type, rec.content);
    }

    // Scan all
    let records = client.scan_all("my-app", "main", 0).await?;
    for r in &records {
        println!("[{}] {}", r.seq, r.event_type);
    }

    // Tail (live subscription)
    let mut stream = client
        .tail("my-app", "main")
        .consumer_group("workers", "w1")
        .start(&mut client).await?;
    while let Some(rec) = stream.next().await? {
        println!("[{}] {}", rec.seq, rec.event_type);
    }

    Ok(())
}
```

## Query (structured filter)

Query reads the log segment directly at the committed cursor — no SQL, no
SQLite cache. Build a `QueryRequest` with predicates and a `QueryResult` shape,
then match on the typed `QueryResponse::result` oneof.

```rust
use logdb_client::{QueryRequest, QueryResult, query_response};

// Fetch the 10 most recent llm.call records
let resp = client.query(QueryRequest {
    namespace: "my-app".into(),
    stream: "main".into(),
    event_types: vec!["llm.call".into()],
    descending: true,
    limit: 10,
    ..Default::default()
}).await?;
if let Some(query_response::Result::Records(rr)) = resp.result {
    for r in &rr.records {
        println!("[{}] {}", r.seq, r.event_type);
    }
}

// COUNT matching records
let resp = client.query(QueryRequest {
    namespace: "my-app".into(),
    stream: "main".into(),
    result: QueryResult::Count.into(),
    ..Default::default()
}).await?;
if let Some(query_response::Result::Count(n)) = resp.result {
    println!("total records: {}", n);
}
```

Predicates (all AND-combined): `event_types` (IN), `from_seq`/`to_seq` (inclusive
closed interval, `None` = unbounded), `metadata` (field equality). Result shapes:
`RECORDS` (default), `COUNT`, `EXISTS`, `COUNT_DISTINCT`, `MIN`, `MAX`,
`DISTINCT_VALUES`. `aggregate_field` selects the metadata field for the
aggregations. `absent` expresses an anti-join (NOT EXISTS). Aggregations skip
records lacking `aggregate_field`; `MIN`/`MAX` return 0 when none qualify.

## Subscribe (event-type push)

```rust
use tonic::Streaming;

// Subscribe to matching event types in real-time
let mut stream = client.subscribe(
    "my-app", "main",
    vec!["tool.call".into(), "llm.call".into()],
    "sandbox-processors",  // consumer group
    "worker-1",            // consumer id
).await?;

while let Some(rec) = stream.message().await? {
    println!("[{}] {}: {:?}", rec.seq, rec.event_type, rec.content);
    // Commit progress for resume-after-reconnect
    client.commit_offset(
        "my-app", "main",
        "sandbox-processors", "worker-1",
        rec.seq,
    ).await?;
}
```

## API

| Method | Description |
|--------|-------------|
| `connect(addr)` | Create client |
| `append(ns, stream, event_type, content)` | Write a record |
| `append_full(...)` | Write with full metadata |
| `append_batch(requests)` | Batch write |
| `read(ns, stream, seq)` | Point read |
| `scan_all(ns, stream, from_seq)` | Scan and collect all |
| `tail(ns, stream)` | Create a TailOptions builder |
| `query(request)` | Structured filter against the log segment (typed oneof response) |
| `subscribe(ns, stream, event_types, group, id)` | Event-type push subscription |
| `watermark(ns, stream)` | Get watermarks |
| `list_namespaces()` | List all namespaces |
| `list_streams(ns)` | List streams in a namespace |
| `status()` | Node status |
| `verify_chain(ns, stream, from, to)` | Verify hash chain |
| `commit_offset(...)` | Commit consumer offset |
| `committed_offset(...)` | Get consumer offset |
| `create_stream(ns, stream)` | Create namespace+stream (admin) |
| `delete_stream(ns, stream)` | Soft-delete all records in stream (admin) |
| `checkpoint(seq)` | Advance WAL checkpoint |

## RBAC / Auth

```rust
// Token is passed via gRPC metadata. Set it on the client:
use logdb_client::ClientBuilder;

let mut client = ClientBuilder::new("logdbd.example.com:50051")
    .auth_token("admin-secret-token")
    .connect()
    .await?;

// Roles: admin (all), writer (append), reader (read/query), subscriber (subscribe)
// RPCs are gated: append requires Writer, read/query requires Reader,
// subscribe requires Subscriber, create/delete stream requires Admin.
```

## License

Apache-2.0
