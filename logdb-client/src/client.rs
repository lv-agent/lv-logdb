use logdbd_proto::pb::log_db_service_client::LogDbServiceClient;
use logdbd_proto::pb::*;
use tonic::transport::Channel;

/// Main client for logdbd.
///
/// Wraps a gRPC connection and provides ergonomic async methods.
pub struct Client {
    inner: LogDbServiceClient<Channel>,
    consumer_group: Option<String>,
    consumer_id: Option<String>,
}

impl Client {
    /// Connect to a logdbd server.
    pub async fn connect(addr: &str) -> Result<Self, tonic::transport::Error> {
        let url = if addr.starts_with("http") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };
        let inner = LogDbServiceClient::connect(url).await?;
        Ok(Self { inner, consumer_group: None, consumer_id: None })
    }

    /// Create a builder for advanced configuration.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Set default consumer group for all operations.
    pub fn set_consumer_group(&mut self, group: impl Into<String>, id: impl Into<String>) {
        self.consumer_group = Some(group.into());
        self.consumer_id = Some(id.into());
    }

    /// Get a reference to the underlying gRPC client.
    pub fn inner(&mut self) -> &mut LogDbServiceClient<Channel> {
        &mut self.inner
    }

    // ── Write ──────────────────────────────────────────────────────────

    /// Append a record. Returns the assigned seq.
    pub async fn append(
        &mut self, namespace: &str, stream: &str,
        event_type: &str, content: &[u8],
    ) -> Result<u64, tonic::Status> {
        let resp = self.inner.append(AppendRequest {
            namespace: namespace.into(), stream: stream.into(),
            event_type: event_type.into(),
            content: content.to_vec(),
            ..Default::default()
        }).await?;
        Ok(resp.into_inner().seq)
    }

    /// Append a record with full metadata.
    pub async fn append_full(
        &mut self, namespace: &str, stream: &str,
        event_type: &str, content_type: &str,
        metadata: &std::collections::HashMap<String, String>,
        timestamp_ns: u64, content: &[u8],
    ) -> Result<AppendResponse, tonic::Status> {
        let resp = self.inner.append(AppendRequest {
            namespace: namespace.into(), stream: stream.into(),
            event_type: event_type.into(),
            content_type: content_type.into(),
            metadata: metadata.clone(),
            timestamp_ns,
            content: content.to_vec(),
        }).await?;
        Ok(resp.into_inner())
    }

    /// Batch append multiple records atomically.
    pub async fn append_batch(
        &mut self, requests: Vec<AppendRequest>,
    ) -> Result<AppendBatchResponse, tonic::Status> {
        let resp = self.inner.batch_append(BatchAppendRequest { requests }).await?;
        Ok(resp.into_inner())
    }

    // ── Read ───────────────────────────────────────────────────────────

    /// Read a record by seq. Returns None if not found.
    pub async fn read(
        &mut self, namespace: &str, stream: &str, seq: u64,
    ) -> Result<Option<Record>, tonic::Status> {
        let resp = self.inner.read(ReadRequest {
            namespace: namespace.into(), stream: stream.into(), seq,
        }).await?.into_inner();
        Ok(resp.record)
    }

    /// Scan records in range. Returns a stream of batches.
    pub async fn scan(
        &mut self, namespace: &str, stream: &str,
        from_seq: u64, limit: u32,
    ) -> Result<ScanStream, tonic::Status> {
        let resp = self.inner.scan(ScanRequest {
            namespace: namespace.into(), stream: stream.into(),
            from_seq, to_seq: 0, limit,
        }).await?;
        Ok(ScanStream { inner: resp.into_inner() })
    }

    /// Scan all records and collect into a Vec.
    pub async fn scan_all(
        &mut self, namespace: &str, stream: &str,
        from_seq: u64,
    ) -> Result<Vec<Record>, tonic::Status> {
        let mut stream = self.scan(namespace, stream, from_seq, 10000).await?;
        let mut all = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            all.extend(batch.records);
            if !batch.has_more { break; }
        }
        Ok(all)
    }

    // ── Tail ───────────────────────────────────────────────────────────

    /// Create a tail subscription.
    pub fn tail(&mut self, namespace: &str, stream: &str) -> TailOptions {
        TailOptions {
            namespace: namespace.into(),
            stream: stream.into(),
            from_seq: 0,
            batch_size: 100,
            consumer_group: self.consumer_group.clone(),
            consumer_id: self.consumer_id.clone(),
        }
    }

    // ── Watermark ──────────────────────────────────────────────────────

    /// Get the watermark for a namespace/stream.
    pub async fn watermark(
        &mut self, namespace: &str, stream: &str,
    ) -> Result<Watermark, tonic::Status> {
        let resp = self.inner.get_watermark(GetWatermarkRequest {
            namespace: namespace.into(), stream: stream.into(),
        }).await?;
        Ok(resp.into_inner())
    }

    // ── Admin ──────────────────────────────────────────────────────────

    /// List all namespaces.
    pub async fn list_namespaces(&mut self) -> Result<Vec<NamespaceInfo>, tonic::Status> {
        let resp = self.inner.list_namespaces(ListNamespacesRequest {}).await?;
        Ok(resp.into_inner().namespaces)
    }

    /// List streams in a namespace.
    pub async fn list_streams(
        &mut self, namespace: &str,
    ) -> Result<Vec<StreamInfo>, tonic::Status> {
        let resp = self.inner.list_streams(ListStreamsRequest {
            namespace: namespace.into(),
        }).await?;
        Ok(resp.into_inner().streams)
    }

    /// Get node status.
    pub async fn status(&mut self) -> Result<StatusResponse, tonic::Status> {
        let resp = self.inner.status(StatusRequest {}).await?;
        Ok(resp.into_inner())
    }

    /// Verify hash chain for a stream.
    pub async fn verify_chain(
        &mut self, namespace: &str, stream: &str,
        from_seq: u64, to_seq: u64,
    ) -> Result<VerifyChainResponse, tonic::Status> {
        let resp = self.inner.verify_chain(VerifyChainRequest {
            namespace: namespace.into(), stream: stream.into(),
            from_seq, to_seq,
        }).await?;
        Ok(resp.into_inner())
    }

    /// Commit consumer offset.
    pub async fn commit_offset(
        &mut self, namespace: &str, stream: &str,
        consumer_group: &str, consumer_id: &str, seq: u64,
    ) -> Result<(), tonic::Status> {
        self.inner.commit_offset(CommitOffsetRequest {
            namespace: namespace.into(), stream: stream.into(),
            consumer_group: consumer_group.into(), consumer_id: consumer_id.into(),
            committed_seq: seq,
        }).await?;
        Ok(())
    }

    /// Get committed offset for a consumer.
    pub async fn committed_offset(
        &mut self, namespace: &str, stream: &str,
        consumer_group: &str, consumer_id: &str,
    ) -> Result<u64, tonic::Status> {
        let resp = self.inner.get_committed_offset(GetCommittedOffsetRequest {
            namespace: namespace.into(), stream: stream.into(),
            consumer_group: consumer_group.into(), consumer_id: consumer_id.into(),
        }).await?;
        Ok(resp.into_inner().committed_seq)
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

pub struct ClientBuilder {
    addr: Option<String>,
    consumer_group: Option<String>,
    consumer_id: Option<String>,
}

impl ClientBuilder {
    fn new() -> Self {
        Self { addr: None, consumer_group: None, consumer_id: None }
    }

    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into());
        self
    }

    pub fn consumer_group(mut self, group: impl Into<String>, id: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self.consumer_id = Some(id.into());
        self
    }

    pub async fn connect(self) -> Result<Client, tonic::transport::Error> {
        let addr = self.addr.unwrap_or_else(|| "127.0.0.1:50051".into());
        let mut client = Client::connect(&addr).await?;
        client.consumer_group = self.consumer_group;
        client.consumer_id = self.consumer_id;
        Ok(client)
    }
}

// ── Scan stream ───────────────────────────────────────────────────────────────

pub struct ScanStream {
    inner: tonic::Streaming<ScanResponse>,
}

impl ScanStream {
    /// Get the next batch of records.
    pub async fn next_batch(&mut self) -> Result<Option<ScanResponse>, tonic::Status> {
        match self.inner.message().await? {
            Some(batch) => Ok(Some(batch)),
            None => Ok(None),
        }
    }
}

// ── Tail stream ───────────────────────────────────────────────────────────────

/// Options for configuring a tail subscription.
pub struct TailOptions {
    namespace: String,
    stream: String,
    from_seq: u64,
    batch_size: u32,
    consumer_group: Option<String>,
    consumer_id: Option<String>,
}

impl TailOptions {
    pub fn from_seq(mut self, seq: u64) -> Self { self.from_seq = seq; self }
    pub fn batch_size(mut self, n: u32) -> Self { self.batch_size = n; self }
    pub fn consumer_group(mut self, group: impl Into<String>, id: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self.consumer_id = Some(id.into());
        self
    }

    /// Start the tail stream.
    pub async fn start(self, client: &mut Client) -> Result<TailStream, tonic::Status> {
        let ns = self.namespace.clone();
        let st = self.stream.clone();
        let grp = self.consumer_group.clone().unwrap_or_default();
        let cid = self.consumer_id.clone().unwrap_or_default();

        let resp = client.inner.tail(TailRequest {
            namespace: ns.clone(), stream: st.clone(),
            from_seq: self.from_seq, batch_size: self.batch_size,
            consumer_group: grp.clone(), consumer_id: cid.clone(),
        }).await?;
        Ok(TailStream {
            inner: resp.into_inner(),
            buffer: Vec::new(),
            namespace: ns, stream: st,
            consumer_group: grp, consumer_id: cid,
        })
    }
}

/// A live tail subscription. Call `next()` to receive records.
///
/// Commit progress via `client.commit_offset()` after processing.
pub struct TailStream {
    inner: tonic::Streaming<TailResponse>,
    buffer: Vec<Record>,  // remaining records from last batch
    namespace: String,
    stream: String,
    consumer_group: String,
    consumer_id: String,
}

impl TailStream {
    /// Get the next record.
    /// Heartbeat messages are automatically filtered.
    /// Remaining records from the last batch are buffered and returned one by one.
    pub async fn next(&mut self) -> Result<Option<Record>, tonic::Status> {
        // Drain buffer first
        if !self.buffer.is_empty() {
            return Ok(Some(self.buffer.remove(0)));
        }
        loop {
            let resp = match self.inner.message().await? {
                Some(r) => r,
                None => return Ok(None),
            };
            if resp.heartbeat { continue; }
            if resp.records.is_empty() { continue; }
            if resp.records.len() == 1 {
                return Ok(Some(resp.records.into_iter().next().unwrap()));
            }
            // Buffer remaining
            self.buffer = resp.records;
            return Ok(Some(self.buffer.remove(0)));
        }
    }

    /// Get the next batch of records.
    pub async fn next_batch(&mut self) -> Result<Option<Vec<Record>>, tonic::Status> {
        loop {
            let resp = match self.inner.message().await? {
                Some(r) => r,
                None => return Ok(None),
            };
            if resp.heartbeat { continue; }
            if !resp.records.is_empty() {
                return Ok(Some(resp.records));
            }
        }
    }

    /// Get the consumer group name (empty if not configured).
    pub fn consumer_group(&self) -> &str { &self.consumer_group }
    /// Get the consumer ID (empty if not configured).
    pub fn consumer_id(&self) -> &str { &self.consumer_id }
    /// Get the namespace.
    pub fn namespace(&self) -> &str { &self.namespace }
    /// Get the stream name.
    pub fn stream(&self) -> &str { &self.stream }
}
