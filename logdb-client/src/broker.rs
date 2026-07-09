//! Broker SDK (cr-037) — ergonomic client for the logdb-broker consumer-group
//! coordinator. Clients talk ONLY to the broker (symmetric gateway): producers
//! [`BrokerProducer::produce`], consumers [`GroupConsumer::consume`].
//!
//! `GroupConsumer` handles the join → consume → commit → leave lifecycle. It
//! tracks its generation; if a `Consume` is rejected as stale (another member
//! joined/triggered a rebalance), it transparently re-joins and retries once.
//! Phase 5 replaces this rejoin-sync with the in-stream rebalance protocol.

use std::fmt;
use std::pin::Pin;

use logdb_broker_proto::pb::broker_service_client::BrokerServiceClient;
use logdb_broker_proto::pb::consume_response::Payload as ConsumePayload;
use logdb_broker_proto::pb::{
    CommitShardOffsetRequest, ConsumeRequest, ConsumeResponse, JoinGroupRequest,
    LeaveGroupRequest, ProduceRequest, Record,
};
use tonic::transport::Channel;

use tokio_stream::{Stream, StreamExt};

/// Errors from the broker SDK.
#[derive(Debug)]
pub enum BrokerError {
    Transport(tonic::transport::Error),
    Status(tonic::Status),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "broker transport: {e}"),
            Self::Status(s) => write!(f, "broker status: {s}"),
        }
    }
}

impl std::error::Error for BrokerError {}

impl From<tonic::transport::Error> for BrokerError {
    fn from(e: tonic::transport::Error) -> Self {
        Self::Transport(e)
    }
}
impl From<tonic::Status> for BrokerError {
    fn from(s: tonic::Status) -> Self {
        Self::Status(s)
    }
}

/// A consumer-group member. Owns its broker connection + current generation.
///
/// Build with [`GroupConsumer::join`], then [`GroupConsumer::consume`] for a
/// record stream and [`GroupConsumer::commit_shard`] to record progress.
pub struct GroupConsumer {
    client: BrokerServiceClient<Channel>,
    namespace: String,
    stream: String,
    group: String,
    consumer_id: String,
    generation: u32,
    assigned_shards: Vec<u32>,
}

impl GroupConsumer {
    /// Join `(namespace, stream, group)` as `consumer_id`. Returns a consumer
    /// seeded with its shard assignment + generation.
    pub async fn join(
        broker_addr: impl Into<String>,
        namespace: impl Into<String>,
        stream: impl Into<String>,
        group: impl Into<String>,
        consumer_id: impl Into<String>,
    ) -> Result<Self, BrokerError> {
        let namespace = namespace.into();
        let stream = stream.into();
        let group = group.into();
        let consumer_id = consumer_id.into();
        let mut client = BrokerServiceClient::connect(broker_addr.into()).await?;
        let resp = client
            .join_group(JoinGroupRequest {
                namespace: namespace.clone(),
                stream: stream.clone(),
                group: group.clone(),
                consumer_id: consumer_id.clone(),
            })
            .await?
            .into_inner();
        Ok(Self {
            client,
            namespace,
            stream,
            group,
            consumer_id,
            generation: resp.generation,
            assigned_shards: resp.assigned_shards,
        })
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn assigned_shards(&self) -> &[u32] {
        &self.assigned_shards
    }

    /// Stream records for this consumer's assigned shards. On a stale-generation
    /// rejection (a rebalance happened) the call re-joins and retries once —
    /// Phase 5 replaces this with rebalance signals pushed on the stream.
    pub async fn consume(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Record, BrokerError>> + Send>>, BrokerError> {
        match self.try_consume().await {
            Ok(s) => Ok(s),
            Err(BrokerError::Status(st)) if st.code() == tonic::Code::FailedPrecondition => {
                self.rejoin().await?;
                self.try_consume().await
            }
            Err(e) => Err(e),
        }
    }

    /// Commit `seq` as last-processed on `shard_id`. Returns `true` if the
    /// broker's offset advanced (false = stale/no-op).
    pub async fn commit_shard(&mut self, shard_id: u32, seq: u64) -> Result<bool, BrokerError> {
        let r = self
            .client
            .commit_shard_offset(CommitShardOffsetRequest {
                namespace: self.namespace.clone(),
                stream: self.stream.clone(),
                group: self.group.clone(),
                shard_id,
                committed_seq: seq,
            })
            .await?
            .into_inner();
        Ok(r.advanced)
    }

    /// Leave the group (consumes `self` — a left consumer is done).
    pub async fn leave(mut self) -> Result<(), BrokerError> {
        self.client
            .leave_group(LeaveGroupRequest {
                namespace: self.namespace,
                stream: self.stream,
                group: self.group,
                consumer_id: self.consumer_id,
                generation: self.generation,
            })
            .await?;
        Ok(())
    }

    async fn try_consume(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Record, BrokerError>> + Send>>, BrokerError> {
        let streaming = self
            .client
            .consume(ConsumeRequest {
                namespace: self.namespace.clone(),
                stream: self.stream.clone(),
                group: self.group.clone(),
                consumer_id: self.consumer_id.clone(),
                generation: self.generation,
            })
            .await?
            .into_inner();
        let mapped = streaming.filter_map(|item: Result<ConsumeResponse, tonic::Status>| {
            // Phase 5: surface rebalance/assignment frames here too.
            match item {
                Ok(resp) => match resp.payload {
                    Some(ConsumePayload::Record(r)) => Some(Ok(r)),
                    _ => None,
                },
                Err(e) => Some(Err(BrokerError::Status(e))),
            }
        });
        Ok(Box::pin(mapped))
    }

    async fn rejoin(&mut self) -> Result<(), BrokerError> {
        let resp = self
            .client
            .join_group(JoinGroupRequest {
                namespace: self.namespace.clone(),
                stream: self.stream.clone(),
                group: self.group.clone(),
                consumer_id: self.consumer_id.clone(),
            })
            .await?
            .into_inner();
        self.generation = resp.generation;
        self.assigned_shards = resp.assigned_shards;
        Ok(())
    }
}

/// A broker producer (publishes records). The symmetric counterpart to
/// [`GroupConsumer`] — both talk only to the broker.
pub struct BrokerProducer {
    client: BrokerServiceClient<Channel>,
}

impl BrokerProducer {
    pub async fn connect(addr: impl Into<String>) -> Result<Self, BrokerError> {
        Ok(Self {
            client: BrokerServiceClient::connect(addr.into()).await?,
        })
    }

    /// Publish a record, returning its (gid, seq). `shard_key` routes
    /// deterministically (same key ⇒ same shard); `None` ⇒ logdbd legacy
    /// thread-affine routing.
    pub async fn produce(
        &mut self,
        namespace: &str,
        stream: &str,
        event_type: &str,
        content: &[u8],
        shard_key: Option<&str>,
    ) -> Result<(u64, u64), BrokerError> {
        let resp = self
            .client
            .produce(ProduceRequest {
                namespace: namespace.into(),
                stream: stream.into(),
                event_type: event_type.into(),
                content: content.to_vec(),
                shard_key: shard_key.map(String::from),
                ..Default::default()
            })
            .await?
            .into_inner();
        Ok((resp.gid, resp.seq))
    }
}
