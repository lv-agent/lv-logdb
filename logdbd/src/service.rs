use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::pb;
use crate::pb::log_db_service_server::LogDbService;
use logdb::LogDb;

pub struct LogDbServiceImpl {
    db: Arc<LogDb>,
    tailer_lock: Arc<Mutex<()>>,
    hostname: String,
    role: String, // "primary" or "standby"
}

impl LogDbServiceImpl {
    pub fn new(db: Arc<LogDb>, hostname: String, role: String) -> Self {
        Self {
            db,
            tailer_lock: Arc::new(Mutex::new(())),
            hostname,
            role,
        }
    }

    fn check_write(&self) -> Result<(), Status> {
        if self.role != "primary" {
            Err(Status::permission_denied(
                "not primary — writes only accepted on primary node",
            ))
        } else {
            Ok(())
        }
    }
}

#[tonic::async_trait]
impl LogDbService for LogDbServiceImpl {
    async fn append(
        &self,
        req: Request<pb::AppendRequest>,
    ) -> Result<Response<pb::AppendResponse>, Status> {
        self.check_write()?;
        let len = req.get_ref().content.len();
        let seq = self.db.append(&req.get_ref().content).map_err(|e| {
            tracing::warn!(error = ?e, size = len, "append failed");
            Status::internal(format!("{:?}", e))
        })?;
        tracing::debug!(seq, size = len, "append");
        Ok(Response::new(pb::AppendResponse { sequence: seq }))
    }

    async fn batch_append(
        &self,
        req: Request<pb::BatchAppendRequest>,
    ) -> Result<Response<pb::AppendResponse>, Status> {
        self.check_write()?;
        let count = req.get_ref().contents.len();
        let contents: Vec<&[u8]> = req
            .get_ref()
            .contents
            .iter()
            .map(|c| c.as_slice())
            .collect();
        let seq = self.db.append_batch(&contents).map_err(|e| {
            tracing::warn!(error = ?e, count, "batch_append failed");
            Status::internal(format!("{:?}", e))
        })?;
        tracing::debug!(seq, count, "batch_append");
        Ok(Response::new(pb::AppendResponse { sequence: seq }))
    }

    async fn checkpoint(
        &self,
        req: Request<pb::CheckpointRequest>,
    ) -> Result<Response<pb::CheckpointResponse>, Status> {
        self.check_write()?;
        let seq = req.get_ref().sequence;
        self.db.checkpoint(seq);
        tracing::info!(checkpoint = seq, "checkpoint set");
        Ok(Response::new(pb::CheckpointResponse {}))
    }

    async fn read(&self, req: Request<pb::ReadRequest>) -> Result<Response<pb::Record>, Status> {
        match self
            .db
            .read(req.get_ref().sequence)
            .map_err(|e| Status::internal(format!("{:?}", e)))?
        {
            Some(rec) => Ok(Response::new(pb::Record {
                sequence: rec.id.sequence,
                timestamp_ns: rec.timestamp_ns,
                content: rec.content,
            })),
            None => Err(Status::not_found("record not found")),
        }
    }

    type ScanStream = tokio_stream::wrappers::ReceiverStream<Result<pb::Record, Status>>;

    async fn scan(
        &self,
        req: Request<pb::ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        let from = req.get_ref().from;
        let to = req.get_ref().to;
        let iter = self
            .db
            .scan(from, to)
            .map_err(|e| Status::internal(format!("{:?}", e)))?;

        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            for result in iter {
                match result {
                    Ok(rec) => {
                        // Stop early if the client disconnected — don't scan to
                        // exhaustion into a dead channel.
                        if tx
                            .send(Ok(pb::Record {
                                sequence: rec.id.sequence,
                                timestamp_ns: rec.timestamp_ns,
                                content: rec.content,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("{:?}", e)))).await;
                        return;
                    }
                }
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    type TailStream = tokio_stream::wrappers::ReceiverStream<Result<pb::Record, Status>>;

    async fn tail(
        &self,
        req: Request<pb::TailRequest>,
    ) -> Result<Response<Self::TailStream>, Status> {
        let name = req.get_ref().consumer_name.clone();
        let max_count = req.get_ref().max_count as usize;
        let _lock = self.tailer_lock.lock().await;

        let mut tailer = self.db.new_tailer(&name);

        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            loop {
                match tailer.next_batch(max_count) {
                    Ok(Some(records)) => {
                        for rec in records {
                            // Exit when the client goes away — otherwise this
                            // loop would busy-spin forever sending into a dead
                            // channel.
                            if tx
                                .send(Ok(pb::Record {
                                    sequence: rec.id.sequence,
                                    timestamp_ns: rec.timestamp_ns,
                                    content: rec.content,
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        return;
                    }
                }
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn status(
        &self,
        _req: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let (used, total) = self.db.wal_usage();
        Ok(Response::new(pb::StatusResponse {
            durable_sequence: self.db.durable_cursor(),
            checkpoint: self.db.checkpoint_sequence(),
            wal_bytes_used: used,
            wal_bytes_total: total,
            node_id: self.hostname.clone(),
        }))
    }
}
