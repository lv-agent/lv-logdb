//! Snapshot — full segment file sync from primary to standby.
//!
//! # Primary side
//!
//! Lists all `*.log` files in the data directory and streams them in chunks.
//! Only sealed (non-active) segments are sent; the active segment is handled
//! by streaming replication after the snapshot completes.
//!
//! # Standby side
//!
//! Receives chunks into a temporary directory. After all chunks arrive,
//! atomically replaces the standby's segment files and restarts the Storage.

use std::path::{Path, PathBuf};

use crate::pb;
use crate::pb::snapshot_service_server::SnapshotService;
use tonic::{Request, Response, Status};

const CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB

// ── Primary: SnapshotService handler ──────────────────────────────────────────

pub struct SnapshotServiceImpl {
    data_dir: PathBuf,
}

impl SnapshotServiceImpl {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }
}

#[tonic::async_trait]
impl SnapshotService for SnapshotServiceImpl {
    type PullSnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<pb::SnapshotChunk, Status>>;

    async fn pull_snapshot(
        &self,
        _req: Request<pb::SnapshotRequest>,
    ) -> Result<Response<Self::PullSnapshotStream>, Status> {
        let data_dir = self.data_dir.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::task::spawn_blocking(move || {
            if let Err(e) = stream_segments(&data_dir, tx) {
                tracing::error!(error = %e, "snapshot stream failed");
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

/// Stream all segment files in data_dir, excluding the active segment.
fn stream_segments(
    data_dir: &Path,
    tx: tokio::sync::mpsc::Sender<Result<pb::SnapshotChunk, Status>>,
) -> Result<(), String> {
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(data_dir).map_err(|e| format!("read_dir: {}", e))? {
        match entry {
            Ok(e) => {
                let path = e.path();
                if path.extension().is_some_and(|ext| ext == "log")
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| !n.contains("-active"))
                {
                    files.push(path);
                }
            }
            Err(e) => tracing::warn!(error = %e, "snapshot: skipping unreadable dir entry"),
        }
    }
    files.sort();

    for path in &files {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("non-UTF-8 filename: {:?}", path.file_name()))?
            .to_string();
        let data = std::fs::read(path).map_err(|e| format!("read {}: {}", name, e))?;

        for (i, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
            let last_file = path == files.last().unwrap();
            let last_chunk = i == (data.len().saturating_sub(1) / CHUNK_SIZE);
            if tx
                .blocking_send(Ok(pb::SnapshotChunk {
                    file_name: name.clone(),
                    offset: (i * CHUNK_SIZE) as u64,
                    data: chunk.to_vec(),
                    last: last_file && last_chunk,
                }))
                .is_err()
            {
                return Ok(()); // client disconnected
            }
        }
        tracing::info!(file = %name, size = data.len(), "snapshot sent segment");
    }

    Ok(())
}

// ── Standby: receiver ─────────────────────────────────────────────────────────

/// Receive snapshot chunks from primary and install into `target_dir`.
pub async fn receive_snapshot(
    target_dir: &Path,
    stream: impl tokio_stream::Stream<Item = Result<pb::SnapshotChunk, Status>>,
) -> Result<(), String> {
    tokio::pin!(stream);
    let tmp_dir = target_dir.join("_snapshot_tmp");
    std::fs::create_dir_all(&tmp_dir).map_err(|e| format!("create tmp dir: {}", e))?;

    let mut current_file: Option<(String, std::fs::File)> = None;
    let mut files_received: Vec<String> = Vec::new();

    use tokio_stream::StreamExt;
    while let Some(chunk) = stream
        .next()
        .await
        .transpose()
        .map_err(|e| format!("recv: {}", e))?
    {
        // Open new file if needed
        if current_file
            .as_ref()
            .is_none_or(|(n, _)| *n != chunk.file_name)
        {
            // Close previous file
            if let Some((_, f)) = current_file.take() {
                f.sync_all().map_err(|e| format!("sync: {}", e))?;
            }
            let path = tmp_dir.join(&chunk.file_name);
            let f = std::fs::File::create(&path)
                .map_err(|e| format!("create {}: {}", chunk.file_name, e))?;
            files_received.push(chunk.file_name.clone());
            current_file = Some((chunk.file_name.clone(), f));
        }

        // Write chunk
        use std::io::Write;
        current_file
            .as_mut()
            .unwrap()
            .1
            .write_all(&chunk.data)
            .map_err(|e| format!("write: {}", e))?;

        if chunk.last {
            tracing::info!(files = files_received.len(), "snapshot received");
            // Close and sync
            if let Some((_, f)) = current_file.take() {
                f.sync_all().map_err(|e| format!("sync: {}", e))?;
            }

            // Atomically replace: remove old segments, move new ones in
            install_snapshot(target_dir, &tmp_dir, &files_received)?;
            return Ok(());
        }
    }

    Err("snapshot stream ended without last chunk".into())
}

fn install_snapshot(target_dir: &Path, tmp_dir: &Path, files: &[String]) -> Result<(), String> {
    // Remove existing log files
    for entry in std::fs::read_dir(target_dir).map_err(|e| format!("read_dir: {}", e))? {
        let entry = entry.map_err(|e| format!("entry: {}", e))?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "log") {
            std::fs::remove_file(&path).map_err(|e| format!("remove {:?}: {}", path, e))?;
        }
    }

    // Move new files from tmp to target
    for name in files {
        let src = tmp_dir.join(name);
        let dst = target_dir.join(name);
        std::fs::rename(&src, &dst).map_err(|e| format!("rename {}: {}", name, e))?;
    }

    // Clean up tmp dir
    if let Err(e) = std::fs::remove_dir_all(tmp_dir) {
        tracing::warn!(error = %e, "snapshot: failed to clean up temp directory");
    }

    // Sync directory
    let dir = std::fs::File::open(target_dir).map_err(|e| format!("open dir: {}", e))?;
    dir.sync_all().map_err(|e| format!("sync dir: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snapshot_stream_and_receive() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();

        // Create test segment files in src
        for i in 1..=3 {
            let data = format!("segment-{:08}.log", i).repeat(100); // ~2.2KB each
            std::fs::write(src.path().join(format!("segment-{:08}.log", i)), &data).unwrap();
        }

        // Stream from src
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let src_path = src.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            stream_segments(&src_path, tx).unwrap();
        });

        // Receive into dst
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        receive_snapshot(dst.path(), stream).await.unwrap();

        // Verify all files are in dst
        for i in 1..=3 {
            let name = format!("segment-{:08}.log", i);
            assert!(dst.path().join(&name).exists(), "missing {}", name);
        }
        // Verify no tmp dir left
        assert!(!dst.path().join("_snapshot_tmp").exists());
    }
}
