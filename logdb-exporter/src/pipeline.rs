use std::time::Duration;

use crate::config::Config;
use crate::progress::Progress;
use crate::sink::Sink;
use crate::source::Source;

pub async fn run(config: Config, mut sink: Box<dyn Sink>) -> Result<(), String> {
    let mut source = Source::connect(&config.source.addrs, config.source.tls.clone()).await?;

    // Get watermark to validate identity
    let wm = source.get_watermark(&config.scope.namespace, &config.scope.stream).await?;
    tracing::info!(
        cluster_id = %wm.node_id, role = %wm.role,
        namespace = %config.scope.namespace, stream = %config.scope.stream,
        durable_seq = wm.durable_seq, oldest_seq = wm.oldest_seq,
        "connected to logdbd"
    );

    // Load or create progress
    let mut progress = Progress::load(&config.progress.file)?
        .unwrap_or_else(|| Progress::new(
            wm.node_id.clone(), 0,
            config.scope.namespace.clone(), config.scope.stream.clone(),
            config.progress.file.clone(),
        ));

    // E2 fix: detect OUT_OF_RETENTION — exporter fell behind and old data is gone
    if wm.oldest_seq > 0 && progress.last_seq > 0 && wm.oldest_seq > progress.last_seq + 1 {
        return Err(format!(
            "OUT_OF_RETENTION: oldest available seq is {}, but exporter last exported seq is {}. \
             {} records have been permanently lost due to retention. \
             To reset the exporter to the current oldest seq, run with --reset-progress={}",
            wm.oldest_seq, progress.last_seq,
            wm.oldest_seq.saturating_sub(progress.last_seq + 1),
            wm.oldest_seq.saturating_sub(1),
        ));
    }

    // Phase 1: Scan to catch up
    let from = progress.last_seq + 1;
    if from <= wm.durable_seq {
        tracing::info!(from, to = wm.durable_seq, "scan phase");
        let chunks = source.scan(
            &config.scope.namespace, &config.scope.stream,
            from, config.pipeline.scan_batch_size as u32,
        ).await?;

        for chunk in &chunks {
            if !chunk.records.is_empty() {
                sink.push(&chunk.records).map_err(|e| format!("sink push: {}", e))?;
                progress.last_seq = chunk.records.last().unwrap().seq;
                progress.save()?;
                tracing::debug!(seq = progress.last_seq, "scan progress");
            }
        }
        tracing::info!(seq = progress.last_seq, "scan complete");
    }

    // Phase 2: Tail
    tracing::info!(from = progress.last_seq + 1, "tail phase");
    let checkpoint_interval = Duration::from_millis(config.progress.checkpoint_interval_ms);
    let mut last_save = tokio::time::Instant::now();
    loop {
        match source.tail(
            &config.scope.namespace, &config.scope.stream,
            progress.last_seq + 1, config.pipeline.tail_batch_size,
        ).await {
            Ok(mut stream) => {
                while let Some(resp) = stream.message().await
                    .map_err(|e| format!("tail recv: {}", e))?
                {
                    if resp.heartbeat || resp.records.is_empty() { continue; }
                    sink.push(&resp.records).map_err(|e| format!("sink push: {}", e))?;
                    if let Some(last) = resp.records.last() {
                        progress.last_seq = last.seq;
                        // Throttle progress saves to configured interval (P2-19 fix)
                        if last_save.elapsed() >= checkpoint_interval {
                            progress.save()?;
                            last_save = tokio::time::Instant::now();
                        }
                    }
                }
                // Save on stream end
                progress.save()?;
            }
            Err(e) => {
                tracing::warn!(error = %e, "tail disconnected, reconnecting...");
                tokio::time::sleep(Duration::from_secs(1)).await;
                source.failover().await?;
            }
        }
    }
}
