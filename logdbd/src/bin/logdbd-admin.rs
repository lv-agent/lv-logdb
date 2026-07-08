//! logdbd-admin — command-line management tool for logdbd clusters.
//!
//! Usage:
//!   logdbd-admin status <addr>              — show cluster status
//!   logdbd-admin list <addr>                — list all namespaces
//!   logdbd-admin streams <addr> <namespace> — list streams in a namespace
//!   logdbd-admin ping <addr>                — health check
//!   logdbd-admin append <addr> <ns> <stream> <msg> — append a record

use logdbd::pb::log_db_service_client::LogDbServiceClient;
use logdbd::pb::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage(&args[0]);
        return Ok(());
    }
    let cmd = &args[1];

    // Local (filesystem) commands — operate on a stopped node, no server needed.
    match cmd.as_str() {
        "backup" => {
            let data_dir = flag(&args, "--data-dir")?.ok_or("--data-dir required")?;
            let out = flag(&args, "--out")?.ok_or("--out required")?;
            cmd_backup_local(std::path::Path::new(data_dir), std::path::Path::new(out))?;
            return Ok(());
        }
        "restore" => {
            let backup_path = flag(&args, "--backup")?.ok_or("--backup required")?;
            let data_dir = flag(&args, "--data-dir")?.ok_or("--data-dir required")?;
            let verify = args.iter().any(|a| a == "--verify");
            // Encryption keys for --verify: load the server config (the same
            // YAML the primary runs with) and resolve its key ring. Without
            // these, recovery silently drops encrypted frames and the verify
            // would pass on an empty database.
            let key_ring = match flag(&args, "--config")? {
                Some(cfg_path) => {
                    let cfg = logdbd::config::Config::load(&cfg_path)
                        .map_err(|e| format!("load --config {cfg_path}: {e}"))?;
                    cfg.storage
                        .encryption
                        .resolve_key_ring()
                        .map_err(|e| format!("encryption config: {e}"))?
                }
                None => None,
            };
            cmd_restore(
                std::path::Path::new(backup_path),
                std::path::Path::new(data_dir),
                verify,
                key_ring,
            )?;
            return Ok(());
        }
        _ => {}
    }

    // Remote commands — connect to <addr>.
    if args.len() < 3 {
        usage(&args[0]);
        return Ok(());
    }
    let addr = &args[2];
    let url = if addr.starts_with("http") {
        addr.clone()
    } else {
        format!("http://{}", addr)
    };

    let mut client = LogDbServiceClient::connect(url).await?;

    match cmd.as_str() {
        "status" => cmd_status(&mut client).await?,
        "list" => cmd_list(&mut client).await?,
        "streams" => {
            let ns = args.get(3).ok_or("namespace required")?;
            cmd_streams(&mut client, ns).await?;
        }
        "ping" => cmd_ping(&mut client).await?,
        "append" => {
            let ns = args.get(3).ok_or("namespace required")?;
            let stream = args.get(4).ok_or("stream required")?;
            let msg = args.get(5).ok_or("message required")?;
            cmd_append(&mut client, ns, stream, msg).await?;
        }
        "checkpoint" => cmd_checkpoint(&mut client).await?,
        _ => usage(&args[0]),
    }
    Ok(())
}

/// Extract the value of `--flag <value>` from args (returns None if absent).
fn flag<'a>(
    args: &'a [String],
    name: &str,
) -> Result<Option<&'a str>, Box<dyn std::error::Error>> {
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        if a == name {
            return match iter.next() {
                Some(v) => Ok(Some(v.as_str())),
                None => Err(format!("{name} requires a value").into()),
            };
        }
    }
    Ok(None)
}

fn usage(prog: &str) {
    eprintln!("Remote (operate on a running logdbd):");
    eprintln!("  {} status      <addr>", prog);
    eprintln!("  {} list        <addr>", prog);
    eprintln!("  {} streams     <addr> <namespace>", prog);
    eprintln!("  {} ping        <addr>", prog);
    eprintln!("  {} append      <addr> <ns> <stream> <message>", prog);
    eprintln!("  {} checkpoint  <addr>", prog);
    eprintln!();
    eprintln!("Local disaster recovery (run on a STOPPED node):");
    eprintln!(
        "  {} backup   --data-dir <dir> --out <file.logdbbak>",
        prog
    );
    eprintln!(
        "  {} restore  --backup <file.logdbbak> --data-dir <dir> [--verify] [--config <server.yaml>]",
        prog
    );
}

async fn cmd_status(
    client: &mut LogDbServiceClient<tonic::transport::Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let resp = client.status(StatusRequest {}).await?.into_inner();
    println!("Node:       {}", resp.node_id);
    println!("Durable:    {}", resp.durable_sequence);
    println!("Checkpoint: {}", resp.checkpoint);
    println!(
        "WAL used:   {} / {}",
        resp.wal_bytes_used, resp.wal_bytes_total
    );
    if resp.wal_bytes_total > 0 {
        println!(
            "WAL %:      {:.1}%",
            resp.wal_bytes_used as f64 / resp.wal_bytes_total as f64 * 100.0
        );
    }

    // Also try to get namespace/stream info
    match client.list_namespaces(ListNamespacesRequest {}).await {
        Ok(ns) => {
            let ns_list = ns.into_inner();
            println!("Namespaces: {}", ns_list.namespaces.len());
            for n in &ns_list.namespaces {
                println!("  {:>4}  {}  ({} streams)", n.id, n.name, n.stream_count);
            }
        }
        Err(_) => {}
    }
    Ok(())
}

async fn cmd_list(
    client: &mut LogDbServiceClient<tonic::transport::Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let resp = client
        .list_namespaces(ListNamespacesRequest {})
        .await?
        .into_inner();
    if resp.namespaces.is_empty() {
        println!("No namespaces found.");
    } else {
        for ns in &resp.namespaces {
            println!("{:>4}  {}  ({} streams)", ns.id, ns.name, ns.stream_count);
        }
    }
    Ok(())
}

async fn cmd_streams(
    client: &mut LogDbServiceClient<tonic::transport::Channel>,
    ns: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let resp = client
        .list_streams(ListStreamsRequest {
            namespace: ns.into(),
        })
        .await?
        .into_inner();
    if resp.streams.is_empty() {
        println!("No streams in namespace '{}'", ns);
    } else {
        for s in &resp.streams {
            println!(
                "{:>8}  {}  (seq 1-{}, {} records)",
                s.id, s.name, s.durable_seq, s.record_count
            );
        }
    }
    Ok(())
}

async fn cmd_ping(
    client: &mut LogDbServiceClient<tonic::transport::Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let resp = client.status(StatusRequest {}).await?.into_inner();
    println!(
        "OK  node={} durable={}",
        resp.node_id, resp.durable_sequence
    );
    Ok(())
}

async fn cmd_append(
    client: &mut LogDbServiceClient<tonic::transport::Channel>,
    ns: &str,
    stream: &str,
    msg: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let resp = client
        .append(AppendRequest {
            namespace: ns.into(),
            stream: stream.into(),
            event_type: "admin.cli".into(),
            content: msg.as_bytes().to_vec(),
            ..Default::default()
        })
        .await?
        .into_inner();
    println!(
        "Appended: namespace_id={} stream_id={} seq={} gid={}",
        resp.namespace_id, resp.stream_id, resp.seq, resp.gid
    );
    Ok(())
}

async fn cmd_checkpoint(
    client: &mut LogDbServiceClient<tonic::transport::Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let status = client.status(StatusRequest {}).await?.into_inner();
    let seq = status.durable_sequence;
    client
        .checkpoint(CheckpointRequest { sequence: seq })
        .await?;
    println!("WAL checkpoint advanced to {}", seq);
    println!("Records with gid < {} are now safe to archive/backup.", seq);
    Ok(())
}

/// Local backup: tar a stopped node's data_dir into a `.logdbbak` archive.
fn cmd_backup_local(
    data_dir: &std::path::Path,
    out: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = logdbd::backup::backup(data_dir, out)?;
    println!("Backup written: {}", out.display());
    println!("  source:   {}", manifest.source_data_dir);
    println!("  files:    {}", manifest.file_count);
    println!("  bytes:    {}", manifest.total_bytes);
    println!(
        "  version:  logdbd {} (format v{})",
        manifest.logdbd_version, manifest.format_version
    );
    println!("  checksum: {}.sha256", out.display());
    Ok(())
}

/// Local restore: reconstruct a data_dir from a `.logdbbak` archive.
fn cmd_restore(
    backup_path: &std::path::Path,
    data_dir: &std::path::Path,
    verify: bool,
    key_ring: Option<std::sync::Arc<logdb::KeyRing>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = logdbd::backup::restore(backup_path, data_dir, verify, key_ring)?;
    println!(
        "Restored {} → {}",
        backup_path.display(),
        data_dir.display()
    );
    println!(
        "  source: {} (format v{}, logdbd {})",
        manifest.source_data_dir, manifest.format_version, manifest.logdbd_version
    );
    println!("  files:  {}", manifest.file_count);
    if verify {
        println!("  verify: OK (recovery passed)");
    }
    Ok(())
}
