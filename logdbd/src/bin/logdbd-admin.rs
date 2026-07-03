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
    if args.len() < 3 {
        usage(&args[0]);
        return Ok(());
    }
    let cmd = &args[1];
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
        "backup" => cmd_backup(&mut client).await?,
        _ => usage(&args[0]),
    }
    Ok(())
}

fn usage(prog: &str) {
    eprintln!("Usage:");
    eprintln!("  {} status      <addr>", prog);
    eprintln!("  {} list        <addr>", prog);
    eprintln!("  {} streams     <addr> <namespace>", prog);
    eprintln!("  {} ping        <addr>", prog);
    eprintln!("  {} append      <addr> <ns> <stream> <message>", prog);
    eprintln!("  {} checkpoint  <addr>", prog);
    eprintln!("  {} backup      <addr>  (prints rsync instructions)", prog);
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

async fn cmd_backup(
    client: &mut LogDbServiceClient<tonic::transport::Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Advance checkpoint to current durable position
    let status = client.status(StatusRequest {}).await?.into_inner();
    let seq = status.durable_sequence;
    client
        .checkpoint(CheckpointRequest { sequence: seq })
        .await?;

    println!("# Backup Instructions");
    println!("# WAL checkpoint advanced to {}", seq);
    println!("# Run on the logdbd server:");
    println!();
    println!("  BACKUP_DIR=/backup/logdbd-$(date +%Y%m%d-%H%M%S)");
    println!("  mkdir -p $BACKUP_DIR");
    println!("  rsync -av --delete <DATA_DIR>/ $BACKUP_DIR/");
    println!();
    println!("# To restore:");
    println!("  systemctl stop logdbd");
    println!("  rsync -av $BACKUP_DIR/ <DATA_DIR>/");
    println!("  systemctl start logdbd");
    println!();
    println!("# Segment files are immutable; rsync is safe because checkpoint");
    println!("# ensures records before {} are fully durable.", seq);
    Ok(())
}
