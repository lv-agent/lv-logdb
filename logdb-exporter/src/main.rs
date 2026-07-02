use logdb_exporter::sink::stdout::StdoutSink;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .ok_or("Usage: logdb-exporter <config.yaml>")?;

    let config = logdb_exporter::config::Config::load(std::path::Path::new(&config_path))?;

    let ns = &config.scope.namespace;
    let stream = &config.scope.stream;

    let sink: Box<dyn logdb_exporter::sink::Sink> = match config.sink.sink_type.as_str() {
        "stdout" => Box::new(StdoutSink),
        "clickhouse" => {
            #[cfg(feature = "clickhouse")]
            {
                Box::new(logdb_exporter::sink::clickhouse::ClickHouseSink::new(
                    &config.sink.clickhouse, ns, stream,
                ))
            }
            #[cfg(not(feature = "clickhouse"))]
            return Err("clickhouse feature not enabled".into());
        }
        other => return Err(format!("unknown sink type: {}", other).into()),
    };

    logdb_exporter::pipeline::run(config, sink).await?;
    Ok(())
}
