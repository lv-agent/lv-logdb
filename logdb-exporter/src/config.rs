use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_exporter_id")]
    pub exporter_id: String,
    pub source: SourceConfig,
    #[serde(default)]
    pub scope: ScopeConfig,
    pub sink: SinkConfig,
    #[serde(default)]
    pub progress: ProgressConfig,
    #[serde(default)]
    pub report_progress: ReportProgressConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
}

fn default_exporter_id() -> String {
    "logdb-exporter".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub addrs: Vec<String>,
    #[serde(default = "default_true")]
    pub prefer_primary: bool,
    #[serde(default)]
    pub tls: TlsConfig,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default = "default_tls_mode")]
    pub mode: TlsMode,
    #[serde(default)]
    pub cert_file: Option<String>,
    #[serde(default)]
    pub key_file: Option<String>,
    #[serde(default)]
    pub ca_file: Option<String>,
}

fn default_tls_mode() -> TlsMode {
    TlsMode::Disabled
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    #[default]
    Disabled,
    Tls,
    Mtls,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeConfig {
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub stream: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    #[serde(rename = "type")]
    pub sink_type: String,
    #[serde(default)]
    pub stdout: StdoutConfig,
    #[serde(default)]
    pub clickhouse: ClickHouseConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StdoutConfig {
    #[serde(default = "default_json_line")]
    pub format: String,
}

fn default_json_line() -> String {
    "json_line".into()
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub table: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
}

fn default_batch_size() -> usize {
    10000
}
fn default_flush_interval_ms() -> u64 {
    5000
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgressConfig {
    #[serde(default = "default_progress_file")]
    pub file: PathBuf,
    #[serde(default = "default_checkpoint_interval_ms")]
    pub checkpoint_interval_ms: u64,
}

fn default_progress_file() -> PathBuf {
    PathBuf::from("/var/lib/logdb-exporter/progress.dat")
}
fn default_checkpoint_interval_ms() -> u64 {
    1000
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportProgressConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_report_interval_ms")]
    pub interval_ms: u64,
}

fn default_report_interval_ms() -> u64 {
    5000
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    #[serde(default = "default_scan_batch")]
    pub scan_batch_size: usize,
    #[serde(default = "default_tail_batch")]
    pub tail_batch_size: u32,
}

fn default_scan_batch() -> usize {
    10000
}
fn default_tail_batch() -> u32 {
    1000
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            scan_batch_size: 10000,
            tail_batch_size: 1000,
        }
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path).map_err(|e| format!("read: {}", e))?;
        let config: Self = serde_yaml::from_str(&raw).map_err(|e| format!("parse: {}", e))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.source.addrs.is_empty() {
            return Err("source.addrs must not be empty".into());
        }
        Ok(())
    }
}
