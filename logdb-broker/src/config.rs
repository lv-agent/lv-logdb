//! Broker configuration.

use std::path::PathBuf;

/// Broker configuration (loaded from YAML via `LOGDB_BROKER_CONFIG`).
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfig {
    /// Address the broker gRPC server binds (e.g. "127.0.0.1:9091").
    pub bind_addr: String,
    /// Unique id for this broker instance (used in leader election, cr-037 E).
    /// Default: "broker-1". Set to distinct values per instance for HA.
    #[serde(default = "default_broker_id")]
    pub broker_id: String,
    /// logdbd address the broker Tails from / Appends to (symmetric gateway).
    /// When `embedded` is true this is the bind address for the in-process
    /// logdbd that the broker starts — set to e.g. "127.0.0.1:0" for auto-port.
    pub logdbd_addr: String,
    /// When true, start an embedded in-process logdbd instead of connecting to
    /// an external one. Single-binary development / low-volume deployments.
    #[serde(default)]
    pub embedded: bool,
    /// Path to the logdb data directory (only used when `embedded` is true).
    /// Defaults to `./data` under the broker's working directory.
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    /// Number of shards in the logdbd stream(s). Must match logdbd's `shards`
    /// config — the broker assigns these round-robin.
    pub num_shards: u32,
    /// Optional Prometheus `/metrics` endpoint (e.g. "127.0.0.1:9100").
    #[serde(default)]
    pub metrics_addr: Option<String>,
    /// Consumer session timeout in ms (0 = no eviction).
    #[serde(default)]
    pub session_timeout_ms: u64,
}

fn default_broker_id() -> String {
    "broker-1".into()
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9091".into(),
            broker_id: default_broker_id(),
            logdbd_addr: "http://127.0.0.1:9090".into(),
            embedded: false,
            data_dir: None,
            num_shards: 1,
            metrics_addr: None,
            session_timeout_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sample_config() {
        let yaml = "\
bind_addr: \"0.0.0.0:9091\"
logdbd_addr: \"http://logdbd:50051\"
num_shards: 4
metrics_addr: \"0.0.0.0:9100\"
";
        let cfg: BrokerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.bind_addr, "0.0.0.0:9091");
        assert_eq!(cfg.logdbd_addr, "http://logdbd:50051");
        assert_eq!(cfg.num_shards, 4);
        assert!(!cfg.embedded);
        assert_eq!(cfg.metrics_addr.as_deref(), Some("0.0.0.0:9100"));
    }

    #[test]
    fn metrics_addr_is_optional() {
        let yaml = "\
bind_addr: \"127.0.0.1:9091\"
logdbd_addr: \"http://127.0.0.1:9090\"
num_shards: 2
";
        let cfg: BrokerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.metrics_addr.is_none());
    }

    #[test]
    fn embedded_with_data_dir() {
        let yaml = "\
bind_addr: \"127.0.0.1:9091\"
logdbd_addr: \"127.0.0.1:0\"
num_shards: 4
embedded: true
data_dir: \"./db\"
";
        let cfg: BrokerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.embedded);
        assert_eq!(cfg.data_dir.as_deref(), Some(std::path::Path::new("./db")));
        assert_eq!(cfg.logdbd_addr, "127.0.0.1:0");
    }

    #[test]
    fn rejects_unknown_field() {
        let yaml = "\
bind_addr: \"x\"
logdbd_addr: \"y\"
num_shards: 1
bogus: true
";
        assert!(serde_yaml::from_str::<BrokerConfig>(yaml).is_err());
    }

    #[test]
    fn shipped_sample_config_parses() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.yaml");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read shipped config {}: {e}", path.display()));
        let cfg: BrokerConfig = serde_yaml::from_str(&raw)
            .unwrap_or_else(|e| panic!("parse shipped config {}: {e}", path.display()));
        assert!(cfg.num_shards >= 1);
        assert!(!cfg.bind_addr.is_empty());
    }
}
