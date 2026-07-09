//! Broker configuration.

use std::path::PathBuf;

/// Broker configuration (loaded from YAML via `LOGDB_BROKER_CONFIG`).
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfig {
    /// Address the broker gRPC server binds (e.g. "127.0.0.1:9091").
    pub bind_addr: String,
    /// logdbd address the broker Tails from / Appends to (symmetric gateway).
    pub logdbd_addr: String,
    /// Number of shards in the logdbd stream(s). Must match logdbd's `shards`
    /// config — the broker assigns these shards round-robin across consumers.
    //
    // TODO(discovery): auto-discover num_shards from logdbd instead of
    // requiring the operator to keep them in sync.  Options: (a) expose
    // num_shards on a logdbd RPC (Status / Watermark), (b) broker queries
    // logdbd at startup, (c) broker infers from the first append response.
    pub num_shards: u32,
    /// Optional Prometheus `/metrics` endpoint (e.g. "127.0.0.1:9100"). Absent
    /// ⇒ metrics counters are emitted to the facade but not scraped.
    #[serde(default)]
    pub metrics_addr: Option<String>,
    /// Consumer session timeout in milliseconds. A consumer that misses this
    /// many ms of heartbeats is evicted (0 or absent ⇒ no eviction).
    #[serde(default)]
    pub session_timeout_ms: u64,
    /// Optional path to persist coordination state (Phase 6; currently offsets
    /// are event-sourced into logdbd's meta stream regardless of this field).
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9091".into(),
            logdbd_addr: "http://127.0.0.1:9090".into(),
            num_shards: 1,
            metrics_addr: None,
            session_timeout_ms: 0,
            data_dir: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sample_config() {
        // Mirrors logdb-broker/config.yaml (the shipped sample).
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
