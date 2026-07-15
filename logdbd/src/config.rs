//! Configuration loading and validation.
//!
//! Loads a `logdbd.yaml` file, substitutes `${ENV_VAR}` placeholders,
//! and validates all fields before the daemon starts.

use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Top-level logdbd configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: NodeConfig,
    #[serde(default)]
    pub server: ServerConfig,
    pub logdb: LogDbConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub audit: AuditConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub id: String,
    pub role: NodeRole,
    pub cluster_id: String,
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Primary,
    Standby,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Standby => write!(f, "standby"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default = "default_tail_heartbeat_ms")]
    pub tail_heartbeat_interval_ms: u64,
}

fn default_tail_heartbeat_ms() -> u64 {
    1000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:50051".into(),
            tls: TlsConfig::default(),
            auth: AuthConfig::default(),
            tail_heartbeat_interval_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
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

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            mode: TlsMode::Disabled,
            cert_file: None,
            key_file: None,
            ca_file: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    Mtls,
    Tls,
    Disabled,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default = "default_auth_type")]
    pub r#type: AuthType,
    #[serde(default)]
    pub token_file: Option<String>,
    #[serde(default)]
    pub tokens: Vec<TokenConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConfig {
    pub token: String,
    #[serde(default = "default_token_roles")]
    pub roles: Vec<String>,
}

fn default_token_roles() -> Vec<String> {
    vec!["admin".into()]
}

fn default_auth_type() -> AuthType {
    AuthType::Token
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            r#type: AuthType::Token,
            token_file: None,
            tokens: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    Token,
    Mtls,
    Both,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogDbConfig {
    pub data_dir: PathBuf,
    #[serde(default = "default_shards")]
    pub shards: usize,
    #[serde(default = "default_segment_size")]
    pub segment_size: u64,
    #[serde(default = "default_ring_size")]
    pub ring_size: usize,
    #[serde(default = "default_durability_mode")]
    pub durability_mode: DurabilityMode,
    #[serde(default = "default_flush_timeout_ms")]
    pub flush_timeout_ms: u64,
    #[serde(default)]
    pub backpressure: BackpressureConfig,
}

fn default_shards() -> usize {
    4
}
fn default_segment_size() -> u64 {
    256 * 1024 * 1024
} // 256 MiB
fn default_ring_size() -> usize {
    65536
}
fn default_durability_mode() -> DurabilityMode {
    DurabilityMode::Sync
}
fn default_flush_timeout_ms() -> u64 {
    5000
}

impl Default for LogDbConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/logdbd"),
            shards: 4,
            segment_size: 256 * 1024 * 1024,
            ring_size: 65536,
            durability_mode: DurabilityMode::Sync,
            flush_timeout_ms: 5000,
            backpressure: BackpressureConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityMode {
    Sync,
    Batch,
    Async,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackpressureConfig {
    #[serde(default = "default_backpressure_policy")]
    pub policy: BackpressurePolicy,
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,
}

fn default_backpressure_policy() -> BackpressurePolicy {
    BackpressurePolicy::Block
}
fn default_max_in_flight() -> usize {
    65536
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            policy: BackpressurePolicy::Block,
            max_in_flight: 65536,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackpressurePolicy {
    Block,
    Reject,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default = "default_index_stride")]
    pub index_stride: usize,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default)]
    pub encryption: EncryptionConfig,
}

fn default_index_stride() -> usize {
    1024
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            index_stride: 1024,
            compression: CompressionConfig::default(),
            encryption: EncryptionConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompressionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_compression_algo")]
    pub algorithm: String,
    #[serde(default = "default_compression_level")]
    pub level: u32,
}

fn default_true() -> bool {
    true
}
fn default_compression_algo() -> String {
    "zstd".into()
}
fn default_compression_level() -> u32 {
    1
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: "zstd".into(),
            level: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_encryption_algo")]
    pub algorithm: String,
    #[serde(default)]
    pub keys: Vec<EncryptionKey>,
    #[serde(default)]
    pub active_key_id: Option<String>,
    /// Where keys come from (cr-032 Phase 2). `file` (default) reads keys from
    /// this config; `awskms` / `vault` select out-of-tree provider crates.
    #[serde(default)]
    pub provider: ProviderType,
}

fn default_encryption_algo() -> String {
    "aes-256-gcm".into()
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: "aes-256-gcm".into(),
            keys: vec![],
            active_key_id: None,
            provider: ProviderType::default(),
        }
    }
}

/// The encryption key source (cr-032 Phase 2). `file` reads keys from the YAML
/// (built-in [`crate::crypto::FileKeyProvider`]); `awskms` / `vault` are
/// out-of-tree provider crates selected via `encryption.provider`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderType {
    /// Keys read from the config (default; built-in).
    #[default]
    File,
    /// AWS KMS — requires the out-of-tree `logdb-keyprovider-awskms` crate.
    AwsKms,
    /// HashiCorp Vault — requires the out-of-tree `logdb-keyprovider-vault` crate.
    Vault,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionKey {
    pub key_id: String,
    pub key_hex: String,
}

impl EncryptionConfig {
    /// Resolve the configured keys into a [`logdb::KeyRing`], or `Ok(None)` when
    /// encryption is disabled.
    ///
    /// This is the thin facade over the provider layer (cr-032 Phase 2): it
    /// short-circuits to `None` when disabled, then delegates to
    /// [`crate::crypto::build_provider`] so every key source — the built-in file
    /// provider and out-of-tree KMS adapters — flows through the same
    /// [`crate::crypto::KeyProvider`] port. The core only ever sees the resolved
    /// ring, never a provider, so it carries no vendor dependency. The active key
    /// is chosen by `active_key_id`; every other configured key stays in the
    /// decrypt ring so records written under a prior key remain readable after a
    /// rotation (cr-032 Phase 1).
    pub fn resolve_key_ring(&self) -> Result<Option<Arc<logdb::KeyRing>>, String> {
        if !self.enabled {
            return Ok(None);
        }
        let provider = crate::crypto::build_provider(self).map_err(|e| e.to_string())?;
        provider.resolve().map(Some).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditConfig {
    #[serde(default = "default_true")]
    pub hash_chain: bool,
    #[serde(default = "default_hash_algo")]
    pub hash_algorithm: String,
}

fn default_hash_algo() -> String {
    "blake3".into()
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            hash_chain: true,
            hash_algorithm: "blake3".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    #[serde(default = "default_max_record_size")]
    pub max_record_size: usize,
    #[serde(default = "default_max_batch_records")]
    pub max_batch_records: usize,
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    #[serde(default = "default_max_scan_limit")]
    pub max_scan_limit: usize,
    #[serde(default = "default_max_tail_batch")]
    pub max_tail_batch_size: usize,
    #[serde(default)]
    pub quotas: Vec<StreamQuota>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamQuota {
    pub namespace: String,
    pub stream: String,
    #[serde(default)]
    pub max_records: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
}

fn default_max_record_size() -> usize {
    1024 * 1024
}
fn default_max_batch_records() -> usize {
    1000
}
fn default_max_batch_bytes() -> usize {
    16 * 1024 * 1024
}
fn default_max_scan_limit() -> usize {
    10000
}
fn default_max_tail_batch() -> usize {
    10000
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_record_size: 1024 * 1024,
            max_batch_records: 1000,
            max_batch_bytes: 16 * 1024 * 1024,
            max_scan_limit: 10000,
            max_tail_batch_size: 10000,
            quotas: vec![],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    #[serde(default = "default_replication_mode")]
    pub mode: ReplicationMode,
    #[serde(default = "default_sync_policy")]
    pub sync_policy: SyncPolicy,
    #[serde(default)]
    pub required_acks: u32,
    #[serde(default = "default_sync_timeout_ms")]
    pub sync_timeout_ms: u64,
    #[serde(default = "default_on_sync_timeout")]
    pub on_sync_timeout: OnSyncTimeout,
    #[serde(default = "default_repl_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_repl_batch_bytes")]
    pub batch_bytes: usize,
    #[serde(default)]
    pub standbys: Vec<StandbyConfig>,
}

fn default_replication_mode() -> ReplicationMode {
    ReplicationMode::Sync
}
fn default_sync_policy() -> SyncPolicy {
    SyncPolicy::All
}
fn default_sync_timeout_ms() -> u64 {
    5000
}
fn default_on_sync_timeout() -> OnSyncTimeout {
    OnSyncTimeout::Fail
}
fn default_repl_batch_size() -> usize {
    1024
}
fn default_repl_batch_bytes() -> usize {
    256 * 1024
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Sync,
            sync_policy: SyncPolicy::All,
            required_acks: 0,
            sync_timeout_ms: 5000,
            on_sync_timeout: OnSyncTimeout::Fail,
            batch_size: 1024,
            batch_bytes: 256 * 1024,
            standbys: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationMode {
    Sync,
    Async,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncPolicy {
    All,
    Quorum,
    #[serde(rename = "n")]
    N,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnSyncTimeout {
    Fail,
    #[serde(rename = "async_warn")]
    AsyncWarn,
    Block,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandbyConfig {
    pub id: String,
    pub addr: String,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub auth_token_file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_max_segments")]
    pub max_segments: usize,
    #[serde(default = "default_max_age_days")]
    pub max_age_days: u32,
    #[serde(default)]
    pub require_replicated: bool,
    #[serde(default)]
    pub require_exported: bool,
}

fn default_max_segments() -> usize {
    100
}
fn default_max_age_days() -> u32 {
    7
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_segments: 100,
            max_age_days: 7,
            require_replicated: false,
            require_exported: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    #[serde(default = "default_health_bind")]
    pub health_bind: String,
    #[serde(default = "default_true")]
    pub metrics: bool,
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_health_bind() -> String {
    "0.0.0.0:9090".into()
}
fn default_metrics_bind() -> String {
    "0.0.0.0:9091".into()
}
fn default_log_level() -> String {
    "info".into()
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            health_bind: "0.0.0.0:9090".into(),
            metrics: true,
            metrics_bind: "0.0.0.0:9091".into(),
            log_level: "info".into(),
        }
    }
}

// ── Loading ──────────────────────────────────────────────────────────────────

impl Config {
    /// Load configuration from a YAML file.
    ///
    /// Reads the file, substitutes `${ENV_VAR}` placeholders, and validates.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigLoadError> {
        let raw = std::fs::read_to_string(path.as_ref()).map_err(ConfigLoadError::Io)?;
        let substituted = substitute_env(&raw)?;
        let config: Self = serde_yaml::from_str(&substituted).map_err(ConfigLoadError::Parse)?;
        config.validate()?;
        Ok(config)
    }
}

/// Substitute `${VAR_NAME}` placeholders with environment variable values.
fn substitute_env(raw: &str) -> Result<String, ConfigLoadError> {
    let mut result = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    let mut in_comment = false;

    while let Some(ch) = chars.next() {
        // Track YAML comments (# after whitespace or at line start)
        if ch == '\n' {
            in_comment = false;
            result.push(ch);
            continue;
        }
        if !in_comment
            && ch == '#'
            && (result.ends_with('\n')
                || result.is_empty()
                || result
                    .as_bytes()
                    .last()
                    .is_some_and(|&b| b == b' ' || b == b'\t'))
        {
            // Start of a YAML comment — pass through everything until EOL, no substitution
            in_comment = true;
            result.push(ch);
            continue;
        }
        if in_comment {
            result.push(ch);
            continue;
        }

        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                var_name.push(c);
            }
            let val = std::env::var(&var_name)
                .map_err(|_| ConfigLoadError::MissingEnv(var_name.clone()))?;
            result.push_str(&val);
        } else {
            result.push(ch);
        }
    }
    Ok(result)
}

// ── Validation ───────────────────────────────────────────────────────────────

/// Error returned when configuration fails validation.
#[derive(Debug)]
pub enum ConfigLoadError {
    Io(std::io::Error),
    Parse(serde_yaml::Error),
    MissingEnv(String),
    Invalid(ConfigError),
}

impl std::fmt::Display for ConfigLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "failed to read config file: {}", e),
            Self::Parse(e) => write!(f, "failed to parse config: {}", e),
            Self::MissingEnv(v) => write!(f, "environment variable '{}' not set", v),
            Self::Invalid(e) => write!(f, "invalid config: {}", e),
        }
    }
}

impl std::error::Error for ConfigLoadError {}

#[derive(Debug)]
pub enum ConfigError {
    NodeIdEmpty,
    InvalidClusterId(String),
    InvalidEpoch(u64),
    NamespaceName(String),
    StreamName(String),
    MetadataKey(String),
    EventType(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeIdEmpty => write!(f, "node.id must not be empty"),
            Self::InvalidClusterId(s) => {
                write!(f, "node.cluster_id '{}' must be [a-z0-9_-], 1-64 chars", s)
            }
            Self::InvalidEpoch(e) => write!(f, "node.epoch must be >= 1, got {}", e),
            Self::NamespaceName(s) => write!(f, "namespace name '{}' invalid", s),
            Self::StreamName(s) => write!(f, "stream name '{}' invalid", s),
            Self::MetadataKey(s) => write!(f, "metadata key '{}' invalid", s),
            Self::EventType(s) => write!(f, "event_type '{}' invalid", s),
        }
    }
}

impl Config {
    fn validate(&self) -> Result<(), ConfigLoadError> {
        // node.id
        if self.node.id.is_empty() {
            return Err(ConfigLoadError::Invalid(ConfigError::NodeIdEmpty));
        }

        // node.cluster_id: [a-z0-9_-], 1-64
        validate_name(&self.node.cluster_id, 1, 64, false).map_err(|_| {
            ConfigLoadError::Invalid(ConfigError::InvalidClusterId(self.node.cluster_id.clone()))
        })?;

        // node.epoch >= 1
        if self.node.epoch < 1 {
            return Err(ConfigLoadError::Invalid(ConfigError::InvalidEpoch(
                self.node.epoch,
            )));
        }

        Ok(())
    }
}

/// Validate a name string: lowercase alphanumeric + allowed special chars, length bounds.
///
/// If `allow_slash` is true, `/` is permitted (for stream names).
pub fn validate_name(
    name: &str,
    min_len: usize,
    max_len: usize,
    allow_slash: bool,
) -> Result<(), String> {
    if name.len() < min_len || name.len() > max_len {
        return Err(format!(
            "name '{}' length {} not in [{}, {}]",
            name,
            name.len(),
            min_len,
            max_len
        ));
    }

    // Reject leading underscore (reserved for system)
    if name.starts_with('_') {
        return Err(format!("name '{}' must not start with '_'", name));
    }

    if allow_slash {
        // Stream name: [a-z0-9_/-], no leading /, no //, no trailing /
        if name.starts_with('/') {
            return Err(format!("stream name '{}' must not start with '/'", name));
        }
        if name.ends_with('/') {
            return Err(format!("stream name '{}' must not end with '/'", name));
        }
        if name.contains("//") {
            return Err(format!("stream name '{}' must not contain '//'", name));
        }
        for ch in name.chars() {
            if !ch.is_ascii_lowercase()
                && !ch.is_ascii_digit()
                && ch != '_'
                && ch != '-'
                && ch != '/'
            {
                return Err(format!(
                    "stream name '{}' contains invalid character '{}'",
                    name, ch
                ));
            }
        }
    } else {
        // Namespace / cluster_id / event_type / metadata key: [a-z0-9_-] (+ . for event_type)
        for ch in name.chars() {
            if !ch.is_ascii_lowercase() && !ch.is_ascii_digit() && ch != '_' && ch != '-' {
                return Err(format!(
                    "name '{}' contains invalid character '{}'",
                    name, ch
                ));
            }
        }
    }

    Ok(())
}

/// Validate a namespace name (1-64, [a-z0-9_-], no leading _).
pub fn validate_namespace_name(name: &str) -> Result<(), String> {
    validate_name(name, 1, 64, false)
}

/// Validate a stream name (1-128, [a-z0-9_/-], no leading _ or /, no //, no trailing /).
pub fn validate_stream_name(name: &str) -> Result<(), String> {
    validate_name(name, 1, 128, true)
}

/// Validate an event_type (1-128, [a-z0-9_.-]).
pub fn validate_event_type(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 128 {
        return Err(format!(
            "event_type '{}' length {} not in [1, 128]",
            name,
            name.len()
        ));
    }
    for ch in name.chars() {
        if !ch.is_ascii_lowercase() && !ch.is_ascii_digit() && ch != '_' && ch != '-' && ch != '.' {
            return Err(format!(
                "event_type '{}' contains invalid character '{}'",
                name, ch
            ));
        }
    }
    Ok(())
}

/// Validate a metadata key (1-64, [a-z0-9_.-]).
pub fn validate_metadata_key(key: &str) -> Result<(), String> {
    if key.is_empty() || key.len() > 64 {
        return Err(format!(
            "metadata key '{}' length {} not in [1, 64]",
            key,
            key.len()
        ));
    }
    for ch in key.chars() {
        if !ch.is_ascii_lowercase() && !ch.is_ascii_digit() && ch != '_' && ch != '-' && ch != '.' {
            return Err(format!(
                "metadata key '{}' contains invalid character '{}'",
                key, ch
            ));
        }
    }
    Ok(())
}

// Provide a Default for Config that passes validation
impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                id: "logdbd".into(),
                role: NodeRole::Primary,
                cluster_id: "default".into(),
                epoch: 1,
            },
            server: ServerConfig::default(),
            logdb: LogDbConfig::default(),
            storage: StorageConfig::default(),
            audit: AuditConfig::default(),
            limits: LimitsConfig::default(),
            replication: ReplicationConfig::default(),
            retention: RetentionConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    // ── EncryptionConfig::resolve_key_ring (cr-032) ────────────────────────

    /// Build an enabled EncryptionConfig from `(key_id, key_hex)` pairs.
    fn enc(keys: Vec<(&str, String)>, active: Option<&str>) -> EncryptionConfig {
        let mut e = EncryptionConfig::default();
        e.enabled = true;
        e.active_key_id = active.map(String::from);
        e.keys = keys
            .into_iter()
            .map(|(id, hex)| EncryptionKey {
                key_id: id.into(),
                key_hex: hex,
            })
            .collect();
        e
    }

    #[test]
    fn resolve_key_ring_disabled_is_none() {
        assert!(EncryptionConfig::default()
            .resolve_key_ring()
            .unwrap()
            .is_none());
    }

    #[test]
    fn resolve_key_ring_single_key_resolves() {
        let e = enc(vec![("k1", "42".repeat(32))], Some("k1"));
        assert!(e.resolve_key_ring().unwrap().is_some());
    }

    #[test]
    fn resolve_key_ring_rejects_unsupported_algorithm() {
        let mut e = enc(vec![("k1", "42".repeat(32))], Some("k1"));
        e.algorithm = "aes-128-gcm".into();
        let err = e.resolve_key_ring().unwrap_err();
        assert!(err.contains("aes-256-gcm"), "{err}");
    }

    #[test]
    fn resolve_key_ring_rejects_empty_keys() {
        let e = enc(vec![], Some("k1"));
        let err = e.resolve_key_ring().unwrap_err();
        assert!(err.contains("keys is empty"), "{err}");
    }

    #[test]
    fn resolve_key_ring_rejects_bad_hex_length() {
        let e = enc(vec![("k1", "11223344".into())], Some("k1")); // 4 bytes
        let err = e.resolve_key_ring().unwrap_err();
        assert!(err.contains("32 bytes"), "{err}");
    }

    #[test]
    fn resolve_key_ring_rejects_unset_active() {
        let e = enc(vec![("k1", "42".repeat(32))], None);
        let err = e.resolve_key_ring().unwrap_err();
        assert!(err.contains("active_key_id is unset"), "{err}");
    }

    #[test]
    fn resolve_key_ring_rejects_unknown_active() {
        let e = enc(vec![("k1", "42".repeat(32))], Some("nope"));
        let err = e.resolve_key_ring().unwrap_err();
        assert!(err.contains("not found"), "{err}");
    }

    /// cr-032 Phase 2: the file provider reads keys through `Config::load`, which
    /// substitutes `${ENV_VAR}` at load time — so a key hex can be supplied via
    /// the environment without ever appearing in the YAML on disk.
    #[test]
    fn encryption_key_hex_can_come_from_env() {
        // Unique var name so parallel tests never collide on the shared env.
        const VAR: &str = "LOGDBD_TEST_ENC_KEY_HEX_CR032";
        const KEY_HEX: &str = "4242424242424242424242424242424242424242424242424242424242424242";
        // SAFETY: unique var name — no other code reads/writes it, so no data race.
        unsafe { std::env::set_var(VAR, KEY_HEX); }

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node:
  id: "primary-1"
  role: primary
  cluster_id: "c"
  epoch: 1
logdb:
  data_dir: "/var/lib/logdbd"
storage:
  encryption:
    enabled: true
    active_key_id: "k1"
    keys:
      - key_id: "k1"
        key_hex: "${LOGDBD_TEST_ENC_KEY_HEX_CR032}"
"#,
        );
        let cfg = Config::load(&p).unwrap();
        // The env-supplied hex resolved into a real key ring.
        assert!(cfg.storage.encryption.resolve_key_ring().unwrap().is_some());

        // SAFETY: unique var name — no other code reads/writes it.
        unsafe { std::env::remove_var(VAR); }
    }

    /// cr-032 Phase 2: `encryption.provider` defaults to `file` and parses the
    /// other types so operators get a clear error if they select a provider that
    /// is not built into this `logdbd`.
    #[test]
    fn provider_type_parses_and_defaults_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node: { id: "n", role: primary, cluster_id: "c", epoch: 1 }
logdb: { data_dir: "/var/lib/logdbd" }
storage:
  encryption:
    enabled: true
    provider: awskms
"#,
        );
        let cfg = Config::load(&p).unwrap();
        assert_eq!(cfg.storage.encryption.provider, super::ProviderType::AwsKms);
        // Default (omitted) is file:
        let p2 = dir.path().join("b.yaml");
        write_temp(
            &p2,
            r#"
node: { id: "n", role: primary, cluster_id: "c", epoch: 1 }
logdb: { data_dir: "/var/lib/logdbd" }
storage: { encryption: { enabled: false } }
"#,
        );
        let cfg2 = Config::load(&p2).unwrap();
        assert_eq!(cfg2.storage.encryption.provider, super::ProviderType::File);
    }

    #[test]
    fn load_minimal_config() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node:
  id: "primary-1"
  role: primary
  cluster_id: "agent-audit-prod"
  epoch: 1
logdb:
  data_dir: "/var/lib/logdbd"
"#,
        );
        let cfg = Config::load(&p).unwrap();
        assert_eq!(cfg.node.id, "primary-1");
        assert_eq!(cfg.node.role, NodeRole::Primary);
        assert_eq!(cfg.node.cluster_id, "agent-audit-prod");
        assert_eq!(cfg.node.epoch, 1);
        // defaults
        assert_eq!(cfg.server.bind, "127.0.0.1:50051");
        assert_eq!(cfg.logdb.shards, 4);
        assert_eq!(cfg.logdb.durability_mode, DurabilityMode::Sync);
        assert!(cfg.audit.hash_chain);
    }

    #[test]
    fn full_config_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node:
  id: "primary-1"
  role: primary
  cluster_id: "test-cluster"
  epoch: 1
server:
  bind: "0.0.0.0:50051"
  tls:
    mode: mtls
    cert_file: "/tmp/cert.pem"
    key_file: "/tmp/key.pem"
    ca_file: "/tmp/ca.pem"
  auth:
    type: both
    token_file: "/tmp/token"
logdb:
  data_dir: "/var/lib/logdbd"
  shards: 2
  segment_size: 134217728
  ring_size: 32768
  durability_mode: batch
  flush_timeout_ms: 3000
storage:
  index_stride: 512
  compression:
    enabled: true
    algorithm: zstd
    level: 3
audit:
  hash_chain: true
  hash_algorithm: sha256
replication:
  mode: sync
  sync_policy: quorum
  sync_timeout_ms: 10000
  on_sync_timeout: async_warn
  standbys:
    - id: "sb-1"
      addr: "standby1:50051"
"#,
        );
        let cfg = Config::load(&p).unwrap();
        assert_eq!(cfg.node.cluster_id, "test-cluster");
        assert_eq!(cfg.server.tls.mode, TlsMode::Mtls);
        assert_eq!(cfg.server.auth.r#type, AuthType::Both);
        assert_eq!(cfg.logdb.shards, 2);
        assert_eq!(cfg.logdb.durability_mode, DurabilityMode::Batch);
        assert_eq!(cfg.storage.index_stride, 512);
        assert_eq!(cfg.storage.compression.level, 3);
        assert_eq!(cfg.audit.hash_algorithm, "sha256");
        assert_eq!(cfg.replication.mode, ReplicationMode::Sync);
        assert_eq!(cfg.replication.sync_policy, SyncPolicy::Quorum);
        assert_eq!(cfg.replication.on_sync_timeout, OnSyncTimeout::AsyncWarn);
        assert_eq!(cfg.replication.standbys.len(), 1);
        assert_eq!(cfg.replication.standbys[0].id, "sb-1");
    }

    #[test]
    fn env_substitution() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node:
  id: "${MY_NODE_ID}"
  role: primary
  cluster_id: "${MY_CLUSTER}"
  epoch: 1
logdb:
  data_dir: "/var/lib/logdbd"
"#,
        );
        // SAFETY: test runs single-threaded
        unsafe {
            std::env::set_var("MY_NODE_ID", "node-007");
            std::env::set_var("MY_CLUSTER", "prod-cluster");
        }
        let cfg = Config::load(&p).unwrap();
        assert_eq!(cfg.node.id, "node-007");
        assert_eq!(cfg.node.cluster_id, "prod-cluster");
    }

    #[test]
    fn env_missing_reports_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node:
  id: "${MISSING_VAR}"
  role: primary
  cluster_id: "test"
  epoch: 1
logdb:
  data_dir: "/var/lib/logdbd"
"#,
        );
        let err = Config::load(&p).unwrap_err();
        assert!(err.to_string().contains("MISSING_VAR"));
    }

    #[test]
    fn reject_empty_node_id() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node:
  id: ""
  role: primary
  cluster_id: "test"
  epoch: 1
logdb:
  data_dir: "/var/lib/logdbd"
"#,
        );
        let err = Config::load(&p).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn reject_invalid_cluster_id_uppercase() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node:
  id: "n1"
  role: primary
  cluster_id: "BadCluster"
  epoch: 1
logdb:
  data_dir: "/var/lib/logdbd"
"#,
        );
        let err = Config::load(&p).unwrap_err();
        assert!(err.to_string().contains("cluster_id"));
    }

    #[test]
    fn reject_epoch_zero() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logdbd.yaml");
        write_temp(
            &p,
            r#"
node:
  id: "n1"
  role: primary
  cluster_id: "test"
  epoch: 0
logdb:
  data_dir: "/var/lib/logdbd"
"#,
        );
        let err = Config::load(&p).unwrap_err();
        assert!(err.to_string().contains("epoch"));
    }

    #[test]
    fn validate_namespace_name_ok() {
        assert!(validate_namespace_name("org-a").is_ok());
        assert!(validate_namespace_name("team_platform").is_ok());
        assert!(validate_namespace_name("a").is_ok());
        assert!(validate_namespace_name(&"x".repeat(64)).is_ok());
    }

    #[test]
    fn validate_namespace_name_bad() {
        assert!(validate_namespace_name("").is_err());
        assert!(validate_namespace_name("Org-A").is_err()); // uppercase
        assert!(validate_namespace_name("_system").is_err()); // leading _
        assert!(validate_namespace_name(&"x".repeat(65)).is_err()); // too long
    }

    #[test]
    fn validate_stream_name_ok() {
        assert!(validate_stream_name("user-1/session-abc").is_ok());
        assert!(validate_stream_name("agent-audit").is_ok());
        assert!(validate_stream_name("a/b/c").is_ok());
        assert!(validate_stream_name(&"x".repeat(128)).is_ok());
    }

    #[test]
    fn validate_stream_name_bad() {
        assert!(validate_stream_name("").is_err());
        assert!(validate_stream_name("/leading-slash").is_err());
        assert!(validate_stream_name("trailing-slash/").is_err());
        assert!(validate_stream_name("double//slash").is_err());
        assert!(validate_stream_name("_reserved").is_err());
        assert!(validate_stream_name(&"x".repeat(129)).is_err());
    }

    #[test]
    fn validate_event_type_ok() {
        assert!(validate_event_type("llm.call").is_ok());
        assert!(validate_event_type("tool_execute").is_ok());
        assert!(validate_event_type("user.input").is_ok());
    }

    #[test]
    fn validate_event_type_bad() {
        assert!(validate_event_type("").is_err());
        assert!(validate_event_type("UPPER").is_err());
    }

    #[test]
    fn validate_metadata_key_ok() {
        assert!(validate_metadata_key("model").is_ok());
        assert!(validate_metadata_key("provider.name").is_ok());
    }

    #[test]
    fn validate_metadata_key_bad() {
        assert!(validate_metadata_key("").is_err());
        assert!(validate_metadata_key("BadKey").is_err());
    }

    #[test]
    fn default_config_is_valid() {
        // The Default impl should pass validation
        let mut cfg = Config::default();
        cfg.node = NodeConfig {
            id: "default-node".into(),
            role: NodeRole::Primary,
            cluster_id: "default-cluster".into(),
            epoch: 1,
        };
        cfg.logdb.data_dir = PathBuf::from("/tmp/logdbd-test");
        assert!(cfg.validate().is_ok());
    }
}
