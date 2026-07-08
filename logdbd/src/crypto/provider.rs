//! The encryption key-provider port (cr-032 Phase 2).
//!
//! The core [`logdb::KeyRing`] is **pure data** — it never knows where keys
//! come from. This module defines [`KeyProvider`]: an object that resolves
//! configured keys into a `KeyRing` at startup (or on a rotation reload). The
//! provider is consulted only then — never on the per-record path — so the core
//! sees only the resolved ring, never a provider, and carries no vendor
//! dependency (cr-032 design rule: no KMS binding in the core library).
//!
//! [`FileKeyProvider`] is the only provider built into `logdbd` (keys read from
//! the YAML, after `${ENV}` interpolation by [`crate::config::Config::load`]).
//! AWS KMS / Vault adapters implement the same trait in **out-of-tree** crates
//! (`logdb-keyprovider-awskms`, …) selected by `encryption.provider`; they plug
//! in behind [`build_provider`] and never enter the `logdb` core dependency
//! graph. When the first real KMS adapter lands, this trait should be extracted
//! into a tiny dedicated port crate so adapters depend on the port, not on the
//! server library.

use std::sync::Arc;

use crate::config::{EncryptionConfig, ProviderType};

/// A source of encryption keys. Resolves configured keys into an in-memory
/// [`logdb::KeyRing`] (pure data). Built-in: [`FileKeyProvider`]. Out-of-tree:
/// AWS KMS / Vault crates implement this and register behind
/// `encryption.provider`.
pub trait KeyProvider: Send + Sync {
    /// Resolve the key ring. Called only at startup / rotation reload.
    fn resolve(&self) -> Result<Arc<logdb::KeyRing>, KeyError>;
}

/// Errors raised while resolving keys. Display strings are stable (asserted by
/// `EncryptionConfig::resolve_key_ring`'s unit tests) so they can be surfaced
/// to the operator verbatim.
#[derive(Debug)]
pub enum KeyError {
    /// `algorithm` is not the one AES-256-GCM the core hardcodes.
    UnsupportedAlgorithm(String),
    /// `keys` is empty while encryption is enabled.
    NoKeys,
    /// A key's `key_hex` is not valid hex.
    InvalidHex { key_id: String, reason: String },
    /// A key is not exactly 32 bytes.
    WrongKeyLength { key_id: String, len: usize },
    /// `active_key_id` is unset.
    NoActiveKeyId,
    /// `active_key_id` does not match any configured key.
    UnknownActiveKeyId { active: String, available: String },
    /// The configured provider type is not built into this `logdbd` (it needs
    /// its out-of-tree crate).
    ProviderUnavailable { kind: String },
}

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedAlgorithm(a) => write!(
                f,
                "unsupported encryption algorithm '{}': only 'aes-256-gcm' is supported",
                a
            ),
            Self::NoKeys => write!(
                f,
                "encryption is enabled but storage.encryption.keys is empty"
            ),
            Self::InvalidHex { key_id, reason } => {
                write!(f, "invalid hex for key '{}': {}", key_id, reason)
            }
            Self::WrongKeyLength { key_id, len } => write!(
                f,
                "key '{}' is {} bytes; AES-256 needs exactly 32 bytes (64 hex chars)",
                key_id, len
            ),
            Self::NoActiveKeyId => write!(
                f,
                "encryption is enabled but storage.encryption.active_key_id is unset"
            ),
            Self::UnknownActiveKeyId { active, available } => write!(
                f,
                "active_key_id '{}' not found among configured keys [{}]",
                active, available
            ),
            Self::ProviderUnavailable { kind } => write!(
                f,
                "encryption provider '{}' is not built into this logdbd; \
                 it requires its out-of-tree crate (cr-032 Phase 2)",
                kind
            ),
        }
    }
}

impl std::error::Error for KeyError {}

/// The built-in "file" provider: keys read from the YAML (after `${ENV}`
/// interpolation). Clones the relevant fields out of the config so the provider
/// owns its inputs and can be moved onto a `Box<dyn KeyProvider>`.
#[derive(Debug, Clone)]
pub struct FileKeyProvider {
    algorithm: String,
    keys: Vec<String>, // (key_id, key_hex) flattened into two parallel vecs
    key_hex: Vec<String>,
    active_key_id: Option<String>,
}

impl FileKeyProvider {
    /// Capture the file-source fields from a (presumably enabled) config. Does
    /// not validate — [`KeyProvider::resolve`] does, in the legacy error order
    /// (algorithm, then keys, then active), so messages stay stable.
    pub fn from_config(enc: &EncryptionConfig) -> Self {
        Self {
            algorithm: enc.algorithm.clone(),
            keys: enc.keys.iter().map(|k| k.key_id.clone()).collect(),
            key_hex: enc.keys.iter().map(|k| k.key_hex.clone()).collect(),
            active_key_id: enc.active_key_id.clone(),
        }
    }
}

impl KeyProvider for FileKeyProvider {
    fn resolve(&self) -> Result<Arc<logdb::KeyRing>, KeyError> {
        if self.algorithm != "aes-256-gcm" {
            return Err(KeyError::UnsupportedAlgorithm(self.algorithm.clone()));
        }
        if self.keys.is_empty() {
            return Err(KeyError::NoKeys);
        }

        // Decode every configured key (key_hex → 32 bytes).
        let mut decoded: Vec<(String, [u8; 32])> = Vec::with_capacity(self.keys.len());
        for (id, hex_val) in self.keys.iter().zip(self.key_hex.iter()) {
            let bytes = hex::decode(hex_val.trim()).map_err(|e| KeyError::InvalidHex {
                key_id: id.clone(),
                reason: e.to_string(),
            })?;
            let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| KeyError::WrongKeyLength {
                key_id: id.clone(),
                len: bytes.len(),
            })?;
            decoded.push((id.clone(), arr));
        }

        // Select the active key; the rest become prior (still-readable) keys.
        let active_id = self.active_key_id.as_deref().ok_or(KeyError::NoActiveKeyId)?;
        let active_idx = decoded
            .iter()
            .position(|(id, _)| id == active_id)
            .ok_or_else(|| KeyError::UnknownActiveKeyId {
                active: active_id.to_string(),
                available: self.keys.join(", "),
            })?;
        let active = decoded[active_idx].1;
        let prior: Vec<[u8; 32]> = decoded
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != active_idx)
            .map(|(_, (_, k))| *k)
            .collect();
        Ok(logdb::KeyRing::new(active, prior))
    }
}

/// Build the provider selected by `encryption.provider`. `file` is built-in;
/// `awskms` / `vault` need their out-of-tree crate and error here until wired
/// in (a no-cost compile-time guarantee that no vendor SDK is pulled in by
/// merely configuring them).
pub fn build_provider(enc: &EncryptionConfig) -> Result<Box<dyn KeyProvider>, KeyError> {
    match enc.provider {
        ProviderType::File => Ok(Box::new(FileKeyProvider::from_config(enc))),
        ProviderType::AwsKms => Err(KeyError::ProviderUnavailable {
            kind: "awskms".to_string(),
        }),
        ProviderType::Vault => Err(KeyError::ProviderUnavailable {
            kind: "vault".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A non-file provider plugs straight into `Box<dyn KeyProvider>` — proving
    /// the port is usable by an arbitrary out-of-tree adapter (the AWS KMS /
    /// Vault crates implement exactly this shape).
    struct StaticProvider {
        key: [u8; 32],
    }
    impl KeyProvider for StaticProvider {
        fn resolve(&self) -> Result<Arc<logdb::KeyRing>, KeyError> {
            Ok(logdb::KeyRing::single(self.key))
        }
    }

    #[test]
    fn arbitrary_provider_impl_is_dispatchable_via_the_trait() {
        let p: Box<dyn KeyProvider> = Box::new(StaticProvider { key: [0x42; 32] });
        let ring = p.resolve().expect("static provider resolves");
        // The ring is real data the core can consume (single active key).
        assert_eq!(format!("{ring:?}").contains("KeyRing"), true);
    }

    #[test]
    fn build_provider_file_returns_file_provider() {
        let enc = EncryptionConfig {
            enabled: true,
            algorithm: "aes-256-gcm".into(),
            keys: vec![crate::config::EncryptionKey {
                key_id: "k1".into(),
                key_hex: "42".repeat(32),
            }],
            active_key_id: Some("k1".into()),
            provider: ProviderType::File,
        };
        let p = build_provider(&enc).expect("file provider builds");
        assert!(p.resolve().is_ok(), "file provider resolves a valid ring");
    }

    #[test]
    fn build_provider_awskms_is_unavailable_without_the_crate() {
        let mut enc = EncryptionConfig::default();
        enc.enabled = true;
        enc.provider = ProviderType::AwsKms;
        let err = match build_provider(&enc) {
            Ok(_) => panic!("awskms provider must not be built without its crate"),
            Err(e) => e,
        };
        assert!(matches!(err, KeyError::ProviderUnavailable { .. }));
        assert!(err.to_string().contains("awskms"), "{}", err);
    }
}
