//! File-level backup/restore for disaster recovery.
//!
//! [`backup`] writes a self-describing `.logdbbak` tar archive of a stopped
//! node's data directory (segment files, sparse indexes, the catalog snapshot,
//! consumer offsets) plus a `<file>.sha256` integrity sidecar. [`restore`]
//! verifies the checksum and reconstructs a bootable data directory, optionally
//! confirming integrity by opening the logdb — recovery re-checks per-record
//! CRC, the BLAKE3 hash chain, and torn-write truncation.
//!
//! Both operate on a **stopped** node's files (backup holds the primary
//! `active.lock` for its duration and refuses if a primary is running).

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::{Archive, Builder};

use crate::config::NodeRole;
use crate::node::{LockError, ProcessLock};

/// Magic string in every backup manifest.
const MAGIC: &str = "logdb-backup";
/// Backup format version (bump on incompatible changes).
const FORMAT_VERSION: u32 = 1;
/// Name of the manifest member inside the archive.
const MANIFEST_NAME: &str = "BACKUP_MANIFEST.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub magic: String,
    pub format_version: u32,
    pub logdbd_version: String,
    pub created_at_ns: u128,
    pub source_data_dir: String,
    pub file_count: u64,
    pub total_bytes: u64,
}

/// Errors returned by backup/restore.
#[derive(Debug)]
pub enum BackupError {
    /// `data_dir` does not look like a logdb data directory.
    NotALogdbDir(PathBuf),
    /// `active.lock` is held — a primary is running; stop it first.
    NodeRunning(PathBuf),
    Io(io::Error),
    Json(serde_json::Error),
    /// Archive is missing its manifest or the magic is wrong.
    BadMagic { expected: String, found: String },
    /// Archive checksum does not match the sidecar.
    ChecksumMismatch { expected: String, found: String },
    /// Restore target exists and is non-empty (refuse to overwrite).
    TargetNotEmpty(PathBuf),
    /// `--verify` opened the logdb and recovery failed.
    VerifyFailed(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotALogdbDir(p) => write!(
                f,
                "{} does not look like a logdb data directory \
                 (no catalog.dat or segment-*.log found)",
                p.display()
            ),
            Self::NodeRunning(p) => write!(
                f,
                "active.lock at {} is held — a primary is running; stop logdbd before backing up",
                p.display()
            ),
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Json(e) => write!(f, "manifest parse error: {}", e),
            Self::BadMagic { expected, found } => {
                write!(f, "not a logdb backup (expected magic {}, found {})", expected, found)
            }
            Self::ChecksumMismatch { expected, found } => {
                write!(f, "backup checksum mismatch: expected {}, found {}", expected, found)
            }
            Self::TargetNotEmpty(p) => write!(
                f,
                "restore target {} exists and is non-empty; refusing to overwrite",
                p.display()
            ),
            Self::VerifyFailed(msg) => write!(f, "restore verify failed: {}", msg),
        }
    }
}

impl std::error::Error for BackupError {}

impl From<io::Error> for BackupError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for BackupError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Back up `data_dir` into `out` (a `.logdbbak` path). Writes a `<out>.sha256`
/// sidecar. Refuses if `data_dir` is not a logdb dir or a primary is running.
pub fn backup(data_dir: &Path, out: &Path) -> Result<BackupManifest, BackupError> {
    validate_logdb_dir(data_dir)?;

    // Hold the primary lock so the node cannot (re)start mid-backup.
    let _lock = ProcessLock::acquire(data_dir, &NodeRole::Primary).map_err(|e| match e {
        LockError::Held(p) => BackupError::NodeRunning(p),
        other => BackupError::Io(io::Error::other(other.to_string())),
    })?;

    let file = fs::File::create(out)?;
    let mut builder = Builder::new(file);

    let (file_count, total_bytes) = add_dir_recursive(&mut builder, data_dir, "")?;

    let manifest = BackupManifest {
        magic: MAGIC.to_string(),
        format_version: FORMAT_VERSION,
        logdbd_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        source_data_dir: data_dir.display().to_string(),
        file_count,
        total_bytes,
    };
    append_manifest(&mut builder, &manifest)?;
    builder.finish()?;

    // Sidecar with the full-file sha256.
    let digest = sha256_of_file(out)?;
    let sidecar = sha256_sidecar_path(out);
    fs::write(&sidecar, format!("{}  {}\n", digest, out.file_name().and_then(|n| n.to_str()).unwrap_or("")))?;

    Ok(manifest)
}

/// Restore `backup_path` into `data_dir` (must not exist or be empty). If a
/// `<backup_path>.sha256` sidecar is present, the archive is verified first.
/// When `verify` is set, the restored data_dir is opened via `LogDb::open`
/// (recovery validates CRC + hash chain + torn writes) then closed.
pub fn restore(
    backup_path: &Path,
    data_dir: &Path,
    verify: bool,
) -> Result<BackupManifest, BackupError> {
    // Verify checksum sidecar if present.
    let sidecar = sha256_sidecar_path(backup_path);
    if sidecar.exists() {
        let expected = read_sidecar_digest(&sidecar)?;
        let actual = sha256_of_file(backup_path)?;
        if !expected.eq_ignore_ascii_case(&actual) {
            return Err(BackupError::ChecksumMismatch {
                expected,
                found: actual,
            });
        }
    }

    // Target must be absent or empty.
    if data_dir.exists() && fs::read_dir(data_dir)?.next().is_some() {
        return Err(BackupError::TargetNotEmpty(data_dir.to_path_buf()));
    }
    fs::create_dir_all(data_dir)?;

    let file = fs::File::open(backup_path)?;
    let mut archive = Archive::new(file);
    let mut manifest: Option<BackupManifest> = None;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path == Path::new(MANIFEST_NAME) {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf)?;
            manifest = Some(serde_json::from_slice(&buf)?);
        } else {
            // unpack_in is path-traversal safe (tar strips leading `/` and `..`).
            entry.unpack_in(data_dir)?;
        }
    }

    let manifest = manifest.ok_or_else(|| BackupError::BadMagic {
        expected: MAGIC.to_string(),
        found: "(no manifest member)".to_string(),
    })?;
    if manifest.magic != MAGIC {
        return Err(BackupError::BadMagic {
            expected: MAGIC.to_string(),
            found: manifest.magic,
        });
    }

    if verify {
        verify_recovers(data_dir)?;
    }
    Ok(manifest)
}

/// Open the restored data_dir to confirm recovery succeeds; close immediately.
fn verify_recovers(data_dir: &Path) -> Result<(), BackupError> {
    // Async is the cheapest durability mode for a verify-only open.
    let cfg = logdb::Config {
        data_dir: data_dir.to_path_buf(),
        durability_mode: logdb::DurabilityMode::Async,
        ..Default::default()
    };
    let db = logdb::LogDb::open(cfg).map_err(|e| BackupError::VerifyFailed(e.to_string()))?;
    match db.drain(std::time::Duration::from_secs(5)) {
        Ok(_) => Ok(()),
        Err(e) => Err(BackupError::VerifyFailed(e.to_string())),
    }
}

fn validate_logdb_dir(dir: &Path) -> Result<(), BackupError> {
    if !dir.is_dir() {
        return Err(BackupError::NotALogdbDir(dir.to_path_buf()));
    }
    let looks_like_logdb = dir.join("catalog.dat").exists()
        || fs::read_dir(dir)?.filter_map(Result::ok).any(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("segment-") && n.ends_with(".log"))
        });
    if looks_like_logdb {
        Ok(())
    } else {
        Err(BackupError::NotALogdbDir(dir.to_path_buf()))
    }
}

/// Recursively add `src`'s contents into the tar under `prefix`, returning
/// (file_count, total_bytes).
fn add_dir_recursive<W: io::Write>(
    builder: &mut Builder<W>,
    src: &Path,
    prefix: &str,
) -> Result<(u64, u64), BackupError> {
    let mut count = 0u64;
    let mut bytes = 0u64;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Skip the advisory lock file — it is process-local, not data.
        if prefix.is_empty() && name == "active.lock" {
            continue;
        }
        let path = entry.path();
        let archive_path = if prefix.is_empty() {
            name.into_owned()
        } else {
            format!("{prefix}/{name}")
        };
        let ft = entry.file_type()?;
        if ft.is_dir() {
            let (c, b) = add_dir_recursive(builder, &path, &archive_path)?;
            count += c;
            bytes += b;
        } else if ft.is_file() {
            let mut f = fs::File::open(&path)?;
            builder.append_file(&archive_path, &mut f)?;
            count += 1;
            bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    Ok((count, bytes))
}

fn append_manifest<W: io::Write>(
    builder: &mut Builder<W>,
    manifest: &BackupManifest,
) -> Result<(), BackupError> {
    let bytes = serde_json::to_vec(manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_path(MANIFEST_NAME)?;
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );
    header.set_cksum();
    builder.append(&header, bytes.as_slice())?;
    Ok(())
}

fn sha256_of_file(path: &Path) -> Result<String, BackupError> {
    let mut hasher = Sha256::new();
    let mut f = fs::File::open(path)?;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn sha256_sidecar_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.sha256", path.display()))
}

fn read_sidecar_digest(sidecar: &Path) -> Result<String, BackupError> {
    let content = fs::read_to_string(sidecar)?;
    // `<hex>  <filename>` (sha256sum format); take the first token.
    Ok(content.split_whitespace().next().unwrap_or("").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn make_fake_data_dir(dir: &Path) {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join("catalog.dat"), b"catalog-bytes").unwrap();
        fs::write(dir.join("segment-00000001.log"), b"segment-data-1").unwrap();
        let offsets = dir.join("offsets");
        fs::create_dir_all(&offsets).unwrap();
        fs::write(offsets.join("ns1.s1"), b"\x00\x01").unwrap();
    }

    fn assert_dirs_equal(a: &Path, b: &Path) {
        let a_files: std::collections::BTreeSet<PathBuf> = walk(a);
        let b_files: std::collections::BTreeSet<PathBuf> = walk(b);
        assert_eq!(a_files, b_files, "file sets differ");
        for rel in &a_files {
            let ca = fs::read(a.join(rel)).unwrap();
            let cb = fs::read(b.join(rel)).unwrap();
            assert_eq!(ca, cb, "content differs for {}", rel.display());
        }
    }

    fn walk(dir: &Path) -> std::collections::BTreeSet<PathBuf> {
        let mut set = std::collections::BTreeSet::new();
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            // active.lock is process-local — exclude from comparison.
            if name == "active.lock" {
                continue;
            }
            let path = entry.path();
            let rel = path.strip_prefix(dir).unwrap().to_path_buf();
            if path.is_dir() {
                for sub in walk(&path) {
                    set.insert(rel.join(sub));
                }
            } else {
                set.insert(rel);
            }
        }
        set
    }

    #[test]
    fn backup_then_restore_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        make_fake_data_dir(&data);

        let out = tmp.path().join("snap.logdbbak");
        let manifest = backup(&data, &out).expect("backup");
        assert_eq!(manifest.magic, MAGIC);
        assert_eq!(manifest.format_version, FORMAT_VERSION);
        assert_eq!(manifest.file_count, 3); // catalog.dat + 1 segment + 1 offset
        assert!(out.exists());
        assert!(sha256_sidecar_path(&out).exists());

        // Restore into a fresh dir.
        let restored = tmp.path().join("restored");
        let m2 = restore(&out, &restored, false).expect("restore");
        assert_eq!(m2, manifest);
        assert_dirs_equal(&data, &restored);
    }

    #[test]
    fn restore_refuses_non_empty_target() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        make_fake_data_dir(&data);
        let out = tmp.path().join("snap.logdbbak");
        backup(&data, &out).unwrap();

        let target = tmp.path().join("target");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("preexisting"), b"x").unwrap();
        let err = restore(&out, &target, false).unwrap_err();
        assert!(matches!(err, BackupError::TargetNotEmpty(_)), "{err}");
    }

    #[test]
    fn restore_detects_checksum_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        make_fake_data_dir(&data);
        let out = tmp.path().join("snap.logdbbak");
        backup(&data, &out).unwrap();

        // Corrupt the archive (append a byte); sidecar stays as-is.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&out)
            .unwrap();
        f.write_all(b"!").unwrap();
        drop(f);

        let target = tmp.path().join("target");
        let err = restore(&out, &target, false).unwrap_err();
        assert!(matches!(err, BackupError::ChecksumMismatch { .. }), "{err}");
    }

    #[test]
    fn backup_refuses_non_logdb_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let empty = tmp.path().join("empty");
        fs::create_dir_all(&empty).unwrap();
        let out = tmp.path().join("snap.logdbbak");
        let err = backup(&empty, &out).unwrap_err();
        assert!(matches!(err, BackupError::NotALogdbDir(_)), "{err}");
    }

    #[test]
    fn backup_skips_active_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        make_fake_data_dir(&data);
        fs::write(data.join("active.lock"), b"stale-lock").unwrap();

        let out = tmp.path().join("snap.logdbbak");
        backup(&data, &out).unwrap();

        // Restore and confirm active.lock was NOT carried over.
        let restored = tmp.path().join("restored");
        restore(&out, &restored, false).unwrap();
        assert!(!restored.join("active.lock").exists(), "active.lock must not be in the backup");
    }
}
