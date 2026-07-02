//! Node identity and process lock.
//!
//! Every logdbd node has a unique identity (id, role, cluster_id, epoch).
//! Primary nodes acquire an exclusive `active.lock` in the data directory
//! to prevent accidental dual-primary start.

use crate::config::{NodeConfig, NodeRole};
use std::fs;
use std::path::{Path, PathBuf};

/// Node identity, resolved from configuration.
#[derive(Debug, Clone)]
pub struct NodeIdentity {
    pub id: String,
    pub role: NodeRole,
    pub cluster_id: String,
    pub epoch: u64,
}

impl NodeIdentity {
    pub fn from_config(config: &NodeConfig) -> Self {
        Self {
            id: config.id.clone(),
            role: config.role.clone(),
            cluster_id: config.cluster_id.clone(),
            epoch: config.epoch,
        }
    }

    pub fn is_primary(&self) -> bool {
        self.role == NodeRole::Primary
    }
}

/// Process lock for the data directory.
///
/// Only primary nodes acquire this lock. Standby nodes skip it.
#[derive(Debug)]
pub struct ProcessLock {
    _file: Option<fs::File>,
}

impl ProcessLock {
    /// Acquire an exclusive lock on `data_dir/active.lock`.
    ///
    /// Returns `Ok(Some(lock))` if the lock was acquired.
    /// Returns `Ok(None)` for standby nodes (no lock needed).
    /// Returns `Err` if the lock is held by another process.
    pub fn acquire(data_dir: &Path, role: &NodeRole) -> Result<Option<Self>, LockError> {
        if *role == NodeRole::Standby {
            return Ok(None);
        }

        fs::create_dir_all(data_dir).map_err(|e| LockError::Io(e))?;

        let lock_path = data_dir.join("active.lock");
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| LockError::Io(e))?;

        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(Self { _file: Some(file) })),
            Err(e) => {
                let kind = e.kind();
                if kind == std::io::ErrorKind::WouldBlock {
                    Err(LockError::Held(lock_path))
                } else {
                    Err(LockError::Failed(lock_path, e))
                }
            }
        }
    }

    /// Release the lock (happens on drop, but explicit is available).
    pub fn release(mut self) {
        self._file = None; // closes file, releases lock
    }
}

#[derive(Debug)]
pub enum LockError {
    Io(std::io::Error),
    Held(PathBuf),
    Failed(PathBuf, std::io::Error),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error acquiring process lock: {}", e),
            Self::Held(p) => write!(
                f,
                "active.lock at {} is held by another process",
                p.display()
            ),
            Self::Failed(p, e) => write!(
                f,
                "cannot acquire active.lock at {}: {}",
                p.display(), e
            ),
        }
    }
}

impl std::error::Error for LockError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_acquires_lock() {
        let dir = tempfile::tempdir().unwrap();
        let lock = ProcessLock::acquire(dir.path(), &NodeRole::Primary).unwrap();
        assert!(lock.is_some(), "primary must acquire lock");
    }

    #[test]
    fn standby_does_not_acquire_lock() {
        let dir = tempfile::tempdir().unwrap();
        let lock = ProcessLock::acquire(dir.path(), &NodeRole::Standby).unwrap();
        assert!(lock.is_none(), "standby must not acquire lock");
    }

    #[test]
    fn two_primaries_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let _lock1 = ProcessLock::acquire(dir.path(), &NodeRole::Primary).unwrap();
        let err = ProcessLock::acquire(dir.path(), &NodeRole::Primary).unwrap_err();
        assert!(
            matches!(err, LockError::Held(_)),
            "second primary must fail to acquire lock, got: {}",
            err
        );
    }

    #[test]
    fn lock_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        {
            let _lock = ProcessLock::acquire(dir.path(), &NodeRole::Primary).unwrap();
        }
        // After drop, a new primary can acquire
        let lock2 = ProcessLock::acquire(dir.path(), &NodeRole::Primary).unwrap();
        assert!(lock2.is_some(), "lock must be acquirable after drop");
    }
}
