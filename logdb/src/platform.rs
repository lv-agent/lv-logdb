//! Platform-specific I/O primitives.
//!
//! Abstracts over Linux/macOS/Windows for `fdatasync`, directory sync,
//! and clock access.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;

/// Perform `fdatasync` on a file (data only, no metadata).
///
/// On Linux this calls `fdatasync(2)`. On macOS it falls back to `fsync`
/// (macOS does not have `fdatasync`). On Windows it calls `FlushFileBuffers`.
#[cfg(target_os = "linux")]
pub fn fdatasync(file: &File) -> io::Result<()> {
    let rc = unsafe { libc::fdatasync(file.as_raw_fd()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn fdatasync(file: &File) -> io::Result<()> {
    // macOS and others: fall back to fsync
    file.sync_data()
}

/// Sync a directory to ensure file creation/rename is durable.
///
/// On Linux: `fsync` on the directory fd.
/// On macOS: `fsync` on the directory fd.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn sync_dir(dir: &File) -> io::Result<()> {
    let rc = unsafe { libc::fsync(dir.as_raw_fd()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
pub fn sync_dir(_dir: &File) -> io::Result<()> {
    // Windows: directory entries are durable after the file is flushed.
    Ok(())
}

/// Get the current time from `CLOCK_REALTIME_COARSE` in nanoseconds.
///
/// Uses vDSO on modern Linux kernels — no syscall in the fast path.
#[inline]
pub fn clock_realtime_coarse_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_REALTIME_COARSE, &mut ts);
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// Check if an I/O error is ENOSPC (disk full).
pub fn is_enospc(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::ENOSPC)
}
