//! Progress file — tracks last exported seq per stream.
//!
//! Format: magic (4) + version (2) + reserved (2) + epoch (8) + last_seq (8) +
//!         updated_at_ns (8) + cluster_id_len (2) + cluster_id (var) +
//!         namespace_len (2) + namespace (var) + stream_len (2) + stream (var) +
//!         crc32c (4)

use std::io::Write;
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x4C474450; // "LGDP"
const VERSION: u16 = 1;

pub struct Progress {
    pub cluster_id: String,
    pub epoch: u64,
    pub namespace: String,
    pub stream: String,
    pub last_seq: u64,
    pub path: PathBuf,
}

impl Progress {
    pub fn new(
        cluster_id: String,
        epoch: u64,
        namespace: String,
        stream: String,
        path: PathBuf,
    ) -> Self {
        Self {
            cluster_id,
            epoch,
            namespace,
            stream,
            last_seq: 0,
            path,
        }
    }
}

impl Progress {
    pub fn load(path: &Path) -> Result<Option<Self>, String> {
        if !path.exists() {
            return Ok(None);
        }
        let data = std::fs::read(path).map_err(|e| format!("read: {}", e))?;
        if data.len() < 14 {
            return Err("progress file too short".into());
        }

        let crc_off = data.len() - 4;
        let stored = u32::from_le_bytes([
            data[crc_off],
            data[crc_off + 1],
            data[crc_off + 2],
            data[crc_off + 3],
        ]);
        if crc32c::crc32c(&data[..crc_off]) != stored {
            return Err("progress CRC mismatch".into());
        }

        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if magic != MAGIC {
            return Err("bad progress magic".into());
        }

        let ver = u16::from_le_bytes([data[4], data[5]]);
        if ver != VERSION {
            return Err(format!("unsupported version {}", ver));
        }

        let epoch = read_u64(&data, 8)?;
        let last_seq = read_u64(&data, 16)?;
        let _updated = read_u64(&data, 24)?;
        let cl_len = u16::from_le_bytes([data[32], data[33]]) as usize;
        let cluster_id = String::from_utf8_lossy(&data[34..34 + cl_len]).to_string();
        let mut pos = 34 + cl_len;
        let ns_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let namespace = String::from_utf8_lossy(&data[pos..pos + ns_len]).to_string();
        pos += ns_len;
        let s_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        let stream = String::from_utf8_lossy(&data[pos..pos + s_len]).to_string();

        Ok(Some(Self {
            cluster_id,
            epoch,
            namespace,
            stream,
            last_seq,
            path: path.to_path_buf(),
        }))
    }

    pub fn save(&self) -> Result<(), String> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
        buf.extend_from_slice(&self.epoch.to_le_bytes());
        buf.extend_from_slice(&self.last_seq.to_le_bytes());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        buf.extend_from_slice(&now.to_le_bytes());

        let cl = self.cluster_id.as_bytes();
        buf.extend_from_slice(&(cl.len() as u16).to_le_bytes());
        buf.extend_from_slice(cl);

        let ns = self.namespace.as_bytes();
        buf.extend_from_slice(&(ns.len() as u16).to_le_bytes());
        buf.extend_from_slice(ns);

        let st = self.stream.as_bytes();
        buf.extend_from_slice(&(st.len() as u16).to_le_bytes());
        buf.extend_from_slice(st);

        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        let tmp = self.path.with_extension("tmp");
        let mut f = std::fs::File::create(&tmp).map_err(|e| format!("create: {}", e))?;
        f.write_all(&buf).map_err(|e| format!("write: {}", e))?;
        f.sync_all().map_err(|e| format!("sync: {}", e))?;
        drop(f);
        std::fs::rename(&tmp, &self.path).map_err(|e| format!("rename: {}", e))?;
        Ok(())
    }
}

fn read_u64(data: &[u8], offset: usize) -> Result<u64, String> {
    if offset + 8 > data.len() {
        return Err("truncated".into());
    }
    Ok(u64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("progress.dat");
        let mut prog = Progress::new(
            "test-cluster".into(),
            1,
            "test".into(),
            "main".into(),
            p.clone(),
        );
        prog.last_seq = 42;
        prog.save().unwrap();

        let loaded = Progress::load(&p).unwrap().unwrap();
        assert_eq!(loaded.cluster_id, "test-cluster");
        assert_eq!(loaded.last_seq, 42);
        assert_eq!(loaded.namespace, "test");
        assert_eq!(loaded.stream, "main");
    }
}
