use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher as Crc32Hasher;

use super::memtable::VersionedValue;

/// Manages periodic snapshots of the MemTable to disk.
pub struct SnapshotManager {
    dir: PathBuf,
}

impl SnapshotManager {
    pub fn new<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// Save a snapshot of the MemTable data to disk.
    /// File format: [4 bytes checksum][data bytes (bincode)]
    pub fn save(&self, data: &BTreeMap<String, VersionedValue>) -> io::Result<PathBuf> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let filename = format!("snapshot_{}.dat", timestamp);
        let path = self.dir.join(&filename);

        let serialized = bincode::serialize(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Compute checksum
        let mut hasher = Crc32Hasher::new();
        hasher.update(&serialized);
        let checksum = hasher.finalize();

        // Write: checksum (4 bytes LE) + data
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&checksum.to_le_bytes())?;
        writer.write_all(&serialized)?;
        writer.flush()?;

        tracing::info!(
            "Snapshot saved: {} ({} entries, {} bytes)",
            filename,
            data.len(),
            serialized.len() + 4
        );

        Ok(path)
    }

    /// Load the latest snapshot from disk.
    pub fn load_latest(&self) -> io::Result<Option<BTreeMap<String, VersionedValue>>> {
        let latest = self.find_latest()?;
        match latest {
            Some(path) => {
                let data = self.load_snapshot(&path)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    /// Load a specific snapshot file.
    pub fn load_snapshot(&self, path: &Path) -> io::Result<BTreeMap<String, VersionedValue>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read checksum
        let mut checksum_bytes = [0u8; 4];
        reader.read_exact(&mut checksum_bytes)?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        // Read data
        let mut data_bytes = Vec::new();
        reader.read_to_end(&mut data_bytes)?;

        // Verify checksum
        let mut hasher = Crc32Hasher::new();
        hasher.update(&data_bytes);
        let actual_checksum = hasher.finalize();

        if expected_checksum != actual_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Snapshot checksum mismatch: expected {}, got {}",
                    expected_checksum, actual_checksum
                ),
            ));
        }

        let data: BTreeMap<String, VersionedValue> = bincode::deserialize(&data_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        tracing::info!(
            "Snapshot loaded: {:?} ({} entries)",
            path.file_name().unwrap_or_default(),
            data.len()
        );

        Ok(data)
    }

    /// Find the most recent snapshot file.
    fn find_latest(&self) -> io::Result<Option<PathBuf>> {
        let mut snapshots: Vec<PathBuf> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("snapshot_") && n.ends_with(".dat"))
                    .unwrap_or(false)
            })
            .collect();

        snapshots.sort();
        Ok(snapshots.into_iter().last())
    }

    /// Clean up old snapshots, keeping only the N most recent.
    pub fn cleanup(&self, keep: usize) -> io::Result<usize> {
        let mut snapshots: Vec<PathBuf> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("snapshot_") && n.ends_with(".dat"))
                    .unwrap_or(false)
            })
            .collect();

        snapshots.sort();

        let mut removed = 0;
        if snapshots.len() > keep {
            let to_remove = snapshots.len() - keep;
            for path in snapshots.iter().take(to_remove) {
                fs::remove_file(path)?;
                removed += 1;
            }
        }

        if removed > 0 {
            tracing::info!("Cleaned up {} old snapshots", removed);
        }

        Ok(removed)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memtable::VersionedValue;
    use tempfile::TempDir;

    fn make_data() -> BTreeMap<String, VersionedValue> {
        let mut data = BTreeMap::new();
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let val = VersionedValue::new(format!("value_{}", i).into_bytes(), "node1", 0);
            data.insert(key, val);
        }
        data
    }

    #[test]
    fn test_save_and_load() {
        let tmp = TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path()).unwrap();
        let data = make_data();

        let path = mgr.save(&data).unwrap();
        assert!(path.exists());

        let loaded = mgr.load_snapshot(&path).unwrap();
        assert_eq!(loaded.len(), data.len());
        for (k, v) in &data {
            assert_eq!(loaded.get(k).unwrap().value, v.value);
        }
    }

    #[test]
    fn test_load_latest() {
        let tmp = TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path()).unwrap();

        // No snapshots yet
        assert!(mgr.load_latest().unwrap().is_none());

        // Save two snapshots
        let data1 = {
            let mut d = BTreeMap::new();
            d.insert(
                "a".to_string(),
                VersionedValue::new(b"1".to_vec(), "n1", 0),
            );
            d
        };
        mgr.save(&data1).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));

        let data2 = {
            let mut d = BTreeMap::new();
            d.insert(
                "b".to_string(),
                VersionedValue::new(b"2".to_vec(), "n1", 0),
            );
            d
        };
        mgr.save(&data2).unwrap();

        let latest = mgr.load_latest().unwrap().unwrap();
        assert!(latest.contains_key("b"));
        assert!(!latest.contains_key("a"));
    }

    #[test]
    fn test_cleanup() {
        let tmp = TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path()).unwrap();
        let data = BTreeMap::new();

        for _ in 0..5 {
            mgr.save(&data).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let removed = mgr.cleanup(2).unwrap();
        assert_eq!(removed, 3);

        // Should be 2 files left
        let count = fs::read_dir(tmp.path())
            .unwrap()
            .filter(|e| e.is_ok())
            .count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_corrupt_snapshot() {
        let tmp = TempDir::new().unwrap();
        let mgr = SnapshotManager::new(tmp.path()).unwrap();
        let data = make_data();
        let path = mgr.save(&data).unwrap();

        // Corrupt the file
        let mut bytes = fs::read(&path).unwrap();
        if let Some(b) = bytes.last_mut() {
            *b ^= 0xff;
        }
        fs::write(&path, &bytes).unwrap();

        let result = mgr.load_snapshot(&path);
        assert!(result.is_err());
    }
}
