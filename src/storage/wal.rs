use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};

use super::memtable::VersionedValue;

/// A single WAL entry representing one mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub key: String,
    pub value: VersionedValue,
    pub checksum: u32,
}

impl WalEntry {
    pub fn new(sequence: u64, key: String, value: VersionedValue) -> Self {
        let mut entry = Self {
            sequence,
            key,
            value,
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    fn compute_checksum(&self) -> u32 {
        let mut hasher = Crc32Hasher::new();
        hasher.update(&self.sequence.to_le_bytes());
        hasher.update(self.key.as_bytes());
        hasher.update(&self.value.value);
        hasher.update(&self.value.timestamp_ms.to_le_bytes());
        hasher.finalize()
    }

    pub fn verify(&self) -> bool {
        let mut copy = self.clone();
        copy.checksum = 0;
        copy.checksum = copy.compute_checksum();
        copy.checksum == self.checksum
    }
}

/// Append-only write-ahead log for crash recovery.
pub struct WriteAheadLog {
    path: PathBuf,
    writer: BufWriter<File>,
    sequence: u64,
}

impl WriteAheadLog {
    /// Open (or create) a WAL file at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Recover sequence number from existing WAL
        let sequence = if path.exists() {
            Self::recover_sequence(&path)?
        } else {
            0
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
            sequence,
        })
    }

    /// Append an entry to the WAL. Returns the sequence number.
    pub fn append(&mut self, key: String, value: VersionedValue) -> io::Result<u64> {
        self.sequence += 1;
        let entry = WalEntry::new(self.sequence, key, value);
        let serialized = serde_json::to_string(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.writer.write_all(serialized.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;

        Ok(self.sequence)
    }

    /// Replay all valid entries from the WAL. Used for crash recovery.
    pub fn replay<P: AsRef<Path>>(path: P) -> io::Result<Vec<WalEntry>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut corrupt_count = 0;

        for (line_no, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!("WAL read error at line {}: {}", line_no, e);
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => {
                    if entry.verify() {
                        entries.push(entry);
                    } else {
                        corrupt_count += 1;
                        tracing::warn!(
                            "Corrupt WAL entry at line {} (checksum mismatch)",
                            line_no
                        );
                    }
                }
                Err(e) => {
                    corrupt_count += 1;
                    tracing::warn!("Unparseable WAL entry at line {}: {}", line_no, e);
                }
            }
        }

        if corrupt_count > 0 {
            tracing::warn!("WAL replay: {} corrupt entries skipped", corrupt_count);
        }

        tracing::info!("WAL replay: {} entries recovered", entries.len());
        Ok(entries)
    }

    /// Truncate the WAL (after a successful snapshot).
    pub fn truncate(&mut self) -> io::Result<()> {
        // Close the current writer by dropping and reopening
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.sequence = 0;
        tracing::info!("WAL truncated");
        Ok(())
    }

    /// Get the current sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Get the path to the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Recover the highest sequence number from an existing WAL.
    fn recover_sequence(path: &Path) -> io::Result<u64> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut max_seq = 0u64;

        for line in reader.lines() {
            if let Ok(line) = line {
                if let Ok(entry) = serde_json::from_str::<WalEntry>(&line) {
                    max_seq = max_seq.max(entry.sequence);
                }
            }
        }

        Ok(max_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memtable::VersionedValue;
    use tempfile::TempDir;

    fn make_value(data: &[u8]) -> VersionedValue {
        VersionedValue::new(data.to_vec(), "test-node", 0)
    }

    #[test]
    fn test_append_and_replay() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        // Write entries
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            wal.append("key1".to_string(), make_value(b"value1"))
                .unwrap();
            wal.append("key2".to_string(), make_value(b"value2"))
                .unwrap();
            wal.append("key3".to_string(), make_value(b"value3"))
                .unwrap();
            assert_eq!(wal.sequence(), 3);
        }

        // Replay
        let entries = WriteAheadLog::replay(&wal_path).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, "key1");
        assert_eq!(entries[1].key, "key2");
        assert_eq!(entries[2].key, "key3");
    }

    #[test]
    fn test_checksum_verification() {
        let val = make_value(b"test");
        let entry = WalEntry::new(1, "k".to_string(), val);
        assert!(entry.verify());

        // Corrupt it
        let mut corrupted = entry.clone();
        corrupted.key = "tampered".to_string();
        assert!(!corrupted.verify());
    }

    #[test]
    fn test_truncate() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let mut wal = WriteAheadLog::open(&wal_path).unwrap();
        wal.append("k1".to_string(), make_value(b"v1")).unwrap();
        wal.append("k2".to_string(), make_value(b"v2")).unwrap();
        wal.truncate().unwrap();

        let entries = WriteAheadLog::replay(&wal_path).unwrap();
        assert!(entries.is_empty());
        assert_eq!(wal.sequence(), 0);
    }

    #[test]
    fn test_reopen_continues_sequence() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            wal.append("k1".to_string(), make_value(b"v1")).unwrap();
            wal.append("k2".to_string(), make_value(b"v2")).unwrap();
        }

        let wal = WriteAheadLog::open(&wal_path).unwrap();
        assert_eq!(wal.sequence(), 2);
    }

    #[test]
    fn test_replay_nonexistent() {
        let entries = WriteAheadLog::replay("/tmp/nonexistent_wal_file.wal").unwrap();
        assert!(entries.is_empty());
    }
}
