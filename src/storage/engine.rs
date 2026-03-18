use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::memtable::{MemTable, VersionedValue};
use super::snapshot::SnapshotManager;
use super::wal::WriteAheadLog;

/// Configuration for the storage engine.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub memtable_max_bytes: u64,
    pub snapshot_interval: Duration,
    pub ttl_reap_interval: Duration,
    pub max_snapshots: usize,
    pub node_id: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            memtable_max_bytes: 64 * 1024 * 1024, // 64 MB
            snapshot_interval: Duration::from_secs(300),
            ttl_reap_interval: Duration::from_secs(60),
            max_snapshots: 3,
            node_id: "node-0".to_string(),
        }
    }
}

/// The unified storage engine combining MemTable + WAL + Snapshots.
pub struct StorageEngine {
    memtable: Arc<MemTable>,
    wal: Arc<Mutex<WriteAheadLog>>,
    snapshots: Arc<SnapshotManager>,
    config: StorageConfig,
    flush_notify: Arc<Notify>,
}

impl StorageEngine {
    /// Create and initialize the storage engine, recovering from WAL/snapshots.
    pub fn open(config: StorageConfig) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let wal_path = config.data_dir.join("wal.log");
        let snap_dir = config.data_dir.join("snapshots");

        let memtable = Arc::new(MemTable::new(config.memtable_max_bytes));
        let snapshots = Arc::new(SnapshotManager::new(&snap_dir)?);
        let wal = WriteAheadLog::open(&wal_path)?;

        // Recovery: load latest snapshot, then replay WAL on top
        if let Some(snapshot_data) = snapshots.load_latest()? {
            tracing::info!("Recovering from snapshot ({} entries)", snapshot_data.len());
            for (key, value) in snapshot_data {
                memtable.put(key, value);
            }
        }

        let wal_entries = WriteAheadLog::replay(&wal_path)?;
        if !wal_entries.is_empty() {
            tracing::info!("Replaying {} WAL entries", wal_entries.len());
            for entry in wal_entries {
                memtable.put(entry.key, entry.value);
            }
        }

        let wal = Arc::new(Mutex::new(wal));
        let flush_notify = Arc::new(Notify::new());

        let engine = Self {
            memtable,
            wal,
            snapshots,
            config,
            flush_notify,
        };

        Ok(engine)
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> Option<VersionedValue> {
        self.memtable.get(key)
    }

    /// Get a raw value (including tombstones) for replication.
    pub fn get_raw(&self, key: &str) -> Option<VersionedValue> {
        self.memtable.get_raw(key)
    }

    /// Put a key-value pair. Writes to WAL first (durability), then MemTable.
    pub fn put(&self, key: String, value: VersionedValue) -> anyhow::Result<()> {
        // WAL first for durability
        {
            let mut wal = self.wal.lock();
            wal.append(key.clone(), value.clone())?;
        }

        // Then MemTable
        let should_flush = self.memtable.put(key, value);
        if should_flush {
            self.flush_notify.notify_one();
        }

        Ok(())
    }

    /// Put a new value by creating a VersionedValue.
    pub fn put_raw(&self, key: String, value: Vec<u8>, ttl_ms: i64) -> anyhow::Result<()> {
        let existing_clock = self
            .memtable
            .get_raw(&key)
            .map(|v| v.vector_clock)
            .unwrap_or_default();

        let mut versioned = VersionedValue::new(value, &self.config.node_id, ttl_ms);
        versioned.vector_clock.merge(&existing_clock);

        self.put(key, versioned)
    }

    /// Delete a key (writes tombstone).
    pub fn delete(&self, key: &str) -> anyhow::Result<()> {
        let clock = self
            .memtable
            .get_raw(key)
            .map(|v| v.vector_clock)
            .unwrap_or_default();

        let tombstone = VersionedValue::tombstone(&self.config.node_id, &clock);

        {
            let mut wal = self.wal.lock();
            wal.append(key.to_string(), tombstone.clone())?;
        }

        self.memtable.put(key.to_string(), tombstone);
        Ok(())
    }

    /// Scan a range of keys.
    pub fn scan(&self, start: &str, end: &str, limit: usize) -> Vec<(String, VersionedValue)> {
        self.memtable.scan(start, end, limit)
    }

    /// Force a snapshot flush (MemTable → disk, then truncate WAL).
    pub fn flush(&self) -> anyhow::Result<()> {
        let data = self.memtable.snapshot();
        if data.is_empty() {
            return Ok(());
        }

        self.snapshots.save(&data)?;
        self.snapshots.cleanup(self.config.max_snapshots)?;

        {
            let mut wal = self.wal.lock();
            wal.truncate()?;
        }

        tracing::info!("Flush complete: {} entries persisted", data.len());
        Ok(())
    }

    /// Start background tasks (snapshot flush, TTL reaper).
    pub fn start_background_tasks(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        let interval = self.config.snapshot_interval;
        let flush_notify = self.flush_notify.clone();

        // Periodic snapshot task
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {},
                    _ = flush_notify.notified() => {
                        tracing::info!("MemTable full, triggering flush");
                    },
                }

                if let Err(e) = engine.flush() {
                    tracing::error!("Snapshot flush failed: {}", e);
                }
            }
        });

        // TTL reaper task
        let engine = Arc::clone(self);
        let reap_interval = self.config.ttl_reap_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(reap_interval).await;
                let removed = engine.memtable.evict_expired();
                if removed > 0 {
                    tracing::debug!("TTL reaper: evicted {} expired entries", removed);
                }
            }
        });
    }

    /// Get a reference to the inner MemTable.
    pub fn memtable(&self) -> &MemTable {
        &self.memtable
    }

    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_engine(tmp: &TempDir) -> Arc<StorageEngine> {
        let config = StorageConfig {
            data_dir: tmp.path().to_path_buf(),
            memtable_max_bytes: 1024 * 1024,
            snapshot_interval: Duration::from_secs(3600),
            ttl_reap_interval: Duration::from_secs(3600),
            max_snapshots: 3,
            node_id: "test-node".to_string(),
        };
        Arc::new(StorageEngine::open(config).unwrap())
    }

    #[test]
    fn test_put_and_get() {
        let tmp = TempDir::new().unwrap();
        let engine = make_engine(&tmp);

        engine.put_raw("hello".into(), b"world".to_vec(), 0).unwrap();
        let val = engine.get("hello").unwrap();
        assert_eq!(val.value, b"world");
    }

    #[test]
    fn test_delete() {
        let tmp = TempDir::new().unwrap();
        let engine = make_engine(&tmp);

        engine.put_raw("key".into(), b"val".to_vec(), 0).unwrap();
        engine.delete("key").unwrap();
        assert!(engine.get("key").is_none());
    }

    #[test]
    fn test_flush_and_recovery() {
        let tmp = TempDir::new().unwrap();

        // Write and flush
        {
            let engine = make_engine(&tmp);
            engine.put_raw("k1".into(), b"v1".to_vec(), 0).unwrap();
            engine.put_raw("k2".into(), b"v2".to_vec(), 0).unwrap();
            engine.flush().unwrap();
        }

        // Recover from snapshot
        {
            let engine = make_engine(&tmp);
            assert_eq!(engine.get("k1").unwrap().value, b"v1");
            assert_eq!(engine.get("k2").unwrap().value, b"v2");
        }
    }

    #[test]
    fn test_wal_recovery_without_snapshot() {
        let tmp = TempDir::new().unwrap();

        // Write without flushing
        {
            let engine = make_engine(&tmp);
            engine.put_raw("k1".into(), b"v1".to_vec(), 0).unwrap();
            engine.put_raw("k2".into(), b"v2".to_vec(), 0).unwrap();
            // No flush — data is only in WAL
        }

        // Recover from WAL
        {
            let engine = make_engine(&tmp);
            assert_eq!(engine.get("k1").unwrap().value, b"v1");
            assert_eq!(engine.get("k2").unwrap().value, b"v2");
        }
    }

    #[test]
    fn test_scan() {
        let tmp = TempDir::new().unwrap();
        let engine = make_engine(&tmp);

        for i in 0..10 {
            engine
                .put_raw(format!("key{:02}", i), format!("val{}", i).into_bytes(), 0)
                .unwrap();
        }

        let results = engine.scan("key03", "key07", 100);
        assert_eq!(results.len(), 4);
    }
}
