use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// A versioned value with vector clock, timestamp, and optional TTL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub vector_clock: VectorClock,
    pub timestamp_ms: i64,
    pub ttl_ms: i64, // 0 = no expiry
    pub deleted: bool, // tombstone marker
}

/// Vector clock for conflict resolution in a leaderless system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct VectorClock {
    pub clocks: std::collections::HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: std::collections::HashMap::new(),
        }
    }

    /// Increment the clock for a given node.
    pub fn increment(&mut self, node_id: &str) {
        let counter = self.clocks.entry(node_id.to_string()).or_insert(0);
        *counter += 1;
    }

    /// Merge another clock into this one (take max of each entry).
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &count) in &other.clocks {
            let entry = self.clocks.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
    }

    /// Returns true if self dominates (is causally after) other.
    pub fn dominates(&self, other: &VectorClock) -> bool {
        let mut dominated = false;
        // Every entry in other must be <= self
        for (node, &count) in &other.clocks {
            let self_count = self.clocks.get(node).copied().unwrap_or(0);
            if self_count < count {
                return false;
            }
            if self_count > count {
                dominated = true;
            }
        }
        // Check if self has entries not in other
        for (node, &count) in &self.clocks {
            if count > 0 && !other.clocks.contains_key(node) {
                dominated = true;
            }
        }
        dominated
    }

    /// Returns true if the two clocks are concurrent (neither dominates).
    pub fn is_concurrent_with(&self, other: &VectorClock) -> bool {
        !self.dominates(other) && !other.dominates(self) && self != other
    }
}

impl VersionedValue {
    pub fn new(value: Vec<u8>, node_id: &str, ttl_ms: i64) -> Self {
        let mut vc = VectorClock::new();
        vc.increment(node_id);
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        Self {
            value,
            vector_clock: vc,
            timestamp_ms,
            ttl_ms,
            deleted: false,
        }
    }

    /// Check if this value has expired.
    pub fn is_expired(&self) -> bool {
        if self.ttl_ms <= 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        now > self.timestamp_ms + self.ttl_ms
    }

    /// Create a tombstone for deletion.
    pub fn tombstone(node_id: &str, clock: &VectorClock) -> Self {
        let mut vc = clock.clone();
        vc.increment(node_id);
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        Self {
            value: Vec::new(),
            vector_clock: vc,
            timestamp_ms,
            ttl_ms: 300_000, // tombstones expire in 5 min
            deleted: true,
        }
    }
}

/// In-memory sorted storage (BTreeMap) with TTL support.
pub struct MemTable {
    data: RwLock<BTreeMap<String, VersionedValue>>,
    size_bytes: AtomicU64,
    max_size_bytes: u64,
}

impl MemTable {
    pub fn new(max_size_bytes: u64) -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            size_bytes: AtomicU64::new(0),
            max_size_bytes,
        }
    }

    /// Get a value by key. Returns None if expired or not found.
    pub fn get(&self, key: &str) -> Option<VersionedValue> {
        let data = self.data.read();
        let val = data.get(key)?;
        if val.is_expired() {
            return None;
        }
        if val.deleted {
            return None;
        }
        Some(val.clone())
    }

    /// Get raw value including tombstones and expired (for replication).
    pub fn get_raw(&self, key: &str) -> Option<VersionedValue> {
        self.data.read().get(key).cloned()
    }

    /// Put a key-value pair. Returns true if memtable is full and should be flushed.
    pub fn put(&self, key: String, value: VersionedValue) -> bool {
        let entry_size = (key.len() + value.value.len() + 128) as u64; // rough estimate
        let mut data = self.data.write();

        // If updating, subtract old size
        if let Some(old) = data.get(&key) {
            let old_size = (key.len() + old.value.len() + 128) as u64;
            self.size_bytes.fetch_sub(old_size, Ordering::Relaxed);
        }

        data.insert(key, value);
        let new_size = self.size_bytes.fetch_add(entry_size, Ordering::Relaxed) + entry_size;
        new_size >= self.max_size_bytes
    }

    /// Delete a key by inserting a tombstone.
    pub fn delete(&self, key: &str, node_id: &str) -> bool {
        let data = self.data.read();
        let clock = data
            .get(key)
            .map(|v| v.vector_clock.clone())
            .unwrap_or_default();
        drop(data);

        let tombstone = VersionedValue::tombstone(node_id, &clock);
        self.put(key.to_string(), tombstone)
    }

    /// Scan a range of keys [start, end).
    pub fn scan(&self, start: &str, end: &str, limit: usize) -> Vec<(String, VersionedValue)> {
        let data = self.data.read();
        data.range(start.to_string()..end.to_string())
            .filter(|(_, v)| !v.is_expired() && !v.deleted)
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Return all entries (for snapshot/flush).
    pub fn drain(&self) -> BTreeMap<String, VersionedValue> {
        let mut data = self.data.write();
        let drained = std::mem::take(&mut *data);
        self.size_bytes.store(0, Ordering::Relaxed);
        drained
    }

    /// Return a snapshot of all entries (non-destructive).
    pub fn snapshot(&self) -> BTreeMap<String, VersionedValue> {
        self.data.read().clone()
    }

    /// Remove expired entries. Returns the number of entries removed.
    pub fn evict_expired(&self) -> usize {
        let mut data = self.data.write();
        let before = data.len();
        data.retain(|_, v| !v.is_expired());
        let removed = before - data.len();

        // Recalculate size
        let total: u64 = data
            .iter()
            .map(|(k, v)| (k.len() + v.value.len() + 128) as u64)
            .sum();
        self.size_bytes.store(total, Ordering::Relaxed);

        removed
    }

    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes() >= self.max_size_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_put_and_get() {
        let mt = MemTable::new(1024 * 1024);
        let val = VersionedValue::new(b"hello".to_vec(), "node1", 0);
        mt.put("key1".to_string(), val.clone());

        let result = mt.get("key1").unwrap();
        assert_eq!(result.value, b"hello");
    }

    #[test]
    fn test_get_nonexistent() {
        let mt = MemTable::new(1024 * 1024);
        assert!(mt.get("nope").is_none());
    }

    #[test]
    fn test_delete() {
        let mt = MemTable::new(1024 * 1024);
        let val = VersionedValue::new(b"hello".to_vec(), "node1", 0);
        mt.put("key1".to_string(), val);
        mt.delete("key1", "node1");
        assert!(mt.get("key1").is_none());
    }

    #[test]
    fn test_ttl_expiry() {
        let mt = MemTable::new(1024 * 1024);
        let val = VersionedValue::new(b"ephemeral".to_vec(), "node1", 50); // 50ms TTL
        mt.put("key1".to_string(), val);

        assert!(mt.get("key1").is_some());
        sleep(Duration::from_millis(60));
        assert!(mt.get("key1").is_none());
    }

    #[test]
    fn test_evict_expired() {
        let mt = MemTable::new(1024 * 1024);
        mt.put(
            "exp".to_string(),
            VersionedValue::new(b"bye".to_vec(), "n1", 10),
        );
        mt.put(
            "keep".to_string(),
            VersionedValue::new(b"stay".to_vec(), "n1", 0),
        );
        sleep(Duration::from_millis(20));
        let removed = mt.evict_expired();
        assert_eq!(removed, 1);
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn test_scan_range() {
        let mt = MemTable::new(1024 * 1024);
        for i in 0..10 {
            let key = format!("key{:02}", i);
            mt.put(
                key,
                VersionedValue::new(format!("val{}", i).into_bytes(), "n1", 0),
            );
        }
        let results = mt.scan("key03", "key07", 100);
        assert_eq!(results.len(), 4); // key03, key04, key05, key06
    }

    #[test]
    fn test_vector_clock_dominates() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");
        vc1.increment("a");

        let mut vc2 = VectorClock::new();
        vc2.increment("a");

        assert!(vc1.dominates(&vc2));
        assert!(!vc2.dominates(&vc1));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");

        let mut vc2 = VectorClock::new();
        vc2.increment("b");

        assert!(vc1.is_concurrent_with(&vc2));
    }

    #[test]
    fn test_drain() {
        let mt = MemTable::new(1024 * 1024);
        mt.put(
            "k1".to_string(),
            VersionedValue::new(b"v1".to_vec(), "n1", 0),
        );
        mt.put(
            "k2".to_string(),
            VersionedValue::new(b"v2".to_vec(), "n1", 0),
        );
        let drained = mt.drain();
        assert_eq!(drained.len(), 2);
        assert!(mt.is_empty());
    }
}
