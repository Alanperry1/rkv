use std::collections::BTreeMap;
use std::fmt;

use parking_lot::RwLock;
use sha2::{Digest, Sha256};

/// A physical node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Node {
    pub id: String,
    pub address: String,
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.address)
    }
}

/// Consistent hash ring with virtual nodes for even distribution.
pub struct HashRing {
    ring: RwLock<BTreeMap<u64, Node>>,
    vnodes_per_node: u32,
    replication_factor: u32,
}

impl HashRing {
    /// Create a new hash ring.
    /// - `vnodes_per_node`: number of virtual nodes per physical node (higher = more balanced)
    /// - `replication_factor`: how many nodes each key is stored on
    pub fn new(vnodes_per_node: u32, replication_factor: u32) -> Self {
        Self {
            ring: RwLock::new(BTreeMap::new()),
            vnodes_per_node,
            replication_factor,
        }
    }

    /// Hash a string to a u64 position on the ring.
    fn hash(input: &str) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        let result = hasher.finalize();
        // Take first 8 bytes as u64
        u64::from_be_bytes(result[..8].try_into().unwrap())
    }

    /// Add a node to the ring with its virtual nodes.
    pub fn add_node(&self, node: Node) {
        let mut ring = self.ring.write();
        for i in 0..self.vnodes_per_node {
            let vnode_key = format!("{}:vnode:{}", node.id, i);
            let hash = Self::hash(&vnode_key);
            ring.insert(hash, node.clone());
        }
        tracing::info!(
            "Added node {} with {} vnodes (ring size: {})",
            node,
            self.vnodes_per_node,
            ring.len()
        );
    }

    /// Remove a node and all its virtual nodes from the ring.
    pub fn remove_node(&self, node_id: &str) {
        let mut ring = self.ring.write();
        ring.retain(|_, node| node.id != node_id);
        tracing::info!("Removed node {} (ring size: {})", node_id, ring.len());
    }

    /// Get the primary node responsible for this key.
    pub fn get_node(&self, key: &str) -> Option<Node> {
        let hash = Self::hash(key);
        let ring = self.ring.read();
        if ring.is_empty() {
            return None;
        }

        // Find the first node with hash >= key hash (clockwise walk)
        ring.range(hash..)
            .next()
            .or_else(|| ring.iter().next()) // wrap around
            .map(|(_, node)| node.clone())
    }

    /// Get the N nodes responsible for this key (preference list).
    /// Returns up to `replication_factor` unique physical nodes.
    pub fn get_nodes(&self, key: &str) -> Vec<Node> {
        let hash = Self::hash(key);
        let ring = self.ring.read();
        if ring.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();
        let target = self.replication_factor as usize;

        // Walk clockwise from the key's position
        let iter = ring
            .range(hash..)
            .chain(ring.iter())
            .map(|(_, node)| node);

        for node in iter {
            if seen_ids.contains(&node.id) {
                continue;
            }
            seen_ids.insert(node.id.clone());
            result.push(node.clone());
            if result.len() >= target {
                break;
            }
        }

        result
    }

    /// Get all unique physical nodes in the ring.
    pub fn get_all_nodes(&self) -> Vec<Node> {
        let ring = self.ring.read();
        let mut seen = std::collections::HashSet::new();
        let mut nodes = Vec::new();

        for (_, node) in ring.iter() {
            if seen.insert(node.id.clone()) {
                nodes.push(node.clone());
            }
        }

        nodes
    }

    /// Get the number of unique physical nodes.
    pub fn node_count(&self) -> usize {
        self.get_all_nodes().len()
    }

    /// Check if a node is responsible for a given key.
    pub fn is_responsible(&self, node_id: &str, key: &str) -> bool {
        self.get_nodes(key).iter().any(|n| n.id == node_id)
    }

    /// Get the keys that should be transferred when a node joins.
    /// Returns ranges of the hash space that the new node now owns.
    pub fn get_hash_for_key(key: &str) -> u64 {
        Self::hash(key)
    }

    pub fn replication_factor(&self) -> u32 {
        self.replication_factor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            address: format!("127.0.0.1:{}", 50000 + id.len()),
        }
    }

    #[test]
    fn test_add_and_get_node() {
        let ring = HashRing::new(150, 3);
        ring.add_node(make_node("node1"));
        ring.add_node(make_node("node2"));
        ring.add_node(make_node("node3"));

        let node = ring.get_node("my-key").unwrap();
        assert!(["node1", "node2", "node3"].contains(&node.id.as_str()));
    }

    #[test]
    fn test_replication_returns_unique_nodes() {
        let ring = HashRing::new(150, 3);
        ring.add_node(make_node("node1"));
        ring.add_node(make_node("node2"));
        ring.add_node(make_node("node3"));

        let nodes = ring.get_nodes("some-key");
        assert_eq!(nodes.len(), 3);

        let ids: std::collections::HashSet<_> = nodes.iter().map(|n| &n.id).collect();
        assert_eq!(ids.len(), 3); // all unique
    }

    #[test]
    fn test_replication_with_fewer_nodes() {
        let ring = HashRing::new(150, 3);
        ring.add_node(make_node("node1"));
        ring.add_node(make_node("node2"));

        let nodes = ring.get_nodes("key");
        assert_eq!(nodes.len(), 2); // only 2 nodes available
    }

    #[test]
    fn test_remove_node() {
        let ring = HashRing::new(150, 3);
        ring.add_node(make_node("node1"));
        ring.add_node(make_node("node2"));
        ring.add_node(make_node("node3"));

        ring.remove_node("node2");
        assert_eq!(ring.node_count(), 2);

        let nodes = ring.get_all_nodes();
        assert!(nodes.iter().all(|n| n.id != "node2"));
    }

    #[test]
    fn test_consistency() {
        let ring = HashRing::new(150, 3);
        ring.add_node(make_node("node1"));
        ring.add_node(make_node("node2"));
        ring.add_node(make_node("node3"));

        // Same key should always map to the same node
        let node1 = ring.get_node("consistent-key").unwrap();
        let node2 = ring.get_node("consistent-key").unwrap();
        assert_eq!(node1.id, node2.id);
    }

    #[test]
    fn test_distribution() {
        let ring = HashRing::new(150, 3);
        ring.add_node(make_node("node1"));
        ring.add_node(make_node("node2"));
        ring.add_node(make_node("node3"));

        let mut counts = std::collections::HashMap::new();
        for i in 0..10000 {
            let key = format!("key-{}", i);
            let node = ring.get_node(&key).unwrap();
            *counts.entry(node.id).or_insert(0) += 1;
        }

        // Each node should have roughly 1/3 of keys (within 20% tolerance)
        for (id, count) in &counts {
            let ratio = *count as f64 / 10000.0;
            assert!(
                ratio > 0.2 && ratio < 0.47,
                "Node {} has {:.1}% of keys — distribution too skewed",
                id,
                ratio * 100.0
            );
        }
    }

    #[test]
    fn test_empty_ring() {
        let ring = HashRing::new(150, 3);
        assert!(ring.get_node("key").is_none());
        assert!(ring.get_nodes("key").is_empty());
    }

    #[test]
    fn test_minimal_disruption_on_add() {
        let ring = HashRing::new(150, 3);
        ring.add_node(make_node("node1"));
        ring.add_node(make_node("node2"));

        // Record primary for 1000 keys
        let mut before: Vec<String> = Vec::new();
        for i in 0..1000 {
            let key = format!("key-{}", i);
            before.push(ring.get_node(&key).unwrap().id);
        }

        // Add a third node
        ring.add_node(make_node("node3"));

        let mut moved = 0;
        for i in 0..1000 {
            let key = format!("key-{}", i);
            let after = ring.get_node(&key).unwrap().id;
            if after != before[i] {
                moved += 1;
            }
        }

        // With consistent hashing, roughly 1/3 of keys should move
        let move_ratio = moved as f64 / 1000.0;
        assert!(
            move_ratio < 0.5,
            "Too many keys moved: {:.1}%",
            move_ratio * 100.0
        );
    }
}
