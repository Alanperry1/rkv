/// Quorum configuration for reads and writes.
#[derive(Debug, Clone)]
pub struct QuorumConfig {
    pub replication_factor: u32, // N
    pub read_quorum: u32,        // R
    pub write_quorum: u32,       // W
}

impl QuorumConfig {
    /// Default: N=3, R=2, W=2 (strong consistency since R+W > N)
    pub fn default_strong() -> Self {
        Self {
            replication_factor: 3,
            read_quorum: 2,
            write_quorum: 2,
        }
    }

    /// Eventual consistency: N=3, R=1, W=1
    pub fn eventual() -> Self {
        Self {
            replication_factor: 3,
            read_quorum: 1,
            write_quorum: 1,
        }
    }

    /// Check if the config guarantees strong consistency.
    pub fn is_strongly_consistent(&self) -> bool {
        self.read_quorum + self.write_quorum > self.replication_factor
    }

    /// Override read quorum for a specific request (0 = use default).
    pub fn effective_read_quorum(&self, override_r: u32) -> u32 {
        if override_r > 0 {
            override_r
        } else {
            self.read_quorum
        }
    }

    /// Override write quorum for a specific request (0 = use default).
    pub fn effective_write_quorum(&self, override_w: u32) -> u32 {
        if override_w > 0 {
            override_w
        } else {
            self.write_quorum
        }
    }
}

/// Result of a quorum operation.
#[derive(Debug)]
pub struct QuorumResult<T> {
    pub value: Option<T>,
    pub successes: u32,
    pub failures: u32,
    pub required: u32,
}

impl<T> QuorumResult<T> {
    pub fn is_success(&self) -> bool {
        self.successes >= self.required
    }
}
