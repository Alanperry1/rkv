use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::time::timeout;

use crate::grpc::proto;
use crate::grpc::proto::internal_service_client::InternalServiceClient;
use crate::ring::consistent_hash::HashRing;
use crate::storage::engine::StorageEngine;
use crate::storage::memtable::{VersionedValue, VectorClock};

use super::quorum::{QuorumConfig, QuorumResult};

/// Coordinates reads/writes across the cluster with quorum semantics.
pub struct ReplicationCoordinator {
    engine: Arc<StorageEngine>,
    ring: Arc<HashRing>,
    quorum: QuorumConfig,
    rpc_timeout: Duration,
}

impl ReplicationCoordinator {
    pub fn new(
        engine: Arc<StorageEngine>,
        ring: Arc<HashRing>,
        quorum: QuorumConfig,
    ) -> Self {
        Self {
            engine,
            ring,
            quorum,
            rpc_timeout: Duration::from_secs(2),
        }
    }

    /// Coordinated PUT: write to local + replicate to peers with quorum.
    pub async fn put(
        &self,
        key: String,
        value: Vec<u8>,
        ttl_ms: i64,
        write_quorum_override: u32,
    ) -> Result<QuorumResult<VectorClock>> {
        let nodes = self.ring.get_nodes(&key);
        let required = self.quorum.effective_write_quorum(write_quorum_override);

        // Create the versioned value
        let existing_clock = self
            .engine
            .get_raw(&key)
            .map(|v| v.vector_clock)
            .unwrap_or_default();

        let mut versioned = VersionedValue::new(value, self.engine.node_id(), ttl_ms);
        versioned.vector_clock.merge(&existing_clock);

        // Write locally first
        let mut successes = 0u32;
        let mut failures = 0u32;

        if self.ring.is_responsible(self.engine.node_id(), &key) {
            match self.engine.put(key.clone(), versioned.clone()) {
                Ok(_) => successes += 1,
                Err(e) => {
                    tracing::error!("Local write failed: {}", e);
                    failures += 1;
                }
            }
        }

        // Replicate to peer nodes
        let my_id = self.engine.node_id().to_string();
        let peer_futures: Vec<_> = nodes
            .iter()
            .filter(|n| n.id != my_id)
            .map(|node| {
                let key = key.clone();
                let value = versioned.clone();
                let addr = node.address.clone();
                let addr_log = node.address.clone();
                let rpc_timeout = self.rpc_timeout;

                async move {
                    let result = timeout(rpc_timeout, async {
                        let endpoint = format!("http://{}", addr);
                        let mut client =
                            InternalServiceClient::connect(endpoint).await.map_err(|e| anyhow::anyhow!(e))?;
                        let req = proto::ReplicateRequest {
                            key,
                            value: Some(to_proto_versioned(&value)),
                            is_delete: false,
                        };
                        client.replicate(req).await.map_err(|e| anyhow::anyhow!(e))
                    })
                    .await;

                    match result {
                        Ok(Ok(resp)) => resp.into_inner().success,
                        Ok(Err(e)) => {
                            tracing::warn!("Replication RPC failed to {}: {}", addr_log, e);
                            false
                        }
                        Err(_) => {
                            tracing::warn!("Replication RPC timed out to {}", addr_log);
                            false
                        }
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(peer_futures).await;
        for success in results {
            if success {
                successes += 1;
            } else {
                failures += 1;
            }
        }

        Ok(QuorumResult {
            value: Some(versioned.vector_clock),
            successes,
            failures,
            required,
        })
    }

    /// Coordinated GET: read from quorum nodes, resolve conflicts.
    pub async fn get(
        &self,
        key: &str,
        read_quorum_override: u32,
    ) -> Result<QuorumResult<VersionedValue>> {
        let nodes = self.ring.get_nodes(key);
        let required = self.quorum.effective_read_quorum(read_quorum_override);

        let mut responses: Vec<Option<VersionedValue>> = Vec::new();
        let mut successes = 0u32;
        let failures = 0u32;

        // Read locally if responsible
        if self.ring.is_responsible(self.engine.node_id(), key) {
            responses.push(self.engine.get_raw(key));
            successes += 1;
        }

        // Read from peers
        let my_id = self.engine.node_id().to_string();
        let peer_futures: Vec<_> = nodes
            .iter()
            .filter(|n| n.id != my_id)
            .map(|node| {
                let key = key.to_string();
                let addr = node.address.clone();
                let addr_log = node.address.clone();
                let rpc_timeout = self.rpc_timeout;

                async move {
                    let result = timeout(rpc_timeout, async {
                        let endpoint = format!("http://{}", addr);
                        let mut client =
                            InternalServiceClient::connect(endpoint).await.map_err(|e| anyhow::anyhow!(e))?;
                        let req = proto::GetInternalRequest { key };
                        let resp = client.get_internal(req).await.map_err(|e| anyhow::anyhow!(e))?;
                        Ok::<_, anyhow::Error>(resp.into_inner())
                    })
                    .await;

                    match result {
                        Ok(Ok(resp)) => {
                            if resp.found {
                                resp.value.map(from_proto_versioned)
                            } else {
                                None
                            }
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("Read RPC failed to {}: {}", addr_log, e);
                            None
                        }
                        Err(_) => {
                            tracing::warn!("Read RPC timed out to {}", addr_log);
                            None
                        }
                    }
                }
            })
            .collect();

        let peer_results = futures::future::join_all(peer_futures).await;
        for result in peer_results {
            successes += 1; // We count the attempt as success even if key not found
            responses.push(result);
        }

        // Resolve: pick the value with the dominant vector clock
        let resolved = self.resolve_conflicts(responses);

        // TODO: Trigger read repair if values diverged

        Ok(QuorumResult {
            value: resolved,
            successes,
            failures,
            required,
        })
    }

    /// Coordinated DELETE.
    pub async fn delete(
        &self,
        key: &str,
        write_quorum_override: u32,
    ) -> Result<QuorumResult<()>> {
        let nodes = self.ring.get_nodes(key);
        let required = self.quorum.effective_write_quorum(write_quorum_override);

        let mut successes = 0u32;
        let mut failures = 0u32;

        // Delete locally
        if self.ring.is_responsible(self.engine.node_id(), key) {
            match self.engine.delete(key) {
                Ok(_) => successes += 1,
                Err(e) => {
                    tracing::error!("Local delete failed: {}", e);
                    failures += 1;
                }
            }
        }

        // Delete on peers
        let my_id = self.engine.node_id().to_string();
        let clock = self
            .engine
            .get_raw(key)
            .map(|v| v.vector_clock)
            .unwrap_or_default();

        let peer_futures: Vec<_> = nodes
            .iter()
            .filter(|n| n.id != my_id)
            .map(|node| {
                let key = key.to_string();
                let addr = node.address.clone();
                let clock = clock.clone();
                let rpc_timeout = self.rpc_timeout;

                async move {
                    let result = timeout(rpc_timeout, async {
                        let endpoint = format!("http://{}", addr);
                        let mut client =
                            InternalServiceClient::connect(endpoint).await.map_err(|e| anyhow::anyhow!(e))?;
                        let req = proto::DeleteInternalRequest {
                            key,
                            vector_clock: Some(to_proto_vector_clock(&clock)),
                        };
                        client.delete_internal(req).await.map_err(|e| anyhow::anyhow!(e))
                    })
                    .await;

                    match result {
                        Ok(Ok(resp)) => resp.into_inner().success,
                        _ => false,
                    }
                }
            })
            .collect();

        let results = futures::future::join_all(peer_futures).await;
        for success in results {
            if success {
                successes += 1;
            } else {
                failures += 1;
            }
        }

        Ok(QuorumResult {
            value: Some(()),
            successes,
            failures,
            required,
        })
    }

    /// Resolve conflicts using vector clocks (last-writer-wins on concurrent).
    fn resolve_conflicts(&self, values: Vec<Option<VersionedValue>>) -> Option<VersionedValue> {
        let mut best: Option<VersionedValue> = None;

        for val in values.into_iter().flatten() {
            if val.deleted {
                continue;
            }
            if val.is_expired() {
                continue;
            }

            best = Some(match best {
                None => val,
                Some(current) => {
                    if val.vector_clock.dominates(&current.vector_clock) {
                        val
                    } else if current.vector_clock.dominates(&val.vector_clock) {
                        current
                    } else {
                        // Concurrent: last-writer-wins by timestamp
                        if val.timestamp_ms > current.timestamp_ms {
                            val
                        } else {
                            current
                        }
                    }
                }
            });
        }

        best
    }

    pub fn quorum_config(&self) -> &QuorumConfig {
        &self.quorum
    }
}

// ─── Proto conversion helpers ───

fn to_proto_versioned(v: &VersionedValue) -> proto::VersionedValue {
    proto::VersionedValue {
        value: v.value.clone(),
        vector_clock: Some(to_proto_vector_clock(&v.vector_clock)),
        timestamp_ms: v.timestamp_ms,
        ttl_ms: v.ttl_ms,
    }
}

fn from_proto_versioned(p: proto::VersionedValue) -> VersionedValue {
    VersionedValue {
        value: p.value,
        vector_clock: p
            .vector_clock
            .map(from_proto_vector_clock)
            .unwrap_or_default(),
        timestamp_ms: p.timestamp_ms,
        ttl_ms: p.ttl_ms,
        deleted: false,
    }
}

fn to_proto_vector_clock(vc: &VectorClock) -> proto::VectorClock {
    proto::VectorClock {
        clocks: vc.clocks.clone(),
    }
}

fn from_proto_vector_clock(p: proto::VectorClock) -> VectorClock {
    VectorClock { clocks: p.clocks }
}

// Re-export for use by gRPC service
pub fn value_to_proto(v: &VersionedValue) -> proto::VersionedValue {
    to_proto_versioned(v)
}

pub fn value_from_proto(p: proto::VersionedValue) -> VersionedValue {
    from_proto_versioned(p)
}

pub fn vc_to_proto(vc: &VectorClock) -> proto::VectorClock {
    to_proto_vector_clock(vc)
}

pub fn vc_from_proto(p: proto::VectorClock) -> VectorClock {
    from_proto_vector_clock(p)
}
