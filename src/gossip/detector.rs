use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use tokio::time::interval;

use crate::grpc::proto;
use crate::grpc::proto::internal_service_client::InternalServiceClient;
use crate::ring::consistent_hash::{HashRing, Node};

/// Status of a node as seen by the gossip protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

/// State tracked for each peer node.
#[derive(Debug, Clone)]
pub struct PeerState {
    pub node_id: String,
    pub address: String,
    pub status: NodeStatus,
    pub heartbeat: u64,
    pub generation: u64,
    pub last_seen: Instant,
}

/// SWIM-style gossip protocol for failure detection.
pub struct GossipService {
    node_id: String,
    peers: Arc<RwLock<HashMap<String, PeerState>>>,
    ring: Arc<HashRing>,
    heartbeat_counter: Arc<std::sync::atomic::AtomicU64>,
    generation: u64,
    config: GossipConfig,
}

#[derive(Debug, Clone)]
pub struct GossipConfig {
    pub gossip_interval: Duration,
    pub suspect_timeout: Duration,
    pub dead_timeout: Duration,
    pub fanout: usize, // number of peers to gossip with each round
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_secs(1),
            suspect_timeout: Duration::from_secs(5),
            dead_timeout: Duration::from_secs(15),
            fanout: 3,
        }
    }
}

impl GossipService {
    pub fn new(
        node_id: String,
        address: String,
        ring: Arc<HashRing>,
        config: GossipConfig,
    ) -> Self {
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let heartbeat_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Add self to ring
        ring.add_node(Node {
            id: node_id.clone(),
            address: address.clone(),
        });

        Self {
            node_id,
            peers,
            ring,
            heartbeat_counter,
            generation: 1,
            config,
        }
    }

    /// Register a seed peer (used at startup to bootstrap).
    pub fn add_seed(&self, node_id: String, address: String) {
        let mut peers = self.peers.write();
        peers.insert(
            node_id.clone(),
            PeerState {
                node_id: node_id.clone(),
                address: address.clone(),
                status: NodeStatus::Alive,
                heartbeat: 0,
                generation: 0,
                last_seen: Instant::now(),
            },
        );

        self.ring.add_node(Node {
            id: node_id,
            address,
        });
    }

    /// Process an incoming gossip message and return our state.
    pub fn handle_gossip(&self, msg: &proto::GossipMessage) -> Vec<proto::NodeState> {
        let mut peers = self.peers.write();

        for node_state in &msg.nodes {
            let existing = peers.get(&node_state.node_id);

            let should_update = match existing {
                None => true,
                Some(existing) => {
                    node_state.generation > existing.generation
                        || (node_state.generation == existing.generation
                            && node_state.heartbeat > existing.heartbeat)
                }
            };

            if should_update && node_state.node_id != self.node_id {
                let status = match proto::NodeStatus::try_from(node_state.status) {
                    Ok(proto::NodeStatus::Alive) => NodeStatus::Alive,
                    Ok(proto::NodeStatus::Suspect) => NodeStatus::Suspect,
                    Ok(proto::NodeStatus::Dead) => NodeStatus::Dead,
                    Err(_) => NodeStatus::Alive,
                };

                let peer = PeerState {
                    node_id: node_state.node_id.clone(),
                    address: node_state.address.clone(),
                    status: status.clone(),
                    heartbeat: node_state.heartbeat,
                    generation: node_state.generation,
                    last_seen: Instant::now(),
                };

                // Update ring membership
                match status {
                    NodeStatus::Dead => {
                        self.ring.remove_node(&node_state.node_id);
                    }
                    NodeStatus::Alive => {
                        self.ring.add_node(Node {
                            id: node_state.node_id.clone(),
                            address: node_state.address.clone(),
                        });
                    }
                    _ => {}
                }

                peers.insert(node_state.node_id.clone(), peer);
            }
        }

        // Return our view of the world
        self.build_node_states(&peers)
    }

    /// Start the periodic gossip loop.
    pub fn start(self: &Arc<Self>) {
        let gossip = Arc::clone(self);
        let gossip_interval = self.config.gossip_interval;

        // Gossip sender
        tokio::spawn(async move {
            let mut tick = interval(gossip_interval);
            loop {
                tick.tick().await;
                gossip.gossip_round().await;
            }
        });

        // Failure detector
        let gossip = Arc::clone(self);
        let suspect_timeout = self.config.suspect_timeout;
        let dead_timeout = self.config.dead_timeout;

        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                gossip.check_failures(suspect_timeout, dead_timeout);
            }
        });
    }

    /// Run one gossip round: pick random peers and exchange state.
    async fn gossip_round(&self) {
        // Increment heartbeat
        self.heartbeat_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let peers_snapshot: Vec<PeerState> = {
            let peers = self.peers.read();
            peers
                .values()
                .filter(|p| p.status != NodeStatus::Dead)
                .cloned()
                .collect()
        };

        if peers_snapshot.is_empty() {
            return;
        }

        // Pick random targets
        let mut rng = rand::thread_rng();
        let targets: Vec<_> = peers_snapshot
            .choose_multiple(&mut rng, self.config.fanout.min(peers_snapshot.len()))
            .cloned()
            .collect();

        let msg = self.build_gossip_message();

        for target in targets {
            let msg = msg.clone();
            let addr = target.address.clone();

            tokio::spawn(async move {
                let endpoint = format!("http://{}", addr);
                match InternalServiceClient::connect(endpoint).await {
                    Ok(mut client) => {
                        if let Err(e) = client.gossip(msg).await {
                            tracing::debug!("Gossip to {} failed: {}", addr, e);
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Failed to connect to {} for gossip: {}", addr, e);
                    }
                }
            });
        }
    }

    /// Check for peer failures based on timeouts.
    fn check_failures(&self, suspect_timeout: Duration, dead_timeout: Duration) {
        let mut peers = self.peers.write();
        let now = Instant::now();

        for (_, peer) in peers.iter_mut() {
            let elapsed = now.duration_since(peer.last_seen);

            match peer.status {
                NodeStatus::Alive if elapsed > suspect_timeout => {
                    tracing::warn!("Node {} is now SUSPECT", peer.node_id);
                    peer.status = NodeStatus::Suspect;
                }
                NodeStatus::Suspect if elapsed > dead_timeout => {
                    tracing::error!("Node {} is now DEAD", peer.node_id);
                    peer.status = NodeStatus::Dead;
                    self.ring.remove_node(&peer.node_id);
                }
                _ => {}
            }
        }
    }

    fn build_gossip_message(&self) -> proto::GossipMessage {
        let peers = self.peers.read();
        let heartbeat = self
            .heartbeat_counter
            .load(std::sync::atomic::Ordering::Relaxed);

        let mut nodes = self.build_node_states(&peers);

        // Include ourselves
        nodes.push(proto::NodeState {
            node_id: self.node_id.clone(),
            address: String::new(), // filled by receiver
            status: proto::NodeStatus::Alive as i32,
            heartbeat,
            generation: self.generation,
        });

        proto::GossipMessage {
            sender_id: self.node_id.clone(),
            nodes,
        }
    }

    fn build_node_states(&self, peers: &HashMap<String, PeerState>) -> Vec<proto::NodeState> {
        peers
            .values()
            .map(|p| proto::NodeState {
                node_id: p.node_id.clone(),
                address: p.address.clone(),
                status: match p.status {
                    NodeStatus::Alive => proto::NodeStatus::Alive as i32,
                    NodeStatus::Suspect => proto::NodeStatus::Suspect as i32,
                    NodeStatus::Dead => proto::NodeStatus::Dead as i32,
                },
                heartbeat: p.heartbeat,
                generation: p.generation,
            })
            .collect()
    }

    /// Get all alive peers.
    pub fn alive_peers(&self) -> Vec<PeerState> {
        self.peers
            .read()
            .values()
            .filter(|p| p.status == NodeStatus::Alive)
            .cloned()
            .collect()
    }

    /// Get status of a specific peer.
    pub fn peer_status(&self, node_id: &str) -> Option<NodeStatus> {
        self.peers.read().get(node_id).map(|p| p.status.clone())
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}
