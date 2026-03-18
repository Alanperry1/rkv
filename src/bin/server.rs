use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

use rkv::gossip::detector::{GossipConfig, GossipService};
use rkv::grpc::proto::internal_service_server::InternalServiceServer;
use rkv::grpc::proto::kv_service_server::KvServiceServer;
use rkv::grpc::service::{InternalServiceImpl, KvServiceImpl};
use rkv::replication::coordinator::ReplicationCoordinator;
use rkv::replication::quorum::QuorumConfig;
use rkv::ring::consistent_hash::HashRing;
use rkv::storage::engine::{StorageConfig, StorageEngine};

#[derive(Parser, Debug)]
#[command(name = "rkv-server", about = "Distributed key-value store node")]
struct Args {
    /// Node ID (must be unique in the cluster)
    #[arg(long, default_value = "node-0")]
    node_id: String,

    /// Listen address for client + internal gRPC
    #[arg(long, default_value = "0.0.0.0:50051")]
    listen: String,

    /// Advertised address (what peers use to reach this node)
    #[arg(long, default_value = "127.0.0.1:50051")]
    advertise: String,

    /// Data directory
    #[arg(long, default_value = "./data")]
    data_dir: String,

    /// Seed nodes to join (comma-separated: id=addr,id=addr)
    #[arg(long, default_value = "")]
    seeds: String,

    /// Replication factor (N)
    #[arg(long, default_value_t = 3)]
    replication_factor: u32,

    /// Read quorum (R)
    #[arg(long, default_value_t = 2)]
    read_quorum: u32,

    /// Write quorum (W)
    #[arg(long, default_value_t = 2)]
    write_quorum: u32,

    /// MemTable max size in MB
    #[arg(long, default_value_t = 64)]
    memtable_mb: u64,

    /// Virtual nodes per physical node
    #[arg(long, default_value_t = 150)]
    vnodes: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    tracing::info!("Starting rkv node: {}", args.node_id);
    tracing::info!("Listen: {}, Advertise: {}", args.listen, args.advertise);

    // ── Storage Engine ──
    let storage_config = StorageConfig {
        data_dir: args.data_dir.into(),
        memtable_max_bytes: args.memtable_mb * 1024 * 1024,
        snapshot_interval: Duration::from_secs(300),
        ttl_reap_interval: Duration::from_secs(60),
        max_snapshots: 3,
        node_id: args.node_id.clone(),
    };
    let engine = Arc::new(StorageEngine::open(storage_config)?);
    engine.start_background_tasks();

    // ── Hash Ring ──
    let ring = Arc::new(HashRing::new(args.vnodes, args.replication_factor));

    // ── Gossip ──
    let gossip_config = GossipConfig::default();
    let gossip = Arc::new(GossipService::new(
        args.node_id.clone(),
        args.advertise.clone(),
        Arc::clone(&ring),
        gossip_config,
    ));

    // Register seed nodes
    if !args.seeds.is_empty() {
        for seed in args.seeds.split(',') {
            let parts: Vec<&str> = seed.split('=').collect();
            if parts.len() == 2 {
                let seed_id = parts[0].trim();
                let seed_addr = parts[1].trim();
                tracing::info!("Adding seed: {} @ {}", seed_id, seed_addr);
                gossip.add_seed(seed_id.to_string(), seed_addr.to_string());
            }
        }
    }

    gossip.start();

    // ── Replication Coordinator ──
    let quorum = QuorumConfig {
        replication_factor: args.replication_factor,
        read_quorum: args.read_quorum,
        write_quorum: args.write_quorum,
    };
    let coordinator = Arc::new(ReplicationCoordinator::new(
        Arc::clone(&engine),
        Arc::clone(&ring),
        quorum,
    ));

    // ── gRPC Server ──
    let kv_service = KvServiceImpl::new(Arc::clone(&coordinator), Arc::clone(&engine));
    let internal_service = InternalServiceImpl::new(Arc::clone(&engine), Arc::clone(&gossip));

    let addr: SocketAddr = args.listen.parse()?;
    tracing::info!("gRPC server listening on {}", addr);

    Server::builder()
        .add_service(KvServiceServer::new(kv_service))
        .add_service(InternalServiceServer::new(internal_service))
        .serve(addr)
        .await?;

    Ok(())
}
