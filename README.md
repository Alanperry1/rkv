# rkv

A distributed key-value store built in Rust, inspired by Amazon Dynamo. It features consistent hashing, tunable quorum replication, vector-clock versioning, and SWIM-style gossip for membership and failure detection.

## Architecture

```
┌─────────────┐     gRPC      ┌─────────────┐
│   rkv-cli   │──────────────▶│  rkv-server  │
└─────────────┘               └──────┬───────┘
                                     │
                 ┌───────────────────┼───────────────────┐
                 │                   │                   │
          ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
          │   Storage   │    │   Ring      │    │   Gossip    │
          │  Engine     │    │  Consistent │    │   SWIM      │
          │  MemTable   │    │  Hashing    │    │   Protocol  │
          │  WAL        │    │  (SHA-256)  │    │   Failure   │
          │  Snapshots  │    │  vNodes     │    │   Detection │
          └─────────────┘    └─────────────┘    └─────────────┘
                 │
          ┌──────▼──────┐
          │ Replication  │
          │ Coordinator  │
          │ Quorum R/W   │
          └──────────────┘
```

### Key Components

- **Storage Engine** — MemTable (sorted BTreeMap) backed by a write-ahead log and periodic snapshots. Supports TTL, tombstone deletes, and CRC32 integrity checks.
- **Consistent Hashing** — SHA-256 hash ring with configurable virtual nodes (default 150 per node) for even key distribution. Dynamo-style preference lists for replication targets.
- **Replication** — Tunable N/R/W quorum (default N=3, R=2, W=2). Coordinated reads and writes with parallel fan-out to replica nodes. Last-writer-wins conflict resolution for concurrent vector clocks.
- **Gossip & Failure Detection** — SWIM-style protocol with configurable fanout. Nodes transition through Alive → Suspect → Dead states, with automatic hash ring updates on membership changes.
- **gRPC API** — Separate client-facing (`KvService`) and inter-node (`InternalService`) services. Includes hinted handoff and key transfer RPCs for rebalancing.

## Getting Started

### Prerequisites

- Rust 1.70+ (edition 2021)
- Protobuf compiler (`protoc`)

### Build

```bash
cargo build --release
```

### Run a Single Node

```bash
cargo run --release --bin rkv-server
```

### Run a 3-Node Local Cluster

```bash
# Terminal 1
cargo run --release --bin rkv-server -- \
  --node-id node-0 --listen 0.0.0.0:50051 --advertise 127.0.0.1:50051 \
  --data-dir ./data/node-0

# Terminal 2
cargo run --release --bin rkv-server -- \
  --node-id node-1 --listen 0.0.0.0:50052 --advertise 127.0.0.1:50052 \
  --data-dir ./data/node-1 \
  --seeds "node-0=127.0.0.1:50051"

# Terminal 3
cargo run --release --bin rkv-server -- \
  --node-id node-2 --listen 0.0.0.0:50053 --advertise 127.0.0.1:50053 \
  --data-dir ./data/node-2 \
  --seeds "node-0=127.0.0.1:50051,node-1=127.0.0.1:50052"
```

### CLI Usage

```bash
# Put a key
cargo run --release --bin rkv-cli -- put mykey myvalue

# Put with TTL (milliseconds)
cargo run --release --bin rkv-cli -- put session abc123 --ttl 60000

# Get a key
cargo run --release --bin rkv-cli -- get mykey

# Delete a key
cargo run --release --bin rkv-cli -- delete mykey

# Range scan
cargo run --release --bin rkv-cli -- scan a z --limit 50

# Simple benchmark
cargo run --release --bin rkv-cli -- bench 10000 --value-size 256

# Connect to a different node
cargo run --release --bin rkv-cli -- --addr http://127.0.0.1:50052 get mykey
```

Per-request quorum overrides are supported with the `--quorum` flag on `get`, `put`, and `delete`.

## Server Configuration

| Flag | Default | Description |
|---|---|---|
| `--node-id` | `node-0` | Unique node identifier |
| `--listen` | `0.0.0.0:50051` | gRPC listen address |
| `--advertise` | `127.0.0.1:50051` | Address peers use to reach this node |
| `--data-dir` | `./data` | Data directory (WAL + snapshots) |
| `--seeds` | | Seed nodes (`id=addr,id=addr`) |
| `--replication-factor` | `3` | Number of replicas (N) |
| `--read-quorum` | `2` | Read quorum (R) |
| `--write-quorum` | `2` | Write quorum (W) |
| `--memtable-mb` | `64` | MemTable size before flush (MB) |
| `--vnodes` | `150` | Virtual nodes per physical node |

For strong consistency, ensure `R + W > N`. The defaults satisfy this (2 + 2 > 3).

## Testing

```bash
# Run all unit tests
cargo test

# Run tests for a specific module
cargo test storage
cargo test ring
cargo test replication
cargo test gossip

# Run benchmarks
cargo bench
```

## Project Structure

```
src/
├── lib.rs                  # Crate root — re-exports modules
├── bin/
│   ├── server.rs           # rkv-server binary
│   └── cli.rs              # rkv-cli binary
├── storage/
│   ├── engine.rs           # StorageEngine facade (recovery, background tasks)
│   ├── memtable.rs         # In-memory BTreeMap with vector clocks & TTL
│   ├── wal.rs              # Append-only write-ahead log (JSON + CRC32)
│   └── snapshot.rs         # Periodic bincode snapshots with checksums
├── ring/
│   └── consistent_hash.rs  # SHA-256 hash ring with virtual nodes
├── replication/
│   ├── coordinator.rs      # Distributed read/write coordination
│   └── quorum.rs           # Quorum configuration and result tracking
├── gossip/
│   └── detector.rs         # SWIM gossip protocol & failure detection
└── grpc/
    ├── service.rs          # KvService + InternalService implementations
    └── rkv.rs              # Generated protobuf/gRPC code
```

## License

This project is unlicensed. See the repository for details.
