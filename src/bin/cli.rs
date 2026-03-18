use clap::{Parser, Subcommand};

use rkv::grpc::proto::kv_service_client::KvServiceClient;
use rkv::grpc::proto::{
    DeleteRequest, GetRequest, PutRequest, ScanRequest,
};

#[derive(Parser, Debug)]
#[command(name = "rkv-cli", about = "CLI client for rkv distributed key-value store")]
struct Args {
    /// Server address
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Get a value by key
    Get {
        key: String,
        /// Read quorum (0 = server default)
        #[arg(long, default_value_t = 0)]
        quorum: u32,
    },
    /// Set a key-value pair
    Put {
        key: String,
        value: String,
        /// TTL in milliseconds (0 = no expiry)
        #[arg(long, default_value_t = 0)]
        ttl: i64,
        /// Write quorum (0 = server default)
        #[arg(long, default_value_t = 0)]
        quorum: u32,
    },
    /// Delete a key
    Delete {
        key: String,
        /// Write quorum (0 = server default)
        #[arg(long, default_value_t = 0)]
        quorum: u32,
    },
    /// Scan a range of keys
    Scan {
        /// Start key (inclusive)
        start: String,
        /// End key (exclusive)
        end: String,
        /// Maximum number of results
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Benchmark: write N keys then read them back
    Bench {
        /// Number of keys
        #[arg(default_value_t = 1000)]
        count: u32,
        /// Value size in bytes
        #[arg(long, default_value_t = 256)]
        value_size: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut client = KvServiceClient::connect(args.addr.clone()).await?;

    match args.command {
        Command::Get { key, quorum } => {
            let resp = client
                .get(GetRequest {
                    key: key.clone(),
                    read_quorum: quorum,
                })
                .await?
                .into_inner();

            if resp.found {
                match String::from_utf8(resp.value.clone()) {
                    Ok(s) => println!("{}", s),
                    Err(_) => println!("{:?} ({} bytes)", resp.value, resp.value.len()),
                }
            } else {
                println!("(not found)");
            }
        }

        Command::Put {
            key,
            value,
            ttl,
            quorum,
        } => {
            let resp = client
                .put(PutRequest {
                    key: key.clone(),
                    value: value.into_bytes(),
                    ttl_ms: ttl,
                    write_quorum: quorum,
                    vector_clock: None,
                })
                .await?
                .into_inner();

            if resp.success {
                println!("OK");
            } else {
                println!("FAILED");
            }
        }

        Command::Delete { key, quorum } => {
            let resp = client
                .delete(DeleteRequest {
                    key: key.clone(),
                    write_quorum: quorum,
                })
                .await?
                .into_inner();

            if resp.success {
                println!("OK");
            } else {
                println!("FAILED");
            }
        }

        Command::Scan { start, end, limit } => {
            let resp = client
                .scan(ScanRequest {
                    start_key: start,
                    end_key: end,
                    limit,
                })
                .await?
                .into_inner();

            if resp.pairs.is_empty() {
                println!("(no results)");
            } else {
                for kv in &resp.pairs {
                    let val = String::from_utf8_lossy(&kv.value);
                    println!("{} = {}", kv.key, val);
                }
                println!("--- {} results ---", resp.pairs.len());
            }
        }

        Command::Bench { count, value_size } => {
            let value = "x".repeat(value_size);

            println!("Writing {} keys ({} byte values)...", count, value_size);
            let start = std::time::Instant::now();

            for i in 0..count {
                let key = format!("bench:{:08}", i);
                client
                    .put(PutRequest {
                        key,
                        value: value.clone().into_bytes(),
                        ttl_ms: 0,
                        write_quorum: 0,
                        vector_clock: None,
                    })
                    .await?;
            }

            let write_elapsed = start.elapsed();
            let write_ops = count as f64 / write_elapsed.as_secs_f64();
            println!(
                "Writes: {:.0} ops/sec ({:.2?} total)",
                write_ops, write_elapsed
            );

            println!("Reading {} keys...", count);
            let start = std::time::Instant::now();

            let mut found = 0;
            for i in 0..count {
                let key = format!("bench:{:08}", i);
                let resp = client
                    .get(GetRequest {
                        key,
                        read_quorum: 0,
                    })
                    .await?
                    .into_inner();
                if resp.found {
                    found += 1;
                }
            }

            let read_elapsed = start.elapsed();
            let read_ops = count as f64 / read_elapsed.as_secs_f64();
            println!(
                "Reads:  {:.0} ops/sec ({:.2?} total), {}/{} found",
                read_ops, read_elapsed, found, count
            );
        }
    }

    Ok(())
}
