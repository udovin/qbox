use std::fs::File;
use std::io::{BufReader, Read, ErrorKind, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use clap::{Parser, Subcommand, Args};
use qbox::qraft::tcp_transport::TcpTransport;
use qbox::qraft::{Raft, Config, Data};
use qbox::qraft::mem_storage::{MemStateMachine, MemLogStorage};
use rand::{thread_rng, Rng};
use serde::{Serialize, Deserialize};

#[derive(Args, Debug)]
struct ServerArgs {
    #[arg(short, long, default_value = "0.0.0.0:4242")]
    addr: SocketAddr,
    #[arg(long, default_value = "false")]
    init: bool,
    #[arg(long, default_value = "/tmp/qbox")]
    data_dir: PathBuf,
    #[arg(long)]
    join: Option<SocketAddr>,
    // TODO: Remove this.
    #[arg(long, default_value = "0.0.0.0:4243")]
    raft_addr: SocketAddr,
}

#[derive(Subcommand, Debug)]
enum Command {
    Server(ServerArgs),
}

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Serialize, Deserialize)]
struct EmptyData {}

impl Data for EmptyData {}

fn get_node_id(dir: &Path) -> Result<u64, std::io::Error> {
    let file_path = dir.join("node_id.bin");
    match File::open(file_path.clone()) {
        Ok(mut file) => {
            let mut buf = [0; 8];
            file.read_exact(&mut buf)?;
            Ok(u64::from_le_bytes(buf))
        }
        Err(err) => {
            match err.kind() {
                ErrorKind::NotFound => {
                    let id: u64 = thread_rng().gen();
                    let mut file = File::create(file_path)?;
                    file.write_all(id.to_le_bytes().as_slice())?;
                    Ok(id)
                }
                _ => Err(err)
            }
        }
    }
}

async fn async_server_main(args: ServerArgs) {
    let config = Config::default();
    let transport = TcpTransport::new();
    let log_storage = MemLogStorage::new();
    let state_machine = MemStateMachine::new();
    let node_id = get_node_id(args.data_dir.as_path()).unwrap();
    let raft = Raft::new(node_id, args.raft_addr, config, transport, log_storage, state_machine).unwrap();
    println!("Node with id {}", node_id);
    if let Some(addr) = args.join {
        println!("Joining cluster");
        // leader_connection.add_node(raft_id, args.raft_addr)
    } else if args.init {
        println!("Initializing cluster");
        raft.init_cluster().await.unwrap();
    }
    let raft = Arc::new(raft);
    let server = TcpTransport::spawn(args.raft_addr, raft.clone());
    tokio::select!{
        _ = server => {}
        _ = raft.join() => {}
    };
    raft.shutdown().await.unwrap();
}

fn server_main(args: ServerArgs) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_server_main(args));
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Server(args) => server_main(args),
    }
}
