use std::net::SocketAddr;
use clap::{Parser, Subcommand, Args};
use qbox::qraft::tcp_transport::TcpTransport;
use qbox::qraft::{Raft, Config, Data};
use qbox::qraft::mem_storage::{MemStateMachine, MemLogStorage};
use serde::Serialize;

#[derive(Args, Debug)]
struct ServerArgs {
    #[arg(short, long, default_value = "0.0.0.0:4242")]
    addr: SocketAddr,
    #[arg(long, default_value = "false")]
    init: bool,
    #[arg(long)]
    join: Option<SocketAddr>,
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

#[derive(Clone, Serialize)]
struct EmptyData {}

impl Data for EmptyData {}

async fn async_server_main(args: ServerArgs) {
    let _ = args;
    let config = Config::default();
    let transport = TcpTransport::new();
    let log_storage = MemLogStorage::<_, EmptyData>::new();
    let state_machine = MemStateMachine::new();
    let raft = Raft::new(1, config, transport, log_storage, state_machine).unwrap();
    raft.initialize_node(args.addr).await.unwrap();
    raft.join().await.unwrap();
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
