use clap::{Args, Parser, Subcommand};
use qbox::raft::mem_storage::{Action, MemLogStorage, MemStateMachine};
use qbox::raft::ws_transport::{handle_connection, WsTransport};
use qbox::raft::{Config, Data, LogStorage, NodeId, Raft, Response, StateMachine, Transport};
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use slog::Drain;
use std::convert::Infallible;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use warp::Filter;

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
        Err(err) => match err.kind() {
            ErrorKind::NotFound => {
                let id: u64 = thread_rng().gen();
                let mut file = File::create(file_path)?;
                file.write_all(id.to_le_bytes().as_slice())?;
                Ok(id)
            }
            _ => Err(err),
        },
    }
}

async fn raft_handle<D, R, TR, LS, SM>(
    ws: warp::ws::Ws,
    raft: Arc<Raft<D, R, TR, LS, SM>>,
) -> Result<impl warp::Reply, Infallible>
where
    D: Data + DeserializeOwned,
    R: Response + Serialize,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    Ok(ws.on_upgrade(|socket| async {
        handle_connection(socket, raft).await.unwrap();
    }))
}

#[derive(Serialize, Deserialize)]
struct AddNodeMessage {
    id: NodeId,
    node: SocketAddr,
}

async fn raft_add_node_handle<R, TR, LS, SM>(
    body: AddNodeMessage,
    raft: Arc<Raft<Action, R, TR, LS, SM>>,
) -> Result<impl warp::Reply, Infallible>
where
    R: Response + Serialize,
    TR: Transport<Action>,
    LS: LogStorage<Action>,
    SM: StateMachine<Action, R>,
{
    let node = Action::Set {
        key: format!("nodes/{}", body.id),
        value: body.node.to_string(),
    };
    raft.write_data(node).await.unwrap();
    raft.add_node(body.id).await.unwrap();
    Ok(warp::reply::with_status("", warp::http::StatusCode::OK))
}

async fn async_server_main(args: ServerArgs) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(4096)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .fuse();
    let logger = slog::Logger::root(drain, slog::o!());
    let config = Config::default();
    let log_storage = Arc::new(MemLogStorage::new());
    let state_machine = Arc::new(MemStateMachine::new());
    let transport = Arc::new(WsTransport::new(state_machine.clone()));
    let node_id = get_node_id(args.data_dir.as_path()).unwrap();
    let raft = Raft::new(
        node_id,
        config,
        logger.clone(),
        transport,
        log_storage,
        state_machine.clone(),
    )
    .unwrap();
    let raft = Arc::new(raft);
    let raft_route = {
        let raft = raft.clone();
        warp::path("raft")
            .and(warp::ws())
            .and(warp::any().map(move || raft.clone()))
            .and_then(raft_handle)
    };
    let raft_add_node_route = {
        let raft = raft.clone();
        warp::path("raft")
            .and(warp::path("add-node"))
            .and(warp::body::json())
            .and(warp::any().map(move || raft.clone()))
            .and_then(raft_add_node_handle)
    };
    let routes = raft_route.or(raft_add_node_route);
    let server = warp::serve(routes).run(args.addr);
    slog::info!(logger, "Initiaizing node"; slog::o!("id" => node_id));
    if let Some(addr) = args.join {
        slog::info!(logger, "Joining cluster");
        let client = reqwest::Client::new();
        let _ = client
            .post(format!("http://{}/raft/add-node", addr))
            .json(&AddNodeMessage {
                id: node_id,
                node: args.addr,
            })
            .send()
            .await
            .unwrap();
        slog::info!(logger, "Cluster joined");
    } else if args.init {
        slog::info!(logger, "Initiaizing cluster");
        raft.init_cluster().await.unwrap();
        let node = Action::Set {
            key: format!("nodes/{}", node_id),
            value: args.addr.to_string(),
        };
        raft.write_data(node).await.unwrap();
        slog::info!(logger, "Cluster initialized");
    }
    tokio::select! {
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
