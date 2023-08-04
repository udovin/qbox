use clap::{Args, Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use qbox::qraft::mem_storage::{Action, ActionResponse, MemLogStorage, MemStateMachine};
use qbox::qraft::tcp_transport::TcpTransport;
use qbox::qraft::{
    AppendEntriesRequest, Config, Data, Entry, Error, HardState, InstallSnapshotRequest, LogId,
    LogStorage, MembershipConfig, Node, Raft, RequestVoteRequest, Response, StateMachine,
    Transport,
};
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::ws::Message;
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

struct SmWrap<N: Node>(Arc<RwLock<MemStateMachine<N>>>);

#[async_trait::async_trait]
impl<N: Node> StateMachine<N, Action, ActionResponse> for SmWrap<N> {
    async fn get_applied_log_id(&self) -> Result<LogId, Error> {
        self.0.read().await.get_applied_log_id().await
    }

    async fn get_membership_config(&self) -> Result<MembershipConfig<N>, Error> {
        self.0.read().await.get_membership_config().await
    }

    async fn apply_entries(
        &mut self,
        entires: Vec<Entry<N, Action>>,
    ) -> Result<Vec<ActionResponse>, Error> {
        self.0.write().await.apply_entries(entires).await
    }

    async fn get_hard_state(&self) -> Result<HardState, Error> {
        self.0.read().await.get_hard_state().await
    }

    async fn save_hard_state(&mut self, hard_state: HardState) -> Result<(), Error> {
        self.0.write().await.save_hard_state(hard_state).await
    }
}

#[derive(Serialize, Deserialize)]
enum WsMessage<N: Node, D: Data> {
    AppendEntries(AppendEntriesRequest<N, D>),
    InstallSnapshot(InstallSnapshotRequest),
    RequestVote(RequestVoteRequest),
}

#[derive(Serialize, Deserialize)]
enum WsError {
    Message(String),
}

async fn handle_raft_connection<N, D, R, TR, LS, SM>(
    ws: warp::ws::WebSocket,
    raft: Arc<Raft<N, D, R, TR, LS, SM>>,
) -> Result<(), Error>
where
    N: Node + DeserializeOwned,
    D: Data + DeserializeOwned,
    R: Response + Serialize,
    TR: Transport<N, D>,
    LS: LogStorage<N, D>,
    SM: StateMachine<N, D, R>,
{
    let (mut tx, mut rx) = ws.split();
    let map_err = |err: Error| WsError::Message(err.to_string());
    while let Some(request) = rx.next().await {
        match request {
            Ok(data) => match bincode::deserialize::<WsMessage<N, D>>(data.as_bytes())? {
                WsMessage::AppendEntries(request) => {
                    let response =
                        bincode::serialize(&raft.append_entries(request).await.map_err(map_err))?;
                    tx.send(Message::binary(response)).await?;
                }
                WsMessage::InstallSnapshot(request) => {
                    let response =
                        bincode::serialize(&raft.install_snapshot(request).await.map_err(map_err))?;
                    tx.send(Message::binary(response)).await?;
                }
                WsMessage::RequestVote(request) => {
                    let response =
                        bincode::serialize(&raft.request_vote(request).await.map_err(map_err))?;
                    tx.send(Message::binary(response)).await?;
                }
            },
            Err(_) => break,
        }
    }
    Ok(())
}

async fn raft_handle<N, D, R, TR, LS, SM>(
    ws: warp::ws::Ws,
    raft: Arc<Raft<N, D, R, TR, LS, SM>>,
) -> Result<impl warp::Reply, Infallible>
where
    N: Node + DeserializeOwned,
    D: Data + DeserializeOwned,
    R: Response + Serialize,
    TR: Transport<N, D>,
    LS: LogStorage<N, D>,
    SM: StateMachine<N, D, R>,
{
    Ok(ws.on_upgrade(|socket| async {
        handle_raft_connection(socket, raft).await.unwrap();
    }))
}

async fn async_server_main(args: ServerArgs) {
    let config = Config::default();
    let transport = TcpTransport::new();
    let log_storage = MemLogStorage::new();
    let state_machine = Arc::new(RwLock::new(MemStateMachine::new()));
    let node_id = get_node_id(args.data_dir.as_path()).unwrap();
    let raft = Raft::new(
        node_id,
        args.addr,
        config,
        transport,
        log_storage,
        SmWrap(state_machine.clone()),
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
    let routes = raft_route;
    let server = warp::serve(routes).run(args.addr);
    println!("Node with id {}", node_id);
    if let Some(addr) = args.join {
        println!("Joining cluster");
        // leader_connection.add_node(raft_id, args.raft_addr)
    } else if args.init {
        println!("Initializing cluster");
        raft.init_cluster().await.unwrap();
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
