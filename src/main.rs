use clap::{Args, Parser, Subcommand};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use qbox::qraft::mem_storage::{Action, ActionResponse, MemLogStorage, MemStateMachine};
use qbox::qraft::tcp_transport::TcpTransport;
use qbox::qraft::{
    AppendEntriesRequest, Config, Data, Entry, Error, HardState, InstallSnapshotRequest, LogId,
    LogStorage, MembershipConfig, Node, Raft, RequestVoteRequest, Response, StateMachine,
    Transport, Connection, AppendEntriesResponse, InstallSnapshotResponse, RequestVoteResponse, NodeId,
};
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};
use std::convert::Infallible;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::marker::PhantomData;
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

#[derive(Debug, Serialize, Deserialize)]
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

struct WsConnection<D: Data> {
    tx: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
    rx: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    _phantom: PhantomData<D>,
}

#[async_trait::async_trait]
impl<D: Data + Serialize> Connection<SocketAddr, D> for WsConnection<D> {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<SocketAddr, D>,
    ) -> Result<AppendEntriesResponse, Error> {
        let message = WsMessage::AppendEntries(request);
        self.tx.send(protocol::Message::binary(bincode::serialize(&message)?)).await?;
        match self.rx.next().await {
            Some(Ok(data)) => {
                Ok(bincode::deserialize::<Result<AppendEntriesResponse, WsError>>(data.to_string().as_bytes())?.unwrap())
            }
            Some(Err(err)) => {
                Err(err)?
            }
            None => Err("unexpected result")?
        }
    }

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Error> {
        let message = WsMessage::<SocketAddr, D>::InstallSnapshot(request);
        self.tx.send(protocol::Message::binary(bincode::serialize(&message)?)).await?;
        match self.rx.next().await {
            Some(Ok(data)) => {
                Ok(bincode::deserialize::<Result<InstallSnapshotResponse, WsError>>(data.to_string().as_bytes())?.unwrap())
            }
            Some(Err(err)) => {
                Err(err)?
            }
            None => Err("unexpected result")?
        }
    }

    async fn request_vote(
        &mut self,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Error> {
        let message = WsMessage::<SocketAddr, D>::RequestVote(request);
        self.tx.send(protocol::Message::binary(bincode::serialize(&message)?)).await?;
        match self.rx.next().await {
            Some(Ok(data)) => {
                Ok(bincode::deserialize::<Result<RequestVoteResponse, WsError>>(data.to_string().as_bytes())?.unwrap())
            }
            Some(Err(err)) => {
                Err(err)?
            }
            None => Err("unexpected result")?
        }
    }
}

struct WsTransport {}

#[async_trait::async_trait]
impl<D: Data + Serialize> Transport<SocketAddr, D> for WsTransport {
    type Connection = WsConnection<D>;

    async fn connect(&self, _id: NodeId, node: &SocketAddr) -> Result<WsConnection<D>, Error> {
        let response = connect_async(format!("http://{}/raft", node)).await?;
        let stream = response.0;
        let (tx, rx) = stream.split();
        Ok(WsConnection {
            tx,
            rx,
            _phantom: PhantomData,
        })
    }
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

#[derive(Serialize, Deserialize)]
struct AddNodeMessage<N: Node> {
    id: NodeId,
    node: N,
}

async fn raft_add_node_handle<N, D, R, TR, LS, SM>(
    body: AddNodeMessage<N>,
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
    raft.add_node(body.id, body.node).await.unwrap();
    Ok(warp::reply::with_status("", warp::http::StatusCode::OK))
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
    ).unwrap();
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
    let routes = raft_route
        .or(raft_add_node_route);
    let server = warp::serve(routes).run(args.addr);
    println!("Node with id {}", node_id);
    if let Some(addr) = args.join {
        let client = reqwest::Client::new();
        let _ = client.post(format!("http://{}/raft/add-node", addr))
            .json(&AddNodeMessage {
                id: node_id,
                node: args.addr,
            })
            .send().await.unwrap();
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
