use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use serde::ser::Serialize;
use serde::de::DeserializeOwned;

use tokio::net::{TcpStream, TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::qraft::{Transport, Connection, NodeId, Error, Node, Data, RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotResponse, InstallSnapshotRequest, Raft, Response, LogStorage, StateMachine};

impl Node for SocketAddr {}

pub struct TcpConnection<D: Data> {
    stream: TcpStream,
    _phantom: PhantomData<D>,
}

const MAX_LEN: u64 = 64 * 1024 * 1024; // 64 MiB.
const APPEND_ENTRIES: u64 = 1;
const INSTALL_SNAPSHOT: u64 = 2;
const REQUEST_VOTE: u64 = 3;

impl<D: Data + Serialize> TcpConnection<D> {
    async fn write_message<T: Serialize>(&mut self, kind: u64, message: T) -> Result<(), Error> {
        write_message(&mut self.stream, kind, message).await
    }

    async fn read_message<T: DeserializeOwned>(&mut self, read_kind: u64) -> Result<T, Error> {
        let kind = self.stream.read_u64().await?;
        if read_kind != kind {
            Err("invalid message kind")?;
        }
        read_message(&mut self.stream).await
    }
}

#[async_trait::async_trait]
impl<D: Data + Serialize> Connection<SocketAddr, D> for TcpConnection<D> {
    async fn append_entries(&mut self, request: AppendEntriesRequest<SocketAddr, D>) -> Result<AppendEntriesResponse, Error> {
        self.write_message(APPEND_ENTRIES, request).await?;
        self.read_message(APPEND_ENTRIES).await
    }

    async fn install_snapshot(&mut self, request: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Error> {
        self.write_message(INSTALL_SNAPSHOT, request).await?;
        self.read_message(INSTALL_SNAPSHOT).await
    }

    async fn request_vote(&mut self, request: RequestVoteRequest) -> Result<RequestVoteResponse, Error> {
        self.write_message(REQUEST_VOTE, request).await?;
        self.read_message(REQUEST_VOTE).await
    }
}

pub struct TcpTransport {}

impl TcpTransport {
    pub fn new() -> Self {
        Self { }
    }

    pub fn spawn<D, R, LS, SM>(addr: SocketAddr, raft: Arc<Raft<SocketAddr, D, R, TcpTransport, LS, SM>>) -> JoinHandle<Result<(), Error>>
    where
        D: Data + Serialize + DeserializeOwned,
        R: Response,
        LS: LogStorage<SocketAddr, D>,
        SM: StateMachine<SocketAddr, D, R>
    {
        let server = async move {
            let listener = TcpListener::bind(addr).await?;
            loop {
                let (stream, _) = listener.accept().await?;
                tokio::spawn(Self::handle_connection(stream, raft.clone()));
            }
        };
        tokio::spawn(server)
    }

    async fn handle_connection<D, R, LS, SM>(mut stream: TcpStream, raft: Arc<Raft<SocketAddr, D, R, TcpTransport, LS, SM>>) -> Result<(), Error>
    where
        D: Data + Serialize + DeserializeOwned,
        R: Response,
        LS: LogStorage<SocketAddr, D>,
        SM: StateMachine<SocketAddr, D, R>
    {
        loop {
            let read_kind = stream.read_u64().await?;
            match read_kind {
                APPEND_ENTRIES => {
                    let response = raft.append_entries(read_message(&mut stream).await?).await?;
                    write_message(&mut stream, APPEND_ENTRIES, response).await?;
                }
                INSTALL_SNAPSHOT => {
                    let response = raft.install_snapshot(read_message(&mut stream).await?).await?;
                    write_message(&mut stream, INSTALL_SNAPSHOT, response).await?;
                }
                REQUEST_VOTE => {
                    let response = raft.request_vote(read_message(&mut stream).await?).await?;
                    write_message(&mut stream, REQUEST_VOTE, response).await?;
                }
                kind => Err(format!("unsupported message: {}", kind))?,
            }
        }
    }
}

#[async_trait::async_trait]
impl<D: Data + Serialize> Transport<SocketAddr, D> for TcpTransport {
    type Connection = TcpConnection<D>;

    async fn connect(&self, _id: NodeId, node: &SocketAddr) -> Result<TcpConnection<D>, Error> {
        let stream = TcpStream::connect(node).await?;
        Ok(TcpConnection { stream, _phantom: PhantomData })
    }
}

async fn read_message<S: Unpin + AsyncReadExt, T: DeserializeOwned>(stream: &mut S) -> Result<T, Error> {
    let len = stream.read_u64().await?;
    if len > MAX_LEN {
        Err("too large message")?;
    }
    let mut bytes = vec![0u8; len as usize];
    stream.read_exact(bytes.as_mut_slice()).await?;
    Ok(bincode::deserialize(bytes.as_slice())?)
}

async fn write_message<S: Unpin + AsyncWriteExt, T: Serialize>(stream: &mut S, kind: u64, message: T) -> Result<(), Error> {
    let bytes = bincode::serialize(&message)?;
    let len = bytes.len() as u64;
    if len > MAX_LEN {
        Err("too large message")?;
    }
    stream.write_u64(kind).await?;
    stream.write_u64(len).await?;
    Ok(stream.write_all(bytes.as_slice()).await?)
}
