use std::marker::PhantomData;
use std::net::SocketAddr;
use serde::ser::Serialize;
use serde::de::DeserializeOwned;

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::qraft::{Transport, Connection, NodeId, Error, Node, Data, RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotResponse, InstallSnapshotRequest};

impl Node for SocketAddr {}

pub struct TcpConnection<D: Data> {
    stream: TcpStream,
    _phantom: PhantomData<D>,
}

const MAX_LEN: u64 = 64 * 1024 * 1024; // 64 MiB.

impl<D: Data + Serialize> TcpConnection<D> {
    async fn write_message<T: Serialize>(&mut self, message: T) -> Result<(), Error> {
        let bytes = bincode::serialize(&message)?;
        let len = bytes.len() as u64;
        if len > MAX_LEN {
            Err("too large message")?;
        }
        self.stream.write_u64(len).await?;
        self.stream.write_u64(!len).await?;
        Ok(self.stream.write_all(bytes.as_slice()).await?)
    }

    async fn read_message<T: DeserializeOwned>(&mut self) -> Result<T, Error> {
        let len = self.stream.read_u64().await?;
        if len > MAX_LEN {
            Err("too large message")?;
        }
        let neg_len = self.stream.read_u64().await?;
        if len != !neg_len {
            Err("corrupted message length")?;
        }
        let mut bytes = vec![0u8; len as usize];
        self.stream.read_exact(bytes.as_mut_slice()).await?;
        Ok(bincode::deserialize(bytes.as_slice())?)
    }
}

#[async_trait::async_trait]
impl<D: Data + Serialize> Connection<SocketAddr, D> for TcpConnection<D> {
    async fn append_entries(&mut self, request: AppendEntriesRequest<SocketAddr, D>) -> Result<AppendEntriesResponse, Error> {
        self.write_message(request).await?;
        self.read_message().await
    }

    async fn install_snapshot(&mut self, request: InstallSnapshotRequest) -> Result<InstallSnapshotResponse, Error> {
        self.write_message(request).await?;
        self.read_message().await
    }

    async fn request_vote(&mut self, request: RequestVoteRequest) -> Result<RequestVoteResponse, Error> {
        self.write_message(request).await?;
        self.read_message().await
    }
}

pub struct TcpTransport {}

impl TcpTransport {
    pub fn new() -> Self {
        Self { }
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
