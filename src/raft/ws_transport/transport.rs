use std::sync::Arc;
use std::{net::SocketAddr, marker::PhantomData};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Serialize, Deserialize};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};

use crate::raft::{Raft, LogStorage, StateMachine, Data, AppendEntriesRequest, InstallSnapshotRequest, RequestVoteRequest, Transport, NodeId, Error, Connection, AppendEntriesResponse, InstallSnapshotResponse, RequestVoteResponse, Response};

#[async_trait::async_trait]
pub trait NodeMetaStorage<D>: Send + Sync + 'static {
    async fn get_node_meta(&self, id: NodeId) -> Result<D, Error>;
}

#[derive(Serialize, Deserialize)]
enum WsMessage<D: Data> {
    AppendEntries(AppendEntriesRequest<D>),
    InstallSnapshot(InstallSnapshotRequest),
    RequestVote(RequestVoteRequest),
}

#[derive(Debug, Serialize, Deserialize)]
enum WsError {
    Message(String),
}

pub struct WsConnection<D: Data> {
    tx: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    rx: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    _phantom: PhantomData<D>,
}

#[async_trait::async_trait]
impl<D: Data + Serialize> Connection<D> for WsConnection<D> {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<D>,
    ) -> Result<AppendEntriesResponse, Error> {
        let message = WsMessage::AppendEntries(request);
        self.tx.send(Message::binary(bincode::serialize(&message)?)).await?;
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
        let message = WsMessage::<D>::InstallSnapshot(request);
        self.tx.send(Message::binary(bincode::serialize(&message)?)).await?;
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
        let message = WsMessage::<D>::RequestVote(request);
        self.tx.send(Message::binary(bincode::serialize(&message)?)).await?;
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

pub struct WsTransport<NM: NodeMetaStorage<SocketAddr>> {
    storage: Arc<NM>,
}

impl<NM: NodeMetaStorage<SocketAddr>> WsTransport<NM> {
    pub fn new(storage: Arc<NM>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl<D, NM> Transport<D> for WsTransport<NM>
where
    D: Data + Serialize,
    NM: NodeMetaStorage<SocketAddr>,
{
    type Connection = WsConnection<D>;

    async fn connect(&self, id: NodeId) -> Result<WsConnection<D>, Error> {
        let node = self.storage.get_node_meta(id).await?;
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

pub async fn handle_connection<D, R, TR, LS, SM>(
    ws: warp::ws::WebSocket,
    raft: Arc<Raft<D, R, TR, LS, SM>>,
) -> Result<(), Error>
where
    D: Data + DeserializeOwned,
    R: Response + Serialize,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    let (mut tx, mut rx) = ws.split();
    let map_err = |err: Error| WsError::Message(err.to_string());
    while let Some(request) = rx.next().await {
        match request {
            Ok(data) => match bincode::deserialize::<WsMessage<D>>(data.as_bytes())? {
                WsMessage::AppendEntries(request) => {
                    let response =
                        bincode::serialize(&raft.append_entries(request).await.map_err(map_err))?;
                    tx.send(warp::ws::Message::binary(response)).await?;
                }
                WsMessage::InstallSnapshot(request) => {
                    let response =
                        bincode::serialize(&raft.install_snapshot(request).await.map_err(map_err))?;
                    tx.send(warp::ws::Message::binary(response)).await?;
                }
                WsMessage::RequestVote(request) => {
                    let response =
                        bincode::serialize(&raft.request_vote(request).await.map_err(map_err))?;
                    tx.send(warp::ws::Message::binary(response)).await?;
                }
            },
            Err(_) => break,
        }
    }
    Ok(())
}
