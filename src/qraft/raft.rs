use std::marker::PhantomData;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use super::{
    AppendEntriesRequest, AppendEntriesResponse, Config, Data, Error, InstallSnapshotRequest,
    InstallSnapshotResponse, LogStorage, Message, Node, NodeId, RaftNode, RequestVoteRequest,
    RequestVoteResponse, Response, StateMachine, Transport,
};

pub struct Raft<N, D, R, TR, LS, SM>
where
    N: Node,
    D: Data,
    R: Response,
    TR: Transport<N, D>,
    LS: LogStorage<N, D>,
    SM: StateMachine<N, D, R>,
{
    tx: mpsc::UnboundedSender<Message<N, D, R>>,
    tx_shutdown: Mutex<Option<oneshot::Sender<()>>>,
    node_handle: Mutex<Option<JoinHandle<Result<(), Error>>>>,
    _phantom: (
        PhantomData<R>,
        PhantomData<TR>,
        PhantomData<LS>,
        PhantomData<SM>,
    ),
}

impl<N, D, R, TR, LS, SM> Raft<N, D, R, TR, LS, SM>
where
    N: Node,
    D: Data,
    R: Response,
    TR: Transport<N, D>,
    LS: LogStorage<N, D>,
    SM: StateMachine<N, D, R>,
{
    pub fn new(
        id: NodeId,
        node: N,
        config: Config,
        transport: TR,
        log_storage: LS,
        state_machine: SM,
    ) -> Result<Self, Error> {
        config.validate()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let (tx_shutdown, rx_shutdown) = oneshot::channel();
        let node_handle = RaftNode::<N, D, R, TR, LS, SM>::spawn(
            id,
            node,
            config,
            transport,
            log_storage,
            state_machine,
            rx,
            rx_shutdown,
        );
        Ok(Self {
            tx,
            tx_shutdown: Mutex::new(Some(tx_shutdown)),
            node_handle: Mutex::new(Some(node_handle)),
            _phantom: (PhantomData, PhantomData, PhantomData, PhantomData),
        })
    }

    pub async fn apply_entry(&self, data: D) -> Result<R, Error> {
        let (tx, rx) = oneshot::channel();
        let message = Message::ApplyEntry { data, tx };
        self.tx.send(message)?;
        Ok(rx.await??)
    }

    pub async fn append_entries(
        &self,
        request: AppendEntriesRequest<N, D>,
    ) -> Result<AppendEntriesResponse, Error> {
        let (tx, rx) = oneshot::channel();
        let message = Message::AppendEntries { request, tx };
        self.tx.send(message)?;
        Ok(rx.await??)
    }

    pub async fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Error> {
        let (tx, rx) = oneshot::channel();
        let message = Message::InstallSnapshot { request, tx };
        self.tx.send(message)?;
        Ok(rx.await??)
    }

    pub async fn request_vote(
        &self,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Error> {
        let (tx, rx) = oneshot::channel();
        let message = Message::RequestVote { request, tx };
        self.tx.send(message)?;
        Ok(rx.await??)
    }

    pub async fn init_cluster(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let message = Message::InitCluster { tx };
        self.tx.send(message)?;
        Ok(rx.await??)
    }

    pub async fn add_node(&self, id: NodeId, node: N) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let message = Message::AddNode { id, node, tx };
        self.tx.send(message)?;
        Ok(rx.await??)
    }

    pub async fn join(&self) -> Result<(), Error> {
        if let Some(handle) = self.node_handle.lock().await.take() {
            handle.await??;
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        if let Some(tx) = self.tx_shutdown.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.node_handle.lock().await.take() {
            handle.await??;
        }
        Ok(())
    }
}
