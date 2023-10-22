use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use super::{
    AppendEntriesRequest, AppendEntriesResponse, Config, Data, DataEntry, EntryPayload, Error,
    InstallSnapshotRequest, InstallSnapshotResponse, LogStorage, Message, NodeId, RaftNode,
    RequestVoteRequest, RequestVoteResponse, Response, StateMachine, Transport,
};

pub struct Raft<D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    tx: mpsc::UnboundedSender<Message<D, R>>,
    tx_shutdown: Mutex<Option<oneshot::Sender<()>>>,
    handle: Mutex<Option<JoinHandle<Result<(), Error>>>>,
    _phantom: (PhantomData<TR>, PhantomData<LS>, PhantomData<SM>),
}

impl<D, R, TR, LS, SM> Raft<D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    pub fn new(
        id: NodeId,
        config: Config,
        logger: slog::Logger,
        transport: Arc<TR>,
        log_storage: Arc<LS>,
        state_machine: Arc<SM>,
    ) -> Result<Self, Error> {
        config.validate()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let (tx_shutdown, rx_shutdown) = oneshot::channel();
        let handle = RaftNode::<D, R, TR, LS, SM>::spawn(
            id,
            config,
            logger,
            transport,
            log_storage,
            state_machine,
            rx,
            rx_shutdown,
        );
        Ok(Self {
            tx,
            tx_shutdown: Mutex::new(Some(tx_shutdown)),
            handle: Mutex::new(Some(handle)),
            _phantom: (PhantomData, PhantomData, PhantomData),
        })
    }

    pub async fn append_entries(
        &self,
        request: AppendEntriesRequest<D>,
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

    pub async fn add_node(&self, id: NodeId) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let message = Message::AddNode { id, tx };
        self.tx.send(message)?;
        Ok(rx.await??)
    }

    pub async fn write_data(&self, data: D) -> Result<R, Error> {
        let (tx, rx) = oneshot::channel();
        let message = Message::WriteEntry {
            entry: EntryPayload::Data(DataEntry { data }),
            tx,
        };
        self.tx.send(message)?;
        Ok(rx.await??)
    }

    pub async fn join(&self) -> Result<(), Error> {
        if let Some(handle) = self.handle.lock().await.take() {
            handle.await??;
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        if let Some(tx) = self.tx_shutdown.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.lock().await.take() {
            handle.await??;
        }
        Ok(())
    }
}
