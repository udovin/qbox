use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Instant};

use super::{
    AppendEntriesRequest, Config, Connection, Data, Entry, Error, LogId, LogStorage, NodeId,
    Transport,
};

pub(super) enum ReplicationMessage<D: Data> {
    Replicate {
        entry: Arc<Entry<D>>,
        commit_index: u64,
    },
    Commit {
        commit_index: u64,
    },
    Terminate,
}

pub(super) enum ReplicationEvent {
    UpdateMatchIndex { node_id: NodeId, log_id: LogId },
    RevertToFollower { node_id: NodeId, term: u64 },
}

pub enum ReplicationState {
    Normal,
    Snapshot,
    Shutdown,
}

pub(super) struct Replication<D, TR, LS>
where
    D: Data,
    TR: Transport<D>,
    LS: LogStorage<D>,
{
    leader_id: NodeId,
    target_id: NodeId,
    logger: slog::Logger,
    transport: Arc<TR>,
    log_storage: Arc<LS>,
    tx: mpsc::UnboundedSender<ReplicationEvent>,
    rx: mpsc::UnboundedReceiver<ReplicationMessage<D>>,
    target_state: ReplicationState,
    current_term: u64,
    last_log_index: u64,
    commit_index: u64,
    heartbeat_timeout: Duration,
    prev_log_id: LogId,
}

impl<D, TR, LS> Replication<D, TR, LS>
where
    D: Data,
    TR: Transport<D>,
    LS: LogStorage<D>,
{
    pub fn spawn(
        leader_id: NodeId,
        target_id: NodeId,
        config: &Config,
        logger: slog::Logger,
        transport: Arc<TR>,
        log_storage: Arc<LS>,
        tx: mpsc::UnboundedSender<ReplicationEvent>,
        rx: mpsc::UnboundedReceiver<ReplicationMessage<D>>,
        current_term: u64,
        last_log_index: u64,
        commit_index: u64,
    ) -> JoinHandle<Result<(), Error>> {
        let this = Self {
            leader_id,
            target_id,
            logger,
            transport,
            log_storage,
            tx,
            rx,
            target_state: ReplicationState::Normal,
            current_term,
            last_log_index,
            commit_index,
            heartbeat_timeout: config.heartbeat_timeout,
            prev_log_id: LogId {
                index: last_log_index,
                term: current_term,
            },
        };
        tokio::spawn(this.run())
    }

    async fn run(mut self) -> Result<(), Error> {
        loop {
            match &self.target_state {
                ReplicationState::Normal => self.run_normal().await.unwrap(),
                ReplicationState::Snapshot => self.run_snapshot().await.unwrap(),
                ReplicationState::Shutdown => return Ok(()),
            }
        }
    }

    async fn run_normal(&mut self) -> Result<(), Error> {
        loop {
            if !matches!(self.target_state, ReplicationState::Normal) {
                return Ok(());
            }
            let heartbeat_timeout = sleep_until(Instant::now() + self.heartbeat_timeout);
            tokio::select! {
                _ = heartbeat_timeout => {
                    self.replicate_append_entries().await?;
                }
                Some(message) = self.rx.recv() => match message {
                    ReplicationMessage::Replicate { entry, commit_index } => {
                        assert!(self.current_term == entry.log_id.term);
                        assert!(self.commit_index <= commit_index);
                        self.last_log_index = entry.log_id.index;
                        self.commit_index = commit_index;
                        self.replicate_append_entries().await?;
                    }
                    ReplicationMessage::Commit { commit_index } => {
                        assert!(self.commit_index <= commit_index);
                        self.commit_index = commit_index;
                        self.replicate_append_entries().await?;
                    }
                    ReplicationMessage::Terminate => {
                        self.target_state = ReplicationState::Shutdown;
                    }
                }
            }
        }
    }

    async fn run_snapshot(&mut self) -> Result<(), Error> {
        loop {
            if !matches!(self.target_state, ReplicationState::Snapshot) {
                return Ok(());
            }
            tokio::select! {
                Some(message) = self.rx.recv() => match message {
                    ReplicationMessage::Replicate { entry, commit_index } => {
                        assert!(self.commit_index <= commit_index);
                        self.last_log_index = entry.log_id.index;
                        self.commit_index = commit_index;
                    }
                    ReplicationMessage::Commit { commit_index } => {
                        assert!(self.commit_index <= commit_index);
                        self.commit_index = commit_index;
                    }
                    ReplicationMessage::Terminate => {
                        self.target_state = ReplicationState::Shutdown;
                    }
                }
            }
        }
    }

    async fn replicate_append_entries(&mut self) -> Result<(), Error> {
        let entries = self
            .log_storage
            .read_entries(self.prev_log_id.index + 1, self.last_log_index + 1)
            .await?;
        slog::debug!(self.logger, "Replicate entries"; slog::o!("count" => entries.len(), "target_id" => self.target_id));
        let request = AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.leader_id,
            prev_log_id: self.prev_log_id,
            leader_commit: self.commit_index,
            entries,
        };
        let mut connection = self.transport.connect(self.target_id).await?;
        let response = match connection.append_entries(request).await {
            Err(err) => return Err(err),
            Ok(response) => response,
        };
        if response.term > self.current_term {
            self.tx.send(ReplicationEvent::RevertToFollower {
                node_id: self.target_id,
                term: response.term,
            })?;
        }
        if !response.success {
            return Ok(());
        }
        self.prev_log_id.index = self.last_log_index;
        self.prev_log_id.term = self.current_term;
        self.tx.send(ReplicationEvent::UpdateMatchIndex {
            node_id: self.target_id,
            log_id: self.prev_log_id,
        })?;
        Ok(())
    }
}
