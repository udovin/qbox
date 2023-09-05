use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Interval, interval};

use super::{NodeId, Error, Config, Transport, Data};

pub(super) enum ReplicationMessage {
    Replicate {
        last_log_index: u64,
        commit_index: u64,
    },
    Terminate,
}

pub(super) struct ReplicationEvent {}

pub(super) enum ReplicationState {
    Normal,
    Snapshot,
    Shutdown,
}

pub(super) struct RaftReplication<D, TR>
where
    D: Data,
    TR: Transport<D>,
{
    leader_id: NodeId,
    target_id: NodeId,
    tx: mpsc::UnboundedSender<ReplicationEvent>,
    rx: mpsc::UnboundedReceiver<ReplicationMessage>,
    target_state: ReplicationState,
    current_term: u64,
    last_log_index: u64,
    commit_index: u64,
    heartbeat: Interval,
    heartbeat_timeout: Duration,
    transport: Arc<TR>,
    _phantom: PhantomData<D>,
}

impl<D, TR> RaftReplication<D, TR>
where
    D: Data,
    TR: Transport<D>,
{
    pub fn spawn(
        leader_id: NodeId,
        target_id: NodeId,
        config: &Config,
        transport: Arc<TR>,
        tx: mpsc::UnboundedSender<ReplicationEvent>,
        rx: mpsc::UnboundedReceiver<ReplicationMessage>,
        current_term: u64,
        last_log_index: u64,
        commit_index: u64,
    ) -> JoinHandle<Result<(), Error>> {
        let this = Self {
            leader_id,
            target_id,
            transport,
            tx,
            rx,
            target_state: ReplicationState::Normal,
            current_term,
            last_log_index,
            commit_index,
            heartbeat: interval(config.heartbeat_timeout),
            heartbeat_timeout: config.heartbeat_timeout,
            _phantom: PhantomData,
        };
        tokio::spawn(this.run())
    }

    async fn run(mut self) -> Result<(), Error> {
        loop {
            match &self.target_state {
                ReplicationState::Normal => self.run_normal().await?,
                ReplicationState::Snapshot => self.run_snapshot().await?,
                ReplicationState::Shutdown => return Ok(()),
            }
        }
    }

    async fn run_normal(&mut self) -> Result<(), Error> {
        loop {
            if !matches!(self.target_state, ReplicationState::Normal) {
                return Ok(());
            }
            tokio::select! {
                _ = self.heartbeat.tick() => {

                }
                Some(message) = self.rx.recv() => match message {
                    ReplicationMessage::Replicate { last_log_index, commit_index } => {
                        self.last_log_index = last_log_index;
                        self.commit_index = commit_index;
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
                _ = self.heartbeat.tick() => {

                }
                Some(message) = self.rx.recv() => match message {
                    ReplicationMessage::Replicate { last_log_index, commit_index } => {
                        self.last_log_index = last_log_index;
                        self.commit_index = commit_index;
                    }
                    ReplicationMessage::Terminate => {
                        self.target_state = ReplicationState::Shutdown;
                    }
                }
            }
        }
    }
}
