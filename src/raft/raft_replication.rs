use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{NodeId, Node, Error};

pub(super) enum ReplicationMessage {
    Terminate,
}

pub(super) struct ReplicationEvent {}

pub(super) enum ReplicationState {
    Normal,
    Snapshot,
    Shutdown,
}

pub(super) struct RaftReplication<N: Node> {
    leader_id: NodeId,
    target_id: NodeId,
    target_node: N,
    tx: mpsc::UnboundedSender<ReplicationEvent>,
    rx: mpsc::UnboundedReceiver<ReplicationMessage>,
    target_state: ReplicationState,
}

impl<N: Node> RaftReplication<N> {
    pub fn spawn(
        leader_id: NodeId,
        target_id: NodeId,
        target_node: N,
        tx: mpsc::UnboundedSender<ReplicationEvent>,
        rx: mpsc::UnboundedReceiver<ReplicationMessage>,
    ) -> JoinHandle<Result<(), Error>> {
        let this = Self {
            leader_id,
            target_id,
            target_node,
            tx,
            rx,
            target_state: ReplicationState::Normal,
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
                Some(message) = self.rx.recv() => match message {
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
                    ReplicationMessage::Terminate => {
                        self.target_state = ReplicationState::Shutdown;
                    }
                }
            }
        }
    }
}
