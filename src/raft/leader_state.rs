use std::collections::{HashMap, HashSet};
use std::iter::once;
use std::sync::Arc;

use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::{
    has_config_change, ConfigChangeEntry, Data, EntryPayload, Error, LogId, LogStorage, Message,
    NodeId, RaftNode, Replication, ReplicationEvent, ReplicationMessage, Response, State,
    StateMachine, Transport,
};

struct ReplicationState<D: Data> {
    tx: mpsc::UnboundedSender<ReplicationMessage<D>>,
    handle: JoinHandle<Result<(), Error>>,
    match_log_id: LogId,
    #[allow(unused)]
    shutdown_index: Option<u64>,
}

impl<D: Data> ReplicationState<D> {
    async fn shutdown(self) -> Result<(), Error> {
        self.tx.send(ReplicationMessage::Terminate)?;
        Ok(self.handle.await??)
    }
}

#[derive(Debug, Clone)]
enum ConsensusState {
    Joint,
    JointCommitted,
    Uniform,
}

pub(super) struct LeaderState<'a, D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    node: &'a mut RaftNode<D, R, TR, LS, SM>,
    nodes: HashMap<NodeId, ReplicationState<D>>,
    consensus_state: ConsensusState,
    awaiting_commit: Vec<(u64, oneshot::Sender<Result<R, Error>>)>,
    awaiting_config_change: Option<oneshot::Sender<Result<(), Error>>>,
    awaiting_consensus_change: FuturesOrdered<oneshot::Receiver<Result<R, Error>>>,
    tx_replica: mpsc::UnboundedSender<ReplicationEvent>,
    rx_replica: mpsc::UnboundedReceiver<ReplicationEvent>,
}

impl<'a, D, R, TR, LS, SM> LeaderState<'a, D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    pub(super) fn new(node: &'a mut RaftNode<D, R, TR, LS, SM>) -> Self {
        let (tx_replica, rx_replica) = mpsc::unbounded_channel();
        Self {
            node,
            nodes: HashMap::new(),
            consensus_state: ConsensusState::Uniform,
            awaiting_commit: Vec::default(),
            awaiting_config_change: None,
            awaiting_consensus_change: FuturesOrdered::default(),
            tx_replica,
            rx_replica,
        }
    }

    pub(super) async fn run(mut self) -> Result<(), Error> {
        slog::info!(self.node.logger, "Enter leader state"; slog::o!("term" => self.node.current_term));
        self.node.next_election_timeout = None;
        self.node.last_heartbeat = None;
        self.node.current_leader = Some(self.node.id);
        self.commit_initial_leader_entry().await?;
        self.update_replication_start().await?;
        loop {
            if !matches!(self.node.target_state, State::Leader) {
                for (_, node) in self.nodes.into_iter() {
                    node.shutdown().await.unwrap();
                }
                return Ok(());
            }
            tokio::select! {
                Some(message) = self.node.rx.recv() => match message {
                    Message::AppendEntries{request, tx} => {
                        let _ = tx.send(self.node.handle_append_entries(request).await);
                    }
                    Message::RequestVote{request, tx} => {
                        let _ = tx.send(self.node.handle_request_vote(request).await);
                    }
                    Message::InstallSnapshot{request, tx} => {
                        let _ = tx.send(self.node.handle_install_snapshot(request).await);
                    }
                    Message::InitCluster{tx, ..} => {
                        let _ = tx.send(Err("node already initialized".into()));
                    }
                    Message::AddNode{id, tx} => {
                        self.handle_add_node(id, tx).await;
                    }
                    Message::RemoveNode{id, tx} => {
                        self.handle_remove_node(id, tx).await;
                    }
                    Message::WriteEntry{entry, tx} => {
                        self.handle_write_entry(entry, tx).await;
                    }
                },
                Some(result) = self.awaiting_consensus_change.next() => match result {
                    Ok(_) => self.handle_consensus_change().await,
                    Err(err) => {
                        if let Some(tx) = self.awaiting_config_change.take() {
                            let _ = tx.send(Err(err.into()));
                        }
                    }
                },
                Some(event) = self.rx_replica.recv() => match event {
                    ReplicationEvent::UpdateMatchIndex { node_id, log_id } => {
                        self.handle_update_match_index(node_id, log_id).await?;
                    }
                    ReplicationEvent::RevertToFollower { node_id, term } => {
                        self.revert_to_follower(node_id, term).await?;
                    }
                },
                Ok(_) = &mut self.node.rx_shutdown => {
                    self.node.target_state = State::Shutdown;
                }
            }
        }
    }

    async fn handle_add_node(&mut self, id: NodeId, tx: oneshot::Sender<Result<(), Error>>) {
        let mut members = self.node.membership.members.clone();
        if !members.insert(id) {
            let _ = tx.send(Err(format!("cluster already has node {}", id).into()));
            return;
        }
        self.handle_change_membership(members, tx).await;
    }

    async fn handle_remove_node(&mut self, id: NodeId, tx: oneshot::Sender<Result<(), Error>>) {
        let mut members = self.node.membership.members.clone();
        if !members.remove(&id) {
            let _ = tx.send(Err(format!("cluster does not have node {}", id).into()));
            return;
        }
        self.handle_change_membership(members, tx).await;
    }

    async fn handle_change_membership(
        &mut self,
        members: HashSet<NodeId>,
        tx: oneshot::Sender<Result<(), Error>>,
    ) {
        if !matches!(self.consensus_state, ConsensusState::Uniform) {
            let _ = tx.send(Err("cluster is not in uniform consensus".into()));
            return;
        }
        self.awaiting_config_change = Some(tx);
        let (tx, rx) = oneshot::channel();
        self.awaiting_consensus_change.push_back(rx);
        let mut membership = self.node.membership.clone();
        assert!(membership.members_after_consensus.is_none());
        membership.members_after_consensus = Some(members);
        self.handle_write_entry(
            EntryPayload::ConfigChange(ConfigChangeEntry { membership }),
            tx,
        )
        .await;
        self.consensus_state = ConsensusState::Joint;
        slog::debug!(self.node.logger, "Change consensus state"; slog::o!("state" => "joint"));
    }

    async fn handle_consensus_change(&mut self) {
        match self.consensus_state {
            ConsensusState::Joint => {
                let mut membership = self.node.membership.clone();
                match membership.members_after_consensus.take() {
                    Some(members) => {
                        membership.members = members;
                    }
                    None => unreachable!(),
                };
                let (tx, rx) = oneshot::channel();
                self.awaiting_consensus_change.push_back(rx);
                self.handle_write_entry(
                    EntryPayload::ConfigChange(ConfigChangeEntry { membership }),
                    tx,
                )
                .await;
                self.consensus_state = ConsensusState::JointCommitted;
                slog::debug!(self.node.logger, "Change consensus state"; slog::o!("state" => "joint committed"));
            }
            ConsensusState::JointCommitted => {
                assert!(self.node.membership.members_after_consensus.is_none());
                self.consensus_state = ConsensusState::Uniform;
                if let Some(tx) = self.awaiting_config_change.take() {
                    let _ = tx.send(Ok(()));
                }
                slog::debug!(self.node.logger, "Change consensus state"; slog::o!("state" => "uniform"));
            }
            ConsensusState::Uniform => unreachable!(),
        }
    }

    async fn handle_write_entry(
        &mut self,
        entry: EntryPayload<D>,
        tx: oneshot::Sender<Result<R, Error>>,
    ) {
        let entry = match self.node.append_entry(entry).await {
            Ok(entry) => entry,
            Err(err) => {
                let _ = tx.send(Err(err.into()));
                return;
            }
        };
        if self.nodes.is_empty() {
            let entries = self
                .node
                .log_storage
                .read_entries(
                    self.node.last_applied_log_id.index + 1,
                    entry.log_id.index + 1,
                )
                .await
                .unwrap();
            let result = self
                .node
                .state_machine
                .apply_entries(entries)
                .await
                .unwrap();
            self.node.last_applied_log_id =
                self.node.state_machine.get_applied_log_id().await.unwrap();
            self.node.membership = self
                .node
                .state_machine
                .get_membership_config()
                .await
                .unwrap();
            self.update_replication_start().await.unwrap();
            self.update_replication_finish().await.unwrap();
            let _ = tx.send(Ok(result.into_iter().next().unwrap()));
            return;
        }
        self.awaiting_commit.push((entry.log_id.index, tx));
        let entry = Arc::new(entry);
        for node in self.nodes.values() {
            let _ = node.tx.send(ReplicationMessage::Replicate {
                entry: entry.clone(),
                commit_index: self.node.last_applied_log_id.index,
            });
        }
    }

    async fn commit_initial_leader_entry(&mut self) -> Result<(), Error> {
        let mut payload = EntryPayload::Blank;
        if self.node.last_log_id.index == 0 {
            payload = EntryPayload::ConfigChange(ConfigChangeEntry {
                membership: self.node.membership.clone(),
            });
        }
        self.node.append_entry(payload).await?;
        Ok(())
    }

    async fn revert_to_follower(&mut self, _: NodeId, term: u64) -> Result<(), Error> {
        if term < self.node.current_term {
            Err("cannot revert to follower with lower term")?
        }
        slog::info!(self.node.logger, "Switch to follower state");
        self.node.target_state = State::Follower;
        self.node.current_term = term;
        self.node.voted_for = None;
        self.node.save_hard_state().await?;
        Ok(())
    }

    async fn handle_update_match_index(
        &mut self,
        node_id: NodeId,
        log_id: LogId,
    ) -> Result<(), Error> {
        {
            let node = match self.nodes.get_mut(&node_id) {
                Some(node) => node,
                None => return Ok(()),
            };
            node.match_log_id = log_id;
        }
        let mut all_indexes: Vec<_> = self
            .nodes
            .iter()
            .map(|node| node.1.match_log_id.index)
            .chain(once(self.node.last_applied_log_id.index))
            .collect();
        let (_, match_index, _) = all_indexes.select_nth_unstable((self.nodes.len() + 1) / 2);
        let match_index = *match_index;
        if match_index <= self.node.last_applied_log_id.index {
            return Ok(());
        }
        self.commit_entries(match_index).await
    }

    async fn commit_entries(&mut self, commit_index: u64) -> Result<(), Error> {
        if self.node.last_applied_log_id.index >= commit_index {
            return Ok(());
        }
        let entries = self
            .node
            .log_storage
            .read_entries(self.node.last_applied_log_id.index + 1, commit_index + 1)
            .await?;
        let update_membership = has_config_change(&entries);
        let indexes: Vec<_> = entries.iter().map(|entry| entry.log_id.index).collect();
        let results = self.node.state_machine.apply_entries(entries).await?;
        assert!(results.len() == indexes.len());
        self.node.last_applied_log_id = self.node.state_machine.get_applied_log_id().await?;
        if update_membership {
            self.node.membership = self.node.state_machine.get_membership_config().await?;
            self.update_replication_start().await?;
        }
        for node in self.nodes.values() {
            let _ = node.tx.send(ReplicationMessage::Commit {
                commit_index: self.node.last_applied_log_id.index,
            });
        }
        if update_membership {
            self.update_replication_finish().await?;
        }
        let index = match indexes.last() {
            Some(index) => index,
            None => unreachable!(),
        };
        let uncommited_pos = self.awaiting_commit.partition_point(|(id, _)| id <= &index);
        let mut indexes = indexes.into_iter();
        let mut results = results.into_iter();
        let mut curr_index = indexes.next();
        let mut curr_result = results.next();
        for (id, tx) in self.awaiting_commit.drain(..uncommited_pos) {
            while let Some(index) = curr_index {
                if index >= id {
                    break;
                }
                curr_index = indexes.next();
                curr_result = results.next();
            }
            if let Some(index) = curr_index {
                if index == id {
                    let _ = tx.send(Ok(curr_result.take().unwrap()));
                    curr_index = indexes.next();
                    curr_result = results.next();
                } else {
                    unreachable!();
                }
            } else {
                unreachable!();
            }
        }
        Ok(())
    }

    async fn update_replication_start(&mut self) -> Result<(), Error> {
        let all_members = self.node.membership.all_members();
        if !all_members.contains(&self.node.id) {
            self.node.target_state = State::NonVoter;
        }
        for id in all_members {
            if id != self.node.id && !self.nodes.contains_key(&id) {
                self.start_replication(id).await?;
            }
        }
        Ok(())
    }

    async fn update_replication_finish(&mut self) -> Result<(), Error> {
        let all_members = self.node.membership.all_members();
        let remove_ids: Vec<_> = self
            .nodes
            .iter()
            .filter(|(id, _)| !all_members.contains(id))
            .map(|(id, _)| *id)
            .collect();
        for id in remove_ids {
            self.stop_replication(id).await?;
        }
        Ok(())
    }

    async fn start_replication(&mut self, id: u64) -> Result<(), Error> {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = Replication::spawn(
            self.node.id,
            id,
            &self.node.config,
            self.node.logger.new(slog::o!("target_id" => id)),
            self.node.transport.clone(),
            self.node.log_storage.clone(),
            self.tx_replica.clone(),
            rx,
            self.node.current_term,
            self.node.last_log_id.index,
            self.node.last_applied_log_id.index,
        );
        slog::info!(self.node.logger, "Start replication"; slog::o!("target_id" => id));
        if let Some(_) = self.nodes.insert(
            id,
            ReplicationState {
                tx,
                handle,
                match_log_id: LogId::default(),
                shutdown_index: None,
            },
        ) {
            unreachable!();
        }
        Ok(())
    }

    async fn stop_replication(&mut self, id: u64) -> Result<(), Error> {
        match self.nodes.remove(&id) {
            Some(node) => {
                slog::info!(self.node.logger, "Stop replication"; slog::o!("target_id" => id));
                node.shutdown().await
            }
            None => unreachable!(),
        }
    }
}
