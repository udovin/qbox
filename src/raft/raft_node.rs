use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::iter::once;
use std::sync::Arc;
use std::time::Instant;

use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep_until;

use super::replication::{ReplicationEvent, ReplicationMessage};
use super::{
    AppendEntriesRequest, AppendEntriesResponse, Config, ConfigChangeEntry, Data, Entry,
    EntryPayload, Error, HardState, InstallSnapshotRequest, InstallSnapshotResponse, LogId,
    LogStorage, MembershipConfig, Message, NodeId, Replication, RequestVoteRequest,
    RequestVoteResponse, Response, StateMachine, Transport,
};

pub(super) enum State {
    NonVoter,
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

pub(super) struct RaftNode<D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    id: NodeId,
    config: Config,
    logger: slog::Logger,
    transport: Arc<TR>,
    log_storage: Arc<LS>,
    state_machine: Arc<SM>,
    rx: mpsc::UnboundedReceiver<Message<D, R>>,
    rx_shutdown: oneshot::Receiver<()>,
    last_log_id: LogId,
    current_term: u64,
    voted_for: Option<u64>,
    membership: MembershipConfig,
    last_applied_log_id: LogId,
    target_state: State,
    next_election_timeout: Option<Instant>,
    last_heartbeat: Option<Instant>,
    current_leader: Option<NodeId>,
}

impl<D, R, TR, LS, SM> RaftNode<D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    pub(super) fn spawn(
        id: NodeId,
        config: Config,
        logger: slog::Logger,
        transport: Arc<TR>,
        log_storage: Arc<LS>,
        state_machine: Arc<SM>,
        rx: mpsc::UnboundedReceiver<Message<D, R>>,
        rx_shutdown: oneshot::Receiver<()>,
    ) -> JoinHandle<Result<(), Error>> {
        let this = Self {
            id,
            config,
            logger,
            transport,
            log_storage,
            state_machine,
            rx,
            rx_shutdown,
            last_log_id: LogId::default(),
            current_term: 0,
            voted_for: None,
            membership: MembershipConfig::default(),
            last_applied_log_id: LogId::default(),
            target_state: State::NonVoter,
            next_election_timeout: None,
            last_heartbeat: None,
            current_leader: None,
        };
        tokio::spawn(this.run())
    }

    async fn run(mut self) -> Result<(), Error> {
        let log_state = self.log_storage.get_log_state().await?;
        self.last_log_id = log_state.last_log_id;
        let hard_state = self.state_machine.get_hard_state().await?;
        self.current_term = hard_state.current_term;
        self.voted_for = hard_state.voted_for;
        self.membership = self.state_machine.get_membership_config().await?;
        self.last_applied_log_id = self.state_machine.get_applied_log_id().await?;
        if self.last_log_id.index < self.last_applied_log_id.index {
            // Remove gap in log storage.
            self.log_storage
                .purge(self.last_applied_log_id.index)
                .await?;
            self.last_log_id = self.last_applied_log_id;
        }
        let is_only_configured_member =
            self.membership.members.len() == 1 && self.membership.members.contains(&self.id);
        if is_only_configured_member {
            self.target_state = State::Leader;
        } else if self.membership.members.contains(&self.id) {
            self.target_state = State::Follower;
        } else {
            self.target_state = State::NonVoter;
        }
        loop {
            match &self.target_state {
                State::Leader => RaftLeader::new(&mut self).run().await?,
                State::Candidate => self.run_candidate().await?,
                State::Follower => self.run_follower().await?,
                State::NonVoter => self.run_non_voter().await?,
                State::Shutdown => return Ok(()),
            }
        }
    }

    async fn run_candidate(&mut self) -> Result<(), Error> {
        let now = Instant::now();
        self.next_election_timeout = Some(now + self.config.new_rand_election_timeout());
        loop {
            if !matches!(self.target_state, State::Candidate) {
                return Ok(());
            }
            let election_timeout = sleep_until(self.next_election_timeout.unwrap().into());
            tokio::select! {
                _ = election_timeout => return Ok(()),
                Some(message) = self.rx.recv() => match message {
                    Message::AppendEntries{request, tx} => {
                        let _ = tx.send(self.handle_append_entries(request).await);
                    }
                    Message::RequestVote{request, tx} => {
                        let _ = tx.send(self.handle_request_vote(request).await);
                    }
                    Message::InstallSnapshot{request, tx} => {
                        let _ = tx.send(self.handle_install_snapshot(request).await);
                    }
                    Message::InitCluster{tx, ..} => {
                        let _ = tx.send(Err("node already initialized".into()));
                    }
                    Message::AddNode{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                    Message::WriteEntry{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                },
                Ok(_) = &mut self.rx_shutdown => {
                    self.target_state = State::Shutdown;
                }
            }
        }
    }

    async fn run_follower(&mut self) -> Result<(), Error> {
        let now = Instant::now();
        self.next_election_timeout = Some(now + self.config.new_rand_election_timeout());
        loop {
            if !matches!(self.target_state, State::Follower) {
                return Ok(());
            }
            let election_timeout = sleep_until(self.next_election_timeout.unwrap().into());
            tokio::select! {
                _ = election_timeout => self.target_state = State::Candidate,
                Some(message) = self.rx.recv() => match message {
                    Message::AppendEntries{request, tx} => {
                        let _ = tx.send(self.handle_append_entries(request).await);
                    }
                    Message::RequestVote{request, tx} => {
                        let _ = tx.send(self.handle_request_vote(request).await);
                    }
                    Message::InstallSnapshot{request, tx} => {
                        let _ = tx.send(self.handle_install_snapshot(request).await);
                    }
                    Message::InitCluster{tx, ..} => {
                        let _ = tx.send(Err("node already initialized".into()));
                    }
                    Message::AddNode{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                    Message::WriteEntry{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                },
                Ok(_) = &mut self.rx_shutdown => {
                    self.target_state = State::Shutdown;
                }
            }
        }
    }

    async fn run_non_voter(&mut self) -> Result<(), Error> {
        loop {
            if !matches!(self.target_state, State::NonVoter) {
                return Ok(());
            }
            tokio::select! {
                Some(message) = self.rx.recv() => match message {
                    Message::AppendEntries{request, tx} => {
                        let _ = tx.send(self.handle_append_entries(request).await);
                    }
                    Message::RequestVote{request, tx} => {
                        let _ = tx.send(self.handle_request_vote(request).await);
                    }
                    Message::InstallSnapshot{request, tx} => {
                        let _ = tx.send(self.handle_install_snapshot(request).await);
                    }
                    Message::InitCluster{tx} => {
                        let _ = tx.send(self.handle_init_cluster().await);
                    }
                    Message::AddNode{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                    Message::WriteEntry{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                },
                Ok(_) = &mut self.rx_shutdown => {
                    self.target_state = State::Shutdown;
                }
            }
        }
    }

    async fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest<D>,
    ) -> Result<AppendEntriesResponse, Error> {
        if request.term < self.current_term {
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: false,
            });
        }
        let now = Instant::now();
        self.next_election_timeout = Some(now + self.config.new_rand_election_timeout());
        self.last_heartbeat = Some(now);
        if request.term != self.current_term {
            self.current_term = request.term;
            self.voted_for = None;
            self.save_hard_state().await?;
        }
        if Some(request.leader_id) != self.current_leader {
            self.current_leader = Some(request.leader_id);
        }
        if matches!(self.target_state, State::Leader | State::Candidate) {
            self.target_state = State::Follower;
        }
        if request.prev_log_id.index > self.last_log_id.index {
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: false,
            });
        }
        if request.prev_log_id != self.last_log_id {
            match self
                .log_storage
                .read_entries(request.prev_log_id.index, request.prev_log_id.index + 1)
                .await?
                .first()
            {
                Some(prev_entry) => {
                    if prev_entry.log_id != request.prev_log_id {
                        return Ok(AppendEntriesResponse {
                            term: self.current_term,
                            success: false,
                        });
                    }
                    self.log_storage
                        .truncate(request.prev_log_id.index + 1)
                        .await?;
                }
                None => {
                    return Ok(AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                    })
                }
            };
        }
        self.log_storage.append_entries(request.entries).await?;
        self.last_log_id = self.log_storage.get_log_state().await?.last_log_id;
        let leader_commit = min(request.leader_commit, self.last_log_id.index);
        if self.last_applied_log_id.index < leader_commit {
            let entries = self
                .log_storage
                .read_entries(self.last_applied_log_id.index + 1, leader_commit + 1)
                .await?;
            self.state_machine.apply_entries(entries).await?;
            self.last_applied_log_id = self.state_machine.get_applied_log_id().await?;
            assert!(self.last_applied_log_id.index == leader_commit);
            self.membership = self.state_machine.get_membership_config().await?;
        }
        Ok(AppendEntriesResponse {
            term: self.current_term,
            success: true,
        })
    }

    async fn handle_request_vote(
        &mut self,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Error> {
        todo!()
    }

    async fn handle_install_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Error> {
        todo!()
    }

    async fn handle_init_cluster(&mut self) -> Result<(), Error> {
        if self.last_log_id.index != 0 {
            Err("not allowed")?
        }
        let mut members = HashSet::new();
        members.insert(self.id);
        self.membership = MembershipConfig {
            members,
            members_after_consensus: None,
        };
        self.current_term = 1;
        self.voted_for = Some(self.id);
        self.target_state = State::Leader;
        self.save_hard_state().await?;
        Ok(())
    }

    async fn save_hard_state(&mut self) -> Result<(), Error> {
        let hs = HardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
        };
        self.state_machine.save_hard_state(hs).await
    }

    async fn append_entry(&mut self, payload: EntryPayload<D>) -> Result<Entry<D>, Error> {
        let log_id = LogId {
            index: self.last_log_id.index + 1,
            term: self.current_term,
        };
        let entry = Entry { log_id, payload };
        self.log_storage.append_entries(vec![entry.clone()]).await?;
        self.last_log_id = log_id;
        Ok(entry)
    }
}

struct ReplicationNode<D: Data> {
    tx: mpsc::UnboundedSender<ReplicationMessage<D>>,
    handle: JoinHandle<Result<(), Error>>,
    match_log_id: LogId,
}

#[derive(Debug, Clone)]
enum ConsensusState {
    Joint,
    JointCommitted,
    Uniform,
}

struct RaftLeader<'a, D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    node: &'a mut RaftNode<D, R, TR, LS, SM>,
    nodes: HashMap<NodeId, ReplicationNode<D>>,
    consensus_state: ConsensusState,
    awaiting_data: Vec<(u64, oneshot::Sender<Result<R, Error>>)>,
    awaiting_config_change: Option<oneshot::Sender<Result<(), Error>>>,
    awaiting_consensus_change: FuturesOrdered<oneshot::Receiver<Result<R, Error>>>,
    tx_replica: mpsc::UnboundedSender<ReplicationEvent>,
    rx_replica: mpsc::UnboundedReceiver<ReplicationEvent>,
}

impl<'a, D, R, TR, LS, SM> RaftLeader<'a, D, R, TR, LS, SM>
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
            awaiting_data: Vec::default(),
            awaiting_config_change: None,
            awaiting_consensus_change: FuturesOrdered::default(),
            tx_replica,
            rx_replica,
        }
    }

    pub(super) async fn run(mut self) -> Result<(), Error> {
        self.node.next_election_timeout = None;
        self.node.last_heartbeat = None;
        self.node.current_leader = Some(self.node.id);
        self.commit_initial_leader_entry().await?;
        self.update_replication().await?;
        loop {
            if !matches!(self.node.target_state, State::Leader) {
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

    async fn handle_change_membership(
        &mut self,
        members: HashSet<NodeId>,
        tx: oneshot::Sender<Result<(), Error>>,
    ) {
        if !matches!(self.consensus_state, ConsensusState::Uniform) {
            let _ = tx.send(Err("cluster is not in uniform consensus".into()));
            return;
        }
        dbg!(self.consensus_state.clone());
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
    }

    async fn handle_consensus_change(&mut self) {
        dbg!(self.consensus_state.clone());
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
            }
            ConsensusState::JointCommitted => {
                assert!(self.node.membership.members_after_consensus.is_none());
                self.consensus_state = ConsensusState::Uniform;
                if let Some(tx) = self.awaiting_config_change.take() {
                    let _ = tx.send(Ok(()));
                }
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
            self.update_replication().await.unwrap();
            let _ = tx.send(Ok(result.into_iter().next().unwrap()));
            return;
        }
        self.awaiting_data.push((entry.log_id.index, tx));
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
        self.node.current_term = term;
        self.node.voted_for = None;
        self.node.save_hard_state().await?;
        if self.node.membership.all_members().contains(&self.node.id) {
            self.node.target_state = State::Follower;
        } else {
            self.node.target_state = State::NonVoter;
        }
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
                None => Err("cannot update match index")?,
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
        let entries = self
            .node
            .log_storage
            .read_entries(self.node.last_applied_log_id.index + 1, match_index + 1)
            .await?;
        let update_membership = entries.iter().any(|entry| match entry.payload {
            EntryPayload::ConfigChange(_) => true,
            _ => false,
        });
        self.node.state_machine.apply_entries(entries).await?;
        self.node.last_applied_log_id = self.node.state_machine.get_applied_log_id().await?;
        if update_membership {
            self.node.membership = self.node.state_machine.get_membership_config().await?;
            self.update_replication().await?;
        }
        for node in self.nodes.values() {
            let _ = node.tx.send(ReplicationMessage::Commit {
                commit_index: self.node.last_applied_log_id.index,
            });
        }
        Ok(())
    }

    async fn update_replication(&mut self) -> Result<(), Error> {
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
        for id in all_members {
            if id != self.node.id && !self.nodes.contains_key(&id) {
                self.start_replication(id).await?;
            }
        }
        Ok(())
    }

    async fn start_replication(&mut self, id: u64) -> Result<(), Error> {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = Replication::spawn(
            self.node.id,
            id,
            &self.node.config,
            self.node.logger.clone(),
            self.node.transport.clone(),
            self.node.log_storage.clone(),
            self.tx_replica.clone(),
            rx,
            self.node.current_term,
            self.node.last_log_id.index,
            self.node.last_applied_log_id.index,
        );
        if let Some(_) = self.nodes.insert(
            id,
            ReplicationNode {
                tx,
                handle,
                match_log_id: LogId::default(),
            },
        ) {
            unreachable!();
        }
        Ok(())
    }

    async fn stop_replication(&mut self, id: u64) -> Result<(), Error> {
        let node = match self.nodes.remove(&id) {
            Some(node) => node,
            None => unreachable!(),
        };
        node.tx.send(ReplicationMessage::Terminate)?;
        node.handle.await??;
        Ok(())
    }
}
