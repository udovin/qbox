use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep_until;

use super::raft_replication::{ReplicationEvent, ReplicationMessage};
use super::{
    AppendEntriesRequest, AppendEntriesResponse, Config, ConfigChangeEntry, Data, Entry,
    EntryPayload, Error, HardState, InstallSnapshotRequest, InstallSnapshotResponse, LogId,
    LogStorage, MembershipConfig, Message, NodeId, RequestVoteRequest,
    RequestVoteResponse, Response, StateMachine, Transport, RaftReplication,
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
        transport: Arc<TR>,
        log_storage: Arc<LS>,
        state_machine: Arc<SM>,
        rx: mpsc::UnboundedReceiver<Message<D, R>>,
        rx_shutdown: oneshot::Receiver<()>,
    ) -> JoinHandle<Result<(), Error>> {
        let this = Self {
            id,
            config,
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
            match self.log_storage
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
                    self.log_storage.truncate(request.prev_log_id.index + 1).await?;
                }
                None => return Ok(AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                }),
            };
        }
        self.log_storage.append_entries(request.entries).await?;
        self.last_log_id = self.log_storage.get_log_state().await?.last_log_id;
        let leader_commit = min(request.leader_commit, self.last_log_id.index);
        if self.last_applied_log_id.index < leader_commit {
            let entries = self.log_storage
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
}

struct RaftLeader<'a, D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>
{
    node: &'a mut RaftNode<D, R, TR, LS, SM>,
    nodes: HashMap<NodeId, ReplicationNode<D>>,
    awaiting_data: Vec<(u64, oneshot::Sender<Result<R, Error>>)>,
    awaiting_config_change: Option<oneshot::Sender<Result<(), Error>>>,
    awaiting_joint: FuturesOrdered<oneshot::Receiver<Result<R, Error>>>,
    awaiting_uniform: FuturesOrdered<oneshot::Receiver<Result<R, Error>>>,
}

impl<'a, D, R, TR, LS, SM> RaftLeader<'a, D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>
{
    pub(super) fn new(node: &'a mut RaftNode<D, R, TR, LS, SM>) -> Self {
        Self {
            node,
            nodes: HashMap::new(),
            awaiting_data: Vec::default(),
            awaiting_config_change: None,
            awaiting_joint: FuturesOrdered::default(),
            awaiting_uniform: FuturesOrdered::default(),
        }
    }

    pub(super) async fn run(mut self) -> Result<(), Error> {
        self.node.next_election_timeout = None;
        self.node.last_heartbeat = None;
        self.node.current_leader = Some(self.node.id);
        self.commit_initial_leader_entry().await?;
        let (tx_replica, mut rx_replica) = mpsc::unbounded_channel();
        for target_id in self.node.membership.all_members() {
            if target_id == self.node.id {
                continue;
            }
            let (tx, rx) = mpsc::unbounded_channel();
            let handle = RaftReplication::spawn(
                self.node.id,
                target_id,
                &self.node.config,
                self.node.transport.clone(),
                self.node.log_storage.clone(),
                tx_replica.clone(),
                rx,
                self.node.current_term,
                self.node.last_log_id.index,
                self.node.last_applied_log_id.index,
            );
            self.nodes.insert(target_id, ReplicationNode { tx, handle });
        }
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
                Some(result) = self.awaiting_joint.next() => match result {
                    Ok(_) => todo!(),
                    Err(err) => {
                        if let Some(tx) = self.awaiting_config_change.take() {
                            let _ = tx.send(Err(err.into()));
                        }
                    }
                },
                Some(result) = self.awaiting_uniform.next() => match result {
                    Ok(_) => todo!(),
                    Err(err) => {
                        if let Some(tx) = self.awaiting_config_change.take() {
                            let _ = tx.send(Err(err.into()));
                        }
                    }
                },
                Some(event) = rx_replica.recv() => match event {
                    ReplicationEvent::UpdateMatchIndex { node_id, log_id } => {

                    }
                    ReplicationEvent::RevertToFollower { node_id, term } => {
                        self.revert_to_follower(node_id, term).await?
                    }
                },
                Ok(_) = &mut self.node.rx_shutdown => {
                    self.node.target_state = State::Shutdown;
                }
            }
        }
    }

    async fn handle_add_node(&mut self, id: NodeId, tx: oneshot::Sender<Result<(), Error>>) {
        if self.awaiting_config_change.is_some() {
            let _ = tx.send(Err("cluster in config change state".into()));
            return;
        }
        self.awaiting_config_change = Some(tx);
        let (awaiting_joint_tx, awaiting_joint_rx) = oneshot::channel();
        self.awaiting_joint.push_back(awaiting_joint_rx);
        let mut membership = self.node.membership.clone();
        assert!(membership.members_after_consensus.is_none());
        let mut members_after_consensus = membership.members.clone();
        members_after_consensus.insert(id);
        membership.members_after_consensus = Some(members_after_consensus);
        self.handle_write_entry(EntryPayload::ConfigChange(ConfigChangeEntry { membership }), awaiting_joint_tx).await;
    }

    async fn handle_write_entry(&mut self, entry: EntryPayload<D>, tx: oneshot::Sender<Result<R, Error>>) {
        let entry = match self.node.append_entry(entry).await {
            Ok(entry) => entry,
            Err(err) => {
                let _ = tx.send(Err(err.into()));
                return;
            }
        };
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
}
