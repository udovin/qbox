use std::cmp::min;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep_until;

use super::{
    AppendEntriesRequest, AppendEntriesResponse, Config, ConfigChangeEntry, Data, Entry,
    EntryPayload, Error, HardState, InstallSnapshotRequest, InstallSnapshotResponse, LogId,
    LogStorage, MembershipConfig, Message, Node, NodeId, RequestVoteRequest,
    RequestVoteResponse, Response, StateMachine, Transport,
};

pub enum State {
    NonVoter,
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

pub struct RaftNode<N, D, R, TR, LS, SM>
where
    N: Node,
    D: Data,
    R: Response,
    TR: Transport<N, D>,
    LS: LogStorage<N, D>,
    SM: StateMachine<N, D, R>,
{
    id: NodeId,
    node: N,
    config: Config,
    transport: TR,
    log_storage: LS,
    state_machine: SM,
    rx: mpsc::UnboundedReceiver<Message<N, D, R>>,
    rx_shutdown: oneshot::Receiver<()>,
    last_log_id: LogId,
    committed_index: u64,
    current_term: u64,
    voted_for: Option<u64>,
    membership: MembershipConfig<N>,
    last_applied_log_id: LogId,
    target_state: State,
    next_election_timeout: Option<Instant>,
    last_heartbeat: Option<Instant>,
    current_leader: Option<NodeId>,
    _phantom: PhantomData<R>,
}

impl<N, D, R, TR, LS, SM> RaftNode<N, D, R, TR, LS, SM>
where
    N: Node,
    D: Data,
    R: Response,
    TR: Transport<N, D>,
    LS: LogStorage<N, D>,
    SM: StateMachine<N, D, R>,
{
    pub(super) fn spawn(
        id: NodeId,
        node: N,
        config: Config,
        transport: TR,
        log_storage: LS,
        state_machine: SM,
        rx: mpsc::UnboundedReceiver<Message<N, D, R>>,
        rx_shutdown: oneshot::Receiver<()>,
    ) -> JoinHandle<Result<(), Error>> {
        let this = Self {
            id,
            node,
            config,
            transport,
            log_storage,
            state_machine,
            rx,
            rx_shutdown,
            last_log_id: LogId::default(),
            committed_index: 0,
            current_term: 0,
            voted_for: None,
            membership: MembershipConfig::default(),
            last_applied_log_id: LogId::default(),
            target_state: State::NonVoter,
            next_election_timeout: None,
            last_heartbeat: None,
            current_leader: None,
            _phantom: PhantomData,
        };
        tokio::spawn(this.run())
    }

    async fn run(mut self) -> Result<(), Error> {
        let log_state = self.log_storage.get_log_state().await?;
        self.last_log_id = log_state.last_log_id;
        self.committed_index = self.log_storage.get_committed_index().await?;
        let hard_state = self.state_machine.get_hard_state().await?;
        self.current_term = hard_state.current_term;
        self.voted_for = hard_state.voted_for;
        self.membership = self.state_machine.get_membership_config().await?;
        self.last_applied_log_id = self.state_machine.get_applied_log_id().await?;
        if self.committed_index < self.last_applied_log_id.index {
            self.committed_index = self.last_applied_log_id.index;
        }
        if self.last_applied_log_id.index < self.committed_index {
            let entries = self
                .log_storage
                .read_entries(self.last_applied_log_id.index + 1, self.committed_index + 1)
                .await?;
            self.state_machine.apply_entries(entries).await?;
            self.last_applied_log_id = self.state_machine.get_applied_log_id().await?;
            assert!(self.last_applied_log_id.index == self.committed_index);
        }
        if self.last_log_id.index < self.last_applied_log_id.index {
            self.log_storage
                .purge(self.last_applied_log_id.index)
                .await?;
            self.last_log_id = self.last_applied_log_id;
        }
        let is_only_configured_member =
            self.membership.members.len() == 1 && self.membership.members.contains_key(&self.id);
        if is_only_configured_member {
            self.target_state = State::Leader;
        } else if self.membership.members.contains_key(&self.id) {
            self.target_state = State::Follower;
        } else {
            self.target_state = State::NonVoter;
        }
        loop {
            match &self.target_state {
                State::Leader => self.run_leader().await?,
                State::Candidate => self.run_candidate().await?,
                State::Follower => self.run_follower().await?,
                State::NonVoter => self.run_non_voter().await?,
                State::Shutdown => return Ok(()),
            }
        }
    }

    async fn append_entry(&mut self, payload: EntryPayload<N, D>) -> Result<(), Error> {
        let log_id = LogId {
            index: self.last_log_id.index + 1,
            term: self.current_term,
        };
        let entry = Entry { log_id, payload };
        self.log_storage.append_entries(vec![entry]).await?;
        self.last_log_id = log_id;
        Ok(())
    }

    async fn commit_initial_leader_entry(&mut self) -> Result<(), Error> {
        let mut payload = EntryPayload::Blank;
        if self.last_log_id.index == 0 {
            payload = EntryPayload::ConfigChange(ConfigChangeEntry {
                membership: self.membership.clone(),
            });
        }
        self.append_entry(payload).await?;
        println!("Appended: initial_leader_entry");
        return Ok(());
    }

    async fn run_leader(&mut self) -> Result<(), Error> {
        self.next_election_timeout = None;
        self.last_heartbeat = None;
        self.current_leader = Some(self.id);
        self.commit_initial_leader_entry().await?;
        loop {
            if !matches!(self.target_state, State::Leader) {
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
                    Message::InitCluster{tx, ..} => {
                        let _ = tx.send(Err("node already initialized".into()));
                    }
                    Message::AddNode{id, node, tx} => {
                        let _ = tx.send(self.handle_add_node(id, node).await);
                    }
                    Message::WriteData{data, tx} => {
                        let _ = tx.send(self.handle_write_data(data).await);
                    }
                },
                Ok(_) = &mut self.rx_shutdown => {
                    self.target_state = State::Shutdown;
                }
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
                    Message::WriteData{tx, ..} => {
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
                    Message::WriteData{tx, ..} => {
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
                    Message::WriteData{tx, ..} => {
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
        request: AppendEntriesRequest<N, D>,
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
        if leader_commit != self.committed_index {
            self.log_storage
                .save_committed_index(self.committed_index)
                .await?;
            self.committed_index = leader_commit;
        }
        if self.last_applied_log_id.index < self.committed_index {
            let entries = self.log_storage
                .read_entries(self.last_applied_log_id.index + 1, self.committed_index + 1)
                .await?;
            self.state_machine.apply_entries(entries).await?;
            self.last_applied_log_id = self.state_machine.get_applied_log_id().await?;
            assert!(self.last_applied_log_id.index == self.committed_index);
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
        let mut members = HashMap::new();
        members.insert(self.id, self.node.clone());
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

    async fn handle_add_node(&mut self, id: NodeId, node: N) -> Result<(), Error> {
        todo!()
    }

    async fn handle_write_data(&mut self, data: D) -> Result<R, Error> {
        todo!()
    }

    async fn save_hard_state(&mut self) -> Result<(), Error> {
        let hs = HardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
        };
        self.state_machine.save_hard_state(hs).await
    }
}