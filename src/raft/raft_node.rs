use std::cmp::min;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Sleep};

use super::{
    AppendEntriesRequest, AppendEntriesResponse, CandidateState, Config, Data, Entry, EntryPayload,
    Error, HardState, InstallSnapshotRequest, InstallSnapshotResponse, LeaderState, LogId,
    LogStorage, MembershipConfig, Message, NodeId, RequestVoteRequest, RequestVoteResponse,
    Response, StateMachine, Transport,
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
    pub(super) id: NodeId,
    pub(super) config: Config,
    pub(super) logger: slog::Logger,
    pub(super) transport: Arc<TR>,
    pub(super) log_storage: Arc<LS>,
    pub(super) state_machine: Arc<SM>,
    pub(super) rx: mpsc::UnboundedReceiver<Message<D, R>>,
    pub(super) rx_shutdown: oneshot::Receiver<()>,
    pub(super) last_log_id: LogId,
    pub(super) current_term: u64,
    pub(super) voted_for: Option<u64>,
    pub(super) membership: MembershipConfig,
    pub(super) last_applied_log_id: LogId,
    pub(super) target_state: State,
    next_election_timeout: Option<Instant>,
    pub(super) last_heartbeat: Option<Instant>,
    pub(super) current_leader: Option<NodeId>,
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
            self.log_storage
                .append_entries(vec![Entry {
                    log_id: self.last_applied_log_id,
                    payload: EntryPayload::Blank,
                }])
                .await?;
            // Remove gap in log storage.
            self.log_storage
                .purge(self.last_applied_log_id.index + 1)
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
                State::Leader => self.run_leader().await?,
                State::Candidate => CandidateState::new(&mut self).run().await?,
                State::Follower => self.run_follower().await?,
                State::NonVoter => self.run_non_voter().await?,
                State::Shutdown => return Ok(()),
            }
        }
    }

    async fn run_leader(&mut self) -> Result<(), Error> {
        self.next_election_timeout = None;
        LeaderState::new(self).run().await
    }

    async fn run_follower(&mut self) -> Result<(), Error> {
        slog::info!(self.logger, "Enter follower state"; slog::o!("term" => self.current_term));
        let now = Instant::now();
        self.update_election_timeout(now);
        loop {
            if !matches!(self.target_state, State::Follower) {
                return Ok(());
            }
            let election_timeout = self.get_election_timeout();
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
                    Message::RemoveNode{tx, ..} => {
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
        slog::info!(self.logger, "Enter non voter state"; slog::o!("term" => self.current_term));
        self.next_election_timeout = None;
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
                    Message::RemoveNode{tx, ..} => {
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

    pub(super) async fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest<D>,
    ) -> Result<AppendEntriesResponse, Error> {
        if request.term < self.current_term {
            slog::debug!(
                self.logger,
                "Rejected append entries request with old term";
                slog::o!("request_term" => request.term)
            );
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_opt: None,
            });
        }
        if matches!(self.target_state, State::Leader | State::Candidate) {
            slog::info!(self.logger, "Switch to follower state");
            self.target_state = State::Follower;
        }
        let now = Instant::now();
        self.last_heartbeat = Some(now);
        if !matches!(self.target_state, State::NonVoter) {
            self.update_election_timeout(now);
        }
        if Some(request.leader_id) != self.current_leader {
            self.current_leader = Some(request.leader_id);
        }
        if request.term != self.current_term {
            self.update_hard_state(request.term, None).await?;
        }
        if request.prev_log_id != self.last_log_id {
            let entries = self
                .log_storage
                .read_entries(request.prev_log_id.index, request.prev_log_id.index + 1)
                .await?;
            let entry = match entries.first() {
                Some(entry) => entry,
                None => {
                    return Ok(AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        conflict_opt: Some(self.last_log_id),
                    });
                }
            };
            if entry.log_id.term != request.prev_log_id.term {
                return Ok(AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    conflict_opt: Some(self.last_log_id),
                });
            }
            assert!(self.last_applied_log_id.index <= request.prev_log_id.index);
            self.log_storage
                .truncate(request.prev_log_id.index + 1)
                .await?;
        }
        self.log_storage.append_entries(request.entries).await?;
        self.last_log_id = self.log_storage.get_log_state().await?.last_log_id;
        let leader_commit = min(request.leader_commit, self.last_log_id.index);
        assert!(self.last_applied_log_id.index <= leader_commit);
        if self.last_applied_log_id.index < leader_commit {
            let entries = self
                .log_storage
                .read_entries(self.last_applied_log_id.index + 1, leader_commit + 1)
                .await?;
            let update_membership = has_config_change(&entries);
            self.state_machine.apply_entries(entries).await?;
            self.last_applied_log_id = self.state_machine.get_applied_log_id().await?;
            assert!(self.last_applied_log_id.index == leader_commit);
            if update_membership {
                self.update_membership().await?;
            }
        }
        Ok(AppendEntriesResponse {
            term: self.current_term,
            success: true,
            conflict_opt: None,
        })
    }

    pub(super) async fn handle_request_vote(
        &mut self,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Error> {
        if request.term < self.current_term {
            return Ok(RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            });
        }
        let now = Instant::now();
        if request.term != self.current_term {
            if matches!(self.target_state, State::Leader | State::Candidate) {
                slog::info!(self.logger, "Switch to follower state");
                self.target_state = State::Follower;
            }
            self.update_election_timeout(now);
            self.update_hard_state(request.term, None).await?;
        }
        if request.last_log_id.term < self.last_log_id.term
            || request.last_log_id.index < self.last_log_id.index
        {
            return Ok(RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            });
        }
        match &self.voted_for {
            Some(candidate_id) => Ok(RequestVoteResponse {
                term: self.current_term,
                vote_granted: candidate_id == &request.candidate_id,
            }),
            None => {
                if matches!(self.target_state, State::Leader | State::Candidate) {
                    slog::info!(self.logger, "Switch to follower state");
                    self.target_state = State::Follower;
                }
                self.update_election_timeout(now);
                self.update_hard_state(request.term, Some(request.candidate_id))
                    .await?;
                Ok(RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: true,
                })
            }
        }
    }

    pub(super) async fn handle_install_snapshot(
        &mut self,
        _request: InstallSnapshotRequest,
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
        self.update_hard_state(1, Some(self.id)).await?;
        self.target_state = State::Leader;
        Ok(())
    }

    pub(super) fn update_election_timeout(&mut self, now: Instant) {
        assert!(!matches!(
            self.target_state,
            State::Leader | State::NonVoter
        ));
        self.next_election_timeout = Some(now + self.config.new_rand_election_timeout());
    }

    pub(super) fn get_election_timeout(&mut self) -> Sleep {
        sleep_until(self.next_election_timeout.unwrap().into())
    }

    pub(super) async fn update_membership(&mut self) -> Result<(), Error> {
        self.membership = self.state_machine.get_membership_config().await?;
        if !self.membership.contains(&self.id) {
            // Switch to non voter if current node is not in cluster membership.
            self.target_state = State::NonVoter;
        } else if matches!(self.target_state, State::NonVoter) {
            // Switch to follower if current node is in cluster membership.
            self.target_state = State::Follower;
        }
        Ok(())
    }

    pub(super) async fn update_hard_state(
        &mut self,
        current_term: u64,
        voted_for: Option<NodeId>,
    ) -> Result<(), Error> {
        // Current term cannot be decreased.
        assert!(current_term >= self.current_term);
        // We cannot change vote in current term.
        assert!(
            current_term != self.current_term
                || self.voted_for.is_none()
                || self.voted_for == voted_for
        );
        let hard_state = HardState {
            current_term,
            voted_for,
        };
        self.state_machine.save_hard_state(hard_state).await?;
        self.current_term = current_term;
        self.voted_for = voted_for;
        Ok(())
    }

    pub(super) async fn append_entry_payload(
        &mut self,
        payload: EntryPayload<D>,
    ) -> Result<Entry<D>, Error> {
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

pub(super) fn has_config_change<D: Data>(entries: &Vec<Entry<D>>) -> bool {
    entries.iter().any(|entry| match entry.payload {
        EntryPayload::ConfigChange(_) => true,
        _ => false,
    })
}
