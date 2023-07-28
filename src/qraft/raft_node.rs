use std::marker::PhantomData;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::{Error, Transport, LogStorage, StateMachine, Message, Config, NodeId, MembershipConfig, Entry, Node, Data, Response, NormalEntry, EntryPayload};

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
    config: Config,
    transport: TR,
    log_storage: LS,
    state_machine: SM,
    rx: mpsc::UnboundedReceiver<Message<N, D>>,
    rx_shutdown: oneshot::Receiver<()>,
    last_log_index: u64,
    last_log_term: u64,
    commit_index: u64,
    current_term: u64,
    voted_for: Option<u64>,
    membership: MembershipConfig<N>,
    last_applied: u64,
    target_state: State,
    next_election_timeout: Option<Instant>,
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
    pub fn spawn(
        id: NodeId,
        config: Config,
        transport: TR,
        log_storage: LS,
        state_machine: SM,
        rx: mpsc::UnboundedReceiver<Message<N, D>>,
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
            last_log_index: 0,
            last_log_term: 0,
            commit_index: 0,
            current_term: 0,
            voted_for: None,
            membership: MembershipConfig::default(),
            last_applied: 0,
            target_state: State::NonVoter,
            next_election_timeout: None,
            _phantom: PhantomData,
        };
        tokio::spawn(this.run())
    }

    async fn run(mut self) -> Result<(), Error> {
        let log_state = self.log_storage.get_log_state().await?;
        self.last_log_index = log_state.last_index;
        self.last_log_term = log_state.last_term;
        self.commit_index = self.log_storage.get_commited_index().await?;
        let hard_state = self.state_machine.get_hard_state().await?;
        self.current_term = hard_state.current_term;
        self.voted_for = hard_state.voted_for;
        self.membership = self.state_machine.get_membership_config().await?;
        self.last_applied = self.state_machine.get_applied_index().await?;
        if self.commit_index < self.last_applied {
            self.commit_index = self.last_applied;
        }
        if self.last_applied < self.commit_index {
            let entries = self.log_storage.read_entries(self.last_applied+1, self.commit_index+1).await?;
            self.apply_entries(entries).await?;
        }
        if self.last_log_index < self.last_applied {
            self.log_storage.purge(self.last_applied).await?;
            self.last_log_index = self.last_applied;
        }
        let is_only_configured_member = self.membership.members.len() == 1 && self.membership.members.contains_key(&self.id);
        if is_only_configured_member {
            self.target_state = State::Leader;
        }
        else if self.membership.members.contains_key(&self.id) {
            self.target_state = State::Follower;
            let instant = Instant::now() + self.config.new_rand_election_timeout();
            self.next_election_timeout = Some(instant);
        }
        else {
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

    async fn apply_entries(&mut self, entries: Vec<Entry<N, D>>) -> Result<(), Error> {
        let mut batch = Some(Vec::new());
        for entry in entries.iter() {
            match &entry.payload {
                EntryPayload::Blank => {},
                EntryPayload::Normal(payload) => {
                    if let Some(mut e) = batch.to_owned() {
                        e.push((entry.index, &payload.data));
                    } else {
                        batch = Some(vec![(entry.index, &payload.data)]);
                    }
                },
                EntryPayload::ConfigChange(payload) => {
                    if let Some(e) = batch.take() {
                        self.state_machine.apply_entries(e).await?;
                    }
                    self.state_machine.save_membership_config(entry.index, payload.membership.clone()).await?;
                },
            }
        }
        if let Some(e) = batch.take() {
            self.state_machine.apply_entries(e).await?;
        }
        Ok(())
    }

    async fn run_leader(&mut self) -> Result<(), Error> {
        Err("not implemented leader state")?
    }

    async fn run_candidate(&mut self) -> Result<(), Error> {
        Err("not implemented candidate state")?
    }

    async fn run_follower(&mut self) -> Result<(), Error> {
        Err("not implemented follower state")?
    }

    async fn run_non_voter(&mut self) -> Result<(), Error> {
        Err("not implemented non voter state")?
    }
}
