use serde::{Serialize, Deserialize};

use super::{Error, MembershipConfig, Entry, Node, Data, Response};

pub struct LogState {
    pub last_index: u64,
    pub last_term: u64,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
}

#[async_trait::async_trait]
pub trait LogStorage<N: Node, D: Data>: Send + Sync + 'static {
    async fn get_log_state(&self) -> Result<LogState, Error>;

    async fn read_entries(&self, from: u64, to: u64) -> Result<Vec<Entry<N, D>>, Error>;

    async fn append_entries(&mut self, entries: Vec<Entry<N, D>>) -> Result<(), Error>;

    async fn get_commited_index(&self) -> Result<u64, Error>;

    async fn save_commited_index(&mut self, index: u64) -> Result<(), Error>;

    // Purge logs up to `index`, inclusive (`entry.index` <= `index`).
    async fn purge(&mut self, index: u64) -> Result<(), Error>;

    // Truncate logs since `index`, inclusive.
    async fn truncate(&mut self, index: u64) -> Result<(), Error>;
}

#[async_trait::async_trait]
pub trait StateMachine<N: Node, D: Data, R: Response>: Send + Sync + 'static {
    async fn get_hard_state(&self) -> Result<HardState, Error>;

    async fn save_hard_state(&mut self, hard_state: HardState) -> Result<(), Error>;

    async fn get_applied_index(&self) -> Result<u64, Error>;

    async fn apply_entries(&mut self, entires: Vec<(u64, &D)>) -> Result<Vec<R>, Error>;

    async fn get_membership_config(&self) -> Result<MembershipConfig<N>, Error>;

    async fn save_membership_config(&mut self, index: u64, membership: MembershipConfig<N>) -> Result<(), Error>;
}
