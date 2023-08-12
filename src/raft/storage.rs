use serde::{Deserialize, Serialize};

use super::{Data, Entry, Error, LogId, MembershipConfig, Node, Response};

#[derive(Default)]
pub struct LogState {
    pub last_log_id: LogId,
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

    // Purge logs up to `index`, inclusive (`entry.index` <= `index`).
    async fn purge(&mut self, index: u64) -> Result<(), Error>;

    // Truncate logs since `index`, inclusive (`entry.index` >= `index`).
    async fn truncate(&mut self, index: u64) -> Result<(), Error>;
}

#[async_trait::async_trait]
pub trait StateMachine<N: Node, D: Data, R: Response>: Send + Sync + 'static {
    async fn get_applied_log_id(&self) -> Result<LogId, Error>;

    async fn get_membership_config(&self) -> Result<MembershipConfig<N>, Error>;

    async fn apply_entries(&mut self, entires: Vec<Entry<N, D>>) -> Result<Vec<R>, Error>;

    async fn get_hard_state(&self) -> Result<HardState, Error>;

    async fn save_hard_state(&mut self, hard_state: HardState) -> Result<(), Error>;
}
