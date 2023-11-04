use std::future::Future;

use serde::{Deserialize, Serialize};

use super::{Data, Entry, Error, LogId, MembershipConfig, Response};

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct LogState {
    pub last_purged_log_id: LogId,
    pub last_log_id: LogId,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
}

pub trait LogStorage<D: Data>: Send + Sync + 'static {
    fn get_log_state(&self) -> impl Future<Output = Result<LogState, Error>> + Send;

    fn read_entries(
        &self,
        begin: u64,
        end: u64,
    ) -> impl Future<Output = Result<Vec<Entry<D>>, Error>> + Send;

    fn append_entries(
        &self,
        entries: Vec<Entry<D>>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    // Purge logs up to `index`, exclusive (`entry.index` < `index`).
    fn purge(&self, end: u64) -> impl Future<Output = Result<(), Error>> + Send;

    // Truncate logs since `index`, inclusive (`entry.index` >= `index`).
    fn truncate(&self, begin: u64) -> impl Future<Output = Result<(), Error>> + Send;
}

pub trait StateMachine<D: Data, R: Response>: Send + Sync + 'static {
    fn get_applied_log_id(&self) -> impl Future<Output = Result<LogId, Error>> + Send;

    fn get_membership_config(&self)
        -> impl Future<Output = Result<MembershipConfig, Error>> + Send;

    fn apply_entries(
        &self,
        entires: Vec<Entry<D>>,
    ) -> impl Future<Output = Result<Vec<R>, Error>> + Send;

    fn get_hard_state(&self) -> impl Future<Output = Result<HardState, Error>> + Send;

    fn save_hard_state(
        &self,
        hard_state: HardState,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}
