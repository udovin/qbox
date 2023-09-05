use std::collections::BTreeMap;

use crate::raft::{Data, Entry, Error, LogState, LogStorage, LogId};

pub struct MemLogStorage<D: Data> {
    logs: BTreeMap<u64, Entry<D>>,
    last_purged_log_id: LogId,
}

impl<D: Data> MemLogStorage<D> {
    pub fn new() -> Self {
        Self {
            logs: BTreeMap::new(),
            last_purged_log_id: LogId::default(),
        }
    }
}

#[async_trait::async_trait]
impl<D: Data + Clone> LogStorage<D> for MemLogStorage<D> {
    async fn get_log_state(&self) -> Result<LogState, Error> {
        Ok(match self.logs.last_key_value() {
            Some(last_log) => LogState {
                last_purged_log_id: self.last_purged_log_id,
                last_log_id: last_log.1.log_id,
            },
            None => LogState::default(),
        })
    }

    async fn read_entries(&self, from: u64, to: u64) -> Result<Vec<Entry<D>>, Error> {
        Ok(self.logs.range(from..to).map(|v| v.1).cloned().collect())
    }

    async fn append_entries(&mut self, entries: Vec<Entry<D>>) -> Result<(), Error> {
        for entry in entries.into_iter() {
            self.logs.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn purge(&mut self, index: u64) -> Result<(), Error> {
        self.logs = self.logs.split_off(&(index + 1));
        Ok(())
    }

    async fn truncate(&mut self, index: u64) -> Result<(), Error> {
        self.logs.split_off(&index);
        Ok(())
    }
}
