use std::collections::BTreeMap;

use crate::raft::{Data, Entry, Error, LogState, LogStorage, Node};

pub struct MemLogStorage<N: Node, D: Data> {
    logs: BTreeMap<u64, Entry<N, D>>,
    committed_index: u64,
}

impl<N: Node, D: Data> MemLogStorage<N, D> {
    pub fn new() -> Self {
        Self {
            logs: BTreeMap::new(),
            committed_index: 0,
        }
    }
}

#[async_trait::async_trait]
impl<N: Node + Clone, D: Data + Clone> LogStorage<N, D> for MemLogStorage<N, D> {
    async fn get_log_state(&self) -> Result<LogState, Error> {
        Ok(match self.logs.last_key_value() {
            Some(last_log) => LogState {
                last_log_id: last_log.1.log_id,
            },
            None => LogState::default(),
        })
    }

    async fn read_entries(&self, from: u64, to: u64) -> Result<Vec<Entry<N, D>>, Error> {
        Ok(self.logs.range(from..to).map(|v| v.1).cloned().collect())
    }

    async fn append_entries(&mut self, entries: Vec<Entry<N, D>>) -> Result<(), Error> {
        for entry in entries.into_iter() {
            self.logs.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn get_committed_index(&self) -> Result<u64, Error> {
        Ok(self.committed_index)
    }

    async fn save_committed_index(&mut self, index: u64) -> Result<(), Error> {
        let state = self.get_log_state().await?;
        if state.last_log_id.index < index {
            Err(format!(
                "commited index is greater than last_index: {} > {}",
                index, state.last_log_id.index
            ))?
        }
        self.committed_index = index;
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
