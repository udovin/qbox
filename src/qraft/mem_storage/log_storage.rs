use std::collections::BTreeMap;

use crate::qraft::{LogStorage, LogState, Error, Entry, Data, Node};

pub struct MemLogStorage<N: Node, D: Data> {
    logs: BTreeMap<u64, Entry<N, D>>,
    commited_index: u64,
}

impl<N: Node, D: Data> MemLogStorage<N, D> {
    pub fn new() -> Self {
        Self {
            logs: BTreeMap::new(),
            commited_index: 0,
        }
    }
}

#[async_trait::async_trait]
impl<N: Node + Clone, D: Data + Clone> LogStorage<N, D> for MemLogStorage<N, D> {
    async fn get_log_state(&self) -> Result<LogState, Error> {
        Ok(match self.logs.last_key_value() {
            Some(last_log) => LogState {
                last_index: last_log.1.index,
                last_term: last_log.1.term,
            },
            None => LogState { last_index: 0, last_term: 0 },
        })
    }

    async fn read_entries(&self, from: u64, to: u64) -> Result<Vec<Entry<N, D>>, Error> {
        Ok(self.logs.range(from..to).map(|v| v.1).cloned().collect())
    }

    async fn append_entries(&mut self, entries: Vec<Entry<N, D>>) -> Result<(), Error> {
        for entry in entries.into_iter() {
            self.logs.insert(entry.index, entry);
        }
        Ok(())
    }

    async fn get_commited_index(&self) -> Result<u64, Error> {
        Ok(self.commited_index)
    }

    async fn save_commited_index(&mut self, index: u64) -> Result<(), Error> {
        let state = self.get_log_state().await?;
        if state.last_index < index {
            Err(format!("commited index is greater than last_index: {} > {}", index, state.last_index))?
        }
        self.commited_index = index;
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
