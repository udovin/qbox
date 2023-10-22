use std::collections::BTreeMap;

use tokio::sync::RwLock;

use crate::raft::{Data, Entry, Error, LogId, LogState, LogStorage};

struct MemLogStorageInner<D: Data> {
    logs: BTreeMap<u64, Entry<D>>,
    last_purged_log_id: LogId,
}

pub struct MemLogStorage<D: Data> {
    inner: RwLock<MemLogStorageInner<D>>,
}

impl<D: Data> MemLogStorage<D> {
    pub fn new() -> Self {
        let inner = MemLogStorageInner {
            logs: BTreeMap::new(),
            last_purged_log_id: LogId::default(),
        };
        Self {
            inner: RwLock::new(inner),
        }
    }
}

#[async_trait::async_trait]
impl<D: Data + Clone> LogStorage<D> for MemLogStorage<D> {
    async fn get_log_state(&self) -> Result<LogState, Error> {
        let inner = self.inner.read().await;
        Ok(match inner.logs.last_key_value() {
            Some(last_log) => LogState {
                last_purged_log_id: inner.last_purged_log_id,
                last_log_id: last_log.1.log_id,
            },
            None => LogState::default(),
        })
    }

    async fn read_entries(&self, from: u64, to: u64) -> Result<Vec<Entry<D>>, Error> {
        let inner = self.inner.read().await;
        Ok(inner.logs.range(from..to).map(|v| v.1).cloned().collect())
    }

    async fn append_entries(&self, entries: Vec<Entry<D>>) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        for entry in entries.into_iter() {
            inner.logs.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn purge(&self, index: u64) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        inner.logs = inner.logs.split_off(&(index + 1));
        Ok(())
    }

    async fn truncate(&self, index: u64) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        inner.logs.split_off(&index);
        Ok(())
    }
}
