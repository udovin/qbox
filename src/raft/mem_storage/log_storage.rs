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

impl<D: Data + Clone> LogStorage<D> for MemLogStorage<D> {
    async fn get_log_state(&self) -> Result<LogState, Error> {
        let inner = self.inner.read().await;
        let last_log_id = match inner.logs.last_key_value() {
            Some((_, entry)) => entry.log_id,
            None => inner.last_purged_log_id,
        };
        Ok(LogState {
            last_log_id,
            last_purged_log_id: inner.last_purged_log_id,
        })
    }

    async fn read_entries(&self, begin: u64, end: u64) -> Result<Vec<Entry<D>>, Error> {
        let inner = self.inner.read().await;
        Ok(inner.logs.range(begin..end).map(|v| v.1).cloned().collect())
    }

    async fn append_entries(&self, entries: Vec<Entry<D>>) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        for entry in entries.into_iter() {
            inner.logs.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn purge(&self, end: u64) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        let new_logs = inner.logs.split_off(&end);
        if let Some((_, entry)) = inner.logs.last_key_value() {
            inner.last_purged_log_id = entry.log_id;
        }
        inner.logs = new_logs;
        Ok(())
    }

    async fn truncate(&self, begin: u64) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        inner.logs.split_off(&begin);
        if inner.last_purged_log_id.index >= begin {
            inner.last_purged_log_id = LogId::default();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::mem_storage::Action;
    use crate::raft::{Entry, EntryPayload, LogId, LogStorage};

    use super::MemLogStorage;

    #[tokio::test]
    async fn test_simple() {
        let log_storage = MemLogStorage::<Action>::new();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId::default());
        assert_eq!(state.last_purged_log_id, LogId::default());
        for i in 1..10 {
            log_storage
                .append_entries(vec![Entry {
                    log_id: LogId { term: i, index: i },
                    payload: EntryPayload::Blank,
                }])
                .await
                .unwrap();
            let state = log_storage.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id, LogId { term: i, index: i });
            assert_eq!(state.last_purged_log_id, LogId::default());
        }
        log_storage.purge(1).await.unwrap();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId { term: 9, index: 9 });
        assert_eq!(state.last_purged_log_id, LogId { term: 0, index: 0 });
        log_storage.purge(3).await.unwrap();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId { term: 9, index: 9 });
        assert_eq!(state.last_purged_log_id, LogId { term: 2, index: 2 });
        log_storage.purge(2).await.unwrap();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId { term: 9, index: 9 });
        assert_eq!(state.last_purged_log_id, LogId { term: 2, index: 2 });
        log_storage.purge(6).await.unwrap();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId { term: 9, index: 9 });
        assert_eq!(state.last_purged_log_id, LogId { term: 5, index: 5 });
        log_storage.truncate(8).await.unwrap();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId { term: 7, index: 7 });
        assert_eq!(state.last_purged_log_id, LogId { term: 5, index: 5 });
        log_storage.truncate(9).await.unwrap();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId { term: 7, index: 7 });
        assert_eq!(state.last_purged_log_id, LogId { term: 5, index: 5 });
        log_storage.truncate(6).await.unwrap();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId { term: 5, index: 5 });
        assert_eq!(state.last_purged_log_id, LogId { term: 5, index: 5 });
        // Remove purged log entry.
        log_storage.truncate(5).await.unwrap();
        let state = log_storage.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, LogId::default());
        assert_eq!(state.last_purged_log_id, LogId::default());
    }
}
