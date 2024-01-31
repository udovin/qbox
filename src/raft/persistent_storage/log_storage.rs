use std::marker::PhantomData;
use std::path::PathBuf;

use tokio::sync::RwLock;

use crate::raft::{Data, Entry, Error, LogState, LogStorage};

struct PersistentLogStorageInner<D: Data> {
    path: PathBuf,
    _phantom: PhantomData<D>,
}

pub struct PersistentLogStorage<D: Data> {
    inner: RwLock<PersistentLogStorageInner<D>>,
}

impl<D: Data> PersistentLogStorage<D> {
    pub fn new(path: PathBuf) -> Self {
        let inner = PersistentLogStorageInner {
            path,
            _phantom: PhantomData,
        };
        Self {
            inner: RwLock::new(inner),
        }
    }
}

impl<D: Data> LogStorage<D> for PersistentLogStorage<D> {
    async fn get_log_state(&self) -> Result<LogState, Error> {
        let inner = self.inner.read().await;
        todo!()
    }

    async fn read_entries(&self, begin: u64, end: u64) -> Result<Vec<Entry<D>>, Error> {
        let inner = self.inner.read().await;
        todo!()
    }

    async fn append_entries(&self, entries: Vec<Entry<D>>) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        todo!()
    }

    async fn purge(&self, end: u64) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        todo!()
    }

    async fn truncate(&self, begin: u64) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        todo!()
    }
}
