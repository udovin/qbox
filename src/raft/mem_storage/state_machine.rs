use std::{collections::HashMap, net::SocketAddr};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::raft::{
    Data, Entry, EntryPayload, Error, HardState, LogId, MembershipConfig, Response,
    StateMachine, ws_transport::NodeMetaStorage, NodeId,
};

#[derive(Clone, Serialize, Deserialize)]
pub enum Action {
    Set { key: String, value: String },
    Delete { key: String },
}

impl Data for Action {}

#[derive(Clone, Serialize, Deserialize)]
pub struct ActionResponse(Option<String>);

impl Response for ActionResponse {}

struct MemStateMachineInner {
    data: HashMap<String, String>,
    hard_state: HardState,
    membership: MembershipConfig,
    applied_log_id: LogId,
}

pub struct MemStateMachine {
    inner: RwLock<MemStateMachineInner>,
}

impl MemStateMachine {
    pub fn new() -> Self {
        let inner = MemStateMachineInner {
            data: HashMap::new(),
            hard_state: HardState::default(),
            membership: MembershipConfig::default(),
            applied_log_id: LogId::default(),
        };
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub async fn get<'a>(&self, key: &String) -> Option<String> {
        let inner = self.inner.read().await;
        match inner.data.get(key) {
            Some(value) => Some(value.into()),
            None => None,
        }
    }
}

#[async_trait::async_trait]
impl StateMachine<Action, ActionResponse> for MemStateMachine {
    async fn get_applied_log_id(&self) -> Result<LogId, Error> {
        let inner = self.inner.read().await;
        Ok(inner.applied_log_id)
    }

    async fn get_membership_config(&self) -> Result<MembershipConfig, Error> {
        let inner = self.inner.read().await;
        Ok(inner.membership.clone())
    }

    async fn apply_entries(
        &self,
        entires: Vec<Entry<Action>>,
    ) -> Result<Vec<ActionResponse>, Error> {
        let mut resp = Vec::new();
        let mut inner = self.inner.write().await;
        for entry in entires.into_iter() {
            assert!(inner.applied_log_id.index < entry.log_id.index);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::ConfigChange(config_change) => {
                    inner.membership = config_change.membership;
                }
                EntryPayload::Data(data) => {
                    match data.data {
                        Action::Set { key, value } => {
                            resp.push(ActionResponse(inner.data.insert(key, value)));
                        }
                        Action::Delete { key } => {
                            resp.push(ActionResponse(inner.data.remove(&key)));
                        }
                    };
                }
            };
            inner.applied_log_id = entry.log_id;
        }
        Ok(resp)
    }

    async fn get_hard_state(&self) -> Result<HardState, Error> {
        let inner = self.inner.read().await;
        Ok(inner.hard_state.clone())
    }

    async fn save_hard_state(&self, hard_state: HardState) -> Result<(), Error> {
        let mut inner = self.inner.write().await;
        inner.hard_state = hard_state;
        Ok(())
    }
}

#[async_trait::async_trait]
impl NodeMetaStorage<SocketAddr> for MemStateMachine {
    async fn get_node_meta(&self, id: NodeId) -> Result<SocketAddr, Error> {
        match self.get(&format!("nodes/{}", id)).await {
            Some(addr) => Ok(addr.parse()?),
            None => Err("node not found".into()),
        }
    }
}
