use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::raft::{
    Data, Entry, EntryPayload, Error, HardState, LogId, MembershipConfig, Node, Response,
    StateMachine,
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

pub struct MemStateMachine<N: Node> {
    data: HashMap<String, String>,
    hard_state: HardState,
    membership: MembershipConfig<N>,
    applied_log_id: LogId,
}

impl<N: Node> MemStateMachine<N> {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            hard_state: HardState::default(),
            membership: MembershipConfig::default(),
            applied_log_id: LogId::default(),
        }
    }

    pub fn get(&self, key: &String) -> Option<&String> {
        self.data.get(key)
    }
}

#[async_trait::async_trait]
impl<N: Node + Clone> StateMachine<N, Action, ActionResponse> for MemStateMachine<N> {
    async fn get_applied_log_id(&self) -> Result<LogId, Error> {
        Ok(self.applied_log_id)
    }

    async fn get_membership_config(&self) -> Result<MembershipConfig<N>, Error> {
        Ok(self.membership.clone())
    }

    async fn apply_entries(
        &mut self,
        entires: Vec<Entry<N, Action>>,
    ) -> Result<Vec<ActionResponse>, Error> {
        let mut resp = Vec::new();
        for entry in entires.into_iter() {
            assert!(self.applied_log_id.index < entry.log_id.index);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::ConfigChange(config_change) => {
                    self.membership = config_change.membership;
                }
                EntryPayload::Data(data) => {
                    match data.data {
                        Action::Set { key, value } => {
                            resp.push(ActionResponse(self.data.insert(key, value)));
                        }
                        Action::Delete { key } => {
                            resp.push(ActionResponse(self.data.remove(&key)));
                        }
                    };
                }
            };
            self.applied_log_id = entry.log_id;
        }
        Ok(resp)
    }

    async fn get_hard_state(&self) -> Result<HardState, Error> {
        Ok(self.hard_state.clone())
    }

    async fn save_hard_state(&mut self, hard_state: HardState) -> Result<(), Error> {
        self.hard_state = hard_state;
        Ok(())
    }
}
