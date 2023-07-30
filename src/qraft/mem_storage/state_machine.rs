use crate::qraft::{StateMachine, HardState, Error, MembershipConfig, Node, Response, Data, LogId, Entry, EntryPayload};

pub struct MemStateMachine<N: Node> {
    hard_state: HardState,
    membership: MembershipConfig<N>,
    applied_log_id: LogId,
}

impl<N: Node> MemStateMachine<N> {
    pub fn new() -> Self {
        Self {
            hard_state: HardState::default(),
            membership: MembershipConfig::default(),
            applied_log_id: LogId::default(),
        }
    }
}

#[async_trait::async_trait]
impl<N: Node + Clone, D: Data> StateMachine<N, D, ()> for MemStateMachine<N> {
    async fn get_applied_log_id(&self) -> Result<LogId, Error> {
        Ok(self.applied_log_id)
    }

    async fn get_membership_config(&self) -> Result<MembershipConfig<N>, Error> {
        Ok(self.membership.clone())
    }

    async fn apply_entries(&mut self, entires: Vec<Entry<N, D>>) -> Result<Vec<()>, Error> {
        let mut resp = Vec::new();
        for entry in entires.into_iter() {
            assert!(self.applied_log_id.index < entry.log_id.index);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::ConfigChange(config_change) => {
                    self.membership = config_change.membership;
                }
                EntryPayload::Normal(normal) => {
                    resp.push(());
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
