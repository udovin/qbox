use crate::qraft::{StateMachine, HardState, Error, MembershipConfig, Node, Response, Data};

pub struct MemStateMachine<N: Node> {
    hard_state: HardState,
    membership: MembershipConfig<N>,
    applied_index: u64,
}

impl<N: Node> MemStateMachine<N> {
    pub fn new() -> Self {
        Self {
            hard_state: HardState::default(),
            membership: MembershipConfig::default(),
            applied_index: 0,
        }
    }
}

#[derive(Default)]
pub struct Empty {}

impl Response for Empty {}

#[async_trait::async_trait]
impl<N: Node + Clone, D: Data> StateMachine<N, D, Empty> for MemStateMachine<N> {
    async fn get_hard_state(&self) -> Result<HardState, Error> {
        Ok(self.hard_state.clone())
    }

    async fn save_hard_state(&mut self, hard_state: HardState) -> Result<(), Error> {
        self.hard_state = hard_state;
        Ok(())
    }

    async fn get_applied_index(&self) -> Result<u64, Error> {
        Ok(self.applied_index)
    }

    async fn apply_entries(&mut self, entires: Vec<(u64, &D)>) -> Result<Vec<Empty>, Error> {
        let mut resp = Vec::new();
        for (index, _) in entires.into_iter() {
            assert!(self.applied_index < index);
            self.applied_index = index;
            resp.push(Empty::default());
        }
        Ok(resp)
    }

    async fn get_membership_config(&self) -> Result<MembershipConfig<N>, Error> {
        Ok(self.membership.clone())
    }

    async fn save_membership_config(&mut self, index: u64, membership: MembershipConfig<N>) -> Result<(), Error> {
        assert!(self.applied_index < index);
        self.applied_index = index;
        self.membership = membership;
        Ok(())
    }
}
