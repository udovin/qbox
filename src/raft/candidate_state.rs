use std::time::Instant;

use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;

use crate::raft::{Connection, RequestVoteRequest};

use super::{Data, Error, LogStorage, Message, RaftNode, Response, State, StateMachine, Transport};

pub(super) struct CandidateState<'a, D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    node: &'a mut RaftNode<D, R, TR, LS, SM>,
}

impl<'a, D, R, TR, LS, SM> CandidateState<'a, D, R, TR, LS, SM>
where
    D: Data,
    R: Response,
    TR: Transport<D>,
    LS: LogStorage<D>,
    SM: StateMachine<D, R>,
{
    pub(super) fn new(node: &'a mut RaftNode<D, R, TR, LS, SM>) -> Self {
        Self { node }
    }

    pub(super) async fn run(self) -> Result<(), Error> {
        self.node
            .update_hard_state(self.node.current_term + 1, Some(self.node.id))
            .await?;
        let members = self.node.membership.all_members();
        let mut vote_responses = FuturesOrdered::new();
        let mut vote_grants = 1;
        for member in members.iter() {
            if member == &self.node.id {
                continue;
            }
            let term = self.node.current_term;
            let candidate_id = self.node.id;
            let last_log_id = self.node.last_log_id;
            let transport = self.node.transport.clone();
            let member = *member;
            let join_handle = tokio::spawn(async move {
                let mut connection = transport.connect(member).await?;
                connection
                    .request_vote(RequestVoteRequest {
                        term,
                        candidate_id,
                        last_log_id,
                    })
                    .await
            });
            vote_responses.push_back(join_handle);
        }
        loop {
            if !matches!(self.node.target_state, State::Candidate) {
                return Ok(());
            }
            let election_timeout = self.node.get_election_timeout();
            tokio::select! {
                _ = election_timeout => return Ok(()),
                Some(message) = self.node.rx.recv() => match message {
                    Message::AppendEntries{request, tx} => {
                        let _ = tx.send(self.node.handle_append_entries(request).await);
                    }
                    Message::RequestVote{request, tx} => {
                        let _ = tx.send(self.node.handle_request_vote(request).await);
                    }
                    Message::InstallSnapshot{request, tx} => {
                        let _ = tx.send(self.node.handle_install_snapshot(request).await);
                    }
                    Message::InitCluster{tx, ..} => {
                        let _ = tx.send(Err("node already initialized".into()));
                    }
                    Message::AddNode{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                    Message::RemoveNode{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                    Message::WriteEntry{tx, ..} => {
                        let _ = tx.send(Err("node is not leader".into()));
                    }
                },
                Some(result) = vote_responses.next() => match result? {
                    Ok(result) => {
                        if result.vote_granted {
                            assert!(result.term == self.node.current_term);
                            vote_grants += 1;
                            if vote_grants > members.len() / 2 {
                                self.node.target_state = State::Leader;
                                return Ok(())
                            }
                        }
                        self.node.update_hard_state(result.term, None).await?;
                        self.node.target_state = State::Follower;
                        return Ok(());
                    },
                    Err(_) => {},
                },
                Ok(_) = &mut self.node.rx_shutdown => {
                    self.node.target_state = State::Shutdown;
                }
            }
        }
    }
}
