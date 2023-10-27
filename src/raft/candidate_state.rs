use std::time::Instant;

use tokio::time::sleep_until;

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

    pub(super) async fn run(mut self) -> Result<(), Error> {
        slog::info!(self.node.logger, "Enter candidate state"; slog::o!("term" => self.node.current_term));
        let now = Instant::now();
        self.node.next_election_timeout = Some(now + self.node.config.new_rand_election_timeout());
        loop {
            if !matches!(self.node.target_state, State::Candidate) {
                return Ok(());
            }
            let election_timeout = sleep_until(self.node.next_election_timeout.unwrap().into());
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
                Ok(_) = &mut self.node.rx_shutdown => {
                    self.node.target_state = State::Shutdown;
                }
            }
        }
    }
}
