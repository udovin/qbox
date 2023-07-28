use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use tokio::sync::oneshot;

use super::{Error, Data, Node, NodeId};

pub enum Message<N: Node, D: Data> {
    // From leader.
    AppendEntries {
        request: AppendEntriesRequest<N, D>,
        callback: oneshot::Sender<Result<AppendEntriesResponse, Error>>,
    },
    RequestVote {
        request: RequestVoteRequest,
        callback: oneshot::Sender<Result<RequestVoteResponse, Error>>,
    },
    InstallSnapshot {
        request: InstallSnapshotRequest,
        callback: oneshot::Sender<Result<InstallSnapshotResponse, Error>>,
    },
    // // From client.
    // ClientReadRequest {},
    // ClientWriteRequest {},
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesRequest<N: Node, D: Data> {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<Entry<N, D>>,
    pub leader_commit: u64,
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
}

#[derive(Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Entry<N: Node, D: Data> {
    pub index: u64,
    pub term: u64,
    pub payload: EntryPayload<N, D>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum EntryPayload<N: Node, D: Data> {
    Blank,
    Normal(NormalEntry<D>),
    ConfigChange(ConfigChangeEntry<N>),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NormalEntry<D: Data> {
    pub data: D,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ConfigChangeEntry<N: Node> {
    pub membership: MembershipConfig<N>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MembershipConfig<N: Node> {
    pub members: HashMap<NodeId, N>,
    pub members_after_consensus: Option<HashMap<NodeId, N>>,
}

impl<N: Node> Default for MembershipConfig<N> {
    fn default() -> Self {
        Self {
            members: HashMap::new(),
            members_after_consensus: None,
        }
    }
}
