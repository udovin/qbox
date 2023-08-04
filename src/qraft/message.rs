use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::{Data, Error, Node, NodeId, Response};

pub enum Message<N: Node, D: Data, R: Response> {
    // Raft messages.
    AppendEntries {
        request: AppendEntriesRequest<N, D>,
        tx: oneshot::Sender<Result<AppendEntriesResponse, Error>>,
    },
    RequestVote {
        request: RequestVoteRequest,
        tx: oneshot::Sender<Result<RequestVoteResponse, Error>>,
    },
    InstallSnapshot {
        request: InstallSnapshotRequest,
        tx: oneshot::Sender<Result<InstallSnapshotResponse, Error>>,
    },
    // Managing messages.
    InitCluster {
        tx: oneshot::Sender<Result<(), Error>>,
    },
    AddNode {
        id: NodeId,
        node: N,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    // Client messages.
    ApplyEntry {
        data: D,
        tx: oneshot::Sender<Result<R, Error>>,
    },
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesRequest<N: Node, D: Data> {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_id: LogId,
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
    pub leader_id: NodeId,
    pub last_included_log_id: LogId,
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
    pub candidate_id: NodeId,
    pub last_log_id: LogId,
}

#[derive(Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Clone, Copy, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct LogId {
    pub index: u64,
    pub term: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Entry<N: Node, D: Data> {
    pub log_id: LogId,
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
