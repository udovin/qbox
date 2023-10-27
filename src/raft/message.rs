use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::{Data, Error, NodeId, Response};

pub(super) enum Message<D: Data, R: Response> {
    // Raft messages.
    AppendEntries {
        request: AppendEntriesRequest<D>,
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
        tx: oneshot::Sender<Result<(), Error>>,
    },
    RemoveNode {
        id: NodeId,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    // Client messages.
    WriteEntry {
        entry: EntryPayload<D>,
        tx: oneshot::Sender<Result<R, Error>>,
    },
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesRequest<D: Data> {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_id: LogId,
    pub entries: Vec<Entry<D>>,
    pub leader_commit: u64,
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub conflict_opt: Option<LogId>,
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
pub struct Entry<D: Data> {
    pub log_id: LogId,
    pub payload: EntryPayload<D>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum EntryPayload<D: Data> {
    Blank,
    Data(DataEntry<D>),
    ConfigChange(ConfigChangeEntry),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DataEntry<D: Data> {
    pub data: D,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ConfigChangeEntry {
    pub membership: MembershipConfig,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MembershipConfig {
    pub members: HashSet<NodeId>,
    pub members_after_consensus: Option<HashSet<NodeId>>,
}

impl MembershipConfig {
    pub fn contains(&self, id: &NodeId) -> bool {
        self.members.contains(id)
            || match self.members_after_consensus.as_ref() {
                Some(members) => members.contains(id),
                None => false,
            }
    }

    pub fn all_members(&self) -> HashSet<NodeId> {
        let mut all_members = self.members.clone();
        if let Some(members) = &self.members_after_consensus {
            all_members.extend(members.clone());
        }
        all_members
    }
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            members: HashSet::new(),
            members_after_consensus: None,
        }
    }
}
