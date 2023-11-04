use std::future::Future;

use super::{
    AppendEntriesRequest, AppendEntriesResponse, Data, Error, InstallSnapshotRequest,
    InstallSnapshotResponse, NodeId, RequestVoteRequest, RequestVoteResponse,
};

pub trait Connection<D: Data>: Send {
    fn append_entries(
        &mut self,
        request: AppendEntriesRequest<D>,
    ) -> impl Future<Output = Result<AppendEntriesResponse, Error>> + Send;

    fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> impl Future<Output = Result<InstallSnapshotResponse, Error>> + Send;

    fn request_vote(
        &mut self,
        request: RequestVoteRequest,
    ) -> impl Future<Output = Result<RequestVoteResponse, Error>> + Send;
}

pub trait Transport<D: Data>: Send + Sync + 'static {
    type Connection: Connection<D>;

    fn connect(&self, id: NodeId) -> impl Future<Output = Result<Self::Connection, Error>> + Send;
}
