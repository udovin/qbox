use super::{
    AppendEntriesRequest, AppendEntriesResponse, Data, Error, InstallSnapshotRequest,
    InstallSnapshotResponse, NodeId, RequestVoteRequest, RequestVoteResponse,
};

#[async_trait::async_trait]
pub trait Connection<D: Data> {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<D>,
    ) -> Result<AppendEntriesResponse, Error>;

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Error>;

    async fn request_vote(
        &mut self,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Error>;
}

#[async_trait::async_trait]
pub trait Transport<D: Data>: Send + Sync + 'static {
    type Connection: Connection<D>;

    async fn connect(&self, id: NodeId) -> Result<Self::Connection, Error>;
}
