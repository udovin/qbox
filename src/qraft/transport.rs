use super::{
    AppendEntriesRequest, AppendEntriesResponse, Data, Error, InstallSnapshotRequest,
    InstallSnapshotResponse, Node, NodeId, RequestVoteRequest, RequestVoteResponse,
};

#[async_trait::async_trait]
pub trait Connection<N: Node, D: Data> {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<N, D>,
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
pub trait Transport<N: Node, D: Data>: Send + Sync + 'static {
    type Connection: Connection<N, D>;

    async fn connect(&self, id: NodeId, node: &N) -> Result<Self::Connection, Error>;
}
