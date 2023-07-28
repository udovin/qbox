pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type NodeId = u64;

pub trait Node: Clone + Send + Sync + 'static {}

pub trait Data: Send + Sync + 'static {}

pub trait Response: Send + Sync + 'static {}