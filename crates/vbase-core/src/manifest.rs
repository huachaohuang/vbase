use prost::Message;

#[derive(Message)]
pub(crate) struct Desc {
    #[prost(tag = "1", uint64)]
    pub(crate) last_id: u64,
    #[prost(tag = "2", repeated, message)]
    pub(crate) engines: Vec<EngineDesc>,
}

#[derive(Message)]
pub(crate) struct EngineDesc {
    #[prost(tag = "1", uint64)]
    pub(crate) id: u64,
    #[prost(tag = "2", string)]
    pub(crate) name: String,
}
