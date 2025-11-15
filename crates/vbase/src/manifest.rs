use prost::Message;

/// Database descriptor.
#[derive(Default)]
pub(crate) struct Desc {
    pub(crate) last_id: u64,
    pub(crate) collections: Vec<CollectionDesc>,
}

impl Desc {
    pub(crate) fn merge(&mut self, mut edit: Edit) {
        self.last_id = self.last_id.max(edit.last_id);
        self.collections.append(&mut edit.add_collections);
        for name in edit.delete_collections {
            self.collections.retain(|desc| desc.name != name);
        }
    }

    pub(crate) fn to_edit(&self) -> Edit {
        Edit {
            last_id: self.last_id,
            add_collections: self.collections.clone(),
            delete_collections: Vec::new(),
        }
    }
}

/// An edit to the database descriptor.
#[derive(Message)]
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Edit {
    #[prost(tag = "1", uint64)]
    pub(crate) last_id: u64,
    #[prost(tag = "2", repeated, message)]
    pub(crate) add_collections: Vec<CollectionDesc>,
    #[prost(tag = "3", repeated, string)]
    pub(crate) delete_collections: Vec<String>,
}

/// Collection descriptor.
#[derive(Message)]
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CollectionDesc {
    #[prost(tag = "1", uint64)]
    pub(crate) id: u64,
    #[prost(tag = "2", string)]
    pub(crate) name: String,
    #[prost(tag = "3", uint32)]
    pub(crate) kind: u32,
}
