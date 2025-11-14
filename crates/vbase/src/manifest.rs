use std::collections::HashMap;

use prost::Message;

#[derive(Default)]
pub(crate) struct Desc {
    pub(crate) last_id: u64,
    pub(crate) collections: HashMap<u64, CollectionDesc>,
}

impl Desc {
    pub(crate) fn merge(&mut self, edit: Edit) {
        self.last_id = self.last_id.max(edit.last_id);
        for desc in edit.add_collections {
            self.collections.insert(desc.id, desc);
        }
        for name in edit.delete_collections {
            self.collections.retain(|_, desc| desc.name != name);
        }
    }

    pub(crate) fn to_edit(&self) -> Edit {
        Edit {
            last_id: self.last_id,
            add_collections: self.collections.values().cloned().collect(),
            delete_collections: Vec::new(),
        }
    }
}

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
