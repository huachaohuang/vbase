pub mod tree;
pub use tree::Tree;

/// A list of supported collection kinds.
#[non_exhaustive]
#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Kind {
    Tree = 1,
}

#[doc(hidden)]
impl From<u32> for Kind {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::Tree,
            _ => panic!("invalid collection kind: {value}"),
        }
    }
}

/// A list of supported collection options.
#[non_exhaustive]
pub enum Options {
    Tree(tree::Options),
}

impl Options {
    pub(crate) fn kind(&self) -> Kind {
        match self {
            Self::Tree(_) => Kind::Tree,
        }
    }
}

impl From<tree::Options> for Options {
    fn from(options: tree::Options) -> Self {
        Self::Tree(options)
    }
}

/// A collection in the database.
#[allow(private_bounds)]
pub trait Collection: private::Collection {
    type Options: Into<Options>;
    type WriteBatch;
}

impl Collection for Tree {
    type Options = tree::Options;
    type WriteBatch = tree::WriteBatch;
}

/// Information about a collection.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct CollectionInfo {
    pub id: u64,
    pub name: String,
    pub kind: Kind,
}

pub(crate) mod private {
    use vbase_env::Dir;

    use super::Options;
    use super::tree::{Tree, TreeHandle};
    use crate::database::Database;
    use crate::error::Result;

    #[derive(Clone)]
    pub(crate) enum Handle {
        Tree(TreeHandle),
    }

    impl Handle {
        pub(crate) fn open(dir: Box<dyn Dir>, options: Options) -> Result<Self> {
            match options {
                Options::Tree(options) => TreeHandle::open(dir, options).map(Self::Tree),
            }
        }

        pub(crate) fn shutdown(&self) {
            match self {
                Handle::Tree(handle) => handle.shutdown(),
            }
        }
    }

    pub(crate) trait Collection: Send + Sync {
        fn open(db: Database, handle: Handle) -> Result<Self>
        where
            Self: Sized;
    }

    impl Collection for Tree {
        fn open(db: Database, handle: Handle) -> Result<Self> {
            match handle {
                Handle::Tree(handle) => Ok(Self::open(db, handle)),
            }
        }
    }
}
