pub mod tree;
pub use tree::Tree;

pub enum Options {
    Tree(tree::Options),
}

impl From<tree::Options> for Options {
    fn from(options: tree::Options) -> Self {
        Options::Tree(options)
    }
}

#[allow(private_bounds)]
pub trait Collection: private::Collection {
    type Options: Into<Options>;
    type WriteBatch;
}

impl Collection for Tree {
    type Options = tree::Options;
    type WriteBatch = tree::WriteBatch;
}

pub(crate) mod private {
    use vbase_env::Dir;

    use super::Options;
    use super::tree::{Tree, TreeHandle};
    use crate::database::Database;
    use crate::error::Result;

    #[repr(u32)]
    #[derive(Copy, Clone, Eq, PartialEq)]
    pub(crate) enum Kind {
        Tree = 1,
    }

    impl From<u32> for Kind {
        fn from(value: u32) -> Self {
            match value {
                1 => Kind::Tree,
                _ => panic!("invalid collection kind: {}", value),
            }
        }
    }

    #[derive(Clone)]
    pub(crate) enum Handle {
        Tree(TreeHandle),
    }

    impl Handle {
        pub(crate) fn open(dir: Box<dyn Dir>, options: Options) -> Result<Self> {
            match options {
                Options::Tree(options) => TreeHandle::open(dir, options).map(Handle::Tree),
            }
        }

        pub(crate) fn kind(&self) -> Kind {
            match self {
                Handle::Tree(_) => Kind::Tree,
            }
        }

        pub(crate) fn shutdown(&self) {
            match self {
                Handle::Tree(handle) => handle.shutdown(),
            }
        }
    }

    pub(crate) trait Collection {
        fn open(db: Database, handle: Handle) -> Result<Self>
        where
            Self: Sized;
    }

    impl Collection for Tree {
        fn open(db: Database, handle: Handle) -> Result<Self>
        where
            Self: Sized,
        {
            match handle {
                Handle::Tree(handle) => Ok(Self::open(db, handle)),
            }
        }
    }
}
