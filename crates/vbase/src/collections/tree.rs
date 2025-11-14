use std::fmt;
use std::sync::Arc;

use vbase_env::Dir;
pub use vbase_tree::{Options, WriteBatch};

use crate::database::Database;
use crate::error::Result;

pub struct Tree {
    db: Database,
    handle: TreeHandle,
}

impl Tree {
    pub(crate) fn open(db: Database, handle: TreeHandle) -> Self {
        Self { db, handle }
    }
}

impl fmt::Debug for Tree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tree").finish()
    }
}

#[derive(Clone)]
pub(crate) struct TreeHandle {
    tree: Arc<vbase_tree::Tree>,
}

impl TreeHandle {
    pub(crate) fn open(dir: Box<dyn Dir>, options: Options) -> Result<Self> {
        let tree = vbase_tree::Tree::open(dir, options)?;
        Ok(Self {
            tree: Arc::new(tree),
        })
    }

    pub(crate) fn shutdown(&self) {}
}
