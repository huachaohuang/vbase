use vbase_env::Dir;
use vbase_file::Result;

pub struct Tree {
    dir: Box<dyn Dir>,
    options: Options,
}

impl Tree {
    pub fn open(dir: Box<dyn Dir>, options: Options) -> Result<Self> {
        Ok(Self { dir, options })
    }

    pub fn get(&self, id: &[u8], lsn: u64, options: &ReadOptions) -> Result<Option<()>> {
        todo!()
    }

    pub fn write(&self, batch: &[u8], lsn: u64) {
        todo!()
    }
}

#[derive(Clone)]
pub struct Options;

impl Options {
    pub fn new() -> Self {
        Self
    }
}

pub struct ReadOptions {
    pub cache: bool,
}

pub struct WriteBatch;

impl WriteBatch {
    pub fn new() -> Self {
        Self
    }

    pub fn put(&mut self, id: &[u8], value: &[u8]) {
        todo!()
    }

    pub fn delete(&mut self, id: &[u8]) {
        todo!()
    }
}

impl AsRef<[u8]> for WriteBatch {
    fn as_ref(&self) -> &[u8] {
        todo!()
    }
}
