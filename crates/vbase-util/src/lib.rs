pub mod crc32;

pub use concurrent::*;

#[cfg(feature = "shuttle")]
mod concurrent {
    pub use shuttle::sync;
    pub use shuttle::thread;
}

#[cfg(not(feature = "shuttle"))]
mod concurrent {
    pub use std::sync;
    pub use std::thread;
}
