pub mod crc32;

pub use concurrent::*;

#[cfg(feature = "shuttle")]
mod concurrent {
    pub use shuttle::sync;
    pub use shuttle::thread;

    pub mod rand {
        use shuttle::rand::Rng;
        use shuttle::rand::RngCore;
        use shuttle::rand::thread_rng as rng;

        pub fn random_u32() -> u32 {
            rng().next_u32()
        }

        pub fn random_bool(p: f64) -> bool {
            rng().gen_bool(p)
        }
    }
}

#[cfg(not(feature = "shuttle"))]
mod concurrent {
    pub use std::sync;
    pub use std::thread;

    pub mod rand {
        use rand::Rng;
        use rand::RngCore;
        use rand::rng;

        pub fn random_u32() -> u32 {
            rng().next_u32()
        }

        pub fn random_bool(p: f64) -> bool {
            rng().random_bool(p)
        }
    }
}
