pub mod internal;

/// A database engine.
#[allow(private_bounds)]
pub trait Engine: sealed::Engine {
    type Bucket: Bucket;
}

/// A bucket in the engine.
#[allow(private_bounds)]
pub trait Bucket: sealed::Bucket {
    type WriteBatch<'a>: sealed::WriteBatch<'a>;
}

mod sealed {
    use super::internal;

    pub(super) trait Engine: internal::Engine {}

    impl<T: internal::Engine> Engine for T {}

    pub(super) trait Bucket: internal::Bucket {}

    impl<T: internal::Bucket> Bucket for T {}

    pub(super) trait WriteBatch<'a>: internal::WriteBatch<'a> {}

    impl<'a, T: internal::WriteBatch<'a>> WriteBatch<'a> for T {}
}
