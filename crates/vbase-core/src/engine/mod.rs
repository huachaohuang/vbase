pub mod internal;

/// A database engine.
#[allow(private_bounds)]
pub trait Engine: sealed::Engine {
    type Bucket: Bucket;
}

/// A bucket in the engine.
#[allow(private_bounds)]
pub trait Bucket: sealed::Bucket {
    type Reader<'a>: sealed::Reader<'a>;
    type Writer<'a>: sealed::Writer<'a>;
}

mod sealed {
    use super::internal;

    pub(super) trait Engine: internal::Engine {}

    impl<T: internal::Engine> Engine for T {}

    pub(super) trait Bucket: internal::Bucket {}

    impl<T: internal::Bucket> Bucket for T {}

    pub(super) trait Reader<'a>: internal::Reader<'a> {}

    impl<'a, T: internal::Reader<'a>> Reader<'a> for T {}

    pub(super) trait Writer<'a>: internal::Writer<'a> {}

    impl<'a, T: internal::Writer<'a>> Writer<'a> for T {}
}
