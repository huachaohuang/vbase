/// Miri does not support [`crc32fast`].
#[cfg(miri)]
pub fn checksum(_: &[u8]) -> u32 {
    42
}

/// Miri does not support [`crc32fast`].
#[cfg(miri)]
pub fn checksum_combined(_: &[u8], _: &[u8]) -> u32 {
    42
}

/// Calculates the CRC32 checksum of a slice.
#[cfg(not(miri))]
pub fn checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Calculates the CRC32 checksum of two slices combined.
#[cfg(not(miri))]
pub fn checksum_combined(a: &[u8], b: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(a);
    hasher.update(b);
    hasher.finalize()
}
