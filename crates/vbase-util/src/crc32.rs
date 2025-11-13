/// Calculates the CRC32 checksum of two slices combined.
pub fn checksum_combined(a: &[u8], b: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(a);
    hasher.update(b);
    hasher.finalize()
}
