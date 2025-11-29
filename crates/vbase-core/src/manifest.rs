use prost::Message;
use vbase_util::codec::Decode;
use vbase_util::codec::Encode;
use vbase_util::crc32::checksum;

#[derive(Message)]
pub(crate) struct Desc {
    #[prost(tag = "1", uint64)]
    pub(crate) last_id: u64,
    #[prost(tag = "2", repeated, message)]
    pub(crate) engines: Vec<EngineDesc>,
}

impl Desc {
    pub(crate) fn encode_with_checksum(&self) -> Vec<u8> {
        let mut buf = self.encode_to_vec();
        checksum(&buf).encode_to(&mut buf);
        buf
    }

    pub(crate) fn decode_with_checksum(buf: &[u8]) -> Result<Self, String> {
        let (message, mut crc) = buf
            .split_at_checked(buf.len() - 4)
            .ok_or_else(|| format!("invalid size {}", buf.len()))?;
        let checksum = checksum(message);
        let expected = u32::decode_from(&mut crc);
        if checksum != expected {
            return Err(format!(
                "checksum mismatch (expected {expected}, got {checksum})"
            ));
        }
        Self::decode(message).map_err(|e| format!("{e}"))
    }
}

#[derive(Message)]
pub(crate) struct EngineDesc {
    #[prost(tag = "1", uint64)]
    pub(crate) id: u64,
    #[prost(tag = "2", string)]
    pub(crate) name: String,
}
