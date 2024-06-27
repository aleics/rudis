use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub enum RESPValue {
    String(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Array(Vec<RESPValue>),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct MessageCodec(());

impl MessageCodec {
    pub fn new() -> Self {
        MessageCodec(())
    }
}

impl Decoder for MessageCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>, std::io::Error> {
        if !buf.is_empty() {
            let len = buf.len();
            Ok(Some(buf.split_to(len)))
        } else {
            Ok(None)
        }
    }
}

impl Encoder<Bytes> for MessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, data: Bytes, buf: &mut BytesMut) -> Result<(), std::io::Error> {
        buf.reserve(data.len());
        buf.put(data);
        Ok(())
    }
}
