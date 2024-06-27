use bytes::Bytes;

const PING: &'static str = "*2\r\n$4\r\nping\r\n$4\r\npong\r\n";

pub struct Ping;

impl Ping {
    pub fn parse(self) -> Bytes {
        Bytes::from(PING)
    }
}
