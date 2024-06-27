use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::resp::MessageCodec;

pub struct Connection {
    // TODO: implement custom codec to control the framed messages
    framed: Framed<TcpStream, MessageCodec>,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Connection {
            framed: Framed::new(socket, MessageCodec::new()),
        }
    }

    pub async fn send(&mut self, bytes: Bytes) -> Result<Bytes, ConnectionError> {
        let _ = self.framed.send(bytes).await?;

        if let Some(response) = self.framed.next().await {
            // TODO: it seems that the bytes coming from the response are sent back again to the sink?
            let bytes = response?.freeze();
            Ok(bytes)
        } else {
            Err(ConnectionError::NoResponse)
        }
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConnectionError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("no response was returned after sending")]
    NoResponse,
}
