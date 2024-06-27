use bytes::Bytes;
use thiserror::Error;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{
    cmd::Ping,
    connection::{Connection, ConnectionError},
    list::RList,
    map::RMap,
};

pub struct Client {
    connection: Connection,
}

impl Client {
    pub async fn create<T: ToSocketAddrs>(addr: T) -> Result<Client, ClientError> {
        let socket = TcpStream::connect(addr).await?;

        Ok(Client {
            connection: Connection::new(socket),
        })
    }

    pub async fn ping(mut self) -> Result<Bytes, ClientError> {
        let ping = Ping;
        let pong = self.connection.send(ping.parse()).await?;
        Ok(pong)
    }

    pub async fn read_list() -> RList {
        todo!()
    }

    pub async fn read_map() -> RMap {
        todo!()
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ClientError {
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}
