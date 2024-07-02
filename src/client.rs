use redis::{ConnectionLike, IntoConnectionInfo};
use thiserror::Error;

use crate::{list::RList, map::RMap};

pub struct Client {
    inner: redis::Client,
}

impl Client {
    pub fn create<T: IntoConnectionInfo>(addr: T) -> Result<Client, ClientError> {
        let inner = redis::Client::open(addr)?;

        Ok(Client { inner })
    }

    pub fn is_connected(&self) -> Result<bool, ClientError> {
        let mut conn = self.inner.get_connection()?;
        Ok(conn.check_connection())
    }

    pub fn get_list<'a, T>(&'a self, key: &'a str) -> RList<'a, T> {
        RList::new(key, &self.inner)
    }

    pub fn get_map<'a, K, V>(&'a self, key: &'a str) -> RMap<'a, K, V> {
        RMap::new(key, &self.inner)
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ClientError {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}
