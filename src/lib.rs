use std::time::Duration;

use async_trait::async_trait;
use lock::{RedisLockError, RwLockGuard};
use redis::{aio::MultiplexedConnection, AsyncCommands, Commands, Connection};
use thiserror::Error;

pub mod client;
pub mod list;
pub mod lock;
pub mod map;
pub mod set;

pub trait RObject<'a> {
    fn name(&self) -> &'a str;
    fn connection(&self) -> Result<Connection, RudisError>;
    fn lock(&'a self, duration: Duration) -> Result<RwLockGuard<'a>, RudisError>;
    fn is_acquired(&self) -> Result<bool, RudisError>;

    fn clear(&self) -> Result<(), RudisError> {
        if !self.is_acquired()? {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        let mut conn = self.connection()?;
        conn.del(self.name()).map_err(RudisError::Redis)
    }

    fn exists(&self) -> Result<bool, RudisError> {
        let mut conn = self.connection()?;
        conn.exists(self.name()).map_err(RudisError::Redis)
    }

    fn expire(&self, duration: Duration) -> Result<(), RudisError> {
        if !self.is_acquired()? {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        let mut conn = self.connection()?;
        conn.expire(self.name(), duration.as_secs() as i64)
            .map_err(RudisError::Redis)
    }
}

#[async_trait]
pub trait RObjectAsync<'a>: RObject<'a> {
    async fn async_connection(&self) -> Result<MultiplexedConnection, RudisError>;
    async fn lock_async(&'a self, duration: Duration) -> Result<RwLockGuard<'a>, RudisError>;

    async fn clear_async(&self) -> Result<(), RudisError> {
        if !self.is_acquired()? {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        let mut conn = self.async_connection().await?;
        conn.del(self.name()).await.map_err(RudisError::Redis)
    }

    async fn exists_async(&self) -> Result<bool, RudisError> {
        let mut conn = self.async_connection().await?;
        conn.exists(self.name()).await.map_err(RudisError::Redis)
    }

    async fn expire_async(&self, duration: Duration) -> Result<(), RudisError> {
        if !self.is_acquired()? {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        let mut conn = self.async_connection().await?;
        conn.expire(self.name(), duration.as_secs() as i64)
            .await
            .map_err(RudisError::Redis)
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RudisError {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    LockError(#[from] RedisLockError),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}
