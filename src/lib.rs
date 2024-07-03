use std::time::Duration;

use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, AsyncCommands, Commands, Connection, RedisError};

pub mod client;
pub mod list;
pub mod map;
pub mod set;

pub trait RObject<'a> {
    fn name(&self) -> &'a str;
    fn connection(&self) -> Result<Connection, RedisError>;

    fn clear(&self) -> Result<(), RedisError> {
        let mut conn = self.connection()?;
        conn.del(self.name())
    }

    fn exists(&self) -> Result<bool, RedisError> {
        let mut conn = self.connection()?;
        conn.exists(self.name())
    }

    fn expire(&self, duration: Duration) -> Result<(), RedisError> {
        let mut conn = self.connection()?;
        conn.expire(self.name(), duration.as_secs() as i64)
    }
}

#[async_trait]
pub trait RObjectAsync<'a> {
    fn name(&self) -> &'a str;

    async fn connection(&self) -> Result<MultiplexedConnection, RedisError>;

    async fn clear_async(&self) -> Result<(), RedisError> {
        let mut conn = self.connection().await?;
        conn.del(self.name()).await
    }

    async fn exists_async(&self) -> Result<bool, RedisError> {
        let mut conn = self.connection().await?;
        conn.exists(self.name()).await
    }

    async fn expire_async(&self, duration: Duration) -> Result<(), RedisError> {
        let mut conn = self.connection().await?;
        conn.expire(self.name(), duration.as_secs() as i64).await
    }
}
