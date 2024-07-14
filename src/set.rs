use std::{collections::HashSet, hash::Hash, marker::PhantomData, time::Duration};

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection, AsyncCommands, Commands, Connection, FromRedisValue, ToRedisArgs,
};

use crate::{
    lock::{RLock, RLockGuard},
    RObject, RObjectAsync, RudisError,
};

pub struct RSet<'a, T> {
    name: &'a str,
    client: &'a redis::Client,
    pub(crate) lock: RLock<'a>,
    phantom: PhantomData<T>,
}

impl<'a, T> RSet<'a, T> {
    pub(crate) fn new(name: &'a str, client: &'a redis::Client) -> Self {
        RSet {
            name,
            client,
            lock: RLock::new(name, client),
            phantom: PhantomData,
        }
    }
}

impl<'a, T> RSet<'a, T>
where
    T: ToRedisArgs + FromRedisValue + Hash + Eq,
{
    pub fn read_all(&self) -> Result<HashSet<T>, RudisError> {
        let mut conn = self.connection()?;
        conn.smembers(self.name).map_err(RudisError::Redis)
    }

    pub fn add(&self, value: T) -> Result<(), RudisError> {
        self.add_all(&[value])
    }

    pub fn add_all(&self, values: &[T]) -> Result<(), RudisError> {
        let mut conn = self.connection()?;
        conn.sadd(self.name, values).map_err(RudisError::Redis)
    }

    pub fn contains(&self, value: &T) -> Result<bool, RudisError> {
        let mut conn = self.connection()?;
        conn.sismember(self.name, value).map_err(RudisError::Redis)
    }

    pub fn remove(&self, value: &T) -> Result<(), RudisError> {
        let mut conn = self.connection()?;
        conn.srem(self.name, value).map_err(RudisError::Redis)
    }

    pub fn size(&self) -> Result<usize, RudisError> {
        let mut conn = self.connection()?;
        conn.scard(self.name).map_err(RudisError::Redis)
    }
}

impl<'a, T> RSet<'a, T>
where
    T: ToRedisArgs + FromRedisValue + Sync + Send + Hash + Eq,
{
    pub async fn read_all_async(&self) -> Result<HashSet<T>, RudisError> {
        let mut conn = self.async_connection().await?;
        conn.smembers(self.name).await.map_err(RudisError::Redis)
    }

    pub async fn add_async(&self, value: T) -> Result<(), RudisError> {
        self.add_all_async(&[value]).await
    }

    pub async fn add_all_async(&self, values: &[T]) -> Result<(), RudisError> {
        let mut conn = self.async_connection().await?;
        conn.sadd(self.name, values)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn contains_async(&self, value: &T) -> Result<bool, RudisError> {
        let mut conn = self.async_connection().await?;
        conn.sismember(self.name, value)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn remove_async(&self, value: &T) -> Result<(), RudisError> {
        let mut conn = self.async_connection().await?;
        conn.srem(self.name, value).await.map_err(RudisError::Redis)
    }

    pub async fn size_async(&self) -> Result<usize, RudisError> {
        let mut conn = self.async_connection().await?;
        conn.scard(self.name).await.map_err(RudisError::Redis)
    }
}

impl<'a, T> RObject<'a> for RSet<'a, T> {
    fn name(&self) -> &'a str {
        self.name
    }

    fn connection(&self) -> Result<Connection, RudisError> {
        self.client.get_connection().map_err(RudisError::Redis)
    }

    fn lock(&'a self, duration: Duration) -> Result<RLockGuard<'a>, RudisError> {
        self.lock.lock(duration).map_err(RudisError::LockError)
    }
}

#[async_trait]
impl<'a, T> RObjectAsync<'a> for RSet<'a, T>
where
    T: Send + Sync,
{
    fn name(&self) -> &'a str {
        self.name
    }

    async fn async_connection(&self) -> Result<MultiplexedConnection, RudisError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(RudisError::Redis)
    }

    async fn lock_async(&'a self, duration: Duration) -> Result<RLockGuard<'a>, RudisError> {
        self.lock
            .lock_async(duration)
            .await
            .map_err(RudisError::LockError)
    }
}
