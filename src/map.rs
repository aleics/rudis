use std::{marker::PhantomData, time::Duration};

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection, AsyncCommands, Commands, Connection, FromRedisValue, ToRedisArgs,
};

use crate::{
    lock::{RedisLockError, RwLock, RwLockGuard},
    RObject, RObjectAsync, RudisError,
};

pub struct RMap<'a, K, V> {
    name: &'a str,
    client: &'a redis::Client,
    lock: RwLock<'a>,
    key_phantom: PhantomData<K>,
    value_phantom: PhantomData<V>,
}

impl<'a, K, V> RMap<'a, K, V> {
    pub(crate) fn new(name: &'a str, client: &'a redis::Client) -> Self {
        RMap {
            name,
            client,
            key_phantom: PhantomData,
            lock: RwLock::new(name, client),
            value_phantom: PhantomData,
        }
    }
}

impl<'a, K, V> RMap<'a, K, V>
where
    K: ToRedisArgs + FromRedisValue,
    V: ToRedisArgs + FromRedisValue,
{
    pub fn get(&self, key: K) -> Result<Option<V>, RudisError> {
        let mut conn = self.client.get_connection()?;
        conn.hget(self.name, key).map_err(RudisError::Redis)
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), RudisError> {
        if !self.is_acquired()? {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        let mut conn = self.client.get_connection()?;
        conn.hset(self.name, key, value).map_err(RudisError::Redis)
    }

    pub fn contains(&self, key: K) -> Result<bool, RudisError> {
        let mut conn = self.client.get_connection()?;
        conn.hexists(self.name, key).map_err(RudisError::Redis)
    }

    pub fn values(&self) -> Result<Vec<V>, RudisError> {
        let mut conn = self.client.get_connection()?;
        conn.hvals(self.name).map_err(RudisError::Redis)
    }
}

impl<'a, K, V> RMap<'a, K, V>
where
    K: ToRedisArgs + FromRedisValue + Sync + Send + 'a,
    V: ToRedisArgs + FromRedisValue + Sync + Send + 'a,
{
    pub async fn get_async(&self, key: K) -> Result<Option<V>, RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.hget(self.name, key).await.map_err(RudisError::Redis)
    }

    pub async fn insert_async(&self, key: K, value: V) -> Result<(), RudisError> {
        if !self.is_acquired()? {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.hset(self.name, key, value)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn contains_async(&self, key: K) -> Result<bool, RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.hexists(self.name, key)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn values_async(&self) -> Result<Vec<V>, RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.hvals(self.name).await.map_err(RudisError::Redis)
    }
}

impl<'a, K, V> RObject<'a> for RMap<'a, K, V> {
    fn name(&self) -> &'a str {
        self.name
    }

    fn connection(&self) -> Result<Connection, RudisError> {
        self.client.get_connection().map_err(RudisError::Redis)
    }

    fn lock(&'a self, duration: Duration) -> Result<RwLockGuard<'a>, RudisError> {
        self.lock.lock(duration).map_err(RudisError::LockError)
    }

    fn is_acquired(&self) -> Result<bool, RudisError> {
        self.lock.is_acquired().map_err(RudisError::LockError)
    }
}

#[async_trait]
impl<'a, K, V> RObjectAsync<'a> for RMap<'a, K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    async fn async_connection(&self) -> Result<MultiplexedConnection, RudisError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(RudisError::Redis)
    }

    async fn lock_async(&'a self, duration: Duration) -> Result<RwLockGuard<'a>, RudisError> {
        self.lock
            .lock_async(duration)
            .await
            .map_err(RudisError::LockError)
    }
}
