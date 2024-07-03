use std::{marker::PhantomData, time::Duration};

use redis::{AsyncCommands, Commands, FromRedisValue, RedisError, ToRedisArgs};

pub struct RMap<'a, K, V> {
    name: &'a str,
    client: &'a redis::Client,
    key_phantom: PhantomData<K>,
    value_phantom: PhantomData<V>,
}

impl<'a, K, V> RMap<'a, K, V> {
    pub(crate) fn new(name: &'a str, client: &'a redis::Client) -> Self {
        RMap {
            name,
            client,
            key_phantom: PhantomData,
            value_phantom: PhantomData,
        }
    }
}

impl<'a, K, V> RMap<'a, K, V>
where
    K: ToRedisArgs + FromRedisValue,
    V: ToRedisArgs + FromRedisValue,
{
    pub fn get(&self, key: K) -> Result<Option<V>, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.hget(self.name, key)
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.hset(self.name, key, value)
    }

    pub fn contains(&self, key: K) -> Result<bool, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.hexists(self.name, key)
    }

    pub fn values(&self) -> Result<Vec<V>, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.hvals(self.name)
    }

    pub fn expire(&self, duration: Duration) -> Result<(), RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.expire(self.name, duration.as_secs() as i64)
    }

    pub fn exists(&self) -> Result<bool, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.exists(self.name)
    }
}

impl<'a, K, V> RMap<'a, K, V>
where
    K: ToRedisArgs + FromRedisValue + Sync + Send,
    V: ToRedisArgs + FromRedisValue + Sync + Send,
{
    pub async fn get_async(&self, key: K) -> Result<Option<V>, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.hget(self.name, key).await
    }

    pub async fn insert_async(&self, key: K, value: V) -> Result<(), RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.hset(self.name, key, value).await
    }

    pub async fn contains_async(&self, key: K) -> Result<bool, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.hexists(self.name, key).await
    }

    pub async fn values_async(&self) -> Result<Vec<V>, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.hvals(self.name).await
    }

    pub async fn expire_async(&self, duration: Duration) -> Result<(), RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.expire(self.name, duration.as_secs() as i64).await
    }

    pub async fn exists_async(&self) -> Result<bool, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.exists(self.name).await
    }
}
