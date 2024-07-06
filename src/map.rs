use std::marker::PhantomData;

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection, AsyncCommands, Commands, Connection, FromRedisValue, RedisError,
    ToRedisArgs,
};

use crate::{RObject, RObjectAsync};

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
}

impl<'a, K, V> RMap<'a, K, V>
where
    K: ToRedisArgs + FromRedisValue + Sync + Send + 'a,
    V: ToRedisArgs + FromRedisValue + Sync + Send + 'a,
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
}

impl<'a, K, V> RObject<'a> for RMap<'a, K, V> {
    fn name(&self) -> &'a str {
        self.name
    }

    fn connection(&self) -> Result<Connection, RedisError> {
        self.client.get_connection()
    }
}

#[async_trait]
impl<'a, K, V> RObjectAsync<'a> for RMap<'a, K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    fn name(&self) -> &'a str {
        self.name
    }

    async fn async_connection(&self) -> Result<MultiplexedConnection, RedisError> {
        self.client.get_multiplexed_async_connection().await
    }
}
