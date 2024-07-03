use std::{collections::HashSet, hash::Hash, marker::PhantomData};

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection, AsyncCommands, Commands, Connection, FromRedisValue, RedisError,
    ToRedisArgs,
};

use crate::{RObject, RObjectAsync};

pub struct RSet<'a, T> {
    name: &'a str,
    client: &'a redis::Client,
    phantom: PhantomData<T>,
}

impl<'a, T> RSet<'a, T> {
    pub(crate) fn new(name: &'a str, client: &'a redis::Client) -> Self {
        RSet {
            name,
            client,
            phantom: PhantomData,
        }
    }
}

impl<'a, T> RSet<'a, T>
where
    T: ToRedisArgs + FromRedisValue + Hash + Eq,
{
    pub fn read_all(&self) -> Result<HashSet<T>, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.smembers(self.name)
    }

    pub fn add(&self, value: T) -> Result<(), RedisError> {
        self.add_all(&[value])
    }

    pub fn add_all(&self, values: &[T]) -> Result<(), RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.sadd(self.name, values)
    }

    pub fn contains(&self, value: &T) -> Result<bool, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.sismember(self.name, value)
    }

    pub fn remove(&self, value: &T) -> Result<(), RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.srem(self.name, value)
    }

    pub fn size(&self) -> Result<usize, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.scard(self.name)
    }
}

impl<'a, T> RSet<'a, T>
where
    T: ToRedisArgs + FromRedisValue + Sync + Send + Hash + Eq,
{
    pub async fn read_all_async(&self) -> Result<HashSet<T>, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.smembers(self.name).await
    }

    pub async fn add_async(&self, value: T) -> Result<(), RedisError> {
        self.add_all_async(&[value]).await
    }

    pub async fn add_all_async(&self, values: &[T]) -> Result<(), RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.sadd(self.name, values).await
    }

    pub async fn contains_async(&self, value: &T) -> Result<bool, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.sismember(self.name, value).await
    }

    pub async fn remove_async(&self, value: &T) -> Result<(), RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.srem(self.name, value).await
    }

    pub async fn size_async(&self) -> Result<usize, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.scard(self.name).await
    }
}

impl<'a, T> RObject<'a> for RSet<'a, T> {
    fn name(&self) -> &'a str {
        self.name
    }

    fn connection(&self) -> Result<Connection, RedisError> {
        self.client.get_connection()
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

    async fn connection(&self) -> Result<MultiplexedConnection, RedisError> {
        self.client.get_multiplexed_async_connection().await
    }
}
