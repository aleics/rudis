use std::marker::PhantomData;

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection, AsyncCommands, Commands, Connection, FromRedisValue, RedisError,
    ToRedisArgs,
};

use crate::{RObject, RObjectAsync};

pub struct RListIter<'a, T> {
    current: usize,
    remaining: usize,
    list: &'a RList<'a, T>,
}

impl<'a, T> RListIter<'a, T> {
    fn new(size: usize, list: &'a RList<'a, T>) -> Self {
        RListIter {
            current: 0,
            remaining: size,
            list,
        }
    }
}

impl<'a, T> Iterator for RListIter<'a, T>
where
    T: ToRedisArgs + FromRedisValue,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let next = self.list.get(self.current as isize).ok().flatten();
        self.current += 1;
        self.remaining -= 1;

        next
    }
}

pub struct RList<'a, T> {
    name: &'a str,
    client: &'a redis::Client,
    phantom: PhantomData<T>,
}

impl<'a, T> RList<'a, T> {
    pub(crate) fn new(name: &'a str, client: &'a redis::Client) -> Self {
        RList {
            name,
            client,
            phantom: PhantomData,
        }
    }
}

impl<'a, T> RList<'a, T>
where
    T: ToRedisArgs + FromRedisValue,
{
    pub fn get(&self, index: isize) -> Result<Option<T>, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.lindex(self.name, index)
    }

    pub fn read_all(&self) -> Result<Vec<T>, RedisError> {
        self.range(0, -1)
    }

    pub fn range(&self, start: isize, end: isize) -> Result<Vec<T>, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.lrange(self.name, start, end)
    }

    pub fn size(&self) -> Result<usize, RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.llen(self.name)
    }

    pub fn find_index(&self, value: T) -> Result<Option<usize>, RedisError> {
        let options = redis::LposOptions::default().count(1);

        let mut conn = self.client.get_connection()?;
        let indices: Vec<usize> = conn.lpos(self.name, value, options)?;

        Ok(indices.first().copied())
    }

    pub fn push(&self, value: T) -> Result<(), RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.rpush(self.name, value)
    }

    pub fn set(&self, index: isize, value: T) -> Result<(), RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.lset(self.name, index, value)
    }

    pub fn remove(&self, value: T) -> Result<(), RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.lrem(self.name, 1, value)
    }

    pub fn trim(&self, start: isize, end: isize) -> Result<(), RedisError> {
        let mut conn = self.client.get_connection()?;
        conn.ltrim(self.name, start, end)
    }

    pub fn iter(&'a self) -> Result<RListIter<'a, T>, RedisError> {
        let size = self.size()?;
        Ok(RListIter::new(size, self))
    }
}

impl<'a, T> RList<'a, T>
where
    T: ToRedisArgs + FromRedisValue + Sync + Send,
{
    pub async fn get_async(&self, index: isize) -> Result<Option<T>, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.lindex(self.name, index).await
    }

    pub async fn read_all_async(&self) -> Result<Vec<T>, RedisError> {
        self.range_async(0, -1).await
    }

    pub async fn range_async(&self, start: isize, end: isize) -> Result<Vec<T>, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.lrange(self.name, start, end).await
    }

    pub async fn size_async(&self) -> Result<usize, RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.llen(self.name).await
    }

    pub async fn find_index_async(&self, value: T) -> Result<Option<usize>, RedisError> {
        let options = redis::LposOptions::default().count(1);

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let indices: Vec<usize> = conn.lpos(self.name, value, options).await?;

        Ok(indices.first().copied())
    }

    pub async fn push_async(&self, value: T) -> Result<(), RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.rpush(self.name, value).await
    }

    pub async fn set_async(&self, index: isize, value: T) -> Result<(), RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.lset(self.name, index, value).await
    }

    pub async fn remove_async(&self, value: T) -> Result<(), RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.lrem(self.name, 1, value).await
    }

    pub async fn trim_async(&self, start: isize, end: isize) -> Result<(), RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.ltrim(self.name, start, end).await
    }
}

impl<'a, T> RObject<'a> for RList<'a, T> {
    fn name(&self) -> &'a str {
        self.name
    }

    fn connection(&self) -> Result<Connection, RedisError> {
        self.client.get_connection()
    }
}

#[async_trait]
impl<'a, T> RObjectAsync<'a> for RList<'a, T>
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
