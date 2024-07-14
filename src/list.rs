use std::{marker::PhantomData, time::Duration};

use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use redis::{
    aio::MultiplexedConnection, AsyncCommands, Commands, Connection, FromRedisValue, ToRedisArgs,
};

use crate::{
    lock::{RLock, RLockGuard},
    RObject, RObjectAsync, RudisError,
};

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

#[derive(Clone, Debug)]
pub struct RList<'a, T> {
    name: &'a str,
    client: &'a redis::Client,
    pub(crate) lock: RLock<'a>,
    phantom: PhantomData<T>,
}

impl<'a, T> RList<'a, T> {
    pub(crate) fn new(name: &'a str, client: &'a redis::Client) -> Self {
        RList {
            name,
            client,
            lock: RLock::new(name, client),
            phantom: PhantomData,
        }
    }
}

impl<'a, T> RList<'a, T>
where
    T: ToRedisArgs + FromRedisValue,
{
    pub fn get(&self, index: isize) -> Result<Option<T>, RudisError> {
        let mut conn = self.connection()?;
        conn.lindex(self.name, index).map_err(RudisError::Redis)
    }

    pub fn read_all(&self) -> Result<Vec<T>, RudisError> {
        self.range(0, -1)
    }

    pub fn range(&self, start: isize, end: isize) -> Result<Vec<T>, RudisError> {
        let mut conn = self.connection()?;
        conn.lrange(self.name, start, end)
            .map_err(RudisError::Redis)
    }

    pub fn size(&self) -> Result<usize, RudisError> {
        let mut conn = self.connection()?;
        conn.llen(self.name).map_err(RudisError::Redis)
    }

    pub fn find_index(&self, value: T) -> Result<Option<usize>, RudisError> {
        let options = redis::LposOptions::default().count(1);

        let mut conn = self.connection()?;
        let indices: Vec<usize> = conn.lpos(self.name, value, options)?;

        Ok(indices.first().copied())
    }

    pub fn push(&self, value: T) -> Result<(), RudisError> {
        let mut conn = self.connection()?;
        conn.rpush(self.name, value).map_err(RudisError::Redis)
    }

    pub fn set(&self, index: isize, value: T) -> Result<(), RudisError> {
        let mut conn = self.connection()?;
        conn.lset(self.name, index, value)
            .map_err(RudisError::Redis)
    }

    pub fn remove(&self, value: T) -> Result<(), RudisError> {
        let mut conn = self.connection()?;
        conn.lrem(self.name, 1, value).map_err(RudisError::Redis)
    }

    pub fn trim(&self, start: isize, end: isize) -> Result<(), RudisError> {
        let mut conn = self.connection()?;
        conn.ltrim(self.name, start, end).map_err(RudisError::Redis)
    }

    pub fn iter(&'a self) -> Result<RListIter<'a, T>, RudisError> {
        let size = self.size()?;
        Ok(RListIter::new(size, self))
    }
}

impl<'a, T> RList<'a, T>
where
    T: ToRedisArgs + FromRedisValue + Sync + Send,
{
    pub async fn get_async(&self, index: isize) -> Result<Option<T>, RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.lindex(self.name, index)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn read_all_async(&self) -> Result<Vec<T>, RudisError> {
        self.range_async(0, -1).await
    }

    pub async fn range_async(&self, start: isize, end: isize) -> Result<Vec<T>, RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.lrange(self.name, start, end)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn size_async(&self) -> Result<usize, RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.llen(self.name).await.map_err(RudisError::Redis)
    }

    pub async fn find_index_async(&self, value: T) -> Result<Option<usize>, RudisError> {
        let options = redis::LposOptions::default().count(1);

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let indices: Vec<usize> = conn.lpos(self.name, value, options).await?;

        Ok(indices.first().copied())
    }

    pub async fn push_async(&self, value: T) -> Result<(), RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.rpush(self.name, value)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn set_async(&self, index: isize, value: T) -> Result<(), RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.lset(self.name, index, value)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn remove_async(&self, value: T) -> Result<(), RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.lrem(self.name, 1, value)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn trim_async(&self, start: isize, end: isize) -> Result<(), RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        conn.ltrim(self.name, start, end)
            .await
            .map_err(RudisError::Redis)
    }

    pub async fn stream(&'a self) -> Result<impl Stream<Item = T> + 'a, RudisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        let size = conn.llen(self.name).await?;

        let stream = stream::iter(0..size).filter_map(move |index| {
            let mut conn = conn.clone();

            async move {
                let result = conn.lindex(self.name, index).await;
                result.ok().flatten()
            }
        });

        Ok(stream)
    }
}

impl<'a, T> RObject<'a> for RList<'a, T> {
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
impl<'a, T> RObjectAsync<'a> for RList<'a, T>
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
