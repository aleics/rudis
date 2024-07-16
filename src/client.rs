use redis::{ConnectionLike, IntoConnectionInfo};

use crate::{list::RList, map::RMap, set::RSet, RudisError};

pub struct RedisClient {
    inner: redis::Client,
}

impl RedisClient {
    pub fn create<T: IntoConnectionInfo>(addr: T) -> Result<RedisClient, RudisError> {
        let inner = redis::Client::open(addr)?;
        Ok(RedisClient { inner })
    }

    pub fn is_connected(&self) -> Result<bool, RudisError> {
        let mut conn = self.inner.get_connection()?;
        Ok(conn.check_connection())
    }

    pub fn get_list<'a, T>(&'a self, key: &'a str) -> RList<'a, T> {
        RList::new(key, &self.inner)
    }

    pub fn get_map<'a, K, V>(&'a self, key: &'a str) -> RMap<'a, K, V> {
        RMap::new(key, &self.inner)
    }

    pub fn get_set<'a, T>(&'a self, key: &'a str) -> RSet<'a, T> {
        RSet::new(key, &self.inner)
    }
}
