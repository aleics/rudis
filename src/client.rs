use redis::{ConnectionLike, IntoConnectionInfo};

use crate::{list::RList, lock::RedisLockError, map::RMap, set::RSet, RudisError};

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

    pub fn get_list<'a, T>(&'a self, key: &'a str) -> Result<RList<'a, T>, RudisError> {
        let list = RList::new(key, &self.inner);

        let is_locked = list.lock.is_locked()?;
        if is_locked {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        Ok(list)
    }

    pub fn get_map<'a, K, V>(&'a self, key: &'a str) -> Result<RMap<'a, K, V>, RudisError> {
        let map = RMap::new(key, &self.inner);

        let is_locked = map.lock.is_locked()?;
        if is_locked {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        Ok(map)
    }

    pub fn get_set<'a, T>(&'a self, key: &'a str) -> Result<RSet<'a, T>, RudisError> {
        let set = RSet::new(key, &self.inner);

        let is_locked = set.lock.is_locked()?;
        if is_locked {
            return Err(RudisError::LockError(RedisLockError::EntryIsLocked));
        }

        Ok(set)
    }
}
