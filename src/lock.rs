use std::time::Duration;

use rand::{thread_rng, RngCore};
use redis::{aio::ConnectionLike as AsyncConnectionLike, cmd, Commands, ConnectionLike};
use thiserror::Error;

const UNLOCK_SCRIPT: &str = r#"
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    else
        return 0
    end
"#;

pub struct RLockGuard<'a> {
    inner: &'a RLock<'a>,
}

impl<'a> RLockGuard<'a> {
    pub(crate) fn new(inner: &'a RLock<'a>) -> Self {
        RLockGuard { inner }
    }

    pub fn unlock(&self) -> Result<(), RedisLockError> {
        self.inner.unlock()
    }

    pub async fn unlock_async(&self) -> Result<(), RedisLockError> {
        self.inner.unlock_async().await
    }
}

impl<'a> Drop for RLockGuard<'a> {
    fn drop(&mut self) {
        #[allow(unused_variables)]
        // In case the user did not unlock manually, we'll try to unlock after drop.
        self.unlock().unwrap_or_else(|err| match err {
            RedisLockError::UnlockMismatchError => (), // it's not there anymore, nothing to do
            _ => panic!("Something went wrong when unlocking lock after guard drop"),
        });
    }
}

#[derive(Debug, Clone)]
pub struct RLock<'a> {
    name: String,
    value: Vec<u8>,
    client: &'a redis::Client,
}

impl<'a> RLock<'a> {
    pub(crate) fn new(name: &'a str, client: &'a redis::Client) -> Self {
        RLock {
            name: format!("{name}_lock"),
            value: generate_value(),
            client,
        }
    }

    pub(crate) fn is_locked(&self) -> Result<bool, RedisLockError> {
        let mut conn = self.client.get_connection()?;
        let exists = conn.exists(&self.name)?;

        Ok(exists)
    }

    pub(crate) fn lock(&'a self, duration: Duration) -> Result<RLockGuard<'a>, RedisLockError> {
        let mut conn = self.client.get_connection()?;

        let ttl: u64 = duration
            .as_millis()
            .try_into()
            .map_err(|_| RedisLockError::TooLargeTTL)?;

        let result = conn.req_command(
            cmd("SET")
                .arg(&self.name)
                .arg(self.value.clone())
                .arg("NX")
                .arg("PX")
                .arg(ttl),
        )?;

        if result == redis::Value::Okay {
            Ok(RLockGuard::new(self))
        } else {
            Err(RedisLockError::AcquireLockError)
        }
    }

    pub(crate) fn unlock(&'a self) -> Result<(), RedisLockError> {
        let mut conn = self.client.get_connection()?;
        let script = redis::Script::new(UNLOCK_SCRIPT);

        let result: i8 = script
            .key(&self.name)
            .arg(self.value.clone())
            .invoke(&mut conn)?;

        if result == 1 {
            Ok(())
        } else {
            Err(RedisLockError::UnlockMismatchError)
        }
    }

    pub(crate) async fn lock_async(
        &'a self,
        duration: Duration,
    ) -> Result<RLockGuard<'a>, RedisLockError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        let ttl: u64 = duration
            .as_millis()
            .try_into()
            .map_err(|_| RedisLockError::TooLargeTTL)?;

        let result = conn
            .req_packed_command(
                cmd("SET")
                    .arg(&self.name)
                    .arg(self.value.clone())
                    .arg("NX")
                    .arg("PX")
                    .arg(ttl),
            )
            .await?;

        if result == redis::Value::Okay {
            Ok(RLockGuard::new(self))
        } else {
            Err(RedisLockError::AcquireLockError)
        }
    }

    pub(crate) async fn unlock_async(&'a self) -> Result<(), RedisLockError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let script = redis::Script::new(UNLOCK_SCRIPT);
        let result: i8 = script
            .key(&self.name)
            .arg(self.value.clone())
            .invoke_async(&mut conn)
            .await?;
        if result == 1 {
            Ok(())
        } else {
            Err(RedisLockError::UnlockMismatchError)
        }
    }
}

fn generate_value() -> Vec<u8> {
    let mut buf = [0u8; 20];
    thread_rng().fill_bytes(&mut buf);
    buf.to_vec()
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RedisLockError {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error("lock could not be acquired")]
    AcquireLockError,
    #[error("entry is locked")]
    EntryIsLocked,
    #[error("lock could not be unlocked due to mismatching key")]
    UnlockMismatchError,
    #[error("TTL value for lock is too large")]
    TooLargeTTL,
    #[error("entry is locked")]
    IsLocked,
}
