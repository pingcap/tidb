// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pool interfaces and sharded registry from `pkg/resourcemanager/util`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

const SHARD_COUNT: usize = 8;

/// Minimum interval between two scheduling decisions.
pub static MIN_SCHEDULER_INTERVAL_NANOS: AtomicI64 = AtomicI64::new(200_000_000);

/// Maximum concurrency increase over a pool's original size.
pub static MAX_OVERCLOCK_COUNT: AtomicI32 = AtomicI32::new(1);

/// A tunable goroutine-pool equivalent.
pub trait GoroutinePool: Send + Sync {
    /// Releases the pool and waits for its workers.
    fn release_and_wait(&self);
    /// Changes the pool concurrency.
    fn tune(&self, size: i32);
    /// Returns the last tuning time.
    fn last_tuner_ts(&self) -> SystemTime;
    /// Returns the configured concurrency.
    fn cap(&self) -> i32;
    /// Returns running worker count.
    fn running(&self) -> i32;
    /// Returns the pool name.
    fn name(&self) -> String;
    /// Returns the original concurrency.
    fn origin_concurrency(&self) -> i32;
}

/// A registered pool and its component.
pub struct PoolContainer {
    /// Tunable pool.
    pub pool: Arc<dyn GoroutinePool>,
    /// Pool component.
    pub component: Component,
}

/// Resource-manager component identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum Component {
    /// Unknown test component.
    Unknown = 0,
    /// DDL component.
    Ddl = 1,
    /// Distributed-task component.
    DistTask = 2,
    /// Admin check-table component.
    CheckTable = 3,
    /// IMPORT INTO component.
    ImportInto = 4,
}

/// Eight-shard concurrent pool registry.
pub struct ShardPoolMap {
    pools: [RwLock<HashMap<String, Arc<PoolContainer>>>; SHARD_COUNT],
}

impl ShardPoolMap {
    /// Creates an empty pool registry.
    pub fn new() -> Self {
        Self {
            pools: std::array::from_fn(|_| RwLock::new(HashMap::new())),
        }
    }

    /// Adds a named pool.
    pub fn add(&self, key: String, pool: Arc<PoolContainer>) -> Result<(), &'static str> {
        let mut pools = self.pools[hash(&key)]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pools.contains_key(&key) && !tidb_util::intest::IN_TEST {
            return Err("pool is already exist");
        }
        pools.insert(key, pool);
        Ok(())
    }

    /// Deletes a named pool.
    pub fn delete(&self, key: &str) {
        self.pools[hash(key)]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }

    /// Visits every pool while retaining each source shard's read lock.
    pub fn iter(&self, mut visit: impl FnMut(&Arc<PoolContainer>)) {
        for pools in &self.pools {
            for pool in pools
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
            {
                visit(pool);
            }
        }
    }
}

fn hash(key: &str) -> usize {
    key.as_bytes()[0] as usize % SHARD_COUNT
}

/// Source test pool implementation.
pub struct MockGPool {
    name: String,
    concurrency: AtomicI32,
    origin_concurrency: i32,
}

impl MockGPool {
    /// Creates a mock pool.
    pub fn new(name: impl Into<String>, concurrency: i32) -> Self {
        Self {
            name: name.into(),
            concurrency: AtomicI32::new(concurrency),
            origin_concurrency: concurrency,
        }
    }

    /// Source mock method outside the pool interface.
    pub fn max_in_flight(&self) -> i64 {
        panic!("implement me")
    }

    /// Source mock method outside the pool interface.
    pub fn in_flight(&self) -> i64 {
        panic!("implement me")
    }

    /// Source mock method outside the pool interface.
    pub fn min_rt(&self) -> u64 {
        panic!("implement me")
    }

    /// Source mock method outside the pool interface.
    pub fn max_pass(&self) -> u64 {
        panic!("implement me")
    }

    /// Source mock method outside the pool interface.
    pub fn long_rtt(&self) -> f64 {
        panic!("implement me")
    }

    /// Source mock method outside the pool interface.
    pub fn update_long_rtt(&self, _update: impl FnOnce(f64) -> f64) {
        panic!("implement me")
    }

    /// Source mock method outside the pool interface.
    pub fn short_rtt(&self) -> u64 {
        panic!("implement me")
    }

    /// Source mock method outside the pool interface.
    pub fn queue_size(&self) -> i64 {
        panic!("implement me")
    }
}

impl GoroutinePool for MockGPool {
    fn release_and_wait(&self) {
        panic!("implement me")
    }

    fn tune(&self, size: i32) {
        self.concurrency.store(size, Ordering::Relaxed);
    }

    fn last_tuner_ts(&self) -> SystemTime {
        SystemTime::now() - Duration::from_secs(10)
    }

    fn cap(&self) -> i32 {
        self.concurrency.load(Ordering::Relaxed)
    }

    fn running(&self) -> i32 {
        panic!("implement me")
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn origin_concurrency(&self) -> i32 {
        self.origin_concurrency
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_pool_map() {
        let pools = ShardPoolMap::new();
        for index in 0..10 {
            let id = index.to_string();
            pools
                .add(
                    id.clone(),
                    Arc::new(PoolContainer {
                        pool: Arc::new(MockGPool::new(id, 10)),
                        component: Component::Ddl,
                    }),
                )
                .unwrap();
        }
        let mut count = 0;
        pools.iter(|_| count += 1);
        assert_eq!(10, count);

        for index in 0..10 {
            pools.delete(&index.to_string());
        }
        count = 0;
        pools.iter(|_| count += 1);
        assert_eq!(0, count);
        pools.delete("0");
    }
}
