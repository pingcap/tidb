// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native Rust equivalent of `pkg/util/zeropool/pool.go`.
//!
//! Rust moves `T` directly into and out of the pool, so it does not need Go's
//! secondary pointer pool to avoid interface boxing. A poisoned Rust mutex is
//! recovered instead of becoming a new failure mode; Go mutexes do not poison.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

const SHARDS: usize = 16;
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static LOCAL_SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
}

// Keep independent locks off the same cache line.
#[repr(align(128))]
struct Shard<T>(T);

/// Cache-aligned shards with a stable shard assignment for each thread.
pub struct ThreadShards<T>(Box<[Shard<T>; SHARDS]>);

impl<T> ThreadShards<T> {
    /// Constructs each shard from the supplied factory.
    pub fn new(mut make: impl FnMut() -> T) -> Self {
        Self(Box::new(std::array::from_fn(|_| Shard(make()))))
    }

    /// Returns the shard assigned to this thread.
    #[inline]
    pub fn local(&self) -> &T {
        &self.0[LOCAL_SHARD.with(|&index| index)].0
    }

    /// Returns the shards in order for operations that need to visit all of them.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &T> {
        self.0.iter().map(|shard| &shard.0)
    }

    fn from_local(&self) -> impl Iterator<Item = &T> {
        let home = LOCAL_SHARD.with(|&index| index);
        self.0[home..]
            .iter()
            .chain(&self.0[..home])
            .map(|shard| &shard.0)
    }
}

type Factory<T> = dyn Fn() -> T + Send + Sync;

enum ItemSource<T> {
    Zero(fn() -> T),
    Factory(Box<Factory<T>>),
}

impl<T> ItemSource<T> {
    fn create(&self) -> T {
        match self {
            Self::Zero(zero) => zero(),
            Self::Factory(factory) => factory(),
        }
    }
}

/// Concurrent pool of reusable values.
///
/// The zero value is valid and produces `T::default()` while the pool is
/// empty, matching Go's generic zero value.
pub struct Pool<T> {
    items: ThreadShards<PoolShard<T>>,
    source: ItemSource<T>,
}

struct PoolShard<T> {
    values: Mutex<Vec<T>>,
    // Updated under the values lock. An empty shard needs no lock on a read.
    available: AtomicBool,
}

impl<T> PoolShard<T> {
    fn new() -> Self {
        Self {
            values: Mutex::new(Vec::new()),
            available: AtomicBool::new(false),
        }
    }
}

impl<T: Default> Default for Pool<T> {
    fn default() -> Self {
        Self {
            items: ThreadShards::new(PoolShard::new),
            source: ItemSource::Zero(T::default),
        }
    }
}

impl<T> Pool<T> {
    /// Creates a pool that calls `factory` whenever no pooled value exists.
    pub fn new(factory: impl Fn() -> T + Send + Sync + 'static) -> Self {
        Self {
            items: ThreadShards::new(PoolShard::new),
            source: ItemSource::Factory(Box::new(factory)),
        }
    }

    /// Gets a pooled value or creates its source-compatible replacement.
    pub fn get(&self) -> T {
        for shard in self.items.from_local() {
            if !shard.available.load(Ordering::Acquire) {
                continue;
            }
            let mut values = shard
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(item) = values.pop() {
                if values.is_empty() {
                    shard.available.store(false, Ordering::Release);
                }
                return item;
            }
        }
        self.source.create()
    }

    /// Returns a value to the pool.
    pub fn put(&self, item: T) {
        let shard = self.items.local();
        let mut values = shard
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        values.push(item);
        if values.len() == 1 {
            shard.available.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Pool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    const ITERATIONS: usize = 1_000_000;
    const CONCURRENCY: usize = u8::MAX as usize;

    #[test]
    #[allow(non_snake_case)]
    fn TestPool() {
        provides_correct_values();
        is_not_racy();
        does_not_allocate_after_warmup();
        zero_value_is_valid();
    }

    #[test]
    fn reuses_an_item_returned_by_another_thread() {
        let pool = &Pool::new(|| -1);
        thread::scope(|scope| {
            for expected in [42, 43] {
                assert_eq!(pool.get(), -1);
                scope.spawn(move || pool.put(expected)).join().unwrap();
                assert_eq!(pool.get(), expected);
            }
        });
    }

    fn provides_correct_values() {
        let pool = Pool::new(|| vec![0_u8; 1024]);
        let item_1 = pool.get();
        assert_eq!(item_1.len(), 1024);
        let item_2 = pool.get();
        assert_eq!(item_2.len(), 1024);
        pool.put(item_1);
        pool.put(item_2);
        assert_eq!(pool.get().len(), 1024);
        assert_eq!(pool.get().len(), 1024);
    }

    fn is_not_racy() {
        let pool = Arc::new(Pool::new(|| vec![0_u8; 1024]));
        let next = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(CONCURRENCY));
        let workers = (0..CONCURRENCY)
            .map(|worker| {
                let pool = Arc::clone(&pool);
                let next = Arc::clone(&next);
                let completed = Arc::clone(&completed);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    loop {
                        let iteration = next.fetch_add(1, Ordering::Relaxed);
                        if iteration >= ITERATIONS {
                            break;
                        }
                        let mut item = pool.get();
                        item[0] = worker as u8;
                        completed.fetch_add(1, Ordering::Relaxed);
                        assert_eq!(item[0], worker as u8);
                        pool.put(item);
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("pool worker");
        }
        assert_eq!(completed.load(Ordering::Relaxed), ITERATIONS);
    }

    fn does_not_allocate_after_warmup() {
        let pool = Pool::new(|| vec![0_u8; 1024]);
        let item = pool.get();
        let allocation = item.as_ptr();
        pool.put(item);
        for _ in 0..1_000 {
            let item = pool.get();
            assert_eq!(item.as_ptr(), allocation);
            pool.put(item);
        }
    }

    fn zero_value_is_valid() {
        let pool = Pool::<Vec<u8>>::default();
        let item = pool.get();
        assert!(item.is_empty());
        pool.put(item);
        for _ in 0..1_000 {
            let item = pool.get();
            assert!(item.is_empty());
            pool.put(item);
        }
    }
}
