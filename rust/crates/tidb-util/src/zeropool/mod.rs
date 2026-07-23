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

//! Type-safe reusable value pool transcreated from `pkg/util/zeropool`.
//!
//! Rust moves `T` directly into and out of the pool, so it does not need Go's
//! secondary pointer pool to avoid interface boxing. A poisoned Rust mutex is
//! recovered instead of becoming a new failure mode; Go mutexes do not poison.

use std::sync::{Arc, Mutex, MutexGuard};

type Factory<T> = dyn Fn() -> T + Send + Sync;

/// Concurrent pool of reusable values.
///
/// The zero value is valid and produces `T::default()` while the pool is
/// empty, matching Go's generic zero value.
pub struct Pool<T> {
    items: Mutex<Vec<T>>,
    factory: Option<Arc<Factory<T>>>,
}

impl<T> Default for Pool<T> {
    fn default() -> Self {
        Self {
            items: Mutex::new(Vec::new()),
            factory: None,
        }
    }
}

impl<T> Pool<T>
where
    T: Default,
{
    /// Creates a pool that calls `factory` whenever no pooled value exists.
    #[must_use]
    pub fn new(factory: impl Fn() -> T + Send + Sync + 'static) -> Self {
        Self {
            items: Mutex::new(Vec::new()),
            factory: Some(Arc::new(factory)),
        }
    }

    /// Gets a pooled value or creates its source-compatible replacement.
    pub fn get(&self) -> T {
        if let Some(item) = self.items().pop() {
            return item;
        }
        self.factory.as_ref().map_or_else(T::default, |new| new())
    }

    /// Returns a value to the pool.
    pub fn put(&self, item: T) {
        self.items().push(item);
    }

    fn items(&self) -> MutexGuard<'_, Vec<T>> {
        self.items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
