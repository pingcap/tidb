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

//! Lockdown owner for `pkg/util/zeropool/pool.go`.
//!
//! `zeropool.inventory.tsv` classifies every declaration, function, branch,
//! and rule in that Go file. The source fingerprint, inventory fingerprint,
//! and Rust symbol gate below make unreviewed source or inventory drift fail.
//!
//! Rust moves `T` directly into and out of the pool, so it does not need Go's
//! secondary pointer pool to avoid interface boxing. The inventory explicitly
//! declines `sync.Pool`'s GC eviction, Go's nullable factory, and Go's universal
//! language zero value. A poisoned Rust mutex is recovered instead of becoming
//! a new failure mode; Go mutexes do not poison.

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
    use sha2::{Digest, Sha256};
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt::Write as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    const GO_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/zeropool/pool.go");
    const LOCKDOWN_INVENTORY: &str = include_str!("zeropool.inventory.tsv");
    const EXPECTED_INVENTORY_SHA256: &str =
        "86d36b5d6a27a1b37459fd1386f5dfeb7b1590eacda2e67ad949d6f32d246adb";
    const EXPECTED_ITEMS: [(&str, (&str, &str)); 30] = [
        ("D01", ("PORTED", "Pool")),
        ("D02", ("PORTED", "Pool::items")),
        ("D03", ("DECLINED", "-")),
        ("R01", ("PORTED", "Pool::default")),
        ("R02", ("DECLINED", "-")),
        ("R03", ("PORTED", "Pool")),
        ("F01", ("PORTED", "Pool::new")),
        ("R04", ("PORTED", "Pool::new")),
        ("R05", ("PORTED", "Pool::factory")),
        ("R06", ("PORTED", "Pool::get")),
        ("R07", ("DECLINED", "-")),
        ("R08", ("DECLINED", "-")),
        ("F02", ("PORTED", "Pool::get")),
        ("R09", ("PORTED", "Pool::get")),
        ("B01", ("PORTED", "Pool::get")),
        ("R10", ("PORTED", "Pool::get")),
        ("B02", ("UNREACHABLE", "-")),
        ("R11", ("PORTED", "Pool::get")),
        ("R12", ("PORTED", "Pool::get")),
        ("R13", ("DECLINED", "-")),
        ("R14", ("PORTED", "Pool::get")),
        ("F03", ("PORTED", "Pool::put")),
        ("B03", ("DECLINED", "-")),
        ("B04", ("DECLINED", "-")),
        ("R15", ("PORTED", "Pool::put")),
        ("R16", ("PORTED", "Pool::put")),
        ("R17", ("PORTED", "Pool")),
        ("R18", ("DECLINED", "-")),
        ("R19", ("PORTED", "Pool")),
        ("R20", ("PORTED", "Pool::items")),
    ];

    trait AmbiguousIfClone<A> {
        fn marker() {}
    }

    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: ?Sized + Clone> AmbiguousIfClone<u8> for T {}

    const ITERATIONS: usize = 1_000_000;
    const CONCURRENCY: usize = u8::MAX as usize;

    #[test]
    fn lockdown_inventory_matches_go_source_and_rust_symbols() {
        let recorded_hash = LOCKDOWN_INVENTORY
            .lines()
            .find_map(|line| line.strip_prefix("# source-sha256\t"))
            .expect("inventory records the owning Go source SHA-256");
        assert_eq!(recorded_hash, sha256_hex(GO_SOURCE), "Go source drifted");
        assert_eq!(
            sha256_hex(LOCKDOWN_INVENTORY.as_bytes()),
            EXPECTED_INVENTORY_SHA256,
            "lockdown inventory drifted"
        );

        let mut lines = LOCKDOWN_INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some("id\tcategory\tgo_item\tstatus\trust_symbol\tevidence")
        );

        let allowed_statuses = BTreeSet::from(["PORTED", "DECLINED", "UNREACHABLE"]);
        let mut actual = BTreeMap::new();
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 6, "invalid inventory row: {line}");
            assert!(
                allowed_statuses.contains(columns[3]),
                "unclassified inventory row: {line}"
            );
            assert!(
                !columns[5].is_empty(),
                "inventory evidence is required: {line}"
            );
            assert!(
                actual
                    .insert(columns[0], (columns[3], columns[4]))
                    .is_none(),
                "duplicate inventory id: {}",
                columns[0]
            );
        }
        assert_eq!(actual, BTreeMap::from(EXPECTED_ITEMS));

        let _: Pool<Vec<u8>> = Pool::default();
        let _: fn() -> Pool<Vec<u8>> = Pool::default;
        let _: fn(&Pool<Vec<u8>>) -> Vec<u8> = Pool::get;
        let _: fn(&Pool<Vec<u8>>, Vec<u8>) = Pool::put;
        let Pool {
            items: _,
            factory: _,
        } = Pool::<Vec<u8>>::default();
        let pool = Pool::new(Vec::<u8>::new);
        let _: Vec<u8> = pool.get();
        pool.put(Vec::new());
    }

    #[test]
    fn source_factory_and_zero_value_boundaries_are_exact() {
        let zero = Pool::<usize>::default();
        assert_eq!(zero.get(), 0);
        zero.put(7);
        assert_eq!(zero.get(), 7);
        assert_eq!(zero.get(), 0);

        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::clone(&calls);
        let pool = Pool::new(move || factory_calls.fetch_add(1, Ordering::SeqCst) + 1);
        assert_eq!(pool.get(), 1);
        assert_eq!(pool.get(), 2);
        pool.put(99);
        assert_eq!(pool.get(), 99);
        assert_eq!(pool.get(), 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn get_moves_value_without_retaining_a_duplicate() {
        struct DropValue(Arc<AtomicUsize>);

        impl Default for DropValue {
            fn default() -> Self {
                Self(Arc::new(AtomicUsize::new(0)))
            }
        }

        impl Drop for DropValue {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let factory_drops = Arc::clone(&drops);
        let pool = Pool::new(move || DropValue(Arc::clone(&factory_drops)));
        let item = pool.get();
        pool.put(item);
        let item = pool.get();
        drop(pool);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(item);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pool_cannot_be_copied_after_use() {
        let _ = <Pool<Vec<u8>> as AmbiguousIfClone<_>>::marker;
    }

    #[test]
    fn poisoned_mutex_does_not_add_a_failure_mode() {
        let pool = Arc::new(Pool::<usize>::default());
        let poisoning_pool = Arc::clone(&pool);
        assert!(thread::spawn(move || {
            let _guard = poisoning_pool.items.lock().expect("unpoisoned mutex");
            panic!("poison the pool mutex");
        })
        .join()
        .is_err());
        assert!(pool.items.is_poisoned());

        pool.put(7);
        assert_eq!(pool.get(), 7);
    }

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

    fn sha256_hex(input: &[u8]) -> String {
        Sha256::digest(input)
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                write!(output, "{byte:02x}").expect("write to String");
                output
            })
    }
}
