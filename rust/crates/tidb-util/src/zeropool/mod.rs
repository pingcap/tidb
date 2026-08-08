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

    const GO_BUILD: &[u8] = include_bytes!("../../../../../pkg/util/zeropool/BUILD.bazel");
    const GO_SOURCE: &[u8] = include_bytes!("../../../../../pkg/util/zeropool/pool.go");
    const GO_TEST: &[u8] = include_bytes!("../../../../../pkg/util/zeropool/pool_test.go");
    const ARTIFACT_MANIFEST: &str = include_str!("zeropool.artifacts.tsv");
    const LOCKDOWN_INVENTORY: &str = include_str!("zeropool.inventory.tsv");
    const SEMANTIC_DIVERGENCES: &str = include_str!("zeropool.semantic-divergences.tsv");
    const RUST_BENCH: &str = include_str!("../../benches/zeropool.rs");
    const EXPECTED_INVENTORY_SHA256: &str =
        "8594a10c478378fd77fe81ab40f2bf3c26f149aee741dc062d500aca4945cf74";
    const EXPECTED_SEMANTIC_SHA256: &str =
        "0d2d87c91709340a7ac4c31517fa1c6faf3b3f0a1b87af6269f9dab2f9bbc180";
    const ARTIFACTS: [(&str, &str, &[u8]); 3] = [
        ("pkg/util/zeropool/BUILD.bazel", "build", GO_BUILD),
        ("pkg/util/zeropool/pool.go", "production", GO_SOURCE),
        ("pkg/util/zeropool/pool_test.go", "test-benchmark", GO_TEST),
    ];
    const SYMBOL_EVIDENCE: &str =
        "rust-test:zeropool_lockdown_inventory_is_complete_and_symbols_compile";
    const EXPECTED_CATEGORIES: [(&str, usize); 12] = [
        ("benchmark", 4),
        ("branch", 4),
        ("closure", 1),
        ("declaration", 1),
        ("field", 2),
        ("function", 3),
        ("test", 1),
        ("test_assertion", 6),
        ("test_branch", 2),
        ("test_helper_closure", 14),
        ("test_loop", 14),
        ("test_row", 3),
    ];
    const EXPECTED_STATUSES: [(&str, usize); 2] = [("DECLINED", 3), ("PORTED", 52)];

    trait AmbiguousIfClone<A> {
        fn marker() {}
    }

    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}

    const ITERATIONS: usize = 1_000_000;
    const CONCURRENCY: usize = u8::MAX as usize;

    #[test]
    fn zeropool_lockdown_inventory_is_complete_and_symbols_compile() {
        let expected_manifest_prefix = [
            "# pkg-zeropool-artifacts-v1",
            "# zero\tbuild_tags\t0",
            "# zero\tplatform_variants\t0",
            "# zero\tcode_generated\t0",
            "# zero\tgo_generate\t0",
            "# zero\tgo_embed\t0",
            "# zero\ttracked_testdata\t0",
            "path\trole\tsha256",
        ];
        let mut manifest_lines = ARTIFACT_MANIFEST.lines();
        for expected in expected_manifest_prefix {
            assert_eq!(manifest_lines.next(), Some(expected));
        }
        let mut manifest = BTreeMap::new();
        for line in manifest_lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 3, "invalid artifact row: {line}");
            assert!(manifest
                .insert(columns[0], (columns[1], columns[2]))
                .is_none());
        }
        assert_eq!(manifest.len(), ARTIFACTS.len());
        for (path, role, bytes) in ARTIFACTS {
            assert_eq!(
                manifest.get(path),
                Some(&(role, sha256_hex(bytes).as_str()))
            );
        }

        assert_eq!(
            sha256_hex(LOCKDOWN_INVENTORY.as_bytes()),
            EXPECTED_INVENTORY_SHA256,
            "lockdown inventory drifted"
        );
        assert_eq!(
            sha256_hex(SEMANTIC_DIVERGENCES.as_bytes()),
            EXPECTED_SEMANTIC_SHA256,
            "semantic divergence evidence drifted"
        );

        let mut lines = LOCKDOWN_INVENTORY
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'));
        assert_eq!(
            lines.next(),
            Some(
                "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner\tstatus\trust_symbol\tevidence\tmutation_policy"
            )
        );

        let allowed_statuses = BTreeSet::from(["PORTED", "DECLINED", "UNREACHABLE"]);
        let mut ids = BTreeSet::new();
        let mut anchors = BTreeSet::new();
        let mut categories = BTreeMap::new();
        let mut statuses = BTreeMap::new();
        let benchmark_names = BTreeSet::from([
            "BenchmarkZeropoolPool",
            "BenchmarkSyncPoolValue",
            "BenchmarkSyncPoolNewPointer",
            "BenchmarkSyncPoolPointer",
        ]);
        for line in lines {
            let columns: Vec<_> = line.split('\t').collect();
            assert_eq!(columns.len(), 10, "invalid inventory row: {line}");
            assert!(
                allowed_statuses.contains(columns[6]),
                "unclassified inventory row: {line}"
            );
            assert!(
                !columns[8].is_empty(),
                "inventory evidence is required: {line}"
            );
            assert!(ids.insert(columns[0]), "duplicate inventory id: {line}");
            assert!(
                anchors.insert((columns[2], columns[3])),
                "duplicate source anchor: {line}"
            );
            *categories.entry(columns[1]).or_insert(0usize) += 1;
            *statuses.entry(columns[6]).or_insert(0usize) += 1;

            match (columns[2], columns[1], columns[5], columns[3]) {
                ("pkg/util/zeropool/pool.go", "declaration", "type:Pool", "type:Pool") => {
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", "Pool", SYMBOL_EVIDENCE, "compile-owner-gate"]
                    );
                }
                ("pkg/util/zeropool/pool.go", "field", "type:Pool", "type:Pool/field:0:items") => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "Pool::items",
                            SYMBOL_EVIDENCE,
                            "compile-owner-gate"
                        ]
                    );
                }
                (
                    "pkg/util/zeropool/pool.go",
                    "field",
                    "type:Pool",
                    "type:Pool/field:1:pointers",
                ) => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "DECLINED",
                            "-",
                            "semantic:zeropool:type:Pool/field:1:pointers",
                            "classification-evidence-gate",
                        ]
                    );
                }
                ("pkg/util/zeropool/pool.go", "function", owner, anchor)
                    if owner == anchor
                        && matches!(owner, "New" | "Pool[T].Get" | "Pool[T].Put") =>
                {
                    let symbol = match owner {
                        "New" => "Pool::new",
                        "Pool[T].Get" => "Pool::get",
                        _ => "Pool::put",
                    };
                    assert_eq!(
                        columns[6..10],
                        ["PORTED", symbol, SYMBOL_EVIDENCE, "compile-owner-gate"]
                    );
                }
                ("pkg/util/zeropool/pool.go", "closure", "New", anchor)
                    if anchor.starts_with("New/") =>
                {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "Pool::new",
                            "rust-test:source_factory_and_zero_value_boundaries_are_exact",
                            "behavior-mutation",
                        ]
                    );
                }
                ("pkg/util/zeropool/pool.go", "branch", "Pool[T].Get", anchor)
                    if anchor.starts_with("Pool[T].Get/") =>
                {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "Pool::get",
                            "rust-test:source_factory_and_zero_value_boundaries_are_exact",
                            "behavior-mutation",
                        ]
                    );
                }
                ("pkg/util/zeropool/pool.go", "branch", "Pool[T].Put", anchor)
                    if anchor == "Pool[T].Put/if:1/false" || anchor == "Pool[T].Put/if:1/true" =>
                {
                    assert_eq!(
                        columns[6..10],
                        [
                            "DECLINED",
                            "-",
                            &format!("semantic:zeropool:{anchor}"),
                            "classification-evidence-gate",
                        ]
                    );
                }
                ("pkg/util/zeropool/pool_test.go", "test", "TestPool", "TestPool") => {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "TestPool",
                            "rust-test:TestPool",
                            "test-evidence-gate"
                        ]
                    );
                }
                ("pkg/util/zeropool/pool_test.go", category, "TestPool", anchor)
                    if matches!(
                        category,
                        "test_assertion" | "test_branch" | "test_helper_closure" | "test_loop"
                    ) && anchor.starts_with("TestPool") =>
                {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            "TestPool",
                            "rust-test:TestPool",
                            "test-evidence-gate"
                        ]
                    );
                }
                ("pkg/util/zeropool/pool_test.go", "benchmark", owner, anchor)
                    if owner == anchor && benchmark_names.contains(owner) =>
                {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            owner,
                            &format!("rust-bench:zeropool:{owner}"),
                            "benchmark-evidence-gate",
                        ]
                    );
                }
                ("pkg/util/zeropool/pool_test.go", category, owner, anchor)
                    if matches!(category, "test_helper_closure" | "test_loop" | "test_row")
                        && benchmark_names.contains(owner)
                        && anchor.starts_with(owner) =>
                {
                    assert_eq!(
                        columns[6..10],
                        [
                            "PORTED",
                            owner,
                            &format!("rust-bench:zeropool:{owner}"),
                            "benchmark-evidence-gate",
                        ]
                    );
                }
                _ => panic!("unexpected zeropool inventory row: {line}"),
            }
        }
        assert_eq!(ids.len(), 55);
        assert_eq!(categories, BTreeMap::from(EXPECTED_CATEGORIES));
        assert_eq!(statuses, BTreeMap::from(EXPECTED_STATUSES));

        let semantic_rows: Vec<Vec<_>> = SEMANTIC_DIVERGENCES
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .skip(1)
            .map(|line| line.split('\t').collect())
            .collect();
        assert_eq!(semantic_rows.len(), 9);
        assert!(semantic_rows.iter().all(|row| row.len() == 6));
        assert_eq!(
            semantic_rows
                .iter()
                .map(|row| row[0])
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["S01", "S02", "S03", "S04", "S05", "S06", "S07", "S08", "S09"])
        );
        assert_eq!(
            semantic_rows
                .iter()
                .filter(|row| row[2] == "DECLINED")
                .count(),
            8
        );
        assert_eq!(
            semantic_rows
                .iter()
                .filter(|row| row[2] == "UNREACHABLE")
                .count(),
            1
        );

        let go_test = std::str::from_utf8(GO_TEST).expect("Go test source is UTF-8");
        assert!(go_test.contains("func TestPool"));
        for benchmark in [
            "BenchmarkZeropoolPool",
            "BenchmarkSyncPoolValue",
            "BenchmarkSyncPoolNewPointer",
            "BenchmarkSyncPoolPointer",
        ] {
            assert!(go_test.contains(&format!("func {benchmark}")));
            assert!(RUST_BENCH.contains(&format!("fn {benchmark}")));
        }

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
