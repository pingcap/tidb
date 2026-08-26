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

//! Ports of `pkg/util/zeropool` unit tests from Go (`pool_test.go`).

use crate::zeropool::Pool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

const ITERATIONS: usize = 1_000_000;
const CONCURRENCY: usize = u8::MAX as usize;

/// Go: pkg/util/zeropool/pool_test.go TestPool/"provides correct values"
#[test]
fn pool_provides_correct_values() {
    let pool = Pool::new(|| vec![0_u8; 1024]);
    let item1 = pool.get();
    assert_eq!(item1.len(), 1024);

    let item2 = pool.get();
    assert_eq!(item2.len(), 1024);

    pool.put(item1);
    pool.put(item2);

    let item1 = pool.get();
    assert_eq!(item1.len(), 1024);

    let item2 = pool.get();
    assert_eq!(item2.len(), 1024);
}

/// Go: pkg/util/zeropool/pool_test.go TestPool/"is not racy"
#[test]
fn pool_is_not_racy() {
    let pool = Arc::new(Pool::new(|| vec![0_u8; 1024]));
    let next = Arc::new(AtomicUsize::new(0));
    let counter = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(CONCURRENCY));

    let workers = (0..CONCURRENCY)
        .map(|worker| {
            let pool = Arc::clone(&pool);
            let next = Arc::clone(&next);
            let counter = Arc::clone(&counter);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                loop {
                    let iteration = next.fetch_add(1, Ordering::Relaxed);
                    if iteration >= ITERATIONS {
                        break;
                    }
                    let mut item = pool.get();
                    item[0] = worker as u8;
                    // Counts and also adds some delay to add raciness.
                    counter.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(item[0], worker as u8, "wrong value");
                    pool.put(item);
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().expect("pool worker panicked");
    }
    assert_eq!(counter.load(Ordering::Relaxed), ITERATIONS);
}

/// Go: pkg/util/zeropool/pool_test.go TestPool/"does not allocate".
///
/// Go measures `testing.AllocsPerRun < 1`; Rust has no equivalent allocation
/// counter hook in stable tests, so we pin the observable zero-allocation
/// behavior instead: after warm-up, Get/Put cycles always hand back the same
/// buffered allocation (no new value is created).
#[test]
fn pool_does_not_allocate_after_warmup() {
    let pool = Pool::new(|| vec![0_u8; 1024]);
    // Warm up; this allocates one Vec.
    let item = pool.get();
    let ptr = item.as_ptr();
    pool.put(item);

    for _ in 0..1000 {
        let item = pool.get();
        assert_eq!(
            item.as_ptr(),
            ptr,
            "Get must reuse the pooled value, not create a new one"
        );
        pool.put(item);
    }
}

/// Go: pkg/util/zeropool/pool_test.go TestPool/"zero value is valid".
///
/// Same measurement caveat as `pool_does_not_allocate_after_warmup`: Go's
/// `AllocsPerRun` bound is pinned via value-reuse identity checks.
#[test]
fn pool_zero_value_is_valid() {
    let pool = Pool::<Vec<u8>>::default();
    let slice = pool.get();
    pool.put(slice);

    let mut first_ptr = None;
    for _ in 0..1000 {
        let slice = pool.get();
        match first_ptr {
            None => first_ptr = Some(slice.as_ptr()),
            Some(ptr) => assert_eq!(slice.as_ptr(), ptr),
        }
        pool.put(slice);
    }
}
