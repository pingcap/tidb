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

//! Executable translations of both `pkg/util/globalconn` benchmark families.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering::SeqCst};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

use tidb_util::globalconn::{
    AutoIncPool, IdPool, LockFreeCircularPool, ID_POOL_INVALID_VALUE,
    LOCAL_CONN_ID_ALLOCATOR64_TRY_COUNT, LOCAL_CONN_ID_BITS32, LOCAL_CONN_ID_BITS64,
};

struct LockBasedCircularPool {
    values: Mutex<VecDeque<u64>>,
    cap: usize,
}

impl LockBasedCircularPool {
    fn new(size: usize, fill_count: u32) -> Self {
        let fill_count = (fill_count as usize).min(size - 1);
        Self {
            values: Mutex::new((1..=fill_count as u64).collect()),
            cap: size - 1,
        }
    }
}

impl fmt::Display for LockBasedCircularPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "len:{}", self.values.lock().unwrap().len())
    }
}

impl IdPool for LockBasedCircularPool {
    fn init(&mut self, size: u64) {
        *self = Self::new(size as usize, 0);
    }

    fn len(&self) -> i64 {
        self.values.lock().unwrap().len() as i64
    }

    fn cap(&self) -> i64 {
        self.cap as i64
    }

    fn put(&self, value: u64) -> bool {
        let mut values = self.values.lock().unwrap();
        if values.len() == self.cap {
            return false;
        }
        values.push_back(value);
        true
    }

    fn get(&self) -> (u64, bool) {
        self.values
            .lock()
            .unwrap()
            .pop_front()
            .map_or((ID_POOL_INVALID_VALUE, false), |value| (value, true))
    }
}

fn exercise_allocator<P: IdPool + Send + Sync + 'static>(name: &str, pool: P, concurrency: usize) {
    let pool = Arc::new(pool);
    let started = Instant::now();
    let handles = (0..concurrency)
        .map(|_| {
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || {
                for _ in 0..10_000 {
                    let (id, ok) = pool.get();
                    assert!(ok);
                    assert!(pool.put(id));
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().expect("allocator benchmark worker");
    }
    println!("{name}: {:?}", started.elapsed());
}

fn benchmark_local_conn_id_allocator() {
    for concurrency in [1, 3, 10, 20, 100] {
        let mut auto = AutoIncPool::default();
        auto.init_ext(
            1 << LOCAL_CONN_ID_BITS64,
            true,
            LOCAL_CONN_ID_ALLOCATOR64_TRY_COUNT,
        );
        exercise_allocator(
            &format!("BenchmarkLocalConnIDAllocator/Allocator_64_x{concurrency}"),
            auto,
            concurrency,
        );

        exercise_allocator(
            &format!("BenchmarkLocalConnIDAllocator/Allocator_32(LockBased)_x{concurrency}"),
            LockBasedCircularPool::new(1 << LOCAL_CONN_ID_BITS32, u32::MAX),
            concurrency,
        );

        let mut lock_free = LockFreeCircularPool::default();
        lock_free.init_ext(1 << LOCAL_CONN_ID_BITS32, u32::MAX);
        exercise_allocator(
            &format!(
                "BenchmarkLocalConnIDAllocator/Allocator_32(LockFreeCircularPool)_x{concurrency}"
            ),
            lock_free,
            concurrency,
        );
    }
}

fn run_concurrency_case<P: IdPool + Send + Sync + 'static>(
    name: &str,
    pool: P,
    producers: usize,
    consumers: usize,
    requests: u64,
) {
    let pool = Arc::new(pool);
    let ready = Arc::new(Barrier::new(producers + consumers));
    let done = Arc::new(AtomicU32::new(0));
    let total = Arc::new(AtomicI64::new(0));
    let per_producer = requests.div_ceil(producers as u64);
    let started = Instant::now();

    let producer_handles = (0..producers)
        .map(|producer| {
            let pool = Arc::clone(&pool);
            let ready = Arc::clone(&ready);
            std::thread::spawn(move || {
                ready.wait();
                let start = producer as u64 * per_producer;
                let end = ((producer + 1) as u64 * per_producer).min(requests);
                for value in start..end {
                    while !pool.put(value) {
                        std::thread::yield_now();
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    let consumer_handles = (0..consumers)
        .map(|_| {
            let pool = Arc::clone(&pool);
            let ready = Arc::clone(&ready);
            let done = Arc::clone(&done);
            let total = Arc::clone(&total);
            std::thread::spawn(move || {
                ready.wait();
                let mut subtotal = 0_i64;
                loop {
                    let (value, ok) = pool.get();
                    if ok {
                        subtotal += value as i64;
                    } else if done.load(SeqCst) != 0 {
                        break;
                    } else {
                        std::thread::yield_now();
                    }
                }
                total.fetch_add(subtotal, SeqCst);
            })
        })
        .collect::<Vec<_>>();

    for handle in producer_handles {
        handle.join().expect("producer");
    }
    done.store(1, SeqCst);
    for handle in consumer_handles {
        handle.join().expect("consumer");
    }
    assert_eq!(
        total.load(SeqCst),
        (requests as i64 - 1) * requests as i64 / 2
    );
    println!("{name}: {:?}", started.elapsed());
}

fn benchmark_pool_concurrency() {
    const REQUESTS: u64 = 1 << 18;
    for concurrency in [1, 3, 10, 20, 100] {
        run_concurrency_case(
            &format!(
                "BenchmarkPoolConcurrency/LockBasedCircularPool_P:C_{concurrency}:{concurrency}"
            ),
            LockBasedCircularPool::new(1 << 16, 0),
            concurrency,
            concurrency,
            REQUESTS,
        );

        let mut lock_free = LockFreeCircularPool::default();
        lock_free.init_ext(1 << 16, 0);
        run_concurrency_case(
            &format!(
                "BenchmarkPoolConcurrency/LockFreeCircularPool_P:C_{concurrency}:{concurrency}"
            ),
            lock_free,
            concurrency,
            concurrency,
            REQUESTS,
        );
    }
}

fn main() {
    benchmark_local_conn_id_allocator();
    benchmark_pool_concurrency();
}
