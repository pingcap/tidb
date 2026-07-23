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

//! Executable translations of every benchmark in
//! `pkg/util/zeropool/pool_test.go`.

#![allow(non_snake_case)]

use std::hint::black_box;
use std::sync::Mutex;
use std::time::Instant;
use tidb_util::zeropool::Pool;

const ITERATIONS: usize = 1_000_000;

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    println!("{name}: {:?}", started.elapsed());
}

fn BenchmarkZeropoolPool() {
    let pool = Pool::new(|| vec![0_u8; 1024]);
    pool.put(pool.get());
    measure("BenchmarkZeropoolPool", || {
        let item = pool.get();
        black_box(&item);
        pool.put(item);
    });
}

fn BenchmarkSyncPoolValue() {
    let pool = Mutex::new(vec![vec![0_u8; 1024]]);
    measure("BenchmarkSyncPoolValue", || {
        let item = pool.lock().expect("value pool").pop().unwrap_or_default();
        black_box(&item);
        pool.lock().expect("value pool").push(item);
    });
}

fn BenchmarkSyncPoolNewPointer() {
    let pool = Mutex::new(vec![Box::new(vec![0_u8; 1024])]);
    measure("BenchmarkSyncPoolNewPointer", || {
        let item = *pool
            .lock()
            .expect("new pointer pool")
            .pop()
            .unwrap_or_default();
        black_box(&item);
        pool.lock().expect("new pointer pool").push(Box::new(item));
    });
}

fn BenchmarkSyncPoolPointer() {
    let pool = Mutex::new(vec![Box::new(vec![0_u8; 1024])]);
    measure("BenchmarkSyncPoolPointer", || {
        let item = pool.lock().expect("pointer pool").pop().unwrap_or_default();
        black_box(&item);
        pool.lock().expect("pointer pool").push(item);
    });
}

fn main() {
    BenchmarkZeropoolPool();
    BenchmarkSyncPoolValue();
    BenchmarkSyncPoolNewPointer();
    BenchmarkSyncPoolPointer();
}
