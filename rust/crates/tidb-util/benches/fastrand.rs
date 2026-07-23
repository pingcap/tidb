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
//! `pkg/util/fastrand/random_test.go`.

#![allow(non_snake_case)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_util::fastrand::{buf, uint32, uint32_n};

const SAMPLE_WINDOW: Duration = Duration::from_millis(100);

fn measure_parallel(name: &str, operation: impl Fn() + Send + Sync + 'static) {
    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(12);
    let operation = Arc::new(operation);
    let started = Instant::now();
    let handles = (0..workers)
        .map(|_| {
            let operation = Arc::clone(&operation);
            std::thread::spawn(move || {
                let mut iterations = 0_u64;
                while started.elapsed() < SAMPLE_WINDOW {
                    operation();
                    iterations += 1;
                }
                iterations
            })
        })
        .collect::<Vec<_>>();
    let iterations = handles
        .into_iter()
        .map(|handle| handle.join().expect("benchmark worker"))
        .sum::<u64>();
    println!(
        "{name}: {:?} across {iterations} iterations on {workers} workers",
        started.elapsed(),
    );
}

fn BenchmarkFastRandBuf() {
    measure_parallel("BenchmarkFastRandBuf", || {
        black_box(buf(20));
    });
}

fn BenchmarkFastRandUint32N() {
    measure_parallel("BenchmarkFastRandUint32N", || {
        black_box(uint32_n(127));
    });
}

fn BenchmarkFastRand() {
    measure_parallel("BenchmarkFastRand", || {
        black_box(uint32());
    });
    println!("{}", uint32());
}

fn BenchmarkGlobalRand() {
    measure_parallel("BenchmarkGlobalRand", || {
        black_box(standard_fastrand::i64(..));
    });
    println!("{}", standard_fastrand::i64(..));
}

fn main() {
    BenchmarkFastRandBuf();
    BenchmarkFastRandUint32N();
    BenchmarkFastRand();
    BenchmarkGlobalRand();
}
