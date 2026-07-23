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
//! `pkg/util/hack/map_abi_test.go`.

#![allow(non_snake_case)]

use hashbrown::HashMap;
use std::collections::hash_map::RandomState;
use std::hint::black_box;
use std::time::{Duration, Instant};
use tidb_hack::MemAwareMap;

const INPUTS: [usize; 4] = [1, 100, 10_000, 1_000_000];
const SAMPLE_WINDOW: Duration = Duration::from_millis(100);

fn mem_aware_int_map(size: usize) -> usize {
    let mut map = MemAwareMap::new(0);
    for value in 0..size {
        map.set(value, value);
    }
    let mut result = 0;
    for value in 0..size {
        result = map.get(&value).copied().unwrap_or_default();
    }
    result
}

fn native_int_map(size: usize) -> usize {
    let mut map = HashMap::<usize, usize, RandomState>::with_hasher(RandomState::new());
    for value in 0..size {
        map.insert(value, value);
    }
    let mut result = 0;
    for value in 0..size {
        result = map.get(&value).copied().unwrap_or_default();
    }
    result
}

fn measure(name: &str, input: usize, operation: impl Fn(usize) -> usize) {
    let started = Instant::now();
    let mut iterations = 0_u64;
    while started.elapsed() < SAMPLE_WINDOW {
        black_box(operation(input));
        iterations += 1;
    }
    println!(
        "{name}_{input}: {:?} across {iterations} iterations",
        started.elapsed()
    );
}

fn BenchmarkMemAwareIntMap() {
    for input in INPUTS {
        measure("BenchmarkMemAwareIntMap", input, mem_aware_int_map);
    }
}

fn BenchmarkNativeIntMap() {
    for input in INPUTS {
        measure("BenchmarkNativeIntMap", input, native_int_map);
    }
}

fn main() {
    BenchmarkMemAwareIntMap();
    BenchmarkNativeIntMap();
}
