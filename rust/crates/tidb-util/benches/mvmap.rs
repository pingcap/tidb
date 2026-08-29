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

//! Executable translations of both `pkg/util/mvmap` benchmarks.

use std::time::Instant;

use tidb_util::mvmap::MVMap;

const ITERATIONS: u64 = 100_000;

fn benchmark_mvmap_put() {
    let mut map = MVMap::new();
    let started = Instant::now();
    for value in 0..ITERATIONS {
        let buffer = value.to_be_bytes();
        map.put(&buffer, &buffer);
    }
    println!("BenchmarkMVMapPut: {:?}", started.elapsed());
}

fn benchmark_mvmap_get() {
    let mut map = MVMap::new();
    for value in 0..ITERATIONS {
        let buffer = value.to_be_bytes();
        map.put(&buffer, &buffer);
    }

    let mut values = Vec::with_capacity(8);
    let started = Instant::now();
    for value in 0..ITERATIONS {
        let buffer = value.to_be_bytes();
        values.clear();
        values = map.get(&buffer, values);
        assert!(values.len() == 1 && values[0] == buffer);
    }
    println!("BenchmarkMVMapGet: {:?}", started.elapsed());
}

fn main() {
    benchmark_mvmap_put();
    benchmark_mvmap_get();
}
