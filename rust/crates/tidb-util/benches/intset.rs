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

//! Executable translations of the six benchmarks in
//! `pkg/util/intset/fast_int_set_bench_test.go`.

use std::collections::{BTreeSet, HashSet};
use std::hint::black_box;
use std::time::Instant;

use tidb_util::intset::FastIntSet;

fn measure(name: &str, operation: impl FnOnce()) {
    let started = Instant::now();
    operation();
    black_box(started.elapsed());
    println!("{name}: {:?}", started.elapsed());
}

fn benchmark_map_int_set_difference() {
    let set_a: HashSet<i64> = (0..200_000).collect();
    let set_b: HashSet<i64> = (100_000..300_000).collect();
    measure("BenchmarkMapIntSet_Difference", || {
        let result: HashSet<i64> = set_a.difference(&set_b).copied().collect();
        black_box(result);
    });
}

fn benchmark_int_set_difference() {
    let mut set_a: BTreeSet<i64> = (0..200_000).collect();
    let set_b = BTreeSet::new();
    // Preserve the source benchmark: its second population loop inserts into A.
    set_a.extend(100_000..300_000);
    measure("BenchmarkIntSet_Difference", || {
        let result: BTreeSet<i64> = set_a.difference(&set_b).copied().collect();
        black_box(result);
    });
}

fn benchmark_fast_int_set_difference() {
    let mut set_a = FastIntSet::new(0..200_000);
    let set_b = FastIntSet::default();
    // Preserve the source benchmark: its second population loop inserts into A.
    for value in 100_000..300_000 {
        set_a.insert(value);
    }
    measure("BenchmarkFastIntSet_Difference", || {
        black_box(set_a.difference(&set_b));
    });
}

fn benchmark_int_set_insert() {
    measure("BenchmarkIntSet_Insert", || {
        let mut set = HashSet::new();
        for value in 0..64 {
            set.insert(value);
        }
        black_box(set);
    });
}

fn benchmark_sparse_insert() {
    measure("BenchmarkSparse_Insert", || {
        let mut set = BTreeSet::new();
        for value in 0..64 {
            set.insert(value);
        }
        black_box(set);
    });
}

fn benchmark_fast_int_set_insert() {
    measure("BenchmarkFastIntSet_Insert", || {
        let mut set = FastIntSet::default();
        for value in 0..64 {
            set.insert(value);
        }
        black_box(set);
    });
}

fn main() {
    benchmark_map_int_set_difference();
    benchmark_int_set_difference();
    benchmark_fast_int_set_difference();
    benchmark_int_set_insert();
    benchmark_sparse_insert();
    benchmark_fast_int_set_insert();
}
