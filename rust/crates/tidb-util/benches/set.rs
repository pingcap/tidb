// Copyright 2021 PingCAP, Inc.
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

//! Executable translations of the three benchmarks in
//! `pkg/util/set/set_with_memory_usage_test.go`.

use std::hint::black_box;
use std::time::Instant;

use tidb_datatype::GoString;
use tidb_util::set::{
    Float64SetWithMemoryUsage, Int64SetWithMemoryUsage, StringSetWithMemoryUsage,
};

const ROW_COUNTS: [usize; 8] = [
    0, 100, 10_000, 1_000_000, 851_968, 851_969, 425_984, 425_985,
];

fn measure(name: &str, rows: usize, mut insert: impl FnMut(usize)) {
    let started = Instant::now();
    for value in 0..rows {
        insert(value);
    }
    black_box(started.elapsed());
    println!("{name}/MapRows {rows}: {:?}", started.elapsed());
}

fn benchmark_float64_set_memory_usage() {
    for rows in ROW_COUNTS {
        let (mut set, _) = Float64SetWithMemoryUsage::new([]);
        measure("BenchmarkFloat64SetMemoryUsage", rows, |value| {
            set.insert(value as f64);
        });
    }
}

fn benchmark_int64_set_memory_usage() {
    for rows in ROW_COUNTS {
        let (mut set, _) = Int64SetWithMemoryUsage::new([]);
        measure("BenchmarkInt64SetMemoryUsage", rows, |value| {
            set.insert(value as i64);
        });
    }
}

fn benchmark_string_set_memory_usage() {
    for rows in ROW_COUNTS {
        let (mut set, _) = StringSetWithMemoryUsage::new([]);
        measure("BenchmarkStringSetMemoryUsage", rows, |value| {
            set.insert(GoString::from(value.to_string()));
        });
    }
}

fn main() {
    benchmark_float64_set_memory_usage();
    benchmark_int64_set_memory_usage();
    benchmark_string_set_memory_usage();
}
