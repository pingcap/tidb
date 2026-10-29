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
//! `pkg/lightning/log/filter_test.go`.

use std::hint::black_box;
use std::time::Instant;

const ITERATIONS: usize = 10_000_000;

fn benchmark_filter_strings_contains() {
    let inputs = [
        "github.com/pingcap/tidb/some/package/path",
        "github.com/tikv/pd/some/package/path",
        "github.com/pingcap/tidb/br/some/package/path",
    ];
    let filters = ["github.com/pingcap/tidb/", "github.com/tikv/pd/"];
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        for input in inputs {
            for filter in filters {
                black_box(input.contains(filter));
            }
        }
    }
    println!("BenchmarkFilterStringsContains: {:?}", started.elapsed());
}

fn benchmark_filter_regex_match_string() {
    let inputs = [
        "github.com/pingcap/tidb/some/package/path",
        "github.com/tikv/pd/some/package/path",
        "github.com/pingcap/tidb/br/some/package/path",
    ];
    let filters = regex::Regex::new(r"github.com/(pingcap/tidb|tikv/pd)/").unwrap();
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        for input in inputs {
            black_box(filters.is_match(input));
        }
    }
    println!("BenchmarkFilterRegexMatchString: {:?}", started.elapsed());
}

fn main() {
    benchmark_filter_strings_contains();
    benchmark_filter_regex_match_string();
}
