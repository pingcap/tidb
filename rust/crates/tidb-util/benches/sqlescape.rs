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

//! Executable translations of all `pkg/util/sqlescape` benchmarks.

#![allow(non_snake_case)]

use std::hint::black_box;
use std::time::{Duration, Instant};
use tidb_util::sqlescape::{escape_sql, SqlArg};

const SAMPLE_WINDOW: Duration = Duration::from_millis(100);

fn measure(name: &str, argument: SqlArg<'_>) {
    let started = Instant::now();
    let mut iterations = 0_u64;
    while started.elapsed() < SAMPLE_WINDOW {
        black_box(escape_sql("select %?", std::slice::from_ref(&argument)).expect("escape"));
        iterations += 1;
    }
    println!(
        "{name}: {:?} across {iterations} iterations",
        started.elapsed()
    );
}

fn BenchmarkEscapeString() {
    measure("BenchmarkEscapeString", SqlArg::String("3"));
}

fn BenchmarkUnderlyingString() {
    measure("BenchmarkUnderlyingString", SqlArg::String("3"));
}

fn BenchmarkEscapeInt() {
    measure("BenchmarkEscapeInt", SqlArg::Signed(3));
}

fn BenchmarkUnderlyingInt() {
    measure("BenchmarkUnderlyingInt", SqlArg::Signed(3));
}

fn main() {
    BenchmarkEscapeString();
    BenchmarkUnderlyingString();
    BenchmarkEscapeInt();
    BenchmarkUnderlyingInt();
}
