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

//! Executable translation of `pkg/util/selection.BenchmarkSelection`.

use std::hint::black_box;
use std::time::Instant;

use tidb_util::selection::{quickselect, select, Selectable};

#[derive(Clone)]
struct TestSlice(Vec<i32>);

impl Selectable for TestSlice {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn less(&self, i: usize, j: usize) -> bool {
        self.0[i] < self.0[j]
    }

    fn swap(&mut self, i: usize, j: usize) {
        self.0.swap(i, j);
    }
}

fn random_test_case(size: usize) -> TestSlice {
    TestSlice(
        (0..size)
            .map(|_| (standard_fastrand::usize(..) % 100) as i32)
            .collect(),
    )
}

fn measure(name: &str, test_case: &TestSlice, operation: impl FnOnce(&mut TestSlice)) {
    let mut data = test_case.clone();
    let started = Instant::now();
    operation(&mut data);
    black_box(data);
    println!("{name}: {:?}", started.elapsed());
}

fn benchmark_size(size: usize) {
    let test_case = random_test_case(size);
    let k = size as isize / 2;
    measure(
        &format!("BenchmarkIntroSelection{size}"),
        &test_case,
        |data| {
            black_box(select(data, k));
        },
    );
    measure(
        &format!("BenchmarkQuickSelection{size}"),
        &test_case,
        |data| {
            black_box(quickselect(data, 0, size as isize - 1, k - 1));
        },
    );
    measure(&format!("BenchmarkSort{size}"), &test_case, |data| {
        data.0.sort_unstable();
    });
}

fn main() {
    for size in [10_000_000, 1_000_000, 100_000, 10_000, 1_000, 100, 50] {
        benchmark_size(size);
    }
}
