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

//! Executable translations of all `pkg/util/stringutil` benchmarks.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use tidb_util::stringutil::{build_string_from_labels, compile_pattern, do_match};

const SAMPLE_WINDOW: Duration = Duration::from_millis(100);

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    let mut iterations = 0_u64;
    while started.elapsed() < SAMPLE_WINDOW {
        operation();
        iterations += 1;
    }
    println!(
        "{name}: {:?} across {iterations} iterations",
        started.elapsed()
    );
}

fn benchmark_do_match() {
    for (pattern, target) in [
        (r"a%_%_%_%_b", "aababab"),
        (r"%_%_a%_%_b", "bbbaaabb"),
        (r"a%_%_a%_%_b", "aaaabbbbbbaaaaaaaaabbbbb"),
    ] {
        let (weights, types) = compile_pattern(pattern, b'\\');
        measure(&format!("BenchmarkDoMatch/{pattern}"), || {
            assert!(black_box(do_match(target, &weights, &types)));
        });
    }
}

fn benchmark_do_match_negative() {
    let pattern = r"a%a%a%a%a%a%a%a%b";
    let target = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let (weights, types) = compile_pattern(pattern, b'\\');
    measure(&format!("BenchmarkDoMatchNegative/{pattern}"), || {
        assert!(!black_box(do_match(target, &weights, &types)));
    });
}

fn benchmark_build_string_from_labels() {
    let labels = HashMap::from([
        ("aaa".to_owned(), "bbb".to_owned()),
        ("foo".to_owned(), "bar".to_owned()),
    ]);
    measure("BenchmarkBuildStringFromLabels/normal_case", || {
        black_box(build_string_from_labels(&labels));
    });
}

fn main() {
    benchmark_do_match();
    benchmark_do_match_negative();
    benchmark_build_string_from_labels();
}
