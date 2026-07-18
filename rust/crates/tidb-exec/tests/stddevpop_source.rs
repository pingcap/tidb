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

//! Source-backed tests for population standard-deviation finalization.

use tidb_exec::stddevpop::population_stddev;

fn assert_close(actual: f64, expected: f64) {
    assert!((actual - expected).abs() < 1e-15, "{actual} != {expected}");
}

#[test]
fn stddevpop_merge_vectors_match_source() {
    // Source: pkg/executor/aggfuncs/func_stddevpop.go:27-35 and :46-65.
    // Direct Go coverage: pkg/executor/aggfuncs/func_stddevpop_test.go:24
    // (TestMergePartialResult4Stddevpop).
    assert_close(
        population_stddev(5, 10.0).expect("non-empty population"),
        std::f64::consts::SQRT_2,
    );
    assert_close(
        population_stddev(3, 2.0).expect("non-empty population"),
        0.816496580927726,
    );
    assert_close(
        population_stddev(7, 12.140625).expect("non-empty population"),
        1.3169567191065923,
    );
}

#[test]
fn stddevpop_empty_and_single_result_match_source() {
    // Direct Go coverage: pkg/executor/aggfuncs/func_stddevpop_test.go:33
    // (TestStddevpop), whose empty input emits NULL and non-empty input emits
    // the population square-root normalization.
    assert_eq!(population_stddev(0, 2.0), None);
    assert_close(
        population_stddev(5, 10.0).expect("non-empty population"),
        std::f64::consts::SQRT_2,
    );
    assert!(population_stddev(1, -1.0).expect("nonzero count").is_nan());
}
