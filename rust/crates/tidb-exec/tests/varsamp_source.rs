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

//! Source-backed tests for sample variance finalization.

use tidb_exec::varsamp::sample_variance;

fn assert_close(actual: f64, expected: f64) {
    assert!((actual - expected).abs() < 1e-15, "{actual} != {expected}");
}

#[test]
fn varsamp_merge_vectors_match_source() {
    // Source: pkg/executor/aggfuncs/func_varsamp.go:25-33 and :44-63.
    // Direct Go coverage: pkg/executor/aggfuncs/func_varsamp_test.go:24
    // (TestMergePartialResult4Varsamp).
    assert_close(
        sample_variance(5, 10.0).expect("sample with five rows"),
        2.5,
    );
    assert_close(sample_variance(2, 1.0).expect("sample with two rows"), 1.0);
    assert_close(
        sample_variance(8, 13.875).expect("sample with eight rows"),
        1.9821428571428572,
    );
}

#[test]
fn varsamp_threshold_and_sign_match_source() {
    // Direct Go coverage: pkg/executor/aggfuncs/func_varsamp_test.go:33
    // (TestVarsamp), whose empty input emits NULL and non-empty input uses
    // sample normalization.
    assert_eq!(sample_variance(0, 10.0), None);
    assert_eq!(sample_variance(1, 10.0), None);
    assert_close(
        sample_variance(5, 10.0).expect("sample with five rows"),
        2.5,
    );
    assert_close(sample_variance(2, -1.0).expect("nonzero sample"), -1.0);
}
