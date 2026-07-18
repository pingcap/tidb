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

//! Source-backed tests for sample standard-deviation finalization.

use tidb_exec::stddevsamp::sample_stddev;

fn assert_close(actual: f64, expected: f64) {
    assert!((actual - expected).abs() < 1e-15, "{actual} != {expected}");
}

#[test]
fn stddevsamp_merge_vectors_match_source() {
    // Source: pkg/executor/aggfuncs/func_stddevsamp.go:27-35 and :46-65.
    // Direct Go coverage: pkg/executor/aggfuncs/func_stddevsamp_test.go:24
    // (TestMergePartialResult4Stddevsamp).
    assert_close(
        sample_stddev(5, 10.0).expect("sample with five rows"),
        1.5811388300841898,
    );
    assert_close(sample_stddev(2, 1.0).expect("sample with two rows"), 1.0);
    assert_close(
        sample_stddev(8, 13.875).expect("sample with eight rows"),
        1.407885953173359,
    );
}

#[test]
fn stddevsamp_threshold_and_nan_match_source() {
    // Direct Go coverage: pkg/executor/aggfuncs/func_stddevsamp_test.go:33
    // (TestStddevsamp), whose empty input emits NULL and non-empty input uses
    // sample normalization.
    assert_eq!(sample_stddev(0, 10.0), None);
    assert_eq!(sample_stddev(1, 10.0), None);
    assert_close(
        sample_stddev(5, 10.0).expect("sample with five rows"),
        1.5811388300841898,
    );
    assert!(sample_stddev(2, -1.0).expect("nonzero sample").is_nan());
}
