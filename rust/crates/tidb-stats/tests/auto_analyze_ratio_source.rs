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

//! Source-backed tests for auto-analyze ratio parsing.

use tidb_stats::{parse_auto_analyze_ratio, DEFAULT_AUTO_ANALYZE_RATIO};

#[test]
fn source_invalid_ratio_uses_default() {
    assert_eq!(
        parse_auto_analyze_ratio("not-a-ratio"),
        DEFAULT_AUTO_ANALYZE_RATIO
    );
    assert_eq!(parse_auto_analyze_ratio(""), DEFAULT_AUTO_ANALYZE_RATIO);
}

#[test]
fn source_valid_ratio_and_negative_clamp_match_go() {
    assert_eq!(parse_auto_analyze_ratio("0.25"), 0.25);
    assert_eq!(parse_auto_analyze_ratio("-0.25"), 0.0);
    assert_eq!(parse_auto_analyze_ratio("-inf"), 0.0);
}

#[test]
fn source_special_float_values_preserve_math_max_behavior() {
    assert!(parse_auto_analyze_ratio("NaN").is_nan());
    assert_eq!(parse_auto_analyze_ratio("inf"), f64::INFINITY);
}
