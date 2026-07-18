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

//! Source-backed tests for triangular out-of-range overlap geometry.

use tidb_stats::{left_overlap_percent, right_overlap_percent};

#[test]
fn source_left_overlap_clips_to_histogram_triangle() {
    assert_eq!(left_overlap_percent(2.0, 6.0, 0.0, 10.0, 10.0), 0.32);
    assert_eq!(left_overlap_percent(-5.0, 5.0, 0.0, 10.0, 10.0), 0.25);
    assert_eq!(left_overlap_percent(10.0, 12.0, 0.0, 10.0, 10.0), 0.0);
    assert_eq!(left_overlap_percent(2.0, 6.0, 0.0, 10.0, 0.0), 0.0);
}

#[test]
fn source_right_overlap_clips_to_histogram_triangle() {
    assert_eq!(right_overlap_percent(12.0, 18.0, 10.0, 20.0, 10.0), 0.60);
    assert_eq!(right_overlap_percent(15.0, 25.0, 10.0, 20.0, 10.0), 0.25);
    assert_eq!(right_overlap_percent(8.0, 10.0, 10.0, 20.0, 10.0), 0.0);
    assert_eq!(right_overlap_percent(12.0, 18.0, 10.0, 20.0, -1.0), 0.0);
}

#[test]
fn source_overlap_keeps_nan_when_bounds_are_not_orderable() {
    assert!(left_overlap_percent(f64::NAN, 6.0, 0.0, 10.0, 10.0).is_nan());
    assert!(right_overlap_percent(12.0, f64::NAN, 10.0, 20.0, 10.0).is_nan());
}
