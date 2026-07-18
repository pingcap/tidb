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

//! Source-backed tests for init-stats progress arithmetic.

use tidb_stats::init_stats_progress;

#[test]
fn source_init_stats_progress_scales_and_offsets_counts() {
    assert_eq!(init_stats_progress(0, 10, 25.0, 10.0), 10.0);
    assert_eq!(init_stats_progress(4, 10, 25.0, 10.0), 20.0);
    assert_eq!(init_stats_progress(10, 10, 25.0, 10.0), 35.0);
}

#[test]
fn source_init_stats_progress_preserves_float_denominator_behavior() {
    assert!(init_stats_progress(0, 0, 25.0, 10.0).is_nan());
    assert!(init_stats_progress(1, 0, 25.0, 10.0).is_infinite());
    assert_eq!(init_stats_progress(u64::MAX, u64::MAX, -2.5, 100.0), 97.5);
}
