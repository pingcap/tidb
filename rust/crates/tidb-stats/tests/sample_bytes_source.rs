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

//! Source-backed tests for sample byte-length decisions.

use tidb_stats::{
    calc_total_size, sample_value_is_usable, MAX_FIELD_VARCHAR_LENGTH, MAX_SAMPLE_VALUE_LENGTH,
};

#[test]
fn source_sample_length_boundary_is_inclusive() {
    assert_eq!(MAX_FIELD_VARCHAR_LENGTH, 65_535);
    assert_eq!(MAX_SAMPLE_VALUE_LENGTH, 32_767);
    assert!(sample_value_is_usable(0));
    assert!(sample_value_is_usable(MAX_SAMPLE_VALUE_LENGTH));
    assert!(!sample_value_is_usable(MAX_SAMPLE_VALUE_LENGTH + 1));
}

#[test]
fn source_sample_total_size_sums_encoded_lengths() {
    assert_eq!(calc_total_size(&[0, 1, 32_767, 65_535]), 98_303);
    assert_eq!(calc_total_size(&[]), 0);
}

#[test]
fn source_sample_total_size_preserves_go_int64_wrapping() {
    assert_eq!(calc_total_size(&[usize::MAX, 1]), 0);
    assert_eq!(calc_total_size(&[usize::MAX]), -1);
}
