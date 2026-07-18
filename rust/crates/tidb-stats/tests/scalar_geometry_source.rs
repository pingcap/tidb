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

//! Source-backed tests for Datum-free scalar geometry.

use tidb_stats::{calc_fraction, common_prefix_length, convert_bytes_to_scalar};

#[test]
fn source_fraction_matches_interval_boundaries_and_fallback() {
    assert_eq!(calc_fraction(0.0, 4.0, 1.0), 0.25);
    assert_eq!(calc_fraction(0.0, 4.0, -1.0), 0.0);
    assert_eq!(calc_fraction(0.0, 4.0, 4.0), 1.0);
    assert_eq!(calc_fraction(4.0, 0.0, 2.0), 0.5);
    assert_eq!(calc_fraction(1.0, 1.0, 1.0), 0.5);
    assert_eq!(calc_fraction(0.0, 4.0, f64::NAN), 0.5);
    assert_eq!(calc_fraction(0.0, 4.0, f64::INFINITY), 1.0);
    assert_eq!(calc_fraction(0.0, f64::INFINITY, f64::INFINITY), 1.0);
}

#[test]
fn source_common_prefix_length_handles_empty_and_multiple_strings() {
    assert_eq!(common_prefix_length(&[]), 0);
    assert_eq!(common_prefix_length(&[b"", b"abc"]), 0);
    assert_eq!(common_prefix_length(&[b"abc", b"abd", b"abz"]), 2);
    assert_eq!(common_prefix_length(&[b"abc", b"abc"]), 3);
    assert_eq!(common_prefix_length(&[b"abc", b"ab"]), 2);
}

#[test]
fn source_byte_scalar_is_left_aligned_big_endian() {
    assert_eq!(convert_bytes_to_scalar(&[]), 0.0);
    assert_eq!(convert_bytes_to_scalar(&[1]), (1_u64 << 56) as f64);
    assert_eq!(
        convert_bytes_to_scalar(&[1, 2]),
        0x0102_0000_0000_0000_u64 as f64
    );
    assert_eq!(
        convert_bytes_to_scalar(&[1, 2, 3, 4, 5, 6, 7, 8]),
        0x0102_0304_0506_0708_u64 as f64
    );
    assert_eq!(
        convert_bytes_to_scalar(&[1, 2, 3, 4, 5, 6, 7, 8, 9]),
        0x0102_0304_0506_0708_u64 as f64
    );
}
