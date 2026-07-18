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

//! Datum-free scalar geometry from `pkg/statistics/scalar.go`.
//!
//! This leaf owns interval fractions, byte common-prefix lengths, and the
//! source's left-aligned base-256 byte scalar. Datum conversion, histogram
//! buckets, time/decimal handling, and planner integration remain external.

/// Calculates the fraction of `[lower, upper]` covered through `value`.
///
/// This follows the source's boundary ordering and its fallback of `0.5` for
/// invalid fractions rather than exposing a NaN/Infinity to callers.
#[must_use]
pub fn calc_fraction(lower: f64, upper: f64, value: f64) -> f64 {
    if upper <= lower {
        return 0.5;
    }
    if value <= lower {
        return 0.0;
    }
    if value >= upper {
        return 1.0;
    }
    let fraction = (value - lower) / (upper - lower);
    if fraction.is_nan() || fraction.is_infinite() || !(0.0..=1.0).contains(&fraction) {
        return 0.5;
    }
    fraction
}

/// Returns the common prefix length of all byte strings.
#[must_use]
pub fn common_prefix_length(strings: &[&[u8]]) -> usize {
    let Some(first) = strings.first() else {
        return 0;
    };
    let min_len = strings.iter().map(|string| string.len()).min().unwrap_or(0);
    for index in 0..min_len {
        let byte = first[index];
        if strings.iter().any(|string| string[index] != byte) {
            return index;
        }
    }
    min_len
}

/// Converts bytes to the source's left-aligned base-256 scalar.
///
/// At most the first eight bytes participate. Shorter values are shifted into
/// the high bits, while eight-or-more-byte values use the first eight bytes as
/// a big-endian `u64`, exactly as the Go helper does.
#[must_use]
pub fn convert_bytes_to_scalar(bytes: &[u8]) -> f64 {
    if bytes.is_empty() {
        return 0.0;
    }
    let used = bytes.len().min(8);
    let mut value = 0_u64;
    for &byte in &bytes[..used] {
        value = (value << 8) | u64::from(byte);
    }
    if used < 8 {
        value <<= (8 - used) * 8;
    }
    value as f64
}
