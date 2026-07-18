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

//! Sample byte-size boundaries from `pkg/statistics/sample.go`.
//!
//! TiDB keeps only sample values no longer than half MySQL's maximum
//! `VARCHAR` field length and recomputes a collector's total size from its
//! encoded sample bytes. This leaf owns those scalar byte-length decisions;
//! Datum/protobuf conversion and collector state remain external.

/// Source `mysql.MaxFieldVarCharLength`.
pub const MAX_FIELD_VARCHAR_LENGTH: usize = 65_535;

/// Source `statistics.MaxSampleValueLength`.
pub const MAX_SAMPLE_VALUE_LENGTH: usize = MAX_FIELD_VARCHAR_LENGTH / 2;

/// Returns whether a sample survives `SampleCollectorFromProto`'s length gate.
#[must_use]
pub const fn sample_value_is_usable(byte_len: usize) -> bool {
    byte_len <= MAX_SAMPLE_VALUE_LENGTH
}

/// Recomputes `SampleCollector.TotalSize` from encoded sample byte lengths.
///
/// Go converts each length to `int64` and adds it to an `int64` accumulator.
/// The wrapping operations preserve that behavior even for synthetic
/// `usize` values that exceed the signed range.
#[must_use]
pub fn calc_total_size(sample_lengths: &[usize]) -> i64 {
    sample_lengths
        .iter()
        .fold(0_i64, |total, length| total.wrapping_add(*length as i64))
}
