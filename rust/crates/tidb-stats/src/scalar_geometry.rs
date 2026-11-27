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

//! Scalar geometry from `pkg/statistics/scalar.go`.
//!
//! This leaf owns interval fractions, byte common-prefix lengths, the source's
//! left-aligned base-256 byte scalar, and scalar conversion of already-decoded
//! [`Datum`] values. Histogram storage, its precomputed scalar cache, and
//! planner integration remain external.

use std::borrow::Cow;

use tidb_datatype::{CoreTime, Datum, Time, TimeType, calc_daynr};

/// Calculates the fraction of `[lower, upper]` covered through `value`.
///
/// This follows the source's boundary ordering and its fallback of `0.5` for
/// invalid fractions rather than exposing a NaN/Infinity to callers.
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
pub fn common_prefix_length(strings: &[&[u8]]) -> usize {
    if let [left, right] = strings {
        let len = left.len().min(right.len());
        let (left_chunks, left_tail) = left[..len].as_chunks::<32>();
        let (right_chunks, right_tail) = right[..len].as_chunks::<32>();
        for (index, (left, right)) in left_chunks.iter().zip(right_chunks).enumerate() {
            if left != right {
                let first_difference = left
                    .iter()
                    .zip(right)
                    .position(|(left, right)| left != right)
                    .expect("different chunks must contain different bytes");
                return index * 32 + first_difference;
            }
        }
        return left_chunks.len() * 32
            + left_tail
                .iter()
                .zip(right_tail)
                .position(|(left, right)| left != right)
                .unwrap_or(left_tail.len());
    }

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
pub fn convert_bytes_to_scalar(bytes: &[u8]) -> f64 {
    if let Some(first) = bytes.first_chunk::<8>() {
        return u64::from_be_bytes(*first) as f64;
    }
    let len = bytes.len();
    if len == 0 {
        return 0.0;
    }
    let mut value = 0_u64;
    for &byte in bytes {
        value = (value << 8) | u64::from(byte);
    }
    (value << ((8 - len) * 8)) as f64
}

const fn min_datetime_core() -> CoreTime {
    CoreTime::from_date(1, 1, 1, 0, 0, 0, 0)
}

fn seconds_of_day(core: CoreTime) -> i64 {
    i64::from(core.hour()) * 3_600 + i64::from(core.minute()) * 60 + i64::from(core.second())
}

/// Go `convertMysqlTimeToScalar`: the duration from the type's minimum bound
/// (MinDatetime for date/datetime, MinTimestamp for timestamp). TIMESTAMP
/// values outside the legal range are clamped just outside the valid scalar
/// range so estimates stay finite and ordered.
fn time_to_scalar(value: Time) -> f64 {
    let core = value.core_time();
    if value.kind() == TimeType::Timestamp {
        // Go `types.MinTimestamp` / `types.MaxTimestamp`.
        const MIN_TIMESTAMP: CoreTime = CoreTime::from_date(1970, 1, 1, 0, 0, 1, 0);
        const MAX_TIMESTAMP: CoreTime = CoreTime::from_date(2038, 1, 19, 3, 14, 7, 999_999);
        if core.compare(MIN_TIMESTAMP).is_lt() {
            // Relaxed SQL modes can leave zero TIMESTAMP values in stats;
            // subtracting MinTimestamp from one saturates to an artificial
            // huge distance that would skew fraction and out-of-range
            // estimates. All before-min values collapse to -1, just before
            // the legal range (calcFraction treats value <= lower as 0).
            return -1.0;
        }
        if core.compare(MAX_TIMESTAMP).is_gt() {
            // Above-max bounds go one ULP after the legal maximum: at this
            // magnitude maxScalar + 1 rounds back to maxScalar, collapsing a
            // [MaxTimestamp, after-max] bucket into a zero-width interval.
            return (timestamp_nanoseconds(MAX_TIMESTAMP) as f64).next_up();
        }
        return timestamp_nanoseconds(core) as f64;
    }

    // Go's DATE/DATETIME subtraction wraps in the signed nanosecond
    // duration. Compute that same result directly from the civil fields.
    let seconds = i64::from(core.date_diff(min_datetime_core())) * 86_400 + seconds_of_day(core);
    seconds
        .wrapping_mul(1_000_000_000)
        .wrapping_add(i64::from(core.microsecond()) * 1_000) as f64
}

/// `pkg/statistics/scalar.go`'s `convertDatumToScalar` over an already-decoded
/// datum.
pub fn convert_datum_to_scalar(value: &Datum, common_prefix_length: usize) -> f64 {
    match value {
        // KindFloat32 retains a float64 raw payload, but GetFloat32 narrows it
        // to IEEE-754 binary32 before the source widens the result again.
        Datum::Float32(value) => f64::from(*value as f32),
        Datum::Real(value) => *value,
        Datum::Int(value) => *value as f64,
        Datum::UInt(value) => *value as f64,
        Datum::Duration(value) => value.nanoseconds() as f64,
        Datum::Decimal(value) => value.to_f64(),
        Datum::Time(value) => time_to_scalar(*value),
        Datum::String(value) => bytes_to_scalar(value.bytes(), common_prefix_length),
        Datum::Bytes(value) => bytes_to_scalar(value, common_prefix_length),
        Datum::MinNotNull => -f64::MAX,
        Datum::MaxValue => f64::MAX,
        _ => 0.0,
    }
}

fn timestamp_nanoseconds(core: CoreTime) -> i64 {
    // Shift both years by a Gregorian cycle so Go's day-number formula
    // stays in positive years even when month zero carries into year -1.
    const YEAR_SHIFT: i64 = 400;
    const EPOCH_DAY: i64 = calc_daynr(1970 + YEAR_SHIFT, 1, 1);

    // Go time.Date carries out-of-range months into the year. Once the
    // month is normalized, day and clock overflow are just UTC offsets.
    let mut year = i64::from(core.year()) + YEAR_SHIFT;
    let mut month = i64::from(core.month());
    if !(1..=12).contains(&month) {
        year += (month - 1).div_euclid(12);
        month = (month - 1).rem_euclid(12) + 1;
    }

    let days = calc_daynr(year, month, i64::from(core.day())) - EPOCH_DAY;
    let seconds = days * 86_400 + seconds_of_day(core) - 1;
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(core.microsecond()) * 1_000;
    nanos.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Numeric getters on Go Datum read its shared raw `i` payload without first
/// checking the tag. A typed Rust datum has no stale payload after retagging,
/// but every fresh source setter's representable raw payload is reproduced.
fn datum_raw_i64(value: &Datum) -> i64 {
    match value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        Datum::Real(value) | Datum::Float32(value) => value.to_bits() as i64,
        Datum::Duration(value) => value.nanoseconds(),
        Datum::Enum(value, _) => value.value() as i64,
        Datum::Set(value, _) => value.value() as i64,
        Datum::Json(value) => i64::from(value.type_code()),
        // Fresh setters for every remaining kind leave `i` at zero.
        _ => 0,
    }
}

fn datum_raw_float64(value: &Datum) -> f64 {
    f64::from_bits(datum_raw_i64(value) as u64)
}

fn datum_go_bytes(value: &Datum) -> Cow<'_, [u8]> {
    match value {
        // These two typed representations retain semantic data rather than
        // Go's backing `b` slice, but reproduce the same bytes on demand.
        Datum::Json(value) => Cow::Borrowed(value.value()),
        Datum::VectorFloat32(value) => Cow::Owned(value.serialize()),
        _ => Cow::Borrowed(value.go_bytes()),
    }
}

pub(crate) fn datum_common_prefix_length(lower: &Datum, upper: &Datum) -> usize {
    let lower = datum_go_bytes(lower);
    let upper = datum_go_bytes(upper);
    common_prefix_length(&[&lower, &upper])
}

pub(crate) fn bytes_to_scalar(bytes: &[u8], common_prefix_length: usize) -> f64 {
    if bytes.len() <= common_prefix_length {
        0.0
    } else {
        convert_bytes_to_scalar(&bytes[common_prefix_length..])
    }
}

/// `pkg/statistics/scalar.go`'s `calcFraction4Datums`, which chooses numeric
/// getters solely from `value`'s kind, even when either bound has another
/// representable fresh datum kind.
pub fn calc_fraction_from_datums(lower: &Datum, upper: &Datum, value: &Datum) -> f64 {
    match value {
        Datum::Float32(_) => calc_fraction(
            f64::from(datum_raw_float64(lower) as f32),
            f64::from(datum_raw_float64(upper) as f32),
            f64::from(datum_raw_float64(value) as f32),
        ),
        Datum::Real(_) => calc_fraction(
            datum_raw_float64(lower),
            datum_raw_float64(upper),
            datum_raw_float64(value),
        ),
        Datum::Int(_) => calc_fraction(
            datum_raw_i64(lower) as f64,
            datum_raw_i64(upper) as f64,
            datum_raw_i64(value) as f64,
        ),
        Datum::UInt(_) => calc_fraction(
            datum_raw_i64(lower) as u64 as f64,
            datum_raw_i64(upper) as u64 as f64,
            datum_raw_i64(value) as u64 as f64,
        ),
        Datum::Duration(_) => calc_fraction(
            datum_raw_i64(lower) as f64,
            datum_raw_i64(upper) as f64,
            datum_raw_i64(value) as f64,
        ),
        Datum::Decimal(_) | Datum::Time(_) => calc_fraction(
            convert_datum_to_scalar(lower, 0),
            convert_datum_to_scalar(upper, 0),
            convert_datum_to_scalar(value, 0),
        ),
        Datum::String(_) | Datum::Bytes(_) => {
            let common_prefix_length = datum_common_prefix_length(lower, upper);
            calc_fraction(
                convert_datum_to_scalar(lower, common_prefix_length),
                convert_datum_to_scalar(upper, common_prefix_length),
                convert_datum_to_scalar(value, common_prefix_length),
            )
        }
        _ => 0.5,
    }
}

#[cfg(test)]
mod tests {
    use super::{CoreTime, timestamp_nanoseconds};

    fn timestamp(core: CoreTime) -> tidb_datatype::Datum {
        tidb_datatype::Datum::Time(
            tidb_datatype::Time::new(core, tidb_datatype::TimeType::Timestamp, 0).unwrap(),
        )
    }

    /// Go `TestConvertMysqlTimeToScalarTimestampBounds` (8c6cb106410): zero
    /// and before-min TIMESTAMP values clamp to -1, MinTimestamp is 0, and an
    /// above-max value lands just after MaxTimestamp.
    #[test]
    fn test_convert_mysql_time_to_scalar_timestamp_bounds() {
        let zero = timestamp(CoreTime::from_date(0, 0, 0, 0, 0, 0, 0));
        assert_eq!(super::convert_datum_to_scalar(&zero, 0), -1.0);
        let min = timestamp(CoreTime::from_date(1970, 1, 1, 0, 0, 1, 0));
        assert_eq!(super::convert_datum_to_scalar(&min, 0), 0.0);
        let max = timestamp(CoreTime::from_date(2038, 1, 19, 3, 14, 7, 999_999));
        let after_max = timestamp(CoreTime::from_date(9999, 12, 31, 23, 59, 59, 0));
        assert!(super::convert_datum_to_scalar(&after_max, 0) > super::convert_datum_to_scalar(&max, 0));
        // Every above-max value sits one ULP after the legal maximum, so a
        // [MaxTimestamp, after-max] bucket keeps a non-zero width.
        let just_after_max = timestamp(CoreTime::from_date(2038, 1, 19, 3, 14, 8, 0));
        let max_scalar = super::convert_datum_to_scalar(&max, 0);
        assert_eq!(super::convert_datum_to_scalar(&just_after_max, 0), max_scalar.next_up());
        assert_eq!(super::convert_datum_to_scalar(&after_max, 0), max_scalar.next_up());
    }

    /// Go `TestCalcFraction`'s TIMESTAMP rows from 8c6cb106410.
    #[test]
    fn test_calc_fraction_timestamp_bounds() {
        let zero = timestamp(CoreTime::from_date(0, 0, 0, 0, 0, 0, 0));
        let lower_value = timestamp(CoreTime::from_date(1970, 1, 1, 0, 0, 2, 0));
        let upper = timestamp(CoreTime::from_date(1970, 1, 1, 0, 0, 3, 0));
        assert!((super::calc_fraction_from_datums(&zero, &upper, &lower_value) - 0.5).abs() < 1e-9);
        let max = timestamp(CoreTime::from_date(2038, 1, 19, 3, 14, 7, 999_999));
        let after_max = timestamp(CoreTime::from_date(9999, 12, 31, 23, 59, 59, 0));
        assert!((super::calc_fraction_from_datums(&max, &after_max, &after_max) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn timestamp_scalar_saturates_after_adding_microseconds() {
        for (timestamp, expected) in [
            (
                CoreTime::from_date(1677, 9, 21, 0, 12, 44, 145_224),
                i64::MIN,
            ),
            (
                CoreTime::from_date(1677, 9, 21, 0, 12, 44, 145_225),
                i64::MIN + 808,
            ),
            (
                CoreTime::from_date(2262, 4, 11, 23, 47, 17, 854_775),
                i64::MAX - 807,
            ),
            (
                CoreTime::from_date(2262, 4, 11, 23, 47, 17, 854_776),
                i64::MAX,
            ),
            (CoreTime::from_date(1970, 1, 1, 0, 0, 0, 999_999), -1_000),
        ] {
            assert_eq!(timestamp_nanoseconds(timestamp), expected, "{timestamp:?}");
        }
    }
}
