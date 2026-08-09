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

use tidb_datatype::{CoreTime, Datum, Time, TimeType, DEFAULT_FSP};

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

const fn min_datetime_core() -> CoreTime {
    CoreTime::from_date(1, 1, 1, 0, 0, 0, 0)
}

const fn min_timestamp_core() -> CoreTime {
    CoreTime::from_date(1970, 1, 1, 0, 0, 1, 0)
}

fn time_to_scalar(value: Time) -> f64 {
    // Go subtracts a per-kind minimum time to get a `time.Duration` and
    // takes its nanosecond count. For DATE/DATETIME, Go's `Time.Sub` builds
    // that duration as `seconds*1e9 + microseconds*1e3` using plain `int64`
    // arithmetic, which silently wraps on overflow. `Time::sub` saturates,
    // so reproduce the source arithmetic directly for those two kinds.
    let kind = value.kind();
    if kind == TimeType::Timestamp {
        let minimum = Time::new(min_timestamp_core(), kind, DEFAULT_FSP)
            .expect("fixed minimum timestamp is valid");
        return match value.sub(minimum, &chrono_tz::UTC) {
            Ok(duration) => duration.nanoseconds() as f64,
            // CoreTime.GoTime returns Go time.Date's normalized instant and
            // an error. Time.Sub logs that error, then still subtracts the
            // normalized instant. The typed Rust conversion rejects first,
            // so derive the same fallback instant explicitly.
            Err(_) => normalized_timestamp_nanoseconds(value.core_time()) as f64,
        };
    }
    let difference = value.core_time().time_diff(min_datetime_core(), 1);
    let magnitude = difference
        .seconds
        .wrapping_mul(1_000_000_000)
        .wrapping_add(i64::from(difference.microseconds).wrapping_mul(1_000));
    (if difference.negative {
        -magnitude
    } else {
        magnitude
    }) as f64
}

/// `pkg/statistics/scalar.go`'s `convertDatumToScalar` over an already-decoded
/// datum.
#[must_use]
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

fn normalized_timestamp_nanoseconds(core: CoreTime) -> i64 {
    // Go time.Date first normalizes the month into the year, then treats day
    // one as the anchor and adds the remaining day and clock fields.
    let month_zero = i64::from(core.month()) - 1;
    let year = i64::from(core.year()) + month_zero.div_euclid(12);
    let month = month_zero.rem_euclid(12) as u32 + 1;
    let date = chrono::NaiveDate::from_ymd_opt(
        i32::try_from(year).expect("CoreTime year fits chrono"),
        month,
        1,
    )
    .expect("normalized CoreTime month is valid");
    let normalized = date
        .and_hms_opt(0, 0, 0)
        .expect("midnight is valid")
        .checked_add_signed(chrono::Duration::days(i64::from(core.day()) - 1))
        .and_then(|value| value.checked_add_signed(chrono::Duration::hours(i64::from(core.hour()))))
        .and_then(|value| {
            value.checked_add_signed(chrono::Duration::minutes(i64::from(core.minute())))
        })
        .and_then(|value| {
            value.checked_add_signed(chrono::Duration::seconds(i64::from(core.second())))
        })
        .and_then(|value| {
            value.checked_add_signed(chrono::Duration::microseconds(i64::from(
                core.microsecond(),
            )))
        })
        .expect("CoreTime normalization stays in chrono range");
    let minimum = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 1)
        .unwrap();
    let difference = normalized.signed_duration_since(minimum);
    difference.num_nanoseconds().unwrap_or_else(|| {
        if normalized < minimum {
            i64::MIN
        } else {
            i64::MAX
        }
    })
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

fn bytes_to_scalar(bytes: &[u8], common_prefix_length: usize) -> f64 {
    if bytes.len() <= common_prefix_length {
        0.0
    } else {
        convert_bytes_to_scalar(&bytes[common_prefix_length..])
    }
}

/// `pkg/statistics/scalar.go`'s `calcFraction4Datums`, which chooses numeric
/// getters solely from `value`'s kind, even when either bound has another
/// representable fresh datum kind.
#[must_use]
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
            let lower_bytes = datum_go_bytes(lower);
            let upper_bytes = datum_go_bytes(upper);
            let common_prefix_length =
                common_prefix_length(&[lower_bytes.as_ref(), upper_bytes.as_ref()]);
            calc_fraction(
                convert_datum_to_scalar(lower, common_prefix_length),
                convert_datum_to_scalar(upper, common_prefix_length),
                convert_datum_to_scalar(value, common_prefix_length),
            )
        }
        _ => 0.5,
    }
}
