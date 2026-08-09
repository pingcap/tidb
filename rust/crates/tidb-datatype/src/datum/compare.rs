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

//! Cross-kind datum ordering.
//!
//! Mirrors the `Datum.Compare` family of `pkg/types/datum.go`: the public
//! entry point plus the per-right-hand-kind helpers (`compareInt64`,
//! `compareFloat64`, `compareString`, `compareMysqlDecimal`,
//! `compareMysqlDuration`, `compareMysqlEnum`, `compareBinaryLiteral`,
//! `compareMysqlSet`, `compareMysqlJSON`, `compareMysqlTime`,
//! `compareVectorFloat32`) and the NULL/range-sentinel rank they share.

use std::cmp::Ordering;

use super::{decimal_from_bytes, Datum, DatumValueError};
use crate::{
    compare_binary_json, parse_datetime, parse_duration, str_to_float, BinaryJSON, BinaryLiteral,
    Collation, Decimal, MySqlDuration, Time, VectorFloat32,
};

impl Datum {
    /// Compares the source-defined NULL/range-sentinel part of `Datum.Compare`.
    ///
    /// Go TiDB orders these four classes as
    /// `NULL < MinNotNull < ordinary scalar < MaxValue`. If both operands are
    /// ordinary scalars, this returns `None`: their comparison needs the
    /// caller's statement context and collator and must not be guessed by the
    /// dependency-leaf representation crate.
    pub fn compare_sentinel_order(&self, other: &Self) -> Option<Ordering> {
        match (sentinel_rank(self), sentinel_rank(other)) {
            (Some(left), Some(right)) => Some(left.cmp(&right)),
            (Some(SentinelRank::MaxValue), None) => Some(Ordering::Greater),
            (Some(_), None) => Some(Ordering::Less),
            (None, Some(SentinelRank::MaxValue)) => Some(Ordering::Less),
            (None, Some(_)) => Some(Ordering::Greater),
            (None, None) => None,
        }
    }

    /// Source `Datum.Compare` with an explicit collator.
    ///
    /// This follows Go's right-hand-kind dispatch. That detail matters for
    /// mixed numeric, string, temporal, enum/set, binary-literal, and JSON
    /// comparisons because the right operand selects the conversion domain.
    pub fn compare(&self, other: &Self, comparer: Collation) -> Result<Ordering, DatumValueError> {
        if matches!(self, Self::Json(_)) && !matches!(other, Self::Json(_)) {
            return other.compare(self, comparer).map(Ordering::reverse);
        }
        if let Self::Json(value) = other {
            return self.compare_json(value);
        }
        if let Some(ordering) = self.compare_sentinel_order(other) {
            return Ok(ordering);
        }
        match other {
            Self::Int(value) => self.compare_i64(*value),
            Self::UInt(value) => self.compare_u64(*value),
            Self::Real(value) | Self::Float32(value) => self.compare_f64(*value),
            Self::String(value) => self.compare_string(value.bytes(), comparer),
            Self::Bytes(value) => self.compare_string(value, comparer),
            Self::Decimal(value) => self.compare_decimal(value),
            Self::Duration(value) => self.compare_duration(*value),
            Self::Enum(value, _) => {
                self.compare_named_number(value.name_bytes(), value.to_number(), comparer)
            }
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                self.compare_binary_literal(value, comparer)
            }
            Self::Set(value, _) => {
                self.compare_named_number(value.name_bytes(), value.to_number(), comparer)
            }
            Self::Json(_) => unreachable!("JSON comparison returned above"),
            Self::Time(value) => self.compare_time(*value),
            Self::VectorFloat32(value) => self.compare_vector(value),
            Self::Raw(_) => Ok(Ordering::Equal),
            Self::Null | Self::MinNotNull | Self::MaxValue => {
                unreachable!("sentinel comparison returned above")
            }
        }
    }

    fn compare_i64(&self, value: i64) -> Result<Ordering, DatumValueError> {
        match self {
            Self::Int(left) => Ok(left.cmp(&value)),
            Self::UInt(left) => Ok(if value < 0 {
                Ordering::Greater
            } else {
                left.cmp(&(value as u64))
            }),
            _ => self.compare_f64(value as f64),
        }
    }

    fn compare_u64(&self, value: u64) -> Result<Ordering, DatumValueError> {
        match self {
            Self::Int(left) => Ok(if *left < 0 {
                Ordering::Less
            } else {
                (*left as u64).cmp(&value)
            }),
            Self::UInt(left) => Ok(left.cmp(&value)),
            _ => self.compare_f64(value as f64),
        }
    }

    fn compare_f64(&self, value: f64) -> Result<Ordering, DatumValueError> {
        let left = match self {
            Self::Int(left) => *left as f64,
            Self::UInt(left) => *left as f64,
            Self::Real(left) | Self::Float32(left) => *left,
            Self::String(left) => numeric_bytes_to_float(left.bytes())?,
            Self::Bytes(left) => numeric_bytes_to_float(left)?,
            Self::Decimal(left) => left.to_f64(),
            Self::Duration(left) => left.nanoseconds() as f64 / 1_000_000_000.0,
            Self::Enum(left, _) => left.to_number(),
            Self::BinaryLiteral(left) | Self::Bit(left) => left.to_int().value() as f64,
            Self::Set(left, _) => left.to_number(),
            Self::Time(left) => left.to_number().to_f64(),
            _ => return Ok(Ordering::Less),
        };
        Ok(float_order(left, value))
    }

    fn compare_string(
        &self,
        value: &[u8],
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => Ok(comparer.compare(left.bytes(), value)),
            Self::Bytes(left) => Ok(comparer.compare(left, value)),
            Self::Decimal(left) => Ok(left.cmp(&decimal_from_bytes(value)?.value)),
            Self::Time(left) => {
                let text = std::str::from_utf8(value)?;
                let parsed = parse_datetime(text, &chrono_tz::UTC, true, false)
                    .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
                Ok(left.compare(parsed.time))
            }
            Self::Duration(left) => {
                let parsed = parse_duration(value, 6)
                    .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
                Ok(left.nanoseconds().cmp(&parsed.nanoseconds()))
            }
            Self::Set(left, _) => Ok(comparer.compare(left.name().as_bytes(), value)),
            Self::Enum(left, _) => Ok(comparer.compare(left.name().as_bytes(), value)),
            Self::BinaryLiteral(left) | Self::Bit(left) => {
                Ok(comparer.compare(left.compare_bytes(), value))
            }
            _ => self.compare_f64(numeric_bytes_to_float(value)?),
        }
    }

    fn compare_decimal(&self, value: &Decimal) -> Result<Ordering, DatumValueError> {
        let left = match self {
            Self::Decimal(left) => left.clone(),
            Self::String(left) => decimal_from_bytes(left.bytes())?.value,
            Self::Bytes(left) => decimal_from_bytes(left)?.value,
            _ => self.to_decimal()?.value,
        };
        Ok(left.cmp(value))
    }

    fn compare_duration(&self, value: MySqlDuration) -> Result<Ordering, DatumValueError> {
        match self {
            Self::Duration(left) => Ok(left.compare(value)),
            Self::String(left) => compare_duration_bytes(left.bytes(), value),
            Self::Bytes(left) => compare_duration_bytes(left, value),
            _ => self.compare_f64(value.nanoseconds() as f64 / 1_000_000_000.0),
        }
    }

    /// The shared rule of `compareMysqlEnum` and `compareMysqlSet`, whose Go
    /// bodies are identical: a right-hand ENUM or SET compares by NAME when
    /// the left operand also has a string view, and by its ordinal NUMBER
    /// against every other kind.
    fn compare_named_number(
        &self,
        name: &[u8],
        number: f64,
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        let left = match self {
            Self::String(left) => left.bytes(),
            Self::Bytes(left) => left,
            Self::Enum(left, _) => left.name().as_bytes(),
            Self::Set(left, _) => left.name().as_bytes(),
            _ => return self.compare_f64(number),
        };
        Ok(comparer.compare(left, name))
    }

    fn compare_binary_literal(
        &self,
        value: &BinaryLiteral,
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        // Go's `KindString`/`KindBytes` case falls through into the literal
        // case, so both operands are read through `GetBinaryLiteral4Cmp`.
        let left = match self {
            Self::String(left) => left.bytes(),
            Self::Bytes(left) => left,
            Self::BinaryLiteral(left) | Self::Bit(left) => left.as_bytes(),
            _ => return self.compare_f64(value.to_int().value() as f64),
        };
        Ok(comparer.compare(BinaryLiteral::compare_bytes_of(left), value.compare_bytes()))
    }

    fn compare_json(&self, value: &BinaryJSON) -> Result<Ordering, DatumValueError> {
        if matches!(self, Self::Null) {
            return Ok(Ordering::Greater);
        }
        Ok(compare_binary_json(&self.to_mysql_json()?, value))
    }

    fn compare_time(&self, value: Time) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => compare_time_bytes(left.bytes(), value),
            Self::Bytes(left) => compare_time_bytes(left, value),
            Self::Time(left) => Ok(left.compare(value)),
            _ => self.compare_f64(value.to_number().to_f64()),
        }
    }

    fn compare_vector(&self, value: &VectorFloat32) -> Result<Ordering, DatumValueError> {
        match self {
            Self::VectorFloat32(left) => Ok(left.compare(value)),
            _ => Err(DatumValueError::Comparison(
                "cannot compare vector and non-vector, cast is required".to_owned(),
            )),
        }
    }
}

fn numeric_bytes_to_float(bytes: &[u8]) -> Result<f64, DatumValueError> {
    Ok(str_to_float(std::str::from_utf8(bytes)?, false).value)
}

fn compare_duration_bytes(bytes: &[u8], value: MySqlDuration) -> Result<Ordering, DatumValueError> {
    let parsed =
        parse_duration(bytes, 6).map_err(|error| DatumValueError::Comparison(error.to_string()))?;
    Ok(parsed.nanoseconds().cmp(&value.nanoseconds()))
}

fn compare_time_bytes(bytes: &[u8], value: Time) -> Result<Ordering, DatumValueError> {
    let text = std::str::from_utf8(bytes)?;
    let parsed = parse_datetime(text, &chrono_tz::UTC, true, false)
        .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
    Ok(parsed.time.compare(value))
}

fn float_order(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or_else(|| {
        if left.is_nan() {
            if right.is_nan() {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        } else {
            Ordering::Greater
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SentinelRank {
    Null,
    MinNotNull,
    MaxValue,
}

const fn sentinel_rank(datum: &Datum) -> Option<SentinelRank> {
    match datum {
        Datum::Null => Some(SentinelRank::Null),
        Datum::MinNotNull => Some(SentinelRank::MinNotNull),
        Datum::MaxValue => Some(SentinelRank::MaxValue),
        Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Decimal(_)
        | Datum::Real(_)
        | Datum::Float32(_)
        | Datum::String(_)
        | Datum::Bytes(_)
        | Datum::BinaryLiteral(_)
        | Datum::Duration(_)
        | Datum::Enum(_, _)
        | Datum::Bit(_)
        | Datum::Set(_, _)
        | Datum::Time(_)
        | Datum::Json(_)
        | Datum::Raw(_)
        | Datum::VectorFloat32(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::Datum;
    use crate::{
        parse_datetime, parse_enum_value, parse_set_value, BinaryJSON, BinaryLiteral, Collation,
        Decimal, MySqlDuration,
    };

    /// Source: `pkg/types/compare_test.go::TestCompare`. Every source row is
    /// kept, including duplicates whose Go operands differ only by `int`
    /// versus `int64`. Each row is also checked in reverse because the Go
    /// table makes antisymmetry part of the contract.
    #[test]
    fn test_compare() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        let decimal_zero = || Datum::new_decimal(Decimal::from_int(0));
        let decimal_one = || Datum::new_decimal(Decimal::from_int(1));
        let literal = |value| Datum::new_binary_literal(BinaryLiteral::from_uint(value, None));
        let enum_one = || Datum::new_enum(parse_enum_value(&["a"], 1).unwrap(), Collation::Binary);
        let set_one = || Datum::new_set(parse_set_value(&["a"], 1).unwrap(), Collation::Binary);
        let zero_time = Datum::new_time(
            parse_datetime("0000-00-00 00:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        // The Go source uses `time.Now()` and `time.Now().Add(10s)`. Fixed
        // instants preserve the intended ordering without making the test
        // depend on wall-clock time.
        let current_time = Datum::new_time(
            parse_datetime("2026-08-10 12:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        let future_time = Datum::new_time(
            parse_datetime("2026-08-10 12:00:10", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        let zero_duration = Datum::new_duration(MySqlDuration::from_nanoseconds(0, 0).unwrap());
        let duration = |nanoseconds| {
            Datum::new_duration(MySqlDuration::from_nanoseconds(nanoseconds, 2).unwrap())
        };

        let rows = vec![
            (Datum::Real(1.0), Datum::Real(1.0), Equal),
            (Datum::Real(1.0), Datum::new_string("1"), Equal),
            (Datum::Int(1), Datum::Int(1), Equal),
            (Datum::Int(-1), Datum::UInt(1), Less),
            (Datum::Int(-1), Datum::new_string("-1"), Equal),
            (Datum::UInt(1), Datum::UInt(1), Equal),
            (Datum::UInt(1), Datum::Int(-1), Greater),
            (Datum::UInt(1), Datum::new_string("1"), Equal),
            (decimal_one(), decimal_one(), Equal),
            (decimal_one(), Datum::new_string("1"), Equal),
            (decimal_one(), Datum::new_bytes(b"1"), Equal),
            (Datum::new_string("1"), Datum::new_string("1"), Equal),
            (Datum::new_string("1"), Datum::Int(-1), Greater),
            (Datum::new_string("1"), Datum::Real(2.0), Less),
            (Datum::new_string("1"), Datum::UInt(1), Equal),
            (Datum::new_string("1"), decimal_one(), Equal),
            (
                Datum::new_string("2011-01-01 11:11:11"),
                current_time.clone(),
                Less,
            ),
            (
                Datum::new_string("12:00:00"),
                zero_duration.clone(),
                Greater,
            ),
            (zero_duration.clone(), zero_duration.clone(), Equal),
            (future_time, current_time.clone(), Greater),
            (Datum::Null, Datum::Int(2), Less),
            (Datum::Null, Datum::Null, Equal),
            (Datum::Int(0), Datum::Null, Greater),
            (Datum::Int(0), Datum::Int(1), Less),
            (Datum::Int(1), Datum::Int(1), Equal),
            (Datum::Int(0), Datum::Int(0), Equal),
            (Datum::Int(1), Datum::Int(2), Less),
            (Datum::Real(1.23), Datum::Null, Greater),
            (Datum::Real(0.0), Datum::Real(3.45), Less),
            (Datum::Real(354.23), Datum::Real(3.45), Greater),
            (Datum::Real(3.452), Datum::Real(3.452), Equal),
            (Datum::Int(432), Datum::Null, Greater),
            (Datum::Int(-4), Datum::Int(32), Less),
            (Datum::Int(4), Datum::Int(-32), Greater),
            (Datum::Int(432), Datum::Int(12), Greater),
            (Datum::Int(23), Datum::Int(128), Less),
            (Datum::Int(123), Datum::Int(123), Equal),
            (Datum::Int(432), Datum::Int(12), Greater),
            (Datum::Int(23), Datum::Int(123), Less),
            (Datum::Int(133), Datum::Int(183), Less),
            (Datum::UInt(133), Datum::UInt(183), Less),
            (Datum::UInt(2), Datum::Int(-2), Greater),
            (Datum::UInt(2), Datum::Int(1), Greater),
            (Datum::new_string(""), Datum::Null, Greater),
            (Datum::new_string(""), Datum::new_string("24"), Less),
            (Datum::new_string("aasf"), Datum::new_string("4"), Greater),
            (Datum::new_string(""), Datum::new_string(""), Equal),
            (Datum::new_bytes(b""), Datum::Null, Greater),
            (Datum::new_bytes(b""), Datum::new_bytes(b"sff"), Less),
            (zero_time.clone(), Datum::Null, Greater),
            (zero_time.clone(), current_time.clone(), Less),
            (
                current_time,
                Datum::new_string("0000-00-00 00:00:00"),
                Greater,
            ),
            (duration(34), Datum::Null, Greater),
            (duration(34), duration(29_034), Less),
            (duration(3_340), duration(34), Greater),
            (duration(34), duration(34), Equal),
            (Datum::new_bytes(b""), Datum::new_bytes(b""), Equal),
            (Datum::new_bytes(b"abc"), Datum::new_bytes(b"ab"), Greater),
            (Datum::new_bytes(b"123"), Datum::Int(1234), Less),
            (Datum::new_bytes(b""), Datum::Null, Greater),
            (literal(1), Datum::Int(1), Equal),
            (
                Datum::new_binary_literal(BinaryLiteral::from_uint(0x004D_7953_514C, None)),
                Datum::new_string("MySQL"),
                Equal,
            ),
            (literal(0), Datum::UInt(10), Less),
            (literal(1), Datum::Real(0.0), Greater),
            (literal(1), decimal_one(), Equal),
            (literal(1), literal(0), Greater),
            (literal(1), literal(1), Equal),
            (enum_one(), Datum::Int(1), Equal),
            (enum_one(), Datum::new_string("a"), Equal),
            (enum_one(), Datum::UInt(10), Less),
            (enum_one(), Datum::Real(0.0), Greater),
            (enum_one(), decimal_one(), Equal),
            (enum_one(), literal(2), Less),
            (enum_one(), literal(1), Equal),
            (enum_one(), enum_one(), Equal),
            (set_one(), Datum::Int(1), Equal),
            (set_one(), Datum::new_string("a"), Equal),
            (set_one(), Datum::UInt(10), Less),
            (set_one(), Datum::Real(0.0), Greater),
            (set_one(), decimal_one(), Equal),
            (set_one(), literal(2), Less),
            (set_one(), literal(1), Equal),
            (set_one(), enum_one(), Equal),
            (set_one(), set_one(), Equal),
            (Datum::new_string("hello"), decimal_zero(), Equal),
            (decimal_zero(), Datum::new_string("hello"), Equal),
        ];
        assert_eq!(rows.len(), 86, "one entry per Go TestCompare source row");

        for (index, (left, right, expected)) in rows.into_iter().enumerate() {
            assert_eq!(
                left.compare(&right, Collation::Binary).unwrap(),
                expected,
                "forward row {index}: {left:?} versus {right:?}"
            );
            assert_eq!(
                right.compare(&left, Collation::Binary).unwrap(),
                expected.reverse(),
                "reverse row {index}: {right:?} versus {left:?}"
            );
        }
    }

    /// Source: `pkg/types/datum.go::Datum.GetBinaryLiteral4Cmp`. A `BIT(16)`
    /// payload is stored zero-padded, so comparison must not let the declared
    /// width decide the order: `b'1'` is `b'1'` at every width.
    #[test]
    fn bit_comparison_ignores_declared_width() {
        let literal = |value| Datum::new_binary_literal(BinaryLiteral::from_uint(value, None));
        let rows = [
            (
                Datum::new_mysql_bit(BinaryLiteral::from(vec![0x00, 0x01])),
                literal(1),
            ),
            (
                Datum::new_mysql_bit(BinaryLiteral::from(vec![0x00, 0x01])),
                Datum::new_string(vec![0x01]),
            ),
            (
                Datum::new_mysql_bit(BinaryLiteral::from(vec![0x00, 0x00])),
                literal(0),
            ),
        ];

        for (left, right) in rows {
            assert_eq!(
                left.compare(&right, Collation::Binary).unwrap(),
                std::cmp::Ordering::Equal
            );
            assert_eq!(
                right.compare(&left, Collation::Binary).unwrap(),
                std::cmp::Ordering::Equal
            );
        }
    }

    #[test]
    fn test_null_not_equal_with_others() {
        let zero_time = Datum::new_time(
            parse_datetime("0000-00-00 00:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        let zero_duration = Datum::new_duration(MySqlDuration::from_nanoseconds(0, 0).unwrap());
        let datums = vec![
            Datum::Int(0),
            Datum::UInt(0),
            Datum::Float32(0.0),
            Datum::Real(0.0),
            Datum::Real(f64::INFINITY),
            Datum::new_decimal(Decimal::from_int(0)),
            Datum::new_string(""),
            Datum::new_collation_string("", Collation::Binary),
            zero_duration,
            zero_time,
            Datum::new_bytes([]),
            Datum::new_binary_literal(BinaryLiteral::from(Vec::<u8>::new())),
            Datum::new_mysql_bit(BinaryLiteral::from(vec![0, 0, 0, 0])),
            Datum::new_json(BinaryJSON::parse("null").unwrap()),
            Datum::MinNotNull,
            Datum::MaxValue,
        ];

        for datum in datums {
            assert_ne!(
                datum.compare(&Datum::Null, Collation::Binary).unwrap(),
                std::cmp::Ordering::Equal,
                "{datum:?}"
            );
        }
    }
}
