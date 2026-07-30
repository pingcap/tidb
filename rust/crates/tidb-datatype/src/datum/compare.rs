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
    Collation, Decimal, MySqlDuration, MysqlEnum, MysqlSet, Time, VectorFloat32,
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
            Self::Enum(value, _) => self.compare_enum(value, comparer),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                self.compare_binary_literal(value, comparer)
            }
            Self::Set(value, _) => self.compare_set(value, comparer),
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
                Ok(comparer.compare(left.as_bytes(), value))
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

    fn compare_enum(
        &self,
        value: &MysqlEnum,
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => Ok(comparer.compare(left.bytes(), value.name().as_bytes())),
            Self::Bytes(left) => Ok(comparer.compare(left, value.name().as_bytes())),
            Self::Enum(left, _) => {
                Ok(comparer.compare(left.name().as_bytes(), value.name().as_bytes()))
            }
            Self::Set(left, _) => {
                Ok(comparer.compare(left.name().as_bytes(), value.name().as_bytes()))
            }
            _ => self.compare_f64(value.to_number()),
        }
    }

    fn compare_binary_literal(
        &self,
        value: &BinaryLiteral,
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => Ok(comparer.compare(left.bytes(), value.as_bytes())),
            Self::Bytes(left) => Ok(comparer.compare(left, value.as_bytes())),
            Self::BinaryLiteral(left) | Self::Bit(left) => {
                Ok(comparer.compare(left.as_bytes(), value.as_bytes()))
            }
            _ => self.compare_f64(value.to_int().value() as f64),
        }
    }

    fn compare_set(
        &self,
        value: &MysqlSet,
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => Ok(comparer.compare(left.bytes(), value.name().as_bytes())),
            Self::Bytes(left) => Ok(comparer.compare(left, value.name().as_bytes())),
            Self::Enum(left, _) => {
                Ok(comparer.compare(left.name().as_bytes(), value.name().as_bytes()))
            }
            Self::Set(left, _) => {
                Ok(comparer.compare(left.name().as_bytes(), value.name().as_bytes()))
            }
            _ => self.compare_f64(value.to_number()),
        }
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

    /// Source: `pkg/types/compare_test.go::TestCompare` and
    /// `TestCompareDatum`. Each row is also checked in reverse because the Go
    /// table makes antisymmetry part of the contract.
    #[test]
    fn source_compare_cross_kind_rows() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        let decimal_one = || Datum::new_decimal(Decimal::from_int(1));
        let literal = |value| Datum::new_binary_literal(BinaryLiteral::from_uint(value, None));
        let enum_one = || Datum::new_enum(parse_enum_value(&["a"], 1).unwrap(), Collation::Binary);
        let set_one = || Datum::new_set(parse_set_value(&["a"], 1).unwrap(), Collation::Binary);
        let zero_time = Datum::new_time(
            parse_datetime("0000-00-00 00:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        let later_time = Datum::new_time(
            parse_datetime("2011-01-01 11:11:11", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        let zero_duration = Datum::new_duration(MySqlDuration::from_nanoseconds(0, 0).unwrap());
        let positive_duration =
            Datum::new_duration(MySqlDuration::from_nanoseconds(29_034, 2).unwrap());

        let rows = vec![
            (Datum::Real(1.0), Datum::new_string("1"), Equal),
            (Datum::Int(-1), Datum::UInt(1), Less),
            (Datum::Int(-1), Datum::new_string("-1"), Equal),
            (Datum::UInt(1), Datum::new_string("1"), Equal),
            (decimal_one(), Datum::new_string("1"), Equal),
            (decimal_one(), Datum::new_bytes(b"1"), Equal),
            (Datum::new_string("1"), Datum::Int(-1), Greater),
            (Datum::new_string("1"), Datum::Real(2.0), Less),
            (
                Datum::new_string("hello"),
                Datum::new_decimal(Decimal::from_int(0)),
                Equal,
            ),
            (Datum::Null, Datum::Int(2), Less),
            (Datum::new_string(""), Datum::Null, Greater),
            (Datum::new_string("aasf"), Datum::new_string("4"), Greater),
            (Datum::new_bytes(b"abc"), Datum::new_bytes(b"ab"), Greater),
            (Datum::new_bytes(b"123"), Datum::Int(1234), Less),
            (literal(1), Datum::Int(1), Equal),
            (
                Datum::new_binary_literal(BinaryLiteral::from_uint(0x004D_7953_514C, None)),
                Datum::new_string("MySQL"),
                Equal,
            ),
            (literal(0), Datum::UInt(10), Less),
            (literal(1), decimal_one(), Equal),
            (enum_one(), Datum::Int(1), Equal),
            (enum_one(), Datum::new_string("a"), Equal),
            (enum_one(), Datum::UInt(10), Less),
            (enum_one(), literal(2), Less),
            (set_one(), Datum::Int(1), Equal),
            (set_one(), Datum::new_string("a"), Equal),
            (set_one(), enum_one(), Equal),
            (zero_time.clone(), later_time.clone(), Less),
            (
                later_time,
                Datum::new_string("0000-00-00 00:00:00"),
                Greater,
            ),
            (zero_duration.clone(), positive_duration, Less),
            (Datum::new_string("12:00:00"), zero_duration, Greater),
            (Datum::MaxValue, Datum::new_string("00:00:00"), Greater),
            (Datum::MinNotNull, Datum::new_string("00:00:00"), Less),
            (Datum::Null, Datum::MinNotNull, Less),
            (Datum::MinNotNull, Datum::MaxValue, Less),
            (
                Datum::new_json(BinaryJSON::parse("1").unwrap()),
                Datum::Null,
                Less,
            ),
        ];

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
}
