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

//! Checked integer arithmetic translated from `pkg/types/overflow.go`.
//!
//! The source returns TiDB's shared `ErrOverflow` object. This leaf preserves
//! the arithmetic and its source-visible error text without importing the
//! broader database error hierarchy into the datatype crate.

use std::{error::Error, fmt};

/// The source field family named by an overflow error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowType {
    /// Signed BIGINT arithmetic.
    BigInt,
    /// Unsigned BIGINT arithmetic.
    BigIntUnsigned,
}

impl OverflowType {
    const fn name(self) -> &'static str {
        match self {
            Self::BigInt => "BIGINT",
            Self::BigIntUnsigned => "BIGINT UNSIGNED",
        }
    }
}

/// A source-shaped out-of-range error for checked arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverflowError {
    kind: OverflowType,
    lhs: i128,
    rhs: i128,
}

impl OverflowError {
    const fn new(kind: OverflowType, lhs: i128, rhs: i128) -> Self {
        Self { kind, lhs, rhs }
    }

    /// Returns the source field family.
    #[must_use]
    pub const fn kind(self) -> OverflowType {
        self.kind
    }
}

impl fmt::Display for OverflowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} value is out of range in '({}, {})'",
            self.kind.name(),
            self.lhs,
            self.rhs
        )
    }
}

impl Error for OverflowError {}

fn unsigned_error(lhs: u64, rhs: impl Into<i128>) -> OverflowError {
    OverflowError::new(OverflowType::BigIntUnsigned, lhs as i128, rhs.into())
}

fn signed_error(lhs: i64, rhs: impl Into<i128>) -> OverflowError {
    OverflowError::new(OverflowType::BigInt, lhs as i128, rhs.into())
}

/// Adds two unsigned BIGINT values, rejecting overflow.
pub fn add_uint64(lhs: u64, rhs: u64) -> Result<u64, OverflowError> {
    lhs.checked_add(rhs)
        .ok_or_else(|| unsigned_error(lhs, rhs as i128))
}

/// Adds two signed BIGINT values, rejecting overflow.
pub fn add_int64(lhs: i64, rhs: i64) -> Result<i64, OverflowError> {
    lhs.checked_add(rhs)
        .ok_or_else(|| signed_error(lhs, rhs as i128))
}

/// Adds two `time.Duration`-shaped signed values.
pub fn add_duration(lhs: i64, rhs: i64) -> Result<i64, OverflowError> {
    add_int64(lhs, rhs)
}

/// Subtracts two `time.Duration`-shaped signed values.
pub fn sub_duration(lhs: i64, rhs: i64) -> Result<i64, OverflowError> {
    sub_int64(lhs, rhs)
}

/// Adds an unsigned BIGINT and a signed BIGINT, returning unsigned output.
pub fn add_integer(lhs: u64, rhs: i64) -> Result<u64, OverflowError> {
    if rhs >= 0 {
        return add_uint64(lhs, rhs as u64);
    }
    let magnitude = rhs.unsigned_abs();
    if magnitude > lhs {
        return Err(unsigned_error(lhs, rhs as i128));
    }
    Ok(lhs - magnitude)
}

/// Subtracts two unsigned BIGINT values.
pub fn sub_uint64(lhs: u64, rhs: u64) -> Result<u64, OverflowError> {
    lhs.checked_sub(rhs)
        .ok_or_else(|| unsigned_error(lhs, rhs as i128))
}

/// Subtracts two signed BIGINT values.
pub fn sub_int64(lhs: i64, rhs: i64) -> Result<i64, OverflowError> {
    lhs.checked_sub(rhs)
        .ok_or_else(|| signed_error(lhs, rhs as i128))
}

/// Subtracts a signed BIGINT from an unsigned BIGINT.
pub fn sub_uint_with_int(lhs: u64, rhs: i64) -> Result<u64, OverflowError> {
    if rhs < 0 {
        return add_uint64(lhs, rhs.unsigned_abs());
    }
    sub_uint64(lhs, rhs as u64)
}

/// Subtracts an unsigned BIGINT from a signed BIGINT, returning unsigned output.
pub fn sub_int_with_uint(lhs: i64, rhs: u64) -> Result<u64, OverflowError> {
    if lhs < 0 || (lhs as u64) < rhs {
        return Err(OverflowError::new(
            OverflowType::BigIntUnsigned,
            lhs as i128,
            rhs as i128,
        ));
    }
    Ok(lhs as u64 - rhs)
}

/// Multiplies two unsigned BIGINT values.
pub fn mul_uint64(lhs: u64, rhs: u64) -> Result<u64, OverflowError> {
    lhs.checked_mul(rhs)
        .ok_or_else(|| unsigned_error(lhs, rhs as i128))
}

/// Multiplies two signed BIGINT values.
pub fn mul_int64(lhs: i64, rhs: i64) -> Result<i64, OverflowError> {
    lhs.checked_mul(rhs)
        .ok_or_else(|| signed_error(lhs, rhs as i128))
}

/// Multiplies an unsigned BIGINT by a signed BIGINT, returning unsigned output.
pub fn mul_integer(lhs: u64, rhs: i64) -> Result<u64, OverflowError> {
    if lhs == 0 || rhs == 0 {
        return Ok(0);
    }
    if rhs < 0 {
        return Err(unsigned_error(lhs, rhs as i128));
    }
    mul_uint64(lhs, rhs as u64)
}

/// Divides two signed BIGINT values, rejecting `MIN / -1`.
pub fn div_int64(lhs: i64, rhs: i64) -> Result<i64, OverflowError> {
    assert_ne!(rhs, 0, "integer divide by zero");
    if lhs == i64::MIN && rhs == -1 {
        return Err(signed_error(lhs, rhs as i128));
    }
    Ok(lhs / rhs)
}

/// Divides an unsigned BIGINT by a signed BIGINT, returning unsigned output.
pub fn div_uint_with_int(lhs: u64, rhs: i64) -> Result<u64, OverflowError> {
    assert_ne!(rhs, 0, "integer divide by zero");
    if rhs < 0 {
        let magnitude = rhs.unsigned_abs();
        if lhs != 0 && magnitude <= lhs {
            return Err(unsigned_error(lhs, rhs as i128));
        }
        return Ok(0);
    }
    Ok(lhs / rhs as u64)
}

/// Divides a signed BIGINT by an unsigned BIGINT, returning unsigned output.
pub fn div_int_with_uint(lhs: i64, rhs: u64) -> Result<u64, OverflowError> {
    assert_ne!(rhs, 0, "integer divide by zero");
    if lhs < 0 {
        let magnitude = lhs.unsigned_abs();
        if magnitude >= rhs {
            return Err(OverflowError::new(
                OverflowType::BigIntUnsigned,
                lhs as i128,
                rhs as i128,
            ));
        }
        return Ok(0);
    }
    Ok(lhs as u64 / rhs)
}
