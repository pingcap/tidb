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

//! Datum arithmetic transcreated from `pkg/types/datum_eval.go`.

use std::fmt;

use crate::{
    add_int64, add_integer, add_uint64, Datum, DatumKind, DecimalCodecWarning, OverflowError,
};

/// `ComputePlus` failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatumArithmeticError {
    /// Source BIGINT overflow.
    Overflow(OverflowError),
    /// Source MyDecimal result overflow.
    DecimalOverflow,
    /// `InvOp2` rejects the operand-kind pair.
    InvalidOperands(DatumKind, DatumKind),
}

impl fmt::Display for DatumArithmeticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow(error) => error.fmt(formatter),
            Self::DecimalOverflow => formatter.write_str("decimal arithmetic overflow"),
            Self::InvalidOperands(left, right) => {
                write!(formatter, "invalid operation: {left:?} + {right:?}")
            }
        }
    }
}

impl std::error::Error for DatumArithmeticError {}

impl From<OverflowError> for DatumArithmeticError {
    fn from(error: OverflowError) -> Self {
        Self::Overflow(error)
    }
}

/// Computes the source-defined subset of `a + b`.
pub fn compute_plus(left: &Datum, right: &Datum) -> Result<Datum, DatumArithmeticError> {
    match (left, right) {
        (Datum::Int(left), Datum::Int(right)) => {
            add_int64(*left, *right).map(Datum::Int).map_err(Into::into)
        }
        (Datum::Int(left), Datum::UInt(right)) => add_integer(*right, *left)
            .map(Datum::UInt)
            .map_err(Into::into),
        (Datum::UInt(left), Datum::Int(right)) => add_integer(*left, *right)
            .map(Datum::UInt)
            .map_err(Into::into),
        (Datum::UInt(left), Datum::UInt(right)) => add_uint64(*left, *right)
            .map(Datum::UInt)
            .map_err(Into::into),
        (Datum::Real(left), Datum::Real(right)) => Ok(Datum::Real(left + right)),
        (Datum::Decimal(left), Datum::Decimal(right)) => {
            let (sum, warning) = left.add_mysql(right);
            if warning == Some(DecimalCodecWarning::Overflow) {
                Err(DatumArithmeticError::DecimalOverflow)
            } else {
                Ok(Datum::Decimal(sum))
            }
        }
        _ => Err(DatumArithmeticError::InvalidOperands(
            left.kind(),
            right.kind(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Decimal;

    #[test]
    fn source_compute_plus_rows() {
        for (left, right, expected) in [
            (Datum::Int(72), Datum::Int(28), Datum::Int(100)),
            (Datum::Int(72), Datum::UInt(28), Datum::UInt(100)),
            (Datum::UInt(72), Datum::UInt(28), Datum::UInt(100)),
            (Datum::UInt(72), Datum::Int(28), Datum::UInt(100)),
            (Datum::Real(72.0), Datum::Real(28.0), Datum::Real(100.0)),
            (
                Datum::Decimal(Decimal::from_signed_literal("72.5")),
                Datum::Decimal(Decimal::from_int(3)),
                Datum::Decimal(Decimal::from_signed_literal("75.5")),
            ),
        ] {
            assert_eq!(compute_plus(&left, &right).unwrap(), expected);
        }
        assert!(matches!(
            compute_plus(&Datum::Int(72), &Datum::Real(42.0)),
            Err(DatumArithmeticError::InvalidOperands(..))
        ));
        assert!(matches!(
            compute_plus(&Datum::new_string("abcd"), &Datum::Int(42)),
            Err(DatumArithmeticError::InvalidOperands(..))
        ));
    }
}
