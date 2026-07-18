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

//! Canonical typed `SUM` partial states.

use tidb_datatype::{Datum, Decimal};
use tidb_expr::EvalError;

use crate::ExecError;

/// Checked integer overflow at the Go `types.Add/Sub{Int,Uint}64` boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SumIntError {
    /// The source integer domain overflowed.
    Overflow,
}

/// Source `partialResult4SumFloat64`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SumFloat64State {
    value: f64,
    non_null_row_count: i64,
}

impl SumFloat64State {
    /// Creates an empty floating-point SUM partial result.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            value: 0.0,
            non_null_row_count: 0,
        }
    }
    /// Resets the partial result to its empty representation.
    pub fn reset(&mut self) {
        *self = Self::new();
    }
    /// Accumulates the non-NULL values in one evaluated batch.
    pub fn update(&mut self, values: &[Option<f64>]) {
        for value in values.iter().flatten().copied() {
            self.value += value;
            self.non_null_row_count = self.non_null_row_count.wrapping_add(1);
        }
    }
    /// Merges one floating-point partial result into this destination.
    pub fn merge_from(&mut self, source: &Self) {
        if source.non_null_row_count == 0 {
            return;
        }
        self.value += source.value;
        self.non_null_row_count = self
            .non_null_row_count
            .wrapping_add(source.non_null_row_count);
    }
    /// Float/decimal SUM adds incoming rows before removing outgoing rows.
    pub fn slide(&mut self, outgoing: &[Option<f64>], incoming: &[Option<f64>]) {
        self.update(incoming);
        for value in outgoing.iter().flatten().copied() {
            self.value -= value;
            self.non_null_row_count = self.non_null_row_count.wrapping_sub(1);
        }
    }
    /// Returns NULL for an empty input, or the accumulated floating-point sum.
    #[must_use]
    pub fn result(&self) -> Option<f64> {
        (self.non_null_row_count != 0).then_some(self.value)
    }
    /// Returns the fixed width of the value-and-count partial result.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<f64>() + std::mem::size_of::<i64>()
    }
    pub(super) const fn parts(self) -> (f64, i64) {
        (self.value, self.non_null_row_count)
    }
    /// Reconstructs the two-field source partial result after spill decoding.
    #[must_use]
    pub const fn from_parts(value: f64, count: i64) -> Self {
        Self {
            value,
            non_null_row_count: count,
        }
    }
}

macro_rules! integer_sum_state {
    ($name:ident, $value:ty) => {
        /// Checked integer SUM partial state for one source integer domain.
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
        pub struct $name {
            value: $value,
            non_null_row_count: i64,
        }

        impl $name {
            /// Creates an empty checked-integer SUM partial result.
            #[must_use]
            pub const fn new() -> Self {
                Self {
                    value: 0,
                    non_null_row_count: 0,
                }
            }
            /// Resets the partial result to its empty representation.
            pub fn reset(&mut self) {
                *self = Self::new();
            }
            /// Accumulates non-NULL values and reports source-domain overflow.
            pub fn update(&mut self, values: &[Option<$value>]) -> Result<(), SumIntError> {
                for value in values.iter().flatten().copied() {
                    if self.non_null_row_count == 0 {
                        self.value = value;
                        self.non_null_row_count = 1;
                    } else {
                        self.value = self.value.checked_add(value).ok_or(SumIntError::Overflow)?;
                        self.non_null_row_count = self.non_null_row_count.wrapping_add(1);
                    }
                }
                Ok(())
            }
            /// Merges a compatible partial result and reports source-domain overflow.
            pub fn merge_from(&mut self, source: &Self) -> Result<(), SumIntError> {
                if source.non_null_row_count == 0 {
                    return Ok(());
                }
                if self.non_null_row_count == 0 {
                    *self = *source;
                    return Ok(());
                }
                self.value = self
                    .value
                    .checked_add(source.value)
                    .ok_or(SumIntError::Overflow)?;
                self.non_null_row_count = self
                    .non_null_row_count
                    .wrapping_add(source.non_null_row_count);
                Ok(())
            }
            /// Removes outgoing values, adds incoming values, and checks every operation.
            pub fn slide(
                &mut self,
                outgoing: &[Option<$value>],
                incoming: &[Option<$value>],
            ) -> Result<(), SumIntError> {
                for value in outgoing.iter().flatten().copied() {
                    self.value = self.value.checked_sub(value).ok_or(SumIntError::Overflow)?;
                    self.non_null_row_count = self.non_null_row_count.wrapping_sub(1);
                }
                for value in incoming.iter().flatten().copied() {
                    self.value = self.value.checked_add(value).ok_or(SumIntError::Overflow)?;
                    self.non_null_row_count = self.non_null_row_count.wrapping_add(1);
                }
                Ok(())
            }
            /// Returns NULL for an empty input, or the accumulated integer sum.
            #[must_use]
            pub const fn result(&self) -> Option<$value> {
                if self.non_null_row_count == 0 {
                    None
                } else {
                    Some(self.value)
                }
            }
            /// Returns the fixed width of the value-and-count partial result.
            #[must_use]
            pub const fn partial_state_size() -> usize {
                std::mem::size_of::<$value>() + std::mem::size_of::<i64>()
            }
        }
    };
}

integer_sum_state!(SumInt64State, i64);
integer_sum_state!(SumUint64State, u64);

/// Source decimal SUM state.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SumDecimalState {
    value: Option<Decimal>,
    non_null_row_count: i64,
}

impl SumDecimalState {
    /// Creates an empty decimal SUM partial result.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            value: None,
            non_null_row_count: 0,
        }
    }
    /// Adds one non-NULL decimal to the partial result.
    pub fn update_one(&mut self, value: &Decimal) {
        self.value = Some(match &self.value {
            Some(current) => current.add(value),
            None => value.clone(),
        });
        self.non_null_row_count = self.non_null_row_count.wrapping_add(1);
    }
    /// Merges a decimal partial result into this destination.
    pub fn merge_from(&mut self, source: &Self) {
        if let Some(value) = &source.value {
            self.value = Some(match &self.value {
                Some(current) => current.add(value),
                None => value.clone(),
            });
            self.non_null_row_count = self
                .non_null_row_count
                .wrapping_add(source.non_null_row_count);
        }
    }
    /// Adds incoming decimals before removing outgoing decimals.
    pub fn slide(&mut self, outgoing: &[Decimal], incoming: &[Decimal]) {
        for value in incoming {
            self.update_one(value);
        }
        for value in outgoing {
            let current = self.value.take().unwrap_or_else(|| Decimal::from_int(0));
            self.value = Some(current.add(&value.negate()));
            self.non_null_row_count = self.non_null_row_count.wrapping_sub(1);
        }
    }
    /// Returns NULL for an empty input, or the accumulated decimal sum.
    #[must_use]
    pub fn result(&self) -> Option<Decimal> {
        (self.non_null_row_count != 0)
            .then(|| self.value.clone())
            .flatten()
    }
}

/// Runtime SUM state selected from the evaluated Datum type.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum SumState {
    /// No non-NULL value has selected a numeric representation yet.
    #[default]
    Empty,
    /// Checked signed-integer partial result.
    Int(SumInt64State),
    /// Checked unsigned-integer partial result.
    UInt(SumUint64State),
    /// Floating-point partial result.
    Real(SumFloat64State),
    /// Exact decimal partial result.
    Decimal(SumDecimalState),
}

impl SumState {
    /// Creates an untyped empty SUM state.
    #[must_use]
    pub const fn new() -> Self {
        Self::Empty
    }

    /// Accumulates one numeric datum, selecting its runtime representation on first use.
    pub fn update(&mut self, value: &Datum) -> Result<(), ExecError> {
        if value.is_null() {
            return Ok(());
        }
        match (self, value) {
            (state @ Self::Empty, Datum::Int(value)) => {
                let mut next = SumInt64State::new();
                next.update(&[Some(*value)]).map_err(sum_overflow)?;
                *state = Self::Int(next);
            }
            (state @ Self::Empty, Datum::UInt(value)) => {
                let mut next = SumUint64State::new();
                next.update(&[Some(*value)]).map_err(sum_overflow)?;
                *state = Self::UInt(next);
            }
            (state @ Self::Empty, Datum::Real(value)) => {
                let mut next = SumFloat64State::new();
                next.update(&[Some(*value)]);
                *state = Self::Real(next);
            }
            (state @ Self::Empty, Datum::Decimal(value)) => {
                let mut next = SumDecimalState::new();
                next.update_one(value);
                *state = Self::Decimal(next);
            }
            (Self::Int(state), Datum::Int(value)) => {
                state.update(&[Some(*value)]).map_err(sum_overflow)?;
            }
            (Self::UInt(state), Datum::UInt(value)) => {
                state.update(&[Some(*value)]).map_err(sum_overflow)?;
            }
            (Self::Real(state), Datum::Real(value)) => state.update(&[Some(*value)]),
            (Self::Decimal(state), Datum::Decimal(value)) => state.update_one(value),
            _ => {
                return Err(ExecError::Eval(EvalError::Unsupported(
                    "SUM of mixed or non-numeric values",
                )))
            }
        }
        Ok(())
    }

    /// Merges a partial result with the same selected numeric representation.
    pub fn merge_from(&mut self, source: &Self) -> Result<(), ExecError> {
        match (self, source) {
            (_, Self::Empty) => Ok(()),
            (state @ Self::Empty, source) => {
                *state = source.clone();
                Ok(())
            }
            (Self::Int(destination), Self::Int(source)) => {
                destination.merge_from(source).map_err(sum_overflow)
            }
            (Self::UInt(destination), Self::UInt(source)) => {
                destination.merge_from(source).map_err(sum_overflow)
            }
            (Self::Real(destination), Self::Real(source)) => {
                destination.merge_from(source);
                Ok(())
            }
            (Self::Decimal(destination), Self::Decimal(source)) => {
                destination.merge_from(source);
                Ok(())
            }
            _ => Err(ExecError::Eval(EvalError::Unsupported(
                "SUM partial type mismatch",
            ))),
        }
    }

    /// Materializes the accumulated value as a datum, or NULL for an empty input.
    #[must_use]
    pub fn result(&self) -> Option<Datum> {
        match self {
            Self::Empty => None,
            Self::Int(state) => state.result().map(Datum::Int),
            Self::UInt(state) => state.result().map(Datum::UInt),
            Self::Real(state) => state.result().map(Datum::Real),
            Self::Decimal(state) => state.result().map(Datum::Decimal),
        }
    }
}

fn sum_overflow(_: SumIntError) -> ExecError {
    ExecError::Eval(EvalError::IntOverflow)
}
