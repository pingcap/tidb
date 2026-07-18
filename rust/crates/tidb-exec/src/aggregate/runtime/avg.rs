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

//! Canonical AVG partial state and its source float specialization.

use tidb_ast::BinaryOp;
use tidb_datatype::Datum;
use tidb_expr::{apply_binary, avg_of_with_div_precision};

use crate::ExecError;

/// Source `partialResult4AvgFloat64`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AvgFloat64State {
    sum: f64,
    count: i64,
}

impl AvgFloat64State {
    /// Creates an empty real-valued AVG accumulator.
    #[must_use]
    pub const fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
    /// Restores the accumulator to the empty-group state.
    pub fn reset(&mut self) {
        *self = Self::new();
    }
    /// Adds every non-NULL real input to the running sum and count.
    pub fn update(&mut self, values: &[Option<f64>]) {
        for value in values.iter().flatten().copied() {
            self.sum += value;
            self.count = self.count.wrapping_add(1);
        }
    }
    /// Combines another partial sum and count into this accumulator.
    pub fn merge_from(&mut self, source: &Self) {
        self.sum += source.sum;
        self.count = self.count.wrapping_add(source.count);
    }
    /// Advances a real-valued window in Go AVG order: incoming rows first,
    /// followed by outgoing rows.
    pub fn slide(&mut self, outgoing: &[Option<f64>], incoming: &[Option<f64>]) {
        self.update(incoming);
        for value in outgoing.iter().flatten().copied() {
            self.sum -= value;
            self.count = self.count.wrapping_sub(1);
        }
    }
    /// Returns `None` for an empty group, otherwise `sum / count`.
    #[must_use]
    pub fn result(&self) -> Option<f64> {
        (self.count != 0).then_some(self.sum / self.count as f64)
    }
    /// Returns the fixed storage width of the real sum/count pair.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<f64>() + std::mem::size_of::<i64>()
    }
    pub(super) const fn parts(self) -> (f64, i64) {
        (self.sum, self.count)
    }
    /// Reconstructs the two-field source partial result after spill decoding.
    #[must_use]
    pub const fn from_parts(sum: f64, count: i64) -> Self {
        Self { sum, count }
    }
}

/// Runtime AVG state over the executor's already-resolved scalar Datum domain.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AvgState {
    sum: Option<Datum>,
    count: i64,
}

impl AvgState {
    /// Creates an empty canonical AVG accumulator.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            sum: None,
            count: 0,
        }
    }

    /// Restores the canonical accumulator to the empty-group state.
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Adds one resolved non-NULL numeric value to the sum and count.
    pub fn update(&mut self, value: &Datum) -> Result<(), ExecError> {
        if value.is_null() {
            return Ok(());
        }
        self.sum = Some(match self.sum.take() {
            Some(sum) => apply_binary(BinaryOp::Plus, sum, value.clone())?,
            None => value.clone(),
        });
        self.count = self.count.wrapping_add(1);
        Ok(())
    }

    /// Combines another canonical partial sum and count into this accumulator.
    pub fn merge_from(&mut self, source: &Self) -> Result<(), ExecError> {
        let Some(source_sum) = &source.sum else {
            return Ok(());
        };
        self.sum = Some(match self.sum.take() {
            Some(sum) => apply_binary(BinaryOp::Plus, sum, source_sum.clone())?,
            None => source_sum.clone(),
        });
        self.count = self.count.wrapping_add(source.count);
        Ok(())
    }

    /// Advances a canonical window in Go AVG order: incoming rows first,
    /// followed by outgoing rows.
    pub fn slide(&mut self, outgoing: &[Datum], incoming: &[Datum]) -> Result<(), ExecError> {
        for value in incoming {
            self.update(value)?;
        }
        for value in outgoing {
            if value.is_null() {
                continue;
            }
            let sum = self.sum.take().unwrap_or(Datum::Int(0));
            self.sum = Some(apply_binary(BinaryOp::Minus, sum, value.clone())?);
            self.count = self.count.wrapping_sub(1);
        }
        Ok(())
    }

    /// Produces NULL for an empty group or divides the sum by its count using
    /// the session's decimal precision increment.
    pub fn result(&self, div_precision_increment: u32) -> Result<Datum, ExecError> {
        match &self.sum {
            Some(sum) => Ok(avg_of_with_div_precision(
                sum.clone(),
                self.count,
                div_precision_increment,
            )?),
            None => Ok(Datum::Null),
        }
    }
}
