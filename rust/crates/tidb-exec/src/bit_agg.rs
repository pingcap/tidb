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

//! Bitwise aggregate partial state from `pkg/executor/aggfuncs/func_bitfuncs.go`.
//!
//! The Go bitwise aggregates share a uint64 partial result with operation
//! identities: zero for OR/XOR and `MaxUint64` for AND. NULL inputs are
//! skipped, updates fold source-order integers, and partial merges apply the
//! same operation. XOR removes departing rows by applying XOR again before it
//! adds arriving rows, and the spill representation is the source's native
//! eight-byte uint64 representation. Descriptor-driven typed `EvalInt`
//! coercion, chunk output, and executor memory tracking remain external.

use std::mem::size_of;

use tidb_datatype::Datum;
use tidb_planner::aggregation_descriptor::AggregateKind;

use crate::ExecError;

/// A source bitwise aggregate operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitAggregateKind {
    /// Bitwise AND, whose empty identity is all ones.
    And,
    /// Bitwise OR, whose empty identity is zero.
    Or,
    /// Bitwise XOR, whose empty identity is zero.
    Xor,
}

/// The shared source-shaped `partialResult4BitFunc` state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitAggregate {
    kind: BitAggregateKind,
    value: u64,
}

impl BitAggregate {
    /// Creates a state with the source operation identity.
    #[must_use]
    pub const fn new(kind: BitAggregateKind) -> Self {
        let value = match kind {
            BitAggregateKind::And => u64::MAX,
            BitAggregateKind::Or | BitAggregateKind::Xor => 0,
        };
        Self { kind, value }
    }

    /// Resets the state to the source operation identity.
    pub fn reset(&mut self) {
        self.value = match self.kind {
            BitAggregateKind::And => u64::MAX,
            BitAggregateKind::Or | BitAggregateKind::Xor => 0,
        };
    }

    /// Folds a source update batch, skipping NULL values.
    ///
    /// Go first obtains an `int64` from `EvalInt`, then converts it to
    /// `uint64`. The current Rust evaluator already carries signed and
    /// unsigned integer values as distinct Datum variants, so both preserve
    /// their raw uint64 bits here. Other Datum domains require descriptor-
    /// driven `EvalInt` coercion that this leaf deliberately does not guess.
    pub fn update(&mut self, values: &[Datum]) -> Result<(), ExecError> {
        for value in values {
            match value {
                Datum::Null => {}
                Datum::Int(value) => self.fold(*value as u64),
                Datum::UInt(value) => self.fold(*value),
                _ => return Err(ExecError::Unsupported("BIT aggregate EvalInt coercion")),
            }
        }
        Ok(())
    }

    /// Merges a source partial state using the same operation.
    pub fn merge_from(&mut self, source: &Self) -> Result<(), ExecError> {
        if self.kind != source.kind {
            return Err(ExecError::Unsupported("BIT aggregate kind mismatch"));
        }
        self.fold(source.value);
        Ok(())
    }

    /// Applies Go `bitXorUint64.Slide`: departing and arriving values are
    /// both XORed into the same state, with NULLs skipped.
    pub fn slide_xor(&mut self, departing: &[Datum], arriving: &[Datum]) -> Result<(), ExecError> {
        if self.kind != BitAggregateKind::Xor {
            return Err(ExecError::Unsupported("BIT_AND/BIT_OR sliding aggregate"));
        }
        self.update(departing)?;
        self.update(arriving)
    }

    fn fold(&mut self, value: u64) {
        self.value = match self.kind {
            BitAggregateKind::And => self.value & value,
            BitAggregateKind::Or => self.value | value,
            BitAggregateKind::Xor => self.value ^ value,
        };
    }

    /// Returns the aggregate's current uint64 result.
    #[must_use]
    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Returns the source partial-state size.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<u64>()
    }

    /// Serializes the partial result exactly as Go `SerializeUint64`: the
    /// host-native eight-byte representation, without a tag or length prefix.
    #[must_use]
    pub const fn serialize_partial(&self) -> [u8; size_of::<u64>()] {
        self.value.to_ne_bytes()
    }

    /// Reconstructs one source partial result from a spill row.
    ///
    /// Go's `DeserializeUint64` consumes the first native-width value from
    /// the row buffer and does not require that the value exhaust the row.
    /// Preserve that cursor-shaped contract: a short row is malformed, while
    /// trailing bytes belong to the caller's remaining buffer.
    pub fn deserialize_partial(kind: BitAggregateKind, bytes: &[u8]) -> Result<Self, ExecError> {
        let bytes = bytes
            .get(..size_of::<u64>())
            .ok_or(ExecError::Unsupported("BIT aggregate spill payload length"))?;
        let bytes: [u8; size_of::<u64>()] = bytes
            .try_into()
            .map_err(|_| ExecError::Unsupported("BIT aggregate spill payload length"))?;
        Ok(Self {
            kind,
            value: u64::from_ne_bytes(bytes),
        })
    }
}

/// Folds already-evaluated integer Datums through the canonical bit state.
///
/// This is the leaf API consumed by the shared aggregate runtime. It performs
/// a source-shaped partial merge before returning an exact unsigned result.
pub fn fold_bit_values(kind: AggregateKind, values: &[Datum]) -> Result<Datum, ExecError> {
    let kind = match kind {
        AggregateKind::BitAnd => BitAggregateKind::And,
        AggregateKind::BitOr => BitAggregateKind::Or,
        AggregateKind::BitXor => BitAggregateKind::Xor,
        _ => return Err(ExecError::Unsupported("non-BIT aggregate kind")),
    };
    let mut partial = BitAggregate::new(kind);
    partial.update(values)?;
    let mut destination = BitAggregate::new(kind);
    destination.merge_from(&partial)?;
    Ok(Datum::UInt(destination.value()))
}
