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

//! Canonical `FIRST_ROW` partial-result state translated from
//! `pkg/executor/aggfuncs/func_first_row.go`.
//!
//! Go repeats the same state machine for every physical value type. This
//! module instead keeps one [`Datum`]-backed state: the first physical row of
//! the first non-empty update wins, including NULL; later updates do not even
//! inspect their values; and a merge copies the source only into a destination
//! that has not observed a row. `is_null` and `got_first_row` remain separate
//! from the payload because the source spill format serializes all three
//! independently, including deliberately inconsistent test fixtures.

use std::fmt;

use tidb_datatype::{Collation, Datum, DatumKind};

/// Go's type-specific FIRST_ROW spill payload selected by aggregate metadata.
///
/// The Go wire row has no Datum tag: the deserializer already knows which
/// `partialResult4FirstRow*` it is reconstructing. String collation is likewise
/// external type metadata and is supplied here only to rebuild the Rust Datum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FirstRowSpillKind {
    /// `partialResult4FirstRowInt`: native-endian `int64`.
    Int,
    /// `partialResult4FirstRowFloat64`: native-endian IEEE-754 bits.
    Float64,
    /// `partialResult4FirstRowString`: native-width length then raw bytes.
    String(Collation),
}

/// A malformed or currently unrepresentable `FIRST_ROW` spill row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FirstRowWireError {
    /// The row ended before a complete field could be read.
    Truncated,
    /// Bytes remained after one complete partial result.
    TrailingBytes(usize),
    /// A source boolean byte was neither zero nor one.
    InvalidBool(u8),
    /// The Datum does not match the type-specific Go spill helper selected by
    /// the caller. NULL is accepted because Go retains that type's zero
    /// payload independently from `isNull`.
    DatumKindMismatch {
        /// Type selected by aggregate metadata.
        expected: FirstRowSpillKind,
        /// Actual Rust Datum kind.
        actual: DatumKind,
    },
}

impl fmt::Display for FirstRowWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated FIRST_ROW spill row"),
            Self::TrailingBytes(count) => {
                write!(formatter, "FIRST_ROW spill row has {count} trailing bytes")
            }
            Self::InvalidBool(value) => {
                write!(formatter, "invalid FIRST_ROW boolean byte {value}")
            }
            Self::DatumKindMismatch { expected, actual } => write!(
                formatter,
                "FIRST_ROW {expected:?} spill helper cannot encode Datum kind {actual:?}"
            ),
        }
    }
}

impl std::error::Error for FirstRowWireError {}

/// One reusable source-shaped serialization buffer.
///
/// The two source booleans lead every row, followed immediately by the payload
/// selected by [`FirstRowSpillKind`]. No Rust-only tag is added: emitted bytes
/// match `pkg/util/serialization` on the same architecture.
#[derive(Clone, Debug)]
pub struct FirstRowSpillSerializer {
    buffer: Vec<u8>,
}

impl Default for FirstRowSpillSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl FirstRowSpillSerializer {
    /// Creates an empty reusable serializer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(64),
        }
    }

    /// Serializes the source flags and one source-compatible typed payload.
    pub fn serialize(
        &mut self,
        state: &FirstRowState,
        kind: FirstRowSpillKind,
    ) -> Result<&[u8], FirstRowWireError> {
        self.buffer.clear();
        self.buffer.push(u8::from(state.is_null));
        self.buffer.push(u8::from(state.got_first_row));
        encode_source_payload(&mut self.buffer, &state.value, kind)?;
        Ok(&self.buffer)
    }

    /// Returns retained reusable capacity, matching the source helper's
    /// allocation-reuse contract for variable-width values.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
}

/// One Datum-backed partial state shared by every `FIRST_ROW` value family.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FirstRowState {
    is_null: bool,
    got_first_row: bool,
    value: Datum,
}

impl FirstRowState {
    /// Creates the source zero state: no row, non-NULL flag, zero Datum.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            is_null: false,
            got_first_row: false,
            value: Datum::Null,
        }
    }

    /// Reconstructs the three independently serialized source fields.
    ///
    /// This intentionally permits combinations that live evaluation cannot
    /// produce because Go's spill-helper tests round-trip such fixtures.
    #[must_use]
    pub fn from_parts(is_null: bool, got_first_row: bool, value: Datum) -> Self {
        Self {
            is_null,
            got_first_row,
            value,
        }
    }

    /// Resets the state for another partition/group.
    pub fn reset(&mut self) {
        self.is_null = false;
        self.got_first_row = false;
        // Go's integer specialization explicitly zeros `val`; every other
        // FIRST_ROW ResetPartialResult clears only these two flags and leaves
        // its typed payload stale until the next non-empty update overwrites
        // it. Preserve that independently observable state shape.
        if matches!(self.value, Datum::Int(_)) {
            self.value = Datum::Int(0);
        }
    }

    /// Evaluates only the first physical row in the first non-empty batch.
    ///
    /// Returns the source-shaped dynamic memory delta: only a retained string
    /// or byte payload charges its byte length, and every ignored row charges
    /// zero. Fixed-size Datum payloads charge zero here.
    pub fn update(&mut self, rows: &[Datum]) -> i64 {
        if self.got_first_row || rows.is_empty() {
            return 0;
        }
        let value = rows[0].clone();
        self.is_null = value.is_null();
        self.got_first_row = true;
        let delta = dynamic_payload_size(&value);
        self.value = value;
        delta
    }

    /// Merges a source partial result only when this destination is unseen.
    pub fn merge_from(&mut self, source: &Self) {
        if !self.got_first_row {
            *self = source.clone();
        }
    }

    /// Returns SQL NULL for no row or first-row NULL, otherwise the payload.
    #[must_use]
    pub fn result(&self) -> Datum {
        if self.is_null || !self.got_first_row {
            Datum::Null
        } else {
            self.value.clone()
        }
    }

    /// Returns whether the source NULL flag is set.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        self.is_null
    }

    /// Returns whether a row (including a NULL row) has been observed.
    #[must_use]
    pub const fn got_first_row(&self) -> bool {
        self.got_first_row
    }

    /// Borrows the independently retained typed payload.
    #[must_use]
    pub const fn value(&self) -> &Datum {
        &self.value
    }

    /// Deserializes one complete source spill row using its external type.
    pub fn deserialize(bytes: &[u8], kind: FirstRowSpillKind) -> Result<Self, FirstRowWireError> {
        let mut decoder = Decoder::new(bytes);
        let is_null = decoder.read_bool()?;
        let got_first_row = decoder.read_bool()?;
        let value = decode_source_payload(&mut decoder, kind)?;
        if decoder.remaining() != 0 {
            return Err(FirstRowWireError::TrailingBytes(decoder.remaining()));
        }
        Ok(Self::from_parts(is_null, got_first_row, value))
    }
}

/// Folds one already-resolved aggregate input group through the canonical
/// state. The shared aggregate dispatcher consumes this seam; SQL parser and
/// planner reachability remain a separate boundary.
#[must_use]
pub fn fold_first_row(values: &[Datum]) -> Datum {
    let mut partial = FirstRowState::new();
    partial.update(values);
    let mut destination = FirstRowState::new();
    destination.merge_from(&partial);
    destination.result()
}

fn dynamic_payload_size(value: &Datum) -> i64 {
    value
        .as_raw_bytes()
        .map_or(0, |bytes| i64::try_from(bytes.len()).unwrap_or(i64::MAX))
}

fn encode_source_payload(
    output: &mut Vec<u8>,
    value: &Datum,
    kind: FirstRowSpillKind,
) -> Result<(), FirstRowWireError> {
    match (kind, value) {
        (FirstRowSpillKind::Int, Datum::Int(value)) => {
            output.extend_from_slice(&value.to_ne_bytes());
        }
        (FirstRowSpillKind::Float64, Datum::Real(value)) => {
            output.extend_from_slice(&value.to_ne_bytes());
        }
        (FirstRowSpillKind::String(_), Datum::String(value)) => {
            encode_bytes(output, value.bytes());
        }
        // Go evaluates NULL into the typed zero payload and records nullness
        // in the independent flag. Rust's Datum::Null has no typed payload,
        // so reconstruct that source zero from the externally selected kind.
        (FirstRowSpillKind::Int, Datum::Null) => {
            output.extend_from_slice(&0_i64.to_ne_bytes());
        }
        (FirstRowSpillKind::Float64, Datum::Null) => {
            output.extend_from_slice(&0_f64.to_ne_bytes());
        }
        (FirstRowSpillKind::String(_), Datum::Null) => encode_bytes(output, &[]),
        (expected, actual) => {
            return Err(FirstRowWireError::DatumKindMismatch {
                expected,
                actual: actual.kind(),
            });
        }
    }
    Ok(())
}

fn decode_source_payload(
    decoder: &mut Decoder<'_>,
    kind: FirstRowSpillKind,
) -> Result<Datum, FirstRowWireError> {
    match kind {
        FirstRowSpillKind::Int => Ok(Datum::Int(i64::from_ne_bytes(decoder.read_array()?))),
        FirstRowSpillKind::Float64 => Ok(Datum::Real(f64::from_ne_bytes(decoder.read_array()?))),
        FirstRowSpillKind::String(collation) => Ok(Datum::new_collation_string(
            decoder.read_bytes()?.to_vec(),
            collation,
        )),
    }
}

fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&bytes.len().to_ne_bytes());
    output.extend_from_slice(bytes);
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn read_u8(&mut self) -> Result<u8, FirstRowWireError> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or(FirstRowWireError::Truncated)?;
        self.offset += 1;
        Ok(value)
    }

    fn read_bool(&mut self) -> Result<bool, FirstRowWireError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(FirstRowWireError::InvalidBool(value)),
        }
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], FirstRowWireError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or(FirstRowWireError::Truncated)?;
        let source = self
            .bytes
            .get(self.offset..end)
            .ok_or(FirstRowWireError::Truncated)?;
        let mut value = [0_u8; N];
        value.copy_from_slice(source);
        self.offset = end;
        Ok(value)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], FirstRowWireError> {
        let length = usize::from_ne_bytes(self.read_array()?);
        let end = self
            .offset
            .checked_add(length)
            .ok_or(FirstRowWireError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(FirstRowWireError::Truncated)?;
        self.offset = end;
        Ok(value)
    }
}
