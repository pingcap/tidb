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

//! Reusable aggregate partial-result spill encoding.

use super::avg::AvgFloat64State;
use super::sum::SumFloat64State;
use tidb_datatype::Decimal;

/// Fixed widths of the source primitive fields.
pub const COUNT_WIRE_SIZE: usize = std::mem::size_of::<i64>();
/// Fixed width of one floating-point value and signed row-count pair.
pub const NUMERIC_PAIR_WIRE_SIZE: usize = std::mem::size_of::<f64>() + std::mem::size_of::<i64>();

/// A malformed fixed-width spill row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidWireLength {
    /// Number of bytes supplied by the spill row.
    pub actual: usize,
    /// Number of bytes required by the selected wire format.
    pub expected: usize,
}

/// One source-shaped reusable `SerializeHelper` buffer.
#[derive(Clone, Debug)]
pub struct SpillSerializer {
    buffer: Vec<u8>,
}

impl Default for SpillSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl SpillSerializer {
    /// Creates a reusable serializer with capacity for the fixed-width formats.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(64),
        }
    }

    /// Serializes one signed COUNT partial result in native byte order.
    pub fn serialize_count(&mut self, value: i64) -> &[u8] {
        self.buffer.clear();
        self.buffer.extend_from_slice(&value.to_ne_bytes());
        &self.buffer
    }

    /// Serializes a floating-point AVG sum-and-count partial result.
    pub fn serialize_avg_float64(&mut self, state: AvgFloat64State) -> &[u8] {
        let (sum, count) = state.parts();
        self.serialize_numeric_pair(sum, count)
    }

    /// Serializes a floating-point SUM value-and-count partial result.
    pub fn serialize_sum_float64(&mut self, state: SumFloat64State) -> &[u8] {
        let (value, count) = state.parts();
        self.serialize_numeric_pair(value, count)
    }

    /// Serializes the representable decimal AVG/SUM partial pair.
    pub fn serialize_decimal_pair(&mut self, value: &Decimal, count: i64) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(u8::from(value.is_negative()));
        self.buffer.extend_from_slice(&value.scale().to_ne_bytes());
        self.buffer
            .extend_from_slice(&value.storage_scale().to_ne_bytes());
        let coefficient = value.coefficient_digits().as_bytes();
        self.buffer
            .extend_from_slice(&(coefficient.len() as u32).to_ne_bytes());
        self.buffer.extend_from_slice(coefficient);
        self.buffer.extend_from_slice(&count.to_ne_bytes());
        &self.buffer
    }

    fn serialize_numeric_pair(&mut self, value: f64, count: i64) -> &[u8] {
        self.buffer.clear();
        self.buffer.extend_from_slice(&value.to_ne_bytes());
        self.buffer.extend_from_slice(&count.to_ne_bytes());
        &self.buffer
    }

    /// Returns the retained allocation available for subsequent spill rows.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
}

/// Decodes one fixed-width signed COUNT partial result.
pub fn deserialize_count(bytes: &[u8]) -> Result<i64, InvalidWireLength> {
    if bytes.len() != COUNT_WIRE_SIZE {
        return Err(InvalidWireLength {
            actual: bytes.len(),
            expected: COUNT_WIRE_SIZE,
        });
    }
    let mut value = [0_u8; COUNT_WIRE_SIZE];
    value.copy_from_slice(bytes);
    Ok(i64::from_ne_bytes(value))
}

/// Decodes a floating-point AVG sum-and-count partial result.
pub fn deserialize_avg_float64(bytes: &[u8]) -> Result<AvgFloat64State, InvalidWireLength> {
    let (sum, count) = deserialize_numeric_pair(bytes)?;
    Ok(AvgFloat64State::from_parts(sum, count))
}

/// Decodes a floating-point SUM value-and-count partial result.
pub fn deserialize_sum_float64(bytes: &[u8]) -> Result<SumFloat64State, InvalidWireLength> {
    let (value, count) = deserialize_numeric_pair(bytes)?;
    Ok(SumFloat64State::from_parts(value, count))
}

fn deserialize_numeric_pair(bytes: &[u8]) -> Result<(f64, i64), InvalidWireLength> {
    if bytes.len() != NUMERIC_PAIR_WIRE_SIZE {
        return Err(InvalidWireLength {
            actual: bytes.len(),
            expected: NUMERIC_PAIR_WIRE_SIZE,
        });
    }
    let mut value = [0_u8; 8];
    value.copy_from_slice(&bytes[..8]);
    let mut count = [0_u8; 8];
    count.copy_from_slice(&bytes[8..]);
    Ok((f64::from_ne_bytes(value), i64::from_ne_bytes(count)))
}

/// Decodes the decimal/count pair emitted by [`SpillSerializer::serialize_decimal_pair`].
pub fn deserialize_decimal_pair(bytes: &[u8]) -> Result<(Decimal, i64), InvalidWireLength> {
    const HEADER: usize = 1 + 4 + 4 + 4;
    const COUNT: usize = 8;
    if bytes.len() < HEADER + COUNT {
        return Err(InvalidWireLength {
            actual: bytes.len(),
            expected: HEADER + COUNT,
        });
    }
    let negative = bytes[0] != 0;
    let mut scale = [0_u8; 4];
    scale.copy_from_slice(&bytes[1..5]);
    let scale = u32::from_ne_bytes(scale);
    let mut storage_scale = [0_u8; 4];
    storage_scale.copy_from_slice(&bytes[5..9]);
    let storage_scale = u32::from_ne_bytes(storage_scale);
    let mut length = [0_u8; 4];
    length.copy_from_slice(&bytes[9..13]);
    let length = u32::from_ne_bytes(length) as usize;
    let expected = HEADER + length + COUNT;
    if bytes.len() != expected || storage_scale != scale {
        return Err(InvalidWireLength {
            actual: bytes.len(),
            expected,
        });
    }
    let coefficient =
        std::str::from_utf8(&bytes[HEADER..HEADER + length]).map_err(|_| InvalidWireLength {
            actual: bytes.len(),
            expected,
        })?;
    let split = coefficient.len().saturating_sub(scale as usize);
    let literal = if scale == 0 {
        coefficient.to_string()
    } else {
        format!("{}.{}", &coefficient[..split], &coefficient[split..])
    };
    let value = Decimal::from_literal(&literal);
    let value = if negative { value.negate() } else { value };
    let mut count = [0_u8; 8];
    count.copy_from_slice(&bytes[HEADER + length..]);
    Ok((value, i64::from_ne_bytes(count)))
}

/// Sequential count-row reader matching Go's `deserializeHelper` loop.
pub struct CountDeserializer<'a> {
    rows: &'a [&'a [u8]],
    next_row: usize,
}

impl<'a> CountDeserializer<'a> {
    /// Creates a sequential reader over borrowed serialized COUNT rows.
    #[must_use]
    pub const fn new(rows: &'a [&'a [u8]]) -> Self {
        Self { rows, next_row: 0 }
    }
    /// Decodes the next row, returning `None` after the input is exhausted.
    pub fn read_next(&mut self) -> Result<Option<i64>, InvalidWireLength> {
        let Some(bytes) = self.rows.get(self.next_row).copied() else {
            return Ok(None);
        };
        let value = deserialize_count(bytes)?;
        self.next_row += 1;
        Ok(Some(value))
    }
    /// Returns the index of the next unread row.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.next_row
    }
}
