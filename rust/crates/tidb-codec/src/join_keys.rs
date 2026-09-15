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

//! Native column access for Go's two-pass, column-wide join key construction.

use crate::{encode_hash_datum, CodecError, SerializeMode, INT_FLAG, UINT_FLAG};
use std::ops::{Index, Range};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

/// Read-only physical column access. Implemented by chunks, without making
/// the low-level codec depend on its higher-level consumers.
pub trait JoinKeySource {
    /// The physical column held for an entire sizing or encoding pass.
    type Column: JoinKeyColumn;
    /// Number of addressable physical columns.
    fn num_columns(&self) -> usize;
    /// Row count before applying selection.
    fn physical_rows(&self) -> usize;
    /// Borrow one column owner across the batch, without retaining a guard
    /// across columns (which may alias the same owner).
    fn with_column<R>(&self, column: usize, f: impl FnOnce(&Self::Column) -> R) -> R;
}

/// A borrowed physical column, shared by sizing, encoding and spill restore.
pub trait JoinKeyColumn {
    /// NULL marker at a physical cell.
    fn is_null(&self, row: usize) -> bool;
    /// Borrow packed data once for a column-wide pass. The implementation
    /// retains any backing guard until the callback returns.
    fn with_raw<R>(&self, f: impl FnOnce(JoinKeyBytes<'_>) -> R) -> R;
    /// Typed access for decimal, temporal, enum/set, bit and JSON encoding.
    /// Integer, real and string columns use raw storage directly.
    fn datum(&self, row: usize, field_type: &FieldType) -> Result<Datum, CodecError>;
}

/// Go column data and offsets, borrowed for a complete raw-key pass.
pub enum JoinKeyBytes<'a> {
    /// Fixed-width native cells, including NULL slots.
    Fixed { data: &'a [u8], width: usize },
    /// Variable-width cells with Go's physical-row offsets.
    Variable { data: &'a [u8], offsets: &'a [i64] },
}

impl JoinKeyBytes<'_> {
    /// Native bytes of one physical cell. Source row bounds are checked by
    /// the serializer before borrowing a column.
    pub fn row(&self, row: usize) -> &[u8] {
        match self {
            Self::Fixed { data, width } => &data[row * width..(row + 1) * width],
            Self::Variable { data, offsets } => {
                &data[offsets[row] as usize..offsets[row + 1] as usize]
            }
        }
    }
}

/// One retained allocation for all keys, with disjoint per-row capacity.
/// Offsets, rather than pointers into the buffer, keep relocation safe.
#[derive(Clone, Debug, Default)]
pub struct SerializedJoinKeys {
    bytes: Vec<u8>,
    rows: Vec<Range<usize>>,
    limits: Vec<usize>,
}

impl SerializedJoinKeys {
    /// Restore saved key bytes without rerunning SQL key evaluation. Only
    /// keys retained in this hash-table round need space in the arena.
    pub fn restore<S: JoinKeySource>(
        &mut self,
        source: &S,
        key_column: usize,
        retained: &[bool],
    ) -> Result<(), CodecError> {
        source.with_column(key_column, |column| {
            column.with_raw(|raw| {
                self.reset(retained.len());
                for (row, &keep) in retained.iter().enumerate() {
                    if keep {
                        self.limits[row] = raw.row(row).len();
                    }
                }
                self.allocate()?;
                for (row, &keep) in retained.iter().enumerate() {
                    if keep {
                        let bytes = raw.row(row);
                        let start = self.rows[row].start;
                        let end = start + bytes.len();
                        self.bytes[start..end].copy_from_slice(&bytes);
                        self.rows[row].end = end;
                    }
                }
                Ok(())
            })
        })
    }
    /// Number of logical keys in the current chunk.
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    /// Whether the current chunk contains no keys.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    /// Borrowed key bytes in logical row order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.rows.iter().map(|range| &self.bytes[range.clone()])
    }
    /// Retained capacity including key descriptors for worker accounting.
    pub fn memory_usage(&self) -> usize {
        self.bytes.capacity()
            + self.rows.capacity() * std::mem::size_of::<Range<usize>>()
            + self.limits.capacity() * std::mem::size_of::<usize>()
    }
    fn reset(&mut self, rows: usize) {
        self.rows.resize(rows, 0..0);
        self.rows.fill(0..0);
        self.limits.resize(rows, 0);
        self.limits.fill(0);
    }
    fn allocate(&mut self) -> Result<(), CodecError> {
        let mut offset = 0usize;
        for (row, size) in self.rows.iter_mut().zip(&mut self.limits) {
            let end = offset
                .checked_add(*size)
                .ok_or(CodecError::InvalidEncoding(
                    "serialized key buffer too large",
                ))?;
            *row = offset..offset;
            *size = end;
            offset = end;
        }
        self.bytes.resize(offset, 0);
        Ok(())
    }
    fn append(&mut self, row: usize, data: &[u8]) -> Result<(), CodecError> {
        let end = self.rows[row]
            .end
            .checked_add(data.len())
            .ok_or(CodecError::InvalidEncoding("serialized key too large"))?;
        if end > self.limits[row] {
            return Err(CodecError::InvalidEncoding(
                "serialized key exceeds preallocation",
            ));
        }
        self.bytes[self.rows[row].end..end].copy_from_slice(data);
        self.rows[row].end = end;
        Ok(())
    }
}

impl Index<usize> for SerializedJoinKeys {
    type Output = [u8];
    fn index(&self, row: usize) -> &[u8] {
        &self.bytes[self.rows[row].clone()]
    }
}

/// Key columns and modes chosen by the join's row layout.
#[derive(Clone, Debug)]
pub struct JoinKeyColumns {
    /// Physical input column indices in key order.
    pub indices: Vec<usize>,
    /// Comparison field types, one per key column.
    pub types: Vec<FieldType>,
    /// Sign and variable-width framing, one per key column.
    pub modes: Vec<SerializeMode>,
}

impl JoinKeyColumns {
    /// Go codec.SerializeKeys: size columns, allocate once, then encode columns.
    /// Keys use logical order; selection/filter/null vectors use physical rows.
    pub fn serialize<S: JoinKeySource>(
        &self,
        source: &S,
        used_rows: &[usize],
        filter: Option<&[bool]>,
        nulls: &mut Vec<bool>,
        keys: &mut SerializedJoinKeys,
    ) -> Result<(), CodecError> {
        if self.indices.len() != self.types.len() || self.modes.len() != self.types.len() {
            return Err(CodecError::InvalidEncoding(
                "serialize column count mismatch",
            ));
        }
        let physical_rows = source.physical_rows();
        if self
            .indices
            .iter()
            .any(|&column| column >= source.num_columns())
            || used_rows.iter().any(|&row| row >= physical_rows)
            || filter.is_some_and(|filter| filter.len() != physical_rows)
        {
            return Err(CodecError::InvalidEncoding("serialize column or row index"));
        }
        nulls.resize(physical_rows, false);
        nulls.fill(false);
        keys.reset(used_rows.len());

        // Go preAllocForSerializedKeyBuffer dispatches once per column.
        // Typed decoders run without a raw backing guard: they may acquire
        // their own read view, including for shallow-shared storage.
        for ((&column, field_type), &mode) in self.indices.iter().zip(&self.types).zip(&self.modes)
        {
            source.with_column(column, |column| {
                let fixed = match field_type.code() {
                    FieldTypeCode::Tiny
                    | FieldTypeCode::Short
                    | FieldTypeCode::Int24
                    | FieldTypeCode::Long
                    | FieldTypeCode::LongLong
                    | FieldTypeCode::Year => {
                        Some(8 + usize::from(mode == SerializeMode::NeedSignFlag))
                    }
                    FieldTypeCode::Float
                    | FieldTypeCode::Double
                    | FieldTypeCode::Date
                    | FieldTypeCode::Datetime
                    | FieldTypeCode::Timestamp
                    | FieldTypeCode::Duration => Some(8),
                    _ => None,
                };
                if let Some(size) = fixed {
                    return reserve_column_keys(column, used_rows, filter, nulls, keys, |_| {
                        Ok(size)
                    });
                }
                match field_type.code() {
                    FieldTypeCode::String
                    | FieldTypeCode::Varchar
                    | FieldTypeCode::VarString
                    | FieldTypeCode::Blob
                    | FieldTypeCode::TinyBlob
                    | FieldTypeCode::MediumBlob
                    | FieldTypeCode::LongBlob => column.with_raw(|raw| {
                        let collator = field_type.runtime_collator();
                        let prefix = usize::from(mode == SerializeMode::KeepVarColumnLength) * 4;
                        reserve_column_keys(column, used_rows, filter, nulls, keys, |row| {
                            Ok(collator.max_key_len(raw.row(row)) + prefix)
                        })
                    }),
                    FieldTypeCode::NewDecimal
                    | FieldTypeCode::Enum
                    | FieldTypeCode::Set
                    | FieldTypeCode::Bit
                    | FieldTypeCode::Json => {
                        reserve_column_keys(column, used_rows, filter, nulls, keys, |row| {
                            let value = column.datum(row, field_type)?;
                            let (_, bytes) = encode_hash_datum(&value, field_type)?;
                            Ok(bytes.len() + prefix_size(field_type, mode))
                        })
                    }
                    FieldTypeCode::Null => {
                        for &row in used_rows {
                            nulls[row] = true;
                        }
                        Ok(())
                    }
                    _ => Err(CodecError::InvalidEncoding("unsupported join key type")),
                }
            })?;
        }
        keys.allocate()?;
        for ((&column, field_type), &mode) in self.indices.iter().zip(&self.types).zip(&self.modes)
        {
            source.with_column(column, |column| {
                let code = field_type.code();
                // Dispatch once per column, not once per row.
                match code {
                    FieldTypeCode::Tiny
                    | FieldTypeCode::Short
                    | FieldTypeCode::Int24
                    | FieldTypeCode::Long
                    | FieldTypeCode::LongLong
                    | FieldTypeCode::Year
                    | FieldTypeCode::Duration => column.with_raw(|raws| {
                        for (logical, &physical) in used_rows.iter().enumerate() {
                            if skip(physical, filter, nulls) {
                                continue;
                            }
                            let raw = raws.row(physical);
                            let word: [u8; 8] = raw.try_into().map_err(|_| {
                                CodecError::InvalidEncoding("invalid integer key width")
                            })?;
                            if code != FieldTypeCode::Duration
                                && mode == SerializeMode::NeedSignFlag
                            {
                                let signed = !field_type.has_flag(FieldTypeFlags::UNSIGNED)
                                    && i64::from_le_bytes(word) < 0;
                                keys.append(logical, &[if signed { INT_FLAG } else { UINT_FLAG }])?;
                            }
                            keys.append(logical, &word)?;
                        }
                        Ok::<(), CodecError>(())
                    })?,
                    FieldTypeCode::Float | FieldTypeCode::Double => column.with_raw(|raws| {
                        for (logical, &physical) in used_rows.iter().enumerate() {
                            if skip(physical, filter, nulls) {
                                continue;
                            }
                            let raw = raws.row(physical);
                            let value = if code == FieldTypeCode::Float {
                                f64::from(f32::from_le_bytes(raw.try_into().map_err(|_| {
                                    CodecError::InvalidEncoding("invalid float key width")
                                })?))
                            } else {
                                f64::from_le_bytes(raw.try_into().map_err(|_| {
                                    CodecError::InvalidEncoding("invalid double key width")
                                })?)
                            };
                            let value = if value == 0.0 { 0.0 } else { value };
                            keys.append(logical, &value.to_le_bytes())?;
                        }
                        Ok::<(), CodecError>(())
                    })?,
                    FieldTypeCode::String
                    | FieldTypeCode::Varchar
                    | FieldTypeCode::VarString
                    | FieldTypeCode::Blob
                    | FieldTypeCode::TinyBlob
                    | FieldTypeCode::MediumBlob
                    | FieldTypeCode::LongBlob => column.with_raw(|raws| {
                        let collator = field_type.runtime_collator();
                        for (logical, &physical) in used_rows.iter().enumerate() {
                            if skip(physical, filter, nulls) {
                                continue;
                            }
                            let raw = raws.row(physical);
                            let bytes = collator.immutable_key(raw);
                            if mode == SerializeMode::KeepVarColumnLength {
                                append_length(keys, logical, bytes.len(), false)?;
                            }
                            keys.append(logical, &bytes)?;
                        }
                        Ok::<(), CodecError>(())
                    })?,
                    FieldTypeCode::Null => {}
                    _ => {
                        for (logical, &physical) in used_rows.iter().enumerate() {
                            if skip(physical, filter, nulls) {
                                continue;
                            }
                            let value = column.datum(physical, field_type)?;
                            let (_, bytes) = encode_hash_datum(&value, field_type)?;
                            match prefix_size(field_type, mode) {
                                1 if code == FieldTypeCode::NewDecimal => {
                                    append_length(keys, logical, bytes.len(), true)?
                                }
                                1 => keys.append(logical, &[UINT_FLAG])?,
                                4 => append_length(keys, logical, bytes.len(), false)?,
                                _ => {}
                            }
                            keys.append(logical, &bytes)?;
                        }
                    }
                }

                Ok::<(), CodecError>(())
            })?;
        }
        Ok(())
    }
}

fn reserve_column_keys<C: JoinKeyColumn>(
    column: &C,
    used_rows: &[usize],
    filter: Option<&[bool]>,
    nulls: &mut [bool],
    keys: &mut SerializedJoinKeys,
    mut size: impl FnMut(usize) -> Result<usize, CodecError>,
) -> Result<(), CodecError> {
    for (logical, &physical) in used_rows.iter().enumerate() {
        nulls[physical] |= column.is_null(physical);
        if skip(physical, filter, nulls) {
            continue;
        }
        keys.limits[logical] = keys.limits[logical]
            .checked_add(size(physical)?)
            .ok_or(CodecError::InvalidEncoding("serialized key too large"))?;
    }
    Ok(())
}

fn skip(row: usize, filter: Option<&[bool]>, nulls: &[bool]) -> bool {
    nulls[row] || filter.is_some_and(|filter| !filter[row])
}

fn prefix_size(field_type: &FieldType, mode: SerializeMode) -> usize {
    match (field_type.code(), mode) {
        (FieldTypeCode::NewDecimal, SerializeMode::KeepVarColumnLength) => 1,
        (FieldTypeCode::Bit, SerializeMode::NeedSignFlag) => 1,
        (FieldTypeCode::Enum, SerializeMode::NeedSignFlag)
            if field_type.has_flag(FieldTypeFlags::ENUM_SET_AS_INT) =>
        {
            1
        }
        (FieldTypeCode::Enum, SerializeMode::KeepVarColumnLength)
            if !field_type.has_flag(FieldTypeFlags::ENUM_SET_AS_INT) =>
        {
            4
        }
        (FieldTypeCode::Set | FieldTypeCode::Json, SerializeMode::KeepVarColumnLength) => 4,
        _ => 0,
    }
}

fn append_length(
    keys: &mut SerializedJoinKeys,
    row: usize,
    size: usize,
    decimal: bool,
) -> Result<(), CodecError> {
    if decimal {
        keys.append(
            row,
            &[u8::try_from(size)
                .map_err(|_| CodecError::InvalidEncoding("decimal hash key too long"))?],
        )
    } else {
        keys.append(
            row,
            &u32::try_from(size)
                .map_err(|_| CodecError::InvalidEncoding("join key too long"))?
                .to_le_bytes(),
        )
    }
}
