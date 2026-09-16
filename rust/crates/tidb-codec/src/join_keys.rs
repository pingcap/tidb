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
    /// Logical/physical pairs that survive the probe filter and NULL-key
    /// check.  Go's serializer evaluates the same `canSkip` closure in every
    /// column loop; retain the result of the sizing pass so the encoding pass
    /// can stay column-wide without repeating those branches.
    active_rows: Vec<(usize, usize)>,
    /// FNV-1/64 hashes accumulated while key bytes are written. Join
    /// build/probe consume these immediately after serialization, avoiding a
    /// second pass over every serialized key. The ordinary serializer leaves
    /// this empty so non-join callers pay no storage cost.
    hashes: Vec<u64>,
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
                self.reset(retained.len(), false);
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
    /// Logical/physical rows that survived filtering and NULL-key handling.
    ///
    /// The serializer computes this set during its sizing pass. Consumers
    /// that hash or probe the keys can iterate it directly instead of
    /// repeating the filter/null checks for every row.
    pub fn active_rows(&self) -> &[(usize, usize)] {
        &self.active_rows
    }
    /// FNV-1/64 hash for each logical row when hash collection was requested
    /// during serialization. Inactive rows contain zero.
    pub fn hashes(&self) -> &[u64] {
        &self.hashes
    }
    /// Retained capacity including key descriptors for worker accounting.
    pub fn memory_usage(&self) -> usize {
        self.bytes.capacity()
            + self.rows.capacity() * std::mem::size_of::<Range<usize>>()
            + self.limits.capacity() * std::mem::size_of::<usize>()
            + self.active_rows.capacity() * std::mem::size_of::<(usize, usize)>()
            + self.hashes.capacity() * std::mem::size_of::<u64>()
    }
    fn reset(&mut self, rows: usize, collect_hashes: bool) {
        self.rows.resize(rows, 0..0);
        self.rows.fill(0..0);
        self.limits.resize(rows, 0);
        self.limits.fill(0);
        self.active_rows.clear();
        if collect_hashes {
            self.hashes.resize(rows, FNV64_OFFSET_BASIS);
            self.hashes.fill(FNV64_OFFSET_BASIS);
        } else {
            self.hashes.clear();
        }
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
    #[inline]
    fn append<const COLLECT_HASHES: bool>(
        &mut self,
        row: usize,
        data: &[u8],
    ) -> Result<(), CodecError> {
        let start = self.rows[row].end;
        let end = start
            .checked_add(data.len())
            .ok_or(CodecError::InvalidEncoding("serialized key too large"))?;
        if end > self.limits[row] {
            return Err(CodecError::InvalidEncoding(
                "serialized key exceeds preallocation",
            ));
        }
        self.bytes[start..end].copy_from_slice(data);
        self.rows[row].end = end;
        if COLLECT_HASHES {
            let hash = &mut self.hashes[row];
            for &byte in data {
                *hash = hash.wrapping_mul(FNV64_PRIME) ^ u64::from(byte);
            }
        }
        Ok(())
    }

    /// Append a length/flag prefix and its payload as one bounded write.
    ///
    /// Go's encoder issues two slice appends for these keys. Keeping the
    /// fragments together here preserves byte and hash order while avoiding a
    /// second range check and hash loop for every variable-width value.
    #[inline]
    fn append_parts<const COLLECT_HASHES: bool>(
        &mut self,
        row: usize,
        prefix: &[u8],
        payload: &[u8],
    ) -> Result<(), CodecError> {
        let start = self.rows[row].end;
        let size = prefix
            .len()
            .checked_add(payload.len())
            .ok_or(CodecError::InvalidEncoding("serialized key too large"))?;
        let end = start
            .checked_add(size)
            .ok_or(CodecError::InvalidEncoding("serialized key too large"))?;
        if end > self.limits[row] {
            return Err(CodecError::InvalidEncoding(
                "serialized key exceeds preallocation",
            ));
        }
        let bytes = &mut self.bytes[start..end];
        bytes[..prefix.len()].copy_from_slice(prefix);
        bytes[prefix.len()..].copy_from_slice(payload);
        self.rows[row].end = end;
        if COLLECT_HASHES {
            let hash = &mut self.hashes[row];
            for &byte in prefix.iter().chain(payload) {
                *hash = hash.wrapping_mul(FNV64_PRIME) ^ u64::from(byte);
            }
        }
        Ok(())
    }
}

const FNV64_OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
const FNV64_PRIME: u64 = 1_099_511_628_211;

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
        self.serialize_impl::<S, true, false>(source, used_rows, filter, Some(nulls), keys)
    }

    /// Go `SerializeKeys` with FNV-1/64 accumulated during encoding.
    /// Join build/probe callers consume the hash immediately after
    /// serialization, so no second pass over each key is needed.
    pub fn serialize_with_hashes<S: JoinKeySource>(
        &self,
        source: &S,
        used_rows: &[usize],
        filter: Option<&[bool]>,
        nulls: &mut Vec<bool>,
        keys: &mut SerializedJoinKeys,
    ) -> Result<(), CodecError> {
        self.serialize_impl::<S, true, true>(source, used_rows, filter, Some(nulls), keys)
    }

    /// Go `SerializeKeys` when `hasNullableKey` is false.
    ///
    /// The source leaves `nullKeyVector` nil for a non-nullable join. Keep
    /// that branch allocation-free instead of materialising and clearing a
    /// physical-row boolean vector that can never contain a true value.
    pub fn serialize_without_nulls<S: JoinKeySource>(
        &self,
        source: &S,
        used_rows: &[usize],
        filter: Option<&[bool]>,
        keys: &mut SerializedJoinKeys,
    ) -> Result<(), CodecError> {
        self.serialize_impl::<S, false, false>(source, used_rows, filter, None, keys)
    }

    /// `serialize_without_nulls` with FNV-1/64 accumulated during encoding.
    /// The null-free build/probe path uses this to avoid a second key scan.
    pub fn serialize_without_nulls_with_hashes<S: JoinKeySource>(
        &self,
        source: &S,
        used_rows: &[usize],
        filter: Option<&[bool]>,
        keys: &mut SerializedJoinKeys,
    ) -> Result<(), CodecError> {
        self.serialize_impl::<S, false, true>(source, used_rows, filter, None, keys)
    }

    fn serialize_impl<S: JoinKeySource, const TRACK_NULLS: bool, const COLLECT_HASHES: bool>(
        &self,
        source: &S,
        used_rows: &[usize],
        filter: Option<&[bool]>,
        mut nulls: Option<&mut Vec<bool>>,
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
        if let Some(nulls) = nulls.as_deref_mut() {
            nulls.resize(physical_rows, false);
            nulls.fill(false);
        }
        keys.reset(used_rows.len(), COLLECT_HASHES);

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
                    return reserve_column_keys::<TRACK_NULLS, _>(
                        column,
                        used_rows,
                        filter,
                        nulls.as_mut().map(|nulls| nulls.as_mut_slice()),
                        keys,
                        |_| Ok(size),
                    );
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
                        reserve_column_keys::<TRACK_NULLS, _>(
                            column,
                            used_rows,
                            filter,
                            nulls.as_mut().map(|nulls| nulls.as_mut_slice()),
                            keys,
                            |row| Ok(collator.max_key_len(raw.row(row)) + prefix),
                        )
                    }),
                    FieldTypeCode::NewDecimal
                    | FieldTypeCode::Enum
                    | FieldTypeCode::Set
                    | FieldTypeCode::Bit
                    | FieldTypeCode::Json => reserve_column_keys::<TRACK_NULLS, _>(
                        column,
                        used_rows,
                        filter,
                        nulls.as_mut().map(|nulls| nulls.as_mut_slice()),
                        keys,
                        |row| {
                            let value = column.datum(row, field_type)?;
                            let (_, bytes) = encode_hash_datum(&value, field_type)?;
                            Ok(bytes.len() + prefix_size(field_type, mode))
                        },
                    ),
                    FieldTypeCode::Null => {
                        if let Some(nulls) = nulls.as_deref_mut() {
                            for &row in used_rows {
                                nulls[row] = true;
                            }
                        }
                        Ok(())
                    }
                    _ => Err(CodecError::InvalidEncoding("unsupported join key type")),
                }
            })?;
        }

        // The first pass has now populated the complete physical-row NULL
        // vector.  Cache the rows that the second pass is allowed to encode;
        // this is especially useful for multi-column joins, where the same
        // filter/null branch otherwise runs once per key column.
        if !TRACK_NULLS && filter.is_none() {
            keys.active_rows.extend(
                used_rows
                    .iter()
                    .enumerate()
                    .map(|(logical, &physical)| (logical, physical)),
            );
        } else {
            keys.active_rows.extend(
                used_rows
                    .iter()
                    .enumerate()
                    .filter(|&(_, &physical)| {
                        !skip::<TRACK_NULLS>(
                            physical,
                            filter,
                            nulls.as_ref().map(|nulls| nulls.as_slice()),
                        )
                    })
                    .map(|(logical, &physical)| (logical, physical)),
            );
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
                        for active in 0..keys.active_rows.len() {
                            let (logical, physical) = keys.active_rows[active];
                            let raw = raws.row(physical);
                            let word: [u8; 8] = raw.try_into().map_err(|_| {
                                CodecError::InvalidEncoding("invalid integer key width")
                            })?;
                            if code != FieldTypeCode::Duration
                                && mode == SerializeMode::NeedSignFlag
                            {
                                let signed = !field_type.has_flag(FieldTypeFlags::UNSIGNED)
                                    && i64::from_le_bytes(word) < 0;
                                let mut flagged_word = [0_u8; 9];
                                flagged_word[0] = if signed { INT_FLAG } else { UINT_FLAG };
                                flagged_word[1..].copy_from_slice(&word);
                                keys.append::<COLLECT_HASHES>(logical, &flagged_word)?;
                            } else {
                                keys.append::<COLLECT_HASHES>(logical, &word)?;
                            }
                        }
                        Ok::<(), CodecError>(())
                    })?,
                    FieldTypeCode::Float | FieldTypeCode::Double => column.with_raw(|raws| {
                        for active in 0..keys.active_rows.len() {
                            let (logical, physical) = keys.active_rows[active];
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
                            keys.append::<COLLECT_HASHES>(logical, &value.to_le_bytes())?;
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
                        for active in 0..keys.active_rows.len() {
                            let (logical, physical) = keys.active_rows[active];
                            let raw = raws.row(physical);
                            let bytes = collator.immutable_key(raw);
                            if mode == SerializeMode::KeepVarColumnLength {
                                let size = u32::try_from(bytes.len()).map_err(|_| {
                                    CodecError::InvalidEncoding("join key too long")
                                })?;
                                keys.append_parts::<COLLECT_HASHES>(
                                    logical,
                                    &size.to_le_bytes(),
                                    &bytes,
                                )?;
                            } else {
                                keys.append::<COLLECT_HASHES>(logical, &bytes)?;
                            }
                        }
                        Ok::<(), CodecError>(())
                    })?,
                    FieldTypeCode::Null => {}
                    _ => {
                        for active in 0..keys.active_rows.len() {
                            let (logical, physical) = keys.active_rows[active];
                            let value = column.datum(physical, field_type)?;
                            let (_, bytes) = encode_hash_datum(&value, field_type)?;
                            match prefix_size(field_type, mode) {
                                1 if code == FieldTypeCode::NewDecimal => {
                                    append_length_and::<COLLECT_HASHES>(
                                        keys, logical, bytes.len(), true, &bytes,
                                    )?;
                                    continue;
                                }
                                1 if bytes.len() == 8 => {
                                    let mut flagged_word = [0_u8; 9];
                                    flagged_word[0] = UINT_FLAG;
                                    flagged_word[1..].copy_from_slice(&bytes);
                                    keys.append::<COLLECT_HASHES>(logical, &flagged_word)?;
                                    continue;
                                }
                                1 => keys.append::<COLLECT_HASHES>(logical, &[UINT_FLAG])?,
                                4 => {
                                    append_length_and::<COLLECT_HASHES>(
                                        keys, logical, bytes.len(), false, &bytes,
                                    )?;
                                    continue;
                                }
                                _ => {}
                            }
                            keys.append::<COLLECT_HASHES>(logical, &bytes)?;
                        }
                    }
                }

                Ok::<(), CodecError>(())
            })?;
        }
        Ok(())
    }
}

fn reserve_column_keys<const TRACK_NULLS: bool, C: JoinKeyColumn>(
    column: &C,
    used_rows: &[usize],
    filter: Option<&[bool]>,
    mut nulls: Option<&mut [bool]>,
    keys: &mut SerializedJoinKeys,
    mut size: impl FnMut(usize) -> Result<usize, CodecError>,
) -> Result<(), CodecError> {
    for (logical, &physical) in used_rows.iter().enumerate() {
        if TRACK_NULLS {
            if let Some(nulls) = nulls.as_deref_mut() {
                nulls[physical] |= column.is_null(physical);
            }
        }
        if skip::<TRACK_NULLS>(physical, filter, nulls.as_deref()) {
            continue;
        }
        keys.limits[logical] = keys.limits[logical]
            .checked_add(size(physical)?)
            .ok_or(CodecError::InvalidEncoding("serialized key too large"))?;
    }
    Ok(())
}

fn skip<const TRACK_NULLS: bool>(
    row: usize,
    filter: Option<&[bool]>,
    nulls: Option<&[bool]>,
) -> bool {
    (TRACK_NULLS && nulls.is_some_and(|nulls| nulls[row]))
        || filter.is_some_and(|filter| !filter[row])
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

fn append_length_and<const COLLECT_HASHES: bool>(
    keys: &mut SerializedJoinKeys,
    row: usize,
    size: usize,
    decimal: bool,
    payload: &[u8],
) -> Result<(), CodecError> {
    if decimal {
        let length = u8::try_from(size)
            .map_err(|_| CodecError::InvalidEncoding("decimal hash key too long"))?;
        keys.append_parts::<COLLECT_HASHES>(row, &[length], payload)
    } else {
        let length = u32::try_from(size)
            .map_err(|_| CodecError::InvalidEncoding("join key too long"))?;
        keys.append_parts::<COLLECT_HASHES>(row, &length.to_le_bytes(), payload)
    }
}
