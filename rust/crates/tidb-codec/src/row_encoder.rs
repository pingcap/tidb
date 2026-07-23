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

//! Dependency-closed encoding decisions from `pkg/util/rowcodec/encoder.go`.
//!
//! This module owns only the new-row metadata and opaque payload boundary. A
//! caller supplies already encoded bytes for each column; typed `Datum`
//! conversion, schema/time-zone handling, checksum handles, and error-policy
//! integration remain outside this leaf. Keeping those boundaries explicit is
//! important: a row byte layout is not a substitute for a typed row codec.

use std::fmt;

use crate::{ROW_CODEC_VERSION, ROW_FLAG_LARGE};

/// One source-owned row column entry for [`encode_raw_row`].
///
/// `None` represents SQL `NULL`; non-null bytes are copied to the row's opaque
/// data section without interpretation. IDs remain signed at the API boundary
/// because Go accepts negative handle/pseudo-column IDs and truncates them to
/// the selected one- or four-byte physical metadata width.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawRowColumn<'a> {
    /// Persisted column ID.
    pub id: i64,
    /// Already encoded payload, or `None` for SQL `NULL`.
    pub value: Option<&'a [u8]>,
}

/// Errors raised while constructing new-row metadata around opaque values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowEncodeError {
    /// The u16 row header cannot represent this number of columns.
    TooManyColumns {
        /// Number of columns supplied by the caller.
        count: usize,
    },
    /// The u32 offset table cannot represent the total payload length.
    DataTooLarge {
        /// Total opaque payload length supplied by the caller.
        length: usize,
    },
}

impl fmt::Display for RowEncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyColumns { count } => {
                write!(formatter, "row has {count} columns; u16 count overflows")
            }
            Self::DataTooLarge { length } => {
                write!(
                    formatter,
                    "row payload has {length} bytes; u32 offset overflows"
                )
            }
        }
    }
}

impl std::error::Error for RowEncodeError {}

/// Appends a source-compatible new-format row around opaque column payloads.
///
/// Non-null and null IDs are sorted independently, with non-null IDs first.
/// Small rows use one-byte IDs and two-byte end offsets. A row becomes large
/// when any ID exceeds 255 or the payload exceeds `u16::MAX`, matching
/// `Encoder::appendColVal` and `Encoder::encodeRowCols`. This function never
/// adds a checksum trailer; checksum version/handle policy remains owned by the
/// higher-level encoder.
pub fn encode_raw_row(
    columns: &[RawRowColumn<'_>],
    buffer: &mut Vec<u8>,
) -> Result<(), RowEncodeError> {
    if columns.len() > usize::from(u16::MAX) {
        return Err(RowEncodeError::TooManyColumns {
            count: columns.len(),
        });
    }

    let mut not_null = Vec::with_capacity(columns.len());
    let mut null = Vec::new();
    let mut data_len = 0_usize;
    let mut large = false;
    for column in columns {
        large |= column.id > i64::from(u8::MAX);
        if let Some(value) = column.value {
            data_len = data_len
                .checked_add(value.len())
                .ok_or(RowEncodeError::DataTooLarge { length: usize::MAX })?;
            not_null.push((column.id, value));
        } else {
            null.push(column.id);
        }
    }
    if data_len > u32::MAX as usize {
        return Err(RowEncodeError::DataTooLarge { length: data_len });
    }
    large |= data_len > usize::from(u16::MAX);

    // Go uses sort.Sort on each partition. Unstable sorting preserves the
    // source's duplicate-ID behavior while keeping this metadata operation
    // independent of schema/typed values.
    if large {
        not_null.sort_unstable_by_key(|(id, _)| *id as u32);
        null.sort_unstable_by_key(|id| *id as u32);
    } else {
        not_null.sort_unstable_by_key(|(id, _)| *id as u8);
        null.sort_unstable_by_key(|id| *id as u8);
    }

    let flags = if large { ROW_FLAG_LARGE } else { 0 };
    buffer.push(ROW_CODEC_VERSION);
    buffer.push(flags);
    buffer.extend_from_slice(&(not_null.len() as u16).to_le_bytes());
    buffer.extend_from_slice(&(null.len() as u16).to_le_bytes());

    if large {
        for (id, _) in &not_null {
            buffer.extend_from_slice(&(*id as u32).to_le_bytes());
        }
        for id in &null {
            buffer.extend_from_slice(&(*id as u32).to_le_bytes());
        }
    } else {
        for (id, _) in &not_null {
            buffer.push(*id as u8);
        }
        for id in &null {
            buffer.push(*id as u8);
        }
    }

    let mut offset = 0_usize;
    for (_, value) in &not_null {
        offset += value.len();
        if large {
            buffer.extend_from_slice(&(offset as u32).to_le_bytes());
        } else {
            // `large` is true whenever data_len exceeds u16::MAX, so this
            // cast is exact for the small path.
            buffer.extend_from_slice(&(offset as u16).to_le_bytes());
        }
    }
    for (_, value) in not_null {
        buffer.extend_from_slice(value);
    }
    Ok(())
}

/// Appends the compact little-endian signed payload used by rowcodec.
///
/// This is deliberately distinct from [`crate::encode_int`], which is the
/// fixed-width mem-comparable key encoding. The rowcodec encoder chooses the
/// shortest 1/2/4/8-byte two's-complement representation.
pub fn encode_raw_int(buffer: &mut Vec<u8>, value: i64) {
    if let Ok(value) = i8::try_from(value) {
        buffer.push(value as u8);
    } else if let Ok(value) = i16::try_from(value) {
        buffer.extend_from_slice(&value.to_le_bytes());
    } else if let Ok(value) = i32::try_from(value) {
        buffer.extend_from_slice(&value.to_le_bytes());
    } else {
        buffer.extend_from_slice(&value.to_le_bytes());
    }
}

/// Appends the compact little-endian unsigned payload used by rowcodec.
///
/// This is deliberately distinct from [`crate::encode_uint`], which is the
/// fixed-width mem-comparable key encoding. The rowcodec encoder chooses the
/// shortest 1/2/4/8-byte representation.
pub fn encode_raw_uint(buffer: &mut Vec<u8>, value: u64) {
    if let Ok(value) = u8::try_from(value) {
        buffer.push(value);
    } else if let Ok(value) = u16::try_from(value) {
        buffer.extend_from_slice(&value.to_le_bytes());
    } else if let Ok(value) = u32::try_from(value) {
        buffer.extend_from_slice(&value.to_le_bytes());
    } else {
        buffer.extend_from_slice(&value.to_le_bytes());
    }
}
