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

//! The source-level framing of `pkg/util/chunk.Codec`.
//!
//! The Go chunk codec serializes one column after another. Each column starts
//! with a row count and null count, optionally carries a null bitmap, then
//! carries either fixed-width values or an offset table followed by variable
//! width bytes. This module ports that byte layout without pretending to know
//! TiDB's `Datum` conversion rules. `ColumnLayout::for_field_type` provides
//! the source-owned `FieldType` to physical-width mapping; callers still own
//! typed interpretation of `data`.

use std::fmt;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

const HEADER_BYTES: usize = 8;
const OFFSET_BYTES: usize = 8;
const EIGHT_BYTES: usize = 8;
const MY_DECIMAL_BYTES: usize = 40;

/// Physical layout required to split one Go chunk column.
///
/// `None` denotes the Go `VarElemLen` path. A caller can provide a width
/// explicitly, or use [`ColumnLayout::for_field_type`] for the source-owned
/// `getFixedLen` mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnLayout {
    fixed_width: Option<usize>,
}

impl ColumnLayout {
    /// Creates a fixed-width layout in bytes per row.
    pub const fn fixed(width: usize) -> Self {
        Self {
            fixed_width: Some(width),
        }
    }

    /// Creates a variable-width layout with an offset table.
    pub const fn variable() -> Self {
        Self { fixed_width: None }
    }

    /// Derives the physical layout used by Go `chunk.getFixedLen`.
    ///
    /// This is intentionally a layout-only operation. It does not inspect
    /// collation, unsignedness, precision, or any other `FieldType` metadata,
    /// and it does not claim that the bytes are already a typed `Datum`.
    pub fn for_field_type(field_type: &FieldType) -> Self {
        Self::for_field_type_code(field_type.code())
    }

    /// Derives the physical layout from the source MySQL type code.
    ///
    /// `None` is the exact Go `VarElemLen` result. The fixed widths are the
    /// source's native `int64`/`float64`, `types.Time`, and `types.MyDecimal`
    /// sizes used by `pkg/util/chunk/codec.go`, not SQL display widths.
    pub const fn for_field_type_code(code: FieldTypeCode) -> Self {
        match code {
            FieldTypeCode::Float => Self::fixed(4),
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Double
            | FieldTypeCode::Year
            | FieldTypeCode::Duration
            | FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::Timestamp => Self::fixed(EIGHT_BYTES),
            FieldTypeCode::NewDecimal => Self::fixed(MY_DECIMAL_BYTES),
            FieldTypeCode::Unspecified
            | FieldTypeCode::NewDate
            | FieldTypeCode::Varchar
            | FieldTypeCode::Bit
            | FieldTypeCode::Json
            | FieldTypeCode::Enum
            | FieldTypeCode::Set
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::String
            | FieldTypeCode::Geometry
            | FieldTypeCode::VectorFloat32
            | FieldTypeCode::Null
            | FieldTypeCode::VarString
            | FieldTypeCode::Blob
            | FieldTypeCode::Unknown(_) => Self::variable(),
        }
    }

    /// Returns the fixed width, or `None` for a variable-width column.
    pub const fn fixed_width(self) -> Option<usize> {
        self.fixed_width
    }
}

/// One column decoded from Go's chunk codec, with all typed values opaque.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawColumn<'a> {
    /// Number of rows declared by the column header.
    pub length: usize,
    /// Number of null rows declared by the column header.
    pub null_count: usize,
    /// Null bitmap when the encoded null count is non-zero.
    ///
    /// A `None` bitmap is the source codec's all-not-null fast path. Bitmap bit
    /// `1` means non-null, matching `Column.IsNull` in Go.
    pub null_bitmap: Option<&'a [u8]>,
    /// Variable-column offsets, including the leading zero and final data end.
    pub offsets: Option<Vec<i64>>,
    /// Fixed-width bytes or the complete variable-width data region.
    pub data: &'a [u8],
}

impl RawColumn<'_> {
    /// Returns whether the row is null according to the source bitmap.
    pub fn is_null(&self, row: usize) -> Result<bool, ColumnCodecError> {
        if row >= self.length {
            return Err(ColumnCodecError::RowOutOfRange {
                row,
                length: self.length,
            });
        }
        Ok(self
            .null_bitmap
            .is_some_and(|bitmap| bitmap[row / 8] & (1_u8 << (row % 8)) == 0))
    }

    /// Returns one variable-width value's byte range.
    pub fn value(&self, row: usize) -> Result<&[u8], ColumnCodecError> {
        if row >= self.length {
            return Err(ColumnCodecError::RowOutOfRange {
                row,
                length: self.length,
            });
        }
        let offsets = self
            .offsets
            .as_ref()
            .ok_or(ColumnCodecError::FixedColumnValueAccess)?;
        let start = usize::try_from(offsets[row]).map_err(|_| ColumnCodecError::InvalidOffset {
            column: 0,
            offset_index: row,
            value: offsets[row],
        })?;
        let end =
            usize::try_from(offsets[row + 1]).map_err(|_| ColumnCodecError::InvalidOffset {
                column: 0,
                offset_index: row + 1,
                value: offsets[row + 1],
            })?;
        self.data
            .get(start..end)
            .ok_or(ColumnCodecError::InvalidOffset {
                column: 0,
                offset_index: row,
                value: offsets[row],
            })
    }

    /// Returns one fixed-width value's native byte range.
    ///
    /// Go's `chunk.Column` stores fixed values in the host representation of
    /// the corresponding Go scalar (`int64`, `uint64`, `float32`, or
    /// `float64`). The typed boundary uses this accessor only after checking
    /// the `FieldType`-derived width; it never interprets temporal, decimal,
    /// or other opaque fixed payloads here.
    pub fn fixed_value(&self, row: usize, width: usize) -> Result<&[u8], ColumnCodecError> {
        if row >= self.length {
            return Err(ColumnCodecError::RowOutOfRange {
                row,
                length: self.length,
            });
        }
        if self.offsets.is_some() {
            return Err(ColumnCodecError::FixedColumnValueAccess);
        }
        let start = row
            .checked_mul(width)
            .ok_or(ColumnCodecError::FixedDataLengthOverflow {
                column: 0,
                width,
                length: self.length,
            })?;
        let end = start
            .checked_add(width)
            .ok_or(ColumnCodecError::FixedDataLengthOverflow {
                column: 0,
                width,
                length: self.length,
            })?;
        self.data
            .get(start..end)
            .ok_or(ColumnCodecError::InsufficientBytes {
                column: 0,
                section: "fixed value",
                needed: end,
                available: self.data.len(),
            })
    }
}

/// Errors raised when a typed `FieldType` interpretation is not source-proven
/// for a raw chunk column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypedColumnError {
    /// The Go `chunk` physical layout does not match the requested type.
    LayoutMismatch {
        /// MySQL type code requested by the caller.
        field_type: FieldTypeCode,
        /// Width expected by `getFixedLen`, or `None` for variable data.
        expected_width: Option<usize>,
        /// Width observed in the raw column, or `None` for variable data.
        actual_width: Option<usize>,
    },
    /// The fixed payload does not contain exactly one value for every row.
    InvalidFixedDataLength {
        /// MySQL type code requested by the caller.
        field_type: FieldTypeCode,
        /// Bytes expected from the physical width and row count.
        expected: usize,
        /// Bytes actually carried by the column.
        actual: usize,
    },
    /// This type requires a codec owner that has not crossed this boundary.
    UnsupportedFieldType(FieldTypeCode),
    /// Raw column framing failed before typed interpretation.
    Column(ColumnCodecError),
}

impl fmt::Display for TypedColumnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LayoutMismatch {
                field_type,
                expected_width,
                actual_width,
            } => write!(
                formatter,
                "typed chunk column {field_type:?} expects width {expected_width:?}, got {actual_width:?}"
            ),
            Self::InvalidFixedDataLength {
                field_type,
                expected,
                actual,
            } => write!(
                formatter,
                "typed chunk column {field_type:?} needs {expected} data bytes, got {actual}"
            ),
            Self::UnsupportedFieldType(field_type) => {
                write!(formatter, "typed chunk column {field_type:?} is not implemented")
            }
            Self::Column(error) => write!(formatter, "raw chunk column error: {error}"),
        }
    }
}

impl std::error::Error for TypedColumnError {}

impl From<ColumnCodecError> for TypedColumnError {
    fn from(error: ColumnCodecError) -> Self {
        Self::Column(error)
    }
}

/// Converts one source-shaped `chunk.Column` into the dependency-closed
/// [`Datum`] subset.
///
/// This is deliberately a small leaf. It handles only the physical values
/// whose Go representation is unambiguous from `chunk.Column`: native signed
/// and unsigned 64-bit integers, native float32/float64, and variable raw
/// bytes/strings. Temporal values, `MyDecimal`, JSON, enum/set, vector, and
/// unknown types remain raw because their source codecs carry additional
/// semantics not represented by this function.
pub fn decode_column_datums(
    column: &RawColumn<'_>,
    field_type: FieldType,
) -> Result<Vec<Datum>, TypedColumnError> {
    let code = field_type.code();
    let layout = ColumnLayout::for_field_type(&field_type);
    match code {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Year => {
            let width = layout.fixed_width().expect("integer chunk types are fixed");
            validate_fixed_layout(column, code, width)?;
            (0..column.length)
                .map(|row| {
                    if column.is_null(row)? {
                        return Ok(Datum::Null);
                    }
                    let bytes = column.fixed_value(row, width)?;
                    let value =
                        bytes
                            .try_into()
                            .map_err(|_| TypedColumnError::InvalidFixedDataLength {
                                field_type: code,
                                expected: width,
                                actual: bytes.len(),
                            })?;
                    Ok(if field_type.is_unsigned() {
                        Datum::new_uint(u64::from_ne_bytes(value))
                    } else {
                        Datum::new_int(i64::from_ne_bytes(value))
                    })
                })
                .collect()
        }
        FieldTypeCode::Float => {
            let width = layout.fixed_width().expect("float chunk type is fixed");
            validate_fixed_layout(column, code, width)?;
            (0..column.length)
                .map(|row| {
                    if column.is_null(row)? {
                        return Ok(Datum::Null);
                    }
                    let bytes = column.fixed_value(row, width)?;
                    let value =
                        bytes
                            .try_into()
                            .map_err(|_| TypedColumnError::InvalidFixedDataLength {
                                field_type: code,
                                expected: width,
                                actual: bytes.len(),
                            })?;
                    Ok(Datum::new_real(f32::from_ne_bytes(value) as f64))
                })
                .collect()
        }
        FieldTypeCode::Double => {
            let width = layout.fixed_width().expect("double chunk type is fixed");
            validate_fixed_layout(column, code, width)?;
            (0..column.length)
                .map(|row| {
                    if column.is_null(row)? {
                        return Ok(Datum::Null);
                    }
                    let bytes = column.fixed_value(row, width)?;
                    let value =
                        bytes
                            .try_into()
                            .map_err(|_| TypedColumnError::InvalidFixedDataLength {
                                field_type: code,
                                expected: width,
                                actual: bytes.len(),
                            })?;
                    Ok(Datum::new_real(f64::from_ne_bytes(value)))
                })
                .collect()
        }
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::Blob => {
            validate_variable_layout(column, code)?;
            (0..column.length)
                .map(|row| {
                    if column.is_null(row)? {
                        return Ok(Datum::Null);
                    }
                    let bytes = column.value(row)?;
                    Ok(if field_type.is_binary_string() {
                        Datum::new_bytes(bytes.to_vec())
                    } else {
                        Datum::new_collation_string(bytes.to_vec(), field_type.collation())
                    })
                })
                .collect()
        }
        _ => Err(TypedColumnError::UnsupportedFieldType(code)),
    }
}

fn validate_fixed_layout(
    column: &RawColumn<'_>,
    field_type: FieldTypeCode,
    width: usize,
) -> Result<(), TypedColumnError> {
    if column.offsets.is_some() {
        return Err(TypedColumnError::LayoutMismatch {
            field_type,
            expected_width: Some(width),
            actual_width: None,
        });
    }
    let expected =
        width
            .checked_mul(column.length)
            .ok_or(TypedColumnError::InvalidFixedDataLength {
                field_type,
                expected: usize::MAX,
                actual: column.data.len(),
            })?;
    if column.data.len() != expected {
        return Err(TypedColumnError::InvalidFixedDataLength {
            field_type,
            expected,
            actual: column.data.len(),
        });
    }
    Ok(())
}

fn validate_variable_layout(
    column: &RawColumn<'_>,
    field_type: FieldTypeCode,
) -> Result<(), TypedColumnError> {
    if column.offsets.is_none() {
        return Err(TypedColumnError::LayoutMismatch {
            field_type,
            expected_width: None,
            actual_width: column.data.len().checked_div(column.length.max(1)),
        });
    }
    Ok(())
}

/// Errors raised while splitting Go chunk-column framing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnCodecError {
    /// A column header or payload ended before the declared bytes were read.
    InsufficientBytes {
        /// Column ordinal in the encoded chunk.
        column: usize,
        /// Logical section that was truncated.
        section: &'static str,
        /// Bytes required by the section.
        needed: usize,
        /// Bytes available at the section boundary.
        available: usize,
    },
    /// The null count cannot describe this column's row count.
    InvalidNullCount {
        /// Column ordinal in the encoded chunk.
        column: usize,
        /// Declared null count.
        null_count: usize,
        /// Declared row count.
        length: usize,
    },
    /// An offset is negative, decreases, or does not begin at zero.
    InvalidOffset {
        /// Column ordinal in the encoded chunk.
        column: usize,
        /// Offset index in the column's offset table.
        offset_index: usize,
        /// Invalid signed offset.
        value: i64,
    },
    /// A row accessor was used with a fixed-width column.
    FixedColumnValueAccess,
    /// A row accessor was used outside the column's declared range.
    RowOutOfRange {
        /// Requested row.
        row: usize,
        /// Declared row count.
        length: usize,
    },
    /// A fixed-width column's width multiplied by its row count overflowed.
    FixedDataLengthOverflow {
        /// Column ordinal in the encoded chunk.
        column: usize,
        /// Fixed width supplied by the caller.
        width: usize,
        /// Declared row count.
        length: usize,
    },
}

impl fmt::Display for ColumnCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientBytes {
                column,
                section,
                needed,
                available,
            } => write!(
                formatter,
                "chunk column {column} needs {needed} {section} bytes, but only {available} remain"
            ),
            Self::InvalidNullCount {
                column,
                null_count,
                length,
            } => write!(
                formatter,
                "chunk column {column} declares {null_count} nulls for {length} rows"
            ),
            Self::InvalidOffset {
                column,
                offset_index,
                value,
            } => write!(
                formatter,
                "chunk column {column} has invalid offset {value} at index {offset_index}"
            ),
            Self::FixedColumnValueAccess => {
                formatter.write_str("fixed-width column has no per-value offset")
            }
            Self::RowOutOfRange { row, length } => {
                write!(formatter, "chunk row {row} is outside {length} rows")
            }
            Self::FixedDataLengthOverflow {
                column,
                width,
                length,
            } => write!(
                formatter,
                "chunk column {column} fixed data length overflows ({width} x {length})"
            ),
        }
    }
}

impl std::error::Error for ColumnCodecError {}

/// Decodes the source `Codec.DecodeToChunk` column sequence.
///
/// The returned suffix is the exact unconsumed input, matching Go's decoder.
/// The parser validates every slice boundary and offset before borrowing the
/// input. It does not validate typed payloads; `RawColumn::data` remains the
/// ownership boundary for `FieldType`/`Datum` implementations.
pub fn decode_columns<'a>(
    mut input: &'a [u8],
    layouts: &[ColumnLayout],
) -> Result<(&'a [u8], Vec<RawColumn<'a>>), ColumnCodecError> {
    let mut columns = Vec::with_capacity(layouts.len());
    for (column, layout) in layouts.iter().copied().enumerate() {
        let (header, remainder) = take(input, HEADER_BYTES, column, "header")?;
        let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let null_count = u32::from_le_bytes(header[4..].try_into().unwrap()) as usize;
        if null_count > length {
            return Err(ColumnCodecError::InvalidNullCount {
                column,
                null_count,
                length,
            });
        }
        input = remainder;

        let bitmap_len = if null_count > 0 {
            length.div_ceil(8)
        } else {
            0
        };
        let null_bitmap = if bitmap_len == 0 {
            None
        } else {
            let (bitmap, remainder) = take(input, bitmap_len, column, "null bitmap")?;
            input = remainder;
            Some(bitmap)
        };

        let (offsets, data_len) = match layout.fixed_width {
            Some(width) => {
                let data_len =
                    width
                        .checked_mul(length)
                        .ok_or(ColumnCodecError::FixedDataLengthOverflow {
                            column,
                            width,
                            length,
                        })?;
                (None, data_len)
            }
            None => {
                let offset_count =
                    length
                        .checked_add(1)
                        .ok_or(ColumnCodecError::FixedDataLengthOverflow {
                            column,
                            width: OFFSET_BYTES,
                            length,
                        })?;
                let offset_bytes = offset_count.checked_mul(OFFSET_BYTES).ok_or(
                    ColumnCodecError::FixedDataLengthOverflow {
                        column,
                        width: OFFSET_BYTES,
                        length: offset_count,
                    },
                )?;
                let (raw_offsets, remainder) = take(input, offset_bytes, column, "offset table")?;
                input = remainder;
                let mut offsets = Vec::with_capacity(offset_count);
                for (index, bytes) in raw_offsets.chunks_exact(OFFSET_BYTES).enumerate() {
                    let value = i64::from_le_bytes(bytes.try_into().unwrap());
                    if value < 0 || (index > 0 && value < offsets[index - 1]) {
                        return Err(ColumnCodecError::InvalidOffset {
                            column,
                            offset_index: index,
                            value,
                        });
                    }
                    offsets.push(value);
                }
                if offsets.first().copied() != Some(0) {
                    return Err(ColumnCodecError::InvalidOffset {
                        column,
                        offset_index: 0,
                        value: offsets[0],
                    });
                }
                let data_len = usize::try_from(*offsets.last().unwrap()).map_err(|_| {
                    ColumnCodecError::InvalidOffset {
                        column,
                        offset_index: offsets.len() - 1,
                        value: *offsets.last().unwrap(),
                    }
                })?;
                (Some(offsets), data_len)
            }
        };

        let (data, remainder) = take(input, data_len, column, "data")?;
        input = remainder;
        columns.push(RawColumn {
            length,
            null_count,
            null_bitmap,
            offsets,
            data,
        });
    }
    Ok((input, columns))
}

fn take<'a>(
    input: &'a [u8],
    needed: usize,
    column: usize,
    section: &'static str,
) -> Result<(&'a [u8], &'a [u8]), ColumnCodecError> {
    let (taken, remainder) =
        input
            .split_at_checked(needed)
            .ok_or(ColumnCodecError::InsufficientBytes {
                column,
                section,
                needed,
                available: input.len(),
            })?;
    Ok((taken, remainder))
}
