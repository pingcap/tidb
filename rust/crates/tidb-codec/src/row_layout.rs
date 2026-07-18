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

//! Dependency-closed framing for TiDB's new row codec.
//!
//! The production row value starts with a six-byte header, followed by sorted
//! not-null and null column IDs, sorted end offsets for the not-null values,
//! and the opaque value bytes.  This module owns only that framing and the
//! column-ID/offset decisions from `pkg/util/rowcodec/{common,row}.go`.
//! Typed datum conversion, checksums, schema defaults, handles, and the
//! encoder/decoder remain separate owners.

use std::fmt;
use std::ops::Range;

/// Version byte of TiDB's new row format (`rowcodec.CodecVer`).
pub const ROW_CODEC_VERSION: u8 = 128;
/// The row uses four-byte column IDs and offsets instead of one-/two-byte
/// metadata when this flag is set.
pub const ROW_FLAG_LARGE: u8 = 1;
/// The row carries a checksum trailer when this flag is set.
pub const ROW_FLAG_CHECKSUM: u8 = 1 << 1;
/// Number of bytes in the fixed row header.
pub const ROW_HEADER_LEN: usize = 6;
/// Checksum version bits in the checksum header.
pub const CHECKSUM_VERSION_MASK: u8 = 0b0111;
/// Extra checksum bit in the checksum header.
pub const CHECKSUM_FLAG_EXTRA: u8 = 0b1000;

/// The fixed metadata at the start of a new-format row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowHeader {
    flags: u8,
    not_null_count: u16,
    null_count: u16,
}

impl RowHeader {
    /// Parses the six-byte header and returns the unconsumed suffix.
    pub fn parse(input: &[u8]) -> Result<(Self, &[u8]), RowCodecError> {
        let header = input
            .get(..ROW_HEADER_LEN)
            .ok_or(RowCodecError::InsufficientBytes {
                section: "row header",
                needed: ROW_HEADER_LEN,
                available: input.len(),
            })?;
        if header[0] != ROW_CODEC_VERSION {
            return Err(RowCodecError::InvalidCodecVersion { found: header[0] });
        }
        Ok((
            Self {
                flags: header[1],
                not_null_count: u16::from_le_bytes([header[2], header[3]]),
                null_count: u16::from_le_bytes([header[4], header[5]]),
            },
            &input[ROW_HEADER_LEN..],
        ))
    }

    /// Creates a header from its source fields.
    pub const fn new(flags: u8, not_null_count: u16, null_count: u16) -> Self {
        Self {
            flags,
            not_null_count,
            null_count,
        }
    }

    /// Returns the source row flags.
    pub const fn flags(self) -> u8 {
        self.flags
    }

    /// Returns the count of values with non-NULL payload bytes.
    pub const fn not_null_count(self) -> u16 {
        self.not_null_count
    }

    /// Returns the count of NULL columns.
    pub const fn null_count(self) -> u16 {
        self.null_count
    }

    /// Returns whether metadata uses four-byte IDs and offsets.
    pub const fn is_large(self) -> bool {
        self.flags & ROW_FLAG_LARGE != 0
    }

    /// Returns whether the value carries a checksum trailer.
    pub const fn has_checksum(self) -> bool {
        self.flags & ROW_FLAG_CHECKSUM != 0
    }

    /// Returns the encoded column-ID width in bytes.
    pub const fn column_id_width(self) -> usize {
        if self.is_large() {
            4
        } else {
            1
        }
    }

    /// Returns the encoded offset width in bytes.
    pub const fn offset_width(self) -> usize {
        if self.is_large() {
            4
        } else {
            2
        }
    }

    /// Returns the total number of column IDs.
    pub const fn column_count(self) -> usize {
        self.not_null_count as usize + self.null_count as usize
    }
}

/// Checksum framing preserved after the row's opaque data bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowChecksum {
    header: u8,
    checksum: u32,
    extra_checksum: Option<u32>,
}

impl RowChecksum {
    /// Returns the checksum header, including version and extra-checksum bit.
    pub const fn header(self) -> u8 {
        self.header
    }

    /// Returns the source checksum version (0, 1, or 2).
    pub const fn version(self) -> u8 {
        self.header & CHECKSUM_VERSION_MASK
    }

    /// Returns the primary checksum value.
    pub const fn checksum(self) -> u32 {
        self.checksum
    }

    /// Returns the optional extra checksum value.
    pub const fn extra_checksum(self) -> Option<u32> {
        self.extra_checksum
    }
}

/// Result of looking up an encoded column ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnLookup {
    /// The column has value bytes at the returned not-null index.
    NotNull(usize),
    /// The column is explicitly encoded as SQL NULL.
    Null,
    /// The row does not carry the requested column.
    Missing,
}

/// Safe, typed framing view over one new-format row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowLayout<'a> {
    header: RowHeader,
    column_ids: Vec<u32>,
    offsets: Vec<u32>,
    data: &'a [u8],
    checksum: Option<RowChecksum>,
}

impl<'a> RowLayout<'a> {
    /// Parses row metadata, offsets, opaque value bytes, and an optional
    /// checksum trailer. The returned suffix is not part of this row.
    pub fn parse(input: &'a [u8]) -> Result<(Self, &'a [u8]), RowCodecError> {
        let (header, mut cursor) = RowHeader::parse(input)?;
        let id_width = header.column_id_width();
        let offset_width = header.offset_width();
        let id_bytes = checked_len(header.column_count(), id_width, "column IDs")?;
        let ids_raw = take(&mut cursor, id_bytes, "column IDs")?;
        let mut column_ids = Vec::with_capacity(header.column_count());
        for bytes in ids_raw.chunks_exact(id_width) {
            column_ids.push(if id_width == 1 {
                bytes[0] as u32
            } else {
                u32::from_le_bytes(bytes.try_into().expect("four-byte column ID"))
            });
        }

        let offset_bytes = checked_len(header.not_null_count() as usize, offset_width, "offsets")?;
        let offsets_raw = take(&mut cursor, offset_bytes, "offsets")?;
        let mut offsets = Vec::with_capacity(header.not_null_count() as usize);
        let mut previous = 0_u32;
        for (index, bytes) in offsets_raw.chunks_exact(offset_width).enumerate() {
            let offset = if offset_width == 2 {
                u16::from_le_bytes(bytes.try_into().expect("two-byte offset")) as u32
            } else {
                u32::from_le_bytes(bytes.try_into().expect("four-byte offset"))
            };
            if offset < previous {
                return Err(RowCodecError::InvalidOffset {
                    index,
                    value: offset,
                });
            }
            offsets.push(offset);
            previous = offset;
        }

        let data_len = offsets.last().copied().unwrap_or(0) as usize;
        let data = take(&mut cursor, data_len, "row data")?;
        let checksum = if header.has_checksum() {
            Some(parse_checksum(&mut cursor)?)
        } else {
            None
        };
        Ok((
            Self {
                header,
                column_ids,
                offsets,
                data,
                checksum,
            },
            cursor,
        ))
    }

    /// Returns the parsed row header.
    pub const fn header(&self) -> RowHeader {
        self.header
    }

    /// Returns all IDs in source order: not-null IDs, then null IDs.
    pub fn column_ids(&self) -> &[u32] {
        &self.column_ids
    }

    /// Returns the not-null ID partition.
    pub fn not_null_column_ids(&self) -> &[u32] {
        &self.column_ids[..self.header.not_null_count as usize]
    }

    /// Returns the null ID partition.
    pub fn null_column_ids(&self) -> &[u32] {
        &self.column_ids[self.header.not_null_count as usize..]
    }

    /// Returns the end offset for each not-null value.
    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }

    /// Returns the opaque data region, excluding checksum bytes.
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// Returns checksum metadata when the row has the checksum flag.
    pub const fn checksum(&self) -> Option<RowChecksum> {
        self.checksum
    }

    /// Finds a column using the source's two sorted ID partitions.
    pub fn find_column(&self, column_id: i64) -> ColumnLookup {
        let id = match u32::try_from(column_id) {
            Ok(id) => id,
            Err(_) => return ColumnLookup::Missing,
        };
        match self.not_null_column_ids().binary_search(&id) {
            Ok(index) => ColumnLookup::NotNull(index),
            Err(_) => match self.null_column_ids().binary_search(&id) {
                Ok(_) => ColumnLookup::Null,
                Err(_) => ColumnLookup::Missing,
            },
        }
    }

    /// Mirrors Go `ColumnIsNull`: an absent column is null iff its default is
    /// absent. This method does not inspect default bytes or schema metadata.
    pub fn column_is_null(&self, column_id: i64, default_is_null: bool) -> bool {
        match self.find_column(column_id) {
            ColumnLookup::NotNull(_) => false,
            ColumnLookup::Null => true,
            ColumnLookup::Missing => default_is_null,
        }
    }

    /// Returns one not-null value's byte range.
    pub fn value_range(&self, index: usize) -> Result<Range<usize>, RowCodecError> {
        let end = *self
            .offsets
            .get(index)
            .ok_or(RowCodecError::ValueIndexOutOfRange {
                index,
                count: self.offsets.len(),
            })? as usize;
        let start = if index == 0 {
            0
        } else {
            self.offsets[index - 1] as usize
        };
        Ok(start..end)
    }

    /// Returns one not-null value's opaque bytes.
    pub fn value(&self, index: usize) -> Result<&'a [u8], RowCodecError> {
        let range = self.value_range(index)?;
        Ok(&self.data[range])
    }
}

/// Returns whether a row begins with the new-format version byte.
#[must_use]
pub fn is_new_format(row_data: &[u8]) -> bool {
    row_data.first() == Some(&ROW_CODEC_VERSION)
}

/// Returns whether a key has the complete legacy table-record row prefix.
///
/// The length check intentionally mirrors `pkg/util/rowcodec/common.go`:
/// this is not a general table-key parser and does not validate the table ID
/// or handle bytes.
#[must_use]
pub fn is_row_key(key: &[u8]) -> bool {
    key.len() >= 19 && key.first() == Some(&b't') && key.get(10) == Some(&b'r')
}

/// Errors raised while parsing row metadata and value boundaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowCodecError {
    /// A declared section does not fit in the input.
    InsufficientBytes {
        /// Logical section that ended early.
        section: &'static str,
        /// Bytes required by the section.
        needed: usize,
        /// Bytes available at the section boundary.
        available: usize,
    },
    /// The row version byte is not the new-format version.
    InvalidCodecVersion {
        /// Version byte observed in the input.
        found: u8,
    },
    /// A checksum version outside the source-compatible 0/1/2 range.
    InvalidChecksumVersion {
        /// Version bits observed in the checksum header.
        version: u8,
    },
    /// An offset decreased relative to the preceding end offset.
    InvalidOffset {
        /// Offset index in the not-null value table.
        index: usize,
        /// Invalid offset value.
        value: u32,
    },
    /// A value accessor named a non-existent not-null value.
    ValueIndexOutOfRange {
        /// Requested value index.
        index: usize,
        /// Number of not-null values.
        count: usize,
    },
}

impl fmt::Display for RowCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientBytes {
                section,
                needed,
                available,
            } => write!(
                formatter,
                "row {section} needs {needed} bytes, but only {available} remain"
            ),
            Self::InvalidCodecVersion { found } => {
                write!(formatter, "invalid row codec version {found}")
            }
            Self::InvalidChecksumVersion { version } => {
                write!(formatter, "invalid row checksum version {version}")
            }
            Self::InvalidOffset { index, value } => {
                write!(formatter, "row offset {value} decreases at index {index}")
            }
            Self::ValueIndexOutOfRange { index, count } => {
                write!(
                    formatter,
                    "row value index {index} is outside {count} values"
                )
            }
        }
    }
}

impl std::error::Error for RowCodecError {}

fn checked_len(count: usize, width: usize, section: &'static str) -> Result<usize, RowCodecError> {
    count
        .checked_mul(width)
        .ok_or(RowCodecError::InsufficientBytes {
            section,
            needed: usize::MAX,
            available: 0,
        })
}

fn take<'a>(
    input: &mut &'a [u8],
    needed: usize,
    section: &'static str,
) -> Result<&'a [u8], RowCodecError> {
    let (taken, remainder) =
        input
            .split_at_checked(needed)
            .ok_or(RowCodecError::InsufficientBytes {
                section,
                needed,
                available: input.len(),
            })?;
    *input = remainder;
    Ok(taken)
}

fn parse_checksum(input: &mut &[u8]) -> Result<RowChecksum, RowCodecError> {
    let header = take(input, 1, "checksum header")?[0];
    let version = header & CHECKSUM_VERSION_MASK;
    if !matches!(version, 0..=2) {
        return Err(RowCodecError::InvalidChecksumVersion { version });
    }
    let checksum = u32::from_le_bytes(take(input, 4, "checksum")?.try_into().unwrap());
    let extra_checksum = if header & CHECKSUM_FLAG_EXTRA != 0 {
        Some(u32::from_le_bytes(
            take(input, 4, "extra checksum")?.try_into().unwrap(),
        ))
    } else {
        None
    };
    Ok(RowChecksum {
        header,
        checksum,
        extra_checksum,
    })
}
