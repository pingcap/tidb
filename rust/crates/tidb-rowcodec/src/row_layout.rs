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
use std::sync::OnceLock;

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
        if input[0] != ROW_CODEC_VERSION {
            return Err(RowCodecError::InvalidCodecVersion { found: input[0] });
        }
        Ok((
            Self {
                flags: input[1],
                not_null_count: u16::from_le_bytes(input[2..4].try_into().unwrap()),
                null_count: u16::from_le_bytes(input[4..6].try_into().unwrap()),
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

/// Borrowed, little-endian metadata with an optional decoded slice view.
#[derive(Clone)]
struct EncodedNumbers<'a> {
    bytes: &'a [u8],
    width: usize,
    decoded: OnceLock<Box<[u32]>>,
}

impl<'a> EncodedNumbers<'a> {
    fn new(bytes: &'a [u8], width: usize) -> Self {
        Self {
            bytes,
            width,
            decoded: OnceLock::new(),
        }
    }

    fn get(&self, index: usize) -> u32 {
        match self.width {
            1 => u32::from(self.bytes[index]),
            2 => u32::from(u16::from_le_bytes(self.bytes.as_chunks::<2>().0[index])),
            _ => u32::from_le_bytes(self.bytes.as_chunks::<4>().0[index]),
        }
    }

    fn as_slice(&self) -> &[u32] {
        self.decoded.get_or_init(|| match self.width {
            1 => self.bytes.iter().copied().map(u32::from).collect(),
            2 => self
                .bytes
                .as_chunks::<2>()
                .0
                .iter()
                .copied()
                .map(|bytes| u32::from(u16::from_le_bytes(bytes)))
                .collect(),
            _ => self
                .bytes
                .as_chunks::<4>()
                .0
                .iter()
                .copied()
                .map(u32::from_le_bytes)
                .collect(),
        })
    }

    fn binary_search(&self, range: Range<usize>, id: u32) -> Option<usize> {
        let start = range.start;
        let index = if self.width == 1 {
            u8::try_from(id).ok()?;
            search_ids(&self.bytes[range], id, |byte| u32::from(*byte))
        } else {
            let ids = &self.bytes.as_chunks::<4>().0[range];
            search_ids(ids, id, |bytes| u32::from_le_bytes(*bytes))
        };
        index.map(|index| start + index)
    }
}

fn search_ids<T>(ids: &[T], id: u32, decode: impl Fn(&T) -> u32) -> Option<usize> {
    if ids.len() <= 8 {
        return search_ids_by_midpoint(ids, id, &decode);
    }
    let middle = ids.len() / 2;
    let (ids, start) = match decode(ids.get(middle)?).cmp(&id) {
        std::cmp::Ordering::Equal => return Some(middle),
        std::cmp::Ordering::Less => (&ids[middle + 1..], middle + 1),
        std::cmp::Ordering::Greater => (&ids[..middle], 0),
    };

    let index = ids.binary_search_by_key(&id, &decode).ok()?;
    let previous_matches = index > 0 && decode(&ids[index - 1]) == id;
    let next_matches = ids.get(index + 1).is_some_and(|next| decode(next) == id);
    if !previous_matches && !next_matches {
        return Some(start + index);
    }
    // With duplicates, Go selects the first matching midpoint.
    search_ids_by_midpoint(ids, id, &decode).map(|index| start + index)
}

fn search_ids_by_midpoint<T>(ids: &[T], id: u32, decode: impl Fn(&T) -> u32) -> Option<usize> {
    let (mut low, mut high) = (0, ids.len());
    while low < high {
        let middle = low + (high - low) / 2;
        match decode(&ids[middle]).cmp(&id) {
            std::cmp::Ordering::Equal => return Some(middle),
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
        }
    }
    None
}

impl PartialEq for EncodedNumbers<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.width == other.width && self.bytes == other.bytes
    }
}

impl Eq for EncodedNumbers<'_> {}

impl fmt::Debug for EncodedNumbers<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(formatter)
    }
}

/// Safe, typed framing view over one new-format row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowLayout<'a> {
    header: RowHeader,
    column_ids: EncodedNumbers<'a>,
    offsets: EncodedNumbers<'a>,
    data: &'a [u8],
    checksum: Option<RowChecksum>,
}

impl<'a> RowLayout<'a> {
    /// Parses row metadata, offsets, opaque value bytes, and an optional
    /// checksum trailer. The returned suffix is not part of this row.
    #[inline]
    pub fn parse(input: &'a [u8]) -> Result<(Self, &'a [u8]), RowCodecError> {
        let (header, mut cursor) = RowHeader::parse(input)?;
        let id_width = header.column_id_width();
        let offset_width = header.offset_width();
        let not_null_count = usize::from(header.not_null_count());
        let column_ids = EncodedNumbers::new(
            take(&mut cursor, header.column_count() * id_width),
            id_width,
        );
        let offsets = EncodedNumbers::new(
            take(&mut cursor, not_null_count * offset_width),
            offset_width,
        );
        let data_len = not_null_count
            .checked_sub(1)
            .map_or(0, |index| offsets.get(index) as usize);
        let data = take(&mut cursor, data_len);
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
        self.column_ids.as_slice()
    }

    /// Returns the not-null ID partition.
    pub fn not_null_column_ids(&self) -> &[u32] {
        &self.column_ids()[..self.header.not_null_count as usize]
    }

    /// Returns the null ID partition.
    pub fn null_column_ids(&self) -> &[u32] {
        &self.column_ids()[self.header.not_null_count as usize..]
    }

    /// Returns the end offset for each not-null value.
    pub fn offsets(&self) -> &[u32] {
        self.offsets.as_slice()
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
    #[inline]
    pub fn find_column(&self, column_id: i64) -> ColumnLookup {
        let id = match u32::try_from(column_id) {
            Ok(id) => id,
            Err(_) => return ColumnLookup::Missing,
        };
        let not_null_count = usize::from(self.header.not_null_count());
        let column_count = self.header.column_count();
        if let Some(index) = self.column_ids.binary_search(0..not_null_count, id) {
            ColumnLookup::NotNull(index)
        } else if self
            .column_ids
            .binary_search(not_null_count..column_count, id)
            .is_some()
        {
            ColumnLookup::Null
        } else {
            ColumnLookup::Missing
        }
    }

    /// Mirrors Go `ColumnIsNull`: an absent column is null iff its default is
    /// absent. This method does not inspect default bytes or schema metadata.
    #[inline]
    pub fn column_is_null(&self, column_id: i64, default_is_null: bool) -> bool {
        match self.find_column(column_id) {
            ColumnLookup::NotNull(_) => false,
            ColumnLookup::Null => true,
            ColumnLookup::Missing => default_is_null,
        }
    }

    /// Returns one not-null value's byte range.
    pub fn value_range(&self, index: usize) -> Result<Range<usize>, RowCodecError> {
        let end = self.offsets.get(index) as usize;
        let start = if index == 0 {
            0
        } else {
            self.offsets.get(index - 1) as usize
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
pub fn is_new_format(row_data: &[u8]) -> bool {
    row_data[0] == ROW_CODEC_VERSION
}

/// Returns whether a key has the complete legacy table-record row prefix.
///
/// The length check intentionally mirrors `pkg/util/rowcodec/common.go`:
/// this is not a general table-key parser and does not validate the table ID
/// or handle bytes.
pub fn is_row_key(key: &[u8]) -> bool {
    key.len() >= 19 && key.first() == Some(&b't') && key.get(10) == Some(&b'r')
}

/// Errors raised while parsing row metadata and value boundaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowCodecError {
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
}

impl fmt::Display for RowCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCodecVersion { found } => {
                write!(formatter, "invalid row codec version {found}")
            }
            Self::InvalidChecksumVersion { version } => {
                write!(formatter, "invalid row checksum version {version}")
            }
        }
    }
}

impl std::error::Error for RowCodecError {}

fn take<'a>(input: &mut &'a [u8], needed: usize) -> &'a [u8] {
    let (taken, remainder) = input.split_at(needed);
    *input = remainder;
    taken
}

#[inline]
fn parse_checksum(input: &mut &[u8]) -> Result<RowChecksum, RowCodecError> {
    let header = take(input, 1)[0];
    let version = header & CHECKSUM_VERSION_MASK;
    if !matches!(version, 0..=2) {
        return Err(RowCodecError::InvalidChecksumVersion { version });
    }
    let checksum = u32::from_le_bytes(take(input, 4).try_into().unwrap());
    let extra_checksum = if header & CHECKSUM_FLAG_EXTRA != 0 {
        Some(u32::from_le_bytes(take(input, 4).try_into().unwrap()))
    } else {
        None
    };
    Ok(RowChecksum {
        header,
        checksum,
        extra_checksum,
    })
}
