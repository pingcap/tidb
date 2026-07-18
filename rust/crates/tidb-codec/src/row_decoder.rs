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

//! Dependency-closed raw decoding from `pkg/util/rowcodec/decoder.go`.
//!
//! The Go decoder combines row framing with schema-aware Datum, chunk,
//! handle, timezone, and old-row conversion.  Those callers are not yet one
//! Rust ownership boundary.  This module therefore ports only the part that
//! is safe to share now: parse one [`RowLayout`], locate a column in its two
//! sorted ID partitions, borrow its opaque bytes, and decode the compact
//! little-endian integer widths used by the source decoder.  Typed FieldType
//! dispatch, defaults, handles, and warning/error policy stay outside this
//! leaf.

use std::fmt;

use crate::{ColumnLookup, RowCodecError, RowLayout};

/// One column result from a raw row lookup.
///
/// A not-null result carries the source not-null ordinal as well as the
/// already-bounded opaque value bytes.  SQL NULL and a missing column remain
/// distinct: the caller owns deciding whether a missing value has a schema
/// default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawRowValue<'a> {
    /// The column is present in the not-null partition.
    NotNull {
        /// Ordinal in the row's not-null value/offset table.
        index: usize,
        /// Opaque bytes bounded by the row's offset table.
        bytes: &'a [u8],
    },
    /// The column is explicitly encoded as SQL NULL.
    Null,
    /// The row does not carry this column.
    Missing,
}

/// Errors raised by raw row decoding after row framing has been validated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowDecodeError {
    /// The row-level framing or offset table is malformed.
    Layout(RowCodecError),
    /// A compact integer payload is not one of Go's 1/2/4/8-byte widths.
    InvalidIntegerWidth {
        /// Whether the requested interpretation is signed.
        signed: bool,
        /// Number of payload bytes supplied by the caller.
        width: usize,
    },
}

impl fmt::Display for RowDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Layout(error) => write!(formatter, "row layout: {error}"),
            Self::InvalidIntegerWidth { signed, width } => {
                let kind = if *signed { "signed" } else { "unsigned" };
                write!(formatter, "row {kind} integer has invalid width {width}")
            }
        }
    }
}

impl std::error::Error for RowDecodeError {}

impl From<RowCodecError> for RowDecodeError {
    fn from(error: RowCodecError) -> Self {
        Self::Layout(error)
    }
}

/// Borrowed decoder for one new-format row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowDecoder<'a> {
    layout: RowLayout<'a>,
}

impl<'a> RowDecoder<'a> {
    /// Parses one row and returns the unconsumed suffix.
    ///
    /// [`RowLayout`] remains the owner of physical header/offset/checksum
    /// framing; this wrapper adds the source decoder's raw lookup surface.
    pub fn parse(input: &'a [u8]) -> Result<(Self, &'a [u8]), RowDecodeError> {
        let (layout, remainder) = RowLayout::parse(input)?;
        Ok((Self { layout }, remainder))
    }

    /// Returns the underlying source-compatible row layout.
    pub const fn layout(&self) -> &RowLayout<'a> {
        &self.layout
    }

    /// Finds a column and borrows its opaque value bytes when present.
    ///
    /// Negative IDs cannot occur in the u32 row metadata and are therefore
    /// classified as missing, matching the source binary-search contract.
    pub fn column(&self, column_id: i64) -> Result<RawRowValue<'a>, RowDecodeError> {
        match self.layout.find_column(column_id) {
            ColumnLookup::NotNull(index) => Ok(RawRowValue::NotNull {
                index,
                bytes: self.layout.value(index)?,
            }),
            ColumnLookup::Null => Ok(RawRowValue::Null),
            ColumnLookup::Missing => Ok(RawRowValue::Missing),
        }
    }
}

/// Decodes one compact signed row payload.
///
/// This is Go `decodeInt`: rowcodec stores signed integers in the shortest
/// little-endian two's-complement width (1, 2, 4, or 8 bytes).  Unlike the Go
/// helper, malformed widths are returned as a typed error instead of causing
/// an out-of-bounds panic in the default 8-byte branch.
pub fn decode_raw_int(input: &[u8]) -> Result<i64, RowDecodeError> {
    match input.len() {
        1 => Ok(i64::from(i8::from_le_bytes([input[0]]))),
        2 => Ok(i64::from(i16::from_le_bytes(
            input.try_into().expect("two-byte width"),
        ))),
        4 => Ok(i64::from(i32::from_le_bytes(
            input.try_into().expect("four-byte width"),
        ))),
        8 => Ok(i64::from_le_bytes(
            input.try_into().expect("eight-byte width"),
        )),
        width => Err(RowDecodeError::InvalidIntegerWidth {
            signed: true,
            width,
        }),
    }
}

/// Decodes one compact unsigned row payload.
///
/// This is Go `decodeUint`: rowcodec stores unsigned integers in the shortest
/// little-endian 1/2/4/8-byte width.  A non-source width is rejected before
/// any integer conversion so malformed row values cannot panic or silently
/// truncate.
pub fn decode_raw_uint(input: &[u8]) -> Result<u64, RowDecodeError> {
    match input.len() {
        1 => Ok(u64::from(input[0])),
        2 => Ok(u64::from(u16::from_le_bytes(
            input.try_into().expect("two-byte width"),
        ))),
        4 => Ok(u64::from(u32::from_le_bytes(
            input.try_into().expect("four-byte width"),
        ))),
        8 => Ok(u64::from_le_bytes(
            input.try_into().expect("eight-byte width"),
        )),
        width => Err(RowDecodeError::InvalidIntegerWidth {
            signed: false,
            width,
        }),
    }
}
