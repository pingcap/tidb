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

//! Raw tipb SelectResponse and Chunk boundaries.
//!
//! TiDB's `selectResult.fetchResp` first unmarshals a tipb `SelectResponse`,
//! then consumes each `Chunk` according to its response-level `EncodeType`.
//! This module owns the wire/metadata boundary plus source-shaped default and
//! columnar byte framing. A bounded scalar conversion is exposed only for
//! source-proven integer/float/string columns; temporal, JSON, vector, and
//! schema-dependent conversions still depend on the unported Go
//! `types`/`chunk` owners and must not be guessed.

use std::fmt;

use prost::Message;
use tidb_codec::{
    decode_column_datums, decode_columns, decode_default_rows, CodecError, ColumnCodecError,
    ColumnLayout, RawColumn, RawValue, TypedColumnError,
};
use tidb_datatype::{Datum, FieldType};
use tidb_proto::{Chunk, EncodeType, RowMeta, SelectResponse};

/// A raw row slice described by a tipb [`RowMeta`].
///
/// `data` borrows the original `Chunk.rows_data`; no row codec or Datum
/// interpretation is implied by this type. The optional handle is retained
/// as decoded instead of replacing a missing proto2 scalar with an invented
/// value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawChunkRow<'a> {
    /// The optional TiDB row handle from the wire metadata.
    pub handle: Option<i64>,
    /// The byte range belonging to this row.
    pub data: &'a [u8],
}

/// A chunk whose protobuf envelope and row metadata are decoded, while its
/// payload remains opaque to the future typed codec owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawChunk<'a> {
    /// The response/channel encoding selected by the caller.
    pub encode_type: EncodeType,
    /// All bytes carried by `Chunk.rows_data`.
    pub rows_data: &'a [u8],
    /// Metadata-described row slices, when the wire payload supplies them.
    pub rows: Vec<RawChunkRow<'a>>,
}

impl RawChunk<'_> {
    /// Returns an explicit boundary error rather than guessing typed Datum
    /// values from an unowned TiDB chunk codec.
    pub fn decode_typed_rows(&self) -> Result<(), ChunkDecodeError> {
        Err(ChunkDecodeError::UnsupportedTypedRowDecoding {
            encode_type: self.encode_type,
        })
    }
}

/// A `TypeChunk` payload after source-shaped column framing, with Datum values
/// still opaque to the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawColumnarChunk<'a> {
    /// Bytes left after the requested columns, matching Go `DecodeToChunk`.
    pub remainder: &'a [u8],
    /// Columns decoded in the caller-provided declaration order.
    pub columns: Vec<RawColumn<'a>>,
}

/// A `TypeChunk` payload after source column framing and the bounded scalar
/// `Datum` conversion have both succeeded.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedColumnarChunk<'a> {
    /// Bytes left after the requested columns, matching Go `DecodeToChunk`.
    pub remainder: &'a [u8],
    /// Typed columns in the caller-provided declaration order.
    pub columns: Vec<Vec<Datum>>,
}

impl RawColumnarChunk<'_> {
    /// Converts only the source-proven scalar subset of each raw column.
    ///
    /// The raw `remainder` is retained exactly. Unsupported field types return
    /// an explicit error instead of consuming bytes with a guessed temporal,
    /// decimal, JSON, enum/set, vector, or CHBlock interpretation.
    pub fn decode_datums(
        &self,
        field_types: &[FieldType],
    ) -> Result<TypedColumnarChunk<'_>, ChunkDecodeError> {
        if field_types.len() != self.columns.len() {
            return Err(ChunkDecodeError::ColumnCountMismatch {
                expected: field_types.len(),
                actual: self.columns.len(),
            });
        }
        let columns = self
            .columns
            .iter()
            .zip(field_types.iter().cloned())
            .map(|(column, field_type)| {
                decode_column_datums(column, field_type).map_err(ChunkDecodeError::TypedColumnCodec)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TypedColumnarChunk {
            remainder: self.remainder,
            columns,
        })
    }
}

/// Errors raised while validating a raw tipb chunk boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChunkDecodeError {
    /// The response contained an enum value not present in the checked-in
    /// tipb contract.
    InvalidEncodeType(i32),
    /// A proto2 row length was omitted. Upstream Go's gogoproto projection
    /// treats this scalar as non-nullable, so accepting the omission here
    /// would silently invent a row boundary.
    MissingRowLength {
        /// Index of the malformed row metadata entry.
        row_index: usize,
    },
    /// Row lengths are signed on the wire; negative lengths cannot describe a
    /// byte range.
    NegativeRowLength {
        /// Index of the malformed row metadata entry.
        row_index: usize,
        /// Signed length received from the wire.
        length: i64,
    },
    /// A declared row extends past the bytes carried by `rows_data`.
    RowLengthExceedsData {
        /// Index of the malformed row metadata entry.
        row_index: usize,
        /// Declared row length.
        length: usize,
        /// Bytes still available at that row boundary.
        remaining: usize,
    },
    /// Metadata did not account for every byte in `rows_data`.
    RowDataLengthMismatch {
        /// Bytes consumed by all row metadata entries.
        declared: usize,
        /// Bytes carried by the chunk payload.
        actual: usize,
    },
    /// The requested encoding has no bounded raw decoder at this boundary, or
    /// still needs typed Datum/native CHBlock semantics from a future owner.
    UnsupportedTypedRowDecoding {
        /// Encoding whose typed codec has not crossed this boundary.
        encode_type: EncodeType,
    },
    /// The bounded default-value framing rejected a payload before typed
    /// Datum conversion.
    DefaultCodec(CodecError),
    /// The bounded columnar framing rejected a payload before typed Datum
    /// conversion.
    ColumnarCodec(ColumnCodecError),
    /// The caller supplied a different number of `FieldType`s than columns.
    ColumnCountMismatch {
        /// Number of supplied field types.
        expected: usize,
        /// Number of framed columns.
        actual: usize,
    },
    /// The bounded scalar typed conversion rejected a raw column.
    TypedColumnCodec(TypedColumnError),
}

impl fmt::Display for ChunkDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEncodeType(value) => write!(f, "invalid tipb encode type {value}"),
            Self::MissingRowLength { row_index } => {
                write!(f, "tipb row metadata {row_index} is missing length")
            }
            Self::NegativeRowLength { row_index, length } => {
                write!(
                    f,
                    "tipb row metadata {row_index} has negative length {length}"
                )
            }
            Self::RowLengthExceedsData {
                row_index,
                length,
                remaining,
            } => write!(
                f,
                "tipb row metadata {row_index} declares {length} bytes, but only {remaining} remain"
            ),
            Self::RowDataLengthMismatch { declared, actual } => write!(
                f,
                "tipb row metadata accounts for {declared} bytes, but rows_data contains {actual}"
            ),
            Self::UnsupportedTypedRowDecoding { encode_type } => write!(
                f,
                "typed tipb row decoding is not implemented for {encode_type:?}"
            ),
            Self::DefaultCodec(error) => write!(f, "default row codec error: {error}"),
            Self::ColumnarCodec(error) => write!(f, "columnar codec error: {error}"),
            Self::ColumnCountMismatch { expected, actual } => write!(
                f,
                "typed chunk decode expects {expected} field types, but {actual} columns were framed"
            ),
            Self::TypedColumnCodec(error) => write!(f, "typed column codec error: {error}"),
        }
    }
}

impl std::error::Error for ChunkDecodeError {}

impl RawChunk<'_> {
    /// Decodes the source `EncodeValue` framing for a `TypeDefault` chunk.
    ///
    /// This returns raw tags and payload slices. Temporal, JSON, vector, and
    /// schema-dependent Datum conversion remain with their future owners.
    pub fn decode_default_values(
        &self,
        column_count: usize,
    ) -> Result<Vec<Vec<RawValue<'_>>>, ChunkDecodeError> {
        if self.encode_type != EncodeType::TypeDefault {
            return Err(ChunkDecodeError::UnsupportedTypedRowDecoding {
                encode_type: self.encode_type,
            });
        }
        decode_default_rows(self.rows_data, column_count).map_err(ChunkDecodeError::DefaultCodec)
    }

    /// Materializes the source-proven scalar subset of a `TypeDefault` row.
    ///
    /// This keeps row framing and typed conversion separate: each value is
    /// decoded by `codec.DecodeOne`-equivalent logic, while temporal, JSON,
    /// vector, and sentinel tags return an explicit codec error until their
    /// owning `Datum` representations are ported.
    pub fn decode_default_datums(
        &self,
        column_count: usize,
    ) -> Result<Vec<Vec<Datum>>, ChunkDecodeError> {
        self.decode_default_values(column_count)?
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(RawValue::decode_datum)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ChunkDecodeError::DefaultCodec)
            })
            .collect()
    }

    /// Decodes source `chunk.Codec` framing for a `TypeChunk` payload.
    ///
    /// `ColumnLayout` is explicit physical metadata, not an inferred MySQL
    /// type map. The returned columns still contain opaque bytes and the exact
    /// unconsumed suffix.
    pub fn decode_columnar(
        &self,
        layouts: &[ColumnLayout],
    ) -> Result<RawColumnarChunk<'_>, ChunkDecodeError> {
        if self.encode_type != EncodeType::TypeChunk {
            return Err(ChunkDecodeError::UnsupportedTypedRowDecoding {
                encode_type: self.encode_type,
            });
        }
        let (remainder, columns) =
            decode_columns(self.rows_data, layouts).map_err(ChunkDecodeError::ColumnarCodec)?;
        Ok(RawColumnarChunk { remainder, columns })
    }
}

/// Decodes one raw tipb [`SelectResponse`] protobuf envelope.
///
/// The returned message owns its protobuf buffers. Chunk payload bytes remain
/// opaque until the caller passes a borrowed chunk to [`decode_chunk`].
pub fn decode_select_response(bytes: &[u8]) -> Result<SelectResponse, prost::DecodeError> {
    SelectResponse::decode(bytes)
}

/// Decodes and validates the main-output chunks from an already-decoded
/// response.
///
/// An omitted response `encode_type` has the proto enum's zero value,
/// `TypeDefault`, matching Go's `GetEncodeType`. Intermediate output channel
/// encodings are intentionally left to their channel owner.
pub fn decode_response_chunks<'a>(
    response: &'a SelectResponse,
) -> Result<Vec<RawChunk<'a>>, ChunkDecodeError> {
    let raw_encode_type = response
        .encode_type
        .unwrap_or(EncodeType::TypeDefault as i32);
    let encode_type = EncodeType::try_from(raw_encode_type)
        .map_err(|_| ChunkDecodeError::InvalidEncodeType(raw_encode_type))?;
    response
        .chunks
        .iter()
        .map(|chunk| decode_chunk(chunk, encode_type))
        .collect()
}

/// Decodes the row metadata and byte ranges of one tipb [`Chunk`].
///
/// The metadata path is useful for CHBlock-style responses, which carry one
/// length per row. Default and columnar chunks can still be inspected as an
/// opaque byte payload when their `rows_meta` list is empty. Any supplied
/// metadata is validated regardless of encoding so malformed lengths never
/// leak into a future typed decoder.
pub fn decode_chunk<'a>(
    chunk: &'a Chunk,
    encode_type: EncodeType,
) -> Result<RawChunk<'a>, ChunkDecodeError> {
    let rows_data = chunk.rows_data.as_deref().unwrap_or_default();
    let rows = split_rows(rows_data, &chunk.rows_meta)?;
    Ok(RawChunk {
        encode_type,
        rows_data,
        rows,
    })
}

fn split_rows<'a>(
    rows_data: &'a [u8],
    rows_meta: &[RowMeta],
) -> Result<Vec<RawChunkRow<'a>>, ChunkDecodeError> {
    // Default and columnar chunks carry their own codec framing in
    // `rows_data` and legitimately omit per-row metadata. Preserve that
    // payload as opaque rather than treating the absence of RowMeta as a
    // claim that zero bytes were sent.
    if rows_meta.is_empty() {
        return Ok(Vec::new());
    }
    let mut offset = 0usize;
    let mut rows = Vec::with_capacity(rows_meta.len());
    for (row_index, meta) in rows_meta.iter().enumerate() {
        let raw_length = meta
            .length
            .ok_or(ChunkDecodeError::MissingRowLength { row_index })?;
        let length =
            usize::try_from(raw_length).map_err(|_| ChunkDecodeError::NegativeRowLength {
                row_index,
                length: raw_length,
            })?;
        let remaining = rows_data.len().saturating_sub(offset);
        if length > remaining {
            return Err(ChunkDecodeError::RowLengthExceedsData {
                row_index,
                length,
                remaining,
            });
        }
        let end = offset + length;
        rows.push(RawChunkRow {
            handle: meta.handle,
            data: &rows_data[offset..end],
        });
        offset = end;
    }
    if offset != rows_data.len() {
        return Err(ChunkDecodeError::RowDataLengthMismatch {
            declared: offset,
            actual: rows_data.len(),
        });
    }
    Ok(rows)
}
