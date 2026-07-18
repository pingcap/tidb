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

//! The bounded `TypeCHBlock` boundary.
//!
//! TiDB's local MPP coordinator marks non-root TiFlash dispatch requests as
//! `tipb.EncodeType_TypeCHBlock`.  The Go `distsql.selectResult` consumer does
//! not decode this payload; it rejects the encoding once a client attempts to
//! materialize rows.  This leaf keeps that contract explicit: protobuf and
//! `RowMeta` byte ranges are validated by [`decode_ch_block`], while the
//! native ClickHouse block bytes remain borrowed and opaque.  A future
//! TiFlash/CHBlock owner can consume [`RawChBlockChunk::payload`] without
//! silently treating it as TiDB's default-row or TypeChunk codec.

use tidb_proto::{Chunk, EncodeType};

use crate::chunk_decode::{decode_chunk, ChunkDecodeError, RawChunk, RawChunkRow};

/// A validated `TypeCHBlock` chunk whose native payload remains opaque.
///
/// The wrapper owns no bytes: `payload` and each row slice borrow the original
/// protobuf `Chunk`.  This prevents an accidental copy or a guessed schema
/// while retaining the exact row handles and lengths supplied by the wire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawChBlockChunk<'a> {
    raw: RawChunk<'a>,
}

impl<'a> RawChBlockChunk<'a> {
    /// Wraps an already validated raw chunk only when it is `TypeCHBlock`.
    pub fn from_raw(raw: RawChunk<'a>) -> Result<Self, ChunkDecodeError> {
        if raw.encode_type != EncodeType::TypeChBlock {
            return Err(ChunkDecodeError::UnsupportedTypedRowDecoding {
                encode_type: raw.encode_type,
            });
        }
        Ok(Self { raw })
    }

    /// Returns the complete native CHBlock payload without decoding it.
    #[must_use]
    pub fn payload(&self) -> &'a [u8] {
        self.raw.rows_data
    }

    /// Returns validated row slices in the source `RowMeta` order.
    #[must_use]
    pub fn rows(&self) -> &[RawChunkRow<'a>] {
        &self.raw.rows
    }

    /// Returns the number of metadata-described rows.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.raw.rows.len()
    }

    /// Returns one metadata-described row, if present.
    #[must_use]
    pub fn row(&self, index: usize) -> Option<&RawChunkRow<'a>> {
        self.raw.rows.get(index)
    }

    /// Returns the raw wrapper after the caller has finished CHBlock handling.
    #[must_use]
    pub fn into_raw(self) -> RawChunk<'a> {
        self.raw
    }

    /// Keeps the source's unsupported typed materialization boundary explicit.
    ///
    /// No native CHBlock layout is defined by TiDB's `pkg/distsql` Go
    /// consumer, so this method cannot safely construct `Datum` values yet.
    pub fn decode_native(&self) -> Result<(), ChunkDecodeError> {
        Err(ChunkDecodeError::UnsupportedTypedRowDecoding {
            encode_type: EncodeType::TypeChBlock,
        })
    }
}

/// Validates one tipb `Chunk` as a raw native CHBlock envelope.
///
/// `rows_meta` is checked for missing, negative, overlong, and trailing
/// lengths by the shared raw chunk decoder.  Empty metadata is valid and
/// leaves the complete payload available through [`RawChBlockChunk::payload`].
pub fn decode_ch_block<'a>(chunk: &'a Chunk) -> Result<RawChBlockChunk<'a>, ChunkDecodeError> {
    RawChBlockChunk::from_raw(decode_chunk(chunk, EncodeType::TypeChBlock)?)
}
