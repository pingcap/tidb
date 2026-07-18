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

//! Raw `tipb::StreamResponse` ownership for streamed coprocessor responses.
//!
//! TiDB's stream handler serializes one `tipb::Chunk` into
//! [`StreamResponse::data`], then serializes the `StreamResponse` itself.  The
//! response metadata (`error`, warnings, output counts, warning count, and
//! NDVs) is a separate protobuf envelope.  This leaf decodes that envelope
//! and preserves every field without interpreting the nested chunk bytes.
//!
//! The Go source does not use `StreamResponse` for intermediate output
//! channels; those are represented by `SelectResponse::intermediate_outputs`
//! and belong to the response-channel/SelectResult owner.  Likewise, default,
//! chunk, and native CHBlock row codecs, MPP packet semantics, and Datum
//! conversion remain explicit downstream boundaries instead of being guessed
//! here.

use prost::Message;
use tidb_proto::{Error, StreamResponse};

/// A decoded streamed coprocessor response whose payload remains raw bytes.
///
/// The optional fields intentionally retain protobuf presence.  In
/// particular, `data: Some(Vec::new())` is distinct from `data: None`, which
/// matches the proto2 wire contract and prevents a caller from inventing a
/// missing payload.  Repeated metadata is kept in wire order and may contain
/// any signed values accepted by the source protobuf; semantic validation is
/// the responsibility of the owner that eventually consumes it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RawStreamResponse {
    /// A store-side error carried by the response envelope.
    pub error: Option<Error>,
    /// The serialized stream payload, normally one `tipb::Chunk`.
    pub data: Option<Vec<u8>>,
    /// Warning messages in the order sent by the store.
    pub warnings: Vec<Error>,
    /// Output row counts for each executor, in source order.
    pub output_counts: Vec<i64>,
    /// Optional warning count supplied by the store.
    pub warning_count: Option<i64>,
    /// Distinct-value estimates in the order supplied by the store.
    pub ndvs: Vec<i64>,
}

impl RawStreamResponse {
    /// Returns the raw payload bytes while preserving absent/present-empty
    /// protobuf presence.
    #[must_use]
    pub fn data(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    /// Reconstructs the generated protobuf message without introducing any
    /// semantic defaults or decoding the nested stream payload.
    #[must_use]
    pub fn into_proto(self) -> StreamResponse {
        self.into()
    }
}

impl From<StreamResponse> for RawStreamResponse {
    fn from(response: StreamResponse) -> Self {
        Self {
            error: response.error,
            data: response.data,
            warnings: response.warnings,
            output_counts: response.output_counts,
            warning_count: response.warning_count,
            ndvs: response.ndvs,
        }
    }
}

impl From<RawStreamResponse> for StreamResponse {
    fn from(response: RawStreamResponse) -> Self {
        Self {
            error: response.error,
            data: response.data,
            warnings: response.warnings,
            output_counts: response.output_counts,
            warning_count: response.warning_count,
            ndvs: response.ndvs,
        }
    }
}

/// Decodes one raw `tipb::StreamResponse` protobuf envelope.
///
/// Prost enforces protobuf wire validity (including truncated fields and
/// malformed varints).  No checks are added for row counts, warning counts,
/// NDV values, MPP metadata, or nested chunk codecs because those are source
/// semantics owned by later DistSQL/storage layers.
pub fn decode_stream_response(bytes: &[u8]) -> Result<RawStreamResponse, prost::DecodeError> {
    StreamResponse::decode(bytes).map(Into::into)
}
