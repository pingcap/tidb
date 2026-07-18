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

//! Source-shaped serialization of the pre-region TiKV coprocessor request.
//!
//! Go creates `coprocessor.Request` in
//! `pkg/store/copr/coprocessor.go:1745-1757` from the already-built
//! `kv.Request`. This leaf owns only that protobuf projection: it preserves
//! the raw DAG/analyze/checksum payload and ordered key-range bytes, while
//! leaving Context, batch tasks, region routing, and RPC ownership explicit.

use prost::Message;
use tidb_proto::{CoprocessorKeyRange, CoprocessorRequest};

use crate::{KvRequestMetadata, RequestKeyRange};

/// A dependency-closed coprocessor request envelope.
///
/// The fields map directly to the source `coprocessor.Request` wire contract.
/// `ranges` must be the ranges for the one task being serialized; this type
/// does not flatten partitioned `kv.KeyRanges` or perform region splitting.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CoprocessorRequestEnvelope {
    /// Opaque serialized `kvrpcpb.Context` bytes (field 1).
    pub context: Option<Vec<u8>>,
    /// Source `kv.Request.Tp` (field 2).
    pub tp: i64,
    /// Exact source `kv.Request.Data` bytes (field 3).
    pub data: Vec<u8>,
    /// Ordered half-open ranges for this coprocessor task (field 4).
    pub ranges: Vec<RequestKeyRange>,
    /// Optional cache flag supplied by a future cache owner (field 5).
    pub is_cache_enabled: bool,
    /// Optional cache-version predicate supplied by a future cache owner (field 6).
    pub cache_if_match_version: u64,
    /// Source transaction start timestamp (field 7).
    pub start_ts: u64,
    /// Source schema version (field 8).
    pub schema_ver: i64,
    /// Trace flag; transport tracing remains unbound (field 9).
    pub is_trace_enabled: bool,
    /// Task-local row paging size (field 10).
    pub paging_size: u64,
    /// Source connection identifier (field 12).
    pub connection_id: u64,
    /// Source connection alias (field 13).
    pub connection_alias: String,
    /// Task-local max-keys budget (field 16).
    pub max_keys_read: u64,
    /// Request byte-page budget (field 17).
    pub paging_size_bytes: u64,
}

impl CoprocessorRequestEnvelope {
    /// Projects request metadata and caller-owned task ranges.
    ///
    /// The caller supplies one task's ranges because Go performs region/task
    /// splitting immediately before constructing `coprocessor.Request`.
    /// Keeping this argument explicit avoids silently flattening partition
    /// boundaries or pretending that a region router already exists.
    #[must_use]
    pub fn from_metadata(metadata: &KvRequestMetadata, ranges: Vec<RequestKeyRange>) -> Self {
        Self {
            tp: metadata.request_type as i64,
            data: metadata.data.clone().unwrap_or_default(),
            ranges,
            start_ts: metadata.start_ts,
            schema_ver: metadata.schema_version,
            connection_id: metadata.connection_id,
            connection_alias: metadata.connection_alias.clone(),
            max_keys_read: metadata.session.max_keys_read,
            paging_size_bytes: metadata.session.paging.size_bytes,
            ..Self::default()
        }
    }

    /// Sets opaque serialized `kvrpcpb.Context` bytes without decoding them.
    #[must_use]
    pub fn with_context(mut self, context: impl Into<Vec<u8>>) -> Self {
        self.context = Some(context.into());
        self
    }

    /// Sets the task-local row paging size.
    #[must_use]
    pub const fn with_paging_size(mut self, paging_size: u64) -> Self {
        self.paging_size = paging_size;
        self
    }

    /// Sets the task-local max-keys remainder selected by a coprocessor worker.
    #[must_use]
    pub const fn with_max_keys_read(mut self, max_keys_read: u64) -> Self {
        self.max_keys_read = max_keys_read;
        self
    }

    /// Enables the cache predicate without owning cache lookup state.
    #[must_use]
    pub const fn with_cache_version(mut self, version: u64) -> Self {
        self.is_cache_enabled = true;
        self.cache_if_match_version = version;
        self
    }

    /// Serializes the exact protobuf field numbers owned by this projection.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        CoprocessorRequest {
            context: self.context.clone(),
            tp: self.tp,
            data: self.data.clone(),
            ranges: self
                .ranges
                .iter()
                .map(|range| CoprocessorKeyRange {
                    start: range.start_key.clone(),
                    end: range.end_key.clone(),
                })
                .collect(),
            is_cache_enabled: self.is_cache_enabled,
            cache_if_match_version: self.cache_if_match_version,
            start_ts: self.start_ts,
            schema_ver: self.schema_ver,
            is_trace_enabled: self.is_trace_enabled,
            paging_size: self.paging_size,
            connection_id: self.connection_id,
            connection_alias: self.connection_alias.clone(),
            max_keys_read: self.max_keys_read,
            paging_size_bytes: self.paging_size_bytes,
        }
        .encode_to_vec()
    }
}
