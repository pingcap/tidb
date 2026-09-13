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

use prost::encoding::{self, WireType};
use tidb_proto::KvrpcContext;

use crate::{KvRequestMetadata, RequestKeyRange};

/// A dependency-closed coprocessor request envelope.
///
/// The fields map directly to the source `coprocessor.Request` wire contract.
/// `ranges` must be the ranges for the one task being serialized; this type
/// does not flatten partitioned `kv.KeyRanges` or perform region splitting.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CoprocessorRequestEnvelope {
    /// Typed `kvrpcpb.Context` (field 1).
    pub context: Option<KvrpcContext>,
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
    /// Whether the caller accepts merged child-task data (field 18).
    pub allow_batch_task_data_merge: bool,
    /// Whether the store should execute batched tasks serially (field 19).
    pub execute_batch_tasks_serially: bool,
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
            tp: metadata.request_type.raw(),
            data: metadata.data.clone().unwrap_or_default(),
            ranges,
            start_ts: metadata.start_ts,
            schema_ver: metadata.schema_version,
            connection_id: metadata.connection_id,
            connection_alias: metadata.connection_alias.clone(),
            max_keys_read: metadata.max_keys_read,
            paging_size_bytes: metadata.paging.size_bytes,
            allow_batch_task_data_merge: metadata.allow_batch_task_data_merge,
            execute_batch_tasks_serially: metadata.execute_batch_tasks_serially,
            ..Self::default()
        }
    }

    /// Sets the typed `kvrpcpb.Context`.
    #[must_use]
    pub fn with_context(mut self, context: KvrpcContext) -> Self {
        self.context = Some(context);
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
    ///
    /// The bytes are those `prost` derives for `coprocessor.Request`
    /// (fields in tag order, default scalars omitted), written straight from
    /// the borrowed payload and ranges: Go marshals the request once with
    /// `Data` shared and `ToPBRanges` aliasing the task's ranges, so a page
    /// costs one copy of its bytes, not one `Vec` per range boundary.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_raw(&mut buf);
        buf
    }

    fn encode_raw(&self, buf: &mut Vec<u8>) {
        if let Some(context) = &self.context {
            encoding::message::encode(1, context, buf);
        }
        if self.tp != 0 {
            encoding::int64::encode(2, &self.tp, buf);
        }
        if !self.data.is_empty() {
            encode_bytes_field(3, &self.data, buf);
        }
        for range in &self.ranges {
            encoding::encode_key(4, WireType::LengthDelimited, buf);
            encoding::encode_varint(key_range_encoded_len(range) as u64, buf);
            let (start, end) = (range.start_key.as_slice(), range.end_key.as_slice());
            if !start.is_empty() {
                encode_bytes_field(1, start, buf);
            }
            if !end.is_empty() {
                encode_bytes_field(2, end, buf);
            }
        }
        if self.is_cache_enabled {
            encoding::bool::encode(5, &self.is_cache_enabled, buf);
        }
        if self.cache_if_match_version != 0 {
            encoding::uint64::encode(6, &self.cache_if_match_version, buf);
        }
        if self.start_ts != 0 {
            encoding::uint64::encode(7, &self.start_ts, buf);
        }
        if self.schema_ver != 0 {
            encoding::int64::encode(8, &self.schema_ver, buf);
        }
        if self.is_trace_enabled {
            encoding::bool::encode(9, &self.is_trace_enabled, buf);
        }
        if self.paging_size != 0 {
            encoding::uint64::encode(10, &self.paging_size, buf);
        }
        if self.connection_id != 0 {
            encoding::uint64::encode(12, &self.connection_id, buf);
        }
        if !self.connection_alias.is_empty() {
            encoding::string::encode(13, &self.connection_alias, buf);
        }
        if self.max_keys_read != 0 {
            encoding::uint64::encode(16, &self.max_keys_read, buf);
        }
        if self.paging_size_bytes != 0 {
            encoding::uint64::encode(17, &self.paging_size_bytes, buf);
        }
        if self.allow_batch_task_data_merge {
            encoding::bool::encode(18, &self.allow_batch_task_data_merge, buf);
        }
        if self.execute_batch_tasks_serially {
            encoding::bool::encode(19, &self.execute_batch_tasks_serially, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if let Some(context) = &self.context {
            len += encoding::message::encoded_len(1, context);
        }
        if self.tp != 0 {
            len += encoding::int64::encoded_len(2, &self.tp);
        }
        if !self.data.is_empty() {
            len += bytes_field_len(3, self.data.len());
        }
        for range in &self.ranges {
            let inner = key_range_encoded_len(range);
            len += encoding::key_len(4) + encoding::encoded_len_varint(inner as u64) + inner;
        }
        if self.is_cache_enabled {
            len += encoding::bool::encoded_len(5, &self.is_cache_enabled);
        }
        if self.cache_if_match_version != 0 {
            len += encoding::uint64::encoded_len(6, &self.cache_if_match_version);
        }
        if self.start_ts != 0 {
            len += encoding::uint64::encoded_len(7, &self.start_ts);
        }
        if self.schema_ver != 0 {
            len += encoding::int64::encoded_len(8, &self.schema_ver);
        }
        if self.is_trace_enabled {
            len += encoding::bool::encoded_len(9, &self.is_trace_enabled);
        }
        if self.paging_size != 0 {
            len += encoding::uint64::encoded_len(10, &self.paging_size);
        }
        if self.connection_id != 0 {
            len += encoding::uint64::encoded_len(12, &self.connection_id);
        }
        if !self.connection_alias.is_empty() {
            len += encoding::string::encoded_len(13, &self.connection_alias);
        }
        if self.max_keys_read != 0 {
            len += encoding::uint64::encoded_len(16, &self.max_keys_read);
        }
        if self.paging_size_bytes != 0 {
            len += encoding::uint64::encoded_len(17, &self.paging_size_bytes);
        }
        if self.allow_batch_task_data_merge {
            len += encoding::bool::encoded_len(18, &self.allow_batch_task_data_merge);
        }
        if self.execute_batch_tasks_serially {
            len += encoding::bool::encoded_len(19, &self.execute_batch_tasks_serially);
        }
        len
    }
}

/// The body length of one `coprocessor.KeyRange` (`start` 1, `end` 2, an
/// empty boundary omitted as `prost` omits a default `bytes` field).
fn key_range_encoded_len(range: &RequestKeyRange) -> usize {
    let (start, end) = (range.start_key.as_slice(), range.end_key.as_slice());
    let mut len = 0;
    if !start.is_empty() {
        len += bytes_field_len(1, start.len());
    }
    if !end.is_empty() {
        len += bytes_field_len(2, end.len());
    }
    len
}

fn bytes_field_len(tag: u32, len: usize) -> usize {
    encoding::key_len(tag) + encoding::encoded_len_varint(len as u64) + len
}

fn encode_bytes_field(tag: u32, bytes: &[u8], buf: &mut Vec<u8>) {
    encoding::encode_key(tag, WireType::LengthDelimited, buf);
    encoding::encode_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}
