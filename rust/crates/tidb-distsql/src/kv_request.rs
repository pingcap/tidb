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

//! Dependency-closed `kv.Request` metadata from `pkg/kv/kv.go`.
//!
//! [`KvRequestBuilder`] stops at the same pre-transport boundary as Go's
//! `distsql.RequestBuilder.Build`: it normalizes defaults, initializes the
//! key-range envelope, applies closest-read labels, and projects DAG limit
//! metadata. It deliberately does not marshal protobuf data, route regions,
//! acquire a TiKV client, or issue RPCs.

use std::ops::{Deref, DerefMut};

use crate::{DistSqlContext, ReadRequestMetadata, TiFlashReplicaRead};
pub use tidb_txnkv::{
    KeyRange as RequestKeyRange, PartitionIdAndRanges, PartitionedKeyRanges as RequestKeyRanges,
};

/// Go's `kv.GlobalReplicaScope` / client-go `oracle.GlobalTxnScope` value.
pub const GLOBAL_REPLICA_SCOPE: &str = "global";
/// Go placement's data-center label key used for closest reads.
pub const DC_LABEL_KEY: &str = "zone";

/// DistSQL-only metadata attached to the canonical `pkg/kv.Request`.
///
/// All KV request fields live in [`tidb_txnkv::Request`]. This wrapper carries
/// only TiFlash's client-send selection policy, which is not a field of Go
/// `kv.Request`.
#[derive(Clone)]
pub struct KvRequestMetadata {
    request: tidb_txnkv::Request,
    /// TiFlash node-selection policy projected into client-send metadata.
    pub tiflash_replica_read: TiFlashReplicaRead,
}

impl Default for KvRequestMetadata {
    fn default() -> Self {
        let mut request = tidb_txnkv::Request::default();
        let paging = crate::PagingConfig::source_defaults();
        request.paging = tidb_txnkv::Paging {
            enabled: paging.enabled,
            min_size: paging.min_size,
            max_size: paging.max_size,
            size_bytes: paging.size_bytes,
        };
        Self {
            request,
            tiflash_replica_read: TiFlashReplicaRead::default(),
        }
    }
}

impl KvRequestMetadata {
    /// Wraps one canonical KV request with default DistSQL-only metadata.
    #[must_use]
    pub fn from_request(request: tidb_txnkv::Request) -> Self {
        Self {
            request,
            tiflash_replica_read: TiFlashReplicaRead::default(),
        }
    }

    /// Consumes the wrapper and returns the canonical KV request.
    #[must_use]
    pub fn into_request(self) -> tidb_txnkv::Request {
        self.request
    }

    /// Projects the source context fields into a new request metadata value.
    #[must_use]
    pub fn from_context(context: &DistSqlContext) -> Self {
        let mut request = Self::default();
        request.apply_session_metadata(ReadRequestMetadata::from_context(context));
        request.connection_id = context.request.session.connection_id;
        request.connection_alias = context.request.session.alias.clone();
        request
    }

    pub(crate) fn apply_session_metadata(&mut self, session: ReadRequestMetadata) {
        self.concurrency = session.concurrency as isize;
        self.isolation_level = session.isolation_level;
        self.priority = session.priority;
        self.not_fill_cache = session.not_fill_cache;
        self.task_id = session.task_id;
        self.replica_read = session.replica_read;
        self.tiflash_replica_read = session.tiflash_replica_read;
        self.paging = tidb_txnkv::Paging {
            enabled: session.paging.enabled,
            min_size: session.paging.min_size,
            max_size: session.paging.max_size,
            size_bytes: session.paging.size_bytes,
        };
        self.request_source = session.request_source;
        self.store_batch_size = session.store_batch_size as isize;
        self.resource_group_name = session.resource_group_name;
        self.store_busy_threshold_ns =
            (session.store_busy_threshold_ms as i64).wrapping_mul(1_000_000);
        self.tikv_client_read_timeout_ms = session.tikv_client_read_timeout_ms;
        self.max_execution_time_ms = session.max_execution_time_ms;
        self.max_keys_read = session.max_keys_read;
        self.max_keys_read_counter = session.max_keys_read_counter;
    }
}

impl Deref for KvRequestMetadata {
    type Target = tidb_txnkv::Request;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

impl DerefMut for KvRequestMetadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.request
    }
}

impl std::fmt::Debug for KvRequestMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvRequestMetadata")
            .field("request_type", &self.request_type)
            .field("start_ts", &self.start_ts)
            .field(
                "range_count",
                &self
                    .key_ranges
                    .as_ref()
                    .map(RequestKeyRanges::total_range_count),
            )
            .field("store_type", &self.store_type)
            .field("connection_id", &self.connection_id)
            .finish_non_exhaustive()
    }
}
