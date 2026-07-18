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

use crate::{DistSqlContext, ReadRequestMetadata};

/// Go's `kv.GlobalReplicaScope` / client-go `oracle.GlobalTxnScope` value.
pub const GLOBAL_REPLICA_SCOPE: &str = "global";
/// Go placement's data-center label key used for closest reads.
pub const DC_LABEL_KEY: &str = "zone";

/// A raw key range carried by the request envelope.
///
/// The bytes are kept as owned metadata and are converted to the canonical
/// `tidb-txnkv::KeyRanges` container by the region task constructor.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestKeyRange {
    /// Inclusive start boundary.
    pub start_key: Vec<u8>,
    /// Exclusive end boundary.
    pub end_key: Vec<u8>,
}

/// Partition-aware key ranges corresponding to Go's `kv.KeyRanges`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestKeyRanges {
    /// Ranges grouped by physical partition.
    pub partitions: Vec<Vec<RequestKeyRange>>,
    /// Estimated source row counts aligned with each partition's ranges.
    pub row_count_hints: Vec<Vec<usize>>,
    /// Whether the outer grouping represents a partitioned table.
    pub partitioned: bool,
}

/// Partition-scoped ranges used by TiFlash partition-table scans.
///
/// This mirrors `kv.PartitionIDAndRanges` (`pkg/kv/kv.go:579-581,678-682`)
/// without interpreting key bytes or selecting a storage client.  The
/// request owner keeps the partition identifier beside the exact ordered
/// ranges so a later TiFlash/RPC adapter cannot accidentally flatten them
/// into the ordinary `KeyRanges` envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionIdAndRanges {
    /// Physical partition/table identifier.
    pub id: i64,
    /// Ordered ranges belonging to this partition.
    pub key_ranges: Vec<RequestKeyRange>,
}

impl RequestKeyRanges {
    /// Creates the Go `NewNonPartitionedKeyRanges` shape.
    #[must_use]
    pub fn new_non_partitioned(ranges: Vec<RequestKeyRange>) -> Self {
        Self {
            partitions: vec![ranges],
            row_count_hints: Vec::new(),
            partitioned: false,
        }
    }

    /// Creates Go `NewNonParitionedKeyRangesWithHint`'s exact shape.
    #[must_use]
    pub fn new_non_partitioned_with_hints(ranges: Vec<RequestKeyRange>, hints: Vec<usize>) -> Self {
        Self {
            partitions: vec![ranges],
            row_count_hints: vec![hints],
            partitioned: false,
        }
    }

    /// Creates partitioned ranges without interpreting key bytes.
    #[must_use]
    pub fn new_partitioned(partitions: Vec<Vec<RequestKeyRange>>) -> Self {
        Self {
            partitions,
            row_count_hints: Vec::new(),
            partitioned: true,
        }
    }

    /// Returns the source partition count.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Returns whether this envelope is non-partitioned.
    #[must_use]
    pub const fn is_non_partitioned(&self) -> bool {
        !self.partitioned
    }
}

/// Store label metadata produced for a closest-read request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoreLabel {
    /// Label key.
    pub key: String,
    /// Label value.
    pub value: String,
}

/// Request type values used by the source `kv.Request.Tp` field.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(i64)]
pub enum RequestType {
    /// No request type selected.
    #[default]
    Unknown = 0,
    /// DAG coprocessor request (`kv.ReqTypeDAG`).
    Dag = 103,
    /// Analyze request (`kv.ReqTypeAnalyze`).
    Analyze = 104,
    /// Checksum request (`kv.ReqTypeChecksum`).
    Checksum = 105,
}

/// Store engine values copied from Go's `kv.StoreType`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum StoreType {
    /// TiKV storage.
    #[default]
    TiKv = 0,
    /// TiFlash storage.
    TiFlash = 1,
    /// TiDB memory-backed storage.
    TiDb = 2,
    /// An unspecified store engine.
    Unspecified = 255,
}

/// Explicit marker for fields that require a future transport owner.
///
/// This marker is preferable to fake region handles or RPC clients. Even when
/// the caller supplies opaque `Data`, protobuf serialization ownership,
/// coprocessor rate limiters, resource group taggers, runaway checkers, and
/// request adjusters remain unbound.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RequestTransportState {
    /// No protobuf or storage transport is attached.
    #[default]
    Unbound,
}

/// The dependency-closed request metadata produced by [`KvRequestBuilder`].
#[derive(Clone, Debug, Default)]
pub struct KvRequestMetadata {
    /// Source request type (`Tp`).
    pub request_type: RequestType,
    /// Source transaction start timestamp (`StartTs`).
    pub start_ts: u64,
    /// Optional already-serialized DAG/analyze/checksum payload. The bytes are
    /// absent by default and are never encoded or interpreted by this crate.
    pub data: Option<Vec<u8>>,
    /// Initialized key-range envelope.
    pub key_ranges: Option<RequestKeyRanges>,
    /// TiFlash partition-scoped ranges, kept separate from `key_ranges`.
    pub partition_id_and_ranges: Vec<PartitionIdAndRanges>,
    /// Session-projected fields from `SetFromSessionVars`.
    pub session: ReadRequestMetadata,
    /// Whether response order must be preserved.
    pub keep_order: bool,
    /// Whether ranges are scanned in descending order.
    pub desc: bool,
    /// Whether this request may be cached.
    pub cacheable: bool,
    /// Schema version for schema-aware storage.
    pub schema_version: i64,
    /// Whether batch coprocessor is requested.
    pub batch_cop: bool,
    /// TiDB server identity; zero means all instances.
    pub tidb_server_id: u64,
    /// Transaction scope.
    pub txn_scope: String,
    /// Replica scope, normalized to [`GLOBAL_REPLICA_SCOPE`] by `build`.
    pub read_replica_scope: String,
    /// Whether this is a staleness read.
    pub is_staleness: bool,
    /// Closest-read store labels selected during `build`.
    pub match_store_labels: Vec<StoreLabel>,
    /// Store engine.
    pub store_type: StoreType,
    /// Session connection identifier copied into a coprocessor request.
    pub connection_id: u64,
    /// Session connection alias copied into a coprocessor request.
    pub connection_alias: String,
    /// Terminal scan limit projected from a DAG envelope.
    pub limit_size: u64,
    /// Optional source-compatible resource-group tag builder consumed at the
    /// pre-transport boundary using the first request key.
    pub resource_group_tagger: Option<tidb_txnkv::ResourceGroupTagBuilder>,
    /// Explicit transport boundary marker.
    pub transport: RequestTransportState,
}

impl KvRequestMetadata {
    /// Projects the source context fields into a new request metadata value.
    #[must_use]
    pub fn from_context(context: &DistSqlContext) -> Self {
        Self {
            session: ReadRequestMetadata::from_context(context),
            connection_id: context.request.session.connection_id,
            connection_alias: context.request.session.alias.clone(),
            ..Self::default()
        }
    }
}
