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

//! Source-shaped request and DistSQL context fields.

use crate::paging::{MIN_ALLOWED_MAX_PAGING_SIZE, MIN_PAGING_SIZE};
use crate::{ExecutionState, TiFlashReplicaRead, Warning, WarningCollector};
use tidb_txnkv::ReplicaReadType;

/// TiDB's default `tidb_distsql_scan_concurrency` used by the Go test helper.
pub const DEFAULT_DIST_SQL_CONCURRENCY: u64 = 15;
/// Statement priority values copied from `pkg/parser/mysql.PriorityEnum`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum Priority {
    /// No explicit priority.
    #[default]
    NoPriority = 0,
    /// `LOW_PRIORITY`.
    Low = 1,
    /// `HIGH_PRIORITY`.
    High = 2,
    /// `DELAYED`.
    Delayed = 3,
}

/// Paging controls copied from `kv.Request.Paging`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PagingConfig {
    /// Whether row paging is enabled.
    pub enabled: bool,
    /// Minimum row-page size.
    pub min_size: u64,
    /// Maximum row-page size.
    pub max_size: u64,
    /// Byte-page size.
    pub size_bytes: u64,
}

impl Default for PagingConfig {
    fn default() -> Self {
        Self::source_defaults()
    }
}

impl PagingConfig {
    /// Returns the defaults from `pkg/distsql/context_test.go` and
    /// `pkg/util/paging/paging.go`.
    #[must_use]
    pub const fn source_defaults() -> Self {
        Self {
            enabled: false,
            min_size: MIN_PAGING_SIZE,
            max_size: MIN_ALLOWED_MAX_PAGING_SIZE,
            size_bytes: 0,
        }
    }
}

/// Session identity needed by a read-only request builder.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionContext {
    /// TiDB connection identifier.
    pub connection_id: u64,
    /// Optional session alias used by request source tagging.
    pub alias: String,
}

/// Request-level subset of Go's `distsql.DistSQLContext`.
///
/// Strings and scalar settings are owned by this value, while the warning
/// collector is intentionally shared when the enclosing context detaches.
/// Fields that require a Go session, TiKV client, protobuf request, or runtime
/// statistics collector are not represented until their real consumer exists.
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Shared statement warning handler.
    pub warning_handler: WarningCollector,
    /// Whether this request is running in restricted SQL mode.
    pub in_restricted_sql: bool,
    /// Original SQL text copied into request metadata.
    pub original_sql: String,
    /// Whether DAG results may use the native chunk-memory RPC encoding.
    ///
    /// Host layout compatibility remains a separate runtime gate; enabling
    /// this setting alone never selects chunk encoding.
    pub enable_chunk_rpc: bool,
    /// Session identity used by the request builder.
    pub session: SessionContext,
    /// Replica routing preference.
    pub replica_read: ReplicaReadType,
    /// TiFlash node-selection policy projected into client-send metadata.
    pub tiflash_replica_read: TiFlashReplicaRead,
    /// Whether weak consistency is enabled.
    pub weak_consistency: bool,
    /// Whether RC timestamp checking is enabled.
    pub rc_check_ts: bool,
    /// Whether TiKV should avoid filling its block cache.
    pub not_fill_cache: bool,
    /// Statement task identifier.
    pub task_id: u64,
    /// Session `tidb_distsql_scan_concurrency` value.
    pub dist_sql_concurrency: u64,
    /// MySQL statement priority.
    pub priority: Priority,
    /// Paging controls.
    pub paging: PagingConfig,
    /// Internal request source type.
    pub request_source_type: String,
    /// Explicit request source type, if supplied by the client.
    pub explicit_request_source_type: String,
    /// Batch size for store requests.
    pub store_batch_size: u64,
    /// Resource group name.
    pub resource_group_name: String,
    /// Load-based replica-read threshold in milliseconds.
    pub load_based_replica_read_threshold_ms: u64,
    /// TiKV client read timeout in milliseconds.
    pub tikv_client_read_timeout_ms: u64,
    /// Maximum execution time in milliseconds.
    pub max_execution_time_ms: u64,
    /// Statement-wide maximum keys-read budget.
    pub max_keys_read: u64,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self {
            warning_handler: WarningCollector::default(),
            in_restricted_sql: false,
            original_sql: String::new(),
            enable_chunk_rpc: false,
            session: SessionContext::default(),
            replica_read: ReplicaReadType::default(),
            tiflash_replica_read: TiFlashReplicaRead::default(),
            weak_consistency: false,
            rc_check_ts: false,
            not_fill_cache: false,
            task_id: 0,
            dist_sql_concurrency: DEFAULT_DIST_SQL_CONCURRENCY,
            priority: Priority::default(),
            paging: PagingConfig::source_defaults(),
            request_source_type: String::new(),
            explicit_request_source_type: String::new(),
            store_batch_size: 0,
            resource_group_name: "default".to_owned(),
            load_based_replica_read_threshold_ms: 0,
            tikv_client_read_timeout_ms: 0,
            max_execution_time_ms: 0,
            max_keys_read: 0,
        }
    }
}

/// DistSQL context containing the request fields and detached execution state.
#[derive(Debug, Default)]
pub struct DistSqlContext {
    /// Read-only request fields consumed by a future request builder.
    pub request: RequestContext,
    /// Shared/deep-copied state needed while executing the request.
    pub execution: ExecutionState,
}

impl DistSqlContext {
    /// Creates a context with default request and execution settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a regular warning to the shared warning handler.
    pub fn append_warning(&self, message: impl Into<String>) {
        self.request.warning_handler.append_warning(message);
    }

    /// Appends an informational note to the shared warning handler.
    pub fn append_note(&self, message: impl Into<String>) {
        self.request.warning_handler.append_note(message);
    }

    /// Returns a snapshot of warnings collected by this context.
    #[must_use]
    pub fn warnings(&self) -> Vec<Warning> {
        self.request.warning_handler.warnings()
    }

    /// Detaches this context from the session while preserving Go's identity
    /// and ownership rules.
    ///
    /// The warning handler, kill handle, cancellation handle, and KV killer
    /// handle remain shared. Owned strings, CPU samples, and KV scalar fields
    /// are copied. A present max-keys accumulator is fresh and zeroed, exactly
    /// like Go's `new(atomic.Uint64)` in `DistSQLContext.Detach`.
    #[must_use]
    pub fn detach(&self) -> Self {
        Self {
            request: self.request.clone(),
            execution: self.execution.detach(),
        }
    }
}
