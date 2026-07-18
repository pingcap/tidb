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

//! Dependency-closed request metadata copied from `pkg/distsql/request_builder.go`.
//!
//! The builder stops before `kv.Request` serialization. It captures the
//! source-owned settings that a future builder can pass to a real protocol
//! leaf, without pretending to encode key ranges, DAG protobufs, or TiKV RPC.

use std::sync::{atomic::AtomicU64, Arc};

use crate::{
    DistSqlContext, PagingConfig, Priority, ReplicaReadType, RequestContext, TiFlashReplicaRead,
};

/// Transaction isolation values copied from `pkg/kv.IsoLevel`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum IsolationLevel {
    /// Snapshot isolation (`kv.SI`).
    #[default]
    Snapshot = 0,
    /// Read committed (`kv.RC`).
    ReadCommitted = 1,
    /// Read committed with timestamp checking (`kv.RCCheckTS`).
    RcCheckTs = 2,
}

/// KV priority values copied from `pkg/kv`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum KvPriority {
    /// Normal priority (`mysql.NoPriority` and `mysql.DelayedPriority`).
    #[default]
    Normal = 0,
    /// Low priority (`mysql.LowPriority`).
    Low = 1,
    /// High priority (`mysql.HighPriority`).
    High = 2,
}

/// Request-source metadata copied from `kv.RequestSource`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestSource {
    /// Whether the source is an internal/restricted SQL request.
    pub internal: bool,
    /// Internal request source type.
    pub source_type: String,
    /// Explicit client-supplied request source type.
    pub explicit_source_type: String,
}

/// The immutable, dependency-closed request metadata produced by the builder.
#[derive(Clone, Debug, Default)]
pub struct ReadRequestMetadata {
    /// Effective request concurrency after the source upper-bound clamp.
    pub concurrency: u64,
    /// Effective transaction isolation.
    pub isolation_level: IsolationLevel,
    /// Effective KV priority.
    pub priority: KvPriority,
    /// Source `NotFillCache` setting.
    pub not_fill_cache: bool,
    /// Source statement task identifier.
    pub task_id: u64,
    /// Effective replica routing preference.
    pub replica_read: ReplicaReadType,
    /// TiFlash node-selection policy copied into the client-send boundary.
    pub tiflash_replica_read: TiFlashReplicaRead,
    /// Paging controls copied without dropping byte size when disabled.
    pub paging: PagingConfig,
    /// Request source metadata.
    pub request_source: RequestSource,
    /// Store batch size.
    pub store_batch_size: u64,
    /// Resource group name.
    pub resource_group_name: String,
    /// Load-based replica-read threshold in milliseconds.
    pub store_busy_threshold_ms: u64,
    /// TiKV client read timeout in milliseconds.
    pub tikv_client_read_timeout_ms: u64,
    /// Maximum execution time in milliseconds.
    pub max_execution_time_ms: u64,
    /// Statement-wide maximum keys-read budget.
    pub max_keys_read: u64,
    /// Shared statement-wide accumulator, when enabled.
    pub max_keys_read_counter: Option<Arc<AtomicU64>>,
}

impl ReadRequestMetadata {
    /// Builds request metadata directly from a DistSQL context.
    #[must_use]
    pub fn from_context(context: &DistSqlContext) -> Self {
        ReadRequestBuilder::new().from_context(context).build()
    }

    /// Returns whether two requests share the same statement-wide counter.
    #[must_use]
    pub fn shares_max_keys_counter_with(&self, other: &Self) -> bool {
        match (&self.max_keys_read_counter, &other.max_keys_read_counter) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
    }
}

/// Small source-shaped request builder for the fields set by
/// `RequestBuilder.SetFromSessionVars`.
#[derive(Clone, Debug, Default)]
pub struct ReadRequestBuilder {
    request: ReadRequestMetadata,
}

impl ReadRequestBuilder {
    /// Creates an empty builder with Go's zero-value request semantics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets an explicit request concurrency before session-variable clamping.
    #[must_use]
    pub fn set_concurrency(mut self, concurrency: u64) -> Self {
        self.request.concurrency = concurrency;
        self
    }

    /// Applies the source session-variable projection to this builder.
    ///
    /// This is the direct dependency-closed portion of Go's
    /// `SetFromSessionVars`: weak consistency wins over RC-check, RC-check
    /// forces leader reads, priorities map to KV's three values, and paging
    /// byte size is retained even when row paging is disabled.
    #[must_use]
    pub fn from_context(mut self, context: &DistSqlContext) -> Self {
        self.apply_request_context(&context.request, &context.execution.max_keys_read_counter);
        self
    }

    /// Returns the immutable metadata snapshot.
    #[must_use]
    pub fn build(self) -> ReadRequestMetadata {
        self.request
    }

    fn apply_request_context(
        &mut self,
        context: &RequestContext,
        max_keys_read_counter: &Option<Arc<AtomicU64>>,
    ) {
        if self.request.concurrency == 0 || self.request.concurrency > context.dist_sql_concurrency
        {
            self.request.concurrency = context.dist_sql_concurrency;
        }

        let mut replica_read = context.replica_read;
        self.request.isolation_level = if context.weak_consistency {
            IsolationLevel::ReadCommitted
        } else if context.rc_check_ts {
            replica_read = ReplicaReadType::Leader;
            IsolationLevel::RcCheckTs
        } else {
            IsolationLevel::Snapshot
        };

        self.request.not_fill_cache = context.not_fill_cache;
        self.request.task_id = context.task_id;
        self.request.priority = kv_priority(context.priority);
        self.request.replica_read = replica_read;
        self.request.tiflash_replica_read = context.tiflash_replica_read;
        self.request.paging = context.paging;
        self.request.request_source = RequestSource {
            internal: context.in_restricted_sql,
            source_type: context.request_source_type.clone(),
            explicit_source_type: context.explicit_request_source_type.clone(),
        };
        self.request.store_batch_size = context.store_batch_size;
        self.request.resource_group_name = context.resource_group_name.clone();
        self.request.store_busy_threshold_ms = context.load_based_replica_read_threshold_ms;
        self.request.tikv_client_read_timeout_ms = context.tikv_client_read_timeout_ms;
        self.request.max_execution_time_ms = context.max_execution_time_ms;
        self.request.max_keys_read = context.max_keys_read;
        self.request.max_keys_read_counter = max_keys_read_counter.clone();
    }
}

const fn kv_priority(priority: Priority) -> KvPriority {
    match priority {
        Priority::NoPriority | Priority::Delayed => KvPriority::Normal,
        Priority::Low => KvPriority::Low,
        Priority::High => KvPriority::High,
    }
}
