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

//! Complete option-key and replica-read contracts from `pkg/kv/option.go`.

/// Transaction and snapshot option keys.
///
/// The discriminants are part of TiDB's package contract because callers use
/// raw integers through `SetOption`/`GetOption`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum OptionKey {
    /// Binlog data and client.
    BinlogInfo = 1,
    /// Schema-validity checker.
    SchemaChecker,
    /// Transaction isolation level.
    IsolationLevel,
    /// Transaction priority.
    Priority,
    /// Do not fill the storage cache.
    NotFillCache,
    /// Retained compatibility option; no longer used.
    SyncLog,
    /// Return keys only.
    KeyOnly,
    /// Pessimistic transaction marker.
    Pessimistic,
    /// Explicit snapshot timestamp.
    SnapshotTs,
    /// Replica-read policy.
    ReplicaRead,
    /// Statement task identifier.
    TaskId,
    /// InfoSchema snapshot.
    InfoSchema,
    /// Runtime statistics collection.
    CollectRuntimeStats,
    /// Pessimistic schema amender.
    SchemaAmender,
    /// Scan sampling step.
    SampleStep,
    /// Post-commit callback.
    CommitHook,
    /// Async-commit enablement.
    EnableAsyncCommit,
    /// One-phase-commit enablement.
    Enable1Pc,
    /// Linearizability guarantee.
    GuaranteeLinearizability,
    /// Transaction scope.
    TxnScope,
    /// Read-replica scope.
    ReadReplicaScope,
    /// Staleness-read-only marker.
    IsStalenessReadOnly,
    /// Required store labels.
    MatchStoreLabels,
    /// Static resource-group tag.
    ResourceGroupTag,
    /// Dynamic resource-group tagger.
    ResourceGroupTagger,
    /// In-memory KV filter.
    KvFilter,
    /// Snapshot interceptor.
    SnapshotInterceptor,
    /// Cached-table commit timestamp bound.
    CommitTsUpperBoundCheck,
    /// RPC interceptor.
    RpcInterceptor,
    /// Cached table-to-column maps.
    TableToColumnMaps,
    /// Assertion strictness.
    AssertionLevel,
    /// Whether the request source is internal.
    RequestSourceInternal,
    /// Current request source type.
    RequestSourceType,
    /// Explicit client request source type.
    ExplicitRequestSourceType,
    /// Replica-read request adjuster.
    ReplicaReadAdjuster,
    /// Iterator scan batch size.
    ScanBatchSize,
    /// Transaction source bitfield.
    TxnSource,
    /// Resource-group name.
    ResourceGroupName,
    /// Load-based replica-read threshold.
    LoadBasedReplicaReadThreshold,
    /// TiKV read timeout.
    TikvClientReadTimeout,
    /// Mem-buffer size limits.
    SizeLimits,
    /// Connection/session identifier.
    SessionId,
    /// Background-task lifecycle hooks.
    BackgroundGoroutineLifecycleHooks,
    /// Prewrite lock-conflict policy.
    PrewriteEncounterLockPolicy,
}

impl OptionKey {
    /// Returns the raw integer accepted by Go's option API.
    #[must_use]
    pub const fn raw(self) -> i32 {
        self as i32
    }
}

/// Mem-buffer entry and total-size limits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TxnSizeLimits {
    /// Maximum one-entry size.
    pub entry: u64,
    /// Maximum total transaction size.
    pub total: u64,
}

/// Replica selection for KV reads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicaReadType(u8);

#[allow(non_upper_case_globals)]
impl ReplicaReadType {
    /// Read from the leader.
    pub const Leader: Self = Self(0);
    /// Read from a follower.
    pub const Follower: Self = Self(1);
    /// Read from leaders and followers.
    pub const Mixed: Self = Self(2);
    /// Read from a replica in the same zone.
    pub const Closest: Self = Self(3);
    /// Adaptively select a same-zone follower.
    pub const ClosestAdaptive: Self = Self(4);
    /// Read from a learner.
    pub const Learner: Self = Self(5);
    /// Prefer the leader and fall back when it is unhealthy.
    pub const PreferLeader: Self = Self(6);

    /// Preserves every raw Go `byte` value.
    #[must_use]
    pub const fn from_raw(value: u8) -> Self {
        Self(value)
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn raw(self) -> u8 {
        self.0
    }

    /// Returns whether followers may be used.
    #[must_use]
    pub const fn is_follower_read(self) -> bool {
        !matches!(self, Self::Leader)
    }

    /// Returns whether strict closest-replica routing is requested.
    #[must_use]
    pub const fn is_closest_read(self) -> bool {
        matches!(self, Self::Closest)
    }
}

/// Request-origin metadata propagated to storage RPCs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestSource {
    /// Whether the request is internal to TiDB.
    pub internal: bool,
    /// Internal request category.
    pub source_type: String,
    /// Explicit user/client category.
    pub explicit_source_type: String,
}

/// Returns the internal source type carried by a typed request context.
#[must_use]
pub fn get_internal_source_type(source: Option<&RequestSource>) -> &str {
    source.map_or("", |source| source.source_type.as_str())
}

/// Low-cardinality internal transaction categories.
pub const INTERNAL_TXN_OTHERS: &str = "others";
/// Garbage collection.
pub const INTERNAL_TXN_GC: &str = "gc";
/// Bootstrap aliases the low-cardinality miscellaneous category.
pub const INTERNAL_TXN_BOOTSTRAP: &str = INTERNAL_TXN_OTHERS;
/// Metadata operations alias the low-cardinality miscellaneous category.
pub const INTERNAL_TXN_META: &str = INTERNAL_TXN_OTHERS;
/// DDL.
pub const INTERNAL_TXN_DDL: &str = "ddl";
/// Prefix for DDL backfill request types.
pub const INTERNAL_TXN_BACKFILL_DDL_PREFIX: &str = "ddl_";
/// Cache-table work aliases the miscellaneous category.
pub const INTERNAL_TXN_CACHE_TABLE: &str = INTERNAL_TXN_OTHERS;
/// Analyze/statistics work.
pub const INTERNAL_TXN_STATS: &str = "stats";
/// Foreground-priority statistics work.
pub const INTERNAL_TXN_STATS_FOREGROUND_PRIORITY: &str = "StatsForegroundPriority";
/// Bind-info work aliases the miscellaneous category.
pub const INTERNAL_TXN_BIND_INFO: &str = INTERNAL_TXN_OTHERS;
/// Workload learning.
pub const INTERNAL_TXN_WORKLOAD_LEARNING: &str = "WorkloadLearning";
/// System-variable work aliases the miscellaneous category.
pub const INTERNAL_TXN_SYS_VAR: &str = INTERNAL_TXN_OTHERS;
/// Telemetry aliases the miscellaneous category.
pub const INTERNAL_TXN_TELEMETRY: &str = INTERNAL_TXN_OTHERS;
/// Administrative work.
pub const INTERNAL_TXN_ADMIN: &str = "admin";
/// Privilege work aliases the miscellaneous category.
pub const INTERNAL_TXN_PRIVILEGE: &str = INTERNAL_TXN_OTHERS;
/// Tooling work.
pub const INTERNAL_TXN_TOOLS: &str = "tools";
/// Backup and restore.
pub const INTERNAL_TXN_BR: &str = "br";
/// Lightning.
pub const INTERNAL_TXN_LIGHTNING: &str = "lightning";
/// TRACE statements.
pub const INTERNAL_TXN_TRACE: &str = "Trace";
/// TTL.
pub const INTERNAL_TXN_TTL: &str = "TTL";
/// LOAD DATA.
pub const INTERNAL_LOAD_DATA: &str = "LoadData";
/// IMPORT INTO.
pub const INTERNAL_IMPORT_INTO: &str = "ImportInto";
/// Distributed tasks.
pub const INTERNAL_DIST_TASK: &str = "DistTask";
/// Internal timers.
pub const INTERNAL_TIMER: &str = "Timer";
/// DDL notifier.
pub const INTERNAL_DDL_NOTIFIER: &str = "DDLNotifier";
