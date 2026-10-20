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

/// Statement-owned options for the native KV snapshot. Coprocessor request
/// options have a different owner; ordinary Get/BatchGet must configure this one.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SnapshotReadOptions {
    /// Statement KV execution counter; clones retain the same admission reference.
    pub kv_exec_counter: tidb_util::topsql_stmtstats::KvExecCounterHandle,
    /// Go IsStalenessReadOnly; independent of replica read preference.
    pub is_staleness: bool,
    /// Go SessionVars.GetReplicaRead, before client-go's mode mapping.
    pub replica_read: ReplicaReadType,
    /// Scope used for snapshot visibility and closest-replica matching.
    pub read_replica_scope: String,
    /// Per-RPC read timeout; zero restores the native default.
    pub read_timeout_ms: u64,
    /// Statement resource group, overriding the activation-time group when supplied.
    pub resource_group_name: Option<String>,
    /// Load-based replica threshold, preserving duration precision until encoding.
    pub busy_threshold_ns: i64,
    /// Adaptive point-read estimate, threshold and configured zone.
    pub adaptive: Option<(f64, i64, String)>,
    /// Go `setOptionForTopSQL`'s statement values on the snapshot: its
    /// `kv.ResourceGroupTagger` and the statement ctx's `RUDetails`.
    pub kv_context: Option<StatementKvContextCell>,
    /// Go `snapshot.SetOption(kv.CollectRuntimeStats, stats)`: the point
    /// reader's own `SnapshotRuntimeStats`, which collect its Get/BatchGet
    /// RPCs while installed; `None` collects into the transaction's own.
    pub runtime_stats: Option<SnapshotRuntimeStatsHandle>,
}

/// A shared client-go `txnsnapshot.SnapshotRuntimeStats`, compared by
/// identity as Go compares the option's pointer.
#[derive(Clone)]
pub struct SnapshotRuntimeStatsHandle(pub std::sync::Arc<tikv_client::SnapshotRuntimeStats>);

impl PartialEq for SnapshotRuntimeStatsHandle {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

impl std::fmt::Debug for SnapshotRuntimeStatsHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SnapshotRuntimeStatsHandle")
            .field(&self.0.to_string())
            .finish()
    }
}

/// Go `StmtCtx.GetResourceGroupTagger`: builds the tag builder from the
/// statement's keyspace, plan digest and SQL digest when it is called.
pub type StatementResourceGroupTagger =
    std::sync::Arc<dyn Fn() -> crate::ResourceGroupTagBuilder + Send + Sync>;

/// Go `newLockCtx`'s `lockCtx.ResourceGroupTagger` (`pkg/executor/select.go`),
/// given the key of the request's first mutation: the encoded tag, or empty
/// (Go `nil`) when the statement has no normalized SQL.
pub type PessimisticLockResourceGroupTagger =
    std::sync::Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

/// The per-statement values Go reaches a transaction or snapshot call
/// through: the statement ctx's `util.RUDetailsCtxKey` and
/// `util.ExecDetailsKey` values (`execdetails.ContextWithInitializedExecDetails`),
/// which client-go charges on every RPC, and the statement's resource-group
/// taggers (`txn.SetOption(kv.ResourceGroupTagger, ...)`,
/// `lockCtx.ResourceGroupTagger`).
#[derive(Clone, Default)]
pub struct StatementKvContext {
    /// Go `ctx.Value(util.RUDetailsCtxKey)`.
    pub ru_details: Option<std::sync::Arc<tikv_client::RuDetails>>,
    /// Go `ctx.Value(util.ExecDetailsKey)`: the statement's KV/PD waits,
    /// backoffs and traffic.
    pub exec_details: Option<std::sync::Arc<tikv_client::util::ExecDetails>>,
    /// The traffic half of Go's `util.ExecDetails`, which client-rust's
    /// dispatch charges to its own collector rather than to the context's
    /// `ExecDetails`.
    pub network_traffic_details:
        Option<std::sync::Arc<tikv_client::traffic::NetworkTrafficDetails>>,
    /// Go `StmtCtx.GetResourceGroupTagger`.
    pub resource_group_tagger: Option<StatementResourceGroupTagger>,
    /// Go `lockCtx.ResourceGroupTagger`.
    pub pessimistic_lock_resource_group_tagger: Option<PessimisticLockResourceGroupTagger>,
}

impl std::fmt::Debug for StatementKvContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatementKvContext")
            .field("ru_details", &self.ru_details)
            .field("exec_details", &self.exec_details.is_some())
            .field("network_traffic_details", &self.network_traffic_details)
            .field(
                "resource_group_tagger",
                &self.resource_group_tagger.is_some(),
            )
            .field(
                "pessimistic_lock_resource_group_tagger",
                &self.pessimistic_lock_resource_group_tagger.is_some(),
            )
            .finish()
    }
}

/// A session's current [`StatementKvContext`], shared with the transactions
/// and snapshots its statements use: the session publishes each statement's
/// values as Go derives that statement's ctx, and a call reads the values in
/// force when it is made, as a Go call reads the ctx it is passed.
#[derive(Clone, Default)]
pub struct StatementKvContextCell(std::sync::Arc<std::sync::Mutex<StatementKvContext>>);

impl StatementKvContextCell {
    /// Installs the values of the statement that now runs.
    pub fn publish(&self, context: StatementKvContext) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = context;
    }

    /// The values of the statement in force.
    #[must_use]
    pub fn get(&self) -> StatementKvContext {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl PartialEq for StatementKvContextCell {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for StatementKvContextCell {}

impl std::fmt::Debug for StatementKvContextCell {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("StatementKvContextCell")
            .field(&self.get())
            .finish()
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
