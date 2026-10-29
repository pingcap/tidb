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

//! A reader's coprocessor read before it is lowered: the scan, its pushed
//! conjuncts, cap, TopN or partial aggregate, and the statement's DistSQL
//! context (Go `distsqlctx.DistSQLContext`). [`super::open_remote_scan`]
//! lowers it into a DAG (`crate::cop_reader`, or the MPP coordinator for a
//! TiFlash read) and sends it through the store's `kv.Client`.
//!
//! # What this is for
//!
//! [`tidb_kv::storage::TableStorage`] speaks keys and bytes, so a scan through
//! it always moves the range's packed bytes to the client and decodes them
//! there. A cluster backend can do better: TiKV evaluates the predicate, the
//! row cap and the column projection at the region, and only the surviving
//! rows cross the network. That is not expressible as a key/value iterator --
//! the answer is *rows*, not pairs.
//!
//! A store without a coprocessor client (the in-process store) is never
//! sent the request and the caller keeps its byte-level cursor.
//!
//! # The staged-buffer rule, made structural
//!
//! A coprocessor answers from the **snapshot** only. Inside an explicit
//! transaction the session's staged mutations are client-side (Go's
//! `MemBuffer` in front of `kv.Snapshot`, and Go's `UnionScan` on top of a
//! coprocessor reader), so a remote scan that returned only what TiKV saw
//! would lose every uncommitted row -- and would wrongly keep rows the
//! transaction has already deleted or changed out of the predicate.
//!
//! A remote read therefore never hands back a row stream alone. [`PushdownScan`]
//! carries the stream *and* the session's staged writes for the same range,
//! so a caller physically cannot consume the remote rows without being handed
//! the overlay it has to merge. The caller re-applies the full pushed
//! predicate to the staged rows, exactly as Go's `UnionScan` filters its
//! membuffer rows through the same conditions.
//!
//! # Why the pushed predicate is best-effort and the answer is still exact
//!
//! The conjuncts in [`PushdownScanRequest::predicates`] are a *request*. A
//! backend may lower all of them, some of them, or none -- whatever its
//! coprocessor lowering accepts. A stream that reports every conjunct applied
//! (`PushdownRowStream::predicates_applied`) is trusted as Go trusts TiKV's
//! Selection; otherwise the caller evaluates every pushed conjunct itself on
//! every row it emits, so a partial remote filter only ever returns a
//! superset of the answer, which the local test narrows. Staged rows are
//! always tested locally. The same holds for [`PushdownScanRequest::limit`]: it
//! is an early-stop hint, and the caller still enforces the cap.

use std::fmt;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_util::context::WarningCollector;
use tidb_expr::expression::Expression;
use tidb_expr::pushdown_catalog::PbConverterContext;
use tidb_txnkv::Key;

/// Go `kv.StoreType` narrowed to the engines a pushdown scan names.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PushdownReadEngine {
    /// Go `kv.TiKV`: the row storage (the only engine the TiKV lowering
    /// serves).
    #[default]
    TiKv,
    /// Go `kv.TiFlash`: the columnar replica, served through MPP dispatch.
    TiFlash,
}
use tidb_kv::storage::StorageError;

/// One column a remote scan must return, in the order the caller wants it.
#[derive(Clone, Debug, PartialEq)]
pub struct PushdownScanColumn {
    /// The table column's stable id, or [`EXTRA_HANDLE_COLUMN_ID`] for the
    /// synthetic handle column of a table whose handle is no column of its
    /// own.
    pub id: i64,
    /// The column's declared type, which decides how its bytes decode.
    pub field_type: FieldType,
    /// Whether the row handle *is* this column's value, so the backend reads
    /// it from the record key rather than from the row value (Go's
    /// `ColumnInfo.PkHandle`).
    pub is_handle: bool,
    /// Go `ColumnInfo.OriginDefaultValue` as the datum a row written before
    /// this column existed reads back — carried so the coprocessor's
    /// `ColumnInfo.default_val` names it (Go `util.ColumnToProto`), because
    /// only the region sees which rows lack the column's bytes.
    pub origin_default: Option<Datum>,
}

/// Go `model.ExtraHandleID`: the column id of the implicit `_tidb_rowid`
/// handle a table without an integer primary key carries.
pub const EXTRA_HANDLE_COLUMN_ID: i64 = -1;

/// One function in a grouped partial aggregation pushed below a reader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushdownAggregateKind {
    /// `COUNT(expr)`; a missing input offset means Go's `COUNT(1)` lowering
    /// of `COUNT(*)`.
    Count,
    /// `SUM(expr)`.
    Sum,
    /// `MIN(expr)`.
    Min,
    /// `MAX(expr)`.
    Max,
    /// `MIN_COUNT(expr)`: count rows tied at the minimum argument.
    MinCount,
    /// `MAX_COUNT(expr)`: count rows tied at the maximum argument.
    MaxCount,
}

/// A pushed aggregate function and the scan-row expression it reads.
#[derive(Clone, Debug)]
pub struct PushdownAggregateFunction {
    /// The aggregate function implemented by TiKV.
    pub kind: PushdownAggregateKind,
    /// Expression evaluated for each qualifying scan row, or `None` for
    /// `COUNT(1)`/`COUNT(*)`.
    pub input: Option<Expression>,
    /// The partial result column returned by TiKV.
    pub output_type: FieldType,
}

/// One aggregation the base scan may execute inside TiKV before rows cross
/// the network. This deliberately covers only partial stages with a typed
/// planner representation and a corresponding TiKV DAG lowering.
#[derive(Clone, Debug)]
pub enum PushdownPartialAggregate {
    /// A grouped partial aggregation. TiKV returns aggregate results first
    /// and group keys last, matching its aggregation-schema contract.
    Grouped {
        /// Group-key offsets in [`PushdownScanRequest::columns`].
        group_offsets: Vec<usize>,
        /// Group-key result types, index-parallel with `group_offsets`.
        group_types: Vec<FieldType>,
        /// Aggregate functions, in physical output order.
        functions: Vec<PushdownAggregateFunction>,
        /// `true` for StreamAgg over ordered input, `false` for HashAgg.
        streamed: bool,
    },
    /// A global partial aggregation. TiKV returns exactly one row containing
    /// the function states, including for empty input.
    Global {
        /// Aggregate functions in physical output order.
        functions: Vec<PushdownAggregateFunction>,
        /// `true` for StreamAgg over ordered input, `false` for HashAgg.
        streamed: bool,
    },
}

impl PushdownPartialAggregate {
    /// The columns the partial stage returns, in TiKV aggregation-schema
    /// order (aggregate functions followed by group keys).
    #[must_use]
    pub fn output_types(&self) -> Vec<FieldType> {
        match self {
            Self::Grouped {
                group_types,
                functions,
                ..
            } => functions
                .iter()
                .map(|function| function.output_type.clone())
                .chain(group_types.iter().cloned())
                .collect(),
            Self::Global { functions, .. } => functions
                .iter()
                .map(|function| function.output_type.clone())
                .collect(),
        }
    }
}

/// Index-scan identity for a partial aggregation request. `None` on
/// [`PushdownScanRequest`] means the existing table-scan path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PushdownIndexScan {
    /// Stable schema index id.
    pub index_id: i64,
    /// Whether the schema declares the index unique.
    pub declared_unique: bool,
    /// Number of indexed key columns.
    pub index_column_count: usize,
    /// Whether the index keys are scanned from high to low.
    pub desc: bool,
}

/// One column order in a coprocessor TopN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PushdownTopNOrder {
    /// Offset in [`PushdownScanRequest::columns`].
    pub offset: usize,
    /// Whether larger values sort first.
    pub desc: bool,
}

/// A bounded sort executed after the scan's complete Selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PushdownTopN {
    /// Ordered comparison keys, in SQL by-item order.
    pub order_by: Vec<PushdownTopNOrder>,
    /// Rows retained from offset zero. The root TopN applies the SQL offset.
    pub limit: u64,
}

/// Go's coprocessor Limit over an already-truncated index column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PushdownPrefixLimit {
    /// Read at least this many rows and retain the final row's entire prefix group.
    pub count: u64,
    /// Position of the prefix column in the index scan's input schema.
    pub column_offset: usize,
}

/// The executor kind of a pushed plan node, which decides the DAG executor
/// it becomes (Go lowers the pushed plan in order, so the nth plan node is
/// the nth executor).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopPlanKind {
    /// `PhysicalTableScan` (also a `TableRowIDScan`).
    TableScan,
    /// `PhysicalIndexScan`.
    IndexScan,
    /// `PhysicalSelection`.
    Selection,
    /// `PhysicalLimit`.
    Limit,
    /// `PhysicalTopN`.
    TopN,
    /// `PhysicalHashAgg` or `PhysicalStreamAgg`.
    Aggregation,
    /// Any other pushed node: it matches no executor this request builds.
    Other,
}

/// One pushed plan node of a reader: Go's `copPlanIDs` entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CopPlanId {
    /// What the node lowers to.
    pub kind: CopPlanKind,
    /// `PhysicalPlan.ID()`.
    pub id: isize,
}

/// One base-table scan a backend may serve remotely.
#[derive(Clone, Debug)]
pub struct PushdownScanRequest {
    /// The table whose record range is scanned.
    pub table_id: i64,
    /// An index source for this request; `None` means a table scan.
    pub index: Option<PushdownIndexScan>,
    /// The columns to return, in output order.
    pub columns: Vec<PushdownScanColumn>,
    /// Which of `columns` carries the integer row handle used to merge a
    /// staged overlay. `None` is a common-handle scan, whose reader derives
    /// identity from the complete primary-key columns instead.
    pub handle_index: Option<usize>,
    /// Stable column IDs forming a common row handle, in handle order.
    pub primary_column_ids: Vec<i64>,
    /// Common-handle columns stored as prefixes rather than whole values.
    pub primary_prefix_column_ids: Vec<i64>,
    /// The scan's complete Selection, as the planner's own physical
    /// conditions: the backend converts all of them to TiPB (Go `ExprToPB`)
    /// or none of them, and reports which through
    /// [`PushdownRowStream::predicates_applied`]. Never a subset: an operator
    /// above the Selection (a limit, a TopN, an aggregate, a narrowed output)
    /// is only sent when the whole list was.
    pub conditions: Vec<Expression>,
    /// Final output offsets into `columns`, after every predicate has been
    /// evaluated. `None` returns every requested scan column.
    ///
    /// A backend must refuse this shape unless it can lower every predicate:
    /// once the narrower row crosses the wire, the caller no longer has the
    /// columns needed to repeat a residual filter locally.
    pub output_offsets: Option<Vec<usize>>,
    /// A bounded sort after the complete remote Selection. Best-effort; the
    /// caller retains an equivalent local TopN when the backend refuses it.
    pub topn: Option<PushdownTopN>,
    /// A row cap the backend may stop at. Best-effort: see the module doc.
    pub limit: Option<u64>,
    /// A prefix-aware limit. Kept separate from the ordinary cap so a backend
    /// that does not apply it cannot truncate the final prefix group.
    pub prefix_limit: Option<PushdownPrefixLimit>,
    /// Go's IndexLookUp worker seeds the index coprocessor request's row
    /// paging window from its calculated handle batch size. `None` retains
    /// the session default used by ordinary table/covering scans.
    pub paging_min_size: Option<u64>,
    /// A partial aggregation above the scan/Selection. When present, every
    /// predicate must be lowered and no staged write may be merged locally.
    pub aggregate: Option<PushdownPartialAggregate>,
    /// Read the ranges BACKWARDS -- Go's `desc` on the `TableScan`
    /// executor. The ranges themselves stay in ascending key order; the
    /// backend walks them last-to-first, each reversed, and the caller's
    /// staged-write merge runs in the same descending key order.
    pub desc: bool,
    /// Whether the coprocessor must preserve record-key order.
    ///
    /// Go's `PhysicalTableScan.KeepOrder` is false for ordinary full scans
    /// such as TPC-H hash-join inputs. Cluster storage raises this only for
    /// order-sensitive staged overlays and streamed partial aggregation.
    pub keep_order: bool,
    /// Whether region responses may arrive in any order. Only sound when the
    /// caller re-orders (or ignores) row order itself -- go's rolling
    /// `fetchHandles` keeps up to `2 x DistSQLConcurrency` region requests in
    /// flight for exactly such unordered double reads, while an ordered
    /// stream is consumed two regions at a time.
    pub allow_unordered_response: bool,
    /// The timestamp the remote scan must read at: the statement's own
    /// snapshot, filled in by the storage that owns it.
    pub snapshot_ts: u64,
    /// Go `PhysicalTableScan.StoreType`: which engine serves this scan.
    /// TiFlash requests are lowered by the MPP dispatch source when the node
    /// has one; the TiKV lowering must refuse them by name instead of
    /// silently serving the table from row storage.
    pub read_engine: PushdownReadEngine,
    /// Go `MPPDispatchRequest.SchemaVar` (`is.SchemaMetaVersion()`): the
    /// schema generation the request's table was resolved against. TiFlash
    /// refuses a dispatch whose version cannot resolve the table, so this
    /// must be nonzero for a TiFlash read; zero names no schema.
    pub schema_version: u64,
    /// The scanned record ranges, as half-open `[start, end)` pairs in
    /// ascending key order.
    ///
    /// A whole-table scan is one range. A `TableRangeScan` over a clustered
    /// handle is one per handle range, which is what a coprocessor request
    /// carries natively (`Request.Ranges` is a list), so the narrowing
    /// reaches the region rather than being re-applied after the rows have
    /// already crossed the network.
    pub ranges: Vec<(Key, Key)>,
    /// Go's `RequestBuilder.SetTableHandles` carries one row-count hint per
    /// grouped handle range so DistSQL can schedule region tasks with the
    /// same cardinality estimate. Empty for ordinary scans.
    pub range_hints: Vec<usize>,
    /// The statement's coprocessor seam: `DAGRequest.flags` and the sink the
    /// warnings TiKV reports must land in. See [`PushdownStatementContext`].
    pub statement: PushdownStatementContext,
}

/// The statement context a request converts its conditions under: Go's
/// `NewPBConverter(client, ctx)` is handed the statement's `EvalContext`, and
/// the coprocessor client converts the request's conditions through it.
#[derive(Clone)]
pub struct StatementEval(pub Arc<dyn PbConverterContext>);

impl StatementEval {
    /// The evaluation context `ExprToPB` reads under.
    #[must_use]
    pub fn context(&self) -> &dyn PbConverterContext {
        &*self.0
    }
}

impl fmt::Debug for StatementEval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StatementEval")
    }
}

/// What a coprocessor request must be told about the STATEMENT that issued it,
/// as opposed to the relation it reads.
///
/// The two halves are one type because they are one bug: `flags` decides
/// whether TiKV degrades a truncation to a warning or fails the request, and
/// `warnings` decides whether that warning is ever seen. Sending the flags
/// without the sink turns a failing query into a silently truncating one --
/// strictly worse than the failure it replaced -- so no call site may thread
/// one and forget the other.
#[derive(Clone, Debug)]
pub struct PushdownStatementContext {
    /// Statement KV execution counter shared with point readers.
    pub kv_exec_counter: tidb_util::topsql_stmtstats::KvExecCounterHandle,
    /// Go's statement ctx values every DistSQL request carries: its
    /// `RUDetails` (`util.RUDetailsCtxKey`) and, through `SetFromSessionVars`,
    /// `StmtCtx.GetResourceGroupTagger`.
    pub kv_context: Option<tidb_txnkv::StatementKvContextCell>,
    /// Shared statement quota and SQL killer for remote response ownership.
    pub memory: tidb_kv::mem_quota::StatementMemory,
    /// One query identity and allocation state for every MPP gather in this statement.
    pub mpp_query_info: Arc<tidb_kv::mpp_query::MppQueryInfo>,
    /// Domain/server-info ID, never the operating system process ID.
    pub mpp_server_id: u64,
    /// Validated session compute placement policy.
    pub tiflash_compute_dispatch_policy: String,
    /// Go AllowFallbackToTiKV[TiFlash], retained for safe MPP recovery.
    pub allow_tiflash_fallback: bool,
    /// Go's TryCopLiteWorker permits only one inline reader per statement.
    pub cop_lite_worker: Arc<std::sync::atomic::AtomicBool>,
    /// Physical scan plan ID used by Go `RuntimeStatsColl`.
    pub plan_id: isize,
    /// Go `copPlanIDs`' source: the reader's pushed plan nodes, leaf first
    /// (`getPhysicalPlanIDs`), which the request's executors are matched
    /// against by kind when the DAG is built.
    pub cop_plan_chain: Vec<CopPlanId>,
    /// Go `rootPlanID` of `SelectWithRuntimeStats`: the plan whose runtime
    /// stats the select result registers. `0` means the scan (`plan_id`).
    pub cop_root_plan_id: isize,
    /// The table side of an index lookup: a batch of handles is read by a
    /// request that carries this chain and root instead of the index
    /// side's (Go's table reader executor owns `tblPlans`).
    pub lookup_cop_plan_chain: Option<(Vec<CopPlanId>, isize)>,
    /// Go `StatementContext.PushDownFlags()`; see
    /// the statement context's `push_down_flags`.
    ///
    /// The `Default` is `0`, TiKV's strictest branch, which is correct for a
    /// caller with no statement behind it (a fixture, a synthetic request):
    /// such a caller has no session to warn either.
    pub push_down_flags: u64,
    /// Go `DistSQLContext.WarnHandler`: the statement's own warning buffer.
    pub warnings: WarningCollector,
    /// Go `ConstructDAGReq`'s `dagReq.TimeZoneName, dagReq.TimeZoneOffset =
    /// timeutil.Zone(ctx.GetSessionVars().Location())`: the zone the REGION
    /// evaluates this request's conditions in.
    ///
    /// It rides the STATEMENT rather than the scanner because Go reads it
    /// fresh from `SessionVars` for every request, while the scanner is one
    /// object shared by every connection of a node: a zone held there would be
    /// a process-wide constant no `SET time_zone` could correct, and a setter
    /// on it would let one connection re-zone another connection's reads.
    ///
    /// The `Default` is UTC, which is the zone a caller with no session behind
    /// it evaluates in.
    pub time_zone: SessionTimeZone,
    /// The statement's evaluation context, which `ExprToPB` reads constants,
    /// parameters and correlated bindings from and the expression pushdown
    /// blacklist is consulted under. `None` for a caller with no statement
    /// behind it: conditions then convert from their strict literals alone.
    pub eval_context: Option<StatementEval>,
    /// Go `StmtCtx.ResourceGroupName`, copied to every DistSQL request.
    pub resource_group_name: String,
    /// Go `SessionVars.GetReplicaRead()` copied to every DistSQL request.
    pub replica_read: tidb_txnkv::ReplicaReadType,
    /// Scope and stale mode selected by the transaction provider.
    pub read_replica_scope: String,
    /// Go StmtCtx.IsStaleness; not inferred from the request timestamp.
    pub is_staleness: bool,
    /// Retained settings, independent of later SET statements.
    pub read_policy: CoprocessorReadPolicy,
    /// Go reader GetNetDataSize; supplied by the retained physical plan.
    pub estimated_net_bytes: f64,
    /// Go table reader average row size for a later index lookup batch.
    pub lookup_avg_row_bytes: Option<f64>,
    /// Effective statement deadline, including MAX_EXECUTION_TIME hints.
    pub max_execution_time_ms: u64,
    /// Go `RequestBuilder.getKVPriority(StmtCtx.Priority)`: the statement's
    /// own priority modifier projected onto KV's three values.
    pub priority: crate::Priority,
    /// Go `StmtCtx.NotFillCache`, set by a SELECT's `SQL_NO_CACHE`.
    pub not_fill_cache: bool,
    /// Go `SessionVars.DistSQLScanConcurrency()`, the request concurrency
    /// `SetFromSessionVars` gives every DistSQL request.
    pub dist_sql_scan_concurrency: u64,
    /// Go `SessionVars.StoreBatchSize`, which `SetFromSessionVars` copies to
    /// every coprocessor request.
    pub store_batch_size: u64,
    /// Go `SessionVars.TiFlashReplicaRead`, which places MPP tasks.
    pub tiflash_replica_read: tidb_txnkv::ReplicaRead,
    /// Go's query-scoped per-store coprocessor limiter, shared by every
    /// remote scan in this statement.
    pub query_cop_store_limiter: Option<std::sync::Arc<tidb_txnkv::QueryCopStoreLimiter>>,
    /// Go `SessionVars.DivPrecisionIncrement`, copied to every DAG request's
    /// `DivPrecisionIncrement` when it differs from the default.
    pub div_precision_increment: u32,
    /// The current transaction's (or, in autocommit, current statement's)
    /// staged-write tracker. See [`tidb_kv::staged_writes::StagedWrites`].
    pub staged_writes: std::sync::Arc<tidb_kv::staged_writes::StagedWrites>,
    /// Go `DistSQLContext.MaxKeysRead`/`MaxKeysReadCounter`: the statement's
    /// `tidb_max_keys_read` budget and the counter `tidb_keys_examined` sums.
    pub keys_read: tidb_kv::keys_read::KeysReadBudget,
    /// Go `selectResult`'s `ctx.ExecDetails`/`RuntimeStatsColl`: where each
    /// response's `CopRuntimeStats` goes; `None` when the statement does not
    /// collect execution info.
    pub cop_response_sink: Option<Arc<dyn crate::CopResponseSink>>,
}

impl Default for PushdownStatementContext {
    fn default() -> Self {
        Self {
            memory: tidb_kv::mem_quota::StatementMemory::default(),
            kv_exec_counter: Default::default(),
            kv_context: None,
            mpp_query_info: Arc::default(),
            mpp_server_id: 0,
            tiflash_compute_dispatch_policy: "consistent_hash".into(),
            allow_tiflash_fallback: false,
            plan_id: 0,
            cop_plan_chain: Vec::new(),
            cop_root_plan_id: 0,
            lookup_cop_plan_chain: None,
            push_down_flags: 0,
            cop_lite_worker: Arc::default(),
            warnings: WarningCollector::default(),
            time_zone: SessionTimeZone::default(),
            eval_context: None,
            resource_group_name: "default".to_owned(),
            replica_read: tidb_txnkv::ReplicaReadType::Leader,
            read_replica_scope: "global".into(),
            is_staleness: false,
            read_policy: CoprocessorReadPolicy::default(),
            estimated_net_bytes: 0.0,
            lookup_avg_row_bytes: None,
            max_execution_time_ms: 0,
            priority: crate::Priority::None,
            not_fill_cache: false,
            dist_sql_scan_concurrency: tidb_vardef::defaults::DEF_DIST_SQL_SCAN_CONCURRENCY as u64,
            store_batch_size: tidb_vardef::defaults::DEF_TIDB_STORE_BATCH_SIZE as u64,
            tiflash_replica_read: tidb_txnkv::ReplicaRead::AllReplicas,
            query_cop_store_limiter: None,
            // Go `vardef.DefDivPrecisionIncrement`; a caller with no statement
            // behind it has no session value to send.
            div_precision_increment: 4,
            staged_writes: Arc::default(),
            keys_read: tidb_kv::keys_read::KeysReadBudget::default(),
            cop_response_sink: None,
        }
    }
}

impl PushdownStatementContext {

    /// A reader whose storage holds no buffered writes to merge -- Go's
    /// reader executors read the statement's snapshot alone, and a union
    /// scan's mem reader the buffered entries alone -- keeps no staged-write
    /// tracker, so nothing in it routes rows as dirty.
    #[must_use]
    pub fn without_staged_writes(mut self) -> Self {
        self.staged_writes = Arc::default();
        self
    }

    /// Retain the response estimate computed from the physical reader subtree.
    #[must_use]
    pub fn with_estimated_net_bytes(mut self, bytes: f64) -> Self {
        self.estimated_net_bytes = bytes;
        self
    }

    /// Derive the table request estimate from this batch's actual handle count.
    #[must_use]
    pub fn for_lookup_batch(&self, count: usize) -> Self {
        let mut statement = self.clone();
        if let Some(avg) = self.lookup_avg_row_bytes {
            statement.estimated_net_bytes = avg * count as f64;
        }
        if let Some((chain, root)) = &self.lookup_cop_plan_chain {
            statement.cop_plan_chain = chain.clone();
            statement.cop_root_plan_id = *root;
        }
        statement
    }

    /// Binds the pushed plan nodes (leaf first) and the plan whose select
    /// result stats this read registers (Go `getPhysicalPlanIDs` and
    /// `rootPlanID`).
    #[must_use]
    pub fn with_cop_plan_chain(mut self, chain: Vec<CopPlanId>, root_plan_id: i64) -> Self {
        self.cop_plan_chain = chain;
        self.cop_root_plan_id = root_plan_id as isize;
        self
    }

    /// Binds the table side of an index lookup (see
    /// [`Self::lookup_cop_plan_chain`]).
    #[must_use]
    pub fn with_lookup_cop_plan_chain(mut self, chain: Vec<CopPlanId>, root_plan_id: i64) -> Self {
        self.lookup_cop_plan_chain = Some((chain, root_plan_id as isize));
        self
    }

    /// Binds the physical scan whose TiKV execution summary owns this read.
    #[must_use]
    pub fn with_plan_id(mut self, plan_id: i64) -> Self {
        self.plan_id = plan_id as isize;
        self
    }
}

/// Session policy consumed by Go's DistSQL request builder and read adjuster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoprocessorReadPolicy {
    /// Minimum estimated response bytes per region for closest-adaptive reads.
    pub closest_read_threshold: i64,
    /// Go duration converted to nanoseconds; converted only when a TiKV task is built.
    pub busy_threshold_ns: i64,
    /// Session `tikv_client_read_timeout`, in milliseconds; zero uses the client default.
    pub read_timeout_ms: u64,
}

impl Default for CoprocessorReadPolicy {
    fn default() -> Self {
        Self {
            closest_read_threshold: tidb_vardef::defaults::DEF_ADAPTIVE_CLOSEST_READ_THRESHOLD,
            busy_threshold_ns: 1_000_000_000,
            read_timeout_ms: 0,
        }
    }
}

/// A lazily pulled stream of snapshot rows a backend served remotely.
///
/// `Send` so a prefetched index-join lookup can hand its remote cursor to a
/// pool worker (Go's inner workers drain their readers off the caller).
pub trait PushdownRowStream: Send {
    /// Go `RuntimeStatsColl.GetCopCountAndRows` for the scan executor.
    fn cop_count_and_rows(&self) -> (u64, u64) {
        (0, 0)
    }
    /// The next row in record-key order, as the requested columns, or `None`
    /// at the end of the answer.
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError>;

    /// Whether this stream can transfer decoded columnar batches without
    /// first materializing every row as `Vec<Datum>`.
    fn supports_chunks(&self) -> bool {
        false
    }

    /// The next decoded columnar batch using the executor's current Go
    /// `Chunk.RequiredRows` demand. Callers use this only after
    /// [`Self::supports_chunks`] returns true; row-only backends keep the
    /// existing [`Self::next_row`] contract.
    fn next_chunk(&mut self, _required_rows: usize) -> Result<Option<Chunk>, StorageError> {
        Ok(None)
    }

    /// How many rows have crossed the network so far. This is the wire
    /// receipt: with a lowered predicate it is smaller than the table holds.
    fn rows_returned(&self) -> u64;

    /// Whether the backend lowered every predicate in the request. A caller
    /// may skip duplicate client-side evaluation only for a clean stream;
    /// staged rows still require the normal local test.
    fn predicates_applied(&self) -> bool {
        false
    }

    /// Releases the request, which an abandoned stream (an early-stopping
    /// `LIMIT`) must still do.
    fn close(&mut self);
}

/// Two (or more) remote scans serving ONE ordered read whose unsigned-handle
/// halves cannot form a single ascending range list. Go opens
/// `firstPartGroupedRanges then `secondPartGroupedRanges as separate results
/// and reads them one after the other through
/// `resultHandler.open(firstResult, secondResult); this is that sequence at
/// the stream seam. Rows from part N+1 begin only after part N returns `None.
///
/// Chunks are deliberately NOT supported: a columnar batch cannot span a part
/// boundary, and the row path keeps the boundary invisible to callers.
pub struct ChainedPushdownStream {
    /// Every part in read order; drained by index.
    parts: Vec<Box<dyn PushdownRowStream>>,
    /// Index of the part currently serving [`PushdownRowStream::next_row].
    current: usize,
    /// Whether a part has returned `None AND no later part exists -- after
    /// which every call answers `None without touching the parts.
    finished: bool,
}

impl ChainedPushdownStream {
    /// Chains the parts in READ order: part 0's rows all precede part 1's.
    #[must_use]
    pub fn new(parts: Vec<Box<dyn PushdownRowStream>>) -> Box<dyn PushdownRowStream> {
        assert!(
            !parts.is_empty(),
            "a chained stream needs at least one part"
        );
        Box::new(ChainedPushdownStream {
            parts,
            current: 0,
            finished: false,
        })
    }
}

impl PushdownRowStream for ChainedPushdownStream {
    fn cop_count_and_rows(&self) -> (u64, u64) {
        self.parts.iter().fold((0, 0), |total, part| {
            let part = part.cop_count_and_rows();
            (total.0.wrapping_add(part.0), total.1.wrapping_add(part.1))
        })
    }
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
        if self.finished {
            return Ok(None);
        }
        loop {
            let Some(part) = self.parts.get_mut(self.current) else {
                self.finished = true;
                return Ok(None);
            };
            match part.next_row()? {
                Some(row) => return Ok(Some(row)),
                None => self.current += 1,
            }
        }
    }

    // A batch cannot span the part boundary, so the chain always serves rows.

    fn rows_returned(&self) -> u64 {
        self.parts.iter().map(|part| part.rows_returned()).sum()
    }

    fn predicates_applied(&self) -> bool {
        self.parts.iter().all(|part| part.predicates_applied())
    }

    fn close(&mut self) {
        for part in &mut self.parts {
            part.close();
        }
    }
}

/// A remote scan plus the client-side overlay it must be merged with.
pub struct PushdownScan {
    /// The snapshot rows, filtered and capped at the backend.
    pub stream: Box<dyn PushdownRowStream>,
    /// The session's staged writes inside the scanned range, in key order and
    /// still encoded: the caller owns decoding them, because row layout is
    /// not this seam's business. `None` is a staged delete.
    pub staged: Vec<(Key, Option<Vec<u8>>)>,
}

impl fmt::Debug for PushdownScan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PushdownScan")
            .field("rows_returned", &self.stream.rows_returned())
            .field("staged", &self.staged.len())
            .finish()
    }
}

/// Why a remote scan did not open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PushdownScannerError {
    /// The backend declines this request shape; the caller must use the
    /// byte-level cursor instead. Never a wrong answer, only a slower one.
    Unsupported(String),
    /// The backend tried and failed.
    Backend(StorageError),
}

impl fmt::Display for PushdownScannerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(reason) => write!(formatter, "remote scan is unsupported: {reason}"),
            Self::Backend(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for PushdownScannerError {}

