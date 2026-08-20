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

//! The seam a storage backend uses to serve a base-table scan *remotely*:
//! Go's coprocessor request, described here without naming distsql, tipb, or
//! a transport.
//!
//! # What this seam is for
//!
//! [`crate::storage::TableStorage`] speaks keys and bytes, so a scan through
//! it always moves the range's packed bytes to the client and decodes them
//! there. A cluster backend can do better: TiKV evaluates the predicate, the
//! row cap and the column projection at the region, and only the surviving
//! rows cross the network. That is not expressible as a key/value iterator --
//! the answer is *rows*, not pairs -- so it is a second, optional method of
//! the storage seam rather than a widening of `iter`.
//!
//! A backend that does not have a coprocessor (the in-process store) returns
//! `None` and the caller keeps its existing byte-level cursor. Nothing above
//! the seam changes shape.
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
//! This seam therefore cannot hand back a row stream alone. [`PushdownScan`]
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
//! coprocessor lowering accepts -- because the caller keeps evaluating every
//! pushed conjunct itself on every row it emits, remote or staged. The remote
//! filter can therefore only ever return a superset of the answer, which the
//! local test narrows. The same holds for [`PushdownScanRequest::limit`]: it
//! is an early-stop hint, and the caller still enforces the cap.

use std::fmt;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_distsql::WarningCollector;
use tidb_expr::expression::Expression;
use tidb_txnkv::Key;

use crate::predicate_pushdown::ScanPredicate;
use crate::storage::StorageError;

/// Ordinary scans keep a bounded decoded read-ahead window ahead of their
/// consumer. Eight batches let cop decode overlap a hash-join consumer while
/// keeping the channel strictly bounded.
pub const DEFAULT_SCAN_READ_AHEAD_BATCHES: usize = 8;

/// Go's default five index-join inner workers materialize their tasks while
/// the current task is consumed. Sixteen 8K batches cover one default 25K
/// outer task's typical fanout while retaining a fixed memory ceiling.
pub const INDEX_JOIN_READ_AHEAD_BATCHES: usize = 16;

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

/// One function in a global partial aggregation. Unlike the older bounded
/// column-offset variants, Go permits any TiKV-pushable scalar expression as
/// an aggregate argument.
#[derive(Clone, Debug)]
pub struct PushdownGlobalAggregateFunction {
    /// The aggregate function implemented by TiKV.
    pub kind: PushdownAggregateKind,
    /// The expression evaluated for each qualifying scan row. `None` is
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
    /// A partial `COUNT(column)`; the root stage sums the per-region counts.
    Count {
        /// Offset in [`PushdownScanRequest::columns`].
        input_offset: usize,
        /// The partial count column returned by TiKV.
        output_type: FieldType,
    },
    /// A partial `SUM(column)`; the root stage sums the per-region sums.
    Sum {
        /// Offset in [`PushdownScanRequest::columns`].
        input_offset: usize,
        /// The partial sum column returned by TiKV.
        output_type: FieldType,
    },
    /// A partial hash aggregation with one group key and no aggregate
    /// functions, used by one-column `SELECT DISTINCT`.
    GroupBy {
        /// Offset in [`PushdownScanRequest::columns`].
        input_offset: usize,
        /// The group-key column returned by TiKV.
        output_type: FieldType,
    },
    /// A partial hash aggregation with one group key and one `SUM` function.
    /// The partial row is returned in TiKV's aggregation-schema order:
    /// aggregate result first, then the group key.
    GroupBySum {
        /// Offset of the group key in [`PushdownScanRequest::columns`].
        group_offset: usize,
        /// Offset of the summed column in [`PushdownScanRequest::columns`].
        sum_offset: usize,
        /// The partial sum column returned by TiKV.
        sum_type: FieldType,
        /// The group-key column returned by TiKV.
        group_type: FieldType,
    },
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
    /// A global partial HashAgg. TiKV returns exactly one row containing the
    /// function states, including for empty input.
    Global {
        /// Aggregate functions in physical output order.
        functions: Vec<PushdownGlobalAggregateFunction>,
    },
}

impl PushdownPartialAggregate {
    /// The aggregate input's scan-column offset.
    #[must_use]
    pub fn input_offset(&self) -> usize {
        match self {
            Self::Count { input_offset, .. }
            | Self::Sum { input_offset, .. }
            | Self::GroupBy { input_offset, .. } => *input_offset,
            Self::GroupBySum { group_offset, .. } => *group_offset,
            Self::Grouped {
                group_offsets,
                functions,
                ..
            } => group_offsets
                .first()
                .copied()
                .or_else(|| {
                    functions
                        .iter()
                        .flat_map(|function| {
                            function.input.iter().flat_map(expression_column_offsets)
                        })
                        .next()
                })
                .unwrap_or(0),
            Self::Global { functions } => functions
                .iter()
                .flat_map(|function| function.input.iter().flat_map(expression_column_offsets))
                .next()
                .unwrap_or(0),
        }
    }

    /// The columns the partial stage returns, in TiKV aggregation-schema
    /// order (aggregate functions followed by group keys).
    #[must_use]
    pub fn output_types(&self) -> Vec<FieldType> {
        match self {
            Self::Count { output_type, .. }
            | Self::Sum { output_type, .. }
            | Self::GroupBy { output_type, .. } => vec![output_type.clone()],
            Self::GroupBySum {
                sum_type,
                group_type,
                ..
            } => vec![sum_type.clone(), group_type.clone()],
            Self::Grouped {
                group_types,
                functions,
                ..
            } => functions
                .iter()
                .map(|function| function.output_type.clone())
                .chain(group_types.iter().cloned())
                .collect(),
            Self::Global { functions } => functions
                .iter()
                .map(|function| function.output_type.clone())
                .collect(),
        }
    }

    /// Every scan-column offset read by the partial stage.
    #[must_use]
    pub fn input_offsets(&self) -> Vec<usize> {
        match self {
            Self::Count { input_offset, .. }
            | Self::Sum { input_offset, .. }
            | Self::GroupBy { input_offset, .. } => vec![*input_offset],
            Self::GroupBySum {
                group_offset,
                sum_offset,
                ..
            } => vec![*group_offset, *sum_offset],
            Self::Grouped {
                group_offsets,
                functions,
                ..
            } => {
                let mut offsets = group_offsets
                    .iter()
                    .copied()
                    .chain(functions.iter().flat_map(|function| {
                        function.input.iter().flat_map(expression_column_offsets)
                    }))
                    .collect::<Vec<_>>();
                offsets.sort_unstable();
                offsets.dedup();
                offsets
            }
            Self::Global { functions } => {
                let mut offsets = functions
                    .iter()
                    .flat_map(|function| function.input.iter().flat_map(expression_column_offsets))
                    .collect::<Vec<_>>();
                offsets.sort_unstable();
                offsets.dedup();
                offsets
            }
        }
    }
}

fn expression_column_offsets(expression: &Expression) -> Vec<usize> {
    fn collect(expression: &Expression, offsets: &mut Vec<usize>) {
        match expression {
            Expression::Column(column) => {
                if let Ok(offset) = usize::try_from(column.index) {
                    offsets.push(offset);
                }
            }
            Expression::ScalarFunction(function) => {
                for argument in &function.args {
                    collect(argument, offsets);
                }
            }
            Expression::Constant(_) | Expression::CorrelatedColumn(_) => {}
        }
    }

    let mut offsets = Vec::new();
    collect(expression, &mut offsets);
    offsets
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
    /// staged overlay. `None` is a common-handle scan with no staged writes,
    /// where the caller consumes the remote rows directly.
    pub handle_index: Option<usize>,
    /// Stable column IDs forming a common row handle, in handle order.
    pub primary_column_ids: Vec<i64>,
    /// Common-handle columns stored as prefixes rather than whole values.
    pub primary_prefix_column_ids: Vec<i64>,
    /// The conjuncts the caller would like evaluated remotely. Best-effort:
    /// see the module doc.
    pub predicates: Vec<ScanPredicate>,
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
    /// such as TPC-H hash-join inputs. Cluster storage raises this for a
    /// staged overlay (whose merge is order-sensitive) and streamed partial
    /// aggregation.
    pub keep_order: bool,
    /// Decoded response batches the backend may retain ahead of this scan's
    /// consumer. The backend clamps this to its supported bounded maximum.
    pub read_ahead_batches: usize,
    /// The timestamp the remote scan must read at: the statement's own
    /// snapshot, filled in by the storage that owns it.
    pub snapshot_ts: u64,
    /// The scanned record ranges, as half-open `[start, end)` pairs in
    /// ascending key order.
    ///
    /// A whole-table scan is one range. A `TableRangeScan` over a clustered
    /// handle is one per handle range, which is what a coprocessor request
    /// carries natively (`Request.Ranges` is a list), so the narrowing
    /// reaches the region rather than being re-applied after the rows have
    /// already crossed the network.
    pub ranges: Vec<(Key, Key)>,
    /// The statement's coprocessor seam: `DAGRequest.flags` and the sink the
    /// warnings TiKV reports must land in. See [`PushdownStatementContext`].
    pub statement: PushdownStatementContext,
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
#[derive(Clone, Debug, Default)]
pub struct PushdownStatementContext {
    /// Go `StatementContext.PushDownFlags()`; see
    /// [`crate::StmtContext::push_down_flags`].
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
}

impl PushdownStatementContext {
    /// The coprocessor seam of one statement, taken from its context.
    #[must_use]
    pub fn from_stmt(ctx: &crate::StmtContext) -> Self {
        Self {
            push_down_flags: ctx.push_down_flags(),
            warnings: ctx.cop_warning_sink(),
            time_zone: ctx.session_zone(),
        }
    }
}

/// A lazily pulled stream of snapshot rows a backend served remotely.
pub trait PushdownRowStream: Send {
    /// The next row in record-key order, as the requested columns, or `None`
    /// at the end of the answer.
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError>;

    /// Whether this stream can transfer decoded columnar batches without
    /// first materializing every row as `Vec<Datum>`.
    fn supports_chunks(&self) -> bool {
        false
    }

    /// The next decoded columnar batch. Callers use this only after
    /// [`Self::supports_chunks`] returns true; row-only backends keep the
    /// existing [`Self::next_row`] contract.
    fn next_chunk(&mut self) -> Result<Option<Chunk>, StorageError> {
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

/// The capability a cluster backend is given so it can serve
/// [`PushdownScanRequest`]s: one coprocessor round trip per open scan.
///
/// It is a separate trait from the storage itself because the storage lives
/// in this crate while the transport does not: the production implementation
/// is injected from the crate that owns distsql.
pub trait PushdownScanner: fmt::Debug + Send + Sync {
    /// Opens one remote scan. An `Err` is a backend failure, not a refusal:
    /// a backend that cannot serve this request shape must say so by
    /// returning [`PushdownScannerError::Unsupported`], which makes the caller
    /// fall back to the byte-level cursor with no change in answer.
    fn open(
        &self,
        request: &PushdownScanRequest,
    ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError>;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;
    use tidb_expr::expression::ScalarFunction;
    use tidb_expr::schema::Schema;
    use tidb_txnkv::Key;

    use super::*;
    use crate::access_path::{IndexJoinLookupExec, LookupObject, LookupProbePart};
    use crate::cluster_storage::{
        ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
    };
    use crate::driver::{run_select_on, Catalog};
    use crate::executor::{Executor, ExecutorMeta};
    use crate::join::{IndexLookupPlan, IndexLookupSource, JoinExec, JoinKind};
    use crate::kv_table::{KvColumn, KvIndex, KvTable, TableHandle};
    use crate::mem_table::MemTableSourceExec;
    use crate::predicate_pushdown::{ScanComparisonOp, ScanPredicate};
    use crate::storage::{capture_storage_ops, MemTableStorage, TableStorage};

    /// The committed half of a cluster read, shared by the snapshot the
    /// session reads through and by the coprocessor below it.
    #[derive(Debug, Default)]
    struct MockSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl ClusterSnapshot for MockSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    /// A coprocessor standing in for TiKV: it reads the committed half only,
    /// evaluates the requested comparisons and cap there, and returns rows.
    ///
    /// It is deliberately blind to the session's staged writes -- that is
    /// precisely the property the merge has to repair, and a fake that quietly
    /// saw the buffer would prove nothing.
    #[derive(Debug)]
    struct FakeCoprocessor {
        snapshot: Arc<Mutex<MockSnapshot>>,
        columns: Vec<KvColumn>,
        /// The column a clustered integer primary key lives in, which the
        /// region's own decode needs: those bytes are in the record KEY, not
        /// the value.
        pk_handle_offset: Option<usize>,
        /// Rows that crossed the wire, across every scan.
        returned: Arc<AtomicU64>,
        /// Rows the coprocessor read before its own filter.
        scanned: Arc<AtomicU64>,
        /// Whether this backend evaluates the requested conjuncts at all.
        ///
        /// Turning it off is the MUTATION PROBE
        /// (`neutering_the_lowering_is_still_correct_and_the_receipt_notices`):
        /// a backend is allowed to lower none of the predicate, and the answer
        /// must be unchanged because the caller re-applies every conjunct. So
        /// the value assertions stay green and only the receipt moves, which
        /// is precisely why value assertions alone cannot see a lost pushdown.
        lower_predicates: std::sync::atomic::AtomicBool,
        /// When set, the backend accepts the request and then refuses on the
        /// first read, as the embedded coprocessor does for a scalar
        /// signature its evaluator has not grown yet.
        refuse_on_read: std::sync::atomic::AtomicBool,
        /// Every `DAGRequest.flags` value this backend was told, in request
        /// order. A region acts on these bits; a fake that ignored them would
        /// let the literal `0` back in unnoticed.
        requested_flags: Arc<Mutex<Vec<u64>>>,
        /// Every decoded-batch read-ahead bound this backend was told, in
        /// request order.
        requested_read_aheads: Arc<Mutex<Vec<usize>>>,
        /// Whether each remote scan was required to preserve key order.
        requested_keep_orders: Arc<Mutex<Vec<bool>>>,
        /// A warning the region reports on each request, standing in for
        /// TiKV's `SelectResponse.warnings`.
        region_warning: Mutex<Option<(i32, String)>>,
        opened: Arc<AtomicUsize>,
        minimum_opens_before_read: Arc<AtomicUsize>,
    }

    impl PushdownScanner for FakeCoprocessor {
        fn open(
            &self,
            request: &PushdownScanRequest,
        ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
            // The real transport refuses a request with no ranges before it
            // reaches a store: `metadata_region_ranges` in
            // `tidb_distsql::cop_paging` answers `missing_ranges`, because the
            // range list is what it turns into region tasks. A fake that
            // quietly returned no rows would hide exactly that.
            self.requested_flags
                .lock()
                .unwrap()
                .push(request.statement.push_down_flags);
            self.requested_read_aheads
                .lock()
                .unwrap()
                .push(request.read_ahead_batches);
            self.requested_keep_orders
                .lock()
                .unwrap()
                .push(request.keep_order);
            if let Some((code, message)) = self.region_warning.lock().unwrap().clone() {
                // Exactly what `tidb_distsql`'s `response_channel` does with
                // `SelectResponse.warnings`, into whatever collector it was
                // handed.
                request
                    .statement
                    .warnings
                    .append_tikv_warning(code, message);
            }
            if request.ranges.is_empty() {
                return Err(PushdownScannerError::Backend(StorageError::Backend(
                    "missing_ranges".to_owned(),
                )));
            }
            // The region's committed bytes for the requested key range.
            let mut store = MemTableStorage::new();
            {
                let mut snapshot = self.snapshot.lock().unwrap();
                for (start, end) in &request.ranges {
                    for (key, value) in snapshot.scan(start, end, None).unwrap() {
                        store.set(Key::from_bytes(key), value).unwrap();
                    }
                }
            }
            let mut table =
                KvTable::with_storage(request.table_id, self.columns.clone(), Box::new(store));
            if let Some(offset) = self.pk_handle_offset {
                table.set_pk_handle_offset(offset);
            }
            if !request.primary_column_ids.is_empty() {
                let offsets = request
                    .primary_column_ids
                    .iter()
                    .map(|id| {
                        self.columns
                            .iter()
                            .position(|column| column.id == *id)
                            .expect("a primary column belongs to the table")
                    })
                    .collect();
                table.set_common_handle_offsets(offsets);
            }
            if self.refuse_on_read.load(Ordering::Relaxed) {
                return Ok(Box::new(RefusingStream {
                    message: "coprocessor other error: scalar signature EqString \
                              waits on its distsql_builtin.go course"
                        .to_owned(),
                }));
            }
            // Every requested column that is one of the table's own; the
            // appended handle column is not, and is filled from the key.
            let appended_handle = request
                .handle_index
                .and_then(|index| request.columns.get(index))
                .is_some_and(|column| column.id == EXTRA_HANDLE_COLUMN_ID);
            let projected = if appended_handle {
                &request.columns[..request.columns.len() - 1]
            } else {
                &request.columns[..]
            };
            let keep: Vec<usize> = projected
                .iter()
                .map(|column| {
                    self.columns
                        .iter()
                        .position(|candidate| candidate.id == column.id)
                        .expect("a requested column belongs to the table")
                })
                .collect();
            let mut cursor = table
                .row_cursor_projected_with_context(
                    Some(&keep),
                    None,
                    &crate::RowDecodeContext::for_test_query_utc(),
                )
                .unwrap();
            let mut rows = Vec::new();
            while let Some((handle, mut row)) = cursor.next_row().unwrap() {
                self.scanned.fetch_add(1, Ordering::Relaxed);
                if self.lower_predicates.load(Ordering::Relaxed)
                    && !request
                        .predicates
                        .iter()
                        .all(|predicate| admits(predicate, &row) == Some(true))
                {
                    continue;
                }
                if appended_handle {
                    row.push(Datum::Int(handle.int_value().unwrap()));
                }
                rows.push(row);
                if request.limit.is_some_and(|cap| rows.len() as u64 >= cap) {
                    break;
                }
            }
            self.returned
                .fetch_add(rows.len() as u64, Ordering::Relaxed);
            self.opened.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FakeStream {
                rows: rows.into_iter(),
                returned: 0,
                opened: Arc::clone(&self.opened),
                minimum_opens_before_read: Arc::clone(&self.minimum_opens_before_read),
                first_read: true,
            }))
        }
    }

    /// Evaluates one description the way the coprocessor would, over the
    /// integer domain the lowering accepts, in MySQL's three-valued logic.
    /// `None` is SQL `UNKNOWN`; a shape outside the integer domain answers
    /// `Some(true)`, the "did not filter" answer a backend is allowed to give.
    fn admits(predicate: &ScanPredicate, row: &[Datum]) -> Option<bool> {
        match predicate {
            // A builtin call is outside this fake's integer domain, so it gives
            // the "did not filter" answer a backend is always allowed to give.
            ScanPredicate::Builtin(_) | ScanPredicate::ScalarIn { .. } => Some(true),
            ScanPredicate::Compare(comparison) => {
                let (Some(value), Datum::Int(literal)) = (
                    row.get(comparison.column_offset as usize),
                    comparison.literal.clone(),
                ) else {
                    return Some(true);
                };
                let value = match value {
                    Datum::Int(value) => *value,
                    Datum::Null => return None,
                    _ => return Some(true),
                };
                let (left, right) = if comparison.column_on_left {
                    (value, literal)
                } else {
                    (literal, value)
                };
                Some(match comparison.op {
                    ScanComparisonOp::Eq => left == right,
                    ScanComparisonOp::Ne => left != right,
                    ScanComparisonOp::Lt => left < right,
                    ScanComparisonOp::Le => left <= right,
                    ScanComparisonOp::Gt => left > right,
                    ScanComparisonOp::Ge => left >= right,
                })
            }
            ScanPredicate::ColumnCompare(comparison) => {
                let (Some(left), Some(right)) = (
                    row.get(comparison.left_offset as usize),
                    row.get(comparison.right_offset as usize),
                ) else {
                    return Some(true);
                };
                if *left == Datum::Null || *right == Datum::Null {
                    return None;
                }
                let ordering = tidb_expr::compare_datums(left, right).ok()?;
                Some(match comparison.op {
                    ScanComparisonOp::Eq => ordering.is_eq(),
                    ScanComparisonOp::Ne => !ordering.is_eq(),
                    ScanComparisonOp::Lt => ordering.is_lt(),
                    ScanComparisonOp::Le => ordering.is_le(),
                    ScanComparisonOp::Gt => ordering.is_gt(),
                    ScanComparisonOp::Ge => ordering.is_ge(),
                })
            }
            ScanPredicate::IsNull {
                column_offset,
                negated,
                ..
            } => {
                let is_null = row.get(*column_offset as usize) == Some(&Datum::Null);
                Some(is_null != *negated)
            }
            ScanPredicate::In {
                column_offset,
                literals,
                negated,
                ..
            } => {
                let Some(value) = row.get(*column_offset as usize) else {
                    return Some(true);
                };
                if *value == Datum::Null {
                    return None;
                }
                let found = literals.iter().any(|literal| literal == value);
                Some(found != *negated)
            }
            // MySQL's `AND`: FALSE dominates UNKNOWN, which dominates TRUE.
            ScanPredicate::And(branches) => {
                let mut unknown = false;
                for branch in branches {
                    match admits(branch, row) {
                        Some(false) => return Some(false),
                        Some(true) => {}
                        None => unknown = true,
                    }
                }
                (!unknown).then_some(true)
            }
            // MySQL's `OR`: TRUE dominates UNKNOWN, which dominates FALSE.
            ScanPredicate::Or(branches) => {
                let mut unknown = false;
                for branch in branches {
                    match admits(branch, row) {
                        Some(true) => return Some(true),
                        Some(false) => {}
                        None => unknown = true,
                    }
                }
                (!unknown).then_some(false)
            }
            ScanPredicate::Not(inner) => admits(inner, row).map(|value| !value),
        }
    }

    /// A backend that accepts the request and then REFUSES on the first
    /// read, which is how the embedded coprocessor reports a scalar
    /// signature it cannot evaluate: nothing is evaluated until a batch is
    /// asked for.
    struct RefusingStream {
        message: String,
    }

    impl PushdownRowStream for RefusingStream {
        fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
            Err(StorageError::Backend(self.message.clone()))
        }

        fn rows_returned(&self) -> u64 {
            0
        }

        fn close(&mut self) {}
    }

    struct FakeStream {
        rows: std::vec::IntoIter<Vec<Datum>>,
        returned: u64,
        opened: Arc<AtomicUsize>,
        minimum_opens_before_read: Arc<AtomicUsize>,
        first_read: bool,
    }

    impl PushdownRowStream for FakeStream {
        fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
            if self.first_read {
                self.first_read = false;
                let opened = self.opened.load(Ordering::SeqCst);
                let required = self.minimum_opens_before_read.load(Ordering::SeqCst);
                if opened < required {
                    return Err(StorageError::Backend(format!(
                        "read began after {opened} inner task opens; required {required}"
                    )));
                }
            }
            let row = self.rows.next();
            if row.is_some() {
                self.returned += 1;
            }
            Ok(row)
        }

        fn rows_returned(&self) -> u64 {
            self.returned
        }

        fn close(&mut self) {}
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    fn commit(buffer: &MutationBuffer, snapshot: &Arc<Mutex<MockSnapshot>>) {
        let mut snapshot = snapshot.lock().unwrap();
        for (key, value) in buffer.staged() {
            match value {
                Some(value) => snapshot.data.insert(key.as_bytes().to_vec(), value),
                None => snapshot.data.remove(key.as_bytes()),
            };
        }
        buffer.reset();
    }

    struct Fixture {
        table: KvTable,
        buffer: MutationBuffer,
        snapshot: Arc<Mutex<MockSnapshot>>,
        returned: Arc<AtomicU64>,
        scanned: Arc<AtomicU64>,
        scanner: Arc<FakeCoprocessor>,
    }

    /// A cluster-backed `t(a, b)` whose scans go through the coprocessor.
    fn fixture() -> Fixture {
        fixture_with(None)
    }

    /// [`fixture`] with `a` as the clustered integer handle, so a `WHERE` over
    /// it builds handle ranges.
    fn clustered_fixture() -> Fixture {
        fixture_with(Some(0))
    }

    /// A cluster fixture with (a, b) as the clustered common handle.
    fn common_handle_fixture() -> Fixture {
        let mut fixture = fixture_with(None);
        fixture.table.set_common_handle_offsets(vec![0, 1]);
        fixture.table.add_index(KvIndex {
            id: 1,
            name: "PRIMARY".to_owned(),
            comment: String::new(),
            unique: true,
            column_offsets: vec![0, 1],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
            visible: true,
            global: false,
        });
        fixture
    }

    fn fixture_with(pk_handle_offset: Option<usize>) -> Fixture {
        let snapshot = Arc::new(Mutex::new(MockSnapshot::default()));
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        let buffer = MutationBuffer::new();
        let columns = vec![column("a", 1), column("b", 2)];
        let returned = Arc::new(AtomicU64::new(0));
        let scanned = Arc::new(AtomicU64::new(0));
        let scanner = Arc::new(FakeCoprocessor {
            snapshot: Arc::clone(&snapshot),
            columns: columns.clone(),
            returned: Arc::clone(&returned),
            scanned: Arc::clone(&scanned),
            pk_handle_offset,
            lower_predicates: std::sync::atomic::AtomicBool::new(true),
            refuse_on_read: std::sync::atomic::AtomicBool::new(false),
            requested_flags: Arc::default(),
            requested_read_aheads: Arc::default(),
            requested_keep_orders: Arc::default(),
            region_warning: Mutex::new(None),
            opened: Arc::default(),
            minimum_opens_before_read: Arc::default(),
        });
        let storage = ClusterTableStorage::new(buffer.clone(), handle)
            .with_remote_scanner(Arc::clone(&scanner) as Arc<dyn PushdownScanner>);
        let mut table = KvTable::with_storage(91, columns, Box::new(storage));
        if let Some(offset) = pk_handle_offset {
            table.set_pk_handle_offset(offset);
        }
        Fixture {
            table,
            buffer,
            snapshot,
            returned,
            scanned,
            scanner,
        }
    }

    #[test]
    fn a_clean_common_handle_range_uses_the_coprocessor() {
        let mut fixture = common_handle_fixture();
        for row in [[1, 10], [1, 20], [2, 30]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.table.clear_dirty_content();

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let (rows, ops) = capture_storage_ops(|| {
            run_select_on("SELECT a, b FROM t WHERE a = 1 ORDER BY b", &catalog, &ctx).unwrap()
        });
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(1), Datum::Int(20)]
            ]
        );
        assert_eq!(ops.cop_scans, 1);
        assert_eq!(ops.cop_rows, 2);
        assert_eq!(ops.gets, 0);
    }

    #[test]
    fn an_index_join_sends_a_complete_common_handle_task_in_one_coprocessor_scan() {
        let mut fixture = common_handle_fixture();
        for row in [[1, 10], [1, 20], [2, 30], [3, 40]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.table.clear_dirty_content();

        let field_types = vec![
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
        ];
        let schema = Schema::new(
            field_types
                .iter()
                .enumerate()
                .map(|(offset, field_type)| {
                    let mut column = Column::new(offset as i64 + 1, field_type.clone());
                    column.index = offset as i64;
                    column
                })
                .collect(),
        );
        let ctx = crate::StmtContext::for_query();
        let mut source = IndexJoinLookupExec::new_with_context(
            ExecutorMeta::new(schema, 0, 32, 1024),
            fixture.table,
            LookupObject::CommonHandle,
            crate::RowDecodeContext::for_query(&ctx),
        );
        source.set_probe_parts(vec![LookupProbePart::Dynamic(0)]);
        // Go `buildKvRangesForIndexJoin` hands every lookup content in one
        // inner task to one table reader. This exceeds the former 4,096-range
        // Rust split, which opened multiple independent scan sessions and
        // made TPC-H q21 associate an incomplete inner set with the task.
        source.set_probes((1..=4097).map(|probe| vec![Datum::Int(probe)]).collect());

        let (rows, ops) = capture_storage_ops(|| {
            source.open().unwrap();
            let mut rows = Vec::new();
            let mut chunk = source.new_chunk();
            loop {
                source.next(&mut chunk).unwrap();
                if chunk.num_rows() == 0 {
                    break;
                }
                for row in 0..chunk.num_rows() {
                    rows.push(
                        field_types
                            .iter()
                            .enumerate()
                            .map(|(column, field_type)| {
                                chunk.get_row(row).get_datum(column, field_type)
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            }
            source.close().unwrap();
            rows
        });
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(1), Datum::Int(20)],
                vec![Datum::Int(2), Datum::Int(30)],
                vec![Datum::Int(3), Datum::Int(40)],
            ]
        );
        assert_eq!(ops.cop_scans, 1);
        assert_eq!(
            ops.scans, 1,
            "the fake coprocessor scans its combined in-memory range set once"
        );
    }

    /// Go's IndexHashJoin opens later inner tasks before it consumes the first
    /// one. The fake stream refuses its first read until two requests exist,
    /// making the overlap a deterministic protocol assertion rather than a
    /// timing benchmark.
    #[test]
    fn an_index_join_prefetches_later_inner_tasks_before_reading_the_first() {
        let mut fixture = common_handle_fixture();
        for row in [[1, 10], [2, 20]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.table.clear_dirty_content();
        fixture
            .scanner
            .minimum_opens_before_read
            .store(2, Ordering::SeqCst);

        let field = FieldType::new(FieldTypeCode::LongLong);
        let schema = |width: usize| {
            Schema::new(
                (0..width)
                    .map(|offset| {
                        let mut column = Column::new(offset as i64 + 1, field.clone());
                        column.index = offset as i64;
                        column
                    })
                    .collect(),
            )
        };
        let outer_rows = (0..1025)
            .map(|row| vec![Datum::Int(row % 2 + 1)])
            .collect::<Vec<_>>();
        let outer: Box<dyn Executor> = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(schema(1), 0, 32, 1024),
            outer_rows,
        ));
        let unused_inner: Box<dyn Executor> = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(schema(2), 0, 32, 1024),
            Vec::new(),
        ));
        let mut left = Column::new(1, field.clone());
        left.index = 0;
        let mut right = Column::new(2, field.clone());
        right.index = 1;
        let equality = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            field.clone(),
            vec![Expression::Column(left), Expression::Column(right)],
        ));
        let ctx = crate::StmtContext::for_query();
        let mut lookup = IndexJoinLookupExec::new_with_context(
            ExecutorMeta::new(schema(2), 0, 32, 1024),
            fixture.table,
            LookupObject::CommonHandle,
            crate::RowDecodeContext::for_query(&ctx),
        );
        lookup.set_probe_parts(vec![LookupProbePart::Dynamic(0)]);
        let mut join = JoinExec::new(
            ExecutorMeta::new(schema(1), 0, 32, 1024),
            JoinKind::Semi,
            vec![equality],
            outer,
            unused_inner,
            ctx.clone(),
            ctx.statement_memory(),
        );
        join.set_index_lookup_plan(IndexLookupPlan {
            lookup_is_left: false,
            probe_keys: vec![0],
            source: IndexLookupSource::Leaf(lookup),
            aggregation: None,
            aggregation_stream_ordered: false,
            outer_not_null: Vec::new(),
            inner_not_null: Vec::new(),
        });

        join.open().unwrap();
        let mut rows = 0;
        let mut chunk = join.new_chunk();
        loop {
            join.next(&mut chunk).unwrap();
            if chunk.num_rows() == 0 {
                break;
            }
            rows += chunk.num_rows();
        }
        join.close().unwrap();
        assert_eq!(rows, 1025);
        assert_eq!(fixture.scanner.opened.load(Ordering::SeqCst), 2);
        assert_eq!(
            *fixture.scanner.requested_read_aheads.lock().unwrap(),
            vec![INDEX_JOIN_READ_AHEAD_BATCHES; 2]
        );
    }

    fn catalog_of(table: KvTable) -> Catalog {
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        catalog
    }

    /// Go carries `PhysicalTableScan.KeepOrder` through
    /// `TableReaderExecutor.keepOrder` into every DistSQL request. A merge
    /// join depends on both child streams having that exact property; merely
    /// printing `keep order:true` while issuing unordered requests can drop
    /// matches when region responses arrive out of key order.
    #[test]
    fn a_merge_join_sends_keep_order_to_both_remote_table_scans() {
        let mut fixture = clustered_fixture();
        for row in [[1, 10], [2, 20], [3, 30]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.table.clear_dirty_content();
        let scanner = Arc::clone(&fixture.scanner);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        let rows = run_select_on(
            "SELECT /*+ TIDB_SMJ(t1, t2) */ t1.a FROM t t1 JOIN t t2 ON t1.a=t2.a",
            &catalog,
            &ctx,
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(
            *scanner.requested_keep_orders.lock().unwrap(),
            vec![true, true],
            "both MergeJoin children must ask DistSQL to preserve record-key order",
        );
    }

    /// `DAGRequest.flags` reaches the region from the STATEMENT, through the
    /// production request builder, and is not the literal `0` it used to be.
    ///
    /// `0` is TiKV's strictest branch: no truncation tolerated, no
    /// zero-in-date tolerated, division by zero an error. A `SELECT` that
    /// TiDB answers with a value plus a 1292 warning made the region fail the
    /// whole request instead.
    ///
    /// The expected value is COMPUTED from the same statement context the
    /// query ran under, not written down, so a change to the derivation moves
    /// both sides together; the second assertion is what keeps that from being
    /// vacuous if the derivation ever collapses to zero.
    #[test]
    fn the_statements_push_down_flags_reach_the_coprocessor_request() {
        let mut fixture = fixture();
        fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);
        let scanner = Arc::clone(&fixture.scanner);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        run_select_on("SELECT a FROM t", &catalog, &ctx).unwrap();

        let flags = scanner.requested_flags.lock().unwrap().clone();
        assert_eq!(flags, vec![ctx.push_down_flags()]);
        assert_ne!(
            flags[0], 0,
            "flags 0 is TiKV's strictest branch, which is the bug this pins"
        );
        assert_eq!(
            flags[0], 482,
            "a plain SELECT: TruncateAsWarning 2 | InSelectStmt 32 | \
             OverflowAsWarning 64 | IgnoreZeroInDate 128 | \
             DividedByZeroAsWarning 256"
        );
    }

    /// A different statement class sends different flags: the derivation's
    /// INPUTS are threaded, not one statement's output.
    ///
    /// Go's `*ast.InsertStmt` arm runs `GetTypeFlagsForInsert`, which under a
    /// strict `sql_mode` clears `TruncateAsWarning` and `IgnoreZeroInDate` and
    /// resolves `ErrGroupDividedByZero` to `LevelError` -- so the read half of
    /// a strict `INSERT ... SELECT` tells the region to FAIL on exactly the
    /// values a plain `SELECT` tells it to warn about.
    #[test]
    fn a_strict_insert_select_sends_different_flags_than_a_plain_select() {
        let mut fixture = fixture();
        fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);
        let scanner = Arc::clone(&fixture.scanner);

        let catalog = catalog_of(fixture.table);
        // `for_dml(error_for_division_by_zero, strict, ignore_err)` with the INSERT arm's
        // statement class: the read half of `INSERT INTO u SELECT a FROM t`.
        let ctx = crate::StmtContext::for_dml(true, true, false)
            // TiDB's default `sql_mode` bits that `GetTypeFlagsForInsert`
            // reads; without them `IgnoreZeroInDate` is true and the flags
            // would be 136 rather than 8.
            .with_date_modes(tidb_datatype::DateModes {
                no_zero_date: true,
                no_zero_in_date: true,
                allow_invalid_dates: false,
            })
            .with_statement_class(crate::StatementClass::Insert);
        run_select_on("SELECT a FROM t", &catalog, &ctx).unwrap();

        let flags = scanner.requested_flags.lock().unwrap().clone();
        assert_eq!(flags, vec![ctx.push_down_flags()]);
        assert_eq!(
            flags[0],
            crate::statement_pushdown::FLAG_IN_INSERT_STMT,
            "a strict INSERT tolerates nothing, and says only which statement \
             it is"
        );
        assert_ne!(
            flags[0],
            crate::StmtContext::for_query().push_down_flags(),
            "hardcoding one statement's flags would make these equal"
        );
    }

    /// A warning TiKV reports lands in the STATEMENT'S buffer, which is what
    /// `SHOW WARNINGS` and the OK packet's count read.
    ///
    /// Every production site used to build a fresh `WarningCollector` here, so
    /// `response_channel` appended TiKV's warnings correctly, in Go's order,
    /// into a buffer that was then dropped.
    ///
    /// DEFERRED LIVE CHECK: only a real TiKV can produce the warning for the
    /// audit's named case, `SELECT ROUND(s) FROM t` with `s = '12abc'`, where
    /// TiDB reports the truncated value plus 1292. The fake below stands in
    /// for the region's `SelectResponse.warnings` field; that the region fills
    /// it under flags 482 is what a playground run must confirm.
    #[test]
    fn a_warning_the_region_reports_lands_in_the_statements_buffer() {
        let mut fixture = fixture();
        fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);
        let scanner = Arc::clone(&fixture.scanner);
        *scanner.region_warning.lock().unwrap() =
            Some((1292, "Truncated incorrect DOUBLE value: '12abc'".to_owned()));

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        run_select_on("SELECT a FROM t", &catalog, &ctx).unwrap();

        assert_eq!(
            ctx.take_warnings(),
            vec![(
                tidb_distsql::WarningLevel::Warning,
                1292u16,
                "Truncated incorrect DOUBLE value: '12abc'".to_owned()
            )],
            "the sink the request carried is the statement's own, and keeps \
             the level TiKV's warning was collected under"
        );
    }

    /// The wire win: with a predicate lowered into the request, the rows that
    /// cross the network are the qualifying ones and not the relation.
    #[test]
    fn a_pushed_predicate_keeps_the_rejected_rows_off_the_wire() {
        let mut fixture = fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);
        fixture.scanned.store(0, Ordering::Relaxed);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let rows = run_select_on("SELECT a FROM t WHERE a > 97", &catalog, &ctx).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(98)],
                vec![Datum::Int(99)],
                vec![Datum::Int(100)]
            ]
        );
        assert_eq!(
            fixture.scanned.load(Ordering::Relaxed),
            100,
            "the coprocessor read the relation, as a full scan must"
        );
        assert_eq!(
            fixture.returned.load(Ordering::Relaxed),
            3,
            "but only the qualifying rows crossed the network"
        );
    }

    /// A clustered handle whose `WHERE` admits NO handle at all reads nothing
    /// -- it does not send a coprocessor request with no ranges.
    ///
    /// Both halves matter. `id > 97 AND id < 97` and a NULL bound each build an
    /// EMPTY range list, which the local cursor states exactly by opening no
    /// iterator; the coprocessor's `Ranges` list cannot state it, and the
    /// transport rejects the request instead (`missing_ranges`). The control
    /// below keeps the ordinary narrowed range on the coprocessor, so this is
    /// not "stop pushing ranges down".
    #[test]
    fn an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request() {
        let mut fixture = clustered_fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a > 97 AND a < 97", &catalog, &ctx).unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM t WHERE a BETWEEN NULL AND NULL",
                &catalog,
                &ctx
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            fixture.returned.load(Ordering::Relaxed),
            0,
            "no row crossed the network for a range that admits none"
        );

        // Control: a range that DOES admit rows still reaches the coprocessor.
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a BETWEEN 98 AND 100", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(98)],
                vec![Datum::Int(99)],
                vec![Datum::Int(100)]
            ]
        );
        assert_eq!(
            fixture.returned.load(Ordering::Relaxed),
            3,
            "the narrowed range is still served remotely"
        );
    }

    /// A cap travels with the request when nothing is staged, so the
    /// coprocessor stops reading instead of returning the relation.
    #[test]
    fn a_pushed_limit_stops_the_remote_scan() {
        let mut fixture = fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.scanned.store(0, Ordering::Relaxed);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let rows = run_select_on("SELECT a FROM t LIMIT 4", &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(
            fixture.scanned.load(Ordering::Relaxed),
            4,
            "the cap reached the coprocessor, which stopped there"
        );
    }

    /// The correctness core. A coprocessor answers from the snapshot, so the
    /// transaction's own staged writes must be merged back in and filtered by
    /// the same predicate: this is the remote twin of the byte-level test in
    /// `crate::predicate_pushdown`, and it must produce the identical answer.
    #[test]
    fn staged_rows_survive_the_remote_scan_and_are_filtered_by_the_same_predicate() {
        let mut fixture = fixture();
        let committed_low = fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        fixture
            .table
            .insert_row(&[Datum::Int(9), Datum::Int(90)], &tidb_expr::NoColumns)
            .unwrap();
        let committed_moved = fixture
            .table
            .insert_row(&[Datum::Int(2), Datum::Int(20)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);

        // One open transaction stages all four shapes.
        fixture
            .table
            .insert_row(&[Datum::Int(7), Datum::Int(70)], &tidb_expr::NoColumns)
            .unwrap();
        fixture
            .table
            .insert_row(&[Datum::Int(3), Datum::Int(30)], &tidb_expr::NoColumns)
            .unwrap();
        fixture
            .table
            .update_row_with_context(
                &committed_moved,
                &[Datum::Int(8), Datum::Int(80)],
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        fixture
            .table
            .delete_row_with_context(
                &committed_low,
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        assert!(!fixture.buffer.is_empty(), "the writes are staged");

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a, b FROM t WHERE a > 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(7), Datum::Int(70)],
                vec![Datum::Int(8), Datum::Int(80)],
                vec![Datum::Int(9), Datum::Int(90)],
            ],
            "a staged INSERT and a staged UPDATE that satisfy the predicate are \
             kept, and the staged row that does not is dropped"
        );
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a < 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(3)]],
            "the staged DELETE hid the committed row the coprocessor still \
             returns, and the updated row's old value went with it"
        );
        assert_eq!(
            run_select_on("SELECT a FROM t ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(8)],
                vec![Datum::Int(9)],
            ],
            "and the merged relation itself is the union-scan answer"
        );
    }

    /// A cap must not travel while writes are staged: the coprocessor's first
    /// `n` snapshot rows are the wrong prefix once a staged delete uncovers a
    /// row past them.
    #[test]
    fn a_cap_does_not_travel_while_writes_are_staged() {
        let mut fixture = fixture();
        for a in 1..=6 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        // A cap of three applied at the coprocessor would have returned rows
        // 1..3, of which only one survives the overlay.
        fixture
            .table
            .delete_row_with_context(
                &TableHandle::Int(1),
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        fixture
            .table
            .delete_row_with_context(
                &TableHandle::Int(2),
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a FROM t LIMIT 3", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(4)],
                vec![Datum::Int(5)]
            ],
            "the cap is enforced on the merged stream, not on the snapshot's prefix"
        );
    }

    /// A relation with the shapes an aggregate can get wrong: a NULL in the
    /// summed column, and a value no predicate below admits.
    fn aggregate_fixture() -> Fixture {
        let mut fixture = fixture();
        for a in 1..=100 {
            // `b` is NULL for the three rows a `WHERE a > 97` keeps, plus one
            // it rejects, so COUNT(*), COUNT(b) and SUM(b) all differ.
            let b = if a > 96 {
                Datum::Null
            } else {
                Datum::Int(a * 10)
            };
            fixture
                .table
                .insert_row(&[Datum::Int(a), b], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);
        fixture
    }

    /// One statement's answer and the rows its coprocessor scans sent.
    fn run_counting(
        sql: &str,
        catalog: &Catalog,
        ctx: &crate::StmtContext,
    ) -> (Vec<Vec<Datum>>, u64) {
        let (rows, ops) = capture_storage_ops(|| run_select_on(sql, catalog, ctx).unwrap());
        (rows, ops.cop_rows)
    }

    /// An aggregate over a filtered table must not forfeit the pushed
    /// predicate.
    ///
    /// The aggregate pipeline returned from the driver BEFORE the predicate
    /// was ever offered to the scan, so `SELECT COUNT(*) ... WHERE a > 97`
    /// dragged all hundred rows across the network to return one -- while the
    /// identical `SELECT a ... WHERE a > 97` sent three. Nothing about the
    /// ANSWER differed, which is why only the receipt could see it. Go pushes
    /// the `Selection` into the `DataSource` in `rule_predicate_push_down`
    /// whether or not an `Aggregation` sits above it.
    #[test]
    fn an_aggregate_does_not_forfeit_the_pushed_predicate() {
        let fixture = aggregate_fixture();
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        // The control: the same predicate WITHOUT an aggregate has always
        // narrowed the wire, so a difference below is the aggregate's doing
        // and not the predicate's pushability.
        let (rows, wire) = run_counting("SELECT a FROM t WHERE a > 97", &catalog, &ctx);
        assert_eq!(rows.len(), 3);
        assert_eq!(wire, 3);

        let (rows, wire) = run_counting(
            "SELECT COUNT(*), COUNT(b), SUM(b) FROM t WHERE a > 97",
            &catalog,
            &ctx,
        );
        assert_eq!(
            rows,
            vec![vec![Datum::Int(3), Datum::Int(0), Datum::Null]],
            "COUNT(*) counts the NULL rows COUNT(b) skips, and a SUM over no \
             non-NULL value is NULL rather than zero"
        );
        assert_eq!(wire, 3, "and only those three rows crossed the network");

        let (rows, wire) = run_counting(
            "SELECT a, COUNT(*) FROM t WHERE a > 97 GROUP BY a ORDER BY a",
            &catalog,
            &ctx,
        );
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(98), Datum::Int(1)],
                vec![Datum::Int(99), Datum::Int(1)],
                vec![Datum::Int(100), Datum::Int(1)],
            ]
        );
        assert_eq!(wire, 3, "GROUP BY does not forfeit it either");
    }

    /// The values a filtered aggregate must still produce, over the group the
    /// predicate leaves EMPTY and over the NULLs it keeps.
    #[test]
    fn a_pushed_predicate_does_not_move_an_aggregate_value() {
        let fixture = aggregate_fixture();
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        // An empty input: SUM is NULL, COUNT is 0 -- and AVG is NULL, which is
        // what makes it something other than SUM/COUNT.
        let (rows, wire) = run_counting(
            "SELECT SUM(b), COUNT(*), COUNT(b), AVG(b) FROM t WHERE a > 1000",
            &catalog,
            &ctx,
        );
        assert_eq!(
            rows,
            vec![vec![Datum::Null, Datum::Int(0), Datum::Int(0), Datum::Null]]
        );
        assert_eq!(wire, 0, "a predicate that admits no row sends no row back");

        // The NULLs the predicate KEEPS, counted the two different ways.
        let (rows, wire) = run_counting(
            "SELECT COUNT(*), COUNT(b), SUM(b) FROM t WHERE b IS NULL",
            &catalog,
            &ctx,
        );
        assert_eq!(rows, vec![vec![Datum::Int(4), Datum::Int(0), Datum::Null]]);
        assert_eq!(wire, 4, "IS NULL is lowered, so only the NULL rows crossed");

        // And the whole relation, as the reference the narrowed answers must
        // agree with.
        let (rows, wire) = run_counting("SELECT COUNT(*), COUNT(b), SUM(b) FROM t", &catalog, &ctx);
        assert_eq!(
            rows,
            vec![vec![
                Datum::Int(100),
                Datum::Int(96),
                // MySQL's `SUM` over an integer column is DECIMAL, not
                // integer, and it keeps the argument's scale -- so the type is
                // pinned here alongside the value.
                Datum::Decimal(tidb_datatype::Decimal::from_literal("46560")),
            ]],
            "the sum of 10..960 by tens, over the 96 non-NULL rows"
        );
        assert_eq!(
            wire, 100,
            "an unfiltered aggregate still drags the relation: the AGGREGATE \
             is not pushed down, only the predicate below it"
        );
    }

    /// The receipt must not be BLIND on the cluster backend: a point get
    /// through it is one key lookup, no scan and no coprocessor request.
    ///
    /// Only the in-process backend used to be counted, so every cluster read
    /// reported zero of everything -- a probe that answers "no reads" for a
    /// statement that read a row is worse than none, because a test that
    /// pins an access path against it passes for the wrong reason.
    #[test]
    fn a_cluster_point_get_is_one_key_lookup_and_no_coprocessor_request() {
        let mut fixture = clustered_fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        let (rows, ops) =
            capture_storage_ops(|| run_select_on("SELECT b FROM t WHERE a = 5", &catalog, &ctx));
        assert_eq!(rows.unwrap(), vec![vec![Datum::Int(50)]]);
        assert_eq!(
            ops,
            crate::storage::StorageOps {
                gets: 1,
                scans: 0,
                cop_scans: 0,
                cop_rows: 0,
            },
            "the handle is known, so the plan reads the one key and opens \
             neither an iterator nor a coprocessor scan"
        );

        // Control: the same table read as a RANGE does send a coprocessor
        // request, so the zeros above are the point plan's and not a probe
        // that cannot see this backend at all.
        let (_, ops) = capture_storage_ops(|| {
            run_select_on("SELECT b FROM t WHERE a BETWEEN 5 AND 7", &catalog, &ctx)
        });
        assert_eq!(ops.gets, 0);
        assert_eq!((ops.cop_scans, ops.cop_rows), (1, 3));
    }

    /// MUTATION PROBE, with its control. A backend that lowers NONE of the
    /// predicate is still exactly correct -- the caller re-applies every
    /// conjunct -- so the value assertions stay green while the wire count
    /// jumps back to the relation. That asymmetry is the reason the receipt
    /// has to exist: value tests structurally cannot see a lost pushdown.
    #[test]
    fn neutering_the_lowering_is_still_correct_and_the_receipt_notices() {
        let fixture = aggregate_fixture();
        let scanner = Arc::clone(&fixture.scanner);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let sql = "SELECT COUNT(*), COUNT(b), SUM(b) FROM t WHERE a > 97";

        let (lowered, lowered_wire) = run_counting(sql, &catalog, &ctx);
        assert_eq!(lowered_wire, 3);

        scanner
            .lower_predicates
            .store(false, std::sync::atomic::Ordering::Relaxed);
        let (neutered, neutered_wire) = run_counting(sql, &catalog, &ctx);
        assert_eq!(
            neutered, lowered,
            "a backend that lowered nothing answers identically"
        );
        assert_eq!(
            neutered_wire, 100,
            "but the receipt sees the relation cross the network"
        );
    }

    /// A backend that REFUSES the pushed-down shape must not fail the
    /// query. `PushdownScannerError::Unsupported` states the contract --
    /// "never a wrong answer, only a slower one" -- and the embedded
    /// coprocessor exercises it for real by refusing a scalar signature its
    /// evaluator has not grown yet (a string comparison, until
    /// distsql_builtin lands). The refusal reaches the caller on the FIRST
    /// READ rather than at open, because nothing is evaluated until a batch
    /// is asked for, so the scan falls back to the byte-level cursor there.
    #[test]
    fn a_refused_pushdown_falls_back_to_the_local_scan() {
        let fixture = aggregate_fixture();
        let scanner = Arc::clone(&fixture.scanner);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let sql = "SELECT COUNT(*), COUNT(b), SUM(b) FROM t WHERE a > 97";

        let (pushed, _) = run_counting(sql, &catalog, &ctx);

        scanner
            .refuse_on_read
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let (refused, refused_wire) = run_counting(sql, &catalog, &ctx);
        assert_eq!(
            refused, pushed,
            "a refused pushdown answers exactly what the accepted one did"
        );
        assert_eq!(
            refused_wire, 0,
            "and nothing crossed the wire, because the local cursor served it"
        );
    }

    /// The remote path may only ever narrow: with the coprocessor lowering
    /// nothing of the predicate, the local test still answers exactly.
    #[test]
    fn an_unlowered_predicate_is_still_answered_exactly() {
        let mut fixture = fixture();
        for a in 1..=20 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE b + 1 > 190", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(19)], vec![Datum::Int(20)]],
            "the arithmetic conjunct is residual, so it runs above the scan"
        );
    }
}
