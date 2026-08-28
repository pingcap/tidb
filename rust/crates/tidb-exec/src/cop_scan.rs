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

//! The wide SQL path's coprocessor-backed base-table scan: the production
//! implementation of [`PushdownScanner`].
//!
//! # What this closes
//!
//! The wide path reads its tables through
//! [`ClusterTableStorage`](tidb_executor::cluster_storage::ClusterTableStorage),
//! whose `iter` scans the record range on the session's transaction thread
//! and ships every key/value pair of it to the client. The predicate, the row
//! cap and the column projection are then applied here, after the bytes have
//! already crossed the network. This module makes the same scan a real
//! coprocessor request: TiKV evaluates the Selection and the Limit at the
//! region and returns rows, so a `WHERE` that rejects most of a table now
//! rejects it *before* the network.
//!
//! Everything above is already built. [`crate::dag_request`] lowers the scan,
//! the Selection and the cap into a `DAGRequest`;
//! [`crate::wide_scan_selection`] converts the wide path's pushed conjuncts
//! into that Selection's condition shape; `tidb-distsql` owns ranges, region
//! tasks, dispatch, retry and response decoding. This module is the seam
//! between them and the storage: it carries the request onto the wire and
//! streams the response back as rows.
//!
//! # The two properties that make it safe
//!
//! * **It answers from the statement's own snapshot.** The request carries
//!   [`PushdownScanRequest::snapshot_ts`], which the storage filled in from
//!   the snapshot the statement is bound to. A scan that read at any other
//!   timestamp would not be repeatable read.
//! * **It never has the last word.** The scan source applies the pushed
//!   conjuncts and the cap to every row it emits, and merges the session's
//!   staged buffer on top (see [`tidb_executor::remote_scan`]). So this
//!   module may lower all, some, or none of the predicate and the answer is
//!   the same; only the number of rows on the wire changes.
//!
//! # What it refuses
//!
//! A column whose coprocessor descriptor this module cannot build faithfully
//! -- anything outside the signed and unsigned integer family
//! (`BIGINT`/`INT`/`MEDIUMINT`/`SMALLINT`/`TINYINT`) and the character-string
//! family (`VARCHAR`/`CHAR`/the `BLOB`s and their `BINARY` spellings) today --
//! makes the whole scan fall back to the byte-level cursor. Note that this is
//! a *projection* gate, separate from the predicate lowering's own type gate:
//! a table with one `DECIMAL` column in the `SELECT` list cannot be scanned
//! remotely at all, however pushable its `WHERE` is. The refusal is
//! [`PushdownScannerError::Unsupported`], which the storage turns into "use
//! `iter`", so a refused shape is slower and never wrong.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::{
    CancelHandle, DistSqlContext, EncodeType, ExecutorKind, ExecutorShape, InjectedQueryRuntime,
    QueryResultContext, QueryTransport, RequestBuilder, RequestEnvelope, SelectInput,
    SelectResponseIter, WarningCollector,
};
use tidb_executor::predicate_pushdown::ScanPredicate;
use tidb_executor::remote_scan::{
    PushdownAggregateKind, PushdownPartialAggregate, PushdownRowStream, PushdownScanColumn,
    PushdownScanRequest, PushdownScanner, PushdownScannerError, EXTRA_HANDLE_COLUMN_ID,
};
use tidb_executor::storage::StorageError;
use tidb_planner::cardinality::live_index_optimizer::{IndexPointStatistics, LiveIndexCandidate};
use tidb_planner::physical_index_scan::PhysicalIndexScanPlan;
use tidb_planner::physical_table_scan::PhysicalTableScanPlan;
use tidb_planner::tikv_scan_spec::{
    ResolvedIndexDescriptor, ScanColumnInfo, TiKvIndexScanSpec, TiKvTableScanSpec,
};
use tidb_proto::tipb::{
    ByItem, ExecType, Executor as PbExecutor, Expr, ExprType, ScalarFuncSig, TopN,
};
use tidb_txnkv::KeyRange;

use crate::dag_request::{
    construct_aggregate_read_only_dag_req_with_conditions,
    construct_capped_read_only_dag_req_with_conditions,
    construct_grouped_aggregate_read_only_dag_req_with_conditions, DagRequestContext, TiKvScanPlan,
};

enum LoweredAggregate {
    Global {
        functions: Vec<Expr>,
        streamed: bool,
    },
    Grouped {
        functions: Vec<Expr>,
        group_by: Vec<Expr>,
        streamed: bool,
    },
}
use crate::real_tikv_read::RealTiKvSessionTransportFactory;
use crate::wide_scan_selection::{accepts, wide_scan_selection_conditions};

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: i32 = 1;
/// Go `mysql.PriKeyFlag`.
const PRI_KEY_FLAG: i32 = 2;
/// Go `charset.CollationBin`, the coprocessor collation of a numeric column.
const BINARY_COLLATION_ID: i32 = 63;
/// Go `mysql.TypeLonglong`.
const MYSQL_TYPE_LONGLONG: i32 = 8;
/// Go `mysql.TypeLong`.
const MYSQL_TYPE_LONG: i32 = 3;
/// Go `mysql.TypeInt24`.
const MYSQL_TYPE_INT24: i32 = 9;
/// Go `mysql.TypeShort`.
const MYSQL_TYPE_SHORT: i32 = 2;
/// Go `mysql.TypeTiny`.
const MYSQL_TYPE_TINY: i32 = 1;

/// Keep TiKV's small type chunks behind one decoder pull. Index scans use
/// Go's MaxChunkSize: IndexLookUp's `readFromChunk`/`extractTaskHandles`
/// relies on those response boundaries to grow table tasks without splitting
/// a typed chunk.
const BATCH_ROWS: usize = 32768;
const INDEX_BATCH_ROWS: usize = 1024;

/// One coprocessor scan capability for a node's sessions.
///
/// The shared scanner opens a query-local transport only after a connection
/// worker asks for a scan. The returned stream owns the lazy response on that
/// same worker: neither the transport nor the iterator crosses threads.
pub struct CopScanSource<F> {
    factory: Arc<F>,
    /// Rows this node has received from coprocessor scans, for the receipt a
    /// live proof reads.
    rows_returned: Arc<AtomicU64>,
    /// Scans this node served remotely, against the ones it refused.
    scans_served: Arc<AtomicU64>,
    scans_refused: Arc<AtomicU64>,
    /// The executor list of each DAG this node sent, read back from the
    /// encoded request. This is the receipt that the Selection and the cap
    /// really travelled, rather than a claim that they did.
    requests: Arc<Mutex<Vec<String>>>,
    /// The same table lookup opens several region scans with identical
    /// columns and predicates. Keep the lowered Selection for that request
    /// shape so a large `IN` list is encoded once per node instead of once per
    /// region window.
    selection_cache: Arc<Mutex<Option<SelectionCache>>>,
}

#[derive(Clone)]
struct SelectionCache {
    columns: Vec<ScanColumnInfo>,
    predicates: Vec<ScanPredicate>,
    lowered: Vec<ScanPredicate>,
    conditions: Vec<Expr>,
}

impl<F> fmt::Debug for CopScanSource<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CopScanSource")
            .field("rows_returned", &self.rows_returned.load(Ordering::Relaxed))
            .field("scans_served", &self.scans_served.load(Ordering::Relaxed))
            .field("scans_refused", &self.scans_refused.load(Ordering::Relaxed))
            .finish()
    }
}

/// What a node's coprocessor scans have done so far, as plain counters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopScanStats {
    /// Rows the coprocessor sent to this node.
    pub rows_returned: u64,
    /// Scans served remotely.
    pub scans_served: u64,
    /// Scans refused, which fell back to the byte-level cursor.
    pub scans_refused: u64,
    /// One line per sent request, naming its DAG executors.
    pub requests: Vec<String>,
}

impl<F> CopScanSource<F> {
    /// Builds the capability over an already-running transport factory.
    ///
    /// The scanner holds no `time_zone` of its own: one object serves every
    /// connection of a node, so a zone here would be a process-wide constant
    /// no `SET time_zone` could correct. Each request carries the zone of the
    /// statement that issued it
    /// (`tidb_executor::PushdownStatementContext::time_zone`), which is where
    /// Go reads it from too (`ConstructDAGReq` -> `SessionVars.Location()`).
    #[must_use]
    pub fn new(factory: Arc<F>) -> Self {
        Self {
            factory,
            rows_returned: Arc::new(AtomicU64::new(0)),
            scans_served: Arc::new(AtomicU64::new(0)),
            scans_refused: Arc::new(AtomicU64::new(0)),
            requests: Arc::new(Mutex::new(Vec::new())),
            selection_cache: Arc::new(Mutex::new(None)),
        }
    }

    /// The node's live coprocessor-scan counters.
    #[must_use]
    pub fn stats(&self) -> CopScanStats {
        CopScanStats {
            rows_returned: self.rows_returned.load(Ordering::Relaxed),
            scans_served: self.scans_served.load(Ordering::Relaxed),
            scans_refused: self.scans_refused.load(Ordering::Relaxed),
            requests: self
                .requests
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone(),
        }
    }
}

impl<F> PushdownScanner for CopScanSource<F>
where
    F: RealTiKvSessionTransportFactory + 'static,
    <F::Transport as QueryTransport>::Response: 'static,
{
    fn open(
        &self,
        request: &PushdownScanRequest,
    ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
        let refuse = |reason: &str| {
            self.scans_refused.fetch_add(1, Ordering::Relaxed);
            PushdownScannerError::Unsupported(reason.to_owned())
        };
        if request.snapshot_ts == 0 {
            return Err(refuse("the statement's snapshot has no timestamp"));
        }
        // Every request shape this lowering does not build into the DAG must
        // be refused BY NAME, never ignored: the caller assumes an accepted
        // request was answered whole, so silently serving raw table rows for
        // an aggregate or index request is wrong data, not degraded service.
        // A refused `topn` or cap is different -- the contract names those
        // best-effort and the caller retains the local stage either way. No
        // index shape is special-cased either: an aggregate-carrying index
        // request composes [IndexScan, Selection?, Aggregation] exactly as Go
        // `ConstructDAGReq` does, with the same all-or-nothing Selection gate
        // every aggregate arm states, and an AGGREGATE-LESS index request
        // lowers through the `TiKvIndexScanSpec` branch below with its
        // declared direction (`spec.desc`) -- routing it here instead of a
        // bespoke refusal is what lets a descending keep-order lookup answer
        // n rows without materializing the whole bounded range locally.
        let columns = request
            .columns
            .iter()
            .map(scan_column)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| refuse("a column has no bounded coprocessor descriptor"))?;
        let mut field_types: Vec<FieldType> = request
            .columns
            .iter()
            .map(|column| column.field_type.clone())
            .collect();

        // Every conjunct this lowering accepts travels; the rest simply stay
        // behind, because the scan source tests all of them locally anyway.
        // Table lookup windows repeat this exact shape, often with a large
        // string `IN` list. Reuse both the admission result and its protobuf
        // tree across those opens; the cache key includes the full descriptors
        // and source predicates, so it cannot cross a schema or statement
        // boundary.
        let cached = self
            .selection_cache
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .filter(|cache| cache.columns == columns && cache.predicates == request.predicates)
            .cloned();
        let (lowered, conditions) = if let Some(cache) = cached {
            (cache.lowered, cache.conditions)
        } else {
            let lowered: Vec<ScanPredicate> = request
                .predicates
                .iter()
                .filter(|predicate| accepts(predicate, &columns))
                .cloned()
                .collect();
            let conditions: Vec<Expr> = if lowered.is_empty() {
                Vec::new()
            } else {
                wide_scan_selection_conditions(&lowered, &columns).map_err(|error| {
                    PushdownScannerError::Backend(StorageError::Backend(error.to_string()))
                })?
            };
            let mut slot = self
                .selection_cache
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            *slot = Some(SelectionCache {
                columns: columns.clone(),
                predicates: request.predicates.clone(),
                lowered: lowered.clone(),
                conditions: conditions.clone(),
            });
            (lowered, conditions)
        };
        let predicates_applied = lowered.len() == request.predicates.len();

        // Go's `ConstructDAGReq` narrows a projected scan by sending
        // `DAGRequest.output_offsets`, so TiKV encodes only the projected
        // columns. The caller's contract
        // (`PushdownScanRequest::output_offsets`) lets a backend refuse a
        // narrowing it cannot answer exactly -- once the narrower row crosses
        // the wire, no residual conjunct can be repeated locally over the
        // dropped columns. So the narrowing travels when EVERY predicate
        // lowers and the executor stack stays [Scan, Selection?]: an
        // aggregate replaces the output schema the offsets index into, and
        // either aggregate or TopN must keep today's refusal.
        let wire_output_offsets: Option<Vec<u32>> = match request.output_offsets.as_ref() {
            None => None,
            Some(offsets)
                if predicates_applied && request.aggregate.is_none() && request.topn.is_none() =>
            {
                if offsets
                    .iter()
                    .any(|offset| *offset >= request.columns.len())
                {
                    return Err(refuse("an output offset is outside the scan output"));
                }
                Some(offsets.iter().map(|offset| *offset as u32).collect())
            }
            Some(_) => {
                return Err(refuse(
                    "this coprocessor lowering does not narrow output columns",
                ))
            }
        };

        // The cap may only travel with a predicate that travelled WHOLE. A
        // conjunct left behind means TiKV counts its `limit` rows against a
        // weaker filter, and the conjuncts applied here then remove some of
        // those -- fewer rows than the query asked for, with nothing to say so.
        // This is the same hazard the staged buffer already guards against by
        // dropping the cap whenever rows are merged in locally.
        let remote_limit = if lowered.len() == request.predicates.len()
            && request.aggregate.is_none()
            && request.topn.is_none()
        {
            request.limit
        } else {
            None
        };

        let aggregate = match request.aggregate.as_ref() {
            None => None,
            Some(PushdownPartialAggregate::Global {
                functions,
                streamed,
            }) => {
                if lowered.len() != request.predicates.len()
                    || request.limit.is_some()
                    || request.topn.is_some()
                    || request.output_offsets.is_some()
                {
                    return Err(refuse(
                        "partial aggregation requires a complete Selection and no competing pushdown",
                    ));
                }
                let lowered = lower_aggregate_functions(functions, &columns).ok_or_else(|| {
                    refuse("a global aggregate function cannot be lowered to TiPB")
                })?;
                field_types = functions
                    .iter()
                    .map(|function| function.output_type.clone())
                    .collect();
                Some(LoweredAggregate::Global {
                    functions: lowered,
                    streamed: *streamed,
                })
            }
            Some(PushdownPartialAggregate::Grouped {
                group_offsets,
                group_types,
                functions,
                streamed,
            }) => {
                if lowered.len() != request.predicates.len()
                    || request.limit.is_some()
                    || request.topn.is_some()
                    || request.output_offsets.is_some()
                {
                    return Err(refuse(
                        "partial aggregation requires a complete Selection and no competing pushdown",
                    ));
                }
                let lowered_functions =
                    lower_aggregate_functions(functions, &columns).ok_or_else(|| {
                        refuse("a grouped aggregate function cannot be lowered to TiPB")
                    })?;
                let group_by = lower_group_by(group_offsets, group_types, &columns)
                    .ok_or_else(|| refuse("a grouped aggregate key cannot be lowered to TiPB"))?;
                field_types = functions
                    .iter()
                    .map(|function| function.output_type.clone())
                    .chain(group_types.iter().cloned())
                    .collect();
                Some(LoweredAggregate::Grouped {
                    functions: lowered_functions,
                    group_by,
                    streamed: *streamed,
                })
            }
        };
        let output_offsets: Vec<u32> = wire_output_offsets
            .clone()
            .unwrap_or_else(|| (0..field_types.len() as u32).collect());
        // The response is encoded against the DAG's (possibly narrowed) output
        // offsets, so both decoders below see the WIRE width.
        if let Some(offsets) = &wire_output_offsets {
            field_types = offsets
                .iter()
                .map(|offset| request.columns[*offset as usize].field_type.clone())
                .collect();
        }

        if request.topn.is_some() && !predicates_applied {
            return Err(refuse(
                "a coprocessor TopN requires every predicate in the TiKV Selection",
            ));
        }
        let index_scan = request
            .index
            .as_ref()
            .map(|index| {
                let candidate = LiveIndexCandidate {
                    index_id: index.index_id,
                    ranges: Vec::new(),
                    proven_equality_range: false,
                    point_statistics: IndexPointStatistics {
                        topn_count: None,
                        cms_count: None,
                        histogram_count: 0,
                    },
                    row_size: 1.0,
                    scan_factor: 1.0,
                    index_scan_cost_factor: 1.0,
                };
                let mut spec = TiKvIndexScanSpec::new(
                    request.table_id,
                    index.index_id,
                    columns.clone(),
                    index.declared_unique,
                    index.index_column_count,
                );
                spec.desc = index.desc;
                spec.primary_column_ids = request.primary_column_ids.clone();
                PhysicalIndexScanPlan::init(0, 0, &candidate, 0.0)
                    .try_with_pushdown(
                        ResolvedIndexDescriptor {
                            index_id: index.index_id,
                            declared_unique: index.declared_unique,
                            index_column_count: index.index_column_count,
                        },
                        spec,
                    )
                    .map_err(|error| refuse(&format!("invalid index scan metadata: {error:?}")))
            })
            .transpose()?;
        let table_scan = if index_scan.is_none() {
            let mut spec = TiKvTableScanSpec::new(request.table_id, columns.clone());
            // Go's `desc` on the TableScan executor: the region walks the
            // ranges backwards. The ranges themselves stay ascending.
            spec.desc = request.desc;
            // The merge above reads the remote rows in record-key order, which is
            // the order it merges the staged buffer against.
            spec.keep_order = request.keep_order;
            spec.primary_column_ids = request.primary_column_ids.clone();
            spec.primary_prefix_column_ids = request.primary_prefix_column_ids.clone();
            Some(PhysicalTableScanPlan::init(0, 0, spec))
        } else {
            None
        };
        let scan = match (&index_scan, &table_scan) {
            (Some(scan), None) => TiKvScanPlan::Index(scan),
            (None, Some(scan)) => TiKvScanPlan::Table(scan),
            _ => unreachable!("one physical scan is built per request"),
        };
        // Go `ConstructDAGReq`: the zone comes from the SESSION VARIABLES of
        // the statement that issued this request, read fresh every time.
        let (time_zone_name, time_zone_offset_secs) = request.statement.time_zone.dag_zone();
        let context = DagRequestContext::new(
            time_zone_name,
            time_zone_offset_secs,
            // Go `builder_utils.go`'s `sc.PushDownFlags()`. The literal
            // `0` this replaced is TiKV's strictest branch: a truncation
            // TiDB degrades to a 1292 warning failed the whole region
            // request instead.
            request.statement.push_down_flags,
            // Go's table readers use chunk RPC when the store supports it.
            // The response iterator transfers decoded TypeChunks directly to
            // this scan's bounded handoff, without per-row materialization.
            EncodeType::Chunk,
        );
        let mut dag = match aggregate.as_ref() {
            Some(LoweredAggregate::Global {
                functions,
                streamed,
            }) => construct_aggregate_read_only_dag_req_with_conditions(
                &context,
                scan,
                &conditions,
                functions,
                *streamed,
                &output_offsets,
            ),
            Some(LoweredAggregate::Grouped {
                functions,
                group_by,
                streamed,
            }) => construct_grouped_aggregate_read_only_dag_req_with_conditions(
                &context,
                scan,
                &conditions,
                functions,
                group_by,
                *streamed,
                &output_offsets,
            ),
            None => construct_capped_read_only_dag_req_with_conditions(
                &context,
                scan,
                &conditions,
                remote_limit,
                &output_offsets,
            ),
        }
        .map_err(|error| PushdownScannerError::Unsupported(error.to_string()))?;
        if let Some(topn) = request.topn.as_ref() {
            let order_by = topn
                .order_by
                .iter()
                .map(|item| {
                    let column = request.columns.get(item.offset).ok_or_else(|| {
                        refuse("a coprocessor TopN column is outside the scan output")
                    })?;
                    let mut expression = tidb_expr::column::Column::new(
                        item.offset as i64 + 1,
                        column.field_type.clone(),
                    );
                    expression.index = item.offset as i64;
                    let expression = tidb_expr::pushdown_catalog::expression_to_pb(
                        &tidb_expr::expression::Expression::Column(expression),
                        &|offset| scan_column_descriptor(&columns, offset),
                    )
                    .ok_or_else(|| refuse("a coprocessor TopN key cannot be lowered to TiPB"))?;
                    Ok(ByItem {
                        expr: Some(expression),
                        desc: Some(item.desc),
                    })
                })
                .collect::<Result<Vec<_>, PushdownScannerError>>()?;
            dag.executors.push(PbExecutor {
                tp: Some(ExecType::TypeTopN as i32),
                tbl_scan: None,
                idx_scan: None,
                selection: None,
                aggregation: None,
                top_n: Some(TopN {
                    order_by,
                    limit: Some(topn.limit),
                }),
                limit: None,
                executor_id: Some(String::new()),
                parent_idx: None,
            });
        }

        let summary = dag_summary(&dag);
        let key_ranges: Vec<KeyRange> = request
            .ranges
            .iter()
            .map(|(start, end)| KeyRange::new(start.clone(), end.clone()))
            .collect();
        let mut shapes = vec![ExecutorShape::new(if request.index.is_some() {
            ExecutorKind::IndexScan
        } else {
            ExecutorKind::TableScan
        })];
        if !conditions.is_empty() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        if remote_limit.is_some() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        if request.topn.is_some() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        if aggregate.is_some() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        let plan = RemoteScanPlan {
            dag,
            envelope: RequestEnvelope::new(shapes),
            key_ranges,
            key_range_hints: request.range_hints.clone(),
            snapshot_ts: request.snapshot_ts,
            keep_order: request.keep_order,
            allow_unordered: request.allow_unordered_response,
            desc: request.desc,
            field_types: field_types.clone(),
            is_index_scan: request.index.is_some(),
            paging_min_size: request.paging_min_size,
            time_zone: request.statement.time_zone.clone(),
            resource_group_name: request.statement.resource_group_name.clone(),
            warnings: request.statement.warnings.clone(),
        };
        let batch_rows = if plan.is_index_scan {
            INDEX_BATCH_ROWS
        } else {
            BATCH_ROWS
        };
        let iter = open_scan(&self.factory, plan)
            .map_err(|error| PushdownScannerError::Backend(StorageError::Backend(error)))?;
        self.scans_served.fetch_add(1, Ordering::Relaxed);
        self.requests
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(summary);
        Ok(Box::new(CopRowStream {
            iter: Some(iter),
            pending: None,
            pending_row: 0,
            field_types,
            batch_rows,
            node_rows: Arc::clone(&self.rows_returned),
            returned: 0,
            predicates_applied,
        }))
    }
}

/// The DAG's executor list, read back out of the built request.
///
/// A receipt is worth more than an assertion here: this reads what is about to
/// be encoded, so it cannot claim a Selection the request does not carry.
fn dag_summary(dag: &tidb_proto::tipb::DagRequest) -> String {
    let executors: Vec<String> = dag
        .executors
        .iter()
        .map(|executor| match executor.tp {
            Some(tp) if tp == ExecType::TypeTableScan as i32 => {
                let columns = executor
                    .tbl_scan
                    .as_ref()
                    .map_or(0, |scan| scan.columns.len());
                let table = executor
                    .tbl_scan
                    .as_ref()
                    .and_then(|scan| scan.table_id)
                    .unwrap_or_default();
                format!("TableScan(table {table}, {columns} columns)")
            }
            Some(tp) if tp == ExecType::TypeIndexScan as i32 => {
                let (table, index, columns) =
                    executor.idx_scan.as_ref().map_or((0, 0, 0), |scan| {
                        (scan.table_id(), scan.index_id(), scan.columns.len())
                    });
                format!("IndexScan(table {table}, index {index}, {columns} columns)")
            }
            Some(tp) if tp == ExecType::TypeSelection as i32 => format!(
                "Selection({} conditions)",
                executor
                    .selection
                    .as_ref()
                    .map_or(0, |selection| selection.conditions.len())
            ),
            Some(tp) if tp == ExecType::TypeAggregation as i32 => format!(
                "HashAgg({} functions)",
                executor
                    .aggregation
                    .as_ref()
                    .map_or(0, |aggregation| aggregation.agg_func.len())
            ),
            Some(tp) if tp == ExecType::TypeLimit as i32 => format!(
                "Limit({})",
                executor
                    .limit
                    .as_ref()
                    .and_then(|limit| limit.limit)
                    .unwrap_or_default()
            ),
            Some(tp)
                if tp == ExecType::TypeAggregation as i32
                    || tp == ExecType::TypeStreamAgg as i32 =>
            {
                let (functions, groups) = executor
                    .aggregation
                    .as_ref()
                    .map_or((0, 0), |agg| (agg.agg_func.len(), agg.group_by.len()));
                let name = if tp == ExecType::TypeStreamAgg as i32 {
                    "StreamAgg"
                } else {
                    "HashAgg"
                };
                format!("{name}({functions} functions, {groups} group keys)")
            }
            other => format!("executor {other:?}"),
        })
        .collect();
    format!(
        "{} -> output offsets {:?}",
        executors.join(" | "),
        dag.output_offsets
    )
}

/// Everything needed to open one response on the query worker.
struct RemoteScanPlan {
    dag: tidb_proto::tipb::DagRequest,
    /// The executor shapes the request builder reads for concurrency, which
    /// must match the DAG's own executor list.
    envelope: RequestEnvelope,
    /// The record intervals to read, ascending by encoded key. A whole-table
    /// scan is one; a `TableRangeScan` over a clustered handle is one per
    /// handle range, and the coprocessor request carries them all. Go's
    /// `SetTableHandles` can intentionally supply equal point intervals for
    /// duplicate index entries, so this list is not universally disjoint.
    key_ranges: Vec<KeyRange>,
    /// Go `RequestBuilder.SetTableHandles` row-count hints, aligned with
    /// `key_ranges`; empty for scans without grouped handle cardinalities.
    key_range_hints: Vec<usize>,
    snapshot_ts: u64,
    /// Whether region tasks and the response stream must preserve key order.
    keep_order: bool,
    /// Whether region answers may be consumed as they complete. Only set for
    /// reads whose caller re-orders (or ignores) row order.
    allow_unordered: bool,
    /// Whether DistSQL must visit region tasks in descending key order. Go
    /// carries this through `pkg/distsql/request_builder.go`'s
    /// `RequestBuilder.SetDesc`; `pkg/store/copr/coprocessor.go`'s
    /// `buildCopTasks` then reverses the task list. The TableScan executor
    /// separately carries the direction for rows inside each region.
    desc: bool,
    field_types: Vec<FieldType>,
    /// IndexLookUp's Go decoder receives the normal MaxChunkSize (1024)
    /// boundary; full table scans retain the larger streaming batches above.
    is_index_scan: bool,
    /// Optional Go IndexLookUp first-window paging floor for index scans.
    paging_min_size: Option<u64>,
    time_zone: tidb_datatype::SessionTimeZone,
    /// Go `StmtCtx.ResourceGroupName` for this request.
    resource_group_name: String,
    /// The statement's warning sink. It is an `Arc` handler, so warnings
    /// appended while the query worker decodes land in the buffer
    /// `SHOW WARNINGS` reads.
    warnings: WarningCollector,
}

/// Opens the lazy response on the query worker. Pulling remains demand-driven:
/// an early-stopping executor closes this iterator before unread regions are
/// decoded, without an intermediate producer or scheduler rendezvous.
fn open_scan<F>(factory: &Arc<F>, plan: RemoteScanPlan) -> Result<SelectResponseIter, String>
where
    F: RealTiKvSessionTransportFactory,
    <F::Transport as QueryTransport>::Response: 'static,
{
    use prost::Message;

    let mut transport = factory.open_session_transport()?;
    let cancellation = Arc::new(CancelHandle::default());
    // Go `SetFromSessionVars`, which EVERY read in `pkg/distsql` runs. The
    // zero-value builder this replaced sent `Concurrency: 0` and an EMPTY
    // `ResourceGroupName`, neither of which any TiDB sends: a stock session
    // is `tidb_distsql_scan_concurrency = 15` and resource group `default`.
    //
    // The remaining `SetFromSessionVars` fields (replica read, statement
    // priority, request source, task id, max_execution_time,
    // tidb_kv_read_timeout, the runaway checker) are session variables no
    // `StmtContext` carries yet. Resource group is statement-scoped in Go and
    // is therefore copied from this request rather than the stock context.
    let mut context = DistSqlContext::new();
    context.request.resource_group_name = plan.resource_group_name;
    if let Some(min_size) = plan.paging_min_size {
        // Go's buildIndexSelectResultForRange raises both paging bounds to
        // the worker's first handle batch. Keep the normal session defaults
        // for every request without this IndexLookUp hint.
        context.request.paging.min_size = context.request.paging.min_size.max(min_size);
        context.request.paging.max_size = context.request.paging.max_size.max(min_size);
    }
    let mut builder = RequestBuilder::from_context(&context);
    // Go's `RequestBuilder.SetTableHandles` preserves one row-count hint per
    // grouped range. Keep the ordinary no-hint path for full/table scans and
    // refuse misaligned metadata rather than attaching a hint to the wrong
    // region task.
    if !plan.key_range_hints.is_empty() && plan.key_range_hints.len() == plan.key_ranges.len() {
        builder.set_key_ranges_with_hints(plan.key_ranges, plan.key_range_hints);
    } else {
        builder.set_non_partitioned_key_ranges(plan.key_ranges);
    }
    builder
        .set_start_ts(plan.snapshot_ts)
        .set_keep_order(plan.keep_order)
        .set_allow_unordered_response(plan.allow_unordered)
        .set_desc(plan.desc)
        .set_dag_request(plan.envelope, plan.dag.encode_to_vec());
    let request = builder
        .build_transport_request(Arc::clone(&cancellation))
        .map_err(|error| format!("{error:?}"))?;
    let mut runtime = InjectedQueryRuntime::new(&mut transport);
    let result = runtime
        .select_with_runtime_stats(
            &request,
            SelectInput::default(),
            // THE SESSION'S collector, not a fresh one: `response_channel`
            // appends TiKV's warnings in Go's order into whatever it is
            // given, and a fresh collector is dropped with them inside.
            QueryResultContext::new(plan.field_types.clone(), plan.warnings)
                .with_time_zone(plan.time_zone),
            vec![0],
            0,
            true,
        )
        .map_err(|error| error.to_string())?;
    Ok(result.into_select_iter(Vec::new()))
}

/// The caller's end of one coprocessor scan.
struct CopRowStream {
    /// Dropping this closes the response and cancels unread region work.
    iter: Option<SelectResponseIter>,
    pending: Option<Chunk>,
    pending_row: usize,
    field_types: Vec<FieldType>,
    batch_rows: usize,
    node_rows: Arc<AtomicU64>,
    returned: u64,
    predicates_applied: bool,
}

impl CopRowStream {
    fn pull_chunk(&mut self) -> Result<Option<Chunk>, StorageError> {
        loop {
            let Some(iter) = self.iter.as_mut() else {
                return Ok(None);
            };
            let batch = iter
                .next_chunk_with_required_rows(self.batch_rows)
                .map_err(|error| StorageError::Backend(error.to_string()))?;
            let Some(batch) = batch else {
                self.iter = None;
                return Ok(None);
            };
            if batch.row.num_rows() == 0 {
                continue;
            }
            self.node_rows
                .fetch_add(batch.row.num_rows() as u64, Ordering::Relaxed);
            return Ok(Some(batch.row));
        }
    }
}

impl PushdownRowStream for CopRowStream {
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
        loop {
            if let Some(batch) = &self.pending {
                if self.pending_row < batch.num_rows() {
                    let row = batch
                        .get_row(self.pending_row)
                        .try_get_datum_row(&self.field_types)
                        .map_err(|error| StorageError::Backend(error.to_string()))?;
                    self.pending_row += 1;
                    if self.pending_row == batch.num_rows() {
                        self.pending = None;
                        self.pending_row = 0;
                    }
                    self.returned += 1;
                    return Ok(Some(row));
                }
                self.pending = None;
                self.pending_row = 0;
            }
            match self.pull_chunk()? {
                Some(batch) => self.pending = Some(batch),
                None => return Ok(None),
            }
        }
    }

    fn supports_chunks(&self) -> bool {
        true
    }

    fn next_chunk(&mut self) -> Result<Option<Chunk>, StorageError> {
        if let Some(batch) = self.pending.take() {
            let start = self.pending_row;
            self.pending_row = 0;
            if start == 0 {
                self.returned += batch.num_rows() as u64;
                return Ok(Some(batch));
            }
            let mut remainder = Chunk::new(
                &self.field_types,
                batch.num_rows().saturating_sub(start),
                batch.num_rows().saturating_sub(start),
            );
            for row in start..batch.num_rows() {
                remainder.append_row(batch.get_row(row));
            }
            self.returned += remainder.num_rows() as u64;
            return Ok(Some(remainder));
        }
        match self.pull_chunk()? {
            Some(batch) => {
                self.returned += batch.num_rows() as u64;
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }

    fn rows_returned(&self) -> u64 {
        self.returned
    }

    fn predicates_applied(&self) -> bool {
        self.predicates_applied
    }

    fn close(&mut self) {
        if let Some(iter) = self.iter.as_mut() {
            iter.close();
        }
        self.iter = None;
        self.pending = None;
        self.pending_row = 0;
    }
}

impl Drop for CopRowStream {
    fn drop(&mut self) {
        self.close();
    }
}

fn scan_column(column: &PushdownScanColumn) -> Option<ScanColumnInfo> {
    // The integer family, with MySQL's default display width for each. The
    // width is metadata TiKV does not evaluate with -- the value is an integer
    // either way -- but it is what the catalog declares, so it is what the
    // descriptor carries.
    let code = column.field_type.code();
    let (tp, column_len, decimal) = match code {
        FieldTypeCode::LongLong => (MYSQL_TYPE_LONGLONG, 20, 0),
        FieldTypeCode::Long => (MYSQL_TYPE_LONG, 11, 0),
        FieldTypeCode::Int24 => (MYSQL_TYPE_INT24, 9, 0),
        FieldTypeCode::Short => (MYSQL_TYPE_SHORT, 6, 0),
        FieldTypeCode::Tiny => (MYSQL_TYPE_TINY, 4, 0),
        FieldTypeCode::NewDecimal => (
            i32::from(code.mysql_type()),
            i32::try_from(column.field_type.flen()).ok()?,
            i32::try_from(column.field_type.decimal()).ok()?,
        ),
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => (
            i32::from(code.mysql_type()),
            i32::try_from(column.field_type.flen()).ok()?,
            i32::try_from(column.field_type.decimal()).ok()?,
        ),
        // The character-string family. Unlike the integer widths above, a
        // string column's declared LENGTH is not decoration TiKV ignores: it
        // is what a `VARCHAR(n)` value is checked and compared against, so it
        // is copied from the catalog rather than defaulted. Go's
        // `util.ColumnToProto` copies `c.GetFlen()` for every family alike.
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::Blob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => (
            i32::from(code.mysql_type()),
            i32::try_from(column.field_type.flen()).unwrap_or(-1),
            i32::try_from(column.field_type.decimal()).unwrap_or(-1),
        ),
        _ => return None,
    };
    let mut flag = i32::try_from(column.field_type.flags()).ok()?;
    if column.is_handle {
        flag |= NOT_NULL_FLAG | PRI_KEY_FLAG;
    }
    // Go `util.ColumnToProto`:
    // `collate.RewriteNewCollationIDIfNeeded(mysql.CollationNames[c.GetCollate()])`.
    // An integer column's collation is `binary`, which resolves to the same
    // constant this used to hard-code; a string column's is its own, and it is
    // what tells TiKV which collator to compare and case-fold with. The
    // predicate lowering reads this very field back
    // (`tidb_exec::wide_scan_selection`), so the leaf and the scan descriptor
    // cannot disagree about the collator by construction.
    let collation = if column.field_type.is_string() {
        tidb_datatype::collation_to_proto(column.field_type.collation_name())
    } else {
        BINARY_COLLATION_ID
    };
    // Go `util.ColumnToProto` encodes the origin default into
    // `ColumnInfo.default_val`; the region fills it in for a row written
    // before the column existed, which is the only place that knows.
    let default_val = match &column.origin_default {
        Some(datum) => Some(tidb_codec::encode_value(std::slice::from_ref(datum)).ok()?),
        None => None,
    };
    Some(ScanColumnInfo {
        column_id: column.id,
        tp,
        collation,
        column_len,
        decimal,
        flag,
        pk_handle: column.is_handle,
        default_val,
        ..ScanColumnInfo::default()
    })
}

fn scan_column_descriptor(
    columns: &[ScanColumnInfo],
    offset: u32,
) -> Option<tidb_expr::pushdown_catalog::ColumnDescriptor> {
    let column = columns.get(offset as usize)?;
    let collation = tidb_datatype::proto_to_collation(column.collation);
    let charset = tidb_datatype::get_collation_by_name(&collation)
        .map_or_else(|_| "binary".to_owned(), |row| row.charset_name);
    Some(tidb_expr::pushdown_catalog::ColumnDescriptor {
        tp: column.tp,
        flag: u32::try_from(column.flag).ok()?,
        flen: column.column_len,
        decimal: column.decimal,
        charset,
        collation,
    })
}

fn lower_aggregate_functions(
    functions: &[tidb_executor::remote_scan::PushdownAggregateFunction],
    columns: &[ScanColumnInfo],
) -> Option<Vec<Expr>> {
    functions
        .iter()
        .map(|function| {
            lower_aggregate_function(
                function.kind,
                function.input.as_ref(),
                &function.output_type,
                columns,
            )
        })
        .collect()
}

fn lower_aggregate_function(
    kind: PushdownAggregateKind,
    input: Option<&tidb_expr::expression::Expression>,
    output_type: &FieldType,
    columns: &[ScanColumnInfo],
) -> Option<Expr> {
    let children = match input {
        Some(input) => vec![tidb_expr::pushdown_catalog::expression_to_pb(
            input,
            &|offset| scan_column_descriptor(columns, offset),
        )?],
        None if kind == PushdownAggregateKind::Count => {
            vec![tidb_expr::pushdown_catalog::to_pb(
                &tidb_expr::pushdown_catalog::PbScalar::IntLiteral(1),
                &|_| None,
            )?]
        }
        None => return None,
    };
    let tp = match kind {
        PushdownAggregateKind::Count => ExprType::Count,
        PushdownAggregateKind::Sum => ExprType::Sum,
        PushdownAggregateKind::Min => ExprType::Min,
        PushdownAggregateKind::Max => ExprType::Max,
    };
    Some(Expr {
        tp: Some(tp as i32),
        val: None,
        children,
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: Some(tidb_expr::pushdown_catalog::field_type_to_pb(output_type)?),
        has_distinct: Some(false),
    })
}

fn lower_group_by(
    group_offsets: &[usize],
    group_types: &[FieldType],
    columns: &[ScanColumnInfo],
) -> Option<Vec<Expr>> {
    if group_offsets.len() != group_types.len() {
        return None;
    }
    group_offsets
        .iter()
        .zip(group_types)
        .map(|(offset, field_type)| {
            let mut column = tidb_expr::column::Column::new(*offset as i64 + 1, field_type.clone());
            column.index = *offset as i64;
            tidb_expr::pushdown_catalog::expression_to_pb(
                &tidb_expr::expression::Expression::Column(column),
                &|offset| scan_column_descriptor(columns, offset),
            )
        })
        .collect()
}

/// Whether a request names the implicit `_tidb_rowid` handle column, which is
/// the shape a table with no integer primary key scans with.
#[must_use]
pub fn requests_extra_handle(request: &PushdownScanRequest) -> bool {
    request
        .handle_index
        .and_then(|index| request.columns.get(index))
        .is_some_and(|column| column.id == EXTRA_HANDLE_COLUMN_ID)
}
