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
//! The scan descriptor follows Go `util.ColumnToProto`: every concrete
//! MySQL/TiDB field type carries its wire type code, declared length/scale,
//! flags, collation, and (for `ENUM`/`SET`) element names. Only an unknown or
//! unresolved type is refused. This projection gate is separate from the
//! predicate lowering's own type gate: a supported column may still leave a
//! `WHERE` expression above the scan when TiKV cannot evaluate that function.
//! A refused shape falls back to the byte-level cursor, so it is slower but
//! never changes the answer.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, OnceLock};

use crate::pinned_thread_pool::PinnedThreadPool;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::{
    CancelHandle, DistSqlContext, EncodeType, ExecutorKind, ExecutorShape, InjectedQueryRuntime,
    QueryResultContext, QueryTransport, RequestBuilder, RequestEnvelope, SelectInput,
    WarningCollector,
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
    Aggregation, ByItem, ExecType, Executor as PbExecutor, Expr, ExprType, ScalarFuncSig, TopN,
};
use tidb_txnkv::KeyRange;

use crate::dag_request::{
    construct_aggregate_read_only_dag_req_with_conditions,
    construct_aggregated_read_only_dag_req_with_conditions,
    construct_capped_read_only_dag_req_with_conditions,
    construct_grouped_aggregate_read_only_dag_req_with_conditions, DagRequestContext, TiKvScanPlan,
};

enum LoweredAggregate {
    Legacy {
        message: Aggregation,
        output_width: usize,
    },
    Global(Vec<Expr>),
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
/// How many decoded rows the reader thread may run ahead of the consumer.
///
/// The point of the bound is that it *is* a bound: a scan holds a few batches
/// of decoded rows, never the relation, so the streaming property the scan
/// source has above the seam survives the thread hop below it.
// Keep the response-channel boundary above TiKV's small type chunks so a
// reader can amortize cross-thread wakeups while the consumer drains a full
// scan. Index scans use Go's MaxChunkSize below: IndexLookUp's
// `readFromChunk`/`extractTaskHandles` relies on those response boundaries to
// grow table tasks without splitting a typed chunk.
const BATCH_ROWS: usize = 32768;
const INDEX_BATCH_ROWS: usize = 1024;
const MAX_BATCHES_AHEAD: usize = 64;
/// Full scans have no early-stop consumer. A deeper bounded queue lets TiKV
/// response decoding overlap local join/aggregate work; scans with LIMIT keep
/// the caller's smaller read-ahead so cancellation remains tight.
const FULL_SCAN_MIN_BATCHES_AHEAD: usize = 16;

/// A process-local pool every coprocessor scan producer runs on.
///
/// Go never creates an OS thread for a scan: `IndexLookUpExecutor` submits
/// each index and table task to a persistent worker pool
/// (`pkg/executor/distsql.go:743-745, 1432-1434`), and a `TableReader`'s
/// producer is a goroutine, which costs microseconds. Rust still needs a
/// second thread per remote scan so response decoding can overlap executor
/// work, but paying `pthread_create` for it on the statement path is ours
/// alone: a bounded `WHERE id BETWEEN ? AND ?` was measured spending 6.3% of
/// its samples in `Thread::new`, 3% of them inside `_pthread_create`.
///
/// [`PinnedThreadPool`] is the right shape because it NEVER queues: a
/// submission takes a parked worker if one is free and starts a new thread if
/// not. That is what lets one pool serve short lookup windows and whole-region
/// streams together -- a long scan cannot convoy a short one behind it,
/// because a short one never waits. The park is kept separate from the
/// transaction pool's only so the two do not contend on one mutex.
fn scan_pool() -> &'static PinnedThreadPool {
    static POOL: OnceLock<PinnedThreadPool> = OnceLock::new();
    POOL.get_or_init(|| PinnedThreadPool::with_limit(SCAN_WORKER_PARK_LIMIT))
}

/// How many idle scan producers the process parks between scans. One per
/// connection that is mid-scan at the same instant is the working set; past
/// that a returning worker ends rather than parks.
const SCAN_WORKER_PARK_LIMIT: usize = 64;

/// Starts one scan producer, reporting the platform's refusal if a new thread
/// was needed and could not be created.
fn enqueue_scan<F>(task: F) -> Result<(), String>
where
    F: FnOnce() + Send + 'static,
{
    scan_pool().run("cop-scan", Box::new(task))
}

/// One coprocessor scan capability for a node's sessions.
///
/// Each opened scan gets its own worker-local transport on its own thread,
/// because the production transport is deliberately not `Send` while the
/// storage that holds this scanner is shared between connection workers. What
/// crosses threads is the request and the decoded rows.
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
            Some(PushdownPartialAggregate::Global { functions }) => {
                if lowered.len() != request.predicates.len()
                    || request.limit.is_some()
                    || request.topn.is_some()
                    || request.output_offsets.is_some()
                {
                    return Err(refuse(
                        "partial aggregation requires a complete Selection and no competing pushdown",
                    ));
                }
                let lowered = lower_global_aggregate(functions, &columns).ok_or_else(|| {
                    refuse("a global aggregate function cannot be lowered to TiPB")
                })?;
                field_types = functions
                    .iter()
                    .map(|function| function.output_type.clone())
                    .collect();
                Some(LoweredAggregate::Global(lowered))
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
                    lower_grouped_aggregate(functions, &columns).ok_or_else(|| {
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
            Some(aggregate) => {
                // The SAME gate the two arms above state, and for the same
                // reason: an aggregate computed at the region IS the answer,
                // so a conjunct left behind is not a weaker pre-filter that
                // the scan source re-tests -- there are no rows left to test.
                // `count(*)` reaches this arm, and without the gate
                // `WHERE u >= 9223372036854775808` over a `BIGINT UNSIGNED`
                // column answered 5 where every row-returning form of the same
                // query answered 2: the unsigned literal is one this lowering
                // refuses, so the Selection stayed home while the COUNT
                // travelled and counted the whole table.
                if lowered.len() != request.predicates.len()
                    || request.limit.is_some()
                    || request.topn.is_some()
                    || request.output_offsets.is_some()
                {
                    return Err(refuse(
                        "partial aggregation requires a complete Selection and no competing pushdown",
                    ));
                }
                let message = aggregation_to_pb(aggregate, &columns)
                    .ok_or_else(|| refuse("a pushed aggregate has no bounded lowering"))?;
                field_types = aggregate.output_types();
                Some(LoweredAggregate::Legacy {
                    message,
                    output_width: field_types.len(),
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
            Some(LoweredAggregate::Legacy {
                message,
                output_width,
            }) => construct_aggregated_read_only_dag_req_with_conditions(
                &context,
                scan,
                &conditions,
                message.clone(),
                *output_width,
            ),
            Some(LoweredAggregate::Global(functions)) => {
                construct_aggregate_read_only_dag_req_with_conditions(
                    &context,
                    scan,
                    &conditions,
                    functions,
                    &output_offsets,
                )
            }
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
            summary: summary.clone(),
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
            warnings: request.statement.warnings.clone(),
        };
        let batches_ahead = request.read_ahead_batches.clamp(1, MAX_BATCHES_AHEAD);
        let batches_ahead = if request.limit.is_none() {
            batches_ahead.max(FULL_SCAN_MIN_BATCHES_AHEAD)
        } else {
            batches_ahead
        };
        let (rows, batches) = sync_channel::<Result<Chunk, String>>(batches_ahead);
        let factory = Arc::clone(&self.factory);
        let node_rows = Arc::clone(&self.rows_returned);
        // A bounded one-row request is consumed immediately by the caller.
        // Serving it on this worker avoids creating and detaching a native
        // thread for every YCSB-E scan while retaining the threaded stream for
        // full scans, where response decoding must overlap executor work.
        if request.limit == Some(1) {
            serve_scan(&factory, plan, &rows, &node_rows);
        } else {
            // Every other scan -- index scans, handle-grouped table-worker
            // windows, and whole-region streams alike -- runs on the shared
            // producer pool, which is what Go does for all three
            // (`pkg/executor/distsql.go:743-745,881-1149`). The pool never
            // queues, so a whole-region stream cannot convoy a short lookup
            // behind it. The channel remains bounded, so cancellation and
            // back-pressure are unchanged from the dedicated-thread path.
            enqueue_scan(move || serve_scan(&factory, plan, &rows, &node_rows))
                .map_err(|error| PushdownScannerError::Backend(StorageError::Backend(error)))?;
        }
        self.scans_served.fetch_add(1, Ordering::Relaxed);
        self.requests
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(summary);
        Ok(Box::new(CopRowStream {
            batches: Some(batches),
            pending: None,
            pending_row: 0,
            field_types,
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

/// Everything the reader thread needs, owned independently of the caller.
struct RemoteScanPlan {
    dag: tidb_proto::tipb::DagRequest,
    /// Read-only identity line for [`drain_scan`]'s env-gated trace: which
    /// executors this request lowers (`IndexScan(table t, index i, ..)` vs
    /// `TableScan(..)`), and the pushed output offsets.
    summary: String,
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
    /// The statement's warning sink, carried onto the scan thread. It is an
    /// `Arc` handler, so a warning appended here lands in the buffer
    /// `SHOW WARNINGS` reads even though the decode happens off-thread.
    warnings: WarningCollector,
}

/// Runs one coprocessor scan on its own thread, handing decoded rows back in
/// bounded batches.
fn serve_scan<F>(
    factory: &Arc<F>,
    plan: RemoteScanPlan,
    rows: &SyncSender<Result<Chunk, String>>,
    node_rows: &Arc<AtomicU64>,
) where
    F: RealTiKvSessionTransportFactory,
    <F::Transport as QueryTransport>::Response: 'static,
{
    if let Err(error) = drain_scan(factory, plan, rows, node_rows) {
        let _ = rows.send(Err(error));
    }
}

fn drain_scan<F>(
    factory: &Arc<F>,
    plan: RemoteScanPlan,
    rows: &SyncSender<Result<Chunk, String>>,
    node_rows: &Arc<AtomicU64>,
) -> Result<(), String>
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
    // The context is the STOCK one, not this session's: the remaining
    // `SetFromSessionVars` fields (replica read, statement priority, paging,
    // request source, task id, max_execution_time, tidb_kv_read_timeout, the
    // runaway checker) are session variables no `StmtContext` carries yet, so
    // threading them is a session-tier change this seam cannot make on its
    // own. What it can do is stop sending values that correspond to no
    // session at all.
    let trace_range_counts = std::env::var_os("TIKV_QUERY_TRACE")
        .is_some()
        .then(|| (plan.key_ranges.len(), plan.key_range_hints.len()));
    let mut context = DistSqlContext::new();
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
    if let Some((ranges, hints)) = trace_range_counts {
        eprintln!(
            "[XTRACE] scan_open {} | ranges={} hints={} keep_order={} allow_unordered={} desc={}",
            plan.summary, ranges, hints, plan.keep_order, plan.allow_unordered, plan.desc,
        );
    }
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
    let mut iter = result.into_select_iter(Vec::new());
    let batch_rows = if plan.is_index_scan {
        INDEX_BATCH_ROWS
    } else {
        BATCH_ROWS
    };
    loop {
        let batch = iter
            .next_chunk_with_required_rows(batch_rows)
            .map_err(|error| error.to_string())?;
        let Some(batch) = batch else {
            break;
        };
        if batch.row.num_rows() != 0 {
            let sent = batch.row.num_rows() as u64;
            // A consumer that stopped pulling -- an early-stopping `LIMIT`, or
            // a failed statement -- drops its receiver, and this is where the
            // scan learns it: the rest of the relation is never read.
            let send_result = rows.send(Ok(batch.row));
            if send_result.is_err() {
                break;
            }
            node_rows.fetch_add(sent, Ordering::Relaxed);
        }
    }
    iter.close();
    Ok(())
}

/// The caller's end of one coprocessor scan.
struct CopRowStream {
    /// Dropping this is what tells the reader thread to stop; see
    /// `drain_scan`.
    batches: Option<Receiver<Result<Chunk, String>>>,
    pending: Option<Chunk>,
    pending_row: usize,
    field_types: Vec<FieldType>,
    returned: u64,
    predicates_applied: bool,
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
            let Some(batches) = self.batches.as_ref() else {
                return Ok(None);
            };
            match batches.recv() {
                Ok(Ok(batch)) => self.pending = Some(batch),
                Ok(Err(error)) => {
                    self.batches = None;
                    return Err(StorageError::Backend(error));
                }
                // The reader thread finished and dropped its sender.
                Err(_) => {
                    self.batches = None;
                    return Ok(None);
                }
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
        let Some(batches) = self.batches.as_ref() else {
            return Ok(None);
        };
        match batches.recv() {
            Ok(Ok(batch)) => {
                self.returned += batch.num_rows() as u64;
                Ok(Some(batch))
            }
            Ok(Err(error)) => {
                self.batches = None;
                Err(StorageError::Backend(error))
            }
            Err(_) => {
                self.batches = None;
                Ok(None)
            }
        }
    }

    fn rows_returned(&self) -> u64 {
        self.returned
    }

    fn predicates_applied(&self) -> bool {
        self.predicates_applied
    }

    fn close(&mut self) {
        self.batches = None;
        self.pending = None;
        self.pending_row = 0;
    }
}

impl Drop for CopRowStream {
    fn drop(&mut self) {
        self.close();
    }
}

/// One column's coprocessor descriptor, or `None` for a type this bounded
/// lowering will not describe.
///
/// The refusal is the honest half: a descriptor built from a guessed
/// collation, length or flag set would make TiKV decode a column differently
/// from the client, which is a wrong answer rather than a slow one.
/// Lowers the pushed partial aggregate into the TiPB `Aggregation` message
/// -- Go `PhysicalHashAgg.ToPB`/`PhysicalStreamAgg.ToPB` over
/// `aggregation.AggFuncToPBExpr`, narrowed to the column-argument shapes
/// [`PushdownPartialAggregate`] models. `None` refuses the whole scan.
///
/// The aggregate leaves carry no `field_type`: every argument is a scan
/// column whose full descriptor already travels in the `TableScan`
/// executor, which is where the region reads types and collations from.
fn aggregation_to_pb(
    aggregate: &PushdownPartialAggregate,
    columns: &[ScanColumnInfo],
) -> Option<Aggregation> {
    // Go `AggFuncToPBExpr` types BOTH halves of every aggregate leaf: the
    // `ColumnRef` child carries the scanned column's declared type and the
    // aggregate expression carries the function's return type (`RetTp`).
    // TiKV builds its aggregate implementation FROM that return type
    // (`components/tidb_query_aggr`), so an untyped expression is not
    // "default-typed" -- it is refused as `Unsupported type: Unspecified`,
    // which sysbench's `SELECT SUM(k)` hit live against a real region.
    let column_ref = |offset: usize| -> Option<Expr> {
        let column = columns.get(offset)?;
        let code = tidb_datatype::FieldTypeCode::from_mysql_type(u8::try_from(column.tp).ok()?);
        let field_type = FieldType::new(code)
            .with_flags(u32::try_from(column.flag).ok()?)
            .with_collation_name(tidb_datatype::proto_to_collation(column.collation));
        tidb_expr::pushdown_catalog::to_pb(
            &tidb_expr::pushdown_catalog::PbScalar::Column {
                offset: u32::try_from(offset).ok()?,
                field_type,
            },
            &|offset| scan_column_descriptor(columns, offset),
        )
    };
    let agg = |tp: ExprType, child: Expr, output: &FieldType| -> Option<Expr> {
        Some(Expr {
            tp: Some(tp as i32),
            val: None,
            children: vec![child],
            sig: Some(ScalarFuncSig::Unspecified as i32),
            field_type: Some(tidb_expr::pushdown_catalog::field_type_to_pb(output)?),
            has_distinct: Some(false),
        })
    };
    let message = |group_by: Vec<Expr>, agg_func: Vec<Expr>, streamed: bool| Aggregation {
        group_by,
        agg_func,
        streamed: Some(streamed),
    };
    match aggregate {
        PushdownPartialAggregate::Count {
            input_offset,
            output_type,
        } => {
            let input = match input_offset {
                Some(offset) => column_ref(*offset)?,
                None => tidb_expr::pushdown_catalog::to_pb(
                    &tidb_expr::pushdown_catalog::PbScalar::IntLiteral(1),
                    &|_| None,
                )?,
            };
            Some(message(
                Vec::new(),
                vec![agg(ExprType::Count, input, output_type)?],
                true,
            ))
        }
        PushdownPartialAggregate::Sum {
            input_offset,
            output_type,
        } => Some(message(
            Vec::new(),
            vec![agg(ExprType::Sum, column_ref(*input_offset)?, output_type)?],
            false,
        )),
        PushdownPartialAggregate::GroupBy { input_offset, .. } => {
            Some(message(vec![column_ref(*input_offset)?], Vec::new(), false))
        }
        PushdownPartialAggregate::GroupBySum {
            group_offset,
            sum_offset,
            sum_type,
            ..
        } => Some(message(
            vec![column_ref(*group_offset)?],
            vec![agg(ExprType::Sum, column_ref(*sum_offset)?, sum_type)?],
            false,
        )),
        PushdownPartialAggregate::Grouped {
            group_offsets,
            functions,
            streamed,
            ..
        } => {
            let group_by = group_offsets
                .iter()
                .map(|offset| column_ref(*offset))
                .collect::<Option<Vec<_>>>()?;
            let agg_func = functions
                .iter()
                .map(|function| {
                    lower_aggregate_function(
                        function.kind,
                        function.input.as_ref(),
                        &function.output_type,
                        columns,
                    )
                })
                .collect::<Option<Vec<_>>>()?;
            Some(message(group_by, agg_func, *streamed))
        }
        PushdownPartialAggregate::Global { functions } => Some(message(
            Vec::new(),
            lower_global_aggregate(functions, columns)?,
            false,
        )),
    }
}

fn scan_column(column: &PushdownScanColumn) -> Option<ScanColumnInfo> {
    let code = column.field_type.code();
    // `FieldTypeCode::Unknown` and `Unspecified` have no stable TiPB
    // interpretation. Every named code below is accepted by Go's
    // `ColumnToProto`, including the integer, temporal, floating-point,
    // JSON, ENUM/SET, BIT, geometry and vector families.
    if matches!(code, FieldTypeCode::Unknown(_) | FieldTypeCode::Unspecified) {
        return None;
    }
    let tp = i32::from(code.mysql_type());
    // Go writes `int32(c.GetFlen())`/`int32(c.GetDecimal())` verbatim.  The
    // parser deliberately leaves some concrete types (for example a bare
    // `GEOMETRY`) at `-1`, and very wide lengths such as JSON's max width also
    // narrow to `-1` in the protobuf field.  Preserve that metadata instead
    // of treating it as an unsupported column.
    let column_len = i32::try_from(column.field_type.flen()).unwrap_or(-1);
    let decimal = i32::try_from(column.field_type.decimal()).unwrap_or(-1);
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
    let collation = if matches!(
        code,
        FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::String
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Enum
            | FieldTypeCode::Set
    ) {
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
        elems: column
            .field_type
            .elems_snapshot()
            .into_iter()
            .map(|elem| elem.to_string())
            .collect(),
        pk_handle: column.is_handle,
        default_val,
        array: column.field_type.is_array(),
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
        elems: column.elems.clone(),
        array: column.array,
    })
}

fn lower_global_aggregate(
    functions: &[tidb_executor::remote_scan::PushdownGlobalAggregateFunction],
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

fn lower_grouped_aggregate(
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::mpsc::sync_channel;

    use super::{enqueue_scan, scan_column};
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
    use tidb_executor::remote_scan::PushdownScanColumn;

    /// A run of scans REUSES producer threads instead of creating one each.
    ///
    /// Go never pays `pthread_create` for a scan producer: `IndexLookUpExecutor`
    /// submits to a persistent pool (`pkg/executor/distsql.go:743-745`) and a
    /// `TableReader`'s producer is a goroutine. A bounded
    /// `WHERE id BETWEEN ? AND ?` was measured spending 6.3% of its samples
    /// inside `Thread::new` when every scan spawned its own, which is why the
    /// thread identity -- not just "the work ran" -- is what this pins.
    ///
    /// Each scan here finishes before the next is submitted, so a pool that
    /// parks its workers answers all of them from one thread. The bound allows
    /// a second only because the park is process-wide: a sibling test's worker
    /// may hold the parked slot when this one submits.
    #[test]
    fn a_run_of_scans_reuses_producer_threads() {
        const SCANS: usize = 8;
        let mut producers = BTreeSet::new();
        for _ in 0..SCANS {
            let (done, received) = sync_channel(1);
            enqueue_scan(move || {
                let _ = done.send(format!("{:?}", std::thread::current().id()));
            })
            .expect("the scan pool should start a producer");
            producers.insert(received.recv().expect("the producer should complete"));
        }
        assert!(
            producers.len() <= 2,
            "{SCANS} sequential scans ran on {} producer threads; a pool that \
             parks its workers answers them from one, and one thread per scan \
             is the `pthread_create` this pool exists to avoid",
            producers.len()
        );
    }

    #[test]
    fn bounded_lookup_scan_pool_runs_submitted_work() {
        let (done, received) = sync_channel(1);
        enqueue_scan(move || done.send(()).expect("lookup producer should run"))
            .expect("the scan pool should start a producer");
        received
            .recv()
            .expect("persistent lookup producer should complete");
    }

    #[test]
    fn scan_column_carries_every_concrete_go_column_type() {
        // Go's util.ColumnToProto does not reject these types: the TiPB
        // descriptor carries the MySQL type code, declared metadata, and
        // ENUM/SET elements for TiKV to decode. Keep this list in lockstep
        // with FieldTypeCode so a new concrete type cannot silently force a
        // whole table scan back to the byte-level path.
        let codes = [
            FieldTypeCode::Tiny,
            FieldTypeCode::Short,
            FieldTypeCode::Int24,
            FieldTypeCode::Long,
            FieldTypeCode::LongLong,
            FieldTypeCode::Float,
            FieldTypeCode::Double,
            FieldTypeCode::NewDecimal,
            FieldTypeCode::Date,
            FieldTypeCode::NewDate,
            FieldTypeCode::Datetime,
            FieldTypeCode::Timestamp,
            FieldTypeCode::Duration,
            FieldTypeCode::Year,
            FieldTypeCode::Bit,
            FieldTypeCode::Varchar,
            FieldTypeCode::VarString,
            FieldTypeCode::String,
            FieldTypeCode::TinyBlob,
            FieldTypeCode::Blob,
            FieldTypeCode::MediumBlob,
            FieldTypeCode::LongBlob,
            FieldTypeCode::Json,
            FieldTypeCode::Enum,
            FieldTypeCode::Set,
            FieldTypeCode::Geometry,
            FieldTypeCode::VectorFloat32,
            FieldTypeCode::Null,
        ];
        for code in codes {
            let field_type = FieldType::new(code)
                .with_flen(17)
                .with_decimal(6)
                .with_flags(FieldTypeFlags::UNSIGNED)
                .with_elems(["red", "green"])
                .with_array(code == FieldTypeCode::Json);
            let descriptor = scan_column(&PushdownScanColumn {
                id: 7,
                field_type,
                is_handle: false,
                origin_default: None,
            })
            .unwrap_or_else(|| panic!("concrete type {code:?} must be encodable"));
            assert_eq!(descriptor.column_id, 7);
            assert_eq!(descriptor.tp, i32::from(code.mysql_type()));
            assert_eq!(descriptor.column_len, 17);
            assert_eq!(descriptor.decimal, 6);
            assert_eq!(descriptor.flag, FieldTypeFlags::UNSIGNED as i32);
            assert_eq!(descriptor.array, code == FieldTypeCode::Json);
            if matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) {
                assert_eq!(descriptor.elems, vec!["red", "green"]);
            }
        }
        assert!(scan_column(&PushdownScanColumn {
            id: 8,
            field_type: FieldType::new(FieldTypeCode::Unknown(0xee)),
            is_handle: false,
            origin_default: None,
        })
        .is_none());
        for code in [
            FieldTypeCode::NewDate,
            FieldTypeCode::Geometry,
            FieldTypeCode::VectorFloat32,
        ] {
            let descriptor = scan_column(&PushdownScanColumn {
                id: 9,
                field_type: FieldType::new(code),
                is_handle: false,
                origin_default: None,
            })
            .unwrap_or_else(|| panic!("unspecified metadata for {code:?} is valid Go metadata"));
            assert_eq!(descriptor.column_len, -1);
            assert_eq!(descriptor.decimal, -1);
        }
    }
}
