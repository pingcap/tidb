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

//! Go `TableReaderExecutor.buildResp` over the TiKV coprocessor: the
//! executor lowers a base-table (or index) read into a `tipb.DAGRequest`
//! (Go `builder_utils.go`'s `constructDAGReq`), encodes it into a
//! `kv.Request` with `distsql.RequestBuilder`, and sends it through the
//! store's `kv.Client` (`distsql.SelectWithRuntimeStats`), streaming the
//! response back as rows.
//!
//! [`crate::dag_request`] lowers the scan, the Selection and the cap into a
//! `DAGRequest`; [`crate::wide_scan_selection`] converts the pushed conjuncts
//! into that Selection's condition shape; `tidb-distsql` owns ranges, region
//! tasks, dispatch, retry and response decoding; the client below
//! ([`tidb_txnkv::Client`], Go `kv.Storage.GetClient()`) carries the encoded
//! request onto the wire.
//!
//! # The two properties that make it safe
//!
//! * **It answers from the statement's own snapshot.** The request carries
//!   [`PushdownScanRequest::snapshot_ts`], which the storage filled in from
//!   the snapshot the statement is bound to. A scan that read at any other
//!   timestamp would not be repeatable read.
//! * **It never has the last word.** The scan source applies the pushed
//!   conjuncts and the cap to every row it emits, and merges the session's
//!   staged buffer on top (see [`crate::remote_scan`]). So this module may
//!   lower all, some, or none of the predicate and the answer is the same;
//!   only the number of rows on the wire changes.
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

use std::sync::{Arc, Mutex};

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::DistSqlContext;
use tidb_distsql::EncodeType;
use tidb_distsql::ExecutorKind;
use tidb_distsql::ExecutorShape;
use tidb_distsql::QueryResultContext;
use tidb_distsql::RequestBuilder;
use tidb_distsql::RequestEnvelope;
use tidb_distsql::SelectInput;
use tidb_distsql::select_response::SelectResponseIter;
use tidb_txnkv::CancelHandle;
use tidb_context::WarningCollector;
use tidb_planner_cardinality::live_index_optimizer::{IndexPointStatistics, LiveIndexCandidate};
use tidb_planner_core_operator_physicalop::{PhysicalIndexScan, PhysicalTableScan};
use tidb_planner_core_operator_logicalop::tikv_scan_spec::{
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
use tidb_executor_internal_exec::predicate_pushdown::ScanPredicate;
use tidb_distsql::remote_scan::EXTRA_HANDLE_COLUMN_ID;
use tidb_distsql::remote_scan::PushdownScannerError;
use tidb_distsql::remote_scan::PushdownScanRequest;
use tidb_distsql::remote_scan::PushdownScanColumn;
use tidb_distsql::remote_scan::PushdownRowStream;
use tidb_distsql::remote_scan::PushdownPartialAggregate;
use tidb_distsql::remote_scan::PushdownAggregateKind;
use tidb_distsql::remote_scan::CopPlanKind;
use tidb_kv::storage::StorageError;
use crate::wide_scan_selection::{accepts_in, wide_scan_selection_conditions_in};

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

/// The per-process wire receipt behind `cluster-session-smoke --cop`:
/// how many rows the coprocessor actually sent over the wire for the
/// current statement, and which executors the last admitted DAG carried.
/// The smoke binary resets before a statement and prints the pair after
/// it; the RealTiKV harness greps those lines as the pushdown receipt
/// instead of trusting a claimed number.
pub mod scan_receipt {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    static WIRE_ROWS: AtomicU64 = AtomicU64::new(0);
    static SHAPE: Mutex<String> = Mutex::new(String::new());

    /// Clears both halves for the next statement.
    pub fn reset() {
        WIRE_ROWS.store(0, Ordering::SeqCst);
        *SHAPE.lock().unwrap_or_else(|poison| poison.into_inner()) = String::new();
    }

    /// Records the executor list the last opened DAG carried.
    pub fn record_shape(shape: String) {
        *SHAPE.lock().unwrap_or_else(|poison| poison.into_inner()) = shape;
    }

    /// Adds the rows one wire chunk carried.
    pub fn add_wire_rows(rows: u64) {
        WIRE_ROWS.fetch_add(rows, Ordering::SeqCst);
    }

    /// The (wire rows, DAG shape) observed since the last reset.
    #[must_use]
    pub fn snapshot() -> (u64, String) {
        (
            WIRE_ROWS.load(Ordering::SeqCst),
            SHAPE
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone(),
        )
    }
}

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: i32 = 1;
/// Go `mysql.PriKeyFlag`.
const PRI_KEY_FLAG: i32 = 2;
/// Go `charset.CollationBin`, the coprocessor collation of a numeric column.
const BINARY_COLLATION_ID: i32 = 63;

#[derive(Clone)]
struct SelectionCache {
    columns: Vec<ScanColumnInfo>,
    predicates: Vec<ScanPredicate>,
    conditions: Vec<Expr>,
}

/// The same table lookup opens several region scans with identical columns
/// and predicates. Keep the lowered Selection for that request shape so a
/// large `IN` list is encoded once instead of once per region window. The
/// key includes the full descriptors and source predicates, so it cannot
/// cross a schema or statement boundary.
static SELECTION_CACHE: Mutex<Option<SelectionCache>> = Mutex::new(None);

/// Go `TableReaderExecutor.buildResp` for a TiKV read: lowers `request` into
/// a `DAGRequest`, builds its `kv.Request`, and selects through `client`.
///
/// Every request shape this lowering does not build into the DAG is refused
/// BY NAME ([`PushdownScannerError::Unsupported`]), never ignored, and the
/// caller then answers from the byte-level cursor.
pub fn open_cop_scan(
    client: &dyn tidb_txnkv::Client,
    request: &PushdownScanRequest,
) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
    let refuse = |reason: &str| PushdownScannerError::Unsupported(reason.to_owned());
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
    // Go `ExprToPB` over the scan's COMPLETE Selection: every condition
    // converts or the request carries none of them. A subset would let an
    // operator above the Selection (a limit, a TopN, an aggregate) count
    // rows against a weaker filter than the one the statement states.
    let eval_context = request
        .statement
        .eval_context
        .as_ref()
        .map(tidb_distsql::remote_scan::StatementEval::context);
    let described: Option<Vec<ScanPredicate>> = request
        .conditions
        .iter()
        .flat_map(tidb_expr::expr_util::split_cnf_items)
        .map(|condition| {
            tidb_executor_internal_exec::predicate_pushdown::describe_request_condition(&condition, eval_context).ok()
        })
        .collect();
    let described = described.filter(|predicates| {
        predicates
            .iter()
            .all(|predicate| accepts_in(predicate, &columns, &request.statement.time_zone))
    });
    let cached = described.as_ref().and_then(|predicates| {
        SELECTION_CACHE
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .filter(|cache| cache.columns == columns && cache.predicates == *predicates)
            .cloned()
    });
    let (predicates_applied, conditions) = match (described, cached) {
        (None, _) => (false, Vec::new()),
        (Some(_), Some(cache)) => (true, cache.conditions),
        (Some(predicates), None) => {
            let conditions: Vec<Expr> = if predicates.is_empty() {
                Vec::new()
            } else {
                wide_scan_selection_conditions_in(
                    &predicates,
                    &columns,
                    &request.statement.time_zone,
                )
                .map_err(|error| {
                    PushdownScannerError::Backend(StorageError::Backend(error.to_string()))
                })?
            };
            let mut slot = SELECTION_CACHE
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            *slot = Some(SelectionCache {
                columns: columns.clone(),
                predicates,
                conditions: conditions.clone(),
            });
            (true, conditions)
        }
    };

    // Go's `ConstructDAGReq` narrows a projected scan by sending
    // `DAGRequest.output_offsets`, so TiKV encodes only the projected
    // columns. The caller's contract
    // (`PushdownScanRequest::output_offsets`) lets a backend refuse a
    // narrowing it cannot answer exactly -- once the narrower row crosses
    // the wire, no residual conjunct can be repeated locally over the
    // dropped columns. So the narrowing travels when EVERY predicate
    // lowers. Selection, Limit and TopN preserve the input schema and
    // run before projection; aggregation replaces that schema.
    let wire_output_offsets: Option<Vec<u32>> = match request.output_offsets.as_ref() {
        None => None,
        Some(offsets) if predicates_applied && request.aggregate.is_none() => {
            if offsets
                .iter()
                .any(|offset| *offset >= request.columns.len())
            {
                return Err(refuse("an output offset is outside the scan output"));
            }
            Some(offsets.iter().map(|offset| *offset as u32).collect())
        }
        Some(_) => {
            // output_offsets specifies the returned row layout. Refuse
            // an unfulfillable request so the caller retains its local
            // filter/projection; never return a different-width row.
            return Err(refuse(
                "projected coprocessor output requires complete predicate pushdown",
            ));
        }
    };

    // The cap may only travel with a predicate that travelled WHOLE. A
    // conjunct left behind means TiKV counts its `limit` rows against a
    // weaker filter, and the conjuncts applied here then remove some of
    // those -- fewer rows than the query asked for, with nothing to say so.
    // This is the same hazard the staged buffer already guards against by
    // dropping the cap whenever rows are merged in locally.
    if let Some(prefix) = &request.prefix_limit {
        if !predicates_applied
            || request.aggregate.is_some()
            || request.topn.is_some()
            || request.limit.is_some()
            || request
                .index
                .as_ref()
                .is_none_or(|index| prefix.column_offset >= index.index_column_count)
        {
            return Err(refuse(
                "a prefix Limit requires a complete index Selection and no competing pushdown",
            ));
        }
    }
    let remote_limit =
        if predicates_applied && request.aggregate.is_none() && request.topn.is_none() {
            request
                .prefix_limit
                .as_ref()
                .map(|limit| limit.count)
                .or(request.limit)
        } else {
            None
        };

    let aggregate = match request.aggregate.as_ref() {
        None => None,
        Some(PushdownPartialAggregate::Global {
            functions,
            streamed,
        }) => {
            if !predicates_applied
                || request.limit.is_some()
                || request.topn.is_some()
                || request.output_offsets.is_some()
            {
                return Err(refuse(
                    "partial aggregation requires a complete Selection and no competing pushdown",
                ));
            }
            let lowered =
                lower_aggregate_functions(functions, &columns, &request.statement.time_zone)
                    .ok_or_else(|| {
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
            if !predicates_applied
                || request.limit.is_some()
                || request.topn.is_some()
                || request.output_offsets.is_some()
            {
                return Err(refuse(
                    "partial aggregation requires a complete Selection and no competing pushdown",
                ));
            }
            let lowered_functions =
                lower_aggregate_functions(functions, &columns, &request.statement.time_zone)
                    .ok_or_else(|| {
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
            PhysicalIndexScan::init(0, 0, &candidate, 0.0)
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
        Some(PhysicalTableScan::init(0, 0, spec))
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
    let mut context = DagRequestContext::new(
        time_zone_name,
        time_zone_offset_secs,
        // Go `builder_utils.go`'s `sc.PushDownFlags()`. The literal
        // `0` this replaced is TiKV's strictest branch: a truncation
        // TiDB degrades to a 1292 warning failed the whole region
        // request instead.
        request.statement.push_down_flags,
        // Go's table readers use chunk RPC when the store supports it.
        // The response iterator transfers decoded TypeChunks directly to
        // the consumer, without per-row materialization.
        EncodeType::Chunk,
    );
    // Go `builder_utils.go:73-76`: `dagReq.DivPrecisionIncrement` is set
    // from the statement's session variable when it differs from the
    // default; the lowering omits the field at the default value.
    context.div_precision_increment = request.statement.div_precision_increment;
    // Go `ConstructDAGReq`: `RuntimeStatsColl != nil` asks the store for
    // per-executor execution summaries.
    context.collect_execution_summaries = request.statement.cop_response_sink.is_some();
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
    if let Some(prefix) = &request.prefix_limit {
        let column = request
            .columns
            .get(prefix.column_offset)
            .ok_or_else(|| refuse("a prefix Limit column is outside the scan schema"))?;
        let mut expression = tidb_expr::column::Column::new(
            prefix.column_offset as i64 + 1,
            column.field_type.clone(),
        );
        expression.index = prefix.column_offset as i64;
        let expression = tidb_expr::pushdown_catalog::expression_to_pb(
            &tidb_expr::expression::Expression::Column(expression),
            &|offset| scan_column_descriptor(&columns, offset),
        )
        .ok_or_else(|| refuse("a prefix Limit column cannot be lowered to TiPB"))?;
        dag.executors
            .last_mut()
            .and_then(|executor| executor.limit.as_mut())
            .ok_or_else(|| refuse("the prefix Limit executor is missing"))?
            .truncate_key_expr
            .push(expression);
    }
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
                    ..Default::default()
                })
            })
            .collect::<Result<Vec<_>, PushdownScannerError>>()?;
        dag.executors.push(PbExecutor {
            tp: Some(ExecType::TypeTopN as i32),
            tbl_scan: None,
            idx_scan: None,
            selection: None,
            aggregation: None,
            top_n: Some(Box::new(TopN {
                order_by,
                limit: Some(topn.limit),
                ..Default::default()
            })),
            limit: None,
            executor_id: Some(String::new()),
            parent_idx: None,
            exchange_sender: None,
            ..Default::default()
        });
    }

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
    if let Some(limit) = remote_limit {
        // Go SetDAGRequest derives both LimitSize and the small-scan
        // concurrency from the actual Limit executor in the wire DAG.
        shapes.push(ExecutorShape::limit(limit, None));
    }
    if request.topn.is_some() {
        shapes.push(ExecutorShape::new(ExecutorKind::Other));
    }
    if aggregate.is_some() {
        shapes.push(ExecutorShape::new(ExecutorKind::Other));
    }
    // The wire receipt's DAG shape: the executor list this request
    // actually carries, with the Selection named when conjuncts
    // travelled. The harness reads the presence of a pushed Selection
    // off this string.
    let receipt_shape = {
        let mut shape = if request.index.is_some() {
            "IndexScan".to_owned()
        } else {
            "TableScan".to_owned()
        };
        if !conditions.is_empty() {
            shape.push_str(" Selection");
        }
        if remote_limit.is_some() {
            shape.push_str(" Limit");
        }
        if request.topn.is_some() {
            shape.push_str(" TopN");
        }
        if aggregate.is_some() {
            shape.push_str(" Aggregate");
        }
        shape
    };
    scan_receipt::record_shape(receipt_shape);
    let (cop_plan_ids, root_plan_id) = cop_plan_ids_for(&dag, &request.statement);
    // Go `TableReaderExecutor`/`IndexReaderExecutor`'s `memTracker`
    // (`NewTracker(e.ID(), -1)` under `StmtCtx.MemTracker`), which the
    // select result charges its responses to. An index lookup's sides
    // and an index merge's partials report under a pushed root, which
    // Go gives no tracker.
    let memory_tracker = (root_plan_id > 0
        && !request.statement.cop_plan_chain.is_empty()
        && !cop_plan_ids.contains(&root_plan_id))
    .then(|| {
        request
            .statement
            .memory
            .operator_tracker(root_plan_id as i64)
    });
    let plan = RemoteScanPlan {
        cancellation: request.statement.memory.coprocessor_request_cancellation(),
        rpc_interceptor: request.statement.rpc_interceptor.clone(),
        kv_context: request
            .statement
            .kv_context
            .as_ref()
            .map(tidb_txnkv::StatementKvContextCell::get)
            .unwrap_or_default(),
        dag,
        envelope: RequestEnvelope::new(shapes),
        key_ranges,
        key_range_hints: request.range_hints.clone(),
        snapshot_ts: request.snapshot_ts,
        keep_order: request.keep_order,
        allow_unordered: request.allow_unordered_response,
        desc: request.desc,
        field_types: field_types.clone(),
        paging_min_size: request.paging_min_size,
        time_zone: request.statement.time_zone.clone(),
        resource_group_name: request.statement.resource_group_name.clone(),
        replica_read: request.statement.replica_read,
        read_replica_scope: request.statement.read_replica_scope.clone(),
        is_staleness: request.statement.is_staleness,
        read_policy: request.statement.read_policy.clone(),
        estimated_net_bytes: request.statement.estimated_net_bytes,
        max_execution_time_ms: request.statement.max_execution_time_ms,
        priority: request.statement.priority,
        not_fill_cache: request.statement.not_fill_cache,
        dist_sql_scan_concurrency: request.statement.dist_sql_scan_concurrency,
        store_batch_size: request.statement.store_batch_size,
        query_cop_store_limiter: request.statement.query_cop_store_limiter.clone(),
        warnings: request.statement.warnings.clone(),
        cop_lite_worker: Arc::clone(&request.statement.cop_lite_worker),
        cop_plan_ids,
        root_plan_id,
        keys_read: request.statement.keys_read.clone(),
        cop_response_sink: request.statement.cop_response_sink.clone(),
        memory_tracker,
    };
    let iter = scan_result(|| open_scan(client, plan))
        .map_err(|error| PushdownScannerError::Backend(StorageError::Backend(error)))?;
    Ok(Box::new(CopRowStream {
        iter: Some(iter),
        pending: None,
        pending_row: 0,
        field_types,
        returned: 0,
        predicates_applied,
        plan_id: request.statement.plan_id,
        exhausted: false,
    }))
}

/// Everything needed to open one response on the query worker.
struct RemoteScanPlan {
    cancellation: Arc<CancelHandle>,
    rpc_interceptor: tidb_txnkv::RpcInterceptorOption,
    /// The issuing statement's ctx values: its `RUDetails` and its
    /// `StmtCtx.GetResourceGroupTagger`.
    kv_context: tidb_txnkv::StatementKvContext,
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
    /// Optional Go IndexLookUp first-window paging floor for index scans.
    paging_min_size: Option<u64>,
    time_zone: tidb_datatype::SessionTimeZone,
    /// Go `StmtCtx.ResourceGroupName` for this request.
    resource_group_name: String,
    /// Go `SessionVars.GetReplicaRead()` for this request.
    replica_read: tidb_txnkv::ReplicaReadType,
    read_replica_scope: String,
    is_staleness: bool,
    read_policy: tidb_distsql::remote_scan::CoprocessorReadPolicy,
    estimated_net_bytes: f64,
    max_execution_time_ms: u64,
    /// Go `RequestBuilder.getKVPriority(StmtCtx.Priority)` for this request.
    priority: tidb_mysql::Priority,
    /// Go `StmtCtx.NotFillCache` for this request.
    not_fill_cache: bool,
    /// Go `SessionVars.DistSQLScanConcurrency()` for this request.
    dist_sql_scan_concurrency: u64,
    /// Go `SessionVars.StoreBatchSize` for this request.
    store_batch_size: u64,
    /// Query-scoped per-store limiter shared by all region tasks for this
    /// statement.
    query_cop_store_limiter: Option<Arc<tidb_txnkv::QueryCopStoreLimiter>>,
    /// The statement's warning sink. It is an `Arc` handler, so warnings
    /// appended while the query worker decodes land in the buffer
    /// `SHOW WARNINGS` reads.
    warnings: WarningCollector,
    cop_lite_worker: Arc<std::sync::atomic::AtomicBool>,
    /// Physical IDs aligned with TiPB executors for runtime summaries.
    cop_plan_ids: Vec<isize>,
    root_plan_id: isize,
    /// The reader's memory tracker the select result charges.
    memory_tracker: Option<Arc<tidb_memory::Tracker>>,
    /// The statement's `tidb_max_keys_read` budget and keys-examined counter.
    keys_read: crate::KeysReadBudget,
    /// The statement's consumer of per-response execution details.
    cop_response_sink: Option<Arc<dyn tidb_distsql::select_response::CopResponseSink>>,
}

/// Go copIteratorWorker/liteCopIteratorWorker turns task panics into errors.
fn scan_result<T>(task: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)).unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload");
        Err(format!("coprocessor scan panicked: {message}"))
    })
}

/// Go executor.newClosestReadAdjuster. The number of tasks is known only
/// after region splitting; paging/retries retain the resulting request.
fn closest_read_adjuster(
    mode: tidb_txnkv::ReplicaReadType,
    bytes: f64,
    threshold: i64,
    zone: String,
) -> Option<Arc<dyn tidb_txnkv::CoprocessorRequestAdjuster>> {
    if mode != tidb_txnkv::ReplicaReadType::ClosestAdaptive {
        return None;
    }
    Some(Arc::new(
        move |request: &mut tidb_txnkv::Request, count: usize| {
            let per_task = bytes / count as f64;
            // Go amd64 converts non-finite/out-of-range float64 to MinInt64.
            let estimate = if !per_task.is_finite() || per_task >= 9_223_372_036_854_775_808.0 {
                i64::MIN
            } else {
                per_task as i64
            };
            if estimate >= threshold {
                request.match_store_labels.push(tidb_txnkv::StoreLabel {
                    key: "zone".to_owned(),
                    value: zone.clone(),
                });
                true
            } else {
                request.replica_read = tidb_txnkv::ReplicaReadType::Leader;
                false
            }
        },
    ))
}

fn open_scan(
    client: &dyn tidb_txnkv::Client,
    plan: RemoteScanPlan,
) -> Result<SelectResponseIter, String> {
    use prost::Message;

    let cancellation = plan.cancellation;
    let cop_response_sink = plan.cop_response_sink.clone();
    let memory_tracker = plan.memory_tracker.clone();
    // Go `SetFromSessionVars`, which EVERY read in `pkg/distsql` runs. The
    // zero-value builder this replaced sent `Concurrency: 0` and an EMPTY
    // `ResourceGroupName`, neither of which any TiDB sends: a stock session
    // is `tidb_distsql_scan_concurrency = 15` and resource group `default`.
    //
    // Copy the retained statement policy, including timeout/hint and busy
    // thresholds, rather than recreating session defaults on the worker.
    let mut context = DistSqlContext::new();
    context.request.dist_sql_concurrency = plan.dist_sql_scan_concurrency;
    context.request.store_batch_size = plan.store_batch_size;
    context.request.resource_group_name = plan.resource_group_name;
    context.request.replica_read = plan.replica_read;
    context.request.load_based_replica_read_threshold_ns = plan.read_policy.busy_threshold_ns;
    context.request.tikv_client_read_timeout_ms = plan.read_policy.read_timeout_ms;
    context.request.max_execution_time_ms = plan.max_execution_time_ms;
    context.request.priority = plan.priority;
    context.request.not_fill_cache = plan.not_fill_cache;
    context.request.query_cop_store_limiter = plan.query_cop_store_limiter;
    // Go `SetFromSessionVars` copies `MaxKeysRead` and the statement-wide
    // `MaxKeysReadCounter`; the counter is passed even without a limit so the
    // processed keys still reach `tidb_keys_examined`.
    context.request.max_keys_read = plan.keys_read.max();
    context.execution.max_keys_read_counter = Some(plan.keys_read.counter());
    if let Some(min_size) = plan.paging_min_size {
        // Go's buildIndexSelectResultForRange raises both paging bounds to
        // the worker's first handle batch. Keep the normal session defaults
        // for every request without this IndexLookUp hint.
        context.request.paging.min_size = context.request.paging.min_size.max(min_size);
        context.request.paging.max_size = context.request.paging.max_size.max(min_size);
    }
    let mut builder = RequestBuilder::from_context(&context);
    builder
        .set_read_replica_scope(plan.read_replica_scope)
        .set_is_staleness(plan.is_staleness);
    // Go `SetFromSessionVars`: `SetResourceGroupTagger(sv.StmtCtx.
    // GetResourceGroupTagger())`.
    if let Some(tagger) = &plan.kv_context.resource_group_tagger {
        builder.set_resource_group_tagger(tagger());
    }
    builder.set_closest_replica_read_adjuster(closest_read_adjuster(
        plan.replica_read,
        plan.estimated_net_bytes,
        plan.read_policy.closest_read_threshold,
        tidb_config::config_tree::config::get_txn_scope_from_config(),
    ));
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
        .map_err(|error| format!("{error:?}"))?
        .with_cop_lite_worker(plan.cop_lite_worker)
        .with_rpc_interceptor(plan.rpc_interceptor)
        .with_kv_context(plan.kv_context);
    let result = tidb_distsql::select_with_runtime_stats_over_client(
        client,
        &request,
        SelectInput::default(),
        // THE SESSION'S collector, not a fresh one: `response_channel`
        // appends TiKV's warnings in Go's order into whatever it is
        // given, and a fresh collector is dropped with them inside.
        QueryResultContext::new(plan.field_types.clone(), plan.warnings)
            .with_time_zone(plan.time_zone),
        plan.cop_plan_ids,
        plan.root_plan_id,
        true,
    )
    .map_err(|error| error.to_string())?;
    Ok(result
        .into_select_iter(Vec::new())
        .with_cop_response_sink(cop_response_sink)
        .with_memory_tracker(memory_tracker))
}

/// Go `getPhysicalPlanIDs`: the plan ID of every executor of `dag`, in DAG
/// order, and the plan whose select-result stats the read registers.
///
/// Go lowers the reader's pushed plan one node per executor, so the nth ID
/// is the nth executor's. This tier builds the DAG from a scan request, which
/// may carry fewer executors than the plan has nodes (a Selection whose
/// conditions did not all travel is kept locally), so each executor takes the
/// next pushed node of its own kind and an executor with none gets `0`, which
/// records nothing.
fn cop_plan_ids_for(
    dag: &tidb_proto::tipb::DagRequest,
    statement: &tidb_distsql::remote_scan::PushdownStatementContext,
) -> (Vec<isize>, isize) {
    let mut ids = vec![0; dag.executors.len()];
    if statement.cop_plan_chain.is_empty() {
        if let Some(scan_id) = ids.first_mut() {
            *scan_id = statement.plan_id;
        }
        return (ids, statement.plan_id);
    }
    let mut next = 0;
    for (slot, executor) in ids.iter_mut().zip(&dag.executors) {
        let kind = match executor.tp.and_then(|tp| ExecType::try_from(tp).ok()) {
            Some(ExecType::TypeTableScan) => CopPlanKind::TableScan,
            Some(ExecType::TypeIndexScan) => CopPlanKind::IndexScan,
            Some(ExecType::TypeSelection) => CopPlanKind::Selection,
            Some(ExecType::TypeLimit) => CopPlanKind::Limit,
            Some(ExecType::TypeTopN) => CopPlanKind::TopN,
            Some(ExecType::TypeAggregation | ExecType::TypeStreamAgg) => CopPlanKind::Aggregation,
            _ => CopPlanKind::Other,
        };
        if let Some(offset) = statement.cop_plan_chain[next..]
            .iter()
            .position(|node| node.kind == kind)
        {
            *slot = statement.cop_plan_chain[next + offset].id;
            next += offset + 1;
        }
    }
    let root = if statement.cop_root_plan_id > 0 {
        statement.cop_root_plan_id
    } else {
        statement.plan_id
    };
    (ids, root)
}

/// The caller's end of one coprocessor scan.
struct CopRowStream {
    /// Dropping this closes the response and cancels unread region work.
    iter: Option<SelectResponseIter>,
    pending: Option<Chunk>,
    pending_row: usize,
    field_types: Vec<FieldType>,
    returned: u64,
    predicates_applied: bool,
    plan_id: isize,
    exhausted: bool,
}

impl CopRowStream {
    fn pull_chunk(&mut self, required_rows: usize) -> Result<Option<Chunk>, StorageError> {
        if self.exhausted {
            return Ok(None);
        }
        loop {
            let Some(iter) = self.iter.as_mut() else {
                return Ok(None);
            };
            let batch = iter
                .next_chunk_with_required_rows(required_rows.max(1))
                .map_err(|error| match error {
                    tidb_copr::response_channel::ResponseChannelError::SelectResponse { code, message } => {
                        StorageError::Sql(tidb_dbterror::ErrClass::tikv_select_response_error(
                            code as u16,
                            message,
                        ))
                    }
                    other => StorageError::Backend(other.to_string()),
                })?;
            let Some(batch) = batch else {
                self.exhausted = true;
                return Ok(None);
            };
            if batch.row.num_rows() == 0 {
                continue;
            }
            scan_receipt::add_wire_rows(batch.row.num_rows() as u64);
            return Ok(Some(batch.row));
        }
    }
}

impl PushdownRowStream for CopRowStream {
    fn cop_count_and_rows(&self) -> (u64, u64) {
        self.iter.as_ref().map_or((0, 0), |iter| {
            iter.runtime_stats().cop_count_and_rows(self.plan_id)
        })
    }
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
            match self.pull_chunk(1)? {
                Some(batch) => self.pending = Some(batch),
                None => return Ok(None),
            }
        }
    }

    fn supports_chunks(&self) -> bool {
        true
    }

    fn next_chunk(&mut self, required_rows: usize) -> Result<Option<Chunk>, StorageError> {
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
        match self.pull_chunk(required_rows)? {
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
        self.exhausted = true;
        self.pending = None;
        self.pending_row = 0;
    }
}

impl Drop for CopRowStream {
    fn drop(&mut self) {
        self.close();
    }
}

/// Go `util.ColumnToProto`: the coprocessor descriptor of one scanned column,
/// or `None` for a type with no stable TiPB interpretation.
pub fn scan_column(column: &PushdownScanColumn) -> Option<ScanColumnInfo> {
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
    // (`tidb_executor::wide_scan_selection`), so the leaf and the scan descriptor
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
    let charset = tidb_charset::get_collation_by_name(&collation)
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

fn lower_aggregate_functions(
    functions: &[tidb_distsql::remote_scan::PushdownAggregateFunction],
    columns: &[ScanColumnInfo],
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Vec<Expr>> {
    functions
        .iter()
        .map(|function| {
            lower_aggregate_function(
                function.kind,
                function.input.as_ref(),
                &function.output_type,
                columns,
                zone,
            )
        })
        .collect()
}

fn lower_aggregate_function(
    kind: PushdownAggregateKind,
    input: Option<&tidb_expr::expression::Expression>,
    output_type: &FieldType,
    columns: &[ScanColumnInfo],
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Expr> {
    let children = match input {
        Some(input) => vec![tidb_expr::pushdown_catalog::expression_to_pb_in(
            input,
            &|offset| scan_column_descriptor(columns, offset),
            zone,
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
        PushdownAggregateKind::MinCount => ExprType::MinCount,
        PushdownAggregateKind::MaxCount => ExprType::MaxCount,
    };
    Some(Expr {
        tp: Some(tp as i32),
        val: None,
        children,
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: Some(tidb_expr::pushdown_catalog::field_type_to_pb(output_type)?),
        has_distinct: Some(false),
        agg_func_mode: Some(tidb_proto::tipb::AggFunctionMode::Partial1Mode as i32),
        ..Default::default()
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
    use super::*;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;

    #[test]
    fn adaptive_read_threshold_is_per_task_and_preserves_existing_labels() {
        for (bytes, tasks, hit) in [
            (8191.9, 2, false),
            (8192.0, 2, true),
            (8192.0, 3, false),
            (0.0, 0, false),
        ] {
            let mut request = tidb_txnkv::Request::default();
            request.replica_read = tidb_txnkv::ReplicaReadType::ClosestAdaptive;
            request.match_store_labels.push(tidb_txnkv::StoreLabel {
                key: "rack".into(),
                value: "r1".into(),
            });
            let adjuster =
                closest_read_adjuster(request.replica_read, bytes, 4096, "zone-a".into()).unwrap();
            assert_eq!(adjuster.adjust(&mut request, tasks), hit);
            assert_eq!(request.match_store_labels.len(), if hit { 2 } else { 1 });
            assert_eq!(
                request.replica_read,
                if hit {
                    tidb_txnkv::ReplicaReadType::ClosestAdaptive
                } else {
                    tidb_txnkv::ReplicaReadType::Leader
                }
            );
            if hit {
                assert_eq!(request.match_store_labels[1].value, "zone-a");
            }
        }
        for mode in [
            tidb_txnkv::ReplicaReadType::Leader,
            tidb_txnkv::ReplicaReadType::Follower,
            tidb_txnkv::ReplicaReadType::Closest,
        ] {
            assert!(closest_read_adjuster(mode, 1e9, 1, "z".into()).is_none());
        }
    }

    #[test]
    fn registered_storage_errors_reach_coprocessor_rows_and_chunks() {
        use tidb_txnkv::QueryResultSubset;
        use tidb_txnkv::QueryResponseError;

        struct Failure;
        impl tidb_txnkv::Response for Failure {
            fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
                Err(QueryResponseError::Sql {
                    code: 9004,
                    message: "[tikv:9004]Resolve lock timeout".to_owned(),
                })
            }

            fn close(&mut self) {}
        }

        for chunk in [false, true] {
            let field_types = vec![FieldType::new(FieldTypeCode::LongLong)];
            let iter = SelectResponseIter::from_query_response(
                Box::new(Failure),
                field_types.clone(),
                Vec::new(),
                tidb_datatype::SessionTimeZone::utc(),
                WarningCollector::new(),
                tidb_distsql::select_result_metadata(SelectInput::default()),
                None,
            );
            let mut stream = CopRowStream {
                iter: Some(iter),
                pending: None,
                pending_row: 0,
                field_types,
                returned: 0,
                predicates_applied: false,
                plan_id: 0,
                exhausted: false,
            };
            let error = if chunk {
                stream.next_chunk(1).unwrap_err()
            } else {
                stream.next_row().unwrap_err()
            };
            match error {
                StorageError::Sql(error) => {
                    assert_eq!(error.code, 9004);
                    assert_eq!(error.state, *b"HY000");
                    assert_eq!(error.message, "[tikv:9004]Resolve lock timeout");
                }
                other => panic!("storage error lost its SQL identity: {other:?}"),
            }
            assert_eq!(stream.rows_returned(), 0);
            stream.close();
            assert!(stream.next_row().unwrap().is_none());
        }
    }

    #[test]
    fn max_min_count_uses_the_go_tipb_aggregate_enums() {
        let mut column = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        column.index = 0;
        let input = Expression::Column(column);
        let columns = [ScanColumnInfo {
            column_id: 1,
            tp: 8,
            collation: 63,
            column_len: 20,
            ..ScanColumnInfo::default()
        }];

        let max = lower_aggregate_function(
            PushdownAggregateKind::MaxCount,
            Some(&input),
            &FieldType::new(FieldTypeCode::LongLong),
            &columns,
            &tidb_datatype::SessionTimeZone::utc(),
        )
        .expect("MAX_COUNT lowers to a tipb aggregate expression");
        let min = lower_aggregate_function(
            PushdownAggregateKind::MinCount,
            Some(&input),
            &FieldType::new(FieldTypeCode::LongLong),
            &columns,
            &tidb_datatype::SessionTimeZone::utc(),
        )
        .expect("MIN_COUNT lowers to a tipb aggregate expression");
        assert_eq!(max.tp, Some(ExprType::MaxCount as i32));
        assert_eq!(min.tp, Some(ExprType::MinCount as i32));
        assert_eq!(ExprType::from_i32(3023), Some(ExprType::MaxCount));
        assert_eq!(ExprType::from_i32(3022), Some(ExprType::MinCount));
    }
}

#[cfg(test)]
mod cop_plan_ids_tests {
    use super::*;
    use tidb_distsql::remote_scan::PushdownStatementContext;
    use tidb_distsql::remote_scan::CopPlanId;
    use tidb_proto::tipb::{DagRequest, Executor};

    fn dag(kinds: &[ExecType]) -> DagRequest {
        DagRequest {
            executors: kinds
                .iter()
                .map(|kind| Executor {
                    tp: Some(*kind as i32),
                    ..Executor::default()
                })
                .collect(),
            ..DagRequest::default()
        }
    }

    fn chain(nodes: &[(CopPlanKind, isize)]) -> Vec<CopPlanId> {
        nodes
            .iter()
            .map(|(kind, id)| CopPlanId {
                kind: *kind,
                id: *id,
            })
            .collect()
    }

    /// Go `getPhysicalPlanIDs`: the nth pushed node is the nth executor.
    #[test]
    fn executors_take_their_pushed_nodes_in_order() {
        let statement = PushdownStatementContext::default().with_cop_plan_chain(
            chain(&[
                (CopPlanKind::TableScan, 5),
                (CopPlanKind::Selection, 6),
                (CopPlanKind::TopN, 16),
            ]),
            7,
        );
        let ids = cop_plan_ids_for(
            &dag(&[
                ExecType::TypeTableScan,
                ExecType::TypeSelection,
                ExecType::TypeTopN,
            ]),
            &statement,
        );
        assert_eq!(ids, (vec![5, 6, 16], 7));
    }

    /// A Selection that did not travel (kept locally) leaves its node
    /// without an executor: the executors after it still find theirs.
    #[test]
    fn a_node_without_an_executor_is_skipped() {
        let statement = PushdownStatementContext::default().with_cop_plan_chain(
            chain(&[
                (CopPlanKind::TableScan, 5),
                (CopPlanKind::Selection, 6),
                (CopPlanKind::Limit, 9),
            ]),
            7,
        );
        let ids = cop_plan_ids_for(
            &dag(&[ExecType::TypeTableScan, ExecType::TypeLimit]),
            &statement,
        );
        assert_eq!(ids, (vec![5, 9], 7));
        // An executor with no node of its kind reports under no plan.
        let ids = cop_plan_ids_for(
            &dag(&[ExecType::TypeTableScan, ExecType::TypeTopN]),
            &statement,
        );
        assert_eq!(ids, (vec![5, 0], 7));
    }

    /// Without a chain the scan keeps its own plan as the root, as before.
    #[test]
    fn a_request_without_a_chain_reports_the_scan_only() {
        let statement = PushdownStatementContext::default().with_plan_id(5);
        let ids = cop_plan_ids_for(
            &dag(&[ExecType::TypeTableScan, ExecType::TypeSelection]),
            &statement,
        );
        assert_eq!(ids, (vec![5, 0], 5));
    }
}
