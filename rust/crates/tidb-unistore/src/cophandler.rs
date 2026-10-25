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

//! Go `pkg/store/mockstore/unistore/cophandler/cop_handler.go` — the
//! coprocessor's front door: request dispatch, DAG decode, and the
//! flat-list-to-tree executor conversion.
//!
//! SEED of `cophandler` (~5k lines): request parsing plus the ordinary table
//! and index scan DAG composition land here, with the pushed-down bounded
//! sort (TopN: leaf order-by keys, per-key direction, heap size). Analyze
//! and the remaining closure-executor expressions refuse by name until they
//! land; checksum answers Go's fixed placeholder stub.
//!
//! # Narrowings, by name
//!
//! * `flagsAndTzToSessionContext` / `globalLocationMap`: the time-zone name
//!   resolves through Go's location cache into a `time.Location`. The parsed
//!   name and offset resolve into the request-owned expression context so
//!   selection, aggregation and TopN use the same zone.
//! * `mppCtx` / `HandleMPPDAGReq`: the MPP arm follows the MPP course.

mod analyze;
mod eval_context;

#[cfg(test)]
#[path = "cophandler/region_group_tests.rs"]
mod region_group_tests;

use eval_context::{RequestEvalContext, SharedExpression};
use std::sync::Arc;

use prost::Message;
use tidb_proto::coprocessor;
use tidb_proto::tipb;

use tidb_codec::table_key::RecordHandle;

use crate::mvcc_store::MvccStore;
use tidb_hack::{go_to_lower, go_to_upper};

/// Go `kv.ReqTypeDAG` / `ReqTypeAnalyze` / `ReqTypeChecksum`
/// (`pkg/kv/kv.go:375-377`).
pub const REQ_TYPE_DAG: i64 = 103;
/// See [`REQ_TYPE_DAG`].
pub const REQ_TYPE_ANALYZE: i64 = 104;
/// See [`REQ_TYPE_DAG`].
pub const REQ_TYPE_CHECKSUM: i64 = 105;

/// Go `dagContext`, the parse half: what `buildDAG` establishes before any
/// executor runs.
#[derive(Debug)]
pub struct DagContext {
    /// Go `dagContext` inherits the session's `DivPrecisionIncrement`,
    /// defaulted to `variable.DefDivPrecisionIncrement` (4) when the DAG
    /// request omits it. `DivideDecimal` widens its result fraction by it.
    pub div_precision_increment: i64,
    /// The decoded `tipb.DAGRequest`.
    pub dag_req: tipb::DagRequest,
    /// `keyRanges` from the coprocessor request.
    pub key_ranges: Vec<coprocessor::KeyRange>,
    /// `startTS` — Go reads `req.StartTs`.
    pub start_ts: u64,
    /// Go `req.PagingSize`: the rows one page may return before the scan
    /// stops and answers its continuation range (`mppExecute`).
    pub paging_size: u64,
    /// The request's time zone, resolved when building its expression context.
    pub time_zone: TimeZoneSpec,
    /// Go `DAGRequest.Flags`: controls error-tolerance in expression
    /// evaluation (truncate-as-warning, ignore-truncate, etc.).
    pub flags: u64,
    expression_context: Arc<RequestEvalContext>,
}

/// Go `buildDAG`'s three-way time-zone switch, as DATA: empty name is a
/// fixed offset from UTC, "System" is the process zone, anything else is a
/// named location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimeZoneSpec {
    /// `time.FixedZone("UTC", offset)`.
    FixedOffset(i64),
    /// `time.Local`.
    System,
    /// `time.LoadLocation(name)`, resolution deferred.
    Named(String),
}

impl TimeZoneSpec {
    fn resolve(&self) -> Result<tidb_datatype::SessionTimeZone, String> {
        match self {
            Self::FixedOffset(offset) => Ok(tidb_datatype::SessionTimeZone::Fixed {
                name: "UTC".to_owned(),
                offset_secs: i32::try_from(*offset)
                    .map_err(|_| format!("invalid time zone offset {offset}"))?,
            }),
            Self::System => Ok(tidb_datatype::SessionTimeZone::Local),
            Self::Named(name) => name
                .parse::<chrono_tz::Tz>()
                .map(tidb_datatype::SessionTimeZone::Named)
                .map_err(|error| error.to_string()),
        }
    }
}

/// Go `HandleCopRequest` (`cop_handler.go`): the type dispatch. The unknown
/// arm's message is Go's exact `fmt.Sprintf`.
pub fn handle_cop_request(
    store: &mut MvccStore,
    req: &coprocessor::Request,
) -> coprocessor::Response {
    match req.tp {
        REQ_TYPE_DAG => handle_cop_dag_request(store, req),
        REQ_TYPE_ANALYZE => analyze::handle(store, req),
        // Go `handleCopChecksumRequest` (`cop_handler.go:759`): a marshalled
        // `tipb.ChecksumResponse{Checksum:1, TotalKvs:1, TotalBytes:1}` --
        // unistore never computes real checksums.
        REQ_TYPE_CHECKSUM => coprocessor::Response {
            data: prost::Message::encode_to_vec(&tipb::ChecksumResponse {
                checksum: Some(1),
                total_kvs: Some(1),
                total_bytes: Some(1),
            })
            .into(),
            ..coprocessor::Response::default()
        },
        other => other_error(&format!("unsupported request type {other}")),
    }
}

fn other_error(message: &str) -> coprocessor::Response {
    coprocessor::Response {
        other_error: message.to_owned(),
        ..coprocessor::Response::default()
    }
}

/// A coprocessor executor's `error` as Go's `toPBError` reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CopExecError {
    /// A TiDB terror, which keeps its MySQL code and message
    /// (`terror.ToSQLError`).
    Sql {
        /// The MySQL error number.
        code: u16,
        /// The rendered message.
        message: String,
    },
    /// Any other error: code 1 and the error's text.
    Other(String),
}

impl CopExecError {
    /// Go `toPBError`.
    fn to_pb_error(&self) -> tipb::Error {
        let (code, message) = match self {
            CopExecError::Sql { code, message } => (i32::from(*code), message.clone()),
            CopExecError::Other(message) => (1, message.clone()),
        };
        tipb::Error {
            code: Some(code),
            msg: Some(message),
        }
    }

    fn message(&self) -> &str {
        match self {
            CopExecError::Sql { message, .. } | CopExecError::Other(message) => message,
        }
    }
}

impl From<tidb_expr::EvalError> for CopExecError {
    fn from(error: tidb_expr::EvalError) -> Self {
        let error = error.to_sql_error();
        CopExecError::Sql {
            code: error.code,
            message: error.message,
        }
    }
}

impl From<String> for CopExecError {
    fn from(message: String) -> Self {
        CopExecError::Other(message)
    }
}

impl From<CopExecError> for String {
    fn from(error: CopExecError) -> Self {
        match error {
            CopExecError::Sql { message, .. } | CopExecError::Other(message) => message,
        }
    }
}

/// Go `genRespWithMPPExec` for a failed executor: no chunks, the error in
/// `SelectResponse.error` (`toPBError`) beside the warnings so far.
fn executor_error_response(
    error: &CopExecError,
    warnings: Vec<tipb::Error>,
) -> coprocessor::Response {
    let warning_count = warnings.len() as i64;
    let select = tipb::SelectResponse {
        error: Some(error.to_pb_error()),
        warnings,
        warning_count: Some(warning_count),
        encode_type: Some(tipb::EncodeType::TypeDefault as i32),
        ..tipb::SelectResponse::default()
    };
    coprocessor::Response {
        data: select.encode_to_vec().into(),
        ..coprocessor::Response::default()
    }
}

/// Go's `%v` rendering of a `[]byte`: space-separated decimals in brackets.
fn go_byte_slice(bytes: &[u8]) -> String {
    let rendered = bytes
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{rendered}]")
}

/// Go `handleCopDAGRequest`'s parse half: `buildDAG`'s guards and decode,
/// with execution refusing by name.
fn handle_cop_dag_request(
    _store: &mut MvccStore,
    req: &coprocessor::Request,
) -> coprocessor::Response {
    let context = match build_dag(req) {
        Ok(context) => context,
        Err(message) => return other_error(&message),
    };
    let mut response = exec_dag(_store, &context);
    let warnings = context.expression_context.take_warnings();
    if !warnings.is_empty() && response.other_error.is_empty() && response.locked.is_none() {
        let mut select = match tipb::SelectResponse::decode(response.data.as_ref()) {
            Ok(select) => select,
            Err(error) => {
                return other_error(&format!("decode coprocessor warnings response: {error}"));
            }
        };
        select.warning_count = Some(select.warning_count.unwrap_or(0) + warnings.len() as i64);
        select
            .warnings
            .extend(warnings.into_iter().map(|(code, message)| tipb::Error {
                code: Some(i32::from(code)),
                msg: Some(message),
            }));
        response.data = select.encode_to_vec().into();
    }
    response
}

/// The execution slice of Go `buildClosureExecutor` + `handleCopDAGRequest`
/// (`closure_exec.go`): a TABLE-SCAN-ONLY DAG runs; anything above the scan
/// refuses by name until its course lands.
fn exec_dag(store: &mut MvccStore, context: &DagContext) -> coprocessor::Response {
    let started = std::time::Instant::now();
    validate_executor_list(&context.dag_req.executors);
    // Go validates every request range while clipping it against the region
    // (`extractKVRanges`, `cop_handler.go:675`); the whole-keyspace engine
    // has no region bounds to clip to, but the malformed-range rejection is
    // wire-visible and kept verbatim, Go's `%v` byte-slice rendering
    // included.
    for range in &context.key_ranges {
        if range.start >= range.end {
            return other_error(&format!(
                "invalid range, start should be smaller than end: {} {}",
                go_byte_slice(&range.start),
                go_byte_slice(&range.end)
            ));
        }
    }
    // Go's composition contract (`closure_exec.go:166`):
    // `tableScan|indexScan [selection] [topN | limit | agg]`. This slice
    // runs the scan and a LIMIT above it — Go's limit is nothing but a
    // break-at-count during the scan (`ce.limit`, `closure_exec.go:144`,
    // checked at `:597`).
    let mut limit = usize::MAX;
    let mut conditions: Vec<SimpleExpr> = Vec::new();
    let mut aggregation: Option<&tipb::Aggregation> = None;
    let mut topn: Option<TopNSpec> = None;
    for above in &context.dag_req.executors[1..] {
        if above.tp() == tipb::ExecType::TypeLimit {
            let Some(body) = above.limit.as_ref() else {
                return other_error("executor missing limit body");
            };
            limit = usize::try_from(body.limit()).unwrap_or(usize::MAX);
        } else if above.tp() == tipb::ExecType::TypeSelection {
            let Some(body) = above.selection.as_ref() else {
                return other_error("executor missing selection body");
            };
            for condition in &body.conditions {
                match convert_expr_with_context(condition, &context.expression_context) {
                    Ok(expr) => conditions.push(expr),
                    Err(message) => return other_error(&message),
                }
            }
        } else if above.tp() == tipb::ExecType::TypeTopN {
            let Some(body) = above.top_n.as_ref() else {
                return other_error("executor missing topN body");
            };
            topn = Some(match TopNSpec::build(body, &context.expression_context) {
                Ok(spec) => spec,
                Err(message) => return other_error(&message),
            });
        } else if above.tp() == tipb::ExecType::TypeAggregation
            || above.tp() == tipb::ExecType::TypeStreamAgg
        {
            let Some(body) = above.aggregation.as_ref() else {
                return other_error("executor missing aggregation body");
            };
            aggregation = Some(body);
        } else {
            return other_error("this closure-executor shape is a later course of this port");
        }
    }
    if aggregation.is_some() && (limit != usize::MAX || topn.is_some()) {
        // Go's closure executor composes them; this port's one lowering
        // never builds the pair, so the combination refuses by name rather
        // than guessing which of Go's two cap positions was meant.
        return other_error("aggregation over a row cap is a later course of this port");
    }
    let scan = &context.dag_req.executors[0];
    if scan.tp() == tipb::ExecType::TypeIndexScan {
        let Some(idx_scan) = scan.idx_scan.as_ref() else {
            return other_error("executor missing idx_scan body");
        };
        let mut produced = ExecutorRows::default();
        let response = exec_index_scan(
            store,
            context,
            idx_scan,
            &conditions,
            limit,
            aggregation,
            topn.as_ref(),
            &mut produced,
        );
        return finish_dag_response(response, context, &produced, started.elapsed());
    }
    if scan.tp() != tipb::ExecType::TypeTableScan {
        return other_error("index scans (closure_exec.go) are a later course of this port");
    }
    let Some(tbl_scan) = scan.tbl_scan.as_ref() else {
        return other_error("executor missing tbl_scan body");
    };
    let mut produced = ExecutorRows::default();
    let response = exec_table_scan(
        store,
        context,
        tbl_scan,
        &conditions,
        limit,
        aggregation,
        topn.as_ref(),
        &mut produced,
    );
    finish_dag_response(response, context, &produced, started.elapsed())
}

/// The rows each executor of the closure shape produced, Go's
/// `execDetail.numProducedRows` per `mppExec`: the scan's every row, the
/// selection's survivors, and what the limit, topN or aggregation above
/// them emitted.
#[derive(Default)]
struct ExecutorRows {
    scanned: u64,
    selected: u64,
    output: u64,
}

/// Go `genRespWithMPPExec`'s tail: the scan detail, and -- when the request
/// collects them -- one execution summary per executor, each charged the
/// whole request's duration ("for simplicity, we assume all executors to be
/// spending the same amount of time as the request") with no iterations.
fn finish_dag_response(
    response: coprocessor::Response,
    context: &DagContext,
    produced: &ExecutorRows,
    elapsed: std::time::Duration,
) -> coprocessor::Response {
    let mut response = with_scan_detail(response, produced.scanned, elapsed);
    if !context.dag_req.collect_execution_summaries()
        || !response.other_error.is_empty()
        || response.locked.is_some()
    {
        return response;
    }
    let mut select = match tipb::SelectResponse::decode(response.data.as_ref()) {
        Ok(select) => select,
        Err(error) => return other_error(&format!("decode coprocessor response: {error}")),
    };
    let time_processed_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    select.execution_summaries = context
        .dag_req
        .executors
        .iter()
        .map(|executor| {
            let rows = match executor.tp() {
                tipb::ExecType::TypeTableScan | tipb::ExecType::TypeIndexScan => produced.scanned,
                tipb::ExecType::TypeSelection => produced.selected,
                _ => produced.output,
            };
            tipb::ExecutorExecutionSummary {
                time_processed_ns: Some(time_processed_ns),
                num_produced_rows: Some(rows),
                num_iterations: Some(0),
                ..tipb::ExecutorExecutionSummary::default()
            }
        })
        .collect();
    response.data = select.encode_to_vec().into();
    response
}

/// Go `buildResp`: every DAG response carries `ExecDetailsV2` whose
/// `ScanDetailV2.ProcessedVersions` is the rows the table and index scan
/// executors produced -- what TiDB's `copIterator` adds up against
/// `tidb_max_keys_read` and into `tidb_keys_examined`.
fn with_scan_detail(
    mut response: coprocessor::Response,
    scanned_rows: u64,
    elapsed: std::time::Duration,
) -> coprocessor::Response {
    // Go's `genRespWithMPPExec` reports the request's wall time at
    // millisecond resolution in both the legacy and the V2 detail.
    let time_detail = tidb_proto::kvrpcpb::TimeDetail {
        process_wall_time_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        ..tidb_proto::kvrpcpb::TimeDetail::default()
    };
    response.exec_details = Some(tidb_proto::kvrpcpb::ExecDetails {
        time_detail: Some(time_detail),
        ..tidb_proto::kvrpcpb::ExecDetails::default()
    });
    response.exec_details_v2 = Some(tidb_proto::kvrpcpb::ExecDetailsV2 {
        time_detail: Some(time_detail),
        scan_detail_v2: Some(tidb_proto::kvrpcpb::ScanDetailV2 {
            processed_versions: scanned_rows,
            ..tidb_proto::kvrpcpb::ScanDetailV2::default()
        }),
        ..tidb_proto::kvrpcpb::ExecDetailsV2::default()
    });
    response
}

/// Go `indexScanProcessor` and `indexScanProcessCore`: decode every index
/// entry into the executor schema, then apply the same optional Selection,
/// Limit, and partial Aggregation processors as a table scan. Handles are
/// recovered from the key suffix for non-unique indexes and from the value
/// for unique indexes by `DecodeIndexKV`, including common handles and
/// restored collation data.
fn exec_index_scan(
    store: &mut MvccStore,
    context: &DagContext,
    idx_scan: &tipb::IndexScan,
    conditions: &[SimpleExpr],
    limit: usize,
    aggregation: Option<&tipb::Aggregation>,
    topn: Option<&TopNSpec>,
    produced: &mut ExecutorRows,
) -> coprocessor::Response {
    use tidb_datatype::Datum;
    const EXTRA_HANDLE_ID: i64 = -1;
    const EXTRA_PHYSICAL_TABLE_ID: i64 = -3;

    let timezone = match context.time_zone.resolve() {
        Ok(timezone) => timezone,
        Err(error) => return other_error(&error),
    };
    let field_types = idx_scan
        .columns
        .iter()
        .map(field_type_from_pb_column)
        .collect::<Vec<_>>();
    let column_infos = idx_scan
        .columns
        .iter()
        .zip(&field_types)
        .filter(|(column, _)| column.column_id() != EXTRA_HANDLE_ID)
        .map(|(column, field_type)| tidb_codec::ColumnInfo {
            id: column.column_id(),
            is_pk_handle: column.pk_handle(),
            virtual_generated: false,
            field_type: field_type.clone(),
        })
        .collect::<Vec<_>>();

    // Go `initIdxScanCtx`: `columnLen` counts only index-key columns. The
    // trailing handle columns are materialized separately by DecodeIndexKV.
    let has_physical_id = idx_scan
        .columns
        .last()
        .is_some_and(|column| column.column_id() == EXTRA_PHYSICAL_TABLE_ID);
    let mut schema_len = idx_scan.columns.len() - usize::from(has_physical_id);
    let handle_status = if !idx_scan.primary_column_ids.is_empty() {
        schema_len = schema_len.saturating_sub(idx_scan.primary_column_ids.len());
        tidb_tablecodec::HandleStatus::Default
    } else if let Some(handle) = schema_len
        .checked_sub(1)
        .and_then(|offset| idx_scan.columns.get(offset))
        .filter(|column| column.pk_handle() || column.column_id() == EXTRA_HANDLE_ID)
    {
        schema_len -= 1;
        if field_type_from_pb_column(handle).is_unsigned() {
            tidb_tablecodec::HandleStatus::Unsigned
        } else {
            tidb_tablecodec::HandleStatus::Default
        }
    } else {
        tidb_tablecodec::HandleStatus::NotNeeded
    };

    let mut aggregator = match aggregation {
        Some(aggregation) => {
            match RegionAggregator::build(aggregation, &context.expression_context) {
                Ok(aggregator) => Some(aggregator),
                Err(message) => return other_error(&message),
            }
        }
        None => None,
    };
    let mut rows = Vec::new();
    let mut eval_warnings = Vec::new();
    let mut emitted = 0_usize;
    // Go keeps the kept rows in a bounded heap; this lowering buffers every
    // surviving row and sorts once -- the same N rows in the same key order,
    // without the intermediate evictions.
    let mut topn_rows: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::new();
    let output_types = dag_output_field_types(context, &field_types, aggregation);
    if aggregation.is_none() && limit == 0 {
        return encode_select_rows(
            rows,
            output_types.as_deref(),
            context,
            &timezone,
            std::mem::take(&mut eval_warnings),
        );
    }
    let descending = idx_scan.desc();
    let mut paging = PagingCut::new(context, descending, aggregator.is_some() || topn.is_some());
    let mut page_end: Option<coprocessor::KeyRange> = None;
    let mut ranges = context.key_ranges.iter().collect::<Vec<_>>();
    if descending {
        ranges.reverse();
    }

    'ranges: for range in ranges {
        let (start_key, end_key) = if descending {
            (range.end.clone(), range.start.clone())
        } else {
            (range.start.clone(), range.end.clone())
        };
        let pairs = store.scan(&crate::mvcc_store::ScanReq {
            start_key,
            end_key,
            limit: u32::MAX,
            version: context.start_ts,
            sample_step: 0,
            reverse: descending,
        });
        for pair in pairs {
            if let Some(page) = paging.as_mut() {
                if let Some(range) = page.before_key() {
                    page_end = Some(range);
                    break 'ranges;
                }
                page.scanned(&pair.key);
            }
            if let Some(err) = pair.error {
                if let crate::mvcc_store::KvError::Locked(lock) = *err {
                    return coprocessor::Response {
                        locked: Some(*lock),
                        ..coprocessor::Response::default()
                    };
                }
                return other_error(&format!("scan error: {err:?}"));
            }
            let encoded = match tidb_tablecodec::decode_index_kv(
                true,
                &pair.key,
                &pair.value,
                schema_len,
                handle_status,
                &column_infos,
            ) {
                Ok(values) => values,
                Err(error) => return other_error(&format!("invalid index entry: {error:?}")),
            };
            let mut row = Vec::with_capacity(idx_scan.columns.len());
            for (offset, value) in encoded.iter().enumerate() {
                let Some(field_type) = field_types.get(offset) else {
                    return other_error("decoded index entry exceeds its executor schema");
                };
                match tidb_tablecodec::decode_column_value(value, field_type, Some(&timezone)) {
                    Ok(value) => row.push(value),
                    Err(error) => {
                        return other_error(&format!("invalid index column: {error:?}"));
                    }
                }
            }
            if has_physical_id && row.len() < idx_scan.columns.len() {
                row.push(Datum::Int(tidb_codec::decode_table_id(&pair.key)));
            }
            if row.len() != idx_scan.columns.len() {
                return other_error("decoded index entry does not match its executor schema");
            }
            // Go `indexScanExec.rowCnt`: every entry the scan produced.
            produced.scanned += 1;
            match conditions_hold(conditions, &row) {
                Ok(true) => produced.selected += 1,
                Ok(false) => continue,
                Err(error) => {
                    if context.flags & 2 != 0 {
                        eval_warnings.push(tipb::Error {
                            code: Some(1265),
                            msg: Some(error.message().to_owned()),
                        });
                        continue;
                    }
                    return executor_error_response(&error, eval_warnings);
                }
            }
            if let Some(aggregator) = aggregator.as_mut() {
                if let Err(message) = aggregator.update(&row) {
                    return other_error(&message);
                }
                continue;
            }
            // Go `topNProcessor`: the surviving row's sort keys are evaluated
            // NOW and the row rides the heap; the replay emits the kept rows
            // in key order. Buffering the whole set and sorting is that same
            // answer without the heap.
            if let Some(spec) = topn {
                let keys = match spec.evaluate(&row) {
                    Ok(keys) => keys,
                    Err(message) => return other_error(&message),
                };
                let projected = if context.dag_req.output_offsets.is_empty() {
                    row
                } else {
                    let mut projected = Vec::with_capacity(context.dag_req.output_offsets.len());
                    for offset in &context.dag_req.output_offsets {
                        let Some(value) = row.get(*offset as usize) else {
                            return other_error(&format!(
                                "output offset {offset} is outside the scanned columns"
                            ));
                        };
                        projected.push(value.clone());
                    }
                    projected
                };
                topn_rows.push((projected, keys));
                continue;
            }
            let projected = if context.dag_req.output_offsets.is_empty() {
                row
            } else {
                let mut projected = Vec::with_capacity(context.dag_req.output_offsets.len());
                for offset in &context.dag_req.output_offsets {
                    let Some(value) = row.get(*offset as usize) else {
                        return other_error(&format!(
                            "output offset {offset} is outside the scanned columns"
                        ));
                    };
                    projected.push(value.clone());
                }
                projected
            };
            rows.push(projected);
            emitted += 1;
            if let Some(page) = paging.as_mut() {
                page.produced_row();
            }
            if emitted == limit {
                break 'ranges;
            }
        }
    }
    if let Some(spec) = topn {
        let topn_rows = go_topn_heap_finish(&spec.keys, topn_rows, spec.limit);
        for (projected, _) in topn_rows {
            rows.push(projected);
            emitted += 1;
            if emitted == limit {
                break;
            }
        }
    }
    if let Some(aggregator) = aggregator {
        for row in aggregator.finish() {
            if context.dag_req.output_offsets.is_empty() {
                rows.push(row);
                continue;
            }
            let mut projected = Vec::with_capacity(context.dag_req.output_offsets.len());
            for offset in &context.dag_req.output_offsets {
                let Some(value) = row.get(*offset as usize) else {
                    return other_error(&format!(
                        "output offset {offset} is outside the aggregate schema"
                    ));
                };
                projected.push(value.clone());
            }
            rows.push(projected);
        }
    }
    produced.output = rows.len() as u64;
    let mut response = encode_select_rows(
        rows,
        output_types.as_deref(),
        context,
        &timezone,
        eval_warnings,
    );
    if let Some(page) = paging {
        response.range = Some(page_end.unwrap_or_else(|| page.finish(&context.key_ranges)));
    }
    response
}

/// Go `closureExecutor`'s output field types (`closure_exec.go:112`): the
/// scan's columns, or with an aggregation its results then its group keys,
/// picked by `output_offsets`. `None` when the request leaves a type out,
/// which only the default row encoding can answer without.
fn dag_output_field_types(
    context: &DagContext,
    scan_types: &[tidb_datatype::FieldType],
    aggregation: Option<&tipb::Aggregation>,
) -> Option<Vec<tidb_datatype::FieldType>> {
    let original: Vec<tidb_datatype::FieldType> = match aggregation {
        Some(aggregation) => aggregation
            .agg_func
            .iter()
            .chain(&aggregation.group_by)
            .map(|expr| {
                expr.field_type
                    .as_ref()
                    .map(tidb_expr::distsql_builtin::pb_type_to_field_type)
            })
            .collect::<Option<_>>()?,
        None => scan_types.to_vec(),
    };
    if context.dag_req.output_offsets.is_empty() {
        return Some(original);
    }
    context
        .dag_req
        .output_offsets
        .iter()
        .map(|offset| original.get(*offset as usize).cloned())
        .collect()
}

/// Go `encodeChunk`: the rows in the request's encode type. `TypeChunk`
/// (`useChunkEncoding`) carries each value as decoded -- a DECIMAL keeps its
/// stored words, which the default row encoding would round to the field
/// type's fraction on the TiDB side (`DecodeOneToChunk`).
fn encode_select_rows(
    rows: Vec<Vec<tidb_datatype::Datum>>,
    field_types: Option<&[tidb_datatype::FieldType]>,
    context: &DagContext,
    timezone: &tidb_datatype::SessionTimeZone,
    warnings: Vec<tipb::Error>,
) -> coprocessor::Response {
    let field_types = match field_types {
        Some(types)
            if context.dag_req.encode_type() == tipb::EncodeType::TypeChunk
                && rows.iter().all(|row| {
                    row.len() == types.len()
                        && row
                            .iter()
                            .zip(types)
                            .all(|(value, field_type)| datum_fits_chunk_column(value, field_type))
                }) =>
        {
            types
        }
        // Go's decoder fills the chunk with typed values. A value this tier
        // decoded untyped (a stored default read back as its raw form) fits
        // no column layout, so the response keeps the row encoding, which
        // the TiDB side types on decode.
        _ => return encode_default_rows(rows, timezone, warnings),
    };
    let codec = tidb_chunk::codec::Codec::new(field_types.to_vec());
    // Go's executors fill chunks of `DefaultBatchSize` rows
    // (`mpp_exec.go:60`, `NewChunkWithCapacity(fieldTypes, DefaultBatchSize)`
    // is full at that many) and `mppExecute` encodes each as its own
    // `tipb.Chunk`, which is the unit the TiDB side's `readFromChunk` either
    // takes whole or splices.
    let chunks = rows
        .chunks(GO_MPP_DEFAULT_BATCH_SIZE)
        .map(|batch| {
            let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(field_types, batch.len());
            for row in batch {
                for (column, value) in row.iter().enumerate() {
                    chunk.append_datum(column, value);
                }
            }
            tipb::Chunk {
                rows_data: Some(codec.encode(&chunk).into()),
                ..tipb::Chunk::default()
            }
        })
        .collect();
    let warning_count = warnings.len() as i64;
    let select = tipb::SelectResponse {
        warnings,
        warning_count: Some(warning_count),
        chunks,
        encode_type: Some(tipb::EncodeType::TypeChunk as i32),
        ..tipb::SelectResponse::default()
    };
    coprocessor::Response {
        data: select.encode_to_vec().into(),
        ..coprocessor::Response::default()
    }
}

/// Whether `value` is the kind a chunk column of `field_type` stores.
fn datum_fits_chunk_column(
    value: &tidb_datatype::Datum,
    field_type: &tidb_datatype::FieldType,
) -> bool {
    use tidb_datatype::{Datum, FieldTypeCode as Code};
    let code = field_type.code();
    match value {
        Datum::Null => true,
        Datum::Int(_) | Datum::UInt(_) => matches!(
            code,
            Code::Tiny | Code::Short | Code::Int24 | Code::Long | Code::LongLong | Code::Year
        ),
        Datum::Real(_) => code == Code::Double,
        Datum::Float32(_) => code == Code::Float,
        Datum::Decimal(_) => code == Code::NewDecimal,
        Datum::Time(_) => matches!(code, Code::Date | Code::Datetime | Code::Timestamp),
        Datum::Duration(_) => code == Code::Duration,
        Datum::String(_) | Datum::Bytes(_) => matches!(
            code,
            Code::Varchar
                | Code::VarString
                | Code::String
                | Code::Blob
                | Code::TinyBlob
                | Code::MediumBlob
                | Code::LongBlob
        ),
        Datum::Enum(..) => code == Code::Enum,
        Datum::Set(..) => code == Code::Set,
        Datum::Bit(_) | Datum::BinaryLiteral(_) => code == Code::Bit,
        Datum::Json(_) => code == Code::Json,
        Datum::VectorFloat32(_) => code == Code::VectorFloat32,
        _ => false,
    }
}

fn encode_default_rows(
    rows: Vec<Vec<tidb_datatype::Datum>>,
    timezone: &tidb_datatype::SessionTimeZone,
    warnings: Vec<tipb::Error>,
) -> coprocessor::Response {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0_usize;
    for row in rows {
        // Go `chunkToOldChunk` calls `codec.EncodeValue(sc.TimeZone(), ...)`.
        // A table/index decoder has already converted TIMESTAMP from UTC into
        // the request location, so the response encoder must flatten it back
        // to UTC before the TiDB-side default-row decoder applies that same
        // location. Encoding without the request zone converts through UTC as
        // if the localized wall clock were UTC and shifts every pushed scan a
        // second time.
        let encoded = match tidb_codec::encode_value_in_timezone(timezone, &row) {
            Ok(encoded) => encoded,
            Err(error) => return other_error(&format!("encode row failed: {error:?}")),
        };
        current.extend_from_slice(&encoded);
        current_rows += 1;
        if current_rows == CHUNK_MAX_ROWS {
            chunks.push(tipb::Chunk {
                rows_data: Some((std::mem::take(&mut current)).into()),
                ..tipb::Chunk::default()
            });
            current_rows = 0;
        }
    }
    if !current.is_empty() {
        chunks.push(tipb::Chunk {
            rows_data: Some((current).into()),
            ..tipb::Chunk::default()
        });
    }
    let warning_count = warnings.len() as i64;
    let select = tipb::SelectResponse {
        warnings,
        warning_count: Some(warning_count),
        chunks,
        encode_type: Some(tipb::EncodeType::TypeDefault as i32),
        ..tipb::SelectResponse::default()
    };
    let mut data = Vec::new();
    select.encode(&mut data).expect("a select response encodes");
    coprocessor::Response {
        data: data.into(),
        ..coprocessor::Response::default()
    }
}

/// Go's chunk cut: `closure_exec.go` grows the output chunk to 1024 rows
/// before starting the next.
const CHUNK_MAX_ROWS: usize = 1024;

/// Go `cophandler.DefaultBatchSize`: the rows of one executor chunk, and so
/// of one chunk-encoded `tipb.Chunk` in a response.
const GO_MPP_DEFAULT_BATCH_SIZE: usize = 32;

/// Go's coprocessor paging over one scan (`tableScanExec.next` +
/// `mppExecute`): the scan hands up chunks of `DefaultBatchSize` scanned
/// keys; after each chunk the rows it produced are counted, and once they
/// pass the page size the response stops there and answers the range it
/// scanned, so the client resumes after it.
struct PagingCut {
    size: u64,
    descending: bool,
    scanned_in_chunk: usize,
    produced_in_chunk: u64,
    produced: u64,
    last_key: Option<Vec<u8>>,
}

impl PagingCut {
    /// Paging applies to a chunk-encoded scan whose rows leave as scanned:
    /// Go counts the encoded output rows, which an aggregation or TopN only
    /// produces after the whole scan.
    fn new(context: &DagContext, descending: bool, buffers_rows: bool) -> Option<Self> {
        (context.paging_size > 0
            && !buffers_rows
            && context.dag_req.encode_type() == tipb::EncodeType::TypeChunk)
            .then_some(Self {
                size: context.paging_size,
                descending,
                scanned_in_chunk: 0,
                produced_in_chunk: 0,
                produced: 0,
                last_key: None,
            })
    }

    /// Before the next key: if the previous chunk is complete, count it, and
    /// answer the page's range when the page is full.
    fn before_key(&mut self) -> Option<coprocessor::KeyRange> {
        if self.scanned_in_chunk < GO_MPP_DEFAULT_BATCH_SIZE {
            return None;
        }
        self.produced += self.produced_in_chunk;
        self.scanned_in_chunk = 0;
        self.produced_in_chunk = 0;
        (self.produced > self.size).then(|| self.scanned_range())
    }

    fn scanned(&mut self, key: &[u8]) {
        self.scanned_in_chunk += 1;
        self.last_key = Some(key.to_vec());
    }

    fn produced_row(&mut self) {
        self.produced_in_chunk += 1;
    }

    /// Go `tableScanExec.next`'s range after the chunk holding `last_key`:
    /// everything up to it ascending (its `Next()`), down to it descending.
    fn scanned_range(&self) -> coprocessor::KeyRange {
        let key = self.last_key.clone().unwrap_or_default();
        if self.descending {
            coprocessor::KeyRange {
                start: key,
                ..coprocessor::KeyRange::default()
            }
        } else {
            let mut end = key;
            end.push(0);
            coprocessor::KeyRange {
                end,
                ..coprocessor::KeyRange::default()
            }
        }
    }

    /// The range of a scan that ran out: a final full chunk past the page
    /// size answers its own range, as Go's chunk-full event does; otherwise
    /// the whole request range (`e.kvRanges[len-1]`), which leaves nothing.
    fn finish(mut self, ranges: &[coprocessor::KeyRange]) -> coprocessor::KeyRange {
        if let Some(range) = self.before_key() {
            return range;
        }
        if self.descending {
            coprocessor::KeyRange {
                start: ranges
                    .first()
                    .map(|range| range.start.clone())
                    .unwrap_or_default(),
                ..coprocessor::KeyRange::default()
            }
        } else {
            coprocessor::KeyRange {
                end: ranges
                    .last()
                    .map(|range| range.end.clone())
                    .unwrap_or_default(),
                ..coprocessor::KeyRange::default()
            }
        }
    }
}

/// The table-scan executor over the MVCC store: each range scanned at the
/// request's start ts, each surviving row decoded into the REQUESTED columns
/// in request order, datum-encoded into default-format chunks — the shape a
/// distsql client decodes.
///
/// A CLUSTERED table's primary key lives in the row KEY, not the value, so
/// Go's `newRowDecoder` (`cop_handler.go:500`) falls back to the request's
/// `PrimaryColumnIds` when no column carries `PkHandle`, and the decoder
/// fills those columns from the key. This does the same: the handle's
/// datums are decoded in key order and matched positionally to
/// `primary_column_ids`.
///
/// Narrowings, by name: partitioned reads follow their course
/// (`RecordHandle::Partition` rows refuse); a requested column absent from
/// the row answers its `default_val` when carried, else NULL — Go's
/// `getDefaultValue` behavior for the null-capable slice.
fn exec_table_scan(
    store: &mut MvccStore,
    context: &DagContext,
    tbl_scan: &tipb::TableScan,
    conditions: &[SimpleExpr],
    limit: usize,
    aggregation: Option<&tipb::Aggregation>,
    topn: Option<&TopNSpec>,
    produced: &mut ExecutorRows,
) -> coprocessor::Response {
    let timezone = match context.time_zone.resolve() {
        Ok(timezone) => timezone,
        Err(error) => return other_error(&error),
    };
    let mut aggregator = match aggregation {
        Some(aggregation) => {
            match RegionAggregator::build(aggregation, &context.expression_context) {
                Ok(aggregator) => Some(aggregator),
                Err(message) => return other_error(&message),
            }
        }
        None => None,
    };
    use tidb_datatype::Datum;
    const EXTRA_PHYSICAL_TABLE_ID: i64 = -3;
    let mut column_types = std::collections::BTreeMap::new();
    for column in &tbl_scan.columns {
        column_types.insert(column.column_id(), field_type_from_pb_column(column));
    }
    let scan_types: Vec<_> = tbl_scan
        .columns
        .iter()
        .map(field_type_from_pb_column)
        .collect();
    let output_types = dag_output_field_types(context, &scan_types, aggregation);
    // Go `tableScanProcessor.Process`: "Add ExtraPhysTblID if requested.
    // Assumes it is always last!" -- the row's physical table ID comes from
    // its key, which is what SelectLock builds a partitioned row's lock key
    // from.
    let physical_id_offset = tbl_scan
        .columns
        .last()
        .filter(|column| column.column_id() == EXTRA_PHYSICAL_TABLE_ID)
        .map(|_| tbl_scan.columns.len() - 1);
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    let mut emitted = 0_usize;
    // Go keeps the kept rows in a bounded heap; this lowering buffers every
    // surviving row and sorts once -- the same N rows in the same key order,
    // without the intermediate evictions.
    let mut topn_rows: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::new();
    // Go's `desc` scan: the ranges (ascending on the wire) are walked
    // last-to-first, each one reversed, so rows leave in descending record
    // order and a Limit above stops after the LARGEST keys.
    let descending = tbl_scan.desc();
    let mut paging = PagingCut::new(context, descending, aggregator.is_some() || topn.is_some());
    let mut page_end: Option<coprocessor::KeyRange> = None;
    let mut ordered_ranges: Vec<_> = context.key_ranges.iter().collect();
    if descending {
        ordered_ranges.reverse();
    }
    'ranges: for range in ordered_ranges {
        // Go's reverse `Scan` contract: the CALLER hands the bounds
        // swapped -- `start_key` is the upper bound, `end_key` the lower
        // -- and the store swaps them back (`mvcc.go`'s `Reverse` arm).
        let (start_key, end_key) = if descending {
            (range.end.clone(), range.start.clone())
        } else {
            (range.start.clone(), range.end.clone())
        };
        let pairs = store.scan(&crate::mvcc_store::ScanReq {
            start_key,
            end_key,
            limit: u32::MAX,
            version: context.start_ts,
            sample_step: 0,
            reverse: descending,
        });
        for pair in pairs {
            if let Some(page) = paging.as_mut() {
                if let Some(range) = page.before_key() {
                    page_end = Some(range);
                    break 'ranges;
                }
                page.scanned(&pair.key);
            }
            if let Some(err) = pair.error {
                // Go: the FIRST lock met answers the whole response.
                if let crate::mvcc_store::KvError::Locked(lock) = *err {
                    return coprocessor::Response {
                        locked: Some(*lock),
                        ..coprocessor::Response::default()
                    };
                }
                return other_error(&format!("scan error: {err:?}"));
            }
            // Go `tablecodec.DecodeRowKey`: an int handle answers directly,
            // a common handle carries the clustered primary key's datums.
            let (handle, common_handle) = match tidb_codec::table_key::decode_row_key(&pair.key) {
                Ok(RecordHandle::Int(handle)) => (handle, None),
                Ok(RecordHandle::Common(encoded)) => {
                    let count = tbl_scan.primary_column_ids.len();
                    match tidb_codec::decode(&encoded, count) {
                        Ok(datums) => (0, Some(datums)),
                        Err(err) => return other_error(&format!("invalid common handle: {err:?}")),
                    }
                }
                Ok(RecordHandle::Partition { .. }) => {
                    return other_error("partitioned scans are a later course of this port")
                }
                Err(err) => return other_error(&format!("invalid record key: {err:?}")),
            };
            let decoded = match tidb_tablecodec::decode_table_row_to_map(
                &pair.value,
                &column_types,
                Some(&timezone),
            ) {
                Ok(map) => map,
                Err(err) => return other_error(&format!("decode row failed: {err:?}")),
            };
            let mut row_datums = Vec::with_capacity(tbl_scan.columns.len());
            for (offset, column) in tbl_scan.columns.iter().enumerate() {
                // Go `ChunkDecoder.DecodeToChunk`: a non-NULL value in the row
                // wins; the handle is consulted "only when there is no
                // corresponding column in the value", because the handle may
                // hold a prefix or a collation sort key rather than the data.
                let stored = decoded.get(&column.column_id());
                if physical_id_offset == Some(offset) {
                    row_datums.push(Datum::Int(tidb_codec::decode_table_id(&pair.key)));
                } else if let Some(datum) = stored.filter(|datum| !matches!(datum, Datum::Null)) {
                    row_datums.push(datum.clone());
                } else if column.pk_handle() {
                    row_datums.push(Datum::Int(handle));
                } else if let Some(datum) = common_handle.as_ref().and_then(|datums| {
                    // Go `tryAppendHandleColumn` matches `PrimaryColumnIds`
                    // positionally against the handle's datums (encoded in key
                    // order), and refuses a column whose key bytes are not its
                    // value (`NeedRestoredData`).
                    let position = tbl_scan
                        .primary_column_ids
                        .iter()
                        .position(|id| *id == column.column_id())?;
                    if column_types[&column.column_id()].need_restored_data() {
                        return None;
                    }
                    datums.get(position)
                }) {
                    // ... and decodes it with the column's type
                    // (`DecodeOne(..., col.Ft)`): a DATETIME key part is a
                    // packed integer until it is restored to its time.
                    match tidb_tablecodec::unflatten_datum(
                        datum.clone(),
                        &column_types[&column.column_id()],
                        Some(&timezone),
                    ) {
                        Ok(datum) => row_datums.push(datum),
                        Err(err) => {
                            return other_error(&format!("decode common handle failed: {err:?}"))
                        }
                    }
                } else if stored.is_some() {
                    row_datums.push(Datum::Null);
                } else if !column.default_val().is_empty() {
                    // Go `getDefaultValue`: a row written before the column
                    // existed answers the encoded origin default.
                    match tidb_codec::decode(column.default_val(), 1) {
                        Ok(mut datums) if !datums.is_empty() => {
                            row_datums.push(datums.remove(0));
                        }
                        _ => {
                            return other_error(&format!(
                                "invalid default value bytes for column {}",
                                column.column_id()
                            ))
                        }
                    }
                } else {
                    row_datums.push(Datum::Null);
                }
            }
            // Go `tableScanExec.rowCnt`: every row the scan produced.
            produced.scanned += 1;
            // Go `selectionProcessor`: a row survives only when EVERY
            // condition evaluates non-null and non-zero.
            match conditions_hold(conditions, &row_datums) {
                Ok(true) => produced.selected += 1,
                Ok(false) => continue,
                Err(error) => return executor_error_response(&error, Vec::new()),
            }
            // A partial aggregation consumes the surviving row instead of
            // emitting it; the grouped rows leave after the scan.
            if let Some(aggregator) = aggregator.as_mut() {
                if let Err(message) = aggregator.update(&row_datums) {
                    return other_error(&message);
                }
                continue;
            }
            // Go's closure executor emits exactly `DAGRequest.output_offsets`
            // when the request names them: the request's own projection, in
            // its order. An empty list is the whole scanned column set.
            let projected: Vec<Datum> = if context.dag_req.output_offsets.is_empty() {
                row_datums.clone()
            } else {
                let mut projected = Vec::with_capacity(context.dag_req.output_offsets.len());
                for offset in &context.dag_req.output_offsets {
                    match row_datums.get(*offset as usize) {
                        Some(datum) => projected.push(datum.clone()),
                        None => {
                            return other_error(&format!(
                                "output offset {offset} is outside the scanned columns"
                            ))
                        }
                    }
                }
                projected
            };
            // Go `topNProcessor`: the surviving row's sort keys are evaluated
            // NOW and the row rides the heap; the replay emits the kept rows
            // in key order. Buffering the whole set and sorting is that same
            // answer without the heap.
            if let Some(spec) = topn {
                let keys = match spec.evaluate(&row_datums) {
                    Ok(keys) => keys,
                    Err(message) => return other_error(&message),
                };
                topn_rows.push((projected, keys));
                continue;
            }
            rows.push(projected);
            emitted += 1;
            if let Some(page) = paging.as_mut() {
                page.produced_row();
            }
            // Go `e.rowCount == e.limit` (`closure_exec.go:597`).
            if emitted == limit {
                break 'ranges;
            }
        }
    }
    if let Some(spec) = topn {
        let topn_rows = go_topn_heap_finish(&spec.keys, topn_rows, spec.limit);
        for (projected, _) in topn_rows {
            rows.push(projected);
            emitted += 1;
            if emitted == limit {
                break;
            }
        }
    }
    if let Some(aggregator) = aggregator {
        // TiKV's aggregation schema: aggregate results first, group keys
        // last, one row per group. `output_offsets` addresses THAT row.
        for row in aggregator.finish() {
            let projected: Vec<Datum> = if context.dag_req.output_offsets.is_empty() {
                row
            } else {
                let mut projected = Vec::with_capacity(context.dag_req.output_offsets.len());
                for offset in &context.dag_req.output_offsets {
                    match row.get(*offset as usize) {
                        Some(datum) => projected.push(datum.clone()),
                        None => {
                            return other_error(&format!(
                                "output offset {offset} is outside the aggregate schema"
                            ))
                        }
                    }
                }
                projected
            };
            rows.push(projected);
            emitted += 1;
        }
    }
    produced.output = emitted as u64;
    let mut response = encode_select_rows(
        rows,
        output_types.as_deref(),
        context,
        &timezone,
        Vec::new(),
    );
    if let Some(page) = paging {
        response.range = Some(page_end.unwrap_or_else(|| page.finish(&context.key_ranges)));
    }
    response
}

/// Go's live `aggExec` shares typed group evaluation and first-seen group
/// order between TypeAggregation and TypeStreamAgg. The deprecated protobuf
/// `Aggregation.streamed` field does not select another grouping lifecycle.
struct RegionAggregator {
    group_by: Vec<tidb_expr::expression::Expression>,
    functions: Vec<tidb_expr::aggregation::DistAggregate>,
    context: Arc<RequestEvalContext>,
    row: tidb_chunk::mutrow::MutRow,
    group_indices: std::collections::HashMap<Vec<u8>, usize>,
    groups: Vec<RegionAggregateGroup>,
}

struct RegionAggregateGroup {
    keys: Vec<tidb_datatype::Datum>,
    values: Vec<tidb_expr::aggregation::DistAggregateState>,
}

impl RegionAggregator {
    fn build(
        aggregation: &tipb::Aggregation,
        context: &Arc<RequestEvalContext>,
    ) -> Result<Self, String> {
        let group_by = aggregation
            .group_by
            .iter()
            .map(|expr| {
                tidb_expr::distsql_builtin::pb_to_expr_in(
                    expr,
                    &context.column_types,
                    &context.zone,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let functions = aggregation
            .agg_func
            .iter()
            .map(|func| {
                tidb_expr::aggregation::DistAggregate::from_pb(
                    func,
                    &context.column_types,
                    &context.zone,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        if group_by.is_empty() && functions.is_empty() {
            return Err("an aggregation names a group key or a function".to_owned());
        }
        Ok(Self {
            group_by,
            functions,
            row: tidb_chunk::mutrow::MutRow::from_types(&context.column_types),
            context: Arc::clone(context),
            group_indices: std::collections::HashMap::new(),
            groups: Vec::new(),
        })
    }

    fn update(&mut self, row: &[tidb_datatype::Datum]) -> Result<(), String> {
        if row.len() != self.row.len() {
            return Err("aggregate row does not match input schema".to_owned());
        }
        self.row.set_datums(row);
        let keys = self
            .group_by
            .iter()
            .map(|expr| {
                expr.eval(self.context.as_ref(), self.row.to_row())
                    .map_err(|error| {
                        tidb_expr::aggregation::DistAggregateError::from(error).to_string()
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Go aggExec.getGroupKey uses EncodeValue, including raw string bytes
        // and the request time zone. Do not fold collation keys or normalize
        // floats here: either changes the observable intermediate group rows.
        let key = tidb_codec::encode_value_in_timezone(&self.context.zone, &keys)
            .map_err(|error| format!("encode group key failed: {error:?}"))?;
        let index = *self.group_indices.entry(key).or_insert_with(|| {
            let index = self.groups.len();
            self.groups.push(RegionAggregateGroup {
                keys,
                values: self
                    .functions
                    .iter()
                    .map(|func| func.create_state())
                    .collect(),
            });
            index
        });
        Self::accumulate(
            &self.functions,
            &mut self.groups[index].values,
            &self.context,
            self.row.to_row(),
        )
    }

    fn accumulate(
        functions: &[tidb_expr::aggregation::DistAggregate],
        values: &mut [tidb_expr::aggregation::DistAggregateState],
        context: &RequestEvalContext,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<(), String> {
        for (function, value) in functions.iter().zip(values.iter_mut()) {
            function
                .update(value, context, row)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn finish_group(
        functions: &[tidb_expr::aggregation::DistAggregate],
        groups: Vec<tidb_datatype::Datum>,
        values: Vec<tidb_expr::aggregation::DistAggregateState>,
    ) -> Vec<tidb_datatype::Datum> {
        functions
            .iter()
            .zip(values)
            .flat_map(|(function, value)| function.partial_result(value))
            .chain(groups)
            .collect()
    }

    fn finish(self) -> Vec<Vec<tidb_datatype::Datum>> {
        self.groups
            .into_iter()
            .map(|group| Self::finish_group(&self.functions, group.keys, group.values))
            .collect()
    }
}

/// Go's `getValidFloatPrefix`: the longest leading prefix that scans as an
/// optional-signed integer, decimal or exponent form. `None` when the
/// prefix carries no digit at all (Go answers 0 with a truncation warning).
fn numeric_prefix(text: &str, allow_float: bool) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut end = 0;
    if end < chars.len() && (chars[end] == '+' || chars[end] == '-') {
        end += 1;
    }
    let mut digits = 0;
    while end < chars.len() && chars[end].is_ascii_digit() {
        end += 1;
        digits += 1;
    }
    let mut fraction = 0;
    if allow_float && end < chars.len() && chars[end] == '.' {
        let mut peek = end + 1;
        while peek < chars.len() && chars[peek].is_ascii_digit() {
            peek += 1;
            fraction += 1;
        }
        if fraction > 0 {
            end = peek;
            digits += fraction;
        }
    }
    if digits == 0 {
        return None;
    }
    if allow_float && end < chars.len() && (chars[end] == 'e' || chars[end] == 'E') {
        let mut exp_end = end + 1;
        if exp_end < chars.len() && (chars[exp_end] == '+' || chars[exp_end] == '-') {
            exp_end += 1;
        }
        let mut exp_digits = 0;
        while exp_end < chars.len() && chars[exp_end].is_ascii_digit() {
            exp_end += 1;
            exp_digits += 1;
        }
        if exp_digits > 0 {
            end = exp_end;
        }
    }
    Some(chars[..end].iter().collect())
}

/// The datum a leaf answers for one row: the column value, or the literal.
fn eval_datum(
    expr: &SimpleExpr,
    row: &[tidb_datatype::Datum],
) -> Result<tidb_datatype::Datum, String> {
    use tidb_datatype::Datum;
    match expr {
        SimpleExpr::Column(offset) => row
            .get(*offset)
            .cloned()
            .ok_or_else(|| "aggregate input is outside the scanned row".to_owned()),
        SimpleExpr::Int(value) => Ok(Datum::Int(*value)),
        SimpleExpr::Bytes(bytes) => Ok(Datum::Bytes(bytes.clone())),
        SimpleExpr::Decimal(value) => Ok(Datum::Decimal(value.clone())),
        SimpleExpr::Json(value) => Ok(Datum::Json(value.clone())),
        SimpleExpr::Real(value) => Ok(Datum::Real(*value)),
        SimpleExpr::Time(value) => Ok(Datum::Time(*value)),
        SimpleExpr::Null => Ok(Datum::Null),
        SimpleExpr::Shared(shared) => eval_shared(shared, row).map_err(String::from),
    }
}

/// Go `topNCtx` (`closure_exec.go:545`): the pushed-down bounded sort --
/// order-by keys with their directions and the heap size.
struct TopNSpec {
    keys: Vec<(SimpleExpr, bool, tidb_datatype::Collation)>,
    limit: usize,
}

impl TopNSpec {
    /// Go `buildTopNProcessor` (`closure_exec.go:384`).
    fn build(top_n: &tipb::TopN, context: &Arc<RequestEvalContext>) -> Result<Self, String> {
        let mut keys = Vec::with_capacity(top_n.order_by.len());
        for item in &top_n.order_by {
            let Some(pb_expr) = item.expr.as_ref() else {
                return Err("order-by item missing expr".to_owned());
            };
            let expr = convert_expr_with_context(pb_expr, context)?;
            // Go orders each key under `by.Expr.FieldType.Collate`
            // (`topn.go:60`).
            let restored = tidb_datatype::restore_collation_id_if_needed(collation_of(pb_expr));
            let collation =
                tidb_datatype::Collation::from_name(&tidb_datatype::proto_to_collation(restored))
                    .unwrap_or(tidb_datatype::Collation::Binary);
            keys.push((expr, item.desc(), collation));
        }
        Ok(Self {
            keys,
            limit: top_n.limit() as usize,
        })
    }

    /// Go `topNProcessor.Process`'s key evaluation (`closure_exec.go:1033`):
    /// every key datum of one surviving row. Computed keys and kinds beyond
    /// [`compare_sort_keys`] are a later course, matching the aggregation
    /// lowering's leaf contract; surfacing the refusal here keeps the sort
    /// comparator total.
    fn evaluate(&self, row: &[tidb_datatype::Datum]) -> Result<Vec<tidb_datatype::Datum>, String> {
        use tidb_datatype::Datum;
        self.keys
            .iter()
            .map(|(expr, _, _)| {
                let datum = eval_datum(expr, row)?;
                match datum {
                    Datum::Null
                    | Datum::Int(_)
                    | Datum::UInt(_)
                    | Datum::Decimal(_)
                    | Datum::Real(_)
                    | Datum::Float32(_)
                    | Datum::String(_)
                    | Datum::Bytes(_)
                    | Datum::Time(_)
                    | Datum::Duration(_) => Ok(datum),
                    _ => Err("ordering over this datum kind is a later course".to_owned()),
                }
            })
            .collect()
    }

    /// Go `topNSorter.Less` / `topNHeap.Less` (`topn.go:51-77`): per-key
    /// datum comparison, negated for DESC. The heap's Enum special case and
    /// Go's unstable `sort.Sort` are unreachable/narrowing here: computed
    /// keys never arrive (leaf keys only), and a stable Rust sort is
    /// deterministic where Go leaves ties unordered.
    fn sort_rows(&self, rows: &mut [(Vec<tidb_datatype::Datum>, Vec<tidb_datatype::Datum>)]) {
        rows.sort_by(|(_, left_keys), (_, right_keys)| {
            for ((_, desc, collation), (left, right)) in self
                .keys
                .iter()
                .zip(left_keys.iter().zip(right_keys.iter()))
            {
                let mut ordering = match compare_sort_keys(left, right, collation) {
                    Ok(ordering) => ordering,
                    // Unreachable: evaluate() gates the kinds this
                    // comparator handles.
                    Err(_) => std::cmp::Ordering::Equal,
                };
                if *desc {
                    ordering = ordering.reverse();
                }
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
}

/// Go `types.Datum.Compare` (`pkg/types/datum.go`) over the datum kinds a
/// lowered scan can produce as a sort key: NULL orders before every
/// non-NULL datum, numerics compare by value across signedness, `Time` and
/// `Duration` through their own comparators, and the string kinds under the
/// key's collation.
/// Go's cop TopN is NOT sort-then-truncate: `topNHeap.tryToAddRow`
/// (`cophandler/topn.go`) admits a row iff it sorts STRICTLY before the heap
/// root -- a tie never evicts, so among equal keys the EARLIEST scanned rows
/// survive -- and `topNProcessor.Finish` (`closure_exec.go:1058`) replays the
/// survivors through `sort.Sort` (insertion sort for n <= 12, stable in
/// effect). Buffer-then-truncate reshuffles ties and can drop the wrong tied
/// rows (oracle g-group: `ORDER BY g LIMIT 100`).
fn go_topn_heap_finish(
    by_items: &[(SimpleExpr, bool, tidb_datatype::Collation)],
    topn_rows: Vec<(Vec<tidb_datatype::Datum>, Vec<tidb_datatype::Datum>)>,
    limit: usize,
) -> Vec<(Vec<tidb_datatype::Datum>, Vec<tidb_datatype::Datum>)> {
    // `heap_phase`: Go's `topNHeap.Less` compares an ENUM key by its ordinal
    // (`GetUint64`), while the closing `topNSorter.Less` goes through
    // `Datum.Compare`, which compares it by name.
    let ordering_in = |a: &[tidb_datatype::Datum], b: &[tidb_datatype::Datum], heap_phase: bool| {
        let mut order = std::cmp::Ordering::Equal;
        for ((_, desc, collation), (left, right)) in by_items.iter().zip(a.iter().zip(b.iter())) {
            let compared = match (left, right) {
                (tidb_datatype::Datum::Enum(l, _), tidb_datatype::Datum::Enum(r, _))
                    if heap_phase =>
                {
                    Ok(l.value().cmp(&r.value()))
                }
                _ => compare_sort_keys(left, right, collation),
            };
            let mut step = match compared {
                Ok(step) => step,
                Err(_) => std::cmp::Ordering::Equal,
            };
            if *desc {
                step = step.reverse();
            }
            if step != std::cmp::Ordering::Equal {
                order = step;
                break;
            }
        }
        order
    };
    let ordering =
        |a: &[tidb_datatype::Datum], b: &[tidb_datatype::Datum]| ordering_in(a, b, false);
    // `heap_less(i, j)`: `topNHeap.Less` returns true when row i's key sorts
    // AFTER row j's (`ret > 0`), making the root the worst retained row.
    let heap_less =
        |heap: &[(Vec<tidb_datatype::Datum>, Vec<tidb_datatype::Datum>)], i: usize, j: usize| {
            ordering_in(&heap[i].1, &heap[j].1, true) == std::cmp::Ordering::Greater
        };
    let heap_up =
        |heap: &mut Vec<(Vec<tidb_datatype::Datum>, Vec<tidb_datatype::Datum>)>, mut j: usize| {
            while j > 0 {
                let i = (j - 1) / 2;
                if i == j || !heap_less(heap, j, i) {
                    break;
                }
                heap.swap(i, j);
                j = i;
            }
        };
    let heap_down =
        |heap: &mut Vec<(Vec<tidb_datatype::Datum>, Vec<tidb_datatype::Datum>)>,
         i0: usize,
         n: usize| {
            let mut i = i0;
            loop {
                let j1 = 2 * i + 1;
                if j1 >= n {
                    break;
                }
                let mut j = j1;
                let j2 = j1 + 1;
                if j2 < n && heap_less(heap, j2, j1) {
                    j = j2;
                }
                if !heap_less(heap, j, i) {
                    break;
                }
                heap.swap(i, j);
                i = j;
            }
            i > i0
        };
    let mut heap: Vec<(Vec<tidb_datatype::Datum>, Vec<tidb_datatype::Datum>)> = Vec::new();
    for candidate in topn_rows {
        if heap.len() == limit {
            // `tryToAddRow`'s full case: park the candidate at index
            // `heapSize`, evict the root iff the root sorts strictly after it
            // (a tie keeps the incumbent), then drop the tail.
            heap.push(candidate);
            let tail = heap.len() - 1;
            if heap_less(&heap, 0, tail) {
                heap.swap(0, tail);
                if !heap_down(&mut heap, 0, tail) {
                    heap_up(&mut heap, 0);
                }
            }
            heap.truncate(tail);
        } else {
            // `heap.Push`: append, then sift up.
            heap.push(candidate);
            let last = heap.len() - 1;
            heap_up(&mut heap, last);
        }
    }
    // `topNProcessor.Finish`: `sort.Sort(&ctx.heap.topNSorter)` over the
    // survivors, ascending under the by-item order.
    // Go's unstable pdqsort: insertion sort up to 12 rows, and beyond that its
    // pivot and pattern schedule, which decides where equal keys end up.
    let mut sorted = heap;
    tidb_stats::go_sort_func_by(&mut sorted, |a, b| ordering(&a.1, &b.1));
    sorted
}

fn compare_sort_keys(
    left: &tidb_datatype::Datum,
    right: &tidb_datatype::Datum,
    collation: &tidb_datatype::Collation,
) -> Result<std::cmp::Ordering, String> {
    use std::cmp::Ordering;
    use tidb_datatype::Datum;
    let ordering = match (left, right) {
        (Datum::Null, Datum::Null) => Ordering::Equal,
        (Datum::Null, _) => Ordering::Less,
        (_, Datum::Null) => Ordering::Greater,
        (Datum::Int(l), Datum::Int(r)) => l.cmp(r),
        (Datum::UInt(l), Datum::UInt(r)) => l.cmp(r),
        (Datum::Int(l), Datum::UInt(r)) => {
            if *l < 0 {
                Ordering::Less
            } else {
                (*l as u64).cmp(r)
            }
        }
        (Datum::UInt(l), Datum::Int(r)) => {
            if *r < 0 {
                Ordering::Greater
            } else {
                l.cmp(&(*r as u64))
            }
        }
        (Datum::Decimal(l), Datum::Decimal(r)) => l.cmp(r),
        (Datum::Real(l), Datum::Real(r)) => l.total_cmp(r),
        (Datum::Float32(l), Datum::Float32(r)) => l.total_cmp(r),
        (Datum::Time(l), Datum::Time(r)) => l.compare(*r),
        (Datum::Duration(l), Datum::Duration(r)) => l.compare(*r),
        // `compareMysqlEnum`/`compareMysqlSet`: by name under the collator.
        (Datum::Enum(l, _), Datum::Enum(r, _)) => collation.compare(l.name_bytes(), r.name_bytes()),
        (Datum::Set(l, _), Datum::Set(r, _)) => collation.compare(l.name_bytes(), r.name_bytes()),
        _ => match (left.as_raw_bytes(), right.as_raw_bytes()) {
            (Some(l), Some(r)) => collation.key(l).cmp(&collation.key(r)),
            _ => return Err("ordering over this datum pair is a later course".to_owned()),
        },
    };
    Ok(ordering)
}

/// Go `buildDAG` (`cop_handler.go`), the guards and decode.
pub fn build_dag(req: &coprocessor::Request) -> Result<DagContext, String> {
    if req.ranges.is_empty() {
        // Go's exact message.
        return Err("request range is null".to_owned());
    }
    if req.tp != REQ_TYPE_DAG {
        return Err(format!("unsupported request type {}", req.tp));
    }
    let dag_req = tipb::DagRequest::decode(req.data.as_slice())
        .map_err(|decode_err| format!("invalid dag request: {decode_err}"))?;
    for executor in &dag_req.executors {
        tipb::ExecType::try_from(executor.tp.unwrap_or_default())
            .map_err(|_| format!("unknown executor type {}", executor.tp.unwrap_or_default()))?;
    }
    let time_zone = match dag_req.time_zone_name() {
        "" => TimeZoneSpec::FixedOffset(dag_req.time_zone_offset()),
        "System" => TimeZoneSpec::System,
        name => TimeZoneSpec::Named(name.to_owned()),
    };
    let mut expression_context = RequestEvalContext::new(
        time_zone.resolve()?,
        dag_req.div_precision_increment.unwrap_or(4),
        dag_req.flags.unwrap_or(0),
    );
    let columns = dag_req.executors.first().and_then(|scan| {
        scan.tbl_scan
            .as_ref()
            .map(|scan| &scan.columns)
            .or_else(|| scan.idx_scan.as_ref().map(|scan| &scan.columns))
    });
    if let Some(columns) = columns {
        expression_context.column_types = columns.iter().map(field_type_from_pb_column).collect();
    }
    Ok(DagContext {
        expression_context: Arc::new(expression_context),
        key_ranges: req.ranges.clone(),
        start_ts: req.start_ts,
        paging_size: req.paging_size,
        flags: dag_req.flags.unwrap_or(0),
        time_zone,
        // Go `buildDAG`: the session default is 4 when the request omits
        // the field (`variable.DefDivPrecisionIncrement`).
        div_precision_increment: dag_req.div_precision_increment.map(i64::from).unwrap_or(4),
        dag_req,
    })
}

/// Go `ExecutorListsToTree` (`cop_handler.go`) — NAMED BOUNDARY. The
/// legacy tree form it builds hangs children on per-type child fields
/// (`Selection.Child`, `Limit.Child`, ...) which the trimmed `tipb` build
/// does not carry: it kept only the MODERN flat-list DAG, where order and
/// `parent_idx` are the structure. The execution course therefore consumes
/// the list directly, and this validation enforces the same invariants the
/// tree conversion would have panicked on.
///
/// Panics carry Go's exact `invalid parentIdx` message; the leaf check
/// mirrors what `buildClosureExecutor` requires — the first executor is the
/// scan and the only scan.
pub fn validate_executor_list(executors: &[tipb::Executor]) {
    let len = executors.len();
    for (i, executor) in executors.iter().enumerate() {
        let tp = executor.tp();
        let is_scan = tp == tipb::ExecType::TypeTableScan || tp == tipb::ExecType::TypeIndexScan;
        assert!(
            (i == 0) == is_scan,
            "executor {i} has type {tp:?}: the first executor is the scan, and the only scan"
        );
        if i + 1 < len {
            let parent_idx = executor
                .parent_idx
                .map_or(i + 1, |idx| usize::try_from(idx).unwrap_or(usize::MAX));
            assert!(
                parent_idx > i && parent_idx < len,
                "invalid parentIdx: {parent_idx}, for index: {i}"
            );
        }
    }
}

/// Coprocessor expression storage. Production scalar functions use `Shared`
/// and Go's recursive PBToExpr contract; simple leaves retain their cheap row
/// representation. `Func` remains for direct legacy evaluator fixtures.
#[derive(Clone, Debug)]
pub enum SimpleExpr {
    /// `ExprType_Null`.
    Null,
    /// `ExprType_Int64`, val codec-int decoded.
    Int(i64),
    /// `ExprType_ColumnRef`, val codec-int decoded to the row OFFSET —
    /// `distsql_builtin.go:1222`.
    Column(usize),
    /// `ExprType_String`, the raw literal bytes with no codec around them.
    /// The comparison's collation decides how these order, as in Go.
    Bytes(Vec<u8>),
    /// `ExprType_MysqlDecimal`, val decimal-codec decoded
    /// (`distsql_builtin.go`'s `decodeValueList` -> `codec.DecodeDecimal`).
    Decimal(tidb_datatype::Decimal),
    /// `ExprType_MysqlJson`, val codec-datum decoded
    /// (`distsql_builtin.go`'s `convertJSON` -> `codec.DecodeOne`).
    Json(tidb_datatype::BinaryJSON),
    /// `ExprType_Float64`, val decoded as Go `codec.DecodeFloat` -- eight
    /// big-endian IEEE-754 bits (`distsql_builtin.go`'s `convertFloat`).
    Real(f64),
    /// `ExprType_MysqlTime`, val codec-uint decoded into Go's PACKED form
    /// (`distsql_builtin.go`'s `MysqlTime` arm -> `types.NewTime` from
    /// `FromPackedUint`). The type and fsp come from the leaf's own field
    /// type, as they do there.
    Time(tidb_datatype::Time),
    /// A catalog-admitted scalar decoded into the shared builtin tree,
    /// retaining the request that owns its evaluation settings.
    Shared(Arc<SharedExpression>),
}

/// Go `PBToExpr` for catalog-admitted signatures and their argument casts.
fn convert_shared(
    expr: &tipb::Expr,
    context: &Arc<RequestEvalContext>,
) -> Result<SimpleExpr, String> {
    tidb_expr::distsql_builtin::pb_to_expr_in(expr, &context.column_types, &context.zone).map(
        |expression| {
            SimpleExpr::Shared(Arc::new(SharedExpression {
                expression,
                context: Arc::clone(context),
            }))
        },
    )
}

/// Evaluates a [`SimpleExpr::Shared`] node over one scanned row.
fn eval_shared(
    expr: &SharedExpression,
    row: &[tidb_datatype::Datum],
) -> Result<tidb_datatype::Datum, CopExecError> {
    let row = tidb_chunk::mutrow::MutRow::from_datums(row);
    expr.expression
        .eval(expr.context.as_ref(), row.to_row())
        .map_err(CopExecError::from)
}

/// Decode without a DAG for standalone callers. Request execution uses
/// `convert_expr_with_context` to retain its own settings and scan types.
pub fn convert_expr(expr: &tipb::Expr) -> Result<SimpleExpr, String> {
    use tidb_expr::Columns;
    let context = Arc::new(RequestEvalContext::new(
        tidb_expr::NoColumns.time_zone(),
        4,
        tidb_model::flags::FLAG_TRUNCATE_AS_WARNING
            | tidb_model::flags::FLAG_DIVIDED_BY_ZERO_AS_WARNING,
    ));
    convert_expr_with_context(expr, &context)
}

fn convert_expr_with_context(
    expr: &tipb::Expr,
    context: &Arc<RequestEvalContext>,
) -> Result<SimpleExpr, String> {
    let tp = expr.tp();
    if tp == tipb::ExprType::ScalarFunc {
        return convert_shared(expr, context);
    }
    if tp != tipb::ExprType::ScalarFunc && tp != tipb::ExprType::ColumnRef {
        use tidb_datatype::Datum;
        use tidb_expr::{constant::Constant, expression::Expression};
        // All wire literals use Go's shared decoding contract, including
        // UTC TIMESTAMP conversion. Keep cheap legacy leaves where their
        // representation preserves the datum, without materializing a row.
        let expression =
            tidb_expr::distsql_builtin::pb_to_expr_in(expr, &context.column_types, &context.zone)?;
        return Ok(match expression {
            Expression::Constant(Constant {
                value: Datum::Null, ..
            }) => SimpleExpr::Null,
            Expression::Constant(Constant {
                value: Datum::Int(value),
                ..
            }) => SimpleExpr::Int(value),
            Expression::Constant(Constant {
                value: Datum::Real(value),
                ..
            }) => SimpleExpr::Real(value),
            Expression::Constant(Constant {
                value: Datum::Bytes(value),
                ..
            }) => SimpleExpr::Bytes(value),
            Expression::Constant(Constant {
                value: Datum::Time(value),
                ..
            }) => SimpleExpr::Time(value),
            Expression::Constant(Constant {
                value: Datum::Decimal(value),
                ..
            }) => SimpleExpr::Decimal(value),
            Expression::Constant(Constant {
                value: Datum::Json(value),
                ..
            }) => SimpleExpr::Json(value),
            expression => SimpleExpr::Shared(Arc::new(SharedExpression {
                expression,
                context: Arc::clone(context),
            })),
        });
    }
    if tp == tipb::ExprType::ColumnRef {
        let (_, offset) = tidb_codec::decode_int(expr.val())
            .map_err(|err| format!("invalid column offset: {err:?}"))?;
        return Ok(SimpleExpr::Column(
            usize::try_from(offset).map_err(|_| "negative column offset".to_owned())?,
        ));
    }
    Err(format!(
        "expr type {tp:?} waits on its distsql_builtin.go course"
    ))
}

// Preserve SQL NULL independently of expression evaluation errors. Each caller
// evaluates children in place, so lazy branches retain Go's evaluation order.
macro_rules! nullable {
    ($value:expr) => {
        match $value {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}

/// The collation id a comparison is evaluated under, taken from the
/// `ScalarFunc`'s own field type as Go does.
///
/// The wire carries the PROTOCOL id, which TiDB negates when new collations
/// are enabled (`rewriteNewCollationIDIfNeeded`). `get_collator_by_id` wants
/// the registry id, so the negation is undone here -- without it every
/// comparison silently ran under the default collator, and a
/// `utf8mb4_general_ci` column compared case-SENSITIVELY.
/// Go `fieldTypeFromPBColumn` (`unistore/cophandler/cop_handler.go`).
///
/// The type CODE alone is not the column's type. Dropping the rest decodes
/// the stored bytes under the wrong rules: without `flag` an UNSIGNED column
/// reads back signed, so `4294967295` in an `INT UNSIGNED` became `-1` on the
/// scan path while the point-get path -- which builds its types from the
/// catalog -- returned the stored value. Two paths, one table, two answers.
fn field_type_from_pb_column(column: &tipb::ColumnInfo) -> tidb_datatype::FieldType {
    use tidb_datatype::{FieldType, FieldTypeCode};
    let code = u8::try_from(column.tp()).unwrap_or(0);
    let mut field_type = FieldType::new(FieldTypeCode::from_mysql_type(code))
        .with_flen(i64::from(column.column_len()))
        .with_decimal(i64::from(column.decimal()));
    field_type.add_flags(u32::try_from(column.flag()).unwrap_or(0));
    if !column.elems.is_empty() {
        // ENUM/SET decode their stored ordinal against this list.
        field_type = field_type.with_elems(column.elems.iter().map(String::as_str));
    }
    // Go `RestoreCollationIDIfNeeded` then `GetCharsetInfoByID`. A zero id is
    // the wire's "unset" and would resolve to a collation the column does not
    // have, so it is left alone.
    //
    // Nothing in this handler's filter CONSULTS this: a pushed-down string
    // comparison carries its collation on the expression (its argument's
    // field type, as `tidb_expr::distsql_builtin::pb_to_expr` decodes it),
    // which is where Go reads it from too. It is carried for fidelity with `fieldTypeFromPBColumn`, so a
    // later reader that does consult the column's own collation finds the
    // one the request named rather than a default.
    if column.collation() != 0 {
        let name = tidb_datatype::proto_to_collation(column.collation());
        if !name.is_empty() {
            field_type.set_collation_name(name);
        }
    }
    field_type
}

fn collation_of(expr: &tipb::Expr) -> i32 {
    let protocol = expr
        .field_type
        .as_ref()
        .map_or(0, |field_type| field_type.collate());
    tidb_datatype::restore_collation_id_if_needed(protocol)
}

/// Go `closureExecutor.processSelection`: the conditions run in order and the
/// first one that is NULL or zero rejects the row WITHOUT evaluating the rest,
/// so `b <> '(' AND regexp_like(a, b)` never compiles the pattern of a row the
/// first condition already dropped.
fn conditions_hold(
    conditions: &[SimpleExpr],
    row: &[tidb_datatype::Datum],
) -> Result<bool, CopExecError> {
    for condition in conditions {
        // A builtin condition's evaluation error keeps its terror code for
        // `SelectResponse.error`.
        let value = match condition {
            SimpleExpr::Shared(shared) => match eval_shared(shared, row)? {
                tidb_datatype::Datum::Int(value) => Some(i128::from(value)),
                tidb_datatype::Datum::UInt(value) => Some(i128::from(value)),
                tidb_datatype::Datum::Null => None,
                other => Some(shared.context.condition_value(&other)?),
            },
            other => eval_expr(other, row)?,
        };
        if !value.is_some_and(|value| value != 0) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Evaluates one pushed-down expression against a scanned row: MySQL's
/// three-valued int (`Some(0/1/n)`) or NULL, beside Go's expression-level
/// error -- the 1690 overflow terror the request-level answer carries.
///
/// The value is carried as `i128` because an integer column is either signed
/// or UNSIGNED and the two domains do not fit in one 64-bit slot: a
/// `BIGINT UNSIGNED` above `i64::MAX` and a negative `BIGINT` must both
/// compare exactly. Go reaches the same result by selecting a signedness
/// -specific comparison signature per operand pair; one wider integer settles
/// every pairing at once, which is what a filter that must never invent a row
/// needs.
pub fn eval_expr(expr: &SimpleExpr, row: &[tidb_datatype::Datum]) -> Result<Option<i128>, String> {
    use tidb_datatype::Datum;
    let value = match expr {
        // Shared builtins retain their result type. Go converts a filter
        // result with ToBool; NULL remains UNKNOWN.
        SimpleExpr::Shared(shared) => match eval_shared(shared, row)? {
            Datum::Int(value) => Some(i128::from(value)),
            Datum::UInt(value) => Some(i128::from(value)),
            Datum::Null => None,
            other => Some(shared.context.condition_value(&other)?),
        },
        SimpleExpr::Null => None,
        SimpleExpr::Int(value) => Some(i128::from(*value)),
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::Int(value)) => Some(i128::from(*value)),
            // An UNSIGNED column decodes to this, and dropping it here made
            // every predicate over such a column filter the row out.
            Some(Datum::UInt(value)) => Some(i128::from(*value)),
            Some(Datum::Null) | None => None,
            Some(_) => None, // non-int columns wait on their course
        },
        // A bare string, decimal, real or json is not a truth value; only a
        // comparison reads them.
        SimpleExpr::Bytes(_)
        | SimpleExpr::Decimal(_)
        | SimpleExpr::Json(_)
        | SimpleExpr::Real(_)
        | SimpleExpr::Time(_) => None,
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    /// The parse anchor for expression evaluation: UTC for the suite.
    fn zone() -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }

    use super::*;

    // All WRITTEN: Go's cop_handler coverage rides the store's RPC suites.

    #[test]
    fn unknown_request_types_answer_gos_exact_message() {
        let mut store = MvccStore::new();
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: 999,
                ..coprocessor::Request::default()
            },
        );
        assert_eq!(resp.other_error, "unsupported request type 999");
    }

    #[test]
    fn analyze_empty_ranges_return_empty_response_like_go() {
        let mut store = MvccStore::new();
        let response = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_ANALYZE,
                ..Default::default()
            },
        );
        assert!(response.other_error.is_empty());
        assert!(response.data.is_empty());
    }

    #[test]
    fn checksum_answers_gos_stub_response() {
        // Go's `handleCopChecksumRequest` (`cop_handler.go:750`): unistore
        // never computes checksums; it answers the fixed placeholder
        // ChecksumResponse{1, 1, 1} as a SUCCESS, not an error.
        let mut store = MvccStore::new();
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_CHECKSUM,
                ..coprocessor::Request::default()
            },
        );
        assert!(
            resp.other_error.is_empty(),
            "the stub must not be an error: {}",
            resp.other_error
        );
        // tipb.ChecksumResponse fields 1-3, each varint 1.
        assert_eq!(resp.data, vec![0x08, 0x01, 0x10, 0x01, 0x18, 0x01]);
    }

    #[test]
    fn a_malformed_range_answers_gos_validation_error() {
        // Go's `extractKVRanges` (`cop_handler.go:678`): start >= end is a
        // wire-visible rejection, with Go's `%v` byte-slice rendering.
        let mut store = MvccStore::new();
        let dag = tipb::DagRequest::default();
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: vec![1, 2],
                    end: vec![1, 2],
                    ..coprocessor::KeyRange::default()
                }],
                ..coprocessor::Request::default()
            },
        );
        assert_eq!(
            resp.other_error,
            "invalid range, start should be smaller than end: [1 2] [1 2]"
        );
    }

    #[test]
    fn an_empty_range_list_is_gos_null_range_error() {
        let err = build_dag(&coprocessor::Request {
            tp: REQ_TYPE_DAG,
            ..coprocessor::Request::default()
        })
        .expect_err("no ranges");
        assert_eq!(err, "request range is null");
    }

    #[test]
    fn unknown_executor_kind_is_not_a_default_table_scan() {
        let dag = tipb::DagRequest {
            executors: vec![tipb::Executor {
                tp: Some(i32::MAX),
                ..Default::default()
            }],
            ..Default::default()
        };
        let request = coprocessor::Request {
            tp: REQ_TYPE_DAG,
            data: dag.encode_to_vec(),
            ranges: vec![coprocessor::KeyRange::default()],
            ..Default::default()
        };
        assert!(build_dag(&request)
            .unwrap_err()
            .contains("unknown executor type"));
    }

    #[test]
    fn a_dag_decodes_with_its_zone_split_three_ways() {
        let dag = tipb::DagRequest {
            time_zone_offset: Some(3600),
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let req = coprocessor::Request {
            tp: REQ_TYPE_DAG,
            data,
            ranges: vec![coprocessor::KeyRange::default()],
            start_ts: 42,
            ..coprocessor::Request::default()
        };
        let context = build_dag(&req).expect("parses");
        assert_eq!(context.start_ts, 42);
        assert_eq!(context.time_zone, TimeZoneSpec::FixedOffset(3600));
        assert_eq!(
            context.time_zone.resolve().expect("fixed zone"),
            tidb_datatype::SessionTimeZone::Fixed {
                name: "UTC".to_owned(),
                offset_secs: 3600,
            }
        );

        let named = tipb::DagRequest {
            time_zone_name: Some("Asia/Shanghai".to_owned()),
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        named.encode(&mut data).expect("encodes");
        let context = build_dag(&coprocessor::Request {
            tp: REQ_TYPE_DAG,
            data,
            ranges: vec![coprocessor::KeyRange::default()],
            ..coprocessor::Request::default()
        })
        .expect("parses");
        assert_eq!(
            context.time_zone,
            TimeZoneSpec::Named("Asia/Shanghai".to_owned())
        );
        assert_eq!(
            context.time_zone.resolve().expect("named zone"),
            tidb_datatype::SessionTimeZone::Named(chrono_tz::Asia::Shanghai)
        );
        assert!(TimeZoneSpec::Named("Not/AZone".to_owned())
            .resolve()
            .is_err());
    }

    #[test]
    fn a_leaf_first_list_validates() {
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        let selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            ..tipb::Executor::default()
        };
        let limit = tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            ..tipb::Executor::default()
        };
        validate_executor_list(&[scan, selection, limit]);
    }

    #[test]
    #[should_panic(expected = "invalid parentIdx")]
    fn a_backward_parent_index_panics_with_gos_message() {
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        let mut selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            ..tipb::Executor::default()
        };
        selection.parent_idx = Some(0);
        let limit = tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            ..tipb::Executor::default()
        };
        validate_executor_list(&[scan, selection, limit]);
    }

    #[test]
    #[should_panic(expected = "the first executor is the scan")]
    fn a_scan_in_the_middle_is_refused() {
        let selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            ..tipb::Executor::default()
        };
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        validate_executor_list(&[selection, scan]);
    }

    #[test]
    fn a_table_scan_reads_back_what_a_transaction_wrote() {
        // The milestone the store roadmap aims at: a row written through the
        // TRANSACTION path, read back through the COPROCESSOR path — the
        // exact two protocols a TiDB node speaks to its store, end to end
        // in-process. The row value is the OLD row format (per-column
        // `EncodeInt(colID) ++ EncodeDatum(value)`), which
        // `decode_table_row_to_map` handles exactly as Go's decoder does.
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};

        let mut store = MvccStore::new();
        let table_id = 42_i64;
        // Two rows: (handle 1, b=77), (handle 2, b=88).
        for (handle, b_value) in [(1_i64, 77_i64), (2, 88)] {
            let key = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
            let value =
                tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(b_value)]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        }

        // The DAG a distsql client sends for `SELECT id, b FROM t`.
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                columns: vec![
                    tipb::ColumnInfo {
                        column_id: Some(1),
                        tp: Some(8), // TypeLonglong
                        pk_handle: Some(true),
                        ..tipb::ColumnInfo::default()
                    },
                    tipb::ColumnInfo {
                        column_id: Some(2),
                        tp: Some(8),
                        ..tipb::ColumnInfo::default()
                    },
                ],
                ..tipb::TableScan::default()
            }),
            ..tipb::Executor::default()
        };
        let dag = tipb::DagRequest {
            executors: vec![scan],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(table_id);
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: range_start,
                    end: range_end,
                }],
                start_ts: 20,
                ..coprocessor::Request::default()
            },
        );
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        assert!(resp.locked.is_none());
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("a select response");
        assert_eq!(
            select.encode_type,
            Some(tipb::EncodeType::TypeDefault as i32)
        );
        assert_eq!(select.chunks.len(), 1);
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        // Decode the datum stream back: 2 rows x 2 columns.
        let decoded = tidb_codec::decode(rows_data, 4).expect("four datums");
        assert_eq!(
            decoded,
            vec![Datum::Int(1), Datum::Int(77), Datum::Int(2), Datum::Int(88)]
        );
    }

    /// Go `newRowDecoder` (`cop_handler.go:500`) falls back to the
    /// request's `PrimaryColumnIds` when no column carries `PkHandle`, and
    /// fills those columns from the row KEY -- which is where a CLUSTERED
    /// table's primary key lives. A scan of such a table used to refuse
    /// outright, and then to answer NULL for every primary-key column.
    #[test]
    fn a_clustered_primary_key_is_read_back_out_of_the_row_key() {
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};

        let mut store = MvccStore::new();
        let table_id = 77_i64;
        // `CREATE TABLE t (a INT, b INT, c INT, PRIMARY KEY (a, b))`: the
        // key carries (a, b), the value carries only c.
        for (a, b, c) in [(1_i64, 2_i64, 30_i64), (3, 4, 50)] {
            let handle =
                tidb_codec::encode_key(&[Datum::Int(a), Datum::Int(b)]).expect("a common handle");
            let key = encode_row_key_with_handle(table_id, &RecordHandle::Common(handle));
            let value = tidb_codec::encode_value(&[Datum::Int(3), Datum::Int(c)]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        }

        let column = |id: i64| tipb::ColumnInfo {
            column_id: Some(id),
            tp: Some(8), // TypeLonglong
            ..tipb::ColumnInfo::default()
        };
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                columns: vec![column(1), column(2), column(3)],
                // No column is `PkHandle`; these ids name the clustered key.
                primary_column_ids: vec![1, 2],
                ..tipb::TableScan::default()
            }),
            ..tipb::Executor::default()
        };
        let dag = tipb::DagRequest {
            executors: vec![scan],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(table_id);
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: range_start,
                    end: range_end,
                }],
                start_ts: 20,
                ..coprocessor::Request::default()
            },
        );
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("a select response");
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        let decoded = tidb_codec::decode(rows_data, 6).expect("six datums");
        assert_eq!(
            decoded,
            vec![
                Datum::Int(1),
                Datum::Int(2),
                Datum::Int(30),
                Datum::Int(3),
                Datum::Int(4),
                Datum::Int(50)
            ],
            "both key columns and the value column come back"
        );
    }

    /// Go `ChunkDecoder.DecodeToChunk` reads a column from the row VALUE and
    /// consults the handle "only when there is no corresponding column in the
    /// value". A `utf8_general_ci` clustered key stores the collation sort key
    /// in the row key and the data in the value; reading the key first
    /// answered the sort key as the column, and the table reader above then
    /// rebuilt a handle that no index entry had requested.
    #[test]
    fn a_clustered_key_column_reads_its_value_before_the_row_key() {
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};

        let mut store = MvccStore::new();
        let table_id = 78_i64;
        // `CREATE TABLE t (c VARCHAR(50) COLLATE utf8_general_ci, a INT,
        // PRIMARY KEY (c))`: the key carries c's sort key, the value carries
        // c itself (`NeedRestoredData`) and a.
        let collator = tidb_datatype::get_collator("utf8_general_ci");
        let handle = tidb_codec::encode_key(&[Datum::Bytes(collator.key(b" A   "))])
            .expect("a common handle");
        let key = encode_row_key_with_handle(table_id, &RecordHandle::Common(handle));
        let value = tidb_codec::encode_value(&[
            Datum::Int(1),
            Datum::Bytes(b" A   ".to_vec()),
            Datum::Int(2),
            Datum::Int(5),
        ])
        .expect("row");
        store
            .prewrite(&crate::mvcc_store::PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Put as i32,
                    key: key.clone(),
                    value,
                    ..KvrpcMutation::default()
                }],
                primary_lock: key.clone(),
                start_version: 10,
                ..crate::mvcc_store::PrewriteReq::default()
            })
            .expect("prewrites");
        store.commit(&[key], 10, 11).expect("commits");

        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                columns: vec![
                    tipb::ColumnInfo {
                        column_id: Some(1),
                        tp: Some(15), // TypeVarchar
                        column_len: Some(50),
                        collation: Some(tidb_datatype::collation_to_proto("utf8_general_ci")),
                        ..tipb::ColumnInfo::default()
                    },
                    tipb::ColumnInfo {
                        column_id: Some(2),
                        tp: Some(8), // TypeLonglong
                        ..tipb::ColumnInfo::default()
                    },
                ],
                primary_column_ids: vec![1],
                ..tipb::TableScan::default()
            }),
            ..tipb::Executor::default()
        };
        let dag = tipb::DagRequest {
            executors: vec![scan],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(table_id);
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: range_start,
                    end: range_end,
                }],
                start_ts: 20,
                ..coprocessor::Request::default()
            },
        );
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("a select response");
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        let decoded = tidb_codec::decode(rows_data, 2).expect("two datums");
        let c = match &decoded[0] {
            Datum::Bytes(bytes) => bytes.clone(),
            Datum::String(string) => string.bytes().to_vec(),
            other => panic!("c decoded as {other:?}"),
        };
        assert_eq!(c, b" A   ", "c is the stored value, not its sort key");
        assert_eq!(decoded[1], Datum::Int(5));
    }

    /// Go `distsql_builtin.go` builds the string comparison signatures with
    /// the collation the request carries, so `utf8mb4_bin` and
    /// `utf8mb4_general_ci` order the same bytes differently. The wire id is
    /// NEGATED when new collations are on, and forgetting to undo that ran
    /// every comparison under the default collator -- caught by running the
    /// server, where a `general_ci` column compared case-sensitively.
    #[test]
    fn a_string_comparison_uses_the_collation_the_request_carries() {
        use tidb_datatype::Datum;

        let compare = |collation_name: &str, literal: &str, value: &str| {
            let expr = tipb::Expr {
                tp: Some(tipb::ExprType::ScalarFunc as i32),
                sig: Some(tipb::ScalarFuncSig::EqString as i32),
                children: vec![
                    tipb::Expr {
                        tp: Some(tipb::ExprType::ColumnRef as i32),
                        val: Some({
                            let mut offset = Vec::new();
                            tidb_codec::encode_int(&mut offset, 0);
                            offset
                        }),
                        field_type: Some(tipb::FieldType {
                            tp: Some(253),
                            charset: Some("utf8mb4".to_owned()),
                            collate: Some(tidb_datatype::collation_to_proto(collation_name)),
                            ..Default::default()
                        }),
                        ..tipb::Expr::default()
                    },
                    tipb::Expr {
                        tp: Some(tipb::ExprType::String as i32),
                        val: Some(literal.as_bytes().to_vec()),
                        field_type: Some(tipb::FieldType {
                            tp: Some(253),
                            charset: Some("utf8mb4".to_owned()),
                            collate: Some(tidb_datatype::collation_to_proto(collation_name)),
                            ..Default::default()
                        }),
                        ..tipb::Expr::default()
                    },
                ],
                field_type: Some(tipb::FieldType {
                    tp: Some(8),
                    collate: Some(tidb_datatype::collation_to_proto(collation_name)),
                    ..tipb::FieldType::default()
                }),
                ..tipb::Expr::default()
            };
            let converted = convert_expr(&expr).expect("a string comparison converts");
            eval_expr(&converted, &[Datum::new_string(value.to_owned())]).expect("evals")
        };

        // Case-sensitive under the binary collation.
        assert_eq!(compare("utf8mb4_bin", "A", "A"), Some(1));
        assert_eq!(compare("utf8mb4_bin", "a", "A"), Some(0));
        // Case-insensitive under the general_ci collation.
        assert_eq!(compare("utf8mb4_general_ci", "a", "A"), Some(1));
        assert_eq!(compare("utf8mb4_general_ci", "A", "A"), Some(1));
        assert_eq!(compare("utf8mb4_general_ci", "b", "A"), Some(0));
    }

    #[test]
    fn a_lock_in_the_scanned_range_answers_locked() {
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        let key = encode_row_key_with_handle(7, &RecordHandle::Int(1));
        store
            .prewrite(&crate::mvcc_store::PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Put as i32,
                    key: key.clone(),
                    value: b"v".to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: key,
                start_version: 5,
                ..crate::mvcc_store::PrewriteReq::default()
            })
            .expect("prewrites");
        let dag = tipb::DagRequest {
            executors: vec![tipb::Executor {
                tp: Some(tipb::ExecType::TypeTableScan as i32),
                tbl_scan: Some(tipb::TableScan::default()),
                ..tipb::Executor::default()
            }],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(7);
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: range_start,
                    end: range_end,
                }],
                start_ts: 9,
                ..coprocessor::Request::default()
            },
        );
        let locked = resp.locked.expect("the first lock answers the response");
        assert_eq!(locked.lock_version, 5);
    }

    #[test]
    fn a_limit_above_the_scan_breaks_at_the_count() {
        // Go `ce.limit`: nothing but a break during the scan.
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        for handle in 1..=3_i64 {
            let key = encode_row_key_with_handle(9, &RecordHandle::Int(handle));
            let value =
                tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(handle * 10)]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        }
        let dag = tipb::DagRequest {
            executors: vec![
                tipb::Executor {
                    tp: Some(tipb::ExecType::TypeTableScan as i32),
                    tbl_scan: Some(tipb::TableScan {
                        table_id: Some(9),
                        columns: vec![tipb::ColumnInfo {
                            column_id: Some(1),
                            tp: Some(8),
                            pk_handle: Some(true),
                            ..tipb::ColumnInfo::default()
                        }],
                        ..tipb::TableScan::default()
                    }),
                    ..tipb::Executor::default()
                },
                tipb::Executor {
                    tp: Some(tipb::ExecType::TypeLimit as i32),
                    limit: Some(Box::new(tipb::Limit {
                        limit: Some(2),
                        ..tipb::Limit::default()
                    })),
                    ..tipb::Executor::default()
                },
            ],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(9);
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: range_start,
                    end: range_end,
                }],
                start_ts: 20,
                ..coprocessor::Request::default()
            },
        );
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("decodes");
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        let decoded = tidb_codec::decode(rows_data, 2).expect("two datums");
        assert_eq!(
            decoded,
            vec![Datum::Int(1), Datum::Int(2)],
            "first two handles"
        );
    }

    #[test]
    fn a_selection_filters_rows_with_mysql_three_valued_logic() {
        // `SELECT id, b FROM t WHERE b = 88` over three rows — only the
        // matching row survives; a condition over NULL drops the row.
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        for (handle, b_value) in [
            (1_i64, Datum::Int(77)),
            (2, Datum::Int(88)),
            (3, Datum::Null),
        ] {
            let key = encode_row_key_with_handle(11, &RecordHandle::Int(handle));
            let value = tidb_codec::encode_value(&[Datum::Int(2), b_value]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        }
        // b = 88: EqInt(ColumnRef(1), Int64(88)).
        let mut col_offset = Vec::new();
        tidb_codec::encode_int(&mut col_offset, 1);
        let mut lit = Vec::new();
        tidb_codec::encode_int(&mut lit, 88);
        let condition = tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(tipb::ScalarFuncSig::EqInt as i32),
            field_type: Some(tipb::FieldType {
                tp: Some(8),
                ..Default::default()
            }),
            children: vec![
                tipb::Expr {
                    tp: Some(tipb::ExprType::ColumnRef as i32),
                    val: Some(col_offset),
                    ..tipb::Expr::default()
                },
                tipb::Expr {
                    tp: Some(tipb::ExprType::Int64 as i32),
                    val: Some(lit),
                    ..tipb::Expr::default()
                },
            ],
            ..tipb::Expr::default()
        };
        let dag = tipb::DagRequest {
            executors: vec![
                tipb::Executor {
                    tp: Some(tipb::ExecType::TypeTableScan as i32),
                    tbl_scan: Some(tipb::TableScan {
                        table_id: Some(11),
                        columns: vec![
                            tipb::ColumnInfo {
                                column_id: Some(1),
                                tp: Some(8),
                                pk_handle: Some(true),
                                ..tipb::ColumnInfo::default()
                            },
                            tipb::ColumnInfo {
                                column_id: Some(2),
                                tp: Some(8),
                                ..tipb::ColumnInfo::default()
                            },
                        ],
                        ..tipb::TableScan::default()
                    }),
                    ..tipb::Executor::default()
                },
                tipb::Executor {
                    tp: Some(tipb::ExecType::TypeSelection as i32),
                    selection: Some(Box::new(tipb::Selection {
                        conditions: vec![condition],
                        ..Default::default()
                    })),
                    ..tipb::Executor::default()
                },
            ],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(11);
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: range_start,
                    end: range_end,
                }],
                start_ts: 20,
                ..coprocessor::Request::default()
            },
        );
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("decodes");
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        let decoded = tidb_codec::decode(rows_data, 2).expect("one row");
        assert_eq!(decoded, vec![Datum::Int(2), Datum::Int(88)]);
    }

    /// The region-side partial aggregation, end to end through the store:
    /// grouped `SUM` and `COUNT` follow MySQL's NULL rules -- a NULL adds
    /// nothing and counts nothing -- and the hash groups leave in
    /// group-key order, each row as `[aggregates..., group keys...]`.
    ///
    /// Focused test: Go's `cophandler` covers aggregation through
    /// `closure_exec.go`'s builder against testkit DAGs, which this trimmed
    /// wire path cannot replay verbatim.
    #[test]
    fn a_grouped_partial_aggregation_answers_per_group_rows() {
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};

        let mut store = MvccStore::new();
        let table_id = 77_i64;
        // (g, v): (1,10), (1,20), (2,40), (2,80), (1,NULL).
        let rows = [
            (1_i64, Datum::Int(1), Datum::Int(10)),
            (2, Datum::Int(1), Datum::Int(20)),
            (3, Datum::Int(2), Datum::Int(40)),
            (4, Datum::Int(2), Datum::Int(80)),
            (5, Datum::Int(1), Datum::Null),
        ];
        for (handle, g, v) in rows {
            let key = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
            // The OLD row format: `EncodeInt(colID) ++ EncodeDatum(value)`
            // per column -- column 1 carries g, column 2 carries v.
            let value =
                tidb_codec::encode_value(&[Datum::Int(1), g, Datum::Int(2), v]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        }

        let column = |id: i64| tipb::ColumnInfo {
            column_id: Some(id),
            tp: Some(8), // TypeLonglong
            ..tipb::ColumnInfo::default()
        };
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                columns: vec![column(1), column(2)],
                ..tipb::TableScan::default()
            }),
            ..tipb::Executor::default()
        };
        let column_ref = |offset: i64| tipb::Expr {
            tp: Some(tipb::ExprType::ColumnRef as i32),
            val: Some({
                let mut encoded = Vec::new();
                tidb_codec::encode_int(&mut encoded, offset);
                encoded
            }),
            ..tipb::Expr::default()
        };
        let agg_of = |tp: tipb::ExprType, child: tipb::Expr| tipb::Expr {
            tp: Some(tp as i32),
            children: vec![child],
            ..tipb::Expr::default()
        };
        let aggregation = tipb::Executor {
            tp: Some(tipb::ExecType::TypeAggregation as i32),
            aggregation: Some(Box::new(tipb::Aggregation {
                group_by: vec![column_ref(0)],
                agg_func: vec![
                    agg_of(tipb::ExprType::Sum, column_ref(1)),
                    agg_of(tipb::ExprType::Count, column_ref(1)),
                ],
                streamed: Some(false),
                ..Default::default()
            })),
            ..tipb::Executor::default()
        };
        let dag = tipb::DagRequest {
            executors: vec![scan, aggregation],
            output_offsets: vec![0, 1, 2],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(table_id);
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: range_start,
                    end: range_end,
                }],
                start_ts: 20,
                ..coprocessor::Request::default()
            },
        );
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("a select response");
        assert_eq!(select.chunks.len(), 1);
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        // 2 groups x 3 columns: [sum, count, g] each.
        let decoded = tidb_codec::decode(rows_data, 6).expect("six datums");
        let shown: Vec<String> = decoded
            .iter()
            .map(|datum| match datum {
                Datum::Int(value) => value.to_string(),
                Datum::Decimal(value) => value.to_string(),
                other => format!("{other:?}"),
            })
            .collect();
        // g=1: SUM skips the NULL (30), COUNT(v) skips the NULL (2).
        // g=2: 40+80 and both rows count.
        assert_eq!(shown, ["30", "2", "1", "120", "2", "2"]);
    }

    /// Both aggregate executor types reuse interleaved groups in first-seen
    /// order, and COUNT(1) includes a row whose column is NULL.
    #[test]
    fn a_streamed_partial_aggregation_reuses_groups_in_first_seen_order() {
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};

        let mut store = MvccStore::new();
        let table_id = 78_i64;
        // Scan order (by handle): g = 5, 3, 5 -- group 5 is reused.
        let rows = [
            (1_i64, Datum::Int(5), Datum::Null),
            (2, Datum::Int(3), Datum::Int(7)),
            (3, Datum::Int(5), Datum::Int(9)),
        ];
        for (handle, g, v) in rows {
            let key = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
            // The OLD row format: `EncodeInt(colID) ++ EncodeDatum(value)`
            // per column -- column 1 carries g, column 2 carries v.
            let value =
                tidb_codec::encode_value(&[Datum::Int(1), g, Datum::Int(2), v]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        }

        let column = |id: i64| tipb::ColumnInfo {
            column_id: Some(id),
            tp: Some(8),
            ..tipb::ColumnInfo::default()
        };
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                columns: vec![column(1), column(2)],
                ..tipb::TableScan::default()
            }),
            ..tipb::Executor::default()
        };
        let column_ref = |offset: i64| tipb::Expr {
            tp: Some(tipb::ExprType::ColumnRef as i32),
            val: Some({
                let mut encoded = Vec::new();
                tidb_codec::encode_int(&mut encoded, offset);
                encoded
            }),
            ..tipb::Expr::default()
        };
        let one = tipb::Expr {
            tp: Some(tipb::ExprType::Int64 as i32),
            val: Some({
                let mut encoded = Vec::new();
                tidb_codec::encode_int(&mut encoded, 1);
                encoded
            }),
            ..tipb::Expr::default()
        };
        let aggregation = tipb::Executor {
            tp: Some(tipb::ExecType::TypeStreamAgg as i32),
            aggregation: Some(Box::new(tipb::Aggregation {
                group_by: vec![column_ref(0)],
                agg_func: vec![tipb::Expr {
                    tp: Some(tipb::ExprType::Count as i32),
                    children: vec![one],
                    ..tipb::Expr::default()
                }],
                streamed: Some(true),
                ..Default::default()
            })),
            ..tipb::Executor::default()
        };
        let dag = tipb::DagRequest {
            executors: vec![scan, aggregation],
            output_offsets: vec![0, 1],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(table_id);
        let resp = handle_cop_request(
            &mut store,
            &coprocessor::Request {
                tp: REQ_TYPE_DAG,
                data,
                ranges: vec![coprocessor::KeyRange {
                    start: range_start,
                    end: range_end,
                }],
                start_ts: 20,
                ..coprocessor::Request::default()
            },
        );
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("a select response");
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        // [count, g] per group, group 5 first: the NULL row still counts.
        let decoded = tidb_codec::decode(rows_data, 4).expect("four datums");
        assert_eq!(
            decoded,
            vec![Datum::Int(2), Datum::Int(5), Datum::Int(1), Datum::Int(3)]
        );
    }

    /// Go's `TestTopNProcessor` shape (`closure_exec_test.go`): a pushed-down
    /// bounded sort over a table scan. The seed rows ride one transaction; the
    /// top-N node keeps the `limit` smallest keys in key order.
    ///
    /// `b` values 30/10/20 across handles 1/2/3.
    fn seed_three_rows(store: &mut MvccStore, table_id: i64) {
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        for (handle, b_value) in [(1_i64, 30_i64), (2, 10), (3, 20)] {
            let key = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
            let value =
                tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(b_value)]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        }
    }

    fn top_n_request(
        table_id: i64,
        column_offset: i64,
        desc: bool,
        limit: u64,
    ) -> coprocessor::Request {
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                columns: vec![
                    tipb::ColumnInfo {
                        column_id: Some(1),
                        tp: Some(8), // TypeLonglong
                        pk_handle: Some(true),
                        ..tipb::ColumnInfo::default()
                    },
                    tipb::ColumnInfo {
                        column_id: Some(2),
                        tp: Some(8),
                        ..tipb::ColumnInfo::default()
                    },
                ],
                ..tipb::TableScan::default()
            }),
            ..tipb::Executor::default()
        };
        let mut key_offset = Vec::new();
        tidb_codec::encode_int(&mut key_offset, column_offset);
        let top_n = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTopN as i32),
            top_n: Some(Box::new(tipb::TopN {
                order_by: vec![tipb::ByItem {
                    expr: Some(tipb::Expr {
                        tp: Some(tipb::ExprType::ColumnRef as i32),
                        val: Some(key_offset),
                        ..tipb::Expr::default()
                    }),
                    desc: Some(desc),
                    ..Default::default()
                }],
                limit: Some(limit),
                ..Default::default()
            })),
            ..tipb::Executor::default()
        };
        let dag = tipb::DagRequest {
            executors: vec![scan, top_n],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_codec::table_key::get_table_handle_key_range(table_id);
        coprocessor::Request {
            tp: REQ_TYPE_DAG,
            data,
            ranges: vec![coprocessor::KeyRange {
                start: range_start,
                end: range_end,
            }],
            start_ts: 20,
            ..coprocessor::Request::default()
        }
    }

    #[test]
    fn top_n_desc_keeps_the_largest_in_key_order() {
        use tidb_datatype::Datum;
        let mut store = MvccStore::new();
        seed_three_rows(&mut store, 71);
        let resp = handle_cop_request(&mut store, &top_n_request(71, 1, true, 2));
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("decodes");
        let mut decoded = Vec::new();
        for chunk in &select.chunks {
            decoded.extend(
                tidb_codec::decode(chunk.rows_data.as_deref().expect("rows"), 2).expect("row"),
            );
        }
        // DESC on b keeps the two largest, largest first.
        assert_eq!(
            decoded,
            vec![Datum::Int(1), Datum::Int(30), Datum::Int(3), Datum::Int(20),]
        );
    }

    #[test]
    fn top_n_asc_keeps_the_smallest_in_key_order() {
        use tidb_datatype::Datum;
        let mut store = MvccStore::new();
        seed_three_rows(&mut store, 72);
        let resp = handle_cop_request(&mut store, &top_n_request(72, 1, false, 2));
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("decodes");
        let mut decoded = Vec::new();
        for chunk in &select.chunks {
            decoded.extend(
                tidb_codec::decode(chunk.rows_data.as_deref().expect("rows"), 2).expect("row"),
            );
        }
        // ASC on b keeps the two smallest, smallest first.
        assert_eq!(
            decoded,
            vec![Datum::Int(2), Datum::Int(10), Datum::Int(3), Datum::Int(20)]
        );
    }

    /// Go's heap never lets a tie evict (`tryToAddRow` admits a row only when
    /// the root sorts STRICTLY after it), and `Finish` replays the survivors in
    /// heap-array order through the insertion sort. Eight handles with the
    /// order key `1, 2, 1, 2, 1, 1, 1, 1` and `LIMIT 3` leave the heap array
    /// `[5, 1, 3]` -- the order Go's `ORDER BY a LIMIT 3` answers.
    #[test]
    fn top_n_ties_never_evict_and_keep_go_heap_array_order() {
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        for (handle, b_value) in [
            (1_i64, 1_i64),
            (2, 2),
            (3, 1),
            (4, 2),
            (5, 1),
            (6, 1),
            (7, 1),
            (8, 1),
        ] {
            let key = encode_row_key_with_handle(73, &RecordHandle::Int(handle));
            let value =
                tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(b_value)]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        }
        let resp = handle_cop_request(&mut store, &top_n_request(73, 1, false, 3));
        assert!(resp.other_error.is_empty(), "{}", resp.other_error);
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("decodes");
        let mut decoded = Vec::new();
        for chunk in &select.chunks {
            decoded.extend(
                tidb_codec::decode(chunk.rows_data.as_deref().expect("rows"), 2).expect("row"),
            );
        }
        let handles: Vec<_> = decoded.chunks(2).map(|row| row[0].clone()).collect();
        assert_eq!(handles, vec![Datum::Int(5), Datum::Int(1), Datum::Int(3)]);
    }

    #[test]
    fn a_float64_literal_decodes_go_convert_float() {
        // Go convertFloat consumes the mem-comparable codec, not raw IEEE bits.
        let mut val = Vec::new();
        tidb_codec::encode_float(&mut val, 1.5);
        let expr = tipb::Expr {
            tp: Some(tipb::ExprType::Float64 as i32),
            val: Some(val),
            ..tipb::Expr::default()
        };
        match convert_expr(&expr).expect("decodes") {
            SimpleExpr::Real(value) => assert_eq!(value, 1.5),
            other => panic!("unexpected leaf: {other:?}"),
        }
    }
    #[test]
    fn encode_default_rows_carries_warnings_into_the_response() {
        let rows = vec![vec![tidb_datatype::Datum::Int(42)]];
        let warnings = vec![
            tipb::Error {
                code: Some(1265),
                msg: Some("Data truncated".to_owned()),
            },
            tipb::Error {
                code: Some(1690),
                msg: Some("BIGINT value is out of range".to_owned()),
            },
        ];
        let resp = encode_default_rows(rows, &zone(), warnings);
        assert!(resp.other_error.is_empty());
        let select = tipb::SelectResponse::decode(resp.data.as_ref()).expect("a select response");
        assert_eq!(select.warning_count, Some(2));
        assert_eq!(select.warnings.len(), 2);
        assert_eq!(select.warnings[0].code, Some(1265));
        assert_eq!(
            select.warnings[1].msg,
            Some("BIGINT value is out of range".to_owned())
        );
        // The data row still encodes correctly alongside the warnings.
        assert!(!select.chunks.is_empty(), "the data row should be present");
    }

    /// Every signature the pushdown catalog admits decodes here: the
    /// admission side and the execution side read the same inventory, as
    /// Go's `scalarExprSupportedByTiKV` and `getSignatureByPB` do.
    #[test]
    fn every_admitted_signature_decodes_like_go_pb_to_expr() {
        let column = |offset: i64| {
            let mut val = Vec::new();
            tidb_codec::encode_int(&mut val, offset);
            tipb::Expr {
                tp: Some(tipb::ExprType::ColumnRef as i32),
                val: Some(val),
                field_type: Some(tipb::FieldType {
                    tp: Some(8),
                    ..Default::default()
                }),
                ..Default::default()
            }
        };
        let mut refused = Vec::new();
        for row in tidb_expr::pushdown_catalog::CATALOG {
            let arity = row.arg_types.len().max(3);
            let expr = tipb::Expr {
                tp: Some(tipb::ExprType::ScalarFunc as i32),
                sig: Some(row.sig as i32),
                children: (0..arity as i64).map(column).collect(),
                field_type: Some(tipb::FieldType {
                    tp: Some(8),
                    ..Default::default()
                }),
                ..Default::default()
            };
            if let Err(message) = convert_expr(&expr) {
                refused.push((row.sig, message));
            }
        }
        assert!(refused.is_empty(), "admitted but undecodable: {refused:?}");
    }
}
