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
//! SEED of `cophandler` (~5k lines): the PARSE half lands here; DAG
//! EXECUTION (`closure_exec.go`'s scan-and-evaluate machine), analyze, and
//! checksum are the following courses, refusing by name until they land.
//!
//! # Narrowings, by name
//!
//! * `flagsAndTzToSessionContext` / `globalLocationMap`: the time-zone name
//!   resolves through Go's location cache into a `time.Location`. The parsed
//!   name and offset are CARRIED here ([`DagContext::time_zone`]) and
//!   resolution is the evaluation course's concern — nothing at the parse
//!   layer reads the zone.
//! * `mppCtx` / `HandleMPPDAGReq`: the MPP arm follows the MPP course.

use prost::Message;
use tidb_proto::coprocessor;
use tidb_proto::tipb;

use tidb_codec::table_key::RecordHandle;

use crate::mvcc_store::MvccStore;

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
    /// The decoded `tipb.DAGRequest`.
    pub dag_req: tipb::DagRequest,
    /// `keyRanges` from the coprocessor request.
    pub key_ranges: Vec<coprocessor::KeyRange>,
    /// `startTS` — Go reads `req.StartTs`.
    pub start_ts: u64,
    /// The request's time zone, parsed but unresolved (module header).
    pub time_zone: TimeZoneSpec,
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

/// Go `HandleCopRequest` (`cop_handler.go`): the type dispatch. The unknown
/// arm's message is Go's exact `fmt.Sprintf`.
pub fn handle_cop_request(
    store: &mut MvccStore,
    req: &coprocessor::Request,
) -> coprocessor::Response {
    match req.tp {
        REQ_TYPE_DAG => handle_cop_dag_request(store, req),
        REQ_TYPE_ANALYZE => other_error(
            "handleCopAnalyzeRequest (cophandler/analyze.go) is a later course of this port",
        ),
        REQ_TYPE_CHECKSUM => other_error(
            "handleCopChecksumRequest (cophandler/cop_handler.go) is a later course of this port",
        ),
        other => other_error(&format!("unsupported request type {other}")),
    }
}

fn other_error(message: &str) -> coprocessor::Response {
    coprocessor::Response {
        other_error: message.to_owned(),
        ..coprocessor::Response::default()
    }
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
    exec_dag(_store, &context)
}

/// The execution slice of Go `buildClosureExecutor` + `handleCopDAGRequest`
/// (`closure_exec.go`): a TABLE-SCAN-ONLY DAG runs; anything above the scan
/// refuses by name until its course lands.
fn exec_dag(store: &mut MvccStore, context: &DagContext) -> coprocessor::Response {
    validate_executor_list(&context.dag_req.executors);
    // Go's composition contract (`closure_exec.go:166`):
    // `tableScan|indexScan [selection] [topN | limit | agg]`. This slice
    // runs the scan and a LIMIT above it — Go's limit is nothing but a
    // break-at-count during the scan (`ce.limit`, `closure_exec.go:144`,
    // checked at `:597`).
    let mut limit = usize::MAX;
    let mut conditions: Vec<SimpleExpr> = Vec::new();
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
                match convert_expr(condition) {
                    Ok(expr) => conditions.push(expr),
                    Err(message) => return other_error(&message),
                }
            }
        } else {
            return other_error(
                "closure_exec.go's top-n and aggregation processors are later courses",
            );
        }
    }
    let scan = &context.dag_req.executors[0];
    if scan.tp() != tipb::ExecType::TypeTableScan {
        return other_error("index scans (closure_exec.go) are a later course of this port");
    }
    let Some(tbl_scan) = scan.tbl_scan.as_ref() else {
        return other_error("executor missing tbl_scan body");
    };
    exec_table_scan(store, context, tbl_scan, &conditions, limit)
}

/// Go's chunk cut: `closure_exec.go` grows the output chunk to 1024 rows
/// before starting the next.
const CHUNK_MAX_ROWS: usize = 1024;

/// The table-scan executor over the MVCC store: each range scanned at the
/// request's start ts, each surviving row decoded into the REQUESTED columns
/// in request order, datum-encoded into default-format chunks — the shape a
/// distsql client decodes.
///
/// Narrowings, by name: common handles and partitioned reads follow their
/// courses (`RecordHandle::Common`/`Partition` rows refuse); a requested
/// column absent from the row answers its `default_val` when carried, else
/// NULL — Go's `getDefaultValue` behavior for the null-capable slice.
fn exec_table_scan(
    store: &mut MvccStore,
    context: &DagContext,
    tbl_scan: &tipb::TableScan,
    conditions: &[SimpleExpr],
    limit: usize,
) -> coprocessor::Response {
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    let mut column_types = std::collections::BTreeMap::new();
    for column in &tbl_scan.columns {
        let code = u8::try_from(column.tp()).unwrap_or(0);
        column_types.insert(
            column.column_id(),
            FieldType::new(FieldTypeCode::from_mysql_type(code)),
        );
    }
    let mut chunks: Vec<tipb::Chunk> = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0_usize;
    let mut emitted = 0_usize;
    'ranges: for range in &context.key_ranges {
        let pairs = store.scan(&crate::mvcc_store::ScanReq {
            start_key: range.start.clone(),
            end_key: range.end.clone(),
            limit: u32::MAX,
            version: context.start_ts,
            sample_step: 0,
            reverse: false,
        });
        for pair in pairs {
            if let Some(lock) = pair.error {
                // Go: the FIRST lock met answers the whole response.
                return coprocessor::Response {
                    locked: Some(*lock),
                    ..coprocessor::Response::default()
                };
            }
            let handle = match tidb_codec::table_key::decode_row_key(&pair.key) {
                Ok(RecordHandle::Int(handle)) => handle,
                Ok(_) => {
                    return other_error(
                        "common-handle and partitioned scans are later courses of this port",
                    )
                }
                Err(err) => return other_error(&format!("invalid record key: {err:?}")),
            };
            let decoded =
                match tidb_tablecodec::decode_table_row_to_map(&pair.value, &column_types, None) {
                    Ok(map) => map,
                    Err(err) => return other_error(&format!("decode row failed: {err:?}")),
                };
            let mut row_datums = Vec::with_capacity(tbl_scan.columns.len());
            for column in &tbl_scan.columns {
                if column.pk_handle() {
                    row_datums.push(Datum::Int(handle));
                } else if let Some(datum) = decoded.get(&column.column_id()) {
                    row_datums.push(datum.clone());
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
            // Go `selectionProcessor`: a row survives only when EVERY
            // condition evaluates non-null and non-zero.
            if !conditions
                .iter()
                .all(|condition| eval_expr(condition, &row_datums).is_some_and(|v| v != 0))
            {
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
            let encoded = match tidb_codec::encode_value(&projected) {
                Ok(encoded) => encoded,
                Err(err) => return other_error(&format!("encode row failed: {err:?}")),
            };
            current.extend_from_slice(&encoded);
            current_rows += 1;
            emitted += 1;
            if current_rows == CHUNK_MAX_ROWS {
                chunks.push(tipb::Chunk {
                    rows_data: Some(std::mem::take(&mut current)),
                    ..tipb::Chunk::default()
                });
                current_rows = 0;
            }
            // Go `e.rowCount == e.limit` (`closure_exec.go:597`).
            if emitted == limit {
                break 'ranges;
            }
        }
    }
    if !current.is_empty() {
        chunks.push(tipb::Chunk {
            rows_data: Some(current),
            ..tipb::Chunk::default()
        });
    }
    let select = tipb::SelectResponse {
        chunks,
        encode_type: Some(tipb::EncodeType::TypeDefault as i32),
        ..tipb::SelectResponse::default()
    };
    let mut data = Vec::new();
    select.encode(&mut data).expect("a select response encodes");
    coprocessor::Response {
        data,
        ..coprocessor::Response::default()
    }
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
    let time_zone = match dag_req.time_zone_name() {
        "" => TimeZoneSpec::FixedOffset(dag_req.time_zone_offset()),
        "System" => TimeZoneSpec::System,
        name => TimeZoneSpec::Named(name.to_owned()),
    };
    Ok(DagContext {
        key_ranges: req.ranges.clone(),
        start_ts: req.start_ts,
        time_zone,
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

/// Go `expression.PBToExpr` (`pkg/expression/distsql_builtin.go`), the
/// INTEGER slice: column refs, null and int literals, the six int
/// comparisons, three-valued AND/OR/NOT, and IS NULL. Everything else
/// refuses naming that file — string comparisons wait on collation, casts
/// on the cast tables, IN on its value-list decode.
#[derive(Clone, Debug)]
pub enum SimpleExpr {
    /// `ExprType_Null`.
    Null,
    /// `ExprType_Int64`, val codec-int decoded.
    Int(i64),
    /// `ExprType_ColumnRef`, val codec-int decoded to the row OFFSET —
    /// `distsql_builtin.go:1222`.
    Column(usize),
    /// `ExprType_ScalarFunc` over a supported signature.
    Func(SimpleSig, Vec<SimpleExpr>),
}

/// The supported `ScalarFuncSig` subset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimpleSig {
    /// `LtInt`/`LeInt`/`GtInt`/`GeInt`/`EqInt`/`NeInt`, by ordering.
    LtInt,
    /// See [`SimpleSig::LtInt`].
    LeInt,
    /// See [`SimpleSig::LtInt`].
    GtInt,
    /// See [`SimpleSig::LtInt`].
    GeInt,
    /// See [`SimpleSig::LtInt`].
    EqInt,
    /// See [`SimpleSig::LtInt`].
    NeInt,
    /// `LogicalAnd` — MySQL three-valued: `NULL AND FALSE` is FALSE.
    LogicalAnd,
    /// `LogicalOr` — `NULL OR TRUE` is TRUE.
    LogicalOr,
    /// `UnaryNotInt`.
    UnaryNot,
    /// `IntIsNull`.
    IntIsNull,
    /// `InInt` — n-ary membership, `tested IN (e1, e2, ...)`.
    ///
    /// Go's cop evaluates this through the shared `expression` package
    /// (`builtinInIntSig`), whose null rule is MySQL's: a match answers TRUE;
    /// no match answers NULL when the tested value or any list element is
    /// NULL, FALSE otherwise.
    InInt,
}

/// Convert one wire expression, refusing what the slice does not carry.
pub fn convert_expr(expr: &tipb::Expr) -> Result<SimpleExpr, String> {
    let tp = expr.tp();
    if tp == tipb::ExprType::Null {
        return Ok(SimpleExpr::Null);
    }
    if tp == tipb::ExprType::Int64 {
        let (_, value) = tidb_codec::decode_int(expr.val())
            .map_err(|err| format!("invalid int literal: {err:?}"))?;
        return Ok(SimpleExpr::Int(value));
    }
    if tp == tipb::ExprType::ColumnRef {
        let (_, offset) = tidb_codec::decode_int(expr.val())
            .map_err(|err| format!("invalid column offset: {err:?}"))?;
        return Ok(SimpleExpr::Column(
            usize::try_from(offset).map_err(|_| "negative column offset".to_owned())?,
        ));
    }
    if tp == tipb::ExprType::ScalarFunc {
        // `expr.sig()` decodes the wire integer, an UNRECOGNIZED value
        // falling to `Unspecified` — which lands in the refusal arm, Go's
        // unsupported-signature error.
        let sig = match expr.sig() {
            tipb::ScalarFuncSig::LtInt => SimpleSig::LtInt,
            tipb::ScalarFuncSig::LeInt => SimpleSig::LeInt,
            tipb::ScalarFuncSig::GtInt => SimpleSig::GtInt,
            tipb::ScalarFuncSig::GeInt => SimpleSig::GeInt,
            tipb::ScalarFuncSig::EqInt => SimpleSig::EqInt,
            tipb::ScalarFuncSig::NeInt => SimpleSig::NeInt,
            // `LogicalAnd` is absent from the trimmed proto build — and the
            // wire rarely carries it: a WHERE conjunction arrives as SEPARATE
            // selection conditions, the list itself being the AND. The
            // evaluator keeps the semantics for that list.
            tipb::ScalarFuncSig::LogicalOr => SimpleSig::LogicalOr,
            tipb::ScalarFuncSig::UnaryNotInt => SimpleSig::UnaryNot,
            tipb::ScalarFuncSig::IntIsNull => SimpleSig::IntIsNull,
            tipb::ScalarFuncSig::InInt => SimpleSig::InInt,
            other => {
                return Err(format!(
                    "scalar signature {other:?} waits on its distsql_builtin.go course"
                ))
            }
        };
        let children = expr
            .children
            .iter()
            .map(convert_expr)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(SimpleExpr::Func(sig, children));
    }
    Err(format!(
        "expr type {tp:?} waits on its distsql_builtin.go course"
    ))
}

/// Evaluate to MySQL's three-valued int: `Some(0/1/n)` or NULL.
#[must_use]
pub fn eval_expr(expr: &SimpleExpr, row: &[tidb_datatype::Datum]) -> Option<i64> {
    use tidb_datatype::Datum;
    match expr {
        SimpleExpr::Null => None,
        SimpleExpr::Int(value) => Some(*value),
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::Int(value)) => Some(*value),
            Some(Datum::Null) | None => None,
            Some(_) => None, // non-int columns wait on their course
        },
        SimpleExpr::Func(sig, children) => {
            let child = |i: usize| children.get(i).and_then(|c| eval_expr(c, row));
            match sig {
                SimpleSig::LtInt
                | SimpleSig::LeInt
                | SimpleSig::GtInt
                | SimpleSig::GeInt
                | SimpleSig::EqInt
                | SimpleSig::NeInt => {
                    let (left, right) = (child(0)?, child(1)?);
                    let truth = match sig {
                        SimpleSig::LtInt => left < right,
                        SimpleSig::LeInt => left <= right,
                        SimpleSig::GtInt => left > right,
                        SimpleSig::GeInt => left >= right,
                        SimpleSig::EqInt => left == right,
                        SimpleSig::NeInt => left != right,
                        _ => unreachable!(),
                    };
                    Some(i64::from(truth))
                }
                SimpleSig::LogicalAnd => {
                    // MySQL: FALSE dominates NULL.
                    let (left, right) = (child(0), child(1));
                    match (left, right) {
                        (Some(0), _) | (_, Some(0)) => Some(0),
                        (Some(_), Some(_)) => Some(1),
                        _ => None,
                    }
                }
                SimpleSig::LogicalOr => {
                    // MySQL: TRUE dominates NULL.
                    let (left, right) = (child(0), child(1));
                    match (left, right) {
                        (Some(l), _) if l != 0 => Some(1),
                        (_, Some(r)) if r != 0 => Some(1),
                        (Some(_), Some(_)) => Some(0),
                        _ => None,
                    }
                }
                SimpleSig::UnaryNot => child(0).map(|v| i64::from(v == 0)),
                SimpleSig::IntIsNull => Some(i64::from(child(0).is_none())),
                SimpleSig::InInt => {
                    // `builtinInIntSig.evalInt`: TRUE on any match; otherwise
                    // NULL if the tested value or any element was NULL.
                    let tested = child(0);
                    let mut saw_null = tested.is_none();
                    for index in 1..children.len() {
                        match (tested, child(index)) {
                            (Some(left), Some(right)) if left == right => return Some(1),
                            (_, None) => saw_null = true,
                            _ => {}
                        }
                    }
                    if saw_null {
                        None
                    } else {
                        Some(0)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // All WRITTEN: Go's cop_handler coverage rides the store's RPC suites.

    /// `builtinInIntSig.evalInt`'s null rule, pinned: match wins, then NULL
    /// poisons a miss, then FALSE.
    #[test]
    fn in_int_follows_mysqls_null_rule() {
        use tidb_datatype::Datum;
        let col = SimpleExpr::Column(0);
        let in_list = |elems: Vec<SimpleExpr>| {
            let mut children = vec![col.clone()];
            children.extend(elems);
            SimpleExpr::Func(SimpleSig::InInt, children)
        };
        let row_int = [Datum::Int(300)];
        let row_null = [Datum::Null];

        // A match answers TRUE even with a NULL elsewhere in the list.
        let with_null = in_list(vec![SimpleExpr::Null, SimpleExpr::Int(300)]);
        assert_eq!(eval_expr(&with_null, &row_int), Some(1));
        // A miss with a NULL element is NULL, not FALSE.
        let miss_with_null = in_list(vec![SimpleExpr::Null, SimpleExpr::Int(7)]);
        assert_eq!(eval_expr(&miss_with_null, &row_int), None);
        // A plain miss is FALSE.
        let plain_miss = in_list(vec![SimpleExpr::Int(7), SimpleExpr::Int(8)]);
        assert_eq!(eval_expr(&plain_miss, &row_int), Some(0));
        // A NULL tested value never answers TRUE or FALSE.
        let any = in_list(vec![SimpleExpr::Int(300)]);
        assert_eq!(eval_expr(&any, &row_null), None);
    }

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
    fn an_empty_range_list_is_gos_null_range_error() {
        let err = build_dag(&coprocessor::Request {
            tp: REQ_TYPE_DAG,
            ..coprocessor::Request::default()
        })
        .expect_err("no ranges");
        assert_eq!(err, "request range is null");
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
        let select = tipb::SelectResponse::decode(resp.data.as_slice()).expect("a select response");
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
                    limit: Some(tipb::Limit { limit: Some(2) }),
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
        let select = tipb::SelectResponse::decode(resp.data.as_slice()).expect("decodes");
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
                    selection: Some(tipb::Selection {
                        conditions: vec![condition],
                    }),
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
        let select = tipb::SelectResponse::decode(resp.data.as_slice()).expect("decodes");
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        let decoded = tidb_codec::decode(rows_data, 2).expect("one row");
        assert_eq!(decoded, vec![Datum::Int(2), Datum::Int(88)]);
    }

    #[test]
    fn three_valued_logic_matches_mysql() {
        use tidb_datatype::Datum;
        let and = |l, r| SimpleExpr::Func(SimpleSig::LogicalAnd, vec![l, r]);
        let or = |l, r| SimpleExpr::Func(SimpleSig::LogicalOr, vec![l, r]);
        let row: [Datum; 0] = [];
        // NULL AND FALSE = FALSE; NULL AND TRUE = NULL.
        assert_eq!(
            eval_expr(&and(SimpleExpr::Null, SimpleExpr::Int(0)), &row),
            Some(0)
        );
        assert_eq!(
            eval_expr(&and(SimpleExpr::Null, SimpleExpr::Int(1)), &row),
            None
        );
        // NULL OR TRUE = TRUE; NULL OR FALSE = NULL.
        assert_eq!(
            eval_expr(&or(SimpleExpr::Null, SimpleExpr::Int(1)), &row),
            Some(1)
        );
        assert_eq!(
            eval_expr(&or(SimpleExpr::Null, SimpleExpr::Int(0)), &row),
            None
        );
        // NOT NULL = NULL; IS NULL answers over null.
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(SimpleSig::UnaryNot, vec![SimpleExpr::Null]),
                &row
            ),
            None
        );
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(SimpleSig::IntIsNull, vec![SimpleExpr::Null]),
                &row
            ),
            Some(1)
        );
    }
}
