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
    let mut aggregation: Option<&tipb::Aggregation> = None;
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
        } else if above.tp() == tipb::ExecType::TypeAggregation
            || above.tp() == tipb::ExecType::TypeStreamAgg
        {
            let Some(body) = above.aggregation.as_ref() else {
                return other_error("executor missing aggregation body");
            };
            aggregation = Some(body);
        } else {
            return other_error("closure_exec.go's top-n processor is a later course");
        }
    }
    if aggregation.is_some() && limit != usize::MAX {
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
        if !conditions.is_empty() || limit != usize::MAX {
            return other_error(
                "an index Selection/Limit is a later course of this port",
            );
        }
        let Some(aggregation) = aggregation else {
            return other_error("a bare index scan is a later course of this port");
        };
        return exec_index_aggregate(store, context, idx_scan, aggregation);
    }
    if scan.tp() != tipb::ExecType::TypeTableScan {
        return other_error("index scans (closure_exec.go) are a later course of this port");
    }
    let Some(tbl_scan) = scan.tbl_scan.as_ref() else {
        return other_error("executor missing tbl_scan body");
    };
    exec_table_scan(store, context, tbl_scan, &conditions, limit, aggregation)
}

/// The covering-index aggregation: index entries scanned in range, the
/// indexed column values decoded out of the KEY (values first, an optional
/// non-unique handle tail ignored), and the partial aggregation applied --
/// Go's `[IndexScan, Aggregation]` closure executor for the shapes the
/// reader pushes (`COUNT` over the leading key column today).
fn exec_index_aggregate(
    store: &mut MvccStore,
    context: &DagContext,
    idx_scan: &tipb::IndexScan,
    aggregation: &tipb::Aggregation,
) -> coprocessor::Response {
    use tidb_datatype::Datum;
    let mut aggregator = match RegionAggregator::build(aggregation, &idx_scan.columns) {
        Ok(aggregator) => aggregator,
        Err(message) => return other_error(&message),
    };
    let width = idx_scan.columns.len();
    for range in &context.key_ranges {
        let pairs = store.scan(&crate::mvcc_store::ScanReq {
            start_key: range.start.clone(),
            end_key: range.end.clone(),
            limit: u32::MAX,
            version: context.start_ts,
            sample_step: 0,
            reverse: false,
        });
        for pair in pairs {
            if let Some(err) = pair.error {
                if let crate::mvcc_store::KvError::Locked(lock) = *err {
                    return coprocessor::Response {
                        locked: Some(*lock),
                        ..coprocessor::Response::default()
                    };
                }
                return other_error(&format!("scan error: {err:?}"));
            }
            let encoded = tidb_codec::table_key::cut_index_prefix(&pair.key);
            let row: Vec<Datum> = match tidb_codec::decode(encoded, width) {
                Ok(datums) => datums.into_iter().take(width).collect(),
                Err(err) => return other_error(&format!("invalid index entry: {err:?}")),
            };
            if let Err(message) = aggregator.update(&row) {
                return other_error(&message);
            }
        }
    }
    let mut chunks: Vec<tipb::Chunk> = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0_usize;
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
        let encoded = match tidb_codec::encode_value(&projected) {
            Ok(encoded) => encoded,
            Err(err) => return other_error(&format!("encode row failed: {err:?}")),
        };
        current.extend_from_slice(&encoded);
        current_rows += 1;
        if current_rows == CHUNK_MAX_ROWS {
            chunks.push(tipb::Chunk {
                rows_data: Some(std::mem::take(&mut current)),
                ..tipb::Chunk::default()
            });
            current_rows = 0;
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

/// Go's chunk cut: `closure_exec.go` grows the output chunk to 1024 rows
/// before starting the next.
const CHUNK_MAX_ROWS: usize = 1024;

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
) -> coprocessor::Response {
    let mut aggregator = match aggregation {
        Some(aggregation) => match RegionAggregator::build(aggregation, &tbl_scan.columns) {
            Ok(aggregator) => Some(aggregator),
            Err(message) => return other_error(&message),
        },
        None => None,
    };
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    let mut column_types = std::collections::BTreeMap::new();
    for column in &tbl_scan.columns {
        column_types.insert(column.column_id(), field_type_from_pb_column(column));
    }
    let mut chunks: Vec<tipb::Chunk> = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0_usize;
    let mut emitted = 0_usize;
    // Go's `desc` scan: the ranges (ascending on the wire) are walked
    // last-to-first, each one reversed, so rows leave in descending record
    // order and a Limit above stops after the LARGEST keys.
    let descending = tbl_scan.desc();
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
            let decoded =
                match tidb_tablecodec::decode_table_row_to_map(&pair.value, &column_types, None) {
                    Ok(map) => map,
                    Err(err) => return other_error(&format!("decode row failed: {err:?}")),
                };
            let mut row_datums = Vec::with_capacity(tbl_scan.columns.len());
            for column in &tbl_scan.columns {
                if column.pk_handle() {
                    row_datums.push(Datum::Int(handle));
                } else if let Some(datum) = common_handle.as_ref().and_then(|datums| {
                    // Go matches `PrimaryColumnIds` positionally against the
                    // handle's datums, which are encoded in key order.
                    tbl_scan
                        .primary_column_ids
                        .iter()
                        .position(|id| *id == column.column_id())
                        .and_then(|position| datums.get(position))
                }) {
                    row_datums.push(datum.clone());
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
            let encoded = match tidb_codec::encode_value(&projected) {
                Ok(encoded) => encoded,
                Err(err) => return other_error(&format!("encode row failed: {err:?}")),
            };
            current.extend_from_slice(&encoded);
            current_rows += 1;
            if current_rows == CHUNK_MAX_ROWS {
                chunks.push(tipb::Chunk {
                    rows_data: Some(std::mem::take(&mut current)),
                    ..tipb::Chunk::default()
                });
                current_rows = 0;
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

/// Go `buildHashAggProcessor` / `buildStreamAggProcessor`
/// (`closure_exec.go`): the region-side partial aggregation over the
/// surviving scan rows, narrowed to the shapes
/// [`crate::cophandler::convert_expr`]'s column/literal leaves can carry --
/// COUNT/SUM/MIN/MAX over one column (or `COUNT(1)`), grouped by columns.
///
/// The partial-row contract is the one the reader's FINAL stage consumes:
/// aggregate results first, then the group keys, `COUNT` as `BIGINT`,
/// `SUM` as `DECIMAL` (MySQL's sum type for integer input), extremes as
/// the input datum. Hash groups leave in group-key order; a streamed
/// aggregation emits each group as the ordered scan leaves it.
struct RegionAggregator {
    group_by: Vec<(SimpleExpr, tidb_datatype::Collation)>,
    functions: Vec<(RegionAggKind, SimpleExpr, tidb_datatype::Collation)>,
    streamed: bool,
    hash: std::collections::BTreeMap<Vec<u8>, (Vec<tidb_datatype::Datum>, Vec<RegionAggValue>)>,
    current: Option<(Vec<u8>, Vec<tidb_datatype::Datum>, Vec<RegionAggValue>)>,
    ordered: Vec<Vec<tidb_datatype::Datum>>,
}

#[derive(Clone, Copy)]
enum RegionAggKind {
    Count,
    Sum,
    Min,
    Max,
}

enum RegionAggValue {
    Count(i64),
    Sum(Option<tidb_datatype::Decimal>),
    Extreme(Option<tidb_datatype::Datum>),
}

impl RegionAggregator {
    fn build(
        aggregation: &tipb::Aggregation,
        columns: &[tipb::ColumnInfo],
    ) -> Result<Self, String> {
        let collation_of_column = |expr: &SimpleExpr| -> tidb_datatype::Collation {
            let id = match expr {
                SimpleExpr::Column(offset) => {
                    columns.get(*offset).map_or(0, |column| column.collation())
                }
                _ => 0,
            };
            let restored = tidb_datatype::restore_collation_id_if_needed(id);
            tidb_datatype::Collation::from_name(&tidb_datatype::proto_to_collation(restored))
                .unwrap_or(tidb_datatype::Collation::Binary)
        };
        let group_by = aggregation
            .group_by
            .iter()
            .map(|expr| {
                let converted = convert_expr(expr)?;
                if !matches!(converted, SimpleExpr::Column(_)) {
                    return Err("a computed group-by key is a later course".to_owned());
                }
                let collation = collation_of_column(&converted);
                Ok((converted, collation))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let functions = aggregation
            .agg_func
            .iter()
            .map(|func| {
                let kind = match func.tp() {
                    tipb::ExprType::Count => RegionAggKind::Count,
                    tipb::ExprType::Sum => RegionAggKind::Sum,
                    tipb::ExprType::Min => RegionAggKind::Min,
                    tipb::ExprType::Max => RegionAggKind::Max,
                    other => {
                        return Err(format!(
                            "aggregate function {other:?} is a later course of this port"
                        ))
                    }
                };
                let [argument] = func.children.as_slice() else {
                    return Err("an aggregate function takes exactly one argument".to_owned());
                };
                let argument = convert_expr(argument)?;
                let collation = collation_of_column(&argument);
                Ok((kind, argument, collation))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if group_by.is_empty() && functions.is_empty() {
            return Err("an aggregation names a group key or a function".to_owned());
        }
        Ok(Self {
            group_by,
            functions,
            streamed: aggregation.streamed == Some(true),
            hash: std::collections::BTreeMap::new(),
            current: None,
            ordered: Vec::new(),
        })
    }

    fn new_values(&self) -> Vec<RegionAggValue> {
        self.functions
            .iter()
            .map(|(kind, _, _)| match kind {
                RegionAggKind::Count => RegionAggValue::Count(0),
                RegionAggKind::Sum => RegionAggValue::Sum(None),
                RegionAggKind::Min | RegionAggKind::Max => RegionAggValue::Extreme(None),
            })
            .collect()
    }

    fn update(&mut self, row: &[tidb_datatype::Datum]) -> Result<(), String> {
        let mut key = Vec::new();
        let mut groups = Vec::with_capacity(self.group_by.len());
        for (expr, collation) in &self.group_by {
            let value = eval_datum(expr, row)?;
            // The fallback cursor's own key rule: collation sort key for
            // byte values, the codec hash for everything else, one 0xff
            // fence between parts.
            match value.as_raw_bytes() {
                Some(bytes) => {
                    tidb_codec::encode_compact_bytes(&mut key, &collation.key(bytes));
                }
                None => key.extend_from_slice(&tidb_codec::hash_code(&value)),
            }
            key.push(0xff);
            groups.push(value);
        }

        if self.streamed {
            if self
                .current
                .as_ref()
                .is_some_and(|(current_key, _, _)| current_key != &key)
            {
                let (_, groups, values) = self.current.take().expect("current group exists");
                self.ordered.push(Self::finish_group(groups, values));
            }
            if self.current.is_none() {
                let values = self.new_values();
                self.current = Some((key, groups, values));
            }
            let functions = &self.functions;
            let (_, _, values) = self.current.as_mut().expect("just ensured");
            return Self::accumulate(functions, values, row);
        }

        if !self.hash.contains_key(&key) {
            let values = self.new_values();
            self.hash.insert(key.clone(), (groups, values));
        }
        let functions = &self.functions;
        let (_, values) = self.hash.get_mut(&key).expect("just ensured");
        Self::accumulate(functions, values, row)
    }

    fn accumulate(
        functions: &[(RegionAggKind, SimpleExpr, tidb_datatype::Collation)],
        values: &mut [RegionAggValue],
        row: &[tidb_datatype::Datum],
    ) -> Result<(), String> {
        use tidb_datatype::{Datum, Decimal};
        for ((kind, argument, collation), value) in functions.iter().zip(values.iter_mut()) {
            let input = eval_datum(argument, row)?;
            match (kind, value) {
                (RegionAggKind::Count, RegionAggValue::Count(count)) => {
                    if !matches!(input, Datum::Null) {
                        *count += 1;
                    }
                }
                (RegionAggKind::Sum, RegionAggValue::Sum(sum)) => {
                    let addend = match input {
                        Datum::Null => continue,
                        Datum::Int(value) => Decimal::from_int(value),
                        Datum::UInt(value) => Decimal::from_uint(value),
                        Datum::Decimal(value) => value,
                        _ => {
                            return Err(
                                "partial SUM requires an integer or decimal input".to_owned()
                            )
                        }
                    };
                    *sum = Some(match sum.take() {
                        Some(current) => current.add(&addend),
                        None => addend,
                    });
                }
                (RegionAggKind::Min | RegionAggKind::Max, RegionAggValue::Extreme(value)) => {
                    if matches!(input, Datum::Null) {
                        continue;
                    }
                    let is_max = matches!(kind, RegionAggKind::Max);
                    let replace = match value.as_ref() {
                        None => true,
                        Some(current) => {
                            let ordering = extreme_ordering(&input, current, collation)?;
                            if is_max {
                                ordering.is_gt()
                            } else {
                                ordering.is_lt()
                            }
                        }
                    };
                    if replace {
                        *value = Some(input);
                    }
                }
                _ => return Err("aggregate value kind mismatch".to_owned()),
            }
        }
        Ok(())
    }

    fn finish_group(
        groups: Vec<tidb_datatype::Datum>,
        values: Vec<RegionAggValue>,
    ) -> Vec<tidb_datatype::Datum> {
        use tidb_datatype::Datum;
        values
            .into_iter()
            .map(|value| match value {
                RegionAggValue::Count(count) => Datum::Int(count),
                RegionAggValue::Sum(sum) => sum.map_or(Datum::Null, Datum::Decimal),
                RegionAggValue::Extreme(value) => value.unwrap_or(Datum::Null),
            })
            .chain(groups)
            .collect()
    }

    fn finish(mut self) -> Vec<Vec<tidb_datatype::Datum>> {
        if self.streamed {
            if let Some((_, groups, values)) = self.current.take() {
                self.ordered.push(Self::finish_group(groups, values));
            }
            return self.ordered;
        }
        self.hash
            .into_values()
            .map(|(groups, values)| Self::finish_group(groups, values))
            .collect()
    }
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
        SimpleExpr::Null => Ok(Datum::Null),
        SimpleExpr::Func(..) => Err("a computed aggregate argument is a later course".to_owned()),
    }
}

/// MIN/MAX ordering over the datum kinds a lowered scan can produce.
/// Byte values order under the argument column's collation; the numeric
/// kinds compare by value, crossing signedness the way Go's `CompareDatum`
/// does for the int/uint pair.
fn extreme_ordering(
    candidate: &tidb_datatype::Datum,
    current: &tidb_datatype::Datum,
    collation: &tidb_datatype::Collation,
) -> Result<std::cmp::Ordering, String> {
    use std::cmp::Ordering;
    use tidb_datatype::Datum;
    let ordering = match (candidate, current) {
        (Datum::Int(left), Datum::Int(right)) => left.cmp(right),
        (Datum::UInt(left), Datum::UInt(right)) => left.cmp(right),
        (Datum::Int(left), Datum::UInt(right)) => {
            if *left < 0 {
                Ordering::Less
            } else {
                (*left as u64).cmp(right)
            }
        }
        (Datum::UInt(left), Datum::Int(right)) => {
            if *right < 0 {
                Ordering::Greater
            } else {
                left.cmp(&(*right as u64))
            }
        }
        (Datum::Decimal(left), Datum::Decimal(right)) => left.cmp(right),
        _ => match (candidate.as_raw_bytes(), current.as_raw_bytes()) {
            (Some(left), Some(right)) => collation.key(left).cmp(&collation.key(right)),
            _ => return Err("MIN/MAX over this datum pair is a later course".to_owned()),
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
    /// `ExprType_String`, the raw literal bytes with no codec around them.
    /// (`Bytes`, Go's `KindBytes` tag, is deliberately absent from the
    /// trimmed proto -- no leaf this path builds is that kind.) The
    /// COMPARISON's collation decides how these order, as in Go.
    Bytes(Vec<u8>),
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
    /// `LtString`/`LeString`/`GtString`/`GeString`/`EqString`/`NeString`.
    /// The `i32` is the comparison's DERIVED collation id, which the
    /// lowering writes into the `ScalarFunc`'s own `field_type.collate` --
    /// so `utf8mb4_bin` and `utf8mb4_general_ci` order the same bytes
    /// differently, as they must.
    LtString(i32),
    /// See [`SimpleSig::LtString`].
    LeString(i32),
    /// See [`SimpleSig::LtString`].
    GtString(i32),
    /// See [`SimpleSig::LtString`].
    GeString(i32),
    /// See [`SimpleSig::LtString`].
    EqString(i32),
    /// See [`SimpleSig::LtString`].
    NeString(i32),
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
    if tp == tipb::ExprType::String {
        return Ok(SimpleExpr::Bytes(expr.val().to_vec()));
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
            // Go reads the comparison's collation off the `ScalarFunc`'s own
            // field type (`distsql_builtin.go`'s `PbToExpr` keeps it there),
            // which is where the lowering writes the DERIVED collation.
            tipb::ScalarFuncSig::LtString => SimpleSig::LtString(collation_of(expr)),
            tipb::ScalarFuncSig::LeString => SimpleSig::LeString(collation_of(expr)),
            tipb::ScalarFuncSig::GtString => SimpleSig::GtString(collation_of(expr)),
            tipb::ScalarFuncSig::GeString => SimpleSig::GeString(collation_of(expr)),
            tipb::ScalarFuncSig::EqString => SimpleSig::EqString(collation_of(expr)),
            tipb::ScalarFuncSig::NeString => SimpleSig::NeString(collation_of(expr)),
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

/// The bytes a string operand carries: a literal, or a string-valued
/// column. Anything else -- including SQL NULL -- is `None`, which the
/// caller propagates as MySQL's UNKNOWN.
fn eval_bytes(expr: &SimpleExpr, row: &[tidb_datatype::Datum]) -> Option<Vec<u8>> {
    use tidb_datatype::Datum;
    match expr {
        SimpleExpr::Bytes(value) => Some(value.clone()),
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::String(value)) => Some(value.bytes().to_vec()),
            Some(Datum::Bytes(value)) => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
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
    field_type
}

fn collation_of(expr: &tipb::Expr) -> i32 {
    let protocol = expr
        .field_type
        .as_ref()
        .map_or(0, |field_type| field_type.collate());
    tidb_datatype::restore_collation_id_if_needed(protocol)
}

/// Evaluate to MySQL's three-valued int: `Some(0/1/n)` or NULL.
#[must_use]
/// Evaluates one pushed-down expression against a scanned row.
///
/// The value is carried as `i128` because an integer column is either signed
/// or UNSIGNED and the two domains do not fit in one 64-bit slot: a
/// `BIGINT UNSIGNED` above `i64::MAX` and a negative `BIGINT` must both
/// compare exactly. Go reaches the same result by selecting a signedness
/// -specific comparison signature per operand pair; one wider integer settles
/// every pairing at once, which is what a filter that must never invent a row
/// needs.
pub fn eval_expr(expr: &SimpleExpr, row: &[tidb_datatype::Datum]) -> Option<i128> {
    use tidb_datatype::Datum;
    match expr {
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
        // A bare string is not a truth value; only a comparison reads it.
        SimpleExpr::Bytes(_) => None,
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
                    Some(i128::from(truth))
                }
                SimpleSig::LtString(collation)
                | SimpleSig::LeString(collation)
                | SimpleSig::GtString(collation)
                | SimpleSig::GeString(collation)
                | SimpleSig::EqString(collation) => {
                    let (left, right) = (
                        eval_bytes(children.first()?, row)?,
                        eval_bytes(children.get(1)?, row)?,
                    );
                    let ordering =
                        tidb_datatype::get_collator_by_id(*collation).compare(&left, &right);
                    let truth = match sig {
                        SimpleSig::LtString(_) => ordering.is_lt(),
                        SimpleSig::LeString(_) => ordering.is_le(),
                        SimpleSig::GtString(_) => ordering.is_gt(),
                        SimpleSig::GeString(_) => ordering.is_ge(),
                        _ => ordering.is_eq(),
                    };
                    Some(i128::from(truth))
                }
                SimpleSig::NeString(collation) => {
                    let (left, right) = (
                        eval_bytes(children.first()?, row)?,
                        eval_bytes(children.get(1)?, row)?,
                    );
                    let equal = tidb_datatype::get_collator_by_id(*collation)
                        .compare(&left, &right)
                        .is_eq();
                    Some(i128::from(!equal))
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
                SimpleSig::UnaryNot => child(0).map(|v| i128::from(v == 0)),
                SimpleSig::IntIsNull => Some(i128::from(child(0).is_none())),
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
        let select = tipb::SelectResponse::decode(resp.data.as_slice()).expect("a select response");
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
                        ..tipb::Expr::default()
                    },
                    tipb::Expr {
                        tp: Some(tipb::ExprType::String as i32),
                        val: Some(literal.as_bytes().to_vec()),
                        ..tipb::Expr::default()
                    },
                ],
                field_type: Some(tipb::FieldType {
                    collate: Some(tidb_datatype::collation_to_proto(collation_name)),
                    ..tipb::FieldType::default()
                }),
                ..tipb::Expr::default()
            };
            let converted = convert_expr(&expr).expect("a string comparison converts");
            eval_expr(&converted, &[Datum::new_string(value.to_owned())])
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
            let value = tidb_codec::encode_value(&[Datum::Int(1), g, Datum::Int(2), v])
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
            aggregation: Some(tipb::Aggregation {
                group_by: vec![column_ref(0)],
                agg_func: vec![
                    agg_of(tipb::ExprType::Sum, column_ref(1)),
                    agg_of(tipb::ExprType::Count, column_ref(1)),
                ],
                streamed: Some(false),
            }),
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
        let select = tipb::SelectResponse::decode(resp.data.as_slice()).expect("a select response");
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

    /// A STREAMED partial aggregation emits each group as the ordered scan
    /// leaves it -- scan order, not group-key order -- and `COUNT(1)`
    /// (Go's lowering of `COUNT(*)`) counts a row whose column is NULL.
    #[test]
    fn a_streamed_partial_aggregation_emits_groups_in_scan_order() {
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};

        let mut store = MvccStore::new();
        let table_id = 78_i64;
        // Scan order (by handle): g = 5, 5, 3 -- group 5 leaves first.
        let rows = [
            (1_i64, Datum::Int(5), Datum::Null),
            (2, Datum::Int(5), Datum::Int(7)),
            (3, Datum::Int(3), Datum::Int(9)),
        ];
        for (handle, g, v) in rows {
            let key = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
            // The OLD row format: `EncodeInt(colID) ++ EncodeDatum(value)`
            // per column -- column 1 carries g, column 2 carries v.
            let value = tidb_codec::encode_value(&[Datum::Int(1), g, Datum::Int(2), v])
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
            aggregation: Some(tipb::Aggregation {
                group_by: vec![column_ref(0)],
                agg_func: vec![tipb::Expr {
                    tp: Some(tipb::ExprType::Count as i32),
                    children: vec![one],
                    ..tipb::Expr::default()
                }],
                streamed: Some(true),
            }),
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
        let select = tipb::SelectResponse::decode(resp.data.as_slice()).expect("a select response");
        let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
        // [count, g] per group, group 5 first: the NULL row still counts.
        let decoded = tidb_codec::decode(rows_data, 4).expect("four datums");
        assert_eq!(
            decoded,
            vec![Datum::Int(2), Datum::Int(5), Datum::Int(1), Datum::Int(3)]
        );
    }
}
