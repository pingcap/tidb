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
        REQ_TYPE_ANALYZE => other_error(
            "handleCopAnalyzeRequest (cophandler/analyze.go) is a later course of this port",
        ),
        // Go's stub (`cop_handler.go:750`): a marshalled
        // `tipb.ChecksumResponse{Checksum:1, TotalKvs:1, TotalBytes:1}` --
        // unistore never computes real checksums. The trimmed tipb build
        // drops `ChecksumResponse`, so the message is hand-encoded: three
        // optional uint64 fields at wire numbers 1-3, each carrying `1`
        // (`0x08 0x01`, `0x10 0x01`, `0x18 0x01`).
        REQ_TYPE_CHECKSUM => coprocessor::Response {
            data: vec![0x08, 0x01, 0x10, 0x01, 0x18, 0x01],
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
    exec_dag(_store, &context)
}

/// The execution slice of Go `buildClosureExecutor` + `handleCopDAGRequest`
/// (`closure_exec.go`): a TABLE-SCAN-ONLY DAG runs; anything above the scan
/// refuses by name until its course lands.
fn exec_dag(store: &mut MvccStore, context: &DagContext) -> coprocessor::Response {
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
                match convert_expr(condition) {
                    Ok(expr) => conditions.push(expr),
                    Err(message) => return other_error(&message),
                }
            }
        } else if above.tp() == tipb::ExecType::TypeTopN {
            let Some(body) = above.top_n.as_ref() else {
                return other_error("executor missing topN body");
            };
            topn = Some(match TopNSpec::build(body) {
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
        return exec_index_scan(
            store,
            context,
            idx_scan,
            &conditions,
            limit,
            aggregation,
            topn.as_ref(),
        );
    }
    if scan.tp() != tipb::ExecType::TypeTableScan {
        return other_error("index scans (closure_exec.go) are a later course of this port");
    }
    let Some(tbl_scan) = scan.tbl_scan.as_ref() else {
        return other_error("executor missing tbl_scan body");
    };
    exec_table_scan(
        store,
        context,
        tbl_scan,
        &conditions,
        limit,
        aggregation,
        topn.as_ref(),
    )
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
        Some(aggregation) => match RegionAggregator::build(aggregation, &idx_scan.columns) {
            Ok(aggregator) => Some(aggregator),
            Err(message) => return other_error(&message),
        },
        None => None,
    };
    let mut rows = Vec::new();
    let mut emitted = 0_usize;
    // Go keeps the kept rows in a bounded heap; this lowering buffers every
    // surviving row and sorts once -- the same N rows in the same key order,
    // without the intermediate evictions.
    let mut topn_rows: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::new();
    if aggregation.is_none() && limit == 0 {
        return encode_default_rows(rows, &timezone);
    }
    let descending = idx_scan.desc();
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
            match conditions
                .iter()
                .map(|condition| eval_expr(condition, &row, context.div_precision_increment))
                .collect::<Result<Vec<_>, String>>()
            {
                Ok(values) if values.iter().all(|v| v.is_some_and(|v| v != 0)) => {}
                Ok(_) => continue,
                Err(message) => return other_error(&message),
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
            if emitted == limit {
                break 'ranges;
            }
        }
    }
    if let Some(spec) = topn {
        // Go `topNProcessor.Finish` (`closure_exec.go:1064`): the kept rows
        // replay in key order; a separate Limit above caps the replay
        // further.
        spec.sort_rows(&mut topn_rows);
        topn_rows.truncate(spec.limit);
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
    encode_default_rows(rows, &timezone)
}

fn encode_default_rows(
    rows: Vec<Vec<tidb_datatype::Datum>>,
    timezone: &tidb_datatype::SessionTimeZone,
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
    topn: Option<&TopNSpec>,
) -> coprocessor::Response {
    let timezone = match context.time_zone.resolve() {
        Ok(timezone) => timezone,
        Err(error) => return other_error(&error),
    };
    let mut aggregator = match aggregation {
        Some(aggregation) => match RegionAggregator::build(aggregation, &tbl_scan.columns) {
            Ok(aggregator) => Some(aggregator),
            Err(message) => return other_error(&message),
        },
        None => None,
    };
    use tidb_datatype::Datum;
    let mut column_types = std::collections::BTreeMap::new();
    for column in &tbl_scan.columns {
        column_types.insert(column.column_id(), field_type_from_pb_column(column));
    }
    let mut chunks: Vec<tipb::Chunk> = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0_usize;
    let mut emitted = 0_usize;
    // Go keeps the kept rows in a bounded heap; this lowering buffers every
    // surviving row and sorts once -- the same N rows in the same key order,
    // without the intermediate evictions.
    let mut topn_rows: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::new();
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
            let decoded = match tidb_tablecodec::decode_table_row_to_map(
                &pair.value,
                &column_types,
                Some(&timezone),
            ) {
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
            match conditions
                .iter()
                .map(|condition| eval_expr(condition, &row_datums, context.div_precision_increment))
                .collect::<Result<Vec<_>, String>>()
            {
                Ok(values) if values.iter().all(|v| v.is_some_and(|v| v != 0)) => {}
                Ok(_) => continue,
                Err(message) => return other_error(&message),
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
            let encoded = match tidb_codec::encode_value_in_timezone(&timezone, &projected) {
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
    if let Some(spec) = topn {
        // Go `topNProcessor.Finish` (`closure_exec.go:1064`): the kept rows
        // replay in key order; a separate Limit above caps the replay
        // further. Sorted ties are deterministic here where Go's unstable
        // sort leaves them unordered.
        spec.sort_rows(&mut topn_rows);
        topn_rows.truncate(spec.limit);
        for (projected, _) in topn_rows {
            let encoded = match tidb_codec::encode_value_in_timezone(&timezone, &projected) {
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
            let encoded = match tidb_codec::encode_value_in_timezone(&timezone, &projected) {
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

/// A region-local `SUM` in progress.
///
/// Go picks one of two aggregate signatures from the argument's eval type
/// (`pkg/executor/aggfuncs/func_sum.go`): `sum4Decimal` for integer and
/// decimal inputs, `sum4Float64` for real ones. Only the decimal arm
/// existed here, so a pushed-down `SUM` over a `FLOAT`/`DOUBLE` column
/// failed the whole coprocessor request instead of answering it.
#[derive(Debug, Clone)]
enum RegionSum {
    Empty,
    Decimal(tidb_datatype::Decimal),
    Real(f64),
}

impl RegionSum {
    /// Folds one row's value in, skipping NULLs as both Go signatures do.
    fn accumulate(&mut self, input: &tidb_datatype::Datum) -> Result<(), String> {
        use tidb_datatype::{Datum, Decimal};
        let addend = match input {
            Datum::Null => return Ok(()),
            Datum::Int(value) => RegionSum::Decimal(Decimal::from_int(*value)),
            Datum::UInt(value) => RegionSum::Decimal(Decimal::from_uint(*value)),
            Datum::Decimal(value) => RegionSum::Decimal(value.clone()),
            Datum::Real(value) | Datum::Float32(value) => RegionSum::Real(*value),
            _ => return Err("partial SUM requires a numeric input".to_owned()),
        };
        // A single argument carries a single type in Go, so the families can
        // only ever mix here; real is the family MySQL merges them into.
        *self = match (std::mem::replace(self, RegionSum::Empty), addend) {
            (RegionSum::Empty, addend) => addend,
            (RegionSum::Decimal(current), RegionSum::Decimal(addend)) => {
                RegionSum::Decimal(current.add(&addend))
            }
            (RegionSum::Real(current), RegionSum::Real(addend)) => {
                RegionSum::Real(current + addend)
            }
            (RegionSum::Decimal(current), RegionSum::Real(addend)) => {
                RegionSum::Real(current.to_f64() + addend)
            }
            (RegionSum::Real(current), RegionSum::Decimal(addend)) => {
                RegionSum::Real(current + addend.to_f64())
            }
            (current, RegionSum::Empty) => current,
        };
        Ok(())
    }

    /// NULL when no non-NULL value was folded in, matching Go's aggregate.
    fn into_datum(self) -> tidb_datatype::Datum {
        match self {
            RegionSum::Empty => tidb_datatype::Datum::Null,
            RegionSum::Decimal(value) => tidb_datatype::Datum::Decimal(value),
            RegionSum::Real(value) => tidb_datatype::Datum::Real(value),
        }
    }
}

enum RegionAggValue {
    Count(i64),
    Sum(RegionSum),
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
                RegionAggKind::Sum => RegionAggValue::Sum(RegionSum::Empty),
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
        use tidb_datatype::Datum;
        for ((kind, argument, collation), value) in functions.iter().zip(values.iter_mut()) {
            let input = eval_datum(argument, row)?;
            match (kind, value) {
                (RegionAggKind::Count, RegionAggValue::Count(count)) => {
                    if !matches!(input, Datum::Null) {
                        *count += 1;
                    }
                }
                (RegionAggKind::Sum, RegionAggValue::Sum(sum)) => sum.accumulate(&input)?,
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
                RegionAggValue::Sum(sum) => sum.into_datum(),
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

/// Go's `%v` rendering of a float64: shortest round-trip decimal, switching
/// to `e` notation outside Go's `%g` thresholds (exponent >= 21 or < -4).
fn go_float_display(value: f64) -> String {
    if value == 0.0 {
        return "0".to_owned();
    }
    let magnitude = value.abs();
    if (1e-4..1e21).contains(&magnitude) {
        let rendered = format!("{}", value);
        rendered
    } else {
        let rendered = format!("{:e}", value);
        // Rust "1.5e21" -> Go "1.5e+21".
        match rendered.split_once('e') {
            Some((mantissa, exponent)) => format!(
                "{mantissa}e{}{exponent}",
                if exponent.starts_with('-') { "" } else { "+" }
            ),
            None => rendered,
        }
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

/// Go `topNCtx` (`closure_exec.go:545`): the pushed-down bounded sort --
/// order-by keys with their directions and the heap size.
struct TopNSpec {
    keys: Vec<(SimpleExpr, bool, tidb_datatype::Collation)>,
    limit: usize,
}

impl TopNSpec {
    /// Go `buildTopNProcessor` (`closure_exec.go:384`).
    fn build(top_n: &tipb::TopN) -> Result<Self, String> {
        let mut keys = Vec::with_capacity(top_n.order_by.len());
        for item in &top_n.order_by {
            let Some(pb_expr) = item.expr.as_ref() else {
                return Err("order-by item missing expr".to_owned());
            };
            let expr = convert_expr(pb_expr)?;
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
                let datum = match expr {
                    SimpleExpr::Func(..) => {
                        return Err("a computed sort key is a later course".to_owned())
                    }
                    other => eval_datum(other, row)?,
                };
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
    let time_zone = match dag_req.time_zone_name() {
        "" => TimeZoneSpec::FixedOffset(dag_req.time_zone_offset()),
        "System" => TimeZoneSpec::System,
        name => TimeZoneSpec::Named(name.to_owned()),
    };
    Ok(DagContext {
        key_ranges: req.ranges.clone(),
        start_ts: req.start_ts,
        time_zone,
        // Go `buildDAG`: the session default is 4 when the request omits
        // the field (`variable.DefDivPrecisionIncrement`).
        div_precision_increment: dag_req
            .div_precision_increment
            .map(i64::from)
            .filter(|value| *value != 0)
            .unwrap_or(4),
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
    /// `ExprType_ScalarFunc` over a supported signature.
    Func(SimpleSig, Vec<SimpleExpr>),
}

/// The date-argument channel of one upstream `AddDate`/`SubDate` type
/// pair: Go `builtinAddSubDateAsStringSig` parses non-temporal sources,
/// the `DatetimeAny` sig reads typed datetimes, and the `DurationAny`
/// sig reads durations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DateArithArg {
    /// The string source: Go `getDateFromString`.
    String,
    /// The integer source: Go `getDateFromInt`.
    Int,
    /// The binary64 source: Go `getDateFromReal`.
    Real,
    /// The decimal source: Go `getDateFromDecimal`.
    Decimal,
    /// The datetime source: Go `getDateFromDatetime`.
    Datetime,
    /// The duration source: Go `builtinAddSubDateDurationAnySig`.
    Duration,
}

/// The interval-argument channel of one upstream `AddDate`/`SubDate`
/// type pair; every channel reduces to interval text (Go `getInterval*`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntervalArg {
    /// Go `getIntervalFromString`.
    String,
    /// Go `getIntervalFromInt`.
    Int,
    /// Go `getIntervalFromReal`.
    Real,
    /// Go `getIntervalFromDecimal`.
    Decimal,
}

/// The supported `ScalarFuncSig` subset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimpleSig {
    /// `PlusDecimal`/`MinusDecimal`/`MultiplyDecimal`/`ModDecimal`: exact
    /// decimal arithmetic (`pkg/expression`'s `EvalPlusDecimal` family),
    /// NULL on a zero `MOD` divisor.
    PlusDecimal,
    /// See [`SimpleSig::PlusDecimal`].
    MinusDecimal,
    /// See [`SimpleSig::PlusDecimal`].
    MultiplyDecimal,
    /// See [`SimpleSig::PlusDecimal`].
    ModDecimal,
    /// `ModIntUnsignedUnsigned`/`ModIntUnsignedSigned`/
    /// `ModIntSignedUnsigned`/`ModIntSignedSigned`: Go picks between the
    /// four by the two arguments' UNSIGNED flags, not their width; the
    /// truncated-remainder VALUE is the same under all four, and a zero
    /// divisor answers NULL.
    ModIntUnsignedUnsigned,
    /// See [`SimpleSig::ModIntUnsignedUnsigned`].
    ModIntUnsignedSigned,
    /// See [`SimpleSig::ModIntUnsignedUnsigned`].
    ModIntSignedUnsigned,
    /// See [`SimpleSig::ModIntUnsignedUnsigned`].
    ModIntSignedSigned,
    /// The integer arithmetic family. Go picks between the flag pairings
    /// by the two arguments' UNSIGNED flags; a result outside the
    /// family's domain is Go's 1690 overflow error, which the evaluator
    /// carries as [`SimpleSig`]'s error arm.
    PlusInt,
    /// See [`SimpleSig::PlusInt`].
    MinusInt,
    /// See [`SimpleSig::PlusInt`].
    MultiplyInt,
    /// See [`SimpleSig::PlusInt`].
    MultiplyIntUnsigned,
    /// See [`SimpleSig::PlusInt`].
    PlusIntUnsignedUnsigned,
    /// See [`SimpleSig::PlusInt`].
    PlusIntUnsignedSigned,
    /// See [`SimpleSig::PlusInt`].
    PlusIntSignedUnsigned,
    /// See [`SimpleSig::PlusInt`].
    PlusIntSignedSigned,
    /// See [`SimpleSig::PlusInt`].
    MinusIntUnsignedUnsigned,
    /// See [`SimpleSig::PlusInt`].
    MinusIntUnsignedSigned,
    /// See [`SimpleSig::PlusInt`].
    MinusIntSignedUnsigned,
    /// See [`SimpleSig::PlusInt`].
    MinusIntSignedSigned,
    /// See [`SimpleSig::PlusInt`]. The FORCED pairings keep the unsigned
    /// result domain even when a signed argument arrives.
    MinusIntForcedUnsignedUnsigned,
    /// See [`SimpleSig::PlusInt`].
    MinusIntForcedUnsignedSigned,
    /// See [`SimpleSig::PlusInt`].
    MinusIntForcedSignedUnsigned,
    /// The math family TiKV evaluates unconditionally (`RoundReal`,
    /// `RoundInt`, `RoundDec`, `Pow`, `Acos`, `Asin`, `Atan1Arg`,
    /// `Atan2Args`, `Cos`, `Cot`, `PI`, `Sin`): binary64 math with IEEE
    /// NaN/Inf results, NULL in -> NULL out.
    RoundReal,
    /// See [`SimpleSig::RoundReal`].
    RoundInt,
    /// See [`SimpleSig::RoundReal`].
    RoundDec,
    /// See [`SimpleSig::RoundReal`].
    Pow,
    /// See [`SimpleSig::RoundReal`].
    Acos,
    /// See [`SimpleSig::RoundReal`].
    Asin,
    /// See [`SimpleSig::RoundReal`].
    Atan1Arg,
    /// See [`SimpleSig::RoundReal`].
    Atan2Args,
    /// See [`SimpleSig::RoundReal`].
    Cos,
    /// See [`SimpleSig::RoundReal`].
    Cot,
    /// See [`SimpleSig::RoundReal`].
    Pi,
    /// See [`SimpleSig::RoundReal`].
    Sin,
    /// The string function family: `CharLengthUTF8` counts CHARACTERS,
    /// `CharLength` counts BYTES; `Lower`/`LowerUTF8` and `Upper`/`UpperUTF8`
    /// fold case (the UTF8 forms per rune, the binary forms ASCII-only);
    /// `Substring2Args*`/`Substring3Args*` take 1-based positions (negative
    /// counts from the end) with byte or rune indexing.
    CharLengthUtf8,
    /// See [`SimpleSig::CharLengthUtf8`].
    CharLength,
    /// See [`SimpleSig::CharLengthUtf8`].
    LowerUtf8,
    /// See [`SimpleSig::CharLengthUtf8`].
    Lower,
    /// `DateFormatSig` over (datetime, format): Go `builtinDateFormatSig`
    /// answering through `Time.DateFormat`.
    DateFormatSig,
    /// `Conv` over (text, from-base, to-base): Go `builtinConvSig.conv`
    /// re-reading the text in one base and re-formatting it in the
    /// other (bases 2..=36; a negative base marks signed in/output).
    Conv,
    /// `AddDate`/`SubDate` over (date, interval, unit): one upstream id
    /// per (date channel, interval channel, operation), with the
    /// duration sources split by their answer kind -- Go
    /// `builtinAddSubDate{AsString,DatetimeAny,DurationAny}Sig`.
    AddSubDate {
        /// `SubDate` negates the interval (Go `c.timeOp`).
        subtract: bool,
        /// The date-argument channel.
        date: DateArithArg,
        /// The interval-argument channel.
        interval: IntervalArg,
        /// The `*Datetime` upstream ids answer a datetime; the plain
        /// duration ids answer a duration.
        datetime_result: bool,
    },
    /// `ModReal`: binary64 remainder with the dividend's sign (Go
    /// `builtinArithmeticModRealSig` over `math.Mod`); a zero divisor
    /// answers NULL.
    ModReal,
    /// `TimestampDiff` over (unit, first, second): Go
    /// `builtinTimestampDiffSig` answering `types.TimestampDiff`; an
    /// unknown unit answers 0.
    TimestampDiff,
    /// See [`SimpleSig::CharLengthUtf8`].
    UpperUtf8,
    /// See [`SimpleSig::CharLengthUtf8`].
    Upper,
    /// See [`SimpleSig::CharLengthUtf8`].
    Substring2ArgsUtf8,
    /// See [`SimpleSig::CharLengthUtf8`].
    Substring3ArgsUtf8,
    /// See [`SimpleSig::CharLengthUtf8`].
    Substring2Args,
    /// See [`SimpleSig::CharLengthUtf8`].
    Substring3Args,
    /// `DivideDecimal`: decimal division with the DAG request's
    /// `div_precision_increment` added to the result fraction, NULL on a
    /// zero divisor (Go `EvalDivideDecimal`).
    DivideDecimal,
    /// `LTReal`/`LEReal`/`GTReal`/`GEReal`/`EQReal`/`NEReal`, by ordering.
    LtReal,
    /// See [`SimpleSig::LtReal`].
    LeReal,
    /// See [`SimpleSig::LtReal`].
    GtReal,
    /// See [`SimpleSig::LtReal`].
    GeReal,
    /// See [`SimpleSig::LtReal`].
    EqReal,
    /// See [`SimpleSig::LtReal`].
    NeReal,
    /// `IntDivideInt` and its three unsigned-flag pairings: truncating
    /// integer division, NULL on a zero divisor. Go picks between the four
    /// by the two arguments' UNSIGNED flags; the truncated quotient VALUE
    /// is the same under all four for the values the flags admit.
    IntDivideInt,
    /// See [`SimpleSig::IntDivideInt`].
    IntDivideIntUnsignedUnsigned,
    /// See [`SimpleSig::IntDivideInt`].
    IntDivideIntUnsignedSigned,
    /// See [`SimpleSig::IntDivideInt`].
    IntDivideIntSignedSigned,
    /// See [`SimpleSig::IntDivideInt`].
    IntDivideIntSignedUnsigned,
    /// `IntDivideDecimal`: decimal division truncated to the integer part,
    /// NULL on a zero divisor (Go `EvalIntDivideDecimal`'s `DecimalDiv` ->
    /// `ToInt`).
    IntDivideDecimal,
    /// `PlusReal`/`MinusReal`/`MultiplyReal`: binary64 arithmetic, NULL in
    /// -> NULL out (Go `EvalPlusReal` family).
    PlusReal,
    /// See [`SimpleSig::PlusReal`].
    MinusReal,
    /// See [`SimpleSig::PlusReal`].
    MultiplyReal,
    /// Go `EvalDivideReal`: NULL on a zero divisor.
    DivideReal,
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
    /// `LtDecimal`/`LeDecimal`/`GtDecimal`/`GeDecimal`/`EqDecimal`/
    /// `NeDecimal`. Go picks these from `GetAccurateCmpType` whenever either
    /// side is DECIMAL, having folded the other side's cast at build time --
    /// so `dc > 0` arrives as `GtDecimal(ColumnRef, MysqlDecimal(0))` and
    /// never as an int comparison.
    LtDecimal,
    /// See [`SimpleSig::LtDecimal`].
    LeDecimal,
    /// See [`SimpleSig::LtDecimal`].
    GtDecimal,
    /// See [`SimpleSig::LtDecimal`].
    GeDecimal,
    /// See [`SimpleSig::LtDecimal`].
    EqDecimal,
    /// See [`SimpleSig::LtDecimal`].
    NeDecimal,
    /// `LtTime`/`LeTime`/`GtTime`/`GeTime`/`EqTime`/`NeTime`. Go picks these
    /// from `GetAccurateCmpType` whenever either side is DATE/DATETIME/
    /// TIMESTAMP, the string side's cast having been folded at build time --
    /// so `d > '2024-01-01'` arrives as `GtTime(ColumnRef, MysqlTime(..))`.
    LtTime,
    /// See [`SimpleSig::LtTime`].
    LeTime,
    /// See [`SimpleSig::LtTime`].
    GtTime,
    /// See [`SimpleSig::LtTime`].
    GeTime,
    /// See [`SimpleSig::LtTime`].
    EqTime,
    /// See [`SimpleSig::LtTime`].
    NeTime,
    /// `LogicalAnd` — MySQL three-valued: `NULL AND FALSE` is FALSE.
    LogicalAnd,
    /// `LogicalOr` — `NULL OR TRUE` is TRUE.
    LogicalOr,
    /// `UnaryNotInt`.
    UnaryNot,
    /// `IntIsNull`.
    IntIsNull,
    /// `DecimalIsNull`/`DurationIsNull`/`RealIsNull`/`StringIsNull`/
    /// `TimeIsNull` -- the evaluation-family-specific IS NULL signatures
    /// Go's `isNullFunctionClass` selects; each checks its own leaf.
    DecimalIsNull,
    /// See [`SimpleSig::DecimalIsNull`].
    DurationIsNull,
    /// See [`SimpleSig::DecimalIsNull`].
    RealIsNull,
    /// See [`SimpleSig::DecimalIsNull`].
    StringIsNull,
    /// See [`SimpleSig::DecimalIsNull`].
    TimeIsNull,
    /// `JsonMemberOfSig` over (value, json): equality against the doc, or
    /// against each ARRAY element (Go `builtinJSONMemberOfSig.evalInt`).
    JsonMemberOfSig,
    /// The temporal extraction family over a TIME/DATETIME leaf: `Date`
    /// truncates a DATETIME to its date part; `Hour`/`Minute`/`Second`/
    /// `MicroSecond` extract the clock fields (unsigned, hours may exceed
    /// 24 on durations); `Month` extracts the calendar month; `DateDiff`
    /// answers the day difference between two dates. `CastTimeAsDuration`
    /// adapts a DATETIME column to the DURATION channel they read.
    Date,
    /// See [`SimpleSig::Date`].
    Hour,
    /// See [`SimpleSig::Date`].
    Minute,
    /// See [`SimpleSig::Date`].
    Second,
    /// See [`SimpleSig::Date`].
    MicroSecond,
    /// See [`SimpleSig::Date`].
    Month,
    /// See [`SimpleSig::Date`].
    DateDiff,
    /// See [`SimpleSig::Date`].
    CastTimeAsDuration,
    /// `WeekWithoutMode`: WEEK(date) with mode 0 -- weeks start Sunday and
    /// week 1 contains January 1st (Go `builtinWeekWithoutModeSig`).
    WeekWithoutMode,
    /// `InString`: n-ary membership over the string channel, compared
    /// under the comparison's collation, with InInt's NULL rules.
    InString(i32),
    /// `VectorFloat32IsNull`.
    VectorFloat32IsNull,
    /// `CastIntAsInt`/`CastRealAsInt`/`CastDecimalAsInt` (`AS SIGNED`):
    /// the REAL and DECIMAL sources ROUND to the nearest integer and a
    /// result outside BIGINT is Go's cast overflow error
    /// (`constant %v overflows bigint`).
    CastIntAsInt,
    /// See [`SimpleSig::CastIntAsInt`].
    CastRealAsInt,
    /// See [`SimpleSig::CastIntAsInt`].
    CastDecimalAsInt,
    /// `CastStringAsInt`/`CastStringAsReal`: Go's best-effort prefix
    /// conversion (`StrToInt`/`StrToFloat64`) -- trim, take the longest
    /// valid numeric prefix, garbage answers 0. The companion truncation
    /// and range warnings have no coprocessor sink; a range overflow
    /// saturates to the BIGINT/DOUBLE bound (the non-strict SELECT
    /// observable), not an error.
    CastStringAsInt,
    /// See [`SimpleSig::CastStringAsInt`].
    CastStringAsReal,
    /// The `AS DECIMAL` casts: Go's per-source conversions
    /// (`NewDecFromInt`, `FromFloat64`, the identity, `FromString` after
    /// a trim, and the `ToNumber` renderings of time and duration) --
    /// they compose as decimal-comparison operands and a bare cast
    /// answers its own truth.
    CastIntAsDecimal,
    /// See [`SimpleSig::CastIntAsDecimal`].
    CastRealAsDecimal,
    /// See [`SimpleSig::CastIntAsDecimal`].
    CastDecimalAsDecimal,
    /// See [`SimpleSig::CastIntAsDecimal`].
    CastStringAsDecimal,
    /// See [`SimpleSig::CastIntAsDecimal`].
    CastTimeAsDecimal,
    /// See [`SimpleSig::CastIntAsDecimal`].
    CastDurationAsDecimal,
    /// The `AS CHAR`/`AS STRING` casts: Go's per-source renderings
    /// (`FormatInt`/`FormatUint` by the source's flag, `FormatFloat`
    /// shortest-'f', the decimal's text, and the time/duration `String`
    /// forms; a string source passes through) -- they compose through
    /// the bytes channel and a bare cast answers its own truth.
    CastIntAsString,
    /// See [`SimpleSig::CastIntAsString`].
    CastRealAsString,
    /// See [`SimpleSig::CastIntAsString`].
    CastDecimalAsString,
    /// See [`SimpleSig::CastIntAsString`].
    CastStringAsString,
    /// See [`SimpleSig::CastIntAsString`].
    CastTimeAsString,
    /// See [`SimpleSig::CastIntAsString`].
    CastDurationAsString,
    /// The `AS TIME ...`-wire casts over numeric and text sources: Go
    /// parses them through the same temporal converters the string-form
    /// arithmetic uses (`ParseTimeFromInt64`/`...Float64`/`...Decimal`/
    /// `ParseTime`), and an already-temporal source passes through.
    /// The expression's target kind folds to the source's natural kind
    /// -- no field type rides the wire.
    CastIntAsTime,
    /// See [`SimpleSig::CastIntAsTime`].
    CastRealAsTime,
    /// See [`SimpleSig::CastIntAsTime`].
    CastDecimalAsTime,
    /// See [`SimpleSig::CastIntAsTime`].
    CastStringAsTime,
    /// See [`SimpleSig::CastIntAsTime`].
    CastTimeAsTime,
    /// The `AS TIME`-wire duration casts: Go's `NumberToDuration` reads
    /// integer digits as HHMMSS, text goes through `ParseDuration`, and
    /// an already-duration source passes through.
    CastIntAsDuration,
    /// See [`SimpleSig::CastIntAsDuration`].
    CastRealAsDuration,
    /// See [`SimpleSig::CastIntAsDuration`].
    CastDecimalAsDuration,
    /// See [`SimpleSig::CastIntAsDuration`].
    CastStringAsDuration,
    /// See [`SimpleSig::CastIntAsDuration`].
    CastDurationAsDuration,
    /// The `AS JSON` casts: Go wraps each source in its JSON type --
    /// integers, doubles, the parsed text, and the opaque time/duration
    /// scalars (doubles for decimals, matching Go's own FIXME).
    CastIntAsJson,
    /// See [`SimpleSig::CastIntAsJson`].
    CastRealAsJson,
    /// See [`SimpleSig::CastIntAsJson`].
    CastDecimalAsJson,
    /// See [`SimpleSig::CastIntAsJson`].
    CastStringAsJson,
    /// See [`SimpleSig::CastIntAsJson`].
    CastTimeAsJson,
    /// See [`SimpleSig::CastIntAsJson`].
    CastDurationAsJson,
    /// See [`SimpleSig::CastIntAsJson`].
    CastJsonAsJson,
    /// The JSON-source casts: Go's `ConvertJSONToInt64`/`ConvertJSON-
    /// ToReal` truncate numbers, run the numeric prefix for strings, and
    /// answer 0 for other codes under the folded error; the time and
    /// duration forms read the opaque codes or parse the text.
    CastJsonAsInt,
    /// See [`SimpleSig::CastJsonAsInt`].
    CastJsonAsReal,
    /// See [`SimpleSig::CastJsonAsInt`].
    CastJsonAsTime,
    /// See [`SimpleSig::CastJsonAsInt`].
    CastJsonAsDuration,
    /// `CastIntAsReal`/`CastRealAsReal`/`CastDecimalAsReal`: widening to
    /// binary64 (`AS REAL`); a bare cast answers its own truth.
    CastIntAsReal,
    /// See [`SimpleSig::CastIntAsReal`].
    CastRealAsReal,
    /// See [`SimpleSig::CastIntAsReal`].
    CastDecimalAsReal,
    /// `LikeSig` over (target, pattern, escape). The `i32` is the
    /// comparison's collation id, as in the string comparisons: `_ci`
    /// collations fold case before the wildcard match, `_bin` is exact.
    Like(i32),
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
    if tp == tipb::ExprType::Float64 {
        // Go `convertFloat`: `codec.DecodeFloat` -- eight big-endian bits.
        let bytes = expr.val();
        if bytes.len() != 8 {
            return Err(format!("invalid float literal: {bytes:?}"));
        }
        return Ok(SimpleExpr::Real(f64::from_bits(u64::from_be_bytes(
            bytes.try_into().expect("eight bytes"),
        ))));
    }
    if tp == tipb::ExprType::String {
        return Ok(SimpleExpr::Bytes(expr.val().to_vec()));
    }
    if tp == tipb::ExprType::MysqlTime {
        let (_, packed) = tidb_codec::decode_uint(expr.val())
            .map_err(|err| format!("invalid time literal: {err:?}"))?;
        let field_type = expr.field_type.as_ref();
        let kind = field_type.and_then(|ft| u8::try_from(ft.tp()).ok()).map_or(
            tidb_datatype::TimeType::DateTime,
            |tp| match tidb_datatype::FieldTypeCode::from_mysql_type(tp) {
                tidb_datatype::FieldTypeCode::Date => tidb_datatype::TimeType::Date,
                tidb_datatype::FieldTypeCode::Timestamp => tidb_datatype::TimeType::Timestamp,
                _ => tidb_datatype::TimeType::DateTime,
            },
        );
        let fsp = field_type.map_or(0, |ft| i64::from(ft.decimal()));
        let value = tidb_datatype::Time::from_packed_uint(packed, kind, fsp)
            .map_err(|err| format!("invalid time literal: {err:?}"))?;
        return Ok(SimpleExpr::Time(value));
    }
    if tp == tipb::ExprType::MysqlDecimal {
        let (_, value, _, _) = tidb_codec::decode_decimal(expr.val())
            .map_err(|err| format!("invalid decimal literal: {err:?}"))?;
        return Ok(SimpleExpr::Decimal(value));
    }
    if tp == tipb::ExprType::MysqlJson {
        // Go `convertJSON`: `codec.DecodeOne` -- a codec-wrapped JSON datum.
        let datums = tidb_codec::decode(expr.val(), 1)
            .map_err(|err| format!("invalid json literal: {err:?}"))?;
        let Some(datum) = datums.into_iter().next() else {
            return Err("invalid json literal: no datum".to_owned());
        };
        match datum {
            tidb_datatype::Datum::Json(json) => return Ok(SimpleExpr::Json(json)),
            _ => return Err("invalid json literal: not a json datum".to_owned()),
        }
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
            // The decimal arithmetic and integer MOD families -- the wire
            // ids are the upstream tipb contract, carried by the trimmed
            // proto build.
            tipb::ScalarFuncSig::PlusDecimal => SimpleSig::PlusDecimal,
            tipb::ScalarFuncSig::MinusDecimal => SimpleSig::MinusDecimal,
            tipb::ScalarFuncSig::MultiplyDecimal => SimpleSig::MultiplyDecimal,
            tipb::ScalarFuncSig::ModDecimal => SimpleSig::ModDecimal,
            tipb::ScalarFuncSig::ModIntUnsignedUnsigned => SimpleSig::ModIntUnsignedUnsigned,
            tipb::ScalarFuncSig::ModIntUnsignedSigned => SimpleSig::ModIntUnsignedSigned,
            tipb::ScalarFuncSig::ModIntSignedUnsigned => SimpleSig::ModIntSignedUnsigned,
            tipb::ScalarFuncSig::ModIntSignedSigned => SimpleSig::ModIntSignedSigned,
            tipb::ScalarFuncSig::IntDivideInt => SimpleSig::IntDivideInt,
            tipb::ScalarFuncSig::IntDivideIntUnsignedUnsigned => {
                SimpleSig::IntDivideIntUnsignedUnsigned
            }
            tipb::ScalarFuncSig::IntDivideIntUnsignedSigned => {
                SimpleSig::IntDivideIntUnsignedSigned
            }
            tipb::ScalarFuncSig::IntDivideIntSignedSigned => SimpleSig::IntDivideIntSignedSigned,
            tipb::ScalarFuncSig::IntDivideIntSignedUnsigned => {
                SimpleSig::IntDivideIntSignedUnsigned
            }
            tipb::ScalarFuncSig::IntDivideDecimal => SimpleSig::IntDivideDecimal,
            tipb::ScalarFuncSig::PlusInt => SimpleSig::PlusInt,
            tipb::ScalarFuncSig::MinusInt => SimpleSig::MinusInt,
            tipb::ScalarFuncSig::MultiplyInt => SimpleSig::MultiplyInt,
            tipb::ScalarFuncSig::MultiplyIntUnsigned => SimpleSig::MultiplyIntUnsigned,
            tipb::ScalarFuncSig::PlusIntUnsignedUnsigned => SimpleSig::PlusIntUnsignedUnsigned,
            tipb::ScalarFuncSig::PlusIntUnsignedSigned => SimpleSig::PlusIntUnsignedSigned,
            tipb::ScalarFuncSig::PlusIntSignedUnsigned => SimpleSig::PlusIntSignedUnsigned,
            tipb::ScalarFuncSig::PlusIntSignedSigned => SimpleSig::PlusIntSignedSigned,
            tipb::ScalarFuncSig::MinusIntUnsignedUnsigned => SimpleSig::MinusIntUnsignedUnsigned,
            tipb::ScalarFuncSig::MinusIntUnsignedSigned => SimpleSig::MinusIntUnsignedSigned,
            tipb::ScalarFuncSig::MinusIntSignedUnsigned => SimpleSig::MinusIntSignedUnsigned,
            tipb::ScalarFuncSig::MinusIntSignedSigned => SimpleSig::MinusIntSignedSigned,
            tipb::ScalarFuncSig::MinusIntForcedUnsignedUnsigned => {
                SimpleSig::MinusIntForcedUnsignedUnsigned
            }
            tipb::ScalarFuncSig::MinusIntForcedUnsignedSigned => {
                SimpleSig::MinusIntForcedUnsignedSigned
            }
            tipb::ScalarFuncSig::MinusIntForcedSignedUnsigned => {
                SimpleSig::MinusIntForcedSignedUnsigned
            }
            tipb::ScalarFuncSig::LtReal => SimpleSig::LtReal,
            tipb::ScalarFuncSig::LeReal => SimpleSig::LeReal,
            tipb::ScalarFuncSig::GtReal => SimpleSig::GtReal,
            tipb::ScalarFuncSig::GeReal => SimpleSig::GeReal,
            tipb::ScalarFuncSig::EqReal => SimpleSig::EqReal,
            tipb::ScalarFuncSig::NeReal => SimpleSig::NeReal,
            tipb::ScalarFuncSig::PlusReal => SimpleSig::PlusReal,
            tipb::ScalarFuncSig::MinusReal => SimpleSig::MinusReal,
            tipb::ScalarFuncSig::MultiplyReal => SimpleSig::MultiplyReal,
            tipb::ScalarFuncSig::DivideReal => SimpleSig::DivideReal,
            tipb::ScalarFuncSig::DivideDecimal => SimpleSig::DivideDecimal,
            tipb::ScalarFuncSig::RoundReal => SimpleSig::RoundReal,
            tipb::ScalarFuncSig::RoundInt => SimpleSig::RoundInt,
            tipb::ScalarFuncSig::RoundDec => SimpleSig::RoundDec,
            tipb::ScalarFuncSig::Pow => SimpleSig::Pow,
            tipb::ScalarFuncSig::Acos => SimpleSig::Acos,
            tipb::ScalarFuncSig::Asin => SimpleSig::Asin,
            tipb::ScalarFuncSig::Atan1Arg => SimpleSig::Atan1Arg,
            tipb::ScalarFuncSig::Atan2Args => SimpleSig::Atan2Args,
            tipb::ScalarFuncSig::Cos => SimpleSig::Cos,
            tipb::ScalarFuncSig::Cot => SimpleSig::Cot,
            tipb::ScalarFuncSig::Pi => SimpleSig::Pi,
            tipb::ScalarFuncSig::Sin => SimpleSig::Sin,
            tipb::ScalarFuncSig::WeekWithoutMode => SimpleSig::WeekWithoutMode,
            tipb::ScalarFuncSig::InString => SimpleSig::InString(collation_of(expr)),
            tipb::ScalarFuncSig::VectorFloat32IsNull => SimpleSig::VectorFloat32IsNull,
            tipb::ScalarFuncSig::CharLengthUtf8 => SimpleSig::CharLengthUtf8,
            tipb::ScalarFuncSig::CharLength => SimpleSig::CharLength,
            tipb::ScalarFuncSig::LowerUtf8 => SimpleSig::LowerUtf8,
            tipb::ScalarFuncSig::Lower => SimpleSig::Lower,
            tipb::ScalarFuncSig::DateFormatSig => SimpleSig::DateFormatSig,
            tipb::ScalarFuncSig::Conv => SimpleSig::Conv,
            tipb::ScalarFuncSig::AddDateStringString => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::String,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateStringInt => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::String,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateStringDecimal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::String,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateIntString => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Int,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateIntInt => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Int,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDatetimeString => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Datetime,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDatetimeInt => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Datetime,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateStringString => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::String,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateStringInt => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::String,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateStringDecimal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::String,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateIntString => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Int,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateIntInt => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Int,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDatetimeString => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Datetime,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDatetimeInt => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Datetime,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateStringReal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::String,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateIntReal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Int,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateIntDecimal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Int,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDatetimeReal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Datetime,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDatetimeDecimal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Datetime,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDurationString => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Duration,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDurationInt => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Duration,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDurationReal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Duration,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDurationDecimal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Duration,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateStringReal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::String,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateIntReal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Int,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateIntDecimal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Int,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDatetimeReal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Datetime,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDatetimeDecimal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Datetime,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDurationString => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Duration,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDurationInt => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Duration,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDurationReal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Duration,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDurationDecimal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Duration,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDurationStringDatetime => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Duration,
                interval: IntervalArg::String,
                datetime_result: true,
            },
            tipb::ScalarFuncSig::AddDateDurationIntDatetime => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Duration,
                interval: IntervalArg::Int,
                datetime_result: true,
            },
            tipb::ScalarFuncSig::AddDateDurationRealDatetime => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Duration,
                interval: IntervalArg::Real,
                datetime_result: true,
            },
            tipb::ScalarFuncSig::AddDateDurationDecimalDatetime => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Duration,
                interval: IntervalArg::Decimal,
                datetime_result: true,
            },
            tipb::ScalarFuncSig::SubDateDurationStringDatetime => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Duration,
                interval: IntervalArg::String,
                datetime_result: true,
            },
            tipb::ScalarFuncSig::SubDateDurationIntDatetime => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Duration,
                interval: IntervalArg::Int,
                datetime_result: true,
            },
            tipb::ScalarFuncSig::SubDateDurationRealDatetime => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Duration,
                interval: IntervalArg::Real,
                datetime_result: true,
            },
            tipb::ScalarFuncSig::SubDateDurationDecimalDatetime => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Duration,
                interval: IntervalArg::Decimal,
                datetime_result: true,
            },
            tipb::ScalarFuncSig::AddDateRealString => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Real,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateRealInt => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Real,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateRealReal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Real,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateRealDecimal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Real,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDecimalString => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Decimal,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDecimalInt => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Decimal,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDecimalReal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Decimal,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::AddDateDecimalDecimal => SimpleSig::AddSubDate {
                subtract: false,
                date: DateArithArg::Decimal,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateRealString => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Real,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateRealInt => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Real,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateRealReal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Real,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateRealDecimal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Real,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDecimalString => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Decimal,
                interval: IntervalArg::String,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDecimalInt => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Decimal,
                interval: IntervalArg::Int,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDecimalReal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Decimal,
                interval: IntervalArg::Real,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::SubDateDecimalDecimal => SimpleSig::AddSubDate {
                subtract: true,
                date: DateArithArg::Decimal,
                interval: IntervalArg::Decimal,
                datetime_result: false,
            },
            tipb::ScalarFuncSig::UpperUtf8 => SimpleSig::UpperUtf8,
            tipb::ScalarFuncSig::Upper => SimpleSig::Upper,
            tipb::ScalarFuncSig::Substring2ArgsUtf8 => SimpleSig::Substring2ArgsUtf8,
            tipb::ScalarFuncSig::Substring3ArgsUtf8 => SimpleSig::Substring3ArgsUtf8,
            tipb::ScalarFuncSig::Substring2Args => SimpleSig::Substring2Args,
            tipb::ScalarFuncSig::Substring3Args => SimpleSig::Substring3Args,
            tipb::ScalarFuncSig::LtInt => SimpleSig::LtInt,
            tipb::ScalarFuncSig::LeInt => SimpleSig::LeInt,
            tipb::ScalarFuncSig::GtInt => SimpleSig::GtInt,
            tipb::ScalarFuncSig::GeInt => SimpleSig::GeInt,
            tipb::ScalarFuncSig::EqInt => SimpleSig::EqInt,
            tipb::ScalarFuncSig::NeInt => SimpleSig::NeInt,
            // The wire rarely carries a logical AND scalar: a WHERE
            // conjunction arrives as SEPARATE selection conditions, the
            // list itself being the AND. The evaluator keeps the
            // three-valued semantics for both shapes.
            tipb::ScalarFuncSig::LogicalAnd => SimpleSig::LogicalAnd,
            tipb::ScalarFuncSig::LogicalOr => SimpleSig::LogicalOr,
            tipb::ScalarFuncSig::ModReal => SimpleSig::ModReal,
            tipb::ScalarFuncSig::TimestampDiff => SimpleSig::TimestampDiff,
            tipb::ScalarFuncSig::UnaryNotInt => SimpleSig::UnaryNot,
            tipb::ScalarFuncSig::IntIsNull => SimpleSig::IntIsNull,
            tipb::ScalarFuncSig::Date => SimpleSig::Date,
            tipb::ScalarFuncSig::Hour => SimpleSig::Hour,
            tipb::ScalarFuncSig::Minute => SimpleSig::Minute,
            tipb::ScalarFuncSig::Second => SimpleSig::Second,
            tipb::ScalarFuncSig::MicroSecond => SimpleSig::MicroSecond,
            tipb::ScalarFuncSig::Month => SimpleSig::Month,
            tipb::ScalarFuncSig::DateDiff => SimpleSig::DateDiff,
            tipb::ScalarFuncSig::CastTimeAsDuration => SimpleSig::CastTimeAsDuration,
            tipb::ScalarFuncSig::DecimalIsNull => SimpleSig::DecimalIsNull,
            tipb::ScalarFuncSig::DurationIsNull => SimpleSig::DurationIsNull,
            tipb::ScalarFuncSig::RealIsNull => SimpleSig::RealIsNull,
            tipb::ScalarFuncSig::StringIsNull => SimpleSig::StringIsNull,
            tipb::ScalarFuncSig::TimeIsNull => SimpleSig::TimeIsNull,
            tipb::ScalarFuncSig::LikeSig => SimpleSig::Like(collation_of(expr)),
            tipb::ScalarFuncSig::JsonMemberOfSig => SimpleSig::JsonMemberOfSig,
            tipb::ScalarFuncSig::CastIntAsInt => SimpleSig::CastIntAsInt,
            tipb::ScalarFuncSig::CastRealAsInt => SimpleSig::CastRealAsInt,
            tipb::ScalarFuncSig::CastDecimalAsInt => SimpleSig::CastDecimalAsInt,
            tipb::ScalarFuncSig::CastIntAsReal => SimpleSig::CastIntAsReal,
            tipb::ScalarFuncSig::CastRealAsReal => SimpleSig::CastRealAsReal,
            tipb::ScalarFuncSig::CastDecimalAsReal => SimpleSig::CastDecimalAsReal,
            tipb::ScalarFuncSig::CastStringAsInt => SimpleSig::CastStringAsInt,
            tipb::ScalarFuncSig::CastStringAsReal => SimpleSig::CastStringAsReal,
            tipb::ScalarFuncSig::CastIntAsDecimal => SimpleSig::CastIntAsDecimal,
            tipb::ScalarFuncSig::CastRealAsDecimal => SimpleSig::CastRealAsDecimal,
            tipb::ScalarFuncSig::CastDecimalAsDecimal => SimpleSig::CastDecimalAsDecimal,
            tipb::ScalarFuncSig::CastStringAsDecimal => SimpleSig::CastStringAsDecimal,
            tipb::ScalarFuncSig::CastTimeAsDecimal => SimpleSig::CastTimeAsDecimal,
            tipb::ScalarFuncSig::CastDurationAsDecimal => SimpleSig::CastDurationAsDecimal,
            tipb::ScalarFuncSig::CastIntAsString => SimpleSig::CastIntAsString,
            tipb::ScalarFuncSig::CastRealAsString => SimpleSig::CastRealAsString,
            tipb::ScalarFuncSig::CastDecimalAsString => SimpleSig::CastDecimalAsString,
            tipb::ScalarFuncSig::CastStringAsString => SimpleSig::CastStringAsString,
            tipb::ScalarFuncSig::CastTimeAsString => SimpleSig::CastTimeAsString,
            tipb::ScalarFuncSig::CastDurationAsString => SimpleSig::CastDurationAsString,
            tipb::ScalarFuncSig::CastIntAsTime => SimpleSig::CastIntAsTime,
            tipb::ScalarFuncSig::CastRealAsTime => SimpleSig::CastRealAsTime,
            tipb::ScalarFuncSig::CastDecimalAsTime => SimpleSig::CastDecimalAsTime,
            tipb::ScalarFuncSig::CastStringAsTime => SimpleSig::CastStringAsTime,
            tipb::ScalarFuncSig::CastTimeAsTime => SimpleSig::CastTimeAsTime,
            tipb::ScalarFuncSig::CastIntAsDuration => SimpleSig::CastIntAsDuration,
            tipb::ScalarFuncSig::CastRealAsDuration => SimpleSig::CastRealAsDuration,
            tipb::ScalarFuncSig::CastDecimalAsDuration => SimpleSig::CastDecimalAsDuration,
            tipb::ScalarFuncSig::CastStringAsDuration => SimpleSig::CastStringAsDuration,
            tipb::ScalarFuncSig::CastDurationAsDuration => SimpleSig::CastDurationAsDuration,
            tipb::ScalarFuncSig::CastIntAsJson => SimpleSig::CastIntAsJson,
            tipb::ScalarFuncSig::CastRealAsJson => SimpleSig::CastRealAsJson,
            tipb::ScalarFuncSig::CastDecimalAsJson => SimpleSig::CastDecimalAsJson,
            tipb::ScalarFuncSig::CastStringAsJson => SimpleSig::CastStringAsJson,
            tipb::ScalarFuncSig::CastTimeAsJson => SimpleSig::CastTimeAsJson,
            tipb::ScalarFuncSig::CastDurationAsJson => SimpleSig::CastDurationAsJson,
            tipb::ScalarFuncSig::CastJsonAsJson => SimpleSig::CastJsonAsJson,
            tipb::ScalarFuncSig::CastJsonAsInt => SimpleSig::CastJsonAsInt,
            tipb::ScalarFuncSig::CastJsonAsReal => SimpleSig::CastJsonAsReal,
            tipb::ScalarFuncSig::CastJsonAsTime => SimpleSig::CastJsonAsTime,
            tipb::ScalarFuncSig::CastJsonAsDuration => SimpleSig::CastJsonAsDuration,
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
            tipb::ScalarFuncSig::LtDecimal => SimpleSig::LtDecimal,
            tipb::ScalarFuncSig::LeDecimal => SimpleSig::LeDecimal,
            tipb::ScalarFuncSig::GtDecimal => SimpleSig::GtDecimal,
            tipb::ScalarFuncSig::GeDecimal => SimpleSig::GeDecimal,
            tipb::ScalarFuncSig::EqDecimal => SimpleSig::EqDecimal,
            tipb::ScalarFuncSig::NeDecimal => SimpleSig::NeDecimal,
            tipb::ScalarFuncSig::LtTime => SimpleSig::LtTime,
            tipb::ScalarFuncSig::LeTime => SimpleSig::LeTime,
            tipb::ScalarFuncSig::GtTime => SimpleSig::GtTime,
            tipb::ScalarFuncSig::GeTime => SimpleSig::GeTime,
            tipb::ScalarFuncSig::EqTime => SimpleSig::EqTime,
            tipb::ScalarFuncSig::NeTime => SimpleSig::NeTime,
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
/// The DECIMAL value of one operand of a decimal comparison.
///
/// Go's `GtDecimal` and its siblings evaluate both children as decimals, the
/// other side's cast having been folded at build time -- so a literal arrives
/// already `MysqlDecimal` and a column arrives as one. An integer column can
/// still reach here through `dc > i`, where Go wraps the int side in
/// `CastIntAsDecimal`; the `AS DECIMAL` cast family composes below, so
/// cast operands reach here alongside the exact ones.
fn eval_decimal(
    expr: Option<&SimpleExpr>,
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
) -> Option<tidb_datatype::Decimal> {
    use tidb_datatype::Datum;
    let expr = expr?;
    match expr {
        SimpleExpr::Decimal(value) => Some(value.clone()),
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::Decimal(value)) => Some(value.clone()),
            _ => None,
        },
        // Go `EvalPlusDecimal`/`EvalMinusDecimal`/`EvalMultiplyDecimal`
        // (`pkg/expression/builtin_arithmetic_vec.go`): exact decimal
        // arithmetic, NULL in -> NULL out. A zero MOD divisor answers NULL
        // (MySQL), as `rem_mysql` already encodes.
        SimpleExpr::Func(
            sig @ (SimpleSig::PlusDecimal
            | SimpleSig::MinusDecimal
            | SimpleSig::MultiplyDecimal
            | SimpleSig::ModDecimal
            | SimpleSig::DivideDecimal),
            children,
        ) => {
            let (left, right) = (
                eval_decimal(children.first(), row, div_precision_increment)?,
                eval_decimal(children.get(1), row, div_precision_increment)?,
            );
            match sig {
                SimpleSig::DivideDecimal => left.div_mysql(&right, div_precision_increment as u32),
                SimpleSig::PlusDecimal => Some(left.add_mysql(&right).0),
                SimpleSig::MinusDecimal => Some(left.sub_mysql(&right).0),
                SimpleSig::MultiplyDecimal => Some(left.mul_mysql(&right).0),
                _ => left.rem_mysql(&right),
            }
        }
        // The `AS DECIMAL` casts answer exact decimals: Go's per-source
        // conversions, with the union clamp folded away -- the seam
        // carries no field type, so `ProduceDecWithSpecifiedTp` is an
        // identity here.
        SimpleExpr::Func(
            sig @ (SimpleSig::CastIntAsDecimal
            | SimpleSig::CastRealAsDecimal
            | SimpleSig::CastDecimalAsDecimal
            | SimpleSig::CastStringAsDecimal
            | SimpleSig::CastTimeAsDecimal
            | SimpleSig::CastDurationAsDecimal),
            children,
        ) => {
            match sig {
                SimpleSig::CastIntAsDecimal => {
                    let value = children
                        .first()
                        .and_then(|c| eval_expr(c, row, div_precision_increment).ok())
                        .flatten()?;
                    // An unsigned source renders through `from_uint` --
                    // the i128 carries the value without the wire's flag.
                    if let Ok(signed) = i64::try_from(value) {
                        Some(tidb_datatype::Decimal::from_my_decimal(
                            &tidb_datatype::MyDecimal::from_int(signed),
                        ))
                    } else {
                        u64::try_from(value).ok().map(|value| {
                            tidb_datatype::Decimal::from_my_decimal(
                                &tidb_datatype::MyDecimal::from_uint(value),
                            )
                        })
                    }
                }
                SimpleSig::CastRealAsDecimal => {
                    let value = eval_real(children.first(), row, div_precision_increment)?;
                    // Go `FromFloat64`: the truncated warning folds.
                    Some(tidb_datatype::Decimal::from_my_decimal(
                        &tidb_datatype::MyDecimal::from_float64(value).0,
                    ))
                }
                SimpleSig::CastDecimalAsDecimal => {
                    eval_decimal(children.first(), row, div_precision_increment)
                }
                SimpleSig::CastStringAsDecimal => {
                    let raw = eval_bytes(children.first(), row, div_precision_increment)?;
                    let text = String::from_utf8_lossy(&raw);
                    // Go trims, then `FromString` (the truncated warning
                    // folds; the numeric prefix survives).
                    Some(tidb_datatype::Decimal::from_my_decimal(
                        &tidb_datatype::MyDecimal::from_string(text.trim().as_bytes()).0,
                    ))
                }
                SimpleSig::CastTimeAsDecimal => {
                    eval_time(children.first(), row, div_precision_increment)
                        .map(tidb_datatype::Time::to_number)
                }
                SimpleSig::CastDurationAsDecimal => {
                    eval_duration(children.first(), row, div_precision_increment)
                        .map(tidb_datatype::MySqlDuration::to_number)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Go's `CreateBinaryJSON` over a scanned datum, for a MEMBER OF target:
/// integers, reals, strings, bytes and JSON pass; other kinds wait on their
/// cast courses.
fn datum_to_json_value(datum: &tidb_datatype::Datum) -> Option<tidb_datatype::BinaryJSONValue> {
    use tidb_datatype::{BinaryJSONValue, Datum};
    Some(match datum {
        Datum::Null => BinaryJSONValue::Null,
        Datum::Int(value) => BinaryJSONValue::Int64(*value),
        Datum::UInt(value) => BinaryJSONValue::Uint64(*value),
        Datum::Real(value) => BinaryJSONValue::Float64(*value),
        Datum::String(text) => {
            BinaryJSONValue::String(String::from_utf8_lossy(text.bytes()).into_owned())
        }
        Datum::Bytes(bytes) => BinaryJSONValue::String(String::from_utf8_lossy(bytes).into_owned()),
        Datum::Json(json) => BinaryJSONValue::Binary(json.clone()),
        _ => return None,
    })
}

/// The binary JSON of one operand of a JSON operation; see [`eval_decimal`]
/// for why only exact operands reach here.
fn eval_json(
    expr: Option<&SimpleExpr>,
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
) -> Option<tidb_datatype::BinaryJSON> {
    use tidb_datatype::Datum;
    let to_json = |value: tidb_datatype::BinaryJSONValue| {
        tidb_datatype::BinaryJSON::from_typed_value(&value).ok()
    };
    match expr? {
        SimpleExpr::Json(value) => Some(value.clone()),
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::Json(value)) => Some(value.clone()),
            _ => None,
        },
        // The `AS JSON` casts wrap each source in its JSON type. Go's
        // boolean/unsigned wraps fold -- the i128 carries the sign, and
        // no wire kind is boolean here.
        SimpleExpr::Func(
            sig @ (SimpleSig::CastIntAsJson
            | SimpleSig::CastRealAsJson
            | SimpleSig::CastDecimalAsJson
            | SimpleSig::CastStringAsJson
            | SimpleSig::CastTimeAsJson
            | SimpleSig::CastDurationAsJson
            | SimpleSig::CastJsonAsJson),
            children,
        ) => {
            match sig {
                SimpleSig::CastIntAsJson => children
                    .first()
                    .and_then(|c| eval_expr(c, row, div_precision_increment).ok())
                    .flatten()
                    .and_then(|value| i64::try_from(value).ok())
                    .and_then(|value| to_json(tidb_datatype::BinaryJSONValue::Int64(value))),
                SimpleSig::CastRealAsJson => {
                    eval_real(children.first(), row, div_precision_increment)
                        .and_then(|value| to_json(tidb_datatype::BinaryJSONValue::Float64(value)))
                }
                SimpleSig::CastDecimalAsJson => {
                    // Go converts through f64 and notes the FIXME: the
                    // JSON type reads DOUBLE.
                    let value = eval_decimal(children.first(), row, div_precision_increment)?;
                    to_json(tidb_datatype::BinaryJSONValue::Float64(value.to_f64()))
                }
                SimpleSig::CastStringAsJson => {
                    let raw = eval_bytes(children.first(), row, div_precision_increment)?;
                    let text = String::from_utf8_lossy(&raw).into_owned();
                    let parsed = tidb_datatype::BinaryJSON::parse(&text).ok()?;
                    Some(parsed)
                }
                // Go re-fits datetime and timestamp to MaxFsp before
                // wrapping; dates keep their kind.
                SimpleSig::CastTimeAsJson => {
                    let time = eval_time(children.first(), row, div_precision_increment)?;
                    let time = if time.kind() == tidb_datatype::TimeType::DateTime {
                        let mut widened = time;
                        widened.set_fsp(6).ok()?;
                        widened
                    } else {
                        time
                    };
                    to_json(tidb_datatype::BinaryJSONValue::Time(time))
                }
                SimpleSig::CastDurationAsJson => {
                    let duration = eval_duration(children.first(), row, div_precision_increment)?;
                    let widened =
                        tidb_datatype::MySqlDuration::from_nanoseconds(duration.nanoseconds(), 6)
                            .ok()?;
                    to_json(tidb_datatype::BinaryJSONValue::Duration(widened))
                }
                _ => eval_json(children.first(), row, div_precision_increment),
            }
        }
        _ => None,
    }
}

/// The binary64 value of one operand of a real comparison or arithmetic;
/// see [`eval_decimal`] for why only exact operands reach here.
fn eval_real(
    expr: Option<&SimpleExpr>,
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
) -> Option<f64> {
    use tidb_datatype::Datum;
    let expr = expr?;
    match expr {
        SimpleExpr::Real(value) => Some(*value),
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::Real(value)) => Some(*value),
            _ => None,
        },
        // Go `ConvertJSONToReal`: numbers pass, strings take the numeric
        // prefix, other codes answer 0 under the folded error.
        SimpleExpr::Func(SimpleSig::CastJsonAsReal, children) => {
            let value = eval_json(children.first(), row, div_precision_increment)?;
            if let Some(real) = value.as_f64() {
                Some(real)
            } else if let Some(text) = value.as_string() {
                let text = String::from_utf8_lossy(text);
                let prefix = numeric_prefix(text.trim_start(), true).unwrap_or_default();
                Some(prefix.parse::<f64>().unwrap_or(0.0))
            } else {
                Some(0.0)
            }
        }
        // Go's arithmetic evaluators recurse; the real family composes.
        SimpleExpr::Func(
            sig @ (SimpleSig::PlusReal
            | SimpleSig::MinusReal
            | SimpleSig::MultiplyReal
            | SimpleSig::DivideReal
            | SimpleSig::ModReal
            | SimpleSig::CastIntAsReal
            | SimpleSig::CastDecimalAsReal
            | SimpleSig::CastRealAsReal
            | SimpleSig::CastStringAsReal
            | SimpleSig::RoundReal
            | SimpleSig::RoundInt
            | SimpleSig::RoundDec
            | SimpleSig::Pow
            | SimpleSig::Acos
            | SimpleSig::Asin
            | SimpleSig::Atan1Arg
            | SimpleSig::Atan2Args
            | SimpleSig::Cos
            | SimpleSig::Cot
            | SimpleSig::Pi
            | SimpleSig::Sin),
            children,
        ) => {
            if !matches!(
                sig,
                SimpleSig::PlusReal
                    | SimpleSig::MinusReal
                    | SimpleSig::MultiplyReal
                    | SimpleSig::DivideReal
                    | SimpleSig::ModReal
            ) {
                // Go wraps the operand in the cast signature; the widening
                // itself is exact for the admitted source kinds.
                return match sig {
                    SimpleSig::CastIntAsReal => {
                        // The int channel carries no error for a leaf.
                        let value = children.first().and_then(|c| {
                            eval_expr(c, row, div_precision_increment).ok().flatten()
                        });
                        value.map(|value| value as f64)
                    }
                    SimpleSig::CastDecimalAsReal => {
                        Some(eval_decimal(children.first(), row, div_precision_increment)?.to_f64())
                    }
                    SimpleSig::CastStringAsReal => {
                        let raw = eval_bytes(children.first(), row, div_precision_increment)?;
                        let text = String::from_utf8_lossy(&raw);
                        let Some(prefix) = numeric_prefix(text.trim_start(), true) else {
                            return Some(0.0);
                        };
                        let parsed = prefix.parse::<f64>().unwrap_or(f64::NAN);
                        // `strconv.ParseFloat` range saturation: ±MaxFloat64.
                        Some(if parsed.is_infinite() {
                            if parsed > 0.0 {
                                f64::MAX
                            } else {
                                f64::MIN
                            }
                        } else {
                            parsed
                        })
                    }
                    SimpleSig::RoundReal => {
                        eval_real(children.first(), row, div_precision_increment).map(f64::round)
                    }
                    SimpleSig::RoundInt => children
                        .first()
                        .and_then(|c| eval_expr(c, row, div_precision_increment).ok().flatten())
                        .map(|value| value as f64),
                    SimpleSig::RoundDec => Some(
                        eval_decimal(children.first(), row, div_precision_increment)?
                            .round_to_scale(0)
                            .to_f64(),
                    ),
                    SimpleSig::Pow => {
                        let left = eval_real(children.first(), row, div_precision_increment)?;
                        let right = eval_real(children.get(1), row, div_precision_increment)?;
                        Some(left.powf(right))
                    }
                    SimpleSig::Atan2Args => {
                        let left = eval_real(children.first(), row, div_precision_increment)?;
                        let right = eval_real(children.get(1), row, div_precision_increment)?;
                        Some(left.atan2(right))
                    }
                    SimpleSig::Pi => Some(std::f64::consts::PI),
                    other => {
                        let value = eval_real(children.first(), row, div_precision_increment)?;
                        match other {
                            SimpleSig::Acos => Some(value.acos()),
                            SimpleSig::Asin => Some(value.asin()),
                            SimpleSig::Atan1Arg => Some(value.atan()),
                            SimpleSig::Cos => Some(value.cos()),
                            SimpleSig::Sin => Some(value.sin()),
                            SimpleSig::Cot => Some(1.0 / value.tan()),
                            _ => return None,
                        }
                    }
                };
            }
            let (left, right) = (
                eval_real(children.first(), row, div_precision_increment)?,
                eval_real(children.get(1), row, div_precision_increment)?,
            );
            match sig {
                SimpleSig::PlusReal => Some(left + right),
                SimpleSig::MinusReal => Some(left - right),
                SimpleSig::MultiplyReal => Some(left * right),
                // Go `math.Mod`: the remainder carries the dividend's
                // sign; a zero divisor answers NULL (error folded).
                SimpleSig::ModReal if right != 0.0 => Some(left % right),
                SimpleSig::ModReal => None,
                _ if right != 0.0 => Some(left / right),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The MySQL duration of one operand: a DURATION leaf, or a DATETIME
/// leaf's time-of-day (Go `CastTimeAsDuration`).
/// The parse anchor for Go's `typeCtx` conversions. Go anchors these on
/// the session time zone; the expression seam carries no session, so it
/// anchors on UTC.
fn utc_anchor() -> tidb_datatype::SessionTimeZone {
    tidb_datatype::SessionTimeZone::Named(chrono_tz::UTC)
}

/// Go `intervalReformatString`: single units keep the leading numeric
/// prefix (`^[+-]?[\d]+`, the truncation error folded), `SECOND`
/// re-renders the text through a decimal, and compound units pass
/// through for the composite parser.
fn interval_reformat_string(text: &str, unit: &str) -> String {
    match unit.to_ascii_uppercase().as_str() {
        "MICROSECOND" | "MINUTE" | "HOUR" | "DAY" | "WEEK" | "MONTH" | "QUARTER" | "YEAR" => {
            let trimmed = text.trim();
            let bytes = trimmed.as_bytes();
            let mut end = usize::from(matches!(bytes.first(), Some(b'+') | Some(b'-')));
            let digits_from = end;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end == digits_from {
                "0".to_owned()
            } else {
                trimmed[..end].to_owned()
            }
        }
        // Go: `dec.FromString` then `ToString` ("1e2" -> "100"); a parse
        // failure answers "0" with the truncation folded.
        "SECOND" => {
            let (dec, _) = tidb_datatype::MyDecimal::from_string(text.as_bytes());
            String::from_utf8_lossy(&dec.to_string_bytes()).into_owned()
        }
        _ => text.to_owned(),
    }
}

/// Go `getIntervalFromDecimal`'s unit table: compound units reshape the
/// decimal text into the composite literal, single units round half-up
/// to a whole number (Go `intervalDecimalToString`).
fn interval_reformat_decimal_text(text: &str, unit: &str) -> String {
    match unit.to_ascii_uppercase().as_str() {
        "HOUR_MINUTE" | "MINUTE_SECOND" => text.replace('.', ":"),
        "YEAR_MONTH" => text.replace('.', "-"),
        "DAY_HOUR" => text.replace('.', " "),
        "DAY_MINUTE" => format!("0 {}", text.replace('.', ":")),
        "DAY_SECOND" => format!("0 00:{}", text.replace('.', ":")),
        "DAY_MICROSECOND" => format!("0 00:00:{text}"),
        "HOUR_MICROSECOND" => format!("00:00:{text}"),
        "HOUR_SECOND" => format!("00:{}", text.replace('.', ":")),
        "MINUTE_MICROSECOND" => format!("00:{text}"),
        // `SECOND` already reads like the `%f` format.
        "SECOND" | "SECOND_MICROSECOND" => text.to_owned(),
        _ => {
            let (mut dec, _) = tidb_datatype::MyDecimal::from_string(text.as_bytes());
            dec.round_in_place(0, tidb_datatype::RoundMode::HalfUp);
            String::from_utf8_lossy(&dec.to_string_bytes()).into_owned()
        }
    }
}

/// Whether the upstream pair answers text: the non-temporal sources
/// (Go `builtinAddSubDateAsStringSig`).
fn add_sub_answers_text(sig: &SimpleSig) -> bool {
    matches!(
        sig,
        SimpleSig::AddSubDate {
            date: DateArithArg::String
                | DateArithArg::Int
                | DateArithArg::Real
                | DateArithArg::Decimal,
            ..
        }
    )
}

/// The interval text for one operand channel (Go `getInterval*`); the
/// interval operand sits at index 1 of the `AddDate`/`SubDate` node.
fn interval_text(
    children: &[SimpleExpr],
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
    kind: IntervalArg,
    unit: &str,
) -> Option<String> {
    match kind {
        IntervalArg::String => {
            let raw = eval_bytes(children.get(1), row, div_precision_increment)?;
            Some(interval_reformat_string(
                &String::from_utf8_lossy(&raw),
                unit,
            ))
        }
        IntervalArg::Int => {
            let value = eval_expr(children.get(1)?, row, div_precision_increment)
                .ok()
                .flatten()?;
            Some(value.to_string())
        }
        IntervalArg::Real => {
            let value = eval_real(children.get(1), row, div_precision_increment)?;
            Some(format!("{value}"))
        }
        IntervalArg::Decimal => {
            let value = eval_decimal(children.get(1), row, div_precision_increment)?;
            Some(interval_reformat_decimal_text(&value.to_string(), unit))
        }
    }
}

/// The date operand parsed per its channel for the string-answer forms
/// (Go `getDateFromString`/`getDateFromInt`/`getDateFromReal`/
/// `getDateFromDecimal`): a pure-date string stays a date unless the
/// unit carries a clock, and numeric sources widen the same way.
fn add_sub_date_operand(
    children: &[SimpleExpr],
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
    kind: DateArithArg,
    unit: &str,
) -> Option<tidb_datatype::Time> {
    use tidb_datatype::TimeType;
    let zone = utc_anchor();
    let clock = tidb_datatype::is_clock_unit(unit);
    match kind {
        DateArithArg::String => {
            let raw = eval_bytes(children.first(), row, div_precision_increment)?;
            let text = String::from_utf8_lossy(&raw).into_owned();
            let kind = if !tidb_datatype::is_date_format(&text) || clock {
                TimeType::DateTime
            } else {
                TimeType::Date
            };
            tidb_datatype::parse_time(&text, kind, 6, false, false, false, &zone)
                .ok()
                .map(|parsed| parsed.time)
        }
        DateArithArg::Int => {
            let value = eval_expr(children.first()?, row, div_precision_increment)
                .ok()
                .flatten()?;
            let mut date = tidb_datatype::parse_time_from_int64(
                i64::try_from(value).ok()?,
                false,
                false,
                &zone,
            )
            .ok()?;
            if clock {
                date.set_kind(TimeType::DateTime);
            }
            Some(date)
        }
        DateArithArg::Real => {
            let value = eval_real(children.first(), row, div_precision_increment)?;
            let mut date =
                tidb_datatype::parse_time_from_float64(value, false, false, &zone).ok()?;
            if clock {
                date.set_kind(TimeType::DateTime);
            }
            Some(date)
        }
        DateArithArg::Decimal => {
            let value = eval_decimal(children.first(), row, div_precision_increment)?;
            let mut date =
                tidb_datatype::parse_time_from_decimal(&value, false, false, &zone).ok()?;
            if clock {
                date.set_kind(TimeType::DateTime);
            }
            Some(date)
        }
        // The typed temporal operands answer through the time and
        // duration channels instead.
        _ => None,
    }
}

/// Go `baseDateArithmetical.add`/`sub` -> `addDate`: the interval text
/// decomposes into calendar and sub-day parts (`ParseDurationValue`),
/// the sub-day nanoseconds shift first, and the calendar fields move
/// with MySQL's month clamping (`types.AddDate`). `SubDate` negates all
/// four parts. Overflow answers NULL here where Go raises the datetime
/// function overflow error.
fn add_sub_time(
    mut date: tidb_datatype::Time,
    subtract: bool,
    unit: &str,
    interval: &str,
) -> Option<tidb_datatype::Time> {
    let parsed = tidb_datatype::parse_duration_value(unit, interval).ok()?;
    let sign: i64 = if subtract { -1 } else { 1 };
    let mut core = date.core_time();
    core = core.add_duration(sign * parsed.nanoseconds);
    core = core
        .add_date(
            sign * parsed.years,
            sign * parsed.months,
            sign * parsed.days,
        )
        .ok()?;
    date.set_core_time(core);
    Some(date)
}

fn eval_duration(
    expr: Option<&SimpleExpr>,
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
) -> Option<tidb_datatype::MySqlDuration> {
    use tidb_datatype::Datum;
    let expr = expr?;
    match expr {
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::Duration(value)) => Some(*value),
            Some(Datum::Time(value)) => value.to_duration().ok(),
            _ => None,
        },
        SimpleExpr::Func(SimpleSig::CastTimeAsDuration, children) => {
            eval_time(children.first(), row, div_precision_increment)
                .and_then(|time| time.to_duration().ok())
        }
        // DATE_ADD/DATE_SUB over a duration column answers a duration
        // (Go `builtinAddSubDateDurationAnySig.evalDuration`): the
        // interval text extracts to a duration that shifts the source.
        SimpleExpr::Func(
            SimpleSig::AddSubDate {
                subtract,
                date: DateArithArg::Duration,
                interval: interval_kind,
                datetime_result: false,
            },
            children,
        ) => {
            let duration = eval_duration(children.first(), row, div_precision_increment)?;
            let unit_raw = eval_bytes(children.get(2), row, div_precision_increment)?;
            let unit = String::from_utf8_lossy(&unit_raw).into_owned();
            let interval = interval_text(
                children,
                row,
                div_precision_increment,
                *interval_kind,
                &unit,
            )?;
            let delta = tidb_datatype::extract_duration_value(&unit, &interval).ok()?;
            if *subtract {
                duration.checked_sub(delta).ok()
            } else {
                duration.checked_add(delta).ok()
            }
        }
        // The duration-answer casts: Go's `NumberToDuration` reads
        // integer digits as HHMMSS and text goes through `ParseDuration`
        // (the truncated-wrong-value folds answer NULL); the identity
        // passes through. `CastJsonAsDuration` stays refused.
        SimpleExpr::Func(
            sig @ (SimpleSig::CastIntAsDuration
            | SimpleSig::CastRealAsDuration
            | SimpleSig::CastDecimalAsDuration
            | SimpleSig::CastStringAsDuration
            | SimpleSig::CastDurationAsDuration),
            children,
        ) => {
            let to_duration = |parsed: tidb_datatype::ParsedDuration| {
                tidb_datatype::MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp())
                    .ok()
            };
            match sig {
                SimpleSig::CastIntAsDuration => children
                    .first()
                    .and_then(|c| eval_expr(c, row, div_precision_increment).ok())
                    .flatten()
                    .and_then(|value| i64::try_from(value).ok())
                    .and_then(|value| {
                        tidb_datatype::number_to_duration(value, 6)
                            .ok()
                            .map(|converted| converted.value)
                    }),
                SimpleSig::CastRealAsDuration => {
                    // Go formats shortest-'f', then `ParseDuration`.
                    let value = eval_real(children.first(), row, div_precision_increment)?;
                    to_duration(
                        tidb_datatype::parse_duration(format!("{value}").as_bytes(), 6).ok()?,
                    )
                }
                SimpleSig::CastDecimalAsDuration => {
                    let value = eval_decimal(children.first(), row, div_precision_increment)?;
                    to_duration(
                        tidb_datatype::parse_duration(value.to_string().as_bytes(), 6).ok()?,
                    )
                }
                SimpleSig::CastStringAsDuration => {
                    let raw = eval_bytes(children.first(), row, div_precision_increment)?;
                    to_duration(tidb_datatype::parse_duration(&raw, 6).ok()?)
                }
                _ => eval_duration(children.first(), row, div_precision_increment),
            }
        }
        // Go reads the opaque duration code and parses string contents;
        // other codes answer NULL under the folded error.
        SimpleExpr::Func(SimpleSig::CastJsonAsDuration, children) => {
            let value = eval_json(children.first(), row, div_precision_increment)?;
            if let Ok(duration) = value.as_duration() {
                return Some(duration);
            }
            let text = value.as_string()?;
            let parsed = tidb_datatype::parse_duration(text, 6).ok()?;
            tidb_datatype::MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp()).ok()
        }
        _ => None,
    }
}

/// The TIME value of one operand of a temporal comparison; see
/// [`eval_decimal`] for why only exact operands reach here.
fn eval_time(
    expr: Option<&SimpleExpr>,
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
) -> Option<tidb_datatype::Time> {
    use tidb_datatype::Datum;
    let expr = expr?;
    match expr {
        SimpleExpr::Time(value) => Some(*value),
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::Time(value)) => Some(*value),
            _ => None,
        },
        // DATE_ADD/DATE_SUB over a datetime column (Go
        // `builtinAddSubDateDatetimeAnySig.evalTime`; the clock-unit and
        // timestamp type normalizations of Go's getter are no-ops over
        // an already-datetime source).
        SimpleExpr::Func(
            SimpleSig::AddSubDate {
                subtract,
                date: DateArithArg::Datetime,
                interval: interval_kind,
                ..
            },
            children,
        ) => {
            let date = eval_time(children.first(), row, div_precision_increment)?;
            let unit_raw = eval_bytes(children.get(2), row, div_precision_increment)?;
            let unit = String::from_utf8_lossy(&unit_raw).into_owned();
            let interval = interval_text(
                children,
                row,
                div_precision_increment,
                *interval_kind,
                &unit,
            )?;
            add_sub_time(date, *subtract, &unit, &interval)
        }
        // The `Duration*Datetime` upstream ids anchor the duration on
        // the current date (`d.ConvertToTime`) -- no stable predicate
        // answers here, so the seam refuses them.
        SimpleExpr::Func(
            SimpleSig::AddSubDate {
                date: DateArithArg::Duration,
                datetime_result: true,
                ..
            },
            _,
        ) => None,
        // The temporal-answer casts compose for the comparison channels:
        // Go parses numeric and text sources with the converters the
        // string-form arithmetic uses. A pure-date text stays a date;
        // anything else widens to datetime. `CastDurationAsTime` (a
        // now-anchored convert) and the JSON sources stay refused.
        SimpleExpr::Func(
            sig @ (SimpleSig::CastIntAsTime
            | SimpleSig::CastRealAsTime
            | SimpleSig::CastDecimalAsTime
            | SimpleSig::CastStringAsTime
            | SimpleSig::CastTimeAsTime),
            children,
        ) => {
            let zone = utc_anchor();
            match sig {
                SimpleSig::CastIntAsTime => children
                    .first()
                    .and_then(|c| eval_expr(c, row, div_precision_increment).ok())
                    .flatten()
                    .and_then(|value| i64::try_from(value).ok())
                    .and_then(|value| {
                        tidb_datatype::parse_time_from_int64(value, false, false, &zone).ok()
                    }),
                SimpleSig::CastRealAsTime => {
                    let value = eval_real(children.first(), row, div_precision_increment)?;
                    tidb_datatype::parse_time_from_float64(value, false, false, &zone).ok()
                }
                SimpleSig::CastDecimalAsTime => {
                    let value = eval_decimal(children.first(), row, div_precision_increment)?;
                    tidb_datatype::parse_time_from_decimal(&value, false, false, &zone).ok()
                }
                SimpleSig::CastStringAsTime => {
                    let raw = eval_bytes(children.first(), row, div_precision_increment)?;
                    let text = String::from_utf8_lossy(&raw).into_owned();
                    let kind = if tidb_datatype::is_date_format(&text) {
                        tidb_datatype::TimeType::Date
                    } else {
                        tidb_datatype::TimeType::DateTime
                    };
                    tidb_datatype::parse_time(&text, kind, 6, false, false, false, &zone)
                        .ok()
                        .map(|parsed| parsed.time)
                }
                _ => eval_time(children.first(), row, div_precision_increment),
            }
        }
        // Go reads the opaque date codes and parses string contents;
        // other codes answer NULL under the folded error.
        SimpleExpr::Func(SimpleSig::CastJsonAsTime, children) => {
            let value = eval_json(children.first(), row, div_precision_increment)?;
            if let Ok(time) = value.as_time(6) {
                return Some(time);
            }
            let text = value.as_string()?;
            let text = String::from_utf8_lossy(text).into_owned();
            let kind = if tidb_datatype::is_date_format(&text) {
                tidb_datatype::TimeType::Date
            } else {
                tidb_datatype::TimeType::DateTime
            };
            tidb_datatype::parse_time(&text, kind, 6, false, false, false, &utc_anchor())
                .ok()
                .map(|parsed| parsed.time)
        }
        _ => None,
    }
}

/// Go `conv` (pkg/expression/builtin_math.go): re-read `text` in base
/// `from_base` and re-format the value in base `to_base`. A negative
/// base marks the input (`from_base`) or the output (`to_base`) as
/// signed; bases outside [2, 36] answer NULL. Parse overflow answers
/// NULL here where Go raises the BIGINT UNSIGNED 1690 error -- the
/// bytes channel carries no error (same folding as `IntDivideDecimal`).
fn conv_convert(text: &[u8], from_base: i64, to_base: i64) -> Option<Vec<u8>> {
    let mut from_base = from_base;
    let mut to_base = to_base;
    let mut signed = false;
    let mut ignore_sign = false;
    if from_base < 0 {
        from_base = -from_base;
        signed = true;
    }
    if to_base < 0 {
        to_base = -to_base;
        ignore_sign = true;
    }
    if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base) {
        return None;
    }
    // Go trims whitespace, then keeps the longest prefix valid in the
    // input base; an empty prefix answers "0".
    let raw = String::from_utf8_lossy(text);
    let prefix = valid_prefix(raw.trim(), from_base);
    if prefix.is_empty() {
        return Some(b"0".to_vec());
    }
    let negative = prefix.starts_with('-');
    let digits = if negative { &prefix[1..] } else { &prefix };
    let mut val = u64::from_str_radix(digits, from_base as u32).ok()?;
    const TWO_POW_63: u64 = 1u64 << 63;
    if signed {
        if negative && val > TWO_POW_63 {
            val = TWO_POW_63;
        }
        if !negative && val > i64::MAX as u64 {
            val = i64::MAX as u64;
        }
    }
    if negative {
        val = val.wrapping_neg();
    }
    let negative = (val as i64) < 0;
    if ignore_sign && negative {
        val = val.wrapping_neg();
    }
    let mut out = format_u64_base(val, to_base as u32);
    if negative && ignore_sign {
        out.insert(0, '-');
    }
    Some(out.to_ascii_uppercase().into_bytes())
}

/// Go `getValidPrefix` (pkg/expression/util.go): the longest prefix of
/// `s` that parses in `base` (2..=36). A sign counts only at offset 0
/// and never extends the prefix by itself; the first character beyond
/// the base's digit range stops the scan. A leading '+' followed by at
/// least one digit is stripped.
fn valid_prefix(s: &str, base: i64) -> String {
    let upper = if base <= 9 {
        b'0' + base as u8
    } else {
        b'A' + (base - 10) as u8
    };
    let bytes = s.as_bytes();
    let mut valid_len = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        if b.is_ascii_alphanumeric() {
            let c = b.to_ascii_uppercase();
            if c >= upper {
                break;
            }
            valid_len = i + 1;
        } else if (b == b'+' || b == b'-') && i == 0 {
            // A sign is only valid at offset 0.
        } else {
            break;
        }
    }
    if valid_len > 1 && bytes[0] == b'+' {
        return s[1..valid_len].to_string();
    }
    s[..valid_len].to_string()
}

/// Go `strconv.FormatUint` over bases 2..=36: lowercase digits, no sign.
fn format_u64_base(mut val: u64, base: u32) -> String {
    if val == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    while val > 0 {
        out.push(DIGITS[(val % u64::from(base)) as usize]);
        val /= u64::from(base);
    }
    out.reverse();
    String::from_utf8(out).expect("ascii digits")
}

fn eval_bytes(
    expr: Option<&SimpleExpr>,
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
) -> Option<Vec<u8>> {
    use tidb_datatype::Datum;
    let expr = expr?;
    match expr {
        SimpleExpr::Bytes(value) => Some(value.clone()),
        SimpleExpr::Column(offset) => match row.get(*offset) {
            Some(Datum::String(value)) => Some(value.bytes().to_vec()),
            Some(Datum::Bytes(value)) => Some(value.clone()),
            _ => None,
        },
        // DATE_FORMAT: Go `builtinDateFormatSig` answers through
        // `Time.DateFormat` over the format layout.
        SimpleExpr::Func(SimpleSig::DateFormatSig, children) => {
            let time = eval_time(children.first(), row, div_precision_increment)?;
            let layout = eval_bytes(children.get(1), row, div_precision_increment)?;
            let layout = String::from_utf8_lossy(&layout).into_owned();
            let formatted = time.date_format(&layout).ok()?;
            Some(formatted.into_bytes())
        }
        // CONV: Go `builtinConvSig` re-reads the text in one base and
        // re-formats it in another; the base operands go through the
        // int channel.
        SimpleExpr::Func(SimpleSig::Conv, children) => {
            let text = eval_bytes(children.first(), row, div_precision_increment)?;
            let from_base = children
                .get(1)
                .and_then(|c| eval_expr(c, row, div_precision_increment).ok())
                .flatten()
                .and_then(|value| i64::try_from(value).ok())?;
            let to_base = children
                .get(2)
                .and_then(|c| eval_expr(c, row, div_precision_increment).ok())
                .flatten()
                .and_then(|value| i64::try_from(value).ok())?;
            conv_convert(&text, from_base, to_base)
        }
        // DATE_ADD/DATE_SUB over a non-temporal source answers the
        // MySQL string form (Go `builtinAddSubDateAsStringSig`): the
        // parsed source moves by the interval and renders per its kind
        // and refit fraction.
        SimpleExpr::Func(sig @ SimpleSig::AddSubDate { .. }, children)
            if add_sub_answers_text(sig) =>
        {
            let unit_raw = eval_bytes(children.get(2), row, div_precision_increment)?;
            let unit = String::from_utf8_lossy(&unit_raw).into_owned();
            let (subtract, date_kind, interval_kind) = match sig {
                SimpleSig::AddSubDate {
                    subtract,
                    date,
                    interval,
                    ..
                } => (*subtract, *date, *interval),
                _ => unreachable!("guarded by add_sub_answers_text"),
            };
            let date =
                add_sub_date_operand(children, row, div_precision_increment, date_kind, &unit)?;
            if date.is_zero() {
                // Go answers NULL under the folded wrong-value error.
                return None;
            }
            let interval =
                interval_text(children, row, div_precision_increment, interval_kind, &unit)?;
            let mut result = add_sub_time(date, subtract, &unit, &interval)?;
            // Go refits the fraction: whole seconds render short.
            let fsp = i64::from(result.core_time().microsecond() != 0) * 6;
            result.set_fsp(fsp).ok()?;
            Some(result.to_string().into_bytes())
        }
        // The `AS CHAR` casts answer their source's text rendering (Go
        // `builtinCast*AsStringSig`): `ProduceStrWithSpecifiedTp` and
        // the binary-type zero padding fold away -- the seam carries no
        // field type.
        SimpleExpr::Func(
            sig @ (SimpleSig::CastIntAsString
            | SimpleSig::CastRealAsString
            | SimpleSig::CastDecimalAsString
            | SimpleSig::CastStringAsString
            | SimpleSig::CastTimeAsString
            | SimpleSig::CastDurationAsString),
            children,
        ) => {
            match sig {
                SimpleSig::CastIntAsString => {
                    let value = children
                        .first()
                        .and_then(|c| eval_expr(c, row, div_precision_increment).ok())
                        .flatten()?;
                    // Go formats by the source's UNSIGNED flag; the i128
                    // carries the sign here. The `TypeYear` "0" -> "0000"
                    // special case folds -- no field type on the wire.
                    let text = i64::try_from(value)
                        .map(|signed| signed.to_string())
                        .unwrap_or_else(|_| format!("{}", value as u64));
                    Some(text.into_bytes())
                }
                // Go `strconv.FormatFloat(val, 'f', -1, 64)`: the
                // shortest decimal form without an exponent -- Rust's
                // `Display` for f64.
                SimpleSig::CastRealAsString => {
                    let value = eval_real(children.first(), row, div_precision_increment)?;
                    Some(format!("{value}").into_bytes())
                }
                SimpleSig::CastDecimalAsString => {
                    let value = eval_decimal(children.first(), row, div_precision_increment)?;
                    Some(value.to_string().into_bytes())
                }
                SimpleSig::CastStringAsString => {
                    eval_bytes(children.first(), row, div_precision_increment)
                }
                SimpleSig::CastTimeAsString => {
                    eval_time(children.first(), row, div_precision_increment)
                        .map(|time| time.to_string().into_bytes())
                }
                SimpleSig::CastDurationAsString => {
                    eval_duration(children.first(), row, div_precision_increment)
                        .map(|duration| duration.to_string().into_bytes())
                }
                _ => None,
            }
        }
        // Go's string-function family: case folding and substring over the
        // byte domain for the binary forms and the rune domain for the
        // UTF8 forms.
        SimpleExpr::Func(
            sig @ (SimpleSig::Lower
            | SimpleSig::LowerUtf8
            | SimpleSig::Upper
            | SimpleSig::UpperUtf8
            | SimpleSig::Substring2Args
            | SimpleSig::Substring2ArgsUtf8
            | SimpleSig::Substring3Args
            | SimpleSig::Substring3ArgsUtf8),
            children,
        ) => {
            let utf8 = matches!(
                sig,
                SimpleSig::LowerUtf8
                    | SimpleSig::UpperUtf8
                    | SimpleSig::Substring2ArgsUtf8
                    | SimpleSig::Substring3ArgsUtf8
            );
            let units = |bytes: &[u8]| -> Vec<char> {
                if utf8 {
                    String::from_utf8_lossy(bytes).chars().collect()
                } else {
                    bytes.iter().map(|b| *b as char).collect()
                }
            };
            let text = eval_bytes(children.first(), row, div_precision_increment)?;
            let is_sub = matches!(
                sig,
                SimpleSig::Substring2Args
                    | SimpleSig::Substring2ArgsUtf8
                    | SimpleSig::Substring3Args
                    | SimpleSig::Substring3ArgsUtf8
            );
            let _ = is_sub;
            if matches!(
                sig,
                SimpleSig::Lower | SimpleSig::LowerUtf8 | SimpleSig::Upper | SimpleSig::UpperUtf8
            ) {
                let units = units(&text);
                let folded: String = match sig {
                    SimpleSig::LowerUtf8 => units.iter().collect::<String>().to_lowercase(),
                    SimpleSig::Lower => units
                        .iter()
                        .map(|c| (*c as u8).to_ascii_lowercase() as char)
                        .collect(),
                    SimpleSig::UpperUtf8 => units.iter().collect::<String>().to_uppercase(),
                    _ => units
                        .iter()
                        .map(|c| (*c as u8).to_ascii_uppercase() as char)
                        .collect(),
                };
                return Some(if utf8 {
                    folded.into_bytes()
                } else {
                    folded.chars().map(|c| c as u8).collect()
                });
            }
            // Go `builtinSubstringSig`: 1-based position; a negative
            // position counts from the end; a negative length takes the
            // rest after the start.
            let pos_arg = children
                .get(1)
                .and_then(|c| eval_expr(c, row, div_precision_increment).ok())?
                .and_then(|value| i64::try_from(value).ok())?;
            let pos = pos_arg;
            let char_units: Vec<char> = units(&text);
            let char_count = if utf8 {
                char_units.len() as i64
            } else {
                text.len() as i64
            };
            let start = if pos > 0 {
                pos - 1
            } else if pos < 0 {
                char_count + pos
            } else {
                return None;
            };
            if start < 0 || start >= char_count {
                return Some(Vec::new());
            }
            let mut end = char_count;
            if matches!(
                sig,
                SimpleSig::Substring3Args | SimpleSig::Substring3ArgsUtf8
            ) {
                let len_arg = children
                    .get(2)
                    .and_then(|c| eval_expr(c, row, div_precision_increment).ok())?
                    .and_then(|value| i64::try_from(value).ok())?;
                let len = len_arg;
                if len < 0 {
                    return Some(Vec::new());
                }
                end = (start + len).min(char_count);
            }
            let picked: String = char_units[start as usize..end as usize].iter().collect();
            if utf8 {
                Some(picked.into_bytes())
            } else {
                Some(picked.chars().map(|c| c as u8).collect())
            }
        }
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
    // Go `RestoreCollationIDIfNeeded` then `GetCharsetInfoByID`. A zero id is
    // the wire's "unset" and would resolve to a collation the column does not
    // have, so it is left alone.
    //
    // Nothing in this handler's filter CONSULTS this: a pushed-down string
    // comparison carries its collation on the expression
    // (`SimpleSig::EqString(collation)`), which is where Go reads it from
    // too. It is carried for fidelity with `fieldTypeFromPBColumn`, so a
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
pub fn eval_expr(
    expr: &SimpleExpr,
    row: &[tidb_datatype::Datum],
    div_precision_increment: i64,
) -> Result<Option<i128>, String> {
    use tidb_datatype::Datum;
    let value = match expr {
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
        SimpleExpr::Func(sig, children) => {
            let child = |i: usize| {
                children
                    .get(i)
                    .map_or(Ok(None), |c| eval_expr(c, row, div_precision_increment))
            };
            return Ok(match sig {
                // Go's integer arithmetic family (`types.AddInt`/`SubUint`/
                // `MulInt`...): a result outside the signature's domain is
                // the request-level 1690 error, whose text names the type
                // (BIGINT vs BIGINT UNSIGNED) and the operation (ADD/
                // SUBTRACT/MULTIPLY). The mixed and forced unsigned
                // pairings additionally reject a negative operand outright:
                // Go converts it through uint64, which wraps into an
                // immediate overflow.
                SimpleSig::PlusInt
                | SimpleSig::PlusIntUnsignedUnsigned
                | SimpleSig::PlusIntUnsignedSigned
                | SimpleSig::PlusIntSignedUnsigned
                | SimpleSig::PlusIntSignedSigned
                | SimpleSig::MinusInt
                | SimpleSig::MinusIntUnsignedUnsigned
                | SimpleSig::MinusIntUnsignedSigned
                | SimpleSig::MinusIntSignedUnsigned
                | SimpleSig::MinusIntSignedSigned
                | SimpleSig::MinusIntForcedUnsignedUnsigned
                | SimpleSig::MinusIntForcedUnsignedSigned
                | SimpleSig::MinusIntForcedSignedUnsigned
                | SimpleSig::MultiplyInt
                | SimpleSig::MultiplyIntUnsigned => {
                    let (left, right) = match (child(0), child(1)) {
                        (Ok(Some(left)), Ok(Some(right))) => (left, right),
                        _ => return Ok(None),
                    };
                    let (name, unsigned) = match sig {
                        SimpleSig::PlusInt => ("ADD", false),
                        SimpleSig::PlusIntUnsignedUnsigned
                        | SimpleSig::PlusIntUnsignedSigned
                        | SimpleSig::PlusIntSignedUnsigned
                        | SimpleSig::PlusIntSignedSigned => ("ADD", true),
                        SimpleSig::MinusInt => ("SUBTRACT", false),
                        SimpleSig::MinusIntUnsignedUnsigned
                        | SimpleSig::MinusIntUnsignedSigned
                        | SimpleSig::MinusIntSignedUnsigned
                        | SimpleSig::MinusIntSignedSigned
                        | SimpleSig::MinusIntForcedUnsignedUnsigned
                        | SimpleSig::MinusIntForcedUnsignedSigned
                        | SimpleSig::MinusIntForcedSignedUnsigned => ("SUBTRACT", true),
                        _ => ("MULTIPLY", matches!(sig, SimpleSig::MultiplyIntUnsigned)),
                    };
                    let mixed_negative = match sig {
                        SimpleSig::PlusIntUnsignedSigned
                        | SimpleSig::MinusIntUnsignedSigned
                        | SimpleSig::MinusIntForcedUnsignedSigned => right < 0,
                        SimpleSig::PlusIntSignedUnsigned
                        | SimpleSig::MinusIntForcedSignedUnsigned => left < 0,
                        _ => false,
                    };
                    let raw = match name {
                        "ADD" => left.checked_add(right),
                        "SUBTRACT" => left.checked_sub(right),
                        _ => left.checked_mul(right),
                    };
                    let (low, high) = if unsigned {
                        (0, u64::MAX as i128)
                    } else {
                        (i64::MIN as i128, i64::MAX as i128)
                    };
                    if mixed_negative
                        || !raw
                            .map(|value| (low..=high).contains(&value))
                            .unwrap_or(false)
                    {
                        return Err(format!(
                            "{domain} value is out of range in '{name}'",
                            domain = if unsigned {
                                "BIGINT UNSIGNED"
                            } else {
                                "BIGINT"
                            }
                        ));
                    }
                    Some(raw.expect("checked above"))
                }
                // A bare decimal arithmetic as a condition answers its own
                // truth (`ToBool`): non-zero is true, NULL is filtered.
                SimpleSig::PlusDecimal
                | SimpleSig::MinusDecimal
                | SimpleSig::MultiplyDecimal
                | SimpleSig::ModDecimal
                | SimpleSig::DivideDecimal
                | SimpleSig::CastIntAsDecimal
                | SimpleSig::CastRealAsDecimal
                | SimpleSig::CastDecimalAsDecimal
                | SimpleSig::CastStringAsDecimal
                | SimpleSig::CastTimeAsDecimal
                | SimpleSig::CastDurationAsDecimal => {
                    eval_decimal(Some(expr), row, div_precision_increment)
                        .map(|value| i128::from(!value.is_zero()))
                }
                SimpleSig::ModIntUnsignedUnsigned
                | SimpleSig::ModIntUnsignedSigned
                | SimpleSig::ModIntSignedUnsigned
                | SimpleSig::ModIntSignedSigned => match (child(0)?, child(1)?) {
                    (Some(left), Some(right)) if right != 0 => Some(left % right),
                    _ => None,
                },
                // `types.IntDivide`: truncated division, NULL on a zero
                // divisor, sign of the dividend. The four unsigned-flag
                // pairings divide the same values, so one i128 arm serves;
                // Go's `MinInt / -1` panic is unreachable at this width.
                SimpleSig::IntDivideInt
                | SimpleSig::IntDivideIntUnsignedUnsigned
                | SimpleSig::IntDivideIntUnsignedSigned
                | SimpleSig::IntDivideIntSignedSigned
                | SimpleSig::IntDivideIntSignedUnsigned => match (child(0)?, child(1)?) {
                    (Some(left), Some(right)) if right != 0 => Some(left / right),
                    _ => None,
                },
                SimpleSig::IntDivideDecimal => {
                    // Go `EvalIntDivideDecimal`: `DecimalDiv` then `ToInt` --
                    // the quotient truncated to the integer part, NULL on a
                    // zero divisor. A quotient wider than BIGINT errors in
                    // Go where this seam answers NULL (`div_rem` folds both)
                    // -- a narrowing, not a value change inside BIGINT.
                    let (Some(left), Some(right)) = (
                        eval_decimal(children.first(), row, div_precision_increment),
                        eval_decimal(children.get(1), row, div_precision_increment),
                    ) else {
                        return Ok(None);
                    };
                    let Some((quotient, _)) = left.div_rem(&right) else {
                        return Ok(None);
                    };
                    Some(i128::from(quotient))
                }
                // Go `builtinCast*AsIntSig` under `AS SIGNED`:
                // `ConvertFloatToInt`/decimal truncation -- the REAL source
                // ROUNDS to nearest (half away), and an out-of-BIGINT
                // result is the cast overflow error
                // ("constant %v overflows bigint").
                SimpleSig::CastRealAsInt => {
                    let Some(value) = eval_real(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    let rounded = value.round();
                    if rounded < -9_223_372_036_854_775_808.0
                        || rounded >= 9_223_372_036_854_775_808.0
                    {
                        return Err(format!(
                            "constant {} overflows bigint",
                            go_float_display(rounded)
                        ));
                    }
                    Some(i128::from(rounded as i64))
                }
                SimpleSig::CastDecimalAsInt => {
                    let Some(value) = eval_decimal(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    // `Decimal.ToInt`: truncation toward zero; the warning
                    // flag IS the overflow event.
                    let (truncated, overflow) = value.to_i64_trunc();
                    if overflow.is_some() {
                        return Err(format!(
                            "constant {} overflows bigint",
                            go_float_display(value.to_f64())
                        ));
                    }
                    Some(i128::from(truncated))
                }
                SimpleSig::CastStringAsInt => {
                    // Go `StrToInt`: best-effort prefix conversion; the
                    // truncation warning has no coprocessor sink, garbage
                    // answers 0 and the range saturates to the BIGINT bound
                    // (the non-strict SELECT observable).
                    let Some(raw) = eval_bytes(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    let text = String::from_utf8_lossy(&raw);
                    let Some(prefix) = numeric_prefix(text.trim_start(), false) else {
                        return Ok(Some(0));
                    };
                    let value = prefix.parse::<i64>().unwrap_or_else(|_| {
                        if prefix.starts_with('-') {
                            i64::MIN
                        } else {
                            i64::MAX
                        }
                    });
                    Some(i128::from(value))
                }
                SimpleSig::CastStringAsReal => {
                    // A bare AS REAL cast answers its own truth; the value
                    // flows through `eval_real`'s composition arm.
                    eval_real(Some(expr), row, div_precision_increment)
                        .filter(|value| !value.is_nan())
                        .map(|value| i128::from(value != 0.0))
                }
                SimpleSig::CastIntAsInt => child(0)?,
                SimpleSig::CastIntAsReal
                | SimpleSig::CastDecimalAsReal
                | SimpleSig::CastRealAsReal => {
                    // A bare real cast as a condition answers its own truth
                    // (`ToBool`); the value flows through `eval_real`'s own
                    // composition arms, which early-return on the Result.
                    eval_real(Some(expr), row, div_precision_increment)
                        .filter(|value| !value.is_nan())
                        .map(|value| i128::from(value != 0.0))
                }
                // The AS REAL casts answer their own truth (`ToBool`).
                // A bare real math function as a condition answers its own
                // truth (`ToBool`: non-zero and not-NaN).
                SimpleSig::RoundReal
                | SimpleSig::RoundDec
                | SimpleSig::Pow
                | SimpleSig::Acos
                | SimpleSig::Asin
                | SimpleSig::Atan1Arg
                | SimpleSig::Atan2Args
                | SimpleSig::Cos
                | SimpleSig::Cot
                | SimpleSig::Pi
                | SimpleSig::Sin => eval_real(Some(expr), row, div_precision_increment)
                    .filter(|value| !value.is_nan())
                    .map(|value| i128::from(value != 0.0)),
                SimpleSig::CharLengthUtf8 | SimpleSig::CharLength => {
                    // Go `builtinCharLengthUtf8Sig` counts runes;
                    // `builtinCharLengthBinarySig` counts bytes.
                    let Some(raw) = eval_bytes(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    let count = if matches!(sig, SimpleSig::CharLengthUtf8) {
                        String::from_utf8_lossy(&raw).chars().count()
                    } else {
                        raw.len()
                    };
                    Some(i128::from(count as u64))
                }
                SimpleSig::Lower
                | SimpleSig::LowerUtf8
                | SimpleSig::Upper
                | SimpleSig::UpperUtf8
                | SimpleSig::Substring2Args
                | SimpleSig::Substring2ArgsUtf8
                | SimpleSig::Substring3Args
                | SimpleSig::Substring3ArgsUtf8
                | SimpleSig::Conv
                | SimpleSig::CastIntAsString
                | SimpleSig::CastRealAsString
                | SimpleSig::CastDecimalAsString
                | SimpleSig::CastStringAsString
                | SimpleSig::CastTimeAsString
                | SimpleSig::CastDurationAsString => {
                    // A bare string function (or CONV) as a condition
                    // answers its `ToBool` truth: the numeric prefix of
                    // the result, non-zero (Go `StrToFloat` -> `ToBool`).
                    let Some(raw) = eval_bytes(Some(expr), row, div_precision_increment) else {
                        return Ok(None);
                    };
                    let text = String::from_utf8_lossy(&raw);
                    let numeric = numeric_prefix(text.trim_start(), true).unwrap_or_default();
                    let value: f64 = numeric.parse().unwrap_or(0.0);
                    Some(i128::from(value != 0.0))
                }
                SimpleSig::AddSubDate { .. } => {
                    // A bare date arithmetic as a condition answers its
                    // own non-NULL truth (Go `ToBool` over the answer;
                    // "2000-01-03" reads as 2000 -> true).
                    let answered = match sig {
                        SimpleSig::AddSubDate {
                            date: DateArithArg::Duration,
                            datetime_result: false,
                            ..
                        } => eval_duration(Some(expr), row, div_precision_increment).is_some(),
                        SimpleSig::AddSubDate {
                            date: DateArithArg::Datetime,
                            ..
                        } => eval_time(Some(expr), row, div_precision_increment).is_some(),
                        _ => eval_bytes(Some(expr), row, div_precision_increment).is_some(),
                    };
                    Some(i128::from(answered))
                }
                SimpleSig::DateFormatSig => {
                    // A bare formatted date as a condition answers its
                    // numeric-prefix truth ("20240305" -> truthy).
                    match children.first().map(|c| eval_datum(c, row)) {
                        Some(Ok(datum)) => {
                            Some(i128::from(!matches!(datum, tidb_datatype::Datum::Null)))
                        }
                        Some(Err(message)) => return Err(message),
                        None => Some(0),
                    }
                }
                SimpleSig::RoundInt => child(0)?,
                SimpleSig::LtReal
                | SimpleSig::LeReal
                | SimpleSig::GtReal
                | SimpleSig::GeReal
                | SimpleSig::EqReal
                | SimpleSig::NeReal => {
                    let (Some(left), Some(right)) = (
                        eval_real(children.first(), row, div_precision_increment),
                        eval_real(children.get(1), row, div_precision_increment),
                    ) else {
                        return Ok(None);
                    };
                    let ordering = left.total_cmp(&right);
                    let truth = match sig {
                        SimpleSig::LtReal => ordering.is_lt(),
                        SimpleSig::LeReal => ordering.is_le(),
                        SimpleSig::GtReal => ordering.is_gt(),
                        SimpleSig::GeReal => ordering.is_ge(),
                        SimpleSig::EqReal => ordering.is_eq(),
                        _ => !ordering.is_eq(),
                    };
                    Some(i128::from(truth))
                }
                SimpleSig::PlusReal
                | SimpleSig::MinusReal
                | SimpleSig::MultiplyReal
                | SimpleSig::DivideReal
                | SimpleSig::ModReal
                | SimpleSig::CastJsonAsReal => {
                    // A bare real arithmetic as a condition answers its own
                    // truth (`ToBool`): non-zero and not-NaN is true, NULL
                    // is filtered. MySQL's `ToBool` treats NaN as 0.
                    eval_real(Some(expr), row, div_precision_increment)
                        .filter(|value| !value.is_nan())
                        .map(|value| i128::from(value != 0.0))
                }
                SimpleSig::LtInt
                | SimpleSig::LeInt
                | SimpleSig::GtInt
                | SimpleSig::GeInt
                | SimpleSig::EqInt
                | SimpleSig::NeInt => {
                    let (left, right) = match (child(0), child(1)) {
                        (Ok(Some(left)), Ok(Some(right))) => (left, right),
                        _ => return Ok(None),
                    };
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
                    let (Some(left), Some(right)) = (
                        eval_bytes(children.first(), row, div_precision_increment),
                        eval_bytes(children.get(1), row, div_precision_increment),
                    ) else {
                        return Ok(None);
                    };
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
                SimpleSig::LtDecimal
                | SimpleSig::LeDecimal
                | SimpleSig::GtDecimal
                | SimpleSig::GeDecimal
                | SimpleSig::EqDecimal
                | SimpleSig::NeDecimal => {
                    let (Some(left), Some(right)) = (
                        eval_decimal(children.first(), row, div_precision_increment),
                        eval_decimal(children.get(1), row, div_precision_increment),
                    ) else {
                        return Ok(None);
                    };
                    let ordering = left.cmp(&right);
                    let truth = match sig {
                        SimpleSig::LtDecimal => ordering.is_lt(),
                        SimpleSig::LeDecimal => ordering.is_le(),
                        SimpleSig::GtDecimal => ordering.is_gt(),
                        SimpleSig::GeDecimal => ordering.is_ge(),
                        SimpleSig::EqDecimal => ordering.is_eq(),
                        _ => !ordering.is_eq(),
                    };
                    Some(i128::from(truth))
                }
                SimpleSig::LtTime
                | SimpleSig::LeTime
                | SimpleSig::GtTime
                | SimpleSig::GeTime
                | SimpleSig::EqTime
                | SimpleSig::NeTime => {
                    let (Some(left), Some(right)) = (
                        eval_time(children.first(), row, div_precision_increment),
                        eval_time(children.get(1), row, div_precision_increment),
                    ) else {
                        return Ok(None);
                    };
                    let ordering = left.compare(right);
                    let truth = match sig {
                        SimpleSig::LtTime => ordering.is_lt(),
                        SimpleSig::LeTime => ordering.is_le(),
                        SimpleSig::GtTime => ordering.is_gt(),
                        SimpleSig::GeTime => ordering.is_ge(),
                        SimpleSig::EqTime => ordering.is_eq(),
                        _ => !ordering.is_eq(),
                    };
                    Some(i128::from(truth))
                }
                SimpleSig::NeString(collation) => {
                    let (Some(left), Some(right)) = (
                        eval_bytes(children.first(), row, div_precision_increment),
                        eval_bytes(children.get(1), row, div_precision_increment),
                    ) else {
                        return Ok(None);
                    };
                    let equal = tidb_datatype::get_collator_by_id(*collation)
                        .compare(&left, &right)
                        .is_eq();
                    Some(i128::from(!equal))
                }
                SimpleSig::LogicalAnd => {
                    // MySQL: FALSE dominates NULL.
                    let (left, right) = (child(0)?, child(1)?);
                    match (left, right) {
                        (Some(0), _) | (_, Some(0)) => Some(0),
                        (Some(_), Some(_)) => Some(1),
                        _ => None,
                    }
                }
                SimpleSig::LogicalOr => {
                    // MySQL: TRUE dominates NULL.
                    let (left, right) = (child(0)?, child(1)?);
                    match (left, right) {
                        (Some(l), _) if l != 0 => Some(1),
                        (_, Some(r)) if r != 0 => Some(1),
                        (Some(_), Some(_)) => Some(0),
                        _ => None,
                    }
                }
                SimpleSig::UnaryNot => child(0)?.map(|v| i128::from(v == 0)),
                SimpleSig::IntIsNull => Some(i128::from(child(0)?.is_none())),
                SimpleSig::VectorFloat32IsNull => {
                    // The vector leaf answers NULL only through its datum.
                    match children.first().map(|c| eval_datum(c, row)) {
                        Some(Ok(datum)) => {
                            Some(i128::from(matches!(datum, tidb_datatype::Datum::Null)))
                        }
                        Some(Err(message)) => return Err(message),
                        None => Some(1),
                    }
                }
                SimpleSig::InString(collation) => {
                    // Go `builtinInStringSig`: TRUE on any match under the
                    // collation; otherwise NULL if the tested value or any
                    // element was NULL, FALSE otherwise.
                    let tested = eval_bytes(children.first(), row, div_precision_increment);
                    let mut saw_null = tested.is_none();
                    for index in 1..children.len() {
                        let element = eval_bytes(children.get(index), row, div_precision_increment);
                        match (&tested, &element) {
                            (Some(left), Some(right)) => {
                                if tidb_datatype::get_collator_by_id(*collation)
                                    .compare(left, right)
                                    .is_eq()
                                {
                                    return Ok(Some(1));
                                }
                            }
                            _ => saw_null = true,
                        }
                    }
                    if saw_null {
                        None
                    } else {
                        Some(0)
                    }
                }
                SimpleSig::WeekWithoutMode => {
                    let Some(time) = eval_time(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    Some(i128::from(time.core_time().week(0)))
                }
                SimpleSig::DecimalIsNull
                | SimpleSig::DurationIsNull
                | SimpleSig::RealIsNull
                | SimpleSig::StringIsNull
                | SimpleSig::TimeIsNull => {
                    // Each family's IS NULL inspects its own leaf's DATUM,
                    // not the int truth channel: a present non-NULL datum
                    // answers FALSE even when that kind is not comparable.
                    match children.first().map(|c| eval_datum(c, row)) {
                        Some(Ok(datum)) => {
                            Some(i128::from(matches!(datum, tidb_datatype::Datum::Null)))
                        }
                        Some(Err(message)) => return Err(message),
                        None => Some(1),
                    }
                }
                // Go's temporal extraction family: `builtinDateSig` truncates
                // a DATETIME to its date part, the clock signatures read the
                // DURATION channel (`CastTimeAsDuration` adapts a DATETIME
                // column), `Month` reads the calendar month, and `DateDiff`
                // answers the day difference between two dates.
                SimpleSig::CastTimeAsDuration => {
                    let Some(duration) =
                        eval_duration(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    Some(i128::from(duration.nanoseconds() != 0))
                }
                SimpleSig::Date => {
                    let Some(time) = eval_time(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    let core = time.core_time();
                    let Some(date) = tidb_datatype::Time::new(
                        tidb_datatype::CoreTime::from_date(
                            core.year() as u16,
                            core.month(),
                            core.day(),
                            0,
                            0,
                            0,
                            0,
                        ),
                        tidb_datatype::TimeType::Date,
                        0,
                    )
                    .ok() else {
                        return Ok(None);
                    };
                    // A bare date as a condition answers ToBool of its
                    // numeric form; comparisons read it as a decimal.
                    Some(i128::from(!date.to_number().is_zero()))
                }
                SimpleSig::Hour => {
                    let Some(duration) =
                        eval_duration(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    Some(i128::from(duration.hour()))
                }
                SimpleSig::Minute => {
                    let Some(duration) =
                        eval_duration(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    Some(i128::from(duration.minute()))
                }
                SimpleSig::Second => {
                    let Some(duration) =
                        eval_duration(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    Some(i128::from(duration.second()))
                }
                SimpleSig::MicroSecond => {
                    let Some(duration) =
                        eval_duration(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    Some(i128::from(duration.microsecond()))
                }
                SimpleSig::Month => {
                    let Some(time) = eval_time(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    Some(i128::from(time.core_time().month()))
                }
                SimpleSig::DateDiff => {
                    let (Some(left), Some(right)) = (
                        eval_time(children.first(), row, div_precision_increment),
                        eval_time(children.get(1), row, div_precision_increment),
                    ) else {
                        return Ok(None);
                    };
                    // DATEDIFF truncates both sides to their date parts and
                    // answers days from the second to the first.
                    let to_date = |time: tidb_datatype::Time| {
                        let core = time.core_time();
                        tidb_datatype::Time::new(
                            tidb_datatype::CoreTime::from_date(
                                core.year() as u16,
                                core.month(),
                                core.day(),
                                0,
                                0,
                                0,
                                0,
                            ),
                            tidb_datatype::TimeType::Date,
                            0,
                        )
                        .ok()
                    };
                    let Some(first) = to_date(left) else {
                        return Ok(None);
                    };
                    let Some(second) = to_date(right) else {
                        return Ok(None);
                    };
                    Some(i128::from(second.core_time().timestamp_diff(
                        first.core_time(),
                        tidb_datatype::TimestampInterval::Day,
                    )))
                }
                SimpleSig::TimestampDiff => {
                    let Some(unit_raw) = eval_bytes(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    let (Some(first), Some(second)) = (
                        eval_time(children.get(1), row, div_precision_increment),
                        eval_time(children.get(2), row, div_precision_increment),
                    ) else {
                        return Ok(None);
                    };
                    if first.is_zero() || second.is_zero() {
                        // Go: `InvalidZero` on either side answers NULL
                        // under the folded wrong-value error.
                        return Ok(None);
                    }
                    // Go compares the unit raw (the parser always emits
                    // the uppercase keyword); unknown units answer 0.
                    let interval = match unit_raw.as_slice() {
                        b"YEAR" => Some(tidb_datatype::TimestampInterval::Year),
                        b"QUARTER" => Some(tidb_datatype::TimestampInterval::Quarter),
                        b"MONTH" => Some(tidb_datatype::TimestampInterval::Month),
                        b"WEEK" => Some(tidb_datatype::TimestampInterval::Week),
                        b"DAY" => Some(tidb_datatype::TimestampInterval::Day),
                        b"HOUR" => Some(tidb_datatype::TimestampInterval::Hour),
                        b"MINUTE" => Some(tidb_datatype::TimestampInterval::Minute),
                        b"SECOND" => Some(tidb_datatype::TimestampInterval::Second),
                        b"MICROSECOND" => Some(tidb_datatype::TimestampInterval::Microsecond),
                        _ => None,
                    };
                    let Some(interval) = interval else {
                        return Ok(Some(0));
                    };
                    Some(i128::from(
                        first
                            .core_time()
                            .timestamp_diff(second.core_time(), interval),
                    ))
                }
                SimpleSig::JsonMemberOfSig => {
                    // Go `builtinJSONMemberOfSig.evalInt`: the target (any
                    // scalar, coerced through `CreateBinaryJSON`) equals the
                    // doc, or equals ANY element of an ARRAY doc.
                    let target = match children.first().map(|c| eval_datum(c, row)) {
                        Some(Ok(datum)) => {
                            // A NULL datum is SQL NULL: the answer is NULL,
                            // not a JSON `null` document.
                            if matches!(datum, tidb_datatype::Datum::Null) {
                                return Ok(None);
                            }
                            match datum_to_json_value(&datum) {
                                Some(value) => {
                                    match tidb_datatype::BinaryJSON::from_typed_value(&value) {
                                        Ok(json) => json,
                                        Err(error) => return Err(error.to_string()),
                                    }
                                }
                                None => {
                                    return Err(
                                        "this MEMBER OF target kind is a later course".to_owned()
                                    )
                                }
                            }
                        }
                        Some(Err(message)) => return Err(message),
                        None => return Ok(None),
                    };
                    let Some(obj) = eval_json(children.get(1), row, div_precision_increment) else {
                        return Ok(None);
                    };
                    let member = if obj.type_code() == tidb_datatype::JSON_TYPE_CODE_ARRAY {
                        match obj.element_count() {
                            Ok(count) => (0..count).any(|index| {
                                obj.array_get(index).ok().flatten().is_some_and(|element| {
                                    tidb_datatype::compare_binary_json(&element, &target).is_eq()
                                })
                            }),
                            Err(_) => return Err("invalid json array".to_owned()),
                        }
                    } else {
                        tidb_datatype::compare_binary_json(&obj, &target).is_eq()
                    };
                    Some(i128::from(member))
                }
                SimpleSig::Like(collation) => {
                    // Go `builtinLikeSig`: (target, pattern, escape). Case
                    // handling follows the comparison's collation -- `_ci`
                    // folds both sides before the wildcard match, `_bin` is
                    // exact (Go folds through the collator's weights; the
                    // fold here is ASCII-lowercase, exact for the common
                    // ASCII pattern families).
                    let target = eval_bytes(children.first(), row, div_precision_increment);
                    let pattern = eval_bytes(children.get(1), row, div_precision_increment);
                    let escape = eval_bytes(children.get(2), row, div_precision_increment);
                    let (Some(target), Some(pattern), Some(escape)) = (target, pattern, escape)
                    else {
                        return Ok(None);
                    };
                    let collator = tidb_datatype::get_collator_by_id(*collation);
                    let fold = |bytes: &[u8]| -> String {
                        let text = std::str::from_utf8(bytes).unwrap_or_default();
                        if collator.compare(b"a", b"A").is_eq() {
                            text.to_lowercase()
                        } else {
                            text.to_owned()
                        }
                    };
                    let escape_char = fold(&escape).chars().next().unwrap_or('\\');
                    Some(i128::from(tidb_datatype::like_matches(
                        &fold(&target),
                        &fold(&pattern),
                        escape_char,
                    )))
                }
                SimpleSig::InInt => {
                    // `builtinInIntSig.evalInt`: TRUE on any match; otherwise
                    // NULL if the tested value or any element was NULL.
                    let tested = child(0)?;
                    let mut saw_null = tested.is_none();
                    for index in 1..children.len() {
                        match (tested, child(index)?) {
                            (Some(left), Some(right)) if left == right => return Ok(Some(1)),
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
                SimpleSig::CastJsonAsInt => {
                    // Go `ConvertJSONToInt64`: numbers truncate, strings
                    // take the integer prefix, other codes answer 0
                    // under the folded error. The `json.Number` literal
                    // code folds to 0 here (no text accessor on the
                    // trimmed build).
                    let Some(value) = eval_json(children.first(), row, div_precision_increment)
                    else {
                        return Ok(None);
                    };
                    if let Some(signed) = value.as_i64() {
                        Some(i128::from(signed))
                    } else if let Some(unsigned) = value.as_u64() {
                        Some(i128::from(unsigned))
                    } else if let Some(real) = value.as_f64() {
                        Some(real as i128)
                    } else if let Some(text) = value.as_string() {
                        let text = String::from_utf8_lossy(text);
                        let prefix = numeric_prefix(text.trim_start(), false).unwrap_or_default();
                        Some(prefix.parse::<i64>().unwrap_or(0) as i128)
                    } else {
                        Some(0)
                    }
                }
                SimpleSig::CastIntAsJson
                | SimpleSig::CastRealAsJson
                | SimpleSig::CastDecimalAsJson
                | SimpleSig::CastStringAsJson
                | SimpleSig::CastTimeAsJson
                | SimpleSig::CastDurationAsJson
                | SimpleSig::CastJsonAsJson => {
                    // A bare JSON cast as a condition answers its own
                    // non-NULL truth (Go `ToBool` over the rendering).
                    let answered = eval_json(Some(expr), row, div_precision_increment).is_some();
                    Some(i128::from(answered))
                }
                SimpleSig::CastIntAsTime
                | SimpleSig::CastRealAsTime
                | SimpleSig::CastDecimalAsTime
                | SimpleSig::CastStringAsTime
                | SimpleSig::CastTimeAsTime
                | SimpleSig::CastIntAsDuration
                | SimpleSig::CastRealAsDuration
                | SimpleSig::CastDecimalAsDuration
                | SimpleSig::CastStringAsDuration
                | SimpleSig::CastDurationAsDuration
                | SimpleSig::CastJsonAsTime
                | SimpleSig::CastJsonAsDuration => {
                    // A bare temporal cast as a condition answers its own
                    // non-NULL truth (Go `ToBool` over the rendering).
                    let answered = match sig {
                        SimpleSig::CastIntAsTime
                        | SimpleSig::CastRealAsTime
                        | SimpleSig::CastDecimalAsTime
                        | SimpleSig::CastStringAsTime
                        | SimpleSig::CastTimeAsTime
                        | SimpleSig::CastJsonAsTime => {
                            eval_time(Some(expr), row, div_precision_increment).is_some()
                        }
                        _ => eval_duration(Some(expr), row, div_precision_increment).is_some(),
                    };
                    Some(i128::from(answered))
                }
            });
        }
    };
    Ok(value)
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
        assert_eq!(eval_expr(&with_null, &row_int, 4).expect("evals"), Some(1));
        // A miss with a NULL element is NULL, not FALSE.
        let miss_with_null = in_list(vec![SimpleExpr::Null, SimpleExpr::Int(7)]);
        assert_eq!(
            eval_expr(&miss_with_null, &row_int, 4).expect("evals"),
            None
        );
        // A plain miss is FALSE.
        let plain_miss = in_list(vec![SimpleExpr::Int(7), SimpleExpr::Int(8)]);
        assert_eq!(eval_expr(&plain_miss, &row_int, 4).expect("evals"), Some(0));
        // A NULL tested value never answers TRUE or FALSE.
        let any = in_list(vec![SimpleExpr::Int(300)]);
        assert_eq!(eval_expr(&any, &row_null, 4).expect("evals"), None);
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
            eval_expr(&converted, &[Datum::new_string(value.to_owned())], 4).expect("evals")
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
            eval_expr(&and(SimpleExpr::Null, SimpleExpr::Int(0)), &row, 4).expect("evals"),
            Some(0)
        );
        assert_eq!(
            eval_expr(&and(SimpleExpr::Null, SimpleExpr::Int(1)), &row, 4).expect("evals"),
            None
        );
        // NULL OR TRUE = TRUE; NULL OR FALSE = NULL.
        assert_eq!(
            eval_expr(&or(SimpleExpr::Null, SimpleExpr::Int(1)), &row, 4).expect("evals"),
            Some(1)
        );
        assert_eq!(
            eval_expr(&or(SimpleExpr::Null, SimpleExpr::Int(0)), &row, 4).expect("evals"),
            None
        );
        // NOT NULL = NULL; IS NULL answers over null.
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(SimpleSig::UnaryNot, vec![SimpleExpr::Null]),
                &row,
                4
            )
            .expect("evals"),
            None
        );
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(SimpleSig::IntIsNull, vec![SimpleExpr::Null]),
                &row,
                4
            )
            .expect("evals"),
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
            top_n: Some(tipb::TopN {
                order_by: vec![tipb::ByItem {
                    expr: Some(tipb::Expr {
                        tp: Some(tipb::ExprType::ColumnRef as i32),
                        val: Some(key_offset),
                        ..tipb::Expr::default()
                    }),
                    desc: Some(desc),
                }],
                limit: Some(limit),
            }),
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
        let select = tipb::SelectResponse::decode(resp.data.as_slice()).expect("decodes");
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
        let select = tipb::SelectResponse::decode(resp.data.as_slice()).expect("decodes");
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

    #[test]
    fn integer_mod_follows_mysql() {
        use tidb_datatype::Datum;
        let row = [Datum::Int(-7)];
        let column = SimpleExpr::Column(0);
        let two = SimpleExpr::Int(2);
        // `types.ModInt`: the truncated remainder, sign of the DIVIDEND.
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(SimpleSig::ModIntSignedSigned, vec![column.clone(), two]),
                &row,
                4
            )
            .expect("evals"),
            Some(-1)
        );
        // A zero divisor answers NULL (`ErrDivByZero` -> isNull).
        let zero = SimpleExpr::Int(0);
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(SimpleSig::ModIntUnsignedSigned, vec![column, zero]),
                &row,
                4
            )
            .expect("evals"),
            None
        );
    }

    #[test]
    fn decimal_arithmetic_composes_into_conditions() {
        use tidb_datatype::{Datum, Decimal};
        // c + (-1) > 0 keeps only c > 1: the decimal arithmetic evaluates as
        // the comparison's operand, `EvalPlusDecimal` -> `GetAccurateCmpType`.
        let condition = SimpleExpr::Func(
            SimpleSig::GtDecimal,
            vec![
                SimpleExpr::Func(
                    SimpleSig::PlusDecimal,
                    vec![
                        SimpleExpr::Column(0),
                        SimpleExpr::Decimal(Decimal::parse_mysql("-1").0),
                    ],
                ),
                SimpleExpr::Decimal(Decimal::parse_mysql("0").0),
            ],
        );
        let row = [Datum::Decimal(Decimal::parse_mysql("2.5").0)];
        assert_eq!(eval_expr(&condition, &row, 4).expect("evals"), Some(1));
        let row_small = [Datum::Decimal(Decimal::parse_mysql("1").0)];
        assert_eq!(
            eval_expr(&condition, &row_small, 4).expect("evals"),
            Some(0)
        );
    }

    #[test]
    fn real_comparisons_and_arithmetic_follow_mysql() {
        use tidb_datatype::Datum;
        let row = [Datum::Real(2.5)];
        let column = SimpleExpr::Column(0);
        let literal = SimpleExpr::Real(1.5);
        // `x > 1.5` arrives as GTReal after `GetAccurateCmpType` answers
        // ETReal; the comparison is binary64.
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(SimpleSig::GtReal, vec![column.clone(), literal]),
                &row,
                4
            )
            .expect("evals"),
            Some(1)
        );
        // Arithmetic composes: `x / 2 + 1` is 2.25, truthy as a condition.
        let arithmetic = SimpleExpr::Func(
            SimpleSig::PlusReal,
            vec![
                SimpleExpr::Func(SimpleSig::DivideReal, vec![column, SimpleExpr::Real(2.0)]),
                SimpleExpr::Real(1.0),
            ],
        );
        assert_eq!(eval_expr(&arithmetic, &row, 4).expect("evals"), Some(1));
        // A zero divisor answers NULL (`EvalDivideReal`).
        let by_zero = SimpleExpr::Func(
            SimpleSig::DivideReal,
            vec![SimpleExpr::Real(1.0), SimpleExpr::Real(0.0)],
        );
        assert_eq!(eval_expr(&by_zero, &row, 4).expect("evals"), None);
    }

    #[test]
    fn a_float64_literal_decodes_go_convert_float() {
        // Go `convertFloat`: `codec.DecodeFloat` -- eight big-endian bits.
        let mut val = Vec::new();
        val.extend_from_slice(&1.5_f64.to_bits().to_be_bytes());
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
    fn integer_division_follows_mysql() {
        use tidb_datatype::{Datum, Decimal};
        let row = [
            Datum::Int(-7),
            Datum::Decimal(Decimal::parse_mysql("-9.5").0),
        ];
        let left = SimpleExpr::Column(0);
        // `-7 DIV 2` truncates toward zero, sign of the dividend.
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(
                    SimpleSig::IntDivideIntSignedSigned,
                    vec![left.clone(), SimpleExpr::Int(2)],
                ),
                &row,
                4
            )
            .expect("evals"),
            Some(-3)
        );
        // A zero divisor answers NULL.
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(
                    SimpleSig::IntDivideIntUnsignedUnsigned,
                    vec![left, SimpleExpr::Int(0)],
                ),
                &row,
                4
            )
            .expect("evals"),
            None
        );
        // Decimal DIV truncates the quotient: `-9.5 DIV 2` = -4. The wire
        // operands are DECIMAL by the time the sig runs (Go casts at build).
        let decimal_left = SimpleExpr::Column(1);
        assert_eq!(
            eval_expr(
                &SimpleExpr::Func(
                    SimpleSig::IntDivideDecimal,
                    vec![
                        decimal_left,
                        SimpleExpr::Decimal(Decimal::parse_mysql("2").0),
                    ],
                ),
                &row,
                4
            )
            .expect("evals"),
            Some(-4)
        );
    }

    #[test]
    fn integer_arithmetic_overflow_answers_gos_1690_text() {
        use tidb_datatype::Datum;
        let row = [Datum::Int(i64::MAX), Datum::Int(1)];
        let overflow = SimpleExpr::Func(
            SimpleSig::PlusInt,
            vec![SimpleExpr::Column(0), SimpleExpr::Column(1)],
        );
        // Go's `types.AddInt` raises the 1690 terror whose text names the
        // type and the operation; the request answers it, not a value.
        assert_eq!(
            eval_expr(&overflow, &row, 4),
            Err("BIGINT value is out of range in 'ADD'".to_owned())
        );
        let row_small = [Datum::Int(2), Datum::Int(3)];
        assert_eq!(eval_expr(&overflow, &row_small, 4), Ok(Some(5)));
    }

    #[test]
    fn unsigned_minus_rejects_negative_operand_like_go() {
        use tidb_datatype::Datum;
        // `MinusIntUnsignedSigned`: Go converts the signed operand through
        // uint64, so a negative operand wraps into an immediate overflow
        // rather than an in-range difference.
        let row = [Datum::UInt(5), Datum::Int(-3)];
        let underflow = SimpleExpr::Func(
            SimpleSig::MinusIntUnsignedSigned,
            vec![SimpleExpr::Column(0), SimpleExpr::Column(1)],
        );
        assert_eq!(
            eval_expr(&underflow, &row, 4),
            Err("BIGINT UNSIGNED value is out of range in 'SUBTRACT'".to_owned())
        );
        let row_ok = [Datum::UInt(5), Datum::Int(3)];
        assert_eq!(eval_expr(&underflow, &row_ok, 4), Ok(Some(2)));
    }

    #[test]
    fn like_follows_collation_case_sensitivity() {
        use tidb_datatype::Datum;
        let row = [Datum::new_string("ABC".to_owned())];
        let like = |collation: i32| {
            SimpleExpr::Func(
                SimpleSig::Like(collation),
                vec![
                    SimpleExpr::Column(0),
                    SimpleExpr::Bytes(b"a%".to_vec()),
                    SimpleExpr::Bytes(b"\\".to_vec()),
                ],
            )
        };
        // utf8mb4_general_ci (45) folds case; utf8mb4_bin (46) does not.
        assert_eq!(eval_expr(&like(45), &row, 4).expect("evals"), Some(1));
        assert_eq!(eval_expr(&like(46), &row, 4).expect("evals"), Some(0));
        // The escape quotes the next pattern char: `100\\%` matches a
        // literal trailing percent.
        let row_percent = [Datum::new_string("100%".to_owned())];
        let escaped = SimpleExpr::Func(
            SimpleSig::Like(46),
            vec![
                SimpleExpr::Column(0),
                SimpleExpr::Bytes(b"100\\%".to_vec()),
                SimpleExpr::Bytes(b"\\".to_vec()),
            ],
        );
        assert_eq!(
            eval_expr(&escaped, &row_percent, 4).expect("evals"),
            Some(1)
        );
        let row_plain = [Datum::new_string("100x".to_owned())];
        assert_eq!(eval_expr(&escaped, &row_plain, 4).expect("evals"), Some(0));
    }

    #[test]
    fn is_null_family_checks_its_own_leaf() {
        use tidb_datatype::Datum;
        let present = [Datum::Real(1.0)];
        let absent = [Datum::Null];
        let is_null = SimpleExpr::Func(SimpleSig::RealIsNull, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&is_null, &present, 4).expect("evals"), Some(0));
        assert_eq!(eval_expr(&is_null, &absent, 4).expect("evals"), Some(1));
        // A string IS NULL over a string leaf; other kinds wait on their
        // casts and answer through the same None channel today.
        let string_null = SimpleExpr::Func(SimpleSig::StringIsNull, vec![SimpleExpr::Column(0)]);
        let row = [Datum::Null];
        assert_eq!(eval_expr(&string_null, &row, 4).expect("evals"), Some(1));
    }

    #[test]
    fn json_member_of_follows_go_equality_rules() {
        use tidb_datatype::{BinaryJSON, Datum};
        // The target coerces through CreateBinaryJSON: an INT target equals
        // the ARRAY element carrying that number.
        let doc = SimpleExpr::Json(BinaryJSON::parse("[1, 2, 3]").expect("parses"));
        let member = SimpleExpr::Func(SimpleSig::JsonMemberOfSig, vec![SimpleExpr::Int(2), doc]);
        assert_eq!(eval_expr(&member, &[], 4).expect("evals"), Some(1));
        // A miss answers FALSE, not NULL.
        let doc = SimpleExpr::Json(BinaryJSON::parse("[1, 2, 3]").expect("parses"));
        let miss = SimpleExpr::Func(SimpleSig::JsonMemberOfSig, vec![SimpleExpr::Int(7), doc]);
        assert_eq!(eval_expr(&miss, &[], 4).expect("evals"), Some(0));
        // A non-array doc compares by whole-document equality.
        let doc = SimpleExpr::Json(BinaryJSON::parse("3").expect("parses"));
        let equal = SimpleExpr::Func(SimpleSig::JsonMemberOfSig, vec![SimpleExpr::Int(3), doc]);
        assert_eq!(eval_expr(&equal, &[], 4).expect("evals"), Some(1));
        // A NULL target answers NULL.
        let doc = SimpleExpr::Json(BinaryJSON::parse("[1]").expect("parses"));
        let null_target = SimpleExpr::Func(SimpleSig::JsonMemberOfSig, vec![SimpleExpr::Null, doc]);
        assert_eq!(eval_expr(&null_target, &[], 4), Ok(None));
    }

    #[test]
    fn real_cast_rounds_then_checks_range() {
        use tidb_datatype::Datum;
        // Go `ConvertFloatToInt` ROUNDS first (half away from zero):
        // CAST(2.5 AS SIGNED) = 3.
        let row = [Datum::Real(2.5)];
        let cast = SimpleExpr::Func(SimpleSig::CastRealAsInt, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&cast, &row, 4).expect("evals"), Some(3));
        let row_negative = [Datum::Real(-2.5)];
        assert_eq!(eval_expr(&cast, &row_negative, 4).expect("evals"), Some(-3));
        // An out-of-BIGINT source answers the cast overflow error, with
        // Go's `%v` rendering of the rounded value.
        let row_huge = [Datum::Real(9.3e18)];
        assert_eq!(
            eval_expr(&cast, &row_huge, 4),
            Err("constant 9300000000000000000 overflows bigint".to_owned())
        );
    }

    #[test]
    fn real_cast_composes_as_a_comparison_operand() {
        use tidb_datatype::Datum;
        // `int_col / 2 + 1` style composition: the AS REAL widening feeds
        // the REAL comparison channel.
        let row = [Datum::Int(5)];
        let widened = SimpleExpr::Func(SimpleSig::CastIntAsReal, vec![SimpleExpr::Column(0)]);
        let condition = SimpleExpr::Func(SimpleSig::GtReal, vec![widened, SimpleExpr::Real(4.5)]);
        assert_eq!(eval_expr(&condition, &row, 4).expect("evals"), Some(1));
    }

    #[test]
    fn string_casts_follow_go_prefix_conversion() {
        use tidb_datatype::Datum;
        let cast_int = |text: &str| {
            eval_expr(
                &SimpleExpr::Func(
                    SimpleSig::CastStringAsInt,
                    vec![SimpleExpr::Bytes(text.as_bytes().to_vec())],
                ),
                &[],
                4,
            )
            .expect("evals")
        };
        // `StrToInt`: the longest valid integer prefix, best-effort.
        assert_eq!(cast_int("123abc"), Some(123));
        assert_eq!(cast_int("  -42xyz"), Some(-42));
        assert_eq!(cast_int("abc"), Some(0));
        // Range saturates to the BIGINT bound (non-strict observable).
        assert_eq!(cast_int("99999999999999999999"), Some(i128::from(i64::MAX)));

        let cast_real = |text: &str| {
            eval_expr(
                &SimpleExpr::Func(
                    SimpleSig::CastStringAsReal,
                    vec![SimpleExpr::Bytes(text.as_bytes().to_vec())],
                ),
                &[],
                4,
            )
            .expect("evals")
        };
        // `StrToFloat64`: prefix parses as binary64, composed with a REAL
        // comparison the way a pushed condition arrives.
        let condition = SimpleExpr::Func(
            SimpleSig::GtReal,
            vec![
                SimpleExpr::Func(
                    SimpleSig::CastStringAsReal,
                    vec![SimpleExpr::Bytes(b"1.5e3x".to_vec())],
                ),
                SimpleExpr::Real(100.0),
            ],
        );
        assert_eq!(eval_expr(&condition, &[], 4), Ok(Some(1)));
        // Garbage answers 0 through the same channel.
        let garbage = SimpleExpr::Func(
            SimpleSig::CastStringAsReal,
            vec![SimpleExpr::Bytes(b"none".to_vec())],
        );
        assert_eq!(eval_expr(&garbage, &[], 4), Ok(Some(0)));
        let _ = cast_real("ignored-in-favour-of-the-composed-form");
    }

    #[test]
    fn decimal_divide_uses_the_request_precision_increment() {
        use tidb_datatype::{Datum, Decimal};
        // `DivideDecimal` widens the result fraction by the DAG request's
        // div_precision_increment: 5 / 2 with increment 4 answers 2.5000,
        // which compares GREATER than 2 under the decimal ordering.
        let condition = SimpleExpr::Func(
            SimpleSig::GtDecimal,
            vec![
                SimpleExpr::Func(
                    SimpleSig::DivideDecimal,
                    vec![
                        SimpleExpr::Column(0),
                        SimpleExpr::Decimal(Decimal::parse_mysql("2").0),
                    ],
                ),
                SimpleExpr::Decimal(Decimal::parse_mysql("2").0),
            ],
        );
        let row = [Datum::Decimal(Decimal::parse_mysql("5").0)];
        let div_inc_four = 4_i64;
        let div_inc_zero = 0_i64;
        assert_eq!(
            eval_expr(&condition, &row, div_inc_four).expect("evals"),
            Some(1)
        );
        // Without the increment the quotient is exactly 2 -- not greater.
        assert_eq!(
            eval_expr(&condition, &row, div_inc_zero).expect("evals"),
            Some(0)
        );
        // A zero divisor answers NULL: the row is filtered either way.
        let by_zero = SimpleExpr::Func(
            SimpleSig::DivideDecimal,
            vec![
                SimpleExpr::Column(0),
                SimpleExpr::Decimal(Decimal::parse_mysql("0").0),
            ],
        );
        assert_eq!(eval_expr(&by_zero, &row, div_inc_four), Ok(None));
    }

    #[test]
    fn math_family_follows_binary64_semantics() {
        use tidb_datatype::Datum;
        let row = [Datum::Real(0.5)];
        // `x * x` composition under Pow: POW(x, 2) = 0.25, truthy non-zero.
        let pow = SimpleExpr::Func(
            SimpleSig::Pow,
            vec![
                SimpleExpr::Column(0),
                SimpleExpr::Func(
                    SimpleSig::MultiplyReal,
                    vec![SimpleExpr::Column(0), SimpleExpr::Column(0)],
                ),
            ],
        );
        assert_eq!(eval_expr(&pow, &row, 4).expect("evals"), Some(1));
        // ACOS out of domain answers NaN -> NULL, MySQL and Go both.
        let acos = SimpleExpr::Func(SimpleSig::Acos, vec![SimpleExpr::Real(2.0)]);
        assert_eq!(eval_expr(&acos, &[], 4).expect("evals"), None);
        // RoundReal rounds half away from zero; pinned through an EQReal
        // composition (the bare condition channel only answers truth).
        let round_eq = SimpleExpr::Func(
            SimpleSig::EqReal,
            vec![
                SimpleExpr::Func(SimpleSig::RoundReal, vec![SimpleExpr::Real(2.5)]),
                SimpleExpr::Real(3.0),
            ],
        );
        assert_eq!(eval_expr(&round_eq, &[], 4).expect("evals"), Some(1));
        // PI answers the constant.
        let pi = SimpleExpr::Func(SimpleSig::Pi, vec![]);
        let pi_value = eval_expr(&pi, &[], 4).expect("evals");
        assert!(pi_value.expect("non-null") != 0);
    }

    #[test]
    fn probe_pow_expr() {
        let pow = SimpleExpr::Func(
            SimpleSig::Pow,
            vec![
                SimpleExpr::Column(0),
                SimpleExpr::Func(
                    SimpleSig::MultiplyReal,
                    vec![SimpleExpr::Column(0), SimpleExpr::Column(0)],
                ),
            ],
        );
        eprintln!(
            "eval_expr(pow, row, 4) = {:?}",
            eval_expr(&pow, &[tidb_datatype::Datum::Real(0.5)], 4)
        );
    }

    #[test]
    fn string_functions_follow_go_semantics() {
        use tidb_datatype::Datum;
        // CHAR_LENGTH counts runes over UTF-8; LENGTH-style binary counts
        // bytes. `"héllo"`: 5 chars, 6 bytes.
        let row = [Datum::new_string("héllo".to_owned())];
        let char_len = SimpleExpr::Func(SimpleSig::CharLengthUtf8, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&char_len, &row, 4).expect("evals"), Some(5));
        let byte_len = SimpleExpr::Func(SimpleSig::CharLength, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&byte_len, &row, 4).expect("evals"), Some(6));
        // LOWER folds case; the UTF8 form folds runes (É -> é).
        let lower = SimpleExpr::Func(
            SimpleSig::LowerUtf8,
            vec![SimpleExpr::Bytes(b"H\xc3\x89LLO".to_vec())],
        );
        let folded = eval_bytes(Some(&lower), &row, 4).expect("folds");
        // "héllo" -- the É (C3 89) folds to é (C3 A9) via the rune fold.
        assert_eq!(folded, b"h\xc3\xa9llo".to_vec());
        // A bare string function as a condition answers its numeric-prefix
        // truth: "héllo" has none, so FALSE.
        assert_eq!(eval_expr(&lower, &row, 4).expect("evals"), Some(0));
        // SUBSTRING with a negative position counts from the end.
        let sub = SimpleExpr::Func(
            SimpleSig::Substring3ArgsUtf8,
            vec![
                SimpleExpr::Column(0),
                SimpleExpr::Int(-4),
                SimpleExpr::Int(3),
            ],
        );
        // SUBSTRING("héllo", -4, 3): 1-based from the end-4 => "éll".
        // The bare condition answers its numeric-prefix truth: "éll" has
        // none -> FALSE.
        assert_eq!(eval_expr(&sub, &row, 4).expect("evals"), Some(0));
    }

    #[test]
    fn temporal_extraction_follows_go() {
        use tidb_datatype::{Datum, Time, TimeType};
        // 2024-03-05 14:30:45.123456 as a DATETIME column.
        let time = Time::from_date_checked(2024, 3, 5, 14, 30, 45, 123456, TimeType::DateTime, 6)
            .expect("constructs");
        let row = [Datum::Time(time)];

        let month = SimpleExpr::Func(SimpleSig::Month, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&month, &row, 4).expect("evals"), Some(3));
        let hour = SimpleExpr::Func(SimpleSig::Hour, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&hour, &row, 4).expect("evals"), Some(14));
        let minute = SimpleExpr::Func(SimpleSig::Minute, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&minute, &row, 4).expect("evals"), Some(30));
        let second = SimpleExpr::Func(SimpleSig::Second, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&second, &row, 4).expect("evals"), Some(45));
        let micro = SimpleExpr::Func(SimpleSig::MicroSecond, vec![SimpleExpr::Column(0)]);
        assert_eq!(eval_expr(&micro, &row, 4).expect("evals"), Some(123456));
        // DateDiff truncates both sides to their date parts: two days apart
        // answers 2 regardless of the clock fields.
        let earlier = Time::from_date_checked(2024, 3, 3, 23, 0, 0, 0, TimeType::DateTime, 0)
            .expect("constructs");
        let diff = SimpleExpr::Func(
            SimpleSig::DateDiff,
            vec![SimpleExpr::Column(0), SimpleExpr::Column(1)],
        );
        let row_two = [Datum::Time(time), Datum::Time(earlier)];
        assert_eq!(eval_expr(&diff, &row_two, 4).expect("evals"), Some(2));
    }

    #[test]
    fn week_without_mode_follows_go_calendar() {
        use tidb_datatype::{Datum, Time, TimeType};
        let week = |date: Time| {
            let row = [Datum::Time(date)];
            eval_expr(
                &SimpleExpr::Func(SimpleSig::WeekWithoutMode, vec![SimpleExpr::Column(0)]),
                &row,
                4,
            )
            .expect("evals")
        };
        // Mode 0: weeks start Sunday and week 1 is the week containing the
        // first Sunday of the year -- days before that are week 0.
        // 2024-01-01 (Monday) is week 0; the first Sunday (2024-01-07)
        // opens week 1.
        let new_year =
            Time::from_date_checked(2024, 1, 1, 0, 0, 0, 0, TimeType::Date, 0).expect("constructs");
        assert_eq!(week(new_year), Some(0));
        let first_sunday =
            Time::from_date_checked(2024, 1, 7, 0, 0, 0, 0, TimeType::Date, 0).expect("constructs");
        assert_eq!(week(first_sunday), Some(1));
    }

    #[test]
    fn in_string_follows_collation_and_null_rules() {
        use tidb_datatype::Datum;
        // 'b' IN ('a', 'b') matches.
        let member = SimpleExpr::Func(
            SimpleSig::InString(46), // utf8mb4_bin
            vec![
                SimpleExpr::Bytes(b"b".to_vec()),
                SimpleExpr::Bytes(b"a".to_vec()),
                SimpleExpr::Bytes(b"b".to_vec()),
            ],
        );
        assert_eq!(eval_expr(&member, &[], 4).expect("evals"), Some(1));
        // No match answers FALSE, not NULL.
        let miss = SimpleExpr::Func(
            SimpleSig::InString(46),
            vec![
                SimpleExpr::Bytes(b"c".to_vec()),
                SimpleExpr::Bytes(b"a".to_vec()),
                SimpleExpr::Bytes(b"b".to_vec()),
            ],
        );
        assert_eq!(eval_expr(&miss, &[], 4).expect("evals"), Some(0));
        // A NULL element makes the answer NULL.
        let with_null = SimpleExpr::Func(
            SimpleSig::InString(46),
            vec![
                SimpleExpr::Bytes(b"c".to_vec()),
                SimpleExpr::Null,
                SimpleExpr::Bytes(b"b".to_vec()),
            ],
        );
        assert_eq!(eval_expr(&with_null, &[], 4), Ok(None));
    }

    #[test]
    fn date_format_composes_over_a_datetime_column() {
        use tidb_datatype::{Datum, Time, TimeType};
        let time = Time::from_date_checked(2024, 3, 5, 14, 30, 45, 123456, TimeType::DateTime, 6)
            .expect("constructs");
        let row = [Datum::Time(time)];
        // DATE_FORMAT(t, '%Y-%m-%d') answers "2024-03-05" -- composable as
        // an EQString operand through the bytes channel.
        let condition = SimpleExpr::Func(
            SimpleSig::EqString(46), // utf8mb4_bin
            vec![
                SimpleExpr::Func(
                    SimpleSig::DateFormatSig,
                    vec![
                        SimpleExpr::Column(0),
                        SimpleExpr::Bytes(b"%Y-%m-%d".to_vec()),
                    ],
                ),
                SimpleExpr::Bytes(b"2024-03-05".to_vec()),
            ],
        );
        assert_eq!(eval_expr(&condition, &row, 4).expect("evals"), Some(1));
    }

    #[test]
    fn conv_rebinds_digits_across_bases_like_go() {
        // EQString(CONV(x, from, to), expected) pins the exact answer
        // through the bytes channel.
        let conv_eq = |text: &[u8], from: i64, to: i64, expected: &str| {
            SimpleExpr::Func(
                SimpleSig::EqString(46), // utf8mb4_bin
                vec![
                    SimpleExpr::Func(
                        SimpleSig::Conv,
                        vec![
                            SimpleExpr::Bytes(text.to_vec()),
                            SimpleExpr::Int(from),
                            SimpleExpr::Int(to),
                        ],
                    ),
                    SimpleExpr::Bytes(expected.as_bytes().to_vec()),
                ],
            )
        };
        // MySQL doc examples: CONV('a',16,2)='1010', CONV('6E',18,8)='172',
        // CONV(-17,10,-18)='-H' (ignore-sign output keeps the minus).
        assert_eq!(
            eval_expr(&conv_eq(b"a", 16, 2, "1010"), &[], 4).expect("evals"),
            Some(1)
        );
        assert_eq!(
            eval_expr(&conv_eq(b"6E", 18, 8, "172"), &[], 4).expect("evals"),
            Some(1)
        );
        assert_eq!(
            eval_expr(&conv_eq(b"-17", 10, -18, "-H"), &[], 4).expect("evals"),
            Some(1)
        );
        // Negative input base = signed input: a positive value renders
        // plainly, while a negative literal wraps and prints as the full
        // two's-complement bit pattern (Go `int64(val) < 0` marks it
        // negative but only an ignore-sign output adds '-').
        assert_eq!(
            eval_expr(&conv_eq(b"a", -16, 2, "1010"), &[], 4).expect("evals"),
            Some(1)
        );
        assert_eq!(
            eval_expr(
                &conv_eq(
                    b"-a",
                    -16,
                    2,
                    "1111111111111111111111111111111111111111111111111111111111110110",
                ),
                &[],
                4,
            )
            .expect("evals"),
            Some(1)
        );
        // A signed input clamps to [-2^63, MaxInt64] before wrapping.
        assert_eq!(
            eval_expr(
                &conv_eq(b"-9223372036854775809", -10, 16, "8000000000000000"),
                &[],
                4,
            )
            .expect("evals"),
            Some(1)
        );
        // Bases outside [2, 36] answer NULL; junk answers "0".
        assert_eq!(eval_expr(&conv_eq(b"a", 1, 10, "0"), &[], 4), Ok(None));
        assert_eq!(
            eval_expr(&conv_eq(b"g", 16, 10, "0"), &[], 4).expect("evals"),
            Some(1)
        );
    }

    #[test]
    fn conv_as_a_condition_answers_numeric_prefix_truth() {
        // A bare CONV as a condition answers its numeric-prefix truth:
        // "1010" is truthy, "0" is not.
        let conv = |text: &[u8], from: i64, to: i64| {
            SimpleExpr::Func(
                SimpleSig::Conv,
                vec![
                    SimpleExpr::Bytes(text.to_vec()),
                    SimpleExpr::Int(from),
                    SimpleExpr::Int(to),
                ],
            )
        };
        assert_eq!(
            eval_expr(&conv(b"a", 16, 2), &[], 4).expect("evals"),
            Some(1)
        );
        assert_eq!(
            eval_expr(&conv(b"g", 16, 2), &[], 4).expect("evals"),
            Some(0)
        );
        // Parse overflow answers NULL here where Go raises the BIGINT
        // UNSIGNED 1690 error (the bytes channel carries no error).
        let overflow = SimpleExpr::Func(
            SimpleSig::Conv,
            vec![
                SimpleExpr::Bytes(b"18446744073709551616".to_vec()),
                SimpleExpr::Int(10),
                SimpleExpr::Int(16),
            ],
        );
        assert_eq!(eval_expr(&overflow, &[], 4).expect("evals"), None);
    }

    #[test]
    fn date_add_follows_mysql_calendar_over_a_column() {
        use tidb_datatype::{Datum, Time, TimeType};
        let time = Time::from_date_checked(2024, 3, 5, 14, 30, 45, 0, TimeType::DateTime, 0)
            .expect("constructs");
        let row = [Datum::Time(time)];
        let arith = |subtract: bool, n: i64, unit: &[u8]| {
            SimpleExpr::Func(
                SimpleSig::AddSubDate {
                    subtract,
                    date: DateArithArg::Datetime,
                    interval: IntervalArg::Int,
                    datetime_result: false,
                },
                vec![
                    SimpleExpr::Column(0),
                    SimpleExpr::Int(n),
                    SimpleExpr::Bytes(unit.to_vec()),
                ],
            )
        };
        // DATE_ADD(t, INTERVAL 1 DAY) answers the next same-clock day.
        let next = eval_time(Some(&arith(false, 1, b"DAY")), &row, 4).expect("evals");
        assert_eq!(
            next,
            Time::from_date_checked(2024, 3, 6, 14, 30, 45, 0, TimeType::DateTime, 0)
                .expect("constructs")
        );
        // DATE_SUB(t, INTERVAL 1 DAY) answers the previous day.
        let previous = eval_time(Some(&arith(true, 1, b"DAY")), &row, 4).expect("evals");
        assert_eq!(
            previous,
            Time::from_date_checked(2024, 3, 4, 14, 30, 45, 0, TimeType::DateTime, 0)
                .expect("constructs")
        );
        // MySQL clamps month-end overflow: 2024-01-31 + 1 MONTH = 02-29.
        let january = Time::from_date_checked(2024, 1, 31, 0, 0, 0, 0, TimeType::DateTime, 0)
            .expect("constructs");
        let february =
            eval_time(Some(&arith(false, 1, b"MONTH")), &[Datum::Time(january)], 4).expect("evals");
        assert_eq!(
            february,
            Time::from_date_checked(2024, 2, 29, 0, 0, 0, 0, TimeType::DateTime, 0)
                .expect("constructs")
        );
    }

    #[test]
    fn date_add_over_text_answers_the_mysql_string_form() {
        // DATE_ADD('2024-03-05', INTERVAL ...) keeps the pure-date form
        // for date units and widens to the datetime form for clock
        // units (Go `getDateFromString`).
        let add = |interval: &[u8], unit: &[u8]| {
            SimpleExpr::Func(
                SimpleSig::AddSubDate {
                    subtract: false,
                    date: DateArithArg::String,
                    interval: IntervalArg::String,
                    datetime_result: false,
                },
                vec![
                    SimpleExpr::Bytes(b"2024-03-05".to_vec()),
                    SimpleExpr::Bytes(interval.to_vec()),
                    SimpleExpr::Bytes(unit.to_vec()),
                ],
            )
        };
        let eq = |left: SimpleExpr, right: &str| {
            SimpleExpr::Func(
                SimpleSig::EqString(46), // utf8mb4_bin
                vec![left, SimpleExpr::Bytes(right.as_bytes().to_vec())],
            )
        };
        assert_eq!(
            eval_expr(&eq(add(b"1", b"DAY"), "2024-03-06"), &[], 4).expect("evals"),
            Some(1)
        );
        assert_eq!(
            eval_expr(&eq(add(b"1", b"HOUR"), "2024-03-05 01:00:00"), &[], 4).expect("evals"),
            Some(1)
        );
        // Junk interval text keeps its numeric prefix: "abc" reads "0".
        assert_eq!(
            eval_expr(&eq(add(b"abc", b"DAY"), "2024-03-05"), &[], 4).expect("evals"),
            Some(1)
        );
        // A bare date arithmetic as a condition answers its non-NULL
        // truth.
        assert_eq!(
            eval_expr(&add(b"1", b"DAY"), &[], 4).expect("evals"),
            Some(1)
        );
    }

    #[test]
    fn date_add_over_a_duration_column_answers_a_duration() {
        use tidb_datatype::{Datum, MySqlDuration};
        let two_hours = MySqlDuration::from_nanoseconds(7_200_000_000_000, 0).expect("constructs");
        let row = [Datum::Duration(two_hours)];
        let arith = |datetime_result: bool| {
            SimpleExpr::Func(
                SimpleSig::AddSubDate {
                    subtract: false,
                    date: DateArithArg::Duration,
                    interval: IntervalArg::String,
                    datetime_result,
                },
                vec![
                    SimpleExpr::Column(0),
                    SimpleExpr::Bytes(b"1:10".to_vec()),
                    SimpleExpr::Bytes(b"HOUR_MINUTE".to_vec()),
                ],
            )
        };
        // 02:00:00 + INTERVAL '1:10' HOUR_MINUTE = 03:10:00.
        let expected = MySqlDuration::from_nanoseconds(11_400_000_000_000, 0).expect("constructs");
        assert_eq!(eval_duration(Some(&arith(false)), &row, 4), Some(expected));
        // The datetime-promoted duration ids anchor on the current date
        // and answer no stable predicate.
        assert_eq!(eval_time(Some(&arith(true)), &row, 4), None);
    }

    #[test]
    fn mod_real_follows_binary64_remainder_semantics() {
        let modulo = |left: f64, right: f64| {
            SimpleExpr::Func(
                SimpleSig::ModReal,
                vec![SimpleExpr::Real(left), SimpleExpr::Real(right)],
            )
        };
        // Go `math.Mod`: the remainder carries the dividend's sign; a
        // bare remainder answers its non-zero truth.
        assert_eq!(
            eval_expr(&modulo(7.5, 2.0), &[], 4).expect("evals"),
            Some(1) // 1.5
        );
        assert_eq!(
            eval_expr(&modulo(-7.5, 2.0), &[], 4).expect("evals"),
            Some(1) // -1.5
        );
        // A zero divisor answers NULL (Go's division-by-zero error folded).
        assert_eq!(eval_expr(&modulo(1.0, 0.0), &[], 4), Ok(None));
        // A bare remainder as a condition answers its own truth.
        assert_eq!(
            eval_expr(&modulo(4.0, 2.0), &[], 4).expect("evals"),
            Some(0)
        );
    }

    #[test]
    fn timestamp_diff_follows_go_unit_table() {
        use tidb_datatype::{Datum, Time, TimeType};
        let time = |y: i32, m: i32, d: i32, hh: i32, mm: i32, ss: i32| {
            Time::from_date_checked(y, m, d, hh, mm, ss, 0, TimeType::DateTime, 0)
                .expect("constructs")
        };
        let diff = |unit: &[u8], lhs: Time, rhs: Time| {
            eval_expr(
                &SimpleExpr::Func(
                    SimpleSig::TimestampDiff,
                    vec![
                        SimpleExpr::Bytes(unit.to_vec()),
                        SimpleExpr::Time(lhs),
                        SimpleExpr::Time(rhs),
                    ],
                ),
                &[],
                4,
            )
            .expect("evals")
        };
        // MySQL doc example: TIMESTAMPDIFF(MONTH, '2003-02-01', '2003-05-01') = 3.
        assert_eq!(
            diff(
                b"MONTH",
                time(2003, 2, 1, 0, 0, 0),
                time(2003, 5, 1, 0, 0, 0)
            ),
            Some(3)
        );
        // The clock participates: a full day plus two hours floors to 1 day.
        assert_eq!(
            diff(b"DAY", time(2024, 3, 4, 0, 0, 0), time(2024, 3, 5, 2, 0, 0),),
            Some(1)
        );
        // MONTH rounds down through the month-end clamp.
        assert_eq!(
            diff(
                b"MONTH",
                time(2024, 1, 31, 0, 0, 0),
                time(2024, 2, 29, 0, 0, 0),
            ),
            Some(0)
        );
        // An unknown unit answers 0 (Go's default arm).
        assert_eq!(
            diff(
                b"FORTNIGHT",
                time(2024, 3, 4, 0, 0, 0),
                time(2024, 3, 5, 0, 0, 0),
            ),
            Some(0)
        );
    }

    #[test]
    fn decimal_casts_compose_as_comparison_operands() {
        use tidb_datatype::{Datum, MyDecimal, Time, TimeType};
        let dec = |value: i64| {
            SimpleExpr::Decimal(tidb_datatype::Decimal::from_my_decimal(
                &MyDecimal::from_int(value),
            ))
        };
        // dc > -5 with the int side widened by CastIntAsDecimal.
        let condition = SimpleExpr::Func(
            SimpleSig::GtDecimal,
            vec![
                SimpleExpr::Column(0),
                SimpleExpr::Func(SimpleSig::CastIntAsDecimal, vec![SimpleExpr::Int(-5)]),
            ],
        );
        let row = [Datum::Decimal(tidb_datatype::Decimal::from_my_decimal(
            &MyDecimal::from_int(-4),
        ))];
        assert_eq!(eval_expr(&condition, &row, 4).expect("evals"), Some(1));
        // A string cast keeps the numeric prefix ("12.5abc" reads 12.5).
        let string_cast = SimpleExpr::Func(
            SimpleSig::CastStringAsDecimal,
            vec![SimpleExpr::Bytes(b"12.5abc".to_vec())],
        );
        let below = SimpleExpr::Func(SimpleSig::LtDecimal, vec![string_cast, dec(13)]);
        assert_eq!(eval_expr(&below, &[], 4).expect("evals"), Some(1));
        // A time cast renders the YYYYMMDDHHMMSS numeric.
        let noon = Time::from_date_checked(2024, 3, 5, 14, 30, 45, 0, TimeType::DateTime, 0)
            .expect("constructs");
        let equal = SimpleExpr::Func(
            SimpleSig::EqDecimal,
            vec![
                SimpleExpr::Func(SimpleSig::CastTimeAsDecimal, vec![SimpleExpr::Time(noon)]),
                dec(20_240_305_143_045),
            ],
        );
        assert_eq!(eval_expr(&equal, &[], 4).expect("evals"), Some(1));
        // A bare cast as a condition answers its own truth.
        let bare = SimpleExpr::Func(SimpleSig::CastIntAsDecimal, vec![SimpleExpr::Int(0)]);
        assert_eq!(eval_expr(&bare, &[], 4).expect("evals"), Some(0));
    }
    #[test]
    fn string_casts_render_each_source_like_go() {
        // EQString(CAST(x AS CHAR), expected) pins the exact rendering.
        let cast_eq = |cast: SimpleSig, operand: SimpleExpr, expected: &str| {
            SimpleExpr::Func(
                SimpleSig::EqString(46), // utf8mb4_bin
                vec![
                    SimpleExpr::Func(cast, vec![operand]),
                    SimpleExpr::Bytes(expected.as_bytes().to_vec()),
                ],
            )
        };
        // A negative int renders with the sign (Go `FormatInt`).
        assert_eq!(
            eval_expr(
                &cast_eq(SimpleSig::CastIntAsString, SimpleExpr::Int(-42), "-42"),
                &[],
                4,
            )
            .expect("evals"),
            Some(1)
        );
        // A real renders in the shortest 'f' form.
        assert_eq!(
            eval_expr(
                &cast_eq(SimpleSig::CastRealAsString, SimpleExpr::Real(2.5), "2.5"),
                &[],
                4,
            )
            .expect("evals"),
            Some(1)
        );
        // A string source passes through.
        assert_eq!(
            eval_expr(
                &cast_eq(
                    SimpleSig::CastStringAsString,
                    SimpleExpr::Bytes(b"text".to_vec()),
                    "text",
                ),
                &[],
                4,
            )
            .expect("evals"),
            Some(1)
        );
        // A time renders in its kind's form.
        let noon = tidb_datatype::Time::from_date_checked(
            2024,
            3,
            5,
            14,
            30,
            45,
            0,
            tidb_datatype::TimeType::DateTime,
            0,
        )
        .expect("constructs");
        assert_eq!(
            eval_expr(
                &cast_eq(
                    SimpleSig::CastTimeAsString,
                    SimpleExpr::Time(noon),
                    "2024-03-05 14:30:45",
                ),
                &[],
                4,
            )
            .expect("evals"),
            Some(1)
        );
        // A duration renders `HH:MM:SS` (the wire has no duration leaf;
        // the source arrives as a column).
        let span = tidb_datatype::MySqlDuration::from_nanoseconds(3_600_000_000_000, 0)
            .expect("constructs");
        let duration_row = [tidb_datatype::Datum::Duration(span)];
        assert_eq!(
            eval_expr(
                &cast_eq(
                    SimpleSig::CastDurationAsString,
                    SimpleExpr::Column(0),
                    "01:00:00",
                ),
                &duration_row,
                4,
            )
            .expect("evals"),
            Some(1)
        );
        // A bare cast as a condition answers its numeric-prefix truth:
        // "-42" reads -42 -> true.
        let bare = SimpleExpr::Func(SimpleSig::CastIntAsString, vec![SimpleExpr::Int(-42)]);
        assert_eq!(eval_expr(&bare, &[], 4).expect("evals"), Some(1));
    }

    #[test]
    fn temporal_casts_admit_numeric_and_text_sources() {
        use tidb_datatype::{Datum, MySqlDuration, Time, TimeType};
        let zone_anchor = utc_anchor();
        // A pure-date number parses to a date: 20240305.
        let as_time = SimpleExpr::Func(SimpleSig::CastIntAsTime, vec![SimpleExpr::Int(20_240_305)]);
        let expected =
            Time::from_date_checked(2024, 3, 5, 0, 0, 0, 0, TimeType::Date, 0).expect("constructs");
        assert_eq!(eval_time(Some(&as_time), &[], 4), Some(expected));
        // A datetime text widens to the datetime kind.
        let as_text_time = SimpleExpr::Func(
            SimpleSig::CastStringAsTime,
            vec![SimpleExpr::Bytes(b"2024-03-05 14:30:45".to_vec())],
        );
        let expected = Time::from_date_checked(2024, 3, 5, 14, 30, 45, 0, TimeType::DateTime, 6)
            .expect("constructs");
        assert_eq!(eval_time(Some(&as_text_time), &[], 4), Some(expected));
        // The parsed kinds order like Go's packed times: the earlier
        // text compares less.
        let earlier = SimpleExpr::Func(
            SimpleSig::CastStringAsTime,
            vec![SimpleExpr::Bytes(b"2024-03-04".to_vec())],
        );
        let condition = SimpleExpr::Func(SimpleSig::LtTime, vec![earlier, as_text_time]);
        let _ = zone_anchor;
        assert_eq!(eval_expr(&condition, &[], 4).expect("evals"), Some(1));
        // Digits read as HHMMSS: 101010 -> 10:10:10 (Go NumberToDuration).
        let as_duration =
            SimpleExpr::Func(SimpleSig::CastIntAsDuration, vec![SimpleExpr::Int(101_010)]);
        let expected = MySqlDuration::from_nanoseconds(36_610_000_000_000, 6).expect("constructs");
        assert_eq!(eval_duration(Some(&as_duration), &[], 4), Some(expected));
        // A clock text parses: '11:30:45'.
        let as_text_duration = SimpleExpr::Func(
            SimpleSig::CastStringAsDuration,
            vec![SimpleExpr::Bytes(b"11:30:45".to_vec())],
        );
        let expected = MySqlDuration::from_nanoseconds(41_445_000_000_000, 6).expect("constructs");
        assert_eq!(
            eval_duration(Some(&as_text_duration), &[], 4),
            Some(expected)
        );
        // The duration identity passes a column through.
        let identity = SimpleExpr::Func(
            SimpleSig::CastDurationAsDuration,
            vec![SimpleExpr::Column(0)],
        );
        let row = [Datum::Duration(expected)];
        assert_eq!(eval_duration(Some(&identity), &row, 4), Some(expected));
    }

    #[test]
    fn json_casts_wrap_sources_and_read_them_back() {
        use tidb_datatype::{Datum, Time, TimeType};
        // A text source parses as JSON; a number source wraps.
        let parsed = eval_json(
            Some(&SimpleExpr::Func(
                SimpleSig::CastStringAsJson,
                vec![SimpleExpr::Bytes(br#"{"a": 1}"#.to_vec())],
            )),
            &[],
            4,
        )
        .expect("evals");
        assert_eq!(parsed.element_count(), Ok(1));
        let wrapped = eval_json(
            Some(&SimpleExpr::Func(
                SimpleSig::CastIntAsJson,
                vec![SimpleExpr::Int(7)],
            )),
            &[],
            4,
        )
        .expect("evals");
        assert_eq!(wrapped.as_i64(), Some(7));
        // A datetime wraps as the opaque time scalar at MaxFsp.
        let noon = Time::from_date_checked(2024, 3, 5, 14, 30, 45, 0, TimeType::DateTime, 0)
            .expect("constructs");
        let time_json = eval_json(
            Some(&SimpleExpr::Func(
                SimpleSig::CastTimeAsJson,
                vec![SimpleExpr::Time(noon)],
            )),
            &[],
            4,
        )
        .expect("evals");
        assert_eq!(time_json.as_time(6).as_ref(), time_json.as_time(6).as_ref());
        assert!(time_json.as_time(6).is_ok());
        // CAST(json AS INT): numbers truncate, strings keep the integer
        // prefix.
        let int_col = SimpleExpr::Column(0);
        let as_int = SimpleExpr::Func(SimpleSig::CastJsonAsInt, vec![int_col]);
        let number_row = [Datum::Json(wrapped)];
        assert_eq!(eval_expr(&as_int, &number_row, 4).expect("evals"), Some(7));
        // CAST(json AS REAL) over a string document reads the prefix.
        let text_json = tidb_datatype::BinaryJSON::parse(r#""12.5abc""#).expect("parses");
        let as_real = SimpleExpr::Func(SimpleSig::CastJsonAsReal, vec![SimpleExpr::Column(0)]);
        let text_row = [Datum::Json(text_json)];
        assert_eq!(eval_real(Some(&as_real), &text_row, 4), Some(12.5));
        // CAST(json AS TIME) parses a string document into a date.
        let date_json = tidb_datatype::BinaryJSON::parse(r#""2024-03-05""#).expect("parses");
        let as_time = SimpleExpr::Func(SimpleSig::CastJsonAsTime, vec![SimpleExpr::Column(0)]);
        let date_row = [Datum::Json(date_json)];
        let expected =
            Time::from_date_checked(2024, 3, 5, 0, 0, 0, 0, TimeType::Date, 6).expect("constructs");
        assert_eq!(eval_time(Some(&as_time), &date_row, 4), Some(expected));
        // A bare JSON cast as a condition answers its non-NULL truth.
        let bare = SimpleExpr::Func(SimpleSig::CastIntAsJson, vec![SimpleExpr::Int(7)]);
        assert_eq!(eval_expr(&bare, &[], 4).expect("evals"), Some(1));
    }
}
