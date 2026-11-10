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
//! coprocessor's front door: request dispatch, DAG decode, the
//! flat-list-to-tree executor conversion, and `handleCopDAGRequest`, which
//! runs every DAG through `buildAndRunMPPExecutor`'s `mppExec` chain
//! (`mpp.rs`, `mpp_exec.rs`) and answers `genRespWithMPPExec`'s response.
//! Checksum answers Go's fixed placeholder stub.
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
mod mpp;
mod mpp_exec;
pub mod mpp_task;

#[cfg(test)]
#[path = "region_group_tests.rs"]
mod region_group_tests;

#[cfg(test)]
#[path = "mpp_exec_tests.rs"]
mod mpp_exec_tests;
#[cfg(test)]
mod mpp_task_tests;

use tidb_datatype::CollationCollator;
use eval_context::{RequestEvalContext, SharedExpression};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use prost::Message;
use tidb_proto::coprocessor;
use tidb_proto::tipb;

use tidb_tablecodec::table_key::RecordHandle;
use tidb_expr::EvalErrorSql;


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
    store: &dyn tidb_unistore_dbreader::DbReader,
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

/// Go `HandleMPPDAGReq`: runs `req`'s DAG as the MPP task `mpp_ctx` names;
/// its root exchange sender sends every row through the task's tunnels.
/// Go panics when the task cannot build, open or run.
///
/// # Panics
/// As Go's: a build, open or running error.
pub fn handle_mpp_dag_req(
    store: &dyn tidb_unistore_dbreader::DbReader,
    req: &coprocessor::Request,
    mpp_ctx: Arc<mpp_task::MppCtx>,
) -> coprocessor::Response {
    let dag_req = match tipb::DagRequest::decode(req.data.as_slice()) {
        Ok(dag_req) => dag_req,
        Err(err) => return other_error(&err.to_string()),
    };
    // Go `flagsAndTzToSessionContext(dagReq.Flags, tz)`; Go drops the error
    // `ConstructTimeZone` answers, leaving the nil (UTC) location.
    let time_zone = match dag_req.time_zone_name() {
        "" => TimeZoneSpec::FixedOffset(dag_req.time_zone_offset()),
        "System" => TimeZoneSpec::System,
        name => TimeZoneSpec::Named(name.to_owned()),
    };
    let sctx = RequestEvalContext::new(
        time_zone.resolve().unwrap_or_else(|_| tidb_datatype::SessionTimeZone::utc()),
        4,
        dag_req.flags.unwrap_or(0),
    );
    let mut builder = mpp::MppExecBuilder {
        sctx: Arc::new(sctx),
        db_reader: store,
        key_ranges: req.ranges.clone(),
        start_ts: req.start_ts,
        counts: None,
        paging: None,
        paging_size: 0,
        mpp_ctx: Some(mpp_ctx),
        output_offsets: dag_req.output_offsets.clone(),
    };
    let root_exec = dag_req
        .root_executor
        .as_ref()
        .unwrap_or_else(|| panic!("build error: the DAG has no root executor"));
    let mut mpp_exec = builder.build_mpp_executor(root_exec).unwrap_or_else(|err| panic!("build error: {err}"));
    mpp_exec.open().unwrap_or_else(|err| panic!("open phase find error: {err:?}"));
    mpp_exec.next().unwrap_or_else(|err| panic!("running phase find error: {err:?}"));
    coprocessor::Response::default()
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

}

impl From<tidb_exprctx::EvalError> for CopExecError {
    fn from(error: tidb_exprctx::EvalError) -> Self {
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

/// Go `handleCopDAGRequest`: the DAG decoded (`buildDAG`), built into its
/// `mppExec` chain and run (`buildAndRunMPPExecutor`), and the response
/// assembled from the chain (`genRespWithMPPExec`).
fn handle_cop_dag_request(
    store: &dyn tidb_unistore_dbreader::DbReader,
    req: &coprocessor::Request,
) -> coprocessor::Response {
    let start_time = std::time::Instant::now();
    let context = match build_dag(req) {
        Ok(context) => context,
        Err(message) => return other_error(&message),
    };
    // A chain that fails to build answers its error as `OtherError` (Go's
    // `ErrExecutorNotSupportedMsg` arm; this port's narrowings likewise).
    let run = match build_and_run_mpp_executor(store, &context) {
        Ok(run) => run,
        Err(message) => return other_error(&message),
    };
    let warnings = context.expression_context.take_warnings();
    gen_resp_with_mpp_exec(run, &context, warnings, start_time.elapsed())
}

/// What `buildAndRunMPPExecutor` hands back: the executor chain, the chunks
/// its root produced, the paging range, the per-range counts and the error
/// that stopped it.
struct MppRun<'a> {
    exec: Box<dyn mpp_exec::MppExec<'a> + 'a>,
    chunks: Vec<mpp_exec::Chunk>,
    last_range: Option<coprocessor::KeyRange>,
    counts: Vec<i64>,
    ndvs: Vec<i64>,
    err: Option<mpp_exec::MppExecError>,
}

/// Go `buildAndRunMPPExecutor`.
fn build_and_run_mpp_executor<'a>(
    store: &'a dyn tidb_unistore_dbreader::DbReader,
    context: &DagContext,
) -> Result<MppRun<'a>, String> {
    let dag_req = &context.dag_req;
    let root_exec = match &dag_req.root_executor {
        Some(root) => root.clone(),
        None => executor_lists_to_tree(&dag_req.executors)?,
    };
    if !dag_req.intermediate_output_channels.is_empty() {
        // Only `indexLookUpExec` produces intermediate results.
        return Err("intermediate output channels are a later course of this port".to_owned());
    }
    let counts = dag_req.collect_range_counts().then(|| {
        Rc::new(RefCell::new(mpp_exec::RangeCounts {
            counts: vec![0; context.key_ranges.len()],
            ndvs: vec![0; context.key_ranges.len()],
        }))
    });
    let paging = (context.paging_size > 0)
        .then(|| Rc::new(RefCell::new(coprocessor::KeyRange::default())));
    let mut builder = mpp::MppExecBuilder {
        sctx: Arc::clone(&context.expression_context),
        db_reader: store,
        key_ranges: context.key_ranges.clone(),
        start_ts: context.start_ts,
        counts: counts.clone(),
        paging: paging.clone(),
        paging_size: context.paging_size,
        mpp_ctx: None,
        output_offsets: Vec::new(),
    };
    let mut exec = builder.build_mpp_executor(&root_exec)?;
    let (chunks, err) = mpp_execute(exec.as_mut(), dag_req, context.paging_size);
    // An empty paging range means the scan never reported one.
    let last_range = paging
        .map(|paging| paging.take())
        .filter(|range| !(range.start.is_empty() && range.end.is_empty()));
    let (counts, ndvs) = counts.map_or_else(Default::default, |counts| {
        let counts = counts.borrow();
        (counts.counts.clone(), counts.ndvs.clone())
    });
    Ok(MppRun {
        exec,
        chunks,
        last_range,
        counts,
        ndvs,
        err,
    })
}

/// Go `mppExecute`: open the root, pull chunks until one comes back empty
/// (or a chunk-encoded page passes the paging size), stop the chain.
fn mpp_execute<'a>(
    exec: &mut (dyn mpp_exec::MppExec<'a> + 'a),
    dag_req: &tipb::DagRequest,
    paging_size: u64,
) -> (Vec<mpp_exec::Chunk>, Option<mpp_exec::MppExecError>) {
    let mut chunks = Vec::new();
    let mut err = exec.open().err();
    if err.is_none() {
        let mut total_rows = 0_u64;
        loop {
            let chk = match exec.next() {
                Ok(chk) => chk,
                Err(error) => {
                    err = Some(error);
                    break;
                }
            };
            let cur_row_cnt = chk.as_ref().map_or(0, Vec::len);
            if cur_row_cnt == 0 {
                break;
            }
            chunks.extend(chk);
            total_rows += cur_row_cnt as u64;
            if dag_req.encode_type() == tipb::EncodeType::TypeChunk
                && paging_size > 0
                && total_rows > paging_size
            {
                break;
            }
        }
    }
    // Go's deferred `exec.stop()`, which panics on an error.
    exec.stop().expect("stopping the executors succeeds");
    (chunks, err)
}

/// Go `flattenMppExec`: the chain in post order, children first.
fn flatten_mpp_exec<'e, 'a>(
    exec: &'e (dyn mpp_exec::MppExec<'a> + 'a),
    execs: &mut Vec<&'e (dyn mpp_exec::MppExec<'a> + 'a)>,
) {
    for child in exec.get_children() {
        flatten_mpp_exec(child, execs);
    }
    execs.push(exec);
}

/// Go `ErrLocked.Error`.
fn locked_error_message(lock: &tidb_proto::kvrpcpb::LockInfo) -> String {
    format!(
        "key is locked, key: {}, Type: {}, primary: {}, startTS: {}",
        tidb_mysql::errors::go_quote_bytes(&lock.key),
        lock.lock_type,
        tidb_mysql::errors::go_quote_bytes(&lock.primary_lock),
        lock.lock_version
    )
}

/// Go `genRespWithMPPExec`: the chunks (none after an error) and the error,
/// one execution summary per request executor -- each charged the whole
/// request's duration, "for simplicity" -- the warnings, a lock, and the
/// scan detail whose processed versions are the scans' `rowCnt`.
fn gen_resp_with_mpp_exec(
    run: MppRun<'_>,
    context: &DagContext,
    warnings: Vec<(u16, String)>,
    dur: std::time::Duration,
) -> coprocessor::Response {
    let dag_req = &context.dag_req;
    // Go `handleCopDAGRequest`: an unsupported executor answers `OtherError`.
    if let Some(mpp_exec::MppExecError::Other(message)) = &run.err {
        if message.starts_with(mpp::ERR_EXECUTOR_NOT_SUPPORTED_MSG) {
            return other_error(message);
        }
    }
    let (chunks, counts, ndvs) = if run.err.is_some() {
        let chunks = EncodedChunks {
            chunks: Vec::new(),
            encode_type: dag_req.encode_type(),
        };
        (chunks, Vec::new(), Vec::new())
    } else {
        match encode_chunks(&run.chunks, run.exec.get_field_types(), context) {
            Ok(chunks) => (chunks, run.counts, run.ndvs),
            Err(message) => return other_error(&message),
        }
    };
    let error = run.err.as_ref().map(|err| match err {
        mpp_exec::MppExecError::Exec(error) => error.to_pb_error(),
        mpp_exec::MppExecError::Locked(lock) => tipb::Error {
            code: Some(1),
            msg: Some(locked_error_message(lock)),
        },
        mpp_exec::MppExecError::Other(message) | mpp_exec::MppExecError::Conflict(message) => {
            tipb::Error {
                code: Some(1),
                msg: Some(message.clone()),
            }
        }
    });
    let mut sel_resp = tipb::SelectResponse {
        error,
        chunks: chunks.chunks,
        output_counts: counts,
        ndvs,
        encode_type: Some(chunks.encode_type as i32),
        ..tipb::SelectResponse::default()
    };
    let mut mpp_execs = Vec::with_capacity(dag_req.executors.len());
    flatten_mpp_exec(run.exec.as_ref(), &mut mpp_execs);
    if dag_req.collect_execution_summaries() {
        let time_processed = u64::try_from(dur.as_nanos()).unwrap_or(u64::MAX);
        sel_resp.execution_summaries = (0..dag_req.executors.len())
            .map(|i| {
                let mut summary = mpp_execs[i].build_summary();
                summary.time_processed_ns = Some(time_processed);
                summary
            })
            .collect();
    }
    sel_resp.warnings = warnings
        .into_iter()
        .map(|(code, message)| tipb::Error {
            code: Some(i32::from(code)),
            msg: Some(message),
        })
        .collect();
    let mut resp = coprocessor::Response {
        range: run.last_range,
        ..coprocessor::Response::default()
    };
    // Go `genRespWithMPPExec`'s tail: a conflict is also the `OtherError`.
    if let Some(mpp_exec::MppExecError::Conflict(message)) = &run.err {
        resp.other_error = message.clone();
    }
    if let Some(mpp_exec::MppExecError::Locked(lock)) = &run.err {
        resp.locked = Some(tidb_proto::kvrpcpb::LockInfo {
            key: lock.key.clone(),
            primary_lock: lock.primary_lock.clone(),
            lock_version: lock.lock_version,
            lock_ttl: lock.lock_ttl,
            ..tidb_proto::kvrpcpb::LockInfo::default()
        });
    }
    let time_detail = tidb_proto::kvrpcpb::TimeDetail {
        process_wall_time_ms: u64::try_from(dur.as_millis()).unwrap_or(u64::MAX),
        ..tidb_proto::kvrpcpb::TimeDetail::default()
    };
    resp.exec_details = Some(tidb_proto::kvrpcpb::ExecDetails {
        time_detail: Some(time_detail),
        ..tidb_proto::kvrpcpb::ExecDetails::default()
    });
    let processed_versions = mpp_execs
        .iter()
        .filter_map(|exec| exec.scan_row_cnt())
        .map(|row_cnt| row_cnt as u64)
        .sum();
    resp.exec_details_v2 = Some(tidb_proto::kvrpcpb::ExecDetailsV2 {
        time_detail: Some(time_detail),
        scan_detail_v2: Some(tidb_proto::kvrpcpb::ScanDetailV2 {
            processed_versions,
            ..tidb_proto::kvrpcpb::ScanDetailV2::default()
        }),
        ..tidb_proto::kvrpcpb::ExecDetailsV2::default()
    });
    resp.data = sel_resp.encode_to_vec().into();
    resp
}

/// The encoded chunks of a response and the encoding they carry.
struct EncodedChunks {
    chunks: Vec<tipb::Chunk>,
    encode_type: tipb::EncodeType,
}

/// Go `encodeChunk` over every chunk the root produced, each picked by
/// `output_offsets`. `TypeChunk` (`useChunkEncoding`) carries each value as
/// decoded -- a DECIMAL keeps its stored words, which the default row
/// encoding would round to the field type's fraction on the TiDB side
/// (`DecodeOneToChunk`).
///
/// Go's decoder fills the chunk with typed values. A value this tier decoded
/// untyped (a stored default read back as its raw form) fits no column
/// layout, so such a response keeps the row encoding, which the TiDB side
/// types on decode.
fn encode_chunks(
    chunks: &[mpp_exec::Chunk],
    fields: &[tidb_datatype::FieldType],
    context: &DagContext,
) -> Result<EncodedChunks, String> {
    let output_offsets = &context.dag_req.output_offsets;
    let project = |row: &mpp_exec::Row| -> Result<mpp_exec::Row, String> {
        if output_offsets.is_empty() {
            return Ok(row.clone());
        }
        output_offsets
            .iter()
            .map(|offset| {
                row.get(*offset as usize).cloned().ok_or_else(|| {
                    format!("output offset {offset} is outside the executor schema")
                })
            })
            .collect()
    };
    let chunks = chunks
        .iter()
        .map(|chk| chk.iter().map(project).collect::<Result<Vec<_>, _>>())
        .collect::<Result<Vec<_>, _>>()?;
    let output_fields: Option<Vec<tidb_datatype::FieldType>> = if output_offsets.is_empty() {
        Some(fields.to_vec())
    } else {
        output_offsets
            .iter()
            .map(|offset| fields.get(*offset as usize).cloned())
            .collect()
    };
    if context.dag_req.encode_type() == tipb::EncodeType::TypeChunk {
        if let Some(fields) = output_fields.filter(|fields| {
            chunks.iter().flatten().all(|row| {
                row.len() == fields.len()
                    && row
                        .iter()
                        .zip(fields)
                        .all(|(value, field_type)| datum_fits_chunk_column(value, field_type))
            })
        }) {
            return Ok(EncodedChunks {
                chunks: chunks
                    .iter()
                    .map(|chk| use_chunk_encoding(chk, &fields))
                    .collect(),
                encode_type: tipb::EncodeType::TypeChunk,
            });
        }
    }
    let mut encoded = Vec::new();
    for chk in &chunks {
        use_default_encoding(chk, &context.expression_context, &mut encoded)?;
    }
    Ok(EncodedChunks {
        chunks: encoded,
        encode_type: tipb::EncodeType::TypeDefault,
    })
}

/// Go `useDefaultEncoding`: each row datum-encoded in the request zone, a
/// new `tipb.Chunk` every `rowsPerChunk` rows of the executor chunk
/// (`appendRow`).
///
/// A table/index decoder has already converted TIMESTAMP from UTC into the
/// request location, so the encoder must flatten it back to UTC before the
/// TiDB-side default-row decoder applies that same location.
pub(crate) fn use_default_encoding(
    chk: &[mpp_exec::Row],
    sctx: &RequestEvalContext,
    chunks: &mut Vec<tipb::Chunk>,
) -> Result<(), String> {
    use tidb_exprctx::Columns;
    for rows in chk.chunks(ROWS_PER_CHUNK) {
        let mut rows_data = Vec::new();
        for row in rows {
            let (buf, err) = tidb_codec::encode_value_in_timezone_with_error(&sctx.zone, row);
            // Go `err = errCtx.HandleError(err)`: a truncated decimal follows
            // the request's truncate level.
            match err {
                Ok(()) => {}
                Err(tidb_codec::CodecError::DecimalTruncated) => sctx
                    .handle_truncate_error(tidb_datatype::types_errors::ERR_TRUNCATED.gen_with_stack_by_args(&[]))
                    .map_err(|error| format!("encode row failed: {error:?}"))?,
                Err(error) => return Err(format!("encode row failed: {error:?}")),
            }
            rows_data.extend_from_slice(&buf);
        }
        chunks.push(tipb::Chunk {
            rows_data: Some(rows_data.into()),
            ..tipb::Chunk::default()
        });
    }
    Ok(())
}

/// Go `useChunkEncoding`: the executor chunk in TiDB's chunk codec.
fn use_chunk_encoding(chk: &[mpp_exec::Row], fields: &[tidb_datatype::FieldType]) -> tipb::Chunk {
    let codec = tidb_chunk::codec::Codec::new(fields.to_vec());
    let mut chunk = tidb_chunk::chunk::Chunk::new_chunk_with_capacity(fields, chk.len());
    for row in chk {
        for (column, value) in row.iter().enumerate() {
            chunk.append_datum(column, value);
        }
    }
    tipb::Chunk {
        rows_data: Some(codec.encode(&chunk).into()),
        ..tipb::Chunk::default()
    }
}

/// Go `rowsPerChunk`.
const ROWS_PER_CHUNK: usize = 64;

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

/// Go's live `aggExec` shares typed group evaluation and first-seen group
/// order between TypeAggregation and TypeStreamAgg. The deprecated protobuf
/// `Aggregation.streamed` field does not select another grouping lifecycle.
struct RegionAggregator {
    inner: tidb_expression_aggregation::DistAggregator,
    context: Arc<RequestEvalContext>,
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
                tidb_expression_aggregation::DistAggregate::from_pb(
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
            inner: tidb_expression_aggregation::DistAggregator::new(
                group_by,
                functions,
                &context.column_types,
            ),
            context: Arc::clone(context),
        })
    }

    fn update(&mut self, row: &[tidb_datatype::Datum]) -> Result<(), String> {
        self.inner.update(self.context.as_ref(), &self.context.zone, row)
    }

    fn finish(self) -> Vec<Vec<tidb_datatype::Datum>> {
        self.inner.finish()
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
        SimpleExpr::Decimal(value) => Ok(Datum::Decimal(value.clone())),
        SimpleExpr::Json(value) => Ok(Datum::Json(value.clone())),
        SimpleExpr::Real(value) => Ok(Datum::Real(*value)),
        SimpleExpr::Time(value) => Ok(Datum::Time(*value)),
        SimpleExpr::Null => Ok(Datum::Null),
        SimpleExpr::Shared(shared) => eval_shared(shared, row).map_err(String::from),
    }
}

/// Go `buildTopN`'s `topNHeap` and order-by `conds` (`mpp.go`): the
/// pushed-down bounded sort -- order-by keys with their directions and the
/// heap size.
struct TopNSpec {
    keys: Vec<(SimpleExpr, bool, tidb_charset::Collation)>,
    limit: usize,
}

impl TopNSpec {
    /// Go `buildTopN`'s `convertToExprs` over the order-by items (`mpp.go`).
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
                tidb_charset::Collation::from_name(&tidb_datatype::proto_to_collation(restored))
                    .unwrap_or(tidb_charset::Collation::Binary);
            keys.push((expr, item.desc(), collation));
        }
        Ok(Self {
            keys,
            limit: top_n.limit() as usize,
        })
    }

    /// Go `topNExec.open`'s key evaluation (`mpp_exec.go`):
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
                    | Datum::Duration(_)
                    | Datum::Enum(..)
                    | Datum::Set(..) => Ok(datum),
                    _ => Err("ordering over this datum kind is a later course".to_owned()),
                }
            })
            .collect()
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
/// survive -- and `topNExec.open` (`mpp_exec.go`) replays the
/// survivors through `sort.Sort` (insertion sort for n <= 12, stable in
/// effect). Buffer-then-truncate reshuffles ties and can drop the wrong tied
/// rows (oracle g-group: `ORDER BY g LIMIT 100`).
fn go_topn_heap_finish(
    by_items: &[(SimpleExpr, bool, tidb_charset::Collation)],
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
    tidb_goruntime::go_pdqsort::go_sort_func_by(&mut sorted, |a, b| ordering(&a.1, &b.1));
    sorted
}

fn compare_sort_keys(
    left: &tidb_datatype::Datum,
    right: &tidb_datatype::Datum,
    collation: &tidb_charset::Collation,
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

/// Go `ExecutorListsToTree` (`cop_handler.go`): the flat executor list
/// hung into a tree, each executor the child of the one its `parent_idx`
/// names (the next one when unset); the last executor is the root.
///
/// Panics carry Go's exact `invalid parentIdx` and `unsupported dag parent
/// executor type` messages. An empty list, which Go indexes out of range,
/// answers an error.
pub fn executor_lists_to_tree(exec: &[tipb::Executor]) -> Result<tipb::Executor, String> {
    fn append_child(parent: &mut tipb::Executor, child: tipb::Executor) {
        let tp = parent.tp();
        let missing = || -> ! { panic!("dag parent executor {tp:?} has no body") };
        let child = Some(Box::new(child));
        match tp {
            tipb::ExecType::TypeAggregation | tipb::ExecType::TypeStreamAgg => {
                parent.aggregation.as_mut().unwrap_or_else(|| missing()).child = child;
            }
            tipb::ExecType::TypeProjection => {
                parent.projection.as_mut().unwrap_or_else(|| missing()).child = child;
            }
            tipb::ExecType::TypeTopN => {
                parent.top_n.as_mut().unwrap_or_else(|| missing()).child = child;
            }
            tipb::ExecType::TypeLimit => {
                parent.limit.as_mut().unwrap_or_else(|| missing()).child = child;
            }
            tipb::ExecType::TypeSelection => {
                parent.selection.as_mut().unwrap_or_else(|| missing()).child = child;
            }
            tipb::ExecType::TypeIndexLookUp => {
                let child = *child.expect("the child");
                parent
                    .index_lookup
                    .as_mut()
                    .unwrap_or_else(|| missing())
                    .children
                    .push(child);
            }
            _ => panic!("unsupported dag parent executor type: {tp:?}"),
        }
    }

    if exec.is_empty() {
        return Err("the DAG request has no executor".to_owned());
    }
    let mut nodes: Vec<Option<tipb::Executor>> = exec.iter().cloned().map(Some).collect();
    for i in 0..exec.len() - 1 {
        let parent_idx = exec[i]
            .parent_idx
            .map_or(i + 1, |idx| usize::try_from(idx).unwrap_or(usize::MAX));
        assert!(
            parent_idx > i && parent_idx < exec.len(),
            "invalid parentIdx: {parent_idx}, for index: {i}"
        );
        // Every child of `i` has a smaller index, so it is already attached.
        let child = nodes[i].take().expect("each executor is attached once");
        append_child(
            nodes[parent_idx].as_mut().expect("a parent is attached after its children"),
            child,
        );
    }
    Ok(nodes.pop().flatten().expect("the root executor"))
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
    let row = tidb_chunk::mutrow::MutRow::mut_row_from_datums(row);
    expr.expression
        .eval(expr.context.as_ref(), row.to_row())
        .map_err(CopExecError::from)
}

/// Decode without a DAG for standalone callers. Request execution uses
/// `convert_expr_with_context` to retain its own settings and scan types.
pub fn convert_expr(expr: &tipb::Expr) -> Result<SimpleExpr, String> {
    use tidb_exprctx::Columns;
    let context = Arc::new(RequestEvalContext::new(
        tidb_exprctx::NoColumns.time_zone(),
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
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
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

/// One condition of Go `selExec`'s filter over one row, Go `EvalBool`: a
/// NULL rejects the row and any other value is its `Datum.ToBool`.
pub(crate) fn condition_holds(
    condition: &SimpleExpr,
    row: &[tidb_datatype::Datum],
    sctx: &RequestEvalContext,
) -> Result<bool, CopExecError> {
    // A builtin condition's evaluation error keeps its terror code for
    // `SelectResponse.error`.
    let value = match condition {
        SimpleExpr::Shared(shared) => eval_shared(shared, row)?,
        leaf => eval_datum(leaf, row)?,
    };
    if value.is_null() {
        return Ok(false);
    }
    Ok(sctx.condition_value(&value)? != 0)
}

/// A pushed-down expression's value over one row as a filter reads it:
/// MySQL's three-valued int, with NULL as `None`.
#[cfg(test)]
pub fn eval_expr(expr: &SimpleExpr, row: &[tidb_datatype::Datum]) -> Result<Option<i128>, String> {
    use tidb_datatype::Datum;
    let value = match expr {
        SimpleExpr::Shared(shared) => match eval_shared(shared, row)? {
            Datum::Int(value) => Some(i128::from(value)),
            Datum::UInt(value) => Some(i128::from(value)),
            Datum::Null => None,
            other => Some(shared.context.condition_value(&other)?),
        },
        leaf => match eval_datum(leaf, row)? {
            Datum::Int(value) => Some(i128::from(value)),
            Datum::UInt(value) => Some(i128::from(value)),
            _ => None,
        },
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
    use tidb_unistore_tikv::mvcc_store::MvccStore;

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
        // wire-visible rejection, with Go's `%v` byte-slice rendering. The
        // scan's builder (`buildMPPTableScan`) extracts the ranges.
        let mut store = MvccStore::new();
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

    /// Go `ExecutorListsToTree`: each executor becomes its parent's child,
    /// the last one the root.
    #[test]
    fn a_leaf_first_list_hangs_into_a_tree() {
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        let selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            selection: Some(Box::default()),
            ..tipb::Executor::default()
        };
        let limit = tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            limit: Some(Box::default()),
            ..tipb::Executor::default()
        };
        let root = executor_lists_to_tree(&[scan, selection, limit]).expect("a tree");
        assert_eq!(root.tp(), tipb::ExecType::TypeLimit);
        let selection = root.limit.unwrap().child.expect("the limit's child");
        assert_eq!(selection.tp(), tipb::ExecType::TypeSelection);
        let scan = selection.selection.unwrap().child.expect("the selection's child");
        assert_eq!(scan.tp(), tipb::ExecType::TypeTableScan);
    }

    #[test]
    #[should_panic(expected = "invalid parentIdx: 0, for index: 1")]
    fn a_backward_parent_index_panics_with_gos_message() {
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        let mut selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            selection: Some(Box::default()),
            ..tipb::Executor::default()
        };
        selection.parent_idx = Some(0);
        let limit = tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            limit: Some(Box::default()),
            ..tipb::Executor::default()
        };
        let _ = executor_lists_to_tree(&[scan, selection, limit]);
    }

    /// Go `appendChild`'s default arm: a scan never parents an executor.
    #[test]
    #[should_panic(expected = "unsupported dag parent executor type: TypeTableScan")]
    fn a_scan_parent_panics_with_gos_message() {
        let selection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            selection: Some(Box::default()),
            ..tipb::Executor::default()
        };
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            ..tipb::Executor::default()
        };
        let _ = executor_lists_to_tree(&[selection, scan]);
    }

    #[test]
    fn a_table_scan_reads_back_what_a_transaction_wrote() {
        // The milestone the store roadmap aims at: a row written through the
        // TRANSACTION path, read back through the COPROCESSOR path — the
        // exact two protocols a TiDB node speaks to its store, end to end
        // in-process. The row value is the OLD row format (per-column
        // `EncodeInt(colID) ++ EncodeDatum(value)`), which
        // `decode_table_row_to_map` handles exactly as Go's decoder does.
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
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
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
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
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
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
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
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
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
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
            .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Put as i32,
                    key: key.clone(),
                    value,
                    ..KvrpcMutation::default()
                }],
                primary_lock: key.clone(),
                start_version: 10,
                ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
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
            eval_expr(&converted, &[Datum::new_string_datum(value.to_owned())]).expect("evals")
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
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        let key = encode_row_key_with_handle(7, &RecordHandle::Int(1));
        store
            .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Put as i32,
                    key: key.clone(),
                    value: b"v".to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: key,
                start_version: 5,
                ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(7);
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

    /// Go `genRespWithMPPExec`: an executor error that is not a lock -- here
    /// a row the scan cannot decode -- travels in `SelectResponse.error`
    /// (`toPBError`: code 1 and the error's text), not as the response's
    /// `OtherError`.
    #[test]
    fn a_row_decode_error_travels_in_the_select_response() {
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        let key = encode_row_key_with_handle(9, &RecordHandle::Int(1));
        store
            .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Put as i32,
                    key: key.clone(),
                    value: vec![0xff, 0x01, 0x02],
                    ..KvrpcMutation::default()
                }],
                primary_lock: key.clone(),
                start_version: 10,
                ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
            })
            .expect("prewrites");
        store.commit(&[key], 10, 11).expect("commits");
        let dag = tipb::DagRequest {
            executors: vec![tipb::Executor {
                tp: Some(tipb::ExecType::TypeTableScan as i32),
                tbl_scan: Some(tipb::TableScan {
                    table_id: Some(9),
                    columns: vec![tipb::ColumnInfo {
                        column_id: Some(2),
                        tp: Some(8),
                        ..tipb::ColumnInfo::default()
                    }],
                    ..tipb::TableScan::default()
                }),
                ..tipb::Executor::default()
            }],
            ..tipb::DagRequest::default()
        };
        let mut data = Vec::new();
        dag.encode(&mut data).expect("encodes");
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(9);
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
        let error = select.error.expect("the decode error is the response's");
        assert_eq!(error.code, Some(1), "{error:?}");
        assert!(select.chunks.is_empty());
    }

    #[test]
    fn a_limit_above_the_scan_breaks_at_the_count() {
        // Go `ce.limit`: nothing but a break during the scan.
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        for handle in 1..=3_i64 {
            let key = encode_row_key_with_handle(9, &RecordHandle::Int(handle));
            let value =
                tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(handle * 10)]).expect("row");
            store
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(9);
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
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
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
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(11);
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
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
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
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
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
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
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
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
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
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        for (handle, b_value) in [(1_i64, 30_i64), (2, 10), (3, 20)] {
            let key = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
            let value =
                tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(b_value)]).expect("row");
            store
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
        let (range_start, range_end) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
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
        use tidb_tablecodec::table_key::{encode_row_key_with_handle, RecordHandle};
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
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
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
    /// Go `useDefaultEncoding` + `appendRow`: a new `tipb.Chunk` every
    /// `rowsPerChunk` (64) rows of one executor chunk.
    #[test]
    fn use_default_encoding_cuts_a_chunk_every_64_rows() {
        let rows = (0..65)
            .map(|value| vec![tidb_datatype::Datum::Int(value)])
            .collect::<Vec<_>>();
        let mut chunks = Vec::new();
        let sctx = RequestEvalContext::new(zone(), 4, 0);
        use_default_encoding(&rows, &sctx, &mut chunks).expect("encodes");
        assert_eq!(chunks.len(), 2);
        let first = tidb_codec::decode(chunks[0].rows_data.as_deref().expect("rows"), 64)
            .expect("64 datums");
        assert_eq!(first.last(), Some(&tidb_datatype::Datum::Int(63)));
        let second = tidb_codec::decode(chunks[1].rows_data.as_deref().expect("rows"), 1)
            .expect("one datum");
        assert_eq!(second, vec![tidb_datatype::Datum::Int(64)]);
    }
}
