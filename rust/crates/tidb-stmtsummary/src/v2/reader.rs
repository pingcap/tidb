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

//! Go `v2/reader.go`, ported whole: [`MemReader`] for the in-memory window,
//! [`HistoryReader`] plus its scan/parse pipeline for persisted log files,
//! and the shared [`StmtChecker`].
//!
//! Go drives the pipeline with goroutines, an unbuffered file channel (so the
//! manager can never accumulate open file handles) and `select`s over
//! contexts. The Rust port keeps the same roles and channel capacities, with
//! `std::sync::mpsc` channels behind shared receivers, unbuffered
//! (`sync_channel(0)`) file dispatch, and a cooperative [`CancelToken`] in
//! place of `context.Context`; the supervisor polls the inner error channel
//! the way Go's monitor `select`s on it.
//!
//! Log lines are the JSON documents [`crate::v2::record`] writes, so the
//! parser maps the same key names back. Like Go's `encoding/json`, unknown
//! keys are ignored, `null` leaves a field at its zero value, and a mistyped
//! value errors the whole line (the caller then skips it).

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use tidb_datatype::Datum;
use tidb_model::ColumnInfo;
use tidb_parser::auth::UserIdentity;

use crate::v2::column::{make_column_factories, ColumnFactory, ColumnInfoSource};
use crate::v2::record::{nanos_to_duration, StmtRecord};
use crate::v2::stmtsummary::{time_now, LockedStmtRecord, StmtSummary, StmtWindow};

/// Go `logFileTimeFormat`: depends on lumberjack's `backupTimeFormat`.
const LOG_FILE_TIME_FORMAT: &str = "%Y-%m-%dT%H-%M-%S%.3f";
/// Go `maxLineSize`: 1 GiB.
const MAX_LINE_SIZE: usize = 1_073_741_824;
/// Go `batchScanSize`.
const BATCH_SCAN_SIZE: usize = 64;

/// How often the blocking-side loops re-check the cancel token, standing in
/// for Go's `select` over `ctx.Done()`.
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Go `StmtTimeRange`: the time range type used in the stmtsummary package.
/// `[Begin, End)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StmtTimeRange {
    pub begin: i64,
    pub end: i64,
}

/// A minimal stand-in for Go's `context.WithCancel`: a flag workers poll.
#[derive(Clone, Debug, Default)]
pub(crate) struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Go `ctx.Done()` being closed.
    pub(crate) fn is_done(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Go `cancel()`.
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Go `stmtChecker`.
#[derive(Clone, Default)]
pub(crate) struct StmtChecker {
    pub(crate) user: Option<UserIdentity>,
    /// If the user has the `PROCESS` privilege, they can read all statements.
    pub(crate) has_process_priv: bool,
    /// `None` is Go's nil set: no digest filter.
    pub(crate) digests: Option<HashSet<String>>,
    pub(crate) time_ranges: Vec<StmtTimeRange>,
}

impl StmtChecker {
    /// Go `(*stmtChecker).hasPrivilege`.
    pub(crate) fn has_privilege(&self, auth_users: &HashSet<String>) -> bool {
        if let Some(user) = &self.user {
            if !self.has_process_priv {
                if auth_users.is_empty() {
                    return false;
                }
                return auth_users.contains(&user.username);
            }
        }
        true
    }

    /// Go `(*stmtChecker).isDigestValid`.
    pub(crate) fn is_digest_valid(&self, digest: &str) -> bool {
        match &self.digests {
            None => true,
            Some(digests) => digests.contains(digest),
        }
    }

    /// Go `(*stmtChecker).isTimeValid`.
    pub(crate) fn is_time_valid(&self, begin: i64, end: i64) -> bool {
        if self.time_ranges.is_empty() {
            return true;
        }
        self.time_ranges
            .iter()
            .any(|tr| time_range_overlap(begin, end, tr.begin, tr.end))
    }

    /// Go `(*stmtChecker).needStop`.
    pub(crate) fn need_stop(&self, cur_begin: i64) -> bool {
        if self.time_ranges.is_empty() {
            return false;
        }
        self.time_ranges
            .iter()
            .all(|tr| tr.end != 0 && tr.end < cur_begin)
    }
}

/// Go `timeRangeOverlap`, with Go's `[a,b)` open-end normalization.
pub(crate) fn time_range_overlap(
    a_begin: i64,
    mut a_end: i64,
    b_begin: i64,
    mut b_end: i64,
) -> bool {
    if a_end == 0 || a_end < a_begin {
        a_end = i64::MAX;
    }
    if b_end == 0 || b_end < b_begin {
        b_end = i64::MAX;
    }
    a_begin <= b_end && a_end >= b_begin
}

/// Go's `select { case ch <- v: case <-ctx.Done(): }` for a bounded channel:
/// poll `try_send` until it fits or the token cancels.
fn send_with_cancel<T>(
    sender: &SyncSender<T>,
    value: T,
    token: &CancelToken,
    block_on_disconnect: bool,
) {
    loop {
        if token.is_done() {
            return;
        }
        match sender.try_send(value) {
            Ok(()) => return,
            Err(mpsc::TrySendError::Full(v)) => {
                std::thread::sleep(CANCEL_POLL_INTERVAL);
                // Re-arm the loop with the taken-back value.
                // (Assigned through the shadow below.)
                return send_with_cancel(sender, v, token, block_on_disconnect);
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                if block_on_disconnect {
                    return;
                }
                return;
            }
        }
    }
}

/// Go `stmtTinyRecord`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct StmtTinyRecord {
    pub(crate) begin: i64,
    pub(crate) end: i64,
}

/// Go `stmtPersistedRecord`: a `StmtRecord` plus the `evicted` flag, exactly
/// the line shape `StmtLogStorage` writes.
#[derive(Debug, Default)]
pub(crate) struct StmtPersistedRecord {
    pub(crate) record: StmtRecord,
    pub(crate) evicted: bool,
}

fn type_error(field: &str, kind: &str) -> String {
    format!("json: cannot unmarshal {kind} into StmtRecord field {field}")
}

/// Reads an `i64`, mirroring `encoding/json`: absent or `null` keeps the zero
/// value; a JSON number is required otherwise.
fn as_i64(field: &str, value: Option<&serde_json::Value>) -> Result<i64, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(0),
        Some(serde_json::Value::Number(n)) => n.as_i64().ok_or_else(|| type_error(field, "number")),
        Some(_) => Err(type_error(field, "non-number value")),
    }
}

/// Reads a `u64` like Go's unsigned integer fields.
fn as_u64(field: &str, value: Option<&serde_json::Value>) -> Result<u64, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(0),
        Some(serde_json::Value::Number(n)) => n.as_u64().ok_or_else(|| type_error(field, "number")),
        Some(_) => Err(type_error(field, "non-number value")),
    }
}

fn as_i32(field: &str, value: Option<&serde_json::Value>) -> Result<i32, String> {
    let raw = as_i64(field, value)?;
    i32::try_from(raw).map_err(|_| type_error(field, "number out of int32 range"))
}

fn as_u32(field: &str, value: Option<&serde_json::Value>) -> Result<u32, String> {
    let raw = as_u64(field, value)?;
    u32::try_from(raw).map_err(|_| type_error(field, "number out of uint32 range"))
}

fn as_f64(field: &str, value: Option<&serde_json::Value>) -> Result<f64, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(0.0),
        Some(serde_json::Value::Number(n)) => n.as_f64().ok_or_else(|| type_error(field, "number")),
        Some(_) => Err(type_error(field, "non-number value")),
    }
}

fn as_bool(field: &str, value: Option<&serde_json::Value>) -> Result<bool, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(false),
        Some(serde_json::Value::Bool(b)) => Ok(*b),
        Some(_) => Err(type_error(field, "non-bool value")),
    }
}

fn as_string(field: &str, value: Option<&serde_json::Value>) -> Result<String, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(String::new()),
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(_) => Err(type_error(field, "non-string value")),
    }
}

fn as_string_vec(field: &str, value: Option<&serde_json::Value>) -> Result<Vec<String>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(serde_json::Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    serde_json::Value::String(s) => out.push(s.clone()),
                    serde_json::Value::Null => out.push(String::new()),
                    _ => return Err(type_error(field, "non-string element")),
                }
            }
            Ok(out)
        }
        Some(_) => Err(type_error(field, "non-array value")),
    }
}

fn as_string_set(
    field: &str,
    value: Option<&serde_json::Value>,
) -> Result<HashSet<String>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(HashSet::new()),
        Some(serde_json::Value::Object(members)) => Ok(members.keys().cloned().collect()),
        Some(_) => Err(type_error(field, "non-object value")),
    }
}

fn as_i64_map(
    field: &str,
    value: Option<&serde_json::Value>,
) -> Result<HashMap<String, i64>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(HashMap::new()),
        Some(serde_json::Value::Object(members)) => {
            let mut out = HashMap::with_capacity(members.len());
            for (key, item) in members {
                out.insert(key.clone(), as_i64(field, Some(item))?);
            }
            Ok(out)
        }
        Some(_) => Err(type_error(field, "non-object value")),
    }
}

/// Go's `time.Time` unmarshalling: a quoted RFC 3339 document, `null` as a
/// no-op.
fn as_utc_datetime(
    field: &str,
    value: Option<&serde_json::Value>,
) -> Result<DateTime<Utc>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(DateTime::<Utc>::default()),
        Some(serde_json::Value::String(s)) => DateTime::parse_from_rfc3339(s)
            .map(|t| t.with_timezone(&Utc))
            .map_err(|_| type_error(field, "non-RFC3339 string")),
        Some(_) => Err(type_error(field, "non-string value")),
    }
}

/// Applies one JSON member to the record field it names. Unknown keys are
/// ignored, exactly like `encoding/json`.
#[allow(clippy::too_many_lines)]
fn apply_record_field(
    record: &mut StmtRecord,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), String> {
    let member = Some(value);
    match key {
        "begin" => record.begin = as_i64(key, member)?,
        "end" => record.end = as_i64(key, member)?,
        "schema_name" => record.schema_name = as_string(key, member)?,
        "digest" => record.digest = as_string(key, member)?,
        "plan_digest" => record.plan_digest = as_string(key, member)?,
        "stmt_type" => record.stmt_type = as_string(key, member)?,
        "normalized_sql" => record.normalized_sql = as_string(key, member)?,
        "table_names" => record.table_names = as_string(key, member)?,
        "is_internal" => record.is_internal = as_bool(key, member)?,
        "binding_sql" => record.binding_sql = as_string(key, member)?,
        "binding_digest" => record.binding_digest = as_string(key, member)?,
        "sample_sql" => record.sample_sql = as_string(key, member)?,
        "charset" => record.charset = as_string(key, member)?,
        "collation" => record.collation = as_string(key, member)?,
        "prev_sql" => record.prev_sql = as_string(key, member)?,
        "sample_plan" => record.sample_plan = as_string(key, member)?,
        "sample_binary_plan" => record.sample_binary_plan = as_string(key, member)?,
        "plan_hint" => record.plan_hint = as_string(key, member)?,
        "index_names" => record.index_names = as_string_vec(key, member)?,
        "exec_count" => record.exec_count = as_i64(key, member)?,
        "sum_errors" => record.sum_errors = as_i64(key, member)?,
        "sum_warnings" => record.sum_warnings = as_i64(key, member)?,
        "sum_latency" => record.sum_latency = nanos_to_duration(as_i64(key, member)?),
        "max_latency" => record.max_latency = nanos_to_duration(as_i64(key, member)?),
        "min_latency" => record.min_latency = nanos_to_duration(as_i64(key, member)?),
        "sum_parse_latency" => record.sum_parse_latency = nanos_to_duration(as_i64(key, member)?),
        "max_parse_latency" => record.max_parse_latency = nanos_to_duration(as_i64(key, member)?),
        "sum_compile_latency" => {
            record.sum_compile_latency = nanos_to_duration(as_i64(key, member)?);
        }
        "max_compile_latency" => {
            record.max_compile_latency = nanos_to_duration(as_i64(key, member)?);
        }
        "sum_num_cop_tasks" => record.sum_num_cop_tasks = as_i64(key, member)?,
        "max_cop_process_time" => {
            record.max_cop_process_time = nanos_to_duration(as_i64(key, member)?);
        }
        "max_cop_process_address" => record.max_cop_process_address = as_string(key, member)?,
        "max_cop_wait_time" => record.max_cop_wait_time = nanos_to_duration(as_i64(key, member)?),
        "max_cop_wait_address" => record.max_cop_wait_address = as_string(key, member)?,
        "sum_process_time" => record.sum_process_time = nanos_to_duration(as_i64(key, member)?),
        "max_process_time" => record.max_process_time = nanos_to_duration(as_i64(key, member)?),
        "sum_wait_time" => record.sum_wait_time = nanos_to_duration(as_i64(key, member)?),
        "max_wait_time" => record.max_wait_time = nanos_to_duration(as_i64(key, member)?),
        "sum_backoff_time" => record.sum_backoff_time = nanos_to_duration(as_i64(key, member)?),
        "max_backoff_time" => record.max_backoff_time = nanos_to_duration(as_i64(key, member)?),
        "sum_total_keys" => record.sum_total_keys = as_i64(key, member)?,
        "max_total_keys" => record.max_total_keys = as_i64(key, member)?,
        "sum_processed_keys" => record.sum_processed_keys = as_i64(key, member)?,
        "max_processed_keys" => record.max_processed_keys = as_i64(key, member)?,
        "sum_rocksdb_delete_skipped_count" => {
            record.sum_rocksdb_delete_skipped_count = as_u64(key, member)?;
        }
        "max_rocksdb_delete_skipped_count" => {
            record.max_rocksdb_delete_skipped_count = as_u64(key, member)?;
        }
        "sum_rocksdb_key_skipped_count" => {
            record.sum_rocksdb_key_skipped_count = as_u64(key, member)?;
        }
        "max_rocksdb_key_skipped_count" => {
            record.max_rocksdb_key_skipped_count = as_u64(key, member)?;
        }
        "sum_rocksdb_block_cache_hit_count" => {
            record.sum_rocksdb_block_cache_hit_count = as_u64(key, member)?;
        }
        "max_rocksdb_block_cache_hit_count" => {
            record.max_rocksdb_block_cache_hit_count = as_u64(key, member)?;
        }
        "sum_rocksdb_block_read_count" => {
            record.sum_rocksdb_block_read_count = as_u64(key, member)?;
        }
        "max_rocksdb_block_read_count" => {
            record.max_rocksdb_block_read_count = as_u64(key, member)?;
        }
        "sum_rocksdb_block_read_byte" => {
            record.sum_rocksdb_block_read_byte = as_u64(key, member)?;
        }
        "max_rocksdb_block_read_byte" => {
            record.max_rocksdb_block_read_byte = as_u64(key, member)?;
        }
        "ia_remote_exec_count" => record.ia_exec_count = as_i64(key, member)?,
        "sum_ia_remote_read_segment_count" => {
            record.sum_ia_remote_read_segment_count = as_u64(key, member)?;
        }
        "max_ia_remote_read_segment_count" => {
            record.max_ia_remote_read_segment_count = as_u64(key, member)?;
        }
        "sum_ia_remote_read_segment_size" => {
            record.sum_ia_remote_read_segment_size = as_u64(key, member)?;
        }
        "max_ia_remote_read_segment_size" => {
            record.max_ia_remote_read_segment_size = as_u64(key, member)?;
        }
        "sum_ia_remote_read_segment_wait_time" => {
            record.sum_ia_remote_read_segment_wait_time = nanos_to_duration(as_i64(key, member)?);
        }
        "max_ia_remote_read_segment_wait_time" => {
            record.max_ia_remote_read_segment_wait_time = nanos_to_duration(as_i64(key, member)?);
        }
        "commit_count" => record.commit_count = as_i64(key, member)?,
        "sum_get_commit_ts_time" => {
            record.sum_get_commit_ts_time = nanos_to_duration(as_i64(key, member)?);
        }
        "max_get_commit_ts_time" => {
            record.max_get_commit_ts_time = nanos_to_duration(as_i64(key, member)?);
        }
        "sum_prewrite_time" => record.sum_prewrite_time = nanos_to_duration(as_i64(key, member)?),
        "max_prewrite_time" => record.max_prewrite_time = nanos_to_duration(as_i64(key, member)?),
        "sum_commit_time" => record.sum_commit_time = nanos_to_duration(as_i64(key, member)?),
        "max_commit_time" => record.max_commit_time = nanos_to_duration(as_i64(key, member)?),
        "sum_local_latch_time" => {
            record.sum_local_latch_time = nanos_to_duration(as_i64(key, member)?);
        }
        "max_local_latch_time" => {
            record.max_local_latch_time = nanos_to_duration(as_i64(key, member)?);
        }
        "sum_commit_backoff_time" => record.sum_commit_backoff_time = as_i64(key, member)?,
        "max_commit_backoff_time" => record.max_commit_backoff_time = as_i64(key, member)?,
        "sum_resolve_lock_time" => record.sum_resolve_lock_time = as_i64(key, member)?,
        "max_resolve_lock_time" => record.max_resolve_lock_time = as_i64(key, member)?,
        "sum_write_keys" => record.sum_write_keys = as_i64(key, member)?,
        "max_write_keys" => record.max_write_keys = as_i64(key, member)?,
        "sum_write_size" => record.sum_write_size = as_i64(key, member)?,
        "max_write_size" => record.max_write_size = as_i64(key, member)?,
        "sum_prewrite_region_num" => record.sum_prewrite_region_num = as_i64(key, member)?,
        "max_prewrite_region_num" => record.max_prewrite_region_num = as_i32(key, member)?,
        "sum_txn_retry" => record.sum_txn_retry = as_i64(key, member)?,
        "max_txn_retry" => record.max_txn_retry = as_i64(key, member)?,
        "sum_backoff_times" => record.sum_backoff_times = as_i64(key, member)?,
        "backoff_types" => record.backoff_types = as_i64_map(key, member)?,
        "auth_users" => record.auth_users = as_string_set(key, member)?,
        "sum_mem" => record.sum_mem = as_i64(key, member)?,
        "max_mem" => record.max_mem = as_i64(key, member)?,
        "sum_disk" => record.sum_disk = as_i64(key, member)?,
        "max_disk" => record.max_disk = as_i64(key, member)?,
        "sum_affected_rows" => record.sum_affected_rows = as_u64(key, member)?,
        "sum_kv_total" => record.sum_kv_total = nanos_to_duration(as_i64(key, member)?),
        "sum_pd_total" => record.sum_pd_total = nanos_to_duration(as_i64(key, member)?),
        "sum_backoff_total" => record.sum_backoff_total = nanos_to_duration(as_i64(key, member)?),
        "sum_write_sql_resp_total" => {
            record.sum_write_sql_resp_total = nanos_to_duration(as_i64(key, member)?);
        }
        "sum_tidb_cpu" => record.sum_tidb_cpu = as_i64(key, member)?,
        "sum_tikv_cpu" => record.sum_tikv_cpu = as_i64(key, member)?,
        "sum_result_rows" => record.sum_result_rows = as_i64(key, member)?,
        "max_result_rows" => record.max_result_rows = as_i64(key, member)?,
        "min_result_rows" => record.min_result_rows = as_i64(key, member)?,
        "prepared" => record.prepared = as_bool(key, member)?,
        "first_seen" => record.first_seen = as_utc_datetime(key, member)?,
        "last_seen" => record.last_seen = as_utc_datetime(key, member)?,
        "plan_in_cache" => record.plan_in_cache = as_bool(key, member)?,
        "plan_cache_hits" => record.plan_cache_hits = as_i64(key, member)?,
        "plan_in_binding" => record.plan_in_binding = as_bool(key, member)?,
        "exec_retry_count" => record.exec_retry_count = as_u64(key, member)?,
        "exec_retry_time" => record.exec_retry_time = nanos_to_duration(as_i64(key, member)?),
        "keyspace_name" => record.keyspace_name = as_string(key, member)?,
        "keyspace_id" => record.keyspace_id = as_u32(key, member)?,
        "resource_group_name" => record.resource_group_name = as_string(key, member)?,
        "sum_rru" => record.ru.sum_rru = as_f64(key, member)?,
        "sum_wru" => record.ru.sum_wru = as_f64(key, member)?,
        "sum_ru_wait_duration" => {
            record.ru.sum_ru_wait_duration = nanos_to_duration(as_i64(key, member)?);
        }
        "max_rru" => record.ru.max_rru = as_f64(key, member)?,
        "max_wru" => record.ru.max_wru = as_f64(key, member)?,
        "max_ru_wait_duration" => {
            record.ru.max_ru_wait_duration = nanos_to_duration(as_i64(key, member)?);
        }
        "sum_ruv2" => record.ru.sum_ru_v2 = as_f64(key, member)?,
        "max_ruv2" => record.ru.max_ru_v2 = as_f64(key, member)?,
        "plan_cache_unqualified_count" => {
            record.plan_cache_unqualified_count = as_i64(key, member)?;
        }
        "plan_cache_unqualified_last_reason" => {
            record.plan_cache_unqualified_last_reason = as_string(key, member)?;
        }
        "sum_mem_arbitration" => record.sum_mem_arbitration = as_f64(key, member)?,
        "max_mem_arbitration" => record.max_mem_arbitration = as_f64(key, member)?,
        "unpacked_bytes_send_tikv_total" => {
            record.network.unpacked_bytes_sent_tikv_total = as_i64(key, member)?;
        }
        "unpacked_bytes_received_tikv_total" => {
            record.network.unpacked_bytes_received_tikv_total = as_i64(key, member)?;
        }
        "unpacked_bytes_send_tikv_cross_zone" => {
            record.network.unpacked_bytes_sent_tikv_cross_zone = as_i64(key, member)?;
        }
        "unpacked_bytes_received_tikv_cross_zone" => {
            record.network.unpacked_bytes_received_tikv_cross_zone = as_i64(key, member)?;
        }
        "unpacked_bytes_send_tiflash_total" => {
            record.network.unpacked_bytes_sent_tiflash_total = as_i64(key, member)?;
        }
        "unpacked_bytes_received_tiflash_total" => {
            record.network.unpacked_bytes_received_tiflash_total = as_i64(key, member)?;
        }
        "unpacked_bytes_send_tiflash_cross_zone" => {
            record.network.unpacked_bytes_sent_tiflash_cross_zone = as_i64(key, member)?;
        }
        "unpacked_bytes_received_tiflash_cross_zone" => {
            record.network.unpacked_bytes_received_tiflash_cross_zone = as_i64(key, member)?;
        }
        "storage_kv" => record.storage_kv = as_bool(key, member)?,
        "storage_mpp" => record.storage_mpp = as_bool(key, member)?,
        _ => {}
    }
    Ok(())
}

/// Go `json.Unmarshal(line, &stmtTinyRecord)`.
pub(crate) fn unmarshal_tiny_record(raw: &[u8]) -> Result<StmtTinyRecord, String> {
    let value: serde_json::Value =
        serde_json::from_slice(raw).map_err(|error| error.to_string())?;
    let object = value
        .as_object()
        .ok_or_else(|| "json: cannot unmarshal into stmtTinyRecord".to_owned())?;
    Ok(StmtTinyRecord {
        begin: as_i64("begin", object.get("begin"))?,
        end: as_i64("end", object.get("end"))?,
    })
}

/// Go `json.Unmarshal(line, &stmtPersistedRecord)`.
pub(crate) fn unmarshal_persisted_record(raw: &[u8]) -> Result<StmtPersistedRecord, String> {
    let value: serde_json::Value =
        serde_json::from_slice(raw).map_err(|error| error.to_string())?;
    let object = value
        .as_object()
        .ok_or_else(|| "json: cannot unmarshal into stmtPersistedRecord".to_owned())?;
    let mut out = StmtPersistedRecord::default();
    for (key, member) in object {
        if key == "evicted" {
            out.evicted = as_bool(key, Some(member))?;
        } else {
            apply_record_field(&mut out.record, key, member)?;
        }
    }
    Ok(out)
}

/// Go `stmtFile`: one persisted log file plus its parsed `[begin, end)` span.
pub(crate) struct StmtFile {
    path: PathBuf,
    file: Option<std::fs::File>,
    begin: i64,
    end: i64,
}

impl StmtFile {
    /// Go `stmtFile.path`.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// Go `openStmtFile`.
fn open_stmt_file(path: &Path) -> Result<StmtFile, String> {
    let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut file = file;
    let begin = match parse_begin_ts_and_reseek(&mut file) {
        Ok(begin) => begin,
        Err(error) if error == "EOF" => 0,
        Err(error) => {
            return Err(error);
        }
    };
    let end = parse_end_ts(path)?;

    Ok(StmtFile {
        path: path.to_path_buf(),
        file: Some(file),
        begin,
        end,
    })
}

/// Go `parseBeginTsAndReek`: the first valid line's begin, ignoring invalid
/// lines, then back to the start of the file. `Err("EOF")` mirrors Go's
/// `io.EOF` (no valid line at all).
fn parse_begin_ts_and_reseek(file: &mut std::fs::File) -> Result<i64, String> {
    file.seek_read_start()?;
    let mut reader = BufReader::new(&mut *file);
    let mut record = StmtTinyRecord::default();
    loop {
        // ignore invalid lines
        let line = match read_line(&mut reader) {
            Ok(line) => line,
            Err(error) if error == "EOF" => return Err(error),
            Err(error) => return Err(error),
        };
        match unmarshal_tiny_record(&line) {
            Ok(parsed) => {
                record = parsed;
                break;
            }
            Err(_) => continue,
        }
    }
    drop(reader);
    file.seek_read_start()?;
    Ok(record.begin)
}

/// Go `parseEndTs`: the rotated filename's timestamp suffix, or zero for the
/// active file.
fn parse_end_ts(path: &Path) -> Result<i64, String> {
    // tidb-statements.log
    let filename = stmt_summary_filename();
    // .log
    let ext = filepath_ext(&filename);
    // tidb-statements
    let prefix = &filename[..filename.len() - ext.len()];

    // tidb-statements-2022-12-27T16-21-20.245.log
    let name = path
        .file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    // .log
    let name_ext = filepath_ext(&name);
    // tidb-statements-2022-12-27T16-21-20.245
    let stem = &name[..name.len() - name_ext.len()];

    let Some(time_str) = stem.strip_prefix(&format!("{prefix}-")) else {
        return Ok(0);
    };
    // 2022-12-27T16-21-20.245
    let naive = NaiveDateTime::parse_from_str(time_str, LOG_FILE_TIME_FORMAT)
        .map_err(|error| error.to_string())?;
    let local = chrono::Local;
    match local.from_local_datetime(&naive) {
        chrono::LocalResult::Single(value) => Ok(value.timestamp()),
        // Go's ParseInLocation resolves an ambiguous wall clock to the
        // earlier offset.
        chrono::LocalResult::Ambiguous(earliest, _) => Ok(earliest.timestamp()),
        chrono::LocalResult::None => Err(format!("cannot represent {time_str} in the local zone")),
    }
}

impl StmtFile {
    /// Go `(*stmtFile).close`. Rust closes the descriptor on drop, so this
    /// only detaches the handle; it cannot fail where Go's `Close` could.
    fn close(&mut self) -> std::io::Result<()> {
        drop(self.file.take());
        Ok(())
    }

    /// Go `(*stmtFile).closeAndLogError`.
    fn close_and_log_error(&mut self) {
        if let Err(error) = self.close() {
            eprintln!(
                "failed to close statements file [path={:?}] [error={error}]",
                self.path
            );
        }
    }
}

impl Drop for StmtFile {
    fn drop(&mut self) {
        self.close_and_log_error();
    }
}

/// Go `stmtFiles`.
pub(crate) struct StmtFiles {
    pub(crate) files: Vec<StmtFile>,
    /// Go `currentFileInfo` reduced to the `(dev, ino)` pair `os.SameFile`
    /// compares.
    current_file_info: Option<(u64, u64)>,
}

impl StmtFiles {
    pub(crate) fn close(&mut self) {
        for file in &mut self.files {
            file.close_and_log_error();
        }
    }
}

/// Go `newStmtFiles`.
pub(crate) fn new_stmt_files(token: &CancelToken) -> Result<StmtFiles, String> {
    new_stmt_files_with_read_dir(token, read_dir_entries)
}

/// One directory entry reduced to what the walk consumes. Like Go's
/// `os.DirEntry`, the metadata is resolved when the walk asks for it (via
/// `path`), not at enumeration time; `force_info_err` makes that resolution
/// fail, mirroring Go's injected `entry.Info()` error.
pub(crate) struct DirEntryLike {
    pub(crate) name: String,
    pub(crate) is_dir: bool,
    pub(crate) path: PathBuf,
    pub(crate) force_info_err: bool,
}

fn read_dir_entries(dir: &Path) -> std::io::Result<Vec<DirEntryLike>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let is_dir = entry.file_type()?.is_dir();
        entries.push(DirEntryLike {
            name: entry.file_name().to_string_lossy().into_owned(),
            is_dir,
            path: entry.path(),
            force_info_err: false,
        });
    }
    Ok(entries)
}

/// Go `newStmtFilesWithReadDir`: enumerate rotated files around the pinned
/// active inode.
pub(crate) fn new_stmt_files_with_read_dir(
    token: &CancelToken,
    read_dir: impl Fn(&Path) -> std::io::Result<Vec<DirEntryLike>>,
) -> Result<StmtFiles, String> {
    let filename = stmt_summary_filename();
    let ext = filepath_ext(&filename);
    let prefix = &filename[..filename.len() - ext.len()];

    if token.is_done() {
        return Err("context canceled".to_owned());
    }
    // Pin the active inode before enumerating rotated files. If rotation
    // happens during enumeration, the directory entry for this inode is
    // deduplicated below.
    let mut current_file = match open_stmt_file(Path::new(&filename)) {
        Ok(file) => Some(file),
        Err(error) => {
            if !error.contains("No such file or directory") && !error.contains("os error 2") {
                eprintln!(
                    "failed to snapshot current statements file [path={filename}] [error={error}]"
                );
            }
            None
        }
    };

    let mut files: Vec<StmtFile> = Vec::new();
    let mut current_file_info: Option<(u64, u64)> = None;
    if let Some(current) = current_file.as_mut() {
        let file = current.file.as_ref().ok_or("current file not open")?;
        let meta = file.metadata().map_err(|error| error.to_string())?;
        current_file_info = Some((meta.dev(), meta.ino()));
        // `files` owns the pinned handle now; do not close it twice.
        files.push(StmtFile {
            path: current.path.clone(),
            file: current.file.take(),
            begin: current.begin,
            end: current.end,
        });
    }

    let dir = filepath_dir(&filename);
    let entries = {
        let result = read_dir(Path::new(&dir));
        match result {
            Ok(entries) => entries,
            Err(error) => {
                let mut opened = StmtFiles {
                    files,
                    current_file_info: None,
                };
                opened.close();
                return Err(error.to_string());
            }
        }
    };
    if token.is_done() {
        let mut opened = StmtFiles {
            files,
            current_file_info: None,
        };
        opened.close();
        return Err("context canceled".to_owned());
    }

    let mut walk = |path: String, entry: &DirEntryLike| -> Result<(), String> {
        if entry.is_dir {
            return Ok(());
        }
        if !path.starts_with(prefix) {
            return Ok(());
        }
        if token.is_done() {
            return Err("context canceled".to_owned());
        }
        if path == filename {
            if current_file.is_none() {
                files.push(StmtFile {
                    path: PathBuf::from(path),
                    file: None,
                    begin: 0,
                    end: 0,
                });
            }
            return Ok(());
        }
        if let Some(pinned) = current_file_info {
            let info = if entry.force_info_err {
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
            } else {
                std::fs::metadata(&entry.path).map(|meta| (meta.dev(), meta.ino()))
            };
            match info {
                Ok(identity) if identity == pinned => return Ok(()),
                Ok(_) => {}
                // If Info fails, keep the path and deduplicate the opened
                // inode later.
                Err(_) => {}
            }
        }
        files.push(StmtFile {
            path: PathBuf::from(path),
            file: None,
            begin: 0,
            end: 0,
        });
        Ok(())
    };

    for entry in &entries {
        let path = filepath_join(&dir, &entry.name);
        if let Err(error) = walk(path, entry) {
            let mut opened = StmtFiles {
                files,
                current_file_info: None,
            };
            opened.close();
            return Err(error);
        }
    }
    files.sort_by(|i, j| i.path.as_os_str().cmp(j.path.as_os_str()));
    Ok(StmtFiles {
        files,
        current_file_info,
    })
}

/// Go `config.GetGlobalConfig().Instance.StmtSummaryFilename`.
fn stmt_summary_filename() -> String {
    tidb_config::config_tree::config::get_global_config()
        .instance
        .stmt_summary_filename
        .clone()
}

/// Go `filepath.Ext`: the suffix beginning at the final dot in the final
/// slash-separated element, including the dot.
fn filepath_ext(path: &str) -> &str {
    let name = path.rsplit('/').next().unwrap_or(path);
    match name.rfind('.') {
        Some(index) => &name[index..],
        None => "",
    }
}

/// Go `filepath.Dir`.
fn filepath_dir(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".to_owned(),
        Some(index) => path[..index].to_owned(),
        None => ".".to_owned(),
    }
}

/// Go `filepath.Join` for exactly two clean parts.
fn filepath_join(dir: &str, name: &str) -> String {
    if dir == "." {
        return name.to_owned();
    }
    format!("{dir}/{name}")
}

/// Go `stmtScanWorker`.
struct StmtScanWorker {
    token: CancelToken,
    batch_size: usize,
    checker: Arc<StmtChecker>,
}

impl StmtScanWorker {
    fn run(
        &self,
        file_rx: &Mutex<Receiver<StmtFile>>,
        lines_tx: &SyncSender<Vec<Vec<u8>>>,
        err_tx: &SyncSender<String>,
    ) {
        loop {
            let file = {
                // Holding the mutex while blocking is Go's channel semantics:
                // only one worker holds a file at a time, and a close wakes
                // everyone sequentially.
                let receiver = file_rx.lock().expect("files lock poisoned");
                match receiver.recv() {
                    Ok(file) => file,
                    Err(_) => return,
                }
            };
            self.handle_file(file, lines_tx, err_tx);
        }
    }

    fn handle_file(
        &self,
        mut file: StmtFile,
        lines_tx: &SyncSender<Vec<Vec<u8>>>,
        err_tx: &SyncSender<String>,
    ) {
        if file.file.is_none() {
            return;
        }
        let mut reader = BufReader::new(file.file.take().expect("checked above"));
        loop {
            if self.token.is_done() {
                break;
            }
            match self.readlines(&mut reader) {
                Ok(None) => break,
                Ok(Some(lines)) => self.put_lines(lines, lines_tx),
                Err(error) if error == "EOF" => break,
                Err(error) => {
                    self.put_err(error, err_tx);
                    break;
                }
            }
        }
        self.close_and_log_error(&mut file);
    }

    fn put_err(&self, error: String, err_tx: &SyncSender<String>) {
        let _ = err_tx.try_send(error);
    }

    fn put_lines(&self, lines: Vec<Vec<u8>>, lines_tx: &SyncSender<Vec<Vec<u8>>>) {
        // Go: `select { case linesCh <- lines: case <-ctx.Done(): }`.
        send_with_cancel(lines_tx, lines, &self.token, false);
    }

    /// Returns `Ok(None)` for Go's `io.EOF` (file finished or the time range
    /// excludes the rest of it).
    fn readlines(
        &self,
        reader: &mut BufReader<std::fs::File>,
    ) -> Result<Option<Vec<Vec<u8>>>, String> {
        let mut first_line;
        let mut record;
        loop {
            // ingore invalid lines
            first_line = read_line(reader)?;
            match self.parse(&first_line) {
                Ok(parsed) => {
                    record = parsed;
                    break;
                }
                Err(_) => continue,
            }
        }

        if self.need_stop(record) {
            // done because remaining lines in file are not in the time range
            return Ok(None);
        }

        let mut lines = Vec::with_capacity(self.batch_size);
        lines.push(first_line);

        match read_lines(reader, self.batch_size - 1) {
            Ok(new_lines) => lines.extend(new_lines),
            Err(error) if error == "EOF" => return Ok(Some(lines)),
            Err(error) => return Err(error),
        }
        Ok(Some(lines))
    }

    fn parse(&self, raw: &[u8]) -> Result<StmtTinyRecord, String> {
        unmarshal_tiny_record(raw)
    }

    fn need_stop(&self, record: StmtTinyRecord) -> bool {
        self.checker.need_stop(record.begin)
    }

    fn close_and_log_error(&self, file: &mut StmtFile) {
        file.close_and_log_error();
    }
}

/// Go `stmtParseWorker`.
struct StmtParseWorker {
    token: CancelToken,
    instance_addr: String,
    time_location: Tz,
    checker: Arc<StmtChecker>,
    column_factories: Vec<ColumnFactory>,
}

impl ColumnInfoSource for StmtParseWorker {
    fn instance_addr(&self) -> String {
        self.instance_addr.clone()
    }

    fn time_location(&self) -> Tz {
        self.time_location
    }
}

impl StmtParseWorker {
    fn run(
        &self,
        lines_rx: &Mutex<Receiver<Vec<Vec<u8>>>>,
        rows_tx: &SyncSender<Vec<Vec<Datum>>>,
        err_tx: &SyncSender<String>,
    ) {
        loop {
            let lines = {
                let receiver = lines_rx.lock().expect("lines lock poisoned");
                match receiver.recv() {
                    Ok(lines) => lines,
                    Err(_) => return,
                }
            };
            if self.token.is_done() {
                return;
            }
            self.handle_lines(lines, rows_tx, err_tx);
        }
    }

    fn handle_lines(
        &self,
        lines: Vec<Vec<u8>>,
        rows_tx: &SyncSender<Vec<Vec<Datum>>>,
        _err_tx: &SyncSender<String>,
    ) {
        if lines.is_empty() {
            return;
        }

        let mut rows: Vec<Vec<Datum>> = Vec::with_capacity(lines.len());
        for line in &lines {
            let record = match unmarshal_persisted_record(line) {
                Ok(record) => record,
                Err(_) => continue, // ignore invalid lines
            };
            if record.evicted {
                continue;
            }
            let record = record.record;

            if self.need_stop(&record) {
                break;
            }

            if !self.match_conds(&record) {
                continue;
            }

            rows.push(self.build_row(&record));
        }

        if !rows.is_empty() {
            self.put_rows(rows, rows_tx);
        }
    }

    fn put_rows(&self, rows: Vec<Vec<Datum>>, rows_tx: &SyncSender<Vec<Vec<Datum>>>) {
        send_with_cancel(rows_tx, rows, &self.token, false);
    }

    fn need_stop(&self, record: &StmtRecord) -> bool {
        self.checker.need_stop(record.begin)
    }

    fn match_conds(&self, record: &StmtRecord) -> bool {
        if !self.checker.is_time_valid(record.begin, record.end) {
            return false;
        }
        if !self.checker.is_digest_valid(&record.digest) {
            return false;
        }
        if !self.checker.has_privilege(&record.auth_users) {
            return false;
        }
        true
    }

    fn build_row(&self, record: &StmtRecord) -> Vec<Datum> {
        self.column_factories
            .iter()
            .map(|factory| factory(self, record))
            .collect()
    }
}

/// Go `MemReader`: reads the current window's data maintained in memory.
pub struct MemReader {
    s: Option<Arc<StmtSummary>>,
    instance_addr: String,
    time_location: Tz,
    column_factories: Vec<ColumnFactory>,
    checker: Arc<StmtChecker>,
}

/// Go `NewMemReader`.
#[must_use]
pub fn new_mem_reader(
    s: Option<Arc<StmtSummary>>,
    columns: &[ColumnInfo],
    instance_addr: String,
    time_location: Tz,
    user: Option<UserIdentity>,
    has_process_priv: bool,
    digests: Option<HashSet<String>>,
    time_ranges: Vec<StmtTimeRange>,
) -> MemReader {
    MemReader {
        s,
        instance_addr,
        time_location,
        column_factories: make_column_factories(columns),
        checker: Arc::new(StmtChecker {
            user,
            has_process_priv,
            digests,
            time_ranges,
        }),
    }
}

impl ColumnInfoSource for MemReader {
    fn instance_addr(&self) -> String {
        self.instance_addr.clone()
    }

    fn time_location(&self) -> Tz {
        self.time_location
    }
}

impl MemReader {
    /// Go `(*MemReader).Rows`: rows from the current window, with all evicted
    /// data aggregated into one row appended at the end.
    #[must_use]
    pub fn rows(&self) -> Vec<Vec<Datum>> {
        let Some(s) = &self.s else {
            return Vec::new();
        };
        let end = time_now().timestamp();
        let window: Arc<Mutex<StmtWindow>> = s.window();
        let (begin, values, evicted) = {
            let window = window.lock().expect("window lock poisoned");
            if !self.checker.is_time_valid(window.begin.timestamp(), end) {
                return Vec::new();
            }
            (
                window.begin.timestamp(),
                window
                    .lru
                    .values()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<LockedStmtRecord>>(),
                Arc::clone(&window.evicted),
            )
        };
        let mut rows: Vec<Vec<Datum>> = Vec::with_capacity(values.len() + 1);
        for value in values {
            let mut record = value.lock().expect("record lock poisoned");
            if !self.checker.is_digest_valid(&record.digest) {
                continue;
            }
            if !self.checker.has_privilege(&record.auth_users) {
                continue;
            }
            record.begin = begin;
            record.end = end;
            rows.push(self.build_row(&record));
        }
        if self.checker.digests.is_none() {
            let mut evicted = evicted.lock().expect("evicted lock poisoned");
            if evicted.other.exec_count == 0 {
                return rows;
            }
            if !self.checker.has_privilege(&evicted.other.auth_users) {
                return rows;
            }
            evicted.other.begin = begin;
            evicted.other.end = end;
            rows.push(self.build_row(&evicted.other));
        }
        rows
    }

    fn build_row(&self, record: &StmtRecord) -> Vec<Datum> {
        self.column_factories
            .iter()
            .map(|factory| factory(self, record))
            .collect()
    }
}

/// Go `HistoryReader`: reads data that has been persisted to files.
pub struct HistoryReader {
    token: CancelToken,
    supervisor: Option<std::thread::JoinHandle<()>>,
    rows_rx: Receiver<Vec<Vec<Datum>>>,
    err_rx: Receiver<String>,
}

impl HistoryReader {
    /// Go `NewHistoryReader`. If `time_ranges` is present, only files within
    /// the time range are read.
    ///
    /// # Errors
    ///
    /// Returns the file-enumeration error, as Go does.
    pub fn new(
        parent: Option<&CancelToken>,
        columns: &[ColumnInfo],
        instance_addr: String,
        time_location: Tz,
        user: Option<UserIdentity>,
        has_process_priv: bool,
        digests: Option<HashSet<String>>,
        time_ranges: Vec<StmtTimeRange>,
        concurrent: usize,
    ) -> Result<HistoryReader, String> {
        let token = CancelToken::new();
        let files = new_stmt_files(&token)?;

        let concurrent = concurrent.max(2);
        let (rows_tx, rows_rx) = mpsc::sync_channel(concurrent);
        let (err_tx, err_rx) = mpsc::sync_channel(concurrent);

        let checker = Arc::new(StmtChecker {
            user,
            has_process_priv,
            digests,
            time_ranges,
        });

        let column_factories = make_column_factories(columns);
        let supervisor_parent = parent.map_or_else(CancelToken::new, |parent| parent.clone());
        let supervisor = std::thread::Builder::new()
            .name("stmt-history-reader".to_owned())
            .spawn(move || {
                schedule_tasks(
                    supervisor_parent,
                    files,
                    checker,
                    instance_addr,
                    time_location,
                    column_factories,
                    concurrent,
                    rows_tx,
                    err_tx,
                );
            })
            .map_err(|error| error.to_string())?;

        Ok(HistoryReader {
            token,
            supervisor: Some(supervisor),
            rows_rx,
            err_rx,
        })
    }

    /// Go's tests build a `HistoryReader` by struct literal over a caller
    /// supplied `stmtFiles`; this is that constructor, test-only.
    #[cfg(test)]
    pub(crate) fn from_parts(
        parent: Option<&CancelToken>,
        files: StmtFiles,
        instance_addr: String,
        time_location: Tz,
        checker: StmtChecker,
        column_factories: Vec<ColumnFactory>,
        concurrent: usize,
    ) -> HistoryReader {
        let token = CancelToken::new();
        let concurrent = concurrent.max(2);
        let (rows_tx, rows_rx) = mpsc::sync_channel(concurrent);
        let (err_tx, err_rx) = mpsc::sync_channel(concurrent);
        let checker = Arc::new(checker);
        let supervisor_parent = parent.map_or_else(CancelToken::new, |parent| parent.clone());
        let supervisor = std::thread::Builder::new()
            .name("stmt-history-reader".to_owned())
            .spawn(move || {
                schedule_tasks(
                    supervisor_parent,
                    files,
                    checker,
                    instance_addr,
                    time_location,
                    column_factories,
                    concurrent,
                    rows_tx,
                    err_tx,
                );
            })
            .expect("spawn history reader");
        HistoryReader {
            token,
            supervisor: Some(supervisor),
            rows_rx,
            err_rx,
        }
    }

    /// Go `(*HistoryReader).Rows`: rows from records in files. Reading and
    /// parsing work asynchronously. `Ok(None)` means reading has completed.
    ///
    /// # Errors
    ///
    /// Returns the first pipeline error, as Go does.
    pub fn rows(&self) -> Result<Option<Vec<Vec<Datum>>>, String> {
        loop {
            match self.err_rx.try_recv() {
                Ok(error) => return Err(error),
                Err(TryRecvError::Disconnected) => {}
                Err(TryRecvError::Empty) => {}
            }
            match self.rows_rx.recv_timeout(CANCEL_POLL_INTERVAL) {
                Ok(rows) => {
                    if rows.is_empty() {
                        continue;
                    }
                    return Ok(Some(rows));
                }
                Err(RecvTimeoutError::Timeout) => {
                    if self.token.is_done() {
                        // Go selects on ctx.Done(); Close() canceled us.
                        return Err("context canceled".to_owned());
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    // rowsCh closed: surface a queued error or report done.
                    if let Ok(error) = self.err_rx.try_recv() {
                        return Err(error);
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Go `(*HistoryReader).Close`: ends reading and closes all files.
    ///
    /// # Errors
    ///
    /// Always succeeds, like Go.
    pub fn close(&mut self) -> Result<(), String> {
        self.token.cancel();
        if let Some(supervisor) = self.supervisor.take() {
            let _ = supervisor.join();
        }
        Ok(())
    }
}

/// Go `(*HistoryReader).scheduleTasks`, driving the scan/parse pipeline.
///
/// # Pipeline
///
/// ```text
/// .           +--------------+             +---------------+
/// == files => | scan workers | == lines => | parse workers | == rows =>
/// . filesCh   +--------------+   linesCh   +---------------+   rowsCh
/// ```
///
/// # Roles
///
/// - Scan workers (concurrent/2): scan files (I/O) first, then help parse
///   workers to parse lines (CPU).
/// - Parse workers (concurrent - concurrent/2): parse lines (CPU) to rows.
/// - Manager (1): drives the whole process and notifies scan workers to
///   switch role.
/// - Monitor (this thread): covers failures and notifies workers to exit.
#[allow(clippy::too_many_lines)]
fn schedule_tasks(
    parent: CancelToken,
    mut files: StmtFiles,
    checker: Arc<StmtChecker>,
    instance_addr: String,
    time_location: Tz,
    column_factories: Vec<ColumnFactory>,
    concurrent: usize,
    rows_tx: SyncSender<Vec<Vec<Datum>>>,
    err_tx: SyncSender<String>,
) {
    if files.files.is_empty() {
        return;
    }

    let child = CancelToken::new();
    let monitor_token = child.clone();
    let scan_worker = Arc::new(StmtScanWorker {
        token: child.clone(),
        batch_size: BATCH_SCAN_SIZE,
        checker: Arc::clone(&checker),
    });
    let parse_worker = Arc::new(StmtParseWorker {
        token: child.clone(),
        instance_addr,
        time_location,
        checker: Arc::clone(&checker),
        column_factories,
    });

    // Keep the file channel unbuffered so the manager cannot accumulate open
    // file handles.
    let (files_tx, files_rx) = mpsc::sync_channel::<StmtFile>(0);
    let (lines_tx, lines_rx) = mpsc::sync_channel::<Vec<Vec<u8>>>(concurrent);
    let (inner_err_tx, inner_err_rx) = mpsc::sync_channel::<String>(concurrent);
    let files_rx = Arc::new(Mutex::new(files_rx));
    let lines_rx = Arc::new(Mutex::new(lines_rx));

    // Half of the workers are scheduled to scan files and then parse lines;
    // finally ALL workers parse.
    let mut scan_handles = Vec::new();
    let mut parse_handles = Vec::new();
    for _ in 0..concurrent / 2 {
        let files_rx = Arc::clone(&files_rx);
        let lines_rx = Arc::clone(&lines_rx);
        let lines_tx = lines_tx.clone();
        let inner_err_tx = inner_err_tx.clone();
        let rows_tx = rows_tx.clone();
        let scan_worker = Arc::clone(&scan_worker);
        let parse_worker = Arc::clone(&parse_worker);
        scan_handles.push(std::thread::spawn(move || {
            scan_worker.run(&files_rx, &lines_tx, &inner_err_tx);
            // Rust closes a channel by dropping its senders (Go just calls
            // `close(linesCh)`); the worker must release its send side before
            // becoming a parse worker, or the lines channel can never close.
            drop(lines_tx);
            parse_worker.run(&lines_rx, &rows_tx, &inner_err_tx);
        }));
    }
    for _ in concurrent / 2..concurrent {
        let lines_rx = Arc::clone(&lines_rx);
        let inner_err_tx = inner_err_tx.clone();
        let rows_tx = rows_tx.clone();
        let parse_worker = Arc::clone(&parse_worker);
        parse_handles.push(std::thread::spawn(move || {
            parse_worker.run(&lines_rx, &rows_tx, &inner_err_tx);
        }));
    }
    drop(lines_tx);
    drop(inner_err_tx);

    // Manager drives the whole process.
    let manager_token = child.clone();
    let manager = std::thread::Builder::new()
        .name("stmt-history-manager".to_owned())
        .spawn(move || {
            let pinned = files.current_file_info;
            for mut file in std::mem::take(&mut files.files) {
                if manager_token.is_done() {
                    break;
                }
                if file.file.is_none() {
                    let opened = match open_stmt_file(file.path()) {
                        Ok(opened) => opened,
                        Err(error) => {
                            eprintln!(
                                "failed to open or parse statements file [path={:?}] [error={error}]",
                                file.path()
                            );
                            continue;
                        }
                    };
                    // Dedup the pinned active inode (Go `os.SameFile`).
                    if let Some(identity) = pinned {
                        match opened.file.as_ref().map(std::fs::File::metadata) {
                            Some(Ok(meta)) if (meta.dev(), meta.ino()) == identity => {
                                let mut opened = opened;
                                opened.close_and_log_error();
                                continue;
                            }
                            // If the metadata lookup fails, keep the file and
                            // let the time-range check decide.
                            _ => {}
                        }
                    }
                    file = opened;
                }
                if !checker.is_time_valid(file.begin, file.end) {
                    file.close_and_log_error();
                    continue;
                }
                // Go: `select { case filesCh <- file: case <-ctx.Done():
                // file.closeAndLogError(); return }`.
                dispatch_file(file, &files_tx, &manager_token);
            }
            // No scan tasks to be generated. Notify idle scan workers to
            // become parse workers.
            drop(files_tx);
            drop(files_rx);
            for handle in scan_handles {
                let _ = handle.join();
            }
            // No parse tasks to be generated once all scan tasks are done.
            // Notify idle parse workers to exit by closing linesCh.
            drop(lines_rx);
            for handle in parse_handles {
                let _ = handle.join();
            }
            // No rows to be generated once all parse tasks are done. Notify
            // the monitor to close rowsCh.
            child.cancel();
        });

    // Monitor to cover failures and notify workers to exit.
    loop {
        if monitor_token.is_done() || parent.is_done() {
            // notified by manager or the parent context is canceled
            break;
        }
        match inner_err_rx.try_recv() {
            Ok(error) => {
                let _ = err_tx.try_send(error);
                monitor_token.cancel(); // notify workers to exit
                break;
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                std::thread::sleep(CANCEL_POLL_INTERVAL);
            }
        }
    }
    if let Ok(handle) = manager {
        let _ = handle.join();
    }
    drop(rows_tx); // task done: close rowsCh
}

fn dispatch_file(mut file: StmtFile, files_tx: &SyncSender<StmtFile>, token: &CancelToken) {
    loop {
        if token.is_done() {
            file.close_and_log_error();
            return;
        }
        match files_tx.try_send(file) {
            Ok(()) => return,
            Err(mpsc::TrySendError::Full(returned)) => {
                std::thread::sleep(CANCEL_POLL_INTERVAL);
                file = returned;
            }
            Err(mpsc::TrySendError::Disconnected(returned)) => {
                file = returned;
                file.close_and_log_error();
                return;
            }
        }
    }
}

/// Go `util.ReadLine`: one line up to `maxLineSize`; `Err("EOF")` mirrors
/// `io.EOF`.
fn read_line<R: BufRead>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut line = Vec::new();
    let read = reader
        .read_until(b'\n', &mut line)
        .map_err(|e| e.to_string())?;
    if read == 0 {
        return Err("EOF".to_owned());
    }
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    if line.len() > MAX_LINE_SIZE {
        return Err(format!("single line length exceeds limit: {MAX_LINE_SIZE}"));
    }
    Ok(line)
}

/// Go `util.ReadLines`: `count` lines; a trailing short read with at least
/// one line collected is not an error.
fn read_lines<R: BufRead>(reader: &mut R, count: usize) -> Result<Vec<Vec<u8>>, String> {
    let mut lines = Vec::with_capacity(count);
    for _ in 0..count {
        match read_line(reader) {
            Ok(line) => lines.push(line),
            Err(error) if error == "EOF" && !lines.is_empty() => return Ok(lines),
            Err(error) => return Err(error),
        }
    }
    Ok(lines)
}

trait SeekReadStart {
    fn seek_read_start(&mut self) -> Result<(), String>;
}

impl SeekReadStart for std::fs::File {
    fn seek_read_start(&mut self) -> Result<(), String> {
        std::io::Seek::seek(self, std::io::SeekFrom::Start(0))
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::{DIGEST_STR, EXEC_COUNT_STR, IA_EXEC_COUNT_STR};
    use chrono::NaiveDate;
    use std::sync::Mutex;
    use tidb_ast::CiString;
    use tidb_model::ColumnInfo;

    /// `stmt_summary_filename` is process-global; every test that reads or
    /// rewrites it holds this lock so parallel tests cannot steal each
    /// other's configuration.
    static CONFIG_TEST_LOCK: Mutex<()> = Mutex::new(());

    /// Restores the shipped default (relative) `tidb-statements.log`.
    fn use_default_config_filename() {
        tidb_config::config_tree::config::update_global(|conf| {
            conf.instance.stmt_summary_filename = "tidb-statements.log".to_owned();
        });
    }

    /// Points the global config at `dir/tidb-statements.log` and restores the
    /// previous value on return.
    fn set_config_filename(dir: &Path) -> PathBuf {
        let filename = dir.join("tidb-statements.log");
        tidb_config::config_tree::config::update_global(|conf| {
            conf.instance.stmt_summary_filename = filename.to_string_lossy().into_owned();
        });
        filename
    }

    fn columns_for_test() -> Vec<ColumnInfo> {
        [DIGEST_STR, EXEC_COUNT_STR, IA_EXEC_COUNT_STR]
            .iter()
            .enumerate()
            .map(|(i, name)| ColumnInfo {
                id: i64::try_from(i).unwrap(),
                name: CiString::new(*name),
                offset: i64::try_from(i).unwrap(),
                ..ColumnInfo::default()
            })
            .collect()
    }

    fn local_unix(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32, ms: u32) -> i64 {
        let naive = NaiveDate::from_ymd_opt(y, m, d)
            .expect("valid date")
            .and_hms_milli_opt(h, min, s, ms)
            .expect("valid time");
        chrono::Local
            .from_local_datetime(&naive)
            .single()
            .unwrap_or_else(|| chrono::Local.from_utc_datetime(&naive))
            .timestamp()
    }

    /// Go `TestTimeRangeOverlap`.
    #[test]
    fn test_time_range_overlap() {
        assert!(!time_range_overlap(1, 2, 3, 4));
        assert!(!time_range_overlap(3, 4, 1, 2));
        assert!(time_range_overlap(1, 2, 2, 3));
        assert!(time_range_overlap(1, 3, 2, 4));
        assert!(time_range_overlap(2, 4, 1, 3));
        assert!(time_range_overlap(1, 0, 3, 4));
        assert!(time_range_overlap(1, 0, 2, 0));
    }

    /// Go `TestStmtFile`.
    #[test]
    fn test_stmt_file() {
        let _guard = CONFIG_TEST_LOCK.lock().unwrap();
        // Go creates the file in the package directory under the DEFAULT
        // relative config filename: `parseEndTs` only matches a base name
        // against the config prefix.
        use_default_config_filename();
        let path = Path::new("tidb-statements-2022-12-27T16-21-20.245.log");
        std::fs::write(path, "{\"begin\":1,\"end\":2}\n{\"begin\":3,\"end\":4}\n").unwrap();

        let mut f = open_stmt_file(path).unwrap();
        assert_eq!(1, f.begin);
        assert_eq!(local_unix(2022, 12, 27, 16, 21, 20, 245), f.end);

        // Check if seek 0.
        let mut reader = BufReader::new(f.file.as_mut().unwrap());
        let first_line = read_line(&mut reader).unwrap();
        assert_eq!(br#"{"begin":1,"end":2}"#, first_line.as_slice());
        f.close().unwrap();
        let _ = std::fs::remove_file(path);
    }

    /// Go `TestStmtFileInvalidLine`.
    #[test]
    fn test_stmt_file_invalid_line() {
        let _guard = CONFIG_TEST_LOCK.lock().unwrap();
        use_default_config_filename();
        let path = Path::new("tidb-statements-2022-12-27T16-21-20.245.log");
        std::fs::write(
            path,
            "invalid line\n{\"begin\":1,\"end\":2}\n{\"begin\":3,\"end\":4}\n",
        )
        .unwrap();

        let mut f = open_stmt_file(path).unwrap();
        assert_eq!(1, f.begin);
        assert_eq!(local_unix(2022, 12, 27, 16, 21, 20, 245), f.end);
        f.close().unwrap();
        let _ = std::fs::remove_file(path);
    }

    /// Go `TestStmtFiles` (enumeration part).
    #[test]
    fn test_stmt_files() {
        let _guard = CONFIG_TEST_LOCK.lock().unwrap();
        let t1_ms = 245_000_000i64;
        let t1 = local_unix(2022, 12, 27, 16, 21, 20, 245);
        let dir = tempfile::tempdir().unwrap();
        set_config_filename(dir.path());
        let filename1 = dir
            .path()
            .join("tidb-statements-2022-12-27T16-21-20.245.log");
        let filename2 = dir.path().join("tidb-statements.log");

        std::fs::write(
            &filename1,
            format!(
                "{{\"begin\":{},\"end\":{}}}\n{{\"begin\":{},\"end\":{}}}\n",
                t1 - 760,
                t1 - 750,
                t1 - 10,
                t1
            ),
        )
        .unwrap();
        std::fs::write(
            &filename2,
            format!(
                "{{\"begin\":{},\"end\":{}}}\n{{\"begin\":{},\"end\":{}}}\n",
                t1 - 10,
                t1,
                t1 + 100 + t1_ms / t1_ms.max(1) - 1 + 1,
                t1 + 110
            ),
        )
        .unwrap();

        let _ = t1_ms;
        let token = CancelToken::new();
        let files = new_stmt_files(&token).unwrap();
        assert_eq!(2, files.files.len());
        assert_eq!(filename1, files.files[0].path);
        assert_eq!(filename2, files.files[1].path);
        assert!(files.files[0].file.is_none());
        assert!(files.files[1].file.is_some());
    }

    /// Go `TestStmtFiles` rotation scenarios: the pinned active inode must
    /// survive rotation before/after directory enumeration, and a metadata
    /// failure keeps the path for open-time deduplication.
    #[test]
    fn test_stmt_files_rotation_preserves_current_file() {
        // Go's table: rotation after the snapshot (no injection), rotation
        // before it, and rotation before it plus an injected Info() failure.
        for (rotate_after_enumeration, fail_rotated_entry_metadata) in
            [(true, false), (false, false), (false, true)]
        {
            let _guard = CONFIG_TEST_LOCK.lock().unwrap();
            let dir = tempfile::tempdir().unwrap();
            let current_path = set_config_filename(dir.path());
            let rotated_path = dir
                .path()
                .join("tidb-statements-2022-12-27T16-21-20.245.log");

            let old_record = "{\"begin\":1,\"end\":2,\"digest\":\"old\"}";
            let new_record = "{\"begin\":3,\"end\":4,\"digest\":\"new\"}";
            std::fs::write(&current_path, format!("{old_record}\n")).unwrap();
            let rotate = || -> std::io::Result<()> {
                std::fs::rename(&current_path, &rotated_path)?;
                std::fs::write(&current_path, format!("{new_record}\n"))
            };

            let dir_path = dir.path().to_path_buf();
            let files = new_stmt_files_with_read_dir(&CancelToken::new(), move |read_dir| {
                if !rotate_after_enumeration {
                    rotate()?;
                }
                let mut entries = read_dir_entries(read_dir)?;
                if rotate_after_enumeration {
                    rotate()?;
                }
                if fail_rotated_entry_metadata {
                    for entry in &mut entries {
                        if entry.name.starts_with("tidb-statements-2022") {
                            entry.force_info_err = true;
                        }
                    }
                }
                Ok(entries)
            })
            .unwrap();
            let expected_files = if fail_rotated_entry_metadata { 2 } else { 1 };
            assert_eq!(expected_files, files.files.len());
            let snapshot = files
                .files
                .iter()
                .find(|file| file.file.is_some())
                .expect("the pinned current file must be open");

            let mut reader = HistoryReader::from_parts(
                None,
                files,
                String::new(),
                chrono_tz::Tz::UTC,
                StmtChecker::default(),
                make_column_factories(&[ColumnInfo {
                    name: CiString::new(DIGEST_STR),
                    ..ColumnInfo::default()
                }]),
                2,
            );
            let mut all = Vec::new();
            loop {
                match reader.rows().unwrap() {
                    Some(rows) => all.extend(rows),
                    None => break,
                }
            }
            reader.close().unwrap();
            assert_eq!(1, all.len());
            assert_eq!(
                Datum::new_string(b"old"),
                all[0][0],
                "rotate_after={rotate_after_enumeration} fail_meta={fail_rotated_entry_metadata}"
            );
        }
    }

    /// Go `TestStmtChecker`.
    #[test]
    fn test_stmt_checker() {
        let checker = StmtChecker::default();
        assert!(checker.has_privilege(&HashSet::new()));

        let checker = StmtChecker {
            user: Some(UserIdentity {
                username: "user1".to_owned(),
                ..UserIdentity::default()
            }),
            ..StmtChecker::default()
        };
        assert!(!checker.has_privilege(&HashSet::new()));
        assert!(!checker.has_privilege(&HashSet::from(["user2".to_owned()])));
        assert!(checker.has_privilege(&HashSet::from(["user1".to_owned(), "user2".to_owned()])));

        let checker = StmtChecker::default();
        assert!(checker.is_digest_valid("digest1"));

        let checker = StmtChecker {
            digests: Some(HashSet::from(["digest2".to_owned()])),
            ..StmtChecker::default()
        };
        assert!(!checker.is_digest_valid("digest1"));
        assert!(checker.is_digest_valid("digest2"));

        let checker = StmtChecker {
            digests: Some(HashSet::from(["digest1".to_owned(), "digest2".to_owned()])),
            ..StmtChecker::default()
        };
        assert!(checker.is_digest_valid("digest1"));
        assert!(checker.is_digest_valid("digest2"));

        let checker = StmtChecker::default();
        assert!(checker.is_time_valid(1, 2));
        assert!(!checker.need_stop(2));
        assert!(!checker.need_stop(3));

        let checker = StmtChecker {
            time_ranges: vec![StmtTimeRange { begin: 1, end: 2 }],
            ..StmtChecker::default()
        };
        assert!(checker.is_time_valid(1, 2));
        assert!(!checker.is_time_valid(3, 4));
        assert!(!checker.need_stop(2));
        assert!(checker.need_stop(3));
    }

    /// Go `TestMemReader`.
    #[test]
    fn test_mem_reader() {
        let columns = columns_for_test();
        let ss = StmtSummary::new_for_test(3);

        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest1",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest1",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest2",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest2",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest3",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest3",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest4",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest4",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest5",
        ));
        ss.add(&crate::v2::record::generate_stmt_exec_info_4_test(
            "digest5",
        ));
        let reader = new_mem_reader(
            Some(Arc::clone(&ss)),
            &columns,
            String::new(),
            chrono_tz::Tz::UTC,
            None,
            false,
            None,
            Vec::new(),
        );
        let rows = reader.rows();
        assert_eq!(4, rows.len()); // 3 rows + 1 other
        assert_eq!(reader.column_factories.len(), rows[0].len());
        for row in &rows {
            assert_eq!(Datum::new_int(0), row[2]);
        }
        ss.evicted();
    }

    /// Go `TestHistoryReader` plus `TestHistoryReaderInvalidLine`, over a
    /// tempdir-wired config filename.
    #[test]
    fn test_history_reader() {
        let _guard = CONFIG_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let filename1 = dir
            .path()
            .join("tidb-statements-2022-12-27T16-21-20.245.log");
        let filename2 = set_config_filename(dir.path());

        std::fs::write(
            &filename1,
            "{\"begin\":1672128520,\"end\":1672128530,\"digest\":\"digest1\",\"exec_count\":10,\"ia_remote_exec_count\":3}\n\
             {\"begin\":1672129270,\"end\":1672129280,\"digest\":\"digest2\",\"exec_count\":20}\n\
             {\"begin\":1672129270,\"end\":1672129280,\"digest\":\"evicted_digest\",\"exec_count\":99,\"evicted\":true}\n",
        )
        .unwrap();
        std::fs::write(
            &filename2,
            "{\"begin\":1672129270,\"end\":1672129280,\"digest\":\"digest2\",\"exec_count\":30}\n\
             {\"begin\":1672129380,\"end\":1672129390,\"digest\":\"digest3\",\"exec_count\":40}\n",
        )
        .unwrap();

        let columns = columns_for_test();

        macro_rules! read_all {
            ($digests:expr, $ranges:expr, $expected:expr) => {{
                let mut reader = HistoryReader::new(
                    None,
                    &columns,
                    String::new(),
                    chrono_tz::Tz::UTC,
                    None,
                    false,
                    $digests,
                    $ranges,
                    2,
                )
                .unwrap();
                let mut all = Vec::new();
                loop {
                    match reader.rows().unwrap() {
                        Some(rows) => {
                            for row in rows {
                                assert_eq!(columns.len(), row.len());
                                all.push(row);
                            }
                        }
                        None => break,
                    }
                }
                reader.close().unwrap();
                assert_eq!($expected, all.len());
                all
            }};
        }

        let all = read_all!(None, Vec::new(), 4);
        for row in &all {
            if row[0] == Datum::new_string(b"digest1") {
                assert_eq!(Datum::new_int(3), row[2]);
            } else {
                assert_eq!(Datum::new_int(0), row[2]);
            }
        }

        read_all!(Some(HashSet::from(["digest2".to_owned()])), Vec::new(), 2);
        read_all!(
            None,
            vec![StmtTimeRange {
                begin: 0,
                end: 1672128520 - 1
            }],
            0
        );
        read_all!(
            None,
            vec![StmtTimeRange {
                begin: 0,
                end: 1672129270 - 1
            }],
            1
        );
        read_all!(
            None,
            vec![StmtTimeRange {
                begin: 0,
                end: 1672129270
            }],
            3
        );
        read_all!(
            None,
            vec![StmtTimeRange {
                begin: 0,
                end: 1672129380
            }],
            4
        );
        read_all!(
            None,
            vec![StmtTimeRange {
                begin: 1672129270,
                end: 1672129380
            }],
            3
        );
        read_all!(
            None,
            vec![StmtTimeRange {
                begin: 1672129390,
                end: 0
            }],
            1
        );
        read_all!(
            None,
            vec![StmtTimeRange {
                begin: 1672129391,
                end: 0
            }],
            0
        );
        read_all!(None, vec![StmtTimeRange { begin: 0, end: 0 }], 4);
    }

    /// Go `TestHistoryReaderInvalidLine`.
    #[test]
    fn test_history_reader_invalid_line() {
        let _guard = CONFIG_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let filename = set_config_filename(dir.path());

        std::fs::write(
            &filename,
            "invalid header line\n\
             {\"begin\":1672129270,\"end\":1672129280,\"digest\":\"digest2\",\"exec_count\":30}\n\
             corrupted line\n\
             {\"begin\":1672129380,\"end\":1672129390,\"digest\":\"digest3\",\"exec_count\":40}\n\
             invalid footer line",
        )
        .unwrap();

        let columns = vec![ColumnInfo {
            name: CiString::new(DIGEST_STR),
            ..ColumnInfo::default()
        }];
        let mut reader = HistoryReader::new(
            None,
            &columns,
            String::new(),
            chrono_tz::Tz::UTC,
            None,
            false,
            None,
            Vec::new(),
            2,
        )
        .unwrap();
        let mut all = Vec::new();
        loop {
            match reader.rows().unwrap() {
                Some(rows) => all.extend(rows),
                None => break,
            }
        }
        reader.close().unwrap();
        assert_eq!(2, all.len());
        for row in &all {
            assert_eq!(columns.len(), row.len());
        }
    }
}
