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

//! Go `pkg/util/stmtsummary/v2/column.go`: lands complete.
//!
//! Every production symbol of `column.go` is here — the statements-summary
//! column-name constants, the `columnInfo` interface, `columnFactory` and
//! `columnFactoryMap`, `makeColumnFactories`, `formatBackoffTypes`, `avgInt` /
//! `avgFloat` / `avgFloat4Uint` / `avgSumFloat`, and `convertEmptyToNil`.
//!
//! What this file reuses from v1 rather than restating:
//!
//! - The column-name constants. Go's `v2/column.go` re-declares 129 constants
//!   that `v1/reader.go` also declares; all 123 identifiers the two files share
//!   were checked to carry identical string values, and the 6 that only v2
//!   spells (`AvgRequestUnitRead`, `MaxRequestUnitRead`, `AvgRequestUnitWrite`,
//!   `MaxRequestUnitWrite`, `AvgRequestUnitV2`, `MaxRequestUnitV2`) are the
//!   `Str`-suffix-less spellings of v1 constants with the same value. So this
//!   module re-exports v1's rather than duplicating the table.
//! - `avgInt`, `avgFloat`, `avgFloat4Uint`, `avgSumFloat`, `convertEmptyToNil`
//!   and `formatBackoffTypes` are byte-for-byte the same functions v1 declares;
//!   they come from [`crate::statement_summary`]. `formatBackoffTypes` takes v1's
//!   `HashMap<String, i64>`, which is the same map [`StmtRecord::backoff_types`]
//!   holds.
//!
//! Where v2 genuinely diverges from v1's `columnValueFactoryMap`:
//!
//! - A v2 factory takes one [`StmtRecord`] where a v1 factory takes the
//!   `(reader, element, ssbd, stats)` quadruple, because v2 has already folded
//!   the identity, the window bounds, and the statistics into one record.
//! - v2 registers only the `SUM_`/`AVG_`/`MAX_` spellings; v1 additionally
//!   registers the bare cumulative-table spellings (`TOTAL_TIME`,
//!   `PROCESS_TIME`, `ROCKSDB_BLOCK_READ_BYTE`, …), which v2 has no table for.
//! - `SUMMARY_BEGIN_TIME` / `SUMMARY_END_TIME` read `record.begin` / `record.end`
//!   instead of an interval element's `beginTime` / `endTime`.
//! - v2 has no `AVG_COP_PROCESS_TIME` / `AVG_COP_WAIT_TIME` because
//!   [`StmtRecord`] carries no `sum_cop_process_time` / `sum_cop_wait_time`.
//! - v2's `BINDING_DIGEST_TEXT` / `SAMPLE_USER` / `INDEX_NAMES` read the record's
//!   own fields rather than reaching across two locked structures.
//!
//! Narrowings:
//!
//! - Go's `any` return narrows to [`Datum`], the value Go's `types.NewDatum`
//!   would build from that `any`, exactly as v1's factories already do. Go's
//!   `bool` becomes `types.NewDatum(bool)`'s `SetInt64(1)`/`SetInt64(0)`.
//! - Go's package-level `columnFactoryMap` becomes the lookup function
//!   [`column_factory`]; the arms are in Go's literal order.
//! - `columnInfo`'s `getTimeLocation() *time.Location` narrows to
//!   [`ColumnInfoSource::time_location`] returning `chrono_tz::Tz`.
//! - `[]*model.ColumnInfo` is the real `tidb_model::ColumnInfo`.
//! - `logutil.BgLogger()` has no boundary here: a `plancodec::DecodePlan`
//!   failure is not logged, it only yields Go's empty plan string — the same
//!   treatment v1's `reader.rs` gives it.

use chrono::{DateTime, TimeZone};
use chrono_tz::Tz;
use std::time::Duration;
use tidb_datatype::{core_time_from_datetime, Datum, Time, TimeType};
use tidb_model::ColumnInfo;
use tidb_util::plancodec::decode_plan;

use crate::statement_summary::{
    avg_float, avg_float4_uint, avg_int, avg_sum_float, convert_empty_to_nil, format_backoff_types,
};
use crate::v2::record::StmtRecord;

pub use crate::reader::{
    AVG_AFFECTED_ROWS_STR, AVG_BACKOFF_TIME_STR, AVG_BACKOFF_TOTAL_TIME_STR,
    AVG_COMMIT_BACKOFF_TIME_STR, AVG_COMMIT_TIME_STR, AVG_COMPILE_LATENCY_STR, AVG_DISK_STR,
    AVG_GET_COMMIT_TS_TIME_STR, AVG_IA_REMOTE_READ_SEGMENT_COUNT_STR,
    AVG_IA_REMOTE_READ_SEGMENT_SIZE_STR, AVG_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR, AVG_KV_TIME_STR,
    AVG_LATENCY_STR, AVG_LOCAL_LATCH_WAIT_TIME_STR, AVG_MEM_ARBITRATION_STR, AVG_MEM_STR,
    AVG_PARSE_LATENCY_STR, AVG_PD_TIME_STR, AVG_PREWRITE_REGIONS_STR, AVG_PREWRITE_TIME_STR,
    AVG_PROCESSED_KEYS_STR, AVG_PROCESS_TIME_STR, AVG_QUEUED_RC_TIME_STR,
    AVG_REQUEST_UNIT_READ_STR as AVG_REQUEST_UNIT_READ,
    AVG_REQUEST_UNIT_V2_STR as AVG_REQUEST_UNIT_V2,
    AVG_REQUEST_UNIT_WRITE_STR as AVG_REQUEST_UNIT_WRITE, AVG_RESOLVE_LOCK_TIME_STR,
    AVG_RESULT_ROWS_STR, AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR, AVG_ROCKSDB_BLOCK_READ_BYTE_STR,
    AVG_ROCKSDB_BLOCK_READ_COUNT_STR, AVG_ROCKSDB_DELETE_SKIPPED_COUNT_STR,
    AVG_ROCKSDB_KEY_SKIPPED_COUNT_STR, AVG_TIDB_CPU_TIME_STR, AVG_TIKV_CPU_TIME_STR,
    AVG_TOTAL_KEYS_STR, AVG_TXN_RETRY_STR, AVG_WAIT_TIME_STR, AVG_WRITE_KEYS_STR,
    AVG_WRITE_SIZE_STR, AVG_WRITE_SQL_RESP_TIME_STR, BACKOFF_TYPES_STR, BINARY_PLAN,
    BINDING_DIGEST_STR, BINDING_DIGEST_TEXT_STR, CHARSET, CLUSTER_TABLE_INSTANCE_COLUMN_NAME_STR,
    COLLATION, DIGEST_STR, DIGEST_TEXT_STR, EXEC_COUNT_STR, FIRST_SEEN_STR, INDEX_NAMES_STR,
    LAST_SEEN_STR, MAX_BACKOFF_TIME_STR, MAX_COMMIT_BACKOFF_TIME_STR, MAX_COMMIT_TIME_STR,
    MAX_COMPILE_LATENCY_STR, MAX_COP_PROCESS_ADDRESS_STR, MAX_COP_PROCESS_TIME_STR,
    MAX_COP_WAIT_ADDRESS_STR, MAX_COP_WAIT_TIME_STR, MAX_DISK_STR, MAX_GET_COMMIT_TS_TIME_STR,
    MAX_IA_REMOTE_READ_SEGMENT_COUNT_STR, MAX_IA_REMOTE_READ_SEGMENT_SIZE_STR,
    MAX_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR, MAX_LATENCY_STR, MAX_LOCAL_LATCH_WAIT_TIME_STR,
    MAX_MEM_ARBITRATION_STR, MAX_MEM_STR, MAX_PARSE_LATENCY_STR, MAX_PREWRITE_REGIONS_STR,
    MAX_PREWRITE_TIME_STR, MAX_PROCESSED_KEYS_STR, MAX_PROCESS_TIME_STR, MAX_QUEUED_RC_TIME_STR,
    MAX_REQUEST_UNIT_READ_STR as MAX_REQUEST_UNIT_READ,
    MAX_REQUEST_UNIT_V2_STR as MAX_REQUEST_UNIT_V2,
    MAX_REQUEST_UNIT_WRITE_STR as MAX_REQUEST_UNIT_WRITE, MAX_RESOLVE_LOCK_TIME_STR,
    MAX_RESULT_ROWS_STR, MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR, MAX_ROCKSDB_BLOCK_READ_BYTE_STR,
    MAX_ROCKSDB_BLOCK_READ_COUNT_STR, MAX_ROCKSDB_DELETE_SKIPPED_COUNT_STR,
    MAX_ROCKSDB_KEY_SKIPPED_COUNT_STR, MAX_TOTAL_KEYS_STR, MAX_TXN_RETRY_STR, MAX_WAIT_TIME_STR,
    MAX_WRITE_KEYS_STR, MAX_WRITE_SIZE_STR, MIN_LATENCY_STR, MIN_RESULT_ROWS_STR,
    PLAN_CACHE_HITS_STR, PLAN_CACHE_UNQUALIFIED_LAST_REASON_STR, PLAN_CACHE_UNQUALIFIED_STR,
    PLAN_DIGEST_STR, PLAN_HINT, PLAN_IN_BINDING_STR, PLAN_IN_CACHE_STR, PLAN_STR, PREPARED_STR,
    PREV_SAMPLE_TEXT_STR, QUERY_SAMPLE_TEXT_STR, RESOURCE_GROUP_NAME, SAMPLE_USER_STR,
    SCHEMA_NAME_STR, STMT_TYPE_STR, STORAGE_KV_STR, STORAGE_MPP_STR, SUMMARY_BEGIN_TIME_STR,
    SUMMARY_END_TIME_STR, SUM_BACKOFF_TIMES_STR, SUM_COP_TASK_NUM_STR, SUM_ERRORS_STR,
    SUM_EXEC_RETRY_STR, SUM_EXEC_RETRY_TIME_STR, SUM_LATENCY_STR,
    SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL_STR, SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE_STR,
    SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL_STR, SUM_UNPACKED_BYTES_RECEIVE_TIFLASH_CROSS_ZONE_STR,
    SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE_STR, SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL_STR,
    SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE_STR, SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL_STR,
    SUM_WARNINGS_STR, TABLE_NAMES_STR,
};

/// Go `columnInfo`: the per-reader context a column factory may consult.
pub trait ColumnInfoSource {
    /// Go `getInstanceAddr`.
    fn instance_addr(&self) -> String;
    /// Go `getTimeLocation`.
    fn time_location(&self) -> Tz;
}

/// Go `columnFactory`.
pub type ColumnFactory = fn(&dyn ColumnInfoSource, &StmtRecord) -> Datum;

/// Go `time.Duration` rendered as the `int64` nanosecond count the columns
/// report.
fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// Go `types.NewDatum(string)` / `types.NewDatum(nil)` for the `any` that
/// `convertEmptyToNil` returns.
fn opt_string_datum(value: Option<&str>) -> Datum {
    value.map_or(Datum::Null, |value| Datum::new_string(value.as_bytes()))
}

/// Go `types.NewDatum(bool)`: `SetInt64(1)` or `SetInt64(0)`.
fn bool_datum(value: bool) -> Datum {
    Datum::new_int(i64::from(value))
}

/// Go `types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 0)`.
pub(crate) fn timestamp_datum<TZ: TimeZone>(instant: DateTime<TZ>) -> Datum {
    let core = core_time_from_datetime(instant);
    // Go's `NewTime` cannot fail; fsp 0 is always a valid fsp here.
    let time = Time::new(core, TimeType::Timestamp, 0).unwrap_or_else(|error| {
        unreachable!("fsp 0 is always valid for a timestamp: {error:?}");
    });
    Datum::new_time(time)
}

/// Go `time.Unix(seconds, 0).In(loc)`.
fn unix_seconds_in(seconds: i64, tz: Tz) -> DateTime<Tz> {
    DateTime::from_timestamp(seconds, 0)
        .unwrap_or_else(|| DateTime::from_timestamp_nanos(0))
        .with_timezone(&tz)
}

/// Go `columnFactoryMap`.
///
/// Go's package-level map becomes a lookup function; the arms are in Go's
/// literal order.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn column_factory(name: &str) -> Option<ColumnFactory> {
    let factory: ColumnFactory = match name {
        CLUSTER_TABLE_INSTANCE_COLUMN_NAME_STR => {
            |info, _| Datum::new_string(info.instance_addr().as_bytes())
        }
        SUMMARY_BEGIN_TIME_STR => {
            |info, record| timestamp_datum(unix_seconds_in(record.begin, info.time_location()))
        }
        SUMMARY_END_TIME_STR => {
            |info, record| timestamp_datum(unix_seconds_in(record.end, info.time_location()))
        }
        STMT_TYPE_STR => |_, record| Datum::new_string(record.stmt_type.as_bytes()),
        SCHEMA_NAME_STR => |_, record| opt_string_datum(convert_empty_to_nil(&record.schema_name)),
        DIGEST_STR => |_, record| opt_string_datum(convert_empty_to_nil(&record.digest)),
        DIGEST_TEXT_STR => |_, record| Datum::new_string(record.normalized_sql.as_bytes()),
        BINDING_DIGEST_STR => {
            |_, record| opt_string_datum(convert_empty_to_nil(&record.binding_digest))
        }
        BINDING_DIGEST_TEXT_STR => |_, record| Datum::new_string(record.binding_sql.as_bytes()),
        TABLE_NAMES_STR => |_, record| opt_string_datum(convert_empty_to_nil(&record.table_names)),
        INDEX_NAMES_STR => |_, record| {
            let joined = record.index_names.join(",");
            opt_string_datum(convert_empty_to_nil(&joined))
        },
        SAMPLE_USER_STR => |_, record| {
            let sample_user = record.auth_users.iter().next().map_or("", String::as_str);
            opt_string_datum(convert_empty_to_nil(sample_user))
        },
        EXEC_COUNT_STR => |_, record| Datum::new_int(record.exec_count),
        SUM_ERRORS_STR => |_, record| Datum::new_int(record.sum_errors),
        SUM_WARNINGS_STR => |_, record| Datum::new_int(record.sum_warnings),
        SUM_LATENCY_STR => |_, record| Datum::new_int(nanos(record.sum_latency)),
        MAX_LATENCY_STR => |_, record| Datum::new_int(nanos(record.max_latency)),
        MIN_LATENCY_STR => |_, record| Datum::new_int(nanos(record.min_latency)),
        AVG_LATENCY_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_latency), record.exec_count))
        }
        AVG_PARSE_LATENCY_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_parse_latency), record.exec_count))
        }
        MAX_PARSE_LATENCY_STR => |_, record| Datum::new_int(nanos(record.max_parse_latency)),
        AVG_COMPILE_LATENCY_STR => |_, record| {
            Datum::new_int(avg_int(
                nanos(record.sum_compile_latency),
                record.exec_count,
            ))
        },
        MAX_COMPILE_LATENCY_STR => |_, record| Datum::new_int(nanos(record.max_compile_latency)),
        SUM_COP_TASK_NUM_STR => |_, record| Datum::new_int(record.sum_num_cop_tasks),
        MAX_COP_PROCESS_TIME_STR => |_, record| Datum::new_int(nanos(record.max_cop_process_time)),
        MAX_COP_PROCESS_ADDRESS_STR => {
            |_, record| opt_string_datum(convert_empty_to_nil(&record.max_cop_process_address))
        }
        MAX_COP_WAIT_TIME_STR => |_, record| Datum::new_int(nanos(record.max_cop_wait_time)),
        MAX_COP_WAIT_ADDRESS_STR => {
            |_, record| opt_string_datum(convert_empty_to_nil(&record.max_cop_wait_address))
        }
        AVG_PROCESS_TIME_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_process_time), record.exec_count))
        }
        MAX_PROCESS_TIME_STR => |_, record| Datum::new_int(nanos(record.max_process_time)),
        AVG_WAIT_TIME_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_wait_time), record.exec_count))
        }
        MAX_WAIT_TIME_STR => |_, record| Datum::new_int(nanos(record.max_wait_time)),
        AVG_BACKOFF_TIME_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_backoff_time), record.exec_count))
        }
        MAX_BACKOFF_TIME_STR => |_, record| Datum::new_int(nanos(record.max_backoff_time)),
        AVG_TOTAL_KEYS_STR => {
            |_, record| Datum::new_int(avg_int(record.sum_total_keys, record.exec_count))
        }
        MAX_TOTAL_KEYS_STR => |_, record| Datum::new_int(record.max_total_keys),
        AVG_PROCESSED_KEYS_STR => {
            |_, record| Datum::new_int(avg_int(record.sum_processed_keys, record.exec_count))
        }
        MAX_PROCESSED_KEYS_STR => |_, record| Datum::new_int(record.max_processed_keys),
        AVG_ROCKSDB_DELETE_SKIPPED_COUNT_STR => |_, record| {
            Datum::new_real(avg_float4_uint(
                record.sum_rocksdb_delete_skipped_count,
                record.exec_count,
            ))
        },
        MAX_ROCKSDB_DELETE_SKIPPED_COUNT_STR => {
            |_, record| Datum::new_uint(record.max_rocksdb_delete_skipped_count)
        }
        AVG_ROCKSDB_KEY_SKIPPED_COUNT_STR => |_, record| {
            Datum::new_real(avg_float4_uint(
                record.sum_rocksdb_key_skipped_count,
                record.exec_count,
            ))
        },
        MAX_ROCKSDB_KEY_SKIPPED_COUNT_STR => {
            |_, record| Datum::new_uint(record.max_rocksdb_key_skipped_count)
        }
        AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR => |_, record| {
            Datum::new_real(avg_float4_uint(
                record.sum_rocksdb_block_cache_hit_count,
                record.exec_count,
            ))
        },
        MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR => {
            |_, record| Datum::new_uint(record.max_rocksdb_block_cache_hit_count)
        }
        AVG_ROCKSDB_BLOCK_READ_COUNT_STR => |_, record| {
            Datum::new_real(avg_float4_uint(
                record.sum_rocksdb_block_read_count,
                record.exec_count,
            ))
        },
        MAX_ROCKSDB_BLOCK_READ_COUNT_STR => {
            |_, record| Datum::new_uint(record.max_rocksdb_block_read_count)
        }
        AVG_ROCKSDB_BLOCK_READ_BYTE_STR => |_, record| {
            Datum::new_real(avg_float4_uint(
                record.sum_rocksdb_block_read_byte,
                record.exec_count,
            ))
        },
        MAX_ROCKSDB_BLOCK_READ_BYTE_STR => {
            |_, record| Datum::new_uint(record.max_rocksdb_block_read_byte)
        }
        AVG_IA_REMOTE_READ_SEGMENT_COUNT_STR => |_, record| {
            Datum::new_real(avg_float4_uint(
                record.sum_ia_remote_read_segment_count,
                record.exec_count,
            ))
        },
        MAX_IA_REMOTE_READ_SEGMENT_COUNT_STR => {
            |_, record| Datum::new_uint(record.max_ia_remote_read_segment_count)
        }
        AVG_IA_REMOTE_READ_SEGMENT_SIZE_STR => |_, record| {
            Datum::new_real(avg_float4_uint(
                record.sum_ia_remote_read_segment_size,
                record.exec_count,
            ))
        },
        MAX_IA_REMOTE_READ_SEGMENT_SIZE_STR => {
            |_, record| Datum::new_uint(record.max_ia_remote_read_segment_size)
        }
        AVG_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR => |_, record| {
            Datum::new_int(avg_int(
                nanos(record.sum_ia_remote_read_segment_wait_time),
                record.exec_count,
            ))
        },
        MAX_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR => {
            |_, record| Datum::new_int(nanos(record.max_ia_remote_read_segment_wait_time))
        }
        AVG_PREWRITE_TIME_STR => |_, record| {
            Datum::new_int(avg_int(
                nanos(record.sum_prewrite_time),
                record.commit_count,
            ))
        },
        MAX_PREWRITE_TIME_STR => |_, record| Datum::new_int(nanos(record.max_prewrite_time)),
        AVG_COMMIT_TIME_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_commit_time), record.commit_count))
        }
        MAX_COMMIT_TIME_STR => |_, record| Datum::new_int(nanos(record.max_commit_time)),
        AVG_GET_COMMIT_TS_TIME_STR => |_, record| {
            Datum::new_int(avg_int(
                nanos(record.sum_get_commit_ts_time),
                record.commit_count,
            ))
        },
        MAX_GET_COMMIT_TS_TIME_STR => {
            |_, record| Datum::new_int(nanos(record.max_get_commit_ts_time))
        }
        AVG_COMMIT_BACKOFF_TIME_STR => {
            |_, record| Datum::new_int(avg_int(record.sum_commit_backoff_time, record.commit_count))
        }
        MAX_COMMIT_BACKOFF_TIME_STR => |_, record| Datum::new_int(record.max_commit_backoff_time),
        AVG_RESOLVE_LOCK_TIME_STR => {
            |_, record| Datum::new_int(avg_int(record.sum_resolve_lock_time, record.commit_count))
        }
        MAX_RESOLVE_LOCK_TIME_STR => |_, record| Datum::new_int(record.max_resolve_lock_time),
        AVG_LOCAL_LATCH_WAIT_TIME_STR => |_, record| {
            Datum::new_int(avg_int(
                nanos(record.sum_local_latch_time),
                record.commit_count,
            ))
        },
        MAX_LOCAL_LATCH_WAIT_TIME_STR => {
            |_, record| Datum::new_int(nanos(record.max_local_latch_time))
        }
        AVG_WRITE_KEYS_STR => {
            |_, record| Datum::new_real(avg_float(record.sum_write_keys, record.commit_count))
        }
        MAX_WRITE_KEYS_STR => |_, record| Datum::new_int(record.max_write_keys),
        AVG_WRITE_SIZE_STR => {
            |_, record| Datum::new_real(avg_float(record.sum_write_size, record.commit_count))
        }
        MAX_WRITE_SIZE_STR => |_, record| Datum::new_int(record.max_write_size),
        AVG_PREWRITE_REGIONS_STR => |_, record| {
            Datum::new_real(avg_float(
                record.sum_prewrite_region_num,
                record.commit_count,
            ))
        },
        MAX_PREWRITE_REGIONS_STR => {
            |_, record| Datum::new_int(i64::from(record.max_prewrite_region_num))
        }
        AVG_TXN_RETRY_STR => {
            |_, record| Datum::new_real(avg_float(record.sum_txn_retry, record.commit_count))
        }
        MAX_TXN_RETRY_STR => |_, record| Datum::new_int(record.max_txn_retry),
        SUM_EXEC_RETRY_STR => {
            |_, record| Datum::new_int(i64::try_from(record.exec_retry_count).unwrap_or(i64::MAX))
        }
        SUM_EXEC_RETRY_TIME_STR => |_, record| Datum::new_int(nanos(record.exec_retry_time)),
        SUM_BACKOFF_TIMES_STR => |_, record| Datum::new_int(record.sum_backoff_times),
        BACKOFF_TYPES_STR => |_, record| {
            format_backoff_types(&record.backoff_types).map_or(Datum::Null, Datum::new_string)
        },
        AVG_MEM_STR => |_, record| Datum::new_int(avg_int(record.sum_mem, record.exec_count)),
        MAX_MEM_STR => |_, record| Datum::new_int(record.max_mem),
        AVG_MEM_ARBITRATION_STR => |_, record| {
            Datum::new_real(avg_sum_float(record.sum_mem_arbitration, record.exec_count))
        },
        MAX_MEM_ARBITRATION_STR => |_, record| Datum::new_real(record.max_mem_arbitration),
        AVG_DISK_STR => |_, record| Datum::new_int(avg_int(record.sum_disk, record.exec_count)),
        MAX_DISK_STR => |_, record| Datum::new_int(record.max_disk),
        AVG_KV_TIME_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_kv_total), record.commit_count))
        }
        AVG_PD_TIME_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_pd_total), record.commit_count))
        }
        AVG_BACKOFF_TOTAL_TIME_STR => |_, record| {
            Datum::new_int(avg_int(
                nanos(record.sum_backoff_total),
                record.commit_count,
            ))
        },
        AVG_WRITE_SQL_RESP_TIME_STR => |_, record| {
            Datum::new_int(avg_int(
                nanos(record.sum_write_sql_resp_total),
                record.commit_count,
            ))
        },
        AVG_TIDB_CPU_TIME_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_tidb_cpu), record.exec_count))
        }
        AVG_TIKV_CPU_TIME_STR => {
            |_, record| Datum::new_int(avg_int(nanos(record.sum_tikv_cpu), record.exec_count))
        }
        MAX_RESULT_ROWS_STR => |_, record| Datum::new_int(record.max_result_rows),
        MIN_RESULT_ROWS_STR => |_, record| Datum::new_int(record.min_result_rows),
        AVG_RESULT_ROWS_STR => {
            |_, record| Datum::new_int(avg_int(record.sum_result_rows, record.exec_count))
        }
        PREPARED_STR => |_, record| bool_datum(record.prepared),
        AVG_AFFECTED_ROWS_STR => |_, record| {
            Datum::new_real(avg_float4_uint(record.sum_affected_rows, record.exec_count))
        },
        FIRST_SEEN_STR => {
            |info, record| timestamp_datum(record.first_seen.with_timezone(&info.time_location()))
        }
        LAST_SEEN_STR => {
            |info, record| timestamp_datum(record.last_seen.with_timezone(&info.time_location()))
        }
        PLAN_IN_CACHE_STR => |_, record| bool_datum(record.plan_in_cache),
        PLAN_CACHE_HITS_STR => |_, record| Datum::new_int(record.plan_cache_hits),
        PLAN_IN_BINDING_STR => |_, record| bool_datum(record.plan_in_binding),
        QUERY_SAMPLE_TEXT_STR => |_, record| Datum::new_string(record.sample_sql.as_bytes()),
        PREV_SAMPLE_TEXT_STR => |_, record| Datum::new_string(record.prev_sql.as_bytes()),
        PLAN_DIGEST_STR => |_, record| Datum::new_string(record.plan_digest.as_bytes()),
        PLAN_STR => |_, record| {
            // Go logs the decode failure and falls back to the empty plan.
            let plan = decode_plan(&record.sample_plan).unwrap_or_default();
            Datum::new_string(plan)
        },
        BINARY_PLAN => |_, record| Datum::new_string(record.sample_binary_plan.as_bytes()),
        CHARSET => |_, record| Datum::new_string(record.charset.as_bytes()),
        COLLATION => |_, record| Datum::new_string(record.collation.as_bytes()),
        PLAN_HINT => |_, record| Datum::new_string(record.plan_hint.as_bytes()),
        AVG_REQUEST_UNIT_READ => {
            |_, record| Datum::new_real(avg_sum_float(record.ru.sum_rru, record.exec_count))
        }
        MAX_REQUEST_UNIT_READ => |_, record| Datum::new_real(record.ru.max_rru),
        AVG_REQUEST_UNIT_WRITE => {
            |_, record| Datum::new_real(avg_sum_float(record.ru.sum_wru, record.exec_count))
        }
        MAX_REQUEST_UNIT_WRITE => |_, record| Datum::new_real(record.ru.max_wru),
        AVG_QUEUED_RC_TIME_STR => |_, record| {
            Datum::new_int(avg_int(
                nanos(record.ru.sum_ru_wait_duration),
                record.exec_count,
            ))
        },
        MAX_QUEUED_RC_TIME_STR => |_, record| Datum::new_int(nanos(record.ru.max_ru_wait_duration)),
        AVG_REQUEST_UNIT_V2 => {
            |_, record| Datum::new_real(avg_sum_float(record.ru.sum_ru_v2, record.exec_count))
        }
        MAX_REQUEST_UNIT_V2 => |_, record| Datum::new_real(record.ru.max_ru_v2),
        RESOURCE_GROUP_NAME => |_, record| Datum::new_string(record.resource_group_name.as_bytes()),
        PLAN_CACHE_UNQUALIFIED_STR => {
            |_, record| Datum::new_int(record.plan_cache_unqualified_count)
        }
        PLAN_CACHE_UNQUALIFIED_LAST_REASON_STR => {
            |_, record| Datum::new_string(record.plan_cache_unqualified_last_reason.as_bytes())
        }
        SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL_STR => {
            |_, record| Datum::new_int(record.network.unpacked_bytes_sent_tikv_total)
        }
        SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL_STR => {
            |_, record| Datum::new_int(record.network.unpacked_bytes_received_tikv_total)
        }
        SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE_STR => {
            |_, record| Datum::new_int(record.network.unpacked_bytes_sent_tikv_cross_zone)
        }
        SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE_STR => {
            |_, record| Datum::new_int(record.network.unpacked_bytes_received_tikv_cross_zone)
        }
        SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL_STR => {
            |_, record| Datum::new_int(record.network.unpacked_bytes_sent_tiflash_total)
        }
        SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL_STR => {
            |_, record| Datum::new_int(record.network.unpacked_bytes_received_tiflash_total)
        }
        SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE_STR => {
            |_, record| Datum::new_int(record.network.unpacked_bytes_sent_tiflash_cross_zone)
        }
        SUM_UNPACKED_BYTES_RECEIVE_TIFLASH_CROSS_ZONE_STR => {
            |_, record| Datum::new_int(record.network.unpacked_bytes_received_tiflash_cross_zone)
        }
        STORAGE_KV_STR => |_, record| bool_datum(record.storage_kv),
        STORAGE_MPP_STR => |_, record| bool_datum(record.storage_mpp),
        _ => return None,
    };
    Some(factory)
}

/// Go `makeColumnFactories`.
///
/// # Panics
///
/// Go panics when a column has no registered factory; so does this.
#[must_use]
pub fn make_column_factories(columns: &[ColumnInfo]) -> Vec<ColumnFactory> {
    columns
        .iter()
        .map(|col| {
            column_factory(col.name.original()).unwrap_or_else(|| {
                panic!(
                    "should never happen, should register new column {} into columnValueFactoryMap",
                    col.name.original()
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_chunk::mutrow::MutRow;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

    use super::*;
    use crate::v2::record::{generate_stmt_exec_info_4_test, new_stmt_record};

    /// Go `mockColumnInfo`.
    struct MockColumnInfo;

    impl ColumnInfoSource for MockColumnInfo {
        fn instance_addr(&self) -> String {
            "instance_addr".to_owned()
        }

        fn time_location(&self) -> Tz {
            // Go `time.LoadLocation("Asia/Shanghai")`.
            Tz::Asia__Shanghai
        }
    }

    fn columns(names: &[&str]) -> Vec<ColumnInfo> {
        names
            .iter()
            .map(|name| ColumnInfo {
                name: CiString::new(*name),
                ..ColumnInfo::default()
            })
            .collect()
    }

    /// Go `TestColumn`.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_column() {
        let column_names = [
            CLUSTER_TABLE_INSTANCE_COLUMN_NAME_STR,
            STMT_TYPE_STR,
            SCHEMA_NAME_STR,
            DIGEST_STR,
            DIGEST_TEXT_STR,
            TABLE_NAMES_STR,
            INDEX_NAMES_STR,
            SAMPLE_USER_STR,
            EXEC_COUNT_STR,
            SUM_LATENCY_STR,
            MAX_LATENCY_STR,
            AVG_ROCKSDB_DELETE_SKIPPED_COUNT_STR,
            AVG_ROCKSDB_KEY_SKIPPED_COUNT_STR,
            AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR,
            AVG_ROCKSDB_BLOCK_READ_COUNT_STR,
            AVG_ROCKSDB_BLOCK_READ_BYTE_STR,
            AVG_IA_REMOTE_READ_SEGMENT_COUNT_STR,
            AVG_IA_REMOTE_READ_SEGMENT_SIZE_STR,
            AVG_AFFECTED_ROWS_STR,
            AVG_TIDB_CPU_TIME_STR,
            AVG_TIKV_CPU_TIME_STR,
        ];
        let cols = columns(&column_names);
        let factories = make_column_factories(&cols);
        let info = generate_stmt_exec_info_4_test("digest");
        let mut record = new_stmt_record(&info);
        record.add(&info);
        const ROCKSDB_SUM: u64 = 1 << 63;
        record.sum_rocksdb_delete_skipped_count = ROCKSDB_SUM;
        record.sum_rocksdb_key_skipped_count = ROCKSDB_SUM;
        record.sum_rocksdb_block_cache_hit_count = ROCKSDB_SUM;
        record.sum_rocksdb_block_read_count = ROCKSDB_SUM;
        record.sum_rocksdb_block_read_byte = ROCKSDB_SUM;
        record.sum_ia_remote_read_segment_count = ROCKSDB_SUM;
        record.sum_ia_remote_read_segment_size = ROCKSDB_SUM;
        record.sum_affected_rows = ROCKSDB_SUM;
        #[allow(clippy::cast_precision_loss)]
        let avg_rocksdb_sum = ROCKSDB_SUM as f64 / record.exec_count as f64;
        let double_column_expected = [
            AVG_ROCKSDB_DELETE_SKIPPED_COUNT_STR,
            AVG_ROCKSDB_KEY_SKIPPED_COUNT_STR,
            AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR,
            AVG_ROCKSDB_BLOCK_READ_COUNT_STR,
            AVG_ROCKSDB_BLOCK_READ_BYTE_STR,
            AVG_IA_REMOTE_READ_SEGMENT_COUNT_STR,
            AVG_IA_REMOTE_READ_SEGMENT_SIZE_STR,
            AVG_AFFECTED_ROWS_STR,
        ];

        for (n, factory) in factories.iter().enumerate() {
            let column = factory(&MockColumnInfo, &record);
            let column_name = column_names[n];
            if double_column_expected.contains(&column_name) {
                let mut row = MutRow::from_types(&[FieldType::new(FieldTypeCode::Double)]);
                row.set_datums(std::slice::from_ref(&column));
                assert!(
                    (row.to_row().get_float64(0) - avg_rocksdb_sum).abs() < f64::EPSILON,
                    "{column_name}"
                );
            }
            match column_name {
                CLUSTER_TABLE_INSTANCE_COLUMN_NAME_STR => {
                    assert_eq!(column, Datum::new_string("instance_addr"));
                }
                STMT_TYPE_STR => assert_eq!(column, Datum::new_string(record.stmt_type.as_bytes())),
                SCHEMA_NAME_STR => {
                    assert_eq!(column, Datum::new_string(record.schema_name.as_bytes()));
                }
                DIGEST_STR => assert_eq!(column, Datum::new_string(record.digest.as_bytes())),
                DIGEST_TEXT_STR => {
                    assert_eq!(column, Datum::new_string(record.normalized_sql.as_bytes()));
                }
                TABLE_NAMES_STR => {
                    assert_eq!(column, Datum::new_string(record.table_names.as_bytes()));
                }
                INDEX_NAMES_STR => {
                    assert_eq!(column, Datum::new_string(record.index_names.join(",")));
                }
                SAMPLE_USER_STR => assert_eq!(column, Datum::new_string(info.user.as_bytes())),
                EXEC_COUNT_STR => assert_eq!(column, Datum::new_int(1)),
                SUM_LATENCY_STR => {
                    assert_eq!(column, Datum::new_int(nanos(record.sum_latency)));
                }
                MAX_LATENCY_STR => {
                    assert_eq!(column, Datum::new_int(nanos(record.max_latency)));
                }
                AVG_ROCKSDB_DELETE_SKIPPED_COUNT_STR
                | AVG_ROCKSDB_KEY_SKIPPED_COUNT_STR
                | AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR
                | AVG_ROCKSDB_BLOCK_READ_COUNT_STR
                | AVG_ROCKSDB_BLOCK_READ_BYTE_STR
                | AVG_AFFECTED_ROWS_STR => {
                    assert_eq!(column, Datum::new_real(avg_rocksdb_sum), "{column_name}");
                }
                AVG_TIDB_CPU_TIME_STR => {
                    assert_eq!(column, Datum::new_int(nanos(record.sum_tidb_cpu)));
                }
                AVG_TIKV_CPU_TIME_STR => {
                    assert_eq!(column, Datum::new_int(nanos(record.sum_tikv_cpu)));
                }
                _ => {}
            }
        }

        let small_record = StmtRecord {
            exec_count: 1,
            sum_rocksdb_delete_skipped_count: 7,
            sum_rocksdb_key_skipped_count: 19,
            sum_rocksdb_block_cache_hit_count: 60,
            sum_rocksdb_block_read_count: 21103,
            sum_rocksdb_block_read_byte: 4096,
            sum_affected_rows: 3,
            ..StmtRecord::default()
        };
        let small_cases: [(&str, f64); 6] = [
            (AVG_ROCKSDB_DELETE_SKIPPED_COUNT_STR, 7.0),
            (AVG_ROCKSDB_KEY_SKIPPED_COUNT_STR, 19.0),
            (AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR, 60.0),
            (AVG_ROCKSDB_BLOCK_READ_COUNT_STR, 21103.0),
            (AVG_ROCKSDB_BLOCK_READ_BYTE_STR, 4096.0),
            (AVG_AFFECTED_ROWS_STR, 3.0),
        ];
        for (name, expected) in small_cases {
            let factory =
                column_factory(name).unwrap_or_else(|| panic!("missing column factory: {name}"));
            let datum = factory(&MockColumnInfo, &small_record);
            let mut row = MutRow::from_types(&[FieldType::new(FieldTypeCode::Double)]);
            row.set_datums(&[datum]);
            assert!(
                (row.to_row().get_float64(0) - expected).abs() < f64::EPSILON,
                "{name}"
            );
        }
    }

    /// The six-column IA fixture Go's `TestIAAvgColumns` and
    /// `TestIAAvgColumnsChunkRoundTrip` both build.
    fn ia_columns_fixture() -> (Vec<ColumnFactory>, StmtRecord) {
        let cols = columns(&[
            AVG_IA_REMOTE_READ_SEGMENT_COUNT_STR,
            MAX_IA_REMOTE_READ_SEGMENT_COUNT_STR,
            AVG_IA_REMOTE_READ_SEGMENT_SIZE_STR,
            MAX_IA_REMOTE_READ_SEGMENT_SIZE_STR,
            AVG_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR,
            MAX_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR,
        ]);
        let factories = make_column_factories(&cols);

        let mut info1 = generate_stmt_exec_info_4_test("digest");
        {
            let scan = info1
                .exec_detail
                .cop_exec_details
                .scan_detail
                .as_mut()
                .unwrap();
            scan.ia_remote_read_segment_count = 3;
            scan.ia_remote_read_segment_bytes = 4096;
            scan.ia_remote_read_segment_duration = Duration::from_millis(5);
        }

        let mut info2 = generate_stmt_exec_info_4_test("digest");
        {
            let scan = info2
                .exec_detail
                .cop_exec_details
                .scan_detail
                .as_mut()
                .unwrap();
            scan.ia_remote_read_segment_count = 5;
            scan.ia_remote_read_segment_bytes = 8192;
            scan.ia_remote_read_segment_duration = Duration::from_millis(9);
        }

        let mut record = new_stmt_record(&info1);
        record.add(&info1);
        record.add(&info2);
        (factories, record)
    }

    /// Go `TestIAAvgColumns`.
    #[test]
    fn test_ia_avg_columns() {
        let (factories, record) = ia_columns_fixture();
        assert_eq!(factories[0](&MockColumnInfo, &record), Datum::new_real(4.0));
        assert_eq!(factories[1](&MockColumnInfo, &record), Datum::new_uint(5));
        assert_eq!(
            factories[2](&MockColumnInfo, &record),
            Datum::new_real(6144.0)
        );
        assert_eq!(
            factories[3](&MockColumnInfo, &record),
            Datum::new_uint(8192)
        );
        assert_eq!(
            factories[4](&MockColumnInfo, &record),
            Datum::new_int(nanos(Duration::from_millis(7)))
        );
        assert_eq!(
            factories[5](&MockColumnInfo, &record),
            Datum::new_int(nanos(Duration::from_millis(9)))
        );
    }

    /// Go `TestIAAvgColumnsChunkRoundTrip`.
    #[test]
    fn test_ia_avg_columns_chunk_round_trip() {
        let (factories, record) = ia_columns_fixture();
        let row_datums: Vec<Datum> = factories
            .iter()
            .map(|factory| factory(&MockColumnInfo, &record))
            .collect();

        let mut max_unsigned_type = FieldType::new(FieldTypeCode::LongLong);
        max_unsigned_type.set_flags(FieldTypeFlags::UNSIGNED);
        let ret_types = vec![
            FieldType::new(FieldTypeCode::Double),
            max_unsigned_type.clone(),
            FieldType::new(FieldTypeCode::Double),
            max_unsigned_type,
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
        ];
        let mut mut_row = MutRow::from_types(&ret_types);
        mut_row.set_datums(&row_datums);
        let row = mut_row.to_row();

        assert!((row.get_float64(0) - 4.0).abs() < f64::EPSILON);
        assert_eq!(row.get_uint64(1), 5);
        assert!((row.get_float64(2) - 6144.0).abs() < f64::EPSILON);
        assert_eq!(row.get_uint64(3), 8192);
        assert_eq!(row.get_int64(4), nanos(Duration::from_millis(7)));
        assert_eq!(row.get_int64(5), nanos(Duration::from_millis(9)));
    }
}
