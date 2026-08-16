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

//! SEED of Go `pkg/sessionctx/variable`, covering `slow_log.go`'s formatter:
//! the `SlowLog*Str` display-constant block, the `SlowQueryLogItems` value
//! shape, `writeSlowLogItem`, `kvExecDetailFormat`, and
//! `SessionVars.SlowLogFormat` as [`slow_log_format`], byte for byte.
//!
//! Boundaries:
//! - Go's `SlowLogFormat` reads the live `SessionVars`; this tier passes a
//!   [`SlowLogSessionSnapshot`] carrying exactly the fields the function
//!   body reads. Go also clears `s.CurrentDBChanged` after emitting the
//!   `use <db>;` line; the snapshot here is immutable, so resetting that
//!   flag stays with the caller.
//! - `SlowQueryLogItems.UsedStats` (Go `*stmtctx.UsedStatsInfo`, an unported
//!   map wrapper with `Keys`/`GetUsedInfo`) narrows to a
//!   `BTreeMap<i64, UsedStatsInfoForTable>` over the already-ported
//!   [`crate::used_stats`] leaf; the sorted map iteration is Go's
//!   `slices.Sort(keys)` walk.
//! - `KVExecDetail` (client-go `*util.ExecDetails`, atomic counters read
//!   through `execdetails.LoadTiKVExecDetails`) narrows to the plain
//!   post-load [`TikvExecDetailsSnapshot`] value.
//! - `CopTasks` (`*execdetails.CopTasksDetails`) narrows to
//!   [`CopTasksDetails`]/[`TaskTimeStats`] with `BTreeMap`s where Go sorts
//!   map keys before rendering; only the fields `SlowLogFormat` and
//!   `TaskTimeStats.String` read are carried.
//! - `RUDetails` (client-go `*util.RUDetails`) narrows to
//!   [`RuDetailsSnapshot`]: the five accessor results (`RRU`, `WRU`,
//!   `RUWaitDuration`, `TiKVRUV2`, `TiflashRU`) as plain values.
//! - `RUV2Metrics` (`*execdetails.RUV2Metrics`) narrows to
//!   [`RuV2MetricsSnapshot`]: the atomic/label counters
//!   `execdetails.FormatRUV2Summary` snapshots, with Go's nil-`extra` state
//!   collapsing into zero values; [`format_ruv2_summary`] ports that
//!   function and `calculateRUValuesWithWeights` over the snapshot.
//!   `SessionVars.RUV2Weights()` becomes the snapshot field
//!   [`SlowLogSessionSnapshot::ru_v2_weights`].
//! - `config.GetGlobalConfig().GetKeyspaceObservabilitySlowLogFields()`
//!   (global config, unported) narrows to the pre-resolved name/value list
//!   [`SlowLogSessionSnapshot::keyspace_observability_slow_log_fields`].
//! - `s.StmtCtx.WaitLockLeaseTime` narrows to the plain nanosecond count
//!   [`SlowLogSessionSnapshot::stmt_wait_lock_lease_time`].
//! - Go renders `Warnings` and `SessionConnectAttrs` through
//!   `encoding/json` with `SetEscapeHTML(false)`; [`encode_json_string`]
//!   reproduces that escaping, and the attrs `BTreeMap` is the encoder's
//!   sorted-map-key order.
//! - The `SlowLogFieldAccessor` `Setter`/`Match` closures and the rest of
//!   the rule surface stay open; `ParseSlowLogFieldValue` and the rule
//!   grammar are owned by [`crate::slow_log_parse`].

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use crate::exec_details::{
    format_seconds, get_ia_remote_read_segment_stats, ExecDetails,
    IA_REMOTE_READ_SEGMENT_COUNT_STR, IA_REMOTE_READ_SEGMENT_SIZE_STR,
    IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR,
};
use crate::used_stats::UsedStatsInfoForTable;
use tidb_util::sqlescape::format_go_float64;

/// Go `SlowLogRowPrefixStr`: slow log row prefix.
pub const SLOW_LOG_ROW_PREFIX_STR: &str = "# ";
/// Go `SlowLogSpaceMarkStr`: slow log space mark.
pub const SLOW_LOG_SPACE_MARK_STR: &str = ": ";
/// Go `SlowLogSQLSuffixStr`: slow log suffix.
pub const SLOW_LOG_SQL_SUFFIX_STR: &str = ";";
/// Go `SlowLogTimeStr`: slow log field name.
pub const SLOW_LOG_TIME_STR: &str = "Time";
/// Go `SlowLogStartPrefixStr`: slow log start row prefix
/// (`SlowLogRowPrefixStr + SlowLogTimeStr + SlowLogSpaceMarkStr`).
pub const SLOW_LOG_START_PREFIX_STR: &str = "# Time: ";
/// Go `SlowLogTxnStartTSStr`: slow log field name.
pub const SLOW_LOG_TXN_START_TS_STR: &str = "Txn_start_ts";
/// Go `SlowLogKeyspaceName`: slow log field name.
pub const SLOW_LOG_KEYSPACE_NAME: &str = "Keyspace_name";
/// Go `SlowLogKeyspaceID`: slow log field name.
pub const SLOW_LOG_KEYSPACE_ID: &str = "Keyspace_ID";
/// Go `SlowLogUserAndHostStr`: the user and host field name, which is
/// compatible with MySQL.
pub const SLOW_LOG_USER_AND_HOST_STR: &str = "User@Host";
/// Go `SlowLogUserStr`: slow log field name.
pub const SLOW_LOG_USER_STR: &str = "User";
/// Go `SlowLogHostStr`: only for slow_query table usage.
pub const SLOW_LOG_HOST_STR: &str = "Host";
/// Go `SlowLogPreprocSubQueriesStr`: the number of pre-processed
/// sub-queries.
pub const SLOW_LOG_PREPROC_SUB_QUERIES_STR: &str = "Preproc_subqueries";
/// Go `SlowLogPreProcSubQueryTimeStr`: the total time of pre-processing
/// sub-queries.
pub const SLOW_LOG_PRE_PROC_SUB_QUERY_TIME_STR: &str = "Preproc_subqueries_time";
/// Go `SlowLogIndexNamesStr`: slow log field name.
pub const SLOW_LOG_INDEX_NAMES_STR: &str = "Index_names";
/// Go `SlowLogQuerySQLStr`: used for the slow log table; the slow log file
/// prints the SQL directly without this field name.
pub const SLOW_LOG_QUERY_SQL_STR: &str = "Query";
/// Go `SlowLogStatsInfoStr`: plan stats info.
pub const SLOW_LOG_STATS_INFO_STR: &str = "Stats";
/// Go `SlowLogCopProcAvg`: the average process time of all cop-tasks.
pub const SLOW_LOG_COP_PROC_AVG: &str = "Cop_proc_avg";
/// Go `SlowLogCopProcP90`: the p90 process time of all cop-tasks.
pub const SLOW_LOG_COP_PROC_P90: &str = "Cop_proc_p90";
/// Go `SlowLogCopProcMax`: the max process time of all cop-tasks.
pub const SLOW_LOG_COP_PROC_MAX: &str = "Cop_proc_max";
/// Go `SlowLogCopProcAddr`: the address of TiKV where the cop-task which
/// cost max process time run.
pub const SLOW_LOG_COP_PROC_ADDR: &str = "Cop_proc_addr";
/// Go `SlowLogCopWaitAvg`: the average wait time of all cop-tasks.
pub const SLOW_LOG_COP_WAIT_AVG: &str = "Cop_wait_avg";
/// Go `SlowLogCopWaitP90`: the p90 wait time of all cop-tasks.
pub const SLOW_LOG_COP_WAIT_P90: &str = "Cop_wait_p90";
/// Go `SlowLogCopWaitMax`: the max wait time of all cop-tasks.
pub const SLOW_LOG_COP_WAIT_MAX: &str = "Cop_wait_max";
/// Go `SlowLogCopWaitAddr`: the address of TiKV where the cop-task which
/// cost wait process time run.
pub const SLOW_LOG_COP_WAIT_ADDR: &str = "Cop_wait_addr";
/// Go `SlowLogCopBackoffPrefix`: contains backoff information.
pub const SLOW_LOG_COP_BACKOFF_PREFIX: &str = "Cop_backoff_";
/// Go `SlowLogPrepared`: whether this sql executed in prepare.
pub const SLOW_LOG_PREPARED: &str = "Prepared";
/// Go `SlowLogPlanFromCache`: whether this plan is from plan cache.
pub const SLOW_LOG_PLAN_FROM_CACHE: &str = "Plan_from_cache";
/// Go `SlowLogPlanFromBinding`: whether this plan is matched with the hints
/// in the binding.
pub const SLOW_LOG_PLAN_FROM_BINDING: &str = "Plan_from_binding";
/// Go `SlowLogHasMoreResults`: whether this sql has more following results.
pub const SLOW_LOG_HAS_MORE_RESULTS: &str = "Has_more_results";
/// Go `SlowLogPrevStmt`: the previous executed statement.
pub const SLOW_LOG_PREV_STMT: &str = "Prev_stmt";
/// Go `SlowLogPlan`: the query plan.
pub const SLOW_LOG_PLAN: &str = "Plan";
/// Go `SlowLogBinaryPlan`: the binary plan.
pub const SLOW_LOG_BINARY_PLAN: &str = "Binary_plan";
/// Go `SlowLogPlanPrefix`: the prefix of the plan value
/// (`ast.TiDBDecodePlan + "('"`).
pub const SLOW_LOG_PLAN_PREFIX: &str = "tidb_decode_plan('";
/// Go `SlowLogBinaryPlanPrefix`: the prefix of the binary plan value
/// (`ast.TiDBDecodeBinaryPlan + "('"`).
pub const SLOW_LOG_BINARY_PLAN_PREFIX: &str = "tidb_decode_binary_plan('";
/// Go `SlowLogPlanSuffix`: the suffix of the plan value.
pub const SLOW_LOG_PLAN_SUFFIX: &str = "')";
/// Go `SlowLogPrevStmtPrefix`: the prefix of `Prev_stmt` in the slow log
/// file (`SlowLogPrevStmt + SlowLogSpaceMarkStr`).
pub const SLOW_LOG_PREV_STMT_PREFIX: &str = "Prev_stmt: ";
/// Go `SlowLogBackoffTotal`: the total time doing backoff.
pub const SLOW_LOG_BACKOFF_TOTAL: &str = "Backoff_total";
/// Go `SlowLogExecRetryTime`: the execution retry time.
pub const SLOW_LOG_EXEC_RETRY_TIME: &str = "Exec_retry_time";
/// Go `SlowLogBackoffDetail`: the detail of backoff.
pub const SLOW_LOG_BACKOFF_DETAIL: &str = "Backoff_Detail";
/// Go `SlowLogResultRows`: the row count of the SQL result.
pub const SLOW_LOG_RESULT_ROWS: &str = "Result_rows";
/// Go `SlowLogWarnings`: the warnings generated during executing the
/// statement; some extra warnings are also printed through the slow log.
pub const SLOW_LOG_WARNINGS: &str = "Warnings";
/// Go `SlowLogIsExplicitTxn`: whether this sql executed in an explicit
/// transaction.
pub const SLOW_LOG_IS_EXPLICIT_TXN: &str = "IsExplicitTxn";
/// Go `SlowLogIsWriteCacheTable`: whether writing to the cache table needed
/// to wait for the read lock to expire.
pub const SLOW_LOG_IS_WRITE_CACHE_TABLE: &str = "IsWriteCacheTable";
/// Go `SlowLogIsSyncStatsFailed`: whether any failure happened during sync
/// stats.
pub const SLOW_LOG_IS_SYNC_STATS_FAILED: &str = "IsSyncStatsFailed";
/// Go `SlowLogRRU`: the read request_unit(RU) cost.
pub const SLOW_LOG_RRU: &str = "Request_unit_read";
/// Go `SlowLogWRU`: the write request_unit(RU) cost.
pub const SLOW_LOG_WRU: &str = "Request_unit_write";
/// Go `SlowLogWaitRUDuration`: the total duration for kv requests to wait
/// available request-units.
pub const SLOW_LOG_WAIT_RU_DURATION: &str = "Time_queued_by_rc";
/// Go `SlowLogTidbCPUUsageDuration`: the total tidb cpu usages.
pub const SLOW_LOG_TIDB_CPU_USAGE_DURATION: &str = "Tidb_cpu_time";
/// Go `SlowLogTikvCPUUsageDuration`: the total tikv cpu usages.
pub const SLOW_LOG_TIKV_CPU_USAGE_DURATION: &str = "Tikv_cpu_time";
/// Go `SlowLogStorageFromKV`: whether the statement read data from TiKV.
pub const SLOW_LOG_STORAGE_FROM_KV: &str = "Storage_from_kv";
/// Go `SlowLogStorageFromMPP`: whether the statement read data from
/// TiFlash.
pub const SLOW_LOG_STORAGE_FROM_MPP: &str = "Storage_from_mpp";
/// Go `SlowLogRequestUnitV2`: the RU v2 total for the statement.
pub const SLOW_LOG_REQUEST_UNIT_V2: &str = "Request_unit_v2";
/// Go `SlowLogRequestUnitV2Detail`: the RU v2 detailed metrics for the
/// statement.
pub const SLOW_LOG_REQUEST_UNIT_V2_DETAIL: &str = "Request_unit_v2_detail";
/// Go `SlowLogConnIDStr`: slow log field name.
pub const SLOW_LOG_CONN_ID_STR: &str = "Conn_ID";
/// Go `SlowLogSessAliasStr`: the session alias set by the user.
pub const SLOW_LOG_SESS_ALIAS_STR: &str = "Session_alias";
/// Go `SlowLogQueryTimeStr`: slow log field name.
pub const SLOW_LOG_QUERY_TIME_STR: &str = "Query_time";
/// Go `SlowLogParseTimeStr`: the parse sql time.
pub const SLOW_LOG_PARSE_TIME_STR: &str = "Parse_time";
/// Go `SlowLogCompileTimeStr`: the compile plan time.
pub const SLOW_LOG_COMPILE_TIME_STR: &str = "Compile_time";
/// Go `SlowLogRewriteTimeStr`: the rewrite time.
pub const SLOW_LOG_REWRITE_TIME_STR: &str = "Rewrite_time";
/// Go `SlowLogOptimizeTimeStr`: the optimization time.
pub const SLOW_LOG_OPTIMIZE_TIME_STR: &str = "Optimize_time";
/// Go `SlowLogOptimizeLogicalOpt`: the logical optimization time.
pub const SLOW_LOG_OPTIMIZE_LOGICAL_OPT: &str = "Opt_logical";
/// Go `SlowLogOptimizePhysicalOpt`: the physical optimization time.
pub const SLOW_LOG_OPTIMIZE_PHYSICAL_OPT: &str = "Opt_physical";
/// Go `SlowLogOptimizeBindingMatch`: the binding match time.
pub const SLOW_LOG_OPTIMIZE_BINDING_MATCH: &str = "Opt_binding_match";
/// Go `SlowLogOptimizeStatsSyncWait`: the stats sync wait time.
pub const SLOW_LOG_OPTIMIZE_STATS_SYNC_WAIT: &str = "Opt_stats_sync_wait";
/// Go `SlowLogOptimizeStatsDerive`: the stats derive time.
pub const SLOW_LOG_OPTIMIZE_STATS_DERIVE: &str = "Opt_stats_derive";
/// Go `SlowLogWaitTSTimeStr`: the time of waiting TS.
pub const SLOW_LOG_WAIT_TS_TIME_STR: &str = "Wait_TS";
/// Go `SlowLogDBStr`: slow log field name.
pub const SLOW_LOG_DB_STR: &str = "DB";
/// Go `SlowLogIsInternalStr`: slow log field name.
pub const SLOW_LOG_IS_INTERNAL_STR: &str = "Is_internal";
/// Go `SlowLogDigestStr`: slow log field name.
pub const SLOW_LOG_DIGEST_STR: &str = "Digest";
/// Go `SlowLogNumCopTasksStr`: the number of cop-tasks.
pub const SLOW_LOG_NUM_COP_TASKS_STR: &str = "Num_cop_tasks";
/// Go `SlowLogMemMax`: the max number bytes of memory used in this
/// statement.
pub const SLOW_LOG_MEM_MAX: &str = "Mem_max";
/// Go `SlowLogMemArbitration`: the total wait time(ns) of mem arbitration.
pub const SLOW_LOG_MEM_ARBITRATION: &str = "Mem_arbitration";
/// Go `SlowLogDiskMax`: the max number bytes of disk used in this
/// statement.
pub const SLOW_LOG_DISK_MAX: &str = "Disk_max";
/// Go `SlowLogKVTotal`: the total time waiting for kv.
pub const SLOW_LOG_KV_TOTAL: &str = "KV_total";
/// Go `SlowLogPDTotal`: the total time waiting for pd.
pub const SLOW_LOG_PD_TOTAL: &str = "PD_total";
/// Go `SlowLogUnpackedBytesSentTiKVTotal`: the total bytes sent by tikv.
pub const SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_TOTAL: &str = "Unpacked_bytes_sent_tikv_total";
/// Go `SlowLogUnpackedBytesReceivedTiKVTotal`: the total bytes received by
/// tikv.
pub const SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL: &str = "Unpacked_bytes_received_tikv_total";
/// Go `SlowLogUnpackedBytesSentTiKVCrossZone`: the cross zone bytes sent by
/// tikv.
pub const SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE: &str =
    "Unpacked_bytes_sent_tikv_cross_zone";
/// Go `SlowLogUnpackedBytesReceivedTiKVCrossZone`: the cross zone bytes
/// received by tikv.
pub const SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE: &str =
    "Unpacked_bytes_received_tikv_cross_zone";
/// Go `SlowLogUnpackedBytesSentTiFlashTotal`: the total bytes sent by
/// tiflash.
pub const SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_TOTAL: &str = "Unpacked_bytes_sent_tiflash_total";
/// Go `SlowLogUnpackedBytesReceivedTiFlashTotal`: the total bytes received
/// by tiflash.
pub const SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL: &str =
    "Unpacked_bytes_received_tiflash_total";
/// Go `SlowLogUnpackedBytesSentTiFlashCrossZone`: the cross zone bytes sent
/// by tiflash.
pub const SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE: &str =
    "Unpacked_bytes_sent_tiflash_cross_zone";
/// Go `SlowLogUnpackedBytesReceivedTiFlashCrossZone`: the cross zone bytes
/// received by tiflash.
pub const SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE: &str =
    "Unpacked_bytes_received_tiflash_cross_zone";
/// Go `SlowLogWriteSQLRespTotal`: the total time used to write the response
/// to the client.
pub const SLOW_LOG_WRITE_SQL_RESP_TOTAL: &str = "Write_sql_response_total";
/// Go `SlowLogSucc`: whether this sql executed successfully.
pub const SLOW_LOG_SUCC: &str = "Succ";
/// Go `SlowLogPlanDigest`: the query plan digest.
pub const SLOW_LOG_PLAN_DIGEST: &str = "Plan_digest";
/// Go `SlowLogExecRetryCount`: the execution retry count.
pub const SLOW_LOG_EXEC_RETRY_COUNT: &str = "Exec_retry_count";
/// Go `SlowLogResourceGroup`: the resource group name that the current
/// session binds.
pub const SLOW_LOG_RESOURCE_GROUP: &str = "Resource_group";
/// Go `SlowLogCopMVCCReadAmplification`: total_keys / processed_keys in
/// coprocessor scan detail.
pub const SLOW_LOG_COP_MVCC_READ_AMPLIFICATION: &str = "cop_mvcc_read_amplification";
/// Go `SlowLogSessionConnectAttrs`: the session connection attributes from
/// the client.
pub const SLOW_LOG_SESSION_CONNECT_ATTRS: &str = "Session_connect_attrs";

/// Go's private `zeroStr`.
const ZERO_STR: &str = "0";

/// Go `JSONSQLWarnForSlowLog`: one SQL warning printed through the slow log
/// in JSON format.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JsonSqlWarnForSlowLog {
    /// Go `JSONSQLWarnForSlowLog.Level`.
    pub level: String,
    /// Go `JSONSQLWarnForSlowLog.Message`.
    pub message: String,
    /// Go `JSONSQLWarnForSlowLog.IsExtra` (`json:",omitempty"`): the warning
    /// was recorded only to help diagnostics.
    pub is_extra: bool,
}

/// Go `RewritePhaseInfo` (`session.go`): durations of the rewriting phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RewritePhaseInfo {
    /// Go `RewritePhaseInfo.DurationRewrite`.
    pub duration_rewrite: Duration,
    /// Go `RewritePhaseInfo.DurationPreprocessSubQuery`.
    pub duration_preprocess_sub_query: Duration,
    /// Go `RewritePhaseInfo.PreprocessSubQueries` (`int`).
    pub preprocess_sub_queries: i64,
}

/// Go `ppcpuusage.CPUUsages`: tidb/tikv cpu time of a statement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CpuUsages {
    /// Go `CPUUsages.TidbCPUTime`.
    pub tidb_cpu_time: Duration,
    /// Go `CPUUsages.TikvCPUTime`.
    pub tikv_cpu_time: Duration,
}

/// Go `execdetails.TaskTimeStats` — the fields `SlowLogFormat` and
/// `TaskTimeStats.String` read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskTimeStats {
    /// Go `TaskTimeStats.AvgTime`.
    pub avg_time: Duration,
    /// Go `TaskTimeStats.P90Time`.
    pub p90_time: Duration,
    /// Go `TaskTimeStats.MaxAddress`.
    pub max_address: String,
    /// Go `TaskTimeStats.MaxTime`.
    pub max_time: Duration,
    /// Go `TaskTimeStats.TotTime`.
    pub tot_time: Duration,
}

impl TaskTimeStats {
    /// Go `TaskTimeStats.String`: the `%v`-spelled avg/p90/max/addr line,
    /// with the two-column short form for a single cop task.
    #[must_use]
    pub fn render(
        &self,
        num_cop_tasks: i64,
        space_mark_str: &str,
        avg_str: &str,
        p90_str: &str,
        max_str: &str,
        addr_str: &str,
    ) -> String {
        if num_cop_tasks == 1 {
            return format!(
                "{avg_str}{space_mark_str}{} {addr_str}{space_mark_str}{}",
                format_go_float64(self.avg_time.as_secs_f64()),
                self.max_address,
            );
        }
        format!(
            "{avg_str}{space_mark_str}{} {p90_str}{space_mark_str}{} \
             {max_str}{space_mark_str}{} {addr_str}{space_mark_str}{}",
            format_go_float64(self.avg_time.as_secs_f64()),
            format_go_float64(self.p90_time.as_secs_f64()),
            format_go_float64(self.max_time.as_secs_f64()),
            self.max_address,
        )
    }
}

/// Go `execdetails.CopTasksDetails` — the fields `SlowLogFormat` reads.
/// Go's `map[string]...` backoff maps become `BTreeMap`s: `SlowLogFormat`
/// sorts the backoff names before rendering, so sorted iteration is the
/// same walk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopTasksDetails {
    /// Go `CopTasksDetails.NumCopTasks` (`int`).
    pub num_cop_tasks: i64,
    /// Go `CopTasksDetails.ProcessTimeStats`.
    pub process_time_stats: TaskTimeStats,
    /// Go `CopTasksDetails.WaitTimeStats`.
    pub wait_time_stats: TaskTimeStats,
    /// Go `CopTasksDetails.BackoffTimeStatsMap`.
    pub backoff_time_stats_map: BTreeMap<String, TaskTimeStats>,
    /// Go `CopTasksDetails.TotBackoffTimes` (`map[string]int`).
    pub tot_backoff_times: BTreeMap<String, i64>,
}

/// The plain value `execdetails.LoadTiKVExecDetails` produces from a
/// client-go `*util.ExecDetails` (`TiKVExecDetailsSnapshot`): Go's atomic
/// counters arrive here already loaded. Durations stay Go's `int64`
/// nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TikvExecDetailsSnapshot {
    /// Go `TiKVExecDetailsSnapshot.WaitKVRespDuration` (ns).
    pub wait_kv_resp_duration: i64,
    /// Go `TiKVExecDetailsSnapshot.WaitPDRespDuration` (ns).
    pub wait_pd_resp_duration: i64,
    /// Go `TiKVExecDetailsSnapshot.BackoffDuration` (ns).
    pub backoff_duration: i64,
    /// Go `TiKVExecDetailsSnapshot.UnpackedBytesSentKVTotal`.
    pub unpacked_bytes_sent_kv_total: i64,
    /// Go `TiKVExecDetailsSnapshot.UnpackedBytesReceivedKVTotal`.
    pub unpacked_bytes_received_kv_total: i64,
    /// Go `TiKVExecDetailsSnapshot.UnpackedBytesSentKVCrossZone`.
    pub unpacked_bytes_sent_kv_cross_zone: i64,
    /// Go `TiKVExecDetailsSnapshot.UnpackedBytesReceivedKVCrossZone`.
    pub unpacked_bytes_received_kv_cross_zone: i64,
    /// Go `TiKVExecDetailsSnapshot.UnpackedBytesSentMPPTotal`.
    pub unpacked_bytes_sent_mpp_total: i64,
    /// Go `TiKVExecDetailsSnapshot.UnpackedBytesReceivedMPPTotal`.
    pub unpacked_bytes_received_mpp_total: i64,
    /// Go `TiKVExecDetailsSnapshot.UnpackedBytesSentMPPCrossZone`.
    pub unpacked_bytes_sent_mpp_cross_zone: i64,
    /// Go `TiKVExecDetailsSnapshot.UnpackedBytesReceivedMPPCrossZone`.
    pub unpacked_bytes_received_mpp_cross_zone: i64,
}

/// The five accessor results `SlowLogFormat` reads off a client-go
/// `*util.RUDetails` (`RRU`, `WRU`, `RUWaitDuration`, `TiKVRUV2`,
/// `TiflashRU`), as plain values. Go's nil pointer is `None` at the items
/// field; the accessors are nil-safe zeros there.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RuDetailsSnapshot {
    /// Go `RUDetails.RRU()`.
    pub rru: f64,
    /// Go `RUDetails.WRU()`.
    pub wru: f64,
    /// Go `RUDetails.RUWaitDuration()`.
    pub ru_wait_duration: Duration,
    /// Go `RUDetails.TiKVRUV2()`.
    pub tikv_ru_v2: f64,
    /// Go `RUDetails.TiflashRU()`.
    pub tiflash_ru: f64,
}

/// Go `execdetails.RUV2Weights`: the per-counter weights
/// `calculateRUValuesWithWeights` multiplies with.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RuV2Weights {
    /// Go `RUV2Weights.RUScale`.
    pub ru_scale: f64,
    /// Go `RUV2Weights.ResultChunkCells`.
    pub result_chunk_cells: f64,
    /// Go `RUV2Weights.ExecutorL1`.
    pub executor_l1: f64,
    /// Go `RUV2Weights.ExecutorL2`.
    pub executor_l2: f64,
    /// Go `RUV2Weights.ExecutorL3`.
    pub executor_l3: f64,
    /// Go `RUV2Weights.ExecutorL5InsertRows`.
    pub executor_l5_insert_rows: f64,
    /// Go `RUV2Weights.PlanCnt`.
    pub plan_cnt: f64,
    /// Go `RUV2Weights.PlanDeriveStatsPaths`.
    pub plan_derive_stats_paths: f64,
    /// Go `RUV2Weights.ResourceManagerReadCnt`.
    pub resource_manager_read_cnt: f64,
    /// Go `RUV2Weights.ResourceManagerWriteCnt`.
    pub resource_manager_write_cnt: f64,
    /// Go `RUV2Weights.WriteKeys`.
    pub write_keys: f64,
    /// Go `RUV2Weights.SessionParserTotal`.
    pub session_parser_total: f64,
    /// Go `RUV2Weights.TxnCnt`.
    pub txn_cnt: f64,
}

/// The counters `execdetails.FormatRUV2Summary` snapshots off a
/// `*execdetails.RUV2Metrics`: Go's atomics and lazily-allocated `extra`
/// block collapse into plain (zero-defaulted) values. Label maps are
/// `BTreeMap`s — Go sorts label keys before rendering, so sorted iteration
/// is the same walk.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuV2MetricsSnapshot {
    /// Go `RUV2Metrics.Bypass()`.
    pub bypass: bool,
    /// Go `RUV2Metrics.ResultChunkCells()`.
    pub result_chunk_cells: i64,
    /// Go `RUV2Metrics.executorL1.snapshot()`.
    pub executor_l1: BTreeMap<String, i64>,
    /// Go `extra.executorL2` snapshot.
    pub executor_l2: BTreeMap<String, i64>,
    /// Go `extra.executorL3` snapshot.
    pub executor_l3: BTreeMap<String, i64>,
    /// Go `extra.executorL5InsertRows`.
    pub executor_l5_insert_rows: i64,
    /// Go `RUV2Metrics.PlanCnt()`.
    pub plan_cnt: i64,
    /// Go `extra.planDeriveStatsPaths`.
    pub plan_derive_stats_paths: i64,
    /// Go `RUV2Metrics.SessionParserTotal()`.
    pub session_parser_total: i64,
    /// Go `RUV2Metrics.TxnCnt()`.
    pub txn_cnt: i64,
    /// Go `RUV2Metrics.ResourceManagerReadCnt()`.
    pub resource_manager_read_cnt: i64,
    /// Go `extra.resourceManagerWriteCnt`.
    pub resource_manager_write_cnt: i64,
    /// Go `extra.writeKeys`.
    pub write_keys: i64,
    /// Go `extra.writeSize`.
    pub write_size: i64,
    /// Go `RUV2Metrics.TiKVKVEngineCacheMiss()`.
    pub tikv_kv_engine_cache_miss: i64,
    /// Go `extra.tikvCoprocessorExecutorIterations`.
    pub tikv_coprocessor_executor_iterations: i64,
    /// Go `extra.tikvCoprocessorResponseBytes`.
    pub tikv_coprocessor_response_bytes: i64,
    /// Go `extra.tikvRaftstoreStoreWriteTriggerWB`.
    pub tikv_raftstore_store_write_trigger_wb: i64,
    /// Go `RUV2Metrics.TiKVStorageProcessedKeysBatchGet()`.
    pub tikv_storage_processed_keys_batch_get: i64,
    /// Go `RUV2Metrics.TiKVStorageProcessedKeysGet()`.
    pub tikv_storage_processed_keys_get: i64,
    /// Go `extra.tikvCoprocessorWorkTotal` snapshot.
    pub tikv_coprocessor_executor_work_total: BTreeMap<String, i64>,
}

impl RuV2MetricsSnapshot {
    /// Go `RUV2Metrics.calculateRUValuesWithWeights`: the weighted TiDB RU.
    #[must_use]
    fn calculate_ru_values_with_weights(&self, weights: &RuV2Weights) -> f64 {
        let sum = |map: &BTreeMap<String, i64>| map.values().sum::<i64>();
        #[expect(clippy::cast_precision_loss, reason = "Go float64(int64) conversion")]
        let tidb_ru_float = self.result_chunk_cells as f64 * weights.result_chunk_cells
            + sum(&self.executor_l1) as f64 * weights.executor_l1
            + sum(&self.executor_l2) as f64 * weights.executor_l2
            + sum(&self.executor_l3) as f64 * weights.executor_l3
            + self.executor_l5_insert_rows as f64 * weights.executor_l5_insert_rows
            + self.plan_cnt as f64 * weights.plan_cnt
            + self.plan_derive_stats_paths as f64 * weights.plan_derive_stats_paths
            + self.resource_manager_read_cnt as f64 * weights.resource_manager_read_cnt
            + self.resource_manager_write_cnt as f64 * weights.resource_manager_write_cnt
            + self.write_keys as f64 * weights.write_keys
            + self.session_parser_total as f64 * weights.session_parser_total
            + self.txn_cnt as f64 * weights.txn_cnt;
        tidb_ru_float * weights.ru_scale
    }

    /// Whether every counter Go's all-zero check inspects is zero.
    fn is_all_zero(&self) -> bool {
        self.result_chunk_cells == 0
            && self.executor_l1.is_empty()
            && self.executor_l2.is_empty()
            && self.executor_l3.is_empty()
            && self.executor_l5_insert_rows == 0
            && self.plan_cnt == 0
            && self.plan_derive_stats_paths == 0
            && self.session_parser_total == 0
            && self.txn_cnt == 0
            && self.resource_manager_read_cnt == 0
            && self.resource_manager_write_cnt == 0
            && self.write_keys == 0
            && self.write_size == 0
            && self.tikv_kv_engine_cache_miss == 0
            && self.tikv_coprocessor_executor_iterations == 0
            && self.tikv_coprocessor_response_bytes == 0
            && self.tikv_raftstore_store_write_trigger_wb == 0
            && self.tikv_storage_processed_keys_batch_get == 0
            && self.tikv_storage_processed_keys_get == 0
            && self.tikv_coprocessor_executor_work_total.is_empty()
    }
}

/// Go `execdetails.formatRUV2LabelMap`: non-zero labels, sorted, as
/// `{k:v,...}`; empty when nothing is non-zero.
fn format_ruv2_label_map(values: &BTreeMap<String, i64>) -> String {
    let mut builder = String::new();
    for (key, value) in values {
        if *value == 0 {
            continue;
        }
        builder.push(if builder.is_empty() { '{' } else { ',' });
        builder.push_str(key);
        builder.push(':');
        let _ = write!(builder, "{value}");
    }
    if builder.is_empty() {
        return builder;
    }
    builder.push('}');
    builder
}

/// Go `execdetails.FormatRUV2Summary`: the RUv2 total and detailed metrics
/// in one pass, over the narrowed [`RuV2MetricsSnapshot`].
#[must_use]
pub fn format_ruv2_summary(
    metrics: Option<&RuV2MetricsSnapshot>,
    weights: &RuV2Weights,
    tikv_ru: f64,
    tiflash_ru: f64,
) -> (String, String) {
    if let Some(metrics) = metrics {
        if metrics.bypass {
            return (String::new(), String::new());
        }
    }
    let zero = RuV2MetricsSnapshot::default();
    let (snapshot, tidb_ru) = match metrics {
        Some(metrics) => (metrics, metrics.calculate_ru_values_with_weights(weights)),
        None => (&zero, 0.0),
    };
    if snapshot.is_all_zero() && tikv_ru == 0.0 && tiflash_ru == 0.0 {
        return (String::new(), String::new());
    }

    let mut parts: Vec<String> = Vec::with_capacity(19);
    let append_int = |parts: &mut Vec<String>, key: &str, value: i64| {
        if value != 0 {
            parts.push(format!("{key}:{value}"));
        }
    };
    let append_float_always =
        |parts: &mut Vec<String>, key: &str, value: f64| parts.push(format!("{key}:{value:.2}"));
    let append_map = |parts: &mut Vec<String>, key: &str, value: &BTreeMap<String, i64>| {
        if value.is_empty() {
            return;
        }
        let formatted = format_ruv2_label_map(value);
        if !formatted.is_empty() {
            parts.push(format!("{key}:{formatted}"));
        }
    };

    let total_ru = tidb_ru + tikv_ru + tiflash_ru;
    let total = format!("{total_ru:.2}");
    append_float_always(&mut parts, "total_ru", total_ru);
    append_float_always(&mut parts, "tidb_ru", tidb_ru);
    append_float_always(&mut parts, "tikv_ru", tikv_ru);
    append_float_always(&mut parts, "tiflash_ru", tiflash_ru);

    append_int(
        &mut parts,
        "result_chunk_cells",
        snapshot.result_chunk_cells,
    );
    append_map(&mut parts, "executor_l1", &snapshot.executor_l1);
    append_map(&mut parts, "executor_l2", &snapshot.executor_l2);
    append_map(&mut parts, "executor_l3", &snapshot.executor_l3);
    append_int(
        &mut parts,
        "executor_l5_insert_rows",
        snapshot.executor_l5_insert_rows,
    );
    append_int(&mut parts, "plan_cnt", snapshot.plan_cnt);
    append_int(
        &mut parts,
        "plan_derive_stats_paths",
        snapshot.plan_derive_stats_paths,
    );
    append_int(
        &mut parts,
        "session_parser_total",
        snapshot.session_parser_total,
    );
    append_int(&mut parts, "txn_cnt", snapshot.txn_cnt);
    append_int(
        &mut parts,
        "resource_manager_read_cnt",
        snapshot.resource_manager_read_cnt,
    );
    append_int(
        &mut parts,
        "resource_manager_write_cnt",
        snapshot.resource_manager_write_cnt,
    );
    append_int(&mut parts, "write_keys", snapshot.write_keys);
    append_int(&mut parts, "write_size", snapshot.write_size);
    append_int(
        &mut parts,
        "tikv_kv_engine_cache_miss",
        snapshot.tikv_kv_engine_cache_miss,
    );
    append_int(
        &mut parts,
        "tikv_coprocessor_executor_iterations",
        snapshot.tikv_coprocessor_executor_iterations,
    );
    append_int(
        &mut parts,
        "tikv_coprocessor_response_bytes",
        snapshot.tikv_coprocessor_response_bytes,
    );
    append_int(
        &mut parts,
        "tikv_raftstore_store_write_trigger_wb_bytes",
        snapshot.tikv_raftstore_store_write_trigger_wb,
    );
    append_int(
        &mut parts,
        "tikv_storage_processed_keys_batch_get",
        snapshot.tikv_storage_processed_keys_batch_get,
    );
    append_int(
        &mut parts,
        "tikv_storage_processed_keys_get",
        snapshot.tikv_storage_processed_keys_get,
    );
    append_map(
        &mut parts,
        "tikv_coprocessor_executor_work_total",
        &snapshot.tikv_coprocessor_executor_work_total,
    );

    (total, parts.join(", "))
}

/// One resolved keyspace-observability slow-log field: the `Name`/`Value`
/// pair `config.GetKeyspaceObservabilitySlowLogFields` returns.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyspaceObservabilityField {
    /// The field name written as the slow-log key.
    pub name: String,
    /// The pre-resolved field value.
    pub value: String,
}

/// Go `SlowQueryLogItems`: the collection of items that should be included
/// in the slow query log. Pointer-typed Go fields whose owners are unported
/// arrive here as the narrowed snapshots documented on each field.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SlowQueryLogItems {
    /// Go `SlowQueryLogItems.TxnTS`.
    pub txn_ts: u64,
    /// Go `SlowQueryLogItems.KeyspaceName`.
    pub keyspace_name: String,
    /// Go `SlowQueryLogItems.KeyspaceID`.
    pub keyspace_id: u32,
    /// Go `SlowQueryLogItems.SQL`.
    pub sql: String,
    /// Go `SlowQueryLogItems.Digest`.
    pub digest: String,
    /// Go `SlowQueryLogItems.TimeTotal`.
    pub time_total: Duration,
    /// Go `SlowQueryLogItems.IndexNames`.
    pub index_names: String,
    /// Go `SlowQueryLogItems.Succ`.
    pub succ: bool,
    /// Go `SlowQueryLogItems.IsExplicitTxn`.
    pub is_explicit_txn: bool,
    /// Go `SlowQueryLogItems.IsWriteCacheTable`.
    pub is_write_cache_table: bool,
    /// Go `SlowQueryLogItems.IsSyncStatsFailed`.
    pub is_sync_stats_failed: bool,
    /// Go `SlowQueryLogItems.Prepared`.
    pub prepared: bool,
    /// Go `SlowQueryLogItems.PlanFromCache`.
    pub plan_from_cache: bool,
    /// Go `SlowQueryLogItems.PlanFromBinding`.
    pub plan_from_binding: bool,
    /// Go `SlowQueryLogItems.HasMoreResults`.
    pub has_more_results: bool,
    /// Go `SlowQueryLogItems.PrevStmt`.
    pub prev_stmt: String,
    /// Go `SlowQueryLogItems.Plan`.
    pub plan: String,
    /// Go `SlowQueryLogItems.PlanDigest` (a pre-rendered digest string in Go
    /// as well).
    pub plan_digest: String,
    /// Go `SlowQueryLogItems.BinaryPlan`.
    pub binary_plan: String,
    /// Go `SlowQueryLogItems.UsedStats` (`*stmtctx.UsedStatsInfo`), narrowed
    /// to table-ID → per-table info over [`crate::used_stats`]; sorted map
    /// iteration is Go's `slices.Sort(keys)` walk. An empty map is Go's
    /// nil/empty info.
    pub used_stats: BTreeMap<i64, UsedStatsInfoForTable>,
    /// Go `SlowQueryLogItems.CopTasks` (`*execdetails.CopTasksDetails`),
    /// narrowed to the fields `SlowLogFormat` reads.
    pub cop_tasks: Option<CopTasksDetails>,
    /// Go `SlowQueryLogItems.RewriteInfo`.
    pub rewrite_info: RewritePhaseInfo,
    /// Go `SlowQueryLogItems.WriteSQLRespTotal`.
    pub write_sql_resp_total: Duration,
    /// Go `SlowQueryLogItems.KVExecDetail` (client-go `*util.ExecDetails`),
    /// narrowed to the post-`LoadTiKVExecDetails` snapshot; `None` is Go's
    /// nil pointer (the all-`"0"` rendering).
    pub kv_exec_detail: Option<TikvExecDetailsSnapshot>,
    /// Go `SlowQueryLogItems.ExecDetail` (`*execdetails.ExecDetails`); Go
    /// dereferences it unconditionally, so it is a plain value here.
    pub exec_detail: ExecDetails,
    /// Go `SlowQueryLogItems.ExecRetryCount`.
    pub exec_retry_count: u64,
    /// Go `SlowQueryLogItems.ExecRetryTime`.
    pub exec_retry_time: Duration,
    /// Go `SlowQueryLogItems.ResultRows`.
    pub result_rows: i64,
    /// Go `SlowQueryLogItems.Warnings`.
    pub warnings: Vec<JsonSqlWarnForSlowLog>,
    /// Go `SlowQueryLogItems.ResourceGroupName`.
    pub resource_group_name: String,
    /// Go `SlowQueryLogItems.RUDetails` (client-go `*util.RUDetails`),
    /// narrowed to its five accessor results; `None` is Go's nil pointer
    /// (nil-safe zero accessors).
    pub ru_details: Option<RuDetailsSnapshot>,
    /// Go `SlowQueryLogItems.RUV2Metrics` (`*execdetails.RUV2Metrics`),
    /// narrowed to the counters `FormatRUV2Summary` snapshots.
    pub ruv2_metrics: Option<RuV2MetricsSnapshot>,
    /// Go `SlowQueryLogItems.MemMax`.
    pub mem_max: i64,
    /// Go `SlowQueryLogItems.DiskMax`.
    pub disk_max: i64,
    /// Go `SlowQueryLogItems.CPUUsages`.
    pub cpu_usages: CpuUsages,
    /// Go `SlowQueryLogItems.StorageKV`: query read from TiKV.
    pub storage_kv: bool,
    /// Go `SlowQueryLogItems.StorageMPP`: query read from TiFlash.
    pub storage_mpp: bool,
    /// Go `SlowQueryLogItems.MemArbitration`: total mem-arbitration wait in
    /// seconds.
    pub mem_arbitration: f64,
    /// Go `SlowQueryLogItems.SessionConnectAttrs` (`map[string]string`); the
    /// `BTreeMap` order is `encoding/json`'s sorted-map-key order.
    pub session_connect_attrs: BTreeMap<String, String>,
}

/// Go `auth.UserIdentity` — the two fields `SlowLogFormat` reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SlowLogUserIdentity {
    /// Go `UserIdentity.Username`.
    pub username: String,
    /// Go `UserIdentity.Hostname`.
    pub hostname: String,
}

/// Go `SessionVars.DurationOptimizer` — the six duration fields
/// `SlowLogFormat` reads (`TiFlashInfoFetch` is not rendered and stays
/// open).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OptimizerDurations {
    /// Go `DurationOptimizer.Total`.
    pub total: Duration,
    /// Go `DurationOptimizer.LogicalOpt`.
    pub logical_opt: Duration,
    /// Go `DurationOptimizer.PhysicalOpt`.
    pub physical_opt: Duration,
    /// Go `DurationOptimizer.BindingMatch`.
    pub binding_match: Duration,
    /// Go `DurationOptimizer.StatsSyncWait`.
    pub stats_sync_wait: Duration,
    /// Go `DurationOptimizer.StatsDerive`.
    pub stats_derive: Duration,
}

/// The `SessionVars` fields `SlowLogFormat` reads. Go reads these off the
/// live `SessionVars` (plus `s.StmtCtx` and the global config); this tier
/// passes an immutable snapshot, so Go's in-place reset of
/// `CurrentDBChanged` after emitting `use <db>;` stays with the caller.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SlowLogSessionSnapshot {
    /// Go `SessionVars.User` (`*auth.UserIdentity`).
    pub user: Option<SlowLogUserIdentity>,
    /// Go `SessionVars.ConnectionInfo.ClientIP`; `Some` means
    /// `ConnectionInfo != nil` (an empty ClientIP still overrides the user
    /// hostname, as in Go).
    pub connection_info_client_ip: Option<String>,
    /// Go `SessionVars.ConnectionID`.
    pub connection_id: u64,
    /// Go `SessionVars.SessionAlias`.
    pub session_alias: String,
    /// Go `SessionVars.DurationParse`.
    pub duration_parse: Duration,
    /// Go `SessionVars.DurationCompile`.
    pub duration_compile: Duration,
    /// Go `SessionVars.DurationOptimizer`.
    pub duration_optimizer: OptimizerDurations,
    /// Go `SessionVars.DurationWaitTS`.
    pub duration_wait_ts: Duration,
    /// Go `SessionVars.CurrentDB`.
    pub current_db: String,
    /// Go `SessionVars.InRestrictedSQL`.
    pub in_restricted_sql: bool,
    /// Go `s.StmtCtx.WaitLockLeaseTime` (ns), narrowed to a plain count; it
    /// only gates the `IsWriteCacheTable` line.
    pub stmt_wait_lock_lease_time: i64,
    /// Go `SessionVars.RUV2Weights()`, pre-resolved.
    pub ru_v2_weights: RuV2Weights,
    /// Go `config.GetGlobalConfig().GetKeyspaceObservabilitySlowLogFields()`,
    /// pre-resolved name/value pairs.
    pub keyspace_observability_slow_log_fields: Vec<KeyspaceObservabilityField>,
    /// Go `SessionVars.CurrentDBChanged`.
    pub current_db_changed: bool,
}

/// Go's `strconv.FormatFloat(value, 'f', -1, 64)` for a plain `f64`:
/// shortest round-tripping decimal, never exponent notation — Rust `f64`
/// `Display`'s exact contract.
fn format_float_f(value: f64) -> String {
    format!("{value}")
}

/// Go `writeSlowLogItem`: writes one item as `# ${key}: ${value}\n`.
fn write_slow_log_item(buf: &mut String, key: &str, value: &str) {
    buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
    buf.push_str(key);
    buf.push_str(SLOW_LOG_SPACE_MARK_STR);
    buf.push_str(value);
    buf.push('\n');
}

/// Go `encoding/json` string escaping with `SetEscapeHTML(false)`: quote,
/// backslash, `\n`/`\r`/`\t`, other control bytes as `\u00XX`, and the
/// always-escaped U+2028/U+2029.
fn encode_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            control if (control as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", control as u32);
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

/// Go's JSON encoding of `[]JSONSQLWarnForSlowLog` (field order
/// `Level`, `Message`, `IsExtra` with `omitempty`); `json.Encoder.Encode`
/// appends the trailing newline itself.
fn encode_warnings(out: &mut String, warnings: &[JsonSqlWarnForSlowLog]) {
    out.push('[');
    for (index, warning) in warnings.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str("{\"Level\":");
        encode_json_string(out, &warning.level);
        out.push_str(",\"Message\":");
        encode_json_string(out, &warning.message);
        if warning.is_extra {
            out.push_str(",\"IsExtra\":true");
        }
        out.push('}');
    }
    out.push_str("]\n");
}

/// Go's JSON encoding of `map[string]string` (sorted keys, no HTML
/// escaping); `json.Encoder.Encode` appends the trailing newline itself.
fn encode_connect_attrs(out: &mut String, attrs: &BTreeMap<String, String>) {
    out.push('{');
    for (index, (key, value)) in attrs.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        encode_json_string(out, key);
        out.push(':');
        encode_json_string(out, value);
    }
    out.push_str("}\n");
}

/// Go `kvExecDetailFormat`: the KV/PD/backoff totals and the eight unpacked
/// byte counters, all `"0"` for a nil detail.
fn kv_exec_detail_format(buf: &mut String, kv_exec_detail: Option<&TikvExecDetailsSnapshot>) {
    let Some(snapshot) = kv_exec_detail else {
        write_slow_log_item(buf, SLOW_LOG_KV_TOTAL, ZERO_STR);
        write_slow_log_item(buf, SLOW_LOG_PD_TOTAL, ZERO_STR);
        write_slow_log_item(buf, SLOW_LOG_BACKOFF_TOTAL, ZERO_STR);
        write_slow_log_item(buf, SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_TOTAL, ZERO_STR);
        write_slow_log_item(buf, SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL, ZERO_STR);
        write_slow_log_item(buf, SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE, ZERO_STR);
        write_slow_log_item(
            buf,
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE,
            ZERO_STR,
        );
        write_slow_log_item(buf, SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_TOTAL, ZERO_STR);
        write_slow_log_item(
            buf,
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL,
            ZERO_STR,
        );
        write_slow_log_item(
            buf,
            SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE,
            ZERO_STR,
        );
        write_slow_log_item(
            buf,
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE,
            ZERO_STR,
        );
        return;
    };
    // Go converts the atomic int64 nanoseconds through time.Duration; a
    // negative count would carry its sign into Seconds(). Snapshots here are
    // non-negative in practice; saturate at zero rather than panic.
    let ns = |value: i64| Duration::from_nanos(value.try_into().unwrap_or(0));
    write_slow_log_item(
        buf,
        SLOW_LOG_KV_TOTAL,
        &format_seconds(ns(snapshot.wait_kv_resp_duration)),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_PD_TOTAL,
        &format_seconds(ns(snapshot.wait_pd_resp_duration)),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_BACKOFF_TOTAL,
        &format_seconds(ns(snapshot.backoff_duration)),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_TOTAL,
        &snapshot.unpacked_bytes_sent_kv_total.to_string(),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL,
        &snapshot.unpacked_bytes_received_kv_total.to_string(),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE,
        &snapshot.unpacked_bytes_sent_kv_cross_zone.to_string(),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE,
        &snapshot.unpacked_bytes_received_kv_cross_zone.to_string(),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_TOTAL,
        &snapshot.unpacked_bytes_sent_mpp_total.to_string(),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL,
        &snapshot.unpacked_bytes_received_mpp_total.to_string(),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE,
        &snapshot.unpacked_bytes_sent_mpp_cross_zone.to_string(),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE,
        &snapshot.unpacked_bytes_received_mpp_cross_zone.to_string(),
    );
}

/// Go `SessionVars.SlowLogFormat`: formats one statement's slow-log entry,
/// arm for arm and byte for byte, over the [`SlowLogSessionSnapshot`] this
/// tier passes in place of the live `SessionVars`.
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "Go SlowLogFormat is one long arm sequence; splitting would obscure the arm order"
)]
pub fn slow_log_format(session: &SlowLogSessionSnapshot, items: &SlowQueryLogItems) -> String {
    let mut buf = String::new();

    write_slow_log_item(
        &mut buf,
        SLOW_LOG_TXN_START_TS_STR,
        &items.txn_ts.to_string(),
    );
    if !items.keyspace_name.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_KEYSPACE_NAME, &items.keyspace_name);
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_KEYSPACE_ID,
            &items.keyspace_id.to_string(),
        );
    }

    if let Some(user) = &session.user {
        let host_address = session
            .connection_info_client_ip
            .as_deref()
            .unwrap_or(&user.hostname);
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_USER_AND_HOST_STR,
            &format!(
                "{}[{}] @ {} [{}]",
                user.username, user.username, user.hostname, host_address
            ),
        );
    }
    if session.connection_id != 0 {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_CONN_ID_STR,
            &session.connection_id.to_string(),
        );
    }
    if !session.session_alias.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_SESS_ALIAS_STR, &session.session_alias);
    }
    if items.exec_retry_count > 0 {
        buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
        buf.push_str(SLOW_LOG_EXEC_RETRY_TIME);
        buf.push_str(SLOW_LOG_SPACE_MARK_STR);
        buf.push_str(&format_seconds(items.exec_retry_time));
        buf.push(' ');
        buf.push_str(SLOW_LOG_EXEC_RETRY_COUNT);
        buf.push_str(SLOW_LOG_SPACE_MARK_STR);
        #[expect(
            clippy::cast_possible_wrap,
            reason = "Go's strconv.Itoa(int(uint64)) wrap"
        )]
        let _ = write!(buf, "{}", items.exec_retry_count as i64);
        buf.push('\n');
    }
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_QUERY_TIME_STR,
        &format_seconds(items.time_total),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_PARSE_TIME_STR,
        &format_seconds(session.duration_parse),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_COMPILE_TIME_STR,
        &format_seconds(session.duration_compile),
    );

    buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
    buf.push_str(SLOW_LOG_REWRITE_TIME_STR);
    buf.push_str(SLOW_LOG_SPACE_MARK_STR);
    buf.push_str(&format_seconds(items.rewrite_info.duration_rewrite));
    if items.rewrite_info.preprocess_sub_queries > 0 {
        let _ = write!(
            buf,
            " {SLOW_LOG_PREPROC_SUB_QUERIES_STR}{SLOW_LOG_SPACE_MARK_STR}{} \
             {SLOW_LOG_PRE_PROC_SUB_QUERY_TIME_STR}{SLOW_LOG_SPACE_MARK_STR}{}",
            items.rewrite_info.preprocess_sub_queries,
            format_seconds(items.rewrite_info.duration_preprocess_sub_query),
        );
    }
    buf.push('\n');

    // Optimizer time.
    let optimizer = &session.duration_optimizer;
    buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
    let _ = write!(
        buf,
        "{SLOW_LOG_OPTIMIZE_TIME_STR}{SLOW_LOG_SPACE_MARK_STR}{} \
         {SLOW_LOG_OPTIMIZE_LOGICAL_OPT}{SLOW_LOG_SPACE_MARK_STR}{} \
         {SLOW_LOG_OPTIMIZE_PHYSICAL_OPT}{SLOW_LOG_SPACE_MARK_STR}{} \
         {SLOW_LOG_OPTIMIZE_BINDING_MATCH}{SLOW_LOG_SPACE_MARK_STR}{} \
         {SLOW_LOG_OPTIMIZE_STATS_SYNC_WAIT}{SLOW_LOG_SPACE_MARK_STR}{} \
         {SLOW_LOG_OPTIMIZE_STATS_DERIVE}{SLOW_LOG_SPACE_MARK_STR}{}",
        format_seconds(optimizer.total),
        format_seconds(optimizer.logical_opt),
        format_seconds(optimizer.physical_opt),
        format_seconds(optimizer.binding_match),
        format_seconds(optimizer.stats_sync_wait),
        format_seconds(optimizer.stats_derive),
    );
    buf.push('\n');

    write_slow_log_item(
        &mut buf,
        SLOW_LOG_WAIT_TS_TIME_STR,
        &format_seconds(session.duration_wait_ts),
    );

    let exec_detail_str = items.exec_detail.to_string();
    if !exec_detail_str.is_empty() {
        buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
        buf.push_str(&exec_detail_str);
        buf.push('\n');
    }
    let ia_stats =
        get_ia_remote_read_segment_stats(items.exec_detail.cop_exec_details.scan_detail.as_ref());
    if ia_stats.count > 0 {
        write_slow_log_item(
            &mut buf,
            IA_REMOTE_READ_SEGMENT_COUNT_STR,
            &ia_stats.count.to_string(),
        );
    }
    if ia_stats.bytes > 0 {
        write_slow_log_item(
            &mut buf,
            IA_REMOTE_READ_SEGMENT_SIZE_STR,
            &ia_stats.bytes.to_string(),
        );
    }
    if ia_stats.wait_time > Duration::ZERO {
        write_slow_log_item(
            &mut buf,
            IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR,
            &format_seconds(ia_stats.wait_time),
        );
    }

    if !session.current_db.is_empty() {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_DB_STR,
            &session.current_db.to_lowercase(),
        );
    }
    if !items.index_names.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_INDEX_NAMES_STR, &items.index_names);
    }

    write_slow_log_item(
        &mut buf,
        SLOW_LOG_IS_INTERNAL_STR,
        if session.in_restricted_sql {
            "true"
        } else {
            "false"
        },
    );
    if !items.digest.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_DIGEST_STR, &items.digest);
    }
    if !items.used_stats.is_empty() {
        buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
        buf.push_str(SLOW_LOG_STATS_INFO_STR);
        buf.push_str(SLOW_LOG_SPACE_MARK_STR);
        let mut first_comma = false;
        for used_stats_for_tbl in items.used_stats.values() {
            if first_comma {
                buf.push(',');
            }
            buf.push_str(&used_stats_for_tbl.write_to_slow_log());
            first_comma = true;
        }
        buf.push('\n');
    }
    if let Some(cop_tasks) = &items.cop_tasks {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_NUM_COP_TASKS_STR,
            &cop_tasks.num_cop_tasks.to_string(),
        );
        if cop_tasks.num_cop_tasks > 0 {
            let task_num = cop_tasks.num_cop_tasks;
            buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
            buf.push_str(&cop_tasks.process_time_stats.render(
                task_num,
                SLOW_LOG_SPACE_MARK_STR,
                SLOW_LOG_COP_PROC_AVG,
                SLOW_LOG_COP_PROC_P90,
                SLOW_LOG_COP_PROC_MAX,
                SLOW_LOG_COP_PROC_ADDR,
            ));
            buf.push('\n');
            buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
            buf.push_str(&cop_tasks.wait_time_stats.render(
                task_num,
                SLOW_LOG_SPACE_MARK_STR,
                SLOW_LOG_COP_WAIT_AVG,
                SLOW_LOG_COP_WAIT_P90,
                SLOW_LOG_COP_WAIT_MAX,
                SLOW_LOG_COP_WAIT_ADDR,
            ));
            buf.push('\n');

            // Go collects the backoff names and sorts them; the BTreeMap
            // walk here is that sorted order.
            let default_stats = TaskTimeStats::default();
            for (backoff, &total_times) in &cop_tasks.tot_backoff_times {
                let backoff_prefix = format!("{SLOW_LOG_COP_BACKOFF_PREFIX}{backoff}_");
                let backoff_time_stats = cop_tasks
                    .backoff_time_stats_map
                    .get(backoff)
                    .unwrap_or(&default_stats);
                if task_num == 1 {
                    let _ = writeln!(
                        buf,
                        "{SLOW_LOG_ROW_PREFIX_STR}{backoff_prefix}total_times\
                         {SLOW_LOG_SPACE_MARK_STR}{total_times} \
                         {backoff_prefix}total_time{SLOW_LOG_SPACE_MARK_STR}{}",
                        format_go_float64(backoff_time_stats.tot_time.as_secs_f64()),
                    );
                } else {
                    let _ = writeln!(
                        buf,
                        "{SLOW_LOG_ROW_PREFIX_STR}{backoff_prefix}total_times\
                         {SLOW_LOG_SPACE_MARK_STR}{total_times} \
                         {backoff_prefix}total_time{SLOW_LOG_SPACE_MARK_STR}{} \
                         {backoff_prefix}max_time{SLOW_LOG_SPACE_MARK_STR}{} \
                         {backoff_prefix}max_addr{SLOW_LOG_SPACE_MARK_STR}{} \
                         {backoff_prefix}avg_time{SLOW_LOG_SPACE_MARK_STR}{} \
                         {backoff_prefix}p90_time{SLOW_LOG_SPACE_MARK_STR}{}",
                        format_go_float64(backoff_time_stats.tot_time.as_secs_f64()),
                        format_go_float64(backoff_time_stats.max_time.as_secs_f64()),
                        backoff_time_stats.max_address,
                        format_go_float64(backoff_time_stats.avg_time.as_secs_f64()),
                        format_go_float64(backoff_time_stats.p90_time.as_secs_f64()),
                    );
                }
            }
        }
    }
    if items.mem_max > 0 {
        write_slow_log_item(&mut buf, SLOW_LOG_MEM_MAX, &items.mem_max.to_string());
    }
    if items.mem_arbitration > 0.0 {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_MEM_ARBITRATION,
            &format_float_f(items.mem_arbitration),
        );
    }
    if items.disk_max > 0 {
        write_slow_log_item(&mut buf, SLOW_LOG_DISK_MAX, &items.disk_max.to_string());
    }

    let format_bool = |value: bool| if value { "true" } else { "false" };
    write_slow_log_item(&mut buf, SLOW_LOG_PREPARED, format_bool(items.prepared));
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_PLAN_FROM_CACHE,
        format_bool(items.plan_from_cache),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_PLAN_FROM_BINDING,
        format_bool(items.plan_from_binding),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_HAS_MORE_RESULTS,
        format_bool(items.has_more_results),
    );
    kv_exec_detail_format(&mut buf, items.kv_exec_detail.as_ref());
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_WRITE_SQL_RESP_TOTAL,
        &format_seconds(items.write_sql_resp_total),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_RESULT_ROWS,
        &items.result_rows.to_string(),
    );
    if !items.warnings.is_empty() {
        buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
        buf.push_str(SLOW_LOG_WARNINGS);
        buf.push_str(SLOW_LOG_SPACE_MARK_STR);
        encode_warnings(&mut buf, &items.warnings);
    }
    write_slow_log_item(&mut buf, SLOW_LOG_SUCC, format_bool(items.succ));
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_IS_EXPLICIT_TXN,
        format_bool(items.is_explicit_txn),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_IS_SYNC_STATS_FAILED,
        format_bool(items.is_sync_stats_failed),
    );
    if session.stmt_wait_lock_lease_time > 0 {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_IS_WRITE_CACHE_TABLE,
            format_bool(items.is_write_cache_table),
        );
    }
    if !items.plan.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_PLAN, &items.plan);
    }
    if !items.plan_digest.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_PLAN_DIGEST, &items.plan_digest);
    }
    if !items.binary_plan.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_BINARY_PLAN, &items.binary_plan);
    }

    if !items.resource_group_name.is_empty() {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_RESOURCE_GROUP,
            &items.resource_group_name,
        );
    }
    let ru_details = items.ru_details.unwrap_or_default();
    if ru_details.rru > 0.0 {
        write_slow_log_item(&mut buf, SLOW_LOG_RRU, &format_float_f(ru_details.rru));
    }
    if ru_details.wru > 0.0 {
        write_slow_log_item(&mut buf, SLOW_LOG_WRU, &format_float_f(ru_details.wru));
    }
    if ru_details.ru_wait_duration > Duration::ZERO {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_WAIT_RU_DURATION,
            &format_seconds(ru_details.ru_wait_duration),
        );
    }
    if items.cpu_usages.tidb_cpu_time > Duration::ZERO {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_TIDB_CPU_USAGE_DURATION,
            &format_seconds(items.cpu_usages.tidb_cpu_time),
        );
    }
    if items.cpu_usages.tikv_cpu_time > Duration::ZERO {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_TIKV_CPU_USAGE_DURATION,
            &format_seconds(items.cpu_usages.tikv_cpu_time),
        );
    }
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_STORAGE_FROM_KV,
        format_bool(items.storage_kv),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_STORAGE_FROM_MPP,
        format_bool(items.storage_mpp),
    );
    let (tikv_ru, tiflash_ru) = match items.ru_details {
        Some(details) => (details.tikv_ru_v2, details.tiflash_ru),
        None => (0.0, 0.0),
    };
    let (total, formatted) = format_ruv2_summary(
        items.ruv2_metrics.as_ref(),
        &session.ru_v2_weights,
        tikv_ru,
        tiflash_ru,
    );
    if !total.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_REQUEST_UNIT_V2, &total);
    }
    if !formatted.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_REQUEST_UNIT_V2_DETAIL, &formatted);
    }
    if !items.session_connect_attrs.is_empty() {
        buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
        buf.push_str(SLOW_LOG_SESSION_CONNECT_ATTRS);
        buf.push_str(SLOW_LOG_SPACE_MARK_STR);
        encode_connect_attrs(&mut buf, &items.session_connect_attrs);
    }
    if !items.prev_stmt.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_PREV_STMT, &items.prev_stmt);
    }
    for field in &session.keyspace_observability_slow_log_fields {
        write_slow_log_item(&mut buf, &field.name, &field.value);
    }

    if session.current_db_changed {
        // Go also clears s.CurrentDBChanged here; the immutable snapshot
        // leaves that reset to the caller.
        let _ = writeln!(buf, "use {};", session.current_db.to_lowercase());
    }

    buf.push_str(&items.sql);
    if items.sql.as_bytes().last() != Some(&b';') {
        buf.push(';');
    }

    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec_details::{CopExecDetails, ScanDetail, TimeDetail};

    /// The SessionVars setup shared by Go `TestSlowLogFormat`, as the
    /// snapshot this tier passes.
    fn go_test_session() -> SlowLogSessionSnapshot {
        SlowLogSessionSnapshot {
            user: Some(SlowLogUserIdentity {
                username: "root".to_owned(),
                hostname: "192.168.0.1".to_owned(),
            }),
            connection_info_client_ip: Some("192.168.0.1".to_owned()),
            connection_id: 1,
            session_alias: "aliasabc".to_owned(),
            current_db: "TeST".to_owned(),
            in_restricted_sql: true,
            stmt_wait_lock_lease_time: 1,
            duration_parse: Duration::from_nanos(10),
            duration_compile: Duration::from_nanos(10),
            duration_optimizer: OptimizerDurations {
                total: Duration::from_nanos(10),
                logical_opt: Duration::from_nanos(10),
                physical_opt: Duration::from_nanos(10),
                binding_match: Duration::from_nanos(10),
                stats_sync_wait: Duration::from_nanos(10),
                stats_derive: Duration::from_nanos(10),
            },
            duration_wait_ts: Duration::from_nanos(3),
            ..SlowLogSessionSnapshot::default()
        }
    }

    /// The logItems fixture of Go `TestSlowLogFormat`, with the narrowed
    /// fields fed the values the live surfaces would produce.
    fn go_test_items() -> SlowQueryLogItems {
        let exec_detail = ExecDetails {
            request_count: 2,
            cop_exec_details: CopExecDetails {
                backoff_time: Duration::from_millis(1),
                scan_detail: Some(ScanDetail {
                    processed_keys: 20001,
                    total_keys: 10000,
                    ia_remote_read_segment_count: 4,
                    ia_remote_read_segment_bytes: 4096,
                    ia_remote_read_segment_duration: Duration::from_millis(15),
                    ..ScanDetail::default()
                }),
                time_detail: TimeDetail {
                    process_time: Duration::from_secs(2),
                    wait_time: Duration::from_secs(60),
                    ..TimeDetail::default()
                },
                ..CopExecDetails::default()
            },
            ..ExecDetails::default()
        };

        let used_stats_1 = UsedStatsInfoForTable {
            name: "t1".to_owned(),
            version: 123,
            realtime_count: 1000,
            modify_count: 0,
            column_stats_load_status: [
                (2, "allEvicted".to_owned()),
                (3, "onlyCmsEvicted".to_owned()),
            ]
            .into_iter()
            .collect(),
            index_stats_load_status: [(1, "allLoaded".to_owned()), (2, "allLoaded".to_owned())]
                .into_iter()
                .collect(),
        };
        let used_stats_2 = UsedStatsInfoForTable {
            name: "t2".to_owned(),
            version: 0,
            realtime_count: 10000,
            modify_count: 0,
            column_stats_load_status: [(2, "unInitialized".to_owned())].into_iter().collect(),
            ..UsedStatsInfoForTable::default()
        };

        let process_time_stats = TaskTimeStats {
            avg_time: Duration::from_secs(1),
            p90_time: Duration::from_secs(2),
            max_address: "10.6.131.78".to_owned(),
            max_time: Duration::from_secs(3),
            ..TaskTimeStats::default()
        };
        let wait_time_stats = TaskTimeStats {
            avg_time: Duration::from_millis(10),
            p90_time: Duration::from_millis(20),
            max_time: Duration::from_millis(30),
            max_address: "10.6.131.79".to_owned(),
            ..TaskTimeStats::default()
        };
        let mut cop_tasks = CopTasksDetails {
            num_cop_tasks: 10,
            process_time_stats,
            wait_time_stats,
            ..CopTasksDetails::default()
        };
        for backoff in ["rpcTiKV", "rpcPD", "regionMiss"] {
            cop_tasks.backoff_time_stats_map.insert(
                backoff.to_owned(),
                TaskTimeStats {
                    max_time: Duration::from_millis(200),
                    max_address: "127.0.0.1".to_owned(),
                    avg_time: Duration::from_millis(200),
                    p90_time: Duration::from_millis(200),
                    tot_time: Duration::from_millis(200),
                },
            );
            cop_tasks.tot_backoff_times.insert(backoff.to_owned(), 200);
        }

        SlowQueryLogItems {
            txn_ts: 406_649_736_972_468_225,
            keyspace_name: "keyspace_a".to_owned(),
            keyspace_id: 1,
            sql: "select * from t;".to_owned(),
            digest: "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7".to_owned(),
            time_total: Duration::from_secs(1),
            index_names: "[t1:a,t2:b]".to_owned(),
            cop_tasks: Some(cop_tasks),
            exec_detail,
            mem_max: 2333,
            disk_max: 6666,
            prepared: true,
            plan_from_cache: true,
            plan_from_binding: true,
            has_more_results: true,
            kv_exec_detail: Some(TikvExecDetailsSnapshot {
                wait_kv_resp_duration: Duration::from_secs(10).as_nanos() as i64,
                wait_pd_resp_duration: Duration::from_secs(11).as_nanos() as i64,
                backoff_duration: Duration::from_secs(12).as_nanos() as i64,
                ..TikvExecDetailsSnapshot::default()
            }),
            write_sql_resp_total: Duration::from_secs(1),
            result_rows: 12345,
            succ: true,
            rewrite_info: RewritePhaseInfo {
                duration_rewrite: Duration::from_nanos(3),
                duration_preprocess_sub_query: Duration::from_nanos(2),
                preprocess_sub_queries: 2,
            },
            exec_retry_count: 3,
            exec_retry_time: Duration::from_secs(5) + Duration::from_millis(100),
            is_explicit_txn: true,
            is_write_cache_table: true,
            used_stats: [(1, used_stats_1), (2, used_stats_2)].into_iter().collect(),
            resource_group_name: "rg1".to_owned(),
            // Go util.NewRUDetailsWith(50.0, 100.56, 134*time.Millisecond).
            ru_details: Some(RuDetailsSnapshot {
                rru: 50.0,
                wru: 100.56,
                ru_wait_duration: Duration::from_millis(134),
                tikv_ru_v2: 0.0,
                tiflash_ru: 0.0,
            }),
            storage_kv: true,
            storage_mpp: false,
            mem_arbitration: Duration::from_nanos(54321).as_secs_f64(),
            ..SlowQueryLogItems::default()
        }
    }

    /// The `resultFields` literal of Go `TestSlowLogFormat`, byte-exact.
    const RESULT_FIELDS: &str = "\
# Txn_start_ts: 406649736972468225
# Keyspace_name: keyspace_a
# Keyspace_ID: 1
# User@Host: root[root] @ 192.168.0.1 [192.168.0.1]
# Conn_ID: 1
# Session_alias: aliasabc
# Exec_retry_time: 5.1 Exec_retry_count: 3
# Query_time: 1
# Parse_time: 0.00000001
# Compile_time: 0.00000001
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Optimize_time: 0.00000001 Opt_logical: 0.00000001 Opt_physical: 0.00000001 \
Opt_binding_match: 0.00000001 Opt_stats_sync_wait: 0.00000001 Opt_stats_derive: 0.00000001
# Wait_TS: 0.000000003
# Process_time: 2 Wait_time: 60 Backoff_time: 0.001 Request_count: 2 Process_keys: 20001 \
Total_keys: 10000
# IA_remote_read_segment_count: 4
# IA_remote_read_segment_size: 4096
# IA_remote_read_segment_wait_time: 0.015
# DB: test
# Index_names: [t1:a,t2:b]
# Is_internal: true
# Digest: e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7
# Stats: t1:stats_meta_version=123[realtime_count=1000;modify_count=0]\
[ID 1:allLoaded,ID 2:allLoaded][ID 2:allEvicted,ID 3:onlyCmsEvicted],\
t2:stats_meta_version=pseudo[realtime_count=10000;modify_count=0]
# Num_cop_tasks: 10
# Cop_proc_avg: 1 Cop_proc_p90: 2 Cop_proc_max: 3 Cop_proc_addr: 10.6.131.78
# Cop_wait_avg: 0.01 Cop_wait_p90: 0.02 Cop_wait_max: 0.03 Cop_wait_addr: 10.6.131.79
# Cop_backoff_regionMiss_total_times: 200 Cop_backoff_regionMiss_total_time: 0.2 \
Cop_backoff_regionMiss_max_time: 0.2 Cop_backoff_regionMiss_max_addr: 127.0.0.1 \
Cop_backoff_regionMiss_avg_time: 0.2 Cop_backoff_regionMiss_p90_time: 0.2
# Cop_backoff_rpcPD_total_times: 200 Cop_backoff_rpcPD_total_time: 0.2 \
Cop_backoff_rpcPD_max_time: 0.2 Cop_backoff_rpcPD_max_addr: 127.0.0.1 \
Cop_backoff_rpcPD_avg_time: 0.2 Cop_backoff_rpcPD_p90_time: 0.2
# Cop_backoff_rpcTiKV_total_times: 200 Cop_backoff_rpcTiKV_total_time: 0.2 \
Cop_backoff_rpcTiKV_max_time: 0.2 Cop_backoff_rpcTiKV_max_addr: 127.0.0.1 \
Cop_backoff_rpcTiKV_avg_time: 0.2 Cop_backoff_rpcTiKV_p90_time: 0.2
# Mem_max: 2333
# Mem_arbitration: 0.000054321
# Disk_max: 6666
# Prepared: true
# Plan_from_cache: true
# Plan_from_binding: true
# Has_more_results: true
# KV_total: 10
# PD_total: 11
# Backoff_total: 12
# Unpacked_bytes_sent_tikv_total: 0
# Unpacked_bytes_received_tikv_total: 0
# Unpacked_bytes_sent_tikv_cross_zone: 0
# Unpacked_bytes_received_tikv_cross_zone: 0
# Unpacked_bytes_sent_tiflash_total: 0
# Unpacked_bytes_received_tiflash_total: 0
# Unpacked_bytes_sent_tiflash_cross_zone: 0
# Unpacked_bytes_received_tiflash_cross_zone: 0
# Write_sql_response_total: 1
# Result_rows: 12345
# Succ: true
# IsExplicitTxn: true
# IsSyncStatsFailed: false
# IsWriteCacheTable: true
# Resource_group: rg1
# Request_unit_read: 50
# Request_unit_write: 100.56
# Time_queued_by_rc: 0.134
# Storage_from_kv: true
# Storage_from_mpp: false";

    /// Port of Go `TestSlowLogFormat` (`tests/session_test.go`): the
    /// byte-exact expected literal, the `use <db>;` line, the connection
    /// attributes serialization and placement, and the resolved
    /// keyspace-observability fields. The trailing
    /// `PrepareSlowLogItemsForRules`/`CompleteSlowLogItemsForRules` half of
    /// the Go test exercises executor-side collection and stays with that
    /// surface.
    #[test]
    fn slow_log_format_matches_go_test_slow_log_format() {
        let mut session = go_test_session();
        let mut items = go_test_items();
        let sql = items.sql.clone();

        let log_string = slow_log_format(&session, &items);
        assert_eq!(format!("{RESULT_FIELDS}\n{sql}"), log_string);
        assert!(!log_string.contains(SLOW_LOG_SESSION_CONNECT_ATTRS));

        // Go sets seVar.CurrentDBChanged = true and observes both the
        // `use test;` line and the reset; the reset stays with the caller
        // of this immutable-snapshot tier.
        session.current_db_changed = true;
        let log_string = slow_log_format(&session, &items);
        assert_eq!(format!("{RESULT_FIELDS}\nuse test;\n{sql}"), log_string);
        session.current_db_changed = false;

        // Verify SessionConnectAttrs serialization.
        items.session_connect_attrs = [
            ("_client_name", "libmysql"),
            ("_os", "Linux"),
            ("app_name", "test_svc"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect();
        let expected_attrs_line =
            "# Session_connect_attrs: {\"_client_name\":\"libmysql\",\"_os\":\"Linux\",\
             \"app_name\":\"test_svc\"}";
        let log_string = slow_log_format(&session, &items);
        assert!(log_string.contains(expected_attrs_line));
        // Session_connect_attrs appears after Storage_from_mpp, before
        // Prev_stmt, and before the SQL.
        let attrs_idx = log_string
            .find("Session_connect_attrs")
            .expect("attrs line present");
        let mpp_idx = log_string
            .find(SLOW_LOG_STORAGE_FROM_MPP)
            .expect("mpp line present");
        let sql_idx = log_string.find(&sql).expect("sql present");
        assert!(attrs_idx > mpp_idx);
        assert!(attrs_idx < sql_idx);
        if let Some(prev_stmt_idx) = log_string.find(SLOW_LOG_PREV_STMT) {
            assert!(attrs_idx < prev_stmt_idx);
        }

        // The reserved truncation metadata key serializes as expected.
        items.session_connect_attrs = [("_truncated", "4"), ("app_name", "test_svc")]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        let log_string = slow_log_format(&session, &items);
        assert!(log_string
            .contains("# Session_connect_attrs: {\"_truncated\":\"4\",\"app_name\":\"test_svc\"}"));
        items.session_connect_attrs.clear();

        // Go resolves {Source: "meta_a", SlowLogField: "Keyspace_meta_slow_a"}
        // against {"meta_a": "value_a"} through the global config; the
        // snapshot carries the resolved pair.
        session.keyspace_observability_slow_log_fields = vec![KeyspaceObservabilityField {
            name: "Keyspace_meta_slow_a".to_owned(),
            value: "value_a".to_owned(),
        }];
        let log_string = slow_log_format(&session, &items);
        assert_eq!(
            format!("{RESULT_FIELDS}\n# Keyspace_meta_slow_a: value_a\n{sql}"),
            log_string
        );
    }

    /// Port of Go `TestSlowLogFormatIncludesTiFlashRUInRUV2Metrics`'s
    /// formatter half: TiKV and TiFlash RU flow into the RU v2 total and
    /// detail lines even with empty metrics. The Go subtest asserting that
    /// `NewSessionVars(nil).RUV2Weights()` equals the config defaults reads
    /// the unported config surface and stays with the sysvar owner.
    #[test]
    fn slow_log_format_includes_tiflash_ru_in_ruv2_metrics() {
        let session = SlowLogSessionSnapshot::default();
        let items = SlowQueryLogItems {
            sql: "select 1".to_owned(),
            digest: "digest".to_owned(),
            time_total: Duration::from_secs(1),
            succ: true,
            // Go: RUDetails.AddTiKVRUV2(100) and
            // UpdateTiFlash(&rmpb.Consumption{RRU: 20, WRU: 30}).
            ru_details: Some(RuDetailsSnapshot {
                tikv_ru_v2: 100.0,
                tiflash_ru: 50.0,
                ..RuDetailsSnapshot::default()
            }),
            // Go: execdetails.NewRUV2Metrics() — non-nil, all zero.
            ruv2_metrics: Some(RuV2MetricsSnapshot::default()),
            ..SlowQueryLogItems::default()
        };

        let log_string = slow_log_format(&session, &items);
        assert!(log_string.contains("# Request_unit_v2: 150.00"));
        assert!(log_string.contains(
            "# Request_unit_v2_detail: total_ru:150.00, tidb_ru:0.00, tikv_ru:100.00, \
             tiflash_ru:50.00"
        ));
    }

    /// The remaining `format_ruv2_summary` arms Go's fixture leaves cold:
    /// nil metrics with zero RU renders nothing, bypass renders nothing,
    /// and non-zero counters render weighted tidb_ru plus the int and
    /// label-map arms in Go's arm order.
    #[test]
    fn format_ruv2_summary_arm_coverage() {
        let weights = RuV2Weights {
            ru_scale: 1.0,
            plan_cnt: 2.0,
            ..RuV2Weights::default()
        };
        assert_eq!(
            (String::new(), String::new()),
            format_ruv2_summary(None, &weights, 0.0, 0.0)
        );

        let bypassed = RuV2MetricsSnapshot {
            bypass: true,
            plan_cnt: 5,
            ..RuV2MetricsSnapshot::default()
        };
        assert_eq!(
            (String::new(), String::new()),
            format_ruv2_summary(Some(&bypassed), &weights, 3.0, 0.0)
        );

        let metrics = RuV2MetricsSnapshot {
            plan_cnt: 5,
            executor_l1: [("TableReader".to_owned(), 3), ("Zero".to_owned(), 0)]
                .into_iter()
                .collect(),
            ..RuV2MetricsSnapshot::default()
        };
        let (total, detail) = format_ruv2_summary(Some(&metrics), &weights, 1.0, 0.5);
        assert_eq!("11.50", total);
        assert_eq!(
            "total_ru:11.50, tidb_ru:10.00, tikv_ru:1.00, tiflash_ru:0.50, \
             executor_l1:{TableReader:3}, plan_cnt:5",
            detail
        );
    }

    /// Go `TaskTimeStats.String`'s single-cop-task short form, which the
    /// ten-task fixture leaves cold, plus the taskNum==1 backoff line pair.
    #[test]
    fn single_cop_task_renders_short_forms() {
        let stats = TaskTimeStats {
            avg_time: Duration::from_millis(500),
            max_address: "10.6.131.78".to_owned(),
            ..TaskTimeStats::default()
        };
        assert_eq!(
            "Cop_proc_avg: 0.5 Cop_proc_addr: 10.6.131.78",
            stats.render(
                1,
                SLOW_LOG_SPACE_MARK_STR,
                SLOW_LOG_COP_PROC_AVG,
                SLOW_LOG_COP_PROC_P90,
                SLOW_LOG_COP_PROC_MAX,
                SLOW_LOG_COP_PROC_ADDR,
            )
        );

        let mut items = SlowQueryLogItems::default();
        let mut cop_tasks = CopTasksDetails {
            num_cop_tasks: 1,
            process_time_stats: stats.clone(),
            wait_time_stats: stats,
            ..CopTasksDetails::default()
        };
        cop_tasks
            .tot_backoff_times
            .insert("regionMiss".to_owned(), 3);
        cop_tasks.backoff_time_stats_map.insert(
            "regionMiss".to_owned(),
            TaskTimeStats {
                tot_time: Duration::from_millis(200),
                ..TaskTimeStats::default()
            },
        );
        items.cop_tasks = Some(cop_tasks);
        let log_string = slow_log_format(&SlowLogSessionSnapshot::default(), &items);
        assert!(log_string.contains(
            "# Cop_backoff_regionMiss_total_times: 3 Cop_backoff_regionMiss_total_time: 0.2\n"
        ));
    }

    /// `kvExecDetailFormat`'s nil branch (all `"0"`) and the Warnings JSON
    /// arm with `IsExtra` omitempty, both cold in the Go fixture.
    #[test]
    fn nil_kv_detail_and_warnings_render_as_go_does() {
        let items = SlowQueryLogItems {
            warnings: vec![
                JsonSqlWarnForSlowLog {
                    level: "Warning".to_owned(),
                    message: "quoted \"msg\"".to_owned(),
                    is_extra: false,
                },
                JsonSqlWarnForSlowLog {
                    level: "Note".to_owned(),
                    message: "extra".to_owned(),
                    is_extra: true,
                },
            ],
            ..SlowQueryLogItems::default()
        };
        let log_string = slow_log_format(&SlowLogSessionSnapshot::default(), &items);
        assert!(log_string.contains("# KV_total: 0\n# PD_total: 0\n# Backoff_total: 0\n"));
        assert!(log_string.contains(
            "# Warnings: [{\"Level\":\"Warning\",\"Message\":\"quoted \\\"msg\\\"\"},\
             {\"Level\":\"Note\",\"Message\":\"extra\",\"IsExtra\":true}]\n"
        ));
        // Empty SQL still gets the suffix.
        assert!(log_string.ends_with("# Storage_from_kv: false\n# Storage_from_mpp: false\n;"));
    }

    /// The composite constants spell exactly as Go concatenates them.
    #[test]
    fn composite_constants_match_their_go_concatenations() {
        assert_eq!(
            format!("{SLOW_LOG_ROW_PREFIX_STR}{SLOW_LOG_TIME_STR}{SLOW_LOG_SPACE_MARK_STR}"),
            SLOW_LOG_START_PREFIX_STR
        );
        assert_eq!(
            format!("{SLOW_LOG_PREV_STMT}{SLOW_LOG_SPACE_MARK_STR}"),
            SLOW_LOG_PREV_STMT_PREFIX
        );
    }
}
