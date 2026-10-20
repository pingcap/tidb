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

//! Go `pkg/sessionctx/variable/slow_log.go`'s entry half: the slow-log field
//! names, `SlowQueryLogItems` and `SessionVars.SlowLogFormat`, which renders
//! one statement's entry (everything after the `# Time:` line the slow-query
//! logger's encoder writes) byte for byte. The rule half (`tidb_slow_log_rules`
//! parsing and matching) is [`crate::slow_log_rules`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tidb_exec::exec_details::{
    format_seconds, get_ia_remote_read_segment_stats, CopTasksDetails, ExecDetails,
    IA_REMOTE_READ_SEGMENT_COUNT_STR, IA_REMOTE_READ_SEGMENT_SIZE_STR,
    IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR,
};
use tidb_executor::used_stats::UsedStatsInfoForTable;

/// Go `SlowLogRowPrefixStr`.
pub const SLOW_LOG_ROW_PREFIX_STR: &str = "# ";
/// Go `SlowLogSpaceMarkStr`.
pub const SLOW_LOG_SPACE_MARK_STR: &str = ": ";
/// Go `SlowLogSQLSuffixStr`.
pub const SLOW_LOG_SQL_SUFFIX_STR: &str = ";";
/// Go `SlowLogTimeStr`.
pub const SLOW_LOG_TIME_STR: &str = "Time";
/// Go `SlowLogStartPrefixStr`.
pub const SLOW_LOG_START_PREFIX_STR: &str = "# Time: ";
/// Go `SlowLogTxnStartTSStr`.
pub const SLOW_LOG_TXN_START_TS_STR: &str = "Txn_start_ts";
/// Go `SlowLogKeyspaceName`.
pub const SLOW_LOG_KEYSPACE_NAME: &str = "Keyspace_name";
/// Go `SlowLogKeyspaceID`.
pub const SLOW_LOG_KEYSPACE_ID: &str = "Keyspace_ID";
/// Go `SlowLogUserAndHostStr`.
pub const SLOW_LOG_USER_AND_HOST_STR: &str = "User@Host";
/// Go `SlowLogUserStr`.
pub const SLOW_LOG_USER_STR: &str = "User";
/// Go `SlowLogHostStr`.
pub const SLOW_LOG_HOST_STR: &str = "Host";
/// Go `SlowLogPreprocSubQueriesStr`.
pub const SLOW_LOG_PREPROC_SUB_QUERIES_STR: &str = "Preproc_subqueries";
/// Go `SlowLogPreProcSubQueryTimeStr`.
pub const SLOW_LOG_PRE_PROC_SUB_QUERY_TIME_STR: &str = "Preproc_subqueries_time";
/// Go `SlowLogIndexNamesStr`.
pub const SLOW_LOG_INDEX_NAMES_STR: &str = "Index_names";
/// Go `SlowLogQuerySQLStr`: the SLOW_QUERY column the SQL line fills.
pub const SLOW_LOG_QUERY_SQL_STR: &str = "Query";
/// Go `SlowLogStatsInfoStr`.
pub const SLOW_LOG_STATS_INFO_STR: &str = "Stats";
/// Go `SlowLogCopProcAvg`.
pub const SLOW_LOG_COP_PROC_AVG: &str = "Cop_proc_avg";
/// Go `SlowLogCopProcP90`.
pub const SLOW_LOG_COP_PROC_P90: &str = "Cop_proc_p90";
/// Go `SlowLogCopProcMax`.
pub const SLOW_LOG_COP_PROC_MAX: &str = "Cop_proc_max";
/// Go `SlowLogCopProcAddr`.
pub const SLOW_LOG_COP_PROC_ADDR: &str = "Cop_proc_addr";
/// Go `SlowLogCopWaitAvg`.
pub const SLOW_LOG_COP_WAIT_AVG: &str = "Cop_wait_avg";
/// Go `SlowLogCopWaitP90`.
pub const SLOW_LOG_COP_WAIT_P90: &str = "Cop_wait_p90";
/// Go `SlowLogCopWaitMax`.
pub const SLOW_LOG_COP_WAIT_MAX: &str = "Cop_wait_max";
/// Go `SlowLogCopWaitAddr`.
pub const SLOW_LOG_COP_WAIT_ADDR: &str = "Cop_wait_addr";
/// Go `SlowLogCopBackoffPrefix`.
pub const SLOW_LOG_COP_BACKOFF_PREFIX: &str = "Cop_backoff_";
/// Go `SlowLogPrepared`.
pub const SLOW_LOG_PREPARED: &str = "Prepared";
/// Go `SlowLogPlanFromCache`.
pub const SLOW_LOG_PLAN_FROM_CACHE: &str = "Plan_from_cache";
/// Go `SlowLogPlanFromBinding`.
pub const SLOW_LOG_PLAN_FROM_BINDING: &str = "Plan_from_binding";
/// Go `SlowLogHasMoreResults`.
pub const SLOW_LOG_HAS_MORE_RESULTS: &str = "Has_more_results";
/// Go `SlowLogPrevStmt`.
pub const SLOW_LOG_PREV_STMT: &str = "Prev_stmt";
/// Go `SlowLogPlan`.
pub const SLOW_LOG_PLAN: &str = "Plan";
/// Go `SlowLogBinaryPlan`.
pub const SLOW_LOG_BINARY_PLAN: &str = "Binary_plan";
/// Go `SlowLogPlanPrefix` (`ast.TiDBDecodePlan + "('"`).
pub const SLOW_LOG_PLAN_PREFIX: &str = "tidb_decode_plan('";
/// Go `SlowLogBinaryPlanPrefix` (`ast.TiDBDecodeBinaryPlan + "('"`).
pub const SLOW_LOG_BINARY_PLAN_PREFIX: &str = "tidb_decode_binary_plan('";
/// Go `SlowLogPlanSuffix`.
pub const SLOW_LOG_PLAN_SUFFIX: &str = "')";
/// Go `SlowLogPrevStmtPrefix`.
pub const SLOW_LOG_PREV_STMT_PREFIX: &str = "Prev_stmt: ";
/// Go `SlowLogBackoffTotal`.
pub const SLOW_LOG_BACKOFF_TOTAL: &str = "Backoff_total";
/// Go `SlowLogExecRetryTime`.
pub const SLOW_LOG_EXEC_RETRY_TIME: &str = "Exec_retry_time";
/// Go `SlowLogBackoffDetail`.
pub const SLOW_LOG_BACKOFF_DETAIL: &str = "Backoff_Detail";
/// Go `SlowLogResultRows`.
pub const SLOW_LOG_RESULT_ROWS: &str = "Result_rows";
/// Go `SlowLogWarnings`.
pub const SLOW_LOG_WARNINGS: &str = "Warnings";
/// Go `SlowLogIsExplicitTxn`.
pub const SLOW_LOG_IS_EXPLICIT_TXN: &str = "IsExplicitTxn";
/// Go `SlowLogIsWriteCacheTable`.
pub const SLOW_LOG_IS_WRITE_CACHE_TABLE: &str = "IsWriteCacheTable";
/// Go `SlowLogIsSyncStatsFailed`.
pub const SLOW_LOG_IS_SYNC_STATS_FAILED: &str = "IsSyncStatsFailed";
/// Go `SlowLogRRU`.
pub const SLOW_LOG_RRU: &str = "Request_unit_read";
/// Go `SlowLogWRU`.
pub const SLOW_LOG_WRU: &str = "Request_unit_write";
/// Go `SlowLogWaitRUDuration`.
pub const SLOW_LOG_WAIT_RU_DURATION: &str = "Time_queued_by_rc";
/// Go `SlowLogTidbCPUUsageDuration`.
pub const SLOW_LOG_TIDB_CPU_USAGE_DURATION: &str = "Tidb_cpu_time";
/// Go `SlowLogTikvCPUUsageDuration`.
pub const SLOW_LOG_TIKV_CPU_USAGE_DURATION: &str = "Tikv_cpu_time";
/// Go `SlowLogStorageFromKV`.
pub const SLOW_LOG_STORAGE_FROM_KV: &str = "Storage_from_kv";
/// Go `SlowLogStorageFromMPP`.
pub const SLOW_LOG_STORAGE_FROM_MPP: &str = "Storage_from_mpp";
/// Go `SlowLogRequestUnitV2`.
pub const SLOW_LOG_REQUEST_UNIT_V2: &str = "Request_unit_v2";
/// Go `SlowLogRequestUnitV2Detail`.
pub const SLOW_LOG_REQUEST_UNIT_V2_DETAIL: &str = "Request_unit_v2_detail";
/// Go `SlowLogConnIDStr`.
pub const SLOW_LOG_CONN_ID_STR: &str = "Conn_ID";
/// Go `SlowLogSessAliasStr`.
pub const SLOW_LOG_SESS_ALIAS_STR: &str = "Session_alias";
/// Go `SlowLogQueryTimeStr`.
pub const SLOW_LOG_QUERY_TIME_STR: &str = "Query_time";
/// Go `SlowLogParseTimeStr`.
pub const SLOW_LOG_PARSE_TIME_STR: &str = "Parse_time";
/// Go `SlowLogCompileTimeStr`.
pub const SLOW_LOG_COMPILE_TIME_STR: &str = "Compile_time";
/// Go `SlowLogRewriteTimeStr`.
pub const SLOW_LOG_REWRITE_TIME_STR: &str = "Rewrite_time";
/// Go `SlowLogOptimizeTimeStr`.
pub const SLOW_LOG_OPTIMIZE_TIME_STR: &str = "Optimize_time";
/// Go `SlowLogOptimizeLogicalOpt`.
pub const SLOW_LOG_OPTIMIZE_LOGICAL_OPT: &str = "Opt_logical";
/// Go `SlowLogOptimizePhysicalOpt`.
pub const SLOW_LOG_OPTIMIZE_PHYSICAL_OPT: &str = "Opt_physical";
/// Go `SlowLogOptimizeBindingMatch`.
pub const SLOW_LOG_OPTIMIZE_BINDING_MATCH: &str = "Opt_binding_match";
/// Go `SlowLogOptimizeStatsSyncWait`.
pub const SLOW_LOG_OPTIMIZE_STATS_SYNC_WAIT: &str = "Opt_stats_sync_wait";
/// Go `SlowLogOptimizeStatsDerive`.
pub const SLOW_LOG_OPTIMIZE_STATS_DERIVE: &str = "Opt_stats_derive";
/// Go `SlowLogWaitTSTimeStr`.
pub const SLOW_LOG_WAIT_TS_TIME_STR: &str = "Wait_TS";
/// Go `SlowLogDBStr`.
pub const SLOW_LOG_DB_STR: &str = "DB";
/// Go `SlowLogIsInternalStr`.
pub const SLOW_LOG_IS_INTERNAL_STR: &str = "Is_internal";
/// Go `SlowLogDigestStr`.
pub const SLOW_LOG_DIGEST_STR: &str = "Digest";
/// Go `SlowLogNumCopTasksStr`.
pub const SLOW_LOG_NUM_COP_TASKS_STR: &str = "Num_cop_tasks";
/// Go `SlowLogMemMax`.
pub const SLOW_LOG_MEM_MAX: &str = "Mem_max";
/// Go `SlowLogMemArbitration`.
pub const SLOW_LOG_MEM_ARBITRATION: &str = "Mem_arbitration";
/// Go `SlowLogDiskMax`.
pub const SLOW_LOG_DISK_MAX: &str = "Disk_max";
/// Go `SlowLogKVTotal`.
pub const SLOW_LOG_KV_TOTAL: &str = "KV_total";
/// Go `SlowLogPDTotal`.
pub const SLOW_LOG_PD_TOTAL: &str = "PD_total";
/// Go `SlowLogUnpackedBytesSentTiKVTotal`.
pub const SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_TOTAL: &str = "Unpacked_bytes_sent_tikv_total";
/// Go `SlowLogUnpackedBytesReceivedTiKVTotal`.
pub const SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL: &str = "Unpacked_bytes_received_tikv_total";
/// Go `SlowLogUnpackedBytesSentTiKVCrossZone`.
pub const SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE: &str =
    "Unpacked_bytes_sent_tikv_cross_zone";
/// Go `SlowLogUnpackedBytesReceivedTiKVCrossZone`.
pub const SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE: &str =
    "Unpacked_bytes_received_tikv_cross_zone";
/// Go `SlowLogUnpackedBytesSentTiFlashTotal`.
pub const SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_TOTAL: &str = "Unpacked_bytes_sent_tiflash_total";
/// Go `SlowLogUnpackedBytesReceivedTiFlashTotal`.
pub const SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL: &str =
    "Unpacked_bytes_received_tiflash_total";
/// Go `SlowLogUnpackedBytesSentTiFlashCrossZone`.
pub const SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE: &str =
    "Unpacked_bytes_sent_tiflash_cross_zone";
/// Go `SlowLogUnpackedBytesReceivedTiFlashCrossZone`.
pub const SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE: &str =
    "Unpacked_bytes_received_tiflash_cross_zone";
/// Go `SlowLogWriteSQLRespTotal`.
pub const SLOW_LOG_WRITE_SQL_RESP_TOTAL: &str = "Write_sql_response_total";
/// Go `SlowLogSucc`.
pub const SLOW_LOG_SUCC: &str = "Succ";
/// Go `SlowLogPlanDigest`.
pub const SLOW_LOG_PLAN_DIGEST: &str = "Plan_digest";
/// Go `SlowLogExecRetryCount`.
pub const SLOW_LOG_EXEC_RETRY_COUNT: &str = "Exec_retry_count";
/// Go `SlowLogResourceGroup`.
pub const SLOW_LOG_RESOURCE_GROUP: &str = "Resource_group";
/// Go `SlowLogCopMVCCReadAmplification`.
pub const SLOW_LOG_COP_MVCC_READ_AMPLIFICATION: &str = "cop_mvcc_read_amplification";
/// Go `SlowLogSessionConnectAttrs`.
pub const SLOW_LOG_SESSION_CONNECT_ATTRS: &str = "Session_connect_attrs";

/// Go `JSONSQLWarnForSlowLog`: one statement warning in the `Warnings` line.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JsonSqlWarnForSlowLog {
    /// Go `Level`.
    pub level: String,
    /// Go `Message`.
    pub message: String,
    /// Go `IsExtra` (`json:",omitempty"`).
    pub is_extra: bool,
}

/// Go `RewritePhaseInfo` and `DurationOptimizer`, which the planner sets.
pub use tidb_executor::phase_durations::{DurationOptimizer, RewritePhaseInfo};

/// Go `execdetails.LoadTiKVExecDetails(util.ExecDetails)`'s snapshot: the
/// statement's client-go waits and traffic the `KV_total` .. `Unpacked_*`
/// lines print.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KvExecDetails {
    /// Go `WaitKVRespDuration`.
    pub wait_kv_resp_duration: Duration,
    /// Go `WaitPDRespDuration`.
    pub wait_pd_resp_duration: Duration,
    /// Go `BackoffDuration`.
    pub backoff_duration: Duration,
    /// Go `UnpackedBytesSentKVTotal`.
    pub unpacked_bytes_sent_kv_total: i64,
    /// Go `UnpackedBytesReceivedKVTotal`.
    pub unpacked_bytes_received_kv_total: i64,
    /// Go `UnpackedBytesSentKVCrossZone`.
    pub unpacked_bytes_sent_kv_cross_zone: i64,
    /// Go `UnpackedBytesReceivedKVCrossZone`.
    pub unpacked_bytes_received_kv_cross_zone: i64,
    /// Go `UnpackedBytesSentMPPTotal`.
    pub unpacked_bytes_sent_mpp_total: i64,
    /// Go `UnpackedBytesReceivedMPPTotal`.
    pub unpacked_bytes_received_mpp_total: i64,
    /// Go `UnpackedBytesSentMPPCrossZone`.
    pub unpacked_bytes_sent_mpp_cross_zone: i64,
    /// Go `UnpackedBytesReceivedMPPCrossZone`.
    pub unpacked_bytes_received_mpp_cross_zone: i64,
}

/// Go `execdetails.LoadTiKVExecDetails(ctx.Value(util.ExecDetailsKey))`.
/// client-rust charges RPC traffic to its own collector rather than to the
/// `ExecDetails` value, so the snapshot adds `network_traffic` to the value's
/// own traffic counters.
#[must_use]
pub fn load_tikv_exec_details(
    exec_details: &tikv_client::util::ExecDetails,
    network_traffic: &tikv_client::traffic::NetworkTrafficDetails,
) -> tikv_client::util::ExecDetailsSnapshot {
    let mut snapshot = exec_details.snapshot();
    let network = network_traffic.snapshot();
    let traffic = &mut snapshot.traffic;
    traffic.sent_kv_total += network.sent_kv_total;
    traffic.received_kv_total += network.received_kv_total;
    traffic.sent_kv_cross_zone += network.sent_kv_cross_zone;
    traffic.received_kv_cross_zone += network.received_kv_cross_zone;
    traffic.sent_mpp_total += network.sent_mpp_total;
    traffic.received_mpp_total += network.received_mpp_total;
    traffic.sent_mpp_cross_zone += network.sent_mpp_cross_zone;
    traffic.received_mpp_cross_zone += network.received_mpp_cross_zone;
    snapshot
}

impl From<&tikv_client::util::ExecDetailsSnapshot> for KvExecDetails {
    fn from(snapshot: &tikv_client::util::ExecDetailsSnapshot) -> Self {
        let nanos = |value: i64| Duration::from_nanos(u64::try_from(value).unwrap_or(0));
        let traffic = &snapshot.traffic;
        Self {
            wait_kv_resp_duration: nanos(snapshot.wait_kv_response_duration_ns),
            wait_pd_resp_duration: nanos(snapshot.wait_pd_response_duration_ns),
            backoff_duration: nanos(snapshot.backoff_duration_ns),
            unpacked_bytes_sent_kv_total: traffic.sent_kv_total,
            unpacked_bytes_received_kv_total: traffic.received_kv_total,
            unpacked_bytes_sent_kv_cross_zone: traffic.sent_kv_cross_zone,
            unpacked_bytes_received_kv_cross_zone: traffic.received_kv_cross_zone,
            unpacked_bytes_sent_mpp_total: traffic.sent_mpp_total,
            unpacked_bytes_received_mpp_total: traffic.received_mpp_total,
            unpacked_bytes_sent_mpp_cross_zone: traffic.sent_mpp_cross_zone,
            unpacked_bytes_received_mpp_cross_zone: traffic.received_mpp_cross_zone,
        }
    }
}

/// Go `ppcpuusage.CPUUsages`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CpuUsages {
    /// Go `TidbCPUTime`.
    pub tidb_cpu_time: Duration,
    /// Go `TikvCPUTime`.
    pub tikv_cpu_time: Duration,
}

/// Go `SlowQueryLogItems`: what one statement contributes to its slow-log
/// entry, beyond the session facts [`SlowLogSessionVars`] carries.
#[derive(Clone, Debug, Default)]
pub struct SlowQueryLogItems {
    /// Go `TxnTS`.
    pub txn_ts: u64,
    /// Go `KeyspaceName`.
    pub keyspace_name: String,
    /// Go `KeyspaceID`.
    pub keyspace_id: u32,
    /// Go `SQL`.
    pub sql: String,
    /// Go `Digest`.
    pub digest: String,
    /// Go `TimeTotal`.
    pub time_total: Duration,
    /// Go `IndexNames`.
    pub index_names: String,
    /// Go `Succ`.
    pub succ: bool,
    /// Go `IsExplicitTxn`.
    pub is_explicit_txn: bool,
    /// Go `IsWriteCacheTable`.
    pub is_write_cache_table: bool,
    /// Go `IsSyncStatsFailed`.
    pub is_sync_stats_failed: bool,
    /// Go `Prepared`.
    pub prepared: bool,
    /// Go `PlanFromCache`.
    pub plan_from_cache: bool,
    /// Go `PlanFromBinding`.
    pub plan_from_binding: bool,
    /// Go `HasMoreResults`.
    pub has_more_results: bool,
    /// Go `PrevStmt`.
    pub prev_stmt: String,
    /// Go `Plan`.
    pub plan: String,
    /// Go `PlanDigest`.
    pub plan_digest: String,
    /// Go `BinaryPlan`.
    pub binary_plan: String,
    /// Go `UsedStats`, keyed by physical table id.
    pub used_stats: BTreeMap<i64, UsedStatsInfoForTable>,
    /// Go `CopTasks`.
    pub cop_tasks: Option<CopTasksDetails>,
    /// Go `RewriteInfo`.
    pub rewrite_info: RewritePhaseInfo,
    /// Go `WriteSQLRespTotal`.
    pub write_sql_resp_total: Duration,
    /// Go `KVExecDetail`.
    pub kv_exec_detail: Option<KvExecDetails>,
    /// Go `ExecDetail`.
    pub exec_detail: ExecDetails,
    /// Go `ExecRetryCount`.
    pub exec_retry_count: u64,
    /// Go `ExecRetryTime`.
    pub exec_retry_time: Duration,
    /// Go `ResultRows`.
    pub result_rows: i64,
    /// Go `Warnings`.
    pub warnings: Vec<JsonSqlWarnForSlowLog>,
    /// Go `ResourceGroupName`.
    pub resource_group_name: String,
    /// Go `RUDetails`.
    pub ru_details: Option<Arc<tikv_client::RuDetails>>,
    /// Go `RUV2Metrics`.
    pub ruv2_metrics: Option<tidb_util::ruv2_metrics::RuV2Metrics>,
    /// Go `MemMax`.
    pub mem_max: i64,
    /// Go `DiskMax`.
    pub disk_max: i64,
    /// Go `CPUUsages`.
    pub cpu_usages: CpuUsages,
    /// Go `StorageKV`.
    pub storage_kv: bool,
    /// Go `StorageMPP`.
    pub storage_mpp: bool,
    /// Go `MemArbitration`.
    pub mem_arbitration: f64,
    /// Go `SessionConnectAttrs`.
    pub session_connect_attrs: BTreeMap<String, String>,
}

/// The `SessionVars` facts Go's `SlowLogFormat` reads off its receiver.
#[derive(Clone, Debug, Default)]
pub struct SlowLogSessionVars {
    /// Go `User`: `(Username, Hostname)`, `None` for a session with no user.
    pub user: Option<(String, String)>,
    /// Go `ConnectionInfo.ClientIP`, `None` without connection info.
    pub client_ip: Option<String>,
    /// Go `ConnectionID`.
    pub connection_id: u64,
    /// Go `SessionAlias`.
    pub session_alias: String,
    /// Go `DurationParse`.
    pub duration_parse: Duration,
    /// Go `DurationCompile`.
    pub duration_compile: Duration,
    /// Go `DurationOptimizer`.
    pub duration_optimizer: DurationOptimizer,
    /// Go `DurationWaitTS`.
    pub duration_wait_ts: Duration,
    /// Go `CurrentDB`.
    pub current_db: String,
    /// Go `InRestrictedSQL`.
    pub in_restricted_sql: bool,
    /// Go `StmtCtx.WaitLockLeaseTime > 0`.
    pub waited_lock_lease: bool,
    /// Go `CurrentDBChanged`; the caller clears it after formatting, as Go's
    /// `SlowLogFormat` does.
    pub current_db_changed: bool,
    /// Go `RUV2Weights()`.
    pub ruv2_weights: tidb_util::ruv2_metrics::RuV2Weights,
}

/// Go `SessionVars.RUV2Weights()`: the TiDB-side RU v2 weights of the
/// global config (`ruv2WeightsFromConfig`).
#[must_use]
pub fn ruv2_weights() -> tidb_util::ruv2_metrics::RuV2Weights {
    let config = tidb_config::config_tree::config::get_global_config().ru_v2;
    tidb_util::ruv2_metrics::RuV2Weights {
        ru_scale: config.ru_scale,
        result_chunk_cells: config.result_chunk_cells,
        executor_l1: config.executor_l1,
        executor_l2: config.executor_l2,
        executor_l3: config.executor_l3,
        executor_l5_insert_rows: config.executor_l5_insert_rows,
        plan_cnt: config.plan_cnt,
        plan_derive_stats_paths: config.plan_derive_stats_paths,
        resource_manager_read_cnt: config.resource_manager_read_cnt,
        resource_manager_write_cnt: config.resource_manager_write_cnt,
        write_keys: config.write_keys,
        session_parser_total: config.session_parser_total,
        txn_cnt: config.txn_cnt,
    }
}

/// Go `writeSlowLogItem`: `# key: value\n`.
fn write_slow_log_item(buf: &mut String, key: &str, value: &str) {
    buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
    buf.push_str(key);
    buf.push_str(SLOW_LOG_SPACE_MARK_STR);
    buf.push_str(value);
    buf.push('\n');
}

/// Go `strconv.FormatFloat(v, 'f', -1, 64)`.
fn format_float(value: f64) -> String {
    tidb_datatype::go_strconv::format_float(value, b'f', -1, 64)
}

/// Go `fmt`'s `%v` of a float64.
fn format_float_v(value: f64) -> String {
    tidb_datatype::go_strconv::format_float(value, b'g', -1, 64)
}

const ZERO_STR: &str = "0";

/// Go `kvExecDetailFormat`.
fn kv_exec_detail_format(buf: &mut String, detail: Option<&KvExecDetails>) {
    let Some(detail) = detail else {
        for key in [
            SLOW_LOG_KV_TOTAL,
            SLOW_LOG_PD_TOTAL,
            SLOW_LOG_BACKOFF_TOTAL,
            SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_TOTAL,
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL,
            SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE,
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE,
            SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_TOTAL,
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL,
            SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE,
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE,
        ] {
            write_slow_log_item(buf, key, ZERO_STR);
        }
        return;
    };
    write_slow_log_item(
        buf,
        SLOW_LOG_KV_TOTAL,
        &format_seconds(detail.wait_kv_resp_duration),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_PD_TOTAL,
        &format_seconds(detail.wait_pd_resp_duration),
    );
    write_slow_log_item(
        buf,
        SLOW_LOG_BACKOFF_TOTAL,
        &format_seconds(detail.backoff_duration),
    );
    for (key, value) in [
        (
            SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_TOTAL,
            detail.unpacked_bytes_sent_kv_total,
        ),
        (
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL,
            detail.unpacked_bytes_received_kv_total,
        ),
        (
            SLOW_LOG_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE,
            detail.unpacked_bytes_sent_kv_cross_zone,
        ),
        (
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE,
            detail.unpacked_bytes_received_kv_cross_zone,
        ),
        (
            SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_TOTAL,
            detail.unpacked_bytes_sent_mpp_total,
        ),
        (
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL,
            detail.unpacked_bytes_received_mpp_total,
        ),
        (
            SLOW_LOG_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE,
            detail.unpacked_bytes_sent_mpp_cross_zone,
        ),
        (
            SLOW_LOG_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE,
            detail.unpacked_bytes_received_mpp_cross_zone,
        ),
    ] {
        write_slow_log_item(buf, key, &value.to_string());
    }
}

/// Go `encoding/json`'s string encoding with `SetEscapeHTML(false)`.
pub(crate) fn go_json_string(buf: &mut String, value: &str) {
    buf.push('"');
    for character in value.chars() {
        match character {
            '\\' => buf.push_str("\\\\"),
            '"' => buf.push_str("\\\""),
            '\u{8}' => buf.push_str("\\b"),
            '\u{c}' => buf.push_str("\\f"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            '\u{2028}' => buf.push_str("\\u2028"),
            '\u{2029}' => buf.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                buf.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => buf.push(c),
        }
    }
    buf.push('"');
}

/// Go `json.NewEncoder(&buf).Encode(warnings)` with HTML escaping off,
/// including the newline `Encode` appends.
fn encode_warnings(buf: &mut String, warnings: &[JsonSqlWarnForSlowLog]) {
    buf.push('[');
    for (index, warning) in warnings.iter().enumerate() {
        if index > 0 {
            buf.push(',');
        }
        buf.push_str("{\"Level\":");
        go_json_string(buf, &warning.level);
        buf.push_str(",\"Message\":");
        go_json_string(buf, &warning.message);
        if warning.is_extra {
            buf.push_str(",\"IsExtra\":true");
        }
        buf.push('}');
    }
    buf.push_str("]\n");
}

/// Go `json.NewEncoder(&buf).Encode(map[string]string)` with HTML escaping
/// off: keys in sorted order, then the newline `Encode` appends.
fn encode_string_map(buf: &mut String, map: &BTreeMap<String, String>) {
    buf.push('{');
    for (index, (key, value)) in map.iter().enumerate() {
        if index > 0 {
            buf.push(',');
        }
        go_json_string(buf, key);
        buf.push(':');
        go_json_string(buf, value);
    }
    buf.push_str("}\n");
}

/// Go `SessionVars.SlowLogFormat`: the slow-log entry for one statement.
#[must_use]
pub fn slow_log_format(vars: &SlowLogSessionVars, items: &SlowQueryLogItems) -> String {
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
    if let Some((username, hostname)) = &vars.user {
        let host_address = vars.client_ip.as_deref().unwrap_or(hostname);
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_USER_AND_HOST_STR,
            &format!("{username}[{username}] @ {hostname} [{host_address}]"),
        );
    }
    if vars.connection_id != 0 {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_CONN_ID_STR,
            &vars.connection_id.to_string(),
        );
    }
    if !vars.session_alias.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_SESS_ALIAS_STR, &vars.session_alias);
    }
    if items.exec_retry_count > 0 {
        buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
        buf.push_str(SLOW_LOG_EXEC_RETRY_TIME);
        buf.push_str(SLOW_LOG_SPACE_MARK_STR);
        buf.push_str(&format_seconds(items.exec_retry_time));
        buf.push(' ');
        buf.push_str(SLOW_LOG_EXEC_RETRY_COUNT);
        buf.push_str(SLOW_LOG_SPACE_MARK_STR);
        // Go `strconv.Itoa(int(ExecRetryCount))`.
        buf.push_str(&(items.exec_retry_count as i64).to_string());
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
        &format_seconds(vars.duration_parse),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_COMPILE_TIME_STR,
        &format_seconds(vars.duration_compile),
    );

    buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
    buf.push_str(SLOW_LOG_REWRITE_TIME_STR);
    buf.push_str(SLOW_LOG_SPACE_MARK_STR);
    buf.push_str(&format_seconds(items.rewrite_info.duration_rewrite));
    if items.rewrite_info.preprocess_sub_queries > 0 {
        buf.push_str(&format!(
            " {SLOW_LOG_PREPROC_SUB_QUERIES_STR}{SLOW_LOG_SPACE_MARK_STR}{} {SLOW_LOG_PRE_PROC_SUB_QUERY_TIME_STR}{SLOW_LOG_SPACE_MARK_STR}{}",
            items.rewrite_info.preprocess_sub_queries,
            format_seconds(items.rewrite_info.duration_preprocess_sub_query),
        ));
    }
    buf.push('\n');

    let optimizer = &vars.duration_optimizer;
    buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
    buf.push_str(&format!(
        "{SLOW_LOG_OPTIMIZE_TIME_STR}{SLOW_LOG_SPACE_MARK_STR}{} ",
        format_seconds(optimizer.total)
    ));
    buf.push_str(&format!(
        "{SLOW_LOG_OPTIMIZE_LOGICAL_OPT}{SLOW_LOG_SPACE_MARK_STR}{} ",
        format_seconds(optimizer.logical_opt)
    ));
    buf.push_str(&format!(
        "{SLOW_LOG_OPTIMIZE_PHYSICAL_OPT}{SLOW_LOG_SPACE_MARK_STR}{} ",
        format_seconds(optimizer.physical_opt)
    ));
    buf.push_str(&format!(
        "{SLOW_LOG_OPTIMIZE_BINDING_MATCH}{SLOW_LOG_SPACE_MARK_STR}{} ",
        format_seconds(optimizer.binding_match)
    ));
    buf.push_str(&format!(
        "{SLOW_LOG_OPTIMIZE_STATS_SYNC_WAIT}{SLOW_LOG_SPACE_MARK_STR}{} ",
        format_seconds(optimizer.stats_sync_wait)
    ));
    buf.push_str(&format!(
        "{SLOW_LOG_OPTIMIZE_STATS_DERIVE}{SLOW_LOG_SPACE_MARK_STR}{}",
        format_seconds(optimizer.stats_derive)
    ));
    buf.push('\n');

    write_slow_log_item(
        &mut buf,
        SLOW_LOG_WAIT_TS_TIME_STR,
        &format_seconds(vars.duration_wait_ts),
    );

    let exec_detail = items.exec_detail.to_string();
    if !exec_detail.is_empty() {
        buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
        buf.push_str(&exec_detail);
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

    if !vars.current_db.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_DB_STR, &vars.current_db.to_lowercase());
    }
    if !items.index_names.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_INDEX_NAMES_STR, &items.index_names);
    }

    write_slow_log_item(
        &mut buf,
        SLOW_LOG_IS_INTERNAL_STR,
        if vars.in_restricted_sql {
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
        for used in items.used_stats.values() {
            if first_comma {
                buf.push(',');
            }
            buf.push_str(&used.write_to_slow_log());
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
            buf.push_str(&cop_tasks.process_time_stats.string(
                task_num,
                SLOW_LOG_SPACE_MARK_STR,
                SLOW_LOG_COP_PROC_AVG,
                SLOW_LOG_COP_PROC_P90,
                SLOW_LOG_COP_PROC_MAX,
                SLOW_LOG_COP_PROC_ADDR,
            ));
            buf.push('\n');
            buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
            buf.push_str(&cop_tasks.wait_time_stats.string(
                task_num,
                SLOW_LOG_SPACE_MARK_STR,
                SLOW_LOG_COP_WAIT_AVG,
                SLOW_LOG_COP_WAIT_P90,
                SLOW_LOG_COP_WAIT_MAX,
                SLOW_LOG_COP_WAIT_ADDR,
            ));
            buf.push('\n');
            // The backoff names in sorted order ("make the result stable").
            for (backoff, times) in &cop_tasks.tot_backoff_times {
                let prefix = format!("{SLOW_LOG_COP_BACKOFF_PREFIX}{backoff}_");
                let stats = cop_tasks
                    .backoff_time_stats_map
                    .get(backoff)
                    .cloned()
                    .unwrap_or_default();
                buf.push_str(SLOW_LOG_ROW_PREFIX_STR);
                if task_num == 1 {
                    buf.push_str(&format!(
                        "{prefix}total_times{SLOW_LOG_SPACE_MARK_STR}{times} {prefix}total_time{SLOW_LOG_SPACE_MARK_STR}{}\n",
                        format_float_v(stats.tot_time.as_secs_f64()),
                    ));
                } else {
                    buf.push_str(&format!(
                        "{prefix}total_times{m}{times} {prefix}total_time{m}{} {prefix}max_time{m}{} {prefix}max_addr{m}{} {prefix}avg_time{m}{} {prefix}p90_time{m}{}\n",
                        format_float_v(stats.tot_time.as_secs_f64()),
                        format_float_v(stats.max_time.as_secs_f64()),
                        stats.max_address,
                        format_float_v(stats.avg_time.as_secs_f64()),
                        format_float_v(stats.p90_time.as_secs_f64()),
                        m = SLOW_LOG_SPACE_MARK_STR,
                    ));
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
            &format_float(items.mem_arbitration),
        );
    }
    if items.disk_max > 0 {
        write_slow_log_item(&mut buf, SLOW_LOG_DISK_MAX, &items.disk_max.to_string());
    }

    let bool_text = |value: bool| if value { "true" } else { "false" };
    write_slow_log_item(&mut buf, SLOW_LOG_PREPARED, bool_text(items.prepared));
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_PLAN_FROM_CACHE,
        bool_text(items.plan_from_cache),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_PLAN_FROM_BINDING,
        bool_text(items.plan_from_binding),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_HAS_MORE_RESULTS,
        bool_text(items.has_more_results),
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
    write_slow_log_item(&mut buf, SLOW_LOG_SUCC, bool_text(items.succ));
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_IS_EXPLICIT_TXN,
        bool_text(items.is_explicit_txn),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_IS_SYNC_STATS_FAILED,
        bool_text(items.is_sync_stats_failed),
    );
    if vars.waited_lock_lease {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_IS_WRITE_CACHE_TABLE,
            bool_text(items.is_write_cache_table),
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
    let ru_details = items.ru_details.as_deref();
    let rru = ru_details.map_or(0.0, tikv_client::RuDetails::read_ru);
    if rru > 0.0 {
        write_slow_log_item(&mut buf, SLOW_LOG_RRU, &format_float(rru));
    }
    let wru = ru_details.map_or(0.0, tikv_client::RuDetails::write_ru);
    if wru > 0.0 {
        write_slow_log_item(&mut buf, SLOW_LOG_WRU, &format_float(wru));
    }
    let wait_ru = ru_details.map_or(Duration::ZERO, tikv_client::RuDetails::ru_wait_duration);
    if wait_ru > Duration::ZERO {
        write_slow_log_item(
            &mut buf,
            SLOW_LOG_WAIT_RU_DURATION,
            &format_seconds(wait_ru),
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
        bool_text(items.storage_kv),
    );
    write_slow_log_item(
        &mut buf,
        SLOW_LOG_STORAGE_FROM_MPP,
        bool_text(items.storage_mpp),
    );
    let (tikv_ru, tiflash_ru) = ru_details.map_or((0.0, 0.0), |details| {
        (details.tikv_ru_v2(), details.tiflash_ru())
    });
    let (total, formatted) = tidb_util::ruv2_metrics::format_ruv2_summary(
        items.ruv2_metrics.as_ref(),
        vars.ruv2_weights,
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
        encode_string_map(&mut buf, &items.session_connect_attrs);
    }
    if !items.prev_stmt.is_empty() {
        write_slow_log_item(&mut buf, SLOW_LOG_PREV_STMT, &items.prev_stmt);
    }
    for field in tidb_config::config_tree::config::get_global_config()
        .get_keyspace_observability_slow_log_fields()
    {
        write_slow_log_item(&mut buf, &field.name, &field.value);
    }

    if vars.current_db_changed {
        buf.push_str(&format!("use {};\n", vars.current_db.to_lowercase()));
    }

    buf.push_str(&items.sql);
    if !items.sql.ends_with(';') {
        buf.push(';');
    }
    buf
}

/// Go `vardef.GlobalSlowLogRateLimiter`: a token bucket created as
/// `rate.NewLimiter(rate.Inf, 1)` and reconfigured by
/// `tidb_slow_log_max_per_sec`'s `SetGlobal` (0 is `rate.Inf`, `d` is
/// `SetLimit(d)` + `SetBurst(d)`).
struct SlowLogRateLimiter {
    /// The applied `tidb_slow_log_max_per_sec`; zero is unlimited.
    limit: i64,
    burst: f64,
    tokens: f64,
    last: std::time::Instant,
}

impl SlowLogRateLimiter {
    /// `rate.Limiter.advance`: the tokens refilled since `last`, capped at
    /// the burst.
    fn advance(&mut self, now: std::time::Instant) {
        if self.limit > 0 {
            let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.limit as f64).min(self.burst);
        } else {
            self.tokens = self.burst;
        }
        self.last = now;
    }

    /// `SetGlobal`'s reconfiguration, applied when the variable changed.
    fn configure(&mut self, limit: i64, now: std::time::Instant) {
        if limit == self.limit {
            return;
        }
        self.advance(now);
        self.limit = limit;
        if limit > 0 {
            self.burst = limit as f64;
            self.tokens = self.tokens.min(self.burst);
        }
    }

    /// `rate.Limiter.Allow`.
    fn allow(&mut self, now: std::time::Instant) -> bool {
        if self.limit <= 0 {
            return true;
        }
        self.advance(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return true;
        }
        false
    }
}

static GLOBAL_SLOW_LOG_RATE_LIMITER: std::sync::LazyLock<std::sync::Mutex<SlowLogRateLimiter>> =
    std::sync::LazyLock::new(|| {
        std::sync::Mutex::new(SlowLogRateLimiter {
            limit: 0,
            burst: 1.0,
            tokens: 1.0,
            last: std::time::Instant::now(),
        })
    });

/// Go `vardef.GlobalSlowLogRateLimiter.Allow()` under the current
/// `tidb_slow_log_max_per_sec`.
pub(crate) fn slow_log_rate_limiter_allow(max_per_sec: i64) -> bool {
    let now = std::time::Instant::now();
    let mut limiter = GLOBAL_SLOW_LOG_RATE_LIMITER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    limiter.configure(max_per_sec.max(0), now);
    limiter.allow(now)
}

/// Go `executor.sampleLoggerFactory`: one entry per minute, category
/// `slow log`.
pub(crate) fn slow_log_sample_logger() -> tidb_util::logutil::SampledLogger {
    static LOGGER: std::sync::LazyLock<tidb_util::logutil::SampledLogger> =
        std::sync::LazyLock::new(|| {
            tidb_util::logutil::sample_logger_factory(
                Duration::from_secs(60),
                1,
                vec![tidb_log::Field::new(
                    tidb_util::logutil::LOG_FIELD_CATEGORY,
                    tidb_log::Value::Str("slow log".to_owned()),
                )],
            )()
        });
    LOGGER.clone()
}

/// Go `executor.QueryReplacer`: carriage returns, newlines and tabs become
/// spaces so one statement stays on one log line.
#[must_use]
pub fn query_replacer(sql: &str) -> String {
    sql.replace(['\r', '\n', '\t'], " ")
}

/// Go `executor.formatSQL`: the text cut to `tidb_query_log_max_len` bytes
/// with its full length appended, then [`query_replacer`].
#[must_use]
pub fn format_sql(sql: &str, max_query_len: i32) -> String {
    let length = sql.len();
    if max_query_len <= 0 {
        return query_replacer(sql);
    }
    let max = max_query_len as usize;
    if length > max {
        // Go slices bytes; a cut through a multi-byte rune keeps its prefix
        // bytes, which this text keeps as the replacement character.
        let cut = String::from_utf8_lossy(&sql.as_bytes()[..max]).into_owned();
        return query_replacer(&format!("{cut}(len:{length})"));
    }
    query_replacer(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_vars() -> SlowLogSessionVars {
        SlowLogSessionVars {
            user: Some(("root".to_owned(), "192.168.0.1".to_owned())),
            client_ip: Some("192.168.0.1".to_owned()),
            connection_id: 1,
            current_db: "TeST".to_owned(),
            duration_parse: Duration::from_nanos(10),
            duration_compile: Duration::from_nanos(10),
            duration_optimizer: DurationOptimizer {
                total: Duration::from_nanos(10),
                logical_opt: Duration::from_nanos(10),
                physical_opt: Duration::from_nanos(10),
                binding_match: Duration::from_nanos(10),
                stats_sync_wait: Duration::from_nanos(10),
                stats_derive: Duration::from_nanos(10),
            },
            duration_wait_ts: Duration::from_nanos(3),
            ..SlowLogSessionVars::default()
        }
    }

    /// Go `TestSlowLogFormat` (`pkg/sessionctx/variable/tests/session_test.go`):
    /// the same inputs and the same expected entry, byte for byte, including
    /// `use test;` after a database change and the connection attributes.
    #[test]
    fn slow_log_format_matches_go_test_slow_log_format() {
        let mut vars = session_vars();
        vars.session_alias = "aliasabc".to_owned();
        vars.in_restricted_sql = true;
        vars.waited_lock_lease = true;
        let mut exec_detail = ExecDetails {
            request_count: 2,
            ..ExecDetails::default()
        };
        exec_detail.cop_exec_details.backoff_time = Duration::from_millis(1);
        exec_detail.cop_exec_details.scan_detail = Some(tidb_exec::exec_details::ScanDetail {
            processed_keys: 20001,
            total_keys: 10000,
            ia_cache_hit_count: 2,
            ia_remote_read_segment_count: 4,
            ia_remote_read_segment_bytes: 4096,
            ia_remote_read_segment_duration: Duration::from_millis(15),
            ..tidb_exec::exec_details::ScanDetail::default()
        });
        exec_detail.cop_exec_details.time_detail.process_time = Duration::from_secs(2);
        exec_detail.cop_exec_details.time_detail.wait_time = Duration::from_secs(60);
        let statuses = |pairs: &[(i64, &str)]| {
            pairs
                .iter()
                .map(|(id, status)| (*id, (*status).to_owned()))
                .collect::<BTreeMap<_, _>>()
        };
        let mut used_stats = BTreeMap::new();
        used_stats.insert(
            1,
            UsedStatsInfoForTable {
                name: "t1".to_owned(),
                version: 123,
                realtime_count: 1000,
                modify_count: 0,
                column_stats_load_status: statuses(&[(2, "allEvicted"), (3, "onlyCmsEvicted")]),
                index_stats_load_status: statuses(&[(1, "allLoaded"), (2, "allLoaded")]),
            },
        );
        used_stats.insert(
            2,
            UsedStatsInfoForTable {
                name: "t2".to_owned(),
                version: 0,
                realtime_count: 10000,
                modify_count: 0,
                column_stats_load_status: statuses(&[(2, "unInitialized")]),
                ..UsedStatsInfoForTable::default()
            },
        );
        let mut cop_tasks = CopTasksDetails {
            num_cop_tasks: 10,
            process_time_stats: tidb_exec::exec_details::TaskTimeStats {
                avg_time: Duration::from_secs(1),
                p90_time: Duration::from_secs(2),
                max_address: "10.6.131.78".to_owned(),
                max_time: Duration::from_secs(3),
                tot_time: Duration::ZERO,
            },
            wait_time_stats: tidb_exec::exec_details::TaskTimeStats {
                avg_time: Duration::from_millis(10),
                p90_time: Duration::from_millis(20),
                max_time: Duration::from_millis(30),
                max_address: "10.6.131.79".to_owned(),
                tot_time: Duration::ZERO,
            },
            ..CopTasksDetails::default()
        };
        for backoff in ["rpcTiKV", "rpcPD", "regionMiss"] {
            cop_tasks.backoff_time_stats_map.insert(
                backoff.to_owned(),
                tidb_exec::exec_details::TaskTimeStats {
                    max_time: Duration::from_millis(200),
                    max_address: "127.0.0.1".to_owned(),
                    avg_time: Duration::from_millis(200),
                    p90_time: Duration::from_millis(200),
                    tot_time: Duration::from_millis(200),
                },
            );
            cop_tasks.tot_backoff_times.insert(backoff.to_owned(), 200);
        }
        let sql = "select * from t;";
        let mut items = SlowQueryLogItems {
            txn_ts: 406649736972468225,
            keyspace_name: "keyspace_a".to_owned(),
            keyspace_id: 1,
            sql: sql.to_owned(),
            digest: tidb_parser::normalize_digest(sql).1.to_string(),
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
            kv_exec_detail: Some(KvExecDetails {
                wait_kv_resp_duration: Duration::from_secs(10),
                wait_pd_resp_duration: Duration::from_secs(11),
                backoff_duration: Duration::from_secs(12),
                ..KvExecDetails::default()
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
            exec_retry_time: Duration::from_millis(5100),
            is_explicit_txn: true,
            is_write_cache_table: true,
            used_stats,
            resource_group_name: "rg1".to_owned(),
            ru_details: Some(Arc::new(tikv_client::RuDetails::new_with(
                50.0,
                100.56,
                Duration::from_millis(134),
            ))),
            storage_kv: true,
            storage_mpp: false,
            mem_arbitration: Duration::from_nanos(54321).as_secs_f64(),
            ..SlowQueryLogItems::default()
        };
        let result_fields = "# Txn_start_ts: 406649736972468225
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
# Optimize_time: 0.00000001 Opt_logical: 0.00000001 Opt_physical: 0.00000001 Opt_binding_match: 0.00000001 Opt_stats_sync_wait: 0.00000001 Opt_stats_derive: 0.00000001
# Wait_TS: 0.000000003
# Process_time: 2 Wait_time: 60 Backoff_time: 0.001 Request_count: 2 Process_keys: 20001 Total_keys: 10000
# IA_remote_read_segment_count: 4
# IA_remote_read_segment_size: 4096
# IA_remote_read_segment_wait_time: 0.015
# DB: test
# Index_names: [t1:a,t2:b]
# Is_internal: true
# Digest: e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7
# Stats: t1:stats_meta_version=123[realtime_count=1000;modify_count=0][ID 1:allLoaded,ID 2:allLoaded][ID 2:allEvicted,ID 3:onlyCmsEvicted],t2:stats_meta_version=pseudo[realtime_count=10000;modify_count=0]
# Num_cop_tasks: 10
# Cop_proc_avg: 1 Cop_proc_p90: 2 Cop_proc_max: 3 Cop_proc_addr: 10.6.131.78
# Cop_wait_avg: 0.01 Cop_wait_p90: 0.02 Cop_wait_max: 0.03 Cop_wait_addr: 10.6.131.79
# Cop_backoff_regionMiss_total_times: 200 Cop_backoff_regionMiss_total_time: 0.2 Cop_backoff_regionMiss_max_time: 0.2 Cop_backoff_regionMiss_max_addr: 127.0.0.1 Cop_backoff_regionMiss_avg_time: 0.2 Cop_backoff_regionMiss_p90_time: 0.2
# Cop_backoff_rpcPD_total_times: 200 Cop_backoff_rpcPD_total_time: 0.2 Cop_backoff_rpcPD_max_time: 0.2 Cop_backoff_rpcPD_max_addr: 127.0.0.1 Cop_backoff_rpcPD_avg_time: 0.2 Cop_backoff_rpcPD_p90_time: 0.2
# Cop_backoff_rpcTiKV_total_times: 200 Cop_backoff_rpcTiKV_total_time: 0.2 Cop_backoff_rpcTiKV_max_time: 0.2 Cop_backoff_rpcTiKV_max_addr: 127.0.0.1 Cop_backoff_rpcTiKV_avg_time: 0.2 Cop_backoff_rpcTiKV_p90_time: 0.2
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
        assert_eq!(
            slow_log_format(&vars, &items),
            format!("{result_fields}\n{sql}")
        );
        assert!(!slow_log_format(&vars, &items).contains(SLOW_LOG_SESSION_CONNECT_ATTRS));

        vars.current_db_changed = true;
        assert_eq!(
            slow_log_format(&vars, &items),
            format!("{result_fields}\nuse test;\n{sql}")
        );
        vars.current_db_changed = false;

        items.session_connect_attrs = [
            ("_client_name", "libmysql"),
            ("_os", "Linux"),
            ("app_name", "test_svc"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect();
        let entry = slow_log_format(&vars, &items);
        assert!(entry.contains(
            r#"# Session_connect_attrs: {"_client_name":"libmysql","_os":"Linux","app_name":"test_svc"}"#
        ));
        let attrs = entry.find("Session_connect_attrs").unwrap();
        assert!(attrs > entry.find(SLOW_LOG_STORAGE_FROM_MPP).unwrap());
        assert!(attrs < entry.find(sql).unwrap());

        // A warning line: Go omits `IsExtra` when false and leaves HTML
        // characters unescaped.
        items.session_connect_attrs.clear();
        items.warnings = vec![
            JsonSqlWarnForSlowLog {
                level: "Warning".to_owned(),
                message: "a \"<b>\"\n".to_owned(),
                is_extra: false,
            },
            JsonSqlWarnForSlowLog {
                level: "Note".to_owned(),
                message: "extra".to_owned(),
                is_extra: true,
            },
        ];
        assert!(slow_log_format(&vars, &items).contains(
            r#"# Result_rows: 12345
# Warnings: [{"Level":"Warning","Message":"a \"<b>\"\n"},{"Level":"Note","Message":"extra","IsExtra":true}]
# Succ: true
"#
        ));
    }

    /// The single-task cop lines: Go prints only the average and the address,
    /// and only each backoff's count and total, as `%v` floats.
    #[test]
    fn slow_log_format_single_cop_task() {
        let vars = SlowLogSessionVars::default();
        let mut cop_tasks = CopTasksDetails {
            num_cop_tasks: 1,
            ..CopTasksDetails::default()
        };
        cop_tasks.process_time_stats.avg_time = Duration::from_micros(15);
        cop_tasks.process_time_stats.max_address = "store1".to_owned();
        cop_tasks.backoff_time_stats_map.insert(
            "regionMiss".to_owned(),
            tidb_exec::exec_details::TaskTimeStats {
                tot_time: Duration::from_millis(2),
                ..Default::default()
            },
        );
        cop_tasks
            .tot_backoff_times
            .insert("regionMiss".to_owned(), 1);
        let items = SlowQueryLogItems {
            sql: "select 1".to_owned(),
            cop_tasks: Some(cop_tasks),
            ..SlowQueryLogItems::default()
        };
        let entry = slow_log_format(&vars, &items);
        assert!(
            entry.contains(concat!(
                "# Num_cop_tasks: 1\n",
                "# Cop_proc_avg: 1.5e-05 Cop_proc_addr: store1\n",
                "# Cop_wait_avg: 0 Cop_wait_addr: \n",
                "# Cop_backoff_regionMiss_total_times: 1 Cop_backoff_regionMiss_total_time: 0.002\n",
            )),
            "{entry}"
        );
        assert!(
            entry.starts_with("# Txn_start_ts: 0\n# Query_time: 0\n"),
            "{entry}"
        );
    }

    /// Go `SlowLogFormat`'s tail: `use db;` once after a database change, and
    /// a terminating `;` only when the SQL lacks one.
    #[test]
    fn slow_log_format_db_change_and_sql_suffix() {
        let mut vars = session_vars();
        vars.current_db_changed = true;
        let items = SlowQueryLogItems {
            sql: "select 1".to_owned(),
            ..SlowQueryLogItems::default()
        };
        let got = slow_log_format(&vars, &items);
        assert!(got.ends_with("\nuse test;\nselect 1;"), "{got}");
    }

    /// Go `formatSQL`: truncation to `tidb_query_log_max_len` with the full
    /// length appended, and line breaks folded into spaces.
    #[test]
    fn format_sql_truncates_and_replaces_like_go() {
        assert_eq!(format_sql("select\n1,\t2", 4096), "select 1, 2");
        assert_eq!(format_sql("select 1", 3), "sel(len:8)");
        assert_eq!(format_sql("select 1", 0), "select 1");
    }

    /// Go `LoadTiKVExecDetails` read into `kvExecDetailFormat`: the
    /// statement's waits, backoffs and traffic, client-rust's separate
    /// traffic collector included.
    #[test]
    fn kv_exec_details_load_the_statement_exec_details() {
        let exec_details = tikv_client::util::ExecDetails::default();
        exec_details.add_wait_kv_response(Duration::from_micros(1500));
        exec_details.add_wait_pd_response(Duration::from_micros(5));
        exec_details.add_backoff(Duration::from_millis(2));
        exec_details.traffic.add_request(100, true, false);
        exec_details.traffic.add_response(40, true, true);
        let network = tikv_client::traffic::NetworkTrafficDetails::default();
        let snapshot = load_tikv_exec_details(&exec_details, &network);
        let detail = KvExecDetails::from(&snapshot);
        let mut buf = String::new();
        kv_exec_detail_format(&mut buf, Some(&detail));
        assert_eq!(
            buf,
            "# KV_total: 0.0015\n\
             # PD_total: 0.000005\n\
             # Backoff_total: 0.002\n\
             # Unpacked_bytes_sent_tikv_total: 0\n\
             # Unpacked_bytes_received_tikv_total: 0\n\
             # Unpacked_bytes_sent_tikv_cross_zone: 0\n\
             # Unpacked_bytes_received_tikv_cross_zone: 0\n\
             # Unpacked_bytes_sent_tiflash_total: 100\n\
             # Unpacked_bytes_received_tiflash_total: 40\n\
             # Unpacked_bytes_sent_tiflash_cross_zone: 0\n\
             # Unpacked_bytes_received_tiflash_cross_zone: 40\n"
        );
    }

    /// Go `ruv2WeightsFromConfig(DefaultRUV2Config())`.
    #[test]
    fn ruv2_weights_come_from_the_config() {
        let weights = ruv2_weights();
        let config = tidb_config::config_tree::config::get_global_config().ru_v2;
        assert_eq!(weights.ru_scale, config.ru_scale);
        assert_eq!(weights.executor_l2, config.executor_l2);
        assert_eq!(weights.session_parser_total, config.session_parser_total);
        assert_eq!(weights.txn_cnt, config.txn_cnt);
        assert!(weights.ru_scale > 0.0);
    }
}
