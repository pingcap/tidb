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

//! Go `pkg/util/execdetails` execution-detail formatting surface: the
//! slow-log field-name `*Str` constants, the value shape of
//! `ExecDetails` and the client-go detail types its `String()` reads
//! (`CommitDetails`, `LockKeysDetails`, `ScanDetail`, `TimeDetail`,
//! `ReqDetailInfo`, `TiKVExecDetails`, `WriteDetail`), the byte-exact
//! `ExecDetails.String()` rendering, and `GetIARemoteReadSegmentStats`.
//!
//! The client-go detail types reuse the canonical `tikv-client` types. As in
//! Go, [`load_tikv_exec_details`] takes an atomic snapshot of the per-request
//! execution and traffic counters.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_log::{Field, Value};

use crate::runtime_stats::{
    merge_commit_details, merge_lock_keys_details, DurationWithAddr, Percentile,
};
use crate::slow_log_float::format_go_float64;

/// Go `CopTimeStr`: the sum of cop-task time spent in TiDB distSQL.
pub const COP_TIME_STR: &str = "Cop_time";
/// Go `ProcessTimeStr`: the sum of process time of all coprocessor tasks.
pub const PROCESS_TIME_STR: &str = "Process_time";
/// Go `WaitTimeStr`: the time of all coprocessor wait.
pub const WAIT_TIME_STR: &str = "Wait_time";
/// Go `BackoffTimeStr`: the time of all back-off.
pub const BACKOFF_TIME_STR: &str = "Backoff_time";
/// Go `LockKeysTimeStr`: the pessimistic lock wait interval.
pub const LOCK_KEYS_TIME_STR: &str = "LockKeys_time";
/// Go `RequestCountStr`: the request count.
pub const REQUEST_COUNT_STR: &str = "Request_count";
/// Go `PreWriteTimeStr`: the time of pre-write.
pub const PRE_WRITE_TIME_STR: &str = "Prewrite_time";
/// Go `WaitPrewriteBinlogTimeStr`: the time waiting for prewrite binlog.
pub const WAIT_PREWRITE_BINLOG_TIME_STR: &str = "Wait_prewrite_binlog_time";
/// Go `CommitTimeStr`: the time of commit.
pub const COMMIT_TIME_STR: &str = "Commit_time";
/// Go `GetCommitTSTimeStr`: the time of getting commit ts.
pub const GET_COMMIT_TS_TIME_STR: &str = "Get_commit_ts_time";
/// Go `GetLatestTsTimeStr`: the time of getting latest ts in async commit
/// and 1pc.
pub const GET_LATEST_TS_TIME_STR: &str = "Get_latest_ts_time";
/// Go `CommitBackoffTimeStr`: the time of commit backoff.
pub const COMMIT_BACKOFF_TIME_STR: &str = "Commit_backoff_time";
/// Go `BackoffTypesStr`: the backoff type.
pub const BACKOFF_TYPES_STR: &str = "Backoff_types";
/// Go `SlowestPrewriteRPCDetailStr`: details of the slowest 2pc prewrite RPC.
pub const SLOWEST_PREWRITE_RPC_DETAIL_STR: &str = "Slowest_prewrite_rpc_detail";
/// Go `CommitPrimaryRPCDetailStr`: details of the slowest 2pc commit RPC.
pub const COMMIT_PRIMARY_RPC_DETAIL_STR: &str = "Commit_primary_rpc_detail";
/// Go `ResolveLockTimeStr`: the time of resolving lock.
pub const RESOLVE_LOCK_TIME_STR: &str = "Resolve_lock_time";
/// Go `LocalLatchWaitTimeStr`: the time waiting in local latch.
pub const LOCAL_LATCH_WAIT_TIME_STR: &str = "Local_latch_wait_time";
/// Go `WriteKeysStr`: the count of keys in the transaction.
pub const WRITE_KEYS_STR: &str = "Write_keys";
/// Go `WriteSizeStr`: the key/value size in the transaction.
pub const WRITE_SIZE_STR: &str = "Write_size";
/// Go `PrewriteRegionStr`: the count of regions during pre-write.
pub const PREWRITE_REGION_STR: &str = "Prewrite_region";
/// Go `TxnRetryStr`: the count of transaction retry.
pub const TXN_RETRY_STR: &str = "Txn_retry";
/// Go `GetSnapshotTimeStr`: the time spent getting an engine snapshot.
pub const GET_SNAPSHOT_TIME_STR: &str = "Get_snapshot_time";
/// Go `RocksdbDeleteSkippedCountStr`: rocksdb delete skipped count.
pub const ROCKSDB_DELETE_SKIPPED_COUNT_STR: &str = "Rocksdb_delete_skipped_count";
/// Go `RocksdbKeySkippedCountStr`: rocksdb key skipped count.
pub const ROCKSDB_KEY_SKIPPED_COUNT_STR: &str = "Rocksdb_key_skipped_count";
/// Go `RocksdbBlockCacheHitCountStr`: rocksdb block cache hit count.
pub const ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR: &str = "Rocksdb_block_cache_hit_count";
/// Go `RocksdbBlockReadCountStr`: rocksdb block read count.
pub const ROCKSDB_BLOCK_READ_COUNT_STR: &str = "Rocksdb_block_read_count";
/// Go `RocksdbBlockReadByteStr`: bytes of rocksdb block read.
pub const ROCKSDB_BLOCK_READ_BYTE_STR: &str = "Rocksdb_block_read_byte";
/// Go `RocksdbBlockReadTimeStr`: time spent on rocksdb block read.
pub const ROCKSDB_BLOCK_READ_TIME_STR: &str = "Rocksdb_block_read_time";
/// Go `ProcessKeysStr`: the total processed keys.
pub const PROCESS_KEYS_STR: &str = "Process_keys";
/// Go `TotalKeysStr`: the total scan keys.
pub const TOTAL_KEYS_STR: &str = "Total_keys";
/// Go `IARemoteReadSegmentCountStr`: the number of IA remote segment reads.
pub const IA_REMOTE_READ_SEGMENT_COUNT_STR: &str = "IA_remote_read_segment_count";
/// Go `IARemoteReadSegmentSizeStr`: bytes returned from IA remote segment
/// reads.
pub const IA_REMOTE_READ_SEGMENT_SIZE_STR: &str = "IA_remote_read_segment_size";
/// Go `IARemoteReadSegmentWaitTimeStr`: total time waiting for IA remote
/// segment reads.
pub const IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR: &str = "IA_remote_read_segment_wait_time";

pub use tikv_client::util::{
    CommitDetails, CommitDetailsInner, CommitTsLagDetails, LockKeysDetails, LockKeysDetailsInner,
    ReqDetailInfo, ResolveLockDetail, ScanDetail, TiKvExecDetails as TiKVExecDetails, TimeDetail,
    WriteDetail,
};

/// Go `CopExecDetails`: cop execution detail information.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CopExecDetails {
    /// Go `CopExecDetails.ScanDetail` (`*util.ScanDetail`).
    pub scan_detail: Option<ScanDetail>,
    /// Go `CopExecDetails.TimeDetail`.
    pub time_detail: TimeDetail,
    /// Go `CopExecDetails.CalleeAddress`.
    pub callee_address: String,
    /// Go `CopExecDetails.BackoffTime`.
    pub backoff_time: Duration,
    /// Go `CopExecDetails.BackoffSleep`.
    pub backoff_sleep: HashMap<String, Duration>,
    /// Go `CopExecDetails.BackoffTimes`.
    pub backoff_times: HashMap<String, i64>,
}

/// Go `ExecDetails`: execution detail information. Go embeds
/// `CopExecDetails`; here it is a named field read through the same paths.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExecDetails {
    /// Go's embedded `CopExecDetails`.
    pub cop_exec_details: CopExecDetails,
    /// Go `ExecDetails.CommitDetail` (`*util.CommitDetails`).
    pub commit_detail: Option<CommitDetails>,
    /// Go `ExecDetails.LockKeysDetail` (`*util.LockKeysDetails`).
    pub lock_keys_detail: Option<LockKeysDetails>,
    /// Go `ExecDetails.SharedLockKeysDetail` (`*util.LockKeysDetails`).
    pub shared_lock_keys_detail: Option<LockKeysDetails>,
    /// Go `ExecDetails.CopTime`.
    pub cop_time: Duration,
    /// Go `ExecDetails.LockKeysDuration`.
    pub lock_keys_duration: Duration,
    /// Go `ExecDetails.RequestCount` (`int`).
    pub request_count: i64,
}

/// Go `P90BackoffSummary`: one backoff type's request and duration summary.
#[derive(Clone, Debug, Default)]
pub struct P90BackoffSummary {
    /// Go `ReqTimes`.
    pub req_times: i64,
    /// Go `BackoffPercentile`.
    pub backoff_percentile: Percentile<DurationWithAddr>,
    /// Go `TotBackoffTime`.
    pub tot_backoff_time: Duration,
    /// Go `TotBackoffTimes`.
    pub tot_backoff_times: i64,
}

/// Go `P90Summary`: percentile input accumulated across cop tasks.
#[derive(Clone, Debug, Default)]
pub struct P90Summary {
    /// Go `NumCopTasks`.
    pub num_cop_tasks: i64,
    /// Go `ProcessTimePercentile`.
    pub process_time_percentile: Percentile<DurationWithAddr>,
    /// Go `WaitTimePercentile`.
    pub wait_time_percentile: Percentile<DurationWithAddr>,
    /// Go `BackoffInfo`.
    pub backoff_info: HashMap<String, P90BackoffSummary>,
}

/// Go `TaskTimeStats`.
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
    /// Go `TaskTimeStats.String`.
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

/// Go `CopTasksDetails`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopTasksDetails {
    /// Go `CopTasksDetails.NumCopTasks`.
    pub num_cop_tasks: i64,
    /// Go `CopTasksDetails.ProcessTimeStats`.
    pub process_time_stats: TaskTimeStats,
    /// Go `CopTasksDetails.WaitTimeStats`.
    pub wait_time_stats: TaskTimeStats,
    /// Go `CopTasksDetails.BackoffTimeStatsMap`.
    pub backoff_time_stats_map: BTreeMap<String, TaskTimeStats>,
    /// Go `CopTasksDetails.TotBackoffTimes`.
    pub tot_backoff_times: BTreeMap<String, i64>,
}

impl P90Summary {
    /// Go `P90Summary.Reset`.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Go `P90Summary.Merge`.
    pub fn merge(
        &mut self,
        backoff_sleep: &HashMap<String, Duration>,
        backoff_times: &HashMap<String, i64>,
        callee_address: &str,
        time_detail: &TimeDetail,
    ) {
        self.num_cop_tasks += 1;
        self.process_time_percentile.add(DurationWithAddr {
            d: duration_nanos_i64(time_detail.process_time),
            addr: callee_address.to_owned(),
        });
        self.wait_time_percentile.add(DurationWithAddr {
            d: duration_nanos_i64(time_detail.wait_time),
            addr: callee_address.to_owned(),
        });
        for (backoff, times) in backoff_times {
            let sleep = backoff_sleep.get(backoff).copied().unwrap_or_default();
            let info = self.backoff_info.entry(backoff.clone()).or_default();
            info.req_times += 1;
            info.tot_backoff_time = info.tot_backoff_time.saturating_add(sleep);
            info.tot_backoff_times += times;
            info.backoff_percentile.add(DurationWithAddr {
                d: duration_nanos_i64(sleep),
                addr: callee_address.to_owned(),
            });
        }
    }
}

/// Go `StmtExecDetails`: statement-local execution details and RUv2 metrics.
#[derive(Debug, Default)]
pub struct StmtExecDetails {
    /// Go `WriteSQLRespDuration`.
    pub write_sql_resp_duration: Duration,
    ruv2_metrics: Option<std::sync::Arc<crate::ruv2_metrics::RuV2Metrics>>,
}

/// Rust shared-pointer representation of Go `*StmtExecDetails`.
pub type SharedStmtExecDetails = Arc<Mutex<StmtExecDetails>>;

struct StmtExecDetailsContextKey;
struct Ruv2MetricsContextKey;

fn stmt_exec_details_from_context(
    context: &tikv_client::trace::TraceContext,
) -> Option<&SharedStmtExecDetails> {
    context.value::<StmtExecDetailsContextKey, SharedStmtExecDetails>()
}

/// Go `ContextWithInitializedExecDetails`.
#[must_use]
pub fn context_with_initialized_exec_details(
    context: &tikv_client::trace::TraceContext,
) -> tikv_client::trace::TraceContext {
    let mut stmt_details = StmtExecDetails::default();
    stmt_details.ensure_ruv2_metrics();
    let context = tikv_client::util::context_with_exec_details(
        context,
        Arc::new(tikv_client::util::ExecDetails::default()),
    );
    let context = tikv_client::util::context_with_ru_details(
        &context,
        Arc::new(tikv_client::RuDetails::new()),
    );
    context.with_value::<StmtExecDetailsContextKey, _>(Arc::new(Mutex::new(stmt_details)))
}

/// Go `ContextWithMissingExecDetailsInitialized`.
#[must_use]
pub fn context_with_missing_exec_details_initialized(
    context: &tikv_client::trace::TraceContext,
) -> tikv_client::trace::TraceContext {
    let mut derived = context.clone();
    if tikv_client::util::exec_details_from_context(&derived).is_none() {
        derived = tikv_client::util::context_with_exec_details(
            &derived,
            Arc::new(tikv_client::util::ExecDetails::default()),
        );
    }
    if tikv_client::util::ru_details_from_context(&derived).is_none() {
        derived = tikv_client::util::context_with_ru_details(
            &derived,
            Arc::new(tikv_client::RuDetails::new()),
        );
    }

    let stmt_details = match stmt_exec_details_from_context(&derived).cloned() {
        Some(details) => details,
        None => {
            let inherited = derived
                .value::<Ruv2MetricsContextKey, Arc<crate::ruv2_metrics::RuV2Metrics>>()
                .cloned();
            let mut details = StmtExecDetails::default();
            details.set_ruv2_metrics(inherited);
            let details = Arc::new(Mutex::new(details));
            derived = derived.with_value::<StmtExecDetailsContextKey, _>(details.clone());
            details
        }
    };
    let mut stmt_details = stmt_details.lock().expect("StmtExecDetails mutex poisoned");
    if stmt_details.ruv2_metrics().is_none() {
        if let Some(inherited) = derived
            .value::<Ruv2MetricsContextKey, Arc<crate::ruv2_metrics::RuV2Metrics>>()
            .cloned()
        {
            stmt_details.set_ruv2_metrics(Some(inherited));
        } else {
            stmt_details.ensure_ruv2_metrics();
        }
    }
    drop(stmt_details);
    derived
}

/// Go `ContextWithInheritedRUV2Details`.
#[must_use]
pub fn context_with_inherited_ruv2_details(
    context: &tikv_client::trace::TraceContext,
    source: Option<&tikv_client::trace::TraceContext>,
) -> tikv_client::trace::TraceContext {
    let Some(source) = source else {
        return context.clone();
    };
    let mut derived = context.clone();
    if tikv_client::util::ru_details_from_context(&derived).is_none() {
        if let Some(details) = tikv_client::util::ru_details_from_context(source) {
            derived = tikv_client::util::context_with_ru_details(&derived, details.clone());
        }
    }
    if ruv2_metrics_from_context(&derived).is_none() {
        if let Some(metrics) = ruv2_metrics_from_context(source) {
            derived = context_with_ruv2_metrics(&derived, Some(metrics));
        }
    }
    derived
}

/// Go `ContextWithRUV2Metrics`.
#[must_use]
pub fn context_with_ruv2_metrics(
    context: &tikv_client::trace::TraceContext,
    metrics: Option<Arc<crate::ruv2_metrics::RuV2Metrics>>,
) -> tikv_client::trace::TraceContext {
    let Some(metrics) = metrics else {
        return context.clone();
    };
    if let Some(stmt_details) = stmt_exec_details_from_context(context) {
        stmt_details
            .lock()
            .expect("StmtExecDetails mutex poisoned")
            .set_ruv2_metrics(Some(metrics));
        return context.clone();
    }
    context.with_value::<Ruv2MetricsContextKey, _>(metrics)
}

/// Go `RUV2MetricsFromContext`.
#[must_use]
pub fn ruv2_metrics_from_context(
    context: &tikv_client::trace::TraceContext,
) -> Option<Arc<crate::ruv2_metrics::RuV2Metrics>> {
    if let Some(stmt_details) = stmt_exec_details_from_context(context) {
        if let Some(metrics) = stmt_details
            .lock()
            .expect("StmtExecDetails mutex poisoned")
            .ruv2_metrics()
            .cloned()
        {
            return Some(metrics);
        }
    }
    context
        .value::<Ruv2MetricsContextKey, Arc<crate::ruv2_metrics::RuV2Metrics>>()
        .cloned()
}

/// Go `SyncRUV2MetricsFromContext`.
#[must_use]
pub fn sync_ruv2_metrics_from_context(
    context: &tikv_client::trace::TraceContext,
) -> Option<Arc<crate::ruv2_metrics::RuV2Metrics>> {
    let metrics = ruv2_metrics_from_context(context)?;
    crate::ruv2_metrics::sync_ruv2_metrics_from_ru_details(
        Some(&metrics),
        tikv_client::util::ru_details_from_context(context).map(Arc::as_ref),
    );
    Some(metrics)
}

/// Go `LoadTiKVExecDetails`: snapshots every atomic field in client-go's
/// `util.ExecDetails`. A nil detail maps to the zero value.
#[must_use]
pub fn load_tikv_exec_details(
    detail: Option<&tikv_client::util::ExecDetails>,
) -> tikv_client::util::ExecDetailsSnapshot {
    detail.map_or_else(Default::default, tikv_client::util::ExecDetails::snapshot)
}

/// Go `GetExecDetailsFromContext`. The returned client execution details are
/// an atomic snapshot; a missing RU detail is replaced by a fresh empty one.
#[must_use]
pub fn get_exec_details_from_context(
    context: &tikv_client::trace::TraceContext,
) -> (
    Duration,
    tikv_client::util::ExecDetailsSnapshot,
    Arc<tikv_client::RuDetails>,
) {
    let write_sql_resp_duration = stmt_exec_details_from_context(context)
        .map(|details| {
            details
                .lock()
                .expect("StmtExecDetails mutex poisoned")
                .write_sql_resp_duration
        })
        .unwrap_or_default();
    let exec_details = load_tikv_exec_details(
        tikv_client::util::exec_details_from_context(context).map(Arc::as_ref),
    );
    let ru_details = tikv_client::util::ru_details_from_context(context)
        .cloned()
        .unwrap_or_else(|| Arc::new(tikv_client::RuDetails::new()));
    (write_sql_resp_duration, exec_details, ru_details)
}

impl StmtExecDetails {
    /// Go `ensureRUV2Metrics`.
    pub fn ensure_ruv2_metrics(&mut self) -> std::sync::Arc<crate::ruv2_metrics::RuV2Metrics> {
        self.ruv2_metrics
            .get_or_insert_with(|| std::sync::Arc::new(crate::ruv2_metrics::RuV2Metrics::new()))
            .clone()
    }

    /// Go `getRUV2Metrics`.
    #[must_use]
    pub fn ruv2_metrics(&self) -> Option<&std::sync::Arc<crate::ruv2_metrics::RuV2Metrics>> {
        self.ruv2_metrics.as_ref()
    }

    /// Go `setRUV2Metrics`.
    pub fn set_ruv2_metrics(
        &mut self,
        metrics: Option<std::sync::Arc<crate::ruv2_metrics::RuV2Metrics>>,
    ) {
        self.ruv2_metrics = metrics;
    }
}

/// Go `CopTasksSummary`: statement-summary subset of cop task statistics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopTasksSummary {
    /// Go `NumCopTasks`.
    pub num_cop_tasks: i64,
    /// Go `MaxProcessAddress`.
    pub max_process_address: String,
    /// Go `MaxProcessTime`.
    pub max_process_time: Duration,
    /// Go `TotProcessTime`.
    pub tot_process_time: Duration,
    /// Go `MaxWaitAddress`.
    pub max_wait_address: String,
    /// Go `MaxWaitTime`.
    pub max_wait_time: Duration,
    /// Go `TotWaitTime`.
    pub tot_wait_time: Duration,
}

#[derive(Clone, Debug, Default)]
struct SyncExecDetailsInner {
    exec_details: ExecDetails,
    details_summary: P90Summary,
}

/// Go `SyncExecDetails`: mutex-protected statement execution details.
#[derive(Debug, Default)]
pub struct SyncExecDetails {
    inner: Mutex<SyncExecDetailsInner>,
}

impl SyncExecDetails {
    /// Go `MergeExecDetails`.
    pub fn merge_exec_details(&self, commit_details: Option<&CommitDetails>) {
        let Some(commit_details) = commit_details else {
            return;
        };
        let mut inner = self.inner.lock().expect("SyncExecDetails mutex poisoned");
        match &mut inner.exec_details.commit_detail {
            Some(existing) => merge_commit_details(existing, commit_details),
            None => inner.exec_details.commit_detail = Some(commit_details.clone()),
        }
    }

    /// Go `MergeCopExecDetails`.
    pub fn merge_cop_exec_details(&self, details: Option<&CopExecDetails>, cop_time: Duration) {
        let Some(details) = details else {
            return;
        };
        let mut inner = self.inner.lock().expect("SyncExecDetails mutex poisoned");
        inner.exec_details.cop_time = inner.exec_details.cop_time.saturating_add(cop_time);
        inner.exec_details.cop_exec_details.backoff_time = inner
            .exec_details
            .cop_exec_details
            .backoff_time
            .saturating_add(details.backoff_time);
        inner.exec_details.request_count += 1;
        merge_scan_detail(
            &mut inner.exec_details.cop_exec_details.scan_detail,
            details.scan_detail.as_ref(),
        );
        inner.exec_details.cop_exec_details.time_detail.process_time = inner
            .exec_details
            .cop_exec_details
            .time_detail
            .process_time
            .saturating_add(details.time_detail.process_time);
        inner.exec_details.cop_exec_details.time_detail.wait_time = inner
            .exec_details
            .cop_exec_details
            .time_detail
            .wait_time
            .saturating_add(details.time_detail.wait_time);
        inner.details_summary.merge(
            &details.backoff_sleep,
            &details.backoff_times,
            &details.callee_address,
            &details.time_detail,
        );
    }

    /// Go `MergeLockKeysExecDetails`.
    pub fn merge_lock_keys_exec_details(&self, lock_keys: Option<&LockKeysDetails>) {
        let Some(lock_keys) = lock_keys else {
            return;
        };
        let mut inner = self.inner.lock().expect("SyncExecDetails mutex poisoned");
        match &mut inner.exec_details.lock_keys_detail {
            Some(existing) => merge_lock_keys_details(existing, lock_keys),
            None => inner.exec_details.lock_keys_detail = Some(lock_keys.clone()),
        }
    }

    /// Go `MergeSharedLockKeysExecDetails`.
    pub fn merge_shared_lock_keys_exec_details(&self, lock_keys: Option<&LockKeysDetails>) {
        let Some(lock_keys) = lock_keys else {
            return;
        };
        let mut inner = self.inner.lock().expect("SyncExecDetails mutex poisoned");
        match &mut inner.exec_details.shared_lock_keys_detail {
            Some(existing) => merge_lock_keys_details(existing, lock_keys),
            None => inner.exec_details.shared_lock_keys_detail = Some(lock_keys.clone()),
        }
    }

    /// Go `Reset`.
    pub fn reset(&self) {
        *self.inner.lock().expect("SyncExecDetails mutex poisoned") =
            SyncExecDetailsInner::default();
    }

    /// Go `GetExecDetails` as an ownership-safe snapshot.
    #[must_use]
    pub fn exec_details(&self) -> ExecDetails {
        self.inner
            .lock()
            .expect("SyncExecDetails mutex poisoned")
            .exec_details
            .clone()
    }

    /// Go `CopTasksDetails`.
    #[must_use]
    pub fn cop_tasks_details(&self) -> Option<CopTasksDetails> {
        let mut inner = self.inner.lock().expect("SyncExecDetails mutex poisoned");
        let n = inner.details_summary.num_cop_tasks;
        if n == 0 {
            return None;
        }
        let process_max = inner.details_summary.process_time_percentile.get_max();
        let wait_max = inner.details_summary.wait_time_percentile.get_max();
        let mut detail = CopTasksDetails {
            num_cop_tasks: n,
            process_time_stats: TaskTimeStats {
                tot_time: inner.exec_details.cop_exec_details.time_detail.process_time,
                avg_time: divide_duration(
                    inner.exec_details.cop_exec_details.time_detail.process_time,
                    n,
                ),
                p90_time: duration_from_f64_nanos(
                    inner
                        .details_summary
                        .process_time_percentile
                        .get_percentile(0.9),
                ),
                max_time: duration_from_i64_nanos(process_max.d),
                max_address: process_max.addr,
            },
            wait_time_stats: TaskTimeStats {
                tot_time: inner.exec_details.cop_exec_details.time_detail.wait_time,
                avg_time: divide_duration(
                    inner.exec_details.cop_exec_details.time_detail.wait_time,
                    n,
                ),
                p90_time: duration_from_f64_nanos(
                    inner
                        .details_summary
                        .wait_time_percentile
                        .get_percentile(0.9),
                ),
                max_time: duration_from_i64_nanos(wait_max.d),
                max_address: wait_max.addr,
            },
            ..CopTasksDetails::default()
        };
        for (backoff, info) in &mut inner.details_summary.backoff_info {
            if info.req_times == 0 {
                continue;
            }
            let max = info.backoff_percentile.get_max();
            detail.backoff_time_stats_map.insert(
                backoff.clone(),
                TaskTimeStats {
                    max_address: max.addr,
                    max_time: duration_from_i64_nanos(max.d),
                    p90_time: duration_from_f64_nanos(info.backoff_percentile.get_percentile(0.9)),
                    avg_time: divide_duration(info.tot_backoff_time, info.req_times),
                    tot_time: info.tot_backoff_time,
                },
            );
            detail
                .tot_backoff_times
                .insert(backoff.clone(), info.tot_backoff_times);
        }
        Some(detail)
    }

    /// Go `CopTasksSummary`.
    #[must_use]
    pub fn cop_tasks_summary(&self) -> Option<CopTasksSummary> {
        let inner = self.inner.lock().expect("SyncExecDetails mutex poisoned");
        if inner.details_summary.num_cop_tasks == 0 {
            return None;
        }
        let process_max = inner.details_summary.process_time_percentile.get_max();
        let wait_max = inner.details_summary.wait_time_percentile.get_max();
        Some(CopTasksSummary {
            num_cop_tasks: inner.details_summary.num_cop_tasks,
            max_process_address: process_max.addr,
            max_process_time: duration_from_i64_nanos(process_max.d),
            tot_process_time: inner.exec_details.cop_exec_details.time_detail.process_time,
            max_wait_address: wait_max.addr,
            max_wait_time: duration_from_i64_nanos(wait_max.d),
            tot_wait_time: inner.exec_details.cop_exec_details.time_detail.wait_time,
        })
    }
}

/// Go `IARemoteReadSegmentStats`: IA remote-read scan statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IaRemoteReadSegmentStats {
    /// Go `IARemoteReadSegmentStats.Count` (`uint64`).
    pub count: u64,
    /// Go `IARemoteReadSegmentStats.Bytes` (`uint64`).
    pub bytes: u64,
    /// Go `IARemoteReadSegmentStats.WaitTime`.
    pub wait_time: Duration,
}

/// Go `GetIARemoteReadSegmentStats`: reads IA remote-read scan statistics
/// from a client-go `ScanDetail`, returning zeros for a nil detail.
#[must_use]
pub fn get_ia_remote_read_segment_stats(
    scan_detail: Option<&ScanDetail>,
) -> IaRemoteReadSegmentStats {
    match scan_detail {
        None => IaRemoteReadSegmentStats::default(),
        Some(detail) => IaRemoteReadSegmentStats {
            count: detail.ia_remote_read_segment_count,
            bytes: detail.ia_remote_read_segment_bytes,
            wait_time: detail.ia_remote_read_segment_duration,
        },
    }
}

fn duration_nanos_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn duration_from_i64_nanos(nanos: i64) -> Duration {
    Duration::from_nanos(nanos.max(0) as u64)
}

fn duration_from_f64_nanos(nanos: f64) -> Duration {
    if !nanos.is_finite() || nanos <= 0.0 {
        Duration::ZERO
    } else {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "Go time.Duration(float64) conversion"
        )]
        Duration::from_nanos(nanos as u64)
    }
}

fn divide_duration(duration: Duration, divisor: i64) -> Duration {
    if divisor <= 0 {
        return Duration::ZERO;
    }
    Duration::from_nanos(duration_nanos_i64(duration).div_euclid(divisor) as u64)
}

fn merge_scan_detail(dst: &mut Option<ScanDetail>, src: Option<&ScanDetail>) {
    let Some(src) = src else {
        return;
    };
    let dst = dst.get_or_insert_with(ScanDetail::default);
    dst.processed_keys += src.processed_keys;
    dst.processed_keys_size += src.processed_keys_size;
    dst.total_keys += src.total_keys;
    dst.get_snapshot_duration = dst
        .get_snapshot_duration
        .saturating_add(src.get_snapshot_duration);
    dst.ia_cache_hit_count += src.ia_cache_hit_count;
    dst.rocksdb_delete_skipped_count += src.rocksdb_delete_skipped_count;
    dst.rocksdb_key_skipped_count += src.rocksdb_key_skipped_count;
    dst.rocksdb_block_cache_hit_count += src.rocksdb_block_cache_hit_count;
    dst.rocksdb_block_read_count += src.rocksdb_block_read_count;
    dst.rocksdb_block_read_bytes += src.rocksdb_block_read_bytes;
    dst.rocksdb_block_read_duration = dst
        .rocksdb_block_read_duration
        .saturating_add(src.rocksdb_block_read_duration);
    dst.ia_remote_read_segment_count += src.ia_remote_read_segment_count;
    dst.ia_remote_read_segment_bytes += src.ia_remote_read_segment_bytes;
    dst.ia_remote_read_segment_duration = dst
        .ia_remote_read_segment_duration
        .saturating_add(src.ia_remote_read_segment_duration);
}

fn push_zap_duration_field(fields: &mut Vec<Field>, key: &str, duration: Duration) {
    if duration > Duration::ZERO {
        fields.push(Field::new(
            key,
            Value::Str(format!("{}s", format_seconds(duration))),
        ));
    }
}

/// Renders a duration's seconds the way Go spells
/// `strconv.FormatFloat(d.Seconds(), 'f', -1, 64)`: shortest decimal that
/// round-trips the float64, never exponent notation. Rust's `f64` `Display`
/// has exactly that contract.
#[must_use]
pub fn format_seconds(d: Duration) -> String {
    format!("{}", d.as_secs_f64())
}

/// Renders a duration's seconds the way Go spells
/// `strconv.FormatFloat(d.Seconds(), 'f', 3, 64)`: fixed three decimals.
#[must_use]
pub fn format_seconds_3(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64())
}

/// Renders a duration the way Go `time.Duration.String()` does for
/// non-negative durations (`0s`, `10µs`, `500ms`, `1s`, `1h2m3.5s`).
/// client-go's `util.FormatDuration` — which additionally rounds long
/// durations to three significant digits — is pinned only where the Go
/// `TestString` fixture exercises it, and at every such point it coincides
/// with this spelling.
#[must_use]
pub fn format_go_duration(d: Duration) -> String {
    let total = d.as_nanos();
    if total == 0 {
        return "0s".to_owned();
    }
    if total < 1_000_000_000 {
        let (scale, prec, unit) = if total < 1_000 {
            (1u128, 0usize, "ns")
        } else if total < 1_000_000 {
            (1_000, 3, "µs")
        } else {
            (1_000_000, 6, "ms")
        };
        let mut out = (total / scale).to_string();
        push_fraction(&mut out, total % scale, prec);
        out.push_str(unit);
        return out;
    }
    let secs = total / 1_000_000_000;
    let mut tail = (secs % 60).to_string();
    push_fraction(&mut tail, total % 1_000_000_000, 9);
    tail.push('s');
    let minutes = secs / 60;
    if minutes == 0 {
        return tail;
    }
    let hours = minutes / 60;
    if hours == 0 {
        return format!("{minutes}m{tail}");
    }
    format!("{hours}h{}m{tail}", minutes % 60)
}

/// Appends Go's trimmed fractional digits (`fmtFrac`): `prec` zero-padded
/// digits with trailing zeros removed, and no dot when nothing remains.
fn push_fraction(out: &mut String, frac: u128, prec: usize) {
    if frac == 0 || prec == 0 {
        return;
    }
    let mut digits = format!("{frac:0prec$}");
    while digits.ends_with('0') {
        digits.pop();
    }
    if !digits.is_empty() {
        out.push('.');
        out.push_str(&digits);
    }
}

/// Renders a Go `[]string` the way `fmt.Sprintf("%v", s)` does:
/// space-joined inside brackets.
fn format_go_string_slice(items: &[String]) -> String {
    format!("[{}]", items.join(" "))
}

/// One slowest-RPC part: Go's
/// `{total:<'f',3>s, region_id: <n>, store: <addr>, <TiKVExecDetails>}`.
fn req_detail_part(label: &str, info: &ReqDetailInfo) -> String {
    format!(
        "{label}: {{total:{}s, region_id: {}, store: {}, {}}}",
        format_seconds_3(info.request_total_time),
        info.region,
        info.store_address,
        info.exec_details,
    )
}

/// Go `ExecDetails.String()`: the space-joined slow-log rendering, arm for
/// arm and byte for byte.
impl fmt::Display for ExecDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = Vec::with_capacity(8);
        if self.cop_time > Duration::ZERO {
            parts.push(format!("{COP_TIME_STR}: {}", format_seconds(self.cop_time)));
        }
        let time_detail = &self.cop_exec_details.time_detail;
        if time_detail.process_time > Duration::ZERO {
            parts.push(format!(
                "{PROCESS_TIME_STR}: {}",
                format_seconds(time_detail.process_time)
            ));
        }
        if time_detail.wait_time > Duration::ZERO {
            parts.push(format!(
                "{WAIT_TIME_STR}: {}",
                format_seconds(time_detail.wait_time)
            ));
        }
        if self.cop_exec_details.backoff_time > Duration::ZERO {
            parts.push(format!(
                "{BACKOFF_TIME_STR}: {}",
                format_seconds(self.cop_exec_details.backoff_time)
            ));
        }
        if let Some(lock_key_details) = &self.lock_keys_detail {
            if lock_key_details.total_time > Duration::ZERO {
                parts.push(format!(
                    "{LOCK_KEYS_TIME_STR}: {}",
                    format_seconds(lock_key_details.total_time)
                ));
            }
        }
        if self.request_count > 0 {
            parts.push(format!("{REQUEST_COUNT_STR}: {}", self.request_count));
        }
        if let Some(commit) = &self.commit_detail {
            if commit.prewrite_time > Duration::ZERO {
                parts.push(format!(
                    "{PRE_WRITE_TIME_STR}: {}",
                    format_seconds(commit.prewrite_time)
                ));
            }
            if commit.wait_prewrite_binlog_time > Duration::ZERO {
                parts.push(format!(
                    "{WAIT_PREWRITE_BINLOG_TIME_STR}: {}",
                    format_seconds(commit.wait_prewrite_binlog_time)
                ));
            }
            if commit.commit_time > Duration::ZERO {
                parts.push(format!(
                    "{COMMIT_TIME_STR}: {}",
                    format_seconds(commit.commit_time)
                ));
            }
            if commit.get_commit_ts_time > Duration::ZERO {
                parts.push(format!(
                    "{GET_COMMIT_TS_TIME_STR}: {}",
                    format_seconds(commit.get_commit_ts_time)
                ));
            }
            if commit.get_latest_ts_time > Duration::ZERO {
                parts.push(format!(
                    "{GET_LATEST_TS_TIME_STR}: {}",
                    format_seconds(commit.get_latest_ts_time)
                ));
            }
            if commit.detail.commit_backoff_time_ns > 0 {
                parts.push(format!(
                    "{COMMIT_BACKOFF_TIME_STR}: {}",
                    format_seconds(Duration::from_nanos(
                        commit.detail.commit_backoff_time_ns.unsigned_abs()
                    ))
                ));
            }
            if !commit.detail.prewrite_backoff_types.is_empty() {
                parts.push(format!(
                    "Prewrite_{BACKOFF_TYPES_STR}: {}",
                    format_go_string_slice(&commit.detail.prewrite_backoff_types)
                ));
            }
            if !commit.detail.commit_backoff_types.is_empty() {
                parts.push(format!(
                    "Commit_{BACKOFF_TYPES_STR}: {}",
                    format_go_string_slice(&commit.detail.commit_backoff_types)
                ));
            }
            if commit.detail.slowest_prewrite.request_total_time > Duration::ZERO {
                parts.push(req_detail_part(
                    SLOWEST_PREWRITE_RPC_DETAIL_STR,
                    &commit.detail.slowest_prewrite,
                ));
            }
            if commit.detail.commit_primary.request_total_time > Duration::ZERO {
                parts.push(req_detail_part(
                    COMMIT_PRIMARY_RPC_DETAIL_STR,
                    &commit.detail.commit_primary,
                ));
            }
            if commit.resolve_lock.resolve_lock_time_ns > 0 {
                parts.push(format!(
                    "{RESOLVE_LOCK_TIME_STR}: {}",
                    format_seconds(Duration::from_nanos(
                        commit.resolve_lock.resolve_lock_time_ns.unsigned_abs()
                    ))
                ));
            }
            if commit.local_latch_time > Duration::ZERO {
                parts.push(format!(
                    "{LOCAL_LATCH_WAIT_TIME_STR}: {}",
                    format_seconds(commit.local_latch_time)
                ));
            }
            if commit.write_keys > 0 {
                parts.push(format!("{WRITE_KEYS_STR}: {}", commit.write_keys));
            }
            if commit.write_size > 0 {
                parts.push(format!("{WRITE_SIZE_STR}: {}", commit.write_size));
            }
            if commit.prewrite_region_num > 0 {
                parts.push(format!(
                    "{PREWRITE_REGION_STR}: {}",
                    commit.prewrite_region_num
                ));
            }
            if commit.transaction_retry > 0 {
                parts.push(format!("{TXN_RETRY_STR}: {}", commit.transaction_retry));
            }
        }
        if let Some(scan) = &self.cop_exec_details.scan_detail {
            if scan.processed_keys > 0 {
                parts.push(format!("{PROCESS_KEYS_STR}: {}", scan.processed_keys));
            }
            if scan.total_keys > 0 {
                parts.push(format!("{TOTAL_KEYS_STR}: {}", scan.total_keys));
            }
            if scan.get_snapshot_duration > Duration::ZERO {
                parts.push(format!(
                    "{GET_SNAPSHOT_TIME_STR}: {}",
                    format_seconds_3(scan.get_snapshot_duration)
                ));
            }
            if scan.rocksdb_delete_skipped_count > 0 {
                parts.push(format!(
                    "{ROCKSDB_DELETE_SKIPPED_COUNT_STR}: {}",
                    scan.rocksdb_delete_skipped_count
                ));
            }
            if scan.rocksdb_key_skipped_count > 0 {
                parts.push(format!(
                    "{ROCKSDB_KEY_SKIPPED_COUNT_STR}: {}",
                    scan.rocksdb_key_skipped_count
                ));
            }
            if scan.rocksdb_block_cache_hit_count > 0 {
                parts.push(format!(
                    "{ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR}: {}",
                    scan.rocksdb_block_cache_hit_count
                ));
            }
            if scan.rocksdb_block_read_count > 0 {
                parts.push(format!(
                    "{ROCKSDB_BLOCK_READ_COUNT_STR}: {}",
                    scan.rocksdb_block_read_count
                ));
            }
            if scan.rocksdb_block_read_bytes > 0 {
                parts.push(format!(
                    "{ROCKSDB_BLOCK_READ_BYTE_STR}: {}",
                    scan.rocksdb_block_read_bytes
                ));
            }
            if scan.rocksdb_block_read_duration > Duration::ZERO {
                parts.push(format!(
                    "{ROCKSDB_BLOCK_READ_TIME_STR}: {}",
                    format_seconds_3(scan.rocksdb_block_read_duration)
                ));
            }
        }
        f.write_str(&parts.join(" "))
    }
}

impl ExecDetails {
    /// Go `ExecDetails.ToZapFields`.
    #[must_use]
    pub fn to_zap_fields(&self) -> Vec<Field> {
        let mut fields = Vec::with_capacity(16);
        push_zap_duration_field(
            &mut fields,
            &COP_TIME_STR.to_ascii_lowercase(),
            self.cop_time,
        );
        push_zap_duration_field(
            &mut fields,
            &PROCESS_TIME_STR.to_ascii_lowercase(),
            self.cop_exec_details.time_detail.process_time,
        );
        push_zap_duration_field(
            &mut fields,
            &WAIT_TIME_STR.to_ascii_lowercase(),
            self.cop_exec_details.time_detail.wait_time,
        );
        push_zap_duration_field(
            &mut fields,
            &BACKOFF_TIME_STR.to_ascii_lowercase(),
            self.cop_exec_details.backoff_time,
        );
        if self.request_count > 0 {
            fields.push(Field::new(
                REQUEST_COUNT_STR.to_ascii_lowercase(),
                Value::Str(self.request_count.to_string()),
            ));
        }
        if let Some(scan) = &self.cop_exec_details.scan_detail {
            if scan.total_keys > 0 {
                fields.push(Field::new(
                    TOTAL_KEYS_STR.to_ascii_lowercase(),
                    Value::Str(scan.total_keys.to_string()),
                ));
            }
            if scan.processed_keys > 0 {
                fields.push(Field::new(
                    PROCESS_KEYS_STR.to_ascii_lowercase(),
                    Value::Str(scan.processed_keys.to_string()),
                ));
            }
        }
        if let Some(commit) = &self.commit_detail {
            push_zap_duration_field(&mut fields, "prewrite_time", commit.prewrite_time);
            push_zap_duration_field(&mut fields, "commit_time", commit.commit_time);
            push_zap_duration_field(&mut fields, "get_commit_ts_time", commit.get_commit_ts_time);
            if commit.detail.commit_backoff_time_ns > 0 {
                push_zap_duration_field(
                    &mut fields,
                    "commit_backoff_time",
                    Duration::from_nanos(commit.detail.commit_backoff_time_ns as u64),
                );
            }
            if !commit.detail.prewrite_backoff_types.is_empty() {
                fields.push(Field::new(
                    format!("Prewrite_{BACKOFF_TYPES_STR}"),
                    Value::Str(format_go_string_slice(
                        &commit.detail.prewrite_backoff_types,
                    )),
                ));
            }
            if !commit.detail.commit_backoff_types.is_empty() {
                fields.push(Field::new(
                    format!("Commit_{BACKOFF_TYPES_STR}"),
                    Value::Str(format_go_string_slice(&commit.detail.commit_backoff_types)),
                ));
            }
            if commit.detail.slowest_prewrite.request_total_time > Duration::ZERO {
                fields.push(Field::new(
                    SLOWEST_PREWRITE_RPC_DETAIL_STR,
                    Value::Str(format!(
                        "total:{}s, region_id: {}, store: {}, {}}}",
                        format_seconds_3(commit.detail.slowest_prewrite.request_total_time),
                        commit.detail.slowest_prewrite.region,
                        commit.detail.slowest_prewrite.store_address,
                        commit.detail.slowest_prewrite.exec_details,
                    )),
                ));
            }
            if commit.detail.commit_primary.request_total_time > Duration::ZERO {
                fields.push(Field::new(
                    COMMIT_PRIMARY_RPC_DETAIL_STR,
                    Value::Str(format!(
                        "{{total:{}s, region_id: {}, store: {}, {}}}",
                        format_seconds_3(commit.detail.commit_primary.request_total_time),
                        commit.detail.commit_primary.region,
                        commit.detail.commit_primary.store_address,
                        commit.detail.commit_primary.exec_details,
                    )),
                ));
            }
            if commit.resolve_lock.resolve_lock_time_ns > 0 {
                push_zap_duration_field(
                    &mut fields,
                    "resolve_lock_time",
                    Duration::from_nanos(commit.resolve_lock.resolve_lock_time_ns as u64),
                );
            }
            push_zap_duration_field(
                &mut fields,
                "local_latch_wait_time",
                commit.local_latch_time,
            );
            if commit.write_keys > 0 {
                fields.push(Field::new(
                    "write_keys",
                    Value::I64(commit.write_keys as i64),
                ));
            }
            if commit.write_size > 0 {
                fields.push(Field::new(
                    "write_size",
                    Value::I64(commit.write_size as i64),
                ));
            }
            if commit.prewrite_region_num > 0 {
                fields.push(Field::new(
                    "prewrite_region",
                    Value::I64(i64::from(commit.prewrite_region_num)),
                ));
            }
            if commit.transaction_retry > 0 {
                fields.push(Field::new(
                    "txn_retry",
                    Value::I64(commit.transaction_retry as i64),
                ));
            }
        }
        fields
    }
}

impl TaskTimeStats {
    /// Go `TaskTimeStats.FormatFloatFields`.
    #[must_use]
    pub fn format_float_fields(&self) -> (String, String, String) {
        (
            format_seconds(self.avg_time),
            format_seconds(self.p90_time),
            format_seconds(self.max_time),
        )
    }
}

impl CopTasksDetails {
    /// Go `CopTasksDetails.ToZapFields`.
    #[must_use]
    pub fn to_zap_fields(&self) -> Vec<Field> {
        if self.num_cop_tasks == 0 {
            return Vec::new();
        }
        let mut fields = Vec::with_capacity(10);
        fields.push(Field::new("num_cop_tasks", Value::I64(self.num_cop_tasks)));
        let (avg, p90, max) = self.process_time_stats.format_float_fields();
        fields.push(Field::new(
            "process_avg_time",
            Value::Str(format!("{avg}s")),
        ));
        fields.push(Field::new(
            "process_p90_time",
            Value::Str(format!("{p90}s")),
        ));
        fields.push(Field::new(
            "process_max_time",
            Value::Str(format!("{max}s")),
        ));
        fields.push(Field::new(
            "process_max_addr",
            Value::Str(self.process_time_stats.max_address.clone()),
        ));
        let (avg, p90, max) = self.wait_time_stats.format_float_fields();
        fields.push(Field::new("wait_avg_time", Value::Str(format!("{avg}s"))));
        fields.push(Field::new("wait_p90_time", Value::Str(format!("{p90}s"))));
        fields.push(Field::new("wait_max_time", Value::Str(format!("{max}s"))));
        fields.push(Field::new(
            "wait_max_addr",
            Value::Str(self.wait_time_stats.max_address.clone()),
        ));
        fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The nested `TiKVExecDetails` fixture shared by the slowest-prewrite
    /// slot and the lock-keys slowest-request fragment in Go `TestString`.
    fn fixture_tikv_exec_details() -> TiKVExecDetails {
        TiKVExecDetails {
            time_detail: Some(Arc::new(TimeDetail {
                total_rpc_wall_time: Duration::from_millis(500),
                ..TimeDetail::default()
            })),
            scan_detail: Some(Arc::new(ScanDetail {
                processed_keys: 10,
                total_keys: 100,
                rocksdb_delete_skipped_count: 1,
                rocksdb_key_skipped_count: 1,
                rocksdb_block_cache_hit_count: 1,
                rocksdb_block_read_count: 1,
                rocksdb_block_read_bytes: 100,
                rocksdb_block_read_duration: Duration::from_millis(20),
                ..ScanDetail::default()
            })),
            write_detail: Some(Arc::new(WriteDetail {
                store_batch_wait_duration: Duration::from_micros(10),
                propose_send_wait_duration: Duration::from_micros(20),
                persist_log_duration: Duration::from_micros(30),
                raft_db_write_leader_wait_duration: Duration::from_micros(40),
                raft_db_sync_log_duration: Duration::from_micros(45),
                raft_db_write_memtable_duration: Duration::from_micros(50),
                commit_log_duration: Duration::from_micros(60),
                apply_batch_wait_duration: Duration::from_micros(70),
                apply_log_duration: Duration::from_micros(80),
                apply_mutex_lock_duration: Duration::from_micros(90),
                apply_write_leader_wait_duration: Duration::from_micros(100),
                apply_write_wal_duration: Duration::from_micros(101),
                apply_write_memtable_duration: Duration::from_micros(102),
                scheduler_process_duration: Duration::ZERO,
                ..WriteDetail::default()
            })),
        }
    }

    /// Port of Go `TestString` (`execdetails_test.go`), including the
    /// LockKeysDetails slowest-request fragment, with the byte-exact
    /// expected literal, plus the empty-details `""` case.
    #[test]
    fn exec_details_string_matches_go_test_string() {
        let detail = ExecDetails {
            cop_time: Duration::from_secs(1) + Duration::from_millis(3),
            request_count: 1,
            lock_keys_detail: Some(LockKeysDetails {
                total_time: Duration::from_secs(1),
                region_num: 2,
                lock_keys: 10,
                backoff_time_ns: 3_000_000_000,
                detail: LockKeysDetailsInner {
                    backoff_types: vec![
                        "backoff4".to_owned(),
                        "backoff5".to_owned(),
                        "backoff5".to_owned(),
                    ],
                    slowest_request_total_time: Duration::from_secs(1),
                    slowest_region: 1000,
                    slowest_store_address: "tikv-1:20160".to_owned(),
                    slowest_exec_details: fixture_tikv_exec_details(),
                },
                lock_rpc_time_ns: 5_000_000_000,
                lock_rpc_count: 50,
                retry_count: 2,
                resolve_lock: ResolveLockDetail {
                    resolve_lock_time_ns: 2_000_000_000,
                },
                ..LockKeysDetails::default()
            }),
            commit_detail: Some(CommitDetails {
                get_commit_ts_time: Duration::from_secs(1),
                get_latest_ts_time: Duration::from_secs(1),
                prewrite_time: Duration::from_secs(1),
                commit_time: Duration::from_secs(1),
                local_latch_time: Duration::from_secs(1),
                detail: CommitDetailsInner {
                    commit_backoff_time_ns: 1_000_000_000,
                    prewrite_backoff_types: vec!["backoff1".to_owned(), "backoff2".to_owned()],
                    commit_backoff_types: vec!["commit1".to_owned(), "commit2".to_owned()],
                    slowest_prewrite: ReqDetailInfo {
                        request_total_time: Duration::from_secs(1),
                        region: 1000,
                        store_address: "tikv-1:20160".to_owned(),
                        exec_details: fixture_tikv_exec_details(),
                    },
                    commit_primary: ReqDetailInfo {
                        request_total_time: Duration::from_secs(2),
                        region: 2000,
                        store_address: "tikv-2:20160".to_owned(),
                        exec_details: TiKVExecDetails {
                            time_detail: Some(Arc::new(TimeDetail {
                                total_rpc_wall_time: Duration::from_millis(1000),
                                ..TimeDetail::default()
                            })),
                            scan_detail: Some(Arc::new(ScanDetail {
                                processed_keys: 20,
                                total_keys: 200,
                                rocksdb_delete_skipped_count: 2,
                                rocksdb_key_skipped_count: 2,
                                rocksdb_block_cache_hit_count: 2,
                                rocksdb_block_read_count: 2,
                                rocksdb_block_read_bytes: 200,
                                rocksdb_block_read_duration: Duration::from_millis(40),
                                ..ScanDetail::default()
                            })),
                            write_detail: Some(Arc::new(WriteDetail {
                                store_batch_wait_duration: Duration::from_micros(110),
                                propose_send_wait_duration: Duration::from_micros(120),
                                persist_log_duration: Duration::from_micros(130),
                                raft_db_write_leader_wait_duration: Duration::from_micros(140),
                                raft_db_sync_log_duration: Duration::from_micros(145),
                                raft_db_write_memtable_duration: Duration::from_micros(150),
                                commit_log_duration: Duration::from_micros(160),
                                apply_batch_wait_duration: Duration::from_micros(170),
                                apply_log_duration: Duration::from_micros(180),
                                apply_mutex_lock_duration: Duration::from_micros(190),
                                apply_write_leader_wait_duration: Duration::from_micros(200),
                                apply_write_wal_duration: Duration::from_micros(201),
                                apply_write_memtable_duration: Duration::from_micros(202),
                                scheduler_process_duration: Duration::ZERO,
                                ..WriteDetail::default()
                            })),
                        },
                    },
                },
                write_keys: 1,
                write_size: 1,
                prewrite_region_num: 1,
                transaction_retry: 1,
                resolve_lock: ResolveLockDetail {
                    // 10^9 ns = 1s, as the Go fixture spells it.
                    resolve_lock_time_ns: 1_000_000_000,
                },
                ..CommitDetails::default()
            }),
            cop_exec_details: CopExecDetails {
                backoff_time: Duration::from_secs(1),
                scan_detail: Some(ScanDetail {
                    processed_keys: 10,
                    total_keys: 100,
                    rocksdb_delete_skipped_count: 1,
                    rocksdb_key_skipped_count: 1,
                    rocksdb_block_cache_hit_count: 1,
                    rocksdb_block_read_count: 1,
                    rocksdb_block_read_bytes: 100,
                    rocksdb_block_read_duration: Duration::from_millis(1),
                    ..ScanDetail::default()
                }),
                time_detail: TimeDetail {
                    process_time: Duration::from_secs(2) + Duration::from_millis(5),
                    wait_time: Duration::from_secs(1),
                    ..TimeDetail::default()
                },
                ..CopExecDetails::default()
            },
            ..ExecDetails::default()
        };
        let expected = concat!(
            "Cop_time: 1.003 Process_time: 2.005 Wait_time: 1 Backoff_time: 1 ",
            "LockKeys_time: 1 Request_count: 1 Prewrite_time: 1 Commit_time: ",
            "1 Get_commit_ts_time: 1 Get_latest_ts_time: 1 Commit_backoff_time: 1 ",
            "Prewrite_Backoff_types: [backoff1 backoff2] Commit_Backoff_types: [commit1 commit2] ",
            "Slowest_prewrite_rpc_detail: {total:1.000s, region_id: 1000, ",
            "store: tikv-1:20160, time_detail: {tikv_wall_time: 500ms}, scan_detail: ",
            "{total_process_keys: 10, total_keys: 100, ",
            "rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, block: ",
            "{cache_hit_count: 1, read_count: 1, ",
            "read_byte: 100 Bytes, read_time: 20ms}}}, write_detail: ",
            "{store_batch_wait: 10µs, propose_send_wait: 20µs, ",
            "persist_log: {total: 30µs, write_leader_wait: 40µs, sync_log: 45µs, ",
            "write_memtable: 50µs}, ",
            "commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, ",
            "write_leader_wait: 100µs, ",
            "write_wal: 101µs, write_memtable: 102µs}, scheduler: {process: 0s}}} ",
            "Commit_primary_rpc_detail: {total:2.000s, region_id: 2000, ",
            "store: tikv-2:20160, time_detail: {tikv_wall_time: 1s}, scan_detail: ",
            "{total_process_keys: 20, total_keys: 200, ",
            "rocksdb: {delete_skipped_count: 2, key_skipped_count: 2, block: ",
            "{cache_hit_count: 2, read_count: 2, ",
            "read_byte: 200 Bytes, read_time: 40ms}}}, write_detail: ",
            "{store_batch_wait: 110µs, propose_send_wait: 120µs, ",
            "persist_log: {total: 130µs, write_leader_wait: 140µs, sync_log: 145µs, ",
            "write_memtable: 150µs}, ",
            "commit_log: 160µs, apply_batch_wait: 170µs, apply: {total:180µs, mutex_lock: 190µs, ",
            "write_leader_wait: 200µs, ",
            "write_wal: 201µs, write_memtable: 202µs}, scheduler: {process: 0s}}} ",
            "Resolve_lock_time: 1 Local_latch_wait_time: 1 Write_keys: 1 Write_size: ",
            "1 Prewrite_region: 1 Txn_retry: 1 Process_keys: 10 Total_keys: 100 ",
            "Rocksdb_delete_skipped_count: 1 Rocksdb_key_skipped_count: ",
            "1 Rocksdb_block_cache_hit_count: 1 Rocksdb_block_read_count: 1 ",
            "Rocksdb_block_read_byte: 100 Rocksdb_block_read_time: 0.001",
        );
        assert_eq!(expected, detail.to_string());
        assert_eq!("", ExecDetails::default().to_string());
    }

    /// Go `GetIARemoteReadSegmentStats`: nil detail yields zeros; a present
    /// detail is read field for field.
    #[test]
    fn get_ia_remote_read_segment_stats_reads_scan_detail() {
        assert_eq!(
            IaRemoteReadSegmentStats::default(),
            get_ia_remote_read_segment_stats(None)
        );
        let scan = ScanDetail {
            ia_remote_read_segment_count: 3,
            ia_remote_read_segment_bytes: 4096,
            ia_remote_read_segment_duration: Duration::from_millis(7),
            ..ScanDetail::default()
        };
        assert_eq!(
            IaRemoteReadSegmentStats {
                count: 3,
                bytes: 4096,
                wait_time: Duration::from_millis(7),
            },
            get_ia_remote_read_segment_stats(Some(&scan))
        );
    }

    /// Port of Go `TestString/load tikv exec details snapshot`.
    #[test]
    fn load_tikv_exec_details_snapshots_all_atomic_fields() {
        assert_eq!(
            tikv_client::util::ExecDetailsSnapshot::default(),
            load_tikv_exec_details(None)
        );

        let details = tikv_client::util::ExecDetails::default();
        details.add_backoff(Duration::from_secs(3));
        details.add_backoff(Duration::ZERO);
        details.add_wait_kv_response(Duration::from_secs(4));
        details.add_wait_pd_response(Duration::from_secs(5));
        details.traffic.add_request(11, false, false);
        details.traffic.add_response(12, false, false);
        details.traffic.add_request(13, false, true);
        details.traffic.add_response(14, false, true);
        details.traffic.add_request(15, true, false);
        details.traffic.add_response(16, true, false);
        details.traffic.add_request(17, true, true);
        details.traffic.add_response(18, true, true);

        assert_eq!(
            tikv_client::util::ExecDetailsSnapshot {
                backoff_count: 2,
                backoff_duration_ns: 3_000_000_000,
                wait_kv_response_duration_ns: 4_000_000_000,
                wait_pd_response_duration_ns: 5_000_000_000,
                traffic: tikv_client::util::TrafficDetailsSnapshot {
                    sent_kv_total: 24,
                    received_kv_total: 26,
                    sent_kv_cross_zone: 13,
                    received_kv_cross_zone: 14,
                    sent_mpp_total: 32,
                    received_mpp_total: 34,
                    sent_mpp_cross_zone: 17,
                    received_mpp_cross_zone: 18,
                },
            },
            load_tikv_exec_details(Some(&details))
        );
    }
}
