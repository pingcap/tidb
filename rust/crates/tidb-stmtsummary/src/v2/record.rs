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

//! Go `pkg/util/stmtsummary/v2/record.go`: lands complete.
//!
//! Every production symbol of `record.go` is here — `MaxEncodedPlanSizeInBytes`,
//! `StmtRecord` with `NewStmtRecord` / `Add` / `Merge`, `formatSQL`,
//! `maxSQLLength`, and the `GenerateStmtExecInfo4Test` / `mockLazyInfo` fixture
//! pair that `record.go` declares as production code.
//!
//! What this file reuses from v1 rather than restating, mirroring `record.go`'s
//! own `import "github.com/pingcap/tidb/pkg/util/stmtsummary"`:
//!
//! - [`StmtExecInfo`], [`StmtExecLazyInfo`], [`StmtSummaryStmtCtx`],
//!   [`TableEntry`], [`CopTasksSummary`] — v1's, unchanged.
//! - Go's embedded `stmtsummary.StmtRUSummary` and
//!   `stmtsummary.StmtNetworkTrafficSummary` stay v1's [`StmtRuSummary`] and
//!   [`StmtNetworkTrafficSummary`], reached through [`StmtRecord::ru`] and
//!   [`StmtRecord::network`]; Go's embedding also flattens their JSON, which
//!   this file's hand-written [`Serialize`] reproduces.
//!
//! Where v2 genuinely diverges from v1's `stmtSummaryStats`:
//!
//! - `StmtRecord` carries the per-window `begin`/`end` and the immutable
//!   identity (`schema_name`, `digest`, `plan_digest`, `stmt_type`,
//!   `normalized_sql`, `table_names`, `is_internal`, `binding_sql`,
//!   `binding_digest`, `keyspace_name`, `keyspace_id`) that v1 splits across
//!   `stmtSummaryByDigest` and `stmtSummaryByDigestElement`.
//! - `StmtRecord` has no `sum_cop_process_time` / `sum_cop_wait_time`; v2 keeps
//!   only the `max`/address pair from `CopTasksSummary`.
//! - `Merge` has no v1 counterpart at all: v1 never merges two summaries.
//! - `Add` reads the warning count and affected rows off `info.StmtCtx` itself,
//!   where v1's `stmtSummaryStats::add` takes them as arguments.
//! - `formatSQL`'s limit comes from `GlobalStmtSummary`, not from v1's
//!   `StmtSummaryByDigestMap`, and falls back to 32768 when the global is unset.
//! - `MaxEncodedPlanSizeInBytes` is v2's own package variable, independent of
//!   v1's.
//!
//! Narrowings:
//!
//! - `encoding/json` narrows to `serde` with a hand-written [`Serialize`] for
//!   [`StmtRecord`]: it emits Go's field order, Go's `omitempty` on
//!   `keyspace_name` / `keyspace_id`, `time.Duration` as its nanosecond count,
//!   `time.Time` in Go's RFC 3339 nano form, and Go's sorted map key order for
//!   `backoff_types` / `auth_users` (`map[string]struct{}` renders as
//!   `{"user":{}}`). Rust cannot tell a nil Go map/slice from an empty one, so
//!   `index_names` renders as `null` when empty (Go's nil case, which is the
//!   only one `NewStmtRecord` can produce) and `backoff_types` / `auth_users`
//!   render as `{}` when empty (Go's non-nil case, which is the only one
//!   `NewStmtRecord` can produce).
//! - [`marshal_stmt_record`] / [`marshal_evicted_stmt_record`] /
//!   [`marshal_stmt_record_with_evicted`] and [`EvictedStmtRecord`] are carved
//!   out of Go `v2/logger.go`, which is NOT otherwise ported here: `record.go`'s
//!   own upstream test drives them, so they land beside the record they
//!   marshal. They are SEED evidence for `logger.go`; that file's
//!   `stmtLogEncoder`, its zap/lumberjack sink, and its
//!   `metrics.StmtSummaryEvictedLogCounter` wiring are absent.
//! - `config.GetGlobalConfig().GetKeyspaceObservabilityStmtLogFields()` is the
//!   real `tidb_config::config_tree::get_global_config`.
//! - `execdetails.LoadTiKVExecDetails` is already applied: v1's
//!   `StmtExecInfo::tikv_exec_details` arrives as a loaded
//!   `TikvExecDetailsSnapshot`, so Go's atomic loads become field reads.
//! - Go's `sql[:maxSQLLength]` byte slice becomes a UTF-8 boundary-safe
//!   truncation, as in v1's `format_sql`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use serde::ser::{SerializeMap, Serializer};
use serde::Serialize;
use tidb_exec::exec_details::{
    get_ia_remote_read_segment_stats, CommitDetails, CopExecDetails, ExecDetails,
    ResolveLockDetail, ScanDetail, TimeDetail,
};
use tidb_exec::slow_log_format::{RuDetailsSnapshot, TikvExecDetailsSnapshot};
use tidb_util::plancodec::{BINARY_PLAN_DISCARDED_ENCODED, PLAN_DISCARDED_ENCODED};
use tidb_util::ppcpuusage::CpuUsages;

use crate::statement_summary::{
    CopTasksSummary, EncodedPlanError, StmtExecInfo, StmtExecLazyInfo, StmtNetworkTrafficSummary,
    StmtRuSummary, StmtSummaryStmtCtx, TableEntry,
};

/// Go `MaxEncodedPlanSizeInBytes`: the upper limit of the size of the plan and
/// the binary plan in the stmt summary. Go declares it as a mutable package
/// variable, so it stays writable here. This is v2's own variable; v1's
/// [`crate::statement_summary::MAX_ENCODED_PLAN_SIZE_IN_BYTES`] is separate.
pub static MAX_ENCODED_PLAN_SIZE_IN_BYTES: AtomicUsize = AtomicUsize::new(1024 * 1024);

/// Go `defaultMaxSQLLength`, the value `maxSQLLength` falls back to when
/// `GlobalStmtSummary` is nil. `stmtsummary.go` declares the same constant.
pub const DEFAULT_MAX_SQL_LENGTH: u32 = 32768;

/// Go `StmtRecord`: a statement statistics record. `StmtRecord` is addable and
/// mergable.
#[derive(Clone, Debug, PartialEq)]
pub struct StmtRecord {
    /// Go `Begin`: each record is summarized between `[Begin, End)`.
    pub begin: i64,
    /// Go `End`.
    pub end: i64,
    // Immutable.
    /// Go `SchemaName`.
    pub schema_name: String,
    /// Go `Digest`.
    pub digest: String,
    /// Go `PlanDigest`.
    pub plan_digest: String,
    /// Go `StmtType`.
    pub stmt_type: String,
    /// Go `NormalizedSQL`.
    pub normalized_sql: String,
    /// Go `TableNames`.
    pub table_names: String,
    /// Go `IsInternal`.
    pub is_internal: bool,
    /// Go `BindingSQL`.
    pub binding_sql: String,
    /// Go `BindingDigest`.
    pub binding_digest: String,
    // Basic.
    /// Go `SampleSQL`.
    pub sample_sql: String,
    /// Go `Charset`.
    pub charset: String,
    /// Go `Collation`.
    pub collation: String,
    /// Go `PrevSQL`.
    pub prev_sql: String,
    /// Go `SamplePlan`.
    pub sample_plan: String,
    /// Go `SampleBinaryPlan`.
    pub sample_binary_plan: String,
    /// Go `PlanHint`.
    pub plan_hint: String,
    /// Go `IndexNames`.
    pub index_names: Vec<String>,
    /// Go `ExecCount`.
    pub exec_count: i64,
    /// Go `SumErrors` (`int`).
    pub sum_errors: i64,
    /// Go `SumWarnings` (`int`).
    pub sum_warnings: i64,
    // Latency.
    /// Go `SumLatency`.
    pub sum_latency: Duration,
    /// Go `MaxLatency`.
    pub max_latency: Duration,
    /// Go `MinLatency`.
    pub min_latency: Duration,
    /// Go `SumParseLatency`.
    pub sum_parse_latency: Duration,
    /// Go `MaxParseLatency`.
    pub max_parse_latency: Duration,
    /// Go `SumCompileLatency`.
    pub sum_compile_latency: Duration,
    /// Go `MaxCompileLatency`.
    pub max_compile_latency: Duration,
    // Coprocessor.
    /// Go `SumNumCopTasks`.
    pub sum_num_cop_tasks: i64,
    /// Go `MaxCopProcessTime`.
    pub max_cop_process_time: Duration,
    /// Go `MaxCopProcessAddress`.
    pub max_cop_process_address: String,
    /// Go `MaxCopWaitTime`.
    pub max_cop_wait_time: Duration,
    /// Go `MaxCopWaitAddress`.
    pub max_cop_wait_address: String,
    // TiKV.
    /// Go `SumProcessTime`.
    pub sum_process_time: Duration,
    /// Go `MaxProcessTime`.
    pub max_process_time: Duration,
    /// Go `SumWaitTime`.
    pub sum_wait_time: Duration,
    /// Go `MaxWaitTime`.
    pub max_wait_time: Duration,
    /// Go `SumBackoffTime`.
    pub sum_backoff_time: Duration,
    /// Go `MaxBackoffTime`.
    pub max_backoff_time: Duration,
    /// Go `SumTotalKeys`.
    pub sum_total_keys: i64,
    /// Go `MaxTotalKeys`.
    pub max_total_keys: i64,
    /// Go `SumProcessedKeys`.
    pub sum_processed_keys: i64,
    /// Go `MaxProcessedKeys`.
    pub max_processed_keys: i64,
    /// Go `SumRocksdbDeleteSkippedCount`.
    pub sum_rocksdb_delete_skipped_count: u64,
    /// Go `MaxRocksdbDeleteSkippedCount`.
    pub max_rocksdb_delete_skipped_count: u64,
    /// Go `SumRocksdbKeySkippedCount`.
    pub sum_rocksdb_key_skipped_count: u64,
    /// Go `MaxRocksdbKeySkippedCount`.
    pub max_rocksdb_key_skipped_count: u64,
    /// Go `SumRocksdbBlockCacheHitCount`.
    pub sum_rocksdb_block_cache_hit_count: u64,
    /// Go `MaxRocksdbBlockCacheHitCount`.
    pub max_rocksdb_block_cache_hit_count: u64,
    /// Go `SumRocksdbBlockReadCount`.
    pub sum_rocksdb_block_read_count: u64,
    /// Go `MaxRocksdbBlockReadCount`.
    pub max_rocksdb_block_read_count: u64,
    /// Go `SumRocksdbBlockReadByte`.
    pub sum_rocksdb_block_read_byte: u64,
    /// Go `MaxRocksdbBlockReadByte`.
    pub max_rocksdb_block_read_byte: u64,
    /// Go `SumIARemoteReadSegmentCount`.
    pub sum_ia_remote_read_segment_count: u64,
    /// Go `MaxIARemoteReadSegmentCount`.
    pub max_ia_remote_read_segment_count: u64,
    /// Go `SumIARemoteReadSegmentSize`.
    pub sum_ia_remote_read_segment_size: u64,
    /// Go `MaxIARemoteReadSegmentSize`.
    pub max_ia_remote_read_segment_size: u64,
    /// Go `SumIARemoteReadSegmentWaitTime`.
    pub sum_ia_remote_read_segment_wait_time: Duration,
    /// Go `MaxIARemoteReadSegmentWaitTime`.
    pub max_ia_remote_read_segment_wait_time: Duration,
    // Txn.
    /// Go `CommitCount`.
    pub commit_count: i64,
    /// Go `SumGetCommitTsTime`.
    pub sum_get_commit_ts_time: Duration,
    /// Go `MaxGetCommitTsTime`.
    pub max_get_commit_ts_time: Duration,
    /// Go `SumPrewriteTime`.
    pub sum_prewrite_time: Duration,
    /// Go `MaxPrewriteTime`.
    pub max_prewrite_time: Duration,
    /// Go `SumCommitTime`.
    pub sum_commit_time: Duration,
    /// Go `MaxCommitTime`.
    pub max_commit_time: Duration,
    /// Go `SumLocalLatchTime`.
    pub sum_local_latch_time: Duration,
    /// Go `MaxLocalLatchTime`.
    pub max_local_latch_time: Duration,
    /// Go `SumCommitBackoffTime`.
    pub sum_commit_backoff_time: i64,
    /// Go `MaxCommitBackoffTime`.
    pub max_commit_backoff_time: i64,
    /// Go `SumResolveLockTime`.
    pub sum_resolve_lock_time: i64,
    /// Go `MaxResolveLockTime`.
    pub max_resolve_lock_time: i64,
    /// Go `SumWriteKeys`.
    pub sum_write_keys: i64,
    /// Go `MaxWriteKeys` (`int`).
    pub max_write_keys: i64,
    /// Go `SumWriteSize`.
    pub sum_write_size: i64,
    /// Go `MaxWriteSize` (`int`).
    pub max_write_size: i64,
    /// Go `SumPrewriteRegionNum`.
    pub sum_prewrite_region_num: i64,
    /// Go `MaxPrewriteRegionNum` (`int32`).
    pub max_prewrite_region_num: i32,
    /// Go `SumTxnRetry`.
    pub sum_txn_retry: i64,
    /// Go `MaxTxnRetry` (`int`).
    pub max_txn_retry: i64,
    /// Go `SumBackoffTimes`.
    pub sum_backoff_times: i64,
    /// Go `BackoffTypes`.
    pub backoff_types: HashMap<String, i64>,
    /// Go `AuthUsers` (`map[string]struct{}`).
    pub auth_users: HashSet<String>,
    // Other.
    /// Go `SumMem`.
    pub sum_mem: i64,
    /// Go `MaxMem`.
    pub max_mem: i64,
    /// Go `SumDisk`.
    pub sum_disk: i64,
    /// Go `MaxDisk`.
    pub max_disk: i64,
    /// Go `SumAffectedRows`.
    pub sum_affected_rows: u64,
    /// Go `SumKVTotal`.
    pub sum_kv_total: Duration,
    /// Go `SumPDTotal`.
    pub sum_pd_total: Duration,
    /// Go `SumBackoffTotal`.
    pub sum_backoff_total: Duration,
    /// Go `SumWriteSQLRespTotal`.
    pub sum_write_sql_resp_total: Duration,
    /// Go `SumTidbCPU`.
    pub sum_tidb_cpu: Duration,
    /// Go `SumTikvCPU`.
    pub sum_tikv_cpu: Duration,
    /// Go `SumResultRows`.
    pub sum_result_rows: i64,
    /// Go `MaxResultRows`.
    pub max_result_rows: i64,
    /// Go `MinResultRows`.
    pub min_result_rows: i64,
    /// Go `Prepared`.
    pub prepared: bool,
    /// Go `FirstSeen`: the first time this type of SQL executes.
    pub first_seen: DateTime<Utc>,
    /// Go `LastSeen`: the last time this type of SQL executes.
    pub last_seen: DateTime<Utc>,
    // Plan cache.
    /// Go `PlanInCache`.
    pub plan_in_cache: bool,
    /// Go `PlanCacheHits`.
    pub plan_cache_hits: i64,
    /// Go `PlanInBinding`.
    pub plan_in_binding: bool,
    /// Go `ExecRetryCount` (`uint`): pessimistic execution retry information.
    pub exec_retry_count: u64,
    /// Go `ExecRetryTime`.
    pub exec_retry_time: Duration,
    /// Go `KeyspaceName` (`json:",omitempty"`).
    pub keyspace_name: String,
    /// Go `KeyspaceID` (`json:",omitempty"`).
    pub keyspace_id: u32,
    /// Go `ResourceGroupName`.
    pub resource_group_name: String,
    /// Go's embedded `stmtsummary.StmtRUSummary`.
    pub ru: StmtRuSummary,
    /// Go `PlanCacheUnqualifiedCount`.
    pub plan_cache_unqualified_count: i64,
    /// Go `PlanCacheUnqualifiedLastReason`: the reason why this query is
    /// unqualified for the plan cache.
    pub plan_cache_unqualified_last_reason: String,
    /// Go `SumMemArbitration`.
    pub sum_mem_arbitration: f64,
    /// Go `MaxMemArbitration`.
    pub max_mem_arbitration: f64,
    /// Go's embedded `stmtsummary.StmtNetworkTrafficSummary`.
    pub network: StmtNetworkTrafficSummary,
    /// Go `StorageKV`: query read from TiKV.
    pub storage_kv: bool,
    /// Go `StorageMPP`: query read from TiFlash.
    pub storage_mpp: bool,
}

impl Default for StmtRecord {
    fn default() -> Self {
        Self {
            begin: 0,
            end: 0,
            schema_name: String::new(),
            digest: String::new(),
            plan_digest: String::new(),
            stmt_type: String::new(),
            normalized_sql: String::new(),
            table_names: String::new(),
            is_internal: false,
            binding_sql: String::new(),
            binding_digest: String::new(),
            sample_sql: String::new(),
            charset: String::new(),
            collation: String::new(),
            prev_sql: String::new(),
            sample_plan: String::new(),
            sample_binary_plan: String::new(),
            plan_hint: String::new(),
            index_names: Vec::new(),
            exec_count: 0,
            sum_errors: 0,
            sum_warnings: 0,
            sum_latency: Duration::ZERO,
            max_latency: Duration::ZERO,
            min_latency: Duration::ZERO,
            sum_parse_latency: Duration::ZERO,
            max_parse_latency: Duration::ZERO,
            sum_compile_latency: Duration::ZERO,
            max_compile_latency: Duration::ZERO,
            sum_num_cop_tasks: 0,
            max_cop_process_time: Duration::ZERO,
            max_cop_process_address: String::new(),
            max_cop_wait_time: Duration::ZERO,
            max_cop_wait_address: String::new(),
            sum_process_time: Duration::ZERO,
            max_process_time: Duration::ZERO,
            sum_wait_time: Duration::ZERO,
            max_wait_time: Duration::ZERO,
            sum_backoff_time: Duration::ZERO,
            max_backoff_time: Duration::ZERO,
            sum_total_keys: 0,
            max_total_keys: 0,
            sum_processed_keys: 0,
            max_processed_keys: 0,
            sum_rocksdb_delete_skipped_count: 0,
            max_rocksdb_delete_skipped_count: 0,
            sum_rocksdb_key_skipped_count: 0,
            max_rocksdb_key_skipped_count: 0,
            sum_rocksdb_block_cache_hit_count: 0,
            max_rocksdb_block_cache_hit_count: 0,
            sum_rocksdb_block_read_count: 0,
            max_rocksdb_block_read_count: 0,
            sum_rocksdb_block_read_byte: 0,
            max_rocksdb_block_read_byte: 0,
            sum_ia_remote_read_segment_count: 0,
            max_ia_remote_read_segment_count: 0,
            sum_ia_remote_read_segment_size: 0,
            max_ia_remote_read_segment_size: 0,
            sum_ia_remote_read_segment_wait_time: Duration::ZERO,
            max_ia_remote_read_segment_wait_time: Duration::ZERO,
            commit_count: 0,
            sum_get_commit_ts_time: Duration::ZERO,
            max_get_commit_ts_time: Duration::ZERO,
            sum_prewrite_time: Duration::ZERO,
            max_prewrite_time: Duration::ZERO,
            sum_commit_time: Duration::ZERO,
            max_commit_time: Duration::ZERO,
            sum_local_latch_time: Duration::ZERO,
            max_local_latch_time: Duration::ZERO,
            sum_commit_backoff_time: 0,
            max_commit_backoff_time: 0,
            sum_resolve_lock_time: 0,
            max_resolve_lock_time: 0,
            sum_write_keys: 0,
            max_write_keys: 0,
            sum_write_size: 0,
            max_write_size: 0,
            sum_prewrite_region_num: 0,
            max_prewrite_region_num: 0,
            sum_txn_retry: 0,
            max_txn_retry: 0,
            sum_backoff_times: 0,
            backoff_types: HashMap::new(),
            auth_users: HashSet::new(),
            sum_mem: 0,
            max_mem: 0,
            sum_disk: 0,
            max_disk: 0,
            sum_affected_rows: 0,
            sum_kv_total: Duration::ZERO,
            sum_pd_total: Duration::ZERO,
            sum_backoff_total: Duration::ZERO,
            sum_write_sql_resp_total: Duration::ZERO,
            sum_tidb_cpu: Duration::ZERO,
            sum_tikv_cpu: Duration::ZERO,
            sum_result_rows: 0,
            max_result_rows: 0,
            min_result_rows: 0,
            prepared: false,
            first_seen: DateTime::<Utc>::from_timestamp_nanos(0),
            last_seen: DateTime::<Utc>::from_timestamp_nanos(0),
            plan_in_cache: false,
            plan_cache_hits: 0,
            plan_in_binding: false,
            exec_retry_count: 0,
            exec_retry_time: Duration::ZERO,
            keyspace_name: String::new(),
            keyspace_id: 0,
            resource_group_name: String::new(),
            ru: StmtRuSummary::default(),
            plan_cache_unqualified_count: 0,
            plan_cache_unqualified_last_reason: String::new(),
            sum_mem_arbitration: 0.0,
            max_mem_arbitration: 0.0,
            network: StmtNetworkTrafficSummary::default(),
            storage_kv: false,
            storage_mpp: false,
        }
    }
}

/// Go `NewStmtRecord`: creates a new `StmtRecord` from `StmtExecInfo`.
///
/// `StmtExecInfo` is only used to initialize the basic information of
/// `StmtRecord`. Next we need to call [`StmtRecord::add`] to add the statistics
/// of the `StmtExecInfo` into the `StmtRecord`.
///
/// Go's `GetEncodedPlan` returns a third `any` result that `NewStmtRecord`
/// discards with `_`; the ported call discards the [`EncodedPlanError`] the
/// same way, so unlike v1's `newStmtSummaryStats` this never yields `nil`.
#[must_use]
pub fn new_stmt_record(info: &StmtExecInfo) -> StmtRecord {
    // Use "," to separate table names to support FIND_IN_SET.
    let mut buffer = String::new();
    for (i, value) in info.stmt_ctx.tables.iter().enumerate() {
        // In `create database` statement, DB name is not empty but table name
        // is empty.
        if value.table.is_empty() {
            continue;
        }
        buffer.push_str(&value.db.to_lowercase());
        buffer.push('.');
        buffer.push_str(&value.table.to_lowercase());
        if i < info.stmt_ctx.tables.len() - 1 {
            buffer.push(',');
        }
    }
    let table_names = buffer;
    let mut plan_digest = info.plan_digest.clone();
    if plan_digest.is_empty() {
        // It comes here only when the plan is 'Point_Get'.
        plan_digest = info.lazy_info.plan_digest();
    }
    // sampleSQL / authUsers(sampleUser) / samplePlan / prevSQL / indexNames
    // store the values shown at the first time, because it compacts performance
    // to update every time.
    let (mut sample_plan, plan_hint) = info
        .lazy_info
        .encoded_plan()
        .unwrap_or_else(|_| (String::new(), String::new()));
    let limit = MAX_ENCODED_PLAN_SIZE_IN_BYTES.load(Ordering::SeqCst);
    if sample_plan.len() > limit {
        sample_plan = PLAN_DISCARDED_ENCODED.to_owned();
    }
    let mut bin_plan = info.lazy_info.binary_plan();
    if bin_plan.len() > limit {
        bin_plan = BINARY_PLAN_DISCARDED_ENCODED.clone();
    }
    let (binding_sql, binding_digest) = info.lazy_info.binding_sql_and_digest();
    StmtRecord {
        schema_name: info.schema_name.clone(),
        digest: info.digest.clone(),
        plan_digest,
        stmt_type: info.stmt_ctx.stmt_type.clone(),
        normalized_sql: info.normalized_sql.clone(),
        table_names,
        is_internal: info.is_internal,
        binding_sql,
        binding_digest,
        sample_sql: format_sql(&info.lazy_info.original_sql()),
        charset: info.charset.clone(),
        collation: info.collation.clone(),
        // PrevSQL is already truncated to cfg.Log.QueryLogMaxLen.
        prev_sql: info.prev_sql.clone(),
        // SamplePlan needs to be decoded so it can't be truncated.
        sample_plan,
        sample_binary_plan: bin_plan,
        plan_hint,
        index_names: info.stmt_ctx.index_names.clone(),
        min_latency: info.total_latency,
        min_result_rows: i64::MAX,
        prepared: info.prepared,
        first_seen: info.start_time,
        last_seen: info.start_time,
        keyspace_name: info.keyspace_name.clone(),
        keyspace_id: info.keyspace_id,
        resource_group_name: info.resource_group_name.clone(),
        ..StmtRecord::default()
    }
}

impl StmtRecord {
    /// Go `(*StmtRecord).Add`: adds the statistics of `StmtExecInfo` to this
    /// record.
    #[allow(clippy::too_many_lines)]
    pub fn add(&mut self, info: &StmtExecInfo) {
        self.is_internal = self.is_internal && info.is_internal;
        // Add user to auth users set.
        if !info.user.is_empty() {
            self.auth_users.insert(info.user.clone());
        }
        self.exec_count += 1;
        if !info.succeed {
            self.sum_errors += 1;
        }
        self.sum_warnings += i64::from(info.stmt_ctx.warning_count());
        // Latency.
        self.sum_latency += info.total_latency;
        if info.total_latency > self.max_latency {
            self.max_latency = info.total_latency;
        }
        if info.total_latency < self.min_latency {
            self.min_latency = info.total_latency;
        }
        self.sum_parse_latency += info.parse_latency;
        if info.parse_latency > self.max_parse_latency {
            self.max_parse_latency = info.parse_latency;
        }
        self.sum_compile_latency += info.compile_latency;
        if info.compile_latency > self.max_compile_latency {
            self.max_compile_latency = info.compile_latency;
        }
        // Coprocessor.
        if let Some(cop_tasks) = info.cop_tasks.as_ref() {
            self.sum_num_cop_tasks += cop_tasks.num_cop_tasks;
            if cop_tasks.max_process_time > self.max_cop_process_time {
                self.max_cop_process_time = cop_tasks.max_process_time;
                self.max_cop_process_address
                    .clone_from(&cop_tasks.max_process_address);
            }
            if cop_tasks.max_wait_time > self.max_cop_wait_time {
                self.max_cop_wait_time = cop_tasks.max_wait_time;
                self.max_cop_wait_address
                    .clone_from(&cop_tasks.max_wait_address);
            }
        }
        // TiKV.
        let time_detail = &info.exec_detail.cop_exec_details.time_detail;
        self.sum_process_time += time_detail.process_time;
        if time_detail.process_time > self.max_process_time {
            self.max_process_time = time_detail.process_time;
        }
        self.sum_wait_time += time_detail.wait_time;
        if time_detail.wait_time > self.max_wait_time {
            self.max_wait_time = time_detail.wait_time;
        }
        let backoff_time = info.exec_detail.cop_exec_details.backoff_time;
        self.sum_backoff_time += backoff_time;
        if backoff_time > self.max_backoff_time {
            self.max_backoff_time = backoff_time;
        }
        if let Some(scan_detail) = info.exec_detail.cop_exec_details.scan_detail.as_ref() {
            self.sum_total_keys += scan_detail.total_keys;
            if scan_detail.total_keys > self.max_total_keys {
                self.max_total_keys = scan_detail.total_keys;
            }
            self.sum_processed_keys += scan_detail.processed_keys;
            if scan_detail.processed_keys > self.max_processed_keys {
                self.max_processed_keys = scan_detail.processed_keys;
            }
            self.sum_rocksdb_delete_skipped_count += scan_detail.rocksdb_delete_skipped_count;
            if scan_detail.rocksdb_delete_skipped_count > self.max_rocksdb_delete_skipped_count {
                self.max_rocksdb_delete_skipped_count = scan_detail.rocksdb_delete_skipped_count;
            }
            self.sum_rocksdb_key_skipped_count += scan_detail.rocksdb_key_skipped_count;
            if scan_detail.rocksdb_key_skipped_count > self.max_rocksdb_key_skipped_count {
                self.max_rocksdb_key_skipped_count = scan_detail.rocksdb_key_skipped_count;
            }
            self.sum_rocksdb_block_cache_hit_count += scan_detail.rocksdb_block_cache_hit_count;
            if scan_detail.rocksdb_block_cache_hit_count > self.max_rocksdb_block_cache_hit_count {
                self.max_rocksdb_block_cache_hit_count = scan_detail.rocksdb_block_cache_hit_count;
            }
            self.sum_rocksdb_block_read_count += scan_detail.rocksdb_block_read_count;
            if scan_detail.rocksdb_block_read_count > self.max_rocksdb_block_read_count {
                self.max_rocksdb_block_read_count = scan_detail.rocksdb_block_read_count;
            }
            self.sum_rocksdb_block_read_byte += scan_detail.rocksdb_block_read_byte;
            if scan_detail.rocksdb_block_read_byte > self.max_rocksdb_block_read_byte {
                self.max_rocksdb_block_read_byte = scan_detail.rocksdb_block_read_byte;
            }
            let ia_stats = get_ia_remote_read_segment_stats(Some(scan_detail));
            self.sum_ia_remote_read_segment_count += ia_stats.count;
            if ia_stats.count > self.max_ia_remote_read_segment_count {
                self.max_ia_remote_read_segment_count = ia_stats.count;
            }
            self.sum_ia_remote_read_segment_size += ia_stats.bytes;
            if ia_stats.bytes > self.max_ia_remote_read_segment_size {
                self.max_ia_remote_read_segment_size = ia_stats.bytes;
            }
            self.sum_ia_remote_read_segment_wait_time += ia_stats.wait_time;
            if ia_stats.wait_time > self.max_ia_remote_read_segment_wait_time {
                self.max_ia_remote_read_segment_wait_time = ia_stats.wait_time;
            }
        }
        // Txn.
        if let Some(commit_details) = info.exec_detail.commit_detail.as_ref() {
            self.commit_count += 1;
            self.sum_prewrite_time += commit_details.prewrite_time;
            if commit_details.prewrite_time > self.max_prewrite_time {
                self.max_prewrite_time = commit_details.prewrite_time;
            }
            self.sum_commit_time += commit_details.commit_time;
            if commit_details.commit_time > self.max_commit_time {
                self.max_commit_time = commit_details.commit_time;
            }
            self.sum_get_commit_ts_time += commit_details.get_commit_ts_time;
            if commit_details.get_commit_ts_time > self.max_get_commit_ts_time {
                self.max_get_commit_ts_time = commit_details.get_commit_ts_time;
            }
            let resolve_lock_time = commit_details.resolve_lock.resolve_lock_time;
            self.sum_resolve_lock_time += resolve_lock_time;
            if resolve_lock_time > self.max_resolve_lock_time {
                self.max_resolve_lock_time = resolve_lock_time;
            }
            self.sum_local_latch_time += commit_details.local_latch_time;
            if commit_details.local_latch_time > self.max_local_latch_time {
                self.max_local_latch_time = commit_details.local_latch_time;
            }
            self.sum_write_keys += commit_details.write_keys;
            if commit_details.write_keys > self.max_write_keys {
                self.max_write_keys = commit_details.write_keys;
            }
            self.sum_write_size += commit_details.write_size;
            if commit_details.write_size > self.max_write_size {
                self.max_write_size = commit_details.write_size;
            }
            let prewrite_region_num = commit_details.prewrite_region_num;
            self.sum_prewrite_region_num += i64::from(prewrite_region_num);
            if prewrite_region_num > self.max_prewrite_region_num {
                self.max_prewrite_region_num = prewrite_region_num;
            }
            self.sum_txn_retry += commit_details.txn_retry;
            if commit_details.txn_retry > self.max_txn_retry {
                self.max_txn_retry = commit_details.txn_retry;
            }
            let commit_backoff_time = commit_details.commit_backoff_time;
            self.sum_commit_backoff_time += commit_backoff_time;
            if commit_backoff_time > self.max_commit_backoff_time {
                self.max_commit_backoff_time = commit_backoff_time;
            }
            self.sum_backoff_times += commit_details.prewrite_backoff_types.len() as i64;
            for backoff_type in &commit_details.prewrite_backoff_types {
                *self.backoff_types.entry(backoff_type.clone()).or_insert(0) += 1;
            }
            self.sum_backoff_times += commit_details.commit_backoff_types.len() as i64;
            for backoff_type in &commit_details.commit_backoff_types {
                *self.backoff_types.entry(backoff_type.clone()).or_insert(0) += 1;
            }
        }
        // Plan cache.
        if info.plan_in_cache {
            self.plan_in_cache = true;
            self.plan_cache_hits += 1;
        } else {
            self.plan_in_cache = false;
        }
        if !info.plan_cache_unqualified.is_empty() {
            self.plan_cache_unqualified_count += 1;
            self.plan_cache_unqualified_last_reason
                .clone_from(&info.plan_cache_unqualified);
        }
        // SPM.
        self.plan_in_binding = info.plan_in_binding;
        // Other.
        self.sum_affected_rows += info.stmt_ctx.affected_rows();
        self.sum_mem += info.mem_max;
        if info.mem_max > self.max_mem {
            self.max_mem = info.mem_max;
        }
        self.sum_mem_arbitration += info.mem_arbitration;
        if info.mem_arbitration > self.max_mem_arbitration {
            self.max_mem_arbitration = info.mem_arbitration;
        }
        self.sum_disk += info.disk_max;
        if info.disk_max > self.max_disk {
            self.max_disk = info.disk_max;
        }
        if info.start_time < self.first_seen {
            self.first_seen = info.start_time;
        }
        if self.last_seen < info.start_time {
            self.last_seen = info.start_time;
        }
        if info.exec_retry_count > 0 {
            self.exec_retry_count += info.exec_retry_count;
            self.exec_retry_time += info.exec_retry_time;
        }
        if info.result_rows > 0 {
            self.sum_result_rows += info.result_rows;
            if self.max_result_rows < info.result_rows {
                self.max_result_rows = info.result_rows;
            }
            if self.min_result_rows > info.result_rows {
                self.min_result_rows = info.result_rows;
            }
        } else {
            self.min_result_rows = 0;
        }
        if let Some(tikv) = info.tikv_exec_details.as_ref() {
            self.sum_kv_total += nanos_to_duration(tikv.wait_kv_resp_duration);
            self.sum_pd_total += nanos_to_duration(tikv.wait_pd_resp_duration);
            self.sum_backoff_total += nanos_to_duration(tikv.backoff_duration);
        }
        self.sum_write_sql_resp_total += info.write_sql_resp_duration;
        self.sum_tidb_cpu += info.cpu_usages.tidb_cpu_time;
        self.sum_tikv_cpu += info.cpu_usages.tikv_cpu_time;

        // Networks.
        self.network.add(info.tikv_exec_details.as_ref());
        // RU.
        self.ru.add(info.ru_detail.as_ref(), info.total_ru_v2);

        self.storage_kv = info.stmt_ctx.is_tikv.load(Ordering::SeqCst);
        self.storage_mpp = info.stmt_ctx.is_tiflash.load(Ordering::SeqCst);
    }

    /// Go `(*StmtRecord).Merge`: merges the statistics of another `StmtRecord`
    /// into this one.
    #[allow(clippy::too_many_lines)]
    pub fn merge(&mut self, other: &Self) {
        // User.
        for user in &other.auth_users {
            self.auth_users.insert(user.clone());
        }
        // ExecCount and SumWarnings.
        self.exec_count += other.exec_count;
        self.sum_warnings += other.sum_warnings;
        // Latency.
        self.sum_latency += other.sum_latency;
        if self.max_latency < other.max_latency {
            self.max_latency = other.max_latency;
        }
        if self.min_latency > other.min_latency {
            self.min_latency = other.min_latency;
        }
        self.sum_parse_latency += other.sum_parse_latency;
        if self.max_parse_latency < other.max_parse_latency {
            self.max_parse_latency = other.max_parse_latency;
        }
        self.sum_compile_latency += other.sum_compile_latency;
        if self.max_compile_latency < other.max_compile_latency {
            self.max_compile_latency = other.max_compile_latency;
        }
        // Coprocessor.
        self.sum_num_cop_tasks += other.sum_num_cop_tasks;
        if self.max_cop_process_time < other.max_cop_process_time {
            self.max_cop_process_time = other.max_cop_process_time;
            self.max_cop_process_address
                .clone_from(&other.max_cop_process_address);
        }
        if self.max_cop_wait_time < other.max_cop_wait_time {
            self.max_cop_wait_time = other.max_cop_wait_time;
            self.max_cop_wait_address
                .clone_from(&other.max_cop_wait_address);
        }
        // TiKV.
        self.sum_process_time += other.sum_process_time;
        if self.max_process_time < other.max_process_time {
            self.max_process_time = other.max_process_time;
        }
        self.sum_wait_time += other.sum_wait_time;
        if self.max_wait_time < other.max_wait_time {
            self.max_wait_time = other.max_wait_time;
        }
        self.sum_backoff_time += other.sum_backoff_time;
        if self.max_backoff_time < other.max_backoff_time {
            self.max_backoff_time = other.max_backoff_time;
        }
        self.sum_total_keys += other.sum_total_keys;
        if self.max_total_keys < other.max_total_keys {
            self.max_total_keys = other.max_total_keys;
        }
        self.sum_processed_keys += other.sum_processed_keys;
        if self.max_processed_keys < other.max_processed_keys {
            self.max_processed_keys = other.max_processed_keys;
        }
        self.sum_rocksdb_delete_skipped_count += other.sum_rocksdb_delete_skipped_count;
        if self.max_rocksdb_delete_skipped_count < other.max_rocksdb_delete_skipped_count {
            self.max_rocksdb_delete_skipped_count = other.max_rocksdb_delete_skipped_count;
        }
        self.sum_rocksdb_key_skipped_count += other.sum_rocksdb_key_skipped_count;
        if self.max_rocksdb_key_skipped_count < other.max_rocksdb_key_skipped_count {
            self.max_rocksdb_key_skipped_count = other.max_rocksdb_key_skipped_count;
        }
        self.sum_rocksdb_block_cache_hit_count += other.sum_rocksdb_block_cache_hit_count;
        if self.max_rocksdb_block_cache_hit_count < other.max_rocksdb_block_cache_hit_count {
            self.max_rocksdb_block_cache_hit_count = other.max_rocksdb_block_cache_hit_count;
        }
        self.sum_rocksdb_block_read_count += other.sum_rocksdb_block_read_count;
        if self.max_rocksdb_block_read_count < other.max_rocksdb_block_read_count {
            self.max_rocksdb_block_read_count = other.max_rocksdb_block_read_count;
        }
        self.sum_rocksdb_block_read_byte += other.sum_rocksdb_block_read_byte;
        if self.max_rocksdb_block_read_byte < other.max_rocksdb_block_read_byte {
            self.max_rocksdb_block_read_byte = other.max_rocksdb_block_read_byte;
        }
        self.sum_ia_remote_read_segment_count += other.sum_ia_remote_read_segment_count;
        if self.max_ia_remote_read_segment_count < other.max_ia_remote_read_segment_count {
            self.max_ia_remote_read_segment_count = other.max_ia_remote_read_segment_count;
        }
        self.sum_ia_remote_read_segment_size += other.sum_ia_remote_read_segment_size;
        if self.max_ia_remote_read_segment_size < other.max_ia_remote_read_segment_size {
            self.max_ia_remote_read_segment_size = other.max_ia_remote_read_segment_size;
        }
        self.sum_ia_remote_read_segment_wait_time += other.sum_ia_remote_read_segment_wait_time;
        if self.max_ia_remote_read_segment_wait_time < other.max_ia_remote_read_segment_wait_time {
            self.max_ia_remote_read_segment_wait_time = other.max_ia_remote_read_segment_wait_time;
        }
        // Txn.
        self.commit_count += other.commit_count;
        self.sum_prewrite_time += other.sum_prewrite_time;
        if self.max_prewrite_time < other.max_prewrite_time {
            self.max_prewrite_time = other.max_prewrite_time;
        }
        self.sum_commit_time += other.sum_commit_time;
        if self.max_commit_time < other.max_commit_time {
            self.max_commit_time = other.max_commit_time;
        }
        self.sum_get_commit_ts_time += other.sum_get_commit_ts_time;
        if self.max_get_commit_ts_time < other.max_get_commit_ts_time {
            self.max_get_commit_ts_time = other.max_get_commit_ts_time;
        }
        self.sum_commit_backoff_time += other.sum_commit_backoff_time;
        if self.max_commit_backoff_time < other.max_commit_backoff_time {
            self.max_commit_backoff_time = other.max_commit_backoff_time;
        }
        self.sum_resolve_lock_time += other.sum_resolve_lock_time;
        if self.max_resolve_lock_time < other.max_resolve_lock_time {
            self.max_resolve_lock_time = other.max_resolve_lock_time;
        }
        self.sum_local_latch_time += other.sum_local_latch_time;
        if self.max_local_latch_time < other.max_local_latch_time {
            self.max_local_latch_time = other.max_local_latch_time;
        }
        self.sum_write_keys += other.sum_write_keys;
        if self.max_write_keys < other.max_write_keys {
            self.max_write_keys = other.max_write_keys;
        }
        self.sum_write_size += other.sum_write_size;
        if self.max_write_size < other.max_write_size {
            self.max_write_size = other.max_write_size;
        }
        self.sum_prewrite_region_num += other.sum_prewrite_region_num;
        if self.max_prewrite_region_num < other.max_prewrite_region_num {
            self.max_prewrite_region_num = other.max_prewrite_region_num;
        }
        self.sum_txn_retry += other.sum_txn_retry;
        if self.max_txn_retry < other.max_txn_retry {
            self.max_txn_retry = other.max_txn_retry;
        }
        self.sum_backoff_times += other.sum_backoff_times;
        for (backoff_type, backoff_value) in &other.backoff_types {
            *self.backoff_types.entry(backoff_type.clone()).or_insert(0) += backoff_value;
        }
        // Plan cache.
        self.plan_cache_hits += other.plan_cache_hits;
        self.plan_cache_unqualified_count += other.plan_cache_unqualified_count;
        if !other.plan_cache_unqualified_last_reason.is_empty() {
            self.plan_cache_unqualified_last_reason
                .clone_from(&other.plan_cache_unqualified_last_reason);
        }
        // Other.
        self.sum_affected_rows += other.sum_affected_rows;
        self.sum_mem += other.sum_mem;
        if self.max_mem < other.max_mem {
            self.max_mem = other.max_mem;
        }
        self.sum_disk += other.sum_disk;
        if self.max_disk < other.max_disk {
            self.max_disk = other.max_disk;
        }
        if self.first_seen > other.first_seen {
            self.first_seen = other.first_seen;
        }
        if self.last_seen < other.last_seen {
            self.last_seen = other.last_seen;
        }
        self.exec_retry_count += other.exec_retry_count;
        self.exec_retry_time += other.exec_retry_time;
        self.sum_kv_total += other.sum_kv_total;
        self.sum_pd_total += other.sum_pd_total;
        self.sum_backoff_total += other.sum_backoff_total;
        self.sum_write_sql_resp_total += other.sum_write_sql_resp_total;
        self.sum_tidb_cpu += other.sum_tidb_cpu;
        self.sum_tikv_cpu += other.sum_tikv_cpu;
        self.sum_errors += other.sum_errors;
        self.ru.merge(&other.ru);
    }
}

/// Go `time.Duration(int64)` for non-negative nanosecond counts; negative
/// counts (which `time.Duration` allows and `Duration` does not) clamp to zero.
fn nanos_to_duration(nanos: i64) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(0))
}

/// Go `formatSQL`: truncates SQL to `maxSQLLength`.
///
/// Go slices raw bytes; this truncates at the nearest UTF-8 boundary at or
/// below the limit, and reports Go's byte length.
#[must_use]
pub fn format_sql(sql: &str) -> String {
    let max_sql_length = max_sql_length() as usize;
    let length = sql.len();
    if length > max_sql_length {
        let mut end = max_sql_length;
        while end > 0 && !sql.is_char_boundary(end) {
            end -= 1;
        }
        return format!("{}(len:{length})", &sql[..end]);
    }
    // Go calls strings.Clone so the result never pins the source buffer.
    sql.to_owned()
}

/// Go `maxSQLLength`: `GlobalStmtSummary.MaxSQLLength()`, or
/// [`DEFAULT_MAX_SQL_LENGTH`] while the global is unset.
#[must_use]
pub fn max_sql_length() -> u32 {
    crate::v2::stmtsummary::global_max_sql_length()
}

/* Go `v2/logger.go`'s marshalling carve-out — see this file's module header. */

/// Go `evictedStmtRecord`: embeds `*StmtRecord` and adds an `"evicted"` JSON
/// tag. Keeping the embedded pointer means the JSON field order matches
/// `StmtRecord` and parsers tolerant of the extra field work unchanged.
#[derive(Clone, Copy, Debug)]
pub struct EvictedStmtRecord<'a> {
    /// Go's embedded `*StmtRecord`.
    pub record: &'a StmtRecord,
    /// Go `Evicted`.
    pub evicted: bool,
}

/// Go `marshalStmtRecord`.
///
/// # Errors
///
/// Returns the serializer's error, as Go returns `json.Marshal`'s.
pub fn marshal_stmt_record(r: &StmtRecord) -> Result<Vec<u8>, serde_json::Error> {
    marshal_stmt_record_with_evicted(r, false)
}

/// Go `marshalEvictedStmtRecord`.
///
/// # Errors
///
/// Returns the serializer's error, as Go returns `json.Marshal`'s.
pub fn marshal_evicted_stmt_record(r: &StmtRecord) -> Result<Vec<u8>, serde_json::Error> {
    marshal_stmt_record_with_evicted(r, true)
}

/// Go `marshalStmtRecordWithEvicted`.
///
/// # Errors
///
/// Returns the serializer's error, as Go returns `json.Marshal`'s.
pub fn marshal_stmt_record_with_evicted(
    r: &StmtRecord,
    evicted: bool,
) -> Result<Vec<u8>, serde_json::Error> {
    let config = tidb_config::config_tree::config::get_global_config();
    let fields = config.get_keyspace_observability_stmt_log_fields();
    if fields.is_empty() {
        if evicted {
            return serde_json::to_vec(&RecordEnvelope {
                record: r,
                additional_fields: None,
                evicted: Some(true),
            });
        }
        return serde_json::to_vec(r);
    }
    serde_json::to_vec(&RecordEnvelope {
        record: r,
        additional_fields: Some(fields),
        evicted: if evicted { Some(true) } else { None },
    })
}

/// Go `stmtRecordWithAdditionalFields` / `evictedStmtRecordWithAdditionalFields`
/// / `evictedStmtRecord` collapsed into one shape: the optional fields are
/// emitted only in the combinations Go's three types cover.
struct RecordEnvelope<'a> {
    record: &'a StmtRecord,
    additional_fields: Option<&'a HashMap<String, String>>,
    evicted: Option<bool>,
}

impl Serialize for RecordEnvelope<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        self.record.serialize_entries(&mut map)?;
        if let Some(fields) = self.additional_fields {
            // Go marshals a map with sorted keys.
            let sorted: BTreeMap<&str, &str> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            map.serialize_entry("additional_fields", &sorted)?;
        }
        if let Some(evicted) = self.evicted {
            map.serialize_entry("evicted", &evicted)?;
        }
        map.end()
    }
}

impl Serialize for StmtRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        self.serialize_entries(&mut map)?;
        map.end()
    }
}

/// Go's `time.Duration` marshals as its nanosecond count.
fn nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// Go's `time.Time` marshals as RFC 3339 with a trailing-zero-trimmed
/// nanosecond fraction (`time.RFC3339Nano`), dropping the dot entirely when the
/// fraction is zero. `chrono`'s own RFC 3339 formatters only emit 0, 3, 6, or 9
/// fractional digits, so the trimming is done here.
fn go_time(t: DateTime<Utc>) -> String {
    let subsec = t.timestamp_subsec_nanos();
    if subsec == 0 {
        return t.to_rfc3339_opts(SecondsFormat::Secs, true);
    }
    let mut frac = format!("{subsec:09}");
    while frac.ends_with('0') {
        frac.pop();
    }
    format!("{}.{frac}Z", t.format("%Y-%m-%dT%H:%M:%S"))
}

impl StmtRecord {
    /// Emits Go's JSON field order for `StmtRecord`, including the fields Go
    /// flattens out of the two embedded summaries.
    #[allow(clippy::too_many_lines)]
    fn serialize_entries<M: SerializeMap>(&self, map: &mut M) -> Result<(), M::Error> {
        map.serialize_entry("begin", &self.begin)?;
        map.serialize_entry("end", &self.end)?;
        map.serialize_entry("schema_name", &self.schema_name)?;
        map.serialize_entry("digest", &self.digest)?;
        map.serialize_entry("plan_digest", &self.plan_digest)?;
        map.serialize_entry("stmt_type", &self.stmt_type)?;
        map.serialize_entry("normalized_sql", &self.normalized_sql)?;
        map.serialize_entry("table_names", &self.table_names)?;
        map.serialize_entry("is_internal", &self.is_internal)?;
        map.serialize_entry("binding_sql", &self.binding_sql)?;
        map.serialize_entry("binding_digest", &self.binding_digest)?;
        map.serialize_entry("sample_sql", &self.sample_sql)?;
        map.serialize_entry("charset", &self.charset)?;
        map.serialize_entry("collation", &self.collation)?;
        map.serialize_entry("prev_sql", &self.prev_sql)?;
        map.serialize_entry("sample_plan", &self.sample_plan)?;
        map.serialize_entry("sample_binary_plan", &self.sample_binary_plan)?;
        map.serialize_entry("plan_hint", &self.plan_hint)?;
        // Go marshals a nil slice as `null`.
        if self.index_names.is_empty() {
            map.serialize_entry("index_names", &Option::<&[String]>::None)?;
        } else {
            map.serialize_entry("index_names", &self.index_names)?;
        }
        map.serialize_entry("exec_count", &self.exec_count)?;
        map.serialize_entry("sum_errors", &self.sum_errors)?;
        map.serialize_entry("sum_warnings", &self.sum_warnings)?;
        map.serialize_entry("sum_latency", &nanos(self.sum_latency))?;
        map.serialize_entry("max_latency", &nanos(self.max_latency))?;
        map.serialize_entry("min_latency", &nanos(self.min_latency))?;
        map.serialize_entry("sum_parse_latency", &nanos(self.sum_parse_latency))?;
        map.serialize_entry("max_parse_latency", &nanos(self.max_parse_latency))?;
        map.serialize_entry("sum_compile_latency", &nanos(self.sum_compile_latency))?;
        map.serialize_entry("max_compile_latency", &nanos(self.max_compile_latency))?;
        map.serialize_entry("sum_num_cop_tasks", &self.sum_num_cop_tasks)?;
        map.serialize_entry("max_cop_process_time", &nanos(self.max_cop_process_time))?;
        map.serialize_entry("max_cop_process_address", &self.max_cop_process_address)?;
        map.serialize_entry("max_cop_wait_time", &nanos(self.max_cop_wait_time))?;
        map.serialize_entry("max_cop_wait_address", &self.max_cop_wait_address)?;
        map.serialize_entry("sum_process_time", &nanos(self.sum_process_time))?;
        map.serialize_entry("max_process_time", &nanos(self.max_process_time))?;
        map.serialize_entry("sum_wait_time", &nanos(self.sum_wait_time))?;
        map.serialize_entry("max_wait_time", &nanos(self.max_wait_time))?;
        map.serialize_entry("sum_backoff_time", &nanos(self.sum_backoff_time))?;
        map.serialize_entry("max_backoff_time", &nanos(self.max_backoff_time))?;
        map.serialize_entry("sum_total_keys", &self.sum_total_keys)?;
        map.serialize_entry("max_total_keys", &self.max_total_keys)?;
        map.serialize_entry("sum_processed_keys", &self.sum_processed_keys)?;
        map.serialize_entry("max_processed_keys", &self.max_processed_keys)?;
        map.serialize_entry(
            "sum_rocksdb_delete_skipped_count",
            &self.sum_rocksdb_delete_skipped_count,
        )?;
        map.serialize_entry(
            "max_rocksdb_delete_skipped_count",
            &self.max_rocksdb_delete_skipped_count,
        )?;
        map.serialize_entry(
            "sum_rocksdb_key_skipped_count",
            &self.sum_rocksdb_key_skipped_count,
        )?;
        map.serialize_entry(
            "max_rocksdb_key_skipped_count",
            &self.max_rocksdb_key_skipped_count,
        )?;
        map.serialize_entry(
            "sum_rocksdb_block_cache_hit_count",
            &self.sum_rocksdb_block_cache_hit_count,
        )?;
        map.serialize_entry(
            "max_rocksdb_block_cache_hit_count",
            &self.max_rocksdb_block_cache_hit_count,
        )?;
        map.serialize_entry(
            "sum_rocksdb_block_read_count",
            &self.sum_rocksdb_block_read_count,
        )?;
        map.serialize_entry(
            "max_rocksdb_block_read_count",
            &self.max_rocksdb_block_read_count,
        )?;
        map.serialize_entry(
            "sum_rocksdb_block_read_byte",
            &self.sum_rocksdb_block_read_byte,
        )?;
        map.serialize_entry(
            "max_rocksdb_block_read_byte",
            &self.max_rocksdb_block_read_byte,
        )?;
        map.serialize_entry(
            "sum_ia_remote_read_segment_count",
            &self.sum_ia_remote_read_segment_count,
        )?;
        map.serialize_entry(
            "max_ia_remote_read_segment_count",
            &self.max_ia_remote_read_segment_count,
        )?;
        map.serialize_entry(
            "sum_ia_remote_read_segment_size",
            &self.sum_ia_remote_read_segment_size,
        )?;
        map.serialize_entry(
            "max_ia_remote_read_segment_size",
            &self.max_ia_remote_read_segment_size,
        )?;
        map.serialize_entry(
            "sum_ia_remote_read_segment_wait_time",
            &nanos(self.sum_ia_remote_read_segment_wait_time),
        )?;
        map.serialize_entry(
            "max_ia_remote_read_segment_wait_time",
            &nanos(self.max_ia_remote_read_segment_wait_time),
        )?;
        map.serialize_entry("commit_count", &self.commit_count)?;
        map.serialize_entry(
            "sum_get_commit_ts_time",
            &nanos(self.sum_get_commit_ts_time),
        )?;
        map.serialize_entry(
            "max_get_commit_ts_time",
            &nanos(self.max_get_commit_ts_time),
        )?;
        map.serialize_entry("sum_prewrite_time", &nanos(self.sum_prewrite_time))?;
        map.serialize_entry("max_prewrite_time", &nanos(self.max_prewrite_time))?;
        map.serialize_entry("sum_commit_time", &nanos(self.sum_commit_time))?;
        map.serialize_entry("max_commit_time", &nanos(self.max_commit_time))?;
        map.serialize_entry("sum_local_latch_time", &nanos(self.sum_local_latch_time))?;
        map.serialize_entry("max_local_latch_time", &nanos(self.max_local_latch_time))?;
        map.serialize_entry("sum_commit_backoff_time", &self.sum_commit_backoff_time)?;
        map.serialize_entry("max_commit_backoff_time", &self.max_commit_backoff_time)?;
        map.serialize_entry("sum_resolve_lock_time", &self.sum_resolve_lock_time)?;
        map.serialize_entry("max_resolve_lock_time", &self.max_resolve_lock_time)?;
        map.serialize_entry("sum_write_keys", &self.sum_write_keys)?;
        map.serialize_entry("max_write_keys", &self.max_write_keys)?;
        map.serialize_entry("sum_write_size", &self.sum_write_size)?;
        map.serialize_entry("max_write_size", &self.max_write_size)?;
        map.serialize_entry("sum_prewrite_region_num", &self.sum_prewrite_region_num)?;
        map.serialize_entry("max_prewrite_region_num", &self.max_prewrite_region_num)?;
        map.serialize_entry("sum_txn_retry", &self.sum_txn_retry)?;
        map.serialize_entry("max_txn_retry", &self.max_txn_retry)?;
        map.serialize_entry("sum_backoff_times", &self.sum_backoff_times)?;
        // Go marshals maps with sorted keys; a nil map marshals as `null`.
        let backoff_types: BTreeMap<&str, i64> = self
            .backoff_types
            .iter()
            .map(|(k, v)| (k.as_str(), *v))
            .collect();
        map.serialize_entry("backoff_types", &backoff_types)?;
        // Go's `map[string]struct{}` marshals each value as `{}`.
        let auth_users: BTreeMap<&str, EmptyStruct> = self
            .auth_users
            .iter()
            .map(|user| (user.as_str(), EmptyStruct))
            .collect();
        map.serialize_entry("auth_users", &auth_users)?;
        map.serialize_entry("sum_mem", &self.sum_mem)?;
        map.serialize_entry("max_mem", &self.max_mem)?;
        map.serialize_entry("sum_disk", &self.sum_disk)?;
        map.serialize_entry("max_disk", &self.max_disk)?;
        map.serialize_entry("sum_affected_rows", &self.sum_affected_rows)?;
        map.serialize_entry("sum_kv_total", &nanos(self.sum_kv_total))?;
        map.serialize_entry("sum_pd_total", &nanos(self.sum_pd_total))?;
        map.serialize_entry("sum_backoff_total", &nanos(self.sum_backoff_total))?;
        map.serialize_entry(
            "sum_write_sql_resp_total",
            &nanos(self.sum_write_sql_resp_total),
        )?;
        map.serialize_entry("sum_tidb_cpu", &nanos(self.sum_tidb_cpu))?;
        map.serialize_entry("sum_tikv_cpu", &nanos(self.sum_tikv_cpu))?;
        map.serialize_entry("sum_result_rows", &self.sum_result_rows)?;
        map.serialize_entry("max_result_rows", &self.max_result_rows)?;
        map.serialize_entry("min_result_rows", &self.min_result_rows)?;
        map.serialize_entry("prepared", &self.prepared)?;
        map.serialize_entry("first_seen", &go_time(self.first_seen))?;
        map.serialize_entry("last_seen", &go_time(self.last_seen))?;
        map.serialize_entry("plan_in_cache", &self.plan_in_cache)?;
        map.serialize_entry("plan_cache_hits", &self.plan_cache_hits)?;
        map.serialize_entry("plan_in_binding", &self.plan_in_binding)?;
        map.serialize_entry("exec_retry_count", &self.exec_retry_count)?;
        map.serialize_entry("exec_retry_time", &nanos(self.exec_retry_time))?;
        if !self.keyspace_name.is_empty() {
            map.serialize_entry("keyspace_name", &self.keyspace_name)?;
        }
        if self.keyspace_id != 0 {
            map.serialize_entry("keyspace_id", &self.keyspace_id)?;
        }
        map.serialize_entry("resource_group_name", &self.resource_group_name)?;
        // Go's embedded StmtRUSummary.
        map.serialize_entry("sum_rru", &self.ru.sum_rru)?;
        map.serialize_entry("sum_wru", &self.ru.sum_wru)?;
        map.serialize_entry("sum_ru_wait_duration", &nanos(self.ru.sum_ru_wait_duration))?;
        map.serialize_entry("max_rru", &self.ru.max_rru)?;
        map.serialize_entry("max_wru", &self.ru.max_wru)?;
        map.serialize_entry("max_ru_wait_duration", &nanos(self.ru.max_ru_wait_duration))?;
        map.serialize_entry("sum_ruv2", &self.ru.sum_ru_v2)?;
        map.serialize_entry("max_ruv2", &self.ru.max_ru_v2)?;
        map.serialize_entry(
            "plan_cache_unqualified_count",
            &self.plan_cache_unqualified_count,
        )?;
        map.serialize_entry(
            "plan_cache_unqualified_last_reason",
            &self.plan_cache_unqualified_last_reason,
        )?;
        map.serialize_entry("sum_mem_arbitration", &self.sum_mem_arbitration)?;
        map.serialize_entry("max_mem_arbitration", &self.max_mem_arbitration)?;
        // Go's embedded StmtNetworkTrafficSummary. Its `sent` counters carry
        // Go's `send` spelling in the JSON tag.
        map.serialize_entry(
            "unpacked_bytes_send_tikv_total",
            &self.network.unpacked_bytes_sent_tikv_total,
        )?;
        map.serialize_entry(
            "unpacked_bytes_received_tikv_total",
            &self.network.unpacked_bytes_received_tikv_total,
        )?;
        map.serialize_entry(
            "unpacked_bytes_send_tikv_cross_zone",
            &self.network.unpacked_bytes_sent_tikv_cross_zone,
        )?;
        map.serialize_entry(
            "unpacked_bytes_received_tikv_cross_zone",
            &self.network.unpacked_bytes_received_tikv_cross_zone,
        )?;
        map.serialize_entry(
            "unpacked_bytes_send_tiflash_total",
            &self.network.unpacked_bytes_sent_tiflash_total,
        )?;
        map.serialize_entry(
            "unpacked_bytes_received_tiflash_total",
            &self.network.unpacked_bytes_received_tiflash_total,
        )?;
        map.serialize_entry(
            "unpacked_bytes_send_tiflash_cross_zone",
            &self.network.unpacked_bytes_sent_tiflash_cross_zone,
        )?;
        map.serialize_entry(
            "unpacked_bytes_received_tiflash_cross_zone",
            &self.network.unpacked_bytes_received_tiflash_cross_zone,
        )?;
        map.serialize_entry("storage_kv", &self.storage_kv)?;
        map.serialize_entry("storage_mpp", &self.storage_mpp)
    }
}

/// Go's `struct{}` value in `map[string]struct{}`, which marshals as `{}`.
struct EmptyStruct;

impl Serialize for EmptyStruct {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_map(Some(0))?.end()
    }
}

/* Go `record.go`'s production test fixture. */

/// Go `mockLazyInfo`.
#[derive(Clone, Copy, Debug, Default)]
pub struct MockLazyInfo;

impl StmtExecLazyInfo for MockLazyInfo {
    fn original_sql(&self) -> String {
        String::new()
    }

    fn encoded_plan(&self) -> Result<(String, String), EncodedPlanError> {
        Ok((String::new(), String::new()))
    }

    fn binary_plan(&self) -> String {
        String::new()
    }

    fn plan_digest(&self) -> String {
        String::new()
    }

    fn binding_sql_and_digest(&self) -> (String, String) {
        (String::new(), String::new())
    }
}

/// Go `GenerateStmtExecInfo4Test`: generates a new `StmtExecInfo` for testing
/// purposes.
///
/// Go's `util.NewRUDetailsWith(1.2, 3.4, 2*time.Millisecond)` and
/// `&util.ExecDetails{}` arrive here as the already-loaded snapshots v1's
/// `StmtExecInfo` carries.
#[must_use]
pub fn generate_stmt_exec_info_4_test(digest: &str) -> StmtExecInfo {
    let tables = vec![
        TableEntry {
            db: "db1".to_owned(),
            table: "tb1".to_owned(),
        },
        TableEntry {
            db: "db2".to_owned(),
            table: "tb2".to_owned(),
        },
    ];
    let indexes = vec!["a".to_owned()];
    let mut sc = StmtSummaryStmtCtx::new();
    sc.stmt_type = "Select".to_owned();
    sc.tables = tables;
    sc.index_names = indexes;
    sc.add_affected_rows(10000);

    StmtExecInfo {
        schema_name: "schema_name".to_owned(),
        charset: String::new(),
        collation: String::new(),
        normalized_sql: "normalized_sql".to_owned(),
        digest: digest.to_owned(),
        prev_sql: String::new(),
        prev_sql_digest: String::new(),
        plan_digest: "plan_digest".to_owned(),
        user: "user".to_owned(),
        total_latency: Duration::from_nanos(10000),
        parse_latency: Duration::from_nanos(100),
        compile_latency: Duration::from_nanos(1000),
        stmt_ctx: Arc::new(sc),
        cop_tasks: Some(CopTasksSummary {
            num_cop_tasks: 10,
            max_process_address: "127".to_owned(),
            max_process_time: Duration::from_nanos(15000),
            max_wait_address: "128".to_owned(),
            max_wait_time: Duration::from_nanos(1500),
            ..CopTasksSummary::default()
        }),
        exec_detail: ExecDetails {
            request_count: 10,
            commit_detail: Some(CommitDetails {
                get_commit_ts_time: Duration::from_nanos(100),
                prewrite_time: Duration::from_nanos(10000),
                commit_time: Duration::from_nanos(1000),
                local_latch_time: Duration::from_nanos(10),
                commit_backoff_time: 200,
                prewrite_backoff_types: vec!["txnlock".to_owned()],
                commit_backoff_types: Vec::new(),
                write_keys: 20000,
                write_size: 200_000,
                prewrite_region_num: 20,
                txn_retry: 2,
                resolve_lock: ResolveLockDetail {
                    resolve_lock_time: 2000,
                },
                ..CommitDetails::default()
            }),
            cop_exec_details: CopExecDetails {
                backoff_time: Duration::from_nanos(80),
                scan_detail: Some(ScanDetail {
                    total_keys: 1000,
                    processed_keys: 500,
                    rocksdb_delete_skipped_count: 100,
                    rocksdb_key_skipped_count: 10,
                    rocksdb_block_cache_hit_count: 10,
                    rocksdb_block_read_count: 10,
                    rocksdb_block_read_byte: 1000,
                    ..ScanDetail::default()
                }),
                time_detail: TimeDetail {
                    process_time: Duration::from_nanos(500),
                    wait_time: Duration::from_nanos(50),
                    ..TimeDetail::default()
                },
                callee_address: "129".to_owned(),
            },
            ..ExecDetails::default()
        },
        mem_max: 10000,
        mem_arbitration: 22222.0,
        disk_max: 10000,
        start_time: Utc
            .with_ymd_and_hms(2019, 1, 1, 10, 10, 10)
            .unwrap()
            .checked_add_signed(chrono::TimeDelta::nanoseconds(10))
            .unwrap(),
        is_internal: false,
        succeed: true,
        plan_in_cache: false,
        plan_in_binding: false,
        exec_retry_count: 0,
        exec_retry_time: Duration::ZERO,
        write_sql_resp_duration: Duration::ZERO,
        result_rows: 0,
        tikv_exec_details: Some(TikvExecDetailsSnapshot::default()),
        prepared: false,
        keyspace_name: "keyspace_a".to_owned(),
        keyspace_id: 1,
        resource_group_name: "rg1".to_owned(),
        ru_detail: Some(RuDetailsSnapshot {
            rru: 1.2,
            wru: 3.4,
            ru_wait_duration: Duration::from_millis(2),
            ..RuDetailsSnapshot::default()
        }),
        total_ru_v2: 12345.0,
        cpu_usages: CpuUsages {
            tidb_cpu_time: Duration::from_nanos(20),
            tikv_cpu_time: Duration::from_nanos(10000),
        },
        plan_cache_unqualified: String::new(),
        lazy_info: Arc::new(MockLazyInfo),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;

    use tidb_config::config_tree::config::{get_global_config, store_global_config, update_global};
    use tidb_config::config_tree::Config;
    use tidb_config::keyspace_observability::{KeyspaceObservability, KeyspaceObservabilityField};

    use super::*;

    /// Serializes a `StmtRecord` (or an evicted one) and parses the result back
    /// into a JSON object, matching Go's `json.Unmarshal` into `map[string]any`.
    fn marshal_to_object(bytes: &[u8]) -> serde_json::Map<String, serde_json::Value> {
        match serde_json::from_slice(bytes).unwrap() {
            serde_json::Value::Object(map) => map,
            other => panic!("expected a JSON object, got {other:?}"),
        }
    }

    /// Go `TestStmtRecord`.
    ///
    /// Go's `config.RestoreFunc()` / `config.UpdateGlobal` becomes a snapshot
    /// of the global config restored at the end of the test.
    #[test]
    fn test_stmt_record() {
        let mut info = generate_stmt_exec_info_4_test("digest1");
        info.exec_detail
            .cop_exec_details
            .scan_detail
            .as_mut()
            .unwrap()
            .ia_remote_read_segment_count = 3;
        let mut record1 = new_stmt_record(&info);
        assert_eq!(record1.schema_name, info.schema_name);
        assert_eq!(record1.digest, info.digest);
        assert_eq!(record1.plan_digest, info.plan_digest);
        assert_eq!(record1.stmt_type, info.stmt_ctx.stmt_type);
        assert_eq!(record1.normalized_sql, info.normalized_sql);
        assert_eq!(record1.table_names, "db1.tb1,db2.tb2");
        assert_eq!(record1.is_internal, info.is_internal);
        assert_eq!(
            record1.sample_sql,
            format_sql(&info.lazy_info.original_sql())
        );
        let (binding_sql, binding_digest) = info.lazy_info.binding_sql_and_digest();
        assert_eq!(record1.binding_sql, binding_sql);
        assert_eq!(record1.binding_digest, binding_digest);
        assert_eq!(record1.charset, info.charset);
        assert_eq!(record1.collation, info.collation);
        assert_eq!(record1.prev_sql, info.prev_sql);
        assert_eq!(record1.index_names, info.stmt_ctx.index_names);
        assert_eq!(record1.min_latency, info.total_latency);
        assert_eq!(record1.prepared, info.prepared);
        assert_eq!(record1.first_seen, info.start_time);
        assert_eq!(record1.last_seen, info.start_time);
        assert_eq!(record1.keyspace_name, info.keyspace_name);
        assert_eq!(record1.keyspace_id, info.keyspace_id);
        assert!(record1.auth_users.is_empty());
        assert_eq!(record1.exec_count, 0);
        assert_eq!(record1.sum_latency, Duration::ZERO);
        assert_eq!(record1.max_latency, Duration::ZERO);
        assert_eq!(record1.resource_group_name, info.resource_group_name);

        record1.add(&info);
        assert_eq!(record1.auth_users.len(), 1);
        assert!(record1.auth_users.contains("user"));
        assert_eq!(record1.exec_count, 1);
        assert_eq!(record1.sum_latency, info.total_latency);
        assert_eq!(record1.max_latency, info.total_latency);
        assert_eq!(record1.min_latency, info.total_latency);
        let ru = info.ru_detail.unwrap();
        assert!((record1.ru.max_rru - ru.rru).abs() < f64::EPSILON);
        assert!((record1.ru.sum_rru - ru.rru).abs() < f64::EPSILON);
        assert!((record1.ru.max_wru - ru.wru).abs() < f64::EPSILON);
        assert!((record1.ru.sum_wru - ru.wru).abs() < f64::EPSILON);
        assert_eq!(record1.ru.max_ru_wait_duration, ru.ru_wait_duration);
        assert_eq!(record1.ru.sum_ru_wait_duration, ru.ru_wait_duration);
        assert!((record1.ru.max_ru_v2 - info.total_ru_v2).abs() < f64::EPSILON);
        assert!((record1.ru.sum_ru_v2 - info.total_ru_v2).abs() < f64::EPSILON);
        assert_eq!(record1.sum_tidb_cpu, info.cpu_usages.tidb_cpu_time);
        assert_eq!(record1.sum_tikv_cpu, info.cpu_usages.tikv_cpu_time);
        assert_eq!(record1.sum_ia_remote_read_segment_count, 3);
        assert_eq!(record1.max_ia_remote_read_segment_count, 3);

        let mut record2 = new_stmt_record(&info);
        record2.add(&info);
        record2.merge(&record1);
        assert_eq!(record2.auth_users.len(), 1);
        assert!(record2.auth_users.contains("user"));
        assert_eq!(record2.exec_count, 2);
        assert_eq!(record2.sum_latency, info.total_latency * 2);
        assert_eq!(record2.max_latency, info.total_latency);
        assert_eq!(record2.min_latency, info.total_latency);
        assert!((record2.ru.sum_rru - ru.rru * 2.0).abs() < f64::EPSILON);
        assert!((record2.ru.sum_wru - ru.wru * 2.0).abs() < f64::EPSILON);
        assert_eq!(record2.ru.sum_ru_wait_duration, ru.ru_wait_duration * 2);
        assert!((record2.ru.sum_ru_v2 - info.total_ru_v2 * 2.0).abs() < f64::EPSILON);
        assert_eq!(record2.sum_tidb_cpu, info.cpu_usages.tidb_cpu_time * 2);
        assert_eq!(record2.sum_tikv_cpu, info.cpu_usages.tikv_cpu_time * 2);
        assert_eq!(record2.sum_ia_remote_read_segment_count, 6);
        assert_eq!(record2.max_ia_remote_read_segment_count, 3);

        let restore: Config = get_global_config();
        update_global(|conf| {
            conf.keyspace_observability = KeyspaceObservability {
                fields: vec![KeyspaceObservabilityField {
                    source: "meta_a".to_owned(),
                    stmt_log_field: "stmt_meta_a".to_owned(),
                    ..KeyspaceObservabilityField::default()
                }],
            };
            let mut values = StdHashMap::new();
            values.insert("meta_a".to_owned(), "value_a".to_owned());
            conf.resolve_keyspace_observability(&values).unwrap();
        });

        let bytes = marshal_stmt_record(&record2).unwrap();
        let items = marshal_to_object(&bytes);
        assert_eq!(
            items["additional_fields"],
            serde_json::json!({"stmt_meta_a": "value_a"})
        );
        assert_eq!(items["digest"], serde_json::json!(record2.digest));
        assert!(items.contains_key("sum_ia_remote_read_segment_count"));
        assert!(items.contains_key("max_ia_remote_read_segment_count"));
        assert!(!items.contains_key("sum_ia_read_segment_count"));
        assert!(!items.contains_key("max_ia_read_segment_count"));

        let bytes = marshal_evicted_stmt_record(&record2).unwrap();
        let items = marshal_to_object(&bytes);
        assert_eq!(
            items["additional_fields"],
            serde_json::json!({"stmt_meta_a": "value_a"})
        );
        assert_eq!(items["evicted"], serde_json::json!(true));
        assert_eq!(items["digest"], serde_json::json!(record2.digest));

        store_global_config(restore);
    }
}
