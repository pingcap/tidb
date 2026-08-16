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

//! Go `pkg/util/stmtsummary/statement_summary.go`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use tidb_exec::exec_details::{get_ia_remote_read_segment_stats, ExecDetails};
use tidb_exec::slow_log_format::{RuDetailsSnapshot, TikvExecDetailsSnapshot};
use tidb_kvcache::{CacheKey, InvalidCapacity, SimpleLruCache};
use tidb_util::plancodec::{BINARY_PLAN_DISCARDED_ENCODED, PLAN_DISCARDED_ENCODED};
use tidb_util::ppcpuusage::CpuUsages;

use crate::evicted::StmtSummaryByDigestEvicted;

/// Go `MaxEncodedPlanSizeInBytes`: the upper limit of the size of the plan and
/// the binary plan in the stmt summary. Go declares it as a mutable package
/// variable, so it stays writable here.
pub static MAX_ENCODED_PLAN_SIZE_IN_BYTES: AtomicUsize = AtomicUsize::new(1024 * 1024);

/// Go `StmtSummaryByDigestMap`: a global map containing all statement
/// summaries.
pub static STMT_SUMMARY_BY_DIGEST_MAP: LazyLock<StmtSummaryByDigestMap> =
    LazyLock::new(StmtSummaryByDigestMap::new);

/// Go `StmtDigestKey`: the key for `stmtSummaryByDigestMap.summaryMap`.
///
/// Go's `StmtDigestKeyPool` (`sync.Pool`) is dropped; keys are allocated per
/// call.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StmtDigestKey {
    /// Go `StmtDigestKey.hash`: the hash value of this object.
    hash: Vec<u8>,
}

impl StmtDigestKey {
    /// Returns an empty key, matching Go's `&StmtDigestKey{}`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `(*StmtDigestKey).Init`: initializes the hash key.
    ///
    /// When user is empty (`group_by_user` disabled), the hash is
    /// byte-identical to the pre-user-dimension layout. When user is
    /// non-empty, the hash appends a length-prefixed user segment after
    /// `resource_group_name` so the boundary is unambiguous and pairs like
    /// `("rg", "alice")` and `("rga", "lice")` cannot collide.
    pub fn init(
        &mut self,
        schema_name: &str,
        digest: &str,
        prev_digest: &str,
        plan_digest: &str,
        resource_group_name: &str,
        user: &str,
    ) {
        let mut length = schema_name.len()
            + digest.len()
            + prev_digest.len()
            + plan_digest.len()
            + resource_group_name.len()
            + user.len();
        if !user.is_empty() {
            length += 4;
        }
        if self.hash.capacity() < length {
            self.hash = Vec::with_capacity(length);
        } else {
            self.hash.clear();
        }
        self.hash.extend_from_slice(digest.as_bytes());
        self.hash.extend_from_slice(schema_name.as_bytes());
        self.hash.extend_from_slice(prev_digest.as_bytes());
        self.hash.extend_from_slice(plan_digest.as_bytes());
        self.hash.extend_from_slice(resource_group_name.as_bytes());
        if !user.is_empty() {
            let user_len = u32::try_from(user.len()).unwrap_or(u32::MAX);
            self.hash.extend_from_slice(&user_len.to_be_bytes());
            self.hash.extend_from_slice(user.as_bytes());
        }
    }

    /// Go `(*StmtDigestKey).Hash`: implements `SimpleLRUCache.Key`.
    ///
    /// Only when the current SQL is `commit` is `prevSQL` recorded; otherwise
    /// `prevSQL` is empty. `prevSQL` is included in the key to distinguish
    /// different transactions.
    #[must_use]
    pub fn hash(&self) -> &[u8] {
        &self.hash
    }
}

impl CacheKey for StmtDigestKey {
    fn hash_bytes(&self) -> &[u8] {
        &self.hash
    }
}

/// Go `stmtctx.TableEntry`: one `db`.`table` pair recorded on the statement
/// context.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableEntry {
    /// Go `TableEntry.DB`.
    pub db: String,
    /// Go `TableEntry.Table`.
    pub table: String,
}

/// Narrowing of Go `*stmtctx.StatementContext` to the fields
/// `statement_summary.go` reads. Go shares one context pointer across several
/// `StmtExecInfo` values and mutates it between `AddStatement` calls, so the
/// mutable counters stay behind atomics and the value is held in an `Arc`.
#[derive(Debug, Default)]
pub struct StmtSummaryStmtCtx {
    /// Go `StatementContext.StmtType`.
    pub stmt_type: String,
    /// Go `StatementContext.Tables`.
    pub tables: Vec<TableEntry>,
    /// Go `StatementContext.IndexNames`.
    pub index_names: Vec<String>,
    /// Go `StatementContext.IsTiKV`.
    pub is_tikv: AtomicBool,
    /// Go `StatementContext.IsTiFlash`.
    pub is_tiflash: AtomicBool,
    affected_rows: AtomicU64,
    warning_count: AtomicU32,
}

impl StmtSummaryStmtCtx {
    /// Go `stmtctx.NewStmtCtx`, restricted to this narrowing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `(*StatementContext).AddAffectedRows`.
    pub fn add_affected_rows(&self, rows: u64) {
        self.affected_rows.fetch_add(rows, Ordering::SeqCst);
    }

    /// Go `(*StatementContext).AffectedRows`.
    #[must_use]
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows.load(Ordering::SeqCst)
    }

    /// Sets Go's warning count, which `StatementContext` derives from its
    /// warning slice.
    pub fn set_warning_count(&self, count: u32) {
        self.warning_count.store(count, Ordering::SeqCst);
    }

    /// Go `(*StatementContext).WarningCount`.
    #[must_use]
    pub fn warning_count(&self) -> u32 {
        self.warning_count.load(Ordering::SeqCst)
    }
}

/// Go `execdetails.CopTasksSummary`: the coprocessor-task rollup
/// `stmtSummaryStats.add` reads. Declared here because `tidb-exec` does not
/// carry it yet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopTasksSummary {
    /// Go `CopTasksSummary.NumCopTasks` (`int`).
    pub num_cop_tasks: i64,
    /// Go `CopTasksSummary.MaxProcessAddress`.
    pub max_process_address: String,
    /// Go `CopTasksSummary.MaxProcessTime`.
    pub max_process_time: Duration,
    /// Go `CopTasksSummary.TotProcessTime`.
    pub tot_process_time: Duration,
    /// Go `CopTasksSummary.MaxWaitAddress`.
    pub max_wait_address: String,
    /// Go `CopTasksSummary.MaxWaitTime`.
    pub max_wait_time: Duration,
    /// Go `CopTasksSummary.TotWaitTime`.
    pub tot_wait_time: Duration,
}

/// The error Go returns as the third (`any`) result of
/// `StmtExecLazyInfo.GetEncodedPlan`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedPlanError(pub String);

/// Go `StmtExecLazyInfo`: the interface for getting lazy information for
/// `StmtExecInfo`.
pub trait StmtExecLazyInfo: Send + Sync {
    /// Go `GetOriginalSQL`.
    fn original_sql(&self) -> String;
    /// Go `GetEncodedPlan`, returning `(plan, hint)` or Go's error result.
    fn encoded_plan(&self) -> Result<(String, String), EncodedPlanError>;
    /// Go `GetBinaryPlan`.
    fn binary_plan(&self) -> String;
    /// Go `GetPlanDigest`.
    fn plan_digest(&self) -> String;
    /// Go `GetBindingSQLAndDigest`.
    fn binding_sql_and_digest(&self) -> (String, String);
}

/// Go `StmtExecInfo`: records execution information of each statement.
pub struct StmtExecInfo {
    /// Go `StmtExecInfo.SchemaName`.
    pub schema_name: String,
    /// Go `StmtExecInfo.Charset`.
    pub charset: String,
    /// Go `StmtExecInfo.Collation`.
    pub collation: String,
    /// Go `StmtExecInfo.NormalizedSQL`.
    pub normalized_sql: String,
    /// Go `StmtExecInfo.Digest`.
    pub digest: String,
    /// Go `StmtExecInfo.PrevSQL`.
    pub prev_sql: String,
    /// Go `StmtExecInfo.PrevSQLDigest`.
    pub prev_sql_digest: String,
    /// Go `StmtExecInfo.PlanDigest`.
    pub plan_digest: String,
    /// Go `StmtExecInfo.User`.
    pub user: String,
    /// Go `StmtExecInfo.TotalLatency`.
    pub total_latency: Duration,
    /// Go `StmtExecInfo.ParseLatency`.
    pub parse_latency: Duration,
    /// Go `StmtExecInfo.CompileLatency`.
    pub compile_latency: Duration,
    /// Go `StmtExecInfo.StmtCtx`.
    pub stmt_ctx: Arc<StmtSummaryStmtCtx>,
    /// Go `StmtExecInfo.CopTasks`.
    pub cop_tasks: Option<CopTasksSummary>,
    /// Go `StmtExecInfo.ExecDetail`.
    pub exec_detail: ExecDetails,
    /// Go `StmtExecInfo.MemMax`.
    pub mem_max: i64,
    /// Go `StmtExecInfo.MemArbitration`.
    pub mem_arbitration: f64,
    /// Go `StmtExecInfo.DiskMax`.
    pub disk_max: i64,
    /// Go `StmtExecInfo.StartTime`.
    pub start_time: DateTime<Utc>,
    /// Go `StmtExecInfo.IsInternal`.
    pub is_internal: bool,
    /// Go `StmtExecInfo.Succeed`.
    pub succeed: bool,
    /// Go `StmtExecInfo.PlanInCache`.
    pub plan_in_cache: bool,
    /// Go `StmtExecInfo.PlanInBinding`.
    pub plan_in_binding: bool,
    /// Go `StmtExecInfo.ExecRetryCount`.
    pub exec_retry_count: u64,
    /// Go `StmtExecInfo.ExecRetryTime`.
    pub exec_retry_time: Duration,
    /// Go `StmtExecInfo.WriteSQLRespDuration`.
    pub write_sql_resp_duration: Duration,
    /// Go `StmtExecInfo.ResultRows`.
    pub result_rows: i64,
    /// Go `StmtExecInfo.TiKVExecDetails`, already loaded out of its atomics.
    pub tikv_exec_details: Option<TikvExecDetailsSnapshot>,
    /// Go `StmtExecInfo.Prepared`.
    pub prepared: bool,
    /// Go `StmtExecInfo.KeyspaceName`.
    pub keyspace_name: String,
    /// Go `StmtExecInfo.KeyspaceID`.
    pub keyspace_id: u32,
    /// Go `StmtExecInfo.ResourceGroupName`.
    pub resource_group_name: String,
    /// Go `StmtExecInfo.RUDetail`, already read through its accessors.
    pub ru_detail: Option<RuDetailsSnapshot>,
    /// Go `StmtExecInfo.TotalRUV2`.
    pub total_ru_v2: f64,
    /// Go `StmtExecInfo.CPUUsages`.
    pub cpu_usages: CpuUsages,
    /// Go `StmtExecInfo.PlanCacheUnqualified`.
    pub plan_cache_unqualified: String,
    /// Go `StmtExecInfo.LazyInfo`.
    pub lazy_info: Arc<dyn StmtExecLazyInfo>,
}

/// Go `StmtRUSummary`: the request-units summary for each type of statements.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StmtRuSummary {
    /// Go `StmtRUSummary.SumRRU`.
    pub sum_rru: f64,
    /// Go `StmtRUSummary.SumWRU`.
    pub sum_wru: f64,
    /// Go `StmtRUSummary.SumRUWaitDuration`.
    pub sum_ru_wait_duration: Duration,
    /// Go `StmtRUSummary.MaxRRU`.
    pub max_rru: f64,
    /// Go `StmtRUSummary.MaxWRU`.
    pub max_wru: f64,
    /// Go `StmtRUSummary.MaxRUWaitDuration`.
    pub max_ru_wait_duration: Duration,
    /// Go `StmtRUSummary.SumRUV2`.
    pub sum_ru_v2: f64,
    /// Go `StmtRUSummary.MaxRUV2`.
    pub max_ru_v2: f64,
}

impl StmtRuSummary {
    /// Go `(*StmtRUSummary).Add`: adds a new sample value to the ru summary
    /// record.
    pub fn add(&mut self, info: Option<&RuDetailsSnapshot>, total_ru_v2: f64) {
        if let Some(info) = info {
            let rru = info.rru;
            self.sum_rru += rru;
            if self.max_rru < rru {
                self.max_rru = rru;
            }
            let wru = info.wru;
            self.sum_wru += wru;
            if self.max_wru < wru {
                self.max_wru = wru;
            }
            let ru_wait_dur = info.ru_wait_duration;
            self.sum_ru_wait_duration += ru_wait_dur;
            if self.max_ru_wait_duration < ru_wait_dur {
                self.max_ru_wait_duration = ru_wait_dur;
            }
        }
        self.sum_ru_v2 += total_ru_v2;
        if self.max_ru_v2 < total_ru_v2 {
            self.max_ru_v2 = total_ru_v2;
        }
    }

    /// Go `(*StmtRUSummary).Merge`: merges the value of 2 ru summary records.
    pub fn merge(&mut self, other: &Self) {
        self.sum_rru += other.sum_rru;
        self.sum_wru += other.sum_wru;
        self.sum_ru_wait_duration += other.sum_ru_wait_duration;
        if self.max_rru < other.max_rru {
            self.max_rru = other.max_rru;
        }
        if self.max_wru < other.max_wru {
            self.max_wru = other.max_wru;
        }
        if self.max_ru_wait_duration < other.max_ru_wait_duration {
            self.max_ru_wait_duration = other.max_ru_wait_duration;
        }
        self.sum_ru_v2 += other.sum_ru_v2;
        if self.max_ru_v2 < other.max_ru_v2 {
            self.max_ru_v2 = other.max_ru_v2;
        }
    }
}

/// Go `StmtNetworkTrafficSummary`: the network traffic summary for each type of
/// statements.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StmtNetworkTrafficSummary {
    /// Go `UnpackedBytesSentTiKVTotal`.
    pub unpacked_bytes_sent_tikv_total: i64,
    /// Go `UnpackedBytesReceivedTiKVTotal`.
    pub unpacked_bytes_received_tikv_total: i64,
    /// Go `UnpackedBytesSentTiKVCrossZone`.
    pub unpacked_bytes_sent_tikv_cross_zone: i64,
    /// Go `UnpackedBytesReceivedTiKVCrossZone`.
    pub unpacked_bytes_received_tikv_cross_zone: i64,
    /// Go `UnpackedBytesSentTiFlashTotal`.
    pub unpacked_bytes_sent_tiflash_total: i64,
    /// Go `UnpackedBytesReceivedTiFlashTotal`.
    pub unpacked_bytes_received_tiflash_total: i64,
    /// Go `UnpackedBytesSentTiFlashCrossZone`.
    pub unpacked_bytes_sent_tiflash_cross_zone: i64,
    /// Go `UnpackedBytesReceivedTiFlashCrossZone`.
    pub unpacked_bytes_received_tiflash_cross_zone: i64,
}

impl StmtNetworkTrafficSummary {
    /// Go `(*StmtNetworkTrafficSummary).Merge`.
    pub fn merge(&mut self, other: Option<&Self>) {
        let Some(other) = other else {
            return;
        };
        self.unpacked_bytes_sent_tikv_total += other.unpacked_bytes_sent_tikv_total;
        self.unpacked_bytes_received_tikv_total += other.unpacked_bytes_received_tikv_total;
        self.unpacked_bytes_sent_tikv_cross_zone += other.unpacked_bytes_sent_tikv_cross_zone;
        self.unpacked_bytes_received_tikv_cross_zone +=
            other.unpacked_bytes_received_tikv_cross_zone;
        self.unpacked_bytes_sent_tiflash_total += other.unpacked_bytes_sent_tiflash_total;
        self.unpacked_bytes_received_tiflash_total += other.unpacked_bytes_received_tiflash_total;
        self.unpacked_bytes_sent_tiflash_cross_zone += other.unpacked_bytes_sent_tiflash_cross_zone;
        self.unpacked_bytes_received_tiflash_cross_zone +=
            other.unpacked_bytes_received_tiflash_cross_zone;
    }

    /// Go `(*StmtNetworkTrafficSummary).Add`.
    pub fn add(&mut self, info: Option<&TikvExecDetailsSnapshot>) {
        let Some(snapshot) = info else {
            return;
        };
        self.unpacked_bytes_sent_tikv_total += snapshot.unpacked_bytes_sent_kv_total;
        self.unpacked_bytes_received_tikv_total += snapshot.unpacked_bytes_received_kv_total;
        self.unpacked_bytes_sent_tikv_cross_zone += snapshot.unpacked_bytes_sent_kv_cross_zone;
        self.unpacked_bytes_received_tikv_cross_zone +=
            snapshot.unpacked_bytes_received_kv_cross_zone;
        self.unpacked_bytes_sent_tiflash_total += snapshot.unpacked_bytes_sent_mpp_total;
        self.unpacked_bytes_received_tiflash_total += snapshot.unpacked_bytes_received_mpp_total;
        self.unpacked_bytes_sent_tiflash_cross_zone += snapshot.unpacked_bytes_sent_mpp_cross_zone;
        self.unpacked_bytes_received_tiflash_cross_zone +=
            snapshot.unpacked_bytes_received_mpp_cross_zone;
    }
}

/// Go `stmtSummaryStats`: the collection of statistics tracked for each
/// statement summary, both cumulatively and for each interval.
#[derive(Clone, Debug, PartialEq)]
pub struct StmtSummaryStats {
    // basic
    /// Go `sampleSQL`.
    pub sample_sql: String,
    /// Go `charset`.
    pub charset: String,
    /// Go `collation`.
    pub collation: String,
    /// Go `prevSQL`.
    pub prev_sql: String,
    /// Go `samplePlan`.
    pub sample_plan: String,
    /// Go `sampleBinaryPlan`.
    pub sample_binary_plan: String,
    /// Go `planHint`.
    pub plan_hint: String,
    /// Go `indexNames`.
    pub index_names: Vec<String>,
    /// Go `execCount`.
    pub exec_count: i64,
    /// Go `sumErrors`.
    pub sum_errors: i64,
    /// Go `sumWarnings`.
    pub sum_warnings: i64,
    // latency
    /// Go `sumLatency`.
    pub sum_latency: Duration,
    /// Go `maxLatency`.
    pub max_latency: Duration,
    /// Go `minLatency`.
    pub min_latency: Duration,
    /// Go `sumParseLatency`.
    pub sum_parse_latency: Duration,
    /// Go `maxParseLatency`.
    pub max_parse_latency: Duration,
    /// Go `sumCompileLatency`.
    pub sum_compile_latency: Duration,
    /// Go `maxCompileLatency`.
    pub max_compile_latency: Duration,
    // coprocessor
    /// Go `sumNumCopTasks`.
    pub sum_num_cop_tasks: i64,
    /// Go `sumCopProcessTime`.
    pub sum_cop_process_time: Duration,
    /// Go `maxCopProcessTime`.
    pub max_cop_process_time: Duration,
    /// Go `maxCopProcessAddress`.
    pub max_cop_process_address: String,
    /// Go `sumCopWaitTime`.
    pub sum_cop_wait_time: Duration,
    /// Go `maxCopWaitTime`.
    pub max_cop_wait_time: Duration,
    /// Go `maxCopWaitAddress`.
    pub max_cop_wait_address: String,
    // TiKV
    /// Go `sumProcessTime`.
    pub sum_process_time: Duration,
    /// Go `maxProcessTime`.
    pub max_process_time: Duration,
    /// Go `sumWaitTime`.
    pub sum_wait_time: Duration,
    /// Go `maxWaitTime`.
    pub max_wait_time: Duration,
    /// Go `sumBackoffTime`.
    pub sum_backoff_time: Duration,
    /// Go `maxBackoffTime`.
    pub max_backoff_time: Duration,
    /// Go `sumTotalKeys`.
    pub sum_total_keys: i64,
    /// Go `maxTotalKeys`.
    pub max_total_keys: i64,
    /// Go `sumProcessedKeys`.
    pub sum_processed_keys: i64,
    /// Go `maxProcessedKeys`.
    pub max_processed_keys: i64,
    /// Go `sumRocksdbDeleteSkippedCount`.
    pub sum_rocksdb_delete_skipped_count: u64,
    /// Go `maxRocksdbDeleteSkippedCount`.
    pub max_rocksdb_delete_skipped_count: u64,
    /// Go `sumRocksdbKeySkippedCount`.
    pub sum_rocksdb_key_skipped_count: u64,
    /// Go `maxRocksdbKeySkippedCount`.
    pub max_rocksdb_key_skipped_count: u64,
    /// Go `sumRocksdbBlockCacheHitCount`.
    pub sum_rocksdb_block_cache_hit_count: u64,
    /// Go `maxRocksdbBlockCacheHitCount`.
    pub max_rocksdb_block_cache_hit_count: u64,
    /// Go `sumRocksdbBlockReadCount`.
    pub sum_rocksdb_block_read_count: u64,
    /// Go `maxRocksdbBlockReadCount`.
    pub max_rocksdb_block_read_count: u64,
    /// Go `sumRocksdbBlockReadByte`.
    pub sum_rocksdb_block_read_byte: u64,
    /// Go `maxRocksdbBlockReadByte`.
    pub max_rocksdb_block_read_byte: u64,
    /// Go `sumIARemoteReadSegmentCount`.
    pub sum_ia_remote_read_segment_count: u64,
    /// Go `maxIARemoteReadSegmentCount`.
    pub max_ia_remote_read_segment_count: u64,
    /// Go `sumIARemoteReadSegmentSize`.
    pub sum_ia_remote_read_segment_size: u64,
    /// Go `maxIARemoteReadSegmentSize`.
    pub max_ia_remote_read_segment_size: u64,
    /// Go `sumIARemoteReadSegmentWaitTime`.
    pub sum_ia_remote_read_segment_wait_time: Duration,
    /// Go `maxIARemoteReadSegmentWaitTime`.
    pub max_ia_remote_read_segment_wait_time: Duration,
    // txn
    /// Go `commitCount`.
    pub commit_count: i64,
    /// Go `sumGetCommitTsTime`.
    pub sum_get_commit_ts_time: Duration,
    /// Go `maxGetCommitTsTime`.
    pub max_get_commit_ts_time: Duration,
    /// Go `sumPrewriteTime`.
    pub sum_prewrite_time: Duration,
    /// Go `maxPrewriteTime`.
    pub max_prewrite_time: Duration,
    /// Go `sumCommitTime`.
    pub sum_commit_time: Duration,
    /// Go `maxCommitTime`.
    pub max_commit_time: Duration,
    /// Go `sumLocalLatchTime`.
    pub sum_local_latch_time: Duration,
    /// Go `maxLocalLatchTime`.
    pub max_local_latch_time: Duration,
    /// Go `sumCommitBackoffTime`.
    pub sum_commit_backoff_time: i64,
    /// Go `maxCommitBackoffTime`.
    pub max_commit_backoff_time: i64,
    /// Go `sumResolveLockTime`.
    pub sum_resolve_lock_time: i64,
    /// Go `maxResolveLockTime`.
    pub max_resolve_lock_time: i64,
    /// Go `sumWriteKeys`.
    pub sum_write_keys: i64,
    /// Go `maxWriteKeys`.
    pub max_write_keys: i64,
    /// Go `sumWriteSize`.
    pub sum_write_size: i64,
    /// Go `maxWriteSize`.
    pub max_write_size: i64,
    /// Go `sumPrewriteRegionNum`.
    pub sum_prewrite_region_num: i64,
    /// Go `maxPrewriteRegionNum`.
    pub max_prewrite_region_num: i32,
    /// Go `sumTxnRetry`.
    pub sum_txn_retry: i64,
    /// Go `maxTxnRetry`.
    pub max_txn_retry: i64,
    /// Go `sumBackoffTimes`.
    pub sum_backoff_times: i64,
    /// Go `backoffTypes`.
    pub backoff_types: HashMap<String, i64>,
    /// Go `authUsers`.
    pub auth_users: HashSet<String>,
    // other
    /// Go `sumMem`.
    pub sum_mem: i64,
    /// Go `maxMem`.
    pub max_mem: i64,
    /// Go `sumDisk`.
    pub sum_disk: i64,
    /// Go `maxDisk`.
    pub max_disk: i64,
    /// Go `sumAffectedRows`.
    pub sum_affected_rows: u64,
    /// Go `sumKVTotal`.
    pub sum_kv_total: Duration,
    /// Go `sumPDTotal`.
    pub sum_pd_total: Duration,
    /// Go `sumBackoffTotal`.
    pub sum_backoff_total: Duration,
    /// Go `sumWriteSQLRespTotal`.
    pub sum_write_sql_resp_total: Duration,
    /// Go `sumTidbCPU`.
    pub sum_tidb_cpu: Duration,
    /// Go `sumTikvCPU`.
    pub sum_tikv_cpu: Duration,
    /// Go `sumResultRows`.
    pub sum_result_rows: i64,
    /// Go `maxResultRows`.
    pub max_result_rows: i64,
    /// Go `minResultRows`.
    pub min_result_rows: i64,
    /// Go `prepared`.
    pub prepared: bool,
    /// Go `firstSeen`: the first time this type of SQL executes.
    pub first_seen: DateTime<Utc>,
    /// Go `lastSeen`: the last time this type of SQL executes.
    pub last_seen: DateTime<Utc>,
    /// Go `planInCache`.
    pub plan_in_cache: bool,
    /// Go `planCacheHits`.
    pub plan_cache_hits: i64,
    /// Go `planInBinding`.
    pub plan_in_binding: bool,
    /// Go `execRetryCount`.
    pub exec_retry_count: u64,
    /// Go `execRetryTime`.
    pub exec_retry_time: Duration,
    /// Go `resourceGroupName`.
    pub resource_group_name: String,
    /// Go's embedded `StmtRUSummary`.
    pub ru: StmtRuSummary,
    /// Go's embedded `StmtNetworkTrafficSummary`.
    pub network: StmtNetworkTrafficSummary,
    /// Go `planCacheUnqualifiedCount`.
    pub plan_cache_unqualified_count: i64,
    /// Go `lastPlanCacheUnqualified`: the reason why this query is unqualified
    /// for the plan cache.
    pub last_plan_cache_unqualified: String,
    /// Go `storageKV`: query read from TiKV.
    pub storage_kv: bool,
    /// Go `storageMPP`: query read from TiFlash.
    pub storage_mpp: bool,
    /// Go `sumMemArbitration`.
    pub sum_mem_arbitration: f64,
    /// Go `maxMemArbitration`.
    pub max_mem_arbitration: f64,
}

impl Default for StmtSummaryStats {
    fn default() -> Self {
        Self {
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
            sum_cop_process_time: Duration::ZERO,
            max_cop_process_time: Duration::ZERO,
            max_cop_process_address: String::new(),
            sum_cop_wait_time: Duration::ZERO,
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
            resource_group_name: String::new(),
            ru: StmtRuSummary::default(),
            network: StmtNetworkTrafficSummary::default(),
            plan_cache_unqualified_count: 0,
            last_plan_cache_unqualified: String::new(),
            storage_kv: false,
            storage_mpp: false,
            sum_mem_arbitration: 0.0,
            max_mem_arbitration: 0.0,
        }
    }
}

/// Go `newStmtSummaryStats`.
///
/// `sampleSQL` / `authUsers` (sampleUser) / `samplePlan` / `prevSQL` /
/// `indexNames` store the values shown at the first time, because it compacts
/// performance to update every time.
///
/// Go returns `nil` when `GetEncodedPlan` errors; that becomes `None` here.
#[must_use]
pub fn new_stmt_summary_stats(sei: &StmtExecInfo) -> Option<StmtSummaryStats> {
    let (mut sample_plan, plan_hint) = sei.lazy_info.encoded_plan().ok()?;
    let limit = MAX_ENCODED_PLAN_SIZE_IN_BYTES.load(Ordering::SeqCst);
    if sample_plan.len() > limit {
        sample_plan = PLAN_DISCARDED_ENCODED.to_owned();
    }
    let mut bin_plan = sei.lazy_info.binary_plan();
    if bin_plan.len() > limit {
        bin_plan = BINARY_PLAN_DISCARDED_ENCODED.clone();
    }
    Some(StmtSummaryStats {
        sample_sql: format_sql(&sei.lazy_info.original_sql()),
        charset: sei.charset.clone(),
        collation: sei.collation.clone(),
        // PrevSQL is already truncated to cfg.Log.QueryLogMaxLen.
        prev_sql: sei.prev_sql.clone(),
        // samplePlan needs to be decoded so it can't be truncated.
        sample_plan,
        sample_binary_plan: bin_plan,
        plan_hint,
        index_names: sei.stmt_ctx.index_names.clone(),
        min_latency: sei.total_latency,
        first_seen: sei.start_time,
        last_seen: sei.start_time,
        prepared: sei.prepared,
        min_result_rows: i64::MAX,
        resource_group_name: sei.resource_group_name.clone(),
        ..StmtSummaryStats::default()
    })
}

impl StmtSummaryStats {
    /// Go `(*stmtSummaryStats).add`.
    #[allow(clippy::too_many_lines)]
    pub fn add(&mut self, sei: &StmtExecInfo, warning_count: i64, affected_rows: u64) {
        // add user to auth users set
        if !sei.user.is_empty() {
            self.auth_users.insert(sei.user.clone());
        }

        self.exec_count += 1;
        if !sei.succeed {
            self.sum_errors += 1;
        }
        self.sum_warnings += warning_count;

        // latency
        self.sum_latency += sei.total_latency;
        if sei.total_latency > self.max_latency {
            self.max_latency = sei.total_latency;
        }
        if sei.total_latency < self.min_latency {
            self.min_latency = sei.total_latency;
        }
        self.sum_parse_latency += sei.parse_latency;
        if sei.parse_latency > self.max_parse_latency {
            self.max_parse_latency = sei.parse_latency;
        }
        self.sum_compile_latency += sei.compile_latency;
        if sei.compile_latency > self.max_compile_latency {
            self.max_compile_latency = sei.compile_latency;
        }

        // coprocessor
        if let Some(cop_tasks) = sei.cop_tasks.as_ref() {
            self.sum_num_cop_tasks += cop_tasks.num_cop_tasks;
            self.sum_cop_process_time += cop_tasks.tot_process_time;
            if cop_tasks.max_process_time > self.max_cop_process_time {
                self.max_cop_process_time = cop_tasks.max_process_time;
                self.max_cop_process_address
                    .clone_from(&cop_tasks.max_process_address);
            }
            self.sum_cop_wait_time += cop_tasks.tot_wait_time;
            if cop_tasks.max_wait_time > self.max_cop_wait_time {
                self.max_cop_wait_time = cop_tasks.max_wait_time;
                self.max_cop_wait_address
                    .clone_from(&cop_tasks.max_wait_address);
            }
        }

        // TiKV
        let time_detail = &sei.exec_detail.cop_exec_details.time_detail;
        self.sum_process_time += time_detail.process_time;
        if time_detail.process_time > self.max_process_time {
            self.max_process_time = time_detail.process_time;
        }
        self.sum_wait_time += time_detail.wait_time;
        if time_detail.wait_time > self.max_wait_time {
            self.max_wait_time = time_detail.wait_time;
        }
        let backoff_time = sei.exec_detail.cop_exec_details.backoff_time;
        self.sum_backoff_time += backoff_time;
        if backoff_time > self.max_backoff_time {
            self.max_backoff_time = backoff_time;
        }

        if let Some(scan_detail) = sei.exec_detail.cop_exec_details.scan_detail.as_ref() {
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

        // txn
        if let Some(commit_details) = sei.exec_detail.commit_detail.as_ref() {
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

        // plan cache
        if sei.plan_in_cache {
            self.plan_in_cache = true;
            self.plan_cache_hits += 1;
        } else {
            self.plan_in_cache = false;
        }
        if !sei.plan_cache_unqualified.is_empty() {
            self.plan_cache_unqualified_count += 1;
            self.last_plan_cache_unqualified
                .clone_from(&sei.plan_cache_unqualified);
        }

        // SPM
        self.plan_in_binding = sei.plan_in_binding;

        // other
        self.sum_affected_rows += affected_rows;
        self.sum_mem += sei.mem_max;
        if sei.mem_max > self.max_mem {
            self.max_mem = sei.mem_max;
        }

        self.sum_mem_arbitration += sei.mem_arbitration;
        if sei.mem_arbitration > self.max_mem_arbitration {
            self.max_mem_arbitration = sei.mem_arbitration;
        }

        self.sum_disk += sei.disk_max;
        if sei.disk_max > self.max_disk {
            self.max_disk = sei.disk_max;
        }
        if sei.start_time < self.first_seen {
            self.first_seen = sei.start_time;
        }
        if self.last_seen < sei.start_time {
            self.last_seen = sei.start_time;
        }
        if sei.exec_retry_count > 0 {
            self.exec_retry_count += sei.exec_retry_count;
            self.exec_retry_time += sei.exec_retry_time;
        }
        if sei.result_rows > 0 {
            self.sum_result_rows += sei.result_rows;
            if self.max_result_rows < sei.result_rows {
                self.max_result_rows = sei.result_rows;
            }
            if self.min_result_rows > sei.result_rows {
                self.min_result_rows = sei.result_rows;
            }
        } else {
            self.min_result_rows = 0;
        }
        if let Some(tikv) = sei.tikv_exec_details.as_ref() {
            self.sum_kv_total += nanos_to_duration(tikv.wait_kv_resp_duration);
            self.sum_pd_total += nanos_to_duration(tikv.wait_pd_resp_duration);
            self.sum_backoff_total += nanos_to_duration(tikv.backoff_duration);
        }
        self.sum_write_sql_resp_total += sei.write_sql_resp_duration;
        self.sum_tidb_cpu += sei.cpu_usages.tidb_cpu_time;
        self.sum_tikv_cpu += sei.cpu_usages.tikv_cpu_time;

        // network traffic
        self.network.add(sei.tikv_exec_details.as_ref());

        // request-units
        self.ru.add(sei.ru_detail.as_ref(), sei.total_ru_v2);

        self.storage_kv = sei.stmt_ctx.is_tikv.load(Ordering::SeqCst);
        self.storage_mpp = sei.stmt_ctx.is_tiflash.load(Ordering::SeqCst);
    }
}

/// Go `time.Duration(int64)` for non-negative nanosecond counts; negative
/// counts (which `time.Duration` allows and `Duration` does not) clamp to zero.
fn nanos_to_duration(nanos: i64) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(0))
}

/// Go `stmtSummaryByDigestElement`: the summary for each type of statements in
/// the current interval.
#[derive(Clone, Debug, PartialEq)]
pub struct StmtSummaryByDigestElement {
    /// Go `beginTime`: each summary is summarized between `[beginTime, endTime)`.
    pub begin_time: i64,
    /// Go `endTime`.
    pub end_time: i64,
    /// Go's embedded `stmtSummaryStats`.
    pub stats: StmtSummaryStats,
}

impl StmtSummaryByDigestElement {
    /// Go `newStmtSummaryByDigestElement`.
    #[must_use]
    pub fn new(
        sei: &StmtExecInfo,
        begin_time: i64,
        interval_seconds: i64,
        warning_count: i64,
        affected_rows: u64,
    ) -> Option<Self> {
        let mut element = Self {
            begin_time,
            end_time: 0,
            stats: new_stmt_summary_stats(sei)?,
        };
        element.add(sei, interval_seconds, warning_count, affected_rows);
        Some(element)
    }

    /// Go `(*stmtSummaryByDigestElement).onExpire`: called when this element
    /// expires to history.
    pub fn on_expire(&mut self, interval_seconds: i64) {
        // refreshInterval may change anytime, so we need to update endTime.
        if self.begin_time + interval_seconds > self.end_time {
            // If interval changes to a bigger value, update endTime to
            // beginTime + interval.
            self.end_time = self.begin_time + interval_seconds;
        } else if self.begin_time + interval_seconds < self.end_time {
            let now = unix_now();
            // If interval changes to a smaller value and now > beginTime +
            // interval, update endTime to current time.
            if now > self.begin_time + interval_seconds {
                self.end_time = now;
            }
        }
    }

    /// Go `(*stmtSummaryByDigestElement).add`.
    pub fn add(
        &mut self,
        sei: &StmtExecInfo,
        interval_seconds: i64,
        warning_count: i64,
        affected_rows: u64,
    ) {
        // refreshInterval may change anytime, update endTime ASAP.
        self.end_time = self.begin_time + interval_seconds;
        self.stats.add(sei, warning_count, affected_rows);
    }
}

/// Go `stmtSummaryByDigest`: the summary for each type of statements.
#[derive(Debug, Default)]
pub struct StmtSummaryByDigest {
    /// Go `initialized`.
    pub initialized: bool,
    /// Go `cumulative`.
    pub cumulative: StmtSummaryStats,
    /// Go `history`: each element is a summary in one interval.
    pub history: VecDeque<Arc<Mutex<StmtSummaryByDigestElement>>>,
    /// Go `schemaName`.
    pub schema_name: String,
    /// Go `digest`.
    pub digest: String,
    /// Go `planDigest`.
    pub plan_digest: String,
    /// Go `stmtType`.
    pub stmt_type: String,
    /// Go `normalizedSQL`.
    pub normalized_sql: String,
    /// Go `tableNames`.
    pub table_names: String,
    /// Go `isInternal`.
    pub is_internal: bool,
    /// Go `bindingSQL`.
    pub binding_sql: String,
    /// Go `bindingDigest`.
    pub binding_digest: String,
}

impl StmtSummaryByDigest {
    /// Go `(*stmtSummaryByDigest).init`: creates a `stmtSummaryByDigest` from
    /// `StmtExecInfo`.
    ///
    /// Go would nil-deref when `newStmtSummaryStats` returns nil; here the
    /// initialization is skipped and `false` is returned.
    fn init(&mut self, sei: &StmtExecInfo) -> bool {
        // Use "," to separate table names to support FIND_IN_SET.
        let mut buffer = String::new();
        let table_count = sei.stmt_ctx.tables.len();
        for (i, value) in sei.stmt_ctx.tables.iter().enumerate() {
            // In `create database` statement, DB name is not empty but table
            // name is empty.
            if value.table.is_empty() {
                continue;
            }
            buffer.push_str(&value.db.to_lowercase());
            buffer.push('.');
            buffer.push_str(&value.table.to_lowercase());
            if i < table_count - 1 {
                buffer.push(',');
            }
        }
        let table_names = buffer;

        let Some(cumulative) = new_stmt_summary_stats(sei) else {
            return false;
        };
        self.cumulative = cumulative;

        let mut plan_digest = sei.plan_digest.clone();
        if plan_digest.is_empty() {
            // It comes here only when the plan is 'Point_Get'.
            plan_digest = sei.lazy_info.plan_digest();
        }
        self.schema_name.clone_from(&sei.schema_name);
        self.digest.clone_from(&sei.digest);
        self.plan_digest = plan_digest;
        self.stmt_type.clone_from(&sei.stmt_ctx.stmt_type);
        self.normalized_sql = format_sql(&sei.normalized_sql);
        self.table_names = table_names;
        self.history = VecDeque::new();
        self.initialized = true;
        let (binding_sql, binding_digest) = sei.lazy_info.binding_sql_and_digest();
        self.binding_sql = binding_sql;
        self.binding_digest = binding_digest;
        true
    }

    /// Go `(*stmtSummaryByDigest).add`.
    ///
    /// Go takes `ssbd.Lock` for the element bookkeeping and releases it before
    /// locking the element; here `&mut self` stands in for the outer lock and
    /// the element mutex is taken only for the non-new path, as in the source.
    pub fn add(
        &mut self,
        sei: &StmtExecInfo,
        begin_time: i64,
        interval_seconds: i64,
        history_size: usize,
    ) {
        let warning_count = i64::from(sei.stmt_ctx.warning_count());
        let affected_rows = sei.stmt_ctx.affected_rows();

        if !self.initialized && !self.init(sei) {
            return;
        }
        self.cumulative.add(sei, warning_count, affected_rows);

        let mut ss_element = None;
        let mut is_element_new = true;
        if let Some(last_element) = self.history.back() {
            if last_element.lock().unwrap().begin_time >= begin_time {
                ss_element = Some(Arc::clone(last_element));
                is_element_new = false;
            } else {
                // The last element expires to the history.
                last_element.lock().unwrap().on_expire(interval_seconds);
            }
        }
        if is_element_new {
            // If the element is new created, `ssElement.add(sei)` should be
            // done inside the lock of `ssbd`.
            let Some(element) = StmtSummaryByDigestElement::new(
                sei,
                begin_time,
                interval_seconds,
                warning_count,
                affected_rows,
            ) else {
                return;
            };
            let element = Arc::new(Mutex::new(element));
            self.history.push_back(Arc::clone(&element));
            ss_element = Some(element);
        }

        // `historySize` might be modified anytime, so check expiration every
        // time. Even if history is set to 0, the current summary is still
        // needed.
        while self.history.len() > history_size && self.history.len() > 1 {
            self.history.pop_front();
        }

        // Lock a single entry, not the whole `ssbd`.
        if !is_element_new {
            if let Some(element) = ss_element {
                element
                    .lock()
                    .unwrap()
                    .add(sei, interval_seconds, warning_count, affected_rows);
            }
        }
    }

    /// Go `(*stmtSummaryByDigest).collectHistorySummaries`: puts at most
    /// `historySize` summaries into an array.
    ///
    /// Go's `*stmtSummaryChecker` parameter lands with `reader.go`.
    #[must_use]
    pub fn collect_history_summaries(
        &self,
        history_size: usize,
    ) -> Vec<Arc<Mutex<StmtSummaryByDigestElement>>> {
        if !self.initialized {
            return Vec::new();
        }
        self.history
            .iter()
            .take(history_size)
            .map(Arc::clone)
            .collect()
    }
}

/// The boundary Go crosses into `evicted.go`'s `stmtSummaryByDigestEvicted`.
/// That file is not ported yet, so only [`NoopEvictedSink`] implements it.
pub trait EvictedSink: Send {
    /// Go `(*stmtSummaryByDigestEvicted).AddEvicted`.
    fn add_evicted(
        &mut self,
        key: &StmtDigestKey,
        value: &Arc<Mutex<StmtSummaryByDigest>>,
        history_size: usize,
    );

    /// Go `(*stmtSummaryByDigestEvicted).Clear`.
    fn clear(&mut self);
}

/// The default [`EvictedSink`]: drops everything until `evicted.go` lands.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopEvictedSink;

impl EvictedSink for NoopEvictedSink {
    fn add_evicted(
        &mut self,
        _key: &StmtDigestKey,
        _value: &Arc<Mutex<StmtSummaryByDigest>>,
        _history_size: usize,
    ) {
    }

    fn clear(&mut self) {}
}

/// Narrowing of Go `metrics.SetStmtSummaryWindowMetrics(metrics.StmtSummaryTypeV1, ...)`.
pub trait WindowMetricsSink: Send + Sync {
    /// Publishes the current window's record and eviction counts.
    fn set_window_metrics(&self, record_count: f64, evicted_count: f64);
}

/// The default [`WindowMetricsSink`]: publishes nowhere.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopWindowMetricsSink;

impl WindowMetricsSink for NoopWindowMetricsSink {
    fn set_window_metrics(&self, _record_count: f64, _evicted_count: f64) {}
}

type SummaryCache = SimpleLruCache<StmtDigestKey, Arc<Mutex<StmtSummaryByDigest>>>;

/// The state Go guards with `stmtSummaryByDigestMap`'s embedded `sync.Mutex`.
struct MapInner {
    summary_map: SummaryCache,
    /// Go `beginTimeForCurInterval`: the begin time for the current summary.
    begin_time_for_cur_interval: i64,
}

/// Go `stmtSummaryByDigestMap`: an LRU cache that stores statement summaries.
pub struct StmtSummaryByDigestMap {
    inner: Mutex<MapInner>,

    // These options are set by global system variables and are accessed
    // concurrently. Go uses `go.uber.org/atomic`.
    opt_enabled: AtomicBool,
    opt_enable_internal_query: AtomicBool,
    opt_history_enabled: AtomicBool,
    opt_max_stmt_count: AtomicU32,
    opt_refresh_interval: AtomicI64,
    opt_history_size: Arc<AtomicI32>,
    opt_max_sql_length: AtomicI32,
    opt_group_by_user: AtomicBool,

    /// Go `other`: stores the summary of evicted data.
    other: Arc<Mutex<Box<dyn EvictedSink>>>,
    /// The same object as `other`, at its concrete type, whenever this map owns
    /// the real `evicted.go` rollup (which Go's `newStmtSummaryByDigestMap`
    /// always does). `None` only for a map built by [`Self::with_sinks`] with
    /// some other sink.
    other_evicted: Option<Arc<Mutex<StmtSummaryByDigestEvicted>>>,
    /// Go `currentWindowEvictedCount`: counts LRU evictions observed in the
    /// current interval.
    current_window_evicted_count: Arc<AtomicI64>,

    metrics: Box<dyn WindowMetricsSink>,
    /// Stands in for the `mockTimeForStatementsSummary` failpoint; a negative
    /// value means "use the wall clock".
    mock_now: AtomicI64,
}

impl Default for StmtSummaryByDigestMap {
    fn default() -> Self {
        Self::new()
    }
}

impl StmtSummaryByDigestMap {
    /// Go `newStmtSummaryByDigestMap`: creates an empty
    /// `stmtSummaryByDigestMap`.
    #[must_use]
    pub fn new() -> Self {
        // Go's `other: newStmtSummaryByDigestEvicted()`.
        let evicted = Arc::new(Mutex::new(StmtSummaryByDigestEvicted::new()));
        let mut map = Self::with_sinks(
            Box::new(Arc::clone(&evicted)),
            Box::new(NoopWindowMetricsSink),
        );
        map.other_evicted = Some(evicted);
        map
    }

    /// Go's `ssMap.other`, at the concrete type `evicted.go` declares.
    #[must_use]
    pub fn evicted(&self) -> Option<&Arc<Mutex<StmtSummaryByDigestEvicted>>> {
        self.other_evicted.as_ref()
    }

    /// Go `newStmtSummaryByDigestMap` with the two narrowed collaborators
    /// injected.
    #[must_use]
    pub fn with_sinks(evicted: Box<dyn EvictedSink>, metrics: Box<dyn WindowMetricsSink>) -> Self {
        // This initializes the map with "compiled defaults" (which are
        // regrettably duplicated from sessionctx/variable/tidb_vars.go).
        // Unfortunately we need to do this to avoid circular dependencies, but
        // the correct values will be applied on startup as soon as
        // domain.LoadSysVarCacheLoop() is called.
        let max_stmt_count: usize = 3000;
        let other = Arc::new(Mutex::new(evicted));
        let evicted_count = Arc::new(AtomicI64::new(0));
        let history_size = Arc::new(AtomicI32::new(24));

        let mut summary_map: SummaryCache = SimpleLruCache::new(max_stmt_count);
        let callback_other = Arc::clone(&other);
        let callback_count = Arc::clone(&evicted_count);
        let callback_history_size = Arc::clone(&history_size);
        summary_map.set_on_evict(move |key, value| {
            callback_count.fetch_add(1, Ordering::SeqCst);
            let history_size = callback_history_size.load(Ordering::SeqCst).max(0) as usize;
            callback_other
                .lock()
                .unwrap()
                .add_evicted(key, value, history_size);
        });

        Self {
            inner: Mutex::new(MapInner {
                summary_map,
                begin_time_for_cur_interval: 0,
            }),
            opt_enabled: AtomicBool::new(true),
            opt_enable_internal_query: AtomicBool::new(false),
            opt_history_enabled: AtomicBool::new(true),
            opt_max_stmt_count: AtomicU32::new(max_stmt_count as u32),
            opt_refresh_interval: AtomicI64::new(1800),
            opt_history_size: history_size,
            opt_max_sql_length: AtomicI32::new(32768),
            opt_group_by_user: AtomicBool::new(false),
            other,
            other_evicted: None,
            current_window_evicted_count: evicted_count,
            metrics,
            mock_now: AtomicI64::new(-1),
        }
    }

    /// Stands in for Go's `mockTimeForStatementsSummary` failpoint: pins the
    /// Unix timestamp `AddStatement` reads.
    pub fn set_mock_now(&self, now: Option<i64>) {
        self.mock_now.store(now.unwrap_or(-1), Ordering::SeqCst);
    }

    /// Go `(*stmtSummaryByDigestMap).AddStatement`: adds a statement to the
    /// map.
    pub fn add_statement(&self, sei: &StmtExecInfo) {
        // All times are counted in seconds.
        let mock_now = self.mock_now.load(Ordering::SeqCst);
        let now = if mock_now >= 0 { mock_now } else { unix_now() };

        let interval_seconds = self.refresh_interval();
        let mut history_size = 0usize;
        if self.history_enabled() {
            history_size = self.history_size();
        }

        let mut key = StmtDigestKey::new();

        // Using a global lock here instead of fine-grained locks because the
        // critical sections are very short and layered locks contended badly.
        let mut inner = self.inner.lock().unwrap();

        // Decide userForKey under the lock so SetGroupByUser's flag flip +
        // Clear is atomic w.r.t. AddStatement; otherwise a post-clear insert
        // could land under the wrong grouping mode.
        let user_for_key = if self.opt_group_by_user.load(Ordering::SeqCst) {
            sei.user.as_str()
        } else {
            ""
        };
        key.init(
            &sei.schema_name,
            &sei.digest,
            &sei.prev_sql_digest,
            &sei.plan_digest,
            &sei.resource_group_name,
            user_for_key,
        );

        // Check again. Statements could be added before disabling the flag and
        // after Clear().
        if !self.enabled() {
            return;
        }
        if sei.is_internal && !self.enabled_internal() {
            return;
        }

        if inner.begin_time_for_cur_interval + interval_seconds <= now {
            // `beginTimeForCurInterval` is a multiple of intervalSeconds, so
            // that when the interval is a multiple of 60 (or 600, 1800, 3600,
            // etc), begin time shows 'XX:XX:00', not 'XX:XX:01'~'XX:XX:59'.
            inner.begin_time_for_cur_interval = now / interval_seconds * interval_seconds;
            self.current_window_evicted_count.store(0, Ordering::SeqCst);
        }

        let begin_time = inner.begin_time_for_cur_interval;
        let summary = match inner.summary_map.get(&key) {
            Some(value) => Arc::clone(value),
            None => {
                // Lazy initialize it to release ssMap.mutex ASAP.
                let summary = Arc::new(Mutex::new(StmtSummaryByDigest::default()));
                inner.summary_map.put(key, Arc::clone(&summary));
                summary
            }
        };
        {
            let mut summary = summary.lock().unwrap();
            summary.is_internal = summary.is_internal && sei.is_internal;
            summary.add(sei, begin_time, interval_seconds, history_size);
        }
        self.update_metrics_locked(&inner);
    }

    /// Go `(*stmtSummaryByDigestMap).Clear`: removes all statement summaries.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        self.clear_locked(&mut inner);
    }

    /// Go `(*stmtSummaryByDigestMap).clearLocked`.
    fn clear_locked(&self, inner: &mut MapInner) {
        inner.summary_map.delete_all();
        self.other.lock().unwrap().clear();
        inner.begin_time_for_cur_interval = 0;
        self.current_window_evicted_count.store(0, Ordering::SeqCst);
        self.update_metrics_locked(inner);
    }

    /// Go `(*stmtSummaryByDigestMap).clearInternal`: removes all statement
    /// summaries which are internal summaries.
    pub fn clear_internal(&self) {
        let mut inner = self.inner.lock().unwrap();
        let hashes: Vec<Vec<u8>> = inner
            .summary_map
            .keys()
            .iter()
            .map(|key| key.hash().to_vec())
            .collect();
        for hash in hashes {
            let is_internal = match inner.summary_map.get(hash.as_slice()) {
                Some(summary) => summary.lock().unwrap().is_internal,
                None => continue,
            };
            if is_internal {
                inner.summary_map.delete(hash.as_slice());
            }
        }
        self.update_metrics_locked(&inner);
    }

    /// Go `(*stmtSummaryByDigestMap).clearHistory`: removes history for all
    /// statement summaries, leaving only the current interval.
    pub fn clear_history(&self) {
        let values: Vec<Arc<Mutex<StmtSummaryByDigest>>> = {
            let inner = self.inner.lock().unwrap();
            inner.summary_map.values().into_iter().cloned().collect()
        };

        for value in values {
            let mut ssbd = value.lock().unwrap();
            if let Some(front) = ssbd.history.front().map(Arc::clone) {
                let mut new_history = VecDeque::new();
                new_history.push_front(front);
                ssbd.history = new_history;
            }
        }
    }

    /// Go `(*stmtSummaryByDigestMap).SetEnabled`.
    pub fn set_enabled(&self, value: bool) {
        // `optEnabled` and the map don't need to be strictly atomically
        // updated.
        self.opt_enabled.store(value, Ordering::SeqCst);
        if !value {
            self.clear();
        }
    }

    /// Go `(*stmtSummaryByDigestMap).Enabled`.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.opt_enabled.load(Ordering::SeqCst)
    }

    /// Go `(*stmtSummaryByDigestMap).SetEnabledInternalQuery`.
    pub fn set_enabled_internal_query(&self, value: bool) {
        self.opt_enable_internal_query
            .store(value, Ordering::SeqCst);
        if !value {
            self.clear_internal();
        }
    }

    /// Go `(*stmtSummaryByDigestMap).EnabledInternal`.
    #[must_use]
    pub fn enabled_internal(&self) -> bool {
        self.opt_enable_internal_query.load(Ordering::SeqCst)
    }

    /// Go `(*stmtSummaryByDigestMap).SetHistoryEnabled`: when history is
    /// disabled, any existing history is cleared.
    pub fn set_history_enabled(&self, value: bool) {
        self.opt_history_enabled.store(value, Ordering::SeqCst);
        if !value {
            self.clear_history();
        }
    }

    /// Go `(*stmtSummaryByDigestMap).historyEnabled`.
    #[must_use]
    pub fn history_enabled(&self) -> bool {
        self.opt_history_enabled.load(Ordering::SeqCst)
    }

    /// Go `(*stmtSummaryByDigestMap).SetRefreshInterval`.
    pub fn set_refresh_interval(&self, value: i64) {
        self.opt_refresh_interval.store(value, Ordering::SeqCst);
    }

    /// Go `(*stmtSummaryByDigestMap).refreshInterval`.
    #[must_use]
    pub fn refresh_interval(&self) -> i64 {
        self.opt_refresh_interval.load(Ordering::SeqCst)
    }

    /// Go `(*stmtSummaryByDigestMap).SetHistorySize`.
    pub fn set_history_size(&self, value: i32) {
        self.opt_history_size.store(value, Ordering::SeqCst);
    }

    /// Go `(*stmtSummaryByDigestMap).historySize`.
    #[must_use]
    pub fn history_size(&self) -> usize {
        self.opt_history_size.load(Ordering::SeqCst).max(0) as usize
    }

    /// Go `(*stmtSummaryByDigestMap).SetGroupByUser`: switching the flag clears
    /// existing data because existing rows were aggregated under a different
    /// grouping key.
    pub fn set_group_by_user(&self, value: bool) {
        // Hold the lock across the flag flip and clear so add_statement (which
        // reads the flag under the same lock) cannot insert a record with the
        // old grouping mode after the clear completes.
        let mut inner = self.inner.lock().unwrap();
        if self.opt_group_by_user.load(Ordering::SeqCst) == value {
            return;
        }
        self.opt_group_by_user.store(value, Ordering::SeqCst);
        self.clear_locked(&mut inner);
    }

    /// Go `(*stmtSummaryByDigestMap).GroupByUser`.
    #[must_use]
    pub fn group_by_user(&self) -> bool {
        self.opt_group_by_user.load(Ordering::SeqCst)
    }

    /// Go `(*stmtSummaryByDigestMap).SetMaxStmtCount`.
    pub fn set_max_stmt_count(&self, value: u32) -> Result<(), InvalidCapacity> {
        self.opt_max_stmt_count.store(value, Ordering::SeqCst);

        let mut inner = self.inner.lock().unwrap();
        let result = inner.summary_map.set_capacity(value as usize);
        self.update_metrics_locked(&inner);
        result
    }

    /// Go `(*stmtSummaryByDigestMap).maxStmtCount`.
    #[must_use]
    pub fn max_stmt_count(&self) -> usize {
        self.opt_max_stmt_count.load(Ordering::SeqCst) as usize
    }

    /// Go `(*stmtSummaryByDigestMap).updateMetricsLocked`.
    fn update_metrics_locked(&self, inner: &MapInner) {
        self.metrics.set_window_metrics(
            inner.summary_map.size() as f64,
            self.current_window_evicted_count.load(Ordering::SeqCst) as f64,
        );
    }

    /// Go `(*stmtSummaryByDigestMap).SetMaxSQLLength`.
    pub fn set_max_sql_length(&self, value: i32) {
        self.opt_max_sql_length.store(value, Ordering::SeqCst);
    }

    /// Go `(*stmtSummaryByDigestMap).maxSQLLength`.
    #[must_use]
    pub fn max_sql_length(&self) -> usize {
        self.opt_max_sql_length.load(Ordering::SeqCst).max(0) as usize
    }

    /// Go's direct test writes to `ssMap.beginTimeForCurInterval`.
    pub fn set_begin_time_for_cur_interval(&self, value: i64) {
        self.inner.lock().unwrap().begin_time_for_cur_interval = value;
    }

    /// Go's direct test reads of `ssMap.beginTimeForCurInterval`.
    #[must_use]
    pub fn begin_time_for_cur_interval(&self) -> i64 {
        self.inner.lock().unwrap().begin_time_for_cur_interval
    }

    /// Go's `ssMap.summaryMap.Size()`.
    #[must_use]
    pub fn summary_map_size(&self) -> usize {
        self.inner.lock().unwrap().summary_map.size()
    }

    /// Go's `ssMap.summaryMap.Get(key)`.
    #[must_use]
    pub fn summary_map_get(&self, key: &StmtDigestKey) -> Option<Arc<Mutex<StmtSummaryByDigest>>> {
        self.inner.lock().unwrap().summary_map.get(key).cloned()
    }

    /// Go's `ssMap.summaryMap.Values()`, in most-recently-used order.
    #[must_use]
    pub fn summary_map_values(&self) -> Vec<Arc<Mutex<StmtSummaryByDigest>>> {
        self.inner
            .lock()
            .unwrap()
            .summary_map
            .values()
            .into_iter()
            .cloned()
            .collect()
    }
}

/// Go's `time.Now().Unix()`.
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Go `formatSQL`: truncates SQL to `maxSQLLength`.
///
/// Go slices raw bytes; this truncates at the nearest UTF-8 boundary at or
/// below the limit, and reports Go's byte length.
#[must_use]
pub fn format_sql(sql: &str) -> String {
    let max_sql_length = STMT_SUMMARY_BY_DIGEST_MAP.max_sql_length();
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

/// Go `formatBackoffTypes`: formats the backoff-type map to a string or nil.
#[must_use]
pub fn format_backoff_types(backoff_map: &HashMap<String, i64>) -> Option<String> {
    if backoff_map.is_empty() {
        return None;
    }

    let mut backoff_array: Vec<(&str, i64)> = backoff_map
        .iter()
        .map(|(backoff_type, count)| (backoff_type.as_str(), *count))
        .collect();
    backoff_array.sort_by_key(|stat| std::cmp::Reverse(stat.1));

    let mut buffer = String::new();
    for (index, stat) in backoff_array.iter().enumerate() {
        buffer.push_str(&format!("{}:{}", stat.0, stat.1));
        if index < backoff_array.len() - 1 {
            buffer.push(',');
        }
    }
    Some(buffer)
}

/// Go `avgInt`.
#[must_use]
pub fn avg_int(sum: i64, count: i64) -> i64 {
    if count > 0 {
        sum / count
    } else {
        0
    }
}

/// Go `avgFloat`.
#[must_use]
pub fn avg_float(sum: i64, count: i64) -> f64 {
    if count > 0 {
        sum as f64 / count as f64
    } else {
        0.0
    }
}

/// Go `avgFloat4Uint`.
#[must_use]
pub fn avg_float4_uint(sum: u64, count: i64) -> f64 {
    if count > 0 {
        sum as f64 / count as f64
    } else {
        0.0
    }
}

/// Go `avgSumFloat`.
#[must_use]
pub fn avg_sum_float(sum: f64, count: i64) -> f64 {
    if count > 0 {
        sum / count as f64
    } else {
        0.0
    }
}

/// Go `convertEmptyToNil`.
#[must_use]
pub fn convert_empty_to_nil(str: &str) -> Option<&str> {
    if str.is_empty() {
        None
    } else {
        Some(str)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::thread;

    use tidb_exec::exec_details::{
        CommitDetails, CopExecDetails, ResolveLockDetail, ScanDetail, TimeDetail,
    };

    use super::*;

    /// Go `boTxnLockName`.
    const BO_TXN_LOCK_NAME: &str = "txnlock";

    /// Go `fakePlanDigestGenerator`.
    fn fake_plan_digest_generator() -> String {
        "point_get".to_owned()
    }

    /// Go `mockLazyInfo`.
    #[derive(Clone, Debug, Default)]
    struct MockLazyInfo {
        original_sql: String,
        plan: String,
        hint_str: String,
        bin_plan: String,
        plan_digest: String,
        binding_sql: String,
        binding_digest: String,
    }

    impl StmtExecLazyInfo for MockLazyInfo {
        fn original_sql(&self) -> String {
            self.original_sql.clone()
        }

        fn encoded_plan(&self) -> Result<(String, String), EncodedPlanError> {
            Ok((self.plan.clone(), self.hint_str.clone()))
        }

        fn binary_plan(&self) -> String {
            self.bin_plan.clone()
        }

        fn plan_digest(&self) -> String {
            self.plan_digest.clone()
        }

        fn binding_sql_and_digest(&self) -> (String, String) {
            (self.binding_sql.clone(), self.binding_digest.clone())
        }
    }

    fn mock_lazy_info(
        original_sql: &str,
        binding_sql: &str,
        binding_digest: &str,
    ) -> Arc<MockLazyInfo> {
        Arc::new(MockLazyInfo {
            original_sql: original_sql.to_owned(),
            binding_sql: binding_sql.to_owned(),
            binding_digest: binding_digest.to_owned(),
            ..MockLazyInfo::default()
        })
    }

    /// Go `time.Date(2019, 1, 1, h, m, s, 10, time.UTC)`.
    fn start_time(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        DateTime::from_timestamp(
            chrono::NaiveDate::from_ymd_opt(2019, 1, 1)
                .unwrap()
                .and_hms_opt(hour, minute, second)
                .unwrap()
                .and_utc()
                .timestamp(),
            10,
        )
        .unwrap()
    }

    /// Go `generateAnyExecInfo`.
    pub(crate) fn generate_any_exec_info() -> StmtExecInfo {
        let sc = StmtSummaryStmtCtx {
            stmt_type: "Select".to_owned(),
            tables: vec![
                TableEntry {
                    db: "db1".to_owned(),
                    table: "tb1".to_owned(),
                },
                TableEntry {
                    db: "db2".to_owned(),
                    table: "tb2".to_owned(),
                },
            ],
            index_names: vec!["a".to_owned()],
            ..StmtSummaryStmtCtx::default()
        };
        sc.is_tikv.store(true, Ordering::SeqCst);
        sc.is_tiflash.store(true, Ordering::SeqCst);
        sc.add_affected_rows(10000);

        StmtExecInfo {
            schema_name: "schema_name".to_owned(),
            charset: String::new(),
            collation: String::new(),
            normalized_sql: "normalized_sql".to_owned(),
            digest: "digest".to_owned(),
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
                tot_process_time: Duration::from_nanos(10000),
                max_wait_address: "128".to_owned(),
                max_wait_time: Duration::from_nanos(1500),
                tot_wait_time: Duration::from_nanos(1000),
            }),
            exec_detail: ExecDetails {
                request_count: 10,
                commit_detail: Some(CommitDetails {
                    get_commit_ts_time: Duration::from_nanos(100),
                    prewrite_time: Duration::from_nanos(10000),
                    commit_time: Duration::from_nanos(1000),
                    local_latch_time: Duration::from_nanos(10),
                    commit_backoff_time: 200,
                    prewrite_backoff_types: vec![BO_TXN_LOCK_NAME.to_owned()],
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
            mem_arbitration: 10000.0,
            disk_max: 10000,
            start_time: start_time(10, 10, 10),
            is_internal: false,
            succeed: true,
            plan_in_cache: false,
            plan_in_binding: false,
            exec_retry_count: 0,
            exec_retry_time: Duration::ZERO,
            write_sql_resp_duration: Duration::ZERO,
            result_rows: 0,
            tikv_exec_details: Some(TikvExecDetailsSnapshot {
                unpacked_bytes_sent_kv_total: 10,
                unpacked_bytes_received_kv_total: 1000,
                unpacked_bytes_received_kv_cross_zone: 1,
                unpacked_bytes_sent_kv_cross_zone: 100,
                ..TikvExecDetailsSnapshot::default()
            }),
            prepared: false,
            keyspace_name: String::new(),
            keyspace_id: 0,
            resource_group_name: "rg1".to_owned(),
            ru_detail: Some(RuDetailsSnapshot {
                rru: 1.1,
                wru: 2.5,
                ru_wait_duration: Duration::from_millis(2),
                ..RuDetailsSnapshot::default()
            }),
            total_ru_v2: 23456.0,
            cpu_usages: CpuUsages {
                tidb_cpu_time: Duration::from_nanos(20),
                tikv_cpu_time: Duration::from_nanos(100),
            },
            plan_cache_unqualified: String::new(),
            lazy_info: mock_lazy_info("original_sql1", "binding_sql1", "binding_digest1"),
        }
    }

    /// Go `matchStmtSummaryByDigest`.
    fn match_stmt_summary_by_digest(
        first: &StmtSummaryByDigest,
        second: &StmtSummaryByDigest,
    ) -> bool {
        if first.schema_name != second.schema_name
            || first.digest != second.digest
            || first.normalized_sql != second.normalized_sql
            || first.plan_digest != second.plan_digest
            || first.table_names != second.table_names
            || !first.stmt_type.eq_ignore_ascii_case(&second.stmt_type)
        {
            return false;
        }
        if first.history.len() != second.history.len() {
            return false;
        }
        for (ele1, ele2) in first.history.iter().zip(second.history.iter()) {
            let e1 = ele1.lock().unwrap();
            let e2 = ele2.lock().unwrap();
            let (s1, s2) = (&e1.stats, &e2.stats);
            if e1.begin_time != e2.begin_time
                || e1.end_time != e2.end_time
                || s1.sample_sql != s2.sample_sql
                || s1.sample_plan != s2.sample_plan
                || s1.prev_sql != s2.prev_sql
                || s1.exec_count != s2.exec_count
                || s1.sum_errors != s2.sum_errors
                || s1.sum_warnings != s2.sum_warnings
                || s1.sum_latency != s2.sum_latency
                || s1.max_latency != s2.max_latency
                || s1.min_latency != s2.min_latency
                || s1.sum_parse_latency != s2.sum_parse_latency
                || s1.max_parse_latency != s2.max_parse_latency
                || s1.sum_compile_latency != s2.sum_compile_latency
                || s1.max_compile_latency != s2.max_compile_latency
                || s1.sum_num_cop_tasks != s2.sum_num_cop_tasks
                || s1.sum_cop_process_time != s2.sum_cop_process_time
                || s1.max_cop_process_time != s2.max_cop_process_time
                || s1.max_cop_process_address != s2.max_cop_process_address
                || s1.sum_cop_wait_time != s2.sum_cop_wait_time
                || s1.max_cop_wait_time != s2.max_cop_wait_time
                || s1.max_cop_wait_address != s2.max_cop_wait_address
                || s1.sum_process_time != s2.sum_process_time
                || s1.max_process_time != s2.max_process_time
                || s1.sum_wait_time != s2.sum_wait_time
                || s1.max_wait_time != s2.max_wait_time
                || s1.sum_backoff_time != s2.sum_backoff_time
                || s1.max_backoff_time != s2.max_backoff_time
                || s1.sum_total_keys != s2.sum_total_keys
                || s1.max_total_keys != s2.max_total_keys
                || s1.sum_processed_keys != s2.sum_processed_keys
                || s1.max_processed_keys != s2.max_processed_keys
                || s1.sum_get_commit_ts_time != s2.sum_get_commit_ts_time
                || s1.max_get_commit_ts_time != s2.max_get_commit_ts_time
                || s1.sum_prewrite_time != s2.sum_prewrite_time
                || s1.max_prewrite_time != s2.max_prewrite_time
                || s1.sum_commit_time != s2.sum_commit_time
                || s1.max_commit_time != s2.max_commit_time
                || s1.sum_local_latch_time != s2.sum_local_latch_time
                || s1.max_local_latch_time != s2.max_local_latch_time
                || s1.sum_commit_backoff_time != s2.sum_commit_backoff_time
                || s1.max_commit_backoff_time != s2.max_commit_backoff_time
                || s1.sum_resolve_lock_time != s2.sum_resolve_lock_time
                || s1.max_resolve_lock_time != s2.max_resolve_lock_time
                || s1.sum_write_keys != s2.sum_write_keys
                || s1.max_write_keys != s2.max_write_keys
                || s1.sum_write_size != s2.sum_write_size
                || s1.max_write_size != s2.max_write_size
                || s1.sum_prewrite_region_num != s2.sum_prewrite_region_num
                || s1.max_prewrite_region_num != s2.max_prewrite_region_num
                || s1.sum_txn_retry != s2.sum_txn_retry
                || s1.max_txn_retry != s2.max_txn_retry
                || s1.sum_backoff_times != s2.sum_backoff_times
                || s1.sum_mem != s2.sum_mem
                || s1.max_mem != s2.max_mem
                || (s1.sum_mem_arbitration - s2.sum_mem_arbitration).abs() > f64::EPSILON
                || (s1.max_mem_arbitration - s2.max_mem_arbitration).abs() > f64::EPSILON
                || s1.sum_affected_rows != s2.sum_affected_rows
                || s1.first_seen != s2.first_seen
                || s1.last_seen != s2.last_seen
                || s1.resource_group_name != s2.resource_group_name
                || s1.ru != s2.ru
                || s1.network != s2.network
                || s1.storage_kv != s2.storage_kv
                || s1.storage_mpp != s2.storage_mpp
            {
                return false;
            }
            if s1.backoff_types != s2.backoff_types {
                return false;
            }
            if s1.index_names != s2.index_names {
                return false;
            }
        }
        true
    }

    /// Go `TestSetUp`.
    #[test]
    fn test_set_up() {
        let ss_map = StmtSummaryByDigestMap::new();
        ss_map.set_enabled(true);
        ss_map.set_refresh_interval(1800);
        ss_map.set_history_size(24);
        assert!(ss_map.enabled());
        assert_eq!(ss_map.refresh_interval(), 1800);
        assert_eq!(ss_map.history_size(), 24);
    }

    /// Go `TestAddStatement`.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_add_statement() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();
        ss_map.set_begin_time_for_cur_interval(now + 60);

        let sc = Arc::new(StmtSummaryStmtCtx {
            stmt_type: "Select".to_owned(),
            tables: vec![
                TableEntry {
                    db: "db1".to_owned(),
                    table: "tb1".to_owned(),
                },
                TableEntry {
                    db: "db2".to_owned(),
                    table: "tb2".to_owned(),
                },
            ],
            index_names: vec!["a".to_owned(), "b".to_owned()],
            ..StmtSummaryStmtCtx::default()
        });

        // first statement
        let mut info1 = generate_any_exec_info();
        info1
            .exec_detail
            .commit_detail
            .as_mut()
            .unwrap()
            .prewrite_backoff_types = Vec::new();
        let mut key = StmtDigestKey::new();
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        let (sample_plan, _) = info1.lazy_info.encoded_plan().unwrap();
        let commit1 = info1.exec_detail.commit_detail.clone().unwrap();
        let cop1 = info1.cop_tasks.clone().unwrap();
        let scan1 = info1
            .exec_detail
            .cop_exec_details
            .scan_detail
            .clone()
            .unwrap();
        let time1 = info1.exec_detail.cop_exec_details.time_detail.clone();
        let tikv1 = info1.tikv_exec_details.unwrap();
        let ru1 = info1.ru_detail.unwrap();

        let mut expected_element = StmtSummaryByDigestElement {
            begin_time: now + 60,
            end_time: now + 1860,
            stats: StmtSummaryStats {
                sample_sql: info1.lazy_info.original_sql(),
                sample_plan,
                index_names: info1.stmt_ctx.index_names.clone(),
                exec_count: 1,
                sum_latency: info1.total_latency,
                max_latency: info1.total_latency,
                min_latency: info1.total_latency,
                sum_parse_latency: info1.parse_latency,
                max_parse_latency: info1.parse_latency,
                sum_compile_latency: info1.compile_latency,
                max_compile_latency: info1.compile_latency,
                sum_num_cop_tasks: cop1.num_cop_tasks,
                sum_cop_process_time: cop1.tot_process_time,
                max_cop_process_time: cop1.max_process_time,
                max_cop_process_address: cop1.max_process_address.clone(),
                sum_cop_wait_time: cop1.tot_wait_time,
                max_cop_wait_time: cop1.max_wait_time,
                max_cop_wait_address: cop1.max_wait_address.clone(),
                sum_process_time: time1.process_time,
                max_process_time: time1.process_time,
                sum_wait_time: time1.wait_time,
                max_wait_time: time1.wait_time,
                sum_backoff_time: info1.exec_detail.cop_exec_details.backoff_time,
                max_backoff_time: info1.exec_detail.cop_exec_details.backoff_time,
                sum_total_keys: scan1.total_keys,
                max_total_keys: scan1.total_keys,
                sum_processed_keys: scan1.processed_keys,
                max_processed_keys: scan1.processed_keys,
                sum_get_commit_ts_time: commit1.get_commit_ts_time,
                max_get_commit_ts_time: commit1.get_commit_ts_time,
                sum_prewrite_time: commit1.prewrite_time,
                max_prewrite_time: commit1.prewrite_time,
                sum_commit_time: commit1.commit_time,
                max_commit_time: commit1.commit_time,
                sum_local_latch_time: commit1.local_latch_time,
                max_local_latch_time: commit1.local_latch_time,
                sum_commit_backoff_time: commit1.commit_backoff_time,
                max_commit_backoff_time: commit1.commit_backoff_time,
                sum_resolve_lock_time: commit1.resolve_lock.resolve_lock_time,
                max_resolve_lock_time: commit1.resolve_lock.resolve_lock_time,
                sum_write_keys: commit1.write_keys,
                max_write_keys: commit1.write_keys,
                sum_write_size: commit1.write_size,
                max_write_size: commit1.write_size,
                sum_prewrite_region_num: i64::from(commit1.prewrite_region_num),
                max_prewrite_region_num: commit1.prewrite_region_num,
                sum_txn_retry: commit1.txn_retry,
                max_txn_retry: commit1.txn_retry,
                backoff_types: HashMap::new(),
                sum_mem: info1.mem_max,
                max_mem: info1.mem_max,
                sum_disk: info1.disk_max,
                max_disk: info1.disk_max,
                sum_affected_rows: info1.stmt_ctx.affected_rows(),
                first_seen: info1.start_time,
                last_seen: info1.start_time,
                ru: StmtRuSummary {
                    sum_rru: ru1.rru,
                    max_rru: ru1.rru,
                    sum_wru: ru1.wru,
                    max_wru: ru1.wru,
                    sum_ru_wait_duration: ru1.ru_wait_duration,
                    max_ru_wait_duration: ru1.ru_wait_duration,
                    sum_ru_v2: info1.total_ru_v2,
                    max_ru_v2: info1.total_ru_v2,
                },
                resource_group_name: info1.resource_group_name.clone(),
                network: StmtNetworkTrafficSummary {
                    unpacked_bytes_sent_tikv_total: tikv1.unpacked_bytes_sent_kv_total,
                    unpacked_bytes_received_tikv_total: tikv1.unpacked_bytes_received_kv_total,
                    unpacked_bytes_sent_tikv_cross_zone: tikv1.unpacked_bytes_sent_kv_cross_zone,
                    unpacked_bytes_received_tikv_cross_zone: tikv1
                        .unpacked_bytes_received_kv_cross_zone,
                    unpacked_bytes_sent_tiflash_total: tikv1.unpacked_bytes_sent_mpp_total,
                    unpacked_bytes_received_tiflash_total: tikv1.unpacked_bytes_received_mpp_total,
                    unpacked_bytes_sent_tiflash_cross_zone: tikv1
                        .unpacked_bytes_sent_mpp_cross_zone,
                    unpacked_bytes_received_tiflash_cross_zone: tikv1
                        .unpacked_bytes_received_mpp_cross_zone,
                },
                storage_kv: info1.stmt_ctx.is_tikv.load(Ordering::SeqCst),
                storage_mpp: info1.stmt_ctx.is_tiflash.load(Ordering::SeqCst),
                sum_mem_arbitration: info1.mem_arbitration,
                max_mem_arbitration: info1.mem_arbitration,
                ..StmtSummaryStats::default()
            },
        };

        let build_expected = |element: &StmtSummaryByDigestElement, info: &StmtExecInfo| {
            let mut history = VecDeque::new();
            history.push_back(Arc::new(Mutex::new(element.clone())));
            StmtSummaryByDigest {
                schema_name: info.schema_name.clone(),
                stmt_type: info.stmt_ctx.stmt_type.clone(),
                digest: info.digest.clone(),
                normalized_sql: info.normalized_sql.clone(),
                plan_digest: info.plan_digest.clone(),
                table_names: "db1.tb1,db2.tb2".to_owned(),
                history,
                ..StmtSummaryByDigest::default()
            }
        };

        let expected_summary = build_expected(&expected_element, &info1);
        ss_map.add_statement(&info1);
        let summary = ss_map.summary_map_get(&key).expect("summary must exist");
        assert!(match_stmt_summary_by_digest(
            &summary.lock().unwrap(),
            &expected_summary
        ));

        // Second statement is similar with the first statement, and its values
        // are greater than that of the first statement.
        let info2 = StmtExecInfo {
            schema_name: "schema_name".to_owned(),
            normalized_sql: "normalized_sql".to_owned(),
            digest: "digest".to_owned(),
            plan_digest: "plan_digest".to_owned(),
            user: "user2".to_owned(),
            total_latency: Duration::from_nanos(20000),
            parse_latency: Duration::from_nanos(200),
            compile_latency: Duration::from_nanos(2000),
            cop_tasks: Some(CopTasksSummary {
                num_cop_tasks: 20,
                max_process_address: "200".to_owned(),
                max_process_time: Duration::from_nanos(25000),
                tot_process_time: Duration::from_nanos(40000),
                max_wait_address: "201".to_owned(),
                max_wait_time: Duration::from_nanos(2500),
                tot_wait_time: Duration::from_nanos(40000),
            }),
            exec_detail: ExecDetails {
                request_count: 20,
                commit_detail: Some(CommitDetails {
                    get_commit_ts_time: Duration::from_nanos(500),
                    prewrite_time: Duration::from_nanos(50000),
                    commit_time: Duration::from_nanos(5000),
                    local_latch_time: Duration::from_nanos(50),
                    commit_backoff_time: 1000,
                    prewrite_backoff_types: vec![BO_TXN_LOCK_NAME.to_owned()],
                    commit_backoff_types: Vec::new(),
                    write_keys: 100_000,
                    write_size: 1_000_000,
                    prewrite_region_num: 100,
                    txn_retry: 10,
                    resolve_lock: ResolveLockDetail {
                        resolve_lock_time: 10000,
                    },
                    ..CommitDetails::default()
                }),
                cop_exec_details: CopExecDetails {
                    backoff_time: Duration::from_nanos(180),
                    scan_detail: Some(ScanDetail {
                        total_keys: 6000,
                        processed_keys: 1500,
                        rocksdb_delete_skipped_count: 100,
                        rocksdb_key_skipped_count: 10,
                        rocksdb_block_cache_hit_count: 10,
                        rocksdb_block_read_count: 10,
                        rocksdb_block_read_byte: 1000,
                        ..ScanDetail::default()
                    }),
                    time_detail: TimeDetail {
                        process_time: Duration::from_nanos(1500),
                        wait_time: Duration::from_nanos(150),
                        ..TimeDetail::default()
                    },
                    callee_address: "202".to_owned(),
                },
                ..ExecDetails::default()
            },
            stmt_ctx: Arc::clone(&sc),
            mem_max: 20000,
            disk_max: 20000,
            start_time: start_time(10, 10, 20),
            succeed: true,
            ru_detail: Some(RuDetailsSnapshot {
                rru: 123.0,
                wru: 45.6,
                ru_wait_duration: Duration::from_secs(2),
                ..RuDetailsSnapshot::default()
            }),
            total_ru_v2: 34567.0,
            tikv_exec_details: Some(TikvExecDetailsSnapshot {
                unpacked_bytes_sent_kv_total: 100,
                unpacked_bytes_received_kv_total: 200,
                ..TikvExecDetailsSnapshot::default()
            }),
            resource_group_name: "rg1".to_owned(),
            lazy_info: mock_lazy_info("original_sql2", "binding_sql2", "binding_digest2"),
            mem_arbitration: 30000.0,
            charset: String::new(),
            collation: String::new(),
            prev_sql: String::new(),
            prev_sql_digest: String::new(),
            is_internal: false,
            plan_in_cache: false,
            plan_in_binding: false,
            exec_retry_count: 0,
            exec_retry_time: Duration::ZERO,
            write_sql_resp_duration: Duration::ZERO,
            result_rows: 0,
            prepared: false,
            keyspace_name: String::new(),
            keyspace_id: 0,
            cpu_usages: CpuUsages::default(),
            plan_cache_unqualified: String::new(),
        };
        info2.stmt_ctx.add_affected_rows(200);
        let commit2 = info2.exec_detail.commit_detail.clone().unwrap();
        let cop2 = info2.cop_tasks.clone().unwrap();
        let scan2 = info2
            .exec_detail
            .cop_exec_details
            .scan_detail
            .clone()
            .unwrap();
        let time2 = info2.exec_detail.cop_exec_details.time_detail.clone();
        let ru2 = info2.ru_detail.unwrap();
        {
            let s = &mut expected_element.stats;
            s.exec_count += 1;
            s.sum_latency += info2.total_latency;
            s.max_latency = info2.total_latency;
            s.sum_parse_latency += info2.parse_latency;
            s.max_parse_latency = info2.parse_latency;
            s.sum_compile_latency += info2.compile_latency;
            s.max_compile_latency = info2.compile_latency;
            s.sum_num_cop_tasks += cop2.num_cop_tasks;
            s.sum_cop_process_time += cop2.tot_process_time;
            s.max_cop_process_time = cop2.max_process_time;
            s.max_cop_process_address
                .clone_from(&cop2.max_process_address);
            s.sum_cop_wait_time += cop2.tot_wait_time;
            s.max_cop_wait_time = cop2.max_wait_time;
            s.max_cop_wait_address.clone_from(&cop2.max_wait_address);
            s.sum_process_time += time2.process_time;
            s.max_process_time = time2.process_time;
            s.sum_wait_time += time2.wait_time;
            s.max_wait_time = time2.wait_time;
            s.sum_backoff_time += info2.exec_detail.cop_exec_details.backoff_time;
            s.max_backoff_time = info2.exec_detail.cop_exec_details.backoff_time;
            s.sum_total_keys += scan2.total_keys;
            s.max_total_keys = scan2.total_keys;
            s.sum_processed_keys += scan2.processed_keys;
            s.max_processed_keys = scan2.processed_keys;
            s.sum_get_commit_ts_time += commit2.get_commit_ts_time;
            s.max_get_commit_ts_time = commit2.get_commit_ts_time;
            s.sum_prewrite_time += commit2.prewrite_time;
            s.max_prewrite_time = commit2.prewrite_time;
            s.sum_commit_time += commit2.commit_time;
            s.max_commit_time = commit2.commit_time;
            s.sum_local_latch_time += commit2.local_latch_time;
            s.max_local_latch_time = commit2.local_latch_time;
            s.sum_commit_backoff_time += commit2.commit_backoff_time;
            s.max_commit_backoff_time = commit2.commit_backoff_time;
            s.sum_resolve_lock_time += commit2.resolve_lock.resolve_lock_time;
            s.max_resolve_lock_time = commit2.resolve_lock.resolve_lock_time;
            s.sum_write_keys += commit2.write_keys;
            s.max_write_keys = commit2.write_keys;
            s.sum_write_size += commit2.write_size;
            s.max_write_size = commit2.write_size;
            s.sum_prewrite_region_num += i64::from(commit2.prewrite_region_num);
            s.max_prewrite_region_num = commit2.prewrite_region_num;
            s.sum_txn_retry += commit2.txn_retry;
            s.max_txn_retry = commit2.txn_retry;
            s.sum_backoff_times += 1;
            s.backoff_types.insert(BO_TXN_LOCK_NAME.to_owned(), 1);
            s.sum_mem += info2.mem_max;
            s.max_mem = info2.mem_max;
            s.max_mem_arbitration = info2.mem_arbitration;
            s.sum_mem_arbitration += info2.mem_arbitration;
            s.sum_disk += info2.disk_max;
            s.max_disk = info2.disk_max;
            s.sum_affected_rows += info2.stmt_ctx.affected_rows();
            s.last_seen = info2.start_time;
            s.ru.sum_rru += ru2.rru;
            s.ru.max_rru = ru2.rru;
            s.ru.sum_wru += ru2.wru;
            s.ru.max_wru = ru2.wru;
            s.ru.sum_ru_wait_duration += ru2.ru_wait_duration;
            s.ru.max_ru_wait_duration = ru2.ru_wait_duration;
            s.ru.sum_ru_v2 += info2.total_ru_v2;
            s.ru.max_ru_v2 = info2.total_ru_v2;
            s.network.add(info2.tikv_exec_details.as_ref());
            s.storage_kv = info2.stmt_ctx.is_tikv.load(Ordering::SeqCst);
            s.storage_mpp = info2.stmt_ctx.is_tiflash.load(Ordering::SeqCst);
        }

        let expected_summary = build_expected(&expected_element, &info1);
        ss_map.add_statement(&info2);
        let summary = ss_map.summary_map_get(&key).expect("summary must exist");
        assert!(match_stmt_summary_by_digest(
            &summary.lock().unwrap(),
            &expected_summary
        ));

        // Third statement is similar with the first statement, and its values
        // are less than that of the first statement.
        let info3 = StmtExecInfo {
            user: String::from("user3"),
            total_latency: Duration::from_nanos(1000),
            parse_latency: Duration::from_nanos(50),
            compile_latency: Duration::from_nanos(500),
            cop_tasks: Some(CopTasksSummary {
                num_cop_tasks: 2,
                max_process_address: "300".to_owned(),
                max_process_time: Duration::from_nanos(350),
                tot_process_time: Duration::from_nanos(200),
                max_wait_address: "301".to_owned(),
                max_wait_time: Duration::from_nanos(250),
                tot_wait_time: Duration::from_nanos(40),
            }),
            exec_detail: ExecDetails {
                request_count: 2,
                commit_detail: Some(CommitDetails {
                    get_commit_ts_time: Duration::from_nanos(50),
                    prewrite_time: Duration::from_nanos(5000),
                    commit_time: Duration::from_nanos(500),
                    local_latch_time: Duration::from_nanos(5),
                    commit_backoff_time: 100,
                    prewrite_backoff_types: vec![BO_TXN_LOCK_NAME.to_owned()],
                    commit_backoff_types: Vec::new(),
                    write_keys: 10000,
                    write_size: 100_000,
                    prewrite_region_num: 10,
                    txn_retry: 1,
                    resolve_lock: ResolveLockDetail {
                        resolve_lock_time: 1000,
                    },
                    ..CommitDetails::default()
                }),
                cop_exec_details: CopExecDetails {
                    backoff_time: Duration::from_nanos(18),
                    scan_detail: Some(ScanDetail {
                        total_keys: 600,
                        processed_keys: 150,
                        rocksdb_delete_skipped_count: 100,
                        rocksdb_key_skipped_count: 10,
                        rocksdb_block_cache_hit_count: 10,
                        rocksdb_block_read_count: 10,
                        rocksdb_block_read_byte: 1000,
                        ..ScanDetail::default()
                    }),
                    time_detail: TimeDetail {
                        process_time: Duration::from_nanos(150),
                        wait_time: Duration::from_nanos(15),
                        ..TimeDetail::default()
                    },
                    callee_address: "302".to_owned(),
                },
                ..ExecDetails::default()
            },
            stmt_ctx: Arc::clone(&sc),
            mem_max: 200,
            disk_max: 200,
            start_time: start_time(10, 10, 0),
            ru_detail: Some(RuDetailsSnapshot {
                rru: 0.12,
                wru: 0.34,
                ru_wait_duration: Duration::from_micros(5),
                ..RuDetailsSnapshot::default()
            }),
            total_ru_v2: 123.0,
            tikv_exec_details: Some(TikvExecDetailsSnapshot {
                unpacked_bytes_sent_kv_total: 1,
                unpacked_bytes_received_kv_total: 300,
                unpacked_bytes_sent_mpp_total: 1,
                unpacked_bytes_received_mpp_total: 300,
                ..TikvExecDetailsSnapshot::default()
            }),
            lazy_info: mock_lazy_info("original_sql3", "binding_sql3", "binding_digest3"),
            mem_arbitration: 200.0,
            ..info2
        };
        info3.stmt_ctx.add_affected_rows(20000);
        let commit3 = info3.exec_detail.commit_detail.clone().unwrap();
        let cop3 = info3.cop_tasks.clone().unwrap();
        let scan3 = info3
            .exec_detail
            .cop_exec_details
            .scan_detail
            .clone()
            .unwrap();
        let time3 = info3.exec_detail.cop_exec_details.time_detail.clone();
        let ru3 = info3.ru_detail.unwrap();
        {
            let s = &mut expected_element.stats;
            s.exec_count += 1;
            s.sum_latency += info3.total_latency;
            s.min_latency = info3.total_latency;
            s.sum_parse_latency += info3.parse_latency;
            s.sum_compile_latency += info3.compile_latency;
            s.sum_num_cop_tasks += cop3.num_cop_tasks;
            s.sum_cop_process_time += cop3.tot_process_time;
            s.sum_cop_wait_time += cop3.tot_wait_time;
            s.sum_process_time += time3.process_time;
            s.sum_wait_time += time3.wait_time;
            s.sum_backoff_time += info3.exec_detail.cop_exec_details.backoff_time;
            s.sum_total_keys += scan3.total_keys;
            s.sum_processed_keys += scan3.processed_keys;
            s.sum_get_commit_ts_time += commit3.get_commit_ts_time;
            s.sum_prewrite_time += commit3.prewrite_time;
            s.sum_commit_time += commit3.commit_time;
            s.sum_local_latch_time += commit3.local_latch_time;
            s.sum_commit_backoff_time += commit3.commit_backoff_time;
            s.sum_resolve_lock_time += commit3.resolve_lock.resolve_lock_time;
            s.sum_write_keys += commit3.write_keys;
            s.sum_write_size += commit3.write_size;
            s.sum_prewrite_region_num += i64::from(commit3.prewrite_region_num);
            s.sum_txn_retry += commit3.txn_retry;
            s.sum_backoff_times += 1;
            s.backoff_types.insert(BO_TXN_LOCK_NAME.to_owned(), 2);
            s.sum_mem += info3.mem_max;
            s.sum_mem_arbitration += info3.mem_arbitration;
            s.sum_disk += info3.disk_max;
            s.sum_affected_rows += info3.stmt_ctx.affected_rows();
            s.first_seen = info3.start_time;
            s.ru.sum_rru += ru3.rru;
            s.ru.sum_wru += ru3.wru;
            s.ru.sum_ru_wait_duration += ru3.ru_wait_duration;
            s.ru.sum_ru_v2 += info3.total_ru_v2;
            s.network.add(info3.tikv_exec_details.as_ref());
            s.storage_kv = info3.stmt_ctx.is_tikv.load(Ordering::SeqCst);
            s.storage_mpp = info3.stmt_ctx.is_tiflash.load(Ordering::SeqCst);
        }

        let expected_summary = build_expected(&expected_element, &info1);
        ss_map.add_statement(&info3);
        let summary = ss_map.summary_map_get(&key).expect("summary must exist");
        assert!(match_stmt_summary_by_digest(
            &summary.lock().unwrap(),
            &expected_summary
        ));

        // Fourth statement is in a different schema. Go aliases the pointer, so
        // the mutations land on `info1` itself.
        info1.schema_name = "schema2".to_owned();
        info1.exec_detail.commit_detail = None;
        let mut key = StmtDigestKey::new();
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 2);
        assert!(ss_map.summary_map_get(&key).is_some());

        // Fifth statement has a different digest.
        info1.digest = "digest2".to_owned();
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 3);
        assert!(ss_map.summary_map_get(&key).is_some());

        // Sixth statement has a different plan digest.
        info1.plan_digest = "plan_digest2".to_owned();
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 4);
        assert!(ss_map.summary_map_get(&key).is_some());

        // Test for plan too large
        info1.plan_digest = "plan_digest7".to_owned();
        let buf = "a".repeat(MAX_ENCODED_PLAN_SIZE_IN_BYTES.load(Ordering::SeqCst) + 1);
        let original_sql = info1.lazy_info.original_sql();
        info1.lazy_info = Arc::new(MockLazyInfo {
            original_sql: original_sql.clone(),
            plan: buf,
            binding_sql: original_sql,
            ..MockLazyInfo::default()
        });
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 5);
        let value = ss_map.summary_map_get(&key).expect("summary must exist");
        let stmt = value.lock().unwrap();
        let hash = key.hash();
        assert!(hash
            .windows(stmt.schema_name.len())
            .any(|w| w == stmt.schema_name.as_bytes()));
        assert!(hash
            .windows(stmt.digest.len())
            .any(|w| w == stmt.digest.as_bytes()));
        assert!(hash
            .windows(stmt.plan_digest.len())
            .any(|w| w == stmt.plan_digest.as_bytes()));
        let element = stmt.history.back().unwrap().lock().unwrap();
        assert_eq!(element.stats.sample_plan, PLAN_DISCARDED_ENCODED);
    }

    /// Go `TestAddStatementParallel`.
    ///
    /// Go asserts on `reader.GetStmtSummaryCurrentRows()`; `reader.go` is not
    /// ported yet, so the record count is read off the map instead.
    #[test]
    fn test_add_statement_parallel() {
        let ss_map = Arc::new(StmtSummaryByDigestMap::new());
        let now = unix_now();
        // to disable expiration
        ss_map.set_begin_time_for_cur_interval(now + 60);

        let threads = 8;
        let loops = 32;
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let ss_map = Arc::clone(&ss_map);
                thread::spawn(move || {
                    let mut info = generate_any_exec_info();
                    for i in 0..loops {
                        info.digest = format!("digest{i}");
                        ss_map.add_statement(&info);
                    }
                    assert_eq!(ss_map.summary_map_size(), loops);
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(ss_map.summary_map_size(), loops);
    }

    /// Go `TestMaxStmtCount`.
    #[test]
    fn test_max_stmt_count() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();
        // to disable expiration
        ss_map.set_begin_time_for_cur_interval(now + 60);

        // Test the original value and modify it.
        assert_eq!(ss_map.max_stmt_count(), 3000);
        ss_map.set_max_stmt_count(10).unwrap();
        assert_eq!(ss_map.max_stmt_count(), 10);

        // 100 digests
        let mut info = generate_any_exec_info();
        let loops = 100;
        for i in 0..loops {
            info.digest = format!("digest{i}");
            ss_map.add_statement(&info);
        }

        // Summary count should be MaxStmtCount.
        assert_eq!(ss_map.summary_map_size(), 10);

        // LRU cache should work.
        for i in (loops - 10)..loops {
            let mut key = StmtDigestKey::new();
            key.init(
                &info.schema_name,
                &format!("digest{i}"),
                "",
                &info.plan_digest,
                &info.resource_group_name,
                "",
            );
            assert!(ss_map.summary_map_get(&key).is_some());
        }

        // Change to a bigger value.
        ss_map.set_max_stmt_count(50).unwrap();
        for i in 0..loops {
            info.digest = format!("digest{i}");
            ss_map.add_statement(&info);
        }
        assert_eq!(ss_map.summary_map_size(), 50);

        // Change to a smaller value.
        ss_map.set_max_stmt_count(10).unwrap();
        for i in 0..loops {
            info.digest = format!("digest{i}");
            ss_map.add_statement(&info);
        }
        assert_eq!(ss_map.summary_map_size(), 10);

        ss_map.set_max_stmt_count(3000).unwrap();
    }

    /// Go `TestMaxSQLLength`.
    #[test]
    fn test_max_sql_length() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();
        // to disable expiration
        ss_map.set_begin_time_for_cur_interval(now + 60);

        // Test the original value and modify it.
        let max_sql_length = ss_map.max_sql_length();
        assert_eq!(max_sql_length, 32768);

        // Create a long SQL
        let length = max_sql_length * 10;
        let str = "a".repeat(length);

        let mut info = generate_any_exec_info();
        info.lazy_info = Arc::new(MockLazyInfo {
            original_sql: str.clone(),
            ..MockLazyInfo::default()
        });
        info.normalized_sql.clone_from(&str);
        ss_map.add_statement(&info);

        let mut key = StmtDigestKey::new();
        key.init(
            &info.schema_name,
            &info.digest,
            "",
            &info.plan_digest,
            &info.resource_group_name,
            "",
        );
        let value = ss_map.summary_map_get(&key).expect("summary must exist");

        let expected_sql = format!("{}(len:{length})", "a".repeat(max_sql_length));
        let summary = value.lock().unwrap();
        assert_eq!(summary.normalized_sql, expected_sql);
        let element = summary.history.back().unwrap().lock().unwrap();
        assert_eq!(element.stats.sample_sql, expected_sql);
        drop(element);
        drop(summary);

        ss_map.set_max_sql_length(100);
        assert_eq!(ss_map.max_sql_length(), 100);
        ss_map.set_max_sql_length(10);
        assert_eq!(ss_map.max_sql_length(), 10);
        ss_map.set_max_sql_length(32768);
        assert_eq!(ss_map.max_sql_length(), 32768);
    }

    /// Go `TestFormatSQLClone`.
    #[test]
    fn test_format_sql_clone() {
        let base = "x".repeat(1024);
        let sub = &base[100..200];

        let formatted = format_sql(sub);

        assert_eq!(formatted, sub);
        // Verify that the formatted string is a true clone, not pointing to the
        // same underlying data.
        assert!(!std::ptr::eq(sub.as_ptr(), formatted.as_ptr()));
    }

    /// Go `TestSetMaxStmtCountParallel`.
    ///
    /// Go asserts through the reader (one live record plus one evicted row);
    /// with `reader.go` and `evicted.go` deferred, only the live record count
    /// is checked.
    #[test]
    fn test_set_max_stmt_count_parallel() {
        let ss_map = Arc::new(StmtSummaryByDigestMap::new());
        let now = unix_now();
        // to disable expiration
        ss_map.set_begin_time_for_cur_interval(now + 60);

        let threads = 8;
        const LOOPS: i32 = 20;
        fn add_stmt(ss_map: &StmtSummaryByDigestMap) {
            let mut info = generate_any_exec_info();
            for i in 0..LOOPS {
                info.digest = format!("digest{i}");
                ss_map.add_statement(&info);
            }
        }

        let mut handles: Vec<_> = (0..threads)
            .map(|_| {
                let ss_map = Arc::clone(&ss_map);
                thread::spawn(move || add_stmt(&ss_map))
            })
            .collect();

        {
            let ss_map = Arc::clone(&ss_map);
            handles.push(thread::spawn(move || {
                // Turn down MaxStmtCount one by one.
                for i in (1..=10).rev() {
                    ss_map.set_max_stmt_count(i).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // add stmt again to make sure evict occurs after SetMaxStmtCount.
        add_stmt(&ss_map);

        assert_eq!(ss_map.summary_map_size(), 1);
        ss_map.set_max_stmt_count(3000).unwrap();
    }

    /// Go `TestDisableStmtSummary`.
    ///
    /// Go counts reader rows; the record count is read off the map instead.
    #[test]
    fn test_disable_stmt_summary() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();

        ss_map.set_enabled(false);
        ss_map.set_begin_time_for_cur_interval(now + 60);

        let mut info1 = generate_any_exec_info();
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 0);

        ss_map.set_enabled(true);

        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 1);

        ss_map.set_begin_time_for_cur_interval(now + 60);

        info1.lazy_info = mock_lazy_info("original_sql2", "binding_sql1", "binding_digest1");
        info1.normalized_sql = "normalized_sql2".to_owned();
        info1.digest = "digest2".to_owned();
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 2);

        // Unset
        ss_map.set_enabled(false);
        ss_map.set_begin_time_for_cur_interval(now + 60);
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 0);

        // Unset
        ss_map.set_enabled(false);
        ss_map.set_enabled(true);

        ss_map.set_begin_time_for_cur_interval(now + 60);
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 1);

        // Set back.
        ss_map.set_enabled(true);
    }

    /// Go `TestEnableSummaryParallel`.
    ///
    /// Go's concurrent reader call is replaced by a concurrent read of the
    /// cached values, which exercises the same lock ordering.
    #[test]
    fn test_enable_summary_parallel() {
        let ss_map = Arc::new(StmtSummaryByDigestMap::new());

        let threads = 8;
        let loops = 32;
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let ss_map = Arc::clone(&ss_map);
                thread::spawn(move || {
                    let info = generate_any_exec_info();
                    // Add 32 times with the same digest.
                    for i in 0..loops {
                        // Sometimes enable it and sometimes disable it.
                        ss_map.set_enabled(i % 2 == 0);
                        ss_map.add_statement(&info);
                        // Try to read it.
                        let _ = ss_map.summary_map_values();
                    }
                    ss_map.set_enabled(true);
                })
            })
            .collect();
        // Ensure that there's no deadlock.
        for handle in handles {
            handle.join().unwrap();
        }

        // Ensure that it's enabled at last.
        assert!(ss_map.enabled());
    }

    /// Go `TestFormatBackoffTypes`.
    #[test]
    fn test_format_backoff_types() {
        let mut backoff_map: HashMap<String, i64> = HashMap::new();
        assert_eq!(format_backoff_types(&backoff_map), None);
        backoff_map.insert("pdrpc".to_owned(), 1);
        assert_eq!(
            format_backoff_types(&backoff_map).as_deref(),
            Some("pdrpc:1")
        );
        backoff_map.insert("txnlock".to_owned(), 2);

        assert_eq!(
            format_backoff_types(&backoff_map).as_deref(),
            Some("txnlock:2,pdrpc:1")
        );
    }

    /// Go `TestRefreshCurrentSummary`.
    #[test]
    fn test_refresh_current_summary() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();

        ss_map.set_begin_time_for_cur_interval(now + 10);
        let info1 = generate_any_exec_info();
        let mut key = StmtDigestKey::new();
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 1);
        let value = ss_map.summary_map_get(&key).expect("summary must exist");
        let element = value
            .lock()
            .unwrap()
            .history
            .back()
            .map(Arc::clone)
            .unwrap();
        assert_eq!(
            element.lock().unwrap().begin_time,
            ss_map.begin_time_for_cur_interval()
        );
        assert_eq!(element.lock().unwrap().stats.exec_count, 1);

        ss_map.set_begin_time_for_cur_interval(now - 1900);
        element.lock().unwrap().begin_time = now - 1900;
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 1);
        let value = ss_map.summary_map_get(&key).expect("summary must exist");
        assert_eq!(value.lock().unwrap().history.len(), 2);
        let element = value
            .lock()
            .unwrap()
            .history
            .back()
            .map(Arc::clone)
            .unwrap();
        assert!(element.lock().unwrap().begin_time > now - 1900);
        assert_eq!(element.lock().unwrap().stats.exec_count, 1);

        ss_map.set_refresh_interval(10);
        ss_map.set_begin_time_for_cur_interval(now - 20);
        element.lock().unwrap().begin_time = now - 20;
        ss_map.add_statement(&info1);
        assert_eq!(value.lock().unwrap().history.len(), 3);
    }

    /// Go `TestSummaryHistory`.
    ///
    /// Go's reader-row and `ssMap.other` assertions belong to `reader.go` and
    /// `evicted.go` and are dropped here.
    #[test]
    fn test_summary_history() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();
        ss_map.set_refresh_interval(10);
        ss_map.set_history_size(10);

        let mut info1 = generate_any_exec_info();
        let mut key = StmtDigestKey::new();
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        for i in 0..11_i64 {
            ss_map.set_begin_time_for_cur_interval(now + (i + 1) * 10);
            ss_map.add_statement(&info1);
            assert_eq!(ss_map.summary_map_size(), 1);
            let value = ss_map.summary_map_get(&key).expect("summary must exist");
            let ssbd = value.lock().unwrap();
            if i < 10 {
                assert_eq!(ssbd.history.len(), usize::try_from(i + 1).unwrap());
                let element = ssbd.history.back().unwrap().lock().unwrap();
                assert_eq!(element.begin_time, ss_map.begin_time_for_cur_interval());
                assert_eq!(element.stats.exec_count, 1);
            } else {
                assert_eq!(ssbd.history.len(), 10);
                assert_eq!(
                    ssbd.history.back().unwrap().lock().unwrap().begin_time,
                    ss_map.begin_time_for_cur_interval()
                );
                assert_eq!(
                    ssbd.history.front().unwrap().lock().unwrap().begin_time,
                    now + 20
                );
            }
        }

        // test eviction
        ss_map.clear();
        ss_map.set_max_stmt_count(1).unwrap();
        // insert first digest
        for i in 0..6_i64 {
            ss_map.set_begin_time_for_cur_interval(now + i * 10);
            ss_map.add_statement(&info1);
            assert_eq!(ss_map.summary_map_size(), 1);
        }
        // insert another digest to evict it
        info1.digest = "bandit digest".to_owned();
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 1);

        ss_map.set_max_stmt_count(3000).unwrap();
        ss_map.set_refresh_interval(1800);
        ss_map.set_history_size(24);
    }

    /// Go `TestPrevSQL`.
    #[test]
    fn test_prev_sql() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();
        // to disable expiration
        ss_map.set_begin_time_for_cur_interval(now + 60);

        let mut info1 = generate_any_exec_info();
        info1.prev_sql = "prevSQL".to_owned();
        info1.prev_sql_digest = "prevSQLDigest".to_owned();
        ss_map.add_statement(&info1);
        let mut key = StmtDigestKey::new();
        key.init(
            &info1.schema_name,
            &info1.digest,
            &info1.prev_sql_digest,
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        assert_eq!(ss_map.summary_map_size(), 1);
        assert!(ss_map.summary_map_get(&key).is_some());

        // same prevSQL
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 1);

        // different prevSQL
        info1.prev_sql = "prevSQL1".to_owned();
        info1.prev_sql_digest = "prevSQLDigest1".to_owned();
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 2);
        key.init(
            &info1.schema_name,
            &info1.digest,
            &info1.prev_sql_digest,
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        assert!(ss_map.summary_map_get(&key).is_some());
    }

    /// Go `TestEndTime`.
    #[test]
    fn test_end_time() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();
        ss_map.set_begin_time_for_cur_interval(now - 100);

        let info1 = generate_any_exec_info();
        ss_map.add_statement(&info1);
        let mut key = StmtDigestKey::new();
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            &info1.plan_digest,
            &info1.resource_group_name,
            "",
        );
        assert_eq!(ss_map.summary_map_size(), 1);
        let value = ss_map.summary_map_get(&key).expect("summary must exist");
        {
            let ssbd = value.lock().unwrap();
            let element = ssbd.history.back().unwrap().lock().unwrap();
            assert_eq!(element.begin_time, now - 100);
            assert_eq!(element.end_time, now + 1700);
        }

        ss_map.set_refresh_interval(3600);
        ss_map.add_statement(&info1);
        {
            let ssbd = value.lock().unwrap();
            assert_eq!(ssbd.history.len(), 1);
            let element = ssbd.history.back().unwrap().lock().unwrap();
            assert_eq!(element.begin_time, now - 100);
            assert_eq!(element.end_time, now + 3500);
        }

        ss_map.set_refresh_interval(60);
        ss_map.add_statement(&info1);
        let now2 = unix_now();
        {
            let ssbd = value.lock().unwrap();
            assert_eq!(ssbd.history.len(), 2);
            let element = ssbd.history.front().unwrap().lock().unwrap();
            assert_eq!(element.begin_time, now - 100);
            assert!(element.end_time >= now);
            assert!(element.end_time <= now2);
            drop(element);
            let element = ssbd.history.back().unwrap().lock().unwrap();
            assert!(element.begin_time >= now - 60);
            assert!(element.begin_time <= now2);
            assert_eq!(element.end_time - element.begin_time, 60);
        }

        ss_map.set_refresh_interval(1800);
    }

    /// Go `TestPointGet`.
    #[test]
    fn test_point_get() {
        let ss_map = StmtSummaryByDigestMap::new();
        let now = unix_now();
        ss_map.set_begin_time_for_cur_interval(now - 100);

        let mut info1 = generate_any_exec_info();
        info1.plan_digest = String::new();
        info1.lazy_info = Arc::new(MockLazyInfo {
            original_sql: "original_sql1".to_owned(),
            plan: fake_plan_digest_generator(),
            binding_sql: "binding_sql1".to_owned(),
            binding_digest: "binding_digest1".to_owned(),
            ..MockLazyInfo::default()
        });
        ss_map.add_statement(&info1);
        let mut key = StmtDigestKey::new();
        key.init(
            &info1.schema_name,
            &info1.digest,
            "",
            "",
            &info1.resource_group_name,
            "",
        );
        assert_eq!(ss_map.summary_map_size(), 1);
        let value = ss_map.summary_map_get(&key).expect("summary must exist");
        let element = value
            .lock()
            .unwrap()
            .history
            .back()
            .map(Arc::clone)
            .unwrap();
        assert_eq!(element.lock().unwrap().stats.exec_count, 1);

        ss_map.add_statement(&info1);
        assert_eq!(element.lock().unwrap().stats.exec_count, 2);
    }

    /// Go `TestAddStatementGroupByUser`.
    #[test]
    fn test_add_statement_group_by_user() {
        let ss_map = StmtSummaryByDigestMap::new();

        let mut info1 = generate_any_exec_info();
        info1.user = "alice".to_owned();
        let mut info2 = generate_any_exec_info();
        info2.user = "bob".to_owned();

        // Flag off: both statements collapse into one record.
        ss_map.add_statement(&info1);
        ss_map.add_statement(&info2);
        assert_eq!(ss_map.summary_map_size(), 1);

        // Flipping the flag clears prior data (different grouping key).
        ss_map.set_group_by_user(true);
        assert_eq!(ss_map.summary_map_size(), 0);

        ss_map.add_statement(&info1);
        ss_map.add_statement(&info2);
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 2);

        // With grouping ON, each record's authUsers must hold exactly one user
        // — the one that groups it.
        let mut seen: HashSet<String> = HashSet::new();
        for value in ss_map.summary_map_values() {
            let ssbd = value.lock().unwrap();
            let element = ssbd.history.front().unwrap().lock().unwrap();
            assert_eq!(element.stats.auth_users.len(), 1);
            for user in &element.stats.auth_users {
                seen.insert(user.clone());
            }
        }
        assert!(seen.contains("alice"));
        assert!(seen.contains("bob"));

        // Flipping back off clears again, and re-emitted records merge users.
        ss_map.set_group_by_user(false);
        assert_eq!(ss_map.summary_map_size(), 0);
        ss_map.add_statement(&info1);
        ss_map.add_statement(&info2);
        assert_eq!(ss_map.summary_map_size(), 1);
        for value in ss_map.summary_map_values() {
            let ssbd = value.lock().unwrap();
            let element = ssbd.history.front().unwrap().lock().unwrap();
            assert_eq!(element.stats.auth_users.len(), 2);
        }
    }

    /// Go `TestStmtDigestKeyBoundary`.
    #[test]
    fn test_stmt_digest_key_boundary() {
        let mut k1 = StmtDigestKey::new();
        k1.init("schema", "digest", "prev", "plan", "rg", "alice");
        let mut k2 = StmtDigestKey::new();
        k2.init("schema", "digest", "prev", "plan", "rga", "lice");
        assert_ne!(
            k1.hash(),
            k2.hash(),
            "user segment must have an unambiguous boundary"
        );

        // user="" leaves the hash equal to the legacy 5-field layout.
        let mut off = StmtDigestKey::new();
        off.init("schema", "digest", "prev", "plan", "rg", "");
        let mut legacy: Vec<u8> = Vec::new();
        legacy.extend_from_slice(b"digest");
        legacy.extend_from_slice(b"schema");
        legacy.extend_from_slice(b"prev");
        legacy.extend_from_slice(b"plan");
        legacy.extend_from_slice(b"rg");
        assert_eq!(off.hash(), legacy.as_slice());
    }
}
