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

//! Go `pkg/util/stmtsummary/reader.go`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone};
use chrono_tz::Tz;
use tidb_datatype::{core_time_from_datetime, Datum, Time, TimeType};
use tidb_model::ColumnInfo;
use tidb_parser::auth::UserIdentity;

use crate::statement_summary::{
    avg_float, avg_float4_uint, avg_int, avg_sum_float, convert_empty_to_nil, format_backoff_types,
    StmtSummaryByDigest, StmtSummaryByDigestElement, StmtSummaryByDigestMap, StmtSummaryStats,
    STMT_SUMMARY_BY_DIGEST_MAP,
};

/// Go `stmtSummaryReader`: reads the statement summaries data and converts it
/// to `[]types.Datum` rows.
///
/// Go holds `ssMap *stmtSummaryByDigestMap`, which
/// [`NewStmtSummaryReader`](StmtSummaryReader::new) points at the global
/// [`STMT_SUMMARY_BY_DIGEST_MAP`]; the borrow makes the same aliasing explicit,
/// and the field stays public because Go's tests reassign it.
pub struct StmtSummaryReader<'a> {
    /// Go `user`.
    pub user: Option<UserIdentity>,
    /// Go `hasProcessPriv`: if the user has the `PROCESS` privilege, they can
    /// read all the statements.
    pub has_process_priv: bool,
    /// Go `columns`.
    columns: Vec<ColumnInfo>,
    /// Go `instanceAddr`.
    instance_addr: String,
    /// Go `ssMap`.
    pub ss_map: &'a StmtSummaryByDigestMap,
    /// Go `columnValueFactories`.
    column_value_factories: Vec<ColumnValueFactory>,
    /// Go `checker`.
    checker: Option<StmtSummaryChecker>,
    /// Go `tz *time.Location`.
    tz: Tz,
}

impl std::fmt::Debug for StmtSummaryReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StmtSummaryReader")
            .field("user", &self.user)
            .field("has_process_priv", &self.has_process_priv)
            .field("columns", &self.column_names())
            .field("instance_addr", &self.instance_addr)
            .field("checker", &self.checker)
            .field("tz", &self.tz)
            .finish_non_exhaustive()
    }
}

impl StmtSummaryReader<'static> {
    /// Go `NewStmtSummaryReader`: returns a new statement summaries reader.
    ///
    /// # Panics
    ///
    /// Go panics when a column has no registered factory; so does this.
    #[must_use]
    pub fn new(
        user: Option<UserIdentity>,
        has_process_priv: bool,
        cols: Vec<ColumnInfo>,
        instance_addr: String,
        tz: Tz,
    ) -> Self {
        // initialize column value factories.
        let column_value_factories = cols
            .iter()
            .map(|col| {
                column_value_factory(col.name.original()).unwrap_or_else(|| {
                    panic!(
                        "should never happen, should register new column {} into columnValueFactoryMap",
                        col.name.original()
                    )
                })
            })
            .collect();
        Self {
            user,
            has_process_priv,
            columns: cols,
            instance_addr,
            ss_map: &STMT_SUMMARY_BY_DIGEST_MAP,
            column_value_factories,
            checker: None,
            tz,
        }
    }
}

impl StmtSummaryReader<'_> {
    /// The `Name.O` of every column this reader was built for.
    #[must_use]
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|col| col.name.original()).collect()
    }

    /// Go `(*stmtSummaryReader).SetChecker`.
    pub fn set_checker(&mut self, checker: Option<StmtSummaryChecker>) {
        self.checker = checker;
    }

    /// Go `(*stmtSummaryReader).GetStmtSummaryCumulativeRows`: gets statement
    /// summary rows with cumulative metrics.
    ///
    /// Go takes `ssMap`'s lock once around reading `summaryMap.Values()`; the
    /// ported map exposes that read as its own locked accessor, so the lock is
    /// taken and released inside the call.
    #[must_use]
    pub fn get_stmt_summary_cumulative_rows(&self) -> Vec<Vec<Datum>> {
        let values = self.ss_map.summary_map_values();

        let mut rows = Vec::with_capacity(values.len());
        for value in &values {
            let ssbd = value.lock().unwrap();
            if let Some(checker) = &self.checker {
                if !checker.is_digest_valid(&ssbd.digest) {
                    continue;
                }
            }
            if let Some(record) = self.get_stmt_by_digest_cumulative_row(&ssbd) {
                rows.push(record);
            }
        }
        rows
    }

    /// Go `(*stmtSummaryReader).GetStmtSummaryCurrentRows`: gets all current
    /// statement summaries rows.
    ///
    /// Go reads `summaryMap.Values()`, `beginTimeForCurInterval` and `other`
    /// under one `ssMap` lock; here each is its own locked accessor, so a
    /// concurrent `AddStatement` can interleave between them.
    #[must_use]
    pub fn get_stmt_summary_current_rows(&self) -> Vec<Vec<Datum>> {
        let values = self.ss_map.summary_map_values();
        let begin_time = self.ss_map.begin_time_for_cur_interval();

        let mut rows = Vec::with_capacity(values.len());
        for value in &values {
            {
                let ssbd = value.lock().unwrap();
                if let Some(checker) = &self.checker {
                    if !checker.is_digest_valid(&ssbd.digest) {
                        continue;
                    }
                }
            }
            if let Some(record) = self.get_stmt_by_digest_row(value, begin_time) {
                rows.push(record);
            }
        }
        if self.checker.is_none() {
            if let Some(other_datum) = self.get_stmt_evicted_other_row() {
                rows.push(other_datum);
            }
        }
        rows
    }

    /// Go `(*stmtSummaryReader).GetStmtSummaryHistoryRows`: gets all history
    /// statement summaries rows.
    #[must_use]
    pub fn get_stmt_summary_history_rows(&self) -> Vec<Vec<Datum>> {
        let values = self.ss_map.summary_map_values();

        let history_size = self.ss_map.history_size();
        let mut rows = Vec::with_capacity(values.len() * history_size);
        for value in &values {
            rows.extend(self.get_stmt_by_digest_history_row(value, history_size));
        }

        if self.checker.is_none() {
            rows.extend(self.get_stmt_evicted_other_history_row(history_size));
        }
        rows
    }

    /// Go `(*stmtSummaryReader).isAuthed`.
    fn is_authed(&self, ss_stats: &StmtSummaryStats) -> bool {
        match &self.user {
            Some(user) if !self.has_process_priv => ss_stats.auth_users.contains(&user.username),
            _ => true,
        }
    }

    /// Go `(*stmtSummaryReader).getStmtByDigestCumulativeRow`.
    ///
    /// Go locks `ssbd` here; the caller already holds that lock, because the
    /// digest string it filters on lives behind the same mutex.
    fn get_stmt_by_digest_cumulative_row(&self, ssbd: &StmtSummaryByDigest) -> Option<Vec<Datum>> {
        if !self.is_authed(&ssbd.cumulative) {
            return None;
        }

        Some(
            self.column_value_factories
                .iter()
                .map(|factory| factory(self, None, Some(ssbd), &ssbd.cumulative))
                .collect(),
        )
    }

    /// Go `(*stmtSummaryReader).getStmtByDigestRow`.
    ///
    /// Go drops `ssbd`'s lock before locking the element and then reads
    /// `ssbd`'s immutable-after-init fields unlocked; here the `ssbd` guard is
    /// held across the element lock. The order (`ssbd` then element) is the one
    /// `AddStatement` already uses, so it introduces no new lock cycle.
    fn get_stmt_by_digest_row(
        &self,
        ssbd: &Arc<Mutex<StmtSummaryByDigest>>,
        begin_time_for_cur_interval: i64,
    ) -> Option<Vec<Datum>> {
        let ssbd = ssbd.lock().unwrap();
        if !ssbd.initialized {
            return None;
        }
        let ss_element = ssbd.history.back()?;
        let ss_element = ss_element.lock().unwrap();

        // `ssElement` is lazy expired, so expired elements could also be read.
        if ss_element.begin_time < begin_time_for_cur_interval {
            return None;
        }
        self.get_stmt_by_digest_element_row(&ss_element, &ssbd)
    }

    /// Go `(*stmtSummaryReader).getStmtByDigestElementRow`.
    ///
    /// Go locks `ssElement`; here the caller holds that lock and hands over a
    /// shared reference, exactly as `evicted.go`'s ported entry points do.
    fn get_stmt_by_digest_element_row(
        &self,
        ss_element: &StmtSummaryByDigestElement,
        ssbd: &StmtSummaryByDigest,
    ) -> Option<Vec<Datum>> {
        if !self.is_authed(&ss_element.stats) {
            return None;
        }

        Some(
            self.column_value_factories
                .iter()
                .map(|factory| factory(self, Some(ss_element), Some(ssbd), &ss_element.stats))
                .collect(),
        )
    }

    /// Go `(*stmtSummaryReader).getStmtByDigestHistoryRow`.
    fn get_stmt_by_digest_history_row(
        &self,
        ssbd: &Arc<Mutex<StmtSummaryByDigest>>,
        history_size: usize,
    ) -> Vec<Vec<Datum>> {
        // Collect all history summaries to an array.
        let ssbd = ssbd.lock().unwrap();
        let ss_elements = ssbd.collect_history_summaries(self.checker.as_ref(), history_size);

        let mut rows = Vec::with_capacity(ss_elements.len());
        for ss_element in &ss_elements {
            let ss_element = ss_element.lock().unwrap();
            if let Some(record) = self.get_stmt_by_digest_element_row(&ss_element, &ssbd) {
                rows.push(record);
            }
        }
        rows
    }

    /// Go `(*stmtSummaryReader).getStmtEvictedOtherRow`.
    ///
    /// Go reads `ssMap.other`, which is always the `evicted.go` rollup; a map
    /// built by [`StmtSummaryByDigestMap::with_sinks`] with a different sink
    /// has no rollup to read and yields no row.
    fn get_stmt_evicted_other_row(&self) -> Option<Vec<Datum>> {
        let ssbde = self.ss_map.evicted()?;
        let ssbde = ssbde.lock().unwrap();
        let se_element = ssbde.history().back()?;

        self.get_stmt_by_digest_element_row(
            &se_element.other_summary,
            &StmtSummaryByDigest::default(),
        )
    }

    /// Go `(*stmtSummaryReader).getStmtEvictedOtherHistoryRow`.
    fn get_stmt_evicted_other_history_row(&self, history_size: usize) -> Vec<Vec<Datum>> {
        // Collect all history summaries to an array.
        let Some(ssbde) = self.ss_map.evicted() else {
            return Vec::new();
        };
        let ssbde = ssbde.lock().unwrap();
        let se_elements = ssbde.collect_history_summaries(history_size);
        let mut rows = Vec::with_capacity(se_elements.len());

        let ssbd = StmtSummaryByDigest::default();
        for se_element in se_elements {
            if let Some(record) =
                self.get_stmt_by_digest_element_row(&se_element.other_summary, &ssbd)
            {
                rows.push(record);
            }
        }
        rows
    }
}

/// Go `stmtSummaryChecker`.
///
/// Go's `set.StringSet` narrows to a [`HashSet`].
#[derive(Clone, Debug, Default)]
pub struct StmtSummaryChecker {
    digests: HashSet<String>,
}

impl StmtSummaryChecker {
    /// Go `NewStmtSummaryChecker`: returns a new statement summaries checker.
    #[must_use]
    pub fn new(digests: HashSet<String>) -> Self {
        Self { digests }
    }

    /// Go `(*stmtSummaryChecker).isDigestValid`.
    #[must_use]
    pub fn is_digest_valid(&self, digest: &str) -> bool {
        self.digests.contains(digest)
    }
}

// Statements summary table column name.

/// Go `ClusterTableInstanceColumnNameStr`.
pub const CLUSTER_TABLE_INSTANCE_COLUMN_NAME_STR: &str = "INSTANCE";
/// Go `SummaryBeginTimeStr`.
pub const SUMMARY_BEGIN_TIME_STR: &str = "SUMMARY_BEGIN_TIME";
/// Go `SummaryEndTimeStr`.
pub const SUMMARY_END_TIME_STR: &str = "SUMMARY_END_TIME";
/// Go `StmtTypeStr`.
pub const STMT_TYPE_STR: &str = "STMT_TYPE";
/// Go `SchemaNameStr`.
pub const SCHEMA_NAME_STR: &str = "SCHEMA_NAME";
/// Go `DigestStr`.
pub const DIGEST_STR: &str = "DIGEST";
/// Go `DigestTextStr`.
pub const DIGEST_TEXT_STR: &str = "DIGEST_TEXT";
/// Go `TableNamesStr`.
pub const TABLE_NAMES_STR: &str = "TABLE_NAMES";
/// Go `IndexNamesStr`.
pub const INDEX_NAMES_STR: &str = "INDEX_NAMES";
/// Go `SampleUserStr`.
pub const SAMPLE_USER_STR: &str = "SAMPLE_USER";
/// Go `ExecCountStr`.
pub const EXEC_COUNT_STR: &str = "EXEC_COUNT";
/// Go `SumErrorsStr`.
pub const SUM_ERRORS_STR: &str = "SUM_ERRORS";
/// Go `SumWarningsStr`.
pub const SUM_WARNINGS_STR: &str = "SUM_WARNINGS";
/// Go `SumLatencyStr`.
pub const SUM_LATENCY_STR: &str = "SUM_LATENCY";
/// Go `MaxLatencyStr`.
pub const MAX_LATENCY_STR: &str = "MAX_LATENCY";
/// Go `MinLatencyStr`.
pub const MIN_LATENCY_STR: &str = "MIN_LATENCY";
/// Go `AvgLatencyStr`.
pub const AVG_LATENCY_STR: &str = "AVG_LATENCY";
/// Go `AvgParseLatencyStr`.
pub const AVG_PARSE_LATENCY_STR: &str = "AVG_PARSE_LATENCY";
/// Go `MaxParseLatencyStr`.
pub const MAX_PARSE_LATENCY_STR: &str = "MAX_PARSE_LATENCY";
/// Go `AvgCompileLatencyStr`.
pub const AVG_COMPILE_LATENCY_STR: &str = "AVG_COMPILE_LATENCY";
/// Go `MaxCompileLatencyStr`.
pub const MAX_COMPILE_LATENCY_STR: &str = "MAX_COMPILE_LATENCY";
/// Go `SumCopTaskNumStr`.
pub const SUM_COP_TASK_NUM_STR: &str = "SUM_COP_TASK_NUM";
/// Go `MaxCopProcessTimeStr`.
pub const MAX_COP_PROCESS_TIME_STR: &str = "MAX_COP_PROCESS_TIME";
/// Go `MaxCopProcessAddressStr`.
pub const MAX_COP_PROCESS_ADDRESS_STR: &str = "MAX_COP_PROCESS_ADDRESS";
/// Go `MaxCopWaitTimeStr`.
pub const MAX_COP_WAIT_TIME_STR: &str = "MAX_COP_WAIT_TIME";
/// Go `MaxCopWaitAddressStr`.
pub const MAX_COP_WAIT_ADDRESS_STR: &str = "MAX_COP_WAIT_ADDRESS";
/// Go `AvgProcessTimeStr`.
pub const AVG_PROCESS_TIME_STR: &str = "AVG_PROCESS_TIME";
/// Go `MaxProcessTimeStr`.
pub const MAX_PROCESS_TIME_STR: &str = "MAX_PROCESS_TIME";
/// Go `AvgWaitTimeStr`.
pub const AVG_WAIT_TIME_STR: &str = "AVG_WAIT_TIME";
/// Go `MaxWaitTimeStr`.
pub const MAX_WAIT_TIME_STR: &str = "MAX_WAIT_TIME";
/// Go `AvgBackoffTimeStr`.
pub const AVG_BACKOFF_TIME_STR: &str = "AVG_BACKOFF_TIME";
/// Go `MaxBackoffTimeStr`.
pub const MAX_BACKOFF_TIME_STR: &str = "MAX_BACKOFF_TIME";
/// Go `AvgTotalKeysStr`.
pub const AVG_TOTAL_KEYS_STR: &str = "AVG_TOTAL_KEYS";
/// Go `MaxTotalKeysStr`.
pub const MAX_TOTAL_KEYS_STR: &str = "MAX_TOTAL_KEYS";
/// Go `AvgProcessedKeysStr`.
pub const AVG_PROCESSED_KEYS_STR: &str = "AVG_PROCESSED_KEYS";
/// Go `MaxProcessedKeysStr`.
pub const MAX_PROCESSED_KEYS_STR: &str = "MAX_PROCESSED_KEYS";
/// Go `AvgRocksdbDeleteSkippedCountStr`.
pub const AVG_ROCKSDB_DELETE_SKIPPED_COUNT_STR: &str = "AVG_ROCKSDB_DELETE_SKIPPED_COUNT";
/// Go `MaxRocksdbDeleteSkippedCountStr`.
pub const MAX_ROCKSDB_DELETE_SKIPPED_COUNT_STR: &str = "MAX_ROCKSDB_DELETE_SKIPPED_COUNT";
/// Go `AvgRocksdbKeySkippedCountStr`.
pub const AVG_ROCKSDB_KEY_SKIPPED_COUNT_STR: &str = "AVG_ROCKSDB_KEY_SKIPPED_COUNT";
/// Go `MaxRocksdbKeySkippedCountStr`.
pub const MAX_ROCKSDB_KEY_SKIPPED_COUNT_STR: &str = "MAX_ROCKSDB_KEY_SKIPPED_COUNT";
/// Go `AvgRocksdbBlockCacheHitCountStr`.
pub const AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR: &str = "AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT";
/// Go `MaxRocksdbBlockCacheHitCountStr`.
pub const MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR: &str = "MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT";
/// Go `AvgRocksdbBlockReadCountStr`.
pub const AVG_ROCKSDB_BLOCK_READ_COUNT_STR: &str = "AVG_ROCKSDB_BLOCK_READ_COUNT";
/// Go `MaxRocksdbBlockReadCountStr`.
pub const MAX_ROCKSDB_BLOCK_READ_COUNT_STR: &str = "MAX_ROCKSDB_BLOCK_READ_COUNT";
/// Go `AvgRocksdbBlockReadByteStr`.
pub const AVG_ROCKSDB_BLOCK_READ_BYTE_STR: &str = "AVG_ROCKSDB_BLOCK_READ_BYTE";
/// Go `MaxRocksdbBlockReadByteStr`.
pub const MAX_ROCKSDB_BLOCK_READ_BYTE_STR: &str = "MAX_ROCKSDB_BLOCK_READ_BYTE";
/// Go `AvgIARemoteReadSegmentCountStr`.
pub const AVG_IA_REMOTE_READ_SEGMENT_COUNT_STR: &str = "AVG_IA_REMOTE_READ_SEGMENT_COUNT";
/// Go `MaxIARemoteReadSegmentCountStr`.
pub const MAX_IA_REMOTE_READ_SEGMENT_COUNT_STR: &str = "MAX_IA_REMOTE_READ_SEGMENT_COUNT";
/// Go `AvgIARemoteReadSegmentSizeStr`.
pub const AVG_IA_REMOTE_READ_SEGMENT_SIZE_STR: &str = "AVG_IA_REMOTE_READ_SEGMENT_SIZE";
/// Go `MaxIARemoteReadSegmentSizeStr`.
pub const MAX_IA_REMOTE_READ_SEGMENT_SIZE_STR: &str = "MAX_IA_REMOTE_READ_SEGMENT_SIZE";
/// Go `AvgIARemoteReadSegmentWaitTimeStr`.
pub const AVG_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR: &str = "AVG_IA_REMOTE_READ_SEGMENT_WAIT_TIME";
/// Go `MaxIARemoteReadSegmentWaitTimeStr`.
pub const MAX_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR: &str = "MAX_IA_REMOTE_READ_SEGMENT_WAIT_TIME";
/// Go `AvgPrewriteTimeStr`.
pub const AVG_PREWRITE_TIME_STR: &str = "AVG_PREWRITE_TIME";
/// Go `MaxPrewriteTimeStr`.
pub const MAX_PREWRITE_TIME_STR: &str = "MAX_PREWRITE_TIME";
/// Go `AvgCommitTimeStr`.
pub const AVG_COMMIT_TIME_STR: &str = "AVG_COMMIT_TIME";
/// Go `MaxCommitTimeStr`.
pub const MAX_COMMIT_TIME_STR: &str = "MAX_COMMIT_TIME";
/// Go `AvgGetCommitTsTimeStr`.
pub const AVG_GET_COMMIT_TS_TIME_STR: &str = "AVG_GET_COMMIT_TS_TIME";
/// Go `MaxGetCommitTsTimeStr`.
pub const MAX_GET_COMMIT_TS_TIME_STR: &str = "MAX_GET_COMMIT_TS_TIME";
/// Go `AvgCommitBackoffTimeStr`.
pub const AVG_COMMIT_BACKOFF_TIME_STR: &str = "AVG_COMMIT_BACKOFF_TIME";
/// Go `MaxCommitBackoffTimeStr`.
pub const MAX_COMMIT_BACKOFF_TIME_STR: &str = "MAX_COMMIT_BACKOFF_TIME";
/// Go `AvgResolveLockTimeStr`.
pub const AVG_RESOLVE_LOCK_TIME_STR: &str = "AVG_RESOLVE_LOCK_TIME";
/// Go `MaxResolveLockTimeStr`.
pub const MAX_RESOLVE_LOCK_TIME_STR: &str = "MAX_RESOLVE_LOCK_TIME";
/// Go `AvgLocalLatchWaitTimeStr`.
pub const AVG_LOCAL_LATCH_WAIT_TIME_STR: &str = "AVG_LOCAL_LATCH_WAIT_TIME";
/// Go `MaxLocalLatchWaitTimeStr`.
pub const MAX_LOCAL_LATCH_WAIT_TIME_STR: &str = "MAX_LOCAL_LATCH_WAIT_TIME";
/// Go `AvgWriteKeysStr`.
pub const AVG_WRITE_KEYS_STR: &str = "AVG_WRITE_KEYS";
/// Go `MaxWriteKeysStr`.
pub const MAX_WRITE_KEYS_STR: &str = "MAX_WRITE_KEYS";
/// Go `AvgWriteSizeStr`.
pub const AVG_WRITE_SIZE_STR: &str = "AVG_WRITE_SIZE";
/// Go `MaxWriteSizeStr`.
pub const MAX_WRITE_SIZE_STR: &str = "MAX_WRITE_SIZE";
/// Go `AvgPrewriteRegionsStr`.
pub const AVG_PREWRITE_REGIONS_STR: &str = "AVG_PREWRITE_REGIONS";
/// Go `MaxPrewriteRegionsStr`.
pub const MAX_PREWRITE_REGIONS_STR: &str = "MAX_PREWRITE_REGIONS";
/// Go `AvgTxnRetryStr`.
pub const AVG_TXN_RETRY_STR: &str = "AVG_TXN_RETRY";
/// Go `MaxTxnRetryStr`.
pub const MAX_TXN_RETRY_STR: &str = "MAX_TXN_RETRY";
/// Go `SumExecRetryStr`.
pub const SUM_EXEC_RETRY_STR: &str = "SUM_EXEC_RETRY";
/// Go `SumExecRetryTimeStr`.
pub const SUM_EXEC_RETRY_TIME_STR: &str = "SUM_EXEC_RETRY_TIME";
/// Go `SumBackoffTimesStr`.
pub const SUM_BACKOFF_TIMES_STR: &str = "SUM_BACKOFF_TIMES";
/// Go `BackoffTypesStr`.
pub const BACKOFF_TYPES_STR: &str = "BACKOFF_TYPES";
/// Go `AvgMemStr`.
pub const AVG_MEM_STR: &str = "AVG_MEM";
/// Go `MaxMemStr`.
pub const MAX_MEM_STR: &str = "MAX_MEM";
/// Go `AvgMemArbitrationStr`.
pub const AVG_MEM_ARBITRATION_STR: &str = "AVG_MEM_ARBITRATION";
/// Go `MaxMemArbitrationStr`.
pub const MAX_MEM_ARBITRATION_STR: &str = "MAX_MEM_ARBITRATION";
/// Go `AvgDiskStr`.
pub const AVG_DISK_STR: &str = "AVG_DISK";
/// Go `MaxDiskStr`.
pub const MAX_DISK_STR: &str = "MAX_DISK";
/// Go `AvgKvTimeStr`.
pub const AVG_KV_TIME_STR: &str = "AVG_KV_TIME";
/// Go `AvgPdTimeStr`.
pub const AVG_PD_TIME_STR: &str = "AVG_PD_TIME";
/// Go `AvgBackoffTotalTimeStr`.
pub const AVG_BACKOFF_TOTAL_TIME_STR: &str = "AVG_BACKOFF_TOTAL_TIME";
/// Go `AvgWriteSQLRespTimeStr`.
pub const AVG_WRITE_SQL_RESP_TIME_STR: &str = "AVG_WRITE_SQL_RESP_TIME";
/// Go `AvgTidbCPUTimeStr`.
pub const AVG_TIDB_CPU_TIME_STR: &str = "AVG_TIDB_CPU_TIME";
/// Go `AvgTikvCPUTimeStr`.
pub const AVG_TIKV_CPU_TIME_STR: &str = "AVG_TIKV_CPU_TIME";
/// Go `MaxResultRowsStr`.
pub const MAX_RESULT_ROWS_STR: &str = "MAX_RESULT_ROWS";
/// Go `MinResultRowsStr`.
pub const MIN_RESULT_ROWS_STR: &str = "MIN_RESULT_ROWS";
/// Go `AvgResultRowsStr`.
pub const AVG_RESULT_ROWS_STR: &str = "AVG_RESULT_ROWS";
/// Go `PreparedStr`.
pub const PREPARED_STR: &str = "PREPARED";
/// Go `AvgAffectedRowsStr`.
pub const AVG_AFFECTED_ROWS_STR: &str = "AVG_AFFECTED_ROWS";
/// Go `FirstSeenStr`.
pub const FIRST_SEEN_STR: &str = "FIRST_SEEN";
/// Go `LastSeenStr`.
pub const LAST_SEEN_STR: &str = "LAST_SEEN";
/// Go `PlanInCacheStr`.
pub const PLAN_IN_CACHE_STR: &str = "PLAN_IN_CACHE";
/// Go `PlanCacheHitsStr`.
pub const PLAN_CACHE_HITS_STR: &str = "PLAN_CACHE_HITS";
/// Go `PlanCacheUnqualifiedStr`.
pub const PLAN_CACHE_UNQUALIFIED_STR: &str = "PLAN_CACHE_UNQUALIFIED";
/// Go `PlanCacheUnqualifiedLastReasonStr`.
pub const PLAN_CACHE_UNQUALIFIED_LAST_REASON_STR: &str = "PLAN_CACHE_UNQUALIFIED_LAST_REASON";
/// Go `PlanInBindingStr`.
pub const PLAN_IN_BINDING_STR: &str = "PLAN_IN_BINDING";
/// Go `QuerySampleTextStr`.
pub const QUERY_SAMPLE_TEXT_STR: &str = "QUERY_SAMPLE_TEXT";
/// Go `PrevSampleTextStr`.
pub const PREV_SAMPLE_TEXT_STR: &str = "PREV_SAMPLE_TEXT";
/// Go `PlanDigestStr`.
pub const PLAN_DIGEST_STR: &str = "PLAN_DIGEST";
/// Go `PlanStr`.
pub const PLAN_STR: &str = "PLAN";
/// Go `BinaryPlan`.
pub const BINARY_PLAN: &str = "BINARY_PLAN";
/// Go `BindingDigestStr`.
pub const BINDING_DIGEST_STR: &str = "BINDING_DIGEST";
/// Go `BindingDigestTextStr`.
pub const BINDING_DIGEST_TEXT_STR: &str = "BINDING_DIGEST_TEXT";
/// Go `Charset`.
pub const CHARSET: &str = "CHARSET";
/// Go `Collation`.
pub const COLLATION: &str = "COLLATION";
/// Go `PlanHint`.
pub const PLAN_HINT: &str = "PLAN_HINT";
/// Go `AvgRequestUnitReadStr`.
pub const AVG_REQUEST_UNIT_READ_STR: &str = "AVG_REQUEST_UNIT_READ";
/// Go `MaxRequestUnitReadStr`.
pub const MAX_REQUEST_UNIT_READ_STR: &str = "MAX_REQUEST_UNIT_READ";
/// Go `AvgRequestUnitWriteStr`.
pub const AVG_REQUEST_UNIT_WRITE_STR: &str = "AVG_REQUEST_UNIT_WRITE";
/// Go `MaxRequestUnitWriteStr`.
pub const MAX_REQUEST_UNIT_WRITE_STR: &str = "MAX_REQUEST_UNIT_WRITE";
/// Go `AvgQueuedRcTimeStr`.
pub const AVG_QUEUED_RC_TIME_STR: &str = "AVG_QUEUED_RC_TIME";
/// Go `MaxQueuedRcTimeStr`.
pub const MAX_QUEUED_RC_TIME_STR: &str = "MAX_QUEUED_RC_TIME";
/// Go `AvgRequestUnitV2Str`.
pub const AVG_REQUEST_UNIT_V2_STR: &str = "AVG_REQUEST_UNIT_V2";
/// Go `MaxRequestUnitV2Str`.
pub const MAX_REQUEST_UNIT_V2_STR: &str = "MAX_REQUEST_UNIT_V2";
/// Go `ResourceGroupName`.
pub const RESOURCE_GROUP_NAME: &str = "RESOURCE_GROUP";
/// Go `SumUnpackedBytesSentTiKVTotalStr`.
pub const SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL_STR: &str = "SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL";
/// Go `SumUnpackedBytesReceivedTiKVTotalStr`.
pub const SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL_STR: &str =
    "SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL";
/// Go `SumUnpackedBytesSentTiKVCrossZoneStr`.
pub const SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE_STR: &str =
    "SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE";
/// Go `SumUnpackedBytesReceivedTiKVCrossZoneStr`.
pub const SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE_STR: &str =
    "SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE";
/// Go `SumUnpackedBytesSentTiFlashTotalStr`.
pub const SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL_STR: &str = "SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL";
/// Go `SumUnpackedBytesReceivedTiFlashTotalStr`.
pub const SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL_STR: &str =
    "SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL";
/// Go `SumUnpackedBytesSentTiFlashCrossZoneStr`.
pub const SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE_STR: &str =
    "SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE";
/// Go `SumUnpackedBytesReceiveTiFlashCrossZoneStr`.
pub const SUM_UNPACKED_BYTES_RECEIVE_TIFLASH_CROSS_ZONE_STR: &str =
    "SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE";
/// Go `StorageKVStr`.
pub const STORAGE_KV_STR: &str = "STORAGE_KV";
/// Go `StorageMPPStr`.
pub const STORAGE_MPP_STR: &str = "STORAGE_MPP";

// Column names for the statement stats table, including columns that have been
// renamed from their equivalent columns in the statement summary table.

/// Go `ErrorsStr`.
pub const ERRORS_STR: &str = "ERRORS";
/// Go `WarningsStr`.
pub const WARNINGS_STR: &str = "WARNINGS";
/// Go `MemStr`.
pub const MEM_STR: &str = "MEM";
/// Go `MemArbitrationStr`.
pub const MEM_ARBITRATION_STR: &str = "MEM_ARBITRATION";
/// Go `DiskStr`.
pub const DISK_STR: &str = "DISK";
/// Go `TotalTimeStr`.
pub const TOTAL_TIME_STR: &str = "TOTAL_TIME";
/// Go `ParseTimeStr`.
pub const PARSE_TIME_STR: &str = "PARSE_TIME";
/// Go `CompileTimeStr`.
pub const COMPILE_TIME_STR: &str = "COMPILE_TIME";
/// Go `CopTaskNumStr`.
pub const COP_TASK_NUM_STR: &str = "COP_TASK_NUM";
/// Go `CopProcessTimeStr`.
pub const COP_PROCESS_TIME_STR: &str = "COP_PROCESS_TIME";
/// Go `CopWaitTimeStr`.
pub const COP_WAIT_TIME_STR: &str = "COP_WAIT_TIME";
/// Go `PdTimeStr`.
pub const PD_TIME_STR: &str = "PD_TIME";
/// Go `KvTimeStr`.
pub const KV_TIME_STR: &str = "KV_TIME";
/// Go `ProcessTimeStr`.
pub const PROCESS_TIME_STR: &str = "PROCESS_TIME";
/// Go `WaitTimeStr`.
pub const WAIT_TIME_STR: &str = "WAIT_TIME";
/// Go `BackoffTimeStr`.
pub const BACKOFF_TIME_STR: &str = "BACKOFF_TIME";
/// Go `TotalKeysStr`.
pub const TOTAL_KEYS_STR: &str = "TOTAL_KEYS";
/// Go `ProcessedKeysStr`.
pub const PROCESSED_KEYS_STR: &str = "PROCESSED_KEYS";
/// Go `RocksdbDeleteSkippedCountStr`.
pub const ROCKSDB_DELETE_SKIPPED_COUNT_STR: &str = "ROCKSDB_DELETE_SKIPPED_COUNT";
/// Go `RocksdbKeySkippedCountStr`.
pub const ROCKSDB_KEY_SKIPPED_COUNT_STR: &str = "ROCKSDB_KEY_SKIPPED_COUNT";
/// Go `RocksdbBlockCacheHitCountStr`.
pub const ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR: &str = "ROCKSDB_BLOCK_CACHE_HIT_COUNT";
/// Go `RocksdbBlockReadCountStr`.
pub const ROCKSDB_BLOCK_READ_COUNT_STR: &str = "ROCKSDB_BLOCK_READ_COUNT";
/// Go `RocksdbBlockReadByteStr`.
pub const ROCKSDB_BLOCK_READ_BYTE_STR: &str = "ROCKSDB_BLOCK_READ_BYTE";
/// Go `PrewriteTimeStr`.
pub const PREWRITE_TIME_STR: &str = "PREWRITE_TIME";
/// Go `CommitTimeStr`.
pub const COMMIT_TIME_STR: &str = "COMMIT_TIME";
/// Go `CommitTsTimeStr`.
pub const COMMIT_TS_TIME_STR: &str = "COMMIT_TS_TIME";
/// Go `CommitBackoffTimeStr`.
pub const COMMIT_BACKOFF_TIME_STR: &str = "COMMIT_BACKOFF_TIME";
/// Go `ResolveLockTimeStr`.
pub const RESOLVE_LOCK_TIME_STR: &str = "RESOLVE_LOCK_TIME";
/// Go `LocalLatchWaitTimeStr`.
pub const LOCAL_LATCH_WAIT_TIME_STR: &str = "LOCAL_LATCH_WAIT_TIME";
/// Go `WriteKeysStr`.
pub const WRITE_KEYS_STR: &str = "WRITE_KEYS";
/// Go `WriteSizeStr`.
pub const WRITE_SIZE_STR: &str = "WRITE_SIZE";
/// Go `PrewriteRegionsStr`.
pub const PREWRITE_REGIONS_STR: &str = "PREWRITE_REGIONS";
/// Go `TxnRetryStr`.
pub const TXN_RETRY_STR: &str = "TXN_RETRY";
/// Go `ExecRetryStr`.
pub const EXEC_RETRY_STR: &str = "EXEC_RETRY";
/// Go `ExecRetryTimeStr`.
pub const EXEC_RETRY_TIME_STR: &str = "EXEC_RETRY_TIME";
/// Go `BackoffTimesStr`.
pub const BACKOFF_TIMES_STR: &str = "BACKOFF_TIMES";
/// Go `BackoffTotalTimeStr`.
pub const BACKOFF_TOTAL_TIME_STR: &str = "BACKOFF_TOTAL_TIME";
/// Go `WriteSQLRespTimeStr`.
pub const WRITE_SQL_RESP_TIME_STR: &str = "WRITE_SQL_RESP_TIME";
/// Go `ResultRowsStr`.
pub const RESULT_ROWS_STR: &str = "RESULT_ROWS";
/// Go `AffectedRowsStr`.
pub const AFFECTED_ROWS_STR: &str = "AFFECTED_ROWS";
/// Go `RequestUnitReadStr`.
pub const REQUEST_UNIT_READ_STR: &str = "REQUEST_UNIT_READ";
/// Go `RequestUnitWriteStr`.
pub const REQUEST_UNIT_WRITE_STR: &str = "REQUEST_UNIT_WRITE";
/// Go `QueuedRcTimeStr`.
pub const QUEUED_RC_TIME_STR: &str = "QUEUED_RC_TIME";
/// Go `UnpackedBytesSentTiKVTotalStr`.
pub const UNPACKED_BYTES_SENT_TIKV_TOTAL_STR: &str = "UNPACKED_BYTES_SENT_TIKV_TOTAL";
/// Go `UnpackedBytesReceivedTiKVTotalStr`.
pub const UNPACKED_BYTES_RECEIVED_TIKV_TOTAL_STR: &str = "UNPACKED_BYTES_RECEIVED_TIKV_TOTAL";
/// Go `UnpackedBytesSentTiKVCrossZoneStr`.
pub const UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE_STR: &str = "UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE";
/// Go `UnpackedBytesReceivedTiKVCrossZoneStr`.
pub const UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE_STR: &str =
    "UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE";
/// Go `UnpackedBytesSentTiFlashTotalStr`.
pub const UNPACKED_BYTES_SENT_TIFLASH_TOTAL_STR: &str = "UNPACKED_BYTES_SENT_TIFLASH_TOTAL";
/// Go `UnpackedBytesReceivedTiFlashTotalStr`.
pub const UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL_STR: &str = "UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL";
/// Go `UnpackedBytesSentTiFlashCrossZoneStr`.
pub const UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE_STR: &str =
    "UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE";
/// Go `UnpackedBytesReceiveTiFlashCrossZoneStr`.
pub const UNPACKED_BYTES_RECEIVE_TIFLASH_CROSS_ZONE_STR: &str =
    "UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE";

/// Go `columnValueFactory`.
///
/// Go's `any` return goes through `types.NewDatum`; this returns the [`Datum`]
/// that conversion produces, so the kind each column yields is fixed here.
pub type ColumnValueFactory = fn(
    reader: &StmtSummaryReader<'_>,
    ss_element: Option<&StmtSummaryByDigestElement>,
    ssbd: Option<&StmtSummaryByDigest>,
    ss_stats: &StmtSummaryStats,
) -> Datum;

/// Go `int64(d)` on a `time.Duration`: its nanosecond count.
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

/// Go `types.NewTime(types.FromGoTime(t.In(reader.tz)), mysql.TypeTimestamp, 0)`.
fn timestamp_datum<TZ: TimeZone>(instant: DateTime<TZ>) -> Datum {
    let core = core_time_from_datetime(instant);
    // Go's `NewTime` cannot fail; fsp 0 is always a valid fsp here.
    let time = Time::new(core, TimeType::Timestamp, 0).unwrap_or_else(|error| {
        unreachable!("fsp 0 is always valid for a timestamp: {error:?}");
    });
    Datum::new_time(time)
}

/// Go `time.Unix(seconds, 0).In(tz)`.
fn unix_seconds_in(seconds: i64, tz: Tz) -> DateTime<Tz> {
    DateTime::from_timestamp(seconds, 0)
        .unwrap_or_else(|| DateTime::from_timestamp_nanos(0))
        .with_timezone(&tz)
}

/// The element a column factory needs, which Go dereferences without a nil
/// check because only the interval tables select those columns.
fn require_element(ss_element: Option<&StmtSummaryByDigestElement>) -> &StmtSummaryByDigestElement {
    ss_element.expect("interval columns are only selected for interval rows")
}

/// The digest a column factory needs, which Go dereferences without a nil
/// check.
fn require_ssbd(ssbd: Option<&StmtSummaryByDigest>) -> &StmtSummaryByDigest {
    ssbd.expect("digest columns are only selected for digest rows")
}

/// Go `columnValueFactoryMap`.
///
/// Go's package-level map becomes a lookup function; the arms are in Go's
/// literal order.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn column_value_factory(name: &str) -> Option<ColumnValueFactory> {
    let factory: ColumnValueFactory = match name {
        CLUSTER_TABLE_INSTANCE_COLUMN_NAME_STR => {
            |reader, _, _, _| Datum::new_string(reader.instance_addr.as_bytes())
        }
        SUMMARY_BEGIN_TIME_STR => |reader, ss_element, _, _| {
            timestamp_datum(unix_seconds_in(
                require_element(ss_element).begin_time,
                reader.tz,
            ))
        },
        SUMMARY_END_TIME_STR => |reader, ss_element, _, _| {
            timestamp_datum(unix_seconds_in(
                require_element(ss_element).end_time,
                reader.tz,
            ))
        },
        STMT_TYPE_STR => |_, _, ssbd, _| Datum::new_string(require_ssbd(ssbd).stmt_type.as_bytes()),
        SCHEMA_NAME_STR => {
            |_, _, ssbd, _| opt_string_datum(convert_empty_to_nil(&require_ssbd(ssbd).schema_name))
        }
        DIGEST_STR => {
            |_, _, ssbd, _| opt_string_datum(convert_empty_to_nil(&require_ssbd(ssbd).digest))
        }
        DIGEST_TEXT_STR => {
            |_, _, ssbd, _| Datum::new_string(require_ssbd(ssbd).normalized_sql.as_bytes())
        }
        BINDING_DIGEST_STR => |_, _, ssbd, _| {
            opt_string_datum(convert_empty_to_nil(&require_ssbd(ssbd).binding_digest))
        },
        BINDING_DIGEST_TEXT_STR => {
            |_, _, ssbd, _| Datum::new_string(require_ssbd(ssbd).binding_sql.as_bytes())
        }
        TABLE_NAMES_STR => {
            |_, _, ssbd, _| opt_string_datum(convert_empty_to_nil(&require_ssbd(ssbd).table_names))
        }
        INDEX_NAMES_STR => |_, _, _, stats| {
            let joined = stats.index_names.join(",");
            opt_string_datum(convert_empty_to_nil(&joined))
        },
        SAMPLE_USER_STR => |_, _, _, stats| {
            let sample_user = stats.auth_users.iter().next().map_or("", String::as_str);
            opt_string_datum(convert_empty_to_nil(sample_user))
        },
        EXEC_COUNT_STR => |_, _, _, stats| Datum::new_int(stats.exec_count),
        ERRORS_STR | SUM_ERRORS_STR => |_, _, _, stats| Datum::new_int(stats.sum_errors),
        WARNINGS_STR | SUM_WARNINGS_STR => |_, _, _, stats| Datum::new_int(stats.sum_warnings),
        TOTAL_TIME_STR | SUM_LATENCY_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.sum_latency))
        }
        MAX_LATENCY_STR => |_, _, _, stats| Datum::new_int(nanos(stats.max_latency)),
        MIN_LATENCY_STR => |_, _, _, stats| Datum::new_int(nanos(stats.min_latency)),
        AVG_LATENCY_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(nanos(stats.sum_latency), stats.exec_count))
        }
        PARSE_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_parse_latency)),
        AVG_PARSE_LATENCY_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(nanos(stats.sum_parse_latency), stats.exec_count))
        },
        MAX_PARSE_LATENCY_STR => |_, _, _, stats| Datum::new_int(nanos(stats.max_parse_latency)),
        COMPILE_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_compile_latency)),
        AVG_COMPILE_LATENCY_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(nanos(stats.sum_compile_latency), stats.exec_count))
        },
        MAX_COMPILE_LATENCY_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.max_compile_latency))
        }
        COP_TASK_NUM_STR | SUM_COP_TASK_NUM_STR => {
            |_, _, _, stats| Datum::new_int(stats.sum_num_cop_tasks)
        }
        COP_PROCESS_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_cop_process_time)),
        MAX_COP_PROCESS_TIME_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.max_cop_process_time))
        }
        MAX_COP_PROCESS_ADDRESS_STR => {
            |_, _, _, stats| opt_string_datum(convert_empty_to_nil(&stats.max_cop_process_address))
        }
        COP_WAIT_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_cop_wait_time)),
        MAX_COP_WAIT_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.max_cop_wait_time)),
        MAX_COP_WAIT_ADDRESS_STR => {
            |_, _, _, stats| opt_string_datum(convert_empty_to_nil(&stats.max_cop_wait_address))
        }
        PROCESS_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_process_time)),
        AVG_PROCESS_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(nanos(stats.sum_process_time), stats.exec_count))
        },
        MAX_PROCESS_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.max_process_time)),
        WAIT_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_wait_time)),
        AVG_WAIT_TIME_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(nanos(stats.sum_wait_time), stats.exec_count))
        }
        MAX_WAIT_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.max_wait_time)),
        BACKOFF_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_backoff_time)),
        AVG_BACKOFF_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(nanos(stats.sum_backoff_time), stats.exec_count))
        },
        MAX_BACKOFF_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.max_backoff_time)),
        TOTAL_KEYS_STR => |_, _, _, stats| Datum::new_int(stats.sum_total_keys),
        AVG_TOTAL_KEYS_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(stats.sum_total_keys, stats.exec_count))
        }
        MAX_TOTAL_KEYS_STR => |_, _, _, stats| Datum::new_int(stats.max_total_keys),
        PROCESSED_KEYS_STR => |_, _, _, stats| Datum::new_int(stats.sum_processed_keys),
        AVG_PROCESSED_KEYS_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(stats.sum_processed_keys, stats.exec_count))
        }
        MAX_PROCESSED_KEYS_STR => |_, _, _, stats| Datum::new_int(stats.max_processed_keys),
        ROCKSDB_DELETE_SKIPPED_COUNT_STR => {
            |_, _, _, stats| Datum::new_real(stats.sum_rocksdb_delete_skipped_count as f64)
        }
        AVG_ROCKSDB_DELETE_SKIPPED_COUNT_STR => |_, _, _, stats| {
            Datum::new_real(avg_float4_uint(
                stats.sum_rocksdb_delete_skipped_count,
                stats.exec_count,
            ))
        },
        MAX_ROCKSDB_DELETE_SKIPPED_COUNT_STR => {
            |_, _, _, stats| Datum::new_uint(stats.max_rocksdb_delete_skipped_count)
        }
        ROCKSDB_KEY_SKIPPED_COUNT_STR => {
            |_, _, _, stats| Datum::new_real(stats.sum_rocksdb_key_skipped_count as f64)
        }
        AVG_ROCKSDB_KEY_SKIPPED_COUNT_STR => |_, _, _, stats| {
            Datum::new_real(avg_float4_uint(
                stats.sum_rocksdb_key_skipped_count,
                stats.exec_count,
            ))
        },
        MAX_ROCKSDB_KEY_SKIPPED_COUNT_STR => {
            |_, _, _, stats| Datum::new_uint(stats.max_rocksdb_key_skipped_count)
        }
        ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR => {
            |_, _, _, stats| Datum::new_real(stats.sum_rocksdb_block_cache_hit_count as f64)
        }
        AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR => |_, _, _, stats| {
            Datum::new_real(avg_float4_uint(
                stats.sum_rocksdb_block_cache_hit_count,
                stats.exec_count,
            ))
        },
        MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT_STR => {
            |_, _, _, stats| Datum::new_uint(stats.max_rocksdb_block_cache_hit_count)
        }
        ROCKSDB_BLOCK_READ_COUNT_STR => {
            |_, _, _, stats| Datum::new_real(stats.sum_rocksdb_block_read_count as f64)
        }
        AVG_ROCKSDB_BLOCK_READ_COUNT_STR => |_, _, _, stats| {
            Datum::new_real(avg_float4_uint(
                stats.sum_rocksdb_block_read_count,
                stats.exec_count,
            ))
        },
        MAX_ROCKSDB_BLOCK_READ_COUNT_STR => {
            |_, _, _, stats| Datum::new_uint(stats.max_rocksdb_block_read_count)
        }
        ROCKSDB_BLOCK_READ_BYTE_STR => {
            |_, _, _, stats| Datum::new_real(stats.sum_rocksdb_block_read_byte as f64)
        }
        AVG_ROCKSDB_BLOCK_READ_BYTE_STR => |_, _, _, stats| {
            Datum::new_real(avg_float4_uint(
                stats.sum_rocksdb_block_read_byte,
                stats.exec_count,
            ))
        },
        MAX_ROCKSDB_BLOCK_READ_BYTE_STR => {
            |_, _, _, stats| Datum::new_uint(stats.max_rocksdb_block_read_byte)
        }
        AVG_IA_REMOTE_READ_SEGMENT_COUNT_STR => |_, _, _, stats| {
            Datum::new_real(avg_float4_uint(
                stats.sum_ia_remote_read_segment_count,
                stats.exec_count,
            ))
        },
        MAX_IA_REMOTE_READ_SEGMENT_COUNT_STR => {
            |_, _, _, stats| Datum::new_uint(stats.max_ia_remote_read_segment_count)
        }
        AVG_IA_REMOTE_READ_SEGMENT_SIZE_STR => |_, _, _, stats| {
            Datum::new_real(avg_float4_uint(
                stats.sum_ia_remote_read_segment_size,
                stats.exec_count,
            ))
        },
        MAX_IA_REMOTE_READ_SEGMENT_SIZE_STR => {
            |_, _, _, stats| Datum::new_uint(stats.max_ia_remote_read_segment_size)
        }
        AVG_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(
                nanos(stats.sum_ia_remote_read_segment_wait_time),
                stats.exec_count,
            ))
        },
        MAX_IA_REMOTE_READ_SEGMENT_WAIT_TIME_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.max_ia_remote_read_segment_wait_time))
        }
        PREWRITE_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_prewrite_time)),
        AVG_PREWRITE_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(nanos(stats.sum_prewrite_time), stats.commit_count))
        },
        MAX_PREWRITE_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.max_prewrite_time)),
        COMMIT_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_commit_time)),
        AVG_COMMIT_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(nanos(stats.sum_commit_time), stats.commit_count))
        },
        MAX_COMMIT_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.max_commit_time)),
        COMMIT_TS_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_get_commit_ts_time)),
        AVG_GET_COMMIT_TS_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(
                nanos(stats.sum_get_commit_ts_time),
                stats.commit_count,
            ))
        },
        MAX_GET_COMMIT_TS_TIME_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.max_get_commit_ts_time))
        }
        COMMIT_BACKOFF_TIME_STR => |_, _, _, stats| Datum::new_int(stats.sum_commit_backoff_time),
        AVG_COMMIT_BACKOFF_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(stats.sum_commit_backoff_time, stats.commit_count))
        },
        MAX_COMMIT_BACKOFF_TIME_STR => {
            |_, _, _, stats| Datum::new_int(stats.max_commit_backoff_time)
        }
        RESOLVE_LOCK_TIME_STR => |_, _, _, stats| Datum::new_int(stats.sum_resolve_lock_time),
        AVG_RESOLVE_LOCK_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(stats.sum_resolve_lock_time, stats.commit_count))
        },
        MAX_RESOLVE_LOCK_TIME_STR => |_, _, _, stats| Datum::new_int(stats.max_resolve_lock_time),
        LOCAL_LATCH_WAIT_TIME_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.sum_local_latch_time))
        }
        AVG_LOCAL_LATCH_WAIT_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(
                nanos(stats.sum_local_latch_time),
                stats.commit_count,
            ))
        },
        MAX_LOCAL_LATCH_WAIT_TIME_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.max_local_latch_time))
        }
        WRITE_KEYS_STR => |_, _, _, stats| Datum::new_real(stats.sum_write_keys as f64),
        AVG_WRITE_KEYS_STR => {
            |_, _, _, stats| Datum::new_real(avg_float(stats.sum_write_keys, stats.commit_count))
        }
        MAX_WRITE_KEYS_STR => |_, _, _, stats| Datum::new_int(stats.max_write_keys),
        WRITE_SIZE_STR => |_, _, _, stats| Datum::new_real(stats.sum_write_size as f64),
        AVG_WRITE_SIZE_STR => {
            |_, _, _, stats| Datum::new_real(avg_float(stats.sum_write_size, stats.commit_count))
        }
        MAX_WRITE_SIZE_STR => |_, _, _, stats| Datum::new_int(stats.max_write_size),
        PREWRITE_REGIONS_STR => {
            |_, _, _, stats| Datum::new_real(stats.sum_prewrite_region_num as f64)
        }
        AVG_PREWRITE_REGIONS_STR => |_, _, _, stats| {
            Datum::new_real(avg_float(stats.sum_prewrite_region_num, stats.commit_count))
        },
        MAX_PREWRITE_REGIONS_STR => {
            |_, _, _, stats| Datum::new_int(i64::from(stats.max_prewrite_region_num))
        }
        TXN_RETRY_STR => |_, _, _, stats| Datum::new_real(stats.sum_txn_retry as f64),
        AVG_TXN_RETRY_STR => {
            |_, _, _, stats| Datum::new_real(avg_float(stats.sum_txn_retry, stats.commit_count))
        }
        MAX_TXN_RETRY_STR => |_, _, _, stats| Datum::new_int(stats.max_txn_retry),
        EXEC_RETRY_STR | SUM_EXEC_RETRY_STR => |_, _, _, stats| {
            Datum::new_int(i64::try_from(stats.exec_retry_count).unwrap_or(i64::MAX))
        },
        EXEC_RETRY_TIME_STR | SUM_EXEC_RETRY_TIME_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.exec_retry_time))
        }
        BACKOFF_TIMES_STR | SUM_BACKOFF_TIMES_STR => {
            |_, _, _, stats| Datum::new_int(stats.sum_backoff_times)
        }
        BACKOFF_TYPES_STR => {
            |_, _, _, stats| opt_string_datum(format_backoff_types(&stats.backoff_types).as_deref())
        }
        MEM_STR => |_, _, _, stats| Datum::new_int(stats.sum_mem),
        AVG_MEM_STR => |_, _, _, stats| Datum::new_int(avg_int(stats.sum_mem, stats.exec_count)),
        MAX_MEM_STR => |_, _, _, stats| Datum::new_int(stats.max_mem),
        MEM_ARBITRATION_STR => |_, _, _, stats| Datum::new_real(stats.sum_mem_arbitration),
        AVG_MEM_ARBITRATION_STR => |_, _, _, stats| {
            Datum::new_real(avg_sum_float(stats.sum_mem_arbitration, stats.exec_count))
        },
        MAX_MEM_ARBITRATION_STR => |_, _, _, stats| Datum::new_real(stats.max_mem_arbitration),
        DISK_STR => |_, _, _, stats| Datum::new_int(stats.sum_disk),
        AVG_DISK_STR => |_, _, _, stats| Datum::new_int(avg_int(stats.sum_disk, stats.exec_count)),
        MAX_DISK_STR => |_, _, _, stats| Datum::new_int(stats.max_disk),
        KV_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_kv_total)),
        AVG_KV_TIME_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(nanos(stats.sum_kv_total), stats.commit_count))
        }
        PD_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_pd_total)),
        AVG_PD_TIME_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(nanos(stats.sum_pd_total), stats.commit_count))
        }
        BACKOFF_TOTAL_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.sum_backoff_total)),
        AVG_BACKOFF_TOTAL_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(nanos(stats.sum_backoff_total), stats.commit_count))
        },
        WRITE_SQL_RESP_TIME_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.sum_write_sql_resp_total))
        }
        AVG_WRITE_SQL_RESP_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(
                nanos(stats.sum_write_sql_resp_total),
                stats.commit_count,
            ))
        },
        AVG_TIDB_CPU_TIME_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(nanos(stats.sum_tidb_cpu), stats.exec_count))
        }
        AVG_TIKV_CPU_TIME_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(nanos(stats.sum_tikv_cpu), stats.exec_count))
        }
        RESULT_ROWS_STR => |_, _, _, stats| Datum::new_int(stats.sum_result_rows),
        MAX_RESULT_ROWS_STR => |_, _, _, stats| Datum::new_int(stats.max_result_rows),
        MIN_RESULT_ROWS_STR => |_, _, _, stats| Datum::new_int(stats.min_result_rows),
        AFFECTED_ROWS_STR => |_, _, _, stats| Datum::new_real(stats.sum_affected_rows as f64),
        AVG_RESULT_ROWS_STR => {
            |_, _, _, stats| Datum::new_int(avg_int(stats.sum_result_rows, stats.exec_count))
        }
        PREPARED_STR => |_, _, _, stats| bool_datum(stats.prepared),
        AVG_AFFECTED_ROWS_STR => |_, _, _, stats| {
            Datum::new_real(avg_float4_uint(stats.sum_affected_rows, stats.exec_count))
        },
        FIRST_SEEN_STR => {
            |reader, _, _, stats| timestamp_datum(stats.first_seen.with_timezone(&reader.tz))
        }
        LAST_SEEN_STR => {
            |reader, _, _, stats| timestamp_datum(stats.last_seen.with_timezone(&reader.tz))
        }
        PLAN_IN_CACHE_STR => |_, _, _, stats| bool_datum(stats.plan_in_cache),
        PLAN_CACHE_HITS_STR => |_, _, _, stats| Datum::new_int(stats.plan_cache_hits),
        PLAN_IN_BINDING_STR => |_, _, _, stats| bool_datum(stats.plan_in_binding),
        QUERY_SAMPLE_TEXT_STR => |_, _, _, stats| Datum::new_string(stats.sample_sql.as_bytes()),
        PREV_SAMPLE_TEXT_STR => |_, _, _, stats| Datum::new_string(stats.prev_sql.as_bytes()),
        PLAN_DIGEST_STR => {
            |_, _, ssbd, _| Datum::new_string(require_ssbd(ssbd).plan_digest.as_bytes())
        }
        PLAN_STR => |_, _, _, stats| {
            let plan = tidb_util::plancodec::decode_plan(stats.sample_plan.as_bytes())
                .unwrap_or_else(|_error| {
                    // Go logs through `logutil.BgLogger()`; this crate has no
                    // logger boundary, so the failure only reaches the row as
                    // Go's empty plan.
                    Vec::new()
                });
            Datum::new_string(plan)
        },
        BINARY_PLAN => |_, _, _, stats| Datum::new_string(stats.sample_binary_plan.as_bytes()),
        CHARSET => |_, _, _, stats| Datum::new_string(stats.charset.as_bytes()),
        COLLATION => |_, _, _, stats| Datum::new_string(stats.collation.as_bytes()),
        PLAN_HINT => |_, _, _, stats| Datum::new_string(stats.plan_hint.as_bytes()),
        REQUEST_UNIT_READ_STR => |_, _, _, stats| Datum::new_real(stats.ru.sum_rru),
        AVG_REQUEST_UNIT_READ_STR => {
            |_, _, _, stats| Datum::new_real(avg_sum_float(stats.ru.sum_rru, stats.exec_count))
        }
        MAX_REQUEST_UNIT_READ_STR => |_, _, _, stats| Datum::new_real(stats.ru.max_rru),
        REQUEST_UNIT_WRITE_STR => |_, _, _, stats| Datum::new_real(stats.ru.sum_wru),
        AVG_REQUEST_UNIT_WRITE_STR => {
            |_, _, _, stats| Datum::new_real(avg_sum_float(stats.ru.sum_wru, stats.exec_count))
        }
        MAX_REQUEST_UNIT_WRITE_STR => |_, _, _, stats| Datum::new_real(stats.ru.max_wru),
        QUEUED_RC_TIME_STR => |_, _, _, stats| Datum::new_int(nanos(stats.ru.sum_ru_wait_duration)),
        AVG_QUEUED_RC_TIME_STR => |_, _, _, stats| {
            Datum::new_int(avg_int(
                nanos(stats.ru.sum_ru_wait_duration),
                stats.exec_count,
            ))
        },
        MAX_QUEUED_RC_TIME_STR => {
            |_, _, _, stats| Datum::new_int(nanos(stats.ru.max_ru_wait_duration))
        }
        AVG_REQUEST_UNIT_V2_STR => {
            |_, _, _, stats| Datum::new_real(avg_sum_float(stats.ru.sum_ru_v2, stats.exec_count))
        }
        MAX_REQUEST_UNIT_V2_STR => |_, _, _, stats| Datum::new_real(stats.ru.max_ru_v2),
        RESOURCE_GROUP_NAME => {
            |_, _, _, stats| Datum::new_string(stats.resource_group_name.as_bytes())
        }
        PLAN_CACHE_UNQUALIFIED_STR => {
            |_, _, _, stats| Datum::new_int(stats.plan_cache_unqualified_count)
        }
        PLAN_CACHE_UNQUALIFIED_LAST_REASON_STR => {
            |_, _, _, stats| Datum::new_string(stats.last_plan_cache_unqualified.as_bytes())
        }
        SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL_STR | UNPACKED_BYTES_SENT_TIKV_TOTAL_STR => {
            |_, _, _, stats| Datum::new_int(stats.network.unpacked_bytes_sent_tikv_total)
        }
        SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL_STR | UNPACKED_BYTES_RECEIVED_TIKV_TOTAL_STR => {
            |_, _, _, stats| Datum::new_int(stats.network.unpacked_bytes_received_tikv_total)
        }
        SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE_STR | UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE_STR => {
            |_, _, _, stats| Datum::new_int(stats.network.unpacked_bytes_sent_tikv_cross_zone)
        }
        SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE_STR
        | UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE_STR => {
            |_, _, _, stats| Datum::new_int(stats.network.unpacked_bytes_received_tikv_cross_zone)
        }
        SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL_STR | UNPACKED_BYTES_SENT_TIFLASH_TOTAL_STR => {
            |_, _, _, stats| Datum::new_int(stats.network.unpacked_bytes_sent_tiflash_total)
        }
        SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL_STR
        | UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL_STR => {
            |_, _, _, stats| Datum::new_int(stats.network.unpacked_bytes_received_tiflash_total)
        }
        SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE_STR
        | UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE_STR => {
            |_, _, _, stats| Datum::new_int(stats.network.unpacked_bytes_sent_tiflash_cross_zone)
        }
        SUM_UNPACKED_BYTES_RECEIVE_TIFLASH_CROSS_ZONE_STR
        | UNPACKED_BYTES_RECEIVE_TIFLASH_CROSS_ZONE_STR => |_, _, _, stats| {
            Datum::new_int(stats.network.unpacked_bytes_received_tiflash_cross_zone)
        },
        STORAGE_KV_STR => |_, _, _, stats| bool_datum(stats.storage_kv),
        STORAGE_MPP_STR => |_, _, _, stats| bool_datum(stats.storage_mpp),
        _ => return None,
    };
    Some(factory)
}
