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

//! System-variable name constants, script-extracted from `tidb_vars.go` and
//! byte-verified verbatim. Names mirror the Go identifiers in SCREAMING_SNAKE.

/// Go `TiDBDDLSlowOprThreshold`.
pub const TIDB_DDL_SLOW_OPR_THRESHOLD: &str = "ddl_slow_threshold";
/// TiDBSnapshot is used for reading history data, the default value is empty string.
/// The value can be a datetime string like '2017-11-11 20:20:20' or a tso string. When this variable is set, the session reads history data of that time.
/// Go `TiDBSnapshot`.
pub const TIDB_SNAPSHOT: &str = "tidb_snapshot";
/// TiDBOptAggPushDown is used to enable/disable the optimizer rule of aggregation push down.
/// Go `TiDBOptAggPushDown`.
pub const TIDB_OPT_AGG_PUSH_DOWN: &str = "tidb_opt_agg_push_down";
/// TiDBOptDeriveTopN is used to enable/disable the optimizer rule of deriving topN.
/// Go `TiDBOptDeriveTopN`.
pub const TIDB_OPT_DERIVE_TOP_N: &str = "tidb_opt_derive_topn";
/// TiDBOptCartesianBCJ is used to disable/enable broadcast cartesian join in MPP mode
/// Go `TiDBOptCartesianBCJ`.
pub const TIDB_OPT_CARTESIAN_BCJ: &str = "tidb_opt_broadcast_cartesian_join";
/// Go `TiDBOptMPPOuterJoinFixedBuildSide`.
pub const TIDB_OPT_MPP_OUTER_JOIN_FIXED_BUILD_SIDE: &str =
    "tidb_opt_mpp_outer_join_fixed_build_side";
/// TiDBOptDistinctAggPushDown is used to decide whether agg with distinct should be pushed to tikv/tiflash.
/// Go `TiDBOptDistinctAggPushDown`.
pub const TIDB_OPT_DISTINCT_AGG_PUSH_DOWN: &str = "tidb_opt_distinct_agg_push_down";
/// TiDBOptSkewDistinctAgg is used to indicate the distinct agg has data skew
/// Go `TiDBOptSkewDistinctAgg`.
pub const TIDB_OPT_SKEW_DISTINCT_AGG: &str = "tidb_opt_skew_distinct_agg";
/// TiDBOpt3StageDistinctAgg is used to indicate whether to plan and execute the distinct agg in 3 stages
/// Go `TiDBOpt3StageDistinctAgg`.
pub const TIDB_OPT3_STAGE_DISTINCT_AGG: &str = "tidb_opt_three_stage_distinct_agg";
/// TiDBOptEnable3StageMultiDistinctAgg is used to indicate whether to plan and execute the multi distinct agg in 3 stages
/// Go `TiDBOptEnable3StageMultiDistinctAgg`.
pub const TIDB_OPT_ENABLE3_STAGE_MULTI_DISTINCT_AGG: &str =
    "tidb_opt_enable_three_stage_multi_distinct_agg";
/// Go `TiDBOptExplainNoEvaledSubQuery`.
pub const TIDB_OPT_EXPLAIN_NO_EVALED_SUB_QUERY: &str = "tidb_opt_enable_non_eval_scalar_subquery";
/// TiDBBCJThresholdSize is used to limit the size of small table for mpp broadcast join.
/// Its unit is bytes, if the size of small table is larger than it, we will not use bcj.
/// Go `TiDBBCJThresholdSize`.
pub const TIDB_BCJ_THRESHOLD_SIZE: &str = "tidb_broadcast_join_threshold_size";
/// TiDBBCJThresholdCount is used to limit the count of small table for mpp broadcast join.
/// If we can't estimate the size of one side of join child, we will check if its row number exceeds this limitation.
/// Go `TiDBBCJThresholdCount`.
pub const TIDB_BCJ_THRESHOLD_COUNT: &str = "tidb_broadcast_join_threshold_count";
/// TiDBPreferBCJByExchangeDataSize indicates the method used to choose mpp broadcast join
/// Go `TiDBPreferBCJByExchangeDataSize`.
pub const TIDB_PREFER_BCJ_BY_EXCHANGE_DATA_SIZE: &str =
    "tidb_prefer_broadcast_join_by_exchange_data_size";
/// TiDBOptWriteRowID is used to enable/disable the operations of insert、replace and update to _tidb_rowid.
/// Go `TiDBOptWriteRowID`.
pub const TIDB_OPT_WRITE_ROW_ID: &str = "tidb_opt_write_row_id";
/// TiDBAutoAnalyzeRatio will run if (table modify count)/(table row count) is greater than this value.
/// Go `TiDBAutoAnalyzeRatio`.
pub const TIDB_AUTO_ANALYZE_RATIO: &str = "tidb_auto_analyze_ratio";
/// TiDBAutoAnalyzeStartTime will run if current time is within start time and end time.
/// Go `TiDBAutoAnalyzeStartTime`.
pub const TIDB_AUTO_ANALYZE_START_TIME: &str = "tidb_auto_analyze_start_time";
/// Go `TiDBAutoAnalyzeEndTime`.
pub const TIDB_AUTO_ANALYZE_END_TIME: &str = "tidb_auto_analyze_end_time";
/// TiDBChecksumTableConcurrency is used to speed up the ADMIN CHECKSUM TABLE
/// statement, when a table has multiple indices, those indices can be
/// scanned concurrently, with the cost of higher system performance impact.
/// Go `TiDBChecksumTableConcurrency`.
pub const TIDB_CHECKSUM_TABLE_CONCURRENCY: &str = "tidb_checksum_table_concurrency";
/// TiDBCurrentTS is used to get the current transaction timestamp.
/// It is read-only.
/// Go `TiDBCurrentTS`.
pub const TIDB_CURRENT_TS: &str = "tidb_current_ts";
/// TiDBLastTxnInfo is used to get the last transaction info within the current session.
/// Go `TiDBLastTxnInfo`.
pub const TIDB_LAST_TXN_INFO: &str = "tidb_last_txn_info";
/// TiDBLastQueryInfo is used to get the last query info within the current session.
/// Go `TiDBLastQueryInfo`.
pub const TIDB_LAST_QUERY_INFO: &str = "tidb_last_query_info";
/// TiDBLastDDLInfo is used to get the last ddl info within the current session.
/// Go `TiDBLastDDLInfo`.
pub const TIDB_LAST_DDL_INFO: &str = "tidb_last_ddl_info";
/// TiDBLastPlanReplayerToken is used to get the last plan replayer token within the current session
/// Go `TiDBLastPlanReplayerToken`.
pub const TIDB_LAST_PLAN_REPLAYER_TOKEN: &str = "tidb_last_plan_replayer_token";
/// TiDBConfig is a read-only variable that shows the config of the current server.
/// Go `TiDBConfig`.
pub const TIDB_CONFIG: &str = "tidb_config";
/// TiDBBatchInsert is used to enable/disable auto-split insert data. If set this option on, insert executor will automatically
/// insert data into multiple batches and use a single txn for each batch. This will be helpful when inserting large data.
/// Go `TiDBBatchInsert`.
pub const TIDB_BATCH_INSERT: &str = "tidb_batch_insert";
/// TiDBBatchDelete is used to enable/disable auto-split delete data. If set this option on, delete executor will automatically
/// split data into multiple batches and use a single txn for each batch. This will be helpful when deleting large data.
/// Go `TiDBBatchDelete`.
pub const TIDB_BATCH_DELETE: &str = "tidb_batch_delete";
/// TiDBBatchCommit is used to enable/disable auto-split the transaction.
/// If set this option on, the transaction will be committed when it reaches stmt-count-limit and starts a new transaction.
/// Go `TiDBBatchCommit`.
pub const TIDB_BATCH_COMMIT: &str = "tidb_batch_commit";
/// TiDBDMLBatchSize is used to split the insert/delete data into small batches.
/// It only takes effort when tidb_batch_insert/tidb_batch_delete is on.
/// Its default value is 20000. When the row size is large, 20k rows could be larger than 100MB.
/// User could change it to a smaller one to avoid breaking the transaction size limitation.
/// Go `TiDBDMLBatchSize`.
pub const TIDB_DML_BATCH_SIZE: &str = "tidb_dml_batch_size";
/// TiDBMemQuotaQuery controls the memory quota of a query.
/// Go `TiDBMemQuotaQuery`.
pub const TIDB_MEM_QUOTA_QUERY: &str = "tidb_mem_quota_query";
/// TiDBMemQuotaApplyCache controls the memory quota of a query.
/// Go `TiDBMemQuotaApplyCache`.
pub const TIDB_MEM_QUOTA_APPLY_CACHE: &str = "tidb_mem_quota_apply_cache";
/// TiDBGeneralLog is used to log every query in the server in info level.
/// Go `TiDBGeneralLog`.
pub const TIDB_GENERAL_LOG: &str = "tidb_general_log";
/// TiDBTraceEvent controls the experimental trace event instrumentation.
/// Go `TiDBTraceEvent`.
pub const TIDB_TRACE_EVENT: &str = "tidb_trace_event";
/// TiDBLogFileMaxDays is used to log every query in the server in info level.
/// Go `TiDBLogFileMaxDays`.
pub const TIDB_LOG_FILE_MAX_DAYS: &str = "tidb_log_file_max_days";
/// TiDBPProfSQLCPU is used to add label sql label to pprof result.
/// Go `TiDBPProfSQLCPU`.
pub const TIDB_P_PROF_SQLCPU: &str = "tidb_pprof_sql_cpu";
/// TiDBRetryLimit is the maximum number of retries when committing a transaction.
/// Go `TiDBRetryLimit`.
pub const TIDB_RETRY_LIMIT: &str = "tidb_retry_limit";
/// TiDBDisableTxnAutoRetry disables transaction auto retry.
/// Deprecated: This variable is deprecated, please do not use this variable.
/// Go `TiDBDisableTxnAutoRetry`.
pub const TIDB_DISABLE_TXN_AUTO_RETRY: &str = "tidb_disable_txn_auto_retry";
/// TiDBEnableChunkRPC enables TiDB to use Chunk format for coprocessor requests.
/// Go `TiDBEnableChunkRPC`.
pub const TIDB_ENABLE_CHUNK_RPC: &str = "tidb_enable_chunk_rpc";
/// TiDBOptimizerSelectivityLevel is used to control the selectivity estimation level.
/// Go `TiDBOptimizerSelectivityLevel`.
pub const TIDB_OPTIMIZER_SELECTIVITY_LEVEL: &str = "tidb_optimizer_selectivity_level";
/// TiDBOptIndexPruneThreshold is used to control the threshold for index pruning optimization.
/// Go `TiDBOptIndexPruneThreshold`.
pub const TIDB_OPT_INDEX_PRUNE_THRESHOLD: &str = "tidb_opt_index_prune_threshold";
/// TiDBOptimizerEnableNewOnlyFullGroupByCheck is used to open the newly only_full_group_by check by maintaining functional dependency.
/// Go `TiDBOptimizerEnableNewOnlyFullGroupByCheck`.
pub const TIDB_OPTIMIZER_ENABLE_NEW_ONLY_FULL_GROUP_BY_CHECK: &str =
    "tidb_enable_new_only_full_group_by_check";
/// Go `TiDBOptimizerEnableOuterJoinReorder`.
pub const TIDB_OPTIMIZER_ENABLE_OUTER_JOIN_REORDER: &str = "tidb_enable_outer_join_reorder";
/// TiDBOptimizerEnableNAAJ is used to open the newly null-aware anti join
/// Go `TiDBOptimizerEnableNAAJ`.
pub const TIDB_OPTIMIZER_ENABLE_NAAJ: &str = "tidb_enable_null_aware_anti_join";
/// TiDBTxnMode is used to control the transaction behavior.
/// Go `TiDBTxnMode`.
pub const TIDB_TXN_MODE: &str = "tidb_txn_mode";
/// TiDBRowFormatVersion is used to control tidb row format version current.
/// Go `TiDBRowFormatVersion`.
pub const TIDB_ROW_FORMAT_VERSION: &str = "tidb_row_format_version";
/// TiDBEnableRowLevelChecksum is used to control whether to append checksum to row values.
/// Go `TiDBEnableRowLevelChecksum`.
pub const TIDB_ENABLE_ROW_LEVEL_CHECKSUM: &str = "tidb_enable_row_level_checksum";
/// TiDBEnableTablePartition is used to control table partition feature.
/// The valid value include auto/on/off:
/// on or auto: enable table partition if the partition type is implemented.
/// off: always disable table partition.
/// Go `TiDBEnableTablePartition`.
pub const TIDB_ENABLE_TABLE_PARTITION: &str = "tidb_enable_table_partition";
/// TiDBEnableListTablePartition is used to control list table partition feature.
/// Deprecated: This variable is deprecated, please do not use this variable.
/// Go `TiDBEnableListTablePartition`.
pub const TIDB_ENABLE_LIST_TABLE_PARTITION: &str = "tidb_enable_list_partition";
/// TiDBSkipIsolationLevelCheck is used to control whether to return error when set unsupported transaction
/// isolation level.
/// Go `TiDBSkipIsolationLevelCheck`.
pub const TIDB_SKIP_ISOLATION_LEVEL_CHECK: &str = "tidb_skip_isolation_level_check";
/// TiDBLowResolutionTSO is used for reading data with low resolution TSO which is updated once every two seconds
/// Go `TiDBLowResolutionTSO`.
pub const TIDB_LOW_RESOLUTION_TSO: &str = "tidb_low_resolution_tso";
/// TiDBReplicaRead is used for reading data from replicas, followers for example.
/// Go `TiDBReplicaRead`.
pub const TIDB_REPLICA_READ: &str = "tidb_replica_read";
/// TiDBAdaptiveClosestReadThreshold is for reading data from closest replicas(with same 'zone' label).
/// TiKV client should send read request to the closest replica(leader/follower) if the estimated response
/// size exceeds this threshold; otherwise, this request should be sent to leader.
/// This variable only take effect when `tidb_replica_read` is 'closest-adaptive'.
/// Go `TiDBAdaptiveClosestReadThreshold`.
pub const TIDB_ADAPTIVE_CLOSEST_READ_THRESHOLD: &str = "tidb_adaptive_closest_read_threshold";
/// TiDBAllowRemoveAutoInc indicates whether a user can drop the auto_increment column attribute or not.
/// Go `TiDBAllowRemoveAutoInc`.
pub const TIDB_ALLOW_REMOVE_AUTO_INC: &str = "tidb_allow_remove_auto_inc";
/// TiDBMultiStatementMode enables multi statement at the risk of SQL injection
/// provides backwards compatibility
/// Go `TiDBMultiStatementMode`.
pub const TIDB_MULTI_STATEMENT_MODE: &str = "tidb_multi_statement_mode";
/// TiDBEvolvePlanTaskMaxTime controls the max time of a single evolution task.
/// Go `TiDBEvolvePlanTaskMaxTime`.
pub const TIDB_EVOLVE_PLAN_TASK_MAX_TIME: &str = "tidb_evolve_plan_task_max_time";
/// TiDBEvolvePlanTaskStartTime is the start time of evolution task.
/// Go `TiDBEvolvePlanTaskStartTime`.
pub const TIDB_EVOLVE_PLAN_TASK_START_TIME: &str = "tidb_evolve_plan_task_start_time";
/// TiDBEvolvePlanTaskEndTime is the end time of evolution task.
/// Go `TiDBEvolvePlanTaskEndTime`.
pub const TIDB_EVOLVE_PLAN_TASK_END_TIME: &str = "tidb_evolve_plan_task_end_time";
/// TiDBSlowLogThreshold is used to set the slow log threshold in the server.
/// Go `TiDBSlowLogThreshold`.
pub const TIDB_SLOW_LOG_THRESHOLD: &str = "tidb_slow_log_threshold";
/// TiDBSlowLogRules defines multi-dimensional trigger rules for flexible slow log control.
/// Go `TiDBSlowLogRules`.
pub const TIDB_SLOW_LOG_RULES: &str = "tidb_slow_log_rules";
/// TiDBSlowLogMaxPerSec is the maximum number of slow logs that can be recorded per second in the server.
/// The default value is 0, which means no rate limiting is applied.
/// Go `TiDBSlowLogMaxPerSec`.
pub const TIDB_SLOW_LOG_MAX_PER_SEC: &str = "tidb_slow_log_max_per_sec";
/// TiDBSlowTxnLogThreshold is used to set the slow transaction log threshold in the server.
/// Go `TiDBSlowTxnLogThreshold`.
pub const TIDB_SLOW_TXN_LOG_THRESHOLD: &str = "tidb_slow_txn_log_threshold";
/// TiDBRecordPlanInSlowLog is used to log the plan of the slow query.
/// Go `TiDBRecordPlanInSlowLog`.
pub const TIDB_RECORD_PLAN_IN_SLOW_LOG: &str = "tidb_record_plan_in_slow_log";
/// TiDBEnableSlowLog enables TiDB to log slow queries.
/// Go `TiDBEnableSlowLog`.
pub const TIDB_ENABLE_SLOW_LOG: &str = "tidb_enable_slow_log";
/// TiDBCheckMb4ValueInUTF8 is used to control whether to enable the check wrong utf8 value.
/// Go `TiDBCheckMb4ValueInUTF8`.
pub const TIDB_CHECK_MB4_VALUE_IN_UTF8: &str = "tidb_check_mb4_value_in_utf8";
/// TiDBFoundInPlanCache indicates whether the last statement was found in plan cache
/// Go `TiDBFoundInPlanCache`.
pub const TIDB_FOUND_IN_PLAN_CACHE: &str = "last_plan_from_cache";
/// TiDBFoundInBinding indicates whether the last statement was matched with the hints in the binding.
/// Go `TiDBFoundInBinding`.
pub const TIDB_FOUND_IN_BINDING: &str = "last_plan_from_binding";
/// TiDBAllowAutoRandExplicitInsert indicates whether explicit insertion on auto_random column is allowed.
/// Go `TiDBAllowAutoRandExplicitInsert`.
pub const TIDB_ALLOW_AUTO_RAND_EXPLICIT_INSERT: &str = "allow_auto_random_explicit_insert";
/// TiDBTxnReadTS indicates the next transaction should be staleness transaction and provide the startTS
/// Go `TiDBTxnReadTS`.
pub const TIDB_TXN_READ_TS: &str = "tx_read_ts";
/// TiDBReadStaleness indicates the staleness duration for following statement
/// Go `TiDBReadStaleness`.
pub const TIDB_READ_STALENESS: &str = "tidb_read_staleness";
/// TiDBEnablePaging indicates whether paging is enabled in coprocessor requests.
/// Go `TiDBEnablePaging`.
pub const TIDB_ENABLE_PAGING: &str = "tidb_enable_paging";
/// TiDBReadConsistency indicates whether the autocommit read statement goes through TiKV RC.
/// Go `TiDBReadConsistency`.
pub const TIDB_READ_CONSISTENCY: &str = "tidb_read_consistency";
/// TiDBSysdateIsNow is the name of the `tidb_sysdate_is_now` system variable
/// Go `TiDBSysdateIsNow`.
pub const TIDB_SYSDATE_IS_NOW: &str = "tidb_sysdate_is_now";
/// RequireSecureTransport indicates the secure mode for data transport
/// Go `RequireSecureTransport`.
pub const REQUIRE_SECURE_TRANSPORT: &str = "require_secure_transport";
/// TiFlashFastScan indicates whether use fast scan in tiflash.
/// Go `TiFlashFastScan`.
pub const TIFLASH_FAST_SCAN: &str = "tiflash_fastscan";
/// TiDBEnableUnsafeSubstitute indicates whether to enable generate column takes unsafe substitute.
/// Go `TiDBEnableUnsafeSubstitute`.
pub const TIDB_ENABLE_UNSAFE_SUBSTITUTE: &str = "tidb_enable_unsafe_substitute";
/// TiDBEnableTiFlashReadForWriteStmt indicates whether to enable TiFlash to read for write statements.
/// Go `TiDBEnableTiFlashReadForWriteStmt`.
pub const TIDB_ENABLE_TIFLASH_READ_FOR_WRITE_STMT: &str = "tidb_enable_tiflash_read_for_write_stmt";
/// TiDBUseAlloc indicates whether the last statement used chunk alloc
/// Go `TiDBUseAlloc`.
pub const TIDB_USE_ALLOC: &str = "last_sql_use_alloc";
/// TiDBExplicitRequestSourceType indicates the source of the request, it's a complement of RequestSourceType.
/// The value maybe "lightning", "br", "dumpling" etc.
/// Go `TiDBExplicitRequestSourceType`.
pub const TIDB_EXPLICIT_REQUEST_SOURCE_TYPE: &str = "tidb_request_source_type";
/// TiDBBuildStatsConcurrency specifies the number of concurrent workers used for analyzing tables or partitions.
/// When multiple tables or partitions are specified in the analyze statement, TiDB will process them concurrently.
/// Go `TiDBBuildStatsConcurrency`.
pub const TIDB_BUILD_STATS_CONCURRENCY: &str = "tidb_build_stats_concurrency";
/// TiDBBuildSamplingStatsConcurrency is used to control the concurrency of building stats using sampling.
/// 1. The number of concurrent workers to merge FMSketches and Sample Data from different regions.
/// 2. The number of concurrent workers to build TopN and Histogram concurrently.
/// Additionally, this setting controls the concurrency for building NDV (Number of Distinct Values) for special indexes,
/// such as generated columns composed indexes.
/// Go `TiDBBuildSamplingStatsConcurrency`.
pub const TIDB_BUILD_SAMPLING_STATS_CONCURRENCY: &str = "tidb_build_sampling_stats_concurrency";
/// TiDBDistSQLScanConcurrency is used to set the concurrency of a distsql scan task.
/// A distsql scan task can be a table scan or a index scan, which may be distributed to many TiKV nodes.
/// Higher concurrency may reduce latency, but with the cost of higher memory usage and system performance impact.
/// If the query has a LIMIT clause, high concurrency makes the system do much more work than needed.
/// Go `TiDBDistSQLScanConcurrency`.
pub const TIDB_DIST_SQL_SCAN_CONCURRENCY: &str = "tidb_distsql_scan_concurrency";
/// TiDBAnalyzeDistSQLScanConcurrency is the number of concurrent workers to scan regions to collect statistics (FMSketch, Samples).
/// For auto analyze, the value is controlled by tidb_sysproc_scan_concurrency variable.
/// This variable was introduced in v7.6.0 to separate the scan concurrency of ANALYZE operations from normal queries. See: https://github.com/pingcap/tidb/pull/48829
/// For versions earlier than v7.6.0, the scan concurrency of regions during ANALYZE is controlled by the tidb_distsql_scan_concurrency variable.
/// Starting from v7.6.0, this variable also controls the scan concurrency of index serial scans during ANALYZE. See: https://github.com/pingcap/tidb/pull/50639
/// For versions earlier than v7.6.0, the scan concurrency of index serial scans during ANALYZE is controlled by the tidb_index_serial_scan_concurrency variable.
/// Go `TiDBAnalyzeDistSQLScanConcurrency`.
pub const TIDB_ANALYZE_DIST_SQL_SCAN_CONCURRENCY: &str = "tidb_analyze_distsql_scan_concurrency";
/// TiDBOptInSubqToJoinAndAgg is used to enable/disable the optimizer rule of rewriting IN subquery.
/// Go `TiDBOptInSubqToJoinAndAgg`.
pub const TIDB_OPT_IN_SUBQ_TO_JOIN_AND_AGG: &str = "tidb_opt_insubq_to_join_and_agg";
/// TiDBOptPreferRangeScan is used to enable/disable the optimizer to always prefer range scan over table scan, ignoring their costs.
/// Go `TiDBOptPreferRangeScan`.
pub const TIDB_OPT_PREFER_RANGE_SCAN: &str = "tidb_opt_prefer_range_scan";
/// TiDBOptEnableNoDecorrelateInSelect is used to control whether to enable the NO_DECORRELATE hint for subqueries in the select list.
/// Go `TiDBOptEnableNoDecorrelateInSelect`.
pub const TIDB_OPT_ENABLE_NO_DECORRELATE_IN_SELECT: &str =
    "tidb_opt_enable_no_decorrelate_in_select";
/// TiDBOptEnableAlternativeLogicalPlans controls whether the optimizer may build
/// an extra non-decorrelate logical alternative when decorrelation does not
/// produce an equivalent same-order index join candidate.
/// Go `TiDBOptEnableAlternativeLogicalPlans`.
pub const TIDB_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS: &str =
    "tidb_opt_enable_alternative_logical_plans";
/// TiDBEnableSemiJoinRewrite controls automatic rewrite of semi-join to
/// inner-join with aggregation (equivalent to SEMI_JOIN_REWRITE() hint).
/// Go `TiDBOptEnableSemiJoinRewrite`.
pub const TIDB_OPT_ENABLE_SEMI_JOIN_REWRITE: &str = "tidb_opt_enable_semi_join_rewrite";
/// TiDBOptEnableCorrelationAdjustment is used to indicates if enable correlation adjustment.
/// Go `TiDBOptEnableCorrelationAdjustment`.
pub const TIDB_OPT_ENABLE_CORRELATION_ADJUSTMENT: &str = "tidb_opt_enable_correlation_adjustment";
/// TiDBOptLimitPushDownThreshold determines if push Limit or TopN down to TiKV forcibly.
/// Go `TiDBOptLimitPushDownThreshold`.
pub const TIDB_OPT_LIMIT_PUSH_DOWN_THRESHOLD: &str = "tidb_opt_limit_push_down_threshold";
/// TiDBOptCorrelationThreshold is a guard to enable row count estimation using column order correlation.
/// Go `TiDBOptCorrelationThreshold`.
pub const TIDB_OPT_CORRELATION_THRESHOLD: &str = "tidb_opt_correlation_threshold";
/// TiDBOptCorrelationExpFactor is an exponential factor to control heuristic approach when tidb_opt_correlation_threshold is not satisfied.
/// Go `TiDBOptCorrelationExpFactor`.
pub const TIDB_OPT_CORRELATION_EXP_FACTOR: &str = "tidb_opt_correlation_exp_factor";
/// TiDBOptRiskEqSkewRatio controls the amount of skew is applied to equal predicate estimation when a value is not found in TopN/buckets.
/// Go `TiDBOptRiskEqSkewRatio`.
pub const TIDB_OPT_RISK_EQ_SKEW_RATIO: &str = "tidb_opt_risk_eq_skew_ratio";
/// TiDBOptRiskRangeSkewRatio controls the amount of skew that is applied to range predicate estimation when a range falls within a bucket or outside the histogram bucket range.
/// Go `TiDBOptRiskRangeSkewRatio`.
pub const TIDB_OPT_RISK_RANGE_SKEW_RATIO: &str = "tidb_opt_risk_range_skew_ratio";
/// TiDBOptRiskScaleNDVSkewRatio controls the NDV estimation risk strategy for scaling NDV estimation.
/// Go `TiDBOptRiskScaleNDVSkewRatio`.
pub const TIDB_OPT_RISK_SCALE_NDV_SKEW_RATIO: &str = "tidb_opt_scale_ndv_skew_ratio";
/// TiDBOptRiskGroupNDVSkewRatio controls the NDV estimation risk strategy for multi-column operations
/// including GROUP BY, JOIN, and DISTINCT operations.
/// When 0: uses conservative estimate (max of individual column NDVs, production default)
/// When > 0: blends conservative and exponential backoff estimates (0.1=mostly conservative, 1.0=full exponential)
/// Go `TiDBOptRiskGroupNDVSkewRatio`.
pub const TIDB_OPT_RISK_GROUP_NDV_SKEW_RATIO: &str = "tidb_opt_group_ndv_skew_ratio";
/// TiDBOptAlwaysKeepJoinKey indicates the optimizer to always keep join keys during optimization.
/// Join keys are crucial for join optimization like Join Order and Join Algorithm selection, removing
/// join keys might lead to suboptimal plans in some cases.
/// Go `TiDBOptAlwaysKeepJoinKey`.
pub const TIDB_OPT_ALWAYS_KEEP_JOIN_KEY: &str = "tidb_opt_always_keep_join_key";
/// TiDBOptCartesianJoinOrderThreshold controls whether to allow do Cartesian Join first in Join Reorder.
/// This variable is used as a penalty to trade off the risk and join order quality.
/// When 0: never do Cartesian Join first.
/// When > 0: allow Cartesian Join if cost(cartesian join) * threshold < cost(non cartesian join).
/// Go `TiDBOptCartesianJoinOrderThreshold`.
pub const TIDB_OPT_CARTESIAN_JOIN_ORDER_THRESHOLD: &str = "tidb_opt_cartesian_join_order_threshold";
/// TiDBOptCPUFactor is the CPU cost of processing one expression for one row.
/// Go `TiDBOptCPUFactor`.
pub const TIDB_OPT_CPU_FACTOR: &str = "tidb_opt_cpu_factor";
/// TiDBOptCopCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
/// Go `TiDBOptCopCPUFactor`.
pub const TIDB_OPT_COP_CPU_FACTOR: &str = "tidb_opt_copcpu_factor";
/// TiDBOptTiFlashConcurrencyFactor is concurrency number of tiflash computation.
/// Go `TiDBOptTiFlashConcurrencyFactor`.
pub const TIDB_OPT_TIFLASH_CONCURRENCY_FACTOR: &str = "tidb_opt_tiflash_concurrency_factor";
/// TiDBOptNetworkFactor is the network cost of transferring 1 byte data.
/// Go `TiDBOptNetworkFactor`.
pub const TIDB_OPT_NETWORK_FACTOR: &str = "tidb_opt_network_factor";
/// TiDBOptScanFactor is the IO cost of scanning 1 byte data on TiKV.
/// Go `TiDBOptScanFactor`.
pub const TIDB_OPT_SCAN_FACTOR: &str = "tidb_opt_scan_factor";
/// TiDBOptDescScanFactor is the IO cost of scanning 1 byte data on TiKV in desc order.
/// Go `TiDBOptDescScanFactor`.
pub const TIDB_OPT_DESC_SCAN_FACTOR: &str = "tidb_opt_desc_factor";
/// TiDBOptSeekFactor is the IO cost of seeking the start value in a range on TiKV or TiFlash.
/// Go `TiDBOptSeekFactor`.
pub const TIDB_OPT_SEEK_FACTOR: &str = "tidb_opt_seek_factor";
/// TiDBOptMemoryFactor is the memory cost of storing one tuple.
/// Go `TiDBOptMemoryFactor`.
pub const TIDB_OPT_MEMORY_FACTOR: &str = "tidb_opt_memory_factor";
/// TiDBOptDiskFactor is the IO cost of reading/writing one byte to temporary disk.
/// Go `TiDBOptDiskFactor`.
pub const TIDB_OPT_DISK_FACTOR: &str = "tidb_opt_disk_factor";
/// TiDBOptConcurrencyFactor is the CPU cost of additional one goroutine.
/// Go `TiDBOptConcurrencyFactor`.
pub const TIDB_OPT_CONCURRENCY_FACTOR: &str = "tidb_opt_concurrency_factor";
/// The following optimizer cost factors represent a multiplier for each optimizer physical operator.
/// These factors are used to adjust the cost of each operator to influence the optimizer's plan selection.
/// Go `TiDBOptIndexScanCostFactor`.
pub const TIDB_OPT_INDEX_SCAN_COST_FACTOR: &str = "tidb_opt_index_scan_cost_factor";
/// Go `TiDBOptIndexReaderCostFactor`.
pub const TIDB_OPT_INDEX_READER_COST_FACTOR: &str = "tidb_opt_index_reader_cost_factor";
/// Go `TiDBOptTableReaderCostFactor`.
pub const TIDB_OPT_TABLE_READER_COST_FACTOR: &str = "tidb_opt_table_reader_cost_factor";
/// Go `TiDBOptTableFullScanCostFactor`.
pub const TIDB_OPT_TABLE_FULL_SCAN_COST_FACTOR: &str = "tidb_opt_table_full_scan_cost_factor";
/// Go `TiDBOptTableRangeScanCostFactor`.
pub const TIDB_OPT_TABLE_RANGE_SCAN_COST_FACTOR: &str = "tidb_opt_table_range_scan_cost_factor";
/// Go `TiDBOptTableRowIDScanCostFactor`.
pub const TIDB_OPT_TABLE_ROW_ID_SCAN_COST_FACTOR: &str = "tidb_opt_table_rowid_scan_cost_factor";
/// Go `TiDBOptTableTiFlashScanCostFactor`.
pub const TIDB_OPT_TABLE_TIFLASH_SCAN_COST_FACTOR: &str = "tidb_opt_table_tiflash_scan_cost_factor";
/// Go `TiDBOptIndexLookupCostFactor`.
pub const TIDB_OPT_INDEX_LOOKUP_COST_FACTOR: &str = "tidb_opt_index_lookup_cost_factor";
/// Go `TiDBOptIndexMergeCostFactor`.
pub const TIDB_OPT_INDEX_MERGE_COST_FACTOR: &str = "tidb_opt_index_merge_cost_factor";
/// Go `TiDBOptSortCostFactor`.
pub const TIDB_OPT_SORT_COST_FACTOR: &str = "tidb_opt_sort_cost_factor";
/// Go `TiDBOptTopNCostFactor`.
pub const TIDB_OPT_TOP_N_COST_FACTOR: &str = "tidb_opt_topn_cost_factor";
/// Go `TiDBOptLimitCostFactor`.
pub const TIDB_OPT_LIMIT_COST_FACTOR: &str = "tidb_opt_limit_cost_factor";
/// Go `TiDBOptStreamAggCostFactor`.
pub const TIDB_OPT_STREAM_AGG_COST_FACTOR: &str = "tidb_opt_stream_agg_cost_factor";
/// Go `TiDBOptHashAggCostFactor`.
pub const TIDB_OPT_HASH_AGG_COST_FACTOR: &str = "tidb_opt_hash_agg_cost_factor";
/// Go `TiDBOptMergeJoinCostFactor`.
pub const TIDB_OPT_MERGE_JOIN_COST_FACTOR: &str = "tidb_opt_merge_join_cost_factor";
/// Go `TiDBOptHashJoinCostFactor`.
pub const TIDB_OPT_HASH_JOIN_COST_FACTOR: &str = "tidb_opt_hash_join_cost_factor";
/// Go `TiDBOptIndexJoinCostFactor`.
pub const TIDB_OPT_INDEX_JOIN_COST_FACTOR: &str = "tidb_opt_index_join_cost_factor";
/// Go `TiDBOptIndexJoinMaxScanRowsRatio`.
pub const TIDB_OPT_INDEX_JOIN_MAX_SCAN_ROWS_RATIO: &str = "tidb_opt_index_join_max_scan_rows_ratio";
/// The following selectivity factors represent a multiplier for the selectivity of each predicate.
/// These factors are used to determine the selectivity of predicates in the optimizer's cost model.
/// TiDBOptSelectivityFactor: If one condition can't be calculated,
/// we will assume that the selectivity of this condition is 0.8 by default.
/// Go `TiDBOptSelectivityFactor`.
pub const TIDB_OPT_SELECTIVITY_FACTOR: &str = "tidb_opt_selectivity_factor";
/// TiDBOptForceInlineCTE is used to enable/disable inline CTE
/// Go `TiDBOptForceInlineCTE`.
pub const TIDB_OPT_FORCE_INLINE_CTE: &str = "tidb_opt_force_inline_cte";
/// TiDBIndexJoinBatchSize is used to set the batch size of an index lookup join.
/// The index lookup join fetches batches of data from outer executor and constructs ranges for inner executor.
/// This value controls how much of data in a batch to do the index join.
/// Large value may reduce the latency but consumes more system resource.
/// Go `TiDBIndexJoinBatchSize`.
pub const TIDB_INDEX_JOIN_BATCH_SIZE: &str = "tidb_index_join_batch_size";
/// TiDBIndexLookupSize is used for index lookup executor.
/// The index lookup executor first scan a batch of handles from a index, then use those handles to lookup the table
/// rows, this value controls how much of handles in a batch to do a lookup task.
/// Small value sends more RPCs to TiKV, consume more system resource.
/// Large value may do more work than needed if the query has a limit.
/// Go `TiDBIndexLookupSize`.
pub const TIDB_INDEX_LOOKUP_SIZE: &str = "tidb_index_lookup_size";
/// TiDBIndexLookupConcurrency is used for index lookup executor.
/// A lookup task may have 'tidb_index_lookup_size' of handles at maximum, the handles may be distributed
/// in many TiKV nodes, we execute multiple concurrent index lookup tasks concurrently to reduce the time
/// waiting for a task to finish.
/// Set this value higher may reduce the latency but consumes more system resource.
/// tidb_index_lookup_concurrency is deprecated, use tidb_executor_concurrency instead.
/// Go `TiDBIndexLookupConcurrency`.
pub const TIDB_INDEX_LOOKUP_CONCURRENCY: &str = "tidb_index_lookup_concurrency";
/// TiDBIndexLookupJoinConcurrency is used for index lookup join executor.
/// IndexLookUpJoin starts "tidb_index_lookup_join_concurrency" inner workers
/// to fetch inner rows and join the matched (outer, inner) row pairs.
/// tidb_index_lookup_join_concurrency is deprecated, use tidb_executor_concurrency instead.
/// Go `TiDBIndexLookupJoinConcurrency`.
pub const TIDB_INDEX_LOOKUP_JOIN_CONCURRENCY: &str = "tidb_index_lookup_join_concurrency";
/// TiDBIndexSerialScanConcurrency is used for controlling the concurrency of index scan operation
/// when we need to keep the data output order the same as the order of index data.
/// Deprecated: Use tidb_executor_concurrency for sequential scans and tidb_analyze_distsql_scan_concurrency for ANALYZE.
/// Before v5.0.0, this variable was used to control the concurrency of index scan operations for both regular queries and ANALYZE statements. See: https://github.com/pingcap/tidb/pull/16999
/// From version v5.0.0 up to (and including) v8.0.0, this variable was used only to control the concurrency of index scan operations for ANALYZE statements. See: https://github.com/pingcap/tidb/pull/50639
/// Go `TiDBIndexSerialScanConcurrency`.
pub const TIDB_INDEX_SERIAL_SCAN_CONCURRENCY: &str = "tidb_index_serial_scan_concurrency";
/// TiDBMaxChunkSize is used to control the max chunk size during query execution.
/// Go `TiDBMaxChunkSize`.
pub const TIDB_MAX_CHUNK_SIZE: &str = "tidb_max_chunk_size";
/// TiDBAllowBatchCop means if we should send batch coprocessor to TiFlash. It can be set to 0, 1 and 2.
/// 0 means never use batch cop, 1 means use batch cop in case of aggregation and join, 2, means to force sending batch cop for any query.
/// The default value is 0
/// Go `TiDBAllowBatchCop`.
pub const TIDB_ALLOW_BATCH_COP: &str = "tidb_allow_batch_cop";
/// TiDBShardRowIDBits means all the tables created in the current session will be sharded.
/// The default value is 0
/// Go `TiDBShardRowIDBits`.
pub const TIDB_SHARD_ROW_ID_BITS: &str = "tidb_shard_row_id_bits";
/// TiDBPreSplitRegions means all the tables created in the current session will be pre-splited.
/// The default value is 0
/// Go `TiDBPreSplitRegions`.
pub const TIDB_PRE_SPLIT_REGIONS: &str = "tidb_pre_split_regions";
/// TiDBAllowMPPExecution means if we should use mpp way to execute query or not.
/// Default value is `true`, means to be determined by the optimizer.
/// Value set to `false` means never use mpp.
/// Go `TiDBAllowMPPExecution`.
pub const TIDB_ALLOW_MPP_EXECUTION: &str = "tidb_allow_mpp";
/// TiDBAllowTiFlashCop means we only use MPP mode to query data.
/// Default value is `true`, means to be determined by the optimizer.
/// Value set to `false` means we may fall back to TiFlash cop plan if possible.
/// Go `TiDBAllowTiFlashCop`.
pub const TIDB_ALLOW_TIFLASH_COP: &str = "tidb_allow_tiflash_cop";
/// TiDBHashExchangeWithNewCollation means if hash exchange is supported when new collation is on.
/// Default value is `true`, means support hash exchange when new collation is on.
/// Value set to `false` means not support hash exchange when new collation is on.
/// Go `TiDBHashExchangeWithNewCollation`.
pub const TIDB_HASH_EXCHANGE_WITH_NEW_COLLATION: &str = "tidb_hash_exchange_with_new_collation";
/// TiDBEnforceMPPExecution means if we should enforce mpp way to execute query or not.
/// Default value is `false`, means to be determined by variable `tidb_allow_mpp`.
/// Value set to `true` means enforce use mpp.
/// Note if you want to set `tidb_enforce_mpp` to `true`, you must set `tidb_allow_mpp` to `true` first.
/// Go `TiDBEnforceMPPExecution`.
pub const TIDB_ENFORCE_MPP_EXECUTION: &str = "tidb_enforce_mpp";
/// TiDBMaxTiFlashThreads is the maximum number of threads to execute the request which is pushed down to tiflash.
/// Default value is -1, means it will not be pushed down to tiflash.
/// If the value is bigger than -1, it will be pushed down to tiflash and used to create db context in tiflash.
/// Go `TiDBMaxTiFlashThreads`.
pub const TIDB_MAX_TIFLASH_THREADS: &str = "tidb_max_tiflash_threads";
/// TiDBMaxBytesBeforeTiFlashExternalJoin is the maximum bytes used by a TiFlash join before spill to disk
/// Go `TiDBMaxBytesBeforeTiFlashExternalJoin`.
pub const TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN: &str =
    "tidb_max_bytes_before_tiflash_external_join";
/// TiDBMaxBytesBeforeTiFlashExternalGroupBy is the maximum bytes used by a TiFlash hash aggregation before spill to disk
/// Go `TiDBMaxBytesBeforeTiFlashExternalGroupBy`.
pub const TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY: &str =
    "tidb_max_bytes_before_tiflash_external_group_by";
/// TiDBMaxBytesBeforeTiFlashExternalSort is the maximum bytes used by a TiFlash sort/TopN before spill to disk
/// Go `TiDBMaxBytesBeforeTiFlashExternalSort`.
pub const TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT: &str =
    "tidb_max_bytes_before_tiflash_external_sort";
/// TiFlashMemQuotaQueryPerNode is the maximum bytes used by a TiFlash Query on each TiFlash node
/// Go `TiFlashMemQuotaQueryPerNode`.
pub const TIFLASH_MEM_QUOTA_QUERY_PER_NODE: &str = "tiflash_mem_quota_query_per_node";
/// TiFlashQuerySpillRatio is the threshold that TiFlash will trigger auto spill when the memory usage is above this percentage
/// Go `TiFlashQuerySpillRatio`.
pub const TIFLASH_QUERY_SPILL_RATIO: &str = "tiflash_query_spill_ratio";
/// TiFlashHashJoinVersion indicates whether to use hash join implementation v2 in TiFlash.
/// Go `TiFlashHashJoinVersion`.
pub const TIFLASH_HASH_JOIN_VERSION: &str = "tiflash_hash_join_version";
/// TiDBMPPStoreFailTTL is the unavailable time when a store is detected failed. During that time, tidb will not send any task to
/// TiFlash even though the failed TiFlash node has been recovered.
/// Go `TiDBMPPStoreFailTTL`.
pub const TIDB_MPP_STORE_FAIL_TTL: &str = "tidb_mpp_store_fail_ttl";
/// TiDBInitChunkSize is used to control the init chunk size during query execution.
/// Go `TiDBInitChunkSize`.
pub const TIDB_INIT_CHUNK_SIZE: &str = "tidb_init_chunk_size";
/// TiDBMinPagingSize is used to control the min paging size in the coprocessor paging protocol.
/// Go `TiDBMinPagingSize`.
pub const TIDB_MIN_PAGING_SIZE: &str = "tidb_min_paging_size";
/// TiDBMaxPagingSize is used to control the max paging size in the coprocessor paging protocol.
/// Go `TiDBMaxPagingSize`.
pub const TIDB_MAX_PAGING_SIZE: &str = "tidb_max_paging_size";
/// TiDBPagingSizeBytes is the byte budget per coprocessor page.
/// 0 means disabled (no byte-budget paging).
/// Go `TiDBPagingSizeBytes`.
pub const TIDB_PAGING_SIZE_BYTES: &str = "tidb_paging_size_bytes";
/// TiDBEnableCascadesPlanner is used to control whether to enable the cascades planner.
/// Go `TiDBEnableCascadesPlanner`.
pub const TIDB_ENABLE_CASCADES_PLANNER: &str = "tidb_enable_cascades_planner";
/// TiDBSkipUTF8Check skips the UTF8 validate process, validate UTF8 has performance cost, if we can make sure
/// the input string values are valid, we can skip the check.
/// Go `TiDBSkipUTF8Check`.
pub const TIDB_SKIP_UTF8_CHECK: &str = "tidb_skip_utf8_check";
/// TiDBSkipASCIICheck skips the ASCII validate process
/// old tidb may already have fields with invalid ASCII bytes
/// disable ASCII validate can guarantee a safe replication
/// Go `TiDBSkipASCIICheck`.
pub const TIDB_SKIP_ASCII_CHECK: &str = "tidb_skip_ascii_check";
/// TiDBHashJoinConcurrency is used for hash join executor.
/// The hash join outer executor starts multiple concurrent join workers to probe the hash table.
/// tidb_hash_join_concurrency is deprecated, use tidb_executor_concurrency instead.
/// Go `TiDBHashJoinConcurrency`.
pub const TIDB_HASH_JOIN_CONCURRENCY: &str = "tidb_hash_join_concurrency";
/// TiDBProjectionConcurrency is used for projection operator.
/// This variable controls the worker number of projection operator.
/// tidb_projection_concurrency is deprecated, use tidb_executor_concurrency instead.
/// Go `TiDBProjectionConcurrency`.
pub const TIDB_PROJECTION_CONCURRENCY: &str = "tidb_projection_concurrency";
/// TiDBHashAggPartialConcurrency is used for hash agg executor.
/// The hash agg executor starts multiple concurrent partial workers to do partial aggregate works.
/// tidb_hashagg_partial_concurrency is deprecated, use tidb_executor_concurrency instead.
/// Go `TiDBHashAggPartialConcurrency`.
pub const TIDB_HASH_AGG_PARTIAL_CONCURRENCY: &str = "tidb_hashagg_partial_concurrency";
/// TiDBHashAggFinalConcurrency is used for hash agg executor.
/// The hash agg executor starts multiple concurrent final workers to do final aggregate works.
/// tidb_hashagg_final_concurrency is deprecated, use tidb_executor_concurrency instead.
/// Go `TiDBHashAggFinalConcurrency`.
pub const TIDB_HASH_AGG_FINAL_CONCURRENCY: &str = "tidb_hashagg_final_concurrency";
/// TiDBWindowConcurrency is used for window parallel executor.
/// tidb_window_concurrency is deprecated, use tidb_executor_concurrency instead.
/// Go `TiDBWindowConcurrency`.
pub const TIDB_WINDOW_CONCURRENCY: &str = "tidb_window_concurrency";
/// TiDBMergeJoinConcurrency is used for merge join parallel executor
/// Go `TiDBMergeJoinConcurrency`.
pub const TIDB_MERGE_JOIN_CONCURRENCY: &str = "tidb_merge_join_concurrency";
/// TiDBStreamAggConcurrency is used for stream aggregation parallel executor.
/// tidb_stream_agg_concurrency is deprecated, use tidb_executor_concurrency instead.
/// Go `TiDBStreamAggConcurrency`.
pub const TIDB_STREAM_AGG_CONCURRENCY: &str = "tidb_streamagg_concurrency";
/// TiDBIndexMergeIntersectionConcurrency is used for parallel worker of index merge intersection.
/// Go `TiDBIndexMergeIntersectionConcurrency`.
pub const TIDB_INDEX_MERGE_INTERSECTION_CONCURRENCY: &str =
    "tidb_index_merge_intersection_concurrency";
/// TiDBEnableParallelApply is used for parallel apply.
/// Go `TiDBEnableParallelApply`.
pub const TIDB_ENABLE_PARALLEL_APPLY: &str = "tidb_enable_parallel_apply";
/// TiDBBackoffLockFast is used for tikv backoff base time in milliseconds.
/// Go `TiDBBackoffLockFast`.
pub const TIDB_BACKOFF_LOCK_FAST: &str = "tidb_backoff_lock_fast";
/// TiDBBackOffWeight is used to control the max back off time in TiDB.
/// The default maximum back off time is a small value.
/// BackOffWeight could multiply it to let the user adjust the maximum time for retrying.
/// Only positive integers can be accepted, which means that the maximum back off time can only grow.
/// Go `TiDBBackOffWeight`.
pub const TIDB_BACK_OFF_WEIGHT: &str = "tidb_backoff_weight";
/// TiDBDDLReorgWorkerCount defines the count of ddl reorg workers.
/// Go `TiDBDDLReorgWorkerCount`.
pub const TIDB_DDL_REORG_WORKER_COUNT: &str = "tidb_ddl_reorg_worker_cnt";
/// TiDBDDLFlashbackConcurrency defines the count of ddl flashback workers.
/// Go `TiDBDDLFlashbackConcurrency`.
pub const TIDB_DDL_FLASHBACK_CONCURRENCY: &str = "tidb_ddl_flashback_concurrency";
/// TiDBDDLReorgBatchSize defines the transaction batch size of ddl reorg workers.
/// Go `TiDBDDLReorgBatchSize`.
pub const TIDB_DDL_REORG_BATCH_SIZE: &str = "tidb_ddl_reorg_batch_size";
/// TiDBDDLErrorCountLimit defines the count of ddl error limit.
/// Go `TiDBDDLErrorCountLimit`.
pub const TIDB_DDL_ERROR_COUNT_LIMIT: &str = "tidb_ddl_error_count_limit";
/// TiDBDDLReorgPriority defines the operations' priority of adding indices.
/// It can be: PRIORITY_LOW, PRIORITY_NORMAL, PRIORITY_HIGH
/// Go `TiDBDDLReorgPriority`.
pub const TIDB_DDL_REORG_PRIORITY: &str = "tidb_ddl_reorg_priority";
/// TiDBDDLReorgMaxWriteSpeed defines the max write limitation for the lightning local backend
/// Go `TiDBDDLReorgMaxWriteSpeed`.
pub const TIDB_DDL_REORG_MAX_WRITE_SPEED: &str = "tidb_ddl_reorg_max_write_speed";
/// TiDBEnableAutoIncrementInGenerated disables the mysql compatibility check on using auto-incremented columns in
/// expression indexes and generated columns described here https://dev.mysql.com/doc/refman/5.7/en/create-table-generated-columns.html for details.
/// Go `TiDBEnableAutoIncrementInGenerated`.
pub const TIDB_ENABLE_AUTO_INCREMENT_IN_GENERATED: &str = "tidb_enable_auto_increment_in_generated";
/// TiDBEnablePointGetCache is used to control whether to enable the point get cache for special scenario.
/// Go `TiDBEnablePointGetCache`.
pub const TIDB_ENABLE_POINT_GET_CACHE: &str = "tidb_enable_point_get_cache";
/// TiDBPlacementMode is used to control the mode for placement
/// Go `TiDBPlacementMode`.
pub const TIDB_PLACEMENT_MODE: &str = "tidb_placement_mode";
/// TiDBMaxDeltaSchemaCount defines the max length of deltaSchemaInfos.
/// deltaSchemaInfos is a queue that maintains the history of schema changes.
/// Go `TiDBMaxDeltaSchemaCount`.
pub const TIDB_MAX_DELTA_SCHEMA_COUNT: &str = "tidb_max_delta_schema_count";
/// TiDBScatterRegion will scatter the regions for DDLs when it is "table" or "global", "" indicates not trigger scatter.
/// Go `TiDBScatterRegion`.
pub const TIDB_SCATTER_REGION: &str = "tidb_scatter_region";
/// TiDBWaitSplitRegionFinish defines the split region behaviour is sync or async.
/// Go `TiDBWaitSplitRegionFinish`.
pub const TIDB_WAIT_SPLIT_REGION_FINISH: &str = "tidb_wait_split_region_finish";
/// TiDBWaitSplitRegionTimeout uses to set the split and scatter region back off time.
/// Go `TiDBWaitSplitRegionTimeout`.
pub const TIDB_WAIT_SPLIT_REGION_TIMEOUT: &str = "tidb_wait_split_region_timeout";
/// TiDBForcePriority defines the operations' priority of all statements.
/// It can be "NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"
/// Go `TiDBForcePriority`.
pub const TIDB_FORCE_PRIORITY: &str = "tidb_force_priority";
/// TiDBConstraintCheckInPlace indicates to check the constraint when the SQL executing.
/// It could hurt the performance of bulking insert when it is ON.
/// Go `TiDBConstraintCheckInPlace`.
pub const TIDB_CONSTRAINT_CHECK_IN_PLACE: &str = "tidb_constraint_check_in_place";
/// TiDBEnableWindowFunction is used to control whether to enable the window function.
/// Go `TiDBEnableWindowFunction`.
pub const TIDB_ENABLE_WINDOW_FUNCTION: &str = "tidb_enable_window_function";
/// TiDBEnablePipelinedWindowFunction is used to control whether to use pipelined window function, it only works when tidb_enable_window_function = true.
/// Go `TiDBEnablePipelinedWindowFunction`.
pub const TIDB_ENABLE_PIPELINED_WINDOW_FUNCTION: &str = "tidb_enable_pipelined_window_function";
/// TiDBEnableStrictNotNullCheck is used to control whether to enable strict not-null check for single-row insert in non-strict mode.
/// Go `TiDBEnableStrictNotNullCheck`.
pub const TIDB_ENABLE_STRICT_NOT_NULL_CHECK: &str = "tidb_enable_strict_not_null_check";
/// TiDBEnableStrictDoubleTypeCheck is used to control table field double type syntax check.
/// Go `TiDBEnableStrictDoubleTypeCheck`.
pub const TIDB_ENABLE_STRICT_DOUBLE_TYPE_CHECK: &str = "tidb_enable_strict_double_type_check";
/// TiDBOptProjectionPushDown is used to control whether to pushdown projection to coprocessor.
/// Go `TiDBOptProjectionPushDown`.
pub const TIDB_OPT_PROJECTION_PUSH_DOWN: &str = "tidb_opt_projection_push_down";
/// TiDBEnableVectorizedExpression is used to control whether to enable the vectorized expression evaluation.
/// Go `TiDBEnableVectorizedExpression`.
pub const TIDB_ENABLE_VECTORIZED_EXPRESSION: &str = "tidb_enable_vectorized_expression";
/// TiDBOptJoinReorderThreshold defines the threshold less than which
/// we'll choose a rather time-consuming algorithm to calculate the join order.
/// Go `TiDBOptJoinReorderThreshold`.
pub const TIDB_OPT_JOIN_REORDER_THRESHOLD: &str = "tidb_opt_join_reorder_threshold";
/// TiDBOptEnableAdvancedJoinReorder controls whether to use the advanced join reorder framework.
/// Go `TiDBOptEnableAdvancedJoinReorder`.
pub const TIDB_OPT_ENABLE_ADVANCED_JOIN_REORDER: &str = "tidb_opt_enable_advanced_join_reorder";
/// TiDBOptJoinReorderThroughProj enables join reorder to look through projection operators
/// when extracting join groups. This allows join reorder to work with derived columns from CTEs,
/// views, or subqueries that have expression computations in their SELECT list.
/// Go `TiDBOptJoinReorderThroughProj`.
pub const TIDB_OPT_JOIN_REORDER_THROUGH_PROJ: &str = "tidb_opt_join_reorder_through_proj";
/// TiDBOptJoinReorderThroughSel enables pushing selection conditions down to
/// reordered join trees when applicable.
/// Go `TiDBOptJoinReorderThroughSel`.
pub const TIDB_OPT_JOIN_REORDER_THROUGH_SEL: &str = "tidb_opt_join_reorder_through_sel";
/// TiDBSlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
/// Go `TiDBSlowQueryFile`.
pub const TIDB_SLOW_QUERY_FILE: &str = "tidb_slow_query_file";
/// TiDBEnableFastAnalyze indicates to use fast analyze.
/// Deprecated: This variable is deprecated, please do not use this variable.
/// Go `TiDBEnableFastAnalyze`.
pub const TIDB_ENABLE_FAST_ANALYZE: &str = "tidb_enable_fast_analyze";
/// TiDBExpensiveQueryTimeThreshold indicates the time threshold of expensive query.
/// Go `TiDBExpensiveQueryTimeThreshold`.
pub const TIDB_EXPENSIVE_QUERY_TIME_THRESHOLD: &str = "tidb_expensive_query_time_threshold";
/// TiDBExpensiveTxnTimeThreshold indicates the time threshold of expensive transaction.
/// Go `TiDBExpensiveTxnTimeThreshold`.
pub const TIDB_EXPENSIVE_TXN_TIME_THRESHOLD: &str = "tidb_expensive_txn_time_threshold";
/// TiDBEnableIndexMerge indicates to generate IndexMergePath.
/// Go `TiDBEnableIndexMerge`.
pub const TIDB_ENABLE_INDEX_MERGE: &str = "tidb_enable_index_merge";
/// TiDBEnableNoBackslashEscapesInLike controls whether NO_BACKSLASH_ESCAPES affects LIKE default escape.
/// Go `TiDBEnableNoBackslashEscapesInLike`.
pub const TIDB_ENABLE_NO_BACKSLASH_ESCAPES_IN_LIKE: &str =
    "tidb_enable_no_backslash_escapes_in_like";
/// TiDBEnableNoopFuncs set true will enable using fake funcs(like get_lock release_lock)
/// Go `TiDBEnableNoopFuncs`.
pub const TIDB_ENABLE_NOOP_FUNCS: &str = "tidb_enable_noop_functions";
/// TiDBEnableStmtSummary indicates whether the statement summary is enabled.
/// Go `TiDBEnableStmtSummary`.
pub const TIDB_ENABLE_STMT_SUMMARY: &str = "tidb_enable_stmt_summary";
/// TiDBStmtSummaryInternalQuery indicates whether the statement summary contain internal query.
/// Go `TiDBStmtSummaryInternalQuery`.
pub const TIDB_STMT_SUMMARY_INTERNAL_QUERY: &str = "tidb_stmt_summary_internal_query";
/// TiDBStmtSummaryRefreshInterval indicates the refresh interval in seconds for each statement summary.
/// Go `TiDBStmtSummaryRefreshInterval`.
pub const TIDB_STMT_SUMMARY_REFRESH_INTERVAL: &str = "tidb_stmt_summary_refresh_interval";
/// TiDBStmtSummaryHistorySize indicates the history size of each statement summary.
/// Go `TiDBStmtSummaryHistorySize`.
pub const TIDB_STMT_SUMMARY_HISTORY_SIZE: &str = "tidb_stmt_summary_history_size";
/// TiDBStmtSummaryMaxStmtCount indicates the max number of statements kept in memory.
/// Go `TiDBStmtSummaryMaxStmtCount`.
pub const TIDB_STMT_SUMMARY_MAX_STMT_COUNT: &str = "tidb_stmt_summary_max_stmt_count";
/// TiDBStmtSummaryMaxSQLLength indicates the max length of displayed normalized sql and sample sql.
/// Go `TiDBStmtSummaryMaxSQLLength`.
pub const TIDB_STMT_SUMMARY_MAX_SQL_LENGTH: &str = "tidb_stmt_summary_max_sql_length";
/// TiDBStmtSummaryPersistEvicted controls whether per-record LRU evictions
/// in the v2 (persistent) statement summary are persisted to the stmt log.
/// Off by default because it adds log volume proportional to eviction rate.
/// Go `TiDBStmtSummaryPersistEvicted`.
pub const TIDB_STMT_SUMMARY_PERSIST_EVICTED: &str = "tidb_stmt_summary_persist_evicted";
/// TiDBStmtSummaryGroupByUser, when enabled, adds the executing user to the
/// statement summary grouping key so the same digest run by different users
/// produces separate rows. Off by default to avoid cardinality growth.
/// Go `TiDBStmtSummaryGroupByUser`.
pub const TIDB_STMT_SUMMARY_GROUP_BY_USER: &str = "tidb_stmt_summary_group_by_user";
/// TiDBIgnoreInlistPlanDigest enables TiDB to generate the same plan digest with SQL using different in-list arguments.
/// Go `TiDBIgnoreInlistPlanDigest`.
pub const TIDB_IGNORE_INLIST_PLAN_DIGEST: &str = "tidb_ignore_inlist_plan_digest";
/// TiDBCapturePlanBaseline indicates whether the capture of plan baselines is enabled.
/// Go `TiDBCapturePlanBaseline`.
pub const TIDB_CAPTURE_PLAN_BASELINE: &str = "tidb_capture_plan_baselines";
/// TiDBUsePlanBaselines indicates whether the use of plan baselines is enabled.
/// Go `TiDBUsePlanBaselines`.
pub const TIDB_USE_PLAN_BASELINES: &str = "tidb_use_plan_baselines";
/// TiDBEvolvePlanBaselines indicates whether the evolution of plan baselines is enabled.
/// Go `TiDBEvolvePlanBaselines`.
pub const TIDB_EVOLVE_PLAN_BASELINES: &str = "tidb_evolve_plan_baselines";
/// TiDBOptEnableFuzzyBinding indicates whether to enable the universal binding.
/// Go `TiDBOptEnableFuzzyBinding`.
pub const TIDB_OPT_ENABLE_FUZZY_BINDING: &str = "tidb_opt_enable_fuzzy_binding";
/// TiDBEnableExtendedStats is kept only for system variable compatibility. Extended statistics support has been removed.
/// Go `TiDBEnableExtendedStats`.
pub const TIDB_ENABLE_EXTENDED_STATS: &str = "tidb_enable_extended_stats";
/// TiDBIsolationReadEngines indicates the tidb only read from the stores whose engine type is involved in IsolationReadEngines.
/// Now, only support TiKV and TiFlash.
/// Go `TiDBIsolationReadEngines`.
pub const TIDB_ISOLATION_READ_ENGINES: &str = "tidb_isolation_read_engines";
/// TiDBStoreLimit indicates the limit of sending request to a store, 0 means without limit.
/// Go `TiDBStoreLimit`.
pub const TIDB_STORE_LIMIT: &str = "tidb_store_limit";
/// TiDBMetricSchemaStep indicates the step when query metric schema.
/// Go `TiDBMetricSchemaStep`.
pub const TIDB_METRIC_SCHEMA_STEP: &str = "tidb_metric_query_step";
/// TiDBCDCWriteSource indicates the following data is written by TiCDC if it is not 0.
/// Go `TiDBCDCWriteSource`.
pub const TIDB_CDC_WRITE_SOURCE: &str = "tidb_cdc_write_source";
/// TiDBMetricSchemaRangeDuration indicates the range duration when query metric schema.
/// Go `TiDBMetricSchemaRangeDuration`.
pub const TIDB_METRIC_SCHEMA_RANGE_DURATION: &str = "tidb_metric_query_range_duration";
/// TiDBEnableCollectExecutionInfo indicates that whether execution info is collected.
/// Go `TiDBEnableCollectExecutionInfo`.
pub const TIDB_ENABLE_COLLECT_EXECUTION_INFO: &str = "tidb_enable_collect_execution_info";
/// TiDBExecutorConcurrency is used for controlling the concurrency of all types of executors.
/// Go `TiDBExecutorConcurrency`.
pub const TIDB_EXECUTOR_CONCURRENCY: &str = "tidb_executor_concurrency";
/// TiDBEnableClusteredIndex indicates if clustered index feature is enabled.
/// Go `TiDBEnableClusteredIndex`.
pub const TIDB_ENABLE_CLUSTERED_INDEX: &str = "tidb_enable_clustered_index";
/// TiDBEnableGlobalIndex means if we could create an global index on a partition table or not.
/// Deprecated, will always be ON
/// Go `TiDBEnableGlobalIndex`.
pub const TIDB_ENABLE_GLOBAL_INDEX: &str = "tidb_enable_global_index";
/// TiDBPartitionPruneMode indicates the partition prune mode used.
/// Go `TiDBPartitionPruneMode`.
pub const TIDB_PARTITION_PRUNE_MODE: &str = "tidb_partition_prune_mode";
/// TiDBRedactLog indicates that whether redact log.
/// Go `TiDBRedactLog`.
pub const TIDB_REDACT_LOG: &str = "tidb_redact_log";
/// TiDBRestrictedReadOnly is meant for the cloud admin to toggle the cluster read only
/// Go `TiDBRestrictedReadOnly`.
pub const TIDB_RESTRICTED_READ_ONLY: &str = "tidb_restricted_read_only";
/// TiDBSuperReadOnly is tidb's variant of mysql's super_read_only, which has some differences from mysql's super_read_only.
/// Go `TiDBSuperReadOnly`.
pub const TIDB_SUPER_READ_ONLY: &str = "tidb_super_read_only";
/// TiDBShardAllocateStep indicates the max size of continuous rowid shard in one transaction.
/// Go `TiDBShardAllocateStep`.
pub const TIDB_SHARD_ALLOCATE_STEP: &str = "tidb_shard_allocate_step";
/// TiDBEnableTelemetry indicates that whether usage data report to PingCAP is enabled.
/// Deprecated: it is 'off' always since Telemetry has been removed from TiDB.
/// Go `TiDBEnableTelemetry`.
pub const TIDB_ENABLE_TELEMETRY: &str = "tidb_enable_telemetry";
/// TiDBMemoryUsageAlarmRatio indicates the alarm threshold when memory usage of the tidb-server exceeds.
/// Go `TiDBMemoryUsageAlarmRatio`.
pub const TIDB_MEMORY_USAGE_ALARM_RATIO: &str = "tidb_memory_usage_alarm_ratio";
/// TiDBMemoryUsageAlarmKeepRecordNum indicates the number of saved alarm files.
/// Go `TiDBMemoryUsageAlarmKeepRecordNum`.
pub const TIDB_MEMORY_USAGE_ALARM_KEEP_RECORD_NUM: &str = "tidb_memory_usage_alarm_keep_record_num";
/// TiDBEnableRateLimitAction indicates whether enabled ratelimit action
/// Go `TiDBEnableRateLimitAction`.
pub const TIDB_ENABLE_RATE_LIMIT_ACTION: &str = "tidb_enable_rate_limit_action";
/// TiDBEnableAsyncCommit indicates whether to enable the async commit feature.
/// Go `TiDBEnableAsyncCommit`.
pub const TIDB_ENABLE_ASYNC_COMMIT: &str = "tidb_enable_async_commit";
/// TiDBEnable1PC indicates whether to enable the one-phase commit feature.
/// Go `TiDBEnable1PC`.
pub const TIDB_ENABLE1_PC: &str = "tidb_enable_1pc";
/// TiDBGuaranteeLinearizability indicates whether to guarantee linearizability.
/// Go `TiDBGuaranteeLinearizability`.
pub const TIDB_GUARANTEE_LINEARIZABILITY: &str = "tidb_guarantee_linearizability";
/// TiDBAnalyzeVersion indicates how tidb collects the analyzed statistics and how use to it.
/// Go `TiDBAnalyzeVersion`.
pub const TIDB_ANALYZE_VERSION: &str = "tidb_analyze_version";
/// TiDBAutoAnalyzePartitionBatchSize indicates the batch size for partition tables for auto analyze in dynamic mode
/// Deprecated: This variable is deprecated, please do not use this variable.
/// Go `TiDBAutoAnalyzePartitionBatchSize`.
pub const TIDB_AUTO_ANALYZE_PARTITION_BATCH_SIZE: &str = "tidb_auto_analyze_partition_batch_size";
/// TiDBEnableIndexMergeJoin indicates whether to enable index merge join.
/// Go `TiDBEnableIndexMergeJoin`.
pub const TIDB_ENABLE_INDEX_MERGE_JOIN: &str = "tidb_enable_index_merge_join";
/// TiDBTrackAggregateMemoryUsage indicates whether track the memory usage of aggregate function.
/// Go `TiDBTrackAggregateMemoryUsage`.
pub const TIDB_TRACK_AGGREGATE_MEMORY_USAGE: &str = "tidb_track_aggregate_memory_usage";
/// TiDBEnableExchangePartition indicates whether to enable exchange partition.
/// Go `TiDBEnableExchangePartition`.
pub const TIDB_ENABLE_EXCHANGE_PARTITION: &str = "tidb_enable_exchange_partition";
/// TiDBAllowFallbackToTiKV indicates the engine types whose unavailability triggers fallback to TiKV.
/// Now we only support TiFlash.
/// Go `TiDBAllowFallbackToTiKV`.
pub const TIDB_ALLOW_FALLBACK_TO_TIKV: &str = "tidb_allow_fallback_to_tikv";
/// TiDBEnableTopSQL indicates whether the top SQL is enabled.
/// Go `TiDBEnableTopSQL`.
pub const TIDB_ENABLE_TOP_SQL: &str = "tidb_enable_top_sql";
/// TiDBSourceID indicates the source ID of the TiDB server.
/// Go `TiDBSourceID`.
pub const TIDB_SOURCE_ID: &str = "tidb_source_id";
/// TiDBTopSQLMaxTimeSeriesCount indicates the max number of statements been collected in each time series.
/// Go `TiDBTopSQLMaxTimeSeriesCount`.
pub const TIDB_TOP_SQL_MAX_TIME_SERIES_COUNT: &str = "tidb_top_sql_max_time_series_count";
/// TiDBTopSQLMaxMetaCount indicates the max capacity of the collect meta per second.
/// Go `TiDBTopSQLMaxMetaCount`.
pub const TIDB_TOP_SQL_MAX_META_COUNT: &str = "tidb_top_sql_max_meta_count";
/// TiDBEnableLocalTxn indicates whether to enable Local Txn.
/// Go `TiDBEnableLocalTxn`.
pub const TIDB_ENABLE_LOCAL_TXN: &str = "tidb_enable_local_txn";
/// TiDBEnableMDL indicates whether to enable MDL.
/// Go `TiDBEnableMDL`.
pub const TIDB_ENABLE_MDL: &str = "tidb_enable_metadata_lock";
/// TiDBTSOClientBatchMaxWaitTime indicates the max value of the TSO Batch Wait interval time of PD client.
/// Go `TiDBTSOClientBatchMaxWaitTime`.
pub const TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME: &str = "tidb_tso_client_batch_max_wait_time";
/// TiDBTxnCommitBatchSize is used to control the batch size of transaction commit related requests sent by TiDB to TiKV.
/// If a single transaction has a large amount of writes, you can increase the batch size to improve the batch effect,
/// setting too large will exceed TiKV's raft-entry-max-size limit and cause commit failure.
/// Go `TiDBTxnCommitBatchSize`.
pub const TIDB_TXN_COMMIT_BATCH_SIZE: &str = "tidb_txn_commit_batch_size";
/// TiDBEnableTSOFollowerProxy indicates whether to enable the TSO Follower Proxy feature of PD client.
/// Go `TiDBEnableTSOFollowerProxy`.
pub const TIDB_ENABLE_TSO_FOLLOWER_PROXY: &str = "tidb_enable_tso_follower_proxy";
/// PDEnableFollowerHandleRegion indicates whether to enable the PD Follower handle region API.
/// TODO: deprecated this variable to use a format like `tidb_enable_pd_follower_handle_region`.
/// Go `PDEnableFollowerHandleRegion`.
pub const PD_ENABLE_FOLLOWER_HANDLE_REGION: &str = "pd_enable_follower_handle_region";
/// TiDBEnableBatchQueryRegion indicates whether to enable the batch query region feature.
/// Go `TiDBEnableBatchQueryRegion`.
pub const TIDB_ENABLE_BATCH_QUERY_REGION: &str = "tidb_enable_batch_query_region";
/// TiDBEnableOrderedResultMode indicates if stabilize query results.
/// Go `TiDBEnableOrderedResultMode`.
pub const TIDB_ENABLE_ORDERED_RESULT_MODE: &str = "tidb_enable_ordered_result_mode";
/// TiDBRemoveOrderbyInSubquery indicates whether to remove ORDER BY in subquery.
/// Go `TiDBRemoveOrderbyInSubquery`.
pub const TIDB_REMOVE_ORDERBY_IN_SUBQUERY: &str = "tidb_remove_orderby_in_subquery";
/// TiDBEnablePseudoForOutdatedStats indicates whether use pseudo for outdated stats
/// Go `TiDBEnablePseudoForOutdatedStats`.
pub const TIDB_ENABLE_PSEUDO_FOR_OUTDATED_STATS: &str = "tidb_enable_pseudo_for_outdated_stats";
/// TiDBRegardNULLAsPoint indicates whether regard NULL as point when optimizing
/// Go `TiDBRegardNULLAsPoint`.
pub const TIDB_REGARD_NULL_AS_POINT: &str = "tidb_regard_null_as_point";
/// TiDBTmpTableMaxSize indicates the max memory size of temporary tables.
/// Go `TiDBTmpTableMaxSize`.
pub const TIDB_TMP_TABLE_MAX_SIZE: &str = "tidb_tmp_table_max_size";
/// TiDBEnableLegacyInstanceScope indicates if instance scope can be set with SET SESSION.
/// Go `TiDBEnableLegacyInstanceScope`.
pub const TIDB_ENABLE_LEGACY_INSTANCE_SCOPE: &str = "tidb_enable_legacy_instance_scope";
/// TiDBTableCacheLease indicates the read lock lease of a cached table.
/// Go `TiDBTableCacheLease`.
pub const TIDB_TABLE_CACHE_LEASE: &str = "tidb_table_cache_lease";
/// TiDBStatsLoadSyncWait indicates the time sql execution will sync-wait for stats load.
/// Go `TiDBStatsLoadSyncWait`.
pub const TIDB_STATS_LOAD_SYNC_WAIT: &str = "tidb_stats_load_sync_wait";
/// TiDBEnableMutationChecker indicates whether to check data consistency for mutations
/// Go `TiDBEnableMutationChecker`.
pub const TIDB_ENABLE_MUTATION_CHECKER: &str = "tidb_enable_mutation_checker";
/// TiDBTxnAssertionLevel indicates how strict the assertion will be, which helps to detect and preventing data &
/// index inconsistency problems.
/// Go `TiDBTxnAssertionLevel`.
pub const TIDB_TXN_ASSERTION_LEVEL: &str = "tidb_txn_assertion_level";
/// TiDBIgnorePreparedCacheCloseStmt indicates whether to ignore close-stmt commands for prepared statements.
/// Go `TiDBIgnorePreparedCacheCloseStmt`.
pub const TIDB_IGNORE_PREPARED_CACHE_CLOSE_STMT: &str = "tidb_ignore_prepared_cache_close_stmt";
/// TiDBEnableNewCostInterface is a internal switch to indicates whether to use the new cost calculation interface.
/// Go `TiDBEnableNewCostInterface`.
pub const TIDB_ENABLE_NEW_COST_INTERFACE: &str = "tidb_enable_new_cost_interface";
/// TiDBCostModelVersion is a internal switch to indicates the cost model version.
/// Go `TiDBCostModelVersion`.
pub const TIDB_COST_MODEL_VERSION: &str = "tidb_cost_model_version";
/// TiDBIndexJoinDoubleReadPenaltyCostRate indicates whether to add some penalty cost to IndexJoin and how much of it.
/// IndexJoin can cause plenty of extra double read tasks, which consume lots of resources and take a long time.
/// Since the number of double read tasks is hard to estimated accurately, we leave this variable to let us can adjust this
/// part of cost manually.
/// Go `TiDBIndexJoinDoubleReadPenaltyCostRate`.
pub const TIDB_INDEX_JOIN_DOUBLE_READ_PENALTY_COST_RATE: &str =
    "tidb_index_join_double_read_penalty_cost_rate";
/// TiDBBatchPendingTiFlashCount indicates the maximum count of non-available TiFlash tables.
/// Go `TiDBBatchPendingTiFlashCount`.
pub const TIDB_BATCH_PENDING_TIFLASH_COUNT: &str = "tidb_batch_pending_tiflash_count";
/// TiDBQueryLogMaxLen is used to set the max length of the query in the log.
/// Go `TiDBQueryLogMaxLen`.
pub const TIDB_QUERY_LOG_MAX_LEN: &str = "tidb_query_log_max_len";
/// TiDBEnableNoopVariables is used to indicate if noops appear in SHOW [GLOBAL] VARIABLES
/// Go `TiDBEnableNoopVariables`.
pub const TIDB_ENABLE_NOOP_VARIABLES: &str = "tidb_enable_noop_variables";
/// TiDBNonTransactionalIgnoreError is used to ignore error in non-transactional DMLs.
/// When set to false, a non-transactional DML returns when it meets the first error.
/// When set to true, a non-transactional DML finishes all batches even if errors are met in some batches.
/// Go `TiDBNonTransactionalIgnoreError`.
pub const TIDB_NON_TRANSACTIONAL_IGNORE_ERROR: &str = "tidb_nontransactional_ignore_error";
/// Fine grained shuffle is disabled when TiFlashFineGrainedShuffleStreamCount is zero.
/// Go `TiFlashFineGrainedShuffleStreamCount`.
pub const TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT: &str =
    "tiflash_fine_grained_shuffle_stream_count";
/// Go `TiFlashFineGrainedShuffleBatchSize`.
pub const TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE: &str = "tiflash_fine_grained_shuffle_batch_size";
/// TiDBSimplifiedMetrics controls whether to unregister some unused metrics.
/// Go `TiDBSimplifiedMetrics`.
pub const TIDB_SIMPLIFIED_METRICS: &str = "tidb_simplified_metrics";
/// TiDBMemoryDebugModeMinHeapInUse is used to set tidb memory debug mode trigger threshold.
/// When set to 0, the function is disabled.
/// When set to a negative integer, use memory debug mode to detect the issue of frequent allocation and release of memory.
/// We do not actively trigger gc, and check whether the `tracker memory * (1+bias ratio) > heap in use` each 5s.
/// When set to a positive integer, use memory debug mode to detect the issue of memory tracking inaccurate.
/// We trigger runtime.GC() each 5s, and check whether the `tracker memory * (1+bias ratio) > heap in use`.
/// Go `TiDBMemoryDebugModeMinHeapInUse`.
pub const TIDB_MEMORY_DEBUG_MODE_MIN_HEAP_IN_USE: &str = "tidb_memory_debug_mode_min_heap_inuse";
/// TiDBMemoryDebugModeAlarmRatio is used set tidb memory debug mode bias ratio. Treat memory bias less than this ratio as noise.
/// Go `TiDBMemoryDebugModeAlarmRatio`.
pub const TIDB_MEMORY_DEBUG_MODE_ALARM_RATIO: &str = "tidb_memory_debug_mode_alarm_ratio";
/// TiDBEnableAnalyzeSnapshot indicates whether to read data on snapshot when collecting statistics.
/// When set to false, ANALYZE reads the latest data.
/// When set to true, ANALYZE reads data on the snapshot at the beginning of ANALYZE.
/// Go `TiDBEnableAnalyzeSnapshot`.
pub const TIDB_ENABLE_ANALYZE_SNAPSHOT: &str = "tidb_enable_analyze_snapshot";
/// TiDBDefaultStrMatchSelectivity controls some special cardinality estimation strategy for string match functions (like and regexp).
/// When set to 0, Selectivity() will try to evaluate those functions with TopN and NULL in the stats to estimate,
/// and the default selectivity and the selectivity for the histogram part will be 0.1.
/// When set to (0, 1], Selectivity() will use the value of this variable as the default selectivity of those
/// functions instead of the selectionFactor (0.8).
/// Go `TiDBDefaultStrMatchSelectivity`.
pub const TIDB_DEFAULT_STR_MATCH_SELECTIVITY: &str = "tidb_default_string_match_selectivity";
/// TiDBEnablePrepPlanCache indicates whether to enable prepared plan cache
/// Go `TiDBEnablePrepPlanCache`.
pub const TIDB_ENABLE_PREP_PLAN_CACHE: &str = "tidb_enable_prepared_plan_cache";
/// TiDBPrepPlanCacheSize indicates the number of cached statements.
/// This variable is deprecated, use tidb_session_plan_cache_size instead.
/// Go `TiDBPrepPlanCacheSize`.
pub const TIDB_PREP_PLAN_CACHE_SIZE: &str = "tidb_prepared_plan_cache_size";
/// TiDBEnablePrepPlanCacheMemoryMonitor indicates whether to enable prepared plan cache monitor
/// Go `TiDBEnablePrepPlanCacheMemoryMonitor`.
pub const TIDB_ENABLE_PREP_PLAN_CACHE_MEMORY_MONITOR: &str =
    "tidb_enable_prepared_plan_cache_memory_monitor";
/// TiDBEnableNonPreparedPlanCache indicates whether to enable non-prepared plan cache.
/// Go `TiDBEnableNonPreparedPlanCache`.
pub const TIDB_ENABLE_NON_PREPARED_PLAN_CACHE: &str = "tidb_enable_non_prepared_plan_cache";
/// TiDBEnableNonPreparedPlanCacheForDML indicates whether to enable non-prepared plan cache for DML statements.
/// Go `TiDBEnableNonPreparedPlanCacheForDML`.
pub const TIDB_ENABLE_NON_PREPARED_PLAN_CACHE_FOR_DML: &str =
    "tidb_enable_non_prepared_plan_cache_for_dml";
/// TiDBPlanCacheStrategy controls plan cache strategy.
/// Go `TiDBPlanCacheStrategy`.
pub const TIDB_PLAN_CACHE_STRATEGY: &str = "tidb_plan_cache_strategy";
/// TiDBPlanCacheStrategyAll is one strategy value for TiDBPlanCacheStrategy.
/// Go `TiDBPlanCacheStrategyAll`.
pub const TIDB_PLAN_CACHE_STRATEGY_ALL: &str = "all";
/// TiDBPlanCacheStrategyHintOnly is one strategy value for TiDBPlanCacheStrategy.
/// Go `TiDBPlanCacheStrategyHintOnly`.
pub const TIDB_PLAN_CACHE_STRATEGY_HINT_ONLY: &str = "hint_only";
/// TiDBNonPreparedPlanCacheSize controls the size of non-prepared plan cache.
/// This variable is deprecated, use tidb_session_plan_cache_size instead.
/// Go `TiDBNonPreparedPlanCacheSize`.
pub const TIDB_NON_PREPARED_PLAN_CACHE_SIZE: &str = "tidb_non_prepared_plan_cache_size";
/// TiDBPlanCacheMaxPlanSize controls the maximum size of a plan that can be cached.
/// Go `TiDBPlanCacheMaxPlanSize`.
pub const TIDB_PLAN_CACHE_MAX_PLAN_SIZE: &str = "tidb_plan_cache_max_plan_size";
/// TiDBPlanCacheInvalidationOnFreshStats controls if plan cache will be invalidated automatically when
/// related stats are analyzed after the plan cache is generated.
/// Go `TiDBPlanCacheInvalidationOnFreshStats`.
pub const TIDB_PLAN_CACHE_INVALIDATION_ON_FRESH_STATS: &str =
    "tidb_plan_cache_invalidation_on_fresh_stats";
/// TiDBPlanCacheSkipStatsOnBinding controls if plan cache skips stats-version invalidation when
/// a SQL binding is matched. Since a binding pins the plan via hints, stats changes cannot alter
/// the chosen plan, so invalidating the cache entry on stats updates is unnecessary.
/// Go `TiDBPlanCacheSkipStatsOnBinding`.
pub const TIDB_PLAN_CACHE_SKIP_STATS_ON_BINDING: &str = "tidb_plan_cache_skip_stats_on_binding";
/// TiDBSessionPlanCacheSize controls the size of session plan cache.
/// Go `TiDBSessionPlanCacheSize`.
pub const TIDB_SESSION_PLAN_CACHE_SIZE: &str = "tidb_session_plan_cache_size";
/// TiDBEnableInstancePlanCache indicates whether to enable instance plan cache.
/// If this variable is false, session-level plan cache will be used.
/// Go `TiDBEnableInstancePlanCache`.
pub const TIDB_ENABLE_INSTANCE_PLAN_CACHE: &str = "tidb_enable_instance_plan_cache";
/// TiDBInstancePlanCacheReservedPercentage indicates the percentage memory to evict.
/// Go `TiDBInstancePlanCacheReservedPercentage`.
pub const TIDB_INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE: &str =
    "tidb_instance_plan_cache_reserved_percentage";
/// TiDBInstancePlanCacheMaxMemSize indicates the maximum memory size of instance plan cache.
/// Go `TiDBInstancePlanCacheMaxMemSize`.
pub const TIDB_INSTANCE_PLAN_CACHE_MAX_MEM_SIZE: &str = "tidb_instance_plan_cache_max_size";
/// TiDBConstraintCheckInPlacePessimistic controls whether to skip certain kinds of pessimistic locks.
/// Go `TiDBConstraintCheckInPlacePessimistic`.
pub const TIDB_CONSTRAINT_CHECK_IN_PLACE_PESSIMISTIC: &str =
    "tidb_constraint_check_in_place_pessimistic";
/// TiDBEnableForeignKey indicates whether to enable foreign key feature.
/// TODO(crazycs520): remove this after foreign key GA.
/// Go `TiDBEnableForeignKey`.
pub const TIDB_ENABLE_FOREIGN_KEY: &str = "tidb_enable_foreign_key";
/// TiDBForeignKeyCheckInSharedLock indicates whether to use shared lock for foreign key check.
/// Go `TiDBForeignKeyCheckInSharedLock`.
pub const TIDB_FOREIGN_KEY_CHECK_IN_SHARED_LOCK: &str = "tidb_foreign_key_check_in_shared_lock";
/// TiDBOptRangeMaxSize is the max memory limit for ranges. When the optimizer estimates that the memory usage of complete
/// ranges would exceed the limit, it chooses less accurate ranges such as full range. 0 indicates that there is no memory
/// limit for ranges.
/// Go `TiDBOptRangeMaxSize`.
pub const TIDB_OPT_RANGE_MAX_SIZE: &str = "tidb_opt_range_max_size";
/// TiDBOptAdvancedJoinHint indicates whether the join method hint is compatible with join order hint.
/// Go `TiDBOptAdvancedJoinHint`.
pub const TIDB_OPT_ADVANCED_JOIN_HINT: &str = "tidb_opt_advanced_join_hint";
/// TiDBOptUseInvisibleIndexes indicates whether to use invisible indexes.
/// Go `TiDBOptUseInvisibleIndexes`.
pub const TIDB_OPT_USE_INVISIBLE_INDEXES: &str = "tidb_opt_use_invisible_indexes";
/// TiDBAnalyzePartitionConcurrency is the number of concurrent workers to save statistics to the system tables.
/// Go `TiDBAnalyzePartitionConcurrency`.
pub const TIDB_ANALYZE_PARTITION_CONCURRENCY: &str = "tidb_analyze_partition_concurrency";
/// TiDBMergePartitionStatsConcurrency indicates the concurrency when merge partition stats into global stats
/// Go `TiDBMergePartitionStatsConcurrency`.
pub const TIDB_MERGE_PARTITION_STATS_CONCURRENCY: &str = "tidb_merge_partition_stats_concurrency";
/// TiDBEnableAsyncMergeGlobalStats indicates whether to enable async merge global stats
/// Go `TiDBEnableAsyncMergeGlobalStats`.
pub const TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS: &str = "tidb_enable_async_merge_global_stats";
/// TiDBOptPrefixIndexSingleScan indicates whether to do some optimizations to avoid double scan for prefix index.
/// When set to true, `col is (not) null`(`col` is index prefix column) is regarded as index filter rather than table filter.
/// Go `TiDBOptPrefixIndexSingleScan`.
pub const TIDB_OPT_PREFIX_INDEX_SINGLE_SCAN: &str = "tidb_opt_prefix_index_single_scan";
/// TiDBOptPartialOrderedIndexForTopN indicates whether to enable partial ordered index optimization for TOPN queries.
/// Examples of queries that can benefit from this optimization:
/// 1. index a -> order by a, b limit
/// 2. index a, prefix(b) -> order by a, b limit
/// Go `TiDBOptPartialOrderedIndexForTopN`.
pub const TIDB_OPT_PARTIAL_ORDERED_INDEX_FOR_TOP_N: &str =
    "tidb_opt_partial_ordered_index_for_topn";
/// TiDBEnableExternalTSRead indicates whether to enable read through an external ts
/// Go `TiDBEnableExternalTSRead`.
pub const TIDB_ENABLE_EXTERNAL_TS_READ: &str = "tidb_enable_external_ts_read";
/// TiDBEnablePlanReplayerCapture indicates whether to enable plan replayer capture
/// Go `TiDBEnablePlanReplayerCapture`.
pub const TIDB_ENABLE_PLAN_REPLAYER_CAPTURE: &str = "tidb_enable_plan_replayer_capture";
/// TiDBEnablePlanReplayerContinuousCapture indicates whether to enable continuous capture
/// Go `TiDBEnablePlanReplayerContinuousCapture`.
pub const TIDB_ENABLE_PLAN_REPLAYER_CONTINUOUS_CAPTURE: &str =
    "tidb_enable_plan_replayer_continuous_capture";
/// TiDBEnableReusechunk indicates whether to enable chunk alloc
/// Go `TiDBEnableReusechunk`.
pub const TIDB_ENABLE_REUSECHUNK: &str = "tidb_enable_reuse_chunk";
/// TiDBStoreBatchSize indicates the batch size of coprocessor in the same store.
/// Go `TiDBStoreBatchSize`.
pub const TIDB_STORE_BATCH_SIZE: &str = "tidb_store_batch_size";
/// MppExchangeCompressionMode indicates the data compression method in mpp exchange operator
/// Go `MppExchangeCompressionMode`.
pub const MPP_EXCHANGE_COMPRESSION_MODE: &str = "mpp_exchange_compression_mode";
/// MppVersion indicates the mpp-version used to build mpp plan
/// Go `MppVersion`.
pub const MPP_VERSION: &str = "mpp_version";
/// TiDBPessimisticTransactionFairLocking controls whether fair locking for pessimistic transaction
/// is enabled.
/// Go `TiDBPessimisticTransactionFairLocking`.
pub const TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING: &str = "tidb_pessimistic_txn_fair_locking";
/// TiDBEnablePlanCacheForParamLimit controls whether prepare statement with parameterized limit can be cached
/// Go `TiDBEnablePlanCacheForParamLimit`.
pub const TIDB_ENABLE_PLAN_CACHE_FOR_PARAM_LIMIT: &str = "tidb_enable_plan_cache_for_param_limit";
/// TiDBEnableINLJoinInnerMultiPattern indicates whether enable multi pattern for inner side of inl join
/// Go `TiDBEnableINLJoinInnerMultiPattern`.
pub const TIDB_ENABLE_INL_JOIN_INNER_MULTI_PATTERN: &str =
    "tidb_enable_inl_join_inner_multi_pattern";
/// TiFlashComputeDispatchPolicy indicates how to dispatch task to tiflash_compute nodes.
/// Go `TiFlashComputeDispatchPolicy`.
pub const TIFLASH_COMPUTE_DISPATCH_POLICY: &str = "tiflash_compute_dispatch_policy";
/// TiDBEnablePlanCacheForSubquery controls whether prepare statement with subquery can be cached
/// Go `TiDBEnablePlanCacheForSubquery`.
pub const TIDB_ENABLE_PLAN_CACHE_FOR_SUBQUERY: &str = "tidb_enable_plan_cache_for_subquery";
/// TiDBOptEnableLateMaterialization indicates whether to enable late materialization
/// Go `TiDBOptEnableLateMaterialization`.
pub const TIDB_OPT_ENABLE_LATE_MATERIALIZATION: &str = "tidb_opt_enable_late_materialization";
/// TiDBLoadBasedReplicaReadThreshold is the wait duration threshold to enable replica read automatically.
/// Go `TiDBLoadBasedReplicaReadThreshold`.
pub const TIDB_LOAD_BASED_REPLICA_READ_THRESHOLD: &str = "tidb_load_based_replica_read_threshold";
/// TiDBOptOrderingIdxSelThresh is the threshold for optimizer to consider the ordering index.
/// Go `TiDBOptOrderingIdxSelThresh`.
pub const TIDB_OPT_ORDERING_IDX_SEL_THRESH: &str = "tidb_opt_ordering_index_selectivity_threshold";
/// TiDBOptOrderingIdxSelRatio is the ratio the optimizer will assume applies when non indexed filtering rows are found
/// via the ordering index.
/// Go `TiDBOptOrderingIdxSelRatio`.
pub const TIDB_OPT_ORDERING_IDX_SEL_RATIO: &str = "tidb_opt_ordering_index_selectivity_ratio";
/// TiDBOptEnableMPPSharedCTEExecution indicates whether the optimizer try to build shared CTE scan during MPP execution.
/// Go `TiDBOptEnableMPPSharedCTEExecution`.
pub const TIDB_OPT_ENABLE_MPP_SHARED_CTE_EXECUTION: &str =
    "tidb_opt_enable_mpp_shared_cte_execution";
/// TiDBOptFixControl makes the user able to control some details of the optimizer behavior.
/// Go `TiDBOptFixControl`.
pub const TIDB_OPT_FIX_CONTROL: &str = "tidb_opt_fix_control";
/// TiFlashReplicaRead is used to set the policy of TiFlash replica read when the query needs the TiFlash engine.
/// Go `TiFlashReplicaRead`.
pub const TIFLASH_REPLICA_READ: &str = "tiflash_replica_read";
/// TiDBLockUnchangedKeys indicates whether to lock duplicate keys in INSERT IGNORE and REPLACE statements,
/// or unchanged unique keys in UPDATE statements, see PR #42210 and #42713
/// Go `TiDBLockUnchangedKeys`.
pub const TIDB_LOCK_UNCHANGED_KEYS: &str = "tidb_lock_unchanged_keys";
/// TiDBFastCheckTable enables fast check table.
/// Go `TiDBFastCheckTable`.
pub const TIDB_FAST_CHECK_TABLE: &str = "tidb_enable_fast_table_check";
/// TiDBAnalyzeSkipColumnTypes indicates the column types whose statistics would not be collected when executing the ANALYZE command.
/// Go `TiDBAnalyzeSkipColumnTypes`.
pub const TIDB_ANALYZE_SKIP_COLUMN_TYPES: &str = "tidb_analyze_skip_column_types";
/// TiDBEnableCheckConstraint indicates whether to enable check constraint feature.
/// Go `TiDBEnableCheckConstraint`.
pub const TIDB_ENABLE_CHECK_CONSTRAINT: &str = "tidb_enable_check_constraint";
/// TiDBOptEnableHashJoin indicates whether to enable hash join.
/// Go `TiDBOptEnableHashJoin`.
pub const TIDB_OPT_ENABLE_HASH_JOIN: &str = "tidb_opt_enable_hash_join";
/// TiDBHashJoinVersion indicates whether to use hash join implementation v2.
/// Go `TiDBHashJoinVersion`.
pub const TIDB_HASH_JOIN_VERSION: &str = "tidb_hash_join_version";
/// TiDBOptIndexJoinBuild is kept for compatibility. Index join build v2 is always enabled now.
/// Go `TiDBOptIndexJoinBuild`.
pub const TIDB_OPT_INDEX_JOIN_BUILD: &str = "tidb_opt_index_join_build_v2";
/// TiDBOptObjective indicates whether the optimizer should be more stable, predictable or more aggressive.
/// Please see comments of SessionVars.OptObjective for details.
/// Go `TiDBOptObjective`.
pub const TIDB_OPT_OBJECTIVE: &str = "tidb_opt_objective";
/// TiDBEnableParallelHashaggSpill is the name of the `tidb_enable_parallel_hashagg_spill` system variable
/// Go `TiDBEnableParallelHashaggSpill`.
pub const TIDB_ENABLE_PARALLEL_HASHAGG_SPILL: &str = "tidb_enable_parallel_hashagg_spill";
/// TiDBTxnEntrySizeLimit indicates the max size of a entry in membuf.
/// Go `TiDBTxnEntrySizeLimit`.
pub const TIDB_TXN_ENTRY_SIZE_LIMIT: &str = "tidb_txn_entry_size_limit";
/// TiDBSchemaCacheSize indicates the size of infoschema meta data which are cached in V2 implementation.
/// Go `TiDBSchemaCacheSize`.
pub const TIDB_SCHEMA_CACHE_SIZE: &str = "tidb_schema_cache_size";
/// DivPrecisionIncrement indicates the number of digits by which to increase the scale of the result of
/// division operations performed with the / operator.
/// Go `DivPrecisionIncrement`.
pub const DIV_PRECISION_INCREMENT: &str = "div_precision_increment";
/// TiDBEnableSharedLockPromotion indicates whether the `select for share` statement would be executed
/// as `select for update` statements which do acquire pessimistic locks.
/// Go `TiDBEnableSharedLockPromotion`.
pub const TIDB_ENABLE_SHARED_LOCK_PROMOTION: &str = "tidb_enable_shared_lock_promotion";
/// TiDBAccelerateUserCreationUpdate decides whether tidb will load & update the whole user's data in-memory.
/// Go `TiDBAccelerateUserCreationUpdate`.
pub const TIDB_ACCELERATE_USER_CREATION_UPDATE: &str = "tidb_accelerate_user_creation_update";
/// TiDBEnableCachePrepareStmt indicates whether to support cache prepare stmt in plan cache.
/// Go `TiDBEnableCachePrepareStmt`.
pub const TIDB_ENABLE_CACHE_PREPARE_STMT: &str = "tidb_enable_cache_prepare_stmt";
/// TiDBGCEnable turns garbage collection on or OFF
/// Go `TiDBGCEnable`.
pub const TIDB_GC_ENABLE: &str = "tidb_gc_enable";
/// TiDBGCRunInterval sets the interval that GC runs
/// Go `TiDBGCRunInterval`.
pub const TIDB_GC_RUN_INTERVAL: &str = "tidb_gc_run_interval";
/// TiDBGCLifetime sets the retention window of older versions
/// Go `TiDBGCLifetime`.
pub const TIDB_GC_LIFETIME: &str = "tidb_gc_life_time";
/// TiDBGCConcurrency sets the concurrency of garbage collection. -1 = AUTO value
/// Go `TiDBGCConcurrency`.
pub const TIDB_GC_CONCURRENCY: &str = "tidb_gc_concurrency";
/// TiDBGCScanLockMode enables the green GC feature (deprecated)
/// Go `TiDBGCScanLockMode`.
pub const TIDB_GC_SCAN_LOCK_MODE: &str = "tidb_gc_scan_lock_mode";
/// TiDBGCMaxWaitTime sets max time for gc advances the safepoint delayed by active transactions
/// Go `TiDBGCMaxWaitTime`.
pub const TIDB_GC_MAX_WAIT_TIME: &str = "tidb_gc_max_wait_time";
/// TiDBEnableEnhancedSecurity restricts SUPER users from certain operations.
/// Go `TiDBEnableEnhancedSecurity`.
pub const TIDB_ENABLE_ENHANCED_SECURITY: &str = "tidb_enable_enhanced_security";
/// TiDBEnableHistoricalStats enables the historical statistics feature (default off)
/// Go `TiDBEnableHistoricalStats`.
pub const TIDB_ENABLE_HISTORICAL_STATS: &str = "tidb_enable_historical_stats";
/// TiDBPersistAnalyzeOptions persists analyze options for later analyze and auto-analyze
/// Go `TiDBPersistAnalyzeOptions`.
pub const TIDB_PERSIST_ANALYZE_OPTIONS: &str = "tidb_persist_analyze_options";
/// TiDBEnableColumnTracking enables collecting predicate columns.
/// DEPRECATED: This variable is deprecated, please do not use this variable.
/// Go `TiDBEnableColumnTracking`.
pub const TIDB_ENABLE_COLUMN_TRACKING: &str = "tidb_enable_column_tracking";
/// TiDBAnalyzeColumnOptions specifies the default column selection strategy for both manual and automatic analyze operations.
/// It accepts two values:
/// `PREDICATE`: Analyze only the columns that are used in the predicates of the query.
/// `ALL`: Analyze all columns in the table.
/// Go `TiDBAnalyzeColumnOptions`.
pub const TIDB_ANALYZE_COLUMN_OPTIONS: &str = "tidb_analyze_column_options";
/// TiDBAnalyzeDefaultNumBuckets sets the default number of histogram buckets for analyze operations.
/// Go `TiDBAnalyzeDefaultNumBuckets`.
pub const TIDB_ANALYZE_DEFAULT_NUM_BUCKETS: &str = "tidb_analyze_default_num_buckets";
/// TiDBAnalyzeDefaultNumTopN sets the default number of TopN entries for analyze operations.
/// Go `TiDBAnalyzeDefaultNumTopN`.
pub const TIDB_ANALYZE_DEFAULT_NUM_TOP_N: &str = "tidb_analyze_default_num_topn";
/// TiDBDisableColumnTrackingTime records the last time TiDBEnableColumnTracking is set off.
/// It is used to invalidate the collected predicate columns after turning off TiDBEnableColumnTracking, which avoids physical deletion.
/// It doesn't have cache in memory, and we directly get/set the variable value from/to mysql.tidb.
/// DEPRECATED: This variable is deprecated, please do not use this variable.
/// Go `TiDBDisableColumnTrackingTime`.
pub const TIDB_DISABLE_COLUMN_TRACKING_TIME: &str = "tidb_disable_column_tracking_time";
/// TiDBStatsLoadPseudoTimeout indicates whether to fallback to pseudo stats after load timeout.
/// Go `TiDBStatsLoadPseudoTimeout`.
pub const TIDB_STATS_LOAD_PSEUDO_TIMEOUT: &str = "tidb_stats_load_pseudo_timeout";
/// TiDBMemQuotaBindingCache indicates the memory quota for the bind cache.
/// Go `TiDBMemQuotaBindingCache`.
pub const TIDB_MEM_QUOTA_BINDING_CACHE: &str = "tidb_mem_quota_binding_cache";
/// TiDBRCReadCheckTS indicates the tso optimization for read-consistency read is enabled.
/// Go `TiDBRCReadCheckTS`.
pub const TIDB_RC_READ_CHECK_TS: &str = "tidb_rc_read_check_ts";
/// TiDBRCWriteCheckTs indicates whether some special write statements don't get latest tso from PD at RC
/// Go `TiDBRCWriteCheckTs`.
pub const TIDB_RC_WRITE_CHECK_TS: &str = "tidb_rc_write_check_ts";
/// TiDBCommitterConcurrency controls the number of running concurrent requests in the commit phase.
/// Go `TiDBCommitterConcurrency`.
pub const TIDB_COMMITTER_CONCURRENCY: &str = "tidb_committer_concurrency";
/// TiDBPipelinedDmlResourcePolicy controls the number of running concurrent requests in the
/// pipelined flush action.
/// Go `TiDBPipelinedDmlResourcePolicy`.
pub const TIDB_PIPELINED_DML_RESOURCE_POLICY: &str = "tidb_pipelined_dml_resource_policy";
/// TiDBEnableBatchDML enables batch dml.
/// Go `TiDBEnableBatchDML`.
pub const TIDB_ENABLE_BATCH_DML: &str = "tidb_enable_batch_dml";
/// TiDBStatsCacheMemQuota records stats cache quota.
/// Go `TiDBStatsCacheMemQuota`.
pub const TIDB_STATS_CACHE_MEM_QUOTA: &str = "tidb_stats_cache_mem_quota";
/// TiDBMemQuotaAnalyze indicates the memory quota for all analyze jobs.
/// Go `TiDBMemQuotaAnalyze`.
pub const TIDB_MEM_QUOTA_ANALYZE: &str = "tidb_mem_quota_analyze";
/// TiDBEnableAutoAnalyze determines whether TiDB executes automatic analysis.
/// In test, we disable it by default. See GlobalSystemVariableInitialValue for details.
/// Go `TiDBEnableAutoAnalyze`.
pub const TIDB_ENABLE_AUTO_ANALYZE: &str = "tidb_enable_auto_analyze";
/// TiDBEnableAutoAnalyzePriorityQueue determines whether TiDB executes automatic analysis with priority queue.
/// DEPRECATED: This variable is deprecated, please do not use this variable.
/// Go `TiDBEnableAutoAnalyzePriorityQueue`.
pub const TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE: &str = "tidb_enable_auto_analyze_priority_queue";
/// TiDBMemOOMAction indicates what operation TiDB perform when a single SQL statement exceeds
/// the memory quota specified by tidb_mem_quota_query and cannot be spilled to disk.
/// Go `TiDBMemOOMAction`.
pub const TIDB_MEM_OOM_ACTION: &str = "tidb_mem_oom_action";
/// TiDBPrepPlanCacheMemoryGuardRatio is used to prevent [performance.max-memory] from being exceeded
/// Go `TiDBPrepPlanCacheMemoryGuardRatio`.
pub const TIDB_PREP_PLAN_CACHE_MEMORY_GUARD_RATIO: &str =
    "tidb_prepared_plan_cache_memory_guard_ratio";
/// TiDBMaxAutoAnalyzeTime is the max time that auto analyze can run. If auto analyze runs longer than the value, it
/// will be killed. 0 indicates that there is no time limit.
/// Go `TiDBMaxAutoAnalyzeTime`.
pub const TIDB_MAX_AUTO_ANALYZE_TIME: &str = "tidb_max_auto_analyze_time";
/// TiDBAutoAnalyzeConcurrency is the concurrency of the auto analyze
/// Go `TiDBAutoAnalyzeConcurrency`.
pub const TIDB_AUTO_ANALYZE_CONCURRENCY: &str = "tidb_auto_analyze_concurrency";
/// TiDBEnableDistTask indicates whether to enable the distributed execute background tasks(For example DDL, Import etc).
/// Go `TiDBEnableDistTask`.
pub const TIDB_ENABLE_DIST_TASK: &str = "tidb_enable_dist_task";
/// TiDBMaxDistTaskNodes indicates the max node count that could be used by distributed execution framework.
/// Go `TiDBMaxDistTaskNodes`.
pub const TIDB_MAX_DIST_TASK_NODES: &str = "tidb_max_dist_task_nodes";
/// TiDBEnableFastCreateTable indicates whether to enable the fast create table feature.
/// Go `TiDBEnableFastCreateTable`.
pub const TIDB_ENABLE_FAST_CREATE_TABLE: &str = "tidb_enable_fast_create_table";
/// TiDBGenerateBinaryPlan indicates whether binary plan should be generated in slow log and statements summary.
/// Go `TiDBGenerateBinaryPlan`.
pub const TIDB_GENERATE_BINARY_PLAN: &str = "tidb_generate_binary_plan";
/// TiDBEnableDDLAnalyze indicates whether ddl(create/reorg index) is with embedded index analyze.
/// Go `TiDBEnableDDLAnalyze`.
pub const TIDB_ENABLE_DDL_ANALYZE: &str = "tidb_stats_update_during_ddl";
/// TiDBEnableGCAwareMemoryTrack indicates whether to turn-on GC-aware memory track.
/// Go `TiDBEnableGCAwareMemoryTrack`.
pub const TIDB_ENABLE_GC_AWARE_MEMORY_TRACK: &str = "tidb_enable_gc_aware_memory_track";
/// TiDBEnableTmpStorageOnOOM controls whether to enable the temporary storage for some operators
/// when a single SQL statement exceeds the memory quota specified by the memory quota.
/// Go `TiDBEnableTmpStorageOnOOM`.
pub const TIDB_ENABLE_TMP_STORAGE_ON_OOM: &str = "tidb_enable_tmp_storage_on_oom";
/// TiDBDDLEnableFastReorg indicates whether to use lighting backfill process for adding index.
/// Go `TiDBDDLEnableFastReorg`.
pub const TIDB_DDL_ENABLE_FAST_REORG: &str = "tidb_ddl_enable_fast_reorg";
/// TiDBDDLDiskQuota used to set disk quota for lightning add index.
/// Go `TiDBDDLDiskQuota`.
pub const TIDB_DDL_DISK_QUOTA: &str = "tidb_ddl_disk_quota";
/// TiDBCloudStorageURI used to set a cloud storage uri for ddl add index and import into.
/// Go `TiDBCloudStorageURI`.
pub const TIDB_CLOUD_STORAGE_URI: &str = "tidb_cloud_storage_uri";
/// TiDBAutoBuildStatsConcurrency is the number of concurrent workers to automatically analyze tables or partitions.
/// It is very similar to the `tidb_build_stats_concurrency` variable, but it is used for the auto analyze feature.
/// Go `TiDBAutoBuildStatsConcurrency`.
pub const TIDB_AUTO_BUILD_STATS_CONCURRENCY: &str = "tidb_auto_build_stats_concurrency";
/// TiDBSysProcScanConcurrency is used to set the scan concurrency of for backend system processes, like auto-analyze.
/// For now, it controls the number of concurrent workers to scan regions to collect statistics (FMSketch, Samples).
/// Go `TiDBSysProcScanConcurrency`.
pub const TIDB_SYS_PROC_SCAN_CONCURRENCY: &str = "tidb_sysproc_scan_concurrency";
/// TiDBServerMemoryLimit indicates the memory limit of the tidb-server instance.
/// Go `TiDBServerMemoryLimit`.
pub const TIDB_SERVER_MEMORY_LIMIT: &str = "tidb_server_memory_limit";
/// TiDBServerMemoryLimitSessMinSize indicates the minimal memory used of a session, that becomes a candidate for session kill.
/// Go `TiDBServerMemoryLimitSessMinSize`.
pub const TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE: &str = "tidb_server_memory_limit_sess_min_size";
/// TiDBServerMemoryLimitGCTrigger indicates the gc percentage of the TiDBServerMemoryLimit.
/// Go `TiDBServerMemoryLimitGCTrigger`.
pub const TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER: &str = "tidb_server_memory_limit_gc_trigger";
/// TiDBMemArbitratorSoftLimit indicates the soft memory quota limit of the global memory arbitrator
/// Go `TiDBMemArbitratorSoftLimit`.
pub const TIDB_MEM_ARBITRATOR_SOFT_LIMIT: &str = "tidb_mem_arbitrator_soft_limit";
/// TiDBMemArbitratorMode indicates work modes of the global memory arbitrator
/// Go `TiDBMemArbitratorMode`.
pub const TIDB_MEM_ARBITRATOR_MODE: &str = "tidb_mem_arbitrator_mode";
/// TiDBMemArbitratorQueryReserved indicates the memory quota query needs to subscribe from the global memory arbitrator before execution
/// Go `TiDBMemArbitratorQueryReserved`.
pub const TIDB_MEM_ARBITRATOR_QUERY_RESERVED: &str = "tidb_mem_arbitrator_query_reserved";
/// TiDBMemArbitratorWaitAverse indicates whether the query is wait averse
/// Go `TiDBMemArbitratorWaitAverse`.
pub const TIDB_MEM_ARBITRATOR_WAIT_AVERSE: &str = "tidb_mem_arbitrator_wait_averse";
/// TiDBEnableGOGCTuner is to enable GOGC tuner. it can tuner GOGC
/// Go `TiDBEnableGOGCTuner`.
pub const TIDB_ENABLE_GOGC_TUNER: &str = "tidb_enable_gogc_tuner";
/// TiDBGOGCTunerThreshold is to control the threshold of GOGC tuner.
/// Go `TiDBGOGCTunerThreshold`.
pub const TIDB_GOGC_TUNER_THRESHOLD: &str = "tidb_gogc_tuner_threshold";
/// TiDBGOGCTunerMaxValue is the max value of GOGC that GOGC tuner can change to.
/// Go `TiDBGOGCTunerMaxValue`.
pub const TIDB_GOGC_TUNER_MAX_VALUE: &str = "tidb_gogc_tuner_max_value";
/// TiDBGOGCTunerMinValue is the min value of GOGC that GOGC tuner can change to.
/// Go `TiDBGOGCTunerMinValue`.
pub const TIDB_GOGC_TUNER_MIN_VALUE: &str = "tidb_gogc_tuner_min_value";
/// TiDBExternalTS is the ts to read through when the `TiDBEnableExternalTsRead` is on
/// Go `TiDBExternalTS`.
pub const TIDB_EXTERNAL_TS: &str = "tidb_external_ts";
/// TiDBTTLJobEnable is used to enable/disable scheduling ttl job
/// Go `TiDBTTLJobEnable`.
pub const TIDB_TTL_JOB_ENABLE: &str = "tidb_ttl_job_enable";
/// TiDBTTLScanBatchSize is used to control the batch size in the SELECT statement for TTL jobs
/// Go `TiDBTTLScanBatchSize`.
pub const TIDB_TTL_SCAN_BATCH_SIZE: &str = "tidb_ttl_scan_batch_size";
/// TiDBTTLDeleteBatchSize is used to control the batch size in the DELETE statement for TTL jobs
/// Go `TiDBTTLDeleteBatchSize`.
pub const TIDB_TTL_DELETE_BATCH_SIZE: &str = "tidb_ttl_delete_batch_size";
/// TiDBTTLDeleteRateLimit is used to control the delete rate limit for TTL jobs in each node
/// Go `TiDBTTLDeleteRateLimit`.
pub const TIDB_TTL_DELETE_RATE_LIMIT: &str = "tidb_ttl_delete_rate_limit";
/// TiDBTTLJobScheduleWindowStartTime is used to restrict the start time of the time window of scheduling the ttl jobs.
/// Go `TiDBTTLJobScheduleWindowStartTime`.
pub const TIDB_TTL_JOB_SCHEDULE_WINDOW_START_TIME: &str = "tidb_ttl_job_schedule_window_start_time";
/// TiDBTTLJobScheduleWindowEndTime is used to restrict the end time of the time window of scheduling the ttl jobs.
/// Go `TiDBTTLJobScheduleWindowEndTime`.
pub const TIDB_TTL_JOB_SCHEDULE_WINDOW_END_TIME: &str = "tidb_ttl_job_schedule_window_end_time";
/// TiDBTTLScanWorkerCount indicates the count of the scan workers in each TiDB node
/// Go `TiDBTTLScanWorkerCount`.
pub const TIDB_TTL_SCAN_WORKER_COUNT: &str = "tidb_ttl_scan_worker_count";
/// TiDBTTLDeleteWorkerCount indicates the count of the delete workers in each TiDB node
/// Go `TiDBTTLDeleteWorkerCount`.
pub const TIDB_TTL_DELETE_WORKER_COUNT: &str = "tidb_ttl_delete_worker_count";
/// PasswordReuseHistory limit a few passwords to reuse.
/// Go `PasswordReuseHistory`.
pub const PASSWORD_REUSE_HISTORY: &str = "password_history";
/// PasswordReuseTime limit how long passwords can be reused.
/// Go `PasswordReuseTime`.
pub const PASSWORD_REUSE_TIME: &str = "password_reuse_interval";
/// TiDBHistoricalStatsDuration indicates the duration to remain tidb historical stats
/// Go `TiDBHistoricalStatsDuration`.
pub const TIDB_HISTORICAL_STATS_DURATION: &str = "tidb_historical_stats_duration";
/// TiDBEnableHistoricalStatsForCapture indicates whether use historical stats in plan replayer capture
/// Go `TiDBEnableHistoricalStatsForCapture`.
pub const TIDB_ENABLE_HISTORICAL_STATS_FOR_CAPTURE: &str =
    "tidb_enable_historical_stats_for_capture";
/// TiDBEnableResourceControl indicates whether resource control feature is enabled
/// Go `TiDBEnableResourceControl`.
pub const TIDB_ENABLE_RESOURCE_CONTROL: &str = "tidb_enable_resource_control";
/// TiDBResourceControlStrictMode indicates whether resource control strict mode is enabled.
/// When strict mode is enabled, user need certain privilege to change session or statement resource group.
/// Go `TiDBResourceControlStrictMode`.
pub const TIDB_RESOURCE_CONTROL_STRICT_MODE: &str = "tidb_resource_control_strict_mode";
/// TiDBStmtSummaryEnablePersistent indicates whether to enable file persistence for stmtsummary.
/// Go `TiDBStmtSummaryEnablePersistent`.
pub const TIDB_STMT_SUMMARY_ENABLE_PERSISTENT: &str = "tidb_stmt_summary_enable_persistent";
/// TiDBStmtSummaryFilename indicates the file name written by stmtsummary.
/// Go `TiDBStmtSummaryFilename`.
pub const TIDB_STMT_SUMMARY_FILENAME: &str = "tidb_stmt_summary_filename";
/// TiDBStmtSummaryFileMaxDays indicates how many days the files written by stmtsummary will be kept.
/// Go `TiDBStmtSummaryFileMaxDays`.
pub const TIDB_STMT_SUMMARY_FILE_MAX_DAYS: &str = "tidb_stmt_summary_file_max_days";
/// TiDBStmtSummaryFileMaxSize indicates the maximum size (in mb) of a single file written by stmtsummary.
/// Go `TiDBStmtSummaryFileMaxSize`.
pub const TIDB_STMT_SUMMARY_FILE_MAX_SIZE: &str = "tidb_stmt_summary_file_max_size";
/// TiDBStmtSummaryFileMaxBackups indicates the maximum number of files written by stmtsummary.
/// Go `TiDBStmtSummaryFileMaxBackups`.
pub const TIDB_STMT_SUMMARY_FILE_MAX_BACKUPS: &str = "tidb_stmt_summary_file_max_backups";
/// TiDBTTLRunningTasks limits the count of running ttl tasks. Default to 0, means 3 times the count of TiKV (or no
/// limitation, if the storage is not TiKV).
/// Go `TiDBTTLRunningTasks`.
pub const TIDB_TTL_RUNNING_TASKS: &str = "tidb_ttl_running_tasks";
/// AuthenticationLDAPSASLAuthMethodName defines the authentication method used by LDAP SASL authentication plugin
/// Go `AuthenticationLDAPSASLAuthMethodName`.
pub const AUTHENTICATION_LDAPSASL_AUTH_METHOD_NAME: &str =
    "authentication_ldap_sasl_auth_method_name";
/// AuthenticationLDAPSASLCAPath defines the ca certificate to verify LDAP connection in LDAP SASL authentication plugin
/// Go `AuthenticationLDAPSASLCAPath`.
pub const AUTHENTICATION_LDAPSASLCA_PATH: &str = "authentication_ldap_sasl_ca_path";
/// AuthenticationLDAPSASLTLS defines whether to use TLS connection in LDAP SASL authentication plugin
/// Go `AuthenticationLDAPSASLTLS`.
pub const AUTHENTICATION_LDAPSASLTLS: &str = "authentication_ldap_sasl_tls";
/// AuthenticationLDAPSASLServerHost defines the server host of LDAP server for LDAP SASL authentication plugin
/// Go `AuthenticationLDAPSASLServerHost`.
pub const AUTHENTICATION_LDAPSASL_SERVER_HOST: &str = "authentication_ldap_sasl_server_host";
/// AuthenticationLDAPSASLServerPort defines the port of LDAP server for LDAP SASL authentication plugin
/// Go `AuthenticationLDAPSASLServerPort`.
pub const AUTHENTICATION_LDAPSASL_SERVER_PORT: &str = "authentication_ldap_sasl_server_port";
/// AuthenticationLDAPSASLReferral defines whether to enable LDAP referral for LDAP SASL authentication plugin
/// Go `AuthenticationLDAPSASLReferral`.
pub const AUTHENTICATION_LDAPSASL_REFERRAL: &str = "authentication_ldap_sasl_referral";
/// AuthenticationLDAPSASLUserSearchAttr defines the attribute of username in LDAP server
/// Go `AuthenticationLDAPSASLUserSearchAttr`.
pub const AUTHENTICATION_LDAPSASL_USER_SEARCH_ATTR: &str =
    "authentication_ldap_sasl_user_search_attr";
/// AuthenticationLDAPSASLBindBaseDN defines the `dn` to search the users in. It's used to limit the search scope of TiDB.
/// Go `AuthenticationLDAPSASLBindBaseDN`.
pub const AUTHENTICATION_LDAPSASL_BIND_BASE_DN: &str = "authentication_ldap_sasl_bind_base_dn";
/// AuthenticationLDAPSASLBindRootDN defines the `dn` of the user to login the LDAP server and perform search.
/// Go `AuthenticationLDAPSASLBindRootDN`.
pub const AUTHENTICATION_LDAPSASL_BIND_ROOT_DN: &str = "authentication_ldap_sasl_bind_root_dn";
/// AuthenticationLDAPSASLBindRootPWD defines the password of the user to login the LDAP server and perform search.
/// Go `AuthenticationLDAPSASLBindRootPWD`.
pub const AUTHENTICATION_LDAPSASL_BIND_ROOT_PWD: &str = "authentication_ldap_sasl_bind_root_pwd";
/// AuthenticationLDAPSASLInitPoolSize defines the init size of connection pool to LDAP server for SASL plugin.
/// Go `AuthenticationLDAPSASLInitPoolSize`.
pub const AUTHENTICATION_LDAPSASL_INIT_POOL_SIZE: &str = "authentication_ldap_sasl_init_pool_size";
/// AuthenticationLDAPSASLMaxPoolSize defines the max size of connection pool to LDAP server for SASL plugin.
/// Go `AuthenticationLDAPSASLMaxPoolSize`.
pub const AUTHENTICATION_LDAPSASL_MAX_POOL_SIZE: &str = "authentication_ldap_sasl_max_pool_size";
/// AuthenticationLDAPSimpleAuthMethodName defines the authentication method used by LDAP Simple authentication plugin
/// Go `AuthenticationLDAPSimpleAuthMethodName`.
pub const AUTHENTICATION_LDAP_SIMPLE_AUTH_METHOD_NAME: &str =
    "authentication_ldap_simple_auth_method_name";
/// AuthenticationLDAPSimpleCAPath defines the ca certificate to verify LDAP connection in LDAP Simple authentication plugin
/// Go `AuthenticationLDAPSimpleCAPath`.
pub const AUTHENTICATION_LDAP_SIMPLE_CA_PATH: &str = "authentication_ldap_simple_ca_path";
/// AuthenticationLDAPSimpleTLS defines whether to use TLS connection in LDAP Simple authentication plugin
/// Go `AuthenticationLDAPSimpleTLS`.
pub const AUTHENTICATION_LDAP_SIMPLE_TLS: &str = "authentication_ldap_simple_tls";
/// AuthenticationLDAPSimpleServerHost defines the server host of LDAP server for LDAP Simple authentication plugin
/// Go `AuthenticationLDAPSimpleServerHost`.
pub const AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST: &str = "authentication_ldap_simple_server_host";
/// AuthenticationLDAPSimpleServerPort defines the port of LDAP server for LDAP Simple authentication plugin
/// Go `AuthenticationLDAPSimpleServerPort`.
pub const AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT: &str = "authentication_ldap_simple_server_port";
/// AuthenticationLDAPSimpleReferral defines whether to enable LDAP referral for LDAP Simple authentication plugin
/// Go `AuthenticationLDAPSimpleReferral`.
pub const AUTHENTICATION_LDAP_SIMPLE_REFERRAL: &str = "authentication_ldap_simple_referral";
/// AuthenticationLDAPSimpleUserSearchAttr defines the attribute of username in LDAP server
/// Go `AuthenticationLDAPSimpleUserSearchAttr`.
pub const AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR: &str =
    "authentication_ldap_simple_user_search_attr";
/// AuthenticationLDAPSimpleBindBaseDN defines the `dn` to search the users in. It's used to limit the search scope of TiDB.
/// Go `AuthenticationLDAPSimpleBindBaseDN`.
pub const AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN: &str = "authentication_ldap_simple_bind_base_dn";
/// AuthenticationLDAPSimpleBindRootDN defines the `dn` of the user to login the LDAP server and perform search.
/// Go `AuthenticationLDAPSimpleBindRootDN`.
pub const AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN: &str = "authentication_ldap_simple_bind_root_dn";
/// AuthenticationLDAPSimpleBindRootPWD defines the password of the user to login the LDAP server and perform search.
/// Go `AuthenticationLDAPSimpleBindRootPWD`.
pub const AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD: &str =
    "authentication_ldap_simple_bind_root_pwd";
/// AuthenticationLDAPSimpleInitPoolSize defines the init size of connection pool to LDAP server for SASL plugin.
/// Go `AuthenticationLDAPSimpleInitPoolSize`.
pub const AUTHENTICATION_LDAP_SIMPLE_INIT_POOL_SIZE: &str =
    "authentication_ldap_simple_init_pool_size";
/// AuthenticationLDAPSimpleMaxPoolSize defines the max size of connection pool to LDAP server for SASL plugin.
/// Go `AuthenticationLDAPSimpleMaxPoolSize`.
pub const AUTHENTICATION_LDAP_SIMPLE_MAX_POOL_SIZE: &str =
    "authentication_ldap_simple_max_pool_size";
/// TiDBRuntimeFilterTypeName the value of is string, a runtime filter type list split by ",", such as: "IN,MIN_MAX"
/// Go `TiDBRuntimeFilterTypeName`.
pub const TIDB_RUNTIME_FILTER_TYPE_NAME: &str = "tidb_runtime_filter_type";
/// TiDBRuntimeFilterModeName the mode of runtime filter, such as "OFF", "LOCAL"
/// Go `TiDBRuntimeFilterModeName`.
pub const TIDB_RUNTIME_FILTER_MODE_NAME: &str = "tidb_runtime_filter_mode";
/// TiDBSkipMissingPartitionStats controls how to handle missing partition stats when merging partition stats to global stats.
/// When set to true, skip missing partition stats and continue to merge other partition stats to global stats.
/// When set to false, give up merging partition stats to global stats.
/// Go `TiDBSkipMissingPartitionStats`.
pub const TIDB_SKIP_MISSING_PARTITION_STATS: &str = "tidb_skip_missing_partition_stats";
/// TiDBSessionAlias indicates the alias of a session which is used for tracing.
/// Go `TiDBSessionAlias`.
pub const TIDB_SESSION_ALIAS: &str = "tidb_session_alias";
/// TiDBServiceScope indicates the role for tidb for distributed task framework.
/// Go `TiDBServiceScope`.
pub const TIDB_SERVICE_SCOPE: &str = "tidb_service_scope";
/// TiDBSchemaVersionCacheLimit defines the capacity size of domain infoSchema cache.
/// Go `TiDBSchemaVersionCacheLimit`.
pub const TIDB_SCHEMA_VERSION_CACHE_LIMIT: &str = "tidb_schema_version_cache_limit";
/// TiDBEnableTiFlashPipelineMode means if we should use pipeline model to execute query or not in tiflash.
/// It's deprecated and setting it will not have any effect.
/// Go `TiDBEnableTiFlashPipelineMode`.
pub const TIDB_ENABLE_TIFLASH_PIPELINE_MODE: &str = "tidb_enable_tiflash_pipeline_model";
/// TiDBIdleTransactionTimeout indicates the maximum time duration a transaction could be idle, unit is second.
/// Any idle transaction will be killed after being idle for `tidb_idle_transaction_timeout` seconds.
/// This is similar to https://docs.percona.com/percona-server/5.7/management/innodb_kill_idle_trx.html and https://mariadb.com/kb/en/transaction-timeouts/
/// Go `TiDBIdleTransactionTimeout`.
pub const TIDB_IDLE_TRANSACTION_TIMEOUT: &str = "tidb_idle_transaction_timeout";
/// TiDBLowResolutionTSOUpdateInterval defines how often to refresh low resolution timestamps.
/// Go `TiDBLowResolutionTSOUpdateInterval`.
pub const TIDB_LOW_RESOLUTION_TSO_UPDATE_INTERVAL: &str = "tidb_low_resolution_tso_update_interval";
/// TiDBDMLType indicates the execution type of DML in TiDB.
/// The value can be STANDARD, BULK.
/// Currently, the BULK mode only affects auto-committed DML.
/// Go `TiDBDMLType`.
pub const TIDB_DML_TYPE: &str = "tidb_dml_type";
/// TiFlashHashAggPreAggMode indicates the policy of 1st hashagg.
/// Go `TiFlashHashAggPreAggMode`.
pub const TIFLASH_HASH_AGG_PRE_AGG_MODE: &str = "tiflash_hashagg_preaggregation_mode";
/// TiDBEnableLazyCursorFetch defines whether to enable the lazy cursor fetch. If it's `OFF`, all results of
/// of a cursor will be stored in the tidb node in `EXECUTE` command.
/// Go `TiDBEnableLazyCursorFetch`.
pub const TIDB_ENABLE_LAZY_CURSOR_FETCH: &str = "tidb_enable_lazy_cursor_fetch";
/// TiDBTSOClientRPCMode controls how the TSO client performs the TSO RPC requests. It internally controls the
/// concurrency of the RPC. This variable provides an approach to tune the latency of getting timestamps from PD.
/// Go `TiDBTSOClientRPCMode`.
pub const TIDB_TSO_CLIENT_RPC_MODE: &str = "tidb_tso_client_rpc_mode";
/// TiDBCircuitBreakerPDMetadataErrorRateThresholdRatio variable is used to set ratio of errors to trip the circuit breaker for get region calls to PD
/// https://github.com/tikv/rfcs/blob/master/text/0115-circuit-breaker.md
/// Go `TiDBCircuitBreakerPDMetadataErrorRateThresholdRatio`.
pub const TIDB_CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO: &str =
    "tidb_cb_pd_metadata_error_rate_threshold_ratio";
/// TiDBEnableTSValidation controls whether to enable the timestamp validation in client-go.
/// Go `TiDBEnableTSValidation`.
pub const TIDB_ENABLE_TS_VALIDATION: &str = "tidb_enable_ts_validation";
/// TiDBAdvancerCheckPointLagLimit controls the maximum lag could be tolerated for the checkpoint lag.
/// The log backup task will be paused if the checkpoint lag is larger than it.
/// Go `TiDBAdvancerCheckPointLagLimit`.
pub const TIDB_ADVANCER_CHECK_POINT_LAG_LIMIT: &str = "tidb_advancer_check_point_lag_limit";
/// TiDBIndexLookUpPushDownPolicy controls the push down policy of index lookup.
/// Go `TiDBIndexLookUpPushDownPolicy`.
pub const TIDB_INDEX_LOOK_UP_PUSH_DOWN_POLICY: &str = "tidb_index_lookup_pushdown_policy";
/// Go `DefHostname`.
pub const DEF_HOSTNAME: &str = "localhost";
/// Go `DefAutoAnalyzeStartTime`.
pub const DEF_AUTO_ANALYZE_START_TIME: &str = "00:00 +0000";
/// Go `DefAutoAnalyzeEndTime`.
pub const DEF_AUTO_ANALYZE_END_TIME: &str = "23:59 +0000";
/// Go `DefTiDBTraceEvent`.
pub const DEF_TIDB_TRACE_EVENT: &str = "";
/// Go `DefBlockEncryptionMode`.
pub const DEF_BLOCK_ENCRYPTION_MODE: &str = "aes-128-ecb";
/// Go `DefTiDBMPPStoreFailTTL`.
pub const DEF_TIDB_MPP_STORE_FAIL_TTL: &str = "0s";
/// Go `DefTiDBEvolvePlanTaskStartTime`.
pub const DEF_TIDB_EVOLVE_PLAN_TASK_START_TIME: &str = "00:00 +0000";
/// Go `DefTiDBEvolvePlanTaskEndTime`.
pub const DEF_TIDB_EVOLVE_PLAN_TASK_END_TIME: &str = "23:59 +0000";
/// Go `DefTiDBPartitionPruneMode`.
pub const DEF_TIDB_PARTITION_PRUNE_MODE: &str = "dynamic";
/// Go `DefTimestamp`.
pub const DEF_TIMESTAMP: &str = "0";
/// Go `DefTiDBAnalyzeColumnOptions`.
pub const DEF_TIDB_ANALYZE_COLUMN_OPTIONS: &str = "ALL";
/// Go `DefTiDBMemOOMAction`.
pub const DEF_TIDB_MEM_OOM_ACTION: &str = "CANCEL";
/// Go `DefTiDBOptPartialOrderedIndexForTopN`.
pub const DEF_TIDB_OPT_PARTIAL_ORDERED_INDEX_FOR_TOP_N: &str = "DISABLE";
/// Go `DefTiDBTTLJobScheduleWindowStartTime`.
pub const DEF_TIDB_TTL_JOB_SCHEDULE_WINDOW_START_TIME: &str = "00:00 +0000";
/// Go `DefTiDBTTLJobScheduleWindowEndTime`.
pub const DEF_TIDB_TTL_JOB_SCHEDULE_WINDOW_END_TIME: &str = "23:59 +0000";
/// Go `DefAuthenticationLDAPSASLAuthMethodName`.
pub const DEF_AUTHENTICATION_LDAPSASL_AUTH_METHOD_NAME: &str = "SCRAM-SHA-1";
/// Go `DefAuthenticationLDAPSASLUserSearchAttr`.
pub const DEF_AUTHENTICATION_LDAPSASL_USER_SEARCH_ATTR: &str = "uid";
/// Go `DefAuthenticationLDAPSimpleAuthMethodName`.
pub const DEF_AUTHENTICATION_LDAP_SIMPLE_AUTH_METHOD_NAME: &str = "SIMPLE";
/// Go `DefAuthenticationLDAPSimpleUserSearchAttr`.
pub const DEF_AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR: &str = "uid";
/// Go `DefRuntimeFilterType`.
pub const DEF_RUNTIME_FILTER_TYPE: &str = "IN";
/// Go `DefRuntimeFilterMode`.
pub const DEF_RUNTIME_FILTER_MODE: &str = "OFF";
/// Go `DefTiDBDMLType`.
pub const DEF_TIDB_DML_TYPE: &str = "STANDARD";
/// Go `DefDefaultWeekFormat`.
pub const DEF_DEFAULT_WEEK_FORMAT: &str = "0";
/// Go `DefTiDBMemArbitratorQueryReservedText`.
pub const DEF_TIDB_MEM_ARBITRATOR_QUERY_RESERVED_TEXT: &str = "0";
/// Go `DefTiDBMemArbitratorWaitAverse`.
pub const DEF_TIDB_MEM_ARBITRATOR_WAIT_AVERSE: &str = "0";
/// OptObjectiveDeterminate is a possible value for TiDBOptObjective.
/// Go `OptObjectiveDeterminate`.
pub const OPT_OBJECTIVE_DETERMINATE: &str = "determinate";
/// Go `ForcePreAggStr`.
pub const FORCE_PRE_AGG_STR: &str = "force_preagg";
/// Go `AutoStr`.
pub const AUTO_STR: &str = "auto";
/// Go `ForceStreamingStr`.
pub const FORCE_STREAMING_STR: &str = "force_streaming";
/// AllReplicaStr is the string value of AllReplicas.
/// Go `AllReplicaStr`.
pub const ALL_REPLICA_STR: &str = "all_replicas";
/// ClosestAdaptiveStr is the string value of ClosestAdaptive.
/// Go `ClosestAdaptiveStr`.
pub const CLOSEST_ADAPTIVE_STR: &str = "closest_adaptive";
/// ClosestReplicasStr is the string value of ClosestReplicas.
/// Go `ClosestReplicasStr`.
pub const CLOSEST_REPLICAS_STR: &str = "closest_replicas";
/// DispatchPolicyRRStr is string value for DispatchPolicyRR.
/// Go `DispatchPolicyRRStr`.
pub const DISPATCH_POLICY_RR_STR: &str = "round_robin";
/// DispatchPolicyConsistentHashStr is string value for DispatchPolicyConsistentHash.
/// Go `DispatchPolicyConsistentHashStr`.
pub const DISPATCH_POLICY_CONSISTENT_HASH_STR: &str = "consistent_hash";
/// DispatchPolicyInvalidStr is string value for DispatchPolicyInvalid.
/// Go `DispatchPolicyInvalidStr`.
pub const DISPATCH_POLICY_INVALID_STR: &str = "invalid";
/// On is the canonical string for ON
/// Go `On`.
pub const ON: &str = "ON";
/// Off is the canonical string for OFF
/// Go `Off`.
pub const OFF: &str = "OFF";
/// Warn means return warnings
/// Go `Warn`.
pub const WARN: &str = "WARN";
/// IntOnly means enable for int type
/// Go `IntOnly`.
pub const INT_ONLY: &str = "INT_ONLY";
/// Marker is a special log redact behavior
/// Go `Marker`.
pub const MARKER: &str = "MARKER";
/// AssertionStrictStr is a choice of variable TiDBTxnAssertionLevel that means full assertions should be performed,
/// even if the performance might be slowed down.
/// Go `AssertionStrictStr`.
pub const ASSERTION_STRICT_STR: &str = "STRICT";
/// AssertionFastStr is a choice of variable TiDBTxnAssertionLevel that means assertions that doesn't affect
/// performance should be performed.
/// Go `AssertionFastStr`.
pub const ASSERTION_FAST_STR: &str = "FAST";
/// AssertionOffStr is a choice of variable TiDBTxnAssertionLevel that means no assertion should be performed.
/// Go `AssertionOffStr`.
pub const ASSERTION_OFF_STR: &str = "OFF";
/// OOMActionCancel constants represents the valid action configurations for OOMAction "CANCEL".
/// Go `OOMActionCancel`.
pub const OOM_ACTION_CANCEL: &str = "CANCEL";
/// OOMActionLog constants represents the valid action configurations for OOMAction "LOG".
/// Go `OOMActionLog`.
pub const OOM_ACTION_LOG: &str = "LOG";
/// TSOClientRPCModeDefault is a choice of variable TiDBTSOClientRPCMode. In this mode, the TSO client sends batched
/// TSO requests serially.
/// Go `TSOClientRPCModeDefault`.
pub const TSO_CLIENT_RPC_MODE_DEFAULT: &str = "DEFAULT";
/// TSOClientRPCModeParallel is a choice of variable TiDBTSOClientRPCMode. In this mode, the TSO client tries to
/// keep approximately 2 batched TSO requests running in parallel. This option tries to reduce the batch-waiting time
/// by half, at the expense of about twice the amount of TSO RPC calls.
/// Go `TSOClientRPCModeParallel`.
pub const TSO_CLIENT_RPC_MODE_PARALLEL: &str = "PARALLEL";
/// TSOClientRPCModeParallelFast is a choice of variable TiDBTSOClientRPCMode. In this mode, the TSO client tries to
/// keep approximately 4 batched TSO requests running in parallel. This option tries to reduce the batch-waiting time
/// by 3/4, at the expense of about 4 times the amount of TSO RPC calls.
/// Go `TSOClientRPCModeParallelFast`.
pub const TSO_CLIENT_RPC_MODE_PARALLEL_FAST: &str = "PARALLEL-FAST";
/// StrategyStandard is a choice of variable TiDBPipelinedDmlResourcePolicy,
/// the best performance policy
/// Go `StrategyStandard`.
pub const STRATEGY_STANDARD: &str = "standard";
/// StrategyConservative is a choice of variable TiDBPipelinedDmlResourcePolicy,
/// a rather conservative policy
/// Go `StrategyConservative`.
pub const STRATEGY_CONSERVATIVE: &str = "conservative";
/// StrategyCustom is a choice of variable TiDBPipelinedDmlResourcePolicy,
/// Go `StrategyCustom`.
pub const STRATEGY_CUSTOM: &str = "custom";
/// IndexLookUpPushDownPolicyHintOnly indicates only use the hint to decide whether to push down the index lookup or not.
/// Go `IndexLookUpPushDownPolicyHintOnly`.
pub const INDEX_LOOK_UP_PUSH_DOWN_POLICY_HINT_ONLY: &str = "hint-only";
/// IndexLookUpPushDownPolicyAffinityForce indicates to force push down the index lookup for table with affinity options.
/// Go `IndexLookUpPushDownPolicyAffinityForce`.
pub const INDEX_LOOK_UP_PUSH_DOWN_POLICY_AFFINITY_FORCE: &str = "affinity-force";
/// IndexLookUpPushDownPolicyForce indicates to force push down the index lookup for all tables.
/// Go `IndexLookUpPushDownPolicyForce`.
pub const INDEX_LOOK_UP_PUSH_DOWN_POLICY_FORCE: &str = "force";
/// Go `GlobalConfigEnableTopSQL`.
pub const GLOBAL_CONFIG_ENABLE_TOP_SQL: &str = "enable_resource_metering";
/// Go `GlobalConfigSourceID`.
pub const GLOBAL_CONFIG_SOURCE_ID: &str = "source_id";
/// LocalDayTimeFormat is the local format of analyze start time and end time.
/// Go `LocalDayTimeFormat`.
pub const LOCAL_DAY_TIME_FORMAT: &str = "15:04";
/// FullDayTimeFormat is the full format of analyze start time and end time.
/// Go `FullDayTimeFormat`.
pub const FULL_DAY_TIME_FORMAT: &str = "15:04 -0700";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_check_names() {
        assert_eq!(TIDB_SNAPSHOT, "tidb_snapshot");
        assert_eq!(TIDB_OPT_AGG_PUSH_DOWN, "tidb_opt_agg_push_down");
        assert_eq!(TIDB_DDL_SLOW_OPR_THRESHOLD, "ddl_slow_threshold");
        assert_eq!(TIDB_GC_ENABLE, "tidb_gc_enable");
        assert_eq!(TIDB_OPT_CARTESIAN_BCJ, "tidb_opt_broadcast_cartesian_join");
    }
}
