// Copyright 2017 PingCAP, Inc.
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

package variable

import (
	"math"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/atomic"
)

/*
	Steps to add a new TiDB specific system variable:

	1. Add a new variable name with comment in this file.
	2. Add the default value of the new variable in this file.
	3. Add SysVar instance in 'defaultSysVars' slice.
*/

// TiDB system variable names that only in session scope.
const (
	TiDBDDLSlowOprThreshold = "ddl_slow_threshold"

	// TiDBSnapshot is used for reading history data, the default value is empty string.
	// The value can be a datetime string like '2017-11-11 20:20:20' or a tso string. When this variable is set, the session reads history data of that time.
	TiDBSnapshot = "tidb_snapshot"

	// TiDBOptAggPushDown is used to enable/disable the optimizer rule of aggregation push down.
	TiDBOptAggPushDown = "tidb_opt_agg_push_down"

	// TiDBOptCartesianBCJ is used to disable/enable broadcast cartesian join in MPP mode
	TiDBOptCartesianBCJ = "tidb_opt_broadcast_cartesian_join"

	TiDBOptMPPOuterJoinFixedBuildSide = "tidb_opt_mpp_outer_join_fixed_build_side"

	// TiDBOptDistinctAggPushDown is used to decide whether agg with distinct should be pushed to tikv/tiflash.
	TiDBOptDistinctAggPushDown = "tidb_opt_distinct_agg_push_down"

	// TiDBBCJThresholdSize is used to limit the size of small table for mpp broadcast join.
	// Its unit is bytes, if the size of small table is larger than it, we will not use bcj.
	TiDBBCJThresholdSize = "tidb_broadcast_join_threshold_size"

	// TiDBBCJThresholdCount is used to limit the count of small table for mpp broadcast join.
	// If we can't estimate the size of one side of join child, we will check if its row number exceeds this limitation.
	TiDBBCJThresholdCount = "tidb_broadcast_join_threshold_count"

	// TiDBOptWriteRowID is used to enable/disable the operations of insert、replace and update to _tidb_rowid.
	TiDBOptWriteRowID = "tidb_opt_write_row_id"

	// TiDBAutoAnalyzeRatio will run if (table modify count)/(table row count) is greater than this value.
	TiDBAutoAnalyzeRatio = "tidb_auto_analyze_ratio"

	// TiDBAutoAnalyzeStartTime will run if current time is within start time and end time.
	TiDBAutoAnalyzeStartTime = "tidb_auto_analyze_start_time"
	TiDBAutoAnalyzeEndTime   = "tidb_auto_analyze_end_time"

	// TiDBChecksumTableConcurrency is used to speed up the ADMIN CHECKSUM TABLE
	// statement, when a table has multiple indices, those indices can be
	// scanned concurrently, with the cost of higher system performance impact.
	TiDBChecksumTableConcurrency = "tidb_checksum_table_concurrency"

	// TiDBCurrentTS is used to get the current transaction timestamp.
	// It is read-only.
	TiDBCurrentTS = "tidb_current_ts"

	// TiDBLastTxnInfo is used to get the last transaction info within the current session.
	TiDBLastTxnInfo = "tidb_last_txn_info"

	// TiDBLastQueryInfo is used to get the last query info within the current session.
	TiDBLastQueryInfo = "tidb_last_query_info"

	// TiDBLastDDLInfo is used to get the last ddl info within the current session.
	TiDBLastDDLInfo = "tidb_last_ddl_info"

	// TiDBConfig is a read-only variable that shows the config of the current server.
	TiDBConfig = "tidb_config"

	// TiDBBatchInsert is used to enable/disable auto-split insert data. If set this option on, insert executor will automatically
	// insert data into multiple batches and use a single txn for each batch. This will be helpful when inserting large data.
	TiDBBatchInsert = "tidb_batch_insert"

	// TiDBBatchDelete is used to enable/disable auto-split delete data. If set this option on, delete executor will automatically
	// split data into multiple batches and use a single txn for each batch. This will be helpful when deleting large data.
	TiDBBatchDelete = "tidb_batch_delete"

	// TiDBBatchCommit is used to enable/disable auto-split the transaction.
	// If set this option on, the transaction will be committed when it reaches stmt-count-limit and starts a new transaction.
	TiDBBatchCommit = "tidb_batch_commit"

	// TiDBDMLBatchSize is used to split the insert/delete data into small batches.
	// It only takes effort when tidb_batch_insert/tidb_batch_delete is on.
	// Its default value is 20000. When the row size is large, 20k rows could be larger than 100MB.
	// User could change it to a smaller one to avoid breaking the transaction size limitation.
	TiDBDMLBatchSize = "tidb_dml_batch_size"

	// The following session variables controls the memory quota during query execution.

	// TiDBMemQuotaQuery controls the memory quota of a query.
	TiDBMemQuotaQuery = "tidb_mem_quota_query" // Bytes.
	// TiDBMemQuotaApplyCache controls the memory quota of a query.
	TiDBMemQuotaApplyCache = "tidb_mem_quota_apply_cache"

	// TiDBGeneralLog is used to log every query in the server in info level.
	TiDBGeneralLog = "tidb_general_log"

	// TiDBLogFileMaxDays is used to log every query in the server in info level.
	TiDBLogFileMaxDays = "tidb_log_file_max_days"

	// TiDBPProfSQLCPU is used to add label sql label to pprof result.
	TiDBPProfSQLCPU = "tidb_pprof_sql_cpu"

	// TiDBRetryLimit is the maximum number of retries when committing a transaction.
	TiDBRetryLimit = "tidb_retry_limit"

	// TiDBDisableTxnAutoRetry disables transaction auto retry.
	TiDBDisableTxnAutoRetry = "tidb_disable_txn_auto_retry"

	// TiDBEnableChunkRPC enables TiDB to use Chunk format for coprocessor requests.
	TiDBEnableChunkRPC = "tidb_enable_chunk_rpc"

	// TiDBOptimizerSelectivityLevel is used to control the selectivity estimation level.
	TiDBOptimizerSelectivityLevel = "tidb_optimizer_selectivity_level"

	// TiDBOptimizerEnableNewOnlyFullGroupByCheck is used to open the newly only_full_group_by check by maintaining functional dependency.
	TiDBOptimizerEnableNewOnlyFullGroupByCheck = "tidb_enable_new_only_full_group_by_check"

	TiDBOptimizerEnableOuterJoinReorder = "tidb_enable_outer_join_reorder"

	// TiDBTxnMode is used to control the transaction behavior.
	TiDBTxnMode = "tidb_txn_mode"

	// TiDBRowFormatVersion is used to control tidb row format version current.
	TiDBRowFormatVersion = "tidb_row_format_version"

	// TiDBEnableTablePartition is used to control table partition feature.
	// The valid value include auto/on/off:
	// on or auto: enable table partition if the partition type is implemented.
	// off: always disable table partition.
	TiDBEnableTablePartition = "tidb_enable_table_partition"

	// TiDBEnableListTablePartition is used to control list table partition feature.
	TiDBEnableListTablePartition = "tidb_enable_list_partition"

	// TiDBSkipIsolationLevelCheck is used to control whether to return error when set unsupported transaction
	// isolation level.
	TiDBSkipIsolationLevelCheck = "tidb_skip_isolation_level_check"

	// TiDBLowResolutionTSO is used for reading data with low resolution TSO which is updated once every two seconds
	TiDBLowResolutionTSO = "tidb_low_resolution_tso"

	// TiDBReplicaRead is used for reading data from replicas, followers for example.
	TiDBReplicaRead = "tidb_replica_read"

	// TiDBAllowRemoveAutoInc indicates whether a user can drop the auto_increment column attribute or not.
	TiDBAllowRemoveAutoInc = "tidb_allow_remove_auto_inc"

	// TiDBMultiStatementMode enables multi statement at the risk of SQL injection
	// provides backwards compatibility
	TiDBMultiStatementMode = "tidb_multi_statement_mode"

	// TiDBEvolvePlanTaskMaxTime controls the max time of a single evolution task.
	TiDBEvolvePlanTaskMaxTime = "tidb_evolve_plan_task_max_time"

	// TiDBEvolvePlanTaskStartTime is the start time of evolution task.
	TiDBEvolvePlanTaskStartTime = "tidb_evolve_plan_task_start_time"
	// TiDBEvolvePlanTaskEndTime is the end time of evolution task.
	TiDBEvolvePlanTaskEndTime = "tidb_evolve_plan_task_end_time"

	// TiDBSlowLogThreshold is used to set the slow log threshold in the server.
	TiDBSlowLogThreshold = "tidb_slow_log_threshold"

	// TiDBRecordPlanInSlowLog is used to log the plan of the slow query.
	TiDBRecordPlanInSlowLog = "tidb_record_plan_in_slow_log"

	// TiDBEnableSlowLog enables TiDB to log slow queries.
	TiDBEnableSlowLog = "tidb_enable_slow_log"

	// TiDBCheckMb4ValueInUTF8 is used to control whether to enable the check wrong utf8 value.
	TiDBCheckMb4ValueInUTF8 = "tidb_check_mb4_value_in_utf8"

	// TiDBFoundInPlanCache indicates whether the last statement was found in plan cache
	TiDBFoundInPlanCache = "last_plan_from_cache"

	// TiDBFoundInBinding indicates whether the last statement was matched with the hints in the binding.
	TiDBFoundInBinding = "last_plan_from_binding"

	// TiDBAllowAutoRandExplicitInsert indicates whether explicit insertion on auto_random column is allowed.
	TiDBAllowAutoRandExplicitInsert = "allow_auto_random_explicit_insert"

	// TiDBTxnScope indicates whether using global transactions or local transactions.
	TiDBTxnScope = "txn_scope"

	// TiDBTxnReadTS indicates the next transaction should be staleness transaction and provide the startTS
	TiDBTxnReadTS = "tx_read_ts"

	// TiDBReadStaleness indicates the staleness duration for following statement
	TiDBReadStaleness = "tidb_read_staleness"

	// TiDBEnablePaging indicates whether paging is enabled in coprocessor requests.
	TiDBEnablePaging = "tidb_enable_paging"

	// TiDBReadConsistency indicates whether the autocommit read statement goes through TiKV RC.
	TiDBReadConsistency = "tidb_read_consistency"

	// TiDBSysdateIsNow is the name of the `tidb_sysdate_is_now` system variable
	TiDBSysdateIsNow = "tidb_sysdate_is_now"

	// RequireSecureTransport indicates the secure mode for data transport
	RequireSecureTransport = "require_secure_transport"
)

// TiDB system variable names that both in session and global scope.
const (
	// TiDBBuildStatsConcurrency is used to speed up the ANALYZE statement, when a table has multiple indices,
	// those indices can be scanned concurrently, with the cost of higher system performance impact.
	TiDBBuildStatsConcurrency = "tidb_build_stats_concurrency"

	// TiDBDistSQLScanConcurrency is used to set the concurrency of a distsql scan task.
	// A distsql scan task can be a table scan or a index scan, which may be distributed to many TiKV nodes.
	// Higher concurrency may reduce latency, but with the cost of higher memory usage and system performance impact.
	// If the query has a LIMIT clause, high concurrency makes the system do much more work than needed.
	TiDBDistSQLScanConcurrency = "tidb_distsql_scan_concurrency"

	// TiDBOptInSubqToJoinAndAgg is used to enable/disable the optimizer rule of rewriting IN subquery.
	TiDBOptInSubqToJoinAndAgg = "tidb_opt_insubq_to_join_and_agg"

	// TiDBOptPreferRangeScan is used to enable/disable the optimizer to always prefer range scan over table scan, ignoring their costs.
	TiDBOptPreferRangeScan = "tidb_opt_prefer_range_scan"

	// TiDBOptEnableCorrelationAdjustment is used to indicates if enable correlation adjustment.
	TiDBOptEnableCorrelationAdjustment = "tidb_opt_enable_correlation_adjustment"

	// TiDBOptLimitPushDownThreshold determines if push Limit or TopN down to TiKV forcibly.
	TiDBOptLimitPushDownThreshold = "tidb_opt_limit_push_down_threshold"

	// TiDBOptCorrelationThreshold is a guard to enable row count estimation using column order correlation.
	TiDBOptCorrelationThreshold = "tidb_opt_correlation_threshold"

	// TiDBOptCorrelationExpFactor is an exponential factor to control heuristic approach when tidb_opt_correlation_threshold is not satisfied.
	TiDBOptCorrelationExpFactor = "tidb_opt_correlation_exp_factor"

	// TiDBOptCPUFactor is the CPU cost of processing one expression for one row.
	TiDBOptCPUFactor = "tidb_opt_cpu_factor"
	// TiDBOptCopCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
	TiDBOptCopCPUFactor = "tidb_opt_copcpu_factor"
	// TiDBOptTiFlashConcurrencyFactor is concurrency number of tiflash computation.
	TiDBOptTiFlashConcurrencyFactor = "tidb_opt_tiflash_concurrency_factor"
	// TiDBOptNetworkFactor is the network cost of transferring 1 byte data.
	TiDBOptNetworkFactor = "tidb_opt_network_factor"
	// TiDBOptScanFactor is the IO cost of scanning 1 byte data on TiKV.
	TiDBOptScanFactor = "tidb_opt_scan_factor"
	// TiDBOptDescScanFactor is the IO cost of scanning 1 byte data on TiKV in desc order.
	TiDBOptDescScanFactor = "tidb_opt_desc_factor"
	// TiDBOptSeekFactor is the IO cost of seeking the start value in a range on TiKV or TiFlash.
	TiDBOptSeekFactor = "tidb_opt_seek_factor"
	// TiDBOptMemoryFactor is the memory cost of storing one tuple.
	TiDBOptMemoryFactor = "tidb_opt_memory_factor"
	// TiDBOptDiskFactor is the IO cost of reading/writing one byte to temporary disk.
	TiDBOptDiskFactor = "tidb_opt_disk_factor"
	// TiDBOptConcurrencyFactor is the CPU cost of additional one goroutine.
	TiDBOptConcurrencyFactor = "tidb_opt_concurrency_factor"

	// TiDBIndexJoinBatchSize is used to set the batch size of an index lookup join.
	// The index lookup join fetches batches of data from outer executor and constructs ranges for inner executor.
	// This value controls how much of data in a batch to do the index join.
	// Large value may reduce the latency but consumes more system resource.
	TiDBIndexJoinBatchSize = "tidb_index_join_batch_size"

	// TiDBIndexLookupSize is used for index lookup executor.
	// The index lookup executor first scan a batch of handles from a index, then use those handles to lookup the table
	// rows, this value controls how much of handles in a batch to do a lookup task.
	// Small value sends more RPCs to TiKV, consume more system resource.
	// Large value may do more work than needed if the query has a limit.
	TiDBIndexLookupSize = "tidb_index_lookup_size"

	// TiDBIndexLookupConcurrency is used for index lookup executor.
	// A lookup task may have 'tidb_index_lookup_size' of handles at maximum, the handles may be distributed
	// in many TiKV nodes, we execute multiple concurrent index lookup tasks concurrently to reduce the time
	// waiting for a task to finish.
	// Set this value higher may reduce the latency but consumes more system resource.
	// tidb_index_lookup_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBIndexLookupConcurrency = "tidb_index_lookup_concurrency"

	// TiDBIndexLookupJoinConcurrency is used for index lookup join executor.
	// IndexLookUpJoin starts "tidb_index_lookup_join_concurrency" inner workers
	// to fetch inner rows and join the matched (outer, inner) row pairs.
	// tidb_index_lookup_join_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBIndexLookupJoinConcurrency = "tidb_index_lookup_join_concurrency"

	// TiDBIndexSerialScanConcurrency is used for controlling the concurrency of index scan operation
	// when we need to keep the data output order the same as the order of index data.
	TiDBIndexSerialScanConcurrency = "tidb_index_serial_scan_concurrency"

	// TiDBMaxChunkSize is used to control the max chunk size during query execution.
	TiDBMaxChunkSize = "tidb_max_chunk_size"

	// TiDBAllowBatchCop means if we should send batch coprocessor to TiFlash. It can be set to 0, 1 and 2.
	// 0 means never use batch cop, 1 means use batch cop in case of aggregation and join, 2, means to force sending batch cop for any query.
	// The default value is 0
	TiDBAllowBatchCop = "tidb_allow_batch_cop"

	// TiDBAllowMPPExecution means if we should use mpp way to execute query or not.
	// Default value is `true`, means to be determined by the optimizer.
	// Value set to `false` means never use mpp.
	TiDBAllowMPPExecution = "tidb_allow_mpp"

	// TiDBHashExchangeWithNewCollation means if hash exchange is supported when new collation is on.
	// Default value is `true`, means support hash exchange when new collation is on.
	// Value set to `false` means not support hash exchange when new collation is on.
	TiDBHashExchangeWithNewCollation = "tidb_hash_exchange_with_new_collation"

	// TiDBEnforceMPPExecution means if we should enforce mpp way to execute query or not.
	// Default value is `false`, means to be determined by variable `tidb_allow_mpp`.
	// Value set to `true` means enforce use mpp.
	// Note if you want to set `tidb_enforce_mpp` to `true`, you must set `tidb_allow_mpp` to `true` first.
	TiDBEnforceMPPExecution = "tidb_enforce_mpp"

	// TiDBMaxTiFlashThreads is the maximum number of threads to execute the request which is pushed down to tiflash.
	// Default value is -1, means it will not be pushed down to tiflash.
	// If the value is bigger than -1, it will be pushed down to tiflash and used to create db context in tiflash.
	TiDBMaxTiFlashThreads = "tidb_max_tiflash_threads"
	// TiDBMPPStoreFailTTL is the unavailable time when a store is detected failed. During that time, tidb will not send any task to
	// TiFlash even though the failed TiFlash node has been recovered.
	TiDBMPPStoreFailTTL = "tidb_mpp_store_fail_ttl"

	// TiDBInitChunkSize is used to control the init chunk size during query execution.
	TiDBInitChunkSize = "tidb_init_chunk_size"

	// TiDBEnableCascadesPlanner is used to control whether to enable the cascades planner.
	TiDBEnableCascadesPlanner = "tidb_enable_cascades_planner"

	// TiDBSkipUTF8Check skips the UTF8 validate process, validate UTF8 has performance cost, if we can make sure
	// the input string values are valid, we can skip the check.
	TiDBSkipUTF8Check = "tidb_skip_utf8_check"

	// TiDBSkipASCIICheck skips the ASCII validate process
	// old tidb may already have fields with invalid ASCII bytes
	// disable ASCII validate can guarantee a safe replication
	TiDBSkipASCIICheck = "tidb_skip_ascii_check"

	// TiDBHashJoinConcurrency is used for hash join executor.
	// The hash join outer executor starts multiple concurrent join workers to probe the hash table.
	// tidb_hash_join_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashJoinConcurrency = "tidb_hash_join_concurrency"

	// TiDBProjectionConcurrency is used for projection operator.
	// This variable controls the worker number of projection operator.
	// tidb_projection_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBProjectionConcurrency = "tidb_projection_concurrency"

	// TiDBHashAggPartialConcurrency is used for hash agg executor.
	// The hash agg executor starts multiple concurrent partial workers to do partial aggregate works.
	// tidb_hashagg_partial_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashAggPartialConcurrency = "tidb_hashagg_partial_concurrency"

	// TiDBHashAggFinalConcurrency is used for hash agg executor.
	// The hash agg executor starts multiple concurrent final workers to do final aggregate works.
	// tidb_hashagg_final_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashAggFinalConcurrency = "tidb_hashagg_final_concurrency"

	// TiDBWindowConcurrency is used for window parallel executor.
	// tidb_window_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBWindowConcurrency = "tidb_window_concurrency"

	// TiDBMergeJoinConcurrency is used for merge join parallel executor
	TiDBMergeJoinConcurrency = "tidb_merge_join_concurrency"

	// TiDBStreamAggConcurrency is used for stream aggregation parallel executor.
	// tidb_stream_agg_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBStreamAggConcurrency = "tidb_streamagg_concurrency"

	// TiDBEnableParallelApply is used for parallel apply.
	TiDBEnableParallelApply = "tidb_enable_parallel_apply"

	// TiDBBackoffLockFast is used for tikv backoff base time in milliseconds.
	TiDBBackoffLockFast = "tidb_backoff_lock_fast"

	// TiDBBackOffWeight is used to control the max back off time in TiDB.
	// The default maximum back off time is a small value.
	// BackOffWeight could multiply it to let the user adjust the maximum time for retrying.
	// Only positive integers can be accepted, which means that the maximum back off time can only grow.
	TiDBBackOffWeight = "tidb_backoff_weight"

	// TiDBDDLReorgWorkerCount defines the count of ddl reorg workers.
	TiDBDDLReorgWorkerCount = "tidb_ddl_reorg_worker_cnt"

	// TiDBDDLReorgBatchSize defines the transaction batch size of ddl reorg workers.
	TiDBDDLReorgBatchSize = "tidb_ddl_reorg_batch_size"

	// TiDBDDLErrorCountLimit defines the count of ddl error limit.
	TiDBDDLErrorCountLimit = "tidb_ddl_error_count_limit"

	// TiDBDDLReorgPriority defines the operations' priority of adding indices.
	// It can be: PRIORITY_LOW, PRIORITY_NORMAL, PRIORITY_HIGH
	TiDBDDLReorgPriority = "tidb_ddl_reorg_priority"

	// TiDBEnableChangeMultiSchema is used to control whether to enable the change multi schema.
	TiDBEnableChangeMultiSchema = "tidb_enable_change_multi_schema"

	// TiDBEnableAutoIncrementInGenerated disables the mysql compatibility check on using auto-incremented columns in
	// expression indexes and generated columns described here https://dev.mysql.com/doc/refman/5.7/en/create-table-generated-columns.html for details.
	TiDBEnableAutoIncrementInGenerated = "tidb_enable_auto_increment_in_generated"

	// TiDBEnablePointGetCache is used to control whether to enable the point get cache for special scenario.
	TiDBEnablePointGetCache = "tidb_enable_point_get_cache"

	// TiDBPlacementMode is used to control the mode for placement
	TiDBPlacementMode = "tidb_placement_mode"

	// TiDBMaxDeltaSchemaCount defines the max length of deltaSchemaInfos.
	// deltaSchemaInfos is a queue that maintains the history of schema changes.
	TiDBMaxDeltaSchemaCount = "tidb_max_delta_schema_count"

	// TiDBScatterRegion will scatter the regions for DDLs when it is ON.
	TiDBScatterRegion = "tidb_scatter_region"

	// TiDBWaitSplitRegionFinish defines the split region behaviour is sync or async.
	TiDBWaitSplitRegionFinish = "tidb_wait_split_region_finish"

	// TiDBWaitSplitRegionTimeout uses to set the split and scatter region back off time.
	TiDBWaitSplitRegionTimeout = "tidb_wait_split_region_timeout"

	// TiDBForcePriority defines the operations' priority of all statements.
	// It can be "NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"
	TiDBForcePriority = "tidb_force_priority"

	// TiDBConstraintCheckInPlace indicates to check the constraint when the SQL executing.
	// It could hurt the performance of bulking insert when it is ON.
	TiDBConstraintCheckInPlace = "tidb_constraint_check_in_place"

	// TiDBEnableWindowFunction is used to control whether to enable the window function.
	TiDBEnableWindowFunction = "tidb_enable_window_function"

	// TiDBEnablePipelinedWindowFunction is used to control whether to use pipelined window function, it only works when tidb_enable_window_function = true.
	TiDBEnablePipelinedWindowFunction = "tidb_enable_pipelined_window_function"

	// TiDBEnableStrictDoubleTypeCheck is used to control table field double type syntax check.
	TiDBEnableStrictDoubleTypeCheck = "tidb_enable_strict_double_type_check"

	// TiDBOptProjectionPushDown is used to control whether to pushdown projection to coprocessor.
	TiDBOptProjectionPushDown = "tidb_opt_projection_push_down"

	// TiDBEnableVectorizedExpression is used to control whether to enable the vectorized expression evaluation.
	TiDBEnableVectorizedExpression = "tidb_enable_vectorized_expression"

	// TiDBOptJoinReorderThreshold defines the threshold less than which
	// we'll choose a rather time-consuming algorithm to calculate the join order.
	TiDBOptJoinReorderThreshold = "tidb_opt_join_reorder_threshold"

	// TiDBSlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
	TiDBSlowQueryFile = "tidb_slow_query_file"

	// TiDBEnableFastAnalyze indicates to use fast analyze.
	TiDBEnableFastAnalyze = "tidb_enable_fast_analyze"

	// TiDBExpensiveQueryTimeThreshold indicates the time threshold of expensive query.
	TiDBExpensiveQueryTimeThreshold = "tidb_expensive_query_time_threshold"

	// TiDBEnableIndexMerge indicates to generate IndexMergePath.
	TiDBEnableIndexMerge = "tidb_enable_index_merge"

	// TiDBEnableNoopFuncs set true will enable using fake funcs(like get_lock release_lock)
	TiDBEnableNoopFuncs = "tidb_enable_noop_functions"

	// TiDBEnableStmtSummary indicates whether the statement summary is enabled.
	TiDBEnableStmtSummary = "tidb_enable_stmt_summary"

	// TiDBStmtSummaryInternalQuery indicates whether the statement summary contain internal query.
	TiDBStmtSummaryInternalQuery = "tidb_stmt_summary_internal_query"

	// TiDBStmtSummaryRefreshInterval indicates the refresh interval in seconds for each statement summary.
	TiDBStmtSummaryRefreshInterval = "tidb_stmt_summary_refresh_interval"

	// TiDBStmtSummaryHistorySize indicates the history size of each statement summary.
	TiDBStmtSummaryHistorySize = "tidb_stmt_summary_history_size"

	// TiDBStmtSummaryMaxStmtCount indicates the max number of statements kept in memory.
	TiDBStmtSummaryMaxStmtCount = "tidb_stmt_summary_max_stmt_count"

	// TiDBStmtSummaryMaxSQLLength indicates the max length of displayed normalized sql and sample sql.
	TiDBStmtSummaryMaxSQLLength = "tidb_stmt_summary_max_sql_length"

	// TiDBCapturePlanBaseline indicates whether the capture of plan baselines is enabled.
	TiDBCapturePlanBaseline = "tidb_capture_plan_baselines"

	// TiDBUsePlanBaselines indicates whether the use of plan baselines is enabled.
	TiDBUsePlanBaselines = "tidb_use_plan_baselines"

	// TiDBEvolvePlanBaselines indicates whether the evolution of plan baselines is enabled.
	TiDBEvolvePlanBaselines = "tidb_evolve_plan_baselines"

	// TiDBEnableExtendedStats indicates whether the extended statistics feature is enabled.
	TiDBEnableExtendedStats = "tidb_enable_extended_stats"

	// TiDBIsolationReadEngines indicates the tidb only read from the stores whose engine type is involved in IsolationReadEngines.
	// Now, only support TiKV and TiFlash.
	TiDBIsolationReadEngines = "tidb_isolation_read_engines"

	// TiDBStoreLimit indicates the limit of sending request to a store, 0 means without limit.
	TiDBStoreLimit = "tidb_store_limit"

	// TiDBMetricSchemaStep indicates the step when query metric schema.
	TiDBMetricSchemaStep = "tidb_metric_query_step"

	// TiDBMetricSchemaRangeDuration indicates the range duration when query metric schema.
	TiDBMetricSchemaRangeDuration = "tidb_metric_query_range_duration"

	// TiDBEnableCollectExecutionInfo indicates that whether execution info is collected.
	TiDBEnableCollectExecutionInfo = "tidb_enable_collect_execution_info"

	// TiDBExecutorConcurrency is used for controlling the concurrency of all types of executors.
	TiDBExecutorConcurrency = "tidb_executor_concurrency"

	// TiDBEnableClusteredIndex indicates if clustered index feature is enabled.
	TiDBEnableClusteredIndex = "tidb_enable_clustered_index"

	// TiDBPartitionPruneMode indicates the partition prune mode used.
	TiDBPartitionPruneMode = "tidb_partition_prune_mode"

	// TiDBRedactLog indicates that whether redact log.
	TiDBRedactLog = "tidb_redact_log"

	// TiDBRestrictedReadOnly is meant for the cloud admin to toggle the cluster read only
	TiDBRestrictedReadOnly = "tidb_restricted_read_only"

	// TiDBSuperReadOnly is tidb's variant of mysql's super_read_only, which has some differences from mysql's super_read_only.
	TiDBSuperReadOnly = "tidb_super_read_only"

	// TiDBShardAllocateStep indicates the max size of continuous rowid shard in one transaction.
	TiDBShardAllocateStep = "tidb_shard_allocate_step"
	// TiDBEnableTelemetry indicates that whether usage data report to PingCAP is enabled.
	TiDBEnableTelemetry = "tidb_enable_telemetry"

	// TiDBEnableAmendPessimisticTxn indicates if amend pessimistic transactions is enabled.
	TiDBEnableAmendPessimisticTxn = "tidb_enable_amend_pessimistic_txn"

	// TiDBMemoryUsageAlarmRatio indicates the alarm threshold when memory usage of the tidb-server exceeds.
	TiDBMemoryUsageAlarmRatio = "tidb_memory_usage_alarm_ratio"

	// TiDBEnableRateLimitAction indicates whether enabled ratelimit action
	TiDBEnableRateLimitAction = "tidb_enable_rate_limit_action"

	// TiDBEnableAsyncCommit indicates whether to enable the async commit feature.
	TiDBEnableAsyncCommit = "tidb_enable_async_commit"

	// TiDBEnable1PC indicates whether to enable the one-phase commit feature.
	TiDBEnable1PC = "tidb_enable_1pc"

	// TiDBGuaranteeLinearizability indicates whether to guarantee linearizability.
	TiDBGuaranteeLinearizability = "tidb_guarantee_linearizability"

	// TiDBAnalyzeVersion indicates how tidb collects the analyzed statistics and how use to it.
	TiDBAnalyzeVersion = "tidb_analyze_version"

	// TiDBEnableIndexMergeJoin indicates whether to enable index merge join.
	TiDBEnableIndexMergeJoin = "tidb_enable_index_merge_join"

	// TiDBTrackAggregateMemoryUsage indicates whether track the memory usage of aggregate function.
	TiDBTrackAggregateMemoryUsage = "tidb_track_aggregate_memory_usage"

	// TiDBEnableExchangePartition indicates whether to enable exchange partition.
	TiDBEnableExchangePartition = "tidb_enable_exchange_partition"

	// TiDBAllowFallbackToTiKV indicates the engine types whose unavailability triggers fallback to TiKV.
	// Now we only support TiFlash.
	TiDBAllowFallbackToTiKV = "tidb_allow_fallback_to_tikv"

	// TiDBEnableTopSQL indicates whether the top SQL is enabled.
	TiDBEnableTopSQL = "tidb_enable_top_sql"

	// TiDBTopSQLMaxTimeSeriesCount indicates the max number of statements been collected in each time series.
	TiDBTopSQLMaxTimeSeriesCount = "tidb_top_sql_max_time_series_count"

	// TiDBTopSQLMaxMetaCount indicates the max capacity of the collect meta per second.
	TiDBTopSQLMaxMetaCount = "tidb_top_sql_max_meta_count"

	// TiDBEnableLocalTxn indicates whether to enable Local Txn.
	TiDBEnableLocalTxn = "tidb_enable_local_txn"

	// TiDBTSOClientBatchMaxWaitTime indicates the max value of the TSO Batch Wait interval time of PD client.
	TiDBTSOClientBatchMaxWaitTime = "tidb_tso_client_batch_max_wait_time"

	// TiDBTxnCommitBatchSize is used to control the batch size of transaction commit related requests sent by TiDB to TiKV.
	// If a single transaction has a large amount of writes, you can increase the batch size to improve the batch effect,
	// setting too large will exceed TiKV's raft-entry-max-size limit and cause commit failure.
	TiDBTxnCommitBatchSize = "tidb_txn_commit_batch_size"

	// TiDBEnableTSOFollowerProxy indicates whether to enable the TSO Follower Proxy feature of PD client.
	TiDBEnableTSOFollowerProxy = "tidb_enable_tso_follower_proxy"

	// TiDBEnableOrderedResultMode indicates if stabilize query results.
	TiDBEnableOrderedResultMode = "tidb_enable_ordered_result_mode"

	// TiDBRemoveOrderbyInSubquery indicates whether to remove ORDER BY in subquery.
	TiDBRemoveOrderbyInSubquery = "tidb_remove_orderby_in_subquery"

	// TiDBEnablePseudoForOutdatedStats indicates whether use pseudo for outdated stats
	TiDBEnablePseudoForOutdatedStats = "tidb_enable_pseudo_for_outdated_stats"

	// TiDBRegardNULLAsPoint indicates whether regard NULL as point when optimizing
	TiDBRegardNULLAsPoint = "tidb_regard_null_as_point"

	// TiDBTmpTableMaxSize indicates the max memory size of temporary tables.
	TiDBTmpTableMaxSize = "tidb_tmp_table_max_size"

	// TiDBEnableLegacyInstanceScope indicates if instance scope can be set with SET SESSION.
	TiDBEnableLegacyInstanceScope = "tidb_enable_legacy_instance_scope"

	// TiDBTableCacheLease indicates the read lock lease of a cached table.
	TiDBTableCacheLease = "tidb_table_cache_lease"

	// TiDBStatsLoadSyncWait indicates the time sql execution will sync-wait for stats load.
	TiDBStatsLoadSyncWait = "tidb_stats_load_sync_wait"

	// TiDBEnableMutationChecker indicates whether to check data consistency for mutations
	TiDBEnableMutationChecker = "tidb_enable_mutation_checker"
	// TiDBTxnAssertionLevel indicates how strict the assertion will be, which helps to detect and preventing data &
	// index inconsistency problems.
	TiDBTxnAssertionLevel = "tidb_txn_assertion_level"

	// TiDBIgnorePreparedCacheCloseStmt indicates whether to ignore close-stmt commands for prepared statements.
	TiDBIgnorePreparedCacheCloseStmt = "tidb_ignore_prepared_cache_close_stmt"

	// TiDBEnableNewCostInterface is a internal switch to indicates whether to use the new cost calculation interface.
	TiDBEnableNewCostInterface = "tidb_enable_new_cost_interface"

	// TiDBBatchPendingTiFlashCount indicates the maximum count of non-available TiFlash tables.
	TiDBBatchPendingTiFlashCount = "tidb_batch_pending_tiflash_count"

	// TiDBQueryLogMaxLen is used to set the max length of the query in the log.
	TiDBQueryLogMaxLen = "tidb_query_log_max_len"

	// TiDBNonTransactionalIgnoreError is used to ignore error in non-transactional DMLs.
	// When set to false, a non-transactional DML returns when it meets the first error.
	// When set to true, a non-transactional DML finishes all batches even if errors are met in some batches.
	TiDBNonTransactionalIgnoreError = "tidb_nontransactional_ignore_error"
)

// TiDB vars that have only global scope

const (
	// TiDBGCEnable turns garbage collection on or OFF
	TiDBGCEnable = "tidb_gc_enable"
	// TiDBGCRunInterval sets the interval that GC runs
	TiDBGCRunInterval = "tidb_gc_run_interval"
	// TiDBGCLifetime sets the retention window of older versions
	TiDBGCLifetime = "tidb_gc_life_time"
	// TiDBGCConcurrency sets the concurrency of garbage collection. -1 = AUTO value
	TiDBGCConcurrency = "tidb_gc_concurrency"
	// TiDBGCScanLockMode enables the green GC feature (default)
	TiDBGCScanLockMode = "tidb_gc_scan_lock_mode"
	// TiDBGCMaxWaitTime sets max time for gc advances the safepoint delayed by active transactions
	TiDBGCMaxWaitTime = "tidb_gc_max_wait_time"
	// TiDBEnableEnhancedSecurity restricts SUPER users from certain operations.
	TiDBEnableEnhancedSecurity = "tidb_enable_enhanced_security"
	// TiDBEnableHistoricalStats enables the historical statistics feature (default off)
	TiDBEnableHistoricalStats = "tidb_enable_historical_stats"
	// TiDBPersistAnalyzeOptions persists analyze options for later analyze and auto-analyze
	TiDBPersistAnalyzeOptions = "tidb_persist_analyze_options"
	// TiDBEnableColumnTracking enables collecting predicate columns.
	TiDBEnableColumnTracking = "tidb_enable_column_tracking"
	// TiDBDisableColumnTrackingTime records the last time TiDBEnableColumnTracking is set off.
	// It is used to invalidate the collected predicate columns after turning off TiDBEnableColumnTracking, which avoids physical deletion.
	// It doesn't have cache in memory, and we directly get/set the variable value from/to mysql.tidb.
	TiDBDisableColumnTrackingTime = "tidb_disable_column_tracking_time"
	// TiDBStatsLoadPseudoTimeout indicates whether to fallback to pseudo stats after load timeout.
	TiDBStatsLoadPseudoTimeout = "tidb_stats_load_pseudo_timeout"
	// TiDBMemQuotaBindingCache indicates the memory quota for the bind cache.
	TiDBMemQuotaBindingCache = "tidb_mem_quota_binding_cache"
	// TiDBRCReadCheckTS indicates the tso optimization for read-consistency read is enabled.
	TiDBRCReadCheckTS = "tidb_rc_read_check_ts"
	// TiDBCommitterConcurrency controls the number of running concurrent requests in the commit phase.
	TiDBCommitterConcurrency = "tidb_committer_concurrency"
	// TiDBEnableBatchDML enables batch dml.
	TiDBEnableBatchDML = "tidb_enable_batch_dml"
	// TiDBStatsCacheMemQuota records stats cache quota
	TiDBStatsCacheMemQuota = "tidb_stats_cache_mem_quota"
	// TiDBMemQuotaAnalyze indicates the memory quota for all analyze jobs.
	TiDBMemQuotaAnalyze = "tidb_mem_quota_analyze"
	// TiDBEnableAutoAnalyze determines whether TiDB executes automatic analysis.
	TiDBEnableAutoAnalyze = "tidb_enable_auto_analyze"
	//TiDBMemOOMAction indicates what operation TiDB perform when a single SQL statement exceeds
	// the memory quota specified by tidb_mem_quota_query and cannot be spilled to disk.
	TiDBMemOOMAction = "tidb_mem_oom_action"
	// TiDBEnablePrepPlanCache indicates whether to enable prepared plan cache
	TiDBEnablePrepPlanCache = "tidb_enable_prepared_plan_cache"
	// TiDBPrepPlanCacheSize indicates the number of cached statements.
	TiDBPrepPlanCacheSize = "tidb_prepared_plan_cache_size"
	// TiDBPrepPlanCacheMemoryGuardRatio is used to prevent [performance.max-memory] from being exceeded
	TiDBPrepPlanCacheMemoryGuardRatio = "tidb_prepared_plan_cache_memory_guard_ratio"
	// TiDBMaxAutoAnalyzeTime is the max time that auto analyze can run. If auto analyze runs longer than the value, it
	// will be killed. 0 indicates that there is no time limit.
	TiDBMaxAutoAnalyzeTime = "tidb_max_auto_analyze_time"
)

// TiDB intentional limits
// Can be raised in the future.

const (
	// MaxConfigurableConcurrency is the maximum number of "threads" (goroutines) that can be specified
	// for any type of configuration item that has concurrent workers.
	MaxConfigurableConcurrency = 256
)

// Default TiDB system variable values.
const (
	DefHostname                                  = "localhost"
	DefIndexLookupConcurrency                    = ConcurrencyUnset
	DefIndexLookupJoinConcurrency                = ConcurrencyUnset
	DefIndexSerialScanConcurrency                = 1
	DefIndexJoinBatchSize                        = 25000
	DefIndexLookupSize                           = 20000
	DefDistSQLScanConcurrency                    = 15
	DefBuildStatsConcurrency                     = 4
	DefAutoAnalyzeRatio                          = 0.5
	DefAutoAnalyzeStartTime                      = "00:00 +0000"
	DefAutoAnalyzeEndTime                        = "23:59 +0000"
	DefAutoIncrementIncrement                    = 1
	DefAutoIncrementOffset                       = 1
	DefChecksumTableConcurrency                  = 4
	DefSkipUTF8Check                             = false
	DefSkipASCIICheck                            = false
	DefOptAggPushDown                            = false
	DefOptCartesianBCJ                           = 1
	DefOptMPPOuterJoinFixedBuildSide             = false
	DefOptWriteRowID                             = false
	DefOptEnableCorrelationAdjustment            = true
	DefOptLimitPushDownThreshold                 = 100
	DefOptCorrelationThreshold                   = 0.9
	DefOptCorrelationExpFactor                   = 1
	DefOptCPUFactor                              = 3.0
	DefOptCopCPUFactor                           = 3.0
	DefOptTiFlashConcurrencyFactor               = 24.0
	DefOptNetworkFactor                          = 1.0
	DefOptScanFactor                             = 1.5
	DefOptDescScanFactor                         = 3.0
	DefOptSeekFactor                             = 20.0
	DefOptMemoryFactor                           = 0.001
	DefOptDiskFactor                             = 1.5
	DefOptConcurrencyFactor                      = 3.0
	DefOptInSubqToJoinAndAgg                     = true
	DefOptPreferRangeScan                        = false
	DefBatchInsert                               = false
	DefBatchDelete                               = false
	DefBatchCommit                               = false
	DefCurretTS                                  = 0
	DefInitChunkSize                             = 32
	DefMaxChunkSize                              = 1024
	DefDMLBatchSize                              = 0
	DefMaxPreparedStmtCount                      = -1
	DefWaitTimeout                               = 28800
	DefTiDBMemQuotaApplyCache                    = 32 << 20 // 32MB.
	DefTiDBMemQuotaBindingCache                  = 64 << 20 // 64MB.
	DefTiDBGeneralLog                            = false
	DefTiDBPProfSQLCPU                           = 0
	DefTiDBRetryLimit                            = 10
	DefTiDBDisableTxnAutoRetry                   = true
	DefTiDBConstraintCheckInPlace                = false
	DefTiDBHashJoinConcurrency                   = ConcurrencyUnset
	DefTiDBProjectionConcurrency                 = ConcurrencyUnset
	DefBroadcastJoinThresholdSize                = 100 * 1024 * 1024
	DefBroadcastJoinThresholdCount               = 10 * 1024
	DefTiDBOptimizerSelectivityLevel             = 0
	DefTiDBOptimizerEnableNewOFGB                = false
	DefTiDBEnableOuterJoinReorder                = true
	DefTiDBAllowBatchCop                         = 1
	DefTiDBAllowMPPExecution                     = true
	DefTiDBHashExchangeWithNewCollation          = true
	DefTiDBEnforceMPPExecution                   = false
	DefTiFlashMaxThreads                         = -1
	DefTiDBMPPStoreFailTTL                       = "60s"
	DefTiDBTxnMode                               = ""
	DefTiDBRowFormatV1                           = 1
	DefTiDBRowFormatV2                           = 2
	DefTiDBDDLReorgWorkerCount                   = 4
	DefTiDBDDLReorgBatchSize                     = 256
	DefTiDBDDLErrorCountLimit                    = 512
	DefTiDBMaxDeltaSchemaCount                   = 1024
	DefTiDBChangeMultiSchema                     = false
	DefTiDBPointGetCache                         = false
	DefTiDBPlacementMode                         = PlacementModeStrict
	DefTiDBEnableAutoIncrementInGenerated        = false
	DefTiDBHashAggPartialConcurrency             = ConcurrencyUnset
	DefTiDBHashAggFinalConcurrency               = ConcurrencyUnset
	DefTiDBWindowConcurrency                     = ConcurrencyUnset
	DefTiDBMergeJoinConcurrency                  = 1 // disable optimization by default
	DefTiDBStreamAggConcurrency                  = 1
	DefTiDBForcePriority                         = mysql.NoPriority
	DefEnableWindowFunction                      = true
	DefEnablePipelinedWindowFunction             = true
	DefEnableStrictDoubleTypeCheck               = true
	DefEnableVectorizedExpression                = true
	DefTiDBOptJoinReorderThreshold               = 0
	DefTiDBDDLSlowOprThreshold                   = 300
	DefTiDBUseFastAnalyze                        = false
	DefTiDBSkipIsolationLevelCheck               = false
	DefTiDBExpensiveQueryTimeThreshold           = 60 // 60s
	DefTiDBScatterRegion                         = false
	DefTiDBWaitSplitRegionFinish                 = true
	DefWaitSplitRegionTimeout                    = 300 // 300s
	DefTiDBEnableNoopFuncs                       = Off
	DefTiDBAllowRemoveAutoInc                    = false
	DefTiDBUsePlanBaselines                      = true
	DefTiDBEvolvePlanBaselines                   = false
	DefTiDBEvolvePlanTaskMaxTime                 = 600 // 600s
	DefTiDBEvolvePlanTaskStartTime               = "00:00 +0000"
	DefTiDBEvolvePlanTaskEndTime                 = "23:59 +0000"
	DefInnodbLockWaitTimeout                     = 50 // 50s
	DefTiDBStoreLimit                            = 0
	DefTiDBMetricSchemaStep                      = 60 // 60s
	DefTiDBMetricSchemaRangeDuration             = 60 // 60s
	DefTiDBFoundInPlanCache                      = false
	DefTiDBFoundInBinding                        = false
	DefTiDBEnableCollectExecutionInfo            = true
	DefTiDBAllowAutoRandExplicitInsert           = false
	DefTiDBEnableClusteredIndex                  = ClusteredIndexDefModeIntOnly
	DefTiDBRedactLog                             = false
	DefTiDBRestrictedReadOnly                    = false
	DefTiDBSuperReadOnly                         = false
	DefTiDBShardAllocateStep                     = math.MaxInt64
	DefTiDBEnableTelemetry                       = true
	DefTiDBEnableParallelApply                   = false
	DefTiDBEnableAmendPessimisticTxn             = false
	DefTiDBPartitionPruneMode                    = "static"
	DefTiDBEnableRateLimitAction                 = true
	DefTiDBEnableAsyncCommit                     = false
	DefTiDBEnable1PC                             = false
	DefTiDBGuaranteeLinearizability              = true
	DefTiDBAnalyzeVersion                        = 2
	DefTiDBEnableIndexMergeJoin                  = false
	DefTiDBTrackAggregateMemoryUsage             = true
	DefTiDBEnableExchangePartition               = false
	DefCTEMaxRecursionDepth                      = 1000
	DefTiDBTmpTableMaxSize                       = 64 << 20 // 64MB.
	DefTiDBEnableLocalTxn                        = false
	DefTiDBTSOClientBatchMaxWaitTime             = 0.0 // 0ms
	DefTiDBEnableTSOFollowerProxy                = false
	DefTiDBEnableOrderedResultMode               = false
	DefTiDBEnablePseudoForOutdatedStats          = true
	DefTiDBRegardNULLAsPoint                     = true
	DefEnablePlacementCheck                      = true
	DefTimestamp                                 = "0"
	DefTiDBEnableStmtSummary                     = true
	DefTiDBStmtSummaryInternalQuery              = false
	DefTiDBStmtSummaryRefreshInterval            = 1800
	DefTiDBStmtSummaryHistorySize                = 24
	DefTiDBStmtSummaryMaxStmtCount               = 3000
	DefTiDBStmtSummaryMaxSQLLength               = 4096
	DefTiDBCapturePlanBaseline                   = Off
	DefTiDBEnableIndexMerge                      = true
	DefEnableLegacyInstanceScope                 = true
	DefTiDBTableCacheLease                       = 3 // 3s
	DefTiDBPersistAnalyzeOptions                 = true
	DefTiDBEnableColumnTracking                  = false
	DefTiDBStatsLoadSyncWait                     = 0
	DefTiDBStatsLoadPseudoTimeout                = false
	DefSysdateIsNow                              = false
	DefTiDBEnableMutationChecker                 = false
	DefTiDBTxnAssertionLevel                     = AssertionOffStr
	DefTiDBIgnorePreparedCacheCloseStmt          = false
	DefTiDBBatchPendingTiFlashCount              = 4000
	DefRCReadCheckTS                             = false
	DefTiDBRemoveOrderbyInSubquery               = false
	DefTiDBReadStaleness                         = 0
	DefTiDBGCMaxWaitTime                         = 24 * 60 * 60
	DefMaxAllowedPacket                   uint64 = 67108864
	DefTiDBEnableBatchDML                        = false
	DefTiDBMemQuotaQuery                         = 1073741824 // 1GB
	DefTiDBStatsCacheMemQuota                    = 0
	MaxTiDBStatsCacheMemQuota                    = 1024 * 1024 * 1024 * 1024 // 1TB
	DefTiDBQueryLogMaxLen                        = 4096
	DefRequireSecureTransport                    = false
	DefTiDBCommitterConcurrency                  = 128
	DefTiDBBatchDMLIgnoreError                   = false
	DefTiDBMemQuotaAnalyze                       = -1
	DefTiDBEnableAutoAnalyze                     = true
	DefTiDBMemOOMAction                          = "CANCEL"
	DefTiDBMaxAutoAnalyzeTime                    = 12 * 60 * 60
	DefTiDBEnablePrepPlanCache                   = true
	DefTiDBPrepPlanCacheSize                     = 100
	DefTiDBPrepPlanCacheMemoryGuardRatio         = 0.1
)

// Process global variables.
var (
	ProcessGeneralLog           = atomic.NewBool(false)
	RunAutoAnalyze              = atomic.NewBool(DefTiDBEnableAutoAnalyze)
	GlobalLogMaxDays            = atomic.NewInt32(int32(config.GetGlobalConfig().Log.File.MaxDays))
	QueryLogMaxLen              = atomic.NewInt32(DefTiDBQueryLogMaxLen)
	EnablePProfSQLCPU           = atomic.NewBool(false)
	EnableBatchDML              = atomic.NewBool(false)
	ddlReorgWorkerCounter int32 = DefTiDBDDLReorgWorkerCount
	ddlReorgBatchSize     int32 = DefTiDBDDLReorgBatchSize
	ddlErrorCountlimit    int64 = DefTiDBDDLErrorCountLimit
	ddlReorgRowFormat     int64 = DefTiDBRowFormatV2
	maxDeltaSchemaCount   int64 = DefTiDBMaxDeltaSchemaCount
	// MaxDDLReorgBatchSize is exported for testing.
	MaxDDLReorgBatchSize int32 = 10240
	MinDDLReorgBatchSize int32 = 32
	// DDLSlowOprThreshold is the threshold for ddl slow operations, uint is millisecond.
	DDLSlowOprThreshold                   = config.GetGlobalConfig().Instance.DDLSlowOprThreshold
	ForcePriority                         = int32(DefTiDBForcePriority)
	MaxOfMaxAllowedPacket          uint64 = 1073741824
	ExpensiveQueryTimeThreshold    uint64 = DefTiDBExpensiveQueryTimeThreshold
	MinExpensiveQueryTimeThreshold uint64 = 10 // 10s
	DefExecutorConcurrency                = 5
	MemoryUsageAlarmRatio                 = atomic.NewFloat64(config.GetGlobalConfig().Instance.MemoryUsageAlarmRatio)
	EnableLocalTxn                        = atomic.NewBool(DefTiDBEnableLocalTxn)
	MaxTSOBatchWaitInterval               = atomic.NewFloat64(DefTiDBTSOClientBatchMaxWaitTime)
	EnableTSOFollowerProxy                = atomic.NewBool(DefTiDBEnableTSOFollowerProxy)
	RestrictedReadOnly                    = atomic.NewBool(DefTiDBRestrictedReadOnly)
	VarTiDBSuperReadOnly                  = atomic.NewBool(DefTiDBSuperReadOnly)
	PersistAnalyzeOptions                 = atomic.NewBool(DefTiDBPersistAnalyzeOptions)
	TableCacheLease                       = atomic.NewInt64(DefTiDBTableCacheLease)
	EnableColumnTracking                  = atomic.NewBool(DefTiDBEnableColumnTracking)
	StatsLoadSyncWait                     = atomic.NewInt64(DefTiDBStatsLoadSyncWait)
	StatsLoadPseudoTimeout                = atomic.NewBool(DefTiDBStatsLoadPseudoTimeout)
	MemQuotaBindingCache                  = atomic.NewInt64(DefTiDBMemQuotaBindingCache)
	GCMaxWaitTime                         = atomic.NewInt64(DefTiDBGCMaxWaitTime)
	StatsCacheMemQuota                    = atomic.NewInt64(DefTiDBStatsCacheMemQuota)
	OOMAction                             = atomic.NewString(DefTiDBMemOOMAction)
	MaxAutoAnalyzeTime                    = atomic.NewInt64(DefTiDBMaxAutoAnalyzeTime)
	// variables for plan cache
	EnablePreparedPlanCache           = atomic.NewBool(DefTiDBEnablePrepPlanCache)
	PreparedPlanCacheSize             = atomic.NewUint64(DefTiDBPrepPlanCacheSize)
	PreparedPlanCacheMemoryGuardRatio = atomic.NewFloat64(DefTiDBPrepPlanCacheMemoryGuardRatio)
)

var (
	// SetMemQuotaAnalyze is the func registered by global/subglobal tracker to set memory quota.
	SetMemQuotaAnalyze func(quota int64) = nil
	// GetMemQuotaAnalyze is the func registered by global/subglobal tracker to get memory quota.
	GetMemQuotaAnalyze func() int64 = nil
	// SetStatsCacheCapacity is the func registered by domain to set statsCache memory quota.
	SetStatsCacheCapacity atomic.Value
)
