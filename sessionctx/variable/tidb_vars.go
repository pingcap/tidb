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
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"math"
	"os"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/uber-go/atomic"
)

/*
	Steps to add a new TiDB specific system variable:

	1. Add a new variable name with comment in this file.
	2. Add the default value of the new variable in this file.
	3. Add SysVar instance in 'defaultSysVars' slice with the default value.
	4. Add a field in `SessionVars`.
	5. Update the `NewSessionVars` function to set the field to its default value.
	6. Update the `variable.SetSessionSystemVar` function to use the new value when SET statement is executed.
	7. If it is a global variable, add it in `session.loadCommonGlobalVarsSQL`.
	8. Update ValidateSetSystemVar if the variable's value need to be validated.
	9. Use this variable to control the behavior in code.
*/

// TiDB system variable names that only in session scope.
const (
	TiDBDDLSlowOprThreshold = "ddl_slow_threshold"

	// tidb_snapshot is used for reading history data, the default value is empty string.
	// The value can be a datetime string like '2017-11-11 20:20:20' or a tso string. When this variable is set, the session reads history data of that time.
	TiDBSnapshot = "tidb_snapshot"

	// tidb_opt_agg_push_down is used to enable/disable the optimizer rule of aggregation push down.
	TiDBOptAggPushDown = "tidb_opt_agg_push_down"

	TiDBOptBCJ = "tidb_opt_broadcast_join"
	// tidb_opt_distinct_agg_push_down is used to decide whether agg with distinct should be pushed to tikv/tiflash.
	TiDBOptDistinctAggPushDown = "tidb_opt_distinct_agg_push_down"

	// tidb_broadcast_join_threshold_size is used to limit the size of small table for mpp broadcast join.
	// It's unit is bytes, if the size of small table is larger than it, we will not use bcj.
	TiDBBCJThresholdSize = "tidb_broadcast_join_threshold_size"

	// tidb_broadcast_join_threshold_count is used to limit the count of small table for mpp broadcast join.
	// If we can't estimate the size of one side of join child, we will check if its row number exceeds this limitation.
	TiDBBCJThresholdCount = "tidb_broadcast_join_threshold_count"

	// tidb_opt_write_row_id is used to enable/disable the operations of insert、replace and update to _tidb_rowid.
	TiDBOptWriteRowID = "tidb_opt_write_row_id"

	// Auto analyze will run if (table modify count)/(table row count) is greater than this value.
	TiDBAutoAnalyzeRatio = "tidb_auto_analyze_ratio"

	// Auto analyze will run if current time is within start time and end time.
	TiDBAutoAnalyzeStartTime = "tidb_auto_analyze_start_time"
	TiDBAutoAnalyzeEndTime   = "tidb_auto_analyze_end_time"

	// tidb_checksum_table_concurrency is used to speed up the ADMIN CHECKSUM TABLE
	// statement, when a table has multiple indices, those indices can be
	// scanned concurrently, with the cost of higher system performance impact.
	TiDBChecksumTableConcurrency = "tidb_checksum_table_concurrency"

	// TiDBCurrentTS is used to get the current transaction timestamp.
	// It is read-only.
	TiDBCurrentTS = "tidb_current_ts"

	// TiDBLastTxnInfo is used to get the last transaction info within the current session.
	TiDBLastTxnInfo = "tidb_last_txn_info"

	// TiDBLastTxnInfo is used to get the last query info within the current session.
	TiDBLastQueryInfo = "tidb_last_query_info"

	// tidb_config is a read-only variable that shows the config of the current server.
	TiDBConfig = "tidb_config"

	// tidb_batch_insert is used to enable/disable auto-split insert data. If set this option on, insert executor will automatically
	// insert data into multiple batches and use a single txn for each batch. This will be helpful when inserting large data.
	TiDBBatchInsert = "tidb_batch_insert"

	// tidb_batch_delete is used to enable/disable auto-split delete data. If set this option on, delete executor will automatically
	// split data into multiple batches and use a single txn for each batch. This will be helpful when deleting large data.
	TiDBBatchDelete = "tidb_batch_delete"

	// tidb_batch_commit is used to enable/disable auto-split the transaction.
	// If set this option on, the transaction will be committed when it reaches stmt-count-limit and starts a new transaction.
	TiDBBatchCommit = "tidb_batch_commit"

	// tidb_dml_batch_size is used to split the insert/delete data into small batches.
	// It only takes effort when tidb_batch_insert/tidb_batch_delete is on.
	// Its default value is 20000. When the row size is large, 20k rows could be larger than 100MB.
	// User could change it to a smaller one to avoid breaking the transaction size limitation.
	TiDBDMLBatchSize = "tidb_dml_batch_size"

	// The following session variables controls the memory quota during query execution.
	// "tidb_mem_quota_query":				control the memory quota of a query.
	TIDBMemQuotaQuery      = "tidb_mem_quota_query" // Bytes.
	TiDBMemQuotaApplyCache = "tidb_mem_quota_apply_cache"
	// TODO: remove them below sometime, it should have only one Quota(TIDBMemQuotaQuery).
	TIDBMemQuotaHashJoin          = "tidb_mem_quota_hashjoin"          // Bytes.
	TIDBMemQuotaMergeJoin         = "tidb_mem_quota_mergejoin"         // Bytes.
	TIDBMemQuotaSort              = "tidb_mem_quota_sort"              // Bytes.
	TIDBMemQuotaTopn              = "tidb_mem_quota_topn"              // Bytes.
	TIDBMemQuotaIndexLookupReader = "tidb_mem_quota_indexlookupreader" // Bytes.
	TIDBMemQuotaIndexLookupJoin   = "tidb_mem_quota_indexlookupjoin"   // Bytes.

	// tidb_general_log is used to log every query in the server in info level.
	TiDBGeneralLog = "tidb_general_log"

	// tidb_pprof_sql_cpu is used to add label sql label to pprof result.
	TiDBPProfSQLCPU = "tidb_pprof_sql_cpu"

	// tidb_retry_limit is the maximum number of retries when committing a transaction.
	TiDBRetryLimit = "tidb_retry_limit"

	// tidb_disable_txn_auto_retry disables transaction auto retry.
	TiDBDisableTxnAutoRetry = "tidb_disable_txn_auto_retry"

	// tidb_enable_streaming enables TiDB to use streaming API for coprocessor requests.
	TiDBEnableStreaming = "tidb_enable_streaming"

	// tidb_enable_chunk_rpc enables TiDB to use Chunk format for coprocessor requests.
	TiDBEnableChunkRPC = "tidb_enable_chunk_rpc"

	// tidb_optimizer_selectivity_level is used to control the selectivity estimation level.
	TiDBOptimizerSelectivityLevel = "tidb_optimizer_selectivity_level"

	// tidb_txn_mode is used to control the transaction behavior.
	TiDBTxnMode = "tidb_txn_mode"

	// tidb_row_format_version is used to control tidb row format version current.
	TiDBRowFormatVersion = "tidb_row_format_version"

	// tidb_enable_table_partition is used to control table partition feature.
	// The valid value include auto/on/off:
	// on or auto: enable table partition if the partition type is implemented.
	// off: always disable table partition.
	TiDBEnableTablePartition = "tidb_enable_table_partition"

	// tidb_enable_list_partition is used to control list table partition feature.
	TiDBEnableListTablePartition = "tidb_enable_list_partition"

	// tidb_skip_isolation_level_check is used to control whether to return error when set unsupported transaction
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

	// tidb_slow_log_threshold is used to set the slow log threshold in the server.
	TiDBSlowLogThreshold = "tidb_slow_log_threshold"

	// tidb_record_plan_in_slow_log is used to log the plan of the slow query.
	TiDBRecordPlanInSlowLog = "tidb_record_plan_in_slow_log"

	// tidb_enable_slow_log enables TiDB to log slow queries.
	TiDBEnableSlowLog = "tidb_enable_slow_log"

	// tidb_query_log_max_len is used to set the max length of the query in the log.
	TiDBQueryLogMaxLen = "tidb_query_log_max_len"

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
)

// TiDB system variable names that both in session and global scope.
const (
	// tidb_build_stats_concurrency is used to speed up the ANALYZE statement, when a table has multiple indices,
	// those indices can be scanned concurrently, with the cost of higher system performance impact.
	TiDBBuildStatsConcurrency = "tidb_build_stats_concurrency"

	// tidb_distsql_scan_concurrency is used to set the concurrency of a distsql scan task.
	// A distsql scan task can be a table scan or a index scan, which may be distributed to many TiKV nodes.
	// Higher concurrency may reduce latency, but with the cost of higher memory usage and system performance impact.
	// If the query has a LIMIT clause, high concurrency makes the system do much more work than needed.
	// tidb_distsql_scan_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBDistSQLScanConcurrency = "tidb_distsql_scan_concurrency"

	// tidb_opt_insubquery_to_join_and_agg is used to enable/disable the optimizer rule of rewriting IN subquery.
	TiDBOptInSubqToJoinAndAgg = "tidb_opt_insubq_to_join_and_agg"

	// tidb_opt_prefer_range_scan is used to enable/disable the optimizer to always prefer range scan over table scan, ignoring their costs.
	TiDBOptPreferRangeScan = "tidb_opt_prefer_range_scan"

	// tidb_opt_correlation_threshold is a guard to enable row count estimation using column order correlation.
	TiDBOptCorrelationThreshold = "tidb_opt_correlation_threshold"

	// tidb_opt_correlation_exp_factor is an exponential factor to control heuristic approach when tidb_opt_correlation_threshold is not satisfied.
	TiDBOptCorrelationExpFactor = "tidb_opt_correlation_exp_factor"

	// tidb_opt_cpu_factor is the CPU cost of processing one expression for one row.
	TiDBOptCPUFactor = "tidb_opt_cpu_factor"
	// tidb_opt_copcpu_factor is the CPU cost of processing one expression for one row in coprocessor.
	TiDBOptCopCPUFactor = "tidb_opt_copcpu_factor"
	// tidb_opt_tiflash_concurrency_factor is concurrency number of tiflash computation.
	TiDBOptTiFlashConcurrencyFactor = "tidb_opt_tiflash_concurrency_factor"
	// tidb_opt_network_factor is the network cost of transferring 1 byte data.
	TiDBOptNetworkFactor = "tidb_opt_network_factor"
	// tidb_opt_scan_factor is the IO cost of scanning 1 byte data on TiKV.
	TiDBOptScanFactor = "tidb_opt_scan_factor"
	// tidb_opt_desc_factor is the IO cost of scanning 1 byte data on TiKV in desc order.
	TiDBOptDescScanFactor = "tidb_opt_desc_factor"
	// tidb_opt_seek_factor is the IO cost of seeking the start value in a range on TiKV or TiFlash.
	TiDBOptSeekFactor = "tidb_opt_seek_factor"
	// tidb_opt_memory_factor is the memory cost of storing one tuple.
	TiDBOptMemoryFactor = "tidb_opt_memory_factor"
	// tidb_opt_disk_factor is the IO cost of reading/writing one byte to temporary disk.
	TiDBOptDiskFactor = "tidb_opt_disk_factor"
	// tidb_opt_concurrency_factor is the CPU cost of additional one goroutine.
	TiDBOptConcurrencyFactor = "tidb_opt_concurrency_factor"

	// tidb_index_join_batch_size is used to set the batch size of a index lookup join.
	// The index lookup join fetches batches of data from outer executor and constructs ranges for inner executor.
	// This value controls how much of data in a batch to do the index join.
	// Large value may reduce the latency but consumes more system resource.
	TiDBIndexJoinBatchSize = "tidb_index_join_batch_size"

	// tidb_index_lookup_size is used for index lookup executor.
	// The index lookup executor first scan a batch of handles from a index, then use those handles to lookup the table
	// rows, this value controls how much of handles in a batch to do a lookup task.
	// Small value sends more RPCs to TiKV, consume more system resource.
	// Large value may do more work than needed if the query has a limit.
	TiDBIndexLookupSize = "tidb_index_lookup_size"

	// tidb_index_lookup_concurrency is used for index lookup executor.
	// A lookup task may have 'tidb_index_lookup_size' of handles at maximun, the handles may be distributed
	// in many TiKV nodes, we executes multiple concurrent index lookup tasks concurrently to reduce the time
	// waiting for a task to finish.
	// Set this value higher may reduce the latency but consumes more system resource.
	// tidb_index_lookup_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBIndexLookupConcurrency = "tidb_index_lookup_concurrency"

	// tidb_index_lookup_join_concurrency is used for index lookup join executor.
	// IndexLookUpJoin starts "tidb_index_lookup_join_concurrency" inner workers
	// to fetch inner rows and join the matched (outer, inner) row pairs.
	// tidb_index_lookup_join_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBIndexLookupJoinConcurrency = "tidb_index_lookup_join_concurrency"

	// tidb_index_serial_scan_concurrency is used for controlling the concurrency of index scan operation
	// when we need to keep the data output order the same as the order of index data.
	TiDBIndexSerialScanConcurrency = "tidb_index_serial_scan_concurrency"

	// TiDBMaxChunkSize is used to control the max chunk size during query execution.
	TiDBMaxChunkSize = "tidb_max_chunk_size"

	// TiDBAllowBatchCop means if we should send batch coprocessor to TiFlash. It can be set to 0, 1 and 2.
	// 0 means never use batch cop, 1 means use batch cop in case of aggregation and join, 2, means to force to send batch cop for any query.
	// The default value is 0
	TiDBAllowBatchCop = "tidb_allow_batch_cop"

	TiDBAllowMPPExecution = "tidb_allow_mpp"

	// TiDBInitChunkSize is used to control the init chunk size during query execution.
	TiDBInitChunkSize = "tidb_init_chunk_size"

	// tidb_enable_cascades_planner is used to control whether to enable the cascades planner.
	TiDBEnableCascadesPlanner = "tidb_enable_cascades_planner"

	// tidb_skip_utf8_check skips the UTF8 validate process, validate UTF8 has performance cost, if we can make sure
	// the input string values are valid, we can skip the check.
	TiDBSkipUTF8Check = "tidb_skip_utf8_check"

	// tidb_skip_ascii_check skips the ASCII validate process
	// old tidb may already have fields with invalid ASCII bytes
	// disable ASCII validate can guarantee a safe replication
	TiDBSkipASCIICheck = "tidb_skip_ascii_check"

	// tidb_hash_join_concurrency is used for hash join executor.
	// The hash join outer executor starts multiple concurrent join workers to probe the hash table.
	// tidb_hash_join_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashJoinConcurrency = "tidb_hash_join_concurrency"

	// tidb_projection_concurrency is used for projection operator.
	// This variable controls the worker number of projection operator.
	// tidb_projection_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBProjectionConcurrency = "tidb_projection_concurrency"

	// tidb_hashagg_partial_concurrency is used for hash agg executor.
	// The hash agg executor starts multiple concurrent partial workers to do partial aggregate works.
	// tidb_hashagg_partial_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashAggPartialConcurrency = "tidb_hashagg_partial_concurrency"

	// tidb_hashagg_final_concurrency is used for hash agg executor.
	// The hash agg executor starts multiple concurrent final workers to do final aggregate works.
	// tidb_hashagg_final_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBHashAggFinalConcurrency = "tidb_hashagg_final_concurrency"

	// tidb_window_concurrency is used for window parallel executor.
	// tidb_window_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBWindowConcurrency = "tidb_window_concurrency"

	// tidb_merge_join_concurrency is used for merge join parallel executor
	TiDBMergeJoinConcurrency = "tidb_merge_join_concurrency"

	// tidb_stream_agg_concurrency is used for stream aggregation parallel executor.
	// tidb_stream_agg_concurrency is deprecated, use tidb_executor_concurrency instead.
	TiDBStreamAggConcurrency = "tidb_streamagg_concurrency"

	// tidb_enable_parallel_apply is used for parallel apply.
	TiDBEnableParallelApply = "tidb_enable_parallel_apply"

	// tidb_backoff_lock_fast is used for tikv backoff base time in milliseconds.
	TiDBBackoffLockFast = "tidb_backoff_lock_fast"

	// tidb_backoff_weight is used to control the max back off time in TiDB.
	// The default maximum back off time is a small value.
	// BackOffWeight could multiply it to let the user adjust the maximum time for retrying.
	// Only positive integers can be accepted, which means that the maximum back off time can only grow.
	TiDBBackOffWeight = "tidb_backoff_weight"

	// tidb_ddl_reorg_worker_cnt defines the count of ddl reorg workers.
	TiDBDDLReorgWorkerCount = "tidb_ddl_reorg_worker_cnt"

	// tidb_ddl_reorg_batch_size defines the transaction batch size of ddl reorg workers.
	TiDBDDLReorgBatchSize = "tidb_ddl_reorg_batch_size"

	// tidb_ddl_error_count_limit defines the count of ddl error limit.
	TiDBDDLErrorCountLimit = "tidb_ddl_error_count_limit"

	// tidb_ddl_reorg_priority defines the operations priority of adding indices.
	// It can be: PRIORITY_LOW, PRIORITY_NORMAL, PRIORITY_HIGH
	TiDBDDLReorgPriority = "tidb_ddl_reorg_priority"

	// TiDBEnableChangeColumnType is used to control whether to enable the change column type.
	TiDBEnableChangeColumnType = "tidb_enable_change_column_type"

	// TiDBEnableChangeMultiSchema is used to control whether to enable the change multi schema.
	TiDBEnableChangeMultiSchema = "tidb_enable_change_multi_schema"

	// TiDBEnablePointGetCache is used to control whether to enable the point get cache for special scenario.
	TiDBEnablePointGetCache = "tidb_enable_point_get_cache"

	// TiDBEnableAlterPlacement is used to control whether to enable alter table partition.
	TiDBEnableAlterPlacement = "tidb_enable_alter_placement"

	// tidb_max_delta_schema_count defines the max length of deltaSchemaInfos.
	// deltaSchemaInfos is a queue that maintains the history of schema changes.
	TiDBMaxDeltaSchemaCount = "tidb_max_delta_schema_count"

	// tidb_scatter_region will scatter the regions for DDLs when it is ON.
	TiDBScatterRegion = "tidb_scatter_region"

	// TiDBWaitSplitRegionFinish defines the split region behaviour is sync or async.
	TiDBWaitSplitRegionFinish = "tidb_wait_split_region_finish"

	// TiDBWaitSplitRegionTimeout uses to set the split and scatter region back off time.
	TiDBWaitSplitRegionTimeout = "tidb_wait_split_region_timeout"

	// tidb_force_priority defines the operations priority of all statements.
	// It can be "NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"
	TiDBForcePriority = "tidb_force_priority"

	// tidb_enable_radix_join indicates to use radix hash join algorithm to execute
	// HashJoin.
	TiDBEnableRadixJoin = "tidb_enable_radix_join"

	// tidb_constraint_check_in_place indicates to check the constraint when the SQL executing.
	// It could hurt the performance of bulking insert when it is ON.
	TiDBConstraintCheckInPlace = "tidb_constraint_check_in_place"

	// tidb_enable_window_function is used to control whether to enable the window function.
	TiDBEnableWindowFunction = "tidb_enable_window_function"

	// tidb_enable_strict_double_type_check is used to control table field double type syntax check.
	TiDBEnableStrictDoubleTypeCheck = "tidb_enable_strict_double_type_check"

	// tidb_enable_vectorized_expression is used to control whether to enable the vectorized expression evaluation.
	TiDBEnableVectorizedExpression = "tidb_enable_vectorized_expression"

	// TIDBOptJoinReorderThreshold defines the threshold less than which
	// we'll choose a rather time consuming algorithm to calculate the join order.
	TiDBOptJoinReorderThreshold = "tidb_opt_join_reorder_threshold"

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
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

	// DefExecutorConcurrency is used for controlling the concurrency of all types of executors.
	TiDBExecutorConcurrency = "tidb_executor_concurrency"

	// TiDBEnableClusteredIndex indicates if clustered index feature is enabled.
	TiDBEnableClusteredIndex = "tidb_enable_clustered_index"

	// TiDBPartitionPruneMode indicates the partition prune mode used.
	TiDBPartitionPruneMode = "tidb_partition_prune_mode"

	// TiDBSlowLogMasking is deprecated and a alias of TiDBRedactLog.
	// Deprecated: use TiDBRedactLog instead.
	TiDBSlowLogMasking = "tidb_slow_log_masking"

	// TiDBRedactLog indicates that whether redact log.
	TiDBRedactLog = "tidb_redact_log"

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

	// TiDBAnalyzeVersion indicates the how tidb collects the analyzed statistics and how use to it.
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

	// TiDBEnableDynamicPrivileges enables MySQL 8.0 compatible dynamic privileges (experimental).
	TiDBEnableDynamicPrivileges = "tidb_enable_dynamic_privileges"
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
	// TiDBEnableEnhancedSecurity restricts SUPER users from certain operations.
	TiDBEnableEnhancedSecurity = "tidb_enable_enhanced_security"
)

// Default TiDB system variable values.
const (
	DefHostname                        = "localhost"
	DefIndexLookupConcurrency          = ConcurrencyUnset
	DefIndexLookupJoinConcurrency      = ConcurrencyUnset
	DefIndexSerialScanConcurrency      = 1
	DefIndexJoinBatchSize              = 25000
	DefIndexLookupSize                 = 20000
	DefDistSQLScanConcurrency          = 15
	DefBuildStatsConcurrency           = 4
	DefAutoAnalyzeRatio                = 0.5
	DefAutoAnalyzeStartTime            = "00:00 +0000"
	DefAutoAnalyzeEndTime              = "23:59 +0000"
	DefAutoIncrementIncrement          = 1
	DefAutoIncrementOffset             = 1
	DefChecksumTableConcurrency        = 4
	DefSkipUTF8Check                   = false
	DefSkipASCIICheck                  = false
	DefOptAggPushDown                  = false
	DefOptBCJ                          = false
	DefOptWriteRowID                   = false
	DefOptCorrelationThreshold         = 0.9
	DefOptCorrelationExpFactor         = 1
	DefOptCPUFactor                    = 3.0
	DefOptCopCPUFactor                 = 3.0
	DefOptTiFlashConcurrencyFactor     = 24.0
	DefOptNetworkFactor                = 1.0
	DefOptScanFactor                   = 1.5
	DefOptDescScanFactor               = 3.0
	DefOptSeekFactor                   = 20.0
	DefOptMemoryFactor                 = 0.001
	DefOptDiskFactor                   = 1.5
	DefOptConcurrencyFactor            = 3.0
	DefOptInSubqToJoinAndAgg           = true
	DefOptPreferRangeScan              = false
	DefBatchInsert                     = false
	DefBatchDelete                     = false
	DefBatchCommit                     = false
	DefCurretTS                        = 0
	DefInitChunkSize                   = 32
	DefMaxChunkSize                    = 1024
	DefDMLBatchSize                    = 0
	DefMaxPreparedStmtCount            = -1
	DefWaitTimeout                     = 0
	DefTiDBMemQuotaApplyCache          = 32 << 20 // 32MB.
	DefTiDBMemQuotaHashJoin            = 32 << 30 // 32GB.
	DefTiDBMemQuotaMergeJoin           = 32 << 30 // 32GB.
	DefTiDBMemQuotaSort                = 32 << 30 // 32GB.
	DefTiDBMemQuotaTopn                = 32 << 30 // 32GB.
	DefTiDBMemQuotaIndexLookupReader   = 32 << 30 // 32GB.
	DefTiDBMemQuotaIndexLookupJoin     = 32 << 30 // 32GB.
	DefTiDBMemQuotaDistSQL             = 32 << 30 // 32GB.
	DefTiDBGeneralLog                  = false
	DefTiDBPProfSQLCPU                 = 0
	DefTiDBRetryLimit                  = 10
	DefTiDBDisableTxnAutoRetry         = true
	DefTiDBConstraintCheckInPlace      = false
	DefTiDBHashJoinConcurrency         = ConcurrencyUnset
	DefTiDBProjectionConcurrency       = ConcurrencyUnset
	DefBroadcastJoinThresholdSize      = 100 * 1024 * 1024
	DefBroadcastJoinThresholdCount     = 10 * 1024
	DefTiDBOptimizerSelectivityLevel   = 0
	DefTiDBAllowBatchCop               = 1
	DefTiDBAllowMPPExecution           = true
	DefTiDBTxnMode                     = ""
	DefTiDBRowFormatV1                 = 1
	DefTiDBRowFormatV2                 = 2
	DefTiDBDDLReorgWorkerCount         = 4
	DefTiDBDDLReorgBatchSize           = 256
	DefTiDBDDLErrorCountLimit          = 512
	DefTiDBMaxDeltaSchemaCount         = 1024
	DefTiDBChangeColumnType            = false
	DefTiDBChangeMultiSchema           = false
	DefTiDBPointGetCache               = false
	DefTiDBEnableAlterPlacement        = false
	DefTiDBHashAggPartialConcurrency   = ConcurrencyUnset
	DefTiDBHashAggFinalConcurrency     = ConcurrencyUnset
	DefTiDBWindowConcurrency           = ConcurrencyUnset
	DefTiDBMergeJoinConcurrency        = 1 // disable optimization by default
	DefTiDBStreamAggConcurrency        = 1
	DefTiDBForcePriority               = mysql.NoPriority
	DefTiDBUseRadixJoin                = false
	DefEnableWindowFunction            = true
	DefEnableStrictDoubleTypeCheck     = true
	DefEnableVectorizedExpression      = true
	DefTiDBOptJoinReorderThreshold     = 0
	DefTiDBDDLSlowOprThreshold         = 300
	DefTiDBUseFastAnalyze              = false
	DefTiDBSkipIsolationLevelCheck     = false
	DefTiDBExpensiveQueryTimeThreshold = 60 // 60s
	DefTiDBScatterRegion               = false
	DefTiDBWaitSplitRegionFinish       = true
	DefWaitSplitRegionTimeout          = 300 // 300s
	DefTiDBEnableNoopFuncs             = false
	DefTiDBAllowRemoveAutoInc          = false
	DefTiDBUsePlanBaselines            = true
	DefTiDBEvolvePlanBaselines         = false
	DefTiDBEvolvePlanTaskMaxTime       = 600 // 600s
	DefTiDBEvolvePlanTaskStartTime     = "00:00 +0000"
	DefTiDBEvolvePlanTaskEndTime       = "23:59 +0000"
	DefInnodbLockWaitTimeout           = 50 // 50s
	DefTiDBStoreLimit                  = 0
	DefTiDBMetricSchemaStep            = 60 // 60s
	DefTiDBMetricSchemaRangeDuration   = 60 // 60s
	DefTiDBFoundInPlanCache            = false
	DefTiDBFoundInBinding              = false
	DefTiDBEnableCollectExecutionInfo  = true
	DefTiDBAllowAutoRandExplicitInsert = false
	DefTiDBEnableClusteredIndex        = ClusteredIndexDefModeIntOnly
	DefTiDBRedactLog                   = false
	DefTiDBShardAllocateStep           = math.MaxInt64
	DefTiDBEnableTelemetry             = true
	DefTiDBEnableParallelApply         = false
	DefTiDBEnableAmendPessimisticTxn   = false
	DefTiDBPartitionPruneMode          = "static"
	DefTiDBEnableRateLimitAction       = true
	DefTiDBEnableAsyncCommit           = false
	DefTiDBEnable1PC                   = false
	DefTiDBGuaranteeLinearizability    = true
	DefTiDBAnalyzeVersion              = 1
	DefTiDBEnableIndexMergeJoin        = false
	DefTiDBTrackAggregateMemoryUsage   = true
	DefTiDBEnableExchangePartition     = false
	DefCTEMaxRecursionDepth            = 1000
)

// Process global variables.
var (
	ProcessGeneralLog            = atomic.NewBool(false)
	EnablePProfSQLCPU            = atomic.NewBool(false)
	ddlReorgWorkerCounter  int32 = DefTiDBDDLReorgWorkerCount
	maxDDLReorgWorkerCount int32 = 128
	ddlReorgBatchSize      int32 = DefTiDBDDLReorgBatchSize
	ddlErrorCountlimit     int64 = DefTiDBDDLErrorCountLimit
	ddlReorgRowFormat      int64 = DefTiDBRowFormatV2
	maxDeltaSchemaCount    int64 = DefTiDBMaxDeltaSchemaCount
	// Export for testing.
	MaxDDLReorgBatchSize int32 = 10240
	MinDDLReorgBatchSize int32 = 32
	// DDLSlowOprThreshold is the threshold for ddl slow operations, uint is millisecond.
	DDLSlowOprThreshold            uint32 = DefTiDBDDLSlowOprThreshold
	ForcePriority                         = int32(DefTiDBForcePriority)
	ServerHostname, _                     = os.Hostname()
	MaxOfMaxAllowedPacket          uint64 = 1073741824
	ExpensiveQueryTimeThreshold    uint64 = DefTiDBExpensiveQueryTimeThreshold
	MinExpensiveQueryTimeThreshold uint64 = 10 // 10s
	CapturePlanBaseline                   = serverGlobalVariable{globalVal: Off}
	DefExecutorConcurrency                = 5
	MemoryUsageAlarmRatio                 = atomic.NewFloat64(config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio)
)

// FeatureSwitchVariables is used to filter result of show variables, these switches should be turn blind to users.
var FeatureSwitchVariables = []string{
	TiDBEnableChangeColumnType,
	TiDBEnablePointGetCache,
	TiDBEnableAlterPlacement,
	TiDBEnableChangeMultiSchema,
	TiDBEnableAsyncCommit,
	TiDBEnable1PC,
	TiDBGuaranteeLinearizability,
	TiDBTrackAggregateMemoryUsage,
	TiDBAnalyzeVersion,
	TiDBPartitionPruneMode,
	TiDBEnableExtendedStats,
	TiDBEnableIndexMergeJoin,
}

// FilterImplicitFeatureSwitch is used to filter result of show variables, these switches should be turn blind to users.
func FilterImplicitFeatureSwitch(sysVar *SysVar) bool {
	for _, one := range FeatureSwitchVariables {
		if one == sysVar.Name {
			return true
		}
	}
	return false
}
