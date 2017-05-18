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

/*
	Steps to add a new TiDB specific system variable:

	1. Add a new variable name with comment in this file.
	2. Add the default value of the new variable in this file.
	3. Add SysVar instance in 'defaultSysVars' slice with the default value.
	4. Add a field in `SessionVars`.
	5. Update the `NewSessionVars` function to set the field to its default value.
	6. Update the `varsutil.SetSessionSystemVar` function to use the new value when SET statement is executed.
	7. If it is a global variable, add it in `tidb.loadCommonGlobalVarsSQL`.
	8. Use this variable to control the behavior in code.
*/

// TiDB system variable names.
const (
	/* Session only */

	// tidb_snapshot is used for reading history data, the default value is empty string.
	// When the value is set to a datetime string like '2017-11-11 20:20:20', the session reads history data of that time.
	TiDBSnapshot = "tidb_snapshot"

	// tidb_skip_constraint_check is used for loading data from a dump file, to speed up the loading process.
	// When the value is set to true, unique index constraint is not checked.
	TiDBSkipConstraintCheck = "tidb_skip_constraint_check"

	// tidb_opt_agg_push_down is used to endable/disable the optimizer rule of aggregation push down.
	TiDBOptAggPushDown = "tidb_opt_agg_push_down"

	// tidb_opt_insubquery_unfold is used to enable/disable the optimizer rule of in subquery unfold.
	TiDBOptInSubqUnFolding = "tidb_opt_insubquery_unfold"

	// tidb_build_stats_concurrency is used to speed up the ANALYZE statement, when a table has multiple indices,
	// those indices can be scanned concurrently, with the cost of higher system performance impact.
	TiDBBuildStatsConcurrency = "tidb_build_stats_concurrency"

	/* Session and global */

	// tidb_distsql_scan_concurrency is used to set the concurrency of a distsql scan task.
	// A distsql scan task can be a table scan or a index scan, which may be distributed to many TiKV nodes.
	// Higher concurrency may reduce latency, but with the cost of higher memory usage and system performance impact.
	// If the query has a LIMIT clause, high concurrency makes the system do much more work than needed.
	TiDBDistSQLScanConcurrency = "tidb_distsql_scan_concurrency"

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
	TiDBIndexLookupConcurrency = "tidb_index_lookup_concurrency"

	// tidb_index_serial_scan_concurrency is used for controlling the concurrency of index scan operation
	// when we need to keep the data output order the same as the order of index data.
	TiDBIndexSerialScanConcurrency = "tidb_index_serial_scan_concurrency"

	// tidb_skip_ddl_wait skips the wait time of two lease after executing CREATE TABLE statement.
	// When we have multiple TiDB servers in a cluster, the newly created table may not be available on all TiDB server
	// until two lease time later, set this value to true will reduce the time to create a table, with the risk that
	// other TiDB servers may fail to use the newly created table in a small time window.
	TiDBSkipDDLWait = "tidb_skip_ddl_wait"

	// tidb_skip_utf8_check skips the UTF8 validate process, validate UTF8 has performance cost, if we can make sure
	// the input string values are valid, we can skip the check.
	TiDBSkipUTF8Check = "tidb_skip_utf8_check"

	// tidb_batch_insert is used to enable/disable auto-split insert data. If set this option on, insert executor will automatically
	// insert data into multiple batches and use a single txn for each batch. This will be helpful when inserting large data.
	TiDBBatchInsert = "tidb_batch_insert"

	// tidb_max_row_count_for_inlj is used when do index nested loop join.
	// It controls the max row count of outer table when do index nested loop join without hint.
	// After the row count of the inner table is accurate, this variable will be removed.
	TiDBMaxRowCountForINLJ = "tidb_max_row_count_for_inlj"
)

// Default TiDB system variable values.
const (
	DefIndexLookupConcurrency     = 4
	DefIndexSerialScanConcurrency = 1
	DefIndexLookupSize            = 20000
	DefDistSQLScanConcurrency     = 10
	DefBuildStatsConcurrency      = 4
	DefMaxRowCountForINLJ         = 128
	DefSkipDDLWait                = false
	DefSkipUTF8Check              = false
	DefOptAggPushDown             = true
	DefOptInSubqUnfolding         = false
	DefBatchInsert                = false
)
