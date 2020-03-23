// Copyright 2019 PingCAP, Inc.
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

package infoschema

// MetricTableMap records the metric table definition, export for test.
// TODO: read from system table.
var MetricTableMap = map[string]MetricTableDef{
	"tidb_query_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tidb_server_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,sql_type,instance))`,
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.90,
		Comment:  "The quantile of TiDB query durations(second)",
	},
	"tidb_qps": {
		PromQL:  `sum(rate(tidb_server_query_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (result,type,instance)`,
		Labels:  []string{"instance", "type", "result"},
		Comment: "TiDB query processing numbers per second",
	},
	"tidb_qps_ideal": {
		PromQL: `sum(tidb_server_connections) * sum(rate(tidb_server_handle_query_duration_seconds_count[$RANGE_DURATION])) / sum(rate(tidb_server_handle_query_duration_seconds_sum[$RANGE_DURATION]))`,
	},
	"tidb_ops_statement": {
		PromQL:  `sum(rate(tidb_executor_statement_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "TiDB statement statistics",
	},
	"tidb_failed_query_opm": {
		PromQL:  `sum(increase(tidb_server_execute_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "TiDB failed query opm",
	},
	"tidb_slow_query_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_process_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.90,
		Comment:  "The quantile of TiDB slow query statistics with slow query time(second)",
	},
	"tidb_slow_query_cop_process_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_cop_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.90,
		Comment:  "The quantile of TiDB slow query statistics with slow query total cop process time(second)",
	},
	"tidb_slow_query_cop_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_server_slow_query_wait_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.90,
		Comment:  "The quantile of TiDB slow query statistics with slow query total cop wait time(second)",
	},
	"tidb_ops_internal": {
		PromQL:  "sum(rate(tidb_session_restricted_sql_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "TiDB internal SQL is used by TiDB itself.",
	},
	"tidb_process_mem_usage": {
		PromQL:  "process_resident_memory_bytes{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "process rss memory usage",
	},
	"go_heap_mem_usage": {
		PromQL:  "go_memstats_heap_alloc_bytes{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "TiDB heap memory size in use",
	},
	"process_cpu_usage": {
		PromQL: "rate(process_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels: []string{"instance", "job"},
	},
	"tidb_connection_count": {
		PromQL:  "tidb_server_connections{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
		Comment: "TiDB current connection counts",
	},
	"node_process_open_fd_count": {
		PromQL:  "process_open_fds{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Process opened file descriptors count",
	},
	"goroutines_count": {
		PromQL:  " go_goroutines{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Process current goroutines count)",
	},
	"go_gc_duration": {
		PromQL:  "rate(go_gc_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "job"},
		Comment: "Go garbage collection time cost(second)",
	},
	"go_threads": {
		PromQL:  "go_threads{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Total threads TiDB/PD process created currently",
	},
	"go_gc_count": {
		PromQL:  " rate(go_gc_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "job"},
		Comment: "The Go garbage collection counts per second",
	},
	"go_gc_cpu_usage": {
		PromQL:  "go_memstats_gc_cpu_fraction{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "The fraction of TiDB/PD available CPU time used by the GC since the program started.",
	},
	"tidb_event_opm": {
		PromQL:  "increase(tidb_server_event_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "type"},
		Comment: "TiDB Server critical events total, including start/close/shutdown/hang etc",
	},
	"tidb_keep_alive_opm": {
		PromQL:  "sum(increase(tidb_monitor_keep_alive_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "TiDB instance monitor average keep alive times",
	},
	"tidb_prepared_statement_count": {
		PromQL:  "tidb_server_prepared_stmts{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
		Comment: "TiDB prepare statements count",
	},
	"tidb_time_jump_back_ops": {
		PromQL:  "sum(increase(tidb_monitor_time_jump_back_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "TiDB monitor time jump back count",
	},
	"tidb_panic_count": {
		Comment: "TiDB instance panic count",
		PromQL:  "increase(tidb_server_panic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance"},
	},
	"tidb_panic_count_total_count": {
		Comment: "The total count of TiDB instance panic",
		PromQL:  "sum(increase(tidb_server_panic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"tidb_binlog_error_count": {
		Comment: "TiDB write binlog error, skip binlog count",
		PromQL:  "tidb_server_critical_error_total{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
	},
	"tidb_binlog_error_total_count": {
		PromQL:  "sum(increase(tidb_server_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB write binlog error and skip binlog",
	},
	"tidb_get_token_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_server_get_token_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  " The quantile of Duration (us) for getting token, it should be small until concurrency limit is reached(second)",
	},
	"tidb_handshake_error_opm": {
		PromQL:  "sum(increase(tidb_server_handshake_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The OPM of TiDB processing handshake error",
	},
	"tidb_handshake_error_total_count": {
		PromQL:  "sum(increase(tidb_server_handshake_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB processing handshake error",
	},

	"tidb_transaction_ops": {
		PromQL:  "sum(rate(tidb_session_transaction_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "TiDB transaction processing counts by type and source. Internal means TiDB inner transaction calls",
	},
	"tidb_transaction_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_transaction_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,sql_type,instance))",
		Labels:   []string{"instance", "type", "sql_type"},
		Quantile: 0.95,
		Comment:  "The quantile of transaction execution durations, including retry(second)",
	},
	"tidb_transaction_retry_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_retry_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Comment:  "The quantile of TiDB transaction retry num",
		Quantile: 0.95,
	},
	"tidb_transaction_statement_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_transaction_statement_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,sql_type))",
		Labels:   []string{"instance", "sql_type"},
		Comment:  "The quantile of TiDB statements numbers within one transaction. Internal means TiDB inner transaction",
		Quantile: 0.95,
	},
	"tidb_transaction_retry_error_ops": {
		PromQL:  "sum(rate(tidb_session_retry_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "Error numbers of transaction retry",
	},
	"tidb_transaction_retry_error_total_count": {
		PromQL:  "sum(increase(tidb_session_retry_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "The total count of transaction retry",
	},
	"tidb_transaction_local_latch_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_local_latch_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Comment:  "The quantile of TiDB transaction latch wait time on key value storage(second)",
		Quantile: 0.95,
	},
	"tidb_parse_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_parse_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,sql_type,instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The quantile time cost of parsing SQL to AST(second)",
	},
	"tidb_compile_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_compile_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, sql_type,instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The quantile time cost of building the query plan(second)",
	},
	"tidb_execute_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_session_execute_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, sql_type, instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The quantile time cost of executing the SQL which does not include the time to get the results of the query(second)",
	},
	"tidb_expensive_executors_ops": {
		Comment: "TiDB executors using more cpu and memory resources",
		PromQL:  "sum(rate(tidb_executor_expensive_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_query_using_plan_cache_ops": {
		PromQL:  "sum(rate(tidb_server_plan_cache_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "TiDB plan cache hit ops",
	},
	"tidb_distsql_execution_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "The quantile durations of distsql execution(second)",
	},
	"tidb_distsql_qps": {
		PromQL:  "sum(rate(tidb_distsql_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "distsql query handling durations per second",
	},
	"tidb_distsql_partial_qps": {
		PromQL:  "sum(rate(tidb_distsql_scan_keys_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
		Comment: "the numebr of distsql partial scan numbers",
	},
	"tidb_distsql_scan_key_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_scan_keys_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile numebr of distsql scan numbers",
	},
	"tidb_distsql_partial_scan_key_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_scan_keys_partial_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile numebr of distsql partial scan key numbers",
	},
	"tidb_distsql_partial_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_distsql_partial_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile of distsql partial numbers per query",
	},
	"tidb_cop_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_cop_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile of kv storage coprocessor processing durations",
	},
	"tidb_kv_backoff_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_backoff_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "The quantile of kv backoff time durations(second)",
	},
	"tidb_kv_backoff_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_backoff_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "kv storage backoff times",
	},
	"tidb_kv_region_error_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_region_err_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "kv region error times",
	},
	"tidb_kv_region_error_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_region_err_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of kv region error",
	},
	"tidb_lock_resolver_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_lock_resolver_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "lock resolve times",
	},
	"tidb_lock_resolver_total_num": {
		PromQL:  "sum(increase(tidb_tikvclient_lock_resolver_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "The total number of lock resolve",
	},
	"tidb_lock_cleanup_fail_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_lock_cleanup_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "lock cleanup failed ops",
	},
	"tidb_load_safepoint_fail_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "safe point update ops",
	},
	"tidb_kv_request_ops": {
		Comment: "kv request total by instance and command type",
		PromQL:  "sum(rate(tidb_tikvclient_request_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_kv_request_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_request_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,store,instance))",
		Labels:   []string{"instance", "type", "store"},
		Quantile: 0.95,
		Comment:  "The quantile of kv requests durations by store",
	},
	"tidb_kv_txn_ops": {
		Comment: "TiDB total kv transaction counts",
		PromQL:  "sum(rate(tidb_tikvclient_txn_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"tidb_kv_write_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_write_kv_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 1,
		Comment:  "The quantile of kv write count per transaction execution",
	},
	"tidb_kv_write_size": {
		Comment:  "The quantile of kv write size per transaction execution",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_write_size_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 1,
	},
	"tidb_txn_region_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_txn_regions_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Comment:  "The quantile of regions transaction operates on count",
		Quantile: 0.95,
	},
	"tidb_load_safepoint_ops": {
		PromQL:  "sum(rate(tidb_tikvclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The OPS of load safe point loading",
	},
	"tidb_load_safepoint_total_num": {
		PromQL:  "sum(increase(tidb_tikvclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of safe point loading",
	},
	"tidb_kv_snapshot_ops": {
		Comment: "using snapshots total",
		PromQL:  "sum(rate(tidb_tikvclient_snapshot_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"pd_client_cmd_ops": {
		PromQL:  "sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "pd client command ops",
	},
	"pd_client_cmd_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "The quantile of pd client command durations",
	},
	"pd_cmd_fail_ops": {
		PromQL:  "sum(rate(pd_client_cmd_handle_failed_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "pd client command fail count",
	},
	"pd_cmd_fail_total_count": {
		PromQL:  "sum(increase(pd_client_cmd_handle_failed_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of pd client command fail",
	},
	"pd_handle_request_ops": {
		PromQL:  "sum(rate(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "pd handle request operation per second",
	},
	"pd_handle_request_duration": {
		Comment:  "The quantile of pd handle request duration(second)",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.999,
	},
	"pd_tso_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(pd_client_cmd_handle_cmds_duration_seconds_bucket{type=\"wait\"}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.999,
		Comment:  "The quantile duration of a client starting to wait for the TS until received the TS result.",
	},
	"pd_tso_rpc_duration": {
		Comment:  "The quantile duration of a client sending TSO request until received the response.",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(pd_client_request_handle_requests_duration_seconds_bucket{type=\"tso\"}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.999,
	},
	"pd_start_tso_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_pdclient_ts_future_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.999,
		Comment:  "The quantile duration of the waiting time for getting the start timestamp oracle",
	},
	"tidb_load_schema_duration": {
		Comment:  "The quantile of TiDB loading schema time durations by instance",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_domain_load_schema_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"tidb_load_schema_ops": {
		Comment: "TiDB loading schema times including both failed and successful ones",
		PromQL:  "sum(rate(tidb_domain_load_schema_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_schema_lease_error_opm": {
		Comment: "TiDB schema lease error counts",
		PromQL:  "sum(increase(tidb_session_schema_lease_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"tidb_schema_lease_error_total_count": {
		Comment: "The total count of TiDB schema lease error",
		PromQL:  "sum(increase(tidb_session_schema_lease_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"tidb_load_privilege_ops": {
		Comment: "TiDB load privilege counts",
		PromQL:  "sum(rate(tidb_domain_load_privilege_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_ddl_duration": {
		Comment:  "The quantile of TiDB DDL duration statistics",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_handle_job_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"tidb_ddl_batch_add_index_duration": {
		Comment:  "The quantile of TiDB batch add index durations by histogram buckets",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_batch_add_idx_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"tidb_ddl_add_index_speed": {
		Comment: "TiDB add index speed",
		PromQL:  "sum(rate(tidb_ddl_add_index_total[$RANGE_DURATION])) by (type)",
	},
	"tidb_ddl_waiting_jobs_num": {
		Comment: "TiDB ddl request in queue",
		PromQL:  "tidb_ddl_waiting_jobs{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "type"},
	},
	"tidb_ddl_meta_opm": {
		Comment: "TiDB different ddl worker numbers",
		PromQL:  "increase(tidb_ddl_worker_operation_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "type"},
	},
	"tidb_ddl_worker_duration": {
		Comment:  "The quantile of TiDB ddl worker duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(increase(tidb_ddl_worker_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, action, result,instance))",
		Labels:   []string{"instance", "type", "result", "action"},
		Quantile: 0.95,
	},
	"tidb_ddl_deploy_syncer_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_deploy_syncer_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
		Comment:  "The quantile of TiDB ddl schema syncer statistics, including init, start, watch, clear function call time cost",
	},
	"tidb_owner_handle_syncer_duration": {
		Comment:  "The quantile of TiDB ddl owner time operations on etcd duration statistics ",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_owner_handle_syncer_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"tidb_ddl_update_self_version_duration": {
		Comment:  "The quantile of TiDB schema syncer version update time duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_ddl_update_self_ver_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, result,instance))",
		Labels:   []string{"instance", "result"},
		Quantile: 0.95,
	},
	"tidb_ddl_opm": {
		Comment: "The quantile of executed DDL jobs per minute",
		PromQL:  "sum(rate(tidb_ddl_handle_job_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_statistics_auto_analyze_duration": {
		Comment:  "The quantile of TiDB auto analyze time durations within 95 percent histogram buckets",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_statistics_auto_analyze_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"tidb_statistics_auto_analyze_ops": {
		Comment: "TiDB auto analyze query per second",
		PromQL:  "sum(rate(tidb_statistics_auto_analyze_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_statistics_stats_inaccuracy_rate": {
		Comment:  "The quantile of TiDB statistics inaccurate rate",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_statistics_stats_inaccuracy_rate_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"tidb_statistics_pseudo_estimation_ops": {
		Comment: "TiDB optimizer using pseudo estimation counts",
		PromQL:  "sum(rate(tidb_statistics_pseudo_estimation_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"tidb_statistics_pseudo_estimation_total_count": {
		Comment: "The total count of TiDB optimizer using pseudo estimation",
		PromQL:  "sum(increase(tidb_statistics_pseudo_estimation_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"tidb_statistics_dump_feedback_ops": {
		Comment: "TiDB dumping statistics back to kv storage times",
		PromQL:  "sum(rate(tidb_statistics_dump_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_statistics_dump_feedback_total_count": {
		Comment: "The total count of operations that TiDB dumping statistics back to kv storage",
		PromQL:  "sum(increase(tidb_statistics_dump_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_statistics_store_query_feedback_qps": {
		Comment: "TiDB store quering feedback counts",
		PromQL:  "sum(rate(tidb_statistics_store_query_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) ",
		Labels:  []string{"instance", "type"},
	},
	"tidb_statistics_store_query_feedback_total_count": {
		Comment: "The total count of TiDB store quering feedback",
		PromQL:  "sum(increase(tidb_statistics_store_query_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) ",
		Labels:  []string{"instance", "type"},
	},
	"tidb_statistics_significant_feedback": {
		Comment: "Counter of query feedback whose actual count is much different than calculated by current statistics",
		PromQL:  "sum(rate(tidb_statistics_high_error_rate_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"tidb_statistics_update_stats_ops": {
		Comment: "TiDB updating statistics using feed back counts",
		PromQL:  "sum(rate(tidb_statistics_update_stats_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_statistics_update_stats_total_count": {
		Comment: "The total count of TiDB updating statistics using feed back",
		PromQL:  "sum(increase(tidb_statistics_update_stats_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_statistics_fast_analyze_status": {
		Comment:  "The quantile of TiDB fast analyze statistics ",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_statistics_fast_analyze_status_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"tidb_new_etcd_session_duration": {
		Comment:  "The quantile of TiDB new session durations for new etcd sessions",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_owner_new_session_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,result, instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"tidb_owner_watcher_ops": {
		Comment: "TiDB owner watcher counts",
		PromQL:  "sum(rate(tidb_owner_watch_owner_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, result, instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"tidb_auto_id_qps": {
		Comment: "TiDB auto id requests per second including single table/global auto id processing and single table auto id rebase processing",
		PromQL:  "sum(rate(tidb_autoid_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"tidb_auto_id_request_duration": {
		Comment:  "The quantile of TiDB auto id requests durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_autoid_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"tidb_region_cache_ops": {
		Comment: "TiDB region cache operations count",
		PromQL:  "sum(rate(tidb_tikvclient_region_cache_operations_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,result,instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"tidb_meta_operation_duration": {
		Comment:  "The quantile of TiDB meta operation durations including get/set schema and ddl jobs",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_meta_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"tidb_gc_worker_action_opm": {
		Comment: "kv storage garbage collection counts by type",
		PromQL:  "sum(increase(tidb_tikvclient_gc_worker_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_duration": {
		Comment:  "The quantile of kv storage garbage collection time durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_gc_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"tidb_gc_config": {
		Comment: "kv storage garbage collection config including gc_life_time and gc_run_interval",
		PromQL:  "tidb_tikvclient_gc_config{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_fail_opm": {
		Comment: "kv storage garbage collection failing counts",
		PromQL:  "sum(increase(tidb_tikvclient_gc_failure{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_delete_range_fail_opm": {
		Comment: "kv storage unsafe destroy range failed counts",
		PromQL:  "sum(increase(tidb_tikvclient_gc_unsafe_destroy_range_failures{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_too_many_locks_opm": {
		Comment: "kv storage region garbage collection clean too many locks count",
		PromQL:  "sum(increase(tidb_tikvclient_gc_region_too_many_locks[$RANGE_DURATION]))",
	},
	"tidb_gc_action_result_opm": {
		Comment: "kv storage garbage collection results including failed and successful ones",
		PromQL:  "sum(increase(tidb_tikvclient_gc_action_result{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"tidb_gc_delete_range_task_status": {
		Comment: "kv storage delete range task execution status by type",
		PromQL:  "sum(tidb_tikvclient_range_task_stats{$LABEL_CONDITIONS}) by (type, result,instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"tidb_gc_push_task_duration": {
		Comment:  "The quantile of kv storage range worker processing one task duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_range_task_push_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"tidb_batch_client_pending_req_count": {
		Comment: "kv storage batch requests in queue",
		PromQL:  "sum(tidb_tikvclient_pending_batch_requests{$LABEL_CONDITIONS}) by (store,instance)",
		Labels:  []string{"instance", "store"},
	},
	"tidb_batch_client_wait_duration": {
		Comment:  "The quantile of kv storage batch processing durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_batch_wait_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"tidb_batch_client_unavailable_duration": {
		Comment:  "The quantile of kv storage batch processing unvailable durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(tidb_tikvclient_batch_client_unavailable_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"uptime": {
		PromQL:  "(time() - process_start_time_seconds{$LABEL_CONDITIONS})",
		Labels:  []string{"instance", "job"},
		Comment: "TiDB uptime since last restart(second)",
	},
	"up": {
		PromQL:  `up{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "job"},
		Comment: "whether the instance is up. 1 is up, 0 is down(off-line)",
	},
	"pd_role": {
		PromQL:  `delta(pd_tso_events{type="save"}[$RANGE_DURATION]) > bool 0`,
		Labels:  []string{"instance"},
		Comment: "It indicates whether the current PD is the leader or a follower.",
	},
	"normal_stores": {
		PromQL:  `sum(pd_cluster_status{type="store_up_count"}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of healthy stores",
	},
	"abnormal_stores": {
		PromQL: `sum(pd_cluster_status{ type=~"store_disconnected_count|store_unhealth_count|store_low_space_count|store_down_count|store_offline_count|store_tombstone_count"})`,
		Labels: []string{"instance", "type"},
	},
	"pd_scheduler_config": {
		PromQL: `pd_config_status{$LABEL_CONDITIONS}`,
		Labels: []string{"type"},
	},
	"pd_region_label_isolation_level": {
		PromQL: `pd_regions_label_level{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"pd_label_distribution": {
		PromQL: `pd_cluster_placement_status{$LABEL_CONDITIONS}`,
		Labels: []string{"name"},
	},
	"pd_cluster_status": {
		PromQL: `sum(pd_cluster_status{$LABEL_CONDITIONS}) by (instance, type)`,
		Labels: []string{"instance", "type"},
	},
	"pd_cluster_metadata": {
		PromQL: `pd_cluster_metadata{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"pd_region_health": {
		PromQL:  `sum(pd_regions_status{$LABEL_CONDITIONS}) by (instance, type)`,
		Labels:  []string{"instance", "type"},
		Comment: "It records the unusual Regions' count which may include pending peers, down peers, extra peers, offline peers, missing peers or learner peers",
	},
	"pd_schedule_operator": {
		PromQL:  `sum(delta(pd_schedule_operators_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,event,instance)`,
		Labels:  []string{"instance", "type", "event"},
		Comment: "The number of different operators",
	},
	"pd_schedule_operator_total_num": {
		PromQL:  `sum(increase(pd_schedule_operators_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,event,instance)`,
		Labels:  []string{"instance", "type", "event"},
		Comment: "The total number of different operators",
	},
	"pd_operator_finish_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(pd_schedule_finish_operators_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type))`,
		Labels:   []string{"type"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when the operator is finished",
	},
	"pd_operator_step_finish_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(pd_schedule_finish_operator_steps_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type))`,
		Labels:   []string{"type"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when the operator step is finished",
	},
	"pd_scheduler_store_status": {
		PromQL: `pd_scheduler_store_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "address", "store", "type"},
	},
	"store_available_ratio": {
		PromQL:  `sum(pd_scheduler_store_status{type="store_available"}) by (address, store) / sum(pd_scheduler_store_status{type="store_capacity"}) by (address, store)`,
		Labels:  []string{"address", "store"},
		Comment: "It is equal to Store available capacity size over Store capacity size for each TiKV instance",
	},
	"store_size_amplification": {
		PromQL:  `sum(pd_scheduler_store_status{type="region_size"}) by (address, store) / sum(pd_scheduler_store_status{type="store_used"}) by (address, store) * 2^20`,
		Labels:  []string{"address", "store"},
		Comment: "The size amplification, which is equal to Store Region size over Store used capacity size, of each TiKV instance",
	},
	"pd_scheduler_op_influence": {
		PromQL: `pd_scheduler_op_influence{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "scheduler", "store", "type"},
	},
	"pd_scheduler_tolerant_resource": {
		PromQL: `pd_scheduler_tolerant_resource{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "scheduler", "source", "target"},
	},
	"pd_hotspot_status": {
		PromQL: `pd_hotspot_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "address", "store", "type"},
	},
	"pd_scheduler_status": {
		PromQL: `pd_scheduler_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "kind", "type"},
	},
	"pd_scheduler_balance_leader": {
		PromQL:  `sum(delta(pd_scheduler_balance_leader{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store,instance,type)`,
		Labels:  []string{"instance", "address", "store", "type"},
		Comment: "The leader movement details among TiKV instances",
	},
	"pd_scheduler_balance_region": {
		PromQL:  `sum(delta(pd_scheduler_balance_region{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store,instance,type)`,
		Labels:  []string{"instance", "address", "store", "type"},
		Comment: "The Region movement details among TiKV instances",
	},
	"pd_balance_scheduler_status": {
		PromQL:  `sum(delta(pd_scheduler_event_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,name)`,
		Labels:  []string{"instance", "name", "type"},
		Comment: "The inner status of balance leader scheduler",
	},
	"pd_checker_event_count": {
		PromQL:  `sum(delta(pd_checker_event_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance,type)`,
		Labels:  []string{"instance", "name", "type"},
		Comment: "The replica/region checker's status",
	},
	"pd_schedule_filter": {
		PromQL: `sum(delta(pd_schedule_filter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (store, type, scope, instance)`,
		Labels: []string{"instance", "scope", "store", "type"},
	},
	"pd_scheduler_balance_direction": {
		PromQL: `sum(delta(pd_scheduler_balance_direction{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,source,target,instance)`,
		Labels: []string{"instance", "source", "target", "type"},
	},
	"pd_schedule_store_limit": {
		PromQL: `pd_schedule_store_limit{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "store", "type"},
	},

	"pd_grpc_completed_commands_rate": {
		PromQL:  `sum(rate(grpc_server_handling_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (grpc_method,instance)`,
		Labels:  []string{"instance", "grpc_method"},
		Comment: "The rate of completing each kind of gRPC commands",
	},
	"pd_grpc_completed_commands_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(grpc_server_handling_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,grpc_method,instance))`,
		Labels:   []string{"instance", "grpc_method"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed of completing each kind of gRPC commands",
	},
	"pd_handle_transactions_rate": {
		PromQL:  `sum(rate(pd_txn_handle_txns_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, result)`,
		Labels:  []string{"instance", "result"},
		Comment: "The rate of handling etcd transactions",
	},
	"pd_handle_transactions_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(pd_txn_handle_txns_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance, result))`,
		Labels:   []string{"instance", "result"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed of handling etcd transactions",
	},
	"etcd_wal_fsync_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(etcd_disk_wal_fsync_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed of writing WAL into the persistent storage",
	},
	"pd_peer_round_trip_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(etcd_network_peer_round_trip_time_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,To))`,
		Labels:   []string{"instance", "To"},
		Quantile: 0.99,
		Comment:  "The quantile latency of the network in .99",
	},
	"etcd_disk_wal_fsync_rate": {
		PromQL:  `delta(etcd_disk_wal_fsync_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels:  []string{"instance"},
		Comment: "The rate of writing WAL into the persistent storage",
	},
	"pd_server_etcd_state": {
		PromQL:  `pd_server_etcd_state{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "type"},
		Comment: "The current term of Raft",
	},
	"pd_handle_request_duration_avg": {
		PromQL: `avg(rate(pd_client_request_handle_requests_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type) / avg(rate(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)`,
		Labels: []string{"type"},
	},
	"pd_region_heartbeat_duration": {
		PromQL:   `round(histogram_quantile($QUANTILE, sum(rate(pd_scheduler_region_heartbeat_latency_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,address, store)), 1000)`,
		Labels:   []string{"address", "store"},
		Quantile: 0.99,
		Comment:  "The quantile of heartbeat latency of each TiKV instance in",
	},
	"pd_scheduler_region_heartbeat": {
		PromQL: `sum(rate(pd_scheduler_region_heartbeat{$LABEL_CONDITIONS}[$RANGE_DURATION])*60) by (address,instance, store, status,type)`,
		Labels: []string{"instance", "address", "status", "store", "type"},
	},
	"pd_region_syncer_status": {
		PromQL: `pd_region_syncer_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"tikv_engine_size": {
		PromQL:  `sum(tikv_engine_size_bytes{$LABEL_CONDITIONS}) by (instance, type, db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The storage size per TiKV instance",
	},
	"tikv_store_size": {
		PromQL:  `sum(tikv_store_size_bytes{$LABEL_CONDITIONS}) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The available or capacity size of each TiKV instance",
	},
	"tikv_thread_cpu": {
		PromQL:  `sum(rate(tikv_thread_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,name)`,
		Labels:  []string{"instance", "name"},
		Comment: "The CPU usage of each TiKV instance",
	},
	"tikv_memory": {
		PromQL:  `avg(process_resident_memory_bytes{$LABEL_CONDITIONS}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The memory usage per TiKV instance",
	},
	"tikv_io_utilization": {
		PromQL:  `rate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels:  []string{"instance", "device"},
		Comment: "The I/O utilization per TiKV instance",
	},
	"tikv_flow_mbps": {
		PromQL:  `sum(rate(tikv_engine_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The total bytes of read and write in each TiKV instance",
	},
	"tikv_grpc_qps": {
		PromQL:  `sum(rate(tikv_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The QPS per command in each TiKV instance",
	},
	"tikv_grpc_errors": {
		PromQL:  `sum(rate(tikv_grpc_msg_fail_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The OPS of the gRPC message failures",
	},
	"tikv_grpc_error_total_count": {
		PromQL:  `sum(increase(tikv_grpc_msg_fail_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total count of the gRPC message failures",
	},
	"tikv_critical_error": {
		PromQL:  `sum(rate(tikv_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The OPS of the TiKV critical error",
	},
	"tikv_critical_error_total_count": {
		PromQL:  `sum(increase(tikv_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of the TiKV critical error",
	},
	"tikv_pd_heartbeat": {
		PromQL:  `sum(delta(tikv_pd_heartbeat_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of the gRPC message failures",
	},
	"tikv_region_count": {
		PromQL:  `sum(tikv_raftstore_region_count{$LABEL_CONDITIONS}) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of regions on each TiKV instance",
	},
	"tikv_scheduler_is_busy": {
		PromQL:  `sum(rate(tikv_scheduler_too_busy_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,db,type,stage)`,
		Labels:  []string{"instance", "db", "type", "stage"},
		Comment: "Indicates occurrences of Scheduler Busy events that make the TiKV instance unavailable temporarily",
	},
	"tikv_scheduler_is_busy_total_count": {
		PromQL:  `sum(increase(tikv_scheduler_too_busy_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,db,type,stage)`,
		Labels:  []string{"instance", "db", "type", "stage"},
		Comment: "The total count of Scheduler Busy events that make the TiKV instance unavailable temporarily",
	},
	"tikv_channel_full": {
		PromQL:  `sum(rate(tikv_channel_full_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,db)`,
		Labels:  []string{"instance", "db", "type"},
		Comment: "The ops of channel full errors on each TiKV instance, it will make the TiKV instance unavailable temporarily",
	},
	"tikv_channel_full_total_count": {
		PromQL:  `sum(increase(tikv_channel_full_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,db)`,
		Labels:  []string{"instance", "db", "type"},
		Comment: "The total number of channel full errors on each TiKV instance, it will make the TiKV instance unavailable temporarily",
	},
	"tikv_coprocessor_is_busy": {
		PromQL:  `sum(rate(tikv_coprocessor_request_error{type='full'}[$RANGE_DURATION])) by (instance,db,type)`,
		Labels:  []string{"instance", "db"},
		Comment: "The ops of Coprocessor Full events that make the TiKV instance unavailable temporarily",
	},
	"tikv_coprocessor_is_busy_total_count": {
		PromQL:  `sum(increase(tikv_coprocessor_request_error{type='full'}[$RANGE_DURATION])) by (instance,db,type)`,
		Labels:  []string{"instance", "db"},
		Comment: "The total count of Coprocessor Full events that make the TiKV instance unavailable temporarily",
	},
	"tikv_engine_write_stall": {
		PromQL:  `avg(tikv_engine_write_stall{type="write_stall_percentile99"}) by (instance, db)`,
		Labels:  []string{"instance", "db"},
		Comment: "Indicates occurrences of Write Stall events that make the TiKV instance unavailable temporarily",
	},
	"tikv_server_report_failures": {
		PromQL:  `sum(rate(tikv_server_report_failure_msg_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,store_id)`,
		Labels:  []string{"instance", "store_id", "type"},
		Comment: "The total number of reported failure messages",
	},
	"tikv_server_report_failures_total_count": {
		PromQL:  `sum(increase(tikv_server_report_failure_msg_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,store_id)`,
		Labels:  []string{"instance", "store_id", "type"},
		Comment: "The total number of reported failure messages",
	},
	"tikv_storage_async_requests": {
		PromQL:  `sum(rate(tikv_storage_engine_async_request_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, status, type)`,
		Labels:  []string{"instance", "status", "type"},
		Comment: "The number of different raftstore errors on each TiKV instance",
	},
	"tikv_storage_async_requests_total_count": {
		PromQL:  `sum(increase(tikv_storage_engine_async_request_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, status, type)`,
		Labels:  []string{"instance", "status", "type"},
		Comment: "The total number of different raftstore errors on each TiKV instance",
	},
	"tikv_scheduler_stage": {
		PromQL:  `sum(rate(tikv_scheduler_stage_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, stage,type)`,
		Labels:  []string{"instance", "stage", "type"},
		Comment: "The number of scheduler state on each TiKV instance",
	},
	"tikv_scheduler_stage_total_num": {
		PromQL:  `sum(increase(tikv_scheduler_stage_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, stage,type)`,
		Labels:  []string{"instance", "stage", "type"},
		Comment: "The total number of scheduler state on each TiKV instance",
	},

	"tikv_coprocessor_request_error": {
		PromQL:  `sum(rate(tikv_coprocessor_request_error{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)`,
		Labels:  []string{"instance", "reason"},
		Comment: "The number of different coprocessor errors on each TiKV instance",
	},
	"tikv_coprocessor_request_error_total_count": {
		PromQL:  `sum(increase(tikv_coprocessor_request_error{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)`,
		Labels:  []string{"instance", "reason"},
		Comment: "The total number of different coprocessor errors on each TiKV instance",
	},
	"tikv_region_change": {
		PromQL:  `sum(delta(tikv_raftstore_region_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The count of region change per TiKV instance",
	},
	"tikv_leader_missing": {
		PromQL:  `sum(tikv_raftstore_leader_missing{$LABEL_CONDITIONS}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of missing leaders per TiKV instance",
	},
	"tikv_active_written_leaders": {
		PromQL:  `sum(rate(tikv_region_written_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of leaders being written on each TiKV instance",
	},
	"tikv_approximate_region_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_region_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile of approximate Region size, the default value is P99",
	},
	"tikv_approximate_avg_region_size": {
		PromQL:  `sum(rate(tikv_raftstore_region_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_region_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance"},
		Comment: "The avg approximate Region size",
	},
	"tikv_approximate_region_size_histogram": {
		PromQL: `sum(rate(tikv_raftstore_region_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"tikv_region_average_written_bytes": {
		PromQL:  `sum(rate(tikv_region_written_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance) / sum(rate(tikv_region_written_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The average rate of writing bytes to Regions per TiKV instance",
	},
	"tikv_region_written_bytes": {
		PromQL: `sum(rate(tikv_region_written_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"tikv_region_average_written_keys": {
		PromQL:  `sum(rate(tikv_region_written_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance) / sum(rate(tikv_region_written_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The average rate of written keys to Regions per TiKV instance",
	},
	"tikv_region_written_keys": {
		PromQL: `sum(rate(tikv_region_written_keys_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"tikv_request_batch_avg": {
		PromQL:  `sum(rate(tikv_server_request_batch_ratio_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(tikv_server_request_batch_ratio_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The ratio of request batch output to input per TiKV instance",
	},
	"tikv_request_batch_ratio": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_server_request_batch_ratio_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile ratio of request batch output to input per TiKV instance",
	},
	"tikv_request_batch_size_avg": {
		PromQL:  `sum(rate(tikv_server_request_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(tikv_server_request_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The avg size of requests into request batch per TiKV instance",
	},
	"tikv_request_batch_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_server_request_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile size of requests into request batch per TiKV instance",
	},

	"tikv_grpc_messge_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_grpc_msg_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile execution time of gRPC message",
	},
	"tikv_average_grpc_messge_duration": {
		PromQL: `sum(rate(tikv_grpc_msg_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(tikv_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"tikv_grpc_req_batch_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_server_grpc_req_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"tikv_grpc_resp_batch_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_server_grpc_resp_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"tikv_grpc_avg_req_batch_size": {
		PromQL: `sum(rate(tikv_server_grpc_req_batch_size_sum[$RANGE_DURATION])) / sum(rate(tikv_server_grpc_req_batch_size_count[$RANGE_DURATION]))`,
	},
	"tikv_grpc_avg_resp_batch_size": {
		PromQL: `sum(rate(tikv_server_grpc_resp_batch_size_sum[$RANGE_DURATION])) / sum(rate(tikv_server_grpc_resp_batch_size_count[$RANGE_DURATION]))`,
	},
	"tikv_raft_message_batch_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_server_raft_message_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"tikv_raft_message_avg_batch_size": {
		PromQL: `sum(rate(tikv_server_raft_message_batch_size_sum[$RANGE_DURATION])) / sum(rate(tikv_server_raft_message_batch_size_count[$RANGE_DURATION]))`,
	},
	"tikv_pd_request_ops": {
		PromQL:  `sum(rate(tikv_pd_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The OPS of requests that TiKV sends to PD",
	},
	"tikv_pd_request_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_pd_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
	},
	"tikv_pd_request_total_count": {
		PromQL:  `sum(increase(tikv_pd_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The count of requests that TiKV sends to PD",
	},
	"tikv_pd_request_total_time": {
		PromQL:  `sum(increase(tikv_pd_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The count of requests that TiKV sends to PD",
	},
	"tikv_pd_request_avg_duration": {
		PromQL:  `sum(rate(tikv_pd_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance) / sum(rate(tikv_pd_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The time consumed by requests that TiKV sends to PD",
	},
	"tikv_pd_heartbeats": {
		PromQL:  `sum(rate(tikv_pd_heartbeat_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: " The total number of PD heartbeat messages",
	},
	"tikv_pd_validate_peers": {
		PromQL:  `sum(rate(tikv_pd_validate_peer_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of peers validated by the PD worker",
	},
	"tikv_apply_log_avg_duration": {
		PromQL:  `sum(rate(tikv_raftstore_apply_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_apply_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance"},
		Comment: "The average time consumed when Raft applies log",
	},
	"tikv_apply_log_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_apply_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when Raft applies log",
	},
	"tikv_append_log_avg_duration": {
		PromQL:  `sum(rate(tikv_raftstore_append_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_append_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The avg time consumed when Raft appends log",
	},
	"tikv_append_log_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_append_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when Raft appends log",
	},
	"tikv_commit_log_avg_duration": {
		PromQL:  `sum(rate(tikv_raftstore_commit_log_duration_seconds_sum[$RANGE_DURATION])) / sum(rate(tikv_raftstore_commit_log_duration_seconds_count[$RANGE_DURATION]))`,
		Comment: "The time consumed when Raft commits log",
	},
	"tikv_commit_log_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_commit_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when Raft commits log",
	},
	"tikv_ready_handled": {
		PromQL:  `sum(rate(tikv_raftstore_raft_ready_handled_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of ready handled of Raft",
	},
	"tikv_process_handled": {
		PromQL:  `sum(rate(tikv_raftstore_raft_process_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance", "type"},
		Comment: "The count of different process type of Raft",
	},
	"tikv_process_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_raft_process_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed for peer processes in Raft",
	},
	"tikv_raft_store_events_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_event_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed by raftstore events (P99).99",
	},
	"tikv_raft_sent_messages": {
		PromQL:  `sum(rate(tikv_raftstore_raft_sent_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of Raft messages sent by each TiKV instance",
	},
	"tikv_raft_sent_messages_total_num": {
		PromQL:  `sum(increase(tikv_raftstore_raft_sent_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of Raft messages sent by each TiKV instance",
	},
	"tikv_flush_messages": {
		PromQL:  `sum(rate(tikv_server_raft_message_flush_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of Raft messages flushed by each TiKV instance",
	},
	"tikv_flush_messages_total_num": {
		PromQL:  `sum(increase(tikv_server_raft_message_flush_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The total number of Raft messages flushed by each TiKV instance",
	},

	"tikv_receive_messages": {
		PromQL:  `sum(rate(tikv_server_raft_message_recv_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of Raft messages received by each TiKV instance",
	},
	"tikv_receive_messages_total_num": {
		PromQL:  `sum(increase(tikv_server_raft_message_recv_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The total number of Raft messages received by each TiKV instance",
	},
	"tikv_raft_dropped_messages": {
		PromQL:  `sum(rate(tikv_raftstore_raft_dropped_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of dropped Raft messages per type",
	},
	"tikv_raft_dropped_messages_total": {
		PromQL:  `sum(increase(tikv_raftstore_raft_dropped_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of dropped Raft messages per type",
	},
	"tikv_raft_proposals_per_ready": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_apply_proposal_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile proposal count of all Regions in a mio tick",
	},
	"tikv_raft_proposals": {
		PromQL:  `sum(rate(tikv_raftstore_proposal_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of proposals per type in raft",
	},
	"tikv_raft_proposals_total_num": {
		PromQL:  `sum(increase(tikv_raftstore_proposal_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of proposals per type in raft",
	},
	"tikv_propose_wait_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_request_wait_time_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile wait time of each proposal",
	},
	"tikv_propose_avg_wait_duration": {
		PromQL:  `sum(rate(tikv_raftstore_request_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_request_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The average wait time of each proposal",
	},
	"tikv_apply_wait_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_apply_wait_time_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"tikv_apply_avg_wait_duration": {
		PromQL: `sum(rate(tikv_raftstore_apply_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_raftstore_apply_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"tikv_raft_log_speed": {
		PromQL:  `avg(rate(tikv_raftstore_propose_log_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The rate at which peers propose logs",
	},
	"tikv_admin_apply": {
		PromQL:  `sum(rate(tikv_raftstore_admin_cmd_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,status,instance)`,
		Labels:  []string{"instance", "type", "status"},
		Comment: "The number of the processed apply command",
	},
	"tikv_check_split": {
		PromQL:  `sum(rate(tikv_raftstore_check_split_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of raftstore split checks",
	},
	"tikv_check_split_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_check_split_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.9999,
		Comment:  "The quantile of time consumed when running split check in .9999",
	},
	"tikv_local_reader_reject_requests": {
		PromQL:  `sum(rate(tikv_raftstore_local_read_reject_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)`,
		Labels:  []string{"instance", "reason"},
		Comment: "The number of rejections from the local read thread",
	},
	"tikv_local_reader_execute_requests": {
		PromQL:  `sum(rate(tikv_raftstore_local_read_executed_requests{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of total requests from the local read thread",
	},
	"tikv_storage_command_ops": {
		PromQL:  `sum(rate(tikv_storage_command_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total count of different kinds of commands received per seconds",
	},
	"tikv_storage_async_request_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_storage_engine_async_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed by processing asynchronous snapshot requests",
	},
	"tikv_storage_async_request_avg_duration": {
		PromQL:  `sum(rate(tikv_storage_engine_async_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_storage_engine_async_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance", "type"},
		Comment: "The time consumed by processing asynchronous snapshot requests",
	},
	"tikv_scheduler_writing_bytes": {
		PromQL:  `sum(tikv_scheduler_writing_bytes{$LABEL_CONDITIONS}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The total writing bytes of commands on each stage",
	},
	"tikv_scheduler_priority_commands": {
		PromQL:  `sum(rate(tikv_scheduler_commands_pri_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (priority,instance)`,
		Labels:  []string{"instance", "priority"},
		Comment: "The count of different priority commands",
	},

	"tikv_scheduler_pending_commands": {
		PromQL:  `sum(tikv_scheduler_contex_total{$LABEL_CONDITIONS}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of pending commands per TiKV instance",
	},
	"tikv_scheduler_command_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_command_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed when executing command",
	},
	"tikv_scheduler_command_avg_duration": {
		PromQL:  `sum(rate(tikv_scheduler_command_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_scheduler_command_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance", "type"},
		Comment: "The average time consumed when executing command",
	},
	"tikv_scheduler_latch_wait_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_latch_wait_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile time which is caused by latch wait in command",
	},
	"tikv_scheduler_latch_wait_avg_duration": {
		PromQL:  `sum(rate(tikv_scheduler_latch_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_scheduler_latch_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance", "type"},
		Comment: "The average time which is caused by latch wait in command",
	},

	"tikv_scheduler_keys_read": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_kv_command_key_read_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile count of keys read by command",
	},
	"tikv_scheduler_keys_read_avg": {
		PromQL:  `sum(rate(tikv_scheduler_kv_command_key_read_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_scheduler_kv_command_key_read_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance", "type"},
		Comment: "The average count of keys read by command",
	},
	"tikv_scheduler_keys_written": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_scheduler_kv_command_key_write_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile of count of keys written by a command",
	},
	"tikv_scheduler_keys_written_avg": {
		PromQL:  `sum(rate(tikv_scheduler_kv_command_key_write_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_scheduler_kv_command_key_write_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance", "type"},
		Comment: "The average count of keys written by a command",
	},
	"tikv_scheduler_scan_details": {
		PromQL:  `sum(rate(tikv_scheduler_kv_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,instance,req,cf)`,
		Labels:  []string{"instance", "tag", "req", "cf"},
		Comment: "The keys scan details of each CF when executing command",
	},
	"tikv_scheduler_scan_details_total_num": {
		PromQL: `sum(increase(tikv_scheduler_kv_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,instance,req,cf)`,
		Labels: []string{"instance", "tag", "req", "cf"},
	},
	"tikv_mvcc_versions": {
		PromQL:  `sum(rate(tikv_storage_mvcc_versions_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The number of versions for each key",
	},
	"tikv_mvcc_delete_versions": {
		PromQL:  `sum(rate(tikv_storage_mvcc_gc_delete_versions_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of versions deleted by GC for each key",
	},
	"tikv_gc_tasks_ops": {
		PromQL:  `sum(rate(tikv_gcworker_gc_tasks_vec{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)`,
		Labels:  []string{"instance", "task"},
		Comment: "The count of GC total tasks processed by gc_worker per second",
	},
	"tikv_gc_skipped_tasks": {
		PromQL:  `sum(rate(tikv_storage_gc_skipped_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)`,
		Labels:  []string{"instance", "task"},
		Comment: "The count of GC skipped tasks processed by gc_worker",
	},
	"tikv_gc_fail_tasks": {
		PromQL:  `sum(rate(tikv_gcworker_gc_task_fail_vec{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)`,
		Labels:  []string{"instance", "task"},
		Comment: "The count of GC tasks processed fail by gc_worker",
	},
	"tikv_gc_too_busy": {
		PromQL:  `sum(rate(tikv_gc_worker_too_busy{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The count of GC worker too busy",
	},
	"tikv_gc_tasks_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_gcworker_gc_task_duration_vec_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,task,instance))`,
		Labels:   []string{"instance", "task"},
		Quantile: 1,
		Comment:  "The quantile of time consumed when executing GC tasks",
	},
	"tikv_gc_tasks_avg_duration": {
		PromQL:  `sum(rate(tikv_gcworker_gc_task_duration_vec_sum{}[$RANGE_DURATION])) by (task,instance) / sum(rate(tikv_gcworker_gc_task_duration_vec_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)`,
		Labels:  []string{"instance", "task"},
		Comment: "The time consumed when executing GC tasks",
	},
	"tikv_gc_keys": {
		PromQL:  `sum(rate(tikv_gcworker_gc_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,cf,instance)`,
		Labels:  []string{"instance", "tag", "cf"},
		Comment: "The count of keys in write CF affected during GC",
	},
	"tikv_gc_keys_total_num": {
		PromQL:  `sum(increase(tikv_gcworker_gc_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,cf,instance)`,
		Labels:  []string{"instance", "tag", "cf"},
		Comment: "The total number of keys in write CF affected during GC",
	},
	"tikv_gc_speed": {
		PromQL:  `sum(rate(tikv_storage_mvcc_gc_delete_versions_sum[$RANGE_DURATION]))`,
		Comment: "The GC keys per second",
	},
	"tikv_auto_gc_working": {
		PromQL: `sum(max_over_time(tikv_gcworker_autogc_status{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,state)`,
		Labels: []string{"instance", "state"},
	},
	"tikv_client_task_progress": {
		PromQL:  `max(tidb_tikvclient_range_task_stats{$LABEL_CONDITIONS}) by (result,type)`,
		Labels:  []string{"result", "type"},
		Comment: "The progress of tikv client task",
	},
	"tikv_auto_gc_progress": {
		PromQL:  `sum(tikv_gcworker_autogc_processed_regions{type="scan"}) by (instance,type) / sum(tikv_raftstore_region_count{type="region"}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "Progress of TiKV's GC",
	},
	"tikv_auto_gc_safepoint": {
		PromQL:  `max(tikv_gcworker_autogc_safe_point{$LABEL_CONDITIONS}) by (instance) / (2^18)`,
		Labels:  []string{"instance"},
		Comment: "SafePoint used for TiKV's Auto GC",
	},
	"tikv_send_snapshot_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_server_send_snapshot_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed when sending snapshots",
	},
	"tikv_handle_snapshot_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_raftstore_snapshot_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed when handling snapshots",
	},
	"tikv_snapshot_state_count": {
		PromQL:  `sum(tikv_raftstore_snapshot_traffic_total{$LABEL_CONDITIONS}) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of snapshots in different states",
	},
	"tikv_snapshot_state_total_count": {
		PromQL:  `sum(delta(tikv_raftstore_snapshot_traffic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of snapshots in different states",
	},
	"tikv_snapshot_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_snapshot_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.9999,
		Comment:  "The quantile of snapshot size",
	},
	"tikv_snapshot_kv_count": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_snapshot_kv_count_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.9999,
		Comment:  "The quantile of number of KV within a snapshot",
	},
	"tikv_worker_handled_tasks": {
		PromQL:  `sum(rate(tikv_worker_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "The number of tasks handled by worker",
	},
	"tikv_worker_handled_tasks_total_num": {
		PromQL:  `sum(increase(tikv_worker_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Total number of tasks handled by worker",
	},
	"tikv_worker_pending_tasks": {
		PromQL:  `sum(rate(tikv_worker_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Current pending and running tasks of worker",
	},
	"tikv_worker_pending_tasks_total_num": {
		PromQL:  `sum(increase(tikv_worker_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Total pending and running tasks of worker",
	},
	"tikv_futurepool_handled_tasks": {
		PromQL:  `sum(rate(tikv_futurepool_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "The number of tasks handled by future_pool",
	},
	"tikv_futurepool_handled_tasks_total_num": {
		PromQL:  `sum(increase(tikv_futurepool_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Total number of tasks handled by future_pool",
	},
	"tikv_futurepool_pending_tasks": {
		PromQL:  `sum(rate(tikv_futurepool_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Current pending and running tasks of future_pool",
	},
	"tikv_futurepool_pending_tasks_total_num": {
		PromQL:  `sum(increase(tikv_futurepool_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Total pending and running tasks of future_pool",
	},
	"tikv_cop_request_durations": {
		PromQL:  `sum(rate(tikv_coprocessor_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,req)`,
		Labels:  []string{"instance", "req"},
		Comment: "The time consumed to handle coprocessor read requests",
	},
	"tikv_cop_request_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_coprocessor_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance))`,
		Labels:   []string{"instance", "req"},
		Quantile: 1,
		Comment:  "The quantile of time consumed to handle coprocessor read requests",
	},
	"tikv_cop_requests_ops": {
		PromQL: `sum(rate(tikv_coprocessor_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,instance)`,
		Labels: []string{"instance", "req"},
	},
	"tikv_cop_scan_keys_num": {
		PromQL: `sum(rate(tikv_coprocessor_scan_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,instance)`,
		Labels: []string{"instance", "req"},
	},
	"tikv_cop_kv_cursor_operations": {
		PromQL:   `histogram_quantile($QUANTILE, avg(rate(tikv_coprocessor_scan_keys_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance)) `,
		Labels:   []string{"instance", "req"},
		Quantile: 1,
	},
	"tikv_cop_total_rocksdb_perf_statistics": {
		PromQL: `sum(rate(tikv_coprocessor_rocksdb_perf{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,metric,instance)`,
		Labels: []string{"instance", "req", "metric"},
	},
	"tikv_cop_total_response_size_per_seconds": {
		PromQL: `sum(rate(tikv_coprocessor_response_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels: []string{"instance"},
	},
	"tikv_cop_handle_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_coprocessor_request_handle_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance))`,
		Labels:   []string{"instance", "req"},
		Quantile: 1,
		Comment:  "The quantile of time consumed when handling coprocessor requests",
	},
	"tikv_cop_wait_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_coprocessor_request_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance))`,
		Labels:   []string{"instance", "req"},
		Quantile: 1,
		Comment:  "The quantile of time consumed when coprocessor requests are wait for being handled",
	},
	"tikv_cop_dag_requests_ops": {
		PromQL: `sum(rate(tikv_coprocessor_dag_request_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (vec_type,instance)`,
		Labels: []string{"instance", "vec_type"},
	},
	"tikv_cop_dag_executors_ops": {
		PromQL:  `sum(rate(tikv_coprocessor_executor_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of DAG executors per seconds",
	},
	"tikv_cop_scan_details": {
		PromQL: `sum(rate(tikv_coprocessor_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,req,cf,instance)`,
		Labels: []string{"instance", "tag", "req", "cf"},
	},
	"tikv_cop_scan_details_total": {
		PromQL: `sum(increase(tikv_coprocessor_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,req,cf,instance)`,
		Labels: []string{"instance", "tag", "req", "cf"},
	},

	"tikv_threads_state": {
		PromQL: `sum(tikv_threads_state{$LABEL_CONDITIONS}) by (instance,state)`,
		Labels: []string{"instance", "state"},
	},
	"tikv_threads_io": {
		PromQL: `sum(rate(tikv_threads_io_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,io,instance)`,
		Labels: []string{"instance", "io", "name"},
	},
	"tikv_thread_voluntary_context_switches": {
		PromQL: `sum(rate(tikv_thread_voluntary_context_switches{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, name)`,
		Labels: []string{"instance", "name"},
	},
	"tikv_thread_nonvoluntary_context_switches": {
		PromQL: `sum(rate(tikv_thread_nonvoluntary_context_switches{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, name)`,
		Labels: []string{"instance", "name"},
	},
	"tikv_engine_get_cpu_cache_operations": {
		PromQL:  `sum(rate(tikv_engine_get_served{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The count of get l0,l1,l2 operations",
	},
	"tikv_engine_get_block_cache_operations": {
		PromQL:  `sum(rate(tikv_engine_cache_efficiency{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The count of get memtable operations",
	},

	"tikv_engine_get_memtable_operations": {
		PromQL:  `sum(rate(tikv_engine_memtable_efficiency{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The count of get memtable operations",
	},
	"tikv_engine_max_get_duration": {
		PromQL:  `max(tikv_engine_get_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The max time consumed when executing get operations, the unit is microsecond",
	},
	"tikv_engine_avg_get_duration": {
		PromQL:  `avg(tikv_engine_get_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The average time consumed when executing get operations, the unit is microsecond",
	},
	"tikv_engine_seek_operations": {
		PromQL:  `sum(rate(tikv_engine_locate{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The count of seek operations",
	},
	"tikv_engine_max_seek_duration": {
		PromQL:  `max(tikv_engine_seek_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time consumed when executing seek operation, the unit is microsecond",
	},
	"tikv_engine_avg_seek_duration": {
		PromQL:  `avg(tikv_engine_seek_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time consumed when executing seek operation, the unit is microsecond",
	},
	"tikv_engine_write_operations": {
		PromQL:  `sum(rate(tikv_engine_write_served{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The count of write operations",
	},
	"tikv_engine_write_duration": {
		PromQL:  `avg(tikv_engine_write_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time consumed when executing write operation, the unit is microsecond",
	},
	"tikv_engine_write_max_duration": {
		PromQL:  `max(tikv_engine_write_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time consumed when executing write operation, the unit is microsecond",
	},
	"tikv_engine_wal_sync_operations": {
		PromQL:  `sum(rate(tikv_engine_wal_file_synced{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,instance)`,
		Labels:  []string{"instance", "db"},
		Comment: "The count of WAL sync operations",
	},

	"tikv_wal_sync_max_duration": {
		PromQL:  `max(tikv_engine_wal_file_sync_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The max time consumed when executing WAL sync operation, the unit is microsecond",
	},
	"tikv_wal_sync_duration": {
		PromQL:  `avg(tikv_engine_wal_file_sync_micro_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The time consumed when executing WAL sync operation, the unit is microsecond",
	},
	"tikv_compaction_operations": {
		PromQL:  `sum(rate(tikv_engine_event_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The count of compaction and flush operations",
	},
	"tikv_compaction_max_duration": {
		PromQL:  `max(tikv_engine_compaction_time{$LABEL_CONDITIONS}) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time consumed when executing the compaction and flush operations",
	},
	"tikv_compaction_duration": {
		PromQL:  `avg(tikv_engine_compaction_time{$LABEL_CONDITIONS}) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time consumed when executing the compaction and flush operations",
	},
	"tikv_sst_read_max_duration": {
		PromQL:  `max(tikv_engine_sst_read_micros{$LABEL_CONDITIONS}) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The max time consumed when reading SST files",
	},
	"tikv_sst_read_duration": {
		PromQL:  `avg(tikv_engine_sst_read_micros{$LABEL_CONDITIONS}) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time consumed when reading SST files",
	},
	"tikv_write_stall_max_duration": {
		PromQL:  `max(tikv_engine_write_stall{$LABEL_CONDITIONS}) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time which is caused by write stall",
	},
	"tikv_write_stall_avg_duration": {
		PromQL:  `avg(tikv_engine_write_stall{$LABEL_CONDITIONS}) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The time which is caused by write stall",
	},
	"tikv_memtable_size": {
		PromQL:  `avg(tikv_engine_memory_bytes{$LABEL_CONDITIONS}) by (type,instance,db,cf)`,
		Labels:  []string{"instance", "cf", "type", "db"},
		Comment: "The memtable size of each column family",
	},
	"tikv_memtable_hit": {
		PromQL:  `sum(rate(tikv_engine_memtable_efficiency{type="memtable_hit"}[$RANGE_DURATION])) by (instance,db) / (sum(rate(tikv_engine_memtable_efficiency{}[$RANGE_DURATION])) by (instance,db) + sum(rate(tikv_engine_memtable_efficiency{}[$RANGE_DURATION])) by (instance,db))`,
		Labels:  []string{"instance", "db"},
		Comment: "The hit rate of memtable",
	},
	"tikv_block_cache_size": {
		PromQL:  `topk(20, avg(tikv_engine_block_cache_size_bytes{$LABEL_CONDITIONS}) by(cf, instance, db))`,
		Labels:  []string{"instance", "cf", "db"},
		Comment: "The block cache size. Broken down by column family if shared block cache is disabled.",
	},
	"tikv_block_all_cache_hit": {
		PromQL:  `sum(rate(tikv_engine_cache_efficiency{type="block_cache_hit"}[$RANGE_DURATION])) by (db,instance) / (sum(rate(tikv_engine_cache_efficiency{type="block_cache_hit"}[$RANGE_DURATION])) by (db,instance) + sum(rate(tikv_engine_cache_efficiency{type="block_cache_miss"}[$RANGE_DURATION])) by (db,instance))`,
		Labels:  []string{"instance", "db"},
		Comment: "The hit rate of all block cache",
	},
	"tikv_block_data_cache_hit": {
		PromQL:  `sum(rate(tikv_engine_cache_efficiency{type="block_cache_data_hit"}[$RANGE_DURATION])) by (db,instance) / (sum(rate(tikv_engine_cache_efficiency{type="block_cache_data_hit"}[$RANGE_DURATION])) by (db,instance) + sum(rate(tikv_engine_cache_efficiency{type="block_cache_data_miss"}[$RANGE_DURATION])) by (db,instance))`,
		Labels:  []string{"instance", "db"},
		Comment: "The hit rate of data block cache",
	},
	"tikv_block_filter_cache_hit": {
		PromQL:  `sum(rate(tikv_engine_cache_efficiency{type="block_cache_filter_hit"}[$RANGE_DURATION])) by (db,instance) / (sum(rate(tikv_engine_cache_efficiency{type="block_cache_filter_hit"}[$RANGE_DURATION])) by (db,instance) + sum(rate(tikv_engine_cache_efficiency{type="block_cache_filter_miss"}[$RANGE_DURATION])) by (db,instance))`,
		Labels:  []string{"instance", "db"},
		Comment: "The hit rate of data block cache",
	},
	"tikv_block_index_cache_hit": {
		PromQL:  `sum(rate(tikv_engine_cache_efficiency{type="block_cache_index_hit"}[$RANGE_DURATION])) by (db,instance) / (sum(rate(tikv_engine_cache_efficiency{type="block_cache_index_hit"}[$RANGE_DURATION])) by (db,instance) + sum(rate(tikv_engine_cache_efficiency{type="block_cache_index_miss"}[$RANGE_DURATION])) by (db,instance))`,
		Labels:  []string{"instance", "db"},
		Comment: "The hit rate of data block cache",
	},
	"tikv_block_bloom_prefix_cache_hit": {
		PromQL:  `sum(rate(tikv_engine_bloom_efficiency{type="bloom_prefix_useful"}[$RANGE_DURATION])) by (db,instance) / sum(rate(tikv_engine_bloom_efficiency{type="bloom_prefix_checked"}[$RANGE_DURATION])) by (db,instance)`,
		Labels:  []string{"instance", "db"},
		Comment: "The hit rate of data block cache",
	},
	"tikv_corrrput_keys_flow": {
		PromQL:  `sum(rate(tikv_engine_compaction_num_corrupt_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,cf,instance)`,
		Labels:  []string{"instance", "db", "cf"},
		Comment: "The flow of corrupt operations on keys",
	},
	"tikv_total_keys": {
		PromQL:  `sum(tikv_engine_estimate_num_keys{$LABEL_CONDITIONS}) by (db,cf,instance)`,
		Labels:  []string{"instance", "db", "cf"},
		Comment: "The count of keys in each column family",
	},
	"tikv_per_read_max_bytes": {
		PromQL:  `max(tikv_engine_bytes_per_read{$LABEL_CONDITIONS}) by (type,db,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The max bytes per read",
	},
	"tikv_per_read_avg_bytes": {
		PromQL:  `avg(tikv_engine_bytes_per_read{$LABEL_CONDITIONS}) by (type,db,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The avg bytes per read",
	},
	"tikv_per_write_max_bytes": {
		PromQL:  `max(tikv_engine_bytes_per_write{$LABEL_CONDITIONS}) by (type,db,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The max bytes per write",
	},
	"tikv_per_write_avg_bytes": {
		PromQL:  `avg(tikv_engine_bytes_per_write{$LABEL_CONDITIONS}) by (type,db,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The avg bytes per write",
	},
	"tikv_engine_compaction_flow_bytes": {
		PromQL:  `sum(rate(tikv_engine_compaction_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "The flow rate of compaction operations per type",
	},
	"tikv_compaction_pending_bytes": {
		PromQL:  `sum(rate(tikv_engine_pending_compaction_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (cf,instance,db)`,
		Labels:  []string{"instance", "cf", "db"},
		Comment: "The pending bytes to be compacted",
	},
	"tikv_read_amplication": {
		PromQL:  `sum(rate(tikv_engine_read_amp_flow_bytes{type="read_amp_total_read_bytes"}[$RANGE_DURATION])) by (instance,db) / sum(rate(tikv_engine_read_amp_flow_bytes{type="read_amp_estimate_useful_bytes"}[$RANGE_DURATION])) by (instance,db)`,
		Labels:  []string{"instance", "db"},
		Comment: "The read amplification per TiKV instance",
	},
	"tikv_compression_ratio": {
		PromQL:  `avg(tikv_engine_compression_ratio{$LABEL_CONDITIONS}) by (level,instance,db)`,
		Labels:  []string{"instance", "level", "db"},
		Comment: "The compression ratio of each level",
	},
	"tikv_number_of_snapshots": {
		PromQL:  `tikv_engine_num_snapshots{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "db"},
		Comment: "The number of snapshot of each TiKV instance",
	},
	"tikv_oldest_snapshots_duration": {
		PromQL:  `tikv_engine_oldest_snapshot_duration{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "db"},
		Comment: "The time that the oldest unreleased snapshot survivals",
	},
	"tikv_number_files_at_each_level": {
		PromQL:  `avg(tikv_engine_num_files_at_level{$LABEL_CONDITIONS}) by (cf, level,db,instance)`,
		Labels:  []string{"instance", "cf", "level", "db"},
		Comment: "The number of SST files for different column families in each level",
	},
	"tikv_ingest_sst_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_snapshot_ingest_sst_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,db))`,
		Labels:   []string{"instance", "db"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed when ingesting SST files",
	},
	"tikv_ingest_sst_avg_duration": {
		PromQL:  `sum(rate(tikv_snapshot_ingest_sst_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_snapshot_ingest_sst_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The average time consumed when ingesting SST files",
	},
	"tikv_stall_conditions_changed_of_each_cf": {
		PromQL:  `tikv_engine_stall_conditions_changed{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "cf", "type", "db"},
		Comment: "Stall conditions changed of each column family",
	},
	"tikv_write_stall_reason": {
		PromQL: `sum(increase(tikv_engine_write_stall_reason{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_compaction_reason": {
		PromQL: `sum(rate(tikv_engine_compaction_reason{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,cf,reason,instance)`,
		Labels: []string{"instance", "cf", "reason", "db"},
	},
	"tikv_engine_blob_key_max_size": {
		PromQL: `max(tikv_engine_blob_key_size{$LABEL_CONDITIONS}) by (db,instance,type)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_key_avg_size": {
		PromQL: `avg(tikv_engine_blob_key_size{$LABEL_CONDITIONS}) by (db,instance,type)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_value_avg_size": {
		PromQL: `avg(tikv_engine_blob_value_size{$LABEL_CONDITIONS}) by (db,instance,type)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_value_max_size": {
		PromQL: `max(tikv_engine_blob_value_size{$LABEL_CONDITIONS}) by (db,instance,type)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_seek_duration": {
		PromQL:  `avg(tikv_engine_blob_seek_micros_seconds{$LABEL_CONDITIONS}) by (db,type,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "the unit is microsecond",
	},
	"tikv_engine_blob_seek_operations": {
		PromQL: `sum(rate(tikv_engine_blob_locate{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (db,type,instance)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_get_duration": {
		PromQL:  `avg(tikv_engine_blob_get_micros_seconds{$LABEL_CONDITIONS}) by (type,db,instance)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "the unit is microsecond",
	},
	"tikv_engine_blob_bytes_flow": {
		PromQL: `sum(rate(tikv_engine_blob_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_file_read_duration": {
		PromQL:  `avg(tikv_engine_blob_file_read_micros_seconds{$LABEL_CONDITIONS}) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "the unit is microsecond",
	},
	"tikv_engine_blob_file_write_duration": {
		PromQL:  `avg(tikv_engine_blob_file_write_micros_seconds{$LABEL_CONDITIONS}) by (type,instance,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "the unit is microsecond",
	},
	"tikv_engine_blob_file_sync_operations": {
		PromQL: `sum(rate(tikv_engine_blob_file_synced{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"tikv_engine_blob_file_sync_duration": {
		PromQL:  `avg(tikv_engine_blob_file_sync_micros_seconds{$LABEL_CONDITIONS}) by (instance,type,db)`,
		Labels:  []string{"instance", "type", "db"},
		Comment: "the unit is microsecond",
	},
	"tikv_engine_blob_file_count": {
		PromQL: `avg(tikv_engine_titandb_num_obsolete_blob_file{$LABEL_CONDITIONS}) by (instance,db)`,
		Labels: []string{"instance", "db"},
	},
	"tikv_engine_blob_file_size": {
		PromQL: `avg(tikv_engine_titandb_obsolete_blob_file_size{$LABEL_CONDITIONS}) by (instance,db)`,
		Labels: []string{"instance", "db"},
	},
	"tikv_engine_blob_gc_file": {
		PromQL: `sum(rate(tikv_engine_blob_gc_file_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_gc_duration": {
		PromQL: `avg(tikv_engine_blob_gc_micros_seconds{$LABEL_CONDITIONS}) by (db,instance,type)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_gc_bytes_flow": {
		PromQL: `sum(rate(tikv_engine_blob_gc_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_blob_gc_keys_flow": {
		PromQL: `sum(rate(tikv_engine_blob_gc_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,db)`,
		Labels: []string{"instance", "type", "db"},
	},
	"tikv_engine_live_blob_size": {
		PromQL: `avg(tikv_engine_titandb_live_blob_size{$LABEL_CONDITIONS}) by (instance,db)`,
		Labels: []string{"instance", "db"},
	},

	"tikv_lock_manager_handled_tasks": {
		PromQL: `sum(rate(tikv_lock_manager_task_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"tikv_lock_manager_waiter_lifetime_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_lock_manager_waiter_lifetime_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"tikv_lock_manager_waiter_lifetime_avg_duration": {
		PromQL: `sum(rate(tikv_lock_manager_waiter_lifetime_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_lock_manager_waiter_lifetime_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance", "type"},
	},
	"tikv_lock_manager_wait_table": {
		PromQL: `sum(max_over_time(tikv_lock_manager_wait_table_status{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"tikv_lock_manager_deadlock_detect_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_lock_manager_detect_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"tikv_lock_manager_deadlock_detect_avg_duration": {
		PromQL: `sum(rate(tikv_lock_manager_detect_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_lock_manager_detect_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"tikv_lock_manager_detect_error": {
		PromQL: `sum(rate(tikv_lock_manager_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"tikv_lock_manager_detect_error_total_count": {
		PromQL: `sum(increase(tikv_lock_manager_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"tikv_lock_manager_deadlock_detector_leader": {
		PromQL: `sum(max_over_time(tikv_lock_manager_detector_leader_heartbeat{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels: []string{"instance"},
	},
	"tikv_allocator_stats": {
		PromQL: `tikv_allocator_stats{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"tikv_backup_range_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_backup_range_size_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,cf,instance))`,
		Labels:   []string{"instance", "cf"},
		Quantile: 0.99,
	},
	"tikv_backup_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_backup_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"tikv_backup_avg_duration": {
		PromQL: `sum(rate(tikv_backup_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(tikv_backup_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},

	"tikv_backup_flow": {
		PromQL: `sum(rate(tikv_backup_range_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels: []string{"instance"},
	},
	"tikv_disk_read_bytes": {
		PromQL: `sum(irate(node_disk_read_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)`,
		Labels: []string{"instance", "device"},
	},
	"tikv_disk_write_bytes": {
		PromQL: `sum(irate(node_disk_written_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)`,
		Labels: []string{"instance", "device"},
	},
	"tikv_backup_range_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(tikv_backup_range_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
	},
	"tikv_backup_range_avg_duration": {
		PromQL: `sum(rate(tikv_backup_range_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(tikv_backup_range_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"tikv_backup_errors": {
		PromQL: `rate(tikv_backup_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "error"},
	},
	"tikv_backup_errors_total_count": {
		PromQL: `sum(increase(tikv_backup_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,error)`,
		Labels: []string{"instance", "error"},
	},
	"node_virtual_cpus": {
		PromQL:  `count(node_cpu_seconds_total{mode="user"}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "node virtual cpu count",
	},
	"node_total_memory": {
		PromQL:  `node_memory_MemTotal_bytes{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "total memory in node",
	},
	"node_memory_available": {
		PromQL: `node_memory_MemAvailable_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_total_memory_swap": {
		PromQL:  `node_memory_SwapTotal_bytes{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "node total memory swap",
	},
	"node_uptime": {
		PromQL:  `node_time_seconds{$LABEL_CONDITIONS} - node_boot_time_seconds{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "node uptime, units are seconds",
	},
	"node_load1": {
		PromQL:  `node_load1{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "1 minute load averages in node",
	},
	"node_load5": {
		PromQL:  `node_load5{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "5 minutes load averages in node",
	},
	"node_load15": {
		PromQL:  `node_load15{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "15 minutes load averages in node",
	},
	"node_kernel_interrupts": {
		PromQL: `rate(node_intr_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_intr_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance"},
	},
	"node_kernel_forks": {
		PromQL: `rate(node_forks_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_forks_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance"},
	},
	"node_kernel_context_switches": {
		PromQL: `rate(node_context_switches_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_context_switches_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance"},
	},
	"node_cpu_usage": {
		PromQL: `sum(rate(node_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (mode,instance) * 100 / count(node_cpu_seconds_total{$LABEL_CONDITIONS}) by (mode,instance) or sum(irate(node_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (mode,instance) * 100 / count(node_cpu_seconds_total{$LABEL_CONDITIONS}) by (mode,instance)`,
		Labels: []string{"instance", "mode"},
	},
	"node_memory_free": {
		PromQL: `node_memory_MemFree_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_buffers": {
		PromQL: `node_memory_Buffers_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_cached": {
		PromQL: `node_memory_Cached_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_active": {
		PromQL: `node_memory_Active_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_inactive": {
		PromQL: `node_memory_Inactive_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_writeback": {
		PromQL: `node_memory_Writeback_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_writeback_tmp": {
		PromQL: `node_memory_WritebackTmp_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_dirty": {
		PromQL: `node_memory_Dirty_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_shared": {
		PromQL: `node_memory_Shmem_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_mapped": {
		PromQL: `node_memory_Mapped_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_disk_size": {
		PromQL: `node_filesystem_size_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "device", "fstype", "mountpoint"},
	},
	"node_disk_available_size": {
		PromQL: `node_filesystem_avail_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "device", "fstype", "mountpoint"},
	},
	"node_disk_state": {
		PromQL: `node_filesystem_readonly{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "device", "fstype", "mountpoint"},
	},
	"node_disk_io_util": {
		PromQL: `rate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_disk_iops": {
		PromQL: `sum(rate(node_disk_reads_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) + rate(node_disk_writes_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)`,
		Labels: []string{"instance", "device"},
	},
	"node_disk_write_latency": {
		PromQL:  `(rate(node_disk_write_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])/ rate(node_disk_writes_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance", "device"},
		Comment: "node disk write latency(ms)",
	},
	"node_disk_read_latency": {
		PromQL:  `(rate(node_disk_read_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])/ rate(node_disk_reads_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance", "device"},
		Comment: "node disk read latency(ms)",
	},
	"node_disk_throughput": {
		PromQL:  `irate(node_disk_read_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) + irate(node_disk_written_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels:  []string{"instance", "device"},
		Comment: "Units is byte",
	},
	"node_filesystem_space_used": {
		PromQL:  `((node_filesystem_size_bytes{$LABEL_CONDITIONS} - node_filesystem_avail_bytes{$LABEL_CONDITIONS}) / node_filesystem_size_bytes{$LABEL_CONDITIONS}) * 100`,
		Labels:  []string{"instance", "device"},
		Comment: "Filesystem used space. If is > 80% then is Critical.",
	},
	"node_file_descriptor_allocated": {
		PromQL: `node_filefd_allocated{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_network_in_drops": {
		PromQL: `rate(node_network_receive_drop_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) `,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_drops": {
		PromQL: `rate(node_network_transmit_drop_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_in_errors": {
		PromQL: `rate(node_network_receive_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_in_errors_total_count": {
		PromQL: `sum(increase(node_network_receive_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by(instance, device)`,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_errors": {
		PromQL: `rate(node_network_transmit_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_errors_total_count": {
		PromQL: `sum(increase(node_network_transmit_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, device)`,
		Labels: []string{"instance", "device"},
	},
	"node_network_in_traffic": {
		PromQL: `rate(node_network_receive_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_receive_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_traffic": {
		PromQL: `rate(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_in_packets": {
		PromQL: `rate(node_network_receive_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_receive_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_packets": {
		PromQL: `rate(node_network_transmit_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_transmit_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_interface_speed": {
		PromQL:  `node_network_transmit_queue_length{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "device"},
		Comment: "node_network_transmit_queue_length = transmit_queue_length value of /sys/class/net/<iface>.",
	},
	"node_network_utilization_in_hourly": {
		PromQL: `sum(increase(node_network_receive_bytes_total{$LABEL_CONDITIONS}[1h]))`,
		Labels: []string{"instance", "device"},
	},
	"node_network_utilization_out_hourly": {
		PromQL: `sum(increase(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[1h]))`,
		Labels: []string{"instance", "device"},
	},
	"node_tcp_in_use": {
		PromQL: `node_sockstat_TCP_inuse{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_tcp_segments_retransmitted": {
		PromQL: `rate(node_netstat_Tcp_RetransSegs{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_netstat_Tcp_RetransSegs{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance"},
	},
	"node_tcp_connections": {
		PromQL: `node_netstat_Tcp_CurrEstab{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_processes_running": {
		PromQL: `node_procs_running{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_processes_blocked": {
		PromQL: `node_procs_blocked{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},

	"etcd_wal_fsync_total_count": {
		PromQL:  "sum(increase(etcd_disk_wal_fsync_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of writing WAL into the persistent storage",
	},
	"etcd_wal_fsync_total_time": {
		PromQL:  "sum(increase(etcd_disk_wal_fsync_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of writing WAL into the persistent storage",
	},
	"pd_client_cmd_total_count": {
		PromQL:  "sum(increase(pd_client_cmd_handle_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of pd client command durations",
	},
	"pd_client_cmd_total_time": {
		PromQL:  "sum(increase(pd_client_cmd_handle_cmds_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of pd client command durations",
	},
	"pd_grpc_completed_commands_total_count": {
		PromQL:  "sum(increase(grpc_server_handling_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,grpc_method)",
		Labels:  []string{"instance", "grpc_method"},
		Comment: "The total count of completing each kind of gRPC commands",
	},
	"pd_grpc_completed_commands_total_time": {
		PromQL:  "sum(increase(grpc_server_handling_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,grpc_method)",
		Labels:  []string{"instance", "grpc_method"},
		Comment: "The total time of completing each kind of gRPC commands",
	},
	"pd_handle_request_total_count": {
		PromQL:  "sum(increase(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of pd handle request duration(second)",
	},
	"pd_handle_request_total_time": {
		PromQL:  "sum(increase(pd_client_request_handle_requests_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of pd handle request duration(second)",
	},
	"pd_handle_transactions_total_count": {
		PromQL:  "sum(increase(pd_txn_handle_txns_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
		Labels:  []string{"instance", "result"},
		Comment: "The total count of handling etcd transactions",
	},
	"pd_handle_transactions_total_time": {
		PromQL:  "sum(increase(pd_txn_handle_txns_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
		Labels:  []string{"instance", "result"},
		Comment: "The total time of handling etcd transactions",
	},
	"pd_operator_finish_total_count": {
		PromQL:  "sum(increase(pd_schedule_finish_operators_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
		Labels:  []string{"type"},
		Comment: "The total count of the operator is finished",
	},
	"pd_operator_finish_total_time": {
		PromQL:  "sum(increase(pd_schedule_finish_operators_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
		Labels:  []string{"type"},
		Comment: "The total time consumed when the operator is finished",
	},
	"pd_operator_step_finish_total_count": {
		PromQL:  "sum(increase(pd_schedule_finish_operator_steps_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
		Labels:  []string{"type"},
		Comment: "The total count of the operator step is finished",
	},
	"pd_operator_step_finish_total_time": {
		PromQL:  "sum(increase(pd_schedule_finish_operator_steps_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
		Labels:  []string{"type"},
		Comment: "The total time consumed when the operator step is finished",
	},
	"pd_peer_round_trip_total_count": {
		PromQL:  "sum(increase(etcd_network_peer_round_trip_time_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,To)",
		Labels:  []string{"instance", "To"},
		Comment: "The total count of the network in .99",
	},
	"pd_peer_round_trip_total_time": {
		PromQL:  "sum(increase(etcd_network_peer_round_trip_time_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,To)",
		Labels:  []string{"instance", "To"},
		Comment: "The total time of latency of the network in .99",
	},
	"pd_region_heartbeat_total_count": {
		PromQL:  "sum(increase(pd_scheduler_region_heartbeat_latency_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store)",
		Labels:  []string{"address", "store"},
		Comment: "The total count of heartbeat latency of each TiKV instance in",
	},
	"pd_region_heartbeat_total_time": {
		PromQL:  "sum(increase(pd_scheduler_region_heartbeat_latency_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,store)",
		Labels:  []string{"address", "store"},
		Comment: "The total time of heartbeat latency of each TiKV instance in",
	},
	"pd_start_tso_wait_total_count": {
		PromQL:  "sum(increase(tidb_pdclient_ts_future_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of the waiting for getting the start timestamp oracle",
	},
	"pd_start_tso_wait_total_time": {
		PromQL:  "sum(increase(tidb_pdclient_ts_future_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of duration of the waiting time for getting the start timestamp oracle",
	},
	"pd_tso_rpc_total_count": {
		PromQL:  "sum(increase(pd_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of a client sending TSO request until received the response.",
	},
	"pd_tso_rpc_total_time": {
		PromQL:  "sum(increase(pd_client_request_handle_requests_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of a client sending TSO request until received the response.",
	},
	"pd_tso_wait_total_count": {
		PromQL:  "sum(increase(pd_client_cmd_handle_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of a client starting to wait for the TS until received the TS result.",
	},
	"pd_tso_wait_total_time": {
		PromQL:  "sum(increase(pd_client_cmd_handle_cmds_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of a client starting to wait for the TS until received the TS result.",
	},
	"tidb_auto_id_request_total_count": {
		PromQL:  "sum(increase(tidb_autoid_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of TiDB auto id requests durations",
	},
	"tidb_auto_id_request_total_time": {
		PromQL:  "sum(increase(tidb_autoid_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of TiDB auto id requests durations",
	},
	"tidb_batch_client_unavailable_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_batch_client_unavailable_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of kv storage batch processing unvailable durations",
	},
	"tidb_batch_client_unavailable_total_time": {
		PromQL:  "sum(increase(tidb_tikvclient_batch_client_unavailable_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of kv storage batch processing unvailable durations",
	},
	"tidb_batch_client_wait_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_batch_wait_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of kv storage batch processing durations",
	},
	"tidb_batch_client_wait_total_time": {
		PromQL:  "sum(increase(tidb_tikvclient_batch_wait_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of kv storage batch processing durations",
	},
	"tidb_compile_total_count": {
		PromQL:  "sum(increase(tidb_session_compile_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of building the query plan(second)",
	},
	"tidb_compile_total_time": {
		PromQL:  "sum(increase(tidb_session_compile_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total time of cost of building the query plan(second)",
	},
	"tidb_cop_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_cop_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of kv storage coprocessor processing durations",
	},
	"tidb_cop_total_time": {
		PromQL:  "sum(increase(tidb_tikvclient_cop_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of kv storage coprocessor processing durations",
	},
	"tidb_ddl_batch_add_index_total_count": {
		PromQL:  "sum(increase(tidb_ddl_batch_add_idx_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of TiDB batch add index durations by histogram buckets",
	},
	"tidb_ddl_batch_add_index_total_time": {
		PromQL:  "sum(increase(tidb_ddl_batch_add_idx_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of TiDB batch add index durations by histogram buckets",
	},
	"tidb_ddl_deploy_syncer_total_count": {
		PromQL:  "sum(increase(tidb_ddl_deploy_syncer_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total count of TiDB ddl schema syncer statistics, including init, start, watch, clear function call",
	},
	"tidb_ddl_deploy_syncer_total_time": {
		PromQL:  "sum(increase(tidb_ddl_deploy_syncer_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total time of TiDB ddl schema syncer statistics, including init, start, watch, clear function call time cost",
	},
	"tidb_ddl_total_count": {
		PromQL:  "sum(increase(tidb_ddl_handle_job_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of TiDB DDL duration statistics",
	},
	"tidb_ddl_total_time": {
		PromQL:  "sum(increase(tidb_ddl_handle_job_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of TiDB DDL duration statistics",
	},
	"tidb_ddl_update_self_version_total_count": {
		PromQL:  "sum(increase(tidb_ddl_update_self_ver_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
		Labels:  []string{"instance", "result"},
		Comment: "The total count of TiDB schema syncer version update",
	},
	"tidb_ddl_update_self_version_total_time": {
		PromQL:  "sum(increase(tidb_ddl_update_self_ver_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
		Labels:  []string{"instance", "result"},
		Comment: "The total time of TiDB schema syncer version update time duration",
	},
	"tidb_ddl_worker_total_count": {
		PromQL:  "sum(increase(tidb_ddl_worker_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result,action)",
		Labels:  []string{"instance", "type", "result", "action"},
		Comment: "The total count of TiDB ddl worker duration",
	},
	"tidb_ddl_worker_total_time": {
		PromQL:  "sum(increase(tidb_ddl_worker_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result,action)",
		Labels:  []string{"instance", "type", "result", "action"},
		Comment: "The total time of TiDB ddl worker duration",
	},
	"tidb_distsql_execution_total_count": {
		PromQL:  "sum(increase(tidb_distsql_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of distsql execution(second)",
	},
	"tidb_distsql_execution_total_time": {
		PromQL:  "sum(increase(tidb_distsql_handle_query_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of distsql execution(second)",
	},
	"tidb_execute_total_count": {
		PromQL:  "sum(increase(tidb_session_execute_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of of TiDB executing the SQL",
	},
	"tidb_execute_total_time": {
		PromQL:  "sum(increase(tidb_session_execute_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total time cost of executing the SQL which does not include the time to get the results of the query(second)",
	},
	"tidb_gc_push_task_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_range_task_push_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of kv storage range worker processing one task duration",
	},
	"tidb_gc_push_task_total_time": {
		PromQL:  "sum(increase(tidb_tikvclient_range_task_push_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of kv storage range worker processing one task duration",
	},
	"tidb_gc_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_gc_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of kv storage garbage collection",
	},
	"tidb_gc_total_time": {
		PromQL:  "sum(increase(tidb_tikvclient_gc_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of kv storage garbage collection time durations",
	},
	"tidb_get_token_total_count": {
		PromQL:  "sum(increase(tidb_server_get_token_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of Duration (us) for getting token, it should be small until concurrency limit is reached(second)",
	},
	"tidb_get_token_total_time": {
		PromQL:  "sum(increase(tidb_server_get_token_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of Duration (us) for getting token, it should be small until concurrency limit is reached(second)",
	},
	"tidb_kv_backoff_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_backoff_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of kv backoff",
	},
	"tidb_kv_backoff_total_time": {
		PromQL:  "sum(increase(tidb_tikvclient_backoff_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of kv backoff time durations(second)",
	},
	"tidb_kv_request_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_request_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,store)",
		Labels:  []string{"instance", "type", "store"},
		Comment: "The total count of kv requests durations by store",
	},
	"tidb_kv_request_total_time": {
		PromQL:  "sum(increase(tidb_tikvclient_request_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,store)",
		Labels:  []string{"instance", "type", "store"},
		Comment: "The total time of kv requests durations by store",
	},
	"tidb_load_schema_total_count": {
		PromQL:  "sum(increase(tidb_domain_load_schema_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB loading schema by instance",
	},
	"tidb_load_schema_total_time": {
		PromQL:  "sum(increase(tidb_domain_load_schema_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of TiDB loading schema time durations by instance",
	},
	"tidb_meta_operation_total_count": {
		PromQL:  "sum(increase(tidb_meta_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total count of TiDB meta operation durations including get/set schema and ddl jobs",
	},
	"tidb_meta_operation_total_time": {
		PromQL:  "sum(increase(tidb_meta_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total time of TiDB meta operation durations including get/set schema and ddl jobs",
	},
	"tidb_new_etcd_session_total_count": {
		PromQL:  "sum(increase(tidb_owner_new_session_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total count of TiDB new session durations for new etcd sessions",
	},
	"tidb_new_etcd_session_total_time": {
		PromQL:  "sum(increase(tidb_owner_new_session_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total time of TiDB new session durations for new etcd sessions",
	},
	"tidb_owner_handle_syncer_total_count": {
		PromQL:  "sum(increase(tidb_ddl_owner_handle_syncer_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total count of TiDB ddl owner operations on etcd ",
	},
	"tidb_owner_handle_syncer_total_time": {
		PromQL:  "sum(increase(tidb_ddl_owner_handle_syncer_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total time of TiDB ddl owner time operations on etcd duration statistics ",
	},
	"tidb_parse_total_count": {
		PromQL:  "sum(increase(tidb_session_parse_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of parsing SQL to AST(second)",
	},
	"tidb_parse_total_time": {
		PromQL:  "sum(increase(tidb_session_parse_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total time cost of parsing SQL to AST(second)",
	},
	"tidb_query_total_count": {
		PromQL:  "sum(increase(tidb_server_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of TiDB query durations(second)",
	},
	"tidb_query_total_time": {
		PromQL:  "sum(increase(tidb_server_handle_query_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total time of TiDB query durations(second)",
	},
	"tidb_slow_query_cop_process_total_count": {
		PromQL:  "sum(increase(tidb_server_slow_query_cop_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB slow query cop process",
	},
	"tidb_slow_query_cop_process_total_time": {
		PromQL:  "sum(increase(tidb_server_slow_query_cop_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of TiDB slow query statistics with slow query total cop process time(second)",
	},
	"tidb_slow_query_cop_wait_total_count": {
		PromQL:  "sum(increase(tidb_server_slow_query_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB slow query cop wait",
	},
	"tidb_slow_query_cop_wait_total_time": {
		PromQL:  "sum(increase(tidb_server_slow_query_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of TiDB slow query statistics with slow query total cop wait time(second)",
	},
	"tidb_slow_query_total_count": {
		PromQL:  "sum(increase(tidb_server_slow_query_process_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB slow query",
	},
	"tidb_slow_query_total_time": {
		PromQL:  "sum(increase(tidb_server_slow_query_process_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of TiDB slow query statistics with slow query time(second)",
	},
	"tidb_statistics_auto_analyze_total_count": {
		PromQL:  "sum(increase(tidb_statistics_auto_analyze_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB auto analyze",
	},
	"tidb_statistics_auto_analyze_total_time": {
		PromQL:  "sum(increase(tidb_statistics_auto_analyze_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of TiDB auto analyze time durations within 95 percent histogram buckets",
	},
	"tidb_transaction_local_latch_wait_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_local_latch_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB transaction latch wait on key value storage(second)",
	},
	"tidb_transaction_local_latch_wait_total_time": {
		PromQL:  "sum(increase(tidb_tikvclient_local_latch_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of TiDB transaction latch wait time on key value storage(second)",
	},
	"tidb_transaction_total_count": {
		PromQL:  "sum(increase(tidb_session_transaction_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,sql_type)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "The total count of transaction execution durations, including retry(second)",
	},
	"tidb_transaction_total_time": {
		PromQL:  "sum(increase(tidb_session_transaction_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,sql_type)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "The total time of transaction execution durations, including retry(second)",
	},
	"tikv_append_log_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_append_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of Raft appends log",
	},
	"tikv_append_log_total_time": {
		PromQL:  "sum(increase(tikv_raftstore_append_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of Raft appends log",
	},
	"tikv_apply_log_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_apply_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of Raft applies log",
	},
	"tikv_apply_log_total_time": {
		PromQL:  "sum(increase(tikv_raftstore_apply_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of Raft applies log",
	},
	"tikv_apply_wait_total_count": {
		PromQL: "sum(increase(tikv_raftstore_apply_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_apply_wait_total_time": {
		PromQL: "sum(increase(tikv_raftstore_apply_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_backup_range_total_count": {
		PromQL: "sum(increase(tikv_backup_range_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels: []string{"instance", "type"},
	},
	"tikv_backup_range_total_time": {
		PromQL: "sum(increase(tikv_backup_range_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels: []string{"instance", "type"},
	},
	"tikv_backup_total_count": {
		PromQL: "sum(increase(tikv_backup_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_backup_total_time": {
		PromQL: "sum(increase(tikv_backup_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_check_split_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_check_split_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of running split check",
	},
	"tikv_check_split_total_time": {
		PromQL:  "sum(increase(tikv_raftstore_check_split_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of time consumed when running split check in .9999",
	},
	"tikv_commit_log_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_commit_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of Raft commits log",
	},
	"tikv_commit_log_total_time": {
		PromQL:  "sum(increase(tikv_raftstore_commit_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of Raft commits log",
	},
	"tikv_cop_handle_total_count": {
		PromQL:  "sum(increase(tikv_coprocessor_request_handle_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total count of tikv coprocessor handling coprocessor requests",
	},
	"tikv_cop_handle_total_time": {
		PromQL:  "sum(increase(tikv_coprocessor_request_handle_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total time of time consumed when handling coprocessor requests",
	},
	"tikv_cop_request_total_count": {
		PromQL:  "sum(increase(tikv_coprocessor_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total count of tikv handle coprocessor read requests",
	},
	"tikv_cop_request_total_time": {
		PromQL:  "sum(increase(tikv_coprocessor_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total time of time consumed to handle coprocessor read requests",
	},
	"tikv_cop_wait_total_count": {
		PromQL:  "sum(increase(tikv_coprocessor_request_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total count of coprocessor requests that wait for being handled",
	},
	"tikv_cop_wait_total_time": {
		PromQL:  "sum(increase(tikv_coprocessor_request_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total time of time consumed when coprocessor requests are wait for being handled",
	},
	"tikv_raft_store_events_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_event_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of raftstore events (P99).99",
	},
	"tikv_raft_store_events_total_time": {
		PromQL:  "sum(increase(tikv_raftstore_event_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of raftstore events (P99).99",
	},
	"tikv_gc_tasks_total_count": {
		PromQL:  "sum(increase(tikv_gcworker_gc_task_duration_vec_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,task)",
		Labels:  []string{"instance", "task"},
		Comment: "The total count of executing GC tasks",
	},
	"tikv_gc_tasks_total_time": {
		PromQL:  "sum(increase(tikv_gcworker_gc_task_duration_vec_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,task)",
		Labels:  []string{"instance", "task"},
		Comment: "The total time of time consumed when executing GC tasks",
	},
	"tikv_grpc_messge_total_count": {
		PromQL:  "sum(increase(tikv_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of tikv execution gRPC message",
	},
	"tikv_grpc_messge_total_time": {
		PromQL:  "sum(increase(tikv_grpc_msg_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of execution time of gRPC message",
	},
	"tikv_handle_snapshot_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_snapshot_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of tikv handling snapshots",
	},
	"tikv_handle_snapshot_total_time": {
		PromQL:  "sum(increase(tikv_raftstore_snapshot_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of time consumed when handling snapshots",
	},
	"tikv_ingest_sst_total_count": {
		PromQL:  "sum(increase(tikv_snapshot_ingest_sst_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,db)",
		Labels:  []string{"instance", "db"},
		Comment: "The total count of ingesting SST files",
	},
	"tikv_ingest_sst_total_time": {
		PromQL:  "sum(increase(tikv_snapshot_ingest_sst_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,db)",
		Labels:  []string{"instance", "db"},
		Comment: "The total time of time consumed when ingesting SST files",
	},
	"tikv_lock_manager_deadlock_detect_total_count": {
		PromQL: "sum(increase(tikv_lock_manager_detect_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_lock_manager_deadlock_detect_total_time": {
		PromQL: "sum(increase(tikv_lock_manager_detect_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_lock_manager_waiter_lifetime_total_count": {
		PromQL: "sum(increase(tikv_lock_manager_waiter_lifetime_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_lock_manager_waiter_lifetime_total_time": {
		PromQL: "sum(increase(tikv_lock_manager_waiter_lifetime_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_process_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_raft_process_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of peer processes in Raft",
	},
	"tikv_process_total_time": {
		PromQL:  "sum(increase(tikv_raftstore_raft_process_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of peer processes in Raft",
	},
	"tikv_propose_wait_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_request_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of each proposal",
	},
	"tikv_propose_wait_total_time": {
		PromQL:  "sum(increase(tikv_raftstore_request_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of wait time of each proposal",
	},
	"tikv_scheduler_command_total_count": {
		PromQL:  "sum(increase(tikv_scheduler_command_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of tikv scheduler executing command",
	},
	"tikv_scheduler_command_total_time": {
		PromQL:  "sum(increase(tikv_scheduler_command_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of time consumed when executing command",
	},
	"tikv_scheduler_latch_wait_total_count": {
		PromQL:  "sum(increase(tikv_scheduler_latch_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count which is caused by latch wait in command",
	},
	"tikv_scheduler_latch_wait_total_time": {
		PromQL:  "sum(increase(tikv_scheduler_latch_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time which is caused by latch wait in command",
	},
	"tikv_send_snapshot_total_count": {
		PromQL:  "sum(increase(tikv_server_send_snapshot_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of sending snapshots",
	},
	"tikv_send_snapshot_total_time": {
		PromQL:  "sum(increase(tikv_server_send_snapshot_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of time consumed when sending snapshots",
	},
	"tikv_storage_async_request_total_count": {
		PromQL:  "sum(increase(tikv_storage_engine_async_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of processing asynchronous snapshot requests",
	},
	"tikv_storage_async_request_total_time": {
		PromQL:  "sum(increase(tikv_storage_engine_async_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of time consumed by processing asynchronous snapshot requests",
	},

	"tidb_distsql_partial_num_total_count": {
		PromQL:  "sum(increase(tidb_distsql_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of distsql partial numbers per query",
	},
	"tidb_distsql_partial_scan_key_num_total_count": {
		PromQL:  "sum(increase(tidb_distsql_scan_keys_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of distsql partial scan key numbers",
	},
	"tidb_distsql_partial_scan_key_total_num": {
		PromQL:  "sum(increase(tidb_distsql_scan_keys_partial_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of distsql partial scan key numbers",
	},
	"tidb_distsql_partial_total_num": {
		PromQL:  "sum(increase(tidb_distsql_partial_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of distsql partial numbers per query",
	},
	"tidb_distsql_scan_key_num_total_count": {
		PromQL:  "sum(increase(tidb_distsql_scan_keys_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of distsql scan numbers",
	},
	"tidb_distsql_scan_key_total_num": {
		PromQL:  "sum(increase(tidb_distsql_scan_keys_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of distsql scan numbers",
	},
	"tidb_kv_write_num_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_txn_write_kv_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of kv write in transaction execution",
	},
	"tidb_kv_write_size_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_txn_write_size_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of kv write size per transaction execution",
	},
	"tidb_kv_write_total_num": {
		PromQL:  "sum(increase(tidb_tikvclient_txn_write_kv_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of kv write in transaction execution",
	},
	"tidb_kv_write_total_size": {
		PromQL:  "sum(increase(tidb_tikvclient_txn_write_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total kv write size in transaction execution",
	},
	"tidb_statistics_fast_analyze_status_total_count": {
		PromQL:  "sum(increase(tidb_statistics_fast_analyze_status_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of TiDB fast analyze statistics ",
	},
	"tidb_statistics_fast_analyze_total_status": {
		PromQL:  "sum(increase(tidb_statistics_fast_analyze_status_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of TiDB fast analyze statistics ",
	},
	"tidb_statistics_stats_inaccuracy_rate_total_count": {
		PromQL:  "sum(increase(tidb_statistics_stats_inaccuracy_rate_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB statistics inaccurate rate",
	},
	"tidb_statistics_stats_inaccuracy_total_rate": {
		PromQL:  "sum(increase(tidb_statistics_stats_inaccuracy_rate_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of TiDB statistics inaccurate rate",
	},
	"tidb_transaction_retry_num_total_count": {
		PromQL:  "sum(increase(tidb_session_retry_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of TiDB transaction retry num",
	},
	"tidb_transaction_retry_total_num": {
		PromQL:  "sum(increase(tidb_session_retry_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of TiDB transaction retry num",
	},
	"tidb_transaction_statement_num_total_count": {
		PromQL:  "sum(increase(tidb_session_transaction_statement_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of TiDB statements numbers within one transaction. Internal means TiDB inner transaction",
	},
	"tidb_transaction_statement_total_num": {
		PromQL:  "sum(increase(tidb_session_transaction_statement_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total num of TiDB statements numbers within one transaction. Internal means TiDB inner transaction",
	},
	"tidb_txn_region_num_total_count": {
		PromQL:  "sum(increase(tidb_tikvclient_txn_regions_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of regions transaction operates on count",
	},
	"tidb_txn_region_total_num": {
		PromQL:  "sum(increase(tidb_tikvclient_txn_regions_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of regions transaction operates on count",
	},
	"tikv_approximate_region_size_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_region_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of approximate Region size, the default value is P99",
	},
	"tikv_approximate_region_total_size": {
		PromQL:  "sum(increase(tikv_raftstore_region_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total size of approximate Region size",
	},
	"tikv_backup_range_size_total_count": {
		PromQL: "sum(increase(tikv_backup_range_size_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,cf)",
		Labels: []string{"instance", "cf"},
	},
	"tikv_backup_range_total_size": {
		PromQL: "sum(increase(tikv_backup_range_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,cf)",
		Labels: []string{"instance", "cf"},
	},
	"tikv_cop_kv_cursor_operations_total_count": {
		PromQL: "sum(increase(tikv_coprocessor_scan_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels: []string{"instance", "req"},
	},
	"tikv_cop_scan_keys_total_num": {
		PromQL: "sum(increase(tikv_coprocessor_scan_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels: []string{"instance", "req"},
	},
	"tikv_cop_total_response_total_size": {
		PromQL: `sum(increase(tikv_coprocessor_response_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels: []string{"instance"},
	},
	"tikv_grpc_req_batch_size_total_count": {
		PromQL: "sum(increase(tikv_server_grpc_req_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_grpc_req_batch_total_size": {
		PromQL: "sum(increase(tikv_server_grpc_req_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_grpc_resp_batch_size_total_count": {
		PromQL: "sum(increase(tikv_server_grpc_resp_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_grpc_resp_batch_total_size": {
		PromQL: "sum(increase(tikv_server_grpc_resp_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_raft_message_batch_size_total_count": {
		PromQL: "sum(increase(tikv_server_raft_message_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_raft_message_batch_total_size": {
		PromQL: "sum(increase(tikv_server_raft_message_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"tikv_raft_proposals_per_ready_total_count": {
		PromQL:  "sum(increase(tikv_raftstore_apply_proposal_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of proposal count of all Regions in a mio tick",
	},
	"tikv_raft_proposals_per_total_ready": {
		PromQL:  "sum(increase(tikv_raftstore_apply_proposal_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total proposal count of all Regions in a mio tick",
	},
	"tikv_request_batch_ratio_total_count": {
		PromQL:  "sum(increase(tikv_server_request_batch_ratio_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of request batch output to input per TiKV instance",
	},
	"tikv_request_batch_size_total_count": {
		PromQL:  "sum(increase(tikv_server_request_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of request batch per TiKV instance",
	},
	"tikv_request_batch_total_ratio": {
		PromQL:  "sum(increase(tikv_server_request_batch_ratio_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total ratio of request batch output to input per TiKV instance",
	},
	"tikv_request_batch_total_size": {
		PromQL:  "sum(increase(tikv_server_request_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total size of requests into request batch per TiKV instance",
	},
	"tikv_scheduler_keys_read_total_count": {
		PromQL:  "sum(increase(tikv_scheduler_kv_command_key_read_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of keys read by a command",
	},
	"tikv_scheduler_keys_total_read": {
		PromQL:  "sum(increase(tikv_scheduler_kv_command_key_read_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of keys read by command",
	},
	"tikv_scheduler_keys_total_written": {
		PromQL:  "sum(increase(tikv_scheduler_kv_command_key_write_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of keys written by a command",
	},
	"tikv_scheduler_keys_written_total_count": {
		PromQL:  "sum(increase(tikv_scheduler_kv_command_key_write_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of keys written by a command",
	},
	"tikv_snapshot_kv_count_total_count": {
		PromQL:  "sum(increase(tikv_snapshot_kv_count_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of number of KV within a snapshot",
	},
	"tikv_snapshot_kv_total_count": {
		PromQL:  "sum(increase(tikv_snapshot_kv_count_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total number of KV within a snapshot",
	},
	"tikv_snapshot_size_total_count": {
		PromQL:  "sum(increase(tikv_snapshot_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of snapshot size",
	},
	"tikv_snapshot_total_size": {
		PromQL:  "sum(increase(tikv_snapshot_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total size of snapshot size",
	},
	"tikv_config_rocksdb": {
		PromQL:  "tikv_config_rocksdb{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "cf", "name"},
		Comment: "TiKV rocksdb config value",
	},
	"tikv_config_raftstore": {
		PromQL:  "tikv_config_raftstore{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "name"},
		Comment: "TiKV rocksdb config value",
	},
}
