// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"github.com/pingcap/failpoint"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

type inspectionSummaryRetriever struct {
	dummyCloser
	retrieved bool
	table     *model.TableInfo
	extractor *plannercore.InspectionSummaryTableExtractor
	timeRange plannercore.QueryTimeRange
}

// inspectionSummaryRules is used to maintain
var inspectionSummaryRules = map[string][]string{
	"query-summary": {
		"tidb_connection_count",
		"tidb_query_duration",
		"tidb_qps_ideal",
		"tidb_qps",
		"tidb_ops_internal",
		"tidb_ops_statement",
		"tidb_failed_query_opm",
		"tidb_slow_query_duration",
		"tidb_slow_query_cop_wait_duration",
		"tidb_slow_query_cop_process_duration",
	},
	"wait-events": {
		"tidb_get_token_duration",
		"tidb_load_schema_duration",
		"tidb_query_duration",
		"tidb_parse_duration",
		"tidb_compile_duration",
		"tidb_execute_duration",
		"tidb_auto_id_request_duration",
		"pd_tso_wait_duration",
		"pd_tso_rpc_duration",
		"tidb_distsql_execution_duration",
		"pd_start_tso_wait_duration",
		"tidb_transaction_local_latch_wait_duration",
		"tidb_transaction_duration",
		"pd_handle_request_duration",
		"tidb_cop_duration",
		"tidb_batch_client_wait_duration",
		"tidb_batch_client_unavailable_duration",
		"tidb_kv_backoff_duration",
		"tidb_kv_request_duration",
		"pd_client_cmd_duration",
		"tikv_grpc_messge_duration",
		"tikv_average_grpc_messge_duration",
		"tikv_channel_full",
		"tikv_scheduler_is_busy",
		"tikv_coprocessor_is_busy",
		"tikv_engine_write_stall",
		"tikv_apply_log_avg_duration",
		"tikv_apply_log_duration",
		"tikv_append_log_avg_duration",
		"tikv_append_log_duration",
		"tikv_commit_log_avg_duration",
		"tikv_commit_log_duration",
		"tikv_process_duration",
		"tikv_propose_wait_duration",
		"tikv_propose_avg_wait_duration",
		"tikv_apply_wait_duration",
		"tikv_apply_avg_wait_duration",
		"tikv_check_split_duration",
		"tikv_storage_async_request_duration",
		"tikv_storage_async_request_avg_duration",
		"tikv_scheduler_command_duration",
		"tikv_scheduler_command_avg_duration",
		"tikv_scheduler_latch_wait_duration",
		"tikv_scheduler_latch_wait_avg_duration",
		"tikv_send_snapshot_duration",
		"tikv_handle_snapshot_duration",
		"tikv_cop_request_durations",
		"tikv_cop_request_duration",
		"tikv_cop_handle_duration",
		"tikv_cop_wait_duration",
		"tikv_engine_max_get_duration",
		"tikv_engine_avg_get_duration",
		"tikv_engine_avg_seek_duration",
		"tikv_engine_write_duration",
		"tikv_wal_sync_max_duration",
		"tikv_wal_sync_duration",
		"tikv_compaction_max_duration",
		"tikv_compaction_duration",
		"tikv_sst_read_max_duration",
		"tikv_sst_read_duration",
		"tikv_write_stall_max_duration",
		"tikv_write_stall_avg_duration",
		"tikv_oldest_snapshots_duration",
		"tikv_ingest_sst_duration",
		"tikv_ingest_sst_avg_duration",
		"tikv_engine_blob_seek_duration",
		"tikv_engine_blob_get_duration",
		"tikv_engine_blob_file_read_duration",
		"tikv_engine_blob_file_write_duration",
		"tikv_engine_blob_file_sync_duration",
		"tikv_lock_manager_waiter_lifetime_avg_duration",
		"tikv_lock_manager_deadlock_detect_duration",
		"tikv_lock_manager_deadlock_detect_avg_duration",
	},
	"read-link": {
		"tidb_get_token_duration",
		"tidb_parse_duration",
		"tidb_compile_duration",
		"pd_tso_rpc_duration",
		"pd_tso_wait_duration",
		"tidb_execute_duration",
		"tidb_expensive_executors_ops",
		"tidb_query_using_plan_cache_ops",
		"tidb_distsql_execution_duration",
		"tidb_distsql_partial_num",
		"tidb_distsql_partial_qps",
		"tidb_distsql_partial_scan_key_num",
		"tidb_distsql_qps",
		"tidb_distsql_scan_key_num",
		"tidb_region_cache_ops",
		"tidb_batch_client_pending_req_count",
		"tidb_batch_client_unavailable_duration",
		"tidb_batch_client_wait_duration",
		"tidb_kv_backoff_duration",
		"tidb_kv_backoff_ops",
		"tidb_kv_region_error_ops",
		"tidb_kv_request_duration",
		"tidb_kv_request_ops",
		"tidb_kv_snapshot_ops",
		"tidb_kv_txn_ops",
		"tikv_average_grpc_messge_duration",
		"tikv_grpc_avg_req_batch_size",
		"tikv_grpc_avg_resp_batch_size",
		"tikv_grpc_errors",
		"tikv_grpc_messge_duration",
		"tikv_grpc_qps",
		"tikv_grpc_req_batch_size",
		"tikv_grpc_resp_batch_size",
		"tidb_cop_duration",
		"tikv_cop_wait_duration",
		"tikv_coprocessor_is_busy",
		"tikv_coprocessor_request_error",
		"tikv_cop_handle_duration",
		"tikv_cop_kv_cursor_operations",
		"tikv_cop_request_duration",
		"tikv_cop_request_durations",
		"tikv_cop_scan_details",
		"tikv_cop_dag_executors_ops",
		"tikv_cop_dag_requests_ops",
		"tikv_cop_scan_keys_num",
		"tikv_cop_requests_ops",
		"tikv_cop_total_response_size_per_seconds",
		"tikv_cop_total_rocksdb_perf_statistics",
		"tikv_channel_full",
		"tikv_engine_avg_get_duration",
		"tikv_engine_avg_seek_duration",
		"tikv_handle_snapshot_duration",
		"tikv_block_all_cache_hit",
		"tikv_block_bloom_prefix_cache_hit",
		"tikv_block_cache_size",
		"tikv_block_data_cache_hit",
		"tikv_block_filter_cache_hit",
		"tikv_block_index_cache_hit",
		"tikv_engine_get_block_cache_operations",
		"tikv_engine_get_cpu_cache_operations",
		"tikv_engine_get_memtable_operations",
		"tikv_per_read_avg_bytes",
		"tikv_per_read_max_bytes",
	},
	"write-link": {
		"tidb_get_token_duration",
		"tidb_parse_duration",
		"tidb_compile_duration",
		"pd_tso_rpc_duration",
		"pd_tso_wait_duration",
		"tidb_execute_duration",
		"tidb_transaction_duration",
		"tidb_transaction_local_latch_wait_duration",
		"tidb_transaction_ops",
		"tidb_transaction_retry_error_ops",
		"tidb_transaction_retry_num",
		"tidb_transaction_statement_num",
		"tidb_auto_id_qps",
		"tidb_auto_id_request_duration",
		"tidb_region_cache_ops",
		"tidb_kv_backoff_duration",
		"tidb_kv_backoff_ops",
		"tidb_kv_region_error_ops",
		"tidb_kv_request_duration",
		"tidb_kv_request_ops",
		"tidb_kv_snapshot_ops",
		"tidb_kv_txn_ops",
		"tidb_kv_write_num",
		"tidb_kv_write_size",
		"tikv_average_grpc_messge_duration",
		"tikv_grpc_avg_req_batch_size",
		"tikv_grpc_avg_resp_batch_size",
		"tikv_grpc_errors",
		"tikv_grpc_messge_duration",
		"tikv_grpc_qps",
		"tikv_grpc_req_batch_size",
		"tikv_grpc_resp_batch_size",
		"tikv_scheduler_command_avg_duration",
		"tikv_scheduler_command_duration",
		"tikv_scheduler_is_busy",
		"tikv_scheduler_keys_read_avg",
		"tikv_scheduler_keys_read",
		"tikv_scheduler_keys_written_avg",
		"tikv_scheduler_keys_written",
		"tikv_scheduler_latch_wait_avg_duration",
		"tikv_scheduler_latch_wait_duration",
		"tikv_scheduler_pending_commands",
		"tikv_scheduler_priority_commands",
		"tikv_scheduler_scan_details",
		"tikv_scheduler_stage",
		"tikv_scheduler_writing_bytes",
		"tikv_propose_avg_wait_duration",
		"tikv_propose_wait_duration",
		"tikv_append_log_avg_duration",
		"tikv_append_log_duration",
		"tikv_commit_log_avg_duration",
		"tikv_commit_log_duration",
		"tikv_apply_avg_wait_duration",
		"tikv_apply_log_avg_duration",
		"tikv_apply_log_duration",
		"tikv_apply_wait_duration",
		"tikv_engine_wal_sync_operations",
		"tikv_engine_write_duration",
		"tikv_engine_write_operations",
		"tikv_engine_write_stall",
		"tikv_write_stall_avg_duration",
		"tikv_write_stall_max_duration",
		"tikv_write_stall_reason",
	},
	"ddl": {
		"tidb_ddl_add_index_speed",
		"tidb_ddl_batch_add_index_duration",
		"tidb_ddl_deploy_syncer_duration",
		"tidb_ddl_duration",
		"tidb_ddl_meta_opm",
		"tidb_ddl_opm",
		"tidb_ddl_update_self_version_duration",
		"tidb_ddl_waiting_jobs_num",
		"tidb_ddl_worker_duration",
	},
	"stats": {
		"tidb_statistics_auto_analyze_duration",
		"tidb_statistics_auto_analyze_ops",
		"tidb_statistics_dump_feedback_ops",
		"tidb_statistics_fast_analyze_status",
		"tidb_statistics_pseudo_estimation_ops",
		"tidb_statistics_significant_feedback",
		"tidb_statistics_stats_inaccuracy_rate",
		"tidb_statistics_store_query_feedback_qps",
		"tidb_statistics_update_stats_ops",
	},
	"gc": {
		"tidb_gc_action_result_opm",
		"tidb_gc_config",
		"tidb_gc_delete_range_fail_opm",
		"tidb_gc_delete_range_task_status",
		"tidb_gc_duration",
		"tidb_gc_fail_opm",
		"tidb_gc_push_task_duration",
		"tidb_gc_too_many_locks_opm",
		"tidb_gc_worker_action_opm",
		"tikv_engine_blob_gc_duration",
		"tikv_auto_gc_progress",
		"tikv_auto_gc_safepoint",
		"tikv_auto_gc_working",
		"tikv_gc_fail_tasks",
		"tikv_gc_keys",
		"tikv_gc_skipped_tasks",
		"tikv_gc_speed",
		"tikv_gc_tasks_avg_duration",
		"tikv_gc_tasks_duration",
		"tikv_gc_too_busy",
		"tikv_gc_tasks_ops",
	},
	"rocksdb": {
		"tikv_compaction_duration",
		"tikv_compaction_max_duration",
		"tikv_compaction_operations",
		"tikv_compaction_pending_bytes",
		"tikv_compaction_reason",
		"tikv_write_stall_avg_duration",
		"tikv_write_stall_max_duration",
		"tikv_write_stall_reason",
		"store_available_ratio",
		"store_size_amplification",
		"tikv_engine_avg_get_duration",
		"tikv_engine_avg_seek_duration",
		"tikv_engine_blob_bytes_flow",
		"tikv_engine_blob_file_count",
		"tikv_engine_blob_file_read_duration",
		"tikv_engine_blob_file_size",
		"tikv_engine_blob_file_sync_duration",
		"tikv_engine_blob_file_sync_operations",
		"tikv_engine_blob_file_write_duration",
		"tikv_engine_blob_gc_bytes_flow",
		"tikv_engine_blob_gc_duration",
		"tikv_engine_blob_gc_file",
		"tikv_engine_blob_gc_keys_flow",
		"tikv_engine_blob_get_duration",
		"tikv_engine_blob_key_avg_size",
		"tikv_engine_blob_key_max_size",
		"tikv_engine_blob_seek_duration",
		"tikv_engine_blob_seek_operations",
		"tikv_engine_blob_value_avg_size",
		"tikv_engine_blob_value_max_size",
		"tikv_engine_compaction_flow_bytes",
		"tikv_engine_get_block_cache_operations",
		"tikv_engine_get_cpu_cache_operations",
		"tikv_engine_get_memtable_operations",
		"tikv_engine_live_blob_size",
		"tikv_engine_max_get_duration",
		"tikv_engine_max_seek_duration",
		"tikv_engine_seek_operations",
		"tikv_engine_size",
		"tikv_engine_wal_sync_operations",
		"tikv_engine_write_duration",
		"tikv_engine_write_operations",
		"tikv_engine_write_stall",
	},
	"pd": {
		"pd_scheduler_balance_region",
		"pd_balance_scheduler_status",
		"pd_checker_event_count",
		"pd_client_cmd_duration",
		"pd_client_cmd_ops",
		"pd_cluster_metadata",
		"pd_cluster_status",
		"pd_grpc_completed_commands_duration",
		"pd_grpc_completed_commands_rate",
		"pd_handle_request_duration",
		"pd_handle_request_ops",
		"pd_handle_request_duration_avg",
		"pd_handle_transactions_duration",
		"pd_handle_transactions_rate",
		"pd_hotspot_status",
		"pd_label_distribution",
		"pd_operator_finish_duration",
		"pd_operator_step_finish_duration",
		"pd_peer_round_trip_duration",
		"pd_region_health",
		"pd_region_heartbeat_duration",
		"pd_region_label_isolation_level",
		"pd_region_syncer_status",
		"pd_role",
		"pd_schedule_filter",
		"pd_schedule_operator",
		"pd_schedule_store_limit",
		"pd_scheduler_balance_direction",
		"pd_scheduler_balance_leader",
		"pd_scheduler_config",
		"pd_scheduler_op_influence",
		"pd_scheduler_region_heartbeat",
		"pd_scheduler_status",
		"pd_scheduler_store_status",
		"pd_scheduler_tolerant_resource",
		"pd_server_etcd_state",
		"pd_start_tso_wait_duration",
	},
	"raftstore": {
		"tikv_approximate_avg_region_size",
		"tikv_approximate_region_size_histogram",
		"tikv_approximate_region_size",
		"tikv_append_log_avg_duration",
		"tikv_append_log_duration",
		"tikv_commit_log_avg_duration",
		"tikv_commit_log_duration",
		"tikv_apply_avg_wait_duration",
		"tikv_apply_log_avg_duration",
		"tikv_apply_log_duration",
		"tikv_apply_wait_duration",
		"tikv_process_duration",
		"tikv_process_handled",
		"tikv_propose_avg_wait_duration",
		"tikv_propose_wait_duration",
		"tikv_raft_dropped_messages",
		"tikv_raft_log_speed",
		"tikv_raft_message_avg_batch_size",
		"tikv_raft_message_batch_size",
		"tikv_raft_proposals_per_ready",
		"tikv_raft_proposals",
		"tikv_raft_sent_messages",
	},
}

func (e *inspectionSummaryRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipInspection {
		return nil, nil
	}
	e.retrieved = true
	rules := inspectionFilter{set: e.extractor.Rules}
	names := inspectionFilter{set: e.extractor.MetricNames}

	serversInfo, err := infoschema.GetClusterServerInfo(sctx)
	failpoint.Inject("mockClusterServerInfo", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			// erase the error
			serversInfo, err = parseFailpointServerInfo(s), nil
		}
	})
	if err != nil {
		return nil, err
	}

	condition := e.timeRange.Condition()
	var finalRows [][]types.Datum
	clusterInfo := make(map[string]string)
	for _, v := range serversInfo {
		clusterInfo[v.StatusAddr] = clusterInfo[v.Address]
	}
	for rule, tables := range inspectionSummaryRules {
		if !rules.exist(rule) {
			continue
		}
		for _, name := range tables {
			if !names.enable(name) {
				continue
			}
			def, found := infoschema.MetricTableMap[name]
			if !found {
				sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("metrics table: %s not found", name))
				continue
			}
			cols := def.Labels
			cond := condition
			if def.Quantile > 0 {
				cols = append(cols, "quantile")
				if len(e.extractor.Quantiles) > 0 {
					qs := make([]string, len(e.extractor.Quantiles))
					for i, q := range e.extractor.Quantiles {
						qs[i] = fmt.Sprintf("%f", q)
					}
					cond += " and quantile in (" + strings.Join(qs, ",") + ")"
				} else {
					cond += " and quantile=0.99"
				}
			}
			var sql string
			if len(cols) > 0 {
				sql = fmt.Sprintf("select avg(value),min(value),max(value),`%s` from `%s`.`%s` %s group by `%[1]s` order by `%[1]s`",
					strings.Join(cols, "`,`"), util.MetricSchemaName.L, name, cond)
			} else {
				sql = fmt.Sprintf("select avg(value),min(value),max(value) from `%s`.`%s` %s",
					util.MetricSchemaName.L, name, cond)
			}
			rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(ctx, sql)
			if err != nil {
				return nil, errors.Errorf("execute '%s' failed: %v", sql, err)
			}
			nonInstanceLabelIndex := 0
			if len(def.Labels) > 0 && def.Labels[0] == "instance" {
				nonInstanceLabelIndex = 1
			}
			// skip min/max/avg
			const skipCols = 3
			for _, row := range rows {
				instance := ""
				if nonInstanceLabelIndex > 0 {
					instance = row.GetString(skipCols) // skip min/max/avg
				}
				var labels []string
				for i, label := range def.Labels[nonInstanceLabelIndex:] {
					// skip min/max/avg/instance
					val := row.GetString(skipCols + nonInstanceLabelIndex + i)
					if label == "store" || label == "store_id" {
						val = fmt.Sprintf("store_id:%s", val)
					}
					labels = append(labels, val)
				}
				var quantile interface{}
				if def.Quantile > 0 {
					quantile = row.GetFloat64(row.Len() - 1) // quantile will be the last column
				}
				//clusterInfo = sctx.GetSessionVars().ClusterAddrInfo
				if _, ok := clusterInfo[instance]; ok {
					instance = clusterInfo[instance]
				}
				finalRows = append(finalRows, types.MakeDatums(
					rule,
					instance,
					name,
					strings.Join(labels, ", "),
					quantile,
					row.GetFloat64(0), // avg
					row.GetFloat64(1), // min
					row.GetFloat64(2), // max
				))
			}
		}
	}
	return finalRows, nil
}
