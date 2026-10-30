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

//! The `INSPECTION_RULES` and `INSPECTION_SUMMARY` memory tables: Go
//! `inspectionRuleRetriever` (`pkg/executor/inspection_common.go`) and
//! `inspectionSummaryRetriever` (`pkg/executor/inspection_summary.go`).

use tidb_datatype::Datum;
use crate::DriverError;
use tidb_planner_core_operator_logicalop::mem_table::QueryTimeRange;
use tidb_planner::memtable_predicate_extractor::InspectionSummaryTableExtractor;

/// Go `inspectionRuleTypeInspection`.
const INSPECTION_RULE_TYPE_INSPECTION: &str = "inspection";
/// Go `inspectionRuleTypeSummary`.
const INSPECTION_RULE_TYPE_SUMMARY: &str = "summary";

/// The names of Go `inspectionRules` (`inspection_result.go`), in order.
const INSPECTION_RULE_NAMES: &[&str] = &[
    "config",
    "version",
    "node-load",
    "critical-error",
    "threshold-check",
];

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

/// Go `inspectionRuleRetriever.retrieve`: the inspection rules, then the
/// summary rules by name. The `TYPE` filter (and `SkipRequest`) is the scan's
/// extractor, applied to these rows as the scan reads them.
pub fn inspection_rules_rows() -> Vec<Vec<Datum>> {
    let mut summary_rules = INSPECTION_SUMMARY_RULES
        .iter()
        .map(|(rule, _)| *rule)
        .collect::<Vec<_>>();
    summary_rules.sort_unstable();
    INSPECTION_RULE_NAMES
        .iter()
        .map(|name| (*name, INSPECTION_RULE_TYPE_INSPECTION))
        .chain(
            summary_rules
                .into_iter()
                .map(|name| (name, INSPECTION_RULE_TYPE_SUMMARY)),
        )
        .map(|(name, rule_type)| vec![text(name), text(rule_type), text("")])
        .collect()
}

/// Go `inspectionSummaryRetriever.retrieve` for one scan: each metrics table
/// of each summary rule the extractor admits, summarised by the restricted
/// SQL `query` runs over `metrics_schema`. Go walks its rule map in random
/// order; this walks the rules by name.
pub fn inspection_summary_rows(
    extractor: &InspectionSummaryTableExtractor,
    time_range: &QueryTimeRange,
    mut query: impl FnMut(&str) -> Result<Vec<Vec<Datum>>, DriverError>,
    mut warn: impl FnMut(String),
) -> Result<Vec<Vec<Datum>>, DriverError> {
    if extractor.skip_inspection {
        return Ok(Vec::new());
    }
    // Go `QueryTimeRange.Condition`.
    let condition = format!(
        "where time>='{}' and time<='{}'",
        time_range.from, time_range.to
    );
    let mut rules = INSPECTION_SUMMARY_RULES.to_vec();
    rules.sort_unstable_by_key(|(rule, _)| *rule);
    let mut final_rows = Vec::new();
    for (rule, tables) in rules {
        if !extractor.rules.is_empty() && !extractor.rules.contains(rule) {
            continue;
        }
        for name in tables {
            // Go `inspectionFilter.enable`.
            if !extractor.metric_names.is_empty() && !extractor.metric_names.contains(*name) {
                continue;
            }
            let Some(def) = tidb_infoschema::metric_table_def::get_metric_table_def(name) else {
                warn(format!("metrics table: {name} not found"));
                continue;
            };
            let mut cols = def.labels.to_vec();
            let mut cond = condition.clone();
            if def.quantile > 0.0 {
                cols.push("quantile");
                if extractor.quantiles.is_empty() {
                    cond.push_str(" and quantile=0.99");
                } else {
                    let quantiles = extractor
                        .quantiles
                        .iter()
                        .map(|quantile| format!("{quantile:.6}"))
                        .collect::<Vec<_>>();
                    cond.push_str(&format!(" and quantile in ({})", quantiles.join(",")));
                }
            }
            let sql = if cols.is_empty() {
                format!(
                    "select avg(value),min(value),max(value) from `metrics_schema`.`{name}` {cond}"
                )
            } else {
                let cols = cols.join("`,`");
                format!(
                    "select avg(value),min(value),max(value),`{cols}` from `metrics_schema`.`{name}` {cond} group by `{cols}` order by `{cols}`"
                )
            };
            let rows = query(&sql).map_err(|error| {
                DriverError::unsupported(format!(
                    "execute '{sql}' failed: {}",
                    error.to_mysql_error().message
                ))
            })?;
            let non_instance_label_index =
                usize::from(def.labels.first().is_some_and(|label| *label == "instance"));
            // Skip avg/min/max.
            const SKIP_COLS: usize = 3;
            for row in rows {
                // Go `Row.GetString` over the label columns, all strings.
                let string_at = |index: usize| match row.get(index) {
                    Some(Datum::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
                    Some(Datum::String(text)) => String::from_utf8_lossy(text.bytes()).into_owned(),
                    _ => String::new(),
                };
                // Go `Row.GetFloat64`, which reads a NULL cell as zero.
                let float_at = |index: usize| match row.get(index) {
                    Some(Datum::Real(value)) => Datum::Real(*value),
                    _ => Datum::Real(0.0),
                };
                let instance = if non_instance_label_index > 0 {
                    string_at(SKIP_COLS)
                } else {
                    String::new()
                };
                let labels = def.labels[non_instance_label_index..]
                    .iter()
                    .enumerate()
                    .map(|(i, label)| {
                        let value = string_at(SKIP_COLS + non_instance_label_index + i);
                        if *label == "store" || *label == "store_id" {
                            format!("store_id:{value}")
                        } else {
                            value
                        }
                    })
                    .collect::<Vec<_>>();
                let quantile = if def.quantile > 0.0 {
                    float_at(row.len() - 1)
                } else {
                    Datum::Null
                };
                final_rows.push(vec![
                    text(rule),
                    text(&instance),
                    text(name),
                    text(&labels.join(", ")),
                    quantile,
                    float_at(0),
                    float_at(1),
                    float_at(2),
                    text(def.comment),
                ]);
            }
        }
    }
    Ok(final_rows)
}

/// Go `inspectionSummaryRules`: each summary rule's metrics tables, in the
/// source's order.
pub const INSPECTION_SUMMARY_RULES: &[(&str, &[&str])] = &[
    (
        "query-summary",
        &[
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
        ],
    ),
    (
        "wait-events",
        &[
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
            "pd_request_rpc_duration",
            "tidb_cop_duration",
            "tidb_batch_client_wait_duration",
            "tidb_batch_client_unavailable_duration",
            "tidb_kv_backoff_duration",
            "tidb_kv_request_duration",
            "pd_client_cmd_duration",
            "tikv_grpc_message_duration",
            "tikv_average_grpc_messge_duration",
            "tikv_channel_full",
            "tikv_scheduler_is_busy",
            "tikv_coprocessor_is_busy",
            "tikv_engine_write_stall",
            "tikv_raftstore_apply_log_avg_duration",
            "tikv_raftstore_apply_log_duration",
            "tikv_raftstore_append_log_avg_duration",
            "tikv_raftstore_append_log_duration",
            "tikv_raftstore_commit_log_avg_duration",
            "tikv_raftstore_commit_log_duration",
            "tikv_raftstore_process_duration",
            "tikv_raftstore_propose_wait_duration",
            "tikv_propose_avg_wait_duration",
            "tikv_raftstore_apply_wait_duration",
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
        ],
    ),
    (
        "read-link",
        &[
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
            "tidb_distsql_copr_cache",
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
            "tikv_grpc_message_duration",
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
        ],
    ),
    (
        "write-link",
        &[
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
            "tikv_grpc_message_duration",
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
            "tikv_raftstore_propose_wait_duration",
            "tikv_raftstore_append_log_avg_duration",
            "tikv_raftstore_append_log_duration",
            "tikv_raftstore_commit_log_avg_duration",
            "tikv_raftstore_commit_log_duration",
            "tikv_apply_avg_wait_duration",
            "tikv_raftstore_apply_log_avg_duration",
            "tikv_raftstore_apply_log_duration",
            "tikv_raftstore_apply_wait_duration",
            "tikv_engine_wal_sync_operations",
            "tikv_engine_write_duration",
            "tikv_engine_write_operations",
            "tikv_engine_write_stall",
            "tikv_write_stall_avg_duration",
            "tikv_write_stall_max_duration",
            "tikv_write_stall_reason",
        ],
    ),
    (
        "ddl",
        &[
            "tidb_ddl_add_index_speed",
            "tidb_ddl_batch_add_index_duration",
            "tidb_ddl_deploy_syncer_duration",
            "tidb_ddl_duration",
            "tidb_ddl_meta_opm",
            "tidb_ddl_opm",
            "tidb_ddl_update_self_version_duration",
            "tidb_ddl_waiting_jobs_num",
            "tidb_ddl_worker_duration",
        ],
    ),
    (
        "stats",
        &[
            "tidb_statistics_auto_analyze_duration",
            "tidb_statistics_auto_analyze_ops",
            "tidb_statistics_pseudo_estimation_ops",
            "tidb_statistics_stats_inaccuracy_rate",
            "tidb_statistics_update_stats_ops",
        ],
    ),
    (
        "gc",
        &[
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
        ],
    ),
    (
        "rocksdb",
        &[
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
        ],
    ),
    (
        "pd",
        &[
            "pd_scheduler_balance_region",
            "pd_balance_scheduler_status",
            "pd_checker_event_count",
            "pd_client_cmd_duration",
            "pd_client_cmd_ops",
            "pd_cluster_metadata",
            "pd_cluster_status",
            "pd_grpc_completed_commands_duration",
            "pd_grpc_completed_commands_rate",
            "pd_request_rpc_duration",
            "pd_request_rpc_ops",
            "pd_request_rpc_duration_avg",
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
        ],
    ),
    (
        "raftstore",
        &[
            "tikv_approximate_avg_region_size",
            "tikv_approximate_region_size_histogram",
            "tikv_approximate_region_size",
            "tikv_raftstore_append_log_avg_duration",
            "tikv_raftstore_append_log_duration",
            "tikv_raftstore_commit_log_avg_duration",
            "tikv_raftstore_commit_log_duration",
            "tikv_apply_avg_wait_duration",
            "tikv_raftstore_apply_log_avg_duration",
            "tikv_raftstore_apply_log_duration",
            "tikv_raftstore_apply_wait_duration",
            "tikv_raftstore_process_duration",
            "tikv_raftstore_process_handled",
            "tikv_propose_avg_wait_duration",
            "tikv_raftstore_propose_wait_duration",
            "tikv_raft_dropped_messages",
            "tikv_raft_log_speed",
            "tikv_raft_message_avg_batch_size",
            "tikv_raft_message_batch_size",
            "tikv_raft_proposals_per_ready",
            "tikv_raft_proposals",
            "tikv_raft_sent_messages",
        ],
    ),
];
