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

//! All topology-independent `tidb-executor` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod auto_inc_last_insert_id_flow_source;
mod auto_inc_rebase_zero_mode_source;
mod base_join_probe_source;
mod binary_year_write_source;
mod check_constraint_write_source;
mod concurrent_entry_map_source;
mod cteutil_source;
mod db_change_ddl_conflicts_source;
mod db_integration_b103_source;
mod db_integration_ddl_types_source;
mod db_rename_b103_source;
mod db_table_b103_source;
mod ddl_executor_nokit_killflag_source;
mod ddl_fail_injection_source;
mod ddl_integration_reorg_backfill_source;
mod ddl_internal_helper_fns_source;
mod ddl_job_queue_executor_source;
mod ddl_table_max_handle_source;
mod ddl_ttl_info_options_source;
mod decimal_int_boundary_source;
mod default_on_update_source;
mod delete_found_rows_source;
mod delete_subquery_source;
mod dml_expression_values_source;
mod enum_set_write_source;
mod fast_create_table_lifecycle_source;
mod fk_alter_meta_and_privilege_source;
mod fk_create_error_matrix_source;
mod fk_create_meta_info_source;
mod fk_table_lifecycle_source;
mod foreign_key_ddl_owner_checks_source;
mod hash_join_v2_source;
mod hash_join_version_source;
mod hash_table_v2_source;
mod in_statement_duplicate_source;
mod index_change_add_drop_lifecycle_source;
mod index_entry_go_bytes;
mod index_modify_add_index_source;
mod index_nokit_disk_full_pause_source;
mod insert_ignore_check_constraint_source;
mod insert_ignore_downgrade_source;
mod insert_ignore_semantics_source;
mod insert_select_source;
mod insert_statement_atomicity_source;
mod insert_strict_truncate_source;
mod join_row_table_source;
mod join_table_meta_source;
mod json_datetime_write_source;
mod null_not_null_insert_source;
mod odku_ignore_check_delete_limit_source;
mod on_duplicate_key_source;
mod partial_aggregate_primary_ids_source;
mod partition_db_partition_ddl_source;
mod partition_exchange_global_index_source;
mod partition_modify_column_allowlist_source;
mod partition_pk_global_index_source;
mod partition_truncate_issue57780_source;
mod physical_expand_source;
mod physical_union_scan_execution_source;
mod placement_policy_ddl_source;
mod ranger_types_source;
mod reorg_partition_ddl_failures_source;
mod replace_check_constraint_source;
mod replace_semantics_source;
mod row_count_flow_source;
mod row_decoder_source;
mod sequence_create_defaults_source;
mod sequence_function_values_source;
mod serial_auto_random_source;
mod serial_column_flags_and_limits_source;
mod serial_create_table_like_source;
mod shard_row_id_bits_source;
mod shared_physical_select_builder_source;
mod statement_pushdown_source;
mod table_lifecycle_job_source;
mod table_mode_transition_source;
mod table_options_source;
mod tagged_ptr_source;
mod taobench_repro_source;
mod tiflash_replica_test_source;
mod update_accounting_source;
mod update_ignore_check_constraint_source;
mod update_ignore_null_source;
mod update_order_limit_multi_source;
mod update_subquery_source;
mod used_stats_source;
mod values_arity_error_source;
mod window_executor_source;
