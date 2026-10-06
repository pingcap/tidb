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

//! Single integration-test binary for all module-safe `tidb-exec` source tests.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod analyze_added_column_source;
mod analyze_commit_size_source;
mod analyze_generated_column_source;
mod analyze_panic_error_source;
mod auto_pre_split_source;
mod autocommit_point_get_max_ts_source;
mod base_join_probe_source;
mod catalog_reload_source;
mod cluster_account_write_source;
mod cluster_catalog_loader_source;
mod cluster_config_source;
mod cluster_ddl_alter_source;
mod cluster_ddl_source;
mod cluster_index_id_source;
mod cluster_sysvar_write_source;
mod column_flag_builders_agree_source;
mod concurrent_entry_map_source;
mod cop_scan_narrowed_output_source;
mod cop_scan_partial_predicate_limit_source;
mod cop_scan_string_selection_source;
mod dag_zone_contract;
mod direct_unary_cancellation_source;
mod distsql_query_runtime_source;
mod distsql_recordset_source;
mod error_context_source;
mod error_conversion_source;
mod explain_source;
mod fair_locking_session_seam_realtikv_source;
mod global_sysvar_initial_source;
mod hash_join_v2_source;
mod hash_join_version_source;
mod hash_table_v2_source;
mod hint_updatable_vars_source;
mod index_type_passthrough_source;
mod join_row_table_source;
mod join_table_meta_source;
mod keydecoder_source;
mod mysql_bootstrap_source;
mod mysql_bootstrap_tableinfo_source;
mod nontransactional_source;
mod option_values_source;
mod pd_approximate_count_source;
mod placement_delivery_source;
mod prepared_dml_lowering_source;
mod prepared_write_persists_realtikv_source;
mod process_info_source;
mod real_tikv_authority_shutdown_source;
mod real_tikv_bigint_selection_source;
mod real_tikv_clustered_pk_range_source;
mod real_tikv_read_source;
mod real_tikv_session_authority_source;
mod result_field_resolver_source;
mod result_metadata_source;
mod slow_log_match_source;
mod slow_log_rules_source;
mod slow_log_threshold_source;
mod statement_pushdown_source;
mod statement_status_source;
mod system_index_entry_go_bytes;
mod table_index_reader_runtime_source;
mod tagged_ptr_source;
mod tikv_scan_dag_lowering_source;
mod tikv_selection_dag_lowering_source;
mod txn_summary_source;
mod upgrade_versions_source;
mod used_stats_source;
mod warning_publication_source;
mod wide_scan_selection_source;
