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

// The unit-test modules of `tidb-session`, sorted, brought into the crate
// root by `lib.rs` under `#[cfg(test)]`. The files stay at `src/<name>.rs`
// (or `src/<name>/mod.rs`) and keep their module paths, so every test keeps
// its name.

mod tests_admin_check;
mod tests_affinity_storage_class;
mod tests_alter_column;
mod tests_alter_schema;
mod tests_analyze;
mod tests_auth_token;
mod tests_auto_increment;
mod tests_auto_random;
mod tests_bad_null;
mod tests_bdr;
mod tests_binding;
mod tests_builtin_survey;
mod tests_calibrate_resource;
mod tests_cast_int_truncation;
mod tests_cast_vector;
mod tests_charset;
mod tests_charset_introducer;
mod tests_check_constraints;
mod tests_coalesced_joins;
mod tests_collation;
mod tests_column_defaults;
mod tests_column_prune;
mod tests_columnar_index;
mod tests_compare_refinement;
mod tests_compile_path;
mod tests_compiler_plan_cacheable_checker;
mod tests_core;
mod tests_datetime_year_compare;
mod tests_ddl_go_checks;
mod tests_ddl_preprocess;
mod tests_ddl_service;
mod tests_deadlock_history;
mod tests_decorrelate_count_having;
mod tests_derived_agg_pruning;
mod tests_dml_lock_keys;
mod tests_domain_plan_replayer_source;
mod tests_domain_serverinfo_info_source;
mod tests_domain_serverinfo_syncer_source;
mod tests_enum_index_range;
mod tests_error_class;
mod tests_error_codes;
mod tests_eval_bool;
mod tests_expensivequery;
mod tests_explain;
mod tests_explain_derived;
mod tests_explain_merge_join;
mod tests_explain_redact;
mod tests_expression_go_semantics;
mod tests_expression_indexes;
mod tests_expression_time;
mod tests_extension_auth;
mod tests_extension_func;
mod tests_extension_registry;
mod tests_extra_handle;
mod tests_extra_handle_access;
mod tests_fix_control;
mod tests_flashback_cluster;
mod tests_foreign_key;
mod tests_generated_columns;
mod tests_global_index_ddl;
mod tests_global_vars;
mod tests_grants;
mod tests_group_by_resolution;
mod tests_harvested_relation_engine;
mod tests_hash_join_fetcher_eof;
mod tests_identifier_folding;
mod tests_in_list_full_evaluation;
mod tests_index_advisor;
mod tests_index_hints;
mod tests_index_join_inner_pattern;
mod tests_index_key_length;
mod tests_index_lookup_pushdown;
mod tests_index_merge_union;
mod tests_insert_go;
mod tests_inspection_result;
mod tests_join_key_cast;
mod tests_join_predicate_placement;
mod tests_join_reorder_cost;
mod tests_json;
mod tests_masking_policy;
mod tests_max_keys_read;
mod tests_mdl_related;
mod tests_mem_quota;
mod tests_memory_tables;
mod tests_memtable_extractor;
mod tests_merge_join_mixed_key_types;
mod tests_mixed_sign_index_join;
mod tests_modify_column_null;
mod tests_multi_table_dml;
mod tests_multi_valued_index;
mod tests_non_prepared_plan_cache;
mod tests_nontransactional;
mod tests_observation_batch;
mod tests_outer_join_elimination;
mod tests_partition;
mod tests_partition_exchange;
mod tests_partition_processor;
mod tests_partition_projection;
mod tests_partition_prune_collation;
mod tests_partition_reorganize;
mod tests_partition_show_go;
mod tests_planner_checks;
mod tests_planner_core_rewriter;
mod tests_planner_results;
mod tests_positional_orderby;
mod tests_prepared_plan_cache;
mod tests_prepared_statements;
mod tests_privilege_checks;
mod tests_privilege_tls;
mod tests_process_registry;
mod tests_pushdown_blacklist;
mod tests_read_cast;
mod tests_recursive_cte;
mod tests_region_split;
mod tests_resource_group;
mod tests_savepoint;
mod tests_sem_v2;
mod tests_sequence;
mod tests_session_part1_source;
mod tests_session_part2_source;
mod tests_session_var_hooks;
mod tests_set_opr_precedence;
mod tests_show;
mod tests_show_admin;
mod tests_skew_distinct_agg;
mod tests_slow_log;
mod tests_sql_mode_scanner;
mod tests_statement_rollback;
mod tests_stmt_ctx_build;
mod tests_subquery;
mod tests_support;
mod tests_sysbench_access;
mod tests_system_schemas;
mod tests_table_lock;
mod tests_temporary_tables;
mod tests_tidb_decode_key;
mod tests_tidb_encode_key;
mod tests_timestamp_range;
mod tests_timezone_storage;
mod tests_topn;
mod tests_union_all_predicate_push_down;
mod tests_union_scan;
mod tests_user_vars;
mod tests_variable_session;
mod tests_vector;
mod tests_views;
mod tests_window;
mod tests_write_conversion;
mod tests_zero_date;
