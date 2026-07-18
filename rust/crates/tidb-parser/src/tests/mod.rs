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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `tidb-parser` unit test suite: one test per grammar/statement area,
//! asserting `r(sql)` (parse-then-restore) round-trips to the exact
//! canonical SQL real TiDB's own AST would produce (see each test's own
//! doc for what it covers and, where relevant, which `godump restore`
//! probe confirmed the assertion). Split by concern, mirroring the source
//! modules (`admin`/`binding`/`ddl`/`dml`/`expr`/`privilege`/`select`/`set`/
//! `show`/`user`), plus
//! [`hints`] (optimizer-hint grammar, a cohesive family of its own) and
//! [`stmt`] (the remaining statement-level odds and ends) — so two agents
//! extending different grammar areas never touch the same test file. Shared
//! helpers (`r`) and imports live here; every submodule starts with
//! `use super::*;`.

use super::*;
use tidb_ast::{
    AlterTableAction, ColumnDef, ColumnOption, ColumnPosition, ColumnType, ColumnTypeArg,
    CompactReplicaKind, Expr, IndexPart, InlineKeyOption, PrimaryKeyStorage, SelectField,
    TableConstraint, TableOption,
};

/// Parses and restores, asserting the canonical SQL output.
fn r(sql: &str) -> String {
    parse(sql).expect("parse").restore()
}

fn plain_key_parts(names: &[&str]) -> Vec<IndexPart> {
    names
        .iter()
        .map(|name| IndexPart::Column {
            name: (*name).to_string(),
            prefix_len: None,
            desc: false,
        })
        .collect()
}

mod admin;
mod admin_alter_ddl_jobs_source;
mod admin_cleanup_table_lock_source;
mod admin_ddl_job_control_source;
mod admin_flush_plan_cache_source;
mod alter_add_columns_source;
mod alter_analyze_partition_source;
mod alter_auto_id_options_source;
mod alter_auto_increment_source;
mod alter_check_source;
mod alter_column_default_source;
mod alter_drop_check_source;
mod alter_drop_foreign_key_source;
mod alter_drop_primary_key_source;
mod alter_engine_attribute_source;
mod alter_index_visibility_source;
mod alter_lock_source;
mod alter_order_qualified_modify_source;
mod alter_rename_column_source;
mod alter_rename_index_source;
mod alter_shard_row_id_bits_source;
mod alter_table_cache_source;
mod alter_table_comment_source;
mod alter_table_engine_row_format_source;
mod alter_table_generic_options_source;
mod alter_table_multi_spec_source;
mod alter_table_placement_policy_source;
mod alter_table_ttl_source;
mod alter_table_validation_source;
mod analyze;
mod analyze_incremental_source;
mod binding;
mod collation_source;
mod create_binding_with_dml_source;
mod create_table_planner_issue_source;
mod create_table_qualified_column_source;
mod create_table_split_source;
mod create_user_tls_source;
mod create_view_definer_source;
mod ctas_source;
mod cte_scalar_union_source;
mod ddl;
mod ddl_alter_check_current_user_source;
mod ddl_attributes_source;
mod ddl_check_time_source;
mod ddl_column_check_source;
mod ddl_column_options_source;
mod ddl_default_source;
mod ddl_table_parser_source;
mod dml;
mod dml_join_restore_source;
mod dml_restore_source;
mod drop_database_source;
mod exists_setopr_source;
mod explain_binary_source;
mod explain_plan_tree_source;
mod explain_values_source;
mod expr;
mod expressions_restore_source_wave;
mod field_type_source;
mod flush;
mod functions_source;
mod generated_source;
mod grant_revoke_role_source;
mod grant_tls_source;
mod hints;
mod index;
mod index_parser_source;
mod index_source;
mod inline_key_source;
mod insert_binary_escape_source;
mod insert_with_table_for_update_source;
mod lateral_recursive_cte_source;
mod load_data;
mod masking;
mod multi_statement_source;
mod named_table_constraints;
mod parenthesized_setopr_source;
mod partition_alter_add_empty_source;
mod partition_alter_attributes_source;
mod partition_alter_discard_source;
mod partition_alter_interval_source;
mod partition_alter_merge_first_source;
mod partition_alter_placement_policy_source;
mod partition_alter_repartition_source;
mod partition_alter_split_maxvalue_source;
mod partition_check_import_source;
mod partition_create_source;
mod partition_interval_source;
mod partition_key_algorithm_source;
mod placement;
mod privilege;
mod resource_group;
mod restore_context;
mod revoke_all_grant_option_source;
mod revoke_dynamic_privilege_source;
mod select;
mod sequence;
mod set;
mod set_restore_mismatch_source;
mod set_transaction_snapshot_source;
mod show;
mod show_builtins_full_tables_source;
mod show_character_set_source;
mod show_engines_source;
mod show_master_privileges_source;
mod show_open_tables_source;
mod show_stats_buckets_source;
mod show_stats_locked_source;
mod stmt;
mod table_option_charset_source;
mod table_option_source;
mod traffic;
mod update_default_source;
mod use_reserved_name_source;
mod user;
mod with_parenthesized_source;
