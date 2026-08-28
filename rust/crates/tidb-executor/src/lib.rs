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

//! The wired execution engine from `pkg/executor`: the [`Executor`] trait, the
//! shared [`ExecutorMeta`] base state, and concrete operators.
//!
//! This crate is the execution *spine* -- the pull-based `Open`/`Next(chunk)`/
//! `Close` driver that ties parsed plans to results, a native-boundary split of
//! `pkg/executor`.
//!
//! SCOPE: the `Executor` trait core (open/next/close/schema/ret_field_types/
//! init_cap/max_chunk_size/new_chunk) plus the operator set: `TableDualExec`,
//! `ProjectionExec`, `SelectionExec`, `HashAggExec`, `SortExec`, `LimitExec`,
//! `HashJoinExec`/`JoinExec`, `WindowExec`, `ApplyExec`, `ExplainExec`, the KV
//! table scan and index-range/access-path sources, and the `driver` that builds
//! them from an AST. One file per Go operator, comments citing the Go symbol.
//!
//! THIS IS THE LIVE ENGINE. Every TCP connection through the convergence node
//! and every in-process `tidb-session` query executes these operators — the
//! same code on both paths (`tidb-session` -> `run_select_meta_stmt`). The
//! `Executor` trait is an INTERNAL contract: `driver::run_select*` is the only
//! public entry, and no crate outside this one drives `Executor::next`.
//!
//! It stays a separate crate from `tidb-exec` so the engine builds without that
//! crate's cluster/session bulk; the edge runs `tidb-exec` -> `tidb-executor`,
//! so this one is upstream and never sees it. `tidb-exec` once carried a
//! second, non-production query engine; it was deleted, and this is now the
//! only one.
//!
//! DEFERRED (documented): the Go `context.Context`/`sessionctx` propagation,
//! runtime stats, the SQL killer, `Detach`, and parallel projection.

pub mod access_cost;
pub mod access_path;
pub mod admin_check;
pub mod worker_pool;

/// Submits a fire-and-forget task to the persistent exec pool.
pub fn worker_pool_spawn<F>(task: F)
where
    F: FnOnce() + Send + 'static,
{
    worker_pool::enqueue_public(Box::new(task));
}

/// Whether the persistent exec pool is available.
#[must_use]
pub fn worker_pool_available() -> bool {
    worker_pool::available()
}
pub mod agg_spill;
pub mod analyze;
pub mod analyze_col_sampling;
pub mod apply;
pub mod apply_cache;
mod approx_count_distinct;
pub(crate) mod bad_null;
pub mod batch_point_get;
pub mod cluster_storage;
pub mod column_default;
pub(crate) mod column_prune;
pub mod cte_storage;
pub mod ddl;
pub mod ddl_algorithm;
pub mod ddl_copr;
pub mod ddl_exec;
pub mod ddl_label;
pub mod ddl_running_jobs;
#[cfg(test)]
mod tests_ddl_job_submitter_worker_gaps;
#[cfg(test)]
mod tests_ddl_masking_policy_ddl_gaps;
#[cfg(test)]
mod tests_ddl_metabuild_session_context_gap;
#[cfg(test)]
mod tests_ddl_modify_column_reorg_gaps;
#[cfg(test)]
mod tests_ddl_modify_column_types;
#[cfg(test)]
mod tests_ddl_multi_schema_change_cancel_gaps;
#[cfg(test)]
mod tests_ddl_multi_schema_change_job_gaps;
#[cfg(test)]
mod tests_ddl_multi_schema_change_sql;
#[cfg(test)]
mod tests_ddl_mv_index_online_ddl_gaps;
#[cfg(test)]
mod tests_ddl_notifier_pubsub_gaps;
#[cfg(test)]
mod tests_ddl_options_owner_manager_gaps;
#[cfg(test)]
mod tests_ddl_partition_operations_sql;
#[cfg(test)]
mod tests_ddl_partition_reorganize_exchange_gaps;
pub mod ddl_sequence;
pub mod deadlock_history;
pub mod driver;
pub mod error_context;
pub mod executor;
pub mod explain;
pub mod expression_index;
mod farmhash;
pub(crate) mod foreign_key;
pub mod generated_column;
pub mod generated_column_substitute;
mod go_quote;
pub(crate) mod handle_range;
pub mod hash_agg;
#[cfg(test)]
mod hash_agg_spill_tests;
mod hash_join;
pub mod hints_set;
#[cfg(test)]
mod tests_import_into_external_id_gaps;
#[cfg(test)]
mod tests_importer_chunk_process_gaps;
#[cfg(test)]
mod tests_importer_job_gaps;
#[cfg(test)]
mod tests_importer_kv_encode_gaps;
#[cfg(test)]
mod tests_importer_plan_options_gaps;
#[cfg(test)]
mod tests_importer_precheck_sampler_gaps;
#[cfg(test)]
mod tests_importer_table_import_gaps;
#[cfg(test)]
mod tests_importer_verify_postprocess_gaps;
#[cfg(test)]
mod tests_insert_auto_random_id_source;
#[cfg(test)]
mod tests_insert_null_non_strict_source;
#[cfg(test)]
mod tests_insert_on_duplicate_key_source;
#[cfg(test)]
mod tests_insert_write_gaps;
#[cfg(test)]
mod tests_inspection_result_gaps;
mod index_hints;
pub mod index_lookup_hash_join;
pub mod index_lookup_join;
pub mod index_lookup_merge_join;
pub mod index_merge_reader;
mod index_prefix_cut;
mod index_range;
pub mod join;
pub mod joiner;
pub mod keydecoder;
pub mod kv_table;
#[cfg(test)]
mod tests_infoschema_cluster_table_gaps;
#[cfg(test)]
mod tests_infoschema_reader_source_gaps;
pub mod limit;
pub mod load_stats;
pub mod mem_quota;
pub mod mem_reader;
pub mod mem_table;
pub mod memtable_reader;
pub(crate) mod merge_join_plan;
pub mod multi_way_merge;
pub mod mutation_checker;
pub mod parallel_sort_spill_helper;
pub mod partition_pruning;
pub mod partition_routing;
pub(crate) mod ranger_detacher;
mod tidb_decode_key;
#[cfg(test)]
mod tests_adapter_recordset_lockkeys_gaps;
#[cfg(test)]
mod tests_adapter_slow_log_ru_gaps;
#[cfg(test)]
mod tests_adapter_topsql_profiling_gaps;
#[cfg(test)]
mod tests_admin_checksum_gaps;
#[cfg(test)]
mod tests_analyze_broadcast_flush_gaps;
#[cfg(test)]
mod tests_analyze_kill_save_gaps;
#[cfg(test)]
mod tests_analyze_panic_recovery_source;
#[cfg(test)]
mod tests_aggfuncs_approx_pushdown_source;
#[cfg(test)]
mod tests_aggfuncs_avg_bit_count_gaps;
#[cfg(test)]
mod tests_aggfuncs_cume_dist_first_row_gaps;
#[cfg(test)]
mod tests_aggfuncs_group_concat_gaps;
#[cfg(test)]
mod tests_aggfuncs_json_aggs_gaps;
#[cfg(test)]
mod tests_aggfuncs_lead_lag_gaps;
#[cfg(test)]
mod tests_aggfuncs_max_min_deque_gaps;
#[cfg(test)]
mod tests_aggfuncs_parallel_distinct_gaps;
#[cfg(test)]
mod tests_batch_point_get_locking_gaps;
#[cfg(test)]
mod tests_batch_point_get_temporary_source;
#[cfg(test)]
mod tests_brie_task_surface_gaps;
#[cfg(test)]
mod tests_cluster_slow_query_gaps;
#[cfg(test)]
mod tests_compact_table_tiflash_gaps;
#[cfg(test)]
mod tests_index_join_cte_build_cleanup_gaps;
#[cfg(test)]
mod tests_parallel_apply_sql_source;
#[cfg(test)]
mod tests_partition_table_sql_source;
#[cfg(test)]
mod tests_memtable_cluster_source;
#[cfg(test)]
mod tests_merge_join_in_disk_source;
#[cfg(test)]
mod tests_join_probe_source_gaps;
#[cfg(test)]
mod tests_pkg_nested_loop_apply_source;
#[cfg(test)]
mod tests_point_get_visibility_locking_gaps;
#[cfg(test)]
mod tests_prepared_isolation_and_limits_gaps;
#[cfg(test)]
mod tests_prepared_param_types_source;
#[cfg(test)]
mod tests_prepared_parameter_pushdown_source;
#[cfg(test)]
mod tests_resource_group_tag_gaps;
#[cfg(test)]
mod tests_revoke_privilege_gaps;
#[cfg(test)]
mod tests_select_into_outfile_gaps;
#[cfg(test)]
mod tests_set_session_variable_gaps;
#[cfg(test)]
mod tests_show_affinity_gaps;
#[cfg(test)]
mod tests_show_ddl_job_comments_gaps;
#[cfg(test)]
mod tests_show_placement_gaps;
#[cfg(test)]
mod tests_show_stats_meta_gaps;
#[cfg(test)]
mod tests_sql_flags_import_insert_gaps;
#[cfg(test)]
mod tests_tablesample_regions_gaps;
pub use partition_routing::{PartitionDef, PartitionKind, PartitionSpec, RangeBound};
pub mod fts_like_rewrite;
pub mod plan_hints;
mod plan_trace;
pub mod point_get;
pub mod predicate_pushdown;
pub mod projection;
mod pushdown_blacklist;
pub mod qb_hint;
pub mod remote_scan;
pub mod selection;
pub mod sequence;
pub mod show_stats;
pub mod shuffle;
pub mod sort;
pub mod sort_partition;
pub mod sort_util;
pub mod statement_pushdown;
mod stmt_context;
pub mod stmt_hints;
pub mod storage;
pub mod table_access;
pub mod table_dual;
pub mod table_reader;
pub mod tblctx;
pub mod tblsession;
#[cfg(test)]
mod tests_duplicate_entry_message_source;
#[cfg(test)]
mod tests_global_temp_table_source;
#[cfg(test)]
mod tests_index_join;
#[cfg(test)]
mod tests_jointest_join_source;
#[cfg(test)]
mod tests_loaddatatest_source;
#[cfg(test)]
mod tests_loadremotetest_source;
#[cfg(test)]
mod tests_loadremotetest_one_csv_source;
#[cfg(test)]
mod tests_memtest_source;
#[cfg(test)]
mod tests_oomtest_source;
#[cfg(test)]
mod tests_passwordtest_source;
pub mod tiflash_recorder;
pub mod topn;
pub mod topn_chunk_heap;
pub mod topn_spill;
pub mod union_scan;
pub mod vec_group_checker;
pub mod view;
mod window;
pub mod write_stmt_bufs;
pub mod zero_date;

pub use apply::ApplyExec;
pub use cte_storage::{CteStorage, CteTable};
pub use ddl::{
    added_check_constraint_actions, append_partition_defs, check_constraint_count,
    discarded_check_constraint_actions, escape_partition_name, linear_partitioning_warning,
    partition_placement_text, resolve_database_charset, run_alter_placement_policy,
    run_alter_table_in, run_create_index_in, run_create_placement_policy, run_create_table_in,
    run_create_table_on, run_drop_index_in, run_drop_placement_policy, run_drop_table_in,
    run_rename_table_in, run_truncate_table_in, CreateTableSettings,
};
pub use ddl_sequence::{
    run_alter_sequence_in, run_create_sequence_in, run_drop_sequence_in, show_create_sequence,
};
pub use driver::infoschema_meta;
pub use driver::{
    access::{
        build_prepared_point_get_plan, build_prepared_select_plan, run_prepared_point_get,
        run_prepared_select, PreparedPlanCacheEnvironment, PreparedPointGetExecution,
        PreparedPointGetPlan, PreparedSelectExecution, PreparedSelectPlan,
    },
    bind_parameters, bind_prepared_statement, bind_statement, parameter_count,
    parsed_parameter_count, plan_select_meta_stmt, run_delete_in, run_delete_on, run_delete_stmt,
    run_fast_prepared_insert, run_fast_prepared_update, run_insert_in, run_insert_on,
    run_insert_reporting, run_insert_stmt, run_select, run_select_meta_in, run_select_meta_on,
    run_select_meta_stmt, run_select_on, run_set_opr_stmt, run_update_in, run_update_on,
    run_update_stmt, Catalog, DriverError, MemTable, MysqlError, SchemaErrorKind, SelectMeta,
    TableEntry, TxnErrorKind, VarErrorKind, ViewDef, DEFAULT_DATABASE,
};
pub use executor::{ExecError, Executor, ExecutorMeta};
pub use explain::{
    explain_analyze_delete_stmt, explain_analyze_insert_stmt, explain_analyze_select_stmt,
    explain_analyze_set_opr_stmt, explain_analyze_update_stmt, explain_delete_stmt,
    explain_insert_stmt, explain_select_stmt, explain_set_opr_stmt, explain_update_stmt,
    ExplainFormat,
};
pub use hash_agg::{
    AggFunc, AggKind, GroupedStreamAggExec, HashAggContext, HashAggExec, StreamAggExec,
};
pub use join::{JoinExec, JoinKind};
pub use kv_table::{
    DecodedRow, FkAction, GeneratedColumnSelection, IndexRange, KvColumn, KvForeignKey, KvIndex,
    KvTable, RowDecodeContext, RowDecoder, TableCharset, TableHandle, TableScanExec,
};
pub use limit::LimitExec;
pub use mem_quota::{OomAction, SessionMemory, StatementCancellation, StatementMemory};
pub use mem_table::MemTableSourceExec;
pub use predicate_pushdown::{
    PushedScanFilter, ScanColumnComparison, ScanComparison, ScanComparisonOp, ScanPredicate,
};
pub use projection::ProjectionExec;
pub use selection::SelectionExec;
pub use sort::{SortByItem, SortExec};
pub use stmt_context::{
    RetryAutoIds, RowIdShardGenerator, SequenceSnapshot, StatementClass, StmtContext,
    MAX_WARNING_COUNT,
};
pub use table_access::TableAccess;
pub use table_dual::TableDualExec;
pub use tidb_decode_key::TidbDecodeKeySnapshot;
/// The level a statement warning carries -- Go's three `contextutil` levels,
/// re-exported so the session can read the one a `StmtContext` recorded
/// without depending on `tidb-distsql` directly.
pub use tidb_distsql::WarningLevel as WarnLevel;
pub use tidb_expr::builtin_registry::builtin_list;
pub use tidb_expr::infer_pushdown::{blacklist_name, blacklist_store_mask, ExprPushDownBlacklist};
pub use tidb_expr::CurrentTso;
pub use tidb_expr::{
    eval_in, like_match_with_collation, truthy_of, BlockEncryptionMode, Columns, EvalError,
    JsonError, MysqlRng, SessionTimeZone,
};
pub use topn::TopNExec;
pub use view::{resolve_view_definition, run_create_view_in, run_drop_view_in, view_column_list};

/// Explicit isolated spill authorities for executor tests.
#[cfg(test)]
mod tests_distsql_test_source;
#[cfg(test)]
mod tests_set_show_stats_slow_query_source;
#[cfg(test)]
mod tests_executor_suite_metadata_source;
#[cfg(test)]
mod tests_executor_suite_statements_source;
#[cfg(test)]
mod tests_fktest_source;
#[cfg(test)]
mod tests_fktest_b134_source;
#[cfg(test)]
mod tests_indexmergeread_b134_source;
#[cfg(test)]
mod tests_infoschema_b134_source;
#[cfg(test)]
mod tests_admin_check_admintest_source;
#[cfg(test)]
mod tests_hashagg_aggregate_suite_source;
#[cfg(test)]
mod tests_analyze_suite_source;
#[cfg(test)]
pub(crate) mod test_temp_storage {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

    /// A fresh scratch directory named after the test.
    pub(crate) fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tidb_rust_exec_spill_test_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch temp dir");
        dir
    }

    /// Opens one immutable authority over `path` for the test statement.
    pub(crate) fn storage(path: &Path) -> Arc<SpillStorage> {
        Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: path.to_owned(),
                quota_bytes: -1,
                encryption: SpillEncryptionMethod::Plaintext,
            })
            .expect("test spill storage"),
        )
    }
}

/// Session-owned reference counts over shared advisory-lock services.
pub mod advisory_lock_state;
