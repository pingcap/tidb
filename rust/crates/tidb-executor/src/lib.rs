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
//! `HashJoinExec`/`JoinExec`, `ApplyExec`, `ExplainExec`, the KV
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
//! TopSQL registration, RU tracking, `Detach`, and parallel projection.
//! Ordinary executor timing and row counts use the statement's Go-compatible
//! `RuntimeStatsColl` when execution-info collection is enabled.

pub mod base_join_probe;
pub mod concurrent_entry_map;
pub use tidb_util::execdetails;
pub mod sessionctx;
pub mod show;
pub mod compact_table;
pub mod set;
pub mod simple;
pub mod join_stats;
pub mod index_lookup_stats;
pub mod pd_helper;
pub mod hash_join_v2;
pub mod hash_join_version;
pub mod hash_table_v2;
pub mod join_row_table;
pub mod join_table_meta;
pub mod show_regions;
pub mod row_table_builder;
pub mod split_region;
pub mod table_size_stats;
pub mod tagged_ptr;

pub mod access_cost;
pub mod access_path;
pub mod admin_check;
pub mod checksum;
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
pub mod aggfuncs;
pub mod analyze;
pub mod apply;
pub(crate) mod apply_cache;
mod approx_count_distinct;
pub(crate) use tidb_table::bad_null;
#[cfg(test)]
mod tests_bad_null;
pub use tidb_kv::cluster_storage;
#[cfg(test)]
mod tests_cluster_storage;
pub use tidb_table::column_default;
pub mod cte_storage;
pub mod ddl;
pub mod ddl_sequence;
pub use tidb_deadlockhistory as deadlock_history;
pub mod cop_reader;
pub mod mpp_coordinator;
#[cfg(test)]
pub(crate) mod test_region;
pub mod dag_request;
pub mod driver;
pub mod wide_scan_selection;
pub mod executor;
pub mod explain;
pub use tidb_ddl_build::expression_index;
mod farmhash;
pub(crate) mod foreign_key;
pub use tidb_distsql::CopResponseSink;
pub use foreign_key::check_foreign_key_constrain;
pub use tidb_ddl_build::generated_column;
#[cfg(test)]
mod tests_generated_column_ddl;
pub(crate) mod handle_range;
pub use handle_range::split_ranges_across_int64_boundary;
pub mod hash_agg;
#[cfg(test)]
mod hash_agg_spill_tests;
mod hash_join;
pub mod index_merge_reader;
pub mod join;
pub mod joiner;
pub mod import_into_options;
pub use tidb_keydecoder as keydecoder;
pub mod kv_table;
pub mod limit;
pub mod load_data;
pub mod load_stats;
pub mod max_one_row;
pub use tidb_kv::mem_quota;
#[cfg(test)]
mod tests_mem_quota;
pub mod mem_reader;
pub mod mem_table;
pub mod mem_table_retrieval;
use tidb_kv::mpp_query;
#[cfg(test)]
mod tests_mpp_query;
pub(crate) mod merge_join_plan;
pub mod partition_pruning;
pub use tidb_table::partition_routing;
mod physical_cte;
pub mod select_lock;
#[cfg(test)]
mod tests_aggfuncs_approx_pushdown_source;
#[cfg(test)]
mod tests_analyze_panic_recovery_source;
#[cfg(test)]
mod tests_batch_point_get_temporary_source;
#[cfg(test)]
mod tests_insert_auto_random_id_source;
#[cfg(test)]
mod tests_insert_null_non_strict_source;
#[cfg(test)]
mod tests_insert_on_duplicate_key_source;
#[cfg(test)]
mod tests_joiner_required_rows_source;
#[cfg(test)]
mod tests_merge_join_in_disk_source;
#[cfg(test)]
mod tests_parallel_apply_sql_source;
#[cfg(test)]
mod tests_partition_table_sql_source;
#[cfg(test)]
mod tests_pkg_nested_loop_apply_source;
#[cfg(test)]
mod tests_prepared_param_types_source;
#[cfg(test)]
mod tests_prepared_parameter_pushdown_source;
#[cfg(test)]
mod tests_required_rows_source;
#[cfg(test)]
mod tests_table_part1_source;
#[cfg(test)]
mod tests_table_part2_source;
mod tidb_decode_key;
pub mod range_group_merge;
pub mod union_all;
pub mod slow_query;
pub use partition_routing::{PartitionDef, PartitionKind, PartitionSpec, RangeBound};
pub mod expand;
pub mod fts_like_rewrite;
mod plan_trace;
pub mod predicate_pushdown;
pub mod projection;
mod pushdown_blacklist;
pub mod remote_scan;
pub mod selection;
pub use tidb_table::sequence;
pub mod show_stats;
pub mod shuffle;
pub mod sort;
pub mod sort_partition;
pub mod sort_util;
pub mod stats_lock;
mod stmt_context;
pub use tidb_kv::storage;
pub mod table_access;
pub mod table_dual;
mod table_sample;
#[cfg(test)]
mod tests_duplicate_entry_message_source;
#[cfg(test)]
mod tests_executor_part19_source;
#[cfg(test)]
mod tests_global_temp_table_source;
#[cfg(test)]
mod tests_index_join;
#[cfg(test)]
mod tests_jointest_join_source;
#[cfg(test)]
mod tests_memtest_source;
#[cfg(test)]
mod tests_oomtest_source;
#[cfg(test)]
mod tests_passwordtest_source;
pub mod topn;
pub mod topn_chunk_heap;
pub mod topn_spill;
pub mod union_scan;
pub(crate) mod vec_group_checker;
pub mod view;
pub mod window;
pub use tidb_table::zero_date;

pub use apply::ApplyExec;
pub use cte_storage::CteStorage;
pub use ddl::{append_partition_defs, escape_identifier, linear_partitioning_warning, partition_placement_text, run_create_system_table_in, run_create_table_on, run_create_table_on_with, split_drop_table_targets, CreateTableSettings};
pub use ddl_sequence::show_create_sequence;
#[cfg(test)]
pub(crate) use driver::access::run_prepared_select_for_test;
pub use driver::infoschema_meta;
pub use driver::plan_cache::{PlanCacheInvalidation, SessionPlanCache};
pub use driver::{
    access::{
        build_prepared_point_get_plan, build_prepared_select_plan, open_prepared_point_get,
        run_prepared_point_get, PreparedPlanCacheEnvironment, PreparedPointGetExecution,
        PreparedPointGetPlan, PreparedSelectExecution, PreparedSelectPlan,
    },
    bind_parameters, bind_prepared_statement, bind_statement, bound_parameter_values,
    build_prepared_dml_plan, fts_columns_are_strings, parameter_count, parsed_parameter_count,
    plan_query_meta_stmt, plan_query_meta_stmt_with_subqueries, plan_select_meta_stmt,
    run_delete_in, run_delete_on, run_delete_stmt, run_delete_stmt_with_physical, run_insert_in,
    run_insert_on, run_insert_reporting, run_insert_stmt, run_insert_stmt_with_physical,
    run_query_meta_stmt_with_physical, run_select, run_select_meta_in, run_select_meta_on,
    run_select_meta_stmt, run_select_meta_stmt_with_physical, run_select_on, run_set_opr_stmt,
    run_update_in, run_update_on, run_update_stmt, run_update_stmt_with_physical, Catalog,
    DriverError, MemTable, MysqlError, PreparedDmlExecution, PreparedDmlPlan, SelectMeta,
    TableEntry, VarError, ViewDef, DEFAULT_DATABASE,
};
pub use executor::{ExecError, Executor, ExecutorMeta};
pub use explain::{brief_binary_plan, explain_analyze_delete_stmt, explain_analyze_insert_stmt, explain_connection_plan, query_plan_cost, explain_analyze_physical_plan, explain_analyze_select_stmt, explain_analyze_set_opr_stmt, explain_analyze_update_stmt, explain_delete_stmt, explain_insert_stmt, explain_physical_plan, explain_select_stmt, explain_set_opr_stmt, explain_update_stmt, process_plan_info, ExplainFormat};
pub use hash_agg::{AggFunc, AggKind, GroupedStreamAggExec, HashAggContext, HashAggExec, StreamAggExec};
pub use join::{JoinExec, JoinKind};
pub use kv_table::{DecodedRow, FkAction, GeneratedColumnSelection, IndexRange, KvColumn, KvForeignKey, KvIndex, KvTable, KvTableBackfill, KvTableDdl, KvTablePartitionMaintenance, KvTableReader, RowDecodeContext, RowDecodeContextExt, RowDecoder, TableCharset, TableHandle, TableScanExec, TemporaryTableTxnData};
pub use limit::LimitExec;
pub use mem_quota::{OomAction, SessionMemory, StatementCancellation, StatementMemory};
pub use mem_table::MemTableSourceExec;
pub use mpp_query::MppQueryInfo;
pub use predicate_pushdown::{PushedScanFilter, ScanColumnComparison, ScanComparison, ScanComparisonOp, ScanPredicate};
pub use projection::ProjectionExec;
pub use selection::SelectionExec;
pub use sort::{SortByItem, SortExec};
pub use stmt_context::{scanner_sql_mode, DdlReorgVars, MdlRelatedTableRecorder, SessionPrivilegeCheck, ForeignKeyCascadeLockCheck, GlobalSysvarAccessor, KeysReadBudget, ProcessPlanInfo, StoredPlan, RetryAutoIds, RowIdShardGenerator, SequencePrivilegeCheck, SequenceSnapshot, ViewDefinerPrivilegeCheck, SqlDigestTextRetrieval, StatementClass, StatementPhase, StmtContext, StmtContextSessionState};
pub use table_access::TableAccess;
pub use table_dual::TableDualExec;
pub use tidb_decode_key::TidbDecodeKeySnapshot;
pub use tidb_distsql::ReplicaReadType;
/// The level a statement warning carries -- Go's three `contextutil` levels,
/// re-exported so the session can read the one a `StmtContext` recorded
/// without depending on `tidb-distsql` directly.
pub use tidb_distsql::WarningLevel as WarnLevel;
pub use tidb_expr::builtin_registry::builtin_list;
pub use tidb_expr::constant_fold::take_fold_warnings;
pub use tidb_expr::infer_pushdown::{blacklist_name, blacklist_store_mask, ExprPushDownBlacklist};
pub use tidb_expr::CurrentTso;
pub use tidb_expr::{eval_in, like_match_with_collation, truthy_of, BlockEncryptionMode, Columns, EvalError, JsonError, MysqlRng, SessionTimeZone};
pub use topn::TopNExec;
pub use view::{resolve_view_definition, view_column_list};

#[cfg(test)]
mod tests_admin_check_admintest_source;
#[cfg(test)]
mod tests_analyze_suite_source;
/// Explicit isolated spill authorities for executor tests.
#[cfg(test)]
mod tests_executor_internal_source;
#[cfg(test)]
mod tests_executor_part22_source;
#[cfg(test)]
mod tests_executor_suite_metadata_source;
#[cfg(test)]
mod tests_executor_suite_statements_source;
#[cfg(test)]
mod tests_hashagg_aggregate_suite_source;
#[cfg(test)]
mod tests_issuetest_b135_source;
#[cfg(test)]
mod tests_jointest_hashjoin_b135_source;
#[cfg(test)]
mod tests_jointest_join_b135_source;
#[cfg(test)]
mod tests_set_show_stats_slow_query_source;
#[cfg(test)]
pub(crate) mod test_temp_storage {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use tidb_util::spill_storage::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

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
pub mod metrics;
