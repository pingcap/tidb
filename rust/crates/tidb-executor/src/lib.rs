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

pub mod executor_errors;
pub mod consistency_errors;
pub mod admin_errors;
pub mod handle_range;
pub mod exec_stmt_observation;
pub mod adapter;
pub mod sessionctx;
pub mod stats_stmt_ctx;
pub use stats_stmt_ctx::{PendingStatisticsLoad, StatsStmtCtx, StatsStmtCtxData};
pub use sessionctx::StmtOutput;
pub mod admin;
pub mod ddl_exec;
pub mod stats_exec;
pub mod record_set;
pub mod stmt_ctx_build;
pub mod statement_cop_stats;
pub mod table_privilege;
pub mod compiler;
pub mod blacklist;
pub mod index_advisor;
pub mod bind_exec;
pub mod trace;
pub mod cluster_state;
pub mod inspection;
pub mod memtable_reader;
pub mod cluster_config;
pub mod cluster_diagnostics;
pub mod cluster_peer;
pub mod show;
pub mod ddl_jobs;
pub mod infoschema_reader;
pub mod memtable_retrievers;
pub mod compact_table;
pub mod set;
pub mod simple;
pub mod index_lookup_stats;
pub mod show_regions;
pub mod split_region;

pub mod access_cost;
pub mod access_path;
pub mod admin_check;
pub mod checksum;

pub mod analyze;
pub mod cluster_analyze;
pub mod real_tikv_analyze;
pub mod apply;
pub(crate) use tidb_table::bad_null;
#[cfg(test)]
mod tests_bad_null;
#[cfg(test)]
mod tests_predicate_pushdown;
#[cfg(test)]
mod tests_cluster_storage;
pub mod ddl;
pub mod ddl_sequence;
pub mod cop_reader;
pub mod mpp_coordinator;
#[cfg(test)]
pub(crate) mod test_region;
pub mod dag_request;
pub mod driver;
pub mod wide_scan_selection;
pub mod explain;
pub(crate) mod foreign_key;
use tidb_executor_internal_exec::predicate_pushdown::PushedScanFilter;
use tidb_executor_internal_exec::table_access::TableAccess;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_distsql::select_response::CopResponseSink;
#[cfg(test)]
mod tests_generated_column_ddl;
use tidb_distsql::split_ranges_across_int64_boundary;
#[cfg(test)]
mod hash_agg_spill_tests;
pub mod index_merge_reader;
pub mod import_into_options;
pub mod kv_table;
pub mod limit;
pub mod load_data;
pub mod max_one_row;
use tidb_variable::mem_quota;
#[cfg(test)]
mod tests_mem_quota;
pub mod mem_reader;
pub mod mem_table;
pub mod mem_table_retrieval;
use tidb_kv::mpp_query;
#[cfg(test)]
mod tests_mpp_query;
use tidb_table::partition_routing;
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
pub mod range_group_merge;
pub mod union_all;
pub mod slow_query;
pub use partition_routing::{PartitionDef, PartitionKind, PartitionSpec, RangeBound};
pub mod expand;
pub mod fts_like_rewrite;
pub mod projection;
pub mod remote_scan;
pub mod selection;
pub mod show_stats;
pub mod shuffle;
pub mod stats_lock;
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
pub mod union_scan;
pub mod view;

pub use apply::ApplyExec;
pub use ddl::run_create_table_on;
use tidb_ddl_build::table_lifecycle::split_drop_table_targets;
pub use ddl_sequence::show_create_sequence;
#[cfg(test)]
pub(crate) use driver::access::run_prepared_select_for_test;
pub use driver::plan_cache::{PlanCacheInvalidation, SessionPlanCache};
pub use driver::{
    access::{
        build_prepared_point_get_plan, build_prepared_select_plan, fast_point_process_plan_info,
        open_prepared_point_get,
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
    run_update_in, run_update_on, run_update_stmt, run_update_stmt_with_physical,
    PreparedDmlExecution, PreparedDmlPlan, SelectMeta,
};
use tidb_infoschema::{Catalog, MemTable, TableEntry, ViewDef, DEFAULT_DATABASE};
use tidb_driver_error::DriverError;
use tidb_terror::MysqlError;
use tidb_driver_error::ExecError;
pub use explain::{brief_binary_plan, explain_analyze_delete_stmt, explain_analyze_insert_stmt, explain_connection_plan, query_plan_cost, explain_analyze_physical_plan, explain_analyze_select_stmt, explain_analyze_set_opr_stmt, explain_analyze_update_stmt, explain_delete_stmt, explain_insert_stmt, explain_physical_plan, explain_select_stmt, explain_set_opr_stmt, explain_update_stmt, process_plan_info};
use tidb_sessionctx::explain_format::ExplainFormat;
use tidb_executor_join::join::{JoinExec, JoinKind};
pub use kv_table::{KvTableDdl, KvTablePartitionMaintenance, KvTableReader, RowDecodeContextExt, TableScanExec};
use tidb_ddl_build::table_backfill::KvTableBackfill;
use tidb_table::kv_table::{FkAction, KvColumn, KvTable, RowDecodeContext, TableCharset};
pub use limit::LimitExec;
pub use mem_quota::{SessionMemory, StatementMemory};
use tidb_vardef::OomAction;
pub use mem_table::MemTableSourceExec;
pub use mpp_query::MppQueryInfo;
pub use projection::ProjectionExec;
pub use selection::SelectionExec;
use tidb_sessionctx::{scanner_sql_mode, SessionPrivilegeCheck, LatestInfoSchema, LockedTables, RetryAutoIds, RowIdShardGenerator, SequencePrivilegeCheck, ViewDefinerPrivilegeCheck, SqlDigestTextRetrieval, StatementClass, StatementPhase, StmtContext, StmtContextSessionState};
use tidb_stmtctx::{PlanSource, ProcessPlanInfo, StoredPlan};
use tidb_variable::session_vars::DdlReorgVars;
use tidb_variable::MdlRelatedTableRecorder;
use tidb_kv::keys_read::KeysReadBudget;
pub use table_dual::TableDualExec;
use tidb_txnkv::ReplicaReadType;
/// The level a statement warning carries -- Go's three `contextutil` levels,
/// re-exported so the session can read the one a `StmtContext` recorded
/// without depending on `tidb-distsql` directly.
use tidb_expr::builtin_registry::builtin_list;
use tidb_expr::infer_pushdown::{blacklist_name, blacklist_store_mask, ExprPushDownBlacklist};
use tidb_exprctx::CurrentTso;
use tidb_exprctx::BlockEncryptionMode;
use tidb_exprctx::EvalError;
use tidb_datatype::SessionTimeZone;
use tidb_mathutil::MysqlRng;
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
pub mod cluster_analyze_seam;
#[cfg(test)]
pub(crate) mod test_temp_storage {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use tidb_spill_storage::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

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
use tidb_infoschema::tidb_decode_key::TidbDecodeKeySnapshot;

/// Go `ExecStmt.Exec` immediately after `buildExecutor` and before
/// `openExecutor`: `breakpoint.Inject(a.Ctx, sessiontxn.BreakPointBeforeExecutorFirstRun)`,
/// once per statement.
pub(crate) fn notify_before_executor_first_run(ctx: &StmtContext) {
    if ctx.claim_before_executor_first_run() {
        tidb_breakpoint::inject_stored_value(ctx.breakpoint_notify(), "beforeExecutorFirstRun");
    }
}
