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
pub mod analyze;
pub mod apply;
mod approx_count_distinct;
pub(crate) mod bad_null;
pub mod cluster_storage;
pub mod column_default;
pub(crate) mod column_prune;
pub mod ddl;
pub mod ddl_sequence;
pub mod driver;
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
mod hash_join;
mod index_range;
pub mod join;
pub mod kv_table;
pub mod limit;
pub mod mem_quota;
pub mod mem_table;
pub mod partition_routing;
mod plan_trace;
pub mod predicate_pushdown;
pub mod projection;
pub mod remote_scan;
pub mod selection;
pub mod sequence;
mod skyline;
pub mod sort;
mod stmt_context;
pub mod storage;
pub mod table_access;
pub mod table_dual;
pub mod view;
mod window;
pub mod zero_date;

pub use apply::ApplyExec;
pub use ddl::{
    check_constraint_count, linear_partitioning_warning, run_alter_table_in, run_create_index_in,
    run_create_table_in, run_create_table_on, run_drop_index_in, run_drop_table_in,
    run_rename_table_in, run_truncate_table_in,
};
pub use ddl_sequence::{
    run_alter_sequence_in, run_create_sequence_in, run_drop_sequence_in, show_create_sequence,
};
pub use driver::{
    bind_parameters, parameter_count, run_delete_in, run_delete_on, run_insert_in, run_insert_on,
    run_insert_reporting, run_select, run_select_meta_in, run_select_meta_on, run_select_meta_stmt,
    run_select_on, run_set_opr_stmt, run_update_in, run_update_on, Catalog, DriverError, MemTable,
    MysqlError, SchemaErrorKind, SelectMeta, TableEntry, TxnErrorKind, VarErrorKind, ViewDef,
    DEFAULT_DATABASE,
};
pub use executor::{ExecError, Executor, ExecutorMeta};
pub use explain::{
    explain_analyze_delete_stmt, explain_analyze_insert_stmt, explain_analyze_select_stmt,
    explain_analyze_update_stmt, explain_delete_stmt, explain_insert_stmt, explain_select_stmt,
    explain_update_stmt, ExplainFormat,
};
pub use hash_agg::{AggFunc, AggKind, HashAggExec};
pub use join::{JoinExec, JoinKind};
pub use kv_table::{
    FkAction, IndexRange, KvColumn, KvForeignKey, KvIndex, KvTable, TableCharset, TableScanExec,
};
pub use limit::LimitExec;
pub use mem_quota::{OomAction, StatementMemory};
pub use mem_table::MemTableSourceExec;
pub use predicate_pushdown::{PushedScanFilter, ScanComparison, ScanComparisonOp, ScanPredicate};
pub use projection::ProjectionExec;
pub use selection::SelectionExec;
pub use sort::{SortByItem, SortExec};
pub use stmt_context::{SequenceSnapshot, StmtContext};
pub use table_access::TableAccess;
pub use table_dual::TableDualExec;
pub use tidb_expr::{
    eval_in, like_match_with_collation, truthy_of, Columns, EvalError, JsonError, MysqlRng,
    SessionTimeZone,
};
pub use view::{run_create_view_in, run_drop_view_in, view_column_list};
