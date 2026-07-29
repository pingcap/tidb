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
//! `Close` driver that ties parsed plans to results. It is a native-boundary
//! split of `pkg/executor` (kept separate from the large `tidb-exec` crate of
//! individual operator fragments so the wired engine builds fast). It depends
//! only on `tidb-chunk` (the row batches), `tidb-expr` (expression evaluation),
//! and `tidb-datatype`.
//!
//! SEED SCOPE: the `Executor` trait core (open/next/close/schema/ret_field_types/
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
//! so this one is upstream and never sees it. (The older "only three deps, seed
//! scope" framing was outgrown: the real dependency set is 11 crates, and
//! `tidb-exec` carries a second, non-production engine — see its crate doc.)
//!
//! DEFERRED (documented): the Go `context.Context`/`sessionctx` propagation,
//! runtime stats, the SQL killer, `Detach`, and parallel projection.

pub mod access_cost;
pub mod access_path;
pub mod apply;
mod approx_count_distinct;
pub mod cluster_storage;
pub(crate) mod column_prune;
pub mod ddl;
pub mod driver;
pub mod executor;
pub mod explain;
mod farmhash;
pub mod hash_agg;
mod hash_join;
mod index_range;
pub mod join;
pub mod kv_table;
pub mod limit;
pub mod mem_table;
mod plan_trace;
pub mod projection;
pub mod pushdown_scan;
pub mod scan_pushdown;
pub mod selection;
pub mod sort;
mod stmt_context;
pub mod storage;
pub mod table_access;
pub mod table_dual;
pub mod view;
mod window;

pub use apply::ApplyExec;
pub use ddl::{
    run_alter_table_in, run_create_index_in, run_create_table_in, run_create_table_on,
    run_drop_index_in, run_drop_table_in, run_rename_table_in, run_truncate_table_in,
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
pub use kv_table::{IndexRange, KvColumn, KvIndex, KvTable, TableCharset, TableScanExec};
pub use limit::LimitExec;
pub use mem_table::MemTableSourceExec;
pub use projection::ProjectionExec;
pub use scan_pushdown::{PushedScanFilter, ScanComparison, ScanComparisonOp};
pub use selection::SelectionExec;
pub use sort::{SortByItem, SortExec};
pub use stmt_context::StmtContext;
pub use table_access::TableAccess;
pub use table_dual::TableDualExec;
pub use tidb_expr::{
    eval_in, like_match_with_collation, truthy_of, Columns, EvalError, JsonError, MysqlRng,
    SessionTimeZone,
};
pub use view::{run_create_view_in, run_drop_view_in, view_column_list};
