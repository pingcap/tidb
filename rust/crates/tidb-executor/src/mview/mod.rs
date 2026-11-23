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

//! Go master `pkg/executor`'s materialized-view executors: REFRESH
//! (`mview_refresh.go`, `mv_refresh_observability.go`), PURGE MATERIALIZED
//! VIEW LOG (`mlog_purge.go`), COMPARE (`mview_compare.go`), CANCEL ... JOB
//! and the task monitor (`mview_task.go`), and their SHOW arms. They run over
//! [`MvSession`], the session reach Go takes through `sctx`, and on system
//! sessions of the same kind from the domain's pool.

pub mod mlog_purge;
pub mod mv_refresh_observability;
pub mod mview_compare;
pub mod mview_refresh;
pub mod mview_show;
pub mod mview_task;

use crate::DriverError;

/// The session reach Go's materialized-view executors take through `sctx`;
/// their system sessions (Go `sysSessionPool`) are of the same kind.
pub trait MvSession: crate::stmt_ctx_build::ResetContextOfStmt + Sized + Send + 'static {
    /// Go `sysSessionPool.Get()`: a system session from the domain.
    ///
    /// # Errors
    ///
    /// When the session has no domain, or the pool's failure.
    fn open_refresh_system_session(&self) -> Result<Self, DriverError>;
    /// Go `sctx.GetSessionVars()`.
    fn vars(&self) -> &tidb_variable::SessionVars;
    /// Go `sctx.GetSessionVars()`, changed.
    fn vars_mut(&mut self) -> &mut tidb_variable::SessionVars;
    /// Go `StmtCtx.GetMessage`.
    fn statement_message(&self) -> &str;
    /// Go `StmtCtx.SetMessage`.
    fn set_statement_message(&mut self, message: String);
    /// The statement's context (Go `ctx`).
    fn exec_ctx(&self) -> tidb_gocontext::Context;
    /// The refresh observer (`WITH PROFILE` / `DRY RUN`) in force.
    fn mv_refresh_observer(&self) -> Option<std::sync::Arc<std::sync::Mutex<mv_refresh_observability::MvRefreshObserveCollector>>>;
    /// Installs or clears the refresh observer.
    fn set_mv_refresh_observer(
        &mut self,
        observer: Option<std::sync::Arc<std::sync::Mutex<mv_refresh_observability::MvRefreshObserveCollector>>>,
    );
    /// Go `ProcessInfo.Plan` of the statement.
    fn process_plan_info(&self) -> std::sync::Arc<std::sync::Mutex<tidb_stmtctx::ProcessPlanInfo>>;
    /// The refresh plan's read TSO and sources, once.
    fn take_refresh_plan_input(&mut self) -> Option<(u64, Vec<tidb_model::MViewRefreshSource>)>;
    /// Records the refresh plan's read TSO and sources.
    fn set_refresh_plan_input(&mut self, input: Option<(u64, Vec<tidb_model::MViewRefreshSource>)>);
    /// Go `StmtCtx.ResourceGroupName`.
    fn set_active_resource_group(&mut self, group: String);
    /// Go `StmtCtx.AffectedRows()`.
    fn affected_rows(&self) -> u64;
    /// Appends one warning to the statement.
    fn append_warning(&mut self, level: tidb_context::WarningLevel, code: u16, message: String);
    /// The statement's cancellation handle.
    fn begin_query_cancellation(&self) -> tidb_sqlkiller::StatementCancellation;
    /// Go `planner.Optimize`'s build of `stmt`.
    ///
    /// # Errors
    ///
    /// The build's failure.
    #[allow(clippy::type_complexity)]
    fn build_statement_plan(
        &self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<
        Option<(
            tidb_planner::common_plans::Plan,
            Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
            tidb_sessionctx::StmtContext,
        )>,
        DriverError,
    >;
    /// The refresh implement's physical optimization.
    ///
    /// # Errors
    ///
    /// The optimization's failure.
    fn optimize_refresh_implement(
        &self,
        plan: &mut tidb_planner::common_plans::Plan,
        ctx: &tidb_sessionctx::StmtContext,
        retained_upper_tso: &dyn Fn() -> Result<u64, DriverError>,
    ) -> Result<(), DriverError>;
    /// Go `CalculateAsOfTsExpr`.
    ///
    /// # Errors
    ///
    /// The expression's failure.
    fn calculate_as_of_ts_expr(&mut self, expr: &tidb_ast::Expr) -> Result<u64, DriverError>;
    /// The session's current database.
    fn current_database(&self) -> &str;
    /// The statement's for-update TSO.
    fn current_for_update_ts(&self) -> tidb_exprctx::CurrentTso;
    /// `CURRENT_USER()` split into user and host.
    fn current_identity(&self) -> Option<(&str, &str)>;
    /// Go `pm.RequestVerification` on one table.
    fn has_table_privilege(&self, database: &str, table: &str, global_priv: tidb_mysql::PrivilegeType) -> bool;
    /// The variable hooks' warnings, onto the statement.
    fn take_var_warnings(&mut self);
    /// The session's infoschema with or without its local temporary tables.
    ///
    /// # Errors
    ///
    /// `body`'s.
    fn with_catalog_scope<T>(
        &mut self,
        include_local: bool,
        body: impl FnOnce(&mut tidb_infoschema::SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError>;
    /// The planning catalog.
    ///
    /// # Errors
    ///
    /// `plan`'s.
    fn with_planning_catalog<T>(
        &self,
        plan: impl FnOnce(&tidb_infoschema::SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError>;
    /// The transaction's staged catalog.
    ///
    /// # Errors
    ///
    /// `body`'s.
    fn with_staged_catalog<T>(
        &mut self,
        body: impl FnOnce(&mut tidb_infoschema::SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError>;
    /// Go `ExecuteInternal` under `context`: rows and fields.
    ///
    /// # Errors
    ///
    /// The statement's failure.
    #[allow(clippy::type_complexity)]
    fn execute_internal_under(
        &mut self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> Result<(Vec<Vec<tidb_datatype::Datum>>, Vec<tidb_resolve::ResultFieldRef>), tidb_sqlexec::SqlExecError>;
    /// Go `store.CurrentVersion`.
    ///
    /// # Errors
    ///
    /// The store's failure.
    fn store_current_version(&mut self) -> Result<u64, String>;
    /// Go `Txn(true).StartTS()` of an activated explicit transaction.
    fn explicit_start_ts(&mut self) -> Option<u64>;
    /// One ordinary write statement (already planned as `stmt`).
    ///
    /// # Errors
    ///
    /// The statement's failure.
    fn run_ordinary_write(
        &mut self,
        sql: &str,
        stmt: &tidb_ast::Stmt,
    ) -> Result<(u64, u64), tidb_store_driver_error::sql_query_error::SqlQueryError>;
    /// Whether the session has a domain.
    fn has_domain(&self) -> bool;
    /// Go `domain.GetDomain(sctx).InfoSchema()`.
    fn refresh_latest_info_schema(&self) -> std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema>;
    /// Go `staleread.GetSessionSnapshotInfoSchema` at `ts`.
    ///
    /// # Errors
    ///
    /// The snapshot's failure.
    fn snapshot_info_schema(&self, ts: u64) -> Result<tidb_infoschema::SessionExtendedInfoSchema, DriverError>;
    /// Go `gcutil.ValidateSnapshotWithGCSafePoint`.
    ///
    /// # Errors
    ///
    /// When `ts` is behind the GC safe point.
    fn validate_snapshot_gc(&self, ts: u64) -> Result<(), DriverError>;
    /// A fresh timestamp from the store.
    ///
    /// # Errors
    ///
    /// The store's failure.
    fn fresh_timestamp(&self) -> Result<u64, String>;
    /// Go `domain.GetSnapshotMeta(ts)`.
    ///
    /// # Errors
    ///
    /// The snapshot's failure.
    fn snapshot_meta(&self, ts: u64) -> Result<tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot, String>;
    /// Go `domain.Reload()`.
    ///
    /// # Errors
    ///
    /// The reload's failure.
    fn reload_schema(&self) -> Result<(), String>;
    /// Go's DDL executor.
    fn ddl_service(&self) -> Option<std::sync::Arc<dyn crate::ddl_exec::DdlService + Send + Sync>>;
    /// Go `newDataReaderBuilderWithSnapshot`'s reader: `ctx` reading at a
    /// snapshot timestamp.
    #[allow(clippy::type_complexity)]
    fn mview_snapshot_reader_context(
        &self,
        ctx: &tidb_sessionctx::StmtContext,
    ) -> Box<dyn Fn(u64) -> Result<tidb_sessionctx::StmtContext, DriverError>>;
}
