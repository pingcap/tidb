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

//! Go `pkg/executor/builder.go`'s `executorBuilder.build` over the common
//! plans and each executor's `Next`: `SimpleExec`, `DDLExec`, the ADMIN
//! executors, IMPORT INTO, the materialized-view executors and the small
//! single-purpose ones. Queries and DML go through the physical builder
//! (`driver::physical_builder`).

use tidb_ast::Stmt;
use tidb_driver_error::DriverError;
use tidb_planner::common_plans::Plan;

use crate::StmtOutput;

/// What the executors `executorBuilder.build` builds reach through `sctx`
/// beyond their own session traits.
pub trait BuildSession:
    crate::simple::exec::SimpleExec
    + crate::ddl_exec::TemporaryTableDdlExec
    + crate::import_into::ImportIntoExec
    + crate::mview::MvSession
    + crate::show::exec::ShowExec
{
    /// Go's account executors (`SimpleExec`'s user, role and password arms,
    /// `GrantExec`, `RevokeExec`) over the domain's privilege tables: a
    /// system session's pessimistic transaction around `run`, then
    /// `NotifyUpdatePrivilege`.
    ///
    /// # Errors
    ///
    /// `run`'s, or the system session's.
    fn with_account_service(
        &mut self,
        run: &mut dyn FnMut(&mut Self) -> Result<Option<StmtOutput>, DriverError>,
    ) -> Result<Option<StmtOutput>, DriverError>;
    /// Whether `stmt` is an account statement (its executor writes the
    /// privilege tables).
    fn is_account_statement(&self, stmt: &Stmt) -> bool;
    /// The statement's text in the client's character set, which a refresh
    /// records.
    fn client_statement_sql(&self, stmt: &Stmt) -> String;
    /// Go `SetConfigExec`'s servers: the `TestSetConfigServerInfoKey`
    /// value, else `infoschema.GetClusterServerInfo`.
    ///
    /// # Errors
    ///
    /// The topology read's failure.
    fn set_config_server_info(
        &self,
        warnings: &mut Vec<String>,
    ) -> Result<Vec<tidb_domain::cluster_topology::ClusterServer>, String>;
    /// The domain's cluster config client (Go `InternalHTTPClient`'s
    /// scheme and transport).
    ///
    /// # Errors
    ///
    /// A client that cannot be built.
    fn cluster_config_client(&self) -> Result<std::sync::Arc<crate::cluster_config::ClusterConfigClient>, String>;
    /// Go `TestSetConfigHTTPHandlerKey`'s value, which `SetConfigExec`
    /// posts each node's request to instead of `InternalHTTPClient().Do`.
    fn set_config_http_handler(
        &self,
    ) -> Option<std::sync::Arc<dyn Fn(&str, String) -> Result<(u16, String, String), String> + Send + Sync>>;
    /// Go `AlterDDLJobExec` over the domain's DDL.
    ///
    /// # Errors
    ///
    /// Go `processAlterDDLJobConfig`'s error, or no running job.
    fn alter_ddl_job(
        &mut self,
        alter: &tidb_planner::common_plans::AlterDdlJob,
    ) -> Result<(), DriverError>;
}

/// Go `executorBuilder.build` and the built executor's `Next`.
pub trait ExecutorBuild: BuildSession {
    /// The executor of a common plan, run: `None` for a plan another door
    /// runs (a query, a DML, SET).
    ///
    /// # Errors
    ///
    /// The executor's error.
    fn execute_plan(
        &mut self,
        plan: &Plan,
        stmt: &Stmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        match plan {
            // The account statements' executors write the domain's
            // privilege tables.
            Plan::Simple(_) if self.is_account_statement(stmt) => self
                .with_account_service(&mut |session| crate::simple::exec::SimpleExec::simple_next(session, stmt)),
            // Go `ImportIntoActionExec`.
            Plan::Simple(_)
                if matches!(stmt, Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::CancelImportJob(_))) =>
            {
                let Stmt::Admin(admin) = stmt else { unreachable!() };
                let tidb_ast::AdminStmt::CancelImportJob(job_id) = &**admin else { unreachable!() };
                crate::import_into::ImportIntoExec::cancel_import_job(self, *job_id).map(Some)
            }
            // Go master `CancelMaterializedViewJobExec`.
            Plan::Simple(_)
                if matches!(stmt, Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::CancelMaterializedViewJob(_))) =>
            {
                let Stmt::Admin(admin) = stmt else { unreachable!() };
                let tidb_ast::AdminStmt::CancelMaterializedViewJob(cancel) = &**admin else { unreachable!() };
                crate::mview::mview_task::MviewTaskExec::cancel_materialized_view_job(self, cancel).map(Some)
            }
            Plan::Simple(_) => crate::simple::exec::SimpleExec::simple_next(self, stmt),
            Plan::ImportInto(plan) => {
                let Stmt::Dml(dml) = stmt else { unreachable!("ImportInto plan has an IMPORT statement") };
                let tidb_ast::DmlStmt::ImportInto(import) = dml.as_ref() else {
                    unreachable!("ImportInto plan has an IMPORT statement")
                };
                tidb_executor_importer::import_into_options::validate_import_into_options(import)?;
                crate::import_into::ImportIntoExec::import_into_exec(self, plan, import).map(Some)
            }
            Plan::Ddl(_) => crate::ddl_exec::TemporaryTableDdlExec::ddl_exec(self, stmt),
            // Go master `RefreshMaterializedViewExec`.
            Plan::RefreshMaterializedView(refresh) => {
                let sql = self.client_statement_sql(stmt);
                crate::mview::mview_refresh::MviewRefreshExec::refresh_materialized_view(self, refresh, &sql).map(Some)
            }
            // Go master `RefreshMaterializedViewDryRunExec` /
            // `RefreshMaterializedViewProfileExec`.
            Plan::DryRunRefreshMaterializedView(plan) => {
                crate::mview::mv_refresh_observability::MvRefreshObservabilityExec::refresh_materialized_view_dry_run(
                    self,
                    &plan.statement,
                )
                .map(Some)
            }
            Plan::ProfileRefreshMaterializedView(plan) => {
                let sql = self.client_statement_sql(stmt);
                crate::mview::mv_refresh_observability::MvRefreshObservabilityExec::refresh_materialized_view_profile(
                    self,
                    &plan.statement,
                    &sql,
                )
                .map(Some)
            }
            // Go master `PurgeMaterializedViewLogExec`.
            Plan::PurgeMaterializedViewLog(purge) => {
                crate::mview::mlog_purge::MlogPurgeExec::purge_materialized_view_log(self, purge).map(Some)
            }
            // Go master `CompareMaterializedViewExec`.
            Plan::CompareMaterializedView(compare) => {
                crate::mview::mview_compare::MviewCompareExec::compare_materialized_view(self, compare).map(Some)
            }
            // Go master `MViewDeltaMergeAggExec`.
            Plan::MViewDeltaMerge(merge) => {
                crate::mview::mview_refresh::MviewRefreshExec::mview_delta_merge(self, merge).map(Some)
            }
            // Go master `MViewCompleteDeltaApplyExec`.
            Plan::MViewCompleteDeltaApply(apply) => {
                crate::mview::mview_refresh::MviewRefreshExec::mview_complete_delta_apply(self, apply).map(Some)
            }
            Plan::CompactTable(compact) => compact_table_tiflash_exec(self, compact).map(Some),
            Plan::SetConfig(plan) => set_config_exec(self, plan).map(Some),
            // Go `DistributeTableExec`: the job's ID, or no row.
            Plan::DistributeTable(plan) => {
                let job_id = crate::distribute::DistributeTableExec::open((**plan).clone())?.next()?;
                Ok(Some(StmtOutput::Rows {
                    columns: vec![(
                        "JOB_ID".to_owned(),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                    )],
                    rows: job_id.map(|id| vec![tidb_datatype::Datum::UInt(id)]).into_iter().collect(),
                }))
            }
            // Go `AlterDDLJobExec`: the job's row rewritten, no result set.
            Plan::AlterDdlJob(alter) => {
                self.alter_ddl_job(alter)?;
                Ok(Some(StmtOutput::Done(true)))
            }
            Plan::ShowNextRowID(plan) => show_next_row_id_exec(self, plan).map(Some),
            Plan::Logical(_) | Plan::Physical(_) | Plan::Set(_) => Ok(None),
            _ => match stmt {
                Stmt::Admin(admin) => crate::admin::AdminExec::admin_next(self, admin),
                _ => Ok(None),
            },
        }
    }
}

impl<T: BuildSession> ExecutorBuild for T {}

/// Go `SetConfigExec`: `Open` checks the target and converts the value,
/// `Next` posts it to the selected nodes (`GetClusterServerInfo`), each
/// node's failure a warning.
fn set_config_exec<S: BuildSession>(
    session: &mut S,
    plan: &tidb_planner::common_plans::SetConfig,
) -> Result<StmtOutput, DriverError> {
    let ctx = crate::stmt_ctx_build::ResetContextOfStmt::statement_context(session, false);
    let mut exec = crate::set_config::SetConfigExec::new(plan.clone());
    exec.open(&ctx)?;
    let mut warnings = Vec::new();
    let unknown = |error: String| DriverError::Mysql(tidb_terror::MysqlError::unknown(error));
    let servers = session.set_config_server_info(&mut warnings).map_err(unknown)?;
    let client = session.cluster_config_client().map_err(unknown)?;
    let test_handler = session.set_config_http_handler();
    let post = |url: &str, body: String| client.post(url, body);
    let handler: crate::set_config::HttpHandler<'_> = match &test_handler {
        Some(handler) => handler.as_ref(),
        None => &post,
    };
    let result = exec.next(servers, client.scheme(), handler, &mut warnings);
    for warning in warnings {
        crate::sessionctx::SessionContext::append_sql_warning(
            session,
            tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Warning, 1105, warning),
        );
    }
    result.map(|()| StmtOutput::Affected(0))
}

/// Go `CompactTableTiFlashExec.Next` over the table the planner resolved
/// through the session's infoschema (`buildCompactTable`'s
/// `resolveCtx.GetTableName`), a read.
fn compact_table_tiflash_exec<S: BuildSession>(
    session: &mut S,
    compact: &tidb_ast::CompactTableStmt,
) -> Result<StmtOutput, DriverError> {
    let ctx = crate::stmt_ctx_build::ResetContextOfStmt::statement_context(session, false);
    let current_db = crate::sessionctx::SessionContext::session_vars(session).current_db.clone();
    let result = crate::import_into::ImportIntoSession::with_planning_catalog(session, |catalog| {
        crate::compact_table::build_compact_table(compact, catalog, &current_db, &ctx)
    });
    crate::import_into::ImportIntoSession::drain_eval_warnings(session, &ctx);
    result.map(|()| StmtOutput::Affected(0))
}

/// Go `executorBuilder.buildShowNextRowID`'s executor, for both spellings
/// (`SHOW TABLE t NEXT_ROW_ID` and `ADMIN SHOW t NEXT_ROW_ID`).
pub fn show_next_row_id_exec<S: BuildSession>(
    session: &mut S,
    plan: &tidb_planner::common_plans::ShowNextRowID,
) -> Result<StmtOutput, DriverError> {
    let rows = crate::show::exec::ShowExec::show_next_row_id_rows(session, &plan.table_name)?;
    let columns = plan
        .names
        .iter()
        .zip(&plan.schema.columns)
        .map(|(name, column)| {
            (
                name.names.column.original.clone(),
                column
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar)),
            )
        })
        .collect();
    Ok(StmtOutput::Rows { columns, rows })
}
