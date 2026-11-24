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
    /// Go `SetConfigExec`.
    ///
    /// # Errors
    ///
    /// The target check or the post's failure.
    fn set_config_exec(
        &mut self,
        plan: &tidb_planner::common_plans::SetConfig,
    ) -> Result<StmtOutput, DriverError>;
    /// Go `CompactTableTiFlashExec`.
    ///
    /// # Errors
    ///
    /// The compaction's failure.
    fn compact_table_tiflash_exec(
        &mut self,
        compact: &tidb_ast::CompactTableStmt,
    ) -> Result<StmtOutput, DriverError>;
    /// Go `ShowExec` for `SHOW TABLE ... NEXT_ROW_ID`.
    ///
    /// # Errors
    ///
    /// The allocator read's failure.
    fn show_next_row_id_exec(
        &mut self,
        plan: &tidb_planner::common_plans::ShowNextRowID,
    ) -> Result<StmtOutput, DriverError>;
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
            Plan::CompactTable(compact) => self.compact_table_tiflash_exec(compact).map(Some),
            Plan::SetConfig(plan) => self.set_config_exec(plan).map(Some),
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
            Plan::ShowNextRowID(plan) => self.show_next_row_id_exec(plan).map(Some),
            Plan::Logical(_) | Plan::Physical(_) | Plan::Set(_) => Ok(None),
            _ => match stmt {
                Stmt::Admin(admin) => crate::admin::AdminExec::admin_next(self, admin),
                _ => Ok(None),
            },
        }
    }
}

impl<T: BuildSession> ExecutorBuild for T {}
