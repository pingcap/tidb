//! Go `ImportIntoExec`'s `sctx` (`tidb_executor::import_into`), and the
//! test helper that compiles an IMPORT INTO into its import plan.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor_importer::Plan as ImportPlan;
use tidb_gocontext::Context;

use crate::*;

impl Session {
    /// Go `executor.Compiler.Compile` and `NewImportPlan` of the IMPORT INTO
    /// statement `sql`, as Go's `newQueryStepExecutor` test helper builds
    /// the plan the executor submits.
    pub fn new_import_plan_for_test(&mut self, sql: &str) -> Result<ImportPlan, DriverError> {
        let (stmt, _) = self.parse_import_query(sql)?;
        let node = tidb_ast::Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::ImportInto(Box::new(stmt.clone()))));
        let Some((tidb_planner::common_plans::Plan::ImportInto(plan), _, _)) = self.build_statement_plan(&node)? else {
            return Err(DriverError::from(tidb_terror::MysqlError::unknown("IMPORT INTO compiles to an import plan")));
        };
        let ctx = tidb_dxf_storage::with_internal_source_type(&Context::background(), tidb_txnkv::INTERNAL_IMPORT_INTO);
        tidb_executor::import_into::ImportIntoExec::new_import_plan(self, &ctx, &plan, &stmt)
    }
}

/// Go `ImportIntoExec`'s `sctx`.
impl tidb_executor::import_into::ImportIntoSession for Session {
    fn import_identity(&self) -> Option<(String, String)> {
        Session::current_identity(self).map(|(user, host)| (user.to_owned(), host.to_owned()))
    }

    fn has_any_scoped_privilege(&self, database: &str, table: &str, mask: u64) -> bool {
        Session::has_any_scoped_privilege(self, database, table, mask)
    }

    fn import_session_vars(&self) -> &tidb_variable::SessionVars {
        &self.vars
    }

    fn import_statement_context(&self) -> tidb_sessionctx::StmtContext {
        ResetContextOfStmt::statement_context(self, false)
    }

    fn capture_query_source(
        &self,
        sql: &str,
        stmt_ctx: &tidb_sessionctx::StmtContext,
    ) -> Result<tidb_executor_importer::query_source::QuerySource, DriverError> {
        Session::capture_query_source(self, sql, stmt_ctx)
    }

    fn refresh_latest_info_schema(&self) -> std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema> {
        tidb_executor::mview::MvSession::refresh_latest_info_schema(self)
    }

    fn with_planning_catalog<T>(
        &self,
        plan: impl FnOnce(&tidb_infoschema::SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        Session::with_planning_catalog(self, plan)
    }

    fn drain_eval_warnings(&mut self, ctx: &tidb_sessionctx::StmtContext) {
        Session::drain_eval_warnings(self, ctx);
    }

    fn import_stats_collector(&self) -> Option<&std::sync::Arc<tidb_stats_handle_usage::SessionStatsItem>> {
        self.stats_collector.as_ref()
    }

    fn set_statement_message(&mut self, message: String) {
        self.statement_message = message;
    }

    fn sql_killer(&self) -> std::sync::Arc<tidb_sqlkiller::SqlKiller> {
        Session::sql_killer(self)
    }
}
