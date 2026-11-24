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

//! Go master's materialized-view executors' `sctx`
//! (`tidb_executor::mview::MvSession`): this session, its domain's system
//! sessions, snapshots and DDL.

use std::sync::{Arc, Mutex};

use tidb_driver_error::DriverError;
use tidb_executor::mview::mv_refresh_observability::MvRefreshObserveCollector;
use tidb_infoschema::SessionExtendedInfoSchema;

use crate::Session;

impl Session {
    fn domain_handle(&self) -> Option<Arc<tidb_domain_core::domain::Domain>> {
        self.domain_services().and_then(|services| services.domain.upgrade())
    }
}

impl tidb_executor::mview::MvSession for Session {
    fn open_refresh_system_session(&self) -> Result<Self, DriverError> {
        use crate::domain::cluster_session_node::DomainSessions as _;
        let domain = self
            .domain_handle()
            .ok_or_else(|| DriverError::unsupported("the session has no domain to take a system session from"))?;
        domain.open_system_session().map_err(crate::domain::cluster_session_node::sql_query_error_to_driver)
    }

    fn vars(&self) -> &tidb_variable::SessionVars {
        &self.vars
    }

    fn vars_mut(&mut self) -> &mut tidb_variable::SessionVars {
        &mut self.vars
    }

    fn statement_message(&self) -> &str {
        Session::statement_message(self)
    }

    fn set_statement_message(&mut self, message: String) {
        self.statement_message = message;
    }

    fn exec_ctx(&self) -> tidb_gocontext::Context {
        self.exec_ctx.clone()
    }

    fn mv_refresh_observer(&self) -> Option<Arc<Mutex<MvRefreshObserveCollector>>> {
        self.mv_refresh_observer.clone()
    }

    fn set_mv_refresh_observer(&mut self, observer: Option<Arc<Mutex<MvRefreshObserveCollector>>>) {
        self.mv_refresh_observer = observer;
    }

    fn process_plan_info(&self) -> Arc<Mutex<tidb_stmtctx::ProcessPlanInfo>> {
        Arc::clone(&self.process_plan_info)
    }

    fn take_refresh_plan_input(&mut self) -> Option<(u64, Vec<tidb_model::MViewRefreshSource>)> {
        self.refresh_plan_input.take()
    }

    fn set_refresh_plan_input(&mut self, input: Option<(u64, Vec<tidb_model::MViewRefreshSource>)>) {
        self.refresh_plan_input = input;
    }

    fn set_active_resource_group(&mut self, group: String) {
        self.active_resource_group = group;
    }

    fn affected_rows(&self) -> u64 {
        Session::affected_rows(self)
    }

    fn append_warning(&mut self, level: tidb_context::WarningLevel, code: u16, message: String) {
        Session::append_warning(self, level, code, message);
    }

    fn begin_query_cancellation(&self) -> tidb_sqlkiller::StatementCancellation {
        Session::begin_query_cancellation(self)
    }

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
    > {
        // Go `Compile`: the preprocessor, then `PlanBuilder.Build`.
        self.validate_ddl_preprocess(stmt)?;
        Session::build_statement_plan(self, stmt)
    }

    fn optimize_refresh_implement(
        &self,
        plan: &mut tidb_planner::common_plans::Plan,
        ctx: &tidb_sessionctx::StmtContext,
        retained_upper_tso: &dyn Fn() -> Result<u64, DriverError>,
    ) -> Result<(), DriverError> {
        Session::optimize_refresh_implement(self, plan, ctx, retained_upper_tso)
    }

    fn calculate_as_of_ts_expr(&mut self, expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
        Session::calculate_as_of_ts_expr(self, expr)
    }

    fn current_database(&self) -> &str {
        Session::current_database(self)
    }

    fn current_for_update_ts(&self) -> tidb_exprctx::CurrentTso {
        Session::current_for_update_ts(self)
    }

    fn current_identity(&self) -> Option<(&str, &str)> {
        Session::current_identity(self)
    }

    fn has_table_privilege(&self, database: &str, table: &str, global_priv: tidb_mysql::PrivilegeType) -> bool {
        Session::has_table_privilege(self, database, table, global_priv)
    }

    fn take_var_warnings(&mut self) {
        Session::take_var_warnings(self);
    }

    fn with_catalog_scope<T>(
        &mut self,
        include_local: bool,
        body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        Session::with_catalog_scope(self, include_local, body)
    }

    fn with_planning_catalog<T>(
        &self,
        plan: impl FnOnce(&SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        Session::with_planning_catalog(self, plan)
    }

    fn with_staged_catalog<T>(
        &mut self,
        body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        Session::with_staged_catalog(self, body)
    }

    fn execute_internal_under(
        &mut self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> Result<(Vec<Vec<tidb_datatype::Datum>>, Vec<tidb_resolve::ResultFieldRef>), tidb_sqlexec::SqlExecError> {
        crate::domain::cluster_session_node::execute_internal_under(self, context, sql)
    }

    fn store_current_version(&mut self) -> Result<u64, String> {
        self.node_conn().store_current_version()
    }

    fn explicit_start_ts(&mut self) -> Option<u64> {
        self.node_conn().explicit_start_ts()
    }

    fn run_ordinary_write(
        &mut self,
        sql: &str,
        stmt: &tidb_ast::Stmt,
    ) -> Result<(u64, u64), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        self.node_conn().run_ordinary_write(sql, stmt)
    }

    fn has_domain(&self) -> bool {
        self.domain_handle().is_some()
    }

    fn refresh_latest_info_schema(&self) -> Arc<SessionExtendedInfoSchema> {
        if let Some(domain) = self.domain_handle() {
            return domain.catalog.get_latest();
        }
        match &self.node {
            Some(node) => node.latest_info_schema(),
            None => Arc::new(self.shared_catalog().lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()),
        }
    }

    fn snapshot_info_schema(&self, ts: u64) -> Result<SessionExtendedInfoSchema, DriverError> {
        let domain = self
            .domain_handle()
            .ok_or_else(|| DriverError::unsupported("the session has no domain to read a snapshot schema from"))?;
        crate::domain::cluster_session_node::get_snapshot_info_schema(&domain, ts)
    }

    fn validate_snapshot_gc(&self, ts: u64) -> Result<(), DriverError> {
        let restricted = self
            .restricted_sql_executor()
            .ok_or_else(|| DriverError::unsupported("the session has no restricted SQL executor to read the GC safe point"))?;
        crate::gcutil::validate_snapshot(restricted.as_ref(), ts)
    }

    fn fresh_timestamp(&self) -> Result<u64, String> {
        let domain = self.domain_handle().ok_or("the session has no domain")?;
        domain.transactions.fresh_timestamp()
    }

    fn snapshot_meta(&self, ts: u64) -> Result<tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot, String> {
        let domain = self.domain_handle().ok_or("the session has no domain")?;
        domain.get_snapshot_meta(ts)
    }

    fn reload_schema(&self) -> Result<(), String> {
        let domain = self.domain_handle().ok_or("the session has no domain")?;
        domain.ddl.reload_schema()
    }

    fn ddl_service(&self) -> Option<Arc<dyn tidb_executor::ddl_exec::DdlService + Send + Sync>> {
        self.domain_services().map(|services| Arc::clone(&services.ddl))
    }

    fn mview_snapshot_reader_context(
        &self,
        ctx: &tidb_sessionctx::StmtContext,
    ) -> Box<dyn Fn(u64) -> Result<tidb_sessionctx::StmtContext, DriverError>> {
        let domain = self.domain_handle();
        let ctx = ctx.clone();
        Box::new(move |to_ts| {
            let domain =
                domain.as_ref().ok_or_else(|| DriverError::unsupported("the session has no domain to read a snapshot from"))?;
            let snapshot = domain
                .transactions
                .open_snapshot_at(to_ts, tidb_model::DEFAULT_RESOURCE_GROUP_NAME)
                .map_err(DriverError::unsupported)?;
            let store = ctx
                .txn_store()
                .and_then(|store| store.snapshot_reader_at(snapshot))
                .ok_or_else(|| DriverError::unsupported("the statement's store cannot read at a snapshot"))?;
            Ok(ctx.clone().with_txn_store(store))
        })
    }
}
