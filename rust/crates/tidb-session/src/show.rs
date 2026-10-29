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

//! The session side of Go's SHOW and ADMIN statements: `Compile` + `runStmt`
//! for a planned SHOW (`run_planned_show`), and the session primitives
//! `tidb_executor::show::exec::ShowExec`, `tidb_executor::admin::AdminExec`
//! and `tidb_executor::simple::exec::SimpleExec` read -- Go's `sctx`, its
//! domain and its session manager. The executors' bodies live in
//! `tidb_executor`.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_privilege::PrivilegeTypeExt;
use crate::*;
use tidb_executor::show::exec::ShowExec;

impl Session {
    /// Go `Compile` + `runStmt` for a SHOW the planner builds: the statement
    /// preprocessed (`resolveShowStmt`) and planned (`buildShow` and the
    /// optimizer), its `PhysicalShow`'s rows fetched ([`Self::fetch_show`],
    /// Go `ShowExec.fetchAll`), and the plan's selections, limit, projection
    /// and sort run over them. Planning and running read the statement's
    /// catalog (its snapshot and temporary tables); fetching holds none, so
    /// a fetcher reads it as it needs.
    pub(crate) fn run_planned_show(
        &mut self,
        show: &tidb_ast::ShowStmt,
    ) -> Result<StmtOutput, DriverError> {
        let auth_user = self.vars.current_user.clone().and_then(|identity| {
            identity
                .split_once('@')
                .map(|(user, host)| (user.to_owned(), host.to_owned()))
        });
        let ctx = self.statement_context(false);
        let current_db = self.vars.current_db.clone();
        let (plan, visits) = self.with_catalog_mut(|catalog| {
            tidb_executor::driver::plan_show_stmt(
                show,
                auth_user.as_ref().map(|(user, host)| (user.as_str(), host.as_str())),
                catalog,
                &current_db,
                &ctx,
            )
        })?;
        // Go `CheckPrivilege` over `buildShow`'s visit infos.
        self.check_visit_infos(&visits)?;
        let mut physical = match plan {
            tidb_planner::common_plans::Plan::Physical(physical) => physical,
            // Go `executorBuilder.buildShowNextRowID`.
            tidb_planner::common_plans::Plan::ShowNextRowID(plan) => {
                let rows = self.show_next_row_id_rows(&plan.table_name)?;
                let columns = plan
                    .names
                    .iter()
                    .zip(&plan.schema.columns)
                    .map(|(name, column)| {
                        (
                            name.names.column.original.clone(),
                            column.ret_type.clone().unwrap_or_else(|| {
                                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar)
                            }),
                        )
                    })
                    .collect();
                return Ok(StmtOutput::Rows { columns, rows });
            }
            _ => return Err(DriverError::unsupported("a SHOW planned as no SHOW plan")),
        };
        // A selection the optimizer folded to false leaves no `PhysicalShow`
        // to fetch for, as Go builds no `ShowExec` under a `TableDual`.
        if let Some(leaf) = tidb_executor::driver::physical_show_of(&physical).cloned() {
            let rows = self.fetch_show(&leaf)?;
            ctx.show_retrieval().supply(leaf.base.base.id(), rows);
        }
        let (columns, rows) = self.with_catalog_retrieving(&ctx, |catalog| {
            tidb_executor::driver::open_show_plan(&mut physical, catalog, &ctx)?.collect()
        })?;
        Ok(StmtOutput::Rows { columns, rows })
    }

    /// Go `ShowExec.fetchShowStatus`: one `Variable_name | Value` row per
    /// status variable `variable.GetStatusVars` collects from the registered
    /// `Statistics` providers, sorted by name; `GLOBAL` skips the
    /// session-only ones, and SEM hides its invisible ones.
    fn show_status_rows(&self, global: bool) -> Result<Vec<Vec<Datum>>, DriverError> {
        let status_vars = tidb_variable::statusvar::get_status_vars(Some(&self.vars))
            .map_err(|error| DriverError::Mysql(tidb_executor::MysqlError::unknown(error)))?;
        Ok(status_vars
            .into_iter()
            .filter(|(name, status)| {
                !(global && status.scope == tidb_variable::sysvar::SCOPE_SESSION)
                    && !self.sem_hides_status_var(name)
            })
            .map(|(name, status)| {
                vec![
                    Datum::Bytes(name.into_bytes()),
                    Datum::Bytes(status.value.into_bytes()),
                ]
            })
            .collect())
    }

}

impl Session {
    /// Go `buildShow` for SHOW CREATE TABLE/SEQUENCE/VIEW: a view needs SHOW
    /// VIEW, anything else any privilege but CREATE TEMPORARY TABLES (a
    /// local temporary table any at all), each refused as its own command
    /// denied on the lower-cased name. A missing table is checked as a table.
    pub(crate) fn require_show_create_privilege(
        &self,
        database: &str,
        table_name: &str,
    ) -> Result<(), DriverError> {
        let Some((_, user, host)) = self.privilege_context() else {
            return Ok(());
        };
        let is_view = self.lock_catalog()?.is_view_in(database, table_name);
        let (granted, command) = if is_view {
            (
                self.has_scoped_privilege(
                    database,
                    table_name,
                    tidb_mysql::ShowViewPriv,
                ),
                "SHOW VIEW",
            )
        } else {
            let mut mask = tidb_privilege::any_priv_mask();
            if !self.is_local_temporary_table(database, table_name) {
                mask &= !tidb_mysql::CreateTMPTablePriv.bit();
            }
            (
                self.has_any_scoped_privilege(database, table_name, mask),
                "SHOW",
            )
        };
        if granted {
            return Ok(());
        }
        Err(DriverError::from(
            tidb_error::plannererrors::ERR_TABLEACCESS_DENIED.generate_with_stack_by_args(&[
                command.into(),
                user.to_owned().into(),
                host.to_owned().into(),
                (tidb_hack::go_to_lower(table_name)).into(),
            ]),
        ))
    }

}

impl tidb_executor::show::exec::ShowExec for Session {
    fn with_catalog_scope<T>(
        &mut self,
        include_local: bool,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        Session::with_catalog_scope(self, include_local, body)
    }

    fn load_global_bindings(&mut self) -> Result<binding::SessionBindings, DriverError> {
        Session::load_global_bindings(self)
    }

    fn session_bindings_mut(&mut self) -> &mut binding::SessionBindings {
        &mut self.session_bindings
    }

    fn activated_txn_start_ts(&mut self) -> i64 {
        self.activate_statement_txn();
        self.current_tso().value()
    }

    fn privilege_context(&self) -> Option<(&tidb_privilege::PrivilegeRegistry, &str, &str)> {
        Session::privilege_context(self)
    }

    fn has_any_scoped_privilege(&self, database: &str, table: &str, mask: u64) -> bool {
        Session::has_any_scoped_privilege(self, database, table, mask)
    }

    fn database_is_visible(&self, database: &str) -> bool {
        Session::database_is_visible(self, database)
    }

    fn schema_visibility(&self) -> tidb_executor::infoschema_reader::SchemaVisibility {
        Session::schema_visibility(self)
    }

    fn require_visible_database(&self, database: &str) -> Result<(), DriverError> {
        Session::require_visible_database(self, database)
    }

    fn require_current_database(&self) -> Result<&str, DriverError> {
        Session::require_current_database(self)
    }

    fn require_show_create_privilege(
        &self,
        database: &str,
        table_name: &str,
    ) -> Result<(), DriverError> {
        Session::require_show_create_privilege(self, database, table_name)
    }

    fn require_show_stats_privilege(&self, table: &str) -> Result<(), DriverError> {
        Session::require_show_stats_privilege(self, table)
    }

    fn table_storage_statistics_catalog(
        &mut self,
        needs_column_lengths: bool,
    ) -> Result<Catalog, DriverError> {
        Session::table_storage_statistics_catalog(self, needs_column_lengths)
    }

    fn column_stats_usage_provider(&self) -> Option<std::sync::Arc<dyn ColumnStatsUsageProvider>> {
        self.column_stats_usage.clone()
    }

    fn analyze_status_provider(&self) -> Option<std::sync::Arc<dyn AnalyzeStatusProvider>> {
        self.analyze_status.clone()
    }

    fn show_status_rows(&self, global: bool) -> Result<Vec<Vec<Datum>>, DriverError> {
        Session::show_status_rows(self, global)
    }

    fn warning_rows(&self, count_only: bool, errors_only: bool) -> Vec<Vec<Datum>> {
        Session::warning_rows(self, count_only, errors_only)
    }

    fn viewer_state(&self) -> tidb_executor::memtable_reader::ViewerState {
        Session::viewer_state(self)
    }

    fn process_registry(&self) -> Option<tidb_executor::process::ProcessRegistry> {
        self.process.as_ref().map(|guard| guard.registry().clone())
    }

    fn show_session_states_rows(&self) -> Result<Vec<Vec<Datum>>, DriverError> {
        Session::show_session_states_rows(self)
    }

    fn cluster_config_table_rows(
        &mut self,
        filters: &[tidb_planner::memtable_predicate_extractor::ClusterTableFilter],
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        Session::cluster_config_table_rows(self, filters)
    }
}

impl tidb_executor::bind_exec::SqlBindExec for Session {
    fn with_global_binding_storage(
        &mut self,
        operation: impl FnMut(&mut Self) -> Result<u64, DriverError>,
    ) -> Result<u64, DriverError> {
        Session::with_global_binding_storage(self, operation)
    }

    fn validate_binding_statement(&mut self, hinted: &tidb_ast::Stmt) -> Result<(), DriverError> {
        Session::validate_binding_statement(self, hinted)
    }

    fn binding_timestamp(&self) -> String {
        Session::binding_timestamp(self)
    }

    fn global_binding_timestamp(&self) -> String {
        Session::global_binding_timestamp(self)
    }

    fn bind_info_exec(&mut self, sql: &str, params: &[Datum]) -> Result<u64, DriverError> {
        Session::bind_info_exec(self, sql, params)
    }

    fn set_value_rows(
        &mut self,
        sql: &str,
        ctx: &tidb_sessionctx::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        Session::set_value_rows(self, sql, ctx)
    }

    fn escalate_vanished_binding_hints(&self, bind_sql: &str) -> Result<(), DriverError> {
        Session::escalate_vanished_binding_hints(self, bind_sql)
    }

    fn binding_charset(&self) -> String {
        Session::binding_charset(self)
    }

    fn binding_collation(&self) -> String {
        Session::binding_collation(self)
    }
}

impl tidb_executor::index_advisor::IndexAdvisorExec for Session {

    fn set_hypo_indexes(&mut self, indexes: std::sync::Arc<tidb_executor::ddl::HypoIndexes>) {
        self.hypo_indexes = indexes;
    }

    fn warnings_snapshot(&self) -> Vec<tidb_util::context::SqlWarn> {
        self.warnings.clone()
    }

    fn restore_warnings(&mut self, warnings: Vec<tidb_util::context::SqlWarn>) {
        self.warnings = warnings;
    }

    fn run_internal_write(&mut self, sql: &str) -> Result<Option<u64>, DriverError> {
        Session::run_internal_write(self, sql)
    }
}

impl tidb_executor::admin::AdminExec for Session {

    fn all_ddl_jobs(
        &mut self,
    ) -> Result<(Vec<tidb_model::Job>, Vec<tidb_model::Job>), DriverError> {
        Session::all_ddl_jobs(self)
    }

    fn with_catalog_retrieving<T>(
        &mut self,
        ctx: &tidb_sessionctx::StmtContext,
        attempt: impl FnMut(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        Session::with_catalog_retrieving(self, ctx, attempt)
    }

    fn explain_stmt(
        &mut self,
        explain: &tidb_ast::ExplainStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        tidb_executor::compiler::ExplainCompile::explain_stmt(self, explain)
    }

    fn execute_statement_owned(
        &mut self,
        stmt: tidb_ast::Stmt,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        Session::execute_statement_owned(self, stmt, sql)
    }

    fn bdr_role_store(&self) -> Result<std::sync::Arc<dyn BdrRoleStore>, DriverError> {
        Session::bdr_role_store(self)
    }

    fn flush_session_plan_cache(
        &mut self,
        scope: tidb_ast::AdminPlanCacheScope,
    ) -> Result<(), DriverError> {
        Session::flush_session_plan_cache(self, scope)
    }

    fn take_snapshot(&self) -> Option<tidb_executor::admin::TakeSnapshot> {
        self.take_snapshot.clone()
    }

    fn flush_privileges(&mut self) -> Result<(), DriverError> {
        Session::flush_privileges(self)
    }

    fn show_ddl_rows(&self) -> Vec<Vec<Datum>> {
        Session::show_ddl_rows(self)
    }

    fn run_planned_show(&mut self, show: &tidb_ast::ShowStmt) -> Result<StmtOutput, DriverError> {
        Session::run_planned_show(self, show)
    }

    fn server_id(&self) -> u64 {
        (self.server_id_getter)()
    }

    fn cluster_peer(
        &self,
    ) -> Option<&std::sync::Arc<tidb_executor::cluster_peer::ClusterPeerClient>> {
        self.cluster_peer.as_ref()
    }

    fn server_info_syncer(
        &self,
    ) -> Option<&std::sync::Arc<tidb_domain::serverinfo_syncer::Syncer>> {
        self.server_info_syncer.as_ref()
    }
}

impl tidb_executor::simple::exec::SimpleExec for Session {
    fn decode_session_states(&mut self, text: &str) -> Result<(), DriverError> {
        Session::decode_session_states(self, text)
    }

    fn reload_tls(&mut self, no_rollback_on_error: bool) -> Result<(), DriverError> {
        let process = self
            .process
            .as_ref()
            .ok_or_else(|| DriverError::unsupported("TLS reload requires a running server"))?;
        process
            .registry()
            .reload_tls(no_rollback_on_error)
            .map_err(DriverError::unsupported)
    }
}

impl Session {
}
