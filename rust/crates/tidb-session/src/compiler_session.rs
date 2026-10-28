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

//! The session side of `tidb_executor::compiler`: the primitives Go's
//! compiler reads through `sctx`.

use crate::*;

impl tidb_executor::compiler::CompilerSession for Session {
    fn non_prepared_plan_cache_mut(&mut self) -> &mut tidb_executor::compiler::non_prepared_plan_cache::NonPreparedPlanCache {
        &mut self.non_prepared_plan_cache
    }

    fn physical_plan_cache(&self) -> &tidb_executor::SessionPlanCache {
        &self.physical_plan_cache
    }

    fn parse_statement_hints_without_catalog(
        &self,
        stmt: &tidb_ast::Stmt,
        current_database: &str,
    ) -> tidb_hint::StmtHints {
        crate::variables::parse_statement_hints_without_catalog(stmt, current_database)
    }



    fn configure_session_plan_cache(&self) {
        Session::configure_session_plan_cache(self)
    }

    fn session_plan_cache_capacity(&self) -> usize {
        Session::session_plan_cache_capacity(self)
    }

    fn staged_dirty_table_ids(&self) -> Vec<i64> {
        self.staged_writes.dirty_table_ids()
    }

    fn prepared_plan_cache_environment_cache(
        &self,
    ) -> &std::cell::RefCell<Option<tidb_executor::compiler::prepared_ast::PreparedPlanCacheEnvironmentCache>> {
        &self.prepared_plan_cache_environment_cache
    }


    fn refresh_node_catalog(&mut self) -> Result<(), DriverError> {
        Session::refresh_node_catalog(self)
    }

    fn historical_read_ts(&self) -> Option<u64> {
        Session::historical_read_ts(self)
    }

    fn external_timestamp_read_enabled(&self) -> bool {
        Session::external_timestamp_read_enabled(self)
    }

    fn collect_table_privileges(&self, stmt: &tidb_ast::Stmt, preparing: bool) -> Result<Vec<crate::table_privilege::TablePrivilegeRequest>, DriverError> {
        Session::collect_table_privileges(self, stmt, preparing)
    }

    fn check_table_privilege_requests(&self, requests: &[crate::table_privilege::TablePrivilegeRequest]) -> Result<(), DriverError> {
        Session::check_table_privilege_requests(self, requests)
    }
    fn prepared_statements_mut(&mut self) -> &mut tidb_executor::compiler::prepared::PreparedStore {
        &mut self.prepared_statements
    }

    fn pending_plan_cache_refusal_mut(&mut self) -> &mut Option<&'static str> {
        &mut self.pending_plan_cache_refusal
    }


    fn rewrite_fts_for_planning(&self, stmt: &mut Stmt) -> Result<(), DriverError> {
        Session::rewrite_fts_for_planning(self, stmt)
    }

    fn retrieving_memory_tables<T>(&mut self, ctx: &tidb_executor::StmtContext, mut run: impl FnMut(&mut Self) -> Result<T, DriverError>, ) -> Result<T, DriverError> {
        Session::retrieving_memory_tables(self, ctx, run)
    }

    fn release_prepared_plans(&self, select: Option<&tidb_executor::PreparedSelectPlan>, dml: Option<&tidb_executor::PreparedDmlPlan>) {
        Session::release_prepared_plans(self, select, dml)
    }

    fn bind_variables(&mut self, stmt: &mut Stmt) -> Result<(), DriverError> {
        Session::bind_variables(self, stmt)
    }

    fn allocate_prepared_statement_id(&mut self) -> u32 {
        Session::allocate_prepared_statement_id(self)
    }
    fn prepared_statements_ref(&self) -> &tidb_executor::compiler::prepared::PreparedStore {
        &self.prepared_statements
    }


    fn drain_eval_warnings(&mut self, ctx: &tidb_executor::StmtContext) {
        Session::drain_eval_warnings(self, ctx)
    }

    fn with_staged_catalog<T>(&mut self, body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>, ) -> Result<T, DriverError> {
        Session::with_staged_catalog(self, body)
    }

    fn require_explain_statement_privileges(
        &self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<(), DriverError> {
        Session::require_explain_statement_privileges(self, stmt)
    }

    fn bind_statement_hints(&mut self, stmt: &Stmt) -> Option<Stmt> {
        Session::bind_statement_hints(self, stmt)
    }

    fn apply_set_var_hints(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        Session::apply_set_var_hints(self, stmt)
    }

    fn activate_statement_resource_group(&mut self, stmt: &Stmt) {
        Session::activate_statement_resource_group(self, stmt)
    }
    fn set_statement_result(&mut self, message: String, affected_rows: u64) {
        self.statement_message = message;
        self.statement_affected_rows = affected_rows;
    }

}
