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

//! The `CREATE BINDING` / `DROP BINDING` / `SHOW BINDINGS` arms of
//! `tidb_executor::admin::AdminExec::admin_next`, plus the plan-time match every
//! `SELECT` runs through. The mechanics they build on live in
//! [`tidb_bindinfo::binding`]; read that module's doc first.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_ast::Stmt;
use tidb_driver_error::DriverError;

use tidb_bindinfo::binding::{self, Binding};
use crate::Session;

use tidb_bindinfo::binding::BIND_INFO_COLUMNS;

/// Go `GetBindingHandle(sctx)`: the global binding handle a statement
/// matches against.
#[derive(Clone, Default)]
pub enum GlobalBindingSource {
    /// A session outside any domain (the in-process tier) reads
    /// `mysql.bind_info` itself.
    #[default]
    Storage,
    /// A fixed committed cache.
    Cache(tidb_bindinfo::binding_cache::SharedBindingCache),
    /// The domain's handle, read at match time as Go's `GetBindingHandle`
    /// reads `domain.BindingHandle()`: none until the domain initializes it,
    /// and then no global binding matches (Go's nil handle).
    Domain(tidb_domain_core::domain::BindingHandleSlot),
}

impl GlobalBindingSource {
    /// `None` reads storage; `Some(None)` is Go's nil handle.
    fn cache(&self) -> Option<Option<tidb_bindinfo::binding_cache::SharedBindingCache>> {
        match self {
            Self::Storage => None,
            Self::Cache(cache) => Some(Some(cache.clone())),
            Self::Domain(slot) => Some(slot.get().map(|handle| handle.cache())),
        }
    }
}

impl Session {
    /// Install the node's committed global-binding cache.
    pub fn set_global_binding_cache(&mut self, cache: tidb_bindinfo::binding_cache::SharedBindingCache) {
        self.global_binding_source = GlobalBindingSource::Cache(cache);
    }

    /// Match against the domain's binding handle, read at match time.
    pub fn set_global_binding_slot(&mut self, slot: tidb_domain_core::domain::BindingHandleSlot) {
        self.global_binding_source = GlobalBindingSource::Domain(slot);
    }

    /// Install the cluster's independent global-binding transaction owner.
    pub fn set_global_binding_writer(&mut self, writer: std::sync::Arc<dyn GlobalBindingWriter>) {
        self.global_binding_writer = Some(writer);
    }

    /// Go `SQLBindExec.reloadBindings`; a session with no global binding
    /// storage has no cache to refresh.
    pub(crate) fn reload_global_bindings(&mut self) -> Result<(), DriverError> {
        match self.global_binding_writer.clone() {
            Some(writer) => writer.reload(),
            None => Ok(()),
        }
    }

    /// Go `bindingOperator`'s `callWithSCtx(sPool, true, ...)`: the
    /// operation runs on a session of the node's binding pool.
    pub(crate) fn with_global_binding_storage(
        &mut self,
        operation: &mut dyn FnMut(
            &dyn tidb_bindinfo::binding_operator::BindingOperatorSession,
        ) -> Result<u64, DriverError>,
    ) -> Result<u64, DriverError> {
        let writer = self.global_binding_writer.clone().ok_or_else(|| {
            DriverError::unsupported("the session's node has no binding handle")
        })?;
        writer.execute(operation)
    }

    /// Go `planner.optimize`'s binding step: replaces the statement's hints
    /// with the matched binding's and records the match for
    /// `@@last_plan_from_binding`.
    ///
    /// Returns the statement to plan. The caller keeps the original when
    /// nothing matched, so a session with no bindings pays one map-emptiness
    /// test and nothing else.
    pub(crate) fn bind_statement_hints_with_sql(&mut self, stmt: &Stmt) -> Option<(Stmt, String)> {
        tidb_planner::optimize::bind_sql_binding(self, stmt)
    }

    pub(crate) fn bind_statement_hints(&mut self, stmt: &Stmt) -> Option<Stmt> {
        self.bind_statement_hints_with_sql(stmt)
            .map(|(statement, _)| statement)
    }

    /// Applies the binding selected for this EXECUTE and returns its exact
    /// `BindSQL`, which Go includes in the prepared-plan cache key. A miss
    /// borrows the retained tree and reports no key component: Go executes
    /// the `PlanCacheStmt`'s own `PreparedAst` in place, so the common
    /// no-binding EXECUTE must not deep-copy the statement.
    #[must_use]
    pub fn prepared_statement_with_binding<'a>(
        &mut self,
        stmt: &'a Stmt,
    ) -> (std::borrow::Cow<'a, Stmt>, Option<String>) {
        self.bind_statement_hints_with_sql(stmt).map_or_else(
            || (std::borrow::Cow::Borrowed(stmt), None),
            |(stmt, sql)| (std::borrow::Cow::Owned(stmt), Some(sql)),
        )
    }

    /// Whether `@@last_plan_from_binding` should report a hit, which is the
    /// PRECEDING statement's outcome (Go `PrevFoundInBinding`).
    pub(crate) fn last_plan_from_binding(&self) -> bool {
        self.vars.prev_found_in_binding
    }

    /// Go `checkBindingValidation`'s effect: the hinted statement must plan
    /// against the current catalog. Go reaches it through `EXPLAIN
    /// FORMAT='hint'`; this runs the same planner entry `EXPLAIN` runs.
    pub(crate) fn validate_binding_statement(&mut self, hinted: &Stmt) -> Result<(), DriverError> {
        let Stmt::Query(query) = hinted else {
            return Ok(());
        };
        let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
            return Ok(());
        };
        let current_db = self.vars.current_db.clone();
        let ctx = self.statement_context(false);
        let select = select.clone();
        self.with_catalog_retrieving(&ctx, |catalog| {
            tidb_executor::explain_select_stmt(
                &select,
                catalog,
                &current_db,
                &ctx,
                tidb_sessionctx::explain_format::ExplainFormat::Brief,
            )
            .map(|_| ())
        })
    }

    /// Plans `query` the way the planner hooks of `EXPLAIN EXPLORE` do
    /// (Go `Preprocess` + `Optimize` over the session's variables and its
    /// latest catalog).
    pub(crate) fn plan_explore_query<T>(
        &mut self,
        mut plan: impl FnMut(
            &tidb_ast::QueryStmt,
            &tidb_infoschema::SessionExtendedInfoSchema,
            &str,
            &tidb_sessionctx::StmtContext,
        ) -> Result<T, DriverError>,
        query: &tidb_ast::QueryStmt,
    ) -> Result<T, DriverError> {
        let current_db = self.vars.current_db.clone();
        let ctx = self.statement_context(false);
        self.with_catalog_retrieving(&ctx, |catalog| plan(query, catalog, &current_db, &ctx))
    }

    /// Go `GetLatestInfoSchema().TableInfoByName(schema, table)`.
    pub(crate) fn latest_table_info_by_name(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        self.with_catalog_mut(|catalog| Ok(catalog.table_info_by_name(schema, table).map(|(_, info)| info)))
            .ok()
            .flatten()
    }

    /// Go `StmtCtx.GetExecDetails().ScanDetail.ProcessedKeys` of the last
    /// statement.
    pub(crate) fn statement_processed_keys(&self) -> u64 {
        self.statement_keys_read.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Go `BindingHandle().ExplorePlansForSQL(sctx, sqlOrDigest, analyze)`
    /// with the session's current database and connection charset.
    pub(crate) fn explore_plans_for_sql(
        &mut self,
        sql_or_digest: &str,
        analyze: bool,
    ) -> Result<Vec<tidb_bindinfo::binding_auto::BindingPlanInfo>, DriverError> {
        let writer = self.global_binding_writer.clone().ok_or_else(|| {
            DriverError::unsupported("the session's node has no binding handle")
        })?;
        let current_db = self.vars.current_db.clone();
        let (charset, collation) = (self.binding_charset(), self.binding_collation());
        writer.explore_plans_for_sql(&current_db, &charset, &collation, sql_or_digest, analyze)
    }

    /// Go stores `types.NewTime(FromGoTime(now.In(tz)), TypeTimestamp, 3)`,
    /// which prints with three fractional digits.
    pub(crate) fn binding_timestamp(&self) -> String {
        let zone = self.session_time_zone();
        let (seconds, nanos, offset) = self.statement_clock(&zone);
        let at = chrono::DateTime::from_timestamp(seconds, nanos)
            .unwrap_or_else(chrono::Utc::now)
            .naive_utc()
            + chrono::Duration::seconds(i64::from(offset));
        at.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }

    /// Go `SessionVars.GetCharsetInfo`'s first half.
    pub(crate) fn binding_charset(&self) -> String {
        self.vars
            .get_system("character_set_connection")
            .unwrap_or_else(|_| "utf8mb4".to_owned())
    }

    /// Go `prepareHints`: `ParseHintsSet` re-parses the stored `BindSQL`, and
    /// a binding whose hints all vanish there while the hint parser warned
    /// fails with that warning (`hintsStr == "" && len(warns) > 0`) -- so a
    /// binding Go restores as `WRITE_SLOW_LOG(@`q` )`, which re-parses as no
    /// hint, is refused with the 1064 at that text's EOF.
    pub(crate) fn escalate_vanished_binding_hints(
        &self,
        bind_sql: &str,
    ) -> Result<(), DriverError> {
        let reparsed = tidb_parser::parse_with_connection_and_warnings(
            bind_sql,
            &self.binding_charset(),
            &self.binding_collation(),
        )
        .map_err(|error| {
            DriverError::from(tidb_util::misc::syntax_error(
                error.compatibility_message(bind_sql),
            ))
        })?;
        let Some(warning) = reparsed.warnings.into_iter().next() else {
            return Ok(());
        };
        if !binding::collect_hints(&reparsed.statement)
            .restore()
            .is_empty()
        {
            return Ok(());
        }
        let message = warning.body().to_owned();
        Err(DriverError::from(
            (warning
                .terror
                .unwrap_or(&tidb_parser::parser_errors::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT))
            .gen_with_stack(message),
        ))
    }

    /// Go `SessionVars.GetCharsetInfo`'s second half.
    pub(crate) fn binding_collation(&self) -> String {
        self.vars
            .get_system("collation_connection")
            .unwrap_or_else(|_| "utf8mb4_bin".to_owned())
    }

    /// Materialize the published bindings for SHOW. Standalone in-memory
    /// sessions without a node cache decode their local catalog instead.
    pub(crate) fn load_global_bindings(&mut self) -> Result<binding::SessionBindings, DriverError> {
        if let Some(Some(shared)) = self.global_binding_source.cache() {
            let mut loaded = binding::SessionBindings::default();
            for binding in shared.load().get_all_bindings() {
                loaded.create(binding.as_ref().clone());
            }
            return Ok(loaded);
        }
        let ctx = self.statement_context(false);
        let (_, rows) = self.with_catalog_mut(|catalog| {
            tidb_executor::run_select_meta_in(
                &format!("SELECT {BIND_INFO_COLUMNS}, plan_digest FROM mysql.bind_info"),
                catalog,
                "mysql",
                &ctx,
            )
        })?;
        let mut loaded = binding::SessionBindings::default();
        for row in rows {
            if let Some(binding) = Binding::from_storage_row(&row, self.scanner_sql_mode()) {
                if binding.status != binding::STATUS_DELETED {
                    loaded.create(binding);
                }
            }
        }
        Ok(loaded)
    }
}

/// Executes one global-binding storage operation in an independent transaction.
/// Validation and user variables remain on the caller; the supplied session owns
/// only the internal storage transaction, as Go's binding session pool does.
pub trait GlobalBindingWriter: Send + Sync {
    /// Commit the complete operation or discard all its writes on error.
    fn execute(
        &self,
        operation: &mut dyn FnMut(
            &dyn tidb_bindinfo::binding_operator::BindingOperatorSession,
        ) -> Result<u64, tidb_driver_error::DriverError>,
    ) -> Result<u64, tidb_driver_error::DriverError>;

    /// Go `BindingHandle().LoadFromStorageToCache(true, ...)`: the node's
    /// binding cache refreshed in full from `mysql.bind_info`.
    fn reload(&self) -> Result<(), tidb_driver_error::DriverError>;

    /// Go `BindingHandle().ExplorePlansForSQL`: `EXPLAIN EXPLORE`'s plan
    /// candidates, on the handle's own sessions.
    fn explore_plans_for_sql(
        &self,
        current_db: &str,
        charset: &str,
        collation: &str,
        sql_or_digest: &str,
        analyze: bool,
    ) -> Result<Vec<tidb_bindinfo::binding_auto::BindingPlanInfo>, tidb_driver_error::DriverError>;
}

impl Session {
    /// The statement clock in the session's zone, at six fractional digits
    /// (`bind_info.update_time` is a `TIMESTAMP(6)` column): the binding
    /// operator's Go `time.Now()`.
    pub(crate) fn binding_now_timestamp(&self, offset_micros: i64) -> String {
        let zone = self.session_time_zone();
        let (clock_seconds, nanos, offset) = self.statement_clock(&zone);
        let at = chrono::DateTime::from_timestamp(clock_seconds, nanos)
            .unwrap_or_else(chrono::Utc::now)
            .naive_utc()
            + chrono::Duration::seconds(i64::from(offset))
            + chrono::Duration::microseconds(offset_micros);
        at.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }
}

impl binding::BindingMatchContext for Session {
    fn session_bindings(&self) -> &binding::SessionBindings {
        &self.session_bindings
    }

    fn global_binding_cache(&self) -> Option<Option<tidb_bindinfo::binding_cache::SharedBindingCache>> {
        self.global_binding_source.cache()
    }

    fn load_global_bindings(&mut self) -> Result<binding::SessionBindings, DriverError> {
        Session::load_global_bindings(self)
    }

    fn current_db(&self) -> &str {
        &self.vars.current_db
    }

    fn fuzzy_binding_enabled(&self) -> bool {
        self.session_bool("tidb_opt_enable_fuzzy_binding", false)
    }

    fn binding_usage_enabled(&self) -> bool {
        self.vars
            .global_sysvars()
            .get("tidb_enable_binding_usage")
            .ok()
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"))
    }
}

impl tidb_planner::optimize::PlannerBindingContext for Session {
    fn use_plan_baselines(&self) -> bool {
        self.session_bool("tidb_use_plan_baselines", true)
    }

    fn set_binding_match_duration(&mut self, elapsed: std::time::Duration) {
        self.phase_durations.set_binding_match(elapsed);
    }

    fn append_binding_warning(&mut self, message: String) {
        self.append_warning(crate::WarningLevel::Warning, 1105, message);
    }

    fn set_found_in_binding(&mut self) {
        self.vars.found_in_binding = true;
    }
}
