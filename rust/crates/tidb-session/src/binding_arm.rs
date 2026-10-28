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
//! [`crate::binding`]; read that module's doc first.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_ast::Stmt;
use tidb_datatype::Datum;
use tidb_executor::DriverError;

use crate::binding::{self, Binding};
use crate::Session;

/// Go `bindinfo.mayHaveSQLBinding`: INSERT/REPLACE value forms have no
/// binding-capable SELECT source. Keep the filter at the matcher boundary so
/// stored bindings remain visible to administrative operations while ordinary
/// DML and EXPLAIN do not report a false match.
fn may_have_sql_binding(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Query(_) => true,
        Stmt::Dml(dml) => may_have_dml_binding(dml),
        _ => true,
    }
}

fn may_have_dml_binding(dml: &tidb_ast::DmlStmt) -> bool {
    match dml {
        tidb_ast::DmlStmt::Insert(insert) => insert.source.is_some(),
        _ => true,
    }
}

use tidb_bindinfo::binding::BIND_INFO_COLUMNS;

impl Session {
    /// Install the node's committed global-binding cache.
    pub fn set_global_binding_cache(&mut self, cache: crate::binding_cache::SharedBindingCache) {
        self.global_binding_cache = Some(cache);
    }

    /// Install the cluster's independent global-binding transaction owner.
    pub fn set_global_binding_writer(&mut self, writer: std::sync::Arc<dyn GlobalBindingWriter>) {
        self.global_binding_writer = Some(writer);
    }

    pub(crate) fn with_global_binding_storage(
        &mut self,
        mut operation: impl FnMut(&mut Self) -> Result<u64, DriverError>,
    ) -> Result<u64, DriverError> {
        match self.global_binding_writer.clone() {
            Some(writer) => writer.execute(&mut operation),
            None => operation(self),
        }
    }

    /// Go `planner.optimize`'s binding step: replaces the statement's hints
    /// with the matched binding's and records the match for
    /// `@@last_plan_from_binding`.
    ///
    /// Returns the statement to plan. The caller keeps the original when
    /// nothing matched, so a session with no bindings pays one map-emptiness
    /// test and nothing else.
    pub(crate) fn bind_statement_hints_with_sql(&mut self, stmt: &Stmt) -> Option<(Stmt, String)> {
        if !may_have_sql_binding(stmt) {
            return None;
        }
        // Go gates the whole step on `SessionVars.UsePlanBaselines`.
        if !self.session_bool("tidb_use_plan_baselines", true) {
            return None;
        }
        // Go `matchSQLBindingCore`'s deferred `DurationOptimizer.BindingMatch`.
        let begin_binding_match = std::time::Instant::now();
        let matched = self.match_statement_binding(stmt);
        self.phase_durations
            .set_binding_match(begin_binding_match.elapsed());
        let (hints, bind_sql) = matched?;
        let mut bound = stmt.clone();
        binding::bind_hints(&mut bound, &hints);
        self.bind_matched_statement(stmt, bound, bind_sql)
    }

    /// Go `matchSQLBindingCore`: the session binding, else the global one,
    /// matching the statement's no-DB digest and tables -- the matched
    /// binding's hints and bind SQL.
    fn match_statement_binding(&mut self, stmt: &Stmt) -> Option<(binding::HintsSet, String)> {
        let global = self.global_binding_cache.as_ref().map(|cache| cache.load());
        let has_global = match &global {
            Some(cache) => cache.size() != 0,
            None => self.has_global_binding_rows(),
        };
        if self.session_bindings.is_empty() && !has_global {
            return None;
        }
        let no_db_digest = binding::no_db_digest(stmt);
        let table_names = binding::collect_table_names(stmt);
        // Go `crossDBMatchBindings` reads `EnableFuzzyBinding` at MATCH
        // time, so flipping the switch changes what an existing wildcard
        // binding does without touching the row.
        let fuzzy_enabled = self.session_bool("tidb_opt_enable_fuzzy_binding", false);
        // Session bindings shadow global ones, which is Go's order in
        // `planner.optimize`: `getBindingFromSession` first, the domain
        // handle only on a miss.
        let (hints, bind_sql) = match self.session_bindings.match_statement(
            &no_db_digest,
            &table_names,
            &self.vars.current_db,
            fuzzy_enabled,
        ) {
            Some(matched) => (matched.hints.clone(), matched.bind_sql.clone()),
            None if global.is_some() => {
                let matched = global.as_ref()?.matching_binding(
                    &no_db_digest,
                    &table_names,
                    &self.vars.current_db,
                    fuzzy_enabled,
                )?;
                if self
                    .vars
                    .global_sysvars()
                    .get("tidb_enable_binding_usage")
                    .ok()
                    .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"))
                {
                    matched.mark_used();
                }
                (matched.hints.clone(), matched.bind_sql.clone())
            }
            None => {
                let global = self.load_global_bindings().ok()?;
                let matched = global.match_statement(
                    &no_db_digest,
                    &table_names,
                    &self.vars.current_db,
                    fuzzy_enabled,
                )?;
                (matched.hints.clone(), matched.bind_sql.clone())
            }
        };
        Some((hints, bind_sql))
    }

    /// The rest of Go `planner.optimize`'s binding step once a binding
    /// matched: the session records the match and the warnings of the
    /// statement `bound` with the binding's hints.
    fn bind_matched_statement(
        &mut self,
        stmt: &Stmt,
        mut bound: Stmt,
        bind_sql: String,
    ) -> Option<(Stmt, String)> {
        for warning in crate::dispatch::filter_sem_restricted_hints(&mut bound) {
            self.append_warning(crate::WarningLevel::Warning, 1105, warning);
        }
        self.vars.found_in_binding = true;
        // Go `optimize`: `originStmtHints.QueryHasHints` -- the statement
        // wrote hints of its own, which the binding's replace.
        if !tidb_hint::extract_table_hints_from_stmt_node(stmt)
            .0
            .is_empty()
        {
            self.append_warning(
                crate::WarningLevel::Warning,
                1105,
                format!(
                    "The system ignores the hints in the current query and uses the hints \
                     specified in the bindSQL: {bind_sql}"
                ),
            );
        }
        Some((bound, bind_sql))
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
                tidb_executor::ExplainFormat::Brief,
            )
            .map(|_| ())
        })
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
                .unwrap_or(&tidb_error::catalog::parser::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT))
            .generate_with_stack(message),
        ))
    }

    /// Go `SessionVars.GetCharsetInfo`'s second half.
    pub(crate) fn binding_collation(&self) -> String {
        self.vars
            .get_system("collation_connection")
            .unwrap_or_else(|_| "utf8mb4_bin".to_owned())
    }

    /// Whether `mysql.bind_info` can hold a user binding at all: any row
    /// beyond the builtin lock row (`crate::bootstrap`). Go answers the same
    /// question from its binding cache's size; this tier's cache IS the
    /// table, and a ROW COUNT reads no row bytes, which keeps the per-
    /// statement fast path free of `bind_info` decodes -- measured by the
    /// column-prune probes, which would otherwise see this table's columns
    /// under every statement.
    ///
    /// Rows marked `deleted` keep the gate open; the load skips them. That
    /// is the correct cost shape: a catalog where global bindings were NEVER
    /// used pays one integer compare, one that used them pays the read.
    ///
    /// The count must not touch the storage seam: iterating keys shows up in
    /// the storage-op counters the plan-cache tests measure statements by.
    /// `KvTable::len` is a stored size, and every row writes exactly one
    /// record key plus one key per index (NULLs are indexed), so more keys
    /// than one row's worth means a second row exists.
    fn has_global_binding_rows(&mut self) -> bool {
        self.with_catalog_mut(|catalog| {
            Ok(match catalog.table_in("mysql", "bind_info") {
                Some(tidb_executor::TableEntry::Kv(table)) => {
                    table.len() > 1 + table.indexes().len()
                }
                _ => false,
            })
        })
        .unwrap_or(false)
    }

    /// [`Self::binding_timestamp`] at SIX fractional digits: the global
    /// operator stores `types.NewTime(..., 6)` where the session handle uses
    /// 3, and `bind_info.update_time` is a `TIMESTAMP(6)` column.
    pub(crate) fn global_binding_timestamp(&self) -> String {
        self.global_binding_timestamp_offset(0)
    }

    /// [`Self::global_binding_timestamp`] shifted by `seconds` -- the GC
    /// cutoff is "now minus ten leases" in the same TIMESTAMP(6) spelling
    /// the rows store, so the comparison happens in one text domain.
    fn global_binding_timestamp_offset(&self, seconds: i64) -> String {
        let zone = self.session_time_zone();
        let (clock_seconds, nanos, offset) = self.statement_clock(&zone);
        let at = chrono::DateTime::from_timestamp(clock_seconds, nanos)
            .unwrap_or_else(chrono::Utc::now)
            .naive_utc()
            + chrono::Duration::seconds(i64::from(offset) + seconds);
        at.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }

    /// Go `bindingOperator.GCBinding`: `deleted` tombstones whose
    /// `update_time` is older than TEN LEASES (`bindinfo.Lease` = 3s) are
    /// physically removed after peers can observe them. The Domain binding
    /// owner invokes this through the shared internal writer and lock row.
    pub fn gc_global_bindings(&mut self) -> Result<(), DriverError> {
        // A session on a node asks that node's binding owner, which writes
        // the store's table; this session's own catalog is not it.
        if let Some(node) = &self.node {
            return node.gc_global_bindings();
        }
        let cutoff = self.global_binding_timestamp_offset(-30);
        self.bind_info_exec(
            "DELETE FROM mysql.bind_info WHERE status = ? AND update_time < ?",
            &[
                Datum::new_string("deleted".to_owned()),
                Datum::new_string(cutoff),
            ],
        )?;
        Ok(())
    }

    /// One parameterized write against `mysql.bind_info`, the same shape as
    /// Go's `exec(sctx, sql, args...)` in `binding_operator.go`. The
    /// parameters go through [`tidb_executor::bind_parameters`] -- the
    /// prepared-statement binder -- so a bound SQL text (which contains
    /// quotes freely) can never break out of its literal.
    pub(crate) fn bind_info_exec(
        &mut self,
        sql: &str,
        params: &[Datum],
    ) -> Result<u64, DriverError> {
        let text = tidb_executor::bind_parameters(sql, params, self.scanner_sql_mode())?;
        let ctx = self.statement_context_for_update_read(false);
        self.with_staged_catalog(|catalog| match text.trim_start().get(..6) {
            Some(word) if word.eq_ignore_ascii_case("INSERT") => {
                tidb_executor::run_insert_in(&text, catalog, "mysql", &ctx)
            }
            Some(word) if word.eq_ignore_ascii_case("DELETE") => {
                tidb_executor::run_delete_in(&text, catalog, "mysql", &ctx)
            }
            _ => tidb_executor::run_update_in(&text, catalog, "mysql", &ctx),
        })
    }

    /// Materialize the published bindings for SHOW. Standalone in-memory
    /// sessions without a node cache decode their local catalog instead.
    pub(crate) fn load_global_bindings(&mut self) -> Result<binding::SessionBindings, DriverError> {
        if let Some(shared) = &self.global_binding_cache {
            let mut loaded = binding::SessionBindings::default();
            for binding in shared.load().get_all_bindings() {
                loaded.create(binding.as_ref().clone());
            }
            return Ok(loaded);
        }
        if !self.has_global_binding_rows() {
            return Ok(binding::SessionBindings::default());
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
        operation: &mut dyn FnMut(&mut crate::Session) -> Result<u64, tidb_executor::DriverError>,
    ) -> Result<u64, tidb_executor::DriverError>;
}
