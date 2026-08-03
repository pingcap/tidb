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
//! [`crate::Session::dispatch_admin_stmt`], plus the plan-time match every
//! `SELECT` runs through. The mechanics they build on live in
//! [`crate::binding`]; read that module's doc first.

use tidb_ast::{
    BindingScope, CreateBindingSource, CreateBindingStmt, DropBindingStmt, DropBindingTarget,
    ShowBindingsStmt, Stmt,
};
use tidb_datatype::Datum;
use tidb_executor::DriverError;

use crate::binding::{self, Binding, SOURCE_MANUAL, STATUS_ENABLED};
use crate::{Session, StmtOutput};

/// The refusal every GLOBAL-scope binding statement carries.
///
/// A global binding is a ROW in `mysql.bind_info`, read back by every session
/// through the domain's binding handle. This tier has no such table -- see
/// `tests_binding`, which measures its absence rather than asserting it -- so
/// accepting the statement could only store the binding somewhere no other
/// session reads, which is a wrong answer wearing an OK packet.
const GLOBAL_SCOPE_REFUSAL: &str =
    "a GLOBAL binding is a row in mysql.bind_info, which this tier's catalog has no table for; \
     only SESSION bindings are supported";

impl Session {
    /// Go `SQLBindExec`'s create half, for `CREATE [SESSION] BINDING FOR
    /// <origin> USING <hinted>`.
    pub(crate) fn create_binding_stmt(
        &mut self,
        create: &CreateBindingStmt,
    ) -> Result<StmtOutput, DriverError> {
        if create.scope == BindingScope::Global {
            return Err(DriverError::unsupported(GLOBAL_SCOPE_REFUSAL));
        }
        let target = match &create.source {
            CreateBindingSource::Statement { target } => target,
            // `FROM HISTORY USING PLAN DIGEST` reads a captured plan out of
            // `information_schema.statements_summary`, which this tier does
            // not record. Refused rather than answered from nothing.
            CreateBindingSource::History { .. } => {
                return Err(DriverError::unsupported(
                    "CREATE BINDING FROM HISTORY needs a recorded statement summary, \
                     which this tier does not keep",
                ))
            }
        };
        let Some(hinted) = &target.hinted else {
            // The grammar guarantees `USING` for CREATE; a missing one is a
            // parser invariant break, not a user error.
            return Err(DriverError::unsupported(
                "CREATE BINDING without USING is not a statement this tier models",
            ));
        };
        let origin = target.origin.as_ref();
        let hinted = hinted.as_ref();

        let current_db = self.current_db.clone();
        let db = binding::default_db_of(origin, &current_db);
        let (original_sql, sql_digest) = binding::normalize_with_db(origin, &current_db);
        let (hinted_normalized, _) = binding::normalize_with_db(hinted, &current_db);
        // Go's preprocessor check: erasing the hints must leave two identical
        // statements.
        binding::check_origin_matches_hinted(&original_sql, &hinted_normalized)?;
        // Go `checkBindingValidation` runs `EXPLAIN FORMAT='hint'` over the
        // hinted SQL, so a binding naming a table or index that does not
        // exist fails at CREATE time with that statement's own error (1146 /
        // 1176, captured from real TiDB). Planning the hinted statement here
        // raises the same errors from the same catalog.
        self.validate_binding_statement(hinted)?;

        let bind_sql = binding::restore_with_default_db(hinted, &current_db);
        let now = self.binding_timestamp();
        let binding = Binding {
            original_sql,
            bind_sql,
            db,
            status: STATUS_ENABLED,
            charset: self.binding_charset(),
            collation: self.binding_collation(),
            source: SOURCE_MANUAL,
            sql_digest,
            create_time: now.clone(),
            update_time: now,
            no_db_digest: binding::no_db_digest(hinted),
            table_names: binding::collect_table_names(origin),
            hints: binding::collect_hints(hinted),
        };
        self.session_bindings.create(binding);
        Ok(StmtOutput::Affected(0))
    }

    /// Go `SQLBindExec`'s drop half. Dropping a binding that is not there is
    /// NOT an error (measured on real TiDB: `DROP SESSION BINDING FOR <sql>`
    /// with no such binding answers OK), so the affected count is the only
    /// thing that distinguishes the two outcomes.
    pub(crate) fn drop_binding_stmt(
        &mut self,
        drop: &DropBindingStmt,
    ) -> Result<StmtOutput, DriverError> {
        if drop.scope == BindingScope::Global {
            return Err(DriverError::unsupported(GLOBAL_SCOPE_REFUSAL));
        }
        let digests: Vec<String> = match &drop.target {
            DropBindingTarget::Statement(target) => {
                let current_db = self.current_db.clone();
                let (_, digest) = binding::normalize_with_db(target.origin.as_ref(), &current_db);
                vec![digest]
            }
            DropBindingTarget::SqlDigests(values) => values
                .iter()
                .map(|value| match value {
                    tidb_ast::BindingValue::String(text) => Ok(text.clone()),
                    // A `@var` digest is read at execution time from the
                    // session's user variables; refusing is better than
                    // silently dropping nothing.
                    tidb_ast::BindingValue::UserVar(name) => self
                        .user_vars
                        .borrow()
                        .get(&name.to_ascii_lowercase())
                        .and_then(crate::datum_text)
                        .ok_or_else(|| DriverError::unsupported("sql digest is null")),
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        let dropped = digests
            .iter()
            .filter(|digest| self.session_bindings.drop_digest(digest))
            .count();
        Ok(StmtOutput::Affected(dropped as u64))
    }

    /// Go `fetchShowBind`, session scope.
    pub(crate) fn show_bindings_stmt(
        &mut self,
        show: &ShowBindingsStmt,
    ) -> Result<StmtOutput, DriverError> {
        if show.scope == BindingScope::Global {
            return Err(DriverError::unsupported(GLOBAL_SCOPE_REFUSAL));
        }
        if show.filter.is_some() {
            return Err(DriverError::unsupported(
                "SHOW BINDINGS filters are not supported yet",
            ));
        }
        let text = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let columns = [
            "Original_sql",
            "Bind_sql",
            "Default_db",
            "Status",
            "Create_time",
            "Update_time",
            "Charset",
            "Collation",
            "Source",
            "Sql_digest",
            "Plan_digest",
        ]
        .into_iter()
        .map(|name| (name.to_owned(), text()))
        .collect();
        let rows = self
            .session_bindings
            .all_sorted()
            .into_iter()
            .map(|binding| {
                vec![
                    Datum::Bytes(binding.original_sql.clone().into_bytes()),
                    Datum::Bytes(binding.bind_sql.clone().into_bytes()),
                    Datum::Bytes(binding.db.clone().into_bytes()),
                    Datum::Bytes(binding.status.as_bytes().to_vec()),
                    Datum::Bytes(binding.create_time.clone().into_bytes()),
                    Datum::Bytes(binding.update_time.clone().into_bytes()),
                    Datum::Bytes(binding.charset.clone().into_bytes()),
                    Datum::Bytes(binding.collation.clone().into_bytes()),
                    Datum::Bytes(binding.source.as_bytes().to_vec()),
                    Datum::Bytes(binding.sql_digest.clone().into_bytes()),
                    // Go leaves `Plan_digest` empty for a manually created
                    // binding; only `FROM HISTORY` fills it.
                    Datum::Bytes(Vec::new()),
                ]
            })
            .collect();
        Ok(StmtOutput::Rows { columns, rows })
    }

    /// Go `planner.optimize`'s binding step: replaces the statement's hints
    /// with the matched binding's and records the match for
    /// `@@last_plan_from_binding`.
    ///
    /// Returns the statement to plan. The caller keeps the original when
    /// nothing matched, so a session with no bindings pays one map-emptiness
    /// test and nothing else.
    pub(crate) fn bind_statement_hints(&mut self, stmt: &Stmt) -> Option<Stmt> {
        if self.session_bindings.is_empty() {
            return None;
        }
        // Go gates the whole step on `SessionVars.UsePlanBaselines`.
        if !self.session_bool("tidb_use_plan_baselines", true) {
            return None;
        }
        let no_db_digest = binding::no_db_digest(stmt);
        let table_names = binding::collect_table_names(stmt);
        let hints = self
            .session_bindings
            .match_statement(&no_db_digest, &table_names, &self.current_db)?
            .hints
            .clone();
        let mut bound = stmt.clone();
        binding::bind_hints(&mut bound, &hints);
        self.found_in_binding = true;
        Some(bound)
    }

    /// Whether `@@last_plan_from_binding` should report a hit, which is the
    /// PRECEDING statement's outcome (Go `PrevFoundInBinding`).
    pub(crate) fn last_plan_from_binding(&self) -> bool {
        self.prev_found_in_binding
    }

    /// Go `checkBindingValidation`'s effect: the hinted statement must plan
    /// against the current catalog. Go reaches it through `EXPLAIN
    /// FORMAT='hint'`; this runs the same planner entry `EXPLAIN` runs.
    fn validate_binding_statement(&mut self, hinted: &Stmt) -> Result<(), DriverError> {
        let Stmt::Query(query) = hinted else {
            return Ok(());
        };
        let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
            return Ok(());
        };
        let current_db = self.current_db.clone();
        let ctx = self.statement_context(false);
        let select = select.clone();
        self.with_catalog_mut(|catalog| {
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
    fn binding_timestamp(&self) -> String {
        let zone = self.session_time_zone();
        let (seconds, nanos, offset) = self.statement_clock(&zone);
        let at = chrono::DateTime::from_timestamp(seconds, nanos)
            .unwrap_or_else(chrono::Utc::now)
            .naive_utc()
            + chrono::Duration::seconds(i64::from(offset));
        at.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }

    /// Go `SessionVars.GetCharsetInfo`'s first half.
    fn binding_charset(&self) -> String {
        self.vars
            .get_system("character_set_connection")
            .unwrap_or_else(|_| "utf8mb4".to_owned())
    }

    /// Go `SessionVars.GetCharsetInfo`'s second half.
    fn binding_collation(&self) -> String {
        self.vars
            .get_system("collation_connection")
            .unwrap_or_else(|_| "utf8mb4_bin".to_owned())
    }
}
