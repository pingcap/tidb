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


//! Go `SQLBindExec` (`executor/bind.go`): CREATE, DROP and SET BINDING,
//! from a statement, from history or from a plan digest.

use tidb_ast::{
    BindingScope, CreateBindingSource, CreateBindingStmt, DropBindingStmt, DropBindingTarget,
    Stmt,
};
use tidb_bindinfo::binding::{self, Binding, BIND_INFO_COLUMNS, SOURCE_MANUAL, STATUS_ENABLED};
use tidb_datatype::Datum;
use tidb_variable::user_vars::UserVarsReader;
use tidb_hack::go_to_lower;

use crate::{DriverError, StmtOutput};

/// Go's plain `errors.New`, which a client sees as 1105.
pub(crate) fn plain_error(message: &str) -> DriverError {
    DriverError::Mysql(crate::MysqlError::unknown(message))
}

/// Go `ParseOneStmt` over a recorded statement, failing as
/// `binding failed: <parse error>. Plan Digest: <digest>`.
pub(crate) fn parse_for_history(sql: &str, plan_digest: &str) -> Result<Stmt, DriverError> {
    tidb_parser::parse(sql).map_err(|error| {
        plain_error(&format!(
            "binding failed: {}. Plan Digest: {plan_digest}",
            error.compatibility_message(sql)
        ))
    })
}

/// Go's `\(len:\d+\)$` over a summary sample the length limit cut.
pub(crate) fn truncated_sample_text(query: &str) -> bool {
    let Some(open) = query.rfind("(len:") else {
        return false;
    };
    let digits = &query[open + "(len:".len()..query.len() - 1];
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// Go `SQLBindExec`.
pub trait SqlBindExec: crate::show::exec::ShowExec {
    /// Go `GlobalBindingHandle`'s storage write: one operation in an
    /// independent internal transaction.
    fn with_global_binding_storage(
        &mut self,
        operation: impl FnMut(&mut Self) -> Result<u64, DriverError>,
    ) -> Result<u64, DriverError>;
    /// Go `bindinfo.CheckBindingStmt` over the hinted statement.
    fn validate_binding_statement(&mut self, hinted: &Stmt) -> Result<(), DriverError>;
    /// The statement clock as a binding's `create_time` / `update_time`.
    fn binding_timestamp(&self) -> String;
    /// The binding storage's own clock (Go's `now(6)` on the internal session).
    fn global_binding_timestamp(&self) -> String;
    /// A write to `mysql.bind_info` with its parameters bound.
    fn bind_info_exec(&mut self, sql: &str, params: &[Datum]) -> Result<u64, DriverError>;
    /// The rows of an internal read of `sql` under `ctx`.
    fn set_value_rows(
        &mut self,
        sql: &str,
        ctx: &crate::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError>;
    /// Go's warning-to-error escalation for hints the binding parse dropped.
    fn escalate_vanished_binding_hints(&self, bind_sql: &str) -> Result<(), DriverError>;
    /// Go `character_set_connection` as the binding records it.
    fn binding_charset(&self) -> String;
    /// Go `collation_connection` as the binding records it.
    fn binding_collation(&self) -> String;

    /// Go `SQLBindExec.setBindingStatus[ByDigest]` over
    /// `bindingOperator.SetBindingStatus`: one UPDATE flipping the row
    /// between `enabled` and `disabled`, guarded by the CURRENT status --
    /// disabling accepts `using` (the legacy enabled spelling) and
    /// `enabled`, enabling accepts only `disabled`. Zero rows touched is
    /// not an error but the measured warning.
    fn set_binding_stmt(
        &mut self,
        set: &tidb_ast::SetBindingStmt,
    ) -> Result<StmtOutput, DriverError> {
        let digest = match &set.target {
            tidb_ast::SetBindingTarget::Statement(target) => {
                let current_db = self.session_vars().current_db.clone();
                let (_, digest) = binding::normalize_with_db(target.origin.as_ref(), &current_db);
                digest
            }
            tidb_ast::SetBindingTarget::SqlDigest(digest) => digest.clone(),
        };
        let (new_status, old_status0, old_status1) = match set.status {
            tidb_ast::BindingStatus::Enabled => (
                STATUS_ENABLED,
                binding::STATUS_DISABLED,
                binding::STATUS_DISABLED,
            ),
            tidb_ast::BindingStatus::Disabled => (
                binding::STATUS_DISABLED,
                binding::STATUS_USING,
                STATUS_ENABLED,
            ),
        };
        let changed = self.with_global_binding_storage(|storage| {
            let now = storage.global_binding_timestamp();
            storage.bind_info_exec(
                "UPDATE mysql.bind_info SET status = ?, update_time = ? \
             WHERE sql_digest = ? AND update_time < ? AND status IN (?, ?)",
                &[
                    Datum::new_string(new_status.to_owned()),
                    Datum::new_string(now.clone()),
                    Datum::new_string(digest.clone()),
                    Datum::new_string(now),
                    Datum::new_string(old_status0.to_owned()),
                    Datum::new_string(old_status1.to_owned()),
                ],
            )
        })?;
        if changed == 0 {
            // Go wraps `errors.NewNoStackError(...)` as an ordinary 1105.
            self.append_sql_warning(tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Warning, 1105,
                "There are no bindings can be set the status. Please check the SQL text".to_owned(),
            ));
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Go `SQLBindExec`'s create half, for `CREATE [SESSION] BINDING FOR
    /// <origin> USING <hinted>`.
    fn create_binding_stmt(
        &mut self,
        create: &CreateBindingStmt,
    ) -> Result<StmtOutput, DriverError> {
        let target = match &create.source {
            CreateBindingSource::Statement { target } => target,
            CreateBindingSource::History { plan_digests } => {
                return self.create_binding_from_history(create.scope, plan_digests);
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

        let current_db = self.session_vars().current_db.clone();
        // A cross-DB binding (`*.t`) belongs to NO schema: Go stores an
        // empty `Default_db` (measured), and the `*` survives normalization.
        let wildcard = binding::collect_table_names(origin)
            .iter()
            .any(|(schema, _)| schema == "*");
        let db = if wildcard {
            String::new()
        } else {
            binding::default_db_of(origin, &current_db)
        };
        self.refuse_binding_on_temporary_table(origin)?;
        let (original_sql, sql_digest) = binding::normalize_with_db(origin, &current_db);
        let (hinted_normalized, _) = binding::normalize_with_db(hinted, &current_db);
        // Go's preprocessor check: erasing the hints must leave two identical
        // statements.
        binding::check_origin_matches_hinted(&original_sql, &hinted_normalized)?;
        // Go `checkBindingValidation` runs `EXPLAIN FORMAT='hint'` over the
        // hinted SQL, so a binding naming a table or index that does not
        // exist fails at CREATE time with that statement's own error (1146 /
        // 1176, captured from real TiDB). Planning the hinted statement here
        // raises the same errors from the same catalog. A `*` schema names
        // no plannable table, and Go accepts the statement without the check
        // (measured, switch on or off), so the wildcard form skips it.
        if !wildcard {
            self.validate_binding_statement(hinted)?;
        }

        let bind_sql = binding::restore_with_default_db(hinted, &current_db);
        self.escalate_vanished_binding_hints(&bind_sql)?;
        // Go's `Preprocess` ran `handleTableName` over the nested statements
        // before this plan was built, so the names a binding is matched by
        // (`Binding.TableNames`) carry the database current at CREATE -- the
        // same names a statement PREPAREd under that database carries.
        let mut pinned_origin = origin.clone();
        binding::pin_current_database(&mut pinned_origin, &current_db)?;
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
            plan_digest: String::new(),
            usage: Default::default(),
            create_time: now.clone(),
            update_time: now,
            no_db_digest: binding::no_db_digest(hinted),
            table_names: binding::collect_table_names(&pinned_origin),
            hints: binding::collect_hints(hinted),
        };
        if create.scope == BindingScope::Global {
            self.with_global_binding_storage(|storage| {
                storage.create_global_binding(&binding)?;
                Ok(0)
            })?;
        } else {
            self.session_bindings_mut().create(binding);
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Go `buildCreateBindPlanFromPlanDigest` plus `SQLBindExec`'s create
    /// half: each plan digest names a statement the summary recorded, whose
    /// captured plan hint becomes the binding's hint.
    fn create_binding_from_history(
        &mut self,
        scope: BindingScope,
        plan_digests: &[tidb_ast::BindingValue],
    ) -> Result<StmtOutput, DriverError> {
        let digests = self.collect_plan_digests(plan_digests)?;
        if digests.is_empty() {
            return Err(plain_error("plan digest is empty"));
        }
        let mut handled_sql_digests = std::collections::HashSet::new();
        let mut bindings = Vec::with_capacity(digests.len());
        for plan_digest in &digests {
            if let Some(binding) =
                self.binding_from_plan_digest(plan_digest, &mut handled_sql_digests)?
            {
                bindings.push(binding);
            }
        }
        // Go `bindingOperator.CreateBinding`'s `prepareHints`: every binding
        // is validated before the first one is stored.
        for binding in &bindings {
            let hinted = tidb_parser::parse(&binding.bind_sql).map_err(|error| {
                DriverError::from(tidb_util::misc::syntax_error(
                    error.compatibility_message(&binding.bind_sql),
                ))
            })?;
            let cross_db = binding::collect_table_names(&hinted)
                .iter()
                .any(|(schema, _)| schema == "*");
            if !cross_db {
                self.validate_binding_statement(&hinted)?;
            }
        }
        if scope == BindingScope::Global {
            self.with_global_binding_storage(|storage| {
                for binding in &bindings {
                    storage.create_global_binding(binding)?;
                }
                Ok(0)
            })?;
        } else {
            for binding in bindings {
                self.session_bindings_mut().create(binding);
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Go `collectStrOrUserVarList`: each value is a literal or a user
    /// variable, split on commas with blanks dropped.
    fn collect_plan_digests(
        &self,
        values: &[tidb_ast::BindingValue],
    ) -> Result<Vec<String>, DriverError> {
        let mut digests = Vec::with_capacity(values.len());
        for value in values {
            let text = match value {
                tidb_ast::BindingValue::String(text) => text.clone(),
                tidb_ast::BindingValue::UserVar(name) => {
                    let found = self.user_vars().get_user_var_val(&go_to_lower(name));
                    let Some(found) = found else {
                        return Err(plain_error(&format!(
                            "can't find specified user variable: {name}"
                        )));
                    };
                    tidb_datatype::datum_text::sysvar_datum_text(&found).unwrap_or_default()
                }
            };
            digests.extend(
                text.split(',')
                    .map(str::trim)
                    .filter(|digest| !digest.is_empty())
                    .map(ToOwned::to_owned),
            );
        }
        Ok(digests)
    }

    /// Go `fetchRecordFromClusterStmtSummary`: the first recorded statement of
    /// a user (an empty sample user marks an internal query) of a bindable
    /// kind, as `(schema, query, plan hint, charset, collation)`.
    fn fetch_record_from_statement_summary(
        &mut self,
        plan_digest: &str,
    ) -> Result<Option<(String, String, String, String, String)>, DriverError> {
        let fields = "stmt_type, schema_name, digest_text, sample_user, prepared, \
                      query_sample_text, charset, collation, plan_hint, plan_digest";
        let literal = plan_digest.replace('\\', "\\\\").replace('\'', "\\'");
        let sql = format!(
            "select {fields} from information_schema.cluster_tidb_statements_stats \
             where plan_digest = '{literal}' union distinct \
             select {fields} from information_schema.cluster_statements_summary_history \
             where plan_digest = '{literal}' order by length(plan_digest) desc"
        );
        let ctx = self.statement_context(false);
        let rows = self.set_value_rows(&sql, &ctx)?;
        for row in rows {
            let text = |index: usize| {
                row.get(index)
                    .and_then(tidb_datatype::datum_text::sysvar_datum_text)
                    .unwrap_or_default()
            };
            let stmt_type = text(0);
            if !text(3).is_empty()
                && matches!(
                    stmt_type.as_str(),
                    "Select" | "Delete" | "Update" | "Insert" | "Replace"
                )
            {
                let mut query = text(5);
                // A prepared statement's sample carries its arguments.
                let prepared = row.get(4).is_some_and(|value| {
                    tidb_datatype::datum_text::sysvar_datum_text(value).is_some_and(|flag| flag == "1")
                });
                if prepared {
                    if let Some(index) = query.rfind("[arguments:") {
                        query.truncate(index);
                    }
                }
                return Ok(Some((text(1), query, text(8), text(6), text(7))));
            }
        }
        Ok(None)
    }

    /// Go `constructSQLBindOPFromPlanDigest` and the binding `SQLBindExec`
    /// builds from it; `None` when another plan digest already covered this
    /// statement.
    fn binding_from_plan_digest(
        &mut self,
        plan_digest: &str,
        handled_sql_digests: &mut std::collections::HashSet<String>,
    ) -> Result<Option<Binding>, DriverError> {
        let record = self.fetch_record_from_statement_summary(plan_digest)?;
        let (schema, query, plan_hint, charset, collation) = match record {
            Some(record) if !record.1.is_empty() => record,
            _ => {
                return Err(plain_error(&format!(
                    "can't find any plans for '{plan_digest}'"
                )))
            }
        };
        if query.ends_with(')') && truncated_sample_text(&query) {
            return Err(plain_error(&format!(
                "binding failed: SQL query is truncated due to tidb_stmt_summary_max_sql_length \
                 limit. Please increase tidb_stmt_summary_max_sql_length. Plan Digest: {plan_digest}"
            )));
        }
        let origin = parse_for_history(&query, plan_digest)?;
        let (complete, reason) =
            tidb_hint::check_binding_from_history_complete(&origin, &plan_hint);
        let bind_sql = tidb_bindinfo::binding_utils::generate_binding_sql(&origin, &plan_hint, &schema);
        let hinted = parse_for_history(&bind_sql, plan_digest)?;
        let bind_sql = binding::restore_with_default_db(&hinted, &schema);
        let db = binding::default_db_of(&origin, &schema);
        let (original_sql, sql_digest) = binding::normalize_with_db(&origin, &schema);
        if !handled_sql_digests.insert(sql_digest.clone()) {
            self.append_sql_warning(tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Warning, 1105,
                format!(
                    "{plan_digest} is ignored because it corresponds to the same SQL digest as \
                     another Plan Digest"
                ),
            ));
            return Ok(None);
        }
        if !complete {
            self.append_sql_warning(tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Warning, 1105,
                format!("{reason}. Plan Digest: {plan_digest}"),
            ));
        }
        let bound = parse_for_history(&bind_sql, plan_digest)?;
        let now = self.binding_timestamp();
        Ok(Some(Binding {
            original_sql,
            no_db_digest: binding::no_db_digest(&bound),
            table_names: binding::collect_table_names(&bound),
            hints: binding::collect_hints(&bound),
            bind_sql,
            db,
            status: STATUS_ENABLED,
            charset,
            collation,
            source: binding::SOURCE_HISTORY,
            sql_digest,
            plan_digest: plan_digest.to_owned(),
            usage: Default::default(),
            create_time: now.clone(),
            update_time: now,
        }))
    }

    /// Go `preprocessor.checkBindGrammar`'s temporary-table pass
    /// (`pkg/planner/core/preprocess.go:598`).
    ///
    /// A binding is a plan hint stored in `mysql.bind_info` and matched by
    /// every session, while a temporary table is one session's private
    /// object: a plan pinned against a table another connection cannot even
    /// name is meaningless, and against a LOCAL temporary table it would
    /// outlive the table itself. TiDB refuses the whole statement with 8006
    /// rather than storing such a binding.
    ///
    /// Three details are Go's and are observable:
    ///
    /// * the operation name is `create binding` even for `DROP BINDING`,
    ///   which reaches the same check;
    /// * a name that does NOT resolve is skipped, not an error -- Go's own
    ///   comment says "drop table -> drop binding" must keep working, and it
    ///   is also what lets the `cte1` in `WITH cte1 AS (...)` through, since
    ///   a CTE name resolves to no table;
    /// * the check runs BEFORE the origin/hinted comparison, so a statement
    ///   that is both mismatched and over a temporary table reports 8006.
    fn refuse_binding_on_temporary_table(&mut self, origin: &Stmt) -> Result<(), DriverError> {
        let names = binding::collect_table_names(origin);
        let current_db = self.session_vars().current_db.clone();
        self.with_catalog_mut(|catalog| {
            for (schema, table) in &names {
                let database = if schema.is_empty() {
                    current_db.as_str()
                } else {
                    schema.as_str()
                };
                if database.is_empty() {
                    continue;
                }
                if matches!(
                    catalog.table_in(database, table),
                    Some(crate::TableEntry::Kv(found)) if found.is_temporary()
                ) {
                    return Err(DriverError::from(
                        tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                            .generate_with_stack_by_args(&["create binding".into()]),
                    ));
                }
            }
            Ok(())
        })
    }

    /// Go `bindingOperator.CreateBinding`'s storage half: older rows for the
    /// same normalized statement become `deleted`, then the new row is
    /// inserted `enabled`. The timestamps print with SIX fractional digits
    /// (`types.NewTime(..., 6)` there, against the session handle's 3).
    fn create_global_binding(&mut self, binding: &Binding) -> Result<(), DriverError> {
        let now = self.global_binding_timestamp();
        self.bind_info_exec(
            "UPDATE mysql.bind_info SET status = ?, update_time = ? \
             WHERE original_sql = ? AND update_time < ?",
            &[
                Datum::new_string("deleted".to_owned()),
                Datum::new_string(now.clone()),
                Datum::new_string(binding.original_sql.clone()),
                Datum::new_string(now.clone()),
            ],
        )?;
        self.bind_info_exec(
            &format!(
                "INSERT INTO mysql.bind_info({BIND_INFO_COLUMNS}, plan_digest) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            &[
                Datum::new_string(binding.original_sql.clone()),
                Datum::new_string(binding.bind_sql.clone()),
                // Go lowercases the schema on the way in
                // (`strings.ToLower(binding.Db)`).
                Datum::new_string(go_to_lower(&binding.db)),
                Datum::new_string(binding.status.to_owned()),
                Datum::new_string(now.clone()),
                Datum::new_string(now),
                Datum::new_string(binding.charset.clone()),
                Datum::new_string(binding.collation.clone()),
                Datum::new_string(binding.source.to_owned()),
                Datum::new_string(binding.sql_digest.clone()),
                // Go stores NULL for a binding without a plan digest.
                if binding.plan_digest.is_empty() {
                    Datum::Null
                } else {
                    Datum::new_string(binding.plan_digest.clone())
                },
            ],
        )?;
        Ok(())
    }

    /// Go `SQLBindExec`'s drop half. Dropping a binding that is not there is
    /// NOT an error (measured on real TiDB: `DROP SESSION BINDING FOR <sql>`
    /// with no such binding answers OK), so the affected count is the only
    /// thing that distinguishes the two outcomes.
    fn drop_binding_stmt(
        &mut self,
        drop: &DropBindingStmt,
    ) -> Result<StmtOutput, DriverError> {
        let digests: Vec<String> = match &drop.target {
            DropBindingTarget::Statement(target) => {
                // Go `preprocessor` reaches `checkBindGrammar` for a
                // `DropBindingStmt` only when it carries a hinted statement
                // too (`preprocess.go:355`), so `DROP BINDING FOR <sql>`
                // alone is NOT refused over a temporary table -- which is the
                // point of the "drop table -> drop binding" comment inside
                // the check.
                if target.hinted.is_some() {
                    self.refuse_binding_on_temporary_table(target.origin.as_ref())?;
                }
                let current_db = self.session_vars().current_db.clone();
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
                        .user_vars()
                        .get_user_var_val(&tidb_hack::go_to_lower(name))
                        .as_ref()
                        .and_then(tidb_datatype::datum_text::sysvar_datum_text)
                        .ok_or_else(|| DriverError::unsupported("sql digest is null")),
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        if drop.scope == BindingScope::Global {
            // Go `bindingOperator.DropBinding`: a drop is an UPDATE to
            // `deleted`, and the affected count is the answer.
            let dropped = self.with_global_binding_storage(|storage| {
                let now = storage.global_binding_timestamp();
                let mut dropped = 0u64;
                for digest in &digests {
                    dropped += storage.bind_info_exec(
                        "UPDATE mysql.bind_info SET status = ?, update_time = ? \
                     WHERE sql_digest = ? AND update_time < ? AND status != ?",
                        &[
                            Datum::new_string("deleted".to_owned()),
                            Datum::new_string(now.clone()),
                            Datum::new_string(digest.clone()),
                            Datum::new_string(now.clone()),
                            Datum::new_string("deleted".to_owned()),
                        ],
                    )?;
                }
                Ok(dropped)
            })?;
            return Ok(StmtOutput::Affected(dropped));
        }
        let dropped = digests
            .iter()
            .filter(|digest| self.session_bindings_mut().drop_digest(digest))
            .count();
        Ok(StmtOutput::Affected(dropped as u64))
    }
}
