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

//! SQL-level prepared statements: `PREPARE`, `EXECUTE`, `DEALLOCATE PREPARE`.
//!
//! Mirrors `pkg/executor/prepared.go` (`PrepareExec`, `ExecuteExec`,
//! `DeallocateExec`) over the per-session store Go keeps in
//! `SessionVars.PreparedStmtNameToID` / `SessionVars.PreparedStmts`.
//!
//! # What the two sides share and where they differ
//!
//! Like Go, `PREPARE` retains the parsed statement and `EXECUTE` installs each
//! execute-time value on a cloned `?` marker before the ordinary statement
//! planner/executor runs it. SQL PREPARE and the binary protocol therefore
//! share the same bound-AST execution path. Two consequences are handled here:
//!
//! * The value comes from the USER VARIABLE's datum at execute time, which is
//!   Go's `usingParam.Eval` over the `ast.VariableExpr` the parser put in the
//!   `USING` list. An unset variable is NULL, captured from TiDB:
//!   `EXECUTE st USING @never_set` returns NULL rather than failing.
//! * An unaliased select field's COLUMN NAME is the field's original text, so
//!   Go answers `PREPARE st FROM 'select ?+1, ?'` with the headers `?+1` and
//!   `?` -- not the bound literals. [`alias_marker_fields`] pins those names
//!   at prepare time, before any value exists to substitute.
//!
//! Marker-free queries validate their plans at PREPARE. Marker-bearing queries
//! still defer some planning errors until EXECUTE.
//!
//! # Two divergences this exposed that are NOT the binding's
//!
//! Making these statements run reveals gaps that a written statement has
//! equally, and both were checked in that written form before being attributed
//! elsewhere:
//!
//! * `SELECT * FROM t WHERE pk = 1.0` over `t(pk int primary key)` holding
//!   `pk = 1` returns NO ROW here while TiDB returns the row. The written
//!   literal behaves identically to the bound parameter, so the gap is in
//!   comparing an integer column against a decimal or a numeric string, not in
//!   binding (`planner/core/tests/prepare/prepare`'s `execute stmt using @a3`
//!   and `@a4`).
//! * An unaliased `row_number() over ()` reports the column name `__window_0`
//!   rather than its source text. That is the window projection's own naming,
//!   which [`pin_field_names`] cannot reach: the statement carries no marker,
//!   so it is never restored.

#[allow(unused_imports)]
use super::*;
use crate::compiler::PlanCacheableChecker;
use std::collections::HashMap;
use tidb_variable::user_vars::UserVarsReader;

use tidb_ast::{Expr, PrepareSource, QueryStmt, SelectField, Stmt};
use tidb_datatype::Datum;
use crate::DriverError;


/// One prepared statement of a session: Go's retained `PlanCacheStmt` input
/// plus the cache-owned physical SELECT descriptor.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub id: u32,
    pub original_sql: String,
    pub database: String,
    /// The statement text to run, with every unaliased marker-bearing select
    /// field already carrying its Go column name (see [`alias_marker_fields`]).
    pub sql: String,
    /// Go `PlanCacheStmt.PreparedAst`: the one PREPARE-time parse cloned and
    /// bound on every EXECUTE. No execute reparses restored SQL.
    pub statement: Stmt,
    /// Go `PlanCacheStmt.VisitInfos`: derived once at PREPARE against the
    /// database current then, checked by every EXECUTE.
    pub privilege_requests: Vec<crate::table_privilege::TablePrivilegeRequest>,
    /// Go `PlanCacheStmt.ParamCount`: the number of `?` markers the statement
    /// carries, which fixes exactly how many values an `EXECUTE` must supply.
    pub param_count: usize,
    /// Go `PlanCacheStmt.StmtCacheable` and `UncacheableReason`, decided ONCE
    /// at `PREPARE` by `IsASTCacheable` -- an uncacheable statement never
    /// reports a hit, and never pays the walk again.
    pub cacheable: Result<(), String>,
    /// SELECT syntax and cache-key metadata. Physical entries belong to the
    /// session cache; hits rebuild its tree in place before execution.
    pub select_plan: Option<std::sync::Arc<crate::PreparedSelectPlan>>,
    /// INSERT/UPDATE/DELETE syntax and cache-key metadata. The ordinary DML
    /// executor consumes the physical root from the same session cache.
    pub dml_plan: Option<std::sync::Arc<crate::PreparedDmlPlan>>,
    /// The marker orders that stand in a `LIMIT`, whose bound values Go admits
    /// only as a non-negative `int64` or a `uint64`
    /// (`CheckParamTypeInt64orUint64` / `getUintFromNode`). Captured: with
    /// `PREPARE l1 FROM 'select a from t order by a limit ?'`,
    /// `EXECUTE l1 USING @ls` for `@ls = '2'` and `USING @neg` for
    /// `@neg = -1` are both `[planner:1210]Incorrect arguments to LIMIT`,
    /// while `@l = 2` runs.
    pub limit_markers: Vec<usize>,
    /// Go `PlanCacheStmt.SchemaVersion`: the schema version the statement was
    /// last preprocessed against. An EXECUTE under another version
    /// re-preprocesses, and a failure there is `ErrSchemaChanged`.
    pub schema_version: u64,
}

/// SQL names retain their shared connection-wide prepared statement ID.
/// Keys are the spelling `PREPARE` used: TiDB's `PreparedStmtNameToID` is a
/// plain `map[string]uint32`, so lookups are case-SENSITIVE. Captured:
/// `PREPARE MyStmt FROM 'select 1'` followed by `EXECUTE mystmt` is
/// `[planner:8111]Prepared statement not found`.
pub type PreparedStore = HashMap<String, PreparedStatement>;

/// Whether this statement kind may not be prepared at all.
///
/// Go's `GeneratePlanCacheStmtWithAST` refuses `IMPORT INTO`, `LOAD DATA`,
/// `PREPARE`, `EXECUTE`, `DEALLOCATE`, a non-transactional DML and a
/// `SELECT ... INTO OUTFILE` with `ErrUnsupportedPs` -- captured:
/// `prepare pe from 'execute ob using @one'` is
/// `[executor:1295]This command is not supported in the prepared statement
/// protocol yet`, and so is `prepare stmt from "load data local infile ..."`.
fn is_unpreparable(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Session(session) => matches!(
            &**session,
            tidb_ast::SessionStmt::Prepare { .. }
                | tidb_ast::SessionStmt::Execute { .. }
                | tidb_ast::SessionStmt::Deallocate(_)
        ),
        Stmt::Dml(dml) => matches!(
            &**dml,
            tidb_ast::DmlStmt::ImportInto(_)
                | tidb_ast::DmlStmt::LoadData(_)
                | tidb_ast::DmlStmt::Batch(_)
        ),
        Stmt::Query(query) => matches!(
            &**query,
            QueryStmt::Select(select) if select.into_outfile.is_some()
        ),
        _ => false,
    }
}

/// go's plan for a FROM-less `SELECT` roots at a `TableDual`, which the
/// prepared plan cache refuses (oracle m21: `EXECUTE st USING @p` over
/// `SELECT ? + 1` carries `skip prepared plan-cache: get a TableDual plan`).
pub fn plans_over_table_dual(stmt: &Stmt) -> bool {
    let Stmt::Query(query) = stmt else {
        return false;
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        return false;
    };
    select.from.is_none()
}

/// The marker orders standing in the statement's top-level `LIMIT`, count and
/// offset alike.
///
/// A `LIMIT` in a subquery or a set-operation term is not collected: those
/// reach Go's check through a different builder call and the suite writes none,
/// so collecting them would be a guess rather than a port.
fn limit_marker_orders(stmt: &Stmt) -> Vec<usize> {
    let Stmt::Query(query) = stmt else {
        return Vec::new();
    };
    let limit = match &**query {
        QueryStmt::Select(select) => select.limit.as_ref(),
        QueryStmt::SetOpr(set_opr) => set_opr.limit.as_ref(),
    };
    let Some(limit) = limit else {
        return Vec::new();
    };
    let mut orders = Vec::new();
    for expr in std::iter::once(&limit.count).chain(limit.offset.as_ref()) {
        if let Expr::ParamMarker { order, .. } = expr {
            orders.push(*order);
        }
    }
    orders
}

/// Pins the column name of every unaliased top-level select field to the text
/// the user wrote, so the restore that binding performs cannot rename it.
///
/// Go names an unaliased field after its OWN SOURCE TEXT
/// (`ast.SelectField.Text()`), and nothing about executing a prepared
/// statement changes that text. This tier binds by restoring the statement,
/// and restore is a canonical printer rather than the source: it writes
/// `SUM(`b`)` where the user wrote `sum(b)`, and it writes the bound literal
/// where the user wrote `?`. Either would rename the column.
///
/// Both halves are captured from TiDB:
///
/// ```text
/// prepare stmt from 'select ?+1, ?';  execute stmt using @i, @i  -- @i = 7
///   RS[?+1|?]      8|7
/// prepare stmt2 from 'select sum(b) from t ... = ?';  execute stmt2 using @v
///   RS[sum(b)]     10
/// ```
///
/// # A bare COLUMN REFERENCE is not named from the source text, and must not
/// be pinned
///
/// The source-text rule above is `buildProjectionFieldNameFromExpressions`,
/// and `buildProjectionField` only reaches it for a field that is NOT a
/// column reference. A field whose expression is an `ast.ColumnNameExpr`
/// takes `buildProjectionFieldNameFromColumns` instead, which names it
/// `colNameField.Name.Name` -- the column IDENTIFIER, with the qualifier
/// dropped. So `select m1.a` is the column `a` however it is printed, and
/// restore cannot rename it.
///
/// Pinning such a field is therefore not a no-op, it is a REGRESSION: it
/// installs `m1.a` as an explicit alias, and an explicit alias is the one
/// thing that DOES override the column identifier
/// (`if origField.AsName.L == ""`). `executor/jointest/join`'s
/// `execute stmt1 using @a` over `select m1.a from t as m1 where m1.a in
/// (select m2.b+? from t as m2)` is recorded with the header `a`; this tier
/// printed `m1.a` for exactly that reason, and printed the correct `a` for
/// the same statement prepared WITHOUT a `?` (which pins nothing).
///
/// Returns whether anything was pinned, which tells the caller whether the
/// statement has to be restored rather than kept as written.
fn pin_field_names(stmt: &mut Stmt) -> bool {
    let Stmt::Query(query) = stmt else {
        return false;
    };
    let QueryStmt::Select(select) = &mut **query else {
        return false;
    };
    // The parser's per-field source bytes, read before the loop takes the
    // fields mutably. A field whose source was not recorded falls back to its
    // restored form, which is what the name would have been anyway.
    let source: Vec<Option<String>> = (0..select.fields.fields().len())
        .map(|index| {
            select
                .fields
                .original_text(index)
                .map(|bytes| tidb_hack::raw_bytes::bytes_to_str(bytes).trim().to_owned())
                .filter(|text| !text.is_empty())
        })
        .collect();
    let mut pinned = false;
    for (field, source) in select.fields.fields_mut().iter_mut().zip(source) {
        if let SelectField::Expr { expr, alias } = field {
            if alias.is_none() && !matches!(expr, Expr::Column(_)) {
                *alias = Some(source.unwrap_or_else(|| expr.restore()));
                pinned = true;
            }
        }
    }
    pinned
}

/// Go `PrepareExec`, `ExecuteExec` and `DeallocateExec` (`executor/prepared.go`).
pub trait PreparedExec: super::CompilerSession {
    /// The text a prepared name holds, if this session holds that name.
    ///
    /// `EXECUTE` is the one statement whose answer SHAPE is not in its own
    /// parse -- it is whatever the prepared statement answers -- so
    /// [`Session::statement_kind_parsed`] resolves the name through here
    /// rather than guessing from the `EXECUTE` keyword.
    fn prepared_statement_sql(&self, name: &str) -> Option<&str> {
        self.prepared_statements_ref()
            .get(name)
            .map(|prepared| prepared.sql.as_str())
    }

    /// The statement a prepared name holds, if this session holds that name.
    fn prepared_statement_node(&self, name: &str) -> Option<&Stmt> {
        self.prepared_statements_ref()
            .get(name)
            .map(|prepared| &prepared.statement)
    }

    /// One `EXECUTE ... USING @name` value: Go `SetParameterValuesIntoSCtx`
    /// evaluates the entry's `getvar` function (`usingParam.Eval`), and a
    /// string read comes back stamped with the variable's type's collation
    /// (`getStringVarFunctionClass` copies it from `GetUserVarType`;
    /// `ScalarFunction.Eval` -> `SetValue`), which the parameter's type then
    /// follows (`InferParamTypeFromDatum`). A gbk client's `SET @v = '中'`
    /// therefore binds as gbk text.
    fn using_param_value(&self, name: &str) -> Datum {
        let name = tidb_hack::go_to_lower(name);
        let value = self
            .user_vars()
            .get_user_var_val(&name)
            .unwrap_or(Datum::Null);
        let Datum::String(string) = &value else {
            return value;
        };
        let Some(field_type) = self.user_vars().get_user_var_type(&name) else {
            return value;
        };
        use tidb_datatype::EvalType;
        let string_read = !matches!(
            field_type.eval_type(),
            EvalType::Int | EvalType::Real | EvalType::Decimal | EvalType::Datetime
        );
        if !string_read || field_type.charset_name().is_empty() {
            return value;
        }
        match tidb_charset::Collation::from_name(field_type.collation_name()) {
            Some(collation) if collation != string.collation() => {
                Datum::new_collation_string(string.bytes(), collation)
            }
            _ => value,
        }
    }

    /// The text of a prepared DDL statement named by a text-protocol
    /// `EXECUTE name` with no `USING` values, parsed. Go's `ExecuteExec` runs
    /// the prepared plan as the statement's body, so a node that owns DDL
    /// routes this body itself instead of letting the session run it.
    fn prepared_ddl_body(&self, stmt: &Stmt) -> Option<(String, Stmt)> {
        let Stmt::Session(session) = stmt else {
            return None;
        };
        let tidb_ast::SessionStmt::Execute { name, using } = session.as_ref() else {
            return None;
        };
        if !using.is_empty() {
            return None;
        }
        let prepared = self.prepared_statements_ref().get(name)?;
        if prepared.param_count != 0 || !matches!(prepared.statement, Stmt::Ddl(_)) {
            return None;
        }
        Some((prepared.sql.clone(), prepared.statement.clone()))
    }

    /// Go `PrepareExec.Next`: parses the statement text and stores it under
    /// `name`, replacing whatever that name held before.
    ///
    /// The text is PARSED here, which is why a syntax error is reported by
    /// `PREPARE` and not by the `EXECUTE` that would have run it. A text that
    /// parses into more than one statement is Go's `ErrPrepareMulti`.
    fn prepare_statement(
        &mut self,
        name: &str,
        source: &PrepareSource,
    ) -> Result<(), DriverError> {
        let text = match source {
            PrepareSource::Sql(sql) => sql.clone(),
            // Go evaluates the variable and parses its VALUE as SQL, so a
            // variable holding a non-string is stringified first: captured,
            // `SET @num = 5; PREPARE pn FROM @num` is the syntax error
            // `near "5"`, and an unset variable parses the text `NULL`.
            PrepareSource::Var(name) => self.prepare_source_text(name),
        };
        // Go `PrepareExec` for the SQL `PREPARE` (`needReset` false) passes
        // only `CharsetConnection`/`CollationConnection`: the text is a string
        // value, already converted, so the client encoding is the scanner's
        // default.
        let mut statements = tidb_parser::parse_multi_with_sql_mode_and_config(
            &text,
            self.scanner_sql_mode(),
            tidb_parser::ParserConfig {
                charset_client: tidb_parser::ParserConfig::default().charset_client,
                ..self.session_vars().build_parser_config()
            },
        )
        .map_err(|e| {
            let raw = e.compatibility_message(&text);
            // go's PREPARE appends the inner parse failure's RAW error
            // (the positional text, under the generic 1105) at the parse
            // site; the classified 1064 lands afterwards from the
            // statement error return -- SHOW WARNINGS carries both rows.
            self.append_sql_warning(tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Error, 1105, raw.clone()));
            DriverError::from(tidb_util::misc::syntax_error(raw))
        })?;
        if statements.len() != 1 {
            return Err(DriverError::from(
                tidb_dbterror::exeerrors::ERR_PREPARE_MULTI.generate_with_stack_by_args(&[]),
            ));
        }
        let mut statement = statements.remove(0);
        // Go `GeneratePlanCacheStmtWithAST`'s own switch, before any planning.
        if is_unpreparable(&statement) {
            return Err(DriverError::from(
                tidb_dbterror::exeerrors::ERR_UNSUPPORTED_PS.generate_with_stack_by_args(&[]),
            ));
        }
        // Go `Preprocess` at PREPARE (`handleTableName`): the retained
        // statement names its tables by the database current NOW, so its
        // requests below and every EXECUTE after a `USE` agree.
        tidb_bindinfo::binding::pin_current_database(&mut statement, self.session_vars().current_db.as_str())?;
        let param_count = crate::parsed_parameter_count(&statement);
        let limit_markers = limit_marker_orders(&statement);
        // Go's PrepareExec builds the plan with each variable read as its value
        // NOW (`rewriteVariable`); the retained statement keeps the variables,
        // which EXECUTE binds afresh. Only the marker-free query validated
        // below needs the bound copy.
        let validated_query = match &statement {
            tidb_ast::Stmt::Query(_) => {
                let mut bound = statement.clone();
                self.bind_variables(&mut bound)?;
                // EXECUTE plans the tree the statement dispatch rewrites:
                // MATCH ... AGAINST's LIKE fallback is applied there, so the
                // PREPARE-time plan is built over the same tree.
                self.rewrite_fts_for_planning(&mut bound)?;
                Some(bound)
            }
            _ => None,
        };
        // Build statement-local planner state before locking the catalog: the
        // context's sequence/key-decode snapshots consult that same catalog.
        let planner_context = self.statement_context(false);
        // Go `GeneratePlanCacheStmtWithAST` runs `CacheableWithCtx` here, at
        // PREPARE, and stores the verdict on the `PlanCacheStmt`.
        // Go's PREPARE preprocesses against the session's infoschema, LOCAL
        // temporary tables included, and reads under the open transaction.
        let catalog = self.with_catalog_scope(true, |catalog| Ok(catalog.clone()))?;
        let (cacheable, select_plan, dml_plan, skip_reason) = {
            let cacheable = self.prepared_statement_cacheable(&mut statement, &catalog);
            // go's PrepareExec builds the plan AT PREPARE, so plan errors
            // surface here rather than at the first EXECUTE (oracle m21:
            // `PREPARE st2 FROM 'SELECT no_such_col'` answers 1054 with the
            // error row). Marker-carrying statements plan against BOUND
            // parameters at EXECUTE, so only marker-free ones validate here.
            // The built plan is discarded -- the cache envelope above and
            // the EXECUTE-time plan remain the execution source.
            match &validated_query {
                Some(tidb_ast::Stmt::Query(query)) if param_count == 0 => {
                    self.retrieving_memory_tables(&planner_context, |session| {
                        crate::plan_query_meta_stmt(
                            query,
                            &catalog,
                            session.session_vars().current_db.as_str(),
                            &planner_context,
                        )
                    })?;
                }
                // Go builds this plan with each `?` still a ParamMarkerExpr,
                // so what the markers will hold is not checked, but a name the
                // query cannot resolve is refused. A marker stands here as a
                // value, and an error raised under BOTH NULL and 1 does not
                // depend on that value: it is the query's own.
                Some(bound @ tidb_ast::Stmt::Query(_)) => {
                    let stand_in_error = |stand_in: &Datum| -> Option<DriverError> {
                        let values = vec![stand_in.clone(); param_count];
                        let tidb_ast::Stmt::Query(query) =
                            crate::bind_statement(bound.clone(), &values).ok()?
                        else {
                            return None;
                        };
                        crate::plan_query_meta_stmt(
                            &query,
                            &catalog,
                            self.session_vars().current_db.as_str(),
                            &planner_context.with_isolated_warnings(),
                        )
                        .err()
                        // A scan of a memory table needs the session to
                        // retrieve its rows, which says nothing of the query.
                        .filter(|error| !matches!(error, DriverError::MemTableRetrievalNeeded))
                    };
                    // A shape this tier cannot plan is no answer Go gives,
                    // so it never refuses a PREPARE Go would admit.
                    let rust_only = |error: &DriverError| {
                        matches!(
                            error,
                            DriverError::Unsupported(_)
                                | DriverError::Exec(crate::ExecError::Unsupported(_))
                                | DriverError::Exec(crate::ExecError::Eval(
                                    tidb_exprctx::EvalError::Unsupported(_)
                                ))
                        )
                    };
                    if let (Some(with_null), Some(with_one)) =
                        (stand_in_error(&Datum::Null), stand_in_error(&Datum::Int(1)))
                    {
                        let (null_reported, one_reported) = (
                            with_null.clone().to_mysql_error(),
                            with_one.to_mysql_error(),
                        );
                        if null_reported.code == one_reported.code
                            && null_reported.message == one_reported.message
                            && !rust_only(&with_null)
                        {
                            return Err(with_null);
                        }
                    }
                }
                _ => {}
            }
            // go's plan cache records the refusal as a statement warning at
            // PREPARE too (oracle m21: PREPARE st3 FROM 'CREATE TABLE ...'
            // answers (ok) with `skip prepared plan-cache: not a
            // SELECT/UPDATE/INSERT/DELETE/SET statement`).
            let skip_reason = cacheable
                .as_ref()
                .err()
                .map(|reason| format!("skip prepared plan-cache: {reason}"));
            let select_plan = if cacheable.is_ok() {
                crate::build_prepared_select_plan(
                    &statement,
                    param_count,
                    &catalog,
                    self.session_vars().current_db.as_str(),
                    &planner_context,
                )
                .map(|plan| std::sync::Arc::new(plan.with_sql(&text)))
            } else {
                None
            };
            let dml_plan = if cacheable.is_ok() {
                crate::build_prepared_dml_plan(
                    &statement,
                    param_count,
                    &catalog,
                    self.session_vars().current_db.as_str(),
                )?
                .map(|plan| std::sync::Arc::new(plan.with_sql(&text)))
            } else {
                None
            };
            (cacheable, select_plan, dml_plan, skip_reason)
        };
        // go's plan cache records the refusal as a statement warning at
        // PREPARE too (oracle m21: PREPARE st3 FROM 'CREATE TABLE ...'
        // answers (ok) with `skip prepared plan-cache: not a
        // SELECT/UPDATE/INSERT/DELETE/SET statement`).
        if let Some(reason) = skip_reason {
            self.append_sql_warning(tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Warning, 1105, reason));
        }
        // Only a statement that CARRIES markers is ever restored (that is what
        // binding does), so only that statement needs its column names pinned
        // against the restore. A marker-free statement keeps the text the user
        // wrote and runs through the ordinary path, names and all.
        let original_sql = text.clone();
        let sql = if param_count > 0 && pin_field_names(&mut statement) {
            statement.restore()
        } else {
            text
        };
        let privilege_requests = self.collect_table_privileges(&statement, true)?;
        // Go `GeneratePlanCacheStmtWithAST` ends with `checkPreparedPriv`:
        // PREPARE itself is refused without the statement's privileges.
        self.check_table_privilege_requests(&privilege_requests)?;
        let id = self.allocate_prepared_statement_id();
        let schema_version = self.with_catalog_mut(|catalog| Ok(catalog.schema_version()))?;
        let database = self.session_vars().current_db.clone();
        self.prepared_statements_mut().insert(
            name.to_owned(),
            PreparedStatement {
                id,
                original_sql,
                database,
                sql,
                statement,
                privilege_requests,
                param_count,
                limit_markers,
                cacheable,
                select_plan,
                dml_plan,
                schema_version,
            },
        );
        Ok(())
    }

    /// The SQL text a `PREPARE ... FROM @var` parses: the variable's value as
    /// its string form, with an unset variable reading as `NULL` exactly as
    /// Go's `GetVar` -> `Datum.ToString` does.
    fn prepare_source_text(&self, name: &str) -> String {
        let value = self
            .user_vars()
            .get_user_var_val(&tidb_hack::go_to_lower(name))
            .unwrap_or(Datum::Null);
        // A value with no text form reads as `NULL`, the same text an unset
        // variable parses -- which is a syntax error unless the whole prepared
        // text is literally `NULL`, so nothing is silently accepted here.
        tidb_datatype::datum_text::sysvar_datum_text(&value).unwrap_or_else(|| "NULL".to_owned())
    }

    /// Go `DeallocateExec.Next`: drops the name, or reports
    /// `ErrStmtNotFound` when the session does not hold it. `DROP PREPARE`
    /// parses to the same statement and behaves identically.
    fn deallocate_prepared_statement(&mut self, name: &str) -> Result<(), DriverError> {
        match self.prepared_statements_mut().remove(name) {
            Some(prepared) => {
                self.release_prepared_plans(
                    prepared.select_plan.as_deref(),
                    prepared.dml_plan.as_deref(),
                );
                Ok(())
            }
            None => Err(DriverError::from(
                tidb_plannererrors::ERR_STMT_NOT_FOUND.generate_with_stack_by_args(&[]),
            )),
        }
    }
}

impl<T: super::CompilerSession> PreparedExec for T {}
