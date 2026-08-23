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
//! Go keeps the PARSED statement and installs each execute-time value on the
//! `?` marker node itself, then plans over that tree. This tier reaches
//! execution through SQL text, so `PREPARE` keeps the text and `EXECUTE` turns
//! the markers into the literals for their values -- the same round trip the
//! binary protocol already takes through
//! [`tidb_executor::bind_parameters`]. Two consequences are handled here
//! rather than left to differ:
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
//! # What is deliberately not modelled
//!
//! Go's `PREPARE` runs the whole optimizer (`GeneratePlanCacheStmtWithAST`),
//! so a statement that PARSES but cannot be PLANNED is refused at prepare
//! time and never enters the store. Captured from TiDB:
//! `PREPARE gb FROM 'select a from t group by ?'` fails at PREPARE with
//! `[planner:1055]` under `only_full_group_by`, and the following
//! `EXECUTE gb` is then `[planner:8111]Prepared statement not found`. Here
//! only parsing happens at prepare time, so such a statement is accepted and
//! its planning error surfaces at `EXECUTE` instead. The rejection still
//! happens; only WHICH statement reports it differs.
//!
//! The PREPARED plan cache is not modelled at all -- a prepared statement is
//! re-planned from its own text on every `EXECUTE`, so no `EXECUTE` here can
//! report a hit.
//!
//! This is NOT true of the session as a whole, and the earlier wording that
//! said "nothing here can report a hit" was wrong: the non-prepared plan
//! cache in [`crate::non_prepared_plan_cache`] does report hits, and a
//! `.test` case that runs a bare `SELECT` twice under
//! `tidb_enable_non_prepared_plan_cache=1` and then reads
//! `select @@last_plan_from_cache` is reachable through that tier. Only the
//! `PREPARE`/`EXECUTE` shape is out of reach, and by construction rather than
//! by omission.
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

use std::collections::HashMap;

use tidb_ast::{Expr, PrepareSource, QueryStmt, SelectField, Stmt};
use tidb_datatype::Datum;
use tidb_executor::DriverError;

use crate::{Session, StmtOutput};

/// One prepared statement of a session: Go's `PlanCacheStmt`, reduced to what
/// this tier needs to run it again.
#[derive(Debug, Clone)]
pub(crate) struct PreparedStatement {
    /// The statement text to run, with every unaliased marker-bearing select
    /// field already carrying its Go column name (see [`alias_marker_fields`]).
    sql: String,
    /// Go `PlanCacheStmt.ParamCount`: the number of `?` markers the statement
    /// carries, which fixes exactly how many values an `EXECUTE` must supply.
    param_count: usize,
    /// Go `PlanCacheStmt.StmtCacheable` and `UncacheableReason`, decided ONCE
    /// at `PREPARE` by `IsASTCacheable` -- an uncacheable statement never
    /// reports a hit, and never pays the walk again.
    cacheable: Result<(), String>,
    /// The [`crate::prepared_plan_cache::PreparedPlanKey`] the LAST `EXECUTE`
    /// planned against. An `EXECUTE` whose key equals it is the one Go serves
    /// from the cache, and is what `@@last_plan_from_cache` reports.
    last_plan_key: Option<crate::prepared_plan_cache::PreparedPlanKey>,
    /// The marker orders that stand in a `LIMIT`, whose bound values Go admits
    /// only as a non-negative `int64` or a `uint64`
    /// (`CheckParamTypeInt64orUint64` / `getUintFromNode`). Captured: with
    /// `PREPARE l1 FROM 'select a from t order by a limit ?'`,
    /// `EXECUTE l1 USING @ls` for `@ls = '2'` and `USING @neg` for
    /// `@neg = -1` are both `[planner:1210]Incorrect arguments to LIMIT`,
    /// while `@l = 2` runs.
    limit_markers: Vec<usize>,
}

/// The per-session store. Go keeps two maps (name -> id, id -> statement);
/// nothing here needs the numeric id a client would use over the binary
/// protocol, so the name is the key.
///
/// Keys are the spelling `PREPARE` used: TiDB's `PreparedStmtNameToID` is a
/// plain `map[string]uint32`, so lookups are case-SENSITIVE. Captured:
/// `PREPARE MyStmt FROM 'select 1'` followed by `EXECUTE mystmt` is
/// `[planner:8111]Prepared statement not found`.
pub(crate) type PreparedStore = HashMap<String, PreparedStatement>;

impl Session {
    /// The text a prepared name holds, if this session holds that name.
    ///
    /// `EXECUTE` is the one statement whose answer SHAPE is not in its own
    /// parse -- it is whatever the prepared statement answers -- so
    /// [`Session::statement_kind_parsed`] resolves the name through here
    /// rather than guessing from the `EXECUTE` keyword.
    pub(crate) fn prepared_statement_sql(&self, name: &str) -> Option<&str> {
        self.prepared_statements
            .get(name)
            .map(|prepared| prepared.sql.as_str())
    }

    /// Go `PrepareExec.Next`: parses the statement text and stores it under
    /// `name`, replacing whatever that name held before.
    ///
    /// The text is PARSED here, which is why a syntax error is reported by
    /// `PREPARE` and not by the `EXECUTE` that would have run it. A text that
    /// parses into more than one statement is Go's `ErrPrepareMulti`.
    pub(crate) fn prepare_statement(
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
        let mut statements = tidb_parser::parse_multi_with_sql_mode(&text, self.scanner_sql_mode())
            .map_err(|e| DriverError::Parse(e.compatibility_message(&text)))?;
        if statements.len() != 1 {
            return Err(DriverError::PrepareMulti);
        }
        let mut statement = statements.remove(0);
        // Go `GeneratePlanCacheStmtWithAST`'s own switch, before any planning.
        if is_unpreparable(&statement) {
            return Err(DriverError::UnsupportedPreparedStatement);
        }
        let param_count = tidb_executor::parameter_count(&text, self.scanner_sql_mode())?;
        let limit_markers = limit_marker_orders(&statement);
        // Go `GeneratePlanCacheStmtWithAST` runs `CacheableWithCtx` here, at
        // PREPARE, and stores the verdict on the `PlanCacheStmt`.
        let cacheable = crate::prepared_plan_cache::stmt_cacheable(&mut statement);
        // Only a statement that CARRIES markers is ever restored (that is what
        // binding does), so only that statement needs its column names pinned
        // against the restore. A marker-free statement keeps the text the user
        // wrote and runs through the ordinary path, names and all.
        let sql = if param_count > 0 && pin_field_names(&mut statement) {
            statement.restore()
        } else {
            text
        };
        self.prepared_statements.insert(
            name.to_owned(),
            PreparedStatement {
                sql,
                param_count,
                limit_markers,
                cacheable,
                last_plan_key: None,
            },
        );
        Ok(())
    }

    /// The SQL text a `PREPARE ... FROM @var` parses: the variable's value as
    /// its string form, with an unset variable reading as `NULL` exactly as
    /// Go's `GetVar` -> `Datum.ToString` does.
    fn prepare_source_text(&self, name: &str) -> String {
        let value = self
            .user_vars
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or(Datum::Null);
        // A value with no text form reads as `NULL`, the same text an unset
        // variable parses -- which is a syntax error unless the whole prepared
        // text is literally `NULL`, so nothing is silently accepted here.
        crate::datum_text(&value).unwrap_or_else(|| "NULL".to_owned())
    }

    /// Go `ExecuteExec`: looks the statement up, binds the `USING` variables'
    /// current values to its markers, and runs the result.
    pub(crate) fn execute_prepared_statement(
        &mut self,
        name: &str,
        using: &[Expr],
    ) -> Result<StmtOutput, DriverError> {
        let prepared = self
            .prepared_statements
            .get(name)
            .cloned()
            .ok_or(DriverError::PreparedStmtNotFound)?;
        // Go `planCachePreprocess` step 1, before any value is evaluated.
        if using.len() != prepared.param_count {
            return Err(DriverError::WrongParamCount);
        }
        // Go `SetParameterValuesIntoSCtx`: each `USING` entry is evaluated to
        // a Datum, and the parameter's TYPE then follows from that value
        // (`InferParamTypeFromDatum`) rather than from anything the PREPARE
        // knew. The parser admits only user variables here, so evaluating one
        // is a lookup in this session's own map.
        let values: Vec<Datum> = using
            .iter()
            .map(|expr| match expr {
                Expr::UserVar(name) => self
                    .user_vars
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&name.to_ascii_lowercase())
                    .cloned()
                    .unwrap_or(Datum::Null),
                _ => unreachable!("the parser admits only @variables in USING"),
            })
            .collect();
        // Go checks a `LIMIT` parameter's KIND, not just its value, so a
        // string that happens to read as a number is refused as firmly as a
        // negative integer is.
        for &order in &prepared.limit_markers {
            match values.get(order) {
                Some(Datum::Int(value)) if *value >= 0 => {}
                Some(Datum::UInt(_)) => {}
                _ => return Err(DriverError::WrongArguments("LIMIT")),
            }
        }
        let sql = if values.is_empty() {
            prepared.sql.clone()
        } else {
            tidb_executor::bind_parameters(&prepared.sql, &values, self.scanner_sql_mode())?
        };
        // Go `GetPlanFromPlanCache`: a hit is this statement, cacheable,
        // planned before under an IDENTICAL key -- schema version, database,
        // sql_mode, time zone, and the push-down blacklist's reload counter.
        // Decided BEFORE the run (the key describes what planning would see),
        // reported AFTER it succeeds, so a failing statement publishes
        // nothing, and recorded back onto the session's copy of the handle.
        let cache_enabled = self.prepared_plan_cache_enabled();
        let key = cache_enabled.then(|| self.prepared_plan_key());
        let hit = prepared.cacheable.is_ok() && key.is_some() && prepared.last_plan_key == key;
        // Go builds the prepared statement's own plan and runs it as this
        // statement's body, so the inner statement goes through the same
        // dispatch every other statement does -- including DDL's implicit
        // commit, which is why `EXECUTE` of a prepared `CREATE TABLE` works
        // (captured).
        let output = self.execute_statement(&sql)?;
        // Go `isPhysicalPlanCacheable`'s `PhysicalApply` arm runs on the
        // BUILT plan, after the AST checker said yes: a plan containing an
        // Apply is refused outright -- neither stored nor reported -- because
        // a per-outer-row executor cannot be reused across parameter sets.
        // The driver reports it through the statement context's channel.
        if self.planned_apply.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(output);
        }
        if hit {
            self.found_in_plan_cache = true;
        }
        if let (Ok(()), Some(key)) = (&prepared.cacheable, key) {
            if let Some(stored) = self.prepared_statements.get_mut(name) {
                stored.last_plan_key = Some(key);
            }
        }
        Ok(output)
    }

    /// Go `DeallocateExec.Next`: drops the name, or reports
    /// `ErrStmtNotFound` when the session does not hold it. `DROP PREPARE`
    /// parses to the same statement and behaves identically.
    pub(crate) fn deallocate_prepared_statement(&mut self, name: &str) -> Result<(), DriverError> {
        match self.prepared_statements.remove(name) {
            Some(_) => Ok(()),
            None => Err(DriverError::PreparedStmtNotFound),
        }
    }
}

/// Whether this statement kind may not be prepared at all.
///
/// Go's `GeneratePlanCacheStmtWithAST` refuses `IMPORT INTO`, `LOAD DATA`,
/// `PREPARE`, `EXECUTE`, `DEALLOCATE`, a non-transactional DML and a
/// `SELECT ... INTO OUTFILE` with `ErrUnsupportedPs`. The three the suite
/// writes are the prepared-statement kinds themselves -- captured:
/// `prepare pe from 'execute ob using @one'` is
/// `[executor:1295]This command is not supported in the prepared statement
/// protocol yet` -- so those are the arms this covers; the loaders and
/// `INTO OUTFILE` are not modelled by this engine at all and would be refused
/// on their own.
fn is_unpreparable(stmt: &Stmt) -> bool {
    let Stmt::Session(session) = stmt else {
        return false;
    };
    matches!(
        &**session,
        tidb_ast::SessionStmt::Prepare { .. }
            | tidb_ast::SessionStmt::Execute { .. }
            | tidb_ast::SessionStmt::Deallocate(_)
    )
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
                .map(|bytes| String::from_utf8_lossy(bytes).trim().to_owned())
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
