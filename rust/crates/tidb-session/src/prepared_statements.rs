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
//! The plan cache is not modelled at all -- there is no cache to hit, so
//! nothing here can report a hit.

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
        let mut statements =
            tidb_parser::parse_multi(&text).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        if statements.len() != 1 {
            return Err(DriverError::PrepareMulti);
        }
        let mut statement = statements.remove(0);
        let param_count = tidb_executor::parameter_count(&text)?;
        // Restoring is only needed when a column NAME has to be pinned; every
        // other statement keeps the text the user wrote, so nothing that does
        // not need normalizing gets it.
        let sql = if alias_marker_fields(&mut statement) {
            statement.restore()
        } else {
            text
        };
        self.prepared_statements
            .insert(name.to_owned(), PreparedStatement { sql, param_count });
        Ok(())
    }

    /// The SQL text a `PREPARE ... FROM @var` parses: the variable's value as
    /// its string form, with an unset variable reading as `NULL` exactly as
    /// Go's `GetVar` -> `Datum.ToString` does.
    fn prepare_source_text(&self, name: &str) -> String {
        let value = self
            .user_vars
            .borrow()
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
                    .borrow()
                    .get(&name.to_ascii_lowercase())
                    .cloned()
                    .unwrap_or(Datum::Null),
                _ => unreachable!("the parser admits only @variables in USING"),
            })
            .collect();
        let sql = if values.is_empty() {
            prepared.sql.clone()
        } else {
            tidb_executor::bind_parameters(&prepared.sql, &values)?
        };
        // Go builds the prepared statement's own plan and runs it as this
        // statement's body, so the inner statement goes through the same
        // dispatch every other statement does -- including DDL's implicit
        // commit, which is why `EXECUTE` of a prepared `CREATE TABLE` works
        // (captured).
        self.execute_statement(&sql)
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

/// Pins the column name of every unaliased top-level select field that carries
/// a `?`, so binding cannot rename it.
///
/// Go names an unaliased field after its own source text, and a marker's text
/// is `?` however it is later bound: captured,
/// `PREPARE st FROM 'select ?+1, ?'; EXECUTE st USING @i, @i` with `@i = 7`
/// answers `8` and `7` under the headers `?+1` and `?`. Substituting the
/// literal first would have named them `7+1` and `7`.
///
/// Returns whether anything was aliased, which tells the caller whether the
/// statement has to be restored rather than kept as written.
fn alias_marker_fields(stmt: &mut Stmt) -> bool {
    let Stmt::Query(query) = stmt else {
        return false;
    };
    let QueryStmt::Select(select) = &mut **query else {
        return false;
    };
    let mut aliased = false;
    for field in select.fields.fields_mut() {
        if let SelectField::Expr { expr, alias } = field {
            if alias.is_none() && expr.flags() & tidb_ast::FLAG_HAS_PARAM_MARKER != 0 {
                *alias = Some(expr.restore());
                aliased = true;
            }
        }
    }
    aliased
}
