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

//! The variable surfaces: `SET` in all its forms, and the substitution that
//! turns a statement's `@@x` and `@x` references into something the planner --
//! which has no session -- can type.
//!
//! Go splits this between `executor.SetExecutor` (the write side) and the
//! expression rewriter's `rewriteSystemVariable` / `BuildGetVarFunction` (the
//! read side); both read the one [`crate::SessionVars`] this session owns, so
//! both live here.

use tidb_ast::{SessionStmt, Stmt};
use tidb_datatype::Datum;
use tidb_executor::DriverError;

use crate::{sysvar, Session, VarError};

/// Go `SysVar.GetNativeValType` (`pkg/sessionctx/variable/variable.go:455`),
/// which `rewriteSystemVariable` applies to every `@@var` it folds into a
/// constant: the registry's `Type` -- not the variable's name -- decides the
/// value's domain. `TypeBool` becomes the signed `1`/`0` of `TiDBOptOn`, so
/// `SELECT @@autocommit` reports `1` and never the stored `ON`; `TypeUnsigned`
/// becomes a number; every other type stays the stored string.
///
/// Go builds a `Uint` datum for `TypeUnsigned`, which this AST has no literal
/// for: [`Expr::Int`] carries digits that later fail above `i64::MAX`. A value
/// that does not fit stays a string, which renders identically and keeps the
/// arithmetic gap where it already is rather than turning a readable variable
/// into an error.
fn sysvar_native_expr(name: &str, value: String) -> tidb_ast::Expr {
    use tidb_ast::Expr;
    match sysvar::get_sys_var(name).map(|def| def.var_type) {
        Some(sysvar::VarType::Bool) => {
            let on = value.eq_ignore_ascii_case("ON") || value == "1";
            Expr::Int(i32::from(on).to_string())
        }
        Some(sysvar::VarType::Unsigned) if value.parse::<i64>().is_ok() => Expr::Int(value),
        _ => Expr::String(value),
    }
}

/// The call `@name` becomes: Go's `BuildGetVarFunction` chooses one of its
/// typed `GETVAR` signatures from the type the session holds for the name, and
/// the choice rides in the function name so the rewriter -- which has no
/// session -- can type the node (see `getvar_*` in `tidb_expr`'s
/// `builtin_return_type`).
///
/// An UNSET variable has no type to read; Go's own answer is a string-typed
/// NULL, which `getvar_string` produces.
fn uservar_read_expr(name: &str, value: Option<&Datum>) -> tidb_ast::Expr {
    let kind = match value {
        Some(Datum::Int(_)) => "int",
        Some(Datum::UInt(_)) => "uint",
        Some(Datum::Real(_)) => "real",
        Some(Datum::Decimal(_)) => "decimal",
        _ => "string",
    };
    tidb_ast::Expr::Func {
        name: format!("getvar_{kind}"),
        args: vec![tidb_ast::Expr::String(name.to_owned())],
        origin_position: 0,
    }
}

/// Maps a variable error onto the driver error the wire layer renders.
fn var_error(error: VarError) -> DriverError {
    DriverError::Var(match error {
        VarError::UnknownSystemVariable(name) => {
            tidb_executor::VarErrorKind::UnknownSystemVariable(name)
        }
        VarError::ReadOnlyVariable(name) => tidb_executor::VarErrorKind::ReadOnlyVariable(name),
        VarError::WrongTypeForVar(name) => tidb_executor::VarErrorKind::WrongTypeForVar(name),
        VarError::WrongValueForVar(name, value) => {
            tidb_executor::VarErrorKind::WrongValueForVar(name, value)
        }
        VarError::SessionOnlyVariable(name) => {
            tidb_executor::VarErrorKind::SessionOnlyVariable(name)
        }
        VarError::GlobalOnlyVariable(name) => tidb_executor::VarErrorKind::GlobalOnlyVariable(name),
        VarError::NoGlobalCopy(name) => tidb_executor::VarErrorKind::NoGlobalCopy(name),
        VarError::ValidationRefused(message) => {
            tidb_executor::VarErrorKind::ValidationRefused(message.to_owned())
        }
    })
}

/// The text form a system variable stores for a datum (Go keeps every system
/// variable as a string).
pub(crate) fn datum_text(value: &Datum) -> Option<String> {
    match value {
        Datum::Null => None,
        Datum::Int(v) => Some(v.to_string()),
        Datum::UInt(v) => Some(v.to_string()),
        Datum::Real(v) => Some(v.to_string()),
        Datum::Decimal(d) => Some(d.to_string()),
        Datum::String(s) => Some(String::from_utf8_lossy(s.bytes()).into_owned()),
        Datum::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        // `BinaryJSON.String`: the canonical document text a JSON column
        // sends on the wire.
        Datum::Json(j) => Some(j.to_string()),
        _ => None,
    }
}

impl Session {
    /// Applies a `SET` statement.
    ///
    /// Returns `Some(())` when the SQL is a `SET` this handles and `None`
    /// otherwise, so a caller can answer with an OK packet without
    /// re-parsing. Go's `SetExecutor` walks the assignments in source order
    /// and stops at the first error, which this reproduces.
    ///
    /// `SET GLOBAL` writes straight into the shared [`vars::GlobalSysvars`]
    /// this call was given (see [`Self::attach_globals`],
    /// [`Self::swap_globals`]), which is this process's only copy unless a
    /// front end also persists it: the convergence node's
    /// `crate::cluster_sysvar_seam` (in `tidb-server`) is what makes that
    /// table itself a scratch read of `mysql.global_variables`, validates
    /// this call against it, and persists the result. A front end with no
    /// such seam (an in-process session, or a node that serves no cluster)
    /// keeps the in-memory-only behavior this always had.
    ///
    /// DEFERRED (documented): resource groups and the other non-variable
    /// `SET` forms stay unsupported.
    pub fn apply_set(&mut self, sql: &str) -> Result<Option<()>, DriverError> {
        let stmt = self.parse(sql)?;
        self.apply_set_stmt(&stmt)
    }

    /// [`Self::apply_set`] over a statement this session already parsed. The
    /// text form parses and delegates here; the `SET` family is recognized in
    /// exactly one place either way.
    pub(crate) fn apply_set_stmt(&mut self, stmt: &Stmt) -> Result<Option<()>, DriverError> {
        let Stmt::Session(session_stmt) = stmt else {
            return Ok(None);
        };
        match &**session_stmt {
            SessionStmt::Set(set) => {
                for assignment in &set.assignments {
                    self.apply_assignment(assignment)?;
                }
                Ok(Some(()))
            }
            // `SET PASSWORD` shares the `SET` keyword and the front end's
            // OK-packet reply, but writes `mysql.user`, not a variable.
            SessionStmt::SetPassword(set_password) => {
                self.set_password_stmt(set_password)?;
                Ok(Some(()))
            }
            SessionStmt::SetCharset {
                charset,
                collation,
                assignments,
                ..
            } => {
                self.apply_charset(charset.as_deref(), collation.as_deref())?;
                for assignment in assignments {
                    self.apply_assignment(assignment)?;
                }
                Ok(Some(()))
            }
            SessionStmt::SetMixed(items) => {
                for item in items {
                    match item {
                        tidb_ast::SetItem::System(assignment) => {
                            self.apply_assignment(assignment)?;
                        }
                        tidb_ast::SetItem::Charset {
                            charset, collation, ..
                        } => self.apply_charset(charset.as_deref(), collation.as_deref())?,
                    }
                }
                Ok(Some(()))
            }
            SessionStmt::SetUserVar(set) => {
                for assignment in &set.assignments {
                    let value = self.eval_value(&assignment.value)?;
                    let key = assignment.name.to_ascii_lowercase();
                    // Go's `SET @x = NULL` CLEARS the variable
                    // (`UnsetUserVar`), which is the opposite of the inline
                    // `@x := NULL` assignment expression -- that one leaves
                    // the existing value alone.
                    if matches!(value, Datum::Null) {
                        self.user_vars.borrow_mut().remove(&key);
                    } else {
                        self.user_vars.borrow_mut().insert(key, value);
                    }
                }
                Ok(Some(()))
            }
            _ => Ok(None),
        }
    }

    /// One `name = value` assignment.
    ///
    /// `GLOBAL` writes the shared table every session of this factory reads
    /// (see [`vars::GlobalSysvars`]), gated on Go's `ErrSpecificAccessDenied`
    /// (1227): SUPER or the dynamic `SYSTEM_VARIABLES_ADMIN` privilege.
    /// `SESSION`/`INSTANCE`/unqualified write this session's own copy, as
    /// today. Both directions reject a scope the variable does not have
    /// (1228/1229), matching Go's `validateScope`.
    fn apply_assignment(
        &mut self,
        assignment: &tidb_ast::SystemVariableAssignment,
    ) -> Result<(), DriverError> {
        let is_global = assignment.scope == tidb_ast::SystemVariableScope::Global;
        if is_global {
            self.require_set_global_privilege()?;
        }
        let value = match &assignment.value {
            // Go restores a variable to its registry default by clearing the
            // session (or global) override.
            tidb_ast::SetVariableValue::Default => {
                if is_global {
                    self.vars
                        .reset_global(&assignment.name)
                        .map_err(var_error)?;
                } else {
                    self.vars
                        .reset_system(&assignment.name)
                        .map_err(var_error)?;
                    // Go resolves DEFAULT to the registry's default STRING and
                    // then calls `SetSession` with it, so `SET rand_seed1 =
                    // DEFAULT` really does push 0 into the generator rather
                    // than leaving the seed where the last `SET` put it
                    // (captured: after `SET rand_seed1 = 19`, two DEFAULTs make
                    // the next `RAND()` exactly 0).
                    self.seed_rand_from_sysvar(&assignment.name)?;
                }
                return Ok(());
            }
            tidb_ast::SetVariableValue::Expr(expr) => self.eval_literal(expr)?,
        };
        // Go stores every system variable as a string.
        let value = value.unwrap_or_default();
        self.check_read_only_noop(&assignment.name, &value, is_global)?;
        self.warn_removed_feature_var(&assignment.name, &value);
        if is_global {
            return self
                .vars
                .set_global(&assignment.name, value)
                .map_err(var_error);
        }
        let was_autocommit = self.is_autocommit();
        self.vars
            .set_system(&assignment.name, value)
            .map_err(var_error)?;
        self.seed_rand_from_sysvar(&assignment.name)?;
        // Go `sysvar.go`'s `AutoCommit.SetSession`: turning autocommit back
        // ON ends the ongoing transaction ("Implicitly commit the possible
        // ongoing transaction if mode is changed from off to on"). Only the
        // TRANSITION does it -- `SET autocommit = 1` while it is already on
        // leaves an explicit `BEGIN` running, which is why
        // `BEGIN; INSERT; SET autocommit = 1; ROLLBACK` still rolls back
        // (captured).
        if assignment.name.eq_ignore_ascii_case("autocommit")
            && !was_autocommit
            && self.is_autocommit()
        {
            self.commit()?;
        }
        Ok(())
    }

    /// Go's `tidb_enable_fast_analyze` `Validation` closure (`sysvar.go`): the
    /// feature is gone, so turning the switch ON is ACCEPTED and warned about
    /// rather than refused, and turning it OFF says nothing.
    ///
    /// The switch is `ScopeGlobal|ScopeSession` and `Validation` runs for both,
    /// so `SET GLOBAL` warns the same way `SET SESSION` does. Captured through
    /// `gorun`:
    ///
    /// ```text
    /// set @@session.tidb_enable_fast_analyze=1; show warnings;
    ///   Warning|1105|the fast analyze feature has already been removed in TiDB v7.5.0, so this will have no effect
    /// set @@session.tidb_enable_fast_analyze=0; show warnings;  -- empty
    /// set global tidb_enable_fast_analyze=1;    show warnings;
    ///   Warning|1105|the fast analyze feature has already been removed in TiDB v7.5.0, so this will have no effect
    /// ```
    ///
    /// The message is built with `errors.NewNoStackError`, so it carries no
    /// code of its own and files under `ER_UNKNOWN_ERROR` (1105), the same way
    /// [`crate::warnings::CHECK_CONSTRAINT_IS_OFF_CODE`] does.
    ///
    /// Go tests the NORMALIZED value with `TiDBOptOn`, so this normalizes the
    /// typed text through the registry first: `1`, `on` and `ON` all warn,
    /// while a value the type check would reject falls through to the real
    /// rejection below.
    /// The `Validation` closures that WARN. The value half of each closure
    /// lives in [`sysvar::SysVarDef::validate_in_scope`], which has no session
    /// to append to; this is the half that does. Go runs `Validation` before
    /// storing and for BOTH scopes, so this runs on the same footing.
    fn warn_removed_feature_var(&mut self, name: &str, value: &str) {
        // Go tests the NORMALIZED value, so the typed text goes through the
        // registry first: `1`, `on` and `ON` are one case. A value the type
        // check would reject warns about nothing and falls through to the real
        // rejection below.
        let normalized = sysvar::get_sys_var(name)
            .and_then(|def| def.normalize_by_type(value, sysvar::SCOPE_SESSION).ok())
            .map(|validated| validated.value);
        let Some(normalized) = normalized else {
            return;
        };
        // Each message is built with `errors.NewNoStackError` or a deprecation
        // error, so the code is the one the error itself carries; captured
        // through `gorun` with `SHOW WARNINGS` after each `SET`.
        let warning = if name.eq_ignore_ascii_case("tidb_enable_fast_analyze") {
            // The removed feature warns only when TURNED ON.
            (normalized == "ON").then_some((
                1105,
                "the fast analyze feature has already been removed in TiDB v7.5.0, so this will \
                 have no effect",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_table_partition") {
            // Always on: warns only when someone tries to turn it OFF, and the
            // value stored is `ON` regardless (see the validation).
            (normalized == "OFF").then_some((
                1105,
                "tidb_enable_table_partition is always turned on. This variable has been \
                 deprecated and will be removed in the future releases",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_list_partition") {
            // Go `ErrWarnDeprecatedSyntaxSimpleMsg` (1681), appended for EVERY
            // assignment -- including the one the same closure then refuses,
            // which is why `SHOW WARNINGS` after `set ... = off` reports the
            // deprecation warning AND the error.
            Some((
                1681,
                "tidb_enable_list_partition is deprecated and will be removed in a future \
                 release.",
            ))
        } else {
            None
        };
        let Some((code, message)) = warning else {
            return;
        };
        self.warnings.push(crate::warnings::SqlWarning {
            level: crate::warnings::WarningLevel::Warning,
            code,
            message: message.to_owned(),
        });
    }

    /// Go's `rand_seed1`/`rand_seed2` `SetSession` hooks: the value SET is a
    /// raw seed for this session's `RAND()` generator, and is NOT retained as
    /// the variable's value.
    ///
    /// Both sysvars answer `GetSession` with the constant `"0"` in Go, so
    /// `@@rand_seed1`, `@@session.rand_seed1` and `SHOW VARIABLES LIKE
    /// 'rand_seed1'` all report 0 no matter what was set or what the generator
    /// has advanced to (captured on all three surfaces). Clearing the session
    /// override here reproduces that everywhere at once -- the variable table
    /// answers its own default -- instead of teaching each read path to special
    /// case these two names. Only `GetStateValue`, which serializes session
    /// state, ever exposes the live seeds, and this tier has no such surface.
    ///
    /// The value read back is the one `set_system` already NORMALIZED, so Go's
    /// clamping travels with it: `2147483648` arrives as `MaxInt32` and a
    /// negative arrives as 0, which is also what `tidbOptPositiveInt32` would
    /// have produced.
    fn seed_rand_from_sysvar(&mut self, name: &str) -> Result<(), DriverError> {
        let first = name.eq_ignore_ascii_case("rand_seed1");
        if !first && !name.eq_ignore_ascii_case("rand_seed2") {
            return Ok(());
        }
        let seed = self
            .vars
            .get_system(name)
            .map_err(var_error)?
            .parse::<u32>()
            .unwrap_or(0);
        let mut rand = self.rand.borrow_mut();
        if first {
            rand.set_seed1(seed);
        } else {
            rand.set_seed2(seed);
        }
        drop(rand);
        self.vars.reset_system(name).map_err(var_error)
    }

    /// `SET NAMES` / `SET CHARACTER SET`.
    fn apply_charset(
        &mut self,
        charset: Option<&str>,
        collation: Option<&str>,
    ) -> Result<(), DriverError> {
        // `DEFAULT` restores the registry default, which is what the charset
        // variables already hold when nothing has overridden them.
        let charset = charset.unwrap_or("utf8mb4");
        self.vars.set_names(charset, collation).map_err(var_error)
    }

    /// Evaluates a `SET` right-hand side. Go runs it through the expression
    /// evaluator; this evaluates it as a constant expression, which covers the
    /// literals and simple arithmetic a `SET` carries.
    fn eval_literal(&mut self, expr: &tidb_ast::Expr) -> Result<Option<String>, DriverError> {
        Ok(datum_text(&self.eval_value(expr)?))
    }

    /// Evaluates a `SET` right-hand side to its TYPED value, which is what a
    /// user variable stores (Go's `SetUserVarVal` takes a `types.Datum`). A
    /// system variable keeps only the text, so [`Self::eval_literal`] is this
    /// plus `datum_text`.
    ///
    /// The expression may itself reference variables (`SET @z = @x + 1`), so
    /// they are bound to their values first -- the same substitution a
    /// user-facing query gets, for the same reason: the rewriter behind
    /// `run_select_on` knows literals and columns, not session state.
    fn eval_value(&mut self, expr: &tidb_ast::Expr) -> Result<Datum, DriverError> {
        // An unquoted identifier is a bare word value such as `SET sql_mode =
        // ANSI_QUOTES` or `SET autocommit = ON`, which MySQL takes literally
        // (`SET @x = ANSI_QUOTES` stores the string too, confirmed via
        // `gorun`).
        if let tidb_ast::Expr::Column(path) = expr {
            if let [word] = path.as_slice() {
                return Ok(Datum::new_string(word.clone()));
            }
        }
        let bound = self.bind_variables_in(expr)?;
        let sql = format!("SELECT {}", bound.restore());
        let ctx = self.statement_context(false);
        let rows =
            self.with_catalog_mut(|catalog| tidb_executor::run_select_on(&sql, catalog, &ctx))?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(Datum::Null))
    }

    /// Replaces every variable reference in `sql` with the session's value,
    /// so the driver plans against ordinary literals.
    ///
    /// Go resolves `@@x` and `@x` in the expression rewriter using the
    /// session's variables; the values live in the session here, so the
    /// substitution happens here too. An unknown `@@x` is Go's 1193, while an
    /// unset `@x` is NULL rather than an error, as in MySQL.
    pub(crate) fn bind_variables(&self, stmt: &mut Stmt) -> Result<(), DriverError> {
        let Stmt::Query(query) = stmt else {
            return Ok(());
        };
        let tidb_ast::QueryStmt::Select(select) = &mut **query else {
            return Ok(());
        };
        for field in select.fields.fields_mut() {
            if let tidb_ast::SelectField::Expr { expr, .. } = field {
                *expr = self.bind_variables_in(expr)?;
            }
        }
        if let Some(where_clause) = &select.where_clause {
            select.where_clause = Some(self.bind_variables_in(where_clause)?);
        }
        if let Some(having) = &select.having {
            select.having = Some(self.bind_variables_in(having)?);
        }
        for item in &mut select.order_by {
            item.expr = self.bind_variables_in(&item.expr)?;
        }
        for item in &mut select.group_by {
            item.expr = self.bind_variables_in(&item.expr)?;
        }
        Ok(())
    }

    /// Substitutes variable references inside one expression.
    fn bind_variables_in(&self, expr: &tidb_ast::Expr) -> Result<tidb_ast::Expr, DriverError> {
        use tidb_ast::Expr;
        Ok(match expr {
            Expr::SysVar { scope, name } => {
                // `@@last_insert_id` and its `@@identity` alias are the SAME
                // value `LAST_INSERT_ID()` reports -- Go's
                // `StmtCtx.PrevLastInsertID` -- not an entry in the variable
                // table, which is why they are answered from the session's
                // publication rather than from `get_system`. `@@global.` on
                // either is still the variable table's error (captured).
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && (name.eq_ignore_ascii_case("last_insert_id")
                        || name.eq_ignore_ascii_case("identity"))
                {
                    return Ok(Expr::Int(self.last_insert_id.to_string()));
                }
                // `@@global.x` reads the shared table live; every other
                // scope (unqualified, `@@session.x`, `@@instance.x`) reads
                // this session's own copy.
                let result = if *scope == Some(tidb_ast::SysVarScope::Global) {
                    self.vars.get_global(name)
                } else {
                    self.vars.get_system(name)
                };
                match result {
                    Ok(value) => sysvar_native_expr(name, value),
                    Err(error) => return Err(var_error(error)),
                }
            }
            // A user variable's VALUE is not substituted -- it becomes a
            // `getvar_<kind>` call the evaluator resolves against the
            // session's own map, which is the only way `SELECT @last := v,
            // @last FROM t` can see the assignment made for the CURRENT row.
            // What IS decided here is the kind, from the value the session
            // holds now: Go's `BuildGetVarFunction` picks its typed signature
            // the same way, at build time.
            Expr::UserVar(name) => uservar_read_expr(
                name,
                self.user_vars.borrow().get(&name.to_ascii_lowercase()),
            ),
            // The assignment expression keeps its own shape (the rewriter
            // types it from the value), but its value may itself read
            // variables.
            Expr::Assign { name, value } => Expr::Assign {
                name: name.clone(),
                value: Box::new(self.bind_variables_in(value)?),
            },
            Expr::Paren(inner) => Expr::Paren(Box::new(self.bind_variables_in(inner)?)),
            Expr::Unary(op, inner) => Expr::Unary(*op, Box::new(self.bind_variables_in(inner)?)),
            Expr::Binary(op, lhs, rhs) => Expr::Binary(
                *op,
                Box::new(self.bind_variables_in(lhs)?),
                Box::new(self.bind_variables_in(rhs)?),
            ),
            Expr::Is { expr, target, not } => Expr::Is {
                expr: Box::new(self.bind_variables_in(expr)?),
                target: *target,
                not: *not,
            },
            Expr::In { expr, list, not } => Expr::In {
                expr: Box::new(self.bind_variables_in(expr)?),
                list: list
                    .iter()
                    .map(|item| self.bind_variables_in(item))
                    .collect::<Result<_, _>>()?,
                not: *not,
            },
            other => other.clone(),
        })
    }

    /// Go `hint.go`'s `set_var` arm plus `optimize.go`'s application of
    /// `StmtHints.SetVars`: each `SET_VAR(name = value)` writes the session
    /// variable for the duration of THIS statement only, and where the same
    /// name appears twice the FIRST occurrence wins.
    ///
    /// The snapshot goes on [`Session::set_var_hint_restore`], which
    /// [`Session::run_with_columns`] puts back once the statement is over --
    /// so a statement that FAILS restores the overlay too, as Go's does.
    ///
    /// DEFERRED (documented): Go's two hint warnings. An unknown name is
    /// `ErrUnresolvedHintName` and a name whose registry entry is not
    /// `IsHintUpdatableVerified` is `ErrNotHintUpdatable` -- the second needs a
    /// registry field this tier's generated table does not carry. A name this
    /// registry rejects is skipped, which is the outcome Go reaches for an
    /// unknown name.
    pub(crate) fn apply_set_var_hints(&mut self, stmt: &Stmt) {
        let Stmt::Query(query) = stmt else { return };
        // Go attaches a statement's hints to its first SELECT, so a set
        // operation's hints are the first term's.
        let hints = match &**query {
            tidb_ast::QueryStmt::Select(select) => &select.hints,
            tidb_ast::QueryStmt::SetOpr(set_opr) => match set_opr.terms.first() {
                Some(term) => match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => &select.hints,
                    _ => return,
                },
                None => return,
            },
        };
        for hint in hints {
            let tidb_ast::HintKind::SetVar { var_name, value } = &hint.kind else {
                continue;
            };
            let name = var_name.to_ascii_lowercase();
            // The first hint for a name wins; a later one is ignored.
            if self
                .set_var_hint_restore
                .iter()
                .any(|(restored, _)| *restored == name)
            {
                continue;
            }
            let snapshot = self.vars.snapshot_system(&name);
            if self.vars.set_system(&name, value.clone()).is_ok() {
                self.set_var_hint_restore.extend(snapshot);
            }
        }
    }

    /// Go `preprocess.go:TryAddExtraLimit`: while `sql_select_limit` is not
    /// at its `MaxUint64` default, a SELECT or set operation that writes no
    /// LIMIT of its own is given one, so the variable caps the result the same
    /// way an explicit `LIMIT n` would. A statement that DOES write a LIMIT is
    /// left alone, even one asking for more rows than the cap.
    ///
    /// DEFERRED (documented): Go's `ShowStmt` arm, gated on `NeedLimitRSRow()`
    /// -- the subset of SHOW forms whose rows a LIMIT may cut -- and its
    /// `ExplainStmt` arm, which caps the wrapped statement rather than the
    /// EXPLAIN. `SELECT ... INTO OUTFILE` is excluded exactly as Go excludes
    /// it, even though this tier refuses that clause anyway.
    pub(crate) fn try_add_extra_limit(&self, stmt: &mut Stmt) {
        let cap = match self.vars.get_system("sql_select_limit") {
            Ok(value) => match value.parse::<u64>() {
                Ok(cap) if cap != u64::MAX => cap,
                _ => return,
            },
            Err(_) => return,
        };
        let limit = tidb_ast::Limit {
            offset: None,
            count: tidb_ast::Expr::Int(cap.to_string()),
        };
        if let Stmt::Query(query) = stmt {
            match &mut **query {
                tidb_ast::QueryStmt::Select(select) => {
                    if select.limit.is_none() && select.into_outfile.is_none() {
                        select.limit = Some(limit);
                    }
                }
                tidb_ast::QueryStmt::SetOpr(set_opr) => {
                    if set_opr.limit.is_none() {
                        set_opr.limit = Some(limit);
                    }
                }
            }
        }
    }
}
