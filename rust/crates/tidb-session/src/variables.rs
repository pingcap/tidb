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

use tidb_ast::{SessionStmt, Stmt, Visitable, Visitor};
use tidb_datatype::Datum;
use tidb_executor::DriverError;

use crate::vars::validation_var_error;
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

/// One complete mutable AST pass for variable substitution.
///
/// The AST owns the child graph, including functions, CASE, casts, windows,
/// subqueries and set-operation terms. Keeping that traversal generated in
/// `tidb-ast` prevents a new expression variant from silently bypassing the
/// session's scope and visibility checks.
struct VariableBinder<'a> {
    session: &'a Session,
    error: Option<DriverError>,
}

impl Visitor for VariableBinder<'_> {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        if self.error.is_some() {
            return true;
        }
        let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
            return false;
        };
        if !matches!(
            expr,
            tidb_ast::Expr::SysVar { .. } | tidb_ast::Expr::UserVar(_)
        ) {
            return false;
        }
        match self.session.bind_variable_atom(expr) {
            Ok(bound) => *expr = bound,
            Err(error) => self.error = Some(error),
        }
        true
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.error.is_none()
    }
}

/// Maps a variable error onto the driver error the wire layer renders.
pub(crate) fn var_error(error: VarError) -> DriverError {
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
        VarError::IncorrectScope(name, allowed) => {
            tidb_executor::VarErrorKind::IncorrectScope(name, allowed.to_owned())
        }
        VarError::ValidationRefused(message) => {
            tidb_executor::VarErrorKind::ValidationRefused(message)
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
    pub fn apply_set_stmt(&mut self, stmt: &Stmt) -> Result<Option<()>, DriverError> {
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
        // An explicit `SET INSTANCE` is Go's `v.IsInstance`; anything else
        // unqualified/SESSION reaches the tier only through the legacy
        // rewrite, which warns.
        let is_instance = match assignment.scope {
            tidb_ast::SystemVariableScope::Instance => true,
            tidb_ast::SystemVariableScope::Session => {
                self.routes_to_instance_tier(&assignment.name)?
            }
            tidb_ast::SystemVariableScope::Global => false,
        };
        // The GLOBAL and INSTANCE tiers share one node-wide table keyed by
        // scope (`GlobalSysvars::store`), so a DEFAULT or a value written
        // through either lands where the read path looks.
        let is_node_wide = is_global || is_instance;
        let value = match &assignment.value {
            // Go restores a variable to its registry default by clearing the
            // session (or global) override.
            tidb_ast::SetVariableValue::Default => {
                if is_node_wide {
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
        self.check_isolation_level(&assignment.name, &value)?;
        self.check_max_allowed_packet_scope(&assignment.name, &value, is_node_wide)?;
        self.warn_removed_feature_var(&assignment.name, &value);
        if is_node_wide {
            let truncated = if is_global {
                self.vars.set_global(&assignment.name, value.clone())
            } else {
                self.vars.set_instance(&assignment.name, value.clone())
            }
            .map_err(var_error)?;
            if truncated {
                self.warn_truncated_var(&assignment.name, &value);
            }
            return Ok(());
        }
        let was_autocommit = self.is_autocommit();
        let truncated = self
            .vars
            .set_system(&assignment.name, value.clone())
            .map_err(var_error)?;
        if truncated {
            self.warn_truncated_var(&assignment.name, &value);
        }
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

    /// Go `checkIsolationLevel` (`pkg/sessionctx/variable/varsutil.go:116`),
    /// wired to both spellings at `sysvar.go:2100` (`tx_isolation`) and
    /// `:2106` (`transaction_isolation`):
    ///
    /// ```text
    /// if normalizedValue == "SERIALIZABLE" || normalizedValue == "READ-UNCOMMITTED" {
    ///     returnErr := ErrUnsupportedIsolationLevel.FastGenByArgs(normalizedValue)
    ///     if !TiDBOptOn(vars.systems[vardef.TiDBSkipIsolationLevelCheck]) {
    ///         return normalizedValue, ErrUnsupportedIsolationLevel.GenWithStackByArgs(normalizedValue)
    ///     }
    ///     vars.StmtCtx.AppendWarning(returnErr)
    /// }
    /// ```
    ///
    /// The escape hatch does not change the value: with the check skipped the
    /// level is STORED and read back, it merely warns. The two accepted
    /// levels (`READ-COMMITTED`, `REPEATABLE-READ`) pass through untouched.
    ///
    /// This lives here rather than in `SysVarDef::run_validation` because
    /// both halves need the session: the skip switch is read from it, and the
    /// warning is appended to it.
    fn check_isolation_level(&mut self, name: &str, value: &str) -> Result<(), DriverError> {
        if !name.eq_ignore_ascii_case("transaction_isolation")
            && !name.eq_ignore_ascii_case("tx_isolation")
        {
            return Ok(());
        }
        // Go tests the NORMALIZED value: the entry is `TypeEnum`, so
        // `serializable` and the ordinal `3` both arrive here as
        // `SERIALIZABLE`.
        let Some(normalized) = sysvar::get_sys_var(name)
            .and_then(|def| def.normalize_by_type(value, sysvar::SCOPE_SESSION).ok())
            .map(|validated| validated.value)
        else {
            return Ok(());
        };
        if normalized != "SERIALIZABLE" && normalized != "READ-UNCOMMITTED" {
            return Ok(());
        }
        let skip = self
            .vars
            .get_system("tidb_skip_isolation_level_check")
            .is_ok_and(|value| value.eq_ignore_ascii_case("ON"));
        if !skip {
            return Err(DriverError::Var(
                tidb_executor::VarErrorKind::UnsupportedIsolationLevel(normalized),
            ));
        }
        self.append_warning(
            crate::warnings::WarningLevel::Warning,
            8048,
            format!(
                "The isolation level '{normalized}' is not supported. Set \
                 tidb_skip_isolation_level_check=1 to skip this error"
            ),
        );
        Ok(())
    }

    /// Go's `max_allowed_packet` `Validation` (`sysvar.go:2193`) refuses a
    /// SESSION write outright:
    ///
    /// ```text
    /// if scope == vardef.ScopeSession {
    ///     err := ErrReadOnly.GenWithStackByArgs("SESSION", vardef.MaxAllowedPacket, "GLOBAL")
    ///     return normalizedValue, err
    /// }
    /// ```
    ///
    /// The variable still HAS session scope -- `SELECT @@max_allowed_packet`
    /// answers from the session copy -- so this is a write-side refusal only,
    /// which is why it cannot be expressed as a scope bit.
    ///
    /// Go guards it with `vars.StmtCtx.StmtType == "Set"`, so only a user
    /// `SET` is refused; the handshake's own seeding is not.
    ///
    /// The 1024-rounding half of the same closure is in
    /// [`sysvar::SysVarDef::run_validation`], where the clamp flag already
    /// travels to the statement as `ErrTruncatedWrongValue` (1292).
    ///
    /// # The refusal comes AFTER the type validation, not before
    ///
    /// `SysVar.Validate` (`pkg/sessionctx/variable/variable.go:219`) is
    /// `validateScope` -> `ValidateFromType` -> `Validation`, and the closure
    /// above is the LAST of the three. `ValidateFromType` has therefore
    /// already clamped the value into `[MinValue, MaxValue]` and appended its
    /// own `ErrTruncatedWrongValue` by the time the read-only refusal is
    /// reached, so the statement reports BOTH. Captured from TiDB:
    ///
    /// ```text
    /// set @@Max_Allowed_Packet=100;
    ///   ERROR 1621 SESSION variable 'max_allowed_packet' is read-only.
    ///              Use SET GLOBAL to assign the value
    ///   Warning 1292 Truncated incorrect max_allowed_packet value: '100'
    /// set @@max_allowed_packet='abc';
    ///   ERROR 1232 Incorrect argument type to variable 'max_allowed_packet'
    /// ```
    ///
    /// The second capture is why the type check runs here rather than being
    /// skipped on the refusal path: a value the TYPE rejects never reaches
    /// the closure at all, so 1232 outranks 1621.
    fn check_max_allowed_packet_scope(
        &mut self,
        name: &str,
        value: &str,
        is_node_wide: bool,
    ) -> Result<(), DriverError> {
        if is_node_wide || !name.eq_ignore_ascii_case("max_allowed_packet") {
            return Ok(());
        }
        if let Some(def) = sysvar::get_sys_var(name) {
            match def.normalize_by_type(value, sysvar::SCOPE_SESSION) {
                Ok(validated) if validated.truncated => self.warn_truncated_var(name, value),
                Ok(_) => {}
                Err(error) => return Err(var_error(validation_var_error(name, value, error))),
            }
        }
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::SessionScopeIsReadOnly("max_allowed_packet".to_owned()),
        ))
    }

    /// Whether a non-GLOBAL assignment writes the INSTANCE tier, appending
    /// Go's deprecation warning when it does so through the legacy route.
    ///
    /// Go `pkg/executor/set.go:152`:
    ///
    /// ```text
    /// if sysVar.HasInstanceScope() && !v.IsGlobal && sessionVars.EnableLegacyInstanceScope {
    ///     v.IsInstance = true
    ///     sessionVars.StmtCtx.AppendWarning(exeerrors.ErrInstanceScope.FastGenByArgs(sysVar.Name))
    /// }
    /// ```
    ///
    /// `v.IsGlobal` is false for an explicit `SET SESSION` as well as for an
    /// unqualified `SET`, so both take this route -- the switch is what the
    /// variable IS, not how the statement spelled the scope. An explicit `SET
    /// INSTANCE` is Go's `v.IsInstance` and warns about nothing.
    ///
    /// With `tidb_enable_legacy_instance_scope = OFF` the route is not taken
    /// and the assignment falls through to `set_system`, whose
    /// `!has_session_scope()` guard is Go's `errGlobalVariable` (1229) -- the
    /// same refusal Go's `validateScope` reaches once the rewrite is skipped.
    ///
    /// The warning goes through [`crate::Session::append_warning`], the one
    /// door that feeds both `SHOW WARNINGS` and the OK packet's
    /// `wire_warning_count`.
    fn routes_to_instance_tier(&mut self, name: &str) -> Result<bool, DriverError> {
        let Some(def) = sysvar::get_sys_var(name) else {
            return Ok(false);
        };
        if !def.has_instance_scope() {
            return Ok(false);
        }
        if !self.legacy_instance_scope_enabled() {
            return Ok(false);
        }
        self.append_warning(
            crate::warnings::WarningLevel::Warning,
            // Go `errno.ErrInstanceScope` = 8142
            // (`pkg/errno/errcode.go:1063`), message at
            // `pkg/errno/errname.go:1058`.
            8142,
            format!(
                "modifying {} will require SET GLOBAL in a future version of TiDB",
                def.name
            ),
        );
        Ok(true)
    }

    /// Go `SessionVars.EnableLegacyInstanceScope`, fed by
    /// `tidb_enable_legacy_instance_scope` (default ON,
    /// `vardef.DefEnableLegacyInstanceScope`).
    fn legacy_instance_scope_enabled(&self) -> bool {
        self.vars
            .get_system("tidb_enable_legacy_instance_scope")
            .is_ok_and(|value| value.eq_ignore_ascii_case("ON"))
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
        self.append_warning(
            crate::warnings::WarningLevel::Warning,
            code,
            message.to_owned(),
        );
    }

    /// Go `ErrTruncatedWrongValue` (1292) for a system variable whose
    /// assignment was clamped rather than refused.
    ///
    /// Every clamping site in Go -- `checkUInt64SystemVar`,
    /// `checkInt64SystemVar`, `checkFloatSystemVar`, `checkDurationSystemVar`
    /// in `variable.go`, and the per-variable `Validation` closures such as
    /// `tidb_session_alias`'s -- reports the SAME pair: the variable's
    /// registry name, and the value as ORIGINALLY assigned, not the clamped
    /// one that got stored. So `set @@group_concat_max_len=1` stores 4 and
    /// names `1` in the warning.
    ///
    /// The name comes from the registry rather than from the statement, since
    /// Go passes `sv.Name`: `SET @@GROUP_CONCAT_MAX_LEN=1` still warns about
    /// `group_concat_max_len`.
    fn warn_truncated_var(&mut self, name: &str, original: &str) {
        let reported = sysvar::get_sys_var(name)
            .map_or_else(|| name.to_ascii_lowercase(), |def| def.name.to_owned());
        self.append_warning(
            crate::warnings::WarningLevel::Warning,
            1292,
            format!("Truncated incorrect {reported} value: '{original}'"),
        );
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
        if first {
            self.rand.set_seed1(seed);
        } else {
            self.rand.set_seed2(seed);
        }
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
        let mut binder = VariableBinder {
            session: self,
            error: None,
        };
        if !query.accept(&mut binder) {
            return Err(binder
                .error
                .expect("variable traversal stops only after recording an error"));
        }
        Ok(())
    }

    /// Substitutes one variable atom. The complete child walk belongs to
    /// [`VariableBinder`], so scope validation cannot depend on expression
    /// shape.
    fn bind_variable_atom(&self, expr: &tidb_ast::Expr) -> Result<tidb_ast::Expr, DriverError> {
        use tidb_ast::Expr;
        Ok(match expr {
            Expr::SysVar { scope, name } => {
                let def = sysvar::get_sys_var(name).ok_or_else(|| {
                    var_error(VarError::UnknownSystemVariable(name.to_ascii_lowercase()))
                })?;
                if def.scope != sysvar::SCOPE_NONE {
                    let incorrect_scope = match scope {
                        Some(tidb_ast::SysVarScope::Global)
                            if !(def.has_global_scope() || def.has_instance_scope()) =>
                        {
                            Some("SESSION")
                        }
                        Some(tidb_ast::SysVarScope::Instance) if !def.has_instance_scope() => {
                            Some("SESSION or GLOBAL")
                        }
                        Some(tidb_ast::SysVarScope::Session) if !def.has_session_scope() => {
                            Some("GLOBAL")
                        }
                        _ => None,
                    };
                    if let Some(allowed) = incorrect_scope {
                        return Err(var_error(VarError::IncorrectScope(
                            name.to_ascii_lowercase(),
                            allowed,
                        )));
                    }
                    if *scope == Some(tidb_ast::SysVarScope::Session)
                        && def.is_internal_session_variable()
                    {
                        return Err(var_error(VarError::UnknownSystemVariable(
                            name.to_ascii_lowercase(),
                        )));
                    }
                }
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
                // `@@last_plan_from_cache` is Go's `PrevFoundInPlanCache`
                // read (`sysvar.go`'s GetSession hook), not a stored value --
                // the variable table's entry is only the type and the
                // read-only flag.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("last_plan_from_cache")
                {
                    return Ok(Expr::Int(
                        i32::from(self.last_plan_from_cache()).to_string(),
                    ));
                }
                // `@@last_plan_from_binding` is Go's `PrevFoundInBinding`
                // read, for the same reason and at the same boundary as
                // `@@last_plan_from_cache` above.
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && name.eq_ignore_ascii_case("last_plan_from_binding")
                {
                    return Ok(Expr::Int(
                        i32::from(self.last_plan_from_binding()).to_string(),
                    ));
                }
                // A no-scope server property always reads its registry value.
                // GLOBAL and INSTANCE both read the node-wide table; only an
                // unqualified or explicit SESSION read uses the session copy.
                let result = match scope {
                    _ if def.scope == sysvar::SCOPE_NONE => Ok(sysvar::effective_default(def)),
                    Some(tidb_ast::SysVarScope::Global | tidb_ast::SysVarScope::Instance) => {
                        self.vars.get_global(name)
                    }
                    _ => self.vars.get_system(name),
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
            _ => unreachable!("VariableBinder only sends variable atoms to this helper"),
        })
    }

    /// Substitutes variables in a standalone expression, such as a `SET`
    /// value, through the same complete visitor a query uses.
    fn bind_variables_in(&self, expr: &tidb_ast::Expr) -> Result<tidb_ast::Expr, DriverError> {
        let mut bound = expr.clone();
        let mut binder = VariableBinder {
            session: self,
            error: None,
        };
        if !bound.accept(&mut binder) {
            return Err(binder
                .error
                .expect("variable traversal stops only after recording an error"));
        }
        Ok(bound)
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
