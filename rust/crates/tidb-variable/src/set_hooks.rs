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


//! Go `pkg/sessionctx/variable/sysvar.go`'s hooks as a `SET` runs them:
//! each variable's `Validation` closure (which may refuse a value or append a
//! warning), the store, and its `SetSession` / `SetGlobal` hook, through
//! [`SessionVars::set_system_var`], [`SessionVars::set_global_sys_var`] and
//! [`SessionVars::set_instance_sys_var`] -- Go's `SessionVars.SetSystemVar`
//! and `GlobalVarsAccessor.SetGlobalSysVar` / `SetInstanceSysVar`. What the
//! hooks append goes to the statement through [`SessionVars::take_stmt_warnings`].
//!
//! The hooks that reach past the variables -- a transaction autocommit
//! ends, the session's random generator, the domain's global config and
//! workload repository -- stay with the session, as Go reaches them through
//! the session and the domain.

use crate::vars::{validation_var_error, SessionVars};
use crate::VarError;

/// Go's warning levels for what a hook appends (`contextutil.WarnLevel*`).
use tidb_context::WarningLevel;

/// Go `SessionVars.NoopFuncsMode` (`pkg/sessionctx/variable/session.go:1338`),
/// Go's `OffInt` / `OnInt` / `WarnInt`, set by `tidb_enable_noop_functions`
/// (`sysvar.go:2734`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoopFuncsMode {
    /// `OFF` (the default): a noop-only clause is refused.
    Off,
    /// `ON`: the clause is accepted and does nothing.
    On,
    /// `WARN`: the clause is accepted with a warning.
    Warn,
}

impl SessionVars {
    /// Go `StmtCtx.AppendWarning(proto.GenWithStackByArgs(...))` for a
    /// sysvar hook.
    pub fn append_stmt_warning(
        &mut self,
        level: WarningLevel,
        proto: &tidb_terror::TerrorError,
        message: String,
    ) {
        self.stmt_warnings
            .push(tidb_context::SqlWarn::of(level, proto, message));
    }

    /// Go `StmtCtx.AppendWarning(errors.New(message))` for a sysvar hook.
    pub fn append_stmt_plain_warning(&mut self, level: WarningLevel, message: String) {
        self.stmt_warnings.push(tidb_context::SqlWarn::new(
            level,
            tidb_context::WarnErr::Message(message),
        ));
    }

    /// The rows the hooks appended since the last call.
    pub fn take_stmt_warnings(&mut self) -> Vec<tidb_context::SqlWarn> {
        std::mem::take(&mut self.stmt_warnings)
    }

    /// Go `SessionVars.SetSystemVar` as a `SET` of a SESSION value runs it.
    ///
    /// # Errors
    ///
    /// The variable's scope, type or `Validation` refusal.
    pub fn set_system_var(&mut self, name: &str, value: String) -> Result<(), VarError> {
        self.check_noop_gated_variable(name, &value, false)?;
        self.check_isolation_level(name, &value)?;
        self.check_max_allowed_packet_scope(name, &value, false)?;
        self.run_set_closures(name, &value, crate::sysvar::SCOPE_SESSION);
        let truncated = self.set_system(name, value.clone())?;
        if truncated {
            self.warn_truncated_var(name, &value);
        }
        self.append_fix_control_parse_warnings(name, &value);
        Ok(())
    }

    /// Go `GlobalVarsAccessor.SetGlobalSysVar` as a `SET GLOBAL` runs it,
    /// up to the store: `Validation` (Go's `require_secure_transport` closure
    /// included) and the GLOBAL table.
    ///
    /// # Errors
    ///
    /// The variable's scope, type or `Validation` refusal.
    pub fn set_global_sys_var(&mut self, name: &str, value: String) -> Result<(), VarError> {
        self.set_node_wide_sys_var(name, value, true)
    }

    /// Go `GlobalVarsAccessor.SetInstanceSysVar`.
    ///
    /// # Errors
    ///
    /// The variable's scope, type or `Validation` refusal.
    pub fn set_instance_sys_var(&mut self, name: &str, value: String) -> Result<(), VarError> {
        self.set_node_wide_sys_var(name, value, false)
    }

    fn set_node_wide_sys_var(
        &mut self,
        name: &str,
        value: String,
        is_global: bool,
    ) -> Result<(), VarError> {
        self.check_noop_gated_variable(name, &value, is_global)?;
        self.check_isolation_level(name, &value)?;
        self.run_set_closures(
            name,
            &value,
            if is_global { crate::sysvar::SCOPE_GLOBAL } else { crate::sysvar::SCOPE_INSTANCE },
        );
        // Go's `require_secure_transport` Validation closure runs after Bool
        // normalization and the SET GLOBAL privilege check, but before
        // SetGlobal mutates the live table. Direct startup/cache loads do not
        // run this SQL-only guard.
        if is_global && name.eq_ignore_ascii_case("require_secure_transport") {
            if let Some(definition) = crate::sysvar::get_sys_var(name) {
                let normalized = definition
                    .validate_in_scope(&value, crate::sysvar::SCOPE_GLOBAL)
                    .map_err(|error| validation_var_error(name, &value, error))?;
                if normalized.value == "ON" && self.tls_connection_state.is_none() {
                    return Err(tidb_terror::MysqlError::unknown("require_secure_transport can only be set to ON if the connection issuing the change is secure"
                            .to_owned()));
                }
            }
        }
        // Go TiDBSchemaCacheSize.SetGlobal persists and resizes before
        // publishing the vardef value; INSTANCE invokes the same callback.
        let schema_cache_size = name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE);
        if schema_cache_size {
            let scope = if is_global { crate::sysvar::SCOPE_GLOBAL } else { crate::sysvar::SCOPE_INSTANCE };
            let normalized = crate::sysvar::get_sys_var(name).unwrap().validate_in_scope(&value, scope)
                .map_err(|error| validation_var_error(name, &value, error))?.value;
            let bytes = crate::varsutil::parse_byte_size(&normalized).map(|(bytes, _)| bytes)
                .or_else(|| normalized.parse::<u64>().ok());
            if bytes != Some(tidb_vardef::SCHEMA_CACHE_SIZE.load(std::sync::atomic::Ordering::Acquire)) {
                if let Some(globals) = self.globals.clone() { globals.run_set_global_hook(name, &normalized)?; }
            }
        }
        let truncated = if is_global {
            self.set_global(name, value.clone())
        } else {
            self.set_instance(name, value.clone())
        }?;
        if truncated {
            self.warn_truncated_var(name, &value);
        }
        self.append_fix_control_parse_warnings(name, &value);
        if is_global {
            // `SetGlobal` hooks consume the stored, type-normalized value:
            // the original `-1`/`7201` text would make a hook reject an
            // assignment the variable layer already accepted and clamped.
            if let Some(globals) = self.globals.clone() {
                let stored = self.get_global(name)?;
                if !schema_cache_size { globals.run_set_global_hook(name, &stored)?; }
                if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_REDACT_LOG) {
                    self.set_enable_redact_log(stored);
                }
            }
        }
        Ok(())
    }

    /// Go `SessionVars.NoopFuncsMode`, read from this session's copy of
    /// `tidb_enable_noop_functions` or -- for a `SET GLOBAL` being validated
    /// -- from the shared table, which is the scope Go's `checkReadOnly`
    /// consults through `GlobalVarsAccessor`.
    #[must_use]
    pub fn noop_funcs_mode(&self, global: bool) -> NoopFuncsMode {
        let value = if global {
            self.get_global("tidb_enable_noop_functions")
        } else {
            self.get_system("tidb_enable_noop_functions")
        };
        match value
            .unwrap_or_else(|_| "OFF".to_owned())
            .to_ascii_uppercase()
            .as_str()
        {
            "ON" | "1" => NoopFuncsMode::On,
            "WARN" => NoopFuncsMode::Warn,
            _ => NoopFuncsMode::Off,
        }
    }

    /// Go `varsutil.go:checkReadOnly` and the identical branch inlined in
    /// `sql_auto_is_null`'s own registration: turning one of these ON is
    /// refused with 1235 unless `tidb_enable_noop_functions` allows it,
    /// because the server does not actually do what the setting names.
    /// Turning one OFF is always accepted, and so is a value the registry
    /// would reject -- that is the store's job, and Go likewise validates the
    /// type before it runs this hook.
    fn check_noop_gated_variable(
        &mut self,
        name: &str,
        value: &str,
        is_global: bool,
    ) -> Result<(), VarError> {
        let Some(clause) = crate::sysvar::noop_gated_clause(name) else {
            return Ok(());
        };
        let turns_on = crate::sysvar::get_sys_var(name)
            .and_then(|def| def.validate(value).ok())
            .is_some_and(|validated| validated.value == "ON");
        if !turns_on {
            return Ok(());
        }
        let message = format!(
            "function {clause} has only noop implementation in tidb now, use \
             tidb_enable_noop_functions to enable these functions"
        );
        match self.noop_funcs_mode(is_global) {
            NoopFuncsMode::On => Ok(()),
            NoopFuncsMode::Off => {
                // Go appends the statement error into the warning buffer as
                // well, so SHOW WARNINGS after the refused statement carries
                // the same 1235 row.
                self.append_stmt_warning(
                    WarningLevel::Error,
                    &crate::variable_errors::ERR_FUNCTIONS_NOOP_IMPL,
                    message,
                );
                Err(tidb_terror::MysqlError::from_terror(
                    &crate::variable_errors::ERR_FUNCTIONS_NOOP_IMPL
                        .gen_with_stack_by_args(&[(clause).into()]),
                ))
            }
            NoopFuncsMode::Warn => {
                self.append_stmt_warning(WarningLevel::Warning, &crate::variable_errors::ERR_FUNCTIONS_NOOP_IMPL, message);
                Ok(())
            }
        }
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
    fn check_isolation_level(&mut self, name: &str, value: &str) -> Result<(), VarError> {
        if !name.eq_ignore_ascii_case("transaction_isolation")
            && !name.eq_ignore_ascii_case("tx_isolation")
        {
            return Ok(());
        }
        // Go tests the NORMALIZED value: the entry is `TypeEnum`, so
        // `serializable` and the ordinal `3` both arrive here as
        // `SERIALIZABLE`.
        let Some(normalized) = crate::sysvar::get_sys_var(name)
            .and_then(|def| def.normalize_by_type(value, crate::sysvar::SCOPE_SESSION).ok())
            .map(|validated| validated.value)
        else {
            return Ok(());
        };
        if normalized != "SERIALIZABLE" && normalized != "READ-UNCOMMITTED" {
            return Ok(());
        }
        let skip = self
            .get_system("tidb_skip_isolation_level_check")
            .is_ok_and(|value| value.eq_ignore_ascii_case("ON"));
        if !skip {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNSUPPORTED_ISOLATION_LEVEL
                    .gen_with_stack_by_args(&[(normalized).into()]),
            ));
        }
        self.append_stmt_warning(
            WarningLevel::Warning,
            &crate::variable_errors::ERR_UNSUPPORTED_ISOLATION_LEVEL,
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
    /// [`crate::sysvar::SysVarDef::run_validation`], where the clamp flag already
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
    ) -> Result<(), VarError> {
        if is_node_wide || !name.eq_ignore_ascii_case("max_allowed_packet") {
            return Ok(());
        }
        if let Some(def) = crate::sysvar::get_sys_var(name) {
            match def.normalize_by_type(value, crate::sysvar::SCOPE_SESSION) {
                Ok(validated) if validated.truncated => self.warn_truncated_var(name, value),
                Ok(_) => {}
                Err(error) => return Err(validation_var_error(name, value, error)),
            }
        }
        Err(tidb_terror::MysqlError::from_terror(
            &crate::variable_errors::ERR_READ_ONLY.gen_with_stack_by_args(&[
                "SESSION".into(),
                ("max_allowed_packet".to_owned()).into(),
                "GLOBAL".into(),
            ]),
        ))
    }





    /// Go `SysVar.Validate` (the type check, then the variable's
    /// `Validation`) and the scope's `SetSession` / `SetGlobal` hook, as
    /// `SetSystemVar` / `SetGlobalSysVar` run them before the value is
    /// stored: the warnings they append go to the statement, those of a
    /// `Validation` that then refuses the value included. The refusal itself
    /// is reported by the store that follows.
    fn run_set_closures(&mut self, name: &str, value: &str, scope: u8) {
        let Some(definition) = crate::sysvar::get_sys_var(name) else {
            return;
        };
        let cx = SetVarsContext {
            vars: self,
            scope,
            warnings: std::cell::RefCell::default(),
        };
        if let Ok(validated) = definition.validate_in_scope_with_lookup(value, scope, Some(&cx)) {
            let hook = match scope {
                crate::sysvar::SCOPE_SESSION => definition.set_session,
                crate::sysvar::SCOPE_GLOBAL => definition.set_global,
                _ => None,
            };
            if let Some(hook) = hook {
                hook(&cx, &validated.value);
            }
        }
        for warning in cx.warnings.into_inner() {
            match warning.error {
                Some(proto) => self.append_stmt_warning(WarningLevel::Warning, proto, warning.message),
                None => self.append_stmt_plain_warning(WarningLevel::Warning, warning.message),
            }
        }
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
        let reported = crate::sysvar::get_sys_var(name)
            .map_or_else(|| name.to_ascii_lowercase(), |def| def.name.to_owned());
        self.append_stmt_warning(
            WarningLevel::Warning,
            &crate::variable_errors::ERR_TRUNCATED_WRONG_VALUE,
            format!("Truncated incorrect {reported} value: '{original}'"),
        );
    }

    /// Publishes duplicate-key diagnostics after the scope-aware writer has
    /// validated and atomically stored the value. A wrong scope or malformed
    /// value therefore cannot leak a fix-control warning ahead of its error.
    fn append_fix_control_parse_warnings(&mut self, name: &str, value: &str) {
        if !name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL) {
            return;
        }
        let (_, warnings) = tidb_fixcontrol::OptimizerFixControl::parse(value)
            .expect("the system/global writer validated fix-control before publication");
        for warning in warnings {
            self.append_stmt_plain_warning(WarningLevel::Warning, warning);
        }
    }
}

/// The session as a variable's closures see it while one `SET` runs.
struct SetVarsContext<'a> {
    vars: &'a SessionVars,
    scope: u8,
    warnings: std::cell::RefCell<Vec<crate::sysvar::VarWarning>>,
}

impl crate::sysvar::VarsContext for SetVarsContext<'_> {
    fn read(&self, name: &str) -> Option<String> {
        if self.scope == crate::sysvar::SCOPE_SESSION {
            self.vars.get_system(name).ok()
        } else {
            self.vars.get_global(name).ok()
        }
    }
    fn append_warning(&self, warning: crate::sysvar::VarWarning) {
        self.warnings.borrow_mut().push(warning);
    }
}
