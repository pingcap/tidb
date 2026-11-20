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
        self.run_set_closures(name, &value, crate::sysvar::SCOPE_SESSION)?;
        let truncated = self.set_system(name, value.clone())?;
        if truncated {
            self.warn_truncated_var(name, &value);
        }
        self.append_fix_control_parse_warnings(name, &value);
        Ok(())
    }

    /// Go `SessionVars.SetSystemVarWithOldStateAsRet`: [`Self::set_system_var`]
    /// answering the value the variable read before (Go's
    /// `GetSessionOrGlobalSystemVar`).
    ///
    /// # Errors
    ///
    /// The variable is unknown, or [`Self::set_system_var`] refuses the value.
    pub fn set_system_var_with_old_state_as_ret(&mut self, name: &str, value: String) -> Result<String, VarError> {
        let old = self.get_system(name)?;
        self.set_system_var(name, value)?;
        Ok(old)
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
        self.run_set_closures(
            name,
            &value,
            if is_global { crate::sysvar::SCOPE_GLOBAL } else { crate::sysvar::SCOPE_INSTANCE },
        )?;
        let truncated = if is_global {
            self.set_global(name, value.clone())
        } else {
            self.set_instance(name, value.clone())
        }?;
        if truncated {
            self.warn_truncated_var(name, &value);
        }
        self.append_fix_control_parse_warnings(name, &value);
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









    /// Go `SysVar.Validate` (the type check, then the variable's
    /// `Validation`), and for `SET GLOBAL` the variable's `SetGlobal`, as
    /// `SetGlobalSysVar` runs them before the value is stored: the warnings
    /// go to the statement, those of a `Validation` that then refuses the
    /// value included. The refusal itself is reported by the store that
    /// follows.
    fn run_set_closures(&mut self, name: &str, value: &str, scope: u8) -> Result<(), VarError> {
        let Some(definition) = crate::sysvar::get_sys_var(name) else {
            return Ok(());
        };
        let cx = SetVarsContext {
            vars: self,
            scope,
            warnings: std::cell::RefCell::default(),
        };
        let validated = definition.validate_in_scope_with_lookup(value, scope, Some(&cx));
        for warning in cx.warnings.into_inner() {
            let level = if warning.is_error { WarningLevel::Error } else { WarningLevel::Warning };
            match warning.error {
                Some(proto) => self.append_stmt_warning(level, proto, warning.message),
                None => self.append_stmt_plain_warning(level, warning.message),
            }
        }
        // A closure's own refusal (Go `Validation` returning an error) is the
        // statement's, once Go's `validateScope` would have admitted the
        // scope; a scope or type refusal is reported by the store that
        // follows, which checks them in Go's order.
        if let Err(error @ (crate::sysvar::ValidationError::SqlError(_) | crate::sysvar::ValidationError::Refused(_))) = &validated {
            let in_scope = match scope {
                crate::sysvar::SCOPE_SESSION => definition.has_session_scope(),
                crate::sysvar::SCOPE_GLOBAL => definition.has_global_scope(),
                _ => definition.has_instance_scope(),
            };
            if in_scope && !definition.read_only {
                return Err(validation_var_error(name, value, error.clone()));
            }
        }
        // Go `SetGlobalFromHook`, which `SetGlobalSysVar` and
        // `SetInstanceSysVar` both run on the validated value before it is
        // stored (the session's `SetSession` runs where the value is stored,
        // `SetSessionFromHook`).
        //
        // A variable has one `SetGlobal`: its own, or the one a package
        // outside the registry gives it (Go `RegisterSysVar` from `domain`,
        // `ddl`, ...: the GC, MV-service and MDL switches), which the node
        // registers on the GLOBAL accessor.
        if let (Ok(validated), crate::sysvar::SCOPE_GLOBAL | crate::sysvar::SCOPE_INSTANCE) = (validated, scope) {
            match definition.set_global {
                Some(hook) => hook(self, &validated.value)?,
                None => {
                    if let Some(globals) = self.globals.clone() {
                        globals.run_set_global_hook(definition.name, &validated.value)?;
                    }
                }
            }
        }
        Ok(())
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

impl SetVarsContext<'_> {
    /// Go's closures read `tidb_snapshot` as the typed `SnapshotTS`, which a
    /// failed `SET tidb_snapshot` rolls back while its system string stays
    /// (as the store's own lookup does).
    fn session_value(&self, name: &str) -> Option<String> {
        if name.eq_ignore_ascii_case("tidb_snapshot") {
            Some(self.vars.snapshot_ts().to_string())
        } else {
            self.vars.get_system(name).ok()
        }
    }
}

impl crate::sysvar::VarsContext for SetVarsContext<'_> {
    fn read(&self, name: &str) -> Option<String> {
        if self.scope == crate::sysvar::SCOPE_SESSION {
            self.session_value(name)
        } else {
            self.vars.get_global(name).ok()
        }
    }
    fn session_read(&self, name: &str) -> Option<String> {
        self.session_value(name)
    }
    fn stmt_is_set(&self) -> bool {
        self.vars.stmt_type_is_set
    }
    fn tls_connection_secure(&self) -> bool {
        self.vars.tls_connection_state.is_some()
    }
    fn append_warning(&self, warning: crate::sysvar::VarWarning) {
        self.warnings.borrow_mut().push(warning);
    }
}
