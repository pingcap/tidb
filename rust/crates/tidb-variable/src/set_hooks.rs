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
pub use tidb_context::WarningLevel as VarWarningLevel;

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
        level: VarWarningLevel,
        proto: &tidb_error::terror::TerrorError,
        message: String,
    ) {
        self.stmt_warnings
            .push(tidb_context::SqlWarn::of(level, proto, message));
    }

    /// Go `StmtCtx.AppendWarning(errors.New(message))` for a sysvar hook.
    pub fn append_stmt_plain_warning(&mut self, level: VarWarningLevel, message: String) {
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
        if name.eq_ignore_ascii_case("tidb_partition_prune_mode") {
            self.warn_partition_prune_assignment(&value, false);
        }
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::MPP_EXCHANGE_COMPRESSION_MODE) {
            self.warn_mpp_exchange_compression(&value);
        }
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL) {
            self.warn_parallel_hashagg_spill(&value);
        }
        self.warn_sysvar_assignment(name, &value);
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
        if name.eq_ignore_ascii_case("tidb_partition_prune_mode") {
            self.warn_partition_prune_assignment(&value, is_global);
        }
        if !is_global
            && name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::MPP_EXCHANGE_COMPRESSION_MODE)
        {
            self.warn_mpp_exchange_compression(&value);
        }
        if !is_global
            && name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL)
        {
            self.warn_parallel_hashagg_spill(&value);
        }
        self.warn_sysvar_assignment(name, &value);
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
                    return Err(tidb_error::mysql::MysqlError::unknown("require_secure_transport can only be set to ON if the connection issuing the change is secure"
                            .to_owned()));
                }
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
                globals.run_set_global_hook(name, &stored)?;
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
                    VarWarningLevel::Error,
                    &tidb_error::catalog::variable::ERR_FUNCTIONS_NOOP_IMPL,
                    message,
                );
                Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_error::catalog::variable::ERR_FUNCTIONS_NOOP_IMPL
                        .generate_with_stack_by_args(&[(clause).into()]),
                ))
            }
            NoopFuncsMode::Warn => {
                self.append_stmt_warning(VarWarningLevel::Warning, &tidb_error::catalog::variable::ERR_FUNCTIONS_NOOP_IMPL, message);
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
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_UNSUPPORTED_ISOLATION_LEVEL
                    .generate_with_stack_by_args(&[(normalized).into()]),
            ));
        }
        self.append_stmt_warning(
            VarWarningLevel::Warning,
            &tidb_error::catalog::variable::ERR_UNSUPPORTED_ISOLATION_LEVEL,
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
        Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_error::catalog::variable::ERR_READ_ONLY.generate_with_stack_by_args(&[
                "SESSION".into(),
                ("max_allowed_packet".to_owned()).into(),
                "GLOBAL".into(),
            ]),
        ))
    }

    /// Appends warnings produced by Go system-variable `Validation` closures.
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
    /// the session's `CHECK_CONSTRAINT_IS_OFF_CODE` does.
    ///
    /// Appends Go's partition-prune mode warnings from the variable's
    /// `SetSession`/`SetGlobal` hooks. The closure's normalized value is used
    /// here as well, so out-of-date enum spellings warn as their upgraded mode.
    fn warn_partition_prune_assignment(&mut self, value: &str, is_global: bool) {
        let Some(definition) = crate::sysvar::get_sys_var("tidb_partition_prune_mode") else {
            return;
        };
        let Ok(normalized) = definition.validate_in_scope(value, crate::sysvar::SCOPE_SESSION) else {
            return;
        };
        let previous = if is_global {
            self.get_global("tidb_partition_prune_mode").ok()
        } else {
            self.get_system("tidb_partition_prune_mode").ok()
        };
        if normalized.value == "static" {
            self.append_stmt_warning(
                VarWarningLevel::Warning,
                &tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG,
                "static prune mode is deprecated and will be removed in the future release."
                    .to_owned(),
            );
        } else if normalized.value == "dynamic" {
            let warn_analyze = is_global || previous.as_deref() == Some("static");
            if warn_analyze {
                self.append_stmt_plain_warning(
                    VarWarningLevel::Warning,
                    "Please analyze all partition tables again for consistency between partition and global stats".to_owned(),
                );
            }
            if !is_global && previous.as_deref() == Some("static") {
                self.append_stmt_plain_warning(
                    VarWarningLevel::Warning,
                    "Please avoid setting partition prune mode to dynamic at session level and set partition prune mode to dynamic at global level".to_owned(),
                );
            }
        }
    }

    /// Go's `mpp_exchange_compression_mode` SetSession hook warns when a
    /// concrete compression mode is selected while the effective MPP version
    /// is V0. `UNSPECIFIED` deliberately has no warning, and GLOBAL writes do
    /// not invoke SetSession (the caller filters those out before calling).
    fn warn_mpp_exchange_compression(&mut self, value: &str) {
        let Some(definition) =
            crate::sysvar::get_sys_var(tidb_vardef::tidb_vars::MPP_EXCHANGE_COMPRESSION_MODE)
        else {
            return;
        };
        let Ok(normalized) = definition.validate_in_scope(value, crate::sysvar::SCOPE_SESSION) else {
            return;
        };
        let Some(mode) = tidb_vardef::modes::to_exchange_compression_mode(&normalized.value) else {
            return;
        };
        if mode == tidb_vardef::modes::ExchangeCompressionMode::UNSPECIFIED {
            return;
        }
        let mpp_version = self
            .get_system(tidb_vardef::tidb_vars::MPP_VERSION)
            .ok()
            .and_then(|value| tidb_vardef::modes::to_mpp_version(&value))
            .unwrap_or(tidb_vardef::modes::NEWEST_MPP_VERSION);
        if mpp_version == 0 {
            self.append_stmt_plain_warning(
                VarWarningLevel::Warning,
                "mpp exchange compression won't work under current mpp version 0".to_owned(),
            );
        }
    }

    /// Go's `tidb_enable_parallel_hashagg_spill` SetSession hook emits a
    /// deprecation warning only when the session switch is turned OFF. GLOBAL
    /// writes do not invoke SetSession, so the caller filters those out.
    fn warn_parallel_hashagg_spill(&mut self, value: &str) {
        let Some(definition) =
            crate::sysvar::get_sys_var(tidb_vardef::tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL)
        else {
            return;
        };
        let Ok(normalized) = definition.normalize_by_type(value, crate::sysvar::SCOPE_SESSION) else {
            return;
        };
        if normalized.value == "OFF" {
            self.append_stmt_warning(
                VarWarningLevel::Warning,
                &tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG,
                "tidb_enable_parallel_hashagg_spill will be removed in the future and hash aggregate spill will be enabled by default.".to_owned(),
            );
        }
    }

    /// Go tests the NORMALIZED value with `TiDBOptOn`, so this normalizes the
    /// typed text through the registry first: `1`, `on` and `ON` all warn,
    /// while a value the type check would reject falls through to the real
    /// rejection below.
    /// The `Validation` closures that WARN. The value half of each closure
    /// lives in [`crate::sysvar::SysVarDef::validate_in_scope`], which has no session
    /// to append to; this is the half that does. Go runs `Validation` before
    /// storing and for BOTH scopes, so this runs on the same footing.
    fn warn_sysvar_assignment(&mut self, name: &str, value: &str) {
        // Go tests the NORMALIZED value, so the typed text goes through the
        // registry first: `1`, `on` and `ON` are one case. A value the type
        // check would reject warns about nothing and falls through to the real
        // rejection below.
        let normalized = crate::sysvar::get_sys_var(name)
            .and_then(|def| def.normalize_by_type(value, crate::sysvar::SCOPE_SESSION).ok())
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
                None,
                "the fast analyze feature has already been removed in TiDB v7.5.0, so this will \
                 have no effect",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_table_partition") {
            // Always on: warns only when someone tries to turn it OFF, and the
            // value stored is `ON` regardless (see the validation).
            (normalized == "OFF").then_some((
                None,
                "tidb_enable_table_partition is always turned on. This variable has been \
                 deprecated and will be removed in the future releases",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_list_partition") {
            // Go `ErrWarnDeprecatedSyntaxSimpleMsg` (1681), appended for EVERY
            // assignment -- including the one the same closure then refuses,
            // which is why `SHOW WARNINGS` after `set ... = off` reports the
            // deprecation warning AND the error.
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT),
                "tidb_enable_list_partition is deprecated and will be removed in a future \
                 release.",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_tiflash_pipeline_model") {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT),
                "tidb_enable_tiflash_pipeline_model is deprecated and will be removed in a future release.",
            ))
        } else if name.eq_ignore_ascii_case("tidb_mpp_store_fail_ttl") {
            Some((
                None,
                "tidb_mpp_store_fail_ttl is always 0s. This variable has been deprecated and will be removed in the future releases",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_column_tracking") {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT),
                "The 'tidb_enable_column_tracking' variable is deprecated and will be removed in future versions of TiDB. It is always set to 'ON' now.",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_exchange_partition") && normalized == "OFF"
        {
            Some((
                None,
                "tidb_enable_exchange_partition is always turned on. This variable has been deprecated and will be removed in the future releases",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_new_cost_interface") && normalized == "OFF"
        {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX),
                "'OFF' is deprecated and will be removed in a future release. Please use ON instead",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_tiflash_read_for_write_stmt")
            && normalized == "OFF"
        {
            Some((
                None,
                "tidb_enable_tiflash_read_for_write_stmt is always turned on. This variable has been deprecated and will be removed in the future releases",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_clustered_index")
            && normalized == "INT_ONLY"
        {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX),
                "'INT_ONLY' is deprecated and will be removed in a future release. Please use 'ON' or 'OFF' instead",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_global_index") && normalized == "OFF" {
            Some((
                None,
                "tidb_enable_global_index is always turned on. This variable has been deprecated and will be removed in the future releases",
            ))
        } else if name.eq_ignore_ascii_case("tidb_auto_analyze_partition_batch_size") {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT),
                "Updating 'tidb_auto_analyze_partition_batch_size' is deprecated. It will be made read-only in a future release.",
            ))
        } else if name.eq_ignore_ascii_case("tidb_enable_async_merge_global_stats") {
            // Pinned Go warns for EVERY assignment, in both session and
            // global scope, independent of the normalized boolean value.
            Some((
                None,
                "The 'tidb_enable_async_merge_global_stats' variable will always be enabled in a \
                 future release; changing it is discouraged.",
            ))
        } else if name.eq_ignore_ascii_case("tidb_disable_txn_auto_retry") && normalized == "OFF" {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX),
                "'OFF' is deprecated and will be removed in a future release. Please use ON instead",
            ))
        } else if name.eq_ignore_ascii_case("tidb_index_lookup_concurrency") {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX),
                "'tidb_index_lookup_concurrency' is deprecated and will be removed in a future release. Please use tidb_executor_concurrency instead",
            ))
        } else if name.eq_ignore_ascii_case("tidb_index_serial_scan_concurrency") {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX),
                "The 'tidb_index_serial_scan_concurrency' variable is deprecated. Sequential scans follow 'tidb_executor_concurrency', and index statistics collection uses 'tidb_analyze_distsql_scan_concurrency'.",
            ))
        } else if name.eq_ignore_ascii_case("tidb_prepared_plan_cache_size") {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX),
                "'tidb_prepared_plan_cache_size' is deprecated and will be removed in a future release. Please use tidb_session_plan_cache_size instead",
            ))
        } else if name.eq_ignore_ascii_case("tidb_non_prepared_plan_cache_size") {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX),
                "'tidb_non_prepared_plan_cache_size' is deprecated and will be removed in a future release. Please use tidb_session_plan_cache_size instead",
            ))
        } else if name.eq_ignore_ascii_case("tidb_merge_partition_stats_concurrency")
            && normalized != "1"
        {
            Some((
                Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX),
                "tidb_merge_partition_stats_concurrency is deprecated: the merge no longer runs concurrently, so this setting has no effect. Kept for backward compatibility.",
            ))
        } else if name.eq_ignore_ascii_case("default_collation_for_utf8mb4") {
            // Go appends this deprecation warning only when its validation
            // succeeds.  Unlike the always-warning compatibility variables,
            // an unknown or non-utf8mb4 collation must not leave a warning on
            // the failed statement.
            crate::sysvar::get_sys_var(name)
                .and_then(|definition| {
                    definition
                        .validate_in_scope(value, crate::sysvar::SCOPE_SESSION)
                        .ok()
                })
                .map(|_| {
                    (
                        Some(&*tidb_error::catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT),
                        "Updating 'default_collation_for_utf8mb4' is deprecated. It will be made \
                         read-only in a future release.",
                    )
                })
        } else {
            None
        };
        let Some((proto, message)) = warning else {
            return;
        };
        match proto {
            Some(proto) => {
                self.append_stmt_warning(VarWarningLevel::Warning, proto, message.to_owned());
            }
            None => self.append_stmt_plain_warning(VarWarningLevel::Warning, message.to_owned()),
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
            VarWarningLevel::Warning,
            &tidb_error::catalog::variable::ERR_TRUNCATED_WRONG_VALUE,
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
            self.append_stmt_plain_warning(VarWarningLevel::Warning, warning);
        }
    }
}
