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

//! The `tidb_enable_noop_functions` gate: the clauses TiDB parses but only
//! implements as no-ops, and the read-only variables whose ON value the same
//! variable guards.
//!
//! Go spells this in two places that read one setting -- `preprocessor`'s
//! `checkNoopFuncs` for a query's clauses and `varsutil.go`'s `checkReadOnly`
//! for the `noop.go` variables -- so both live behind the one
//! `SessionVars.NoopFuncsMode` read,
//! [`tidb_variable::SessionVars::noop_funcs_mode`].

use tidb_variable::set_hooks::NoopFuncsMode;

use crate::{sysvar, DriverError, Session, WarningLevel};

impl Session {
    /// The value a `SET_VAR` hint actually installs for one of these
    /// variables.
    ///
    /// Go applies the hint through `SetSystemVarWithRelaxedValidation`, which
    /// runs the same `Validation` and then keeps the VALUE it returned while
    /// discarding its error -- and `ValidateWithRelaxedValidation` restores
    /// the warning list around the call, so a warning the hook appended does
    /// not survive either. The refusal branch returns `vardef.Off`, so
    /// `select /*+ set_var(sql_auto_is_null=1) */ @@sql_auto_is_null` answers
    /// `0` while the hint is in force, rather than failing the statement or
    /// taking the requested value.
    pub(crate) fn relaxed_noop_gated_value(&self, name: &str, value: String) -> String {
        if sysvar::noop_gated_clause(name).is_none() || !turns_on(name, &value) {
            return value;
        }
        match self.vars.noop_funcs_mode(false) {
            NoopFuncsMode::On | NoopFuncsMode::Warn => value,
            NoopFuncsMode::Off => "OFF".to_owned(),
        }
    }

    /// The three-way `tidb_enable_noop_functions` decision, which every gated
    /// clause takes: `OFF` refuses with 1235, `WARN` warns with the same text
    /// and continues, `ON` says nothing.
    ///
    /// Go spells this rule once per call site
    /// (`preprocessor.checkNoopFuncs`, `varsutil.checkReadOnly`,
    /// `SimpleExec.executeBegin`); keeping it in one place here is what makes
    /// a new gated clause a one-line addition rather than a fourth copy of
    /// the same `if`.
    pub(crate) fn gate_noop_clause(
        &mut self,
        clause: &'static str,
        global: bool,
    ) -> Result<(), DriverError> {
        match self.vars.noop_funcs_mode(global) {
            NoopFuncsMode::On => Ok(()),
            NoopFuncsMode::Off => {
                // go appends the statement error into the warning buffer as
                // well (driver_tidb.go:376's error door), so SHOW WARNINGS
                // after the refused statement carries the same 1235 row.
                let error = DriverError::from(
                    tidb_expr::expression_errors::ERR_FUNCTIONS_NOOP_IMPL
                        .generate_with_stack_by_args(&[(clause).into()]),
                );
                self.append_driver_error(WarningLevel::Error, &error);
                Err(error)
            }
            NoopFuncsMode::Warn => {
                // Go `StmtCtx.AppendWarning(expression.ErrFunctionsNoopImpl...)`.
                self.append_driver_error(
                    WarningLevel::Warning,
                    &DriverError::from(
                        tidb_expr::expression_errors::ERR_FUNCTIONS_NOOP_IMPL
                            .generate_with_stack_by_args(&[(clause).into()]),
                    ),
                );
                Ok(())
            }
        }
    }

    /// Go `preprocessor.checkNoopFuncs` + `checkGroupBy`: refuses the clauses
    /// TiDB parses but only implements as no-ops, unless
    /// `tidb_enable_noop_functions` says otherwise.
    ///
    /// Captured from TiDB with the variable at its `OFF` default:
    /// `SELECT SQL_CALC_FOUND_ROWS ...`, `... FOR SHARE` and `... LOCK IN
    /// SHARE MODE` all raise 1235; `FOR UPDATE` does not.
    ///
    /// `tidb_enable_shared_lock_promotion` turns `FOR SHARE` into `FOR UPDATE`
    /// before this check. The eventual lock executor still owns the
    /// `ForShareLockEnabledByNoop` statement flag, but admission must already
    /// skip the no-op gate when promotion is enabled.
    pub(crate) fn check_noop_functions(
        &mut self,
        query: &tidb_ast::QueryStmt,
    ) -> Result<(), DriverError> {
        let mode = self.vars.noop_funcs_mode(false);
        let mut gated: Vec<&'static str> = Vec::new();
        tidb_planner::preprocess::collect_noop_clauses(query, &mut gated);
        if self.vars.shared_lock_promotion_enabled() {
            // Go's preprocessor rewrites the shared-lock clause to the real
            // FOR UPDATE path before `checkNoopFuncs`; it must not warn or
            // refuse the source-shaped `FOR SHARE` spelling here.
            gated.retain(|clause| *clause != "LOCK IN SHARE MODE");
        }
        if gated.is_empty() || mode == NoopFuncsMode::On {
            return Ok(());
        }
        for clause in gated {
            let error = DriverError::from(
                tidb_expr::expression_errors::ERR_FUNCTIONS_NOOP_IMPL
                    .generate_with_stack_by_args(&[(clause).into()]),
            );
            if mode == NoopFuncsMode::Off {
                return Err(error);
            }
            self.append_driver_error(WarningLevel::Warning, &error);
        }
        Ok(())
    }
}

/// Whether this assignment is the ON that Go's `TiDBOptOn(normalizedValue)`
/// tests, taken after the registry's own type normalization -- which Go also
/// runs first, in `ValidateFromType`.
fn turns_on(name: &str, value: &str) -> bool {
    sysvar::get_sys_var(name)
        .and_then(|def| def.validate(value).ok())
        .is_some_and(|validated| validated.value == "ON")
}
