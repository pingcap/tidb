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
//! [`Session::noop_funcs_mode`] read here.

use crate::{sysvar, DriverError, Session, WarningLevel};

/// Go `variable.NoopFuncsMode`: how a clause TiDB only implements as a
/// no-op is treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NoopFuncsMode {
    /// `OFF` (the default): the statement is refused.
    Off,
    /// `ON`: the clause is accepted and does nothing.
    On,
    /// `WARN`: the clause is accepted with a warning.
    Warn,
}

impl Session {
    /// Go `SessionVars.NoopFuncsMode`, read from this session's copy of
    /// `tidb_enable_noop_functions` or -- for a `SET GLOBAL` being validated
    /// -- from the shared table, which is the scope Go's `checkReadOnly`
    /// consults through `GlobalVarsAccessor`.
    fn noop_funcs_mode(&self, global: bool) -> NoopFuncsMode {
        let value = if global {
            self.vars.get_global("tidb_enable_noop_functions")
        } else {
            self.vars.get_system("tidb_enable_noop_functions")
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
    /// would reject -- that is [`vars::SessionSysvars::set_system`]'s job,
    /// and Go likewise validates the type before it runs this hook.
    pub(crate) fn check_noop_gated_variable(
        &mut self,
        name: &str,
        value: &str,
        is_global: bool,
    ) -> Result<(), DriverError> {
        let Some(clause) = sysvar::noop_gated_clause(name) else {
            return Ok(());
        };
        if !turns_on(name, value) {
            return Ok(());
        }
        self.gate_noop_clause(clause, is_global)
    }

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
        match self.noop_funcs_mode(false) {
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
        match self.noop_funcs_mode(global) {
            NoopFuncsMode::On => Ok(()),
            NoopFuncsMode::Off => Err(DriverError::FunctionsNoopImpl(clause)),
            NoopFuncsMode::Warn => {
                self.append_warning(
                    WarningLevel::Warning,
                    1235,
                    format!(
                        "function {clause} has only noop implementation in tidb now, use \
                         tidb_enable_noop_functions to enable these functions"
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
    /// DEFERRED (documented): `tidb_enable_shared_lock_promotion`, which
    /// turns `FOR SHARE` into `FOR UPDATE` before this check, and the
    /// `ForShareLockEnabledByNoop` statement flag that only a real locking
    /// layer would read.
    pub(crate) fn check_noop_functions(
        &mut self,
        query: &tidb_ast::QueryStmt,
    ) -> Result<(), DriverError> {
        let mode = self.noop_funcs_mode(false);
        let mut gated: Vec<&'static str> = Vec::new();
        collect_noop_clauses(query, &mut gated);
        if gated.is_empty() || mode == NoopFuncsMode::On {
            return Ok(());
        }
        for clause in gated {
            let message = format!(
                "function {clause} has only noop implementation in tidb now, use \
                 tidb_enable_noop_functions to enable these functions"
            );
            if mode == NoopFuncsMode::Off {
                return Err(DriverError::FunctionsNoopImpl(clause));
            }
            self.append_warning(WarningLevel::Warning, 1235, message);
        }
        Ok(())
    }
}

/// Names every gated clause the query uses, in the order Go's preprocessor
/// would reach them.
///
/// Go walks the whole statement tree, so a gated clause inside a derived
/// table, a CTE or a subquery counts too; this walk covers the same
/// containers.
fn collect_noop_clauses(query: &tidb_ast::QueryStmt, out: &mut Vec<&'static str>) {
    match query {
        tidb_ast::QueryStmt::Select(select) => collect_noop_in_select(select, out),
        tidb_ast::QueryStmt::SetOpr(set_opr) => collect_noop_in_set_opr(set_opr, out),
    }
}

fn collect_noop_in_set_opr(set_opr: &tidb_ast::SetOprStmt, out: &mut Vec<&'static str>) {
    if let Some(with) = &set_opr.with {
        for cte in &with.ctes {
            collect_noop_clauses(&cte.query, out);
        }
    }
    for term in &set_opr.terms {
        match &term.body {
            tidb_ast::SetOprTermBody::Select(select) => collect_noop_in_select(select, out),
            tidb_ast::SetOprTermBody::Nested(nested) => collect_noop_in_set_opr(nested, out),
        }
    }
    // A set operation carries its own trailing locking clause, which the
    // grammar attaches to the whole statement rather than the last term.
    if share_lock(&set_opr.lock) || share_lock(&set_opr.outer_lock) {
        out.push("LOCK IN SHARE MODE");
    }
}

/// Whether a locking clause is the shared kind, which is the gated one --
/// `FOR UPDATE` is a real lock in TiDB and is never gated.
fn share_lock(lock: &Option<tidb_ast::SelectLock>) -> bool {
    matches!(
        lock,
        Some(tidb_ast::SelectLock {
            kind: tidb_ast::LockKind::Share,
            ..
        })
    )
}

fn collect_noop_in_select(select: &tidb_ast::SelectStmt, out: &mut Vec<&'static str>) {
    if select.calc_found_rows {
        out.push("SQL_CALC_FOUND_ROWS");
    }
    if share_lock(&select.lock) {
        out.push("LOCK IN SHARE MODE");
    }
    // Go's `checkGroupBy`: a written ASC/DESC on a GROUP BY item is a no-op,
    // because TiDB does not order groups.
    if select.group_by.iter().any(|item| item.desc.is_some()) {
        out.push("GROUP BY expr ASC|DESC");
    }
    if let Some(with) = &select.with {
        for cte in &with.ctes {
            collect_noop_clauses(&cte.query, out);
        }
    }
    if let Some(from) = &select.from {
        collect_noop_in_join(from, out);
    }
    for expr in select
        .where_clause
        .iter()
        .chain(select.having.iter())
        .chain(select.group_by.iter().map(|item| &item.expr))
        .chain(select.order_by.iter().map(|item| &item.expr))
    {
        collect_noop_in_expr(expr, out);
    }
}

/// The subqueries a `FROM` clause holds, which are derived tables.
fn collect_noop_in_join(join: &tidb_ast::Join, out: &mut Vec<&'static str>) {
    for node in std::iter::once(&join.left).chain(join.right.iter()) {
        match node {
            tidb_ast::JoinNode::Derived { subquery, .. } => collect_noop_clauses(subquery, out),
            tidb_ast::JoinNode::Join(nested) => collect_noop_in_join(nested, out),
            tidb_ast::JoinNode::Table(_) => {}
        }
    }
    if let Some(on) = &join.on {
        collect_noop_in_expr(on, out);
    }
}

/// The subqueries an expression holds.
fn collect_noop_in_expr(expr: &tidb_ast::Expr, out: &mut Vec<&'static str>) {
    match expr {
        tidb_ast::Expr::Subquery(query) => collect_noop_clauses(query, out),
        tidb_ast::Expr::Exists { subquery, .. } => collect_noop_clauses(subquery, out),
        tidb_ast::Expr::InSubquery { expr, subquery, .. } => {
            collect_noop_in_expr(expr, out);
            collect_noop_clauses(subquery, out);
        }
        tidb_ast::Expr::CompareSubquery { left, subquery, .. } => {
            collect_noop_in_expr(left, out);
            collect_noop_clauses(subquery, out);
        }
        _ => {}
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
