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

//! Go `planner.Optimize`'s binding step (`pkg/planner/optimize.go`): the
//! statement's matched binding (`bindinfo.MatchSQLBinding`) replaces its own
//! hints, and the session records the match.

use tidb_ast::Stmt;
use tidb_bindinfo::binding::{self, BindingMatchContext};

/// What Go `Optimize`'s binding step reads and writes on `sctx` beyond the
/// match itself.
pub trait PlannerBindingContext: BindingMatchContext {
    /// Go `SessionVars.UsePlanBaselines`.
    fn use_plan_baselines(&self) -> bool;
    /// Go `matchSQLBindingCore`'s deferred `DurationOptimizer.BindingMatch`.
    fn set_binding_match_duration(&mut self, elapsed: std::time::Duration);
    /// Go `StmtCtx.AppendWarning` of a 1105 warning.
    fn append_binding_warning(&mut self, message: String);
    /// Go `SessionVars.FoundInBinding = true`.
    fn set_found_in_binding(&mut self);
}

/// Go `Optimize`'s binding step: the statement with its matched binding's
/// hints and the binding's `BindSQL`, `None` when no binding matched (the
/// caller keeps the original, so a session with no bindings pays one
/// map-emptiness test and nothing else).
pub fn bind_sql_binding<C: PlannerBindingContext + ?Sized>(
    ctx: &mut C,
    stmt: &Stmt,
) -> Option<(Stmt, String)> {
    if !binding::may_have_sql_binding(stmt) {
        return None;
    }
    // Go gates the whole step on `SessionVars.UsePlanBaselines`.
    if !ctx.use_plan_baselines() {
        return None;
    }
    let begin_binding_match = std::time::Instant::now();
    let matched = binding::match_sql_binding_core(ctx, stmt);
    ctx.set_binding_match_duration(begin_binding_match.elapsed());
    let (hints, bind_sql) = matched?;
    let mut bound = stmt.clone();
    binding::bind_hints(&mut bound, &hints);
    for warning in filter_sem_restricted_hints(&mut bound) {
        ctx.append_binding_warning(warning);
    }
    ctx.set_found_in_binding();
    // Go `optimize`: `originStmtHints.QueryHasHints` -- the statement wrote
    // hints of its own, which the binding's replace.
    if !tidb_hint::extract_table_hints_from_stmt_node(stmt)
        .0
        .is_empty()
    {
        ctx.append_binding_warning(format!(
            "The system ignores the hints in the current query and uses the hints \
             specified in the bindSQL: {bind_sql}"
        ));
    }
    Some((bound, bind_sql))
}

/// Go `hint.RegisterRestrictedHintChecker(semv2.IsRestrictedHint)`
/// (`planner/core/core_init.go`): a hint SEM restricts is dropped from
/// the statement with its warning.
#[must_use]
pub fn filter_sem_restricted_hints(stmt: &mut Stmt) -> Vec<String> {
    struct Filter {
        warnings: Vec<String>,
    }

    impl Filter {
        fn retain(&mut self, hints: &mut Vec<tidb_ast::Hint>) {
            hints.retain(
                |hint| match tidb_sem_v2::is_restricted_hint(&hint.lower_name()) {
                    Ok(()) => true,
                    Err(warning) => {
                        self.warnings.push(warning);
                        false
                    }
                },
            );
        }
    }

    impl tidb_ast::Visitor for Filter {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
                self.retain(&mut select.hints);
            } else if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
                self.retain(&mut insert.hints);
            } else if let Some(update) = node.downcast_mut::<tidb_ast::UpdateStmt>() {
                self.retain(&mut update.hints);
            } else if let Some(delete) = node.downcast_mut::<tidb_ast::DeleteStmt>() {
                self.retain(&mut delete.hints);
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut filter = Filter {
        warnings: Vec::new(),
    };
    tidb_ast::Visitable::accept(stmt, &mut filter);
    filter.warnings
}
