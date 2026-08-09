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

//! WHERE conditions carried down into the inner join that can use them.
//!
//! Mirrors the part of Go's `pkg/planner/core/rule_predicate_push_down.go`
//! that `LogicalJoin.PredicatePushDown` performs for an inner join: a
//! predicate written above the join, in `WHERE`, becomes a condition OF the
//! join when the join is the lowest node whose two inputs together supply the
//! predicate's columns.
//!
//! # Why this is the shape that matters, and not join order
//!
//! `FROM a, b, c, ... WHERE a.x = b.y AND ...` -- the comma spelling with
//! every equality in `WHERE` -- reaches [`super::from::build_join`] as a tree
//! of joins with NO `ON` at all. A join with no equality keeps the nested
//! loop (see [`crate::join`]'s module doc), so each node in the tree
//! materialises the full cross product of everything below it and the filter
//! only runs at the top. The cost is then the product of the inputs' row
//! counts, which is exponential in the number of tables no matter what order
//! the tables are joined in: `executor/jointest/join`'s 21-table join over
//! two-row tables measured a clean doubling per table added (7.4s at 21
//! tables in release, and it is the same 2^k curve from 2 tables up).
//!
//! Pushing the equality down is what removes the exponent, because it is what
//! lets the join hash instead of loop. Reordering the joins does not: a
//! cross product is order-independent.
//!
//! # Why the row set cannot move
//!
//! A pushed conjunct becomes an inner-join condition at the lowest node whose
//! two children together supply its columns. The join-group inventory may
//! then remove the redundant root predicate after the committed physical
//! paths prove that every other conjunct is accounted for; without that
//! proof it stays in `WHERE` as a second, equivalent check. Outer joins are
//! refused here because moving a predicate across null-extension needs a
//! different proof.
//!
//! A bare `col = col` becomes an equality key; every other eligible
//! cross-child predicate becomes the join's `other cond`. Subqueries,
//! variables, assignments, schema-qualified calls and mutable or
//! non-deterministic builtins are refused, matching the safety gates Go puts
//! around predicate movement.

use tidb_ast::Expr;

use super::from::{FromScope, ScopeResolver};

/// The `WHERE` conjuncts an enclosing `SELECT` offers to the joins below it.
///
/// Empty for every caller that has no `WHERE` to offer -- a subquery built
/// through [`super::from::build_join`] directly, or a `FROM` with no filter.
pub(crate) type Offered<'a> = &'a [&'a Expr];

/// Splits `select`'s `WHERE` into the conjuncts eligible for pushdown.
pub(crate) fn offered_conjuncts(where_clause: Option<&Expr>) -> Vec<&Expr> {
    let Some(expr) = where_clause else {
        return Vec::new();
    };
    let mut conjuncts = Vec::new();
    crate::plan_trace::collect_and(expr, &mut conjuncts);
    conjuncts.retain(|conjunct| condition_is_stable(conjunct));
    conjuncts
}

/// Whether moving this condition can neither duplicate side effects nor
/// observe a different volatile value at a lower join node.
fn condition_is_stable(expr: &Expr) -> bool {
    struct Check {
        stable: bool,
    }

    impl tidb_ast::Visitor for Check {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_ref::<Expr>() else {
                return false;
            };
            match expr {
                Expr::UserVar(_)
                | Expr::SysVar { .. }
                | Expr::Assign { .. }
                | Expr::GenericFuncCall { .. } => self.stable = false,
                Expr::Func { name, .. } => {
                    let name = name.to_ascii_lowercase();
                    if super::through_proj::is_mutable_effects(&name)
                        || super::through_proj::is_unfoldable(&name)
                    {
                        self.stable = false;
                    }
                }
                _ => {}
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut check = Check { stable: true };
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut check);
    check.stable
}

/// The offered conjuncts this join is the lowest node able to evaluate.
///
/// "Lowest" needs no search: a conjunct that reads at least one column on
/// EACH side of `left_width` is one neither child could have evaluated alone.
/// A conjunct whose columns all land on one side was already offered to that
/// child's own join node (or belongs to one table, which is scan-level work).
pub(crate) fn spanning_conjuncts<'a>(
    offered: Offered<'a>,
    scope: &FromScope,
    left_width: usize,
) -> Vec<&'a Expr> {
    let resolver = ScopeResolver { scope };
    offered
        .iter()
        .filter(|conjunct| {
            // This is column pruning's exhaustive expression walk. It refuses
            // subqueries, parameter markers and any shape whose references it
            // cannot prove belong to this scope.
            let Some(offsets) = crate::column_prune::expr_column_offsets(conjunct, &resolver)
            else {
                return false;
            };
            offsets.iter().any(|offset| *offset < left_width)
                && offsets.iter().any(|offset| *offset >= left_width)
        })
        .copied()
        .collect()
}
