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

//! WHERE equi-conditions carried down into the inner join that can use them.
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
//! A pushed conjunct is not REMOVED from `WHERE`. It is COPIED into the inner
//! join's condition list, where -- for an inner join, the only kind this
//! touches -- a condition is a filter over the same pairs the `WHERE` above
//! would have filtered. So the output is `WHERE(J(a,b))` before and
//! `WHERE(J_c(a,b))` after with `J_c ⊆ J`, and every pair `J_c` drops is a
//! pair `WHERE` dropped anyway. Redundancy is the proof: no reasoning about
//! null-extension or about condition placement is needed, which is exactly
//! the reasoning an outer join would require -- and outer joins are refused
//! here rather than reasoned about.
//!
//! Only a bare `col = col` between two columns is eligible. That is the whole
//! of what turns a nested loop into a hash join
//! ([`crate::hash_join::split_equi`] indexes nothing else), and it is
//! trivially free of the two hazards a general predicate carries when it is
//! evaluated twice: a subquery (whose cost and, for `EXISTS` over a mutating
//! source, whose answer are not idempotent) and a mutable-effects or
//! non-deterministic expression, which Go screens for by name
//! (`expression.IsMutableEffectsExpr`, `CheckNonDeterministic`).

use tidb_ast::{BinaryOp, Expr};

use super::from::{FromScope, ScopeResolver};
use tidb_expr::rewriter::ColumnResolver;

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
    conjuncts.retain(|c| column_equality(c).is_some());
    conjuncts
}

/// The two column paths of a bare `col = col`, or `None` for anything else.
fn column_equality(expr: &Expr) -> Option<(&[String], &[String])> {
    match expr {
        Expr::Binary(BinaryOp::Eq, lhs, rhs) => match (&**lhs, &**rhs) {
            (Expr::Column(left), Expr::Column(right)) => Some((left, right)),
            _ => None,
        },
        _ => None,
    }
}

/// The offered conjuncts this join is the lowest node able to evaluate.
///
/// "Lowest" needs no search: a conjunct whose two columns land on OPPOSITE
/// sides of `left_width` is one neither child could have evaluated alone, and
/// a conjunct whose columns land on the same side was already offered to that
/// child's own join node (or belongs to a single table, which is a scan-level
/// filter this does not attempt). Testing the sides is therefore the whole
/// placement rule.
pub(crate) fn spanning_conjuncts<'a>(
    offered: Offered<'a>,
    scope: &FromScope,
    left_width: usize,
) -> Vec<&'a Expr> {
    let resolver = ScopeResolver { scope };
    offered
        .iter()
        .filter(|conjunct| {
            let Some((left, right)) = column_equality(conjunct) else {
                return false;
            };
            // An unresolvable column is one this scope does not own -- an
            // outer-query correlation above all -- and is left where it is.
            let (Some((left_offset, _, _)), Some((right_offset, _, _))) =
                (resolver.resolve(left), resolver.resolve(right))
            else {
                return false;
            };
            (left_offset < left_width) != (right_offset < left_width)
        })
        .copied()
        .collect()
}
