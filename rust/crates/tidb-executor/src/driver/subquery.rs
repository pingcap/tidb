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

//! Subquery helpers for the executor's DML drivers. Go plans a subquery in
//! the expression rewriter (`handleScalarSubquery` and its siblings) and the
//! planner's `lower_scalar_subqueries` is that port; what remains here only
//! asks questions of the AST.

use super::*;

/// The scope a subquery inside `select` sees as its OUTER scope: `select`'s
/// own `FROM` tables. An unresolvable `FROM` yields an empty scope, and the
/// error surfaces when the query itself is built.
pub(crate) fn select_outer_scope(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> FromScope {
    let empty = || FromScope::for_statement(ctx);
    select.from.as_ref().map_or_else(empty, |join| {
        super::planner_bridge::logical_from_scope(join, catalog, current_db, ctx)
            .unwrap_or_else(|_| empty())
    })
}

/// Whether `expr` contains a subquery.
pub(crate) fn expr_has_subquery(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. } => true,
        Expr::Paren(inner) | Expr::Unary(_, inner) | Expr::Is { expr: inner, .. } => {
            expr_has_subquery(inner)
        }
        Expr::Binary(_, lhs, rhs) => expr_has_subquery(lhs) || expr_has_subquery(rhs),
        Expr::In { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            value.as_deref().is_some_and(expr_has_subquery)
                || when_clauses.iter().any(|(condition, result)| {
                    expr_has_subquery(condition) || expr_has_subquery(result)
                })
                || else_clause.as_deref().is_some_and(expr_has_subquery)
        }
        Expr::Func { args, .. } | Expr::Aggregate { args, .. } => {
            args.iter().any(expr_has_subquery)
        }
        _ => false,
    }
}
