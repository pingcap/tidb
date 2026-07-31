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

//! Go `pkg/planner/funcdep`: which columns of a query block determine which
//! others.
//!
//! [`fd_graph`] is the graph itself, ported from Go's `fd_graph.go` and
//! verified against Go's own test table. [`null_reject`] is the predicate test
//! that promotes a lax dependency to a strict one.
//!
//! [`scope_fd_set`] plays the role Go's `ExtractFD` methods play on the
//! logical plan tree. Go builds the set bottom-up over `DataSource`,
//! `Selection`, `LogicalJoin` and so on; this tier's `ONLY_FULL_GROUP_BY`
//! check runs on the WRITTEN AST over a flat [`FromScope`], so the same
//! contributions are read straight off the scope and the `WHERE`:
//!
//!  * DATASOURCE -- each base table's keys and its generated columns.
//!  * CARTESIAN PRODUCT -- the `FROM` list's tables are independent.
//!  * SELECTION -- what the `WHERE` proves NOT NULL, fixes to a constant, or
//!    equates.
//!
//! The JOIN contribution is NOT here. Go's `LogicalJoin.ExtractFD` splits into
//! inner and outer forms, and the outer one (`FDSet.MakeOuterJoin`) has to
//! model null-extended rows: a strict dependency from the null-supplying side
//! weakens to a lax one, constants and equivalences from that side are lost,
//! and an equivalence across the join survives only under conditions on which
//! side the other filters touch. It also needs the deferred `ncEdges` this
//! port omits. That is its own unit, and until it lands an `ON` condition
//! contributes nothing here -- which refuses queries TiDB answers, and never
//! answers one TiDB refuses.

pub(crate) mod col_set;
pub(crate) mod fd_graph;
pub(crate) mod null_reject;

use super::{FromScope, FromTable};
use col_set::ColSet;
use fd_graph::FdSet;

/// The dependencies a base table contributes on its own, as column offsets
/// LOCAL to the table (Go `DataSource.ExtractFD`).
#[derive(Clone, Default, Debug)]
pub(crate) struct TableFuncDeps {
    /// Candidate keys: every column is `NOT NULL`, so the key determines the
    /// row outright. The primary key and each all-`NOT NULL` UNIQUE index.
    pub(crate) strict_keys: Vec<Vec<usize>>,
    /// A UNIQUE index with a nullable column. It determines the row only for
    /// rows whose key is not NULL -- repeated NULLs are permitted and each may
    /// carry different values -- so it is a LAX dependency until a predicate
    /// proves the nullable members non-null.
    pub(crate) lax_keys: Vec<Vec<usize>>,
    /// A generated column and the columns its expression reads, which
    /// determine it. Go builds one level per column and lets the graph's
    /// transitivity chain them, so `c AS (a+2)` and `d AS (c+2)` together give
    /// `{a} --> {d}`.
    pub(crate) generated: Vec<(Vec<usize>, usize)>,
    /// The offsets carrying `NOT NULL`.
    pub(crate) not_null: Vec<usize>,
}

/// The functional dependencies holding over a `FROM` scope filtered by
/// `where_clause`, with each column identified by its offset in the scope.
pub(crate) fn scope_fd_set(scope: &FromScope, where_clause: Option<&tidb_ast::Expr>) -> FdSet {
    let mut fds = FdSet::new();
    for table in &scope.tables {
        let one = table_fd_set(table);
        fds.make_cartesian_product(&one);
    }
    if let Some(where_clause) = where_clause {
        apply_selection(&mut fds, scope, where_clause);
    }
    fds
}

/// Go `DataSource.ExtractFD` for one source, translated to scope offsets.
fn table_fd_set(table: &FromTable) -> FdSet {
    let mut fds = FdSet::new();
    let id = |local: usize| (table.offset + local) as i32;
    let all_cols = ColSet::of((0..table.columns.len()).map(id));

    for key in &table.func_deps.strict_keys {
        let key = ColSet::of(key.iter().map(|&local| id(local)));
        fds.add_strict(key.clone(), all_cols.clone());
        fds.make_not_null(key);
    }
    for key in &table.func_deps.lax_keys {
        fds.add_lax(
            ColSet::of(key.iter().map(|&local| id(local))),
            all_cols.clone(),
        );
    }
    for (sources, generated) in &table.func_deps.generated {
        fds.add_strict(
            ColSet::of(sources.iter().map(|&local| id(local))),
            ColSet::of([id(*generated)]),
        );
    }
    // Applied last, as Go does: a column declared `NOT NULL` may promote a lax
    // key added above.
    fds.make_not_null(ColSet::of(
        table.func_deps.not_null.iter().map(|&local| id(local)),
    ));
    fds
}

/// Go `LogicalSelection.ExtractFD`: what a `WHERE` adds.
///
/// Go reads the conjuncts of an already-rewritten condition list; the same
/// conjuncts are the top-level `AND` chain of the written predicate. The three
/// contributions are applied in Go's order, which matters: the not-null
/// promotion must happen before the equivalences that may widen it.
fn apply_selection(fds: &mut FdSet, scope: &FromScope, where_clause: &tidb_ast::Expr) {
    let resolver = super::ScopeResolver { scope };
    let resolve = |path: &[String]| -> Option<usize> {
        use crate::driver::ColumnResolver;
        resolver.resolve(path).map(|(offset, _, _)| offset)
    };
    let column_offset = |expr: &tidb_ast::Expr| match strip(expr) {
        tidb_ast::Expr::Column(path) => resolve(path),
        _ => None,
    };

    let conditions = conjuncts(where_clause);

    // Go `ExtractNotNullFromConds`: a conjunct that cannot be TRUE while a
    // column it reads is NULL proves that column NOT NULL for every surviving
    // row.
    let mut not_null = ColSet::new();
    for condition in &conditions {
        for path in super::only_full_group_by::bare_columns(condition) {
            let Some(offset) = resolve(&path) else {
                continue;
            };
            if null_reject::is_null_rejected(condition, offset, &resolve) {
                not_null.insert(offset as i32);
            }
        }
    }

    // Go `ExtractConstantCols` / `ExtractEquivalenceCols`: `col = <literal>`
    // fixes a column, `col = col` equates two.
    let mut constants = ColSet::new();
    let mut equivalences: Vec<(i32, i32)> = Vec::new();
    for condition in &conditions {
        let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::Eq, lhs, rhs) = strip(condition) else {
            continue;
        };
        match (column_offset(lhs), column_offset(rhs)) {
            (Some(left), Some(right)) => equivalences.push((left as i32, right as i32)),
            (Some(left), None) if is_literal(strip(rhs)) => constants.insert(left as i32),
            (None, Some(right)) if is_literal(strip(lhs)) => constants.insert(right as i32),
            _ => {}
        }
    }

    fds.make_not_null(not_null);
    fds.add_constants(constants);
    for (left, right) in equivalences {
        fds.add_equivalence(ColSet::of([left]), ColSet::of([right]));
    }
}

/// The top-level `AND` chain, which is the conjunct list Go's condition slice
/// holds (Go `splitWhere`).
fn conjuncts(expr: &tidb_ast::Expr) -> Vec<&tidb_ast::Expr> {
    let mut found = Vec::new();
    let mut stack = vec![expr];
    while let Some(expr) = stack.pop() {
        match expr {
            tidb_ast::Expr::Paren(inner) => stack.push(inner),
            tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) => {
                stack.push(rhs);
                stack.push(lhs);
            }
            other => found.push(other),
        }
    }
    found
}

/// Parentheses and a unary `+` are notation (Go
/// `getInnerFromParenthesesAndUnaryPlus`).
fn strip(expr: &tidb_ast::Expr) -> &tidb_ast::Expr {
    match expr {
        tidb_ast::Expr::Paren(inner) => strip(inner),
        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Plus, inner) => strip(inner),
        other => other,
    }
}

/// A constant value: a literal, or a literal behind a sign.
fn is_literal(expr: &tidb_ast::Expr) -> bool {
    match expr {
        tidb_ast::Expr::Unary(_, inner) => is_literal(inner),
        tidb_ast::Expr::Int(_)
        | tidb_ast::Expr::Float(_)
        | tidb_ast::Expr::Decimal(_)
        | tidb_ast::Expr::String(_)
        | tidb_ast::Expr::Bool(_) => true,
        _ => false,
    }
}
