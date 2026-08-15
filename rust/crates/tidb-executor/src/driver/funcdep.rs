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
//! `LogicalJoin`, and `Selection`; this tier walks the written join tree while
//! using [`FromScope`] for the already-resolved column identities:
//!
//!  * DATASOURCE -- each base table's keys and its generated columns.
//!  * JOIN -- child sets combine recursively and `ON`/`USING`/`NATURAL`
//!    predicates add the same inner- or outer-join dependencies Go records.
//!  * SELECTION -- what the `WHERE` proves NOT NULL, fixes to a constant, or
//!    equates.

pub(crate) mod fd_graph;
pub(crate) mod null_reject;

use super::{FromScope, FromTable};
use fd_graph::{FdSet, OuterJoinOptions};
pub(crate) use tidb_util::intset::FastIntSet as ColSet;

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

/// The functional dependencies holding over a `FROM` join tree filtered by
/// `where_clause`, with each column identified by its offset in `scope`.
pub(crate) fn scope_fd_set(
    scope: &FromScope,
    from: Option<&tidb_ast::Join>,
    where_clause: Option<&tidb_ast::Expr>,
) -> FdSet {
    let mut used = vec![false; scope.tables.len()];
    let built = from.and_then(|join| build_join_fds(join, scope, &mut used));
    let mut fds = match built.filter(|_| used.iter().all(|used| *used)) {
        Some(relation) => relation.fds,
        None => flat_scope_fds(scope),
    };
    if let Some(where_clause) = where_clause {
        apply_selection(&mut fds, scope, where_clause);
    }
    fds
}

fn flat_scope_fds(scope: &FromScope) -> FdSet {
    let mut fds = FdSet::new();
    for table in &scope.tables {
        fds.make_cartesian_product(&table_fd_set(table));
    }
    fds
}

#[derive(Debug)]
struct RelationFds {
    fds: FdSet,
    cols: ColSet,
    /// Columns visible to this node's parent, in display order. Coalesced
    /// copies stay in `cols` but not here.
    visible: Vec<(usize, String)>,
}

type ColumnEquality = (usize, usize);
type VisibleColumn = (usize, String);

fn build_node_fds(
    node: &tidb_ast::JoinNode,
    scope: &FromScope,
    used: &mut [bool],
) -> Option<RelationFds> {
    match node {
        tidb_ast::JoinNode::Join(join) => build_join_fds(join, scope, used),
        tidb_ast::JoinNode::Table(table_ref) => {
            let visible = table_ref.alias.as_ref().or_else(|| table_ref.name.last())?;
            let at =
                scope.tables.iter().enumerate().position(|(at, table)| {
                    !used[at] && table.name.eq_ignore_ascii_case(visible)
                })?;
            used[at] = true;
            relation_fds(&scope.tables[at])
        }
        tidb_ast::JoinNode::Derived { alias, .. } => {
            let visible = alias.as_ref()?;
            let at =
                scope.tables.iter().enumerate().position(|(at, table)| {
                    !used[at] && table.name.eq_ignore_ascii_case(visible)
                })?;
            used[at] = true;
            relation_fds(&scope.tables[at])
        }
    }
}

fn relation_fds(table: &FromTable) -> Option<RelationFds> {
    let cols = ColSet::of((0..table.columns.len()).map(|local| {
        i64::try_from(table.offset + local).expect("scope column offset fits source int")
    }));
    let visible = table
        .columns
        .iter()
        .enumerate()
        .map(|(local, (name, _))| (table.offset + local, name.clone()))
        .collect();
    Some(RelationFds {
        fds: table_fd_set(table),
        cols,
        visible,
    })
}

fn build_join_fds(
    join: &tidb_ast::Join,
    scope: &FromScope,
    used: &mut [bool],
) -> Option<RelationFds> {
    let left = build_node_fds(&join.left, scope, used)?;
    let Some(right_node) = &join.right else {
        return Some(left);
    };
    let right = build_node_fds(right_node, scope, used)?;
    let (common, visible) = coalesced_columns(join, &left.visible, &right.visible);

    match join.tp {
        tidb_ast::JoinType::Cross => {
            let mut fds = left.fds;
            fds.make_cartesian_product(&right.fds);
            if let Some(on) = &join.on {
                apply_selection(&mut fds, scope, on);
            }
            apply_column_equalities(&mut fds, &common);
            Some(RelationFds {
                fds,
                cols: left.cols.union(&right.cols),
                visible,
            })
        }
        tidb_ast::JoinType::Left | tidb_ast::JoinType::Right => {
            let (mut outer, inner) = if join.tp == tidb_ast::JoinType::Left {
                (left, right)
            } else {
                (right, left)
            };
            let mut filter = FdSet::new();
            if let Some(on) = &join.on {
                apply_selection(&mut filter, scope, on);
            }
            apply_column_equalities(&mut filter, &common);
            let options =
                outer_join_options(join.on.as_ref(), &common, scope, &outer.cols, &inner.cols);
            outer
                .fds
                .make_outer_join(&inner.fds, &filter, &outer.cols, &inner.cols, options);
            Some(RelationFds {
                fds: outer.fds,
                cols: outer.cols.union(&inner.cols),
                visible,
            })
        }
    }
}

/// The equality pairs and display columns introduced by `NATURAL`/`USING`.
/// `from::coalesce_common_columns` has already validated ambiguity/missing
/// names while building the scope; this read-only twin only reconstructs the
/// pairs needed by the FD graph.
fn coalesced_columns(
    join: &tidb_ast::Join,
    left: &[VisibleColumn],
    right: &[VisibleColumn],
) -> (Vec<ColumnEquality>, Vec<VisibleColumn>) {
    if !join.natural && join.using.is_empty() {
        return (Vec::new(), left.iter().chain(right).cloned().collect());
    }
    let (outer, inner) = if join.tp == tidb_ast::JoinType::Right {
        (right, left)
    } else {
        (left, right)
    };
    let named = |name: &str| {
        join.using.is_empty()
            || join
                .using
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(name))
    };
    let mut common = Vec::new();
    for (outer_offset, name) in outer {
        if !named(name)
            || common.iter().any(|(seen, _)| {
                outer
                    .iter()
                    .find(|(offset, _)| offset == seen)
                    .is_some_and(|(_, seen_name)| seen_name.eq_ignore_ascii_case(name))
            })
        {
            continue;
        }
        if let Some((inner_offset, _)) = inner
            .iter()
            .find(|(_, candidate)| candidate.eq_ignore_ascii_case(name))
        {
            common.push((*outer_offset, *inner_offset));
        }
    }
    let is_common = |offset: usize| {
        common
            .iter()
            .any(|(visible, redundant)| *visible == offset || *redundant == offset)
    };
    let mut visible: Vec<VisibleColumn> = common
        .iter()
        .filter_map(|(offset, _)| outer.iter().find(|(candidate, _)| candidate == offset))
        .cloned()
        .collect();
    visible.extend(
        outer
            .iter()
            .filter(|(offset, _)| !is_common(*offset))
            .cloned(),
    );
    visible.extend(
        inner
            .iter()
            .filter(|(offset, _)| !is_common(*offset))
            .cloned(),
    );
    (common, visible)
}

fn apply_column_equalities(fds: &mut FdSet, equalities: &[(usize, usize)]) {
    for &(left, right) in equalities {
        let cols = ColSet::of([
            i64::try_from(left).expect("scope column offset fits source int"),
            i64::try_from(right).expect("scope column offset fits source int"),
        ]);
        fds.make_not_null(cols.clone());
        fds.add_equivalence(
            ColSet::of([i64::try_from(left).expect("scope column offset fits source int")]),
            ColSet::of([i64::try_from(right).expect("scope column offset fits source int")]),
        );
    }
}

fn outer_join_options(
    on: Option<&tidb_ast::Expr>,
    common: &[(usize, usize)],
    scope: &FromScope,
    outer_cols: &ColSet,
    inner_cols: &ColSet,
) -> OuterJoinOptions {
    let resolver = super::ScopeResolver { scope };
    let resolve = |path: &[String]| {
        use crate::driver::ColumnResolver;
        resolver.resolve(path).map(|(offset, _, _)| {
            i64::try_from(offset).expect("scope column offset fits source int")
        })
    };
    let mut outer_equiv = ColSet::default();
    let mut cross_equivalences = common.len();
    for &(visible, redundant) in common {
        let visible = i64::try_from(visible).expect("scope column offset fits source int");
        let redundant = i64::try_from(redundant).expect("scope column offset fits source int");
        if outer_cols.has(visible) {
            outer_equiv.insert(visible);
        } else if outer_cols.has(redundant) {
            outer_equiv.insert(redundant);
        }
    }

    let mut outer_or_other_cols = ColSet::default();
    let mut has_outer_condition = false;
    let mut has_other_condition = false;
    let mut inner_is_false = false;
    for condition in on.into_iter().flat_map(conjuncts) {
        let refs = ColSet::of(
            super::only_full_group_by::bare_columns(condition)
                .into_iter()
                .filter_map(|path| resolve(&path)),
        );
        let cross_equality = equality_offsets(condition, &resolve).is_some_and(|(left, right)| {
            let spans = (outer_cols.has(left) && inner_cols.has(right))
                || (outer_cols.has(right) && inner_cols.has(left));
            if spans {
                outer_equiv.insert(if outer_cols.has(left) { left } else { right });
            }
            spans
        });
        if cross_equality {
            cross_equivalences += 1;
            continue;
        }
        if refs.subset_of(outer_cols) && !refs.is_empty() {
            has_outer_condition = true;
            outer_or_other_cols.union_with(&refs);
        } else if refs.subset_of(inner_cols) {
            inner_is_false |= refs.is_empty() && literal_is_false(condition);
        } else {
            has_other_condition = true;
            outer_or_other_cols.union_with(&refs.intersection(outer_cols));
        }
    }

    let mut outer_non_equiv = outer_cols.clone();
    outer_non_equiv.difference_with(&outer_equiv);
    OuterJoinOptions {
        skip_rule_331: cross_equivalences == 0 || outer_or_other_cols.intersects(&outer_non_equiv),
        only_inner_filter: cross_equivalences == 0 && !has_outer_condition && !has_other_condition,
        inner_is_false,
    }
}

fn equality_offsets(
    expr: &tidb_ast::Expr,
    resolve: &impl Fn(&[String]) -> Option<i64>,
) -> Option<(i64, i64)> {
    let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::Eq, lhs, rhs) = strip(expr) else {
        return None;
    };
    let tidb_ast::Expr::Column(left) = strip(lhs) else {
        return None;
    };
    let tidb_ast::Expr::Column(right) = strip(rhs) else {
        return None;
    };
    Some((resolve(left)?, resolve(right)?))
}

fn literal_is_false(expr: &tidb_ast::Expr) -> bool {
    match strip(expr) {
        tidb_ast::Expr::Bool(value) => !value,
        tidb_ast::Expr::Int(value) => value.parse::<i128>().is_ok_and(|value| value == 0),
        _ => false,
    }
}

/// Go `DataSource.ExtractFD` for one source, translated to scope offsets.
fn table_fd_set(table: &FromTable) -> FdSet {
    let mut fds = FdSet::new();
    let id = |local: usize| {
        i64::try_from(table.offset + local).expect("scope column offset fits source int")
    };
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
    let mut not_null = ColSet::default();
    for condition in &conditions {
        for path in super::only_full_group_by::bare_columns(condition) {
            let Some(offset) = resolve(&path) else {
                continue;
            };
            if null_reject::is_null_rejected(condition, offset, &resolve) {
                not_null
                    .insert(i64::try_from(offset).expect("scope column offset fits source int"));
            }
        }
    }

    // Go `ExtractConstantCols` / `ExtractEquivalenceCols`: `col = <literal>`
    // fixes a column, `col = col` equates two.
    let mut constants = ColSet::default();
    let mut equivalences: Vec<(i64, i64)> = Vec::new();
    for condition in &conditions {
        let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::Eq, lhs, rhs) = strip(condition) else {
            continue;
        };
        match (column_offset(lhs), column_offset(rhs)) {
            (Some(left), Some(right)) => equivalences.push((
                i64::try_from(left).expect("scope column offset fits source int"),
                i64::try_from(right).expect("scope column offset fits source int"),
            )),
            (Some(left), None) if is_literal(strip(rhs)) => {
                constants.insert(i64::try_from(left).expect("scope column offset fits source int"))
            }
            (None, Some(right)) if is_literal(strip(lhs)) => {
                constants.insert(i64::try_from(right).expect("scope column offset fits source int"))
            }
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
