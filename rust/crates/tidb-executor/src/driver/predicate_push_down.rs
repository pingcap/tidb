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

use std::collections::{BTreeMap, BTreeSet};

use tidb_ast::{
    BinaryOp, Expr, Join, JoinNode, JoinType, TableRef, FLAG_HAS_AGGREGATE_FUNC, FLAG_HAS_SUBQUERY,
    FLAG_HAS_VARIABLE, FLAG_HAS_WINDOW_FUNC,
};
use tidb_datatype::{FieldType, FieldTypeCode};

use super::{
    catalog::split_table_path,
    from::{FromScope, ScopeResolver},
    Catalog,
};
use tidb_expr::rewriter::ColumnResolver;

/// The `WHERE` conjuncts an enclosing `SELECT` offers to the joins below it.
///
/// Empty for every caller that has no `WHERE` to offer -- a subquery built
/// through [`super::from::build_join`] directly, or a `FROM` with no filter.
pub(crate) type Offered<'a> = &'a [Expr];

/// Predicates each base-table leaf may evaluate before its parent join.
#[derive(Default)]
pub(crate) struct Plan {
    filters: BTreeMap<String, Vec<Expr>>,
}

impl Plan {
    pub(crate) fn filters_for(&self, table: &TableRef) -> &[Expr] {
        let qualifier = table
            .alias
            .as_ref()
            .or_else(|| table.name.last())
            .map(|name| name.to_ascii_lowercase());
        qualifier
            .as_ref()
            .and_then(|name| self.filters.get(name))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

#[derive(Clone)]
struct Binding {
    qualifier: String,
    columns: Vec<(String, FieldType)>,
}

/// Builds Go's child-condition distribution for the join tree.
pub(crate) fn plan(
    join: &Join,
    where_clause: Option<&Expr>,
    catalog: &Catalog,
    current_db: &str,
) -> Plan {
    let Some(bindings) = bindings(&JoinNode::Join(Box::new(join.clone())), catalog, current_db)
    else {
        return Plan::default();
    };
    let inherited = where_clause.map(extracted_conjuncts).unwrap_or_default();
    let mut plan = Plan::default();
    distribute_join(join, &inherited, &bindings, catalog, current_db, &mut plan);
    plan
}

fn distribute_join(
    join: &Join,
    inherited: &[Expr],
    bindings: &[Binding],
    catalog: &Catalog,
    current_db: &str,
    plan: &mut Plan,
) {
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        distribute_node(&join.left, inherited, bindings, catalog, current_db, plan);
        return;
    }
    let Some(right) = &join.right else {
        return;
    };
    let Some(left_names) = relation_names(&join.left, catalog, current_db) else {
        return;
    };
    let Some(right_names) = relation_names(right, catalog, current_db) else {
        return;
    };
    let mut left = Vec::new();
    let mut right_filters = Vec::new();
    let mut on = Vec::new();
    if let Some(condition) = &join.on {
        collect_conjuncts(condition, &mut on);
    }

    match join.tp {
        JoinType::Cross => {
            for condition in inherited.iter().chain(&on) {
                derive_for_sides(
                    condition,
                    &left_names,
                    &right_names,
                    bindings,
                    true,
                    true,
                    &mut left,
                    &mut right_filters,
                );
            }
        }
        JoinType::Left => {
            for condition in inherited {
                derive_for_sides(
                    condition,
                    &left_names,
                    &right_names,
                    bindings,
                    true,
                    false,
                    &mut left,
                    &mut right_filters,
                );
            }
            for condition in &on {
                derive_for_sides(
                    condition,
                    &left_names,
                    &right_names,
                    bindings,
                    false,
                    true,
                    &mut left,
                    &mut right_filters,
                );
            }
        }
        JoinType::Right => {
            for condition in inherited {
                derive_for_sides(
                    condition,
                    &left_names,
                    &right_names,
                    bindings,
                    false,
                    true,
                    &mut left,
                    &mut right_filters,
                );
            }
            for condition in &on {
                derive_for_sides(
                    condition,
                    &left_names,
                    &right_names,
                    bindings,
                    true,
                    false,
                    &mut left,
                    &mut right_filters,
                );
            }
        }
    }
    dedup(&mut left);
    dedup(&mut right_filters);
    distribute_node(&join.left, &left, bindings, catalog, current_db, plan);
    distribute_node(right, &right_filters, bindings, catalog, current_db, plan);
}

fn distribute_node(
    node: &JoinNode,
    inherited: &[Expr],
    bindings: &[Binding],
    catalog: &Catalog,
    current_db: &str,
    plan: &mut Plan,
) {
    match node {
        JoinNode::Table(table) => {
            let qualifier = table
                .alias
                .as_ref()
                .or_else(|| table.name.last())
                .map(|name| name.to_ascii_lowercase());
            if let Some(qualifier) = qualifier {
                let filters = plan.filters.entry(qualifier).or_default();
                filters.extend_from_slice(inherited);
                dedup(filters);
            }
        }
        JoinNode::Join(join) => {
            distribute_join(join, inherited, bindings, catalog, current_db, plan)
        }
        // Pushing through a projection requires rewriting its output columns
        // to defining expressions. Leave that separate rule at its boundary.
        JoinNode::Derived { .. } => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn derive_for_sides(
    condition: &Expr,
    left_names: &BTreeSet<String>,
    right_names: &BTreeSet<String>,
    bindings: &[Binding],
    derive_left: bool,
    derive_right: bool,
    left: &mut Vec<Expr>,
    right: &mut Vec<Expr>,
) {
    if !safe_to_duplicate(condition) {
        return;
    }
    let side = expression_side(condition, left_names, right_names, bindings);
    match side {
        Side::Left if derive_left => left.push(condition.clone()),
        Side::Right if derive_right => right.push(condition.clone()),
        Side::Both => {
            if derive_left {
                if let Some(filter) = relaxed_dnf(condition, left_names, bindings) {
                    left.push(filter);
                }
                derive_not_null(condition, left_names, bindings, left);
            }
            if derive_right {
                if let Some(filter) = relaxed_dnf(condition, right_names, bindings) {
                    right.push(filter);
                }
                derive_not_null(condition, right_names, bindings, right);
            }
        }
        Side::Left | Side::Right | Side::Foreign => {}
    }
}

/// Whether copying a predicate below a join preserves its observable value.
///
/// Go refuses mutable-effects and non-deterministic scalar functions before
/// predicate distribution. Variables and subqueries are rejected at the AST
/// boundary as well: they can change between evaluations or own execution
/// state, so evaluating the original predicate plus a pushed copy is unsound.
pub(crate) fn safe_to_duplicate(expr: &Expr) -> bool {
    const UNSAFE_FLAGS: u64 =
        FLAG_HAS_AGGREGATE_FUNC | FLAG_HAS_SUBQUERY | FLAG_HAS_VARIABLE | FLAG_HAS_WINDOW_FUNC;
    if expr.flags() & UNSAFE_FLAGS != 0 {
        return false;
    }

    struct MutableCall(bool);
    impl tidb_ast::Visitor for MutableCall {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr) = node.downcast_ref::<Expr>() {
                match expr {
                    Expr::Func { name, .. } => {
                        let name = name.to_ascii_lowercase();
                        self.0 |= super::through_proj::is_mutable_effects(&name)
                            || super::through_proj::is_unfoldable(&name);
                    }
                    // A schema-qualified function has no builtin purity
                    // contract in this tier, so fail closed.
                    Expr::GenericFuncCall { .. } => self.0 = true,
                    _ => {}
                }
            }
            self.0
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            !self.0
        }
    }

    let mut check = MutableCall(false);
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut check);
    !check.0
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
    Both,
    Foreign,
}

fn expression_side(
    expr: &Expr,
    left: &BTreeSet<String>,
    right: &BTreeSet<String>,
    bindings: &[Binding],
) -> Side {
    let paths = column_paths(expr);
    if paths.is_empty() {
        return Side::Foreign;
    }
    let mut in_left = false;
    let mut in_right = false;
    for path in paths {
        let Some(binding) = resolve_binding(&path, bindings) else {
            return Side::Foreign;
        };
        in_left |= left.contains(&binding.qualifier.to_ascii_lowercase());
        in_right |= right.contains(&binding.qualifier.to_ascii_lowercase());
    }
    match (in_left, in_right) {
        (true, false) => Side::Left,
        (false, true) => Side::Right,
        (true, true) => Side::Both,
        _ => Side::Foreign,
    }
}

fn relaxed_dnf(expr: &Expr, target: &BTreeSet<String>, bindings: &[Binding]) -> Option<Expr> {
    let mut terms = Vec::new();
    flatten(expr, BinaryOp::LogicOr, &mut terms);
    if terms.len() < 2 {
        return None;
    }
    let mut relaxed_terms = Vec::new();
    for term in terms {
        let mut conjuncts = Vec::new();
        flatten(term, BinaryOp::LogicAnd, &mut conjuncts);
        let mut kept = Vec::new();
        for conjunct in conjuncts {
            let mut nested = Vec::new();
            flatten(conjunct, BinaryOp::LogicOr, &mut nested);
            if nested.len() > 1 {
                if let Some(relaxed) = relaxed_dnf(conjunct, target, bindings) {
                    kept.push(relaxed);
                }
            } else if expression_in(conjunct, target, bindings) {
                kept.push(conjunct.clone());
            }
        }
        if kept.is_empty() {
            return None;
        }
        if kept.len() == 1 {
            let only = kept.pop().expect("one relaxed conjunct");
            let mut nested_terms = Vec::new();
            flatten(&only, BinaryOp::LogicOr, &mut nested_terms);
            if nested_terms.len() > 1 {
                relaxed_terms.extend(nested_terms.into_iter().cloned());
            } else {
                relaxed_terms.push(only);
            }
        } else {
            relaxed_terms.push(compose(BinaryOp::LogicAnd, kept));
        }
    }
    Some(compose(BinaryOp::LogicOr, relaxed_terms))
}

fn expression_in(expr: &Expr, target: &BTreeSet<String>, bindings: &[Binding]) -> bool {
    let paths = column_paths(expr);
    !paths.is_empty()
        && paths.iter().all(|path| {
            resolve_binding(path, bindings)
                .is_some_and(|binding| target.contains(&binding.qualifier.to_ascii_lowercase()))
        })
}

fn derive_not_null(
    condition: &Expr,
    target: &BTreeSet<String>,
    bindings: &[Binding],
    out: &mut Vec<Expr>,
) {
    let Expr::Binary(_, lhs, rhs) = strip_parens(condition) else {
        return;
    };
    let (Expr::Column(left), Expr::Column(right)) = (strip_parens(lhs), strip_parens(rhs)) else {
        return;
    };
    if !super::funcdep::null_reject::is_null_rejected_by(condition, &|path| {
        resolve_binding(path, bindings)
            .is_some_and(|binding| target.contains(&binding.qualifier.to_ascii_lowercase()))
    }) {
        return;
    }
    for path in [left, right] {
        let Some(binding) = resolve_binding(path, bindings) else {
            continue;
        };
        if !target.contains(&binding.qualifier.to_ascii_lowercase()) {
            continue;
        }
        let Some(column) = path.last() else { continue };
        let nullable = binding.columns.iter().any(|(name, field_type)| {
            name.eq_ignore_ascii_case(column) && field_type.flags() & 1 == 0
        });
        if nullable {
            out.push(Expr::Is {
                expr: Box::new(Expr::Column(vec![
                    binding.qualifier.clone(),
                    column.clone(),
                ])),
                target: tidb_ast::IsTarget::Null,
                not: true,
            });
        }
    }
}

fn relation_names(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
) -> Option<BTreeSet<String>> {
    Some(
        bindings(node, catalog, current_db)?
            .into_iter()
            .map(|binding| binding.qualifier.to_ascii_lowercase())
            .collect(),
    )
}

fn bindings(node: &JoinNode, catalog: &Catalog, current_db: &str) -> Option<Vec<Binding>> {
    match node {
        JoinNode::Table(table) => {
            let (database, name) = split_table_path(&table.name, current_db).ok()?;
            let qualifier = table.alias.clone().unwrap_or_else(|| name.to_owned());
            let mut columns = catalog.get_in(database, name)?.column_list();
            for (column, field_type) in &mut columns {
                if super::merge_decision::physical_column_is_nullable(
                    node,
                    &super::merge_decision::RelColumn {
                        relation: qualifier.clone(),
                        column: column.clone(),
                    },
                    catalog,
                    current_db,
                ) == Some(false)
                {
                    field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                }
            }
            Some(vec![Binding { qualifier, columns }])
        }
        JoinNode::Join(join) => {
            let mut result = bindings(&join.left, catalog, current_db)?;
            if let Some(right) = &join.right {
                result.extend(bindings(right, catalog, current_db)?);
            }
            Some(result)
        }
        JoinNode::Derived {
            subquery,
            alias: Some(alias),
            column_names,
            ..
        } => {
            let mut names = super::from::derived_field_names_query(subquery)?;
            if !column_names.is_empty() {
                if column_names.len() != names.len() {
                    return None;
                }
                names.clone_from(column_names);
            }
            // Side classification needs the derived relation's namespace even
            // when this rule cannot push through its projection. Keep output
            // types nullable here; a later physical boundary owns exact type
            // inference and may safely eliminate redundant NOT NULL demands.
            let columns = names
                .into_iter()
                .map(|name| (name, FieldType::new(FieldTypeCode::LongLong)))
                .collect();
            Some(vec![Binding {
                qualifier: alias.clone(),
                columns,
            }])
        }
        JoinNode::Derived { alias: None, .. } => None,
    }
}

fn resolve_binding<'a>(path: &[String], bindings: &'a [Binding]) -> Option<&'a Binding> {
    let column = path.last()?;
    let candidates: Vec<&Binding> = if path.len() >= 2 {
        let qualifier = &path[path.len() - 2];
        bindings
            .iter()
            .filter(|binding| {
                binding.qualifier.eq_ignore_ascii_case(qualifier)
                    && binding
                        .columns
                        .iter()
                        .any(|(name, _)| name.eq_ignore_ascii_case(column))
            })
            .collect()
    } else {
        bindings
            .iter()
            .filter(|binding| {
                binding
                    .columns
                    .iter()
                    .any(|(name, _)| name.eq_ignore_ascii_case(column))
            })
            .collect()
    };
    if candidates.len() == 1 {
        Some(candidates[0])
    } else {
        None
    }
}

fn column_paths(expr: &Expr) -> Vec<Vec<String>> {
    struct Collect(Vec<Vec<String>>);
    impl tidb_ast::Visitor for Collect {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() {
                // Go's `ScalarSubQueryExpr` embeds `expression.Constant`, so
                // `ExtractColumns` does not assign it to either join child.
                // Rust retains a scoped pseudo-column for resolution and
                // EXPLAIN identity; keep that representation out of the
                // physical relation set as well.
                if !matches!(path.as_slice(), [scope, _] if scope == super::from::SCALAR_QUERY_SCOPE)
                {
                    self.0.push(path.clone());
                }
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut collect = Collect(Vec::new());
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut collect);
    collect.0
}

fn collect_conjuncts(expr: &Expr, out: &mut Vec<Expr>) {
    let mut borrowed = Vec::new();
    flatten(expr, BinaryOp::LogicAnd, &mut borrowed);
    out.extend(borrowed.into_iter().cloned());
}

/// Go's expression rewriter lowers `BETWEEN` before logical predicate
/// pushdown runs. Preserve that ordering so a bound shared by every DNF branch
/// is visible to [`extract_filters_from_dnfs`].
fn expand_between(expr: &Expr) -> Expr {
    struct ExpandBetween;

    impl tidb_ast::Visitor for ExpandBetween {
        fn enter(&mut self, _node: &mut dyn std::any::Any) -> bool {
            false
        }

        fn leave(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expression) = node.downcast_mut::<Expr>() else {
                return true;
            };
            let Expr::Between {
                expr,
                low,
                high,
                not,
            } = expression
            else {
                return true;
            };
            let bounds = Expr::Binary(
                BinaryOp::LogicAnd,
                Box::new(Expr::Binary(
                    BinaryOp::Ge,
                    Box::new((**expr).clone()),
                    Box::new((**low).clone()),
                )),
                Box::new(Expr::Binary(
                    BinaryOp::Le,
                    Box::new((**expr).clone()),
                    Box::new((**high).clone()),
                )),
            );
            *expression = if *not {
                Expr::Unary(tidb_ast::UnaryOp::NotKeyword, Box::new(bounds))
            } else {
                bounds
            };
            true
        }
    }

    let mut expanded = expr.clone();
    tidb_ast::Visitable::accept(&mut expanded, &mut ExpandBetween);
    expanded
}

fn flatten<'a>(expr: &'a Expr, op: BinaryOp, out: &mut Vec<&'a Expr>) {
    match strip_parens(expr) {
        Expr::Binary(found, left, right) if *found == op => {
            flatten(left, op, out);
            flatten(right, op, out);
        }
        other => out.push(other),
    }
}

pub(crate) fn compose(op: BinaryOp, expressions: Vec<Expr>) -> Expr {
    fn balanced(op: BinaryOp, expressions: &[Expr]) -> Expr {
        match expressions {
            [only] => only.clone(),
            many => {
                let middle = many.len() / 2;
                Expr::Binary(
                    op,
                    Box::new(balanced(op, &many[..middle])),
                    Box::new(balanced(op, &many[middle..])),
                )
            }
        }
    }
    assert!(!expressions.is_empty(), "a derived predicate is nonempty");
    balanced(op, &expressions)
}

/// Go `expression.ExtractFiltersFromDNFs`: lift every CNF item present in
/// every branch of a DNF, and leave the branch-specific residue as one OR.
fn extract_filters_from_dnfs(mut conditions: Vec<Expr>) -> Vec<Expr> {
    let mut extracted = Vec::new();
    for index in (0..conditions.len()).rev() {
        let mut branches = Vec::new();
        flatten(&conditions[index], BinaryOp::LogicOr, &mut branches);
        if branches.len() < 2 {
            continue;
        }

        let mut common = Vec::new();
        flatten(branches[0], BinaryOp::LogicAnd, &mut common);
        let mut unique = Vec::with_capacity(common.len());
        for candidate in common {
            if !unique.contains(&candidate) {
                unique.push(candidate);
            }
        }
        let mut common = unique;
        common.retain(|candidate| {
            branches[1..].iter().all(|branch| {
                let mut conjuncts = Vec::new();
                flatten(branch, BinaryOp::LogicAnd, &mut conjuncts);
                conjuncts.contains(candidate)
            })
        });
        if common.is_empty() {
            continue;
        }

        let mut only_extracted = false;
        let mut residual_branches = Vec::with_capacity(branches.len());
        for branch in branches {
            let mut conjuncts = Vec::new();
            flatten(branch, BinaryOp::LogicAnd, &mut conjuncts);
            let residual = conjuncts
                .into_iter()
                .filter(|conjunct| !common.contains(conjunct))
                .cloned()
                .collect::<Vec<_>>();
            if residual.is_empty() {
                only_extracted = true;
                break;
            }
            residual_branches.push(compose(BinaryOp::LogicAnd, residual));
        }

        let common = common.into_iter().cloned().collect::<Vec<_>>();
        if only_extracted {
            conditions.remove(index);
        } else {
            conditions[index] = compose(BinaryOp::LogicOr, residual_branches);
        }
        extracted.extend(common);
    }
    conditions.extend(extracted);
    conditions
}

/// The logical CNF conditions Go exposes after common DNF filters are
/// extracted. The source WHERE remains intact above the join.
pub(crate) fn extracted_conjuncts(expr: &Expr) -> Vec<Expr> {
    let expanded = expand_between(expr);
    let mut conditions = Vec::new();
    collect_conjuncts(&expanded, &mut conditions);
    extract_filters_from_dnfs(conditions)
}

/// One display/selectivity expression for a leaf's condition list.
pub(crate) fn combined(expressions: &[Expr]) -> Option<Expr> {
    (!expressions.is_empty()).then(|| compose(BinaryOp::LogicAnd, expressions.to_vec()))
}

/// Exact pseudo-row factor for a list containing only derived `IS NOT NULL`
/// filters. Go's range estimate removes one pseudo-equal bucket per nullable
/// key, rather than applying the generic selection factor.
pub(crate) fn derived_not_null_rate(expressions: &[Expr]) -> Option<f64> {
    let all_not_null = expressions.iter().all(|expression| {
        matches!(
            strip_parens(expression),
            Expr::Is {
                target: tidb_ast::IsTarget::Null,
                not: true,
                ..
            }
        )
    });
    all_not_null.then(|| (1.0 - 1.0 / 1000.0_f64).powi(expressions.len() as i32))
}

fn strip_parens(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) => strip_parens(inner),
        other => other,
    }
}

fn dedup(expressions: &mut Vec<Expr>) {
    let mut unique = Vec::with_capacity(expressions.len());
    for expression in expressions.drain(..) {
        if !unique.contains(&expression) {
            unique.push(expression);
        }
    }
    *expressions = unique;
}

/// Splits `select`'s `WHERE` into the conjuncts eligible for pushdown.
pub(crate) fn offered_conjuncts(where_clause: Option<&Expr>) -> Vec<Expr> {
    let Some(expr) = where_clause else {
        return Vec::new();
    };
    let mut conjuncts = extracted_conjuncts(expr);
    conjuncts.retain(|conjunct| column_equality(conjunct).is_some());
    conjuncts
}

/// The two column paths of a `col = col`, or `None` for anything else.
///
/// Parentheses are stripped from both sides. Go has no parenthesis node in an
/// `expression.Expression` at all -- `ast.ParenthesesExpr` is unwrapped while
/// the expression is rewritten, so every rule downstream sees the bare column
/// -- and this tier keeps `Expr::Paren` in the tree instead, which makes
/// seeing through it each matcher's own job.
///
/// Not stripping here was a WRONG-ANSWER bug rather than a lost optimization.
/// `join_reorder::classify` already strips, so `WHERE (a40)=b14` classified as
/// an `Edge`: dropped from the residual `WHERE` because the join was expected
/// to run it, while this matcher refused to recognize it and the join never
/// installed it. The predicate then executed NOWHERE, and
/// `SELECT ... FROM t35,t40,t14 WHERE (a40)=b14` returned the whole cross
/// product. The two matchers have to agree on what an equality is.
fn column_equality(expr: &Expr) -> Option<(&[String], &[String])> {
    match strip_parens(expr) {
        Expr::Binary(BinaryOp::Eq, lhs, rhs) => match (strip_parens(lhs), strip_parens(rhs)) {
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
        .collect()
}
