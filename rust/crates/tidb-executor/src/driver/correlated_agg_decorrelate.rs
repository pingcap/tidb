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

//! Decorrelation of equality-correlated scalar aggregations.
//!
//! Go's `DecorrelateSolver` has two aggregation arms.  When an outer source
//! has a non-null unique key, the first scalar aggregation can be pulled above
//! a left join: the unique key becomes `GROUP BY`, outer values become
//! `FIRST_ROW`, and the scalar aggregate keeps its empty-input NULL through
//! the left join.  Once the outer side is itself aggregated, later scalar
//! aggregations stay below the join: their correlation keys are appended to
//! their own grouping and the Apply becomes a left join to that grouped
//! relation.
//!
//! This module transcribes those two arms over the AST because this executor
//! does not retain a separate logical-plan tree.  Its acceptance boundary is
//! deliberately proof-shaped:
//!
//! * one base-table outer source with a non-null primary/unique key;
//! * selected outer values are bare columns;
//! * each rewritten field is exactly one non-distinct `SUM(column)` scalar
//!   subquery;
//! * every correlation is a column equality, and removing those equalities
//!   leaves no outer reference in the subquery;
//! * the inner `FROM` contains inner joins only.
//!
//! Any clause outside that boundary leaves the statement byte-for-byte
//! unchanged.  In particular, no LIMIT, HAVING, DISTINCT, window, locking,
//! volatile expression, nullable unique key, or non-equality correlation is
//! guessed at.

use std::collections::{BTreeMap, BTreeSet};

use tidb_ast::{
    BinaryOp, Expr, GroupByItem, Join, JoinNode, JoinType, QueryStmt, SelectField, SelectStmt,
    TableRef,
};
use tidb_datatype::FieldTypeFlags;
use tidb_expr::rewriter::ColumnResolver;

use super::catalog::{Catalog, TableEntry};
use super::from::ScopeResolver;

/// Rewrites every eligible derived SELECT first, then the current SELECT.
/// Returning `None` means no node in the tree changed.
pub(crate) fn rewrite(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<SelectStmt> {
    let mut rewritten = select.clone();
    let mut changed = rewritten
        .from
        .as_mut()
        .is_some_and(|from| rewrite_join(from, catalog, current_db, ctx));
    if let Some(current) = rewrite_current(&rewritten, catalog, current_db, ctx) {
        rewritten = current;
        changed = true;
    }
    changed.then_some(rewritten)
}

/// Whether later optimizer rules are looking at the grouped left-join form
/// produced by this module. Derived-table fusion may move that form into a
/// caller after [`rewrite`] returned, so the driver recognizes the invariant
/// shape rather than relying on a transient boolean from the first pass.
pub(crate) fn is_pulled_scalar_sum(select: &SelectStmt) -> bool {
    !select.group_by.is_empty()
        && select
            .from
            .as_ref()
            .is_some_and(|join| join.tp == JoinType::Left)
        && select.fields.fields().iter().any(|field| {
            matches!(
                field,
                SelectField::Expr {
                    expr: Expr::Aggregate { name, .. },
                    ..
                } if name.eq_ignore_ascii_case("SUM")
            )
        })
        && select.fields.fields().iter().all(|field| match field {
            SelectField::Expr { expr, .. } => !super::subquery::expr_has_subquery(expr),
            SelectField::Wildcard(_) => false,
        })
}

fn rewrite_join(
    join: &mut Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    let mut changed = rewrite_node(&mut join.left, catalog, current_db, ctx);
    if let Some(right) = &mut join.right {
        changed |= rewrite_node(right, catalog, current_db, ctx);
    }
    changed
}

fn rewrite_node(
    node: &mut JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    match node {
        JoinNode::Derived { subquery, .. } => {
            let QueryStmt::Select(select) = &mut **subquery else {
                return false;
            };
            let Some(rewritten) = rewrite(select, catalog, current_db, ctx) else {
                return false;
            };
            **select = rewritten;
            true
        }
        JoinNode::Join(join) => rewrite_join(join, catalog, current_db, ctx),
        JoinNode::Table(_) => false,
    }
}

#[derive(Clone)]
struct ScalarSum {
    field_index: usize,
    output_name: String,
    output_alias: Option<String>,
    sum: Expr,
    inner: SelectStmt,
    local_conditions: Vec<Expr>,
    correlations: Vec<Correlation>,
}

#[derive(Clone)]
struct Correlation {
    inner: Vec<String>,
    outer_offset: usize,
}

fn rewrite_current(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<SelectStmt> {
    if !plain_outer(select) {
        return None;
    }
    let outer_ref = single_table_ref(select.from.as_ref()?)?;
    let outer_scope = super::subquery::select_outer_scope(select, catalog, current_db, ctx);
    if outer_scope.tables.len() != 1 {
        return None;
    }
    let outer_resolver = ScopeResolver {
        scope: &outer_scope,
    };
    let field_names = super::from::derived_field_names(select)?;

    let mut output_by_offset = BTreeMap::new();
    let mut sums = Vec::new();
    for (field_index, field) in select.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr, alias } = field else {
            return None;
        };
        if let Some(sum) = scalar_sum(
            expr,
            field_index,
            &field_names[field_index],
            alias,
            &outer_scope,
            catalog,
            current_db,
            ctx,
        ) {
            sums.push(sum);
            continue;
        }
        if super::subquery::expr_has_subquery(expr) {
            return None;
        }
        let Expr::Column(path) = expr else {
            return None;
        };
        let (offset, _, _) = outer_resolver.resolve(path)?;
        if output_by_offset
            .insert(offset, field_names[field_index].clone())
            .is_some()
        {
            return None;
        }
    }
    if sums.is_empty() {
        return None;
    }

    let table = table_entry(outer_ref, catalog, current_db)?;
    let TableEntry::Kv(table) = table else {
        return None;
    };
    let unique_key = non_null_unique_key(table)?;
    let first = &sums[0];
    let mut group_offsets = first
        .correlations
        .iter()
        .map(|correlation| correlation.outer_offset)
        .collect::<Vec<_>>();
    for offset in unique_key {
        if !group_offsets.contains(&offset) {
            group_offsets.push(offset);
        }
    }
    let outer_visible = outer_ref
        .alias
        .as_deref()
        .or_else(|| outer_ref.name.last().map(String::as_str))?;
    let outer_columns = table.visible_columns();
    let group_by = group_offsets
        .iter()
        .map(|offset| GroupByItem {
            expr: Expr::Column(vec![
                outer_visible.to_owned(),
                outer_columns[*offset].name.clone(),
            ]),
            desc: None,
        })
        .collect::<Vec<_>>();

    // The first Apply is pulled above a left join and aggregation.
    let mut inner_from = first.inner.from.clone()?;
    let residual_local =
        attach_to_inner_join(&mut inner_from, combine_and(first.local_conditions.clone()));
    let mut outer_on = first
        .correlations
        .iter()
        .map(|correlation| {
            Expr::Binary(
                BinaryOp::Eq,
                Box::new(Expr::Column(vec![
                    outer_visible.to_owned(),
                    outer_columns[correlation.outer_offset].name.clone(),
                ])),
                Box::new(Expr::Column(correlation.inner.clone())),
            )
        })
        .collect::<Vec<_>>();
    if let Some(residual) = residual_local {
        outer_on.push(residual);
    }
    let left = join_node(select.from.clone()?);
    let right = join_node(inner_from);
    let mut pulled = select.clone();
    pulled.from = Some(Join {
        left,
        right: Some(right),
        tp: JoinType::Left,
        straight: false,
        on: combine_and(outer_on),
        using: Vec::new(),
        natural: false,
        explicit_parens: false,
    });
    pulled.group_by = group_by;
    let later = sums
        .iter()
        .skip(1)
        .map(|sum| sum.field_index)
        .collect::<BTreeSet<_>>();
    let mut pulled_fields = Vec::new();
    for (index, field) in select.fields.fields().iter().enumerate() {
        if later.contains(&index) {
            continue;
        }
        if index == first.field_index {
            pulled_fields.push(SelectField::Expr {
                expr: first.sum.clone(),
                alias: first.output_alias.clone(),
            });
        } else {
            pulled_fields.push(field.clone());
        }
    }
    pulled.fields = pulled_fields.into();

    if sums.len() == 1 {
        return Some(pulled);
    }

    // Later Applies see an already-aggregated outer side. Their own SUM stays
    // below the join, grouped by the inner correlation keys.
    let mut current = pulled;
    let mut available = select
        .fields
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, _)| (!later.contains(&index)).then_some(index))
        .collect::<BTreeSet<_>>();
    for (round, sum) in sums.iter().skip(1).enumerate() {
        let outer_alias = format!("__decorrelated_outer_{round}");
        let inner_alias = format!("__decorrelated_sum_{round}");
        let mut grouped_inner = sum.inner.clone();
        grouped_inner.where_clause = combine_and(sum.local_conditions.clone());
        grouped_inner.group_by = sum
            .correlations
            .iter()
            .map(|correlation| GroupByItem {
                expr: Expr::Column(correlation.inner.clone()),
                desc: None,
            })
            .collect();
        let mut inner_fields = vec![SelectField::Expr {
            expr: sum.sum.clone(),
            alias: Some(sum.output_name.clone()),
        }];
        inner_fields.extend(
            sum.correlations
                .iter()
                .map(|correlation| SelectField::Expr {
                    expr: Expr::Column(correlation.inner.clone()),
                    alias: correlation.inner.last().cloned(),
                }),
        );
        grouped_inner.fields = inner_fields.into();

        let on = combine_and(
            sum.correlations
                .iter()
                .map(|correlation| {
                    let outer_name = output_by_offset.get(&correlation.outer_offset)?;
                    let inner_name = correlation.inner.last()?;
                    Some(Expr::Binary(
                        BinaryOp::Eq,
                        Box::new(Expr::Column(vec![inner_alias.clone(), inner_name.clone()])),
                        Box::new(Expr::Column(vec![outer_alias.clone(), outer_name.clone()])),
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
        );
        let from = Join {
            left: derived_node(current, &outer_alias),
            right: Some(derived_node(grouped_inner, &inner_alias)),
            tp: JoinType::Left,
            straight: false,
            on,
            using: Vec::new(),
            natural: false,
            explicit_parens: false,
        };
        available.insert(sum.field_index);
        let mut fields = Vec::with_capacity(available.len());
        for index in available.iter().copied() {
            let from_inner = index == sum.field_index;
            fields.push(SelectField::Expr {
                expr: Expr::Column(vec![
                    if from_inner {
                        inner_alias.clone()
                    } else {
                        outer_alias.clone()
                    },
                    field_names[index].clone(),
                ]),
                alias: Some(field_names[index].clone()),
            });
        }
        let mut pass_through = select.clone();
        pass_through.fields = fields.into();
        pass_through.from = Some(from);
        pass_through.where_clause = None;
        current = pass_through;
    }
    Some(current)
}

#[allow(clippy::too_many_arguments)]
fn scalar_sum(
    expr: &Expr,
    field_index: usize,
    output_name: &str,
    output_alias: &Option<String>,
    outer_scope: &super::from::FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<ScalarSum> {
    let Expr::Subquery(query) = expr else {
        return None;
    };
    let QueryStmt::Select(inner) = &**query else {
        return None;
    };
    if !plain_scalar_inner(inner) || !inner_joins_only(inner.from.as_ref()?) {
        return None;
    }
    let [SelectField::Expr {
        expr:
            sum @ Expr::Aggregate {
                name,
                distinct: false,
                args,
            },
        ..
    }] = inner.fields.fields()
    else {
        return None;
    };
    if !name.eq_ignore_ascii_case("SUM") || !matches!(args.as_slice(), [Expr::Column(_)]) {
        return None;
    }
    let inner_scope = super::subquery::select_outer_scope(inner, catalog, current_db, ctx);
    let inner_resolver = ScopeResolver {
        scope: &inner_scope,
    };
    let outer_resolver = ScopeResolver { scope: outer_scope };
    let mut correlations = Vec::new();
    let mut local_conditions = Vec::new();
    for conjunct in conjuncts(inner.where_clause.as_ref()?) {
        if let Some((inner_path, outer_path)) =
            correlation_equality(conjunct, &inner_resolver, &outer_resolver)
        {
            let (outer_offset, _, _) = outer_resolver.resolve(&outer_path)?;
            correlations.push(Correlation {
                inner: inner_path,
                outer_offset,
            });
        } else {
            local_conditions.push(normalize_inner_expression(
                conjunct,
                &inner_resolver,
                &inner_scope,
            )?);
        }
    }
    if correlations.is_empty() {
        return None;
    }
    let mut uncorrelated = (**inner).clone();
    uncorrelated.where_clause = combine_and(local_conditions.clone());
    let mut remaining = Vec::new();
    super::subquery::collect_correlated_columns_query(
        &QueryStmt::Select(Box::new(uncorrelated)),
        outer_scope,
        catalog,
        current_db,
        &mut remaining,
        ctx,
    );
    if !remaining.is_empty() {
        return None;
    }
    // The SUM argument itself must belong to the inner row.
    let Expr::Aggregate { args, .. } = sum else {
        unreachable!()
    };
    let [Expr::Column(argument)] = args.as_slice() else {
        unreachable!()
    };
    inner_resolver.resolve(argument)?;
    Some(ScalarSum {
        field_index,
        output_name: output_name.to_owned(),
        output_alias: output_alias.clone(),
        sum: sum.clone(),
        inner: (**inner).clone(),
        local_conditions,
        correlations,
    })
}

/// Replaces parser spelling with the resolved source spelling Go's logical
/// expression carries after name resolution. This affects only plan text;
/// identifier matching and runtime semantics remain case-insensitive.
fn normalize_inner_expression(
    expression: &Expr,
    resolver: &ScopeResolver<'_>,
    scope: &super::from::FromScope,
) -> Option<Expr> {
    struct Normalize<'a> {
        resolver: &'a ScopeResolver<'a>,
        scope: &'a super::from::FromScope,
        valid: bool,
    }
    impl tidb_ast::Visitor for Normalize<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_mut::<Expr>() {
                let Some((offset, _, _)) = self.resolver.resolve(path) else {
                    self.valid = false;
                    return false;
                };
                let Some(qualified) = self.scope.qualified_path(offset) else {
                    self.valid = false;
                    return false;
                };
                *path = qualified;
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut normalized = expression.clone();
    let mut visitor = Normalize {
        resolver,
        scope,
        valid: true,
    };
    tidb_ast::Visitable::accept(&mut normalized, &mut visitor);
    visitor.valid.then_some(normalized)
}

fn correlation_equality(
    expr: &Expr,
    inner: &ScopeResolver<'_>,
    outer: &ScopeResolver<'_>,
) -> Option<(Vec<String>, Vec<String>)> {
    let Expr::Binary(BinaryOp::Eq, left, right) = expr else {
        return None;
    };
    let Expr::Column(left) = &**left else {
        return None;
    };
    let Expr::Column(right) = &**right else {
        return None;
    };
    let classify = |path: &[String]| (inner.resolve(path).is_some(), outer.resolve(path).is_some());
    match (classify(left), classify(right)) {
        ((true, false), (false, true)) => Some((left.clone(), right.clone())),
        ((false, true), (true, false)) => Some((right.clone(), left.clone())),
        _ => None,
    }
}

fn plain_outer(select: &SelectStmt) -> bool {
    select.with.is_none()
        && select.hints.is_empty()
        && !select.sql_small_result
        && !select.sql_big_result
        && !select.sql_buffer_result
        && !select.sql_no_cache
        && !select.straight_join
        && !select.calc_found_rows
        && !select.distinct
        && !select.all
        && select.values.is_empty()
        && select.group_by.is_empty()
        && !select.rollup
        && select.having.is_none()
        && select.windows.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.lock.is_none()
        && select.into_outfile.is_none()
}

fn plain_scalar_inner(select: &SelectStmt) -> bool {
    select.with.is_none()
        && select.hints.is_empty()
        && !select.distinct
        && select.values.is_empty()
        && select.group_by.is_empty()
        && !select.rollup
        && select.having.is_none()
        && select.windows.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.lock.is_none()
        && select.into_outfile.is_none()
        && select.from.is_some()
        && select.where_clause.is_some()
}

fn single_table_ref(join: &Join) -> Option<&TableRef> {
    if join.right.is_some() || join.on.is_some() || !join.using.is_empty() || join.natural {
        return None;
    }
    match &join.left {
        JoinNode::Table(table) => Some(table),
        _ => None,
    }
}

fn table_entry<'a>(
    table: &TableRef,
    catalog: &'a Catalog,
    current_db: &str,
) -> Option<&'a TableEntry> {
    let name = table.name.last()?;
    let database = match table.name.as_slice() {
        [name] if !name.is_empty() => current_db,
        [database, _] => database,
        _ => return None,
    };
    catalog.get_in(database, name)
}

fn non_null_unique_key(table: &crate::KvTable) -> Option<Vec<usize>> {
    let key = table
        .pk_handle_offset()
        .map(|offset| vec![offset])
        .or_else(|| {
            (!table.common_handle_offsets().is_empty())
                .then(|| table.common_handle_offsets().to_vec())
        })
        .or_else(|| {
            table
                .indexes()
                .iter()
                .find(|index| {
                    index.unique
                        && index.name.eq_ignore_ascii_case("PRIMARY")
                        && !index.has_prefix()
                })
                .map(|index| index.column_offsets.clone())
        })
        .or_else(|| {
            table
                .indexes()
                .iter()
                .find(|index| index.unique && !index.has_prefix())
                .map(|index| index.column_offsets.clone())
        })?;
    key.iter()
        .all(|offset| {
            table
                .visible_columns()
                .get(*offset)
                .is_some_and(|column| column.field_type.has_flag(FieldTypeFlags::NOT_NULL))
        })
        .then_some(key)
}

fn inner_joins_only(join: &Join) -> bool {
    if join.tp != JoinType::Cross || join.natural || !join.using.is_empty() {
        return false;
    }
    node_inner_only(&join.left) && join.right.as_ref().is_none_or(node_inner_only)
}

fn node_inner_only(node: &JoinNode) -> bool {
    match node {
        JoinNode::Table(_) => true,
        JoinNode::Join(join) => inner_joins_only(join),
        JoinNode::Derived { .. } => false,
    }
}

fn join_node(join: Join) -> JoinNode {
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        join.left
    } else {
        JoinNode::Join(Box::new(join))
    }
}

fn derived_node(select: SelectStmt, alias: &str) -> JoinNode {
    JoinNode::Derived {
        subquery: tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(select))),
        alias: Some(alias.to_owned()),
        lateral: false,
        column_names: Vec::new(),
    }
}

/// Places inner-local predicates on the inner join itself. A single-table
/// inner has no join node to own them, so they remain part of the outer
/// join's ON condition.
fn attach_to_inner_join(join: &mut Join, conditions: Option<Expr>) -> Option<Expr> {
    let Some(conditions) = conditions else {
        return None;
    };
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        if let JoinNode::Join(inner) = &mut join.left {
            return attach_to_inner_join(inner, Some(conditions));
        }
        return Some(conditions);
    }
    join.on = and(join.on.take(), Some(conditions));
    None
}

fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    let mut result = Vec::new();
    crate::plan_trace::collect_and(expr, &mut result);
    result
}

fn combine_and(mut conditions: Vec<Expr>) -> Option<Expr> {
    let first = conditions.pop()?;
    Some(conditions.into_iter().rev().fold(first, |right, left| {
        Expr::Binary(BinaryOp::LogicAnd, Box::new(left), Box::new(right))
    }))
}

fn and(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Expr::Binary(
            BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}
