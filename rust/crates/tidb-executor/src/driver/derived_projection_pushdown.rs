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

//! Predicate pushdown through a pass-through derived projection.
//!
//! Go builds a `LogicalProjection` for a derived table, substitutes its
//! defining expressions into predicates above it, and then runs predicate
//! pushdown and `simplifyOuterJoin`. The TPCC condition-06 query depends on
//! the whole chain: a `COUNT(1)` over two bare projected columns can fuse with
//! the inner SELECT; the substituted inequality rejects the LEFT JOIN's NULL
//! row and makes it an inner join; its nullable left operand contributes an
//! `IS NOT NULL` leaf filter; and a constant on the left join key reaches the
//! grouped derived table on the right.
//!
//! The rewrites here are deliberately narrow and proof-oriented. Fusion is
//! accepted only for a global `COUNT(1)` over one non-lateral derived SELECT
//! whose projection consists entirely of bare columns and whose row count is
//! not changed by DISTINCT, grouping, ordering, LIMIT, windows, locks, or
//! SELECT modifiers. A filter offered to a grouped derived table is accepted
//! only when every referenced output is itself a selected group-key column;
//! aggregate outputs never move below the aggregation.

use tidb_ast::{
    Expr, IsTarget, Join, JoinNode, JoinType, QueryStmt, SelectField, SelectStatementKind,
    SelectStmt, StatementPriority,
};

use super::catalog::Catalog;

/// Fuses a global COUNT over a pass-through derived SELECT, then applies Go's
/// NULL-rejecting outer-join simplification and derived not-null predicate.
pub(crate) fn fuse_global_count(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Option<SelectStmt> {
    if !plain_global_count(select) {
        return None;
    }
    let from = select.from.as_ref()?;
    if from.right.is_some() || from.on.is_some() || from.natural || !from.using.is_empty() {
        return None;
    }
    let JoinNode::Derived {
        subquery,
        alias: Some(alias),
        lateral: false,
        column_names,
    } = &from.left
    else {
        return None;
    };
    if !column_names.is_empty() {
        return None;
    }
    let QueryStmt::Select(inner) = &**subquery else {
        return None;
    };
    if !pass_through_select(inner) {
        return None;
    }
    let definitions = pass_through_definitions(inner)?;
    let outer_filter = substitute_outputs(select.where_clause.as_ref()?, alias, &definitions)?;

    let mut fused = (**inner).clone();
    fused.fields = select.fields.clone();
    fused.where_clause = and(fused.where_clause.take(), Some(outer_filter.clone()));
    let simplified = simplify_outer_join(&mut fused, catalog, current_db);
    if simplified {
        inject_left_not_null_filters(&mut fused, &outer_filter, catalog, current_db);
    }
    Some(fused)
}

/// Pushes filters attributed to a derived relation through its projection.
/// Grouped SELECTs accept only selected bare group keys, so a predicate can
/// never be moved from HAVING semantics into WHERE semantics by this helper.
pub(crate) fn push_filters_into_derived(
    subquery: &QueryStmt,
    alias: &str,
    column_names: &[String],
    predicate: &Expr,
) -> Option<QueryStmt> {
    if !column_names.is_empty() {
        return None;
    }
    let QueryStmt::Select(select) = subquery else {
        return None;
    };
    if select.from.is_none()
        || select.distinct
        || select.rollup
        || select.having.is_some()
        || select.limit.is_some()
        || !select.windows.is_empty()
        || select.lock.is_some()
        || select.into_outfile.is_some()
    {
        return None;
    }
    let definitions = group_key_definitions(select)?;
    let pushed = substitute_outputs(predicate, alias, &definitions)?;
    let mut rewritten = select.clone();
    rewritten.where_clause = and(rewritten.where_clause.take(), Some(pushed));
    Some(QueryStmt::Select(rewritten))
}

fn plain_global_count(select: &SelectStmt) -> bool {
    if select.with.is_some()
        || !select.hints.is_empty()
        || select.sql_small_result
        || select.sql_big_result
        || select.sql_buffer_result
        || select.sql_no_cache
        || select.straight_join
        || select.calc_found_rows
        || select.kind != SelectStatementKind::Select
        || select.priority != StatementPriority::None
        || select.distinct
        || select.all
        || !select.values.is_empty()
        || !select.group_by.is_empty()
        || select.rollup
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.lock.is_some()
        || select.into_outfile.is_some()
        || select.where_clause.is_none()
    {
        return false;
    }
    matches!(
        select.fields.fields(),
        [SelectField::Expr {
            expr: Expr::Aggregate {
                name,
                distinct: false,
                args,
            },
            ..
        }] if name.eq_ignore_ascii_case("count")
            && matches!(args.as_slice(), [Expr::Int(one)] if one == "1")
    )
}

fn pass_through_select(select: &SelectStmt) -> bool {
    select.from.is_some()
        && select.with.is_none()
        && !select.distinct
        && !select.rollup
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.windows.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.lock.is_none()
        && select.into_outfile.is_none()
        && select.values.is_empty()
        && select.fields.fields().iter().all(|field| {
            matches!(
                field,
                SelectField::Expr {
                    expr: Expr::Column(_),
                    ..
                }
            )
        })
}

fn pass_through_definitions(select: &SelectStmt) -> Option<Vec<(String, Expr)>> {
    let names = super::from::derived_field_names(select)?;
    unique_definitions(
        names
            .into_iter()
            .zip(select.fields.fields())
            .map(|(name, field)| match field {
                SelectField::Expr {
                    expr: expr @ Expr::Column(_),
                    ..
                } => Some((name, expr.clone())),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?,
    )
}

fn group_key_definitions(select: &SelectStmt) -> Option<Vec<(String, Expr)>> {
    let names = super::from::derived_field_names(select)?;
    let definitions = names
        .into_iter()
        .zip(select.fields.fields())
        .filter_map(|(name, field)| match field {
            SelectField::Expr {
                expr: expr @ Expr::Column(path),
                ..
            } if select.group_by.iter().any(
                |group| matches!(&group.expr, Expr::Column(group_path) if group_path == path),
            ) =>
            {
                Some((name, expr.clone()))
            }
            _ => None,
        })
        .collect();
    unique_definitions(definitions)
}

fn unique_definitions(definitions: Vec<(String, Expr)>) -> Option<Vec<(String, Expr)>> {
    for (index, (name, _)) in definitions.iter().enumerate() {
        if definitions[..index]
            .iter()
            .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
        {
            return None;
        }
    }
    Some(definitions)
}

fn substitute_outputs(
    predicate: &Expr,
    alias: &str,
    definitions: &[(String, Expr)],
) -> Option<Expr> {
    struct Rewrite<'a> {
        alias: &'a str,
        definitions: &'a [(String, Expr)],
        ok: bool,
    }
    impl tidb_ast::Visitor for Rewrite<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<Expr>() else {
                return false;
            };
            if matches!(
                expr,
                Expr::Subquery(_)
                    | Expr::Exists { .. }
                    | Expr::InSubquery { .. }
                    | Expr::CompareSubquery { .. }
            ) {
                self.ok = false;
                return true;
            }
            let Expr::Column(path) = expr else {
                return false;
            };
            let name = match path.as_slice() {
                [name] => name,
                [qualifier, name] | [_, qualifier, name]
                    if qualifier.eq_ignore_ascii_case(self.alias) =>
                {
                    name
                }
                [_, _] | [_, _, _] => {
                    self.ok = false;
                    return true;
                }
                _ => {
                    self.ok = false;
                    return true;
                }
            };
            let Some((_, defining)) = self
                .definitions
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            else {
                self.ok = false;
                return true;
            };
            *expr = defining.clone();
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut rewritten = predicate.clone();
    let mut visitor = Rewrite {
        alias,
        definitions,
        ok: true,
    };
    tidb_ast::Visitable::accept(&mut rewritten, &mut visitor);
    visitor.ok.then_some(rewritten)
}

fn simplify_outer_join(select: &mut SelectStmt, catalog: &Catalog, current_db: &str) -> bool {
    let where_clause = match &select.where_clause {
        Some(predicate) => predicate,
        None => return false,
    };
    let join = match select.from.as_ref().and_then(top_join) {
        Some(join) => join,
        None => return false,
    };
    if !matches!(join.tp, JoinType::Left | JoinType::Right)
        || join.natural
        || !join.using.is_empty()
    {
        return false;
    }
    let right = match &join.right {
        Some(right) => right,
        None => return false,
    };
    let offered = super::predicate_push_down::offered_conjuncts(Some(where_clause));
    let joined = match super::merge_decision::join_properties(
        join,
        catalog,
        current_db,
        &offered,
        super::merge_decision::Phase::Promise,
    ) {
        Some(properties) => properties,
        None => return false,
    };
    let left =
        match super::merge_decision::possible_properties(&join.left, catalog, current_db, &offered)
        {
            Some(properties) => properties,
            None => return false,
        };
    let right =
        match super::merge_decision::possible_properties(right, catalog, current_db, &offered) {
            Some(properties) => properties,
            None => return false,
        };
    let rejects = match join.tp {
        JoinType::Left => left.width..left.width + right.width,
        JoinType::Right => 0..left.width,
        JoinType::Cross => return false,
    }
    .any(|offset| {
        super::funcdep::null_reject::is_null_rejected(where_clause, offset, &|path| {
            joined.offset_of(path)
        })
    });
    if !rejects {
        return false;
    }
    let Some(join) = select.from.as_mut().and_then(top_join_mut) else {
        return false;
    };
    join.tp = JoinType::Cross;
    true
}

fn inject_left_not_null_filters(
    select: &mut SelectStmt,
    pushed_filter: &Expr,
    catalog: &Catalog,
    current_db: &str,
) {
    let Some(join) = select.from.as_ref().and_then(top_join) else {
        return;
    };
    if join.tp != JoinType::Cross {
        return;
    }
    let offered = super::predicate_push_down::offered_conjuncts(select.where_clause.as_ref());
    let Some(joined) = super::merge_decision::join_properties(
        join,
        catalog,
        current_db,
        &offered,
        super::merge_decision::Phase::Promise,
    ) else {
        return;
    };
    let Some(left) =
        super::merge_decision::possible_properties(&join.left, catalog, current_db, &offered)
    else {
        return;
    };
    let mut paths = Vec::new();
    collect_column_paths(pushed_filter, &mut paths);
    for path in paths {
        let Some(offset) = joined.offset_of(&path) else {
            continue;
        };
        if offset >= left.width
            || !super::funcdep::null_reject::is_null_rejected(pushed_filter, offset, &|path| {
                joined.offset_of(path)
            })
        {
            continue;
        }
        let predicate = Expr::Is {
            expr: Box::new(Expr::Column(path)),
            target: IsTarget::Null,
            not: true,
        };
        select.where_clause = and(select.where_clause.take(), Some(predicate));
    }
}

fn collect_column_paths(expr: &Expr, out: &mut Vec<Vec<String>>) {
    struct Collect<'a>(&'a mut Vec<Vec<String>>);
    impl tidb_ast::Visitor for Collect<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() {
                if !self.0.contains(path) {
                    self.0.push(path.clone());
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut Collect(out));
}

fn top_join(join: &Join) -> Option<&Join> {
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        if let JoinNode::Join(inner) = &join.left {
            return top_join(inner);
        }
    }
    join.right.as_ref().map(|_| join)
}

fn top_join_mut(join: &mut Join) -> Option<&mut Join> {
    if join.right.is_some() {
        return Some(join);
    }
    if join.on.is_some() || !join.using.is_empty() || join.natural {
        return None;
    }
    match &mut join.left {
        JoinNode::Join(inner) => top_join_mut(inner),
        JoinNode::Table(_) | JoinNode::Derived { .. } => None,
    }
}

fn and(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}
