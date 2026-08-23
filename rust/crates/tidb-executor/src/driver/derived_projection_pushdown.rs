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

use super::catalog::{split_table_path, Catalog};

/// Expands wildcards inside derived SELECTs against their already-known FROM
/// schema. Go performs this while building the derived projection, before
/// predicate pushdown and column pruning inspect that projection's outputs.
///
/// The top-level SELECT is deliberately left alone. Only nested SELECTs are
/// rewritten, and a wildcard is expanded only when every source column can be
/// identified without executing the query. NATURAL/USING joins and set
/// operations therefore remain in their original form.
pub(crate) fn expand_derived_wildcards(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Option<SelectStmt> {
    let mut rewritten = select.clone();
    let changed = rewritten
        .from
        .as_mut()
        .is_some_and(|from| expand_join_derived_wildcards(from, catalog, current_db));
    changed.then_some(rewritten)
}

/// Pushes leaf-local WHERE predicates into derived SELECTs before column
/// pruning. This is the logical half of the same RowSource inventory the
/// physical builder later uses: constants are propagated across inner join
/// equalities, predicates are substituted through projections, and only the
/// predicates proven installed in a derived child are removed from the
/// parent WHERE. Join equalities and `other cond` predicates stay at their
/// join so the physical builder can install them there.
pub(crate) fn push_local_predicates_into_derived(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<SelectStmt> {
    // A base-table query cannot expose a predicate through a derived
    // projection. Avoid constructing the full RowSource/statistics model for
    // this common shape; the physical single-table fast path owns its
    // predicate planning below.
    if !select.from.as_ref().is_some_and(join_contains_derived) {
        return None;
    }
    let mut rewritten = select.clone();
    push_select_predicates(&mut rewritten, catalog, current_db, ctx).then_some(rewritten)
}

fn join_contains_derived(join: &Join) -> bool {
    fn node_contains_derived(node: &JoinNode) -> bool {
        match node {
            JoinNode::Derived { .. } => true,
            JoinNode::Table(_) => false,
            JoinNode::Join(join) => {
                node_contains_derived(&join.left)
                    || join.right.as_ref().is_some_and(node_contains_derived)
            }
        }
    }

    node_contains_derived(&join.left) || join.right.as_ref().is_some_and(node_contains_derived)
}

fn expand_join_derived_wildcards(join: &mut Join, catalog: &Catalog, current_db: &str) -> bool {
    let mut changed = expand_node_derived_wildcards(&mut join.left, catalog, current_db);
    if let Some(right) = &mut join.right {
        changed |= expand_node_derived_wildcards(right, catalog, current_db);
    }
    changed
}

fn expand_node_derived_wildcards(node: &mut JoinNode, catalog: &Catalog, current_db: &str) -> bool {
    match node {
        JoinNode::Table(_) => false,
        JoinNode::Join(join) => expand_join_derived_wildcards(join, catalog, current_db),
        JoinNode::Derived { subquery, .. } => {
            let QueryStmt::Select(select) = &mut **subquery else {
                return false;
            };
            let mut changed = select
                .from
                .as_mut()
                .is_some_and(|from| expand_join_derived_wildcards(from, catalog, current_db));
            if select
                .fields
                .fields()
                .iter()
                .any(|field| matches!(field, SelectField::Wildcard(_)))
            {
                if let Some(fields) = expanded_fields(select, catalog, current_db) {
                    select.fields = fields.into();
                    changed = true;
                }
            }
            changed
        }
    }
}

fn expanded_fields(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Option<Vec<SelectField>> {
    let columns = join_output_columns(select.from.as_ref()?, catalog, current_db)?;
    let mut expanded = Vec::new();
    for field in select.fields.fields() {
        let SelectField::Wildcard(path) = field else {
            expanded.push(field.clone());
            continue;
        };
        let qualifier = path.last();
        let mut matched = false;
        for (relation, column) in &columns {
            if qualifier.is_some_and(|wanted| !relation.eq_ignore_ascii_case(wanted)) {
                continue;
            }
            matched = true;
            expanded.push(SelectField::Expr {
                expr: Expr::Column(vec![relation.clone(), column.clone()]),
                alias: None,
            });
        }
        if !matched {
            return None;
        }
    }
    Some(expanded)
}

fn join_output_columns(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
) -> Option<Vec<(String, String)>> {
    if join.natural || !join.using.is_empty() {
        return None;
    }
    let mut columns = node_output_columns(&join.left, catalog, current_db)?;
    if let Some(right) = &join.right {
        columns.extend(node_output_columns(right, catalog, current_db)?);
    }
    Some(columns)
}

fn node_output_columns(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
) -> Option<Vec<(String, String)>> {
    match node {
        JoinNode::Table(table) => {
            let (database, name) = split_table_path(&table.name, current_db).ok()?;
            let visible = table.alias.as_deref().unwrap_or(name).to_owned();
            Some(
                catalog
                    .get_in(database, name)?
                    .column_list()
                    .into_iter()
                    .map(|(column, _)| (visible.clone(), column))
                    .collect(),
            )
        }
        JoinNode::Join(join) => join_output_columns(join, catalog, current_db),
        JoinNode::Derived {
            subquery,
            alias: Some(alias),
            lateral: false,
            column_names,
        } => {
            let QueryStmt::Select(select) = &**subquery else {
                return None;
            };
            let mut names = super::from::derived_field_names(select)?;
            if !column_names.is_empty() {
                if column_names.len() != names.len() {
                    return None;
                }
                names.clone_from(column_names);
            }
            Some(
                names
                    .into_iter()
                    .map(|column| (alias.clone(), column))
                    .collect(),
            )
        }
        JoinNode::Derived { .. } => None,
    }
}

fn push_select_predicates(
    select: &mut SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    let rows = select.from.as_ref().and_then(|from| {
        super::join_reorder::row_source(
            from,
            select.where_clause.as_ref(),
            catalog,
            current_db,
            ctx,
        )
    });
    let mut changed = false;
    if let (Some(from), Some(rows)) = (&mut select.from, rows.as_ref()) {
        let predicates_pushed = push_join_predicates(from, rows, catalog, current_db, ctx);
        if predicates_pushed {
            select.where_clause = rows.residual_where_after_logical_leaf_pushdown();
            changed = true;
        }
    }
    if let Some(from) = &mut select.from {
        changed |= recurse_join_predicates(from, catalog, current_db, ctx);
    }
    changed
}

fn push_join_predicates(
    join: &mut Join,
    rows: &super::join_reorder::RowSource,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    let mut changed = push_node_predicates(&mut join.left, rows, catalog, current_db, ctx);
    if let Some(right) = &mut join.right {
        changed |= push_node_predicates(right, rows, catalog, current_db, ctx);
    }
    changed
}

fn push_node_predicates(
    node: &mut JoinNode,
    rows: &super::join_reorder::RowSource,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    match node {
        JoinNode::Table(_) => false,
        JoinNode::Join(join) => push_join_predicates(join, rows, catalog, current_db, ctx),
        JoinNode::Derived {
            subquery,
            alias: Some(alias),
            column_names,
            lateral: false,
        } => {
            let QueryStmt::Select(select) = &**subquery else {
                return false;
            };
            if !pass_through_select(select) {
                // Direct grouped relations already receive their local
                // filters from the physical RowSource walk. The early pass is
                // needed only to cross an intervening projection before
                // column pruning decides which projection outputs survive.
                return false;
            }
            let predicate = rows.filters_for(alias).and_then(|filters| {
                filters.iter().cloned().reduce(|left, right| {
                    Expr::Binary(
                        tidb_ast::BinaryOp::LogicAnd,
                        Box::new(left),
                        Box::new(right),
                    )
                })
            });
            let Some(rewritten) = predicate.as_ref().and_then(|predicate| {
                push_filters_into_derived(subquery, alias, column_names, predicate)
            }) else {
                return false;
            };
            **subquery = rewritten;
            rows.mark_leaf_filters_consumed(alias);
            true
        }
        JoinNode::Derived { .. } => false,
    }
}

fn recurse_join_predicates(
    join: &mut Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    let mut changed = recurse_node_predicates(&mut join.left, catalog, current_db, ctx);
    if let Some(right) = &mut join.right {
        changed |= recurse_node_predicates(right, catalog, current_db, ctx);
    }
    changed
}

fn recurse_node_predicates(
    node: &mut JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    match node {
        JoinNode::Table(_) => false,
        JoinNode::Join(join) => recurse_join_predicates(join, catalog, current_db, ctx),
        JoinNode::Derived { subquery, .. } => match &mut **subquery {
            QueryStmt::Select(select) => push_select_predicates(select, catalog, current_db, ctx),
            QueryStmt::SetOpr(_) => false,
        },
    }
}

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
    let definitions = if select.group_by.is_empty() {
        if !projection_only_select(select) {
            return None;
        }
        projection_definitions(select)?
    } else {
        group_key_definitions(select)?
    };
    let pushed = substitute_outputs(predicate, alias, &definitions)?;
    let mut rewritten = select.clone();
    rewritten.where_clause = and_unique(rewritten.where_clause.take(), pushed);
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

/// A SELECT whose row set IS its `FROM`'s row set -- Go's `LogicalProjection`
/// over the join tree, with nothing above it that could change which rows
/// exist. A predicate over such a SELECT's outputs pushes below the projection
/// by substituting each referenced output's defining expression -- Go's
/// `breakDownPredicates` (`pkg/planner/core/operator/logicalop/
/// logical_projection.go:647`), which substitutes through ANY projection
/// expression, computed or bare, and keeps only predicates whose substitution
/// fails (or reads a variable) above.
///
/// This is [`pass_through_select`] minus the bare-column field requirement:
/// the fields may compute, but an AGGREGATE without `GROUP BY` makes the
/// SELECT a global aggregation -- one row, not the `FROM`'s rows -- so any
/// aggregate-flagged field declines the whole shape.
fn projection_only_select(select: &SelectStmt) -> bool {
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
        && select.fields.fields().iter().all(|field| match field {
            SelectField::Expr { expr, .. } => !expr.has_aggregate_flag(),
            SelectField::Wildcard(_) => true,
        })
}

/// One definition per substitutable output of a projection-only SELECT.
///
/// Go substitutes through every projection expression; the flags excluded
/// here are the ones whose substitution Go also refuses (`HasAssignSetVarFunc`
/// over the projection, `HasGetSetVarFunc` over the result -- both
/// variable-carrying) or that cannot be a `LogicalProjection` expression at
/// all in Go (aggregates, windows), plus a subquery-valued output, which this
/// tier declines rather than re-evaluate the subquery inside a moved
/// predicate. An output with no definition simply cannot be referenced by a
/// pushed predicate -- [`substitute_outputs`] fails on it, exactly Go's
/// per-predicate `canNotBePushed` arm.
fn projection_definitions(select: &SelectStmt) -> Option<Vec<(String, Expr)>> {
    use tidb_ast::{
        FLAG_HAS_AGGREGATE_FUNC, FLAG_HAS_DEFAULT, FLAG_HAS_PARAM_MARKER, FLAG_HAS_SUBQUERY,
        FLAG_HAS_VARIABLE, FLAG_HAS_WINDOW_FUNC,
    };
    const UNSUBSTITUTABLE: u64 = FLAG_HAS_AGGREGATE_FUNC
        | FLAG_HAS_WINDOW_FUNC
        | FLAG_HAS_SUBQUERY
        | FLAG_HAS_VARIABLE
        | FLAG_HAS_DEFAULT
        | FLAG_HAS_PARAM_MARKER;
    let names = super::from::derived_field_names(select)?;
    unique_definitions(
        names
            .into_iter()
            .zip(select.fields.fields())
            .filter_map(|(name, field)| match field {
                SelectField::Expr { expr, .. } if expr.flags() & UNSUBSTITUTABLE == 0 => {
                    Some((name, expr.clone()))
                }
                _ => None,
            })
            .collect(),
    )
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
        let Some(column) = left.column_at(offset) else {
            continue;
        };
        if super::merge_decision::physical_column_is_nullable(
            &join.left, &column, catalog, current_db,
        ) != Some(true)
        {
            // Go derives this leaf predicate only for a nullable base
            // column. NOT NULL columns are already proven, while aggregate
            // and computed outputs cannot be pushed through as a base-table
            // null test.
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

fn and_unique(left: Option<Expr>, right: Expr) -> Option<Expr> {
    let mut conjuncts = Vec::new();
    if let Some(left) = &left {
        crate::plan_trace::collect_and(left, &mut conjuncts);
    }
    let mut offered = Vec::new();
    crate::plan_trace::collect_and(&right, &mut offered);
    let mut combined = conjuncts.into_iter().cloned().collect::<Vec<_>>();
    for conjunct in offered {
        if !combined.contains(conjunct) {
            combined.push(conjunct.clone());
        }
    }
    combined.into_iter().reduce(|left, right| {
        Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )
    })
}
