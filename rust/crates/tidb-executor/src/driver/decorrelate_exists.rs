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

//! Correlated `EXISTS` / `NOT EXISTS` lowering.
//!
//! Go first builds a semi Apply, then `rule_decorrelate` moves an inner
//! `LogicalSelection` into the Apply's join conditions and turns the Apply
//! into a semi join once no correlated column remains. This module performs
//! the same rewrite directly for the dependency-closed SELECT shape Rust can
//! execute as one join. Shapes outside that boundary stay on the existing
//! per-row Apply path.

use super::*;

pub(crate) struct DecorrelatedWhere {
    pub(crate) source: Box<dyn Executor>,
    pub(crate) scope: FromScope,
    pub(crate) residual: Option<tidb_ast::Expr>,
    pub(crate) delivered: crate::driver::from::Delivered,
    pub(crate) semi_join_count: usize,
    /// The first semi join's left Projection absorbed the enclosing join
    /// reorder's written-schema restoration.
    pub(crate) consumed_outer_projection: bool,
}

pub(crate) fn has_top_level_exists(predicate: Option<&tidb_ast::Expr>) -> bool {
    let Some(predicate) = predicate else {
        return false;
    };
    let mut conjuncts = Vec::new();
    crate::plan_trace::collect_and(predicate, &mut conjuncts);
    conjuncts
        .into_iter()
        .any(|conjunct| exists(conjunct).is_some())
}

/// Replaces each eligible top-level EXISTS conjunct with a semi join and
/// returns the conjuncts that still need the ordinary Selection/Apply path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn decorrelate_where(
    mut source: Box<dyn Executor>,
    outer: &FromScope,
    predicate: &tidb_ast::Expr,
    select: &tidb_ast::SelectStmt,
    mut delivered: crate::driver::from::Delivered,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    outer_rows_hint: Option<f64>,
    outer_projection: Option<&super::agg_select::JoinOutputProjection>,
    mut trace: Option<&mut PlanTrace>,
) -> Result<DecorrelatedWhere, DriverError> {
    let mut conjuncts = Vec::new();
    crate::plan_trace::collect_and(predicate, &mut conjuncts);
    let mut residual = Vec::new();

    let mut physical_conjuncts = Vec::new();
    crate::plan_trace::collect_and(
        select.where_clause.as_ref().unwrap_or(predicate),
        &mut physical_conjuncts,
    );
    let eligible = physical_conjuncts
        .iter()
        .enumerate()
        .filter_map(|(index, conjunct)| {
            let (query, not) = exists(conjunct)?;
            Some((index, query, not))
        })
        .collect::<Vec<_>>();
    if !eligible.is_empty() {
        let mut prepared = Vec::with_capacity(eligible.len());
        for (target, query, not) in &eligible {
            let Some(subquery) = prepare(query, outer, catalog, current_db, ctx)? else {
                prepared.clear();
                break;
            };
            prepared.push((*target, subquery, *not));
        }
        if let Some(outer_from) = select
            .from
            .as_ref()
            .filter(|_| prepared.len() == eligible.len())
        {
            let semi_join_count = prepared.len();
            let local = physical_conjuncts
                .iter()
                .enumerate()
                .filter(|(index, _)| !eligible.iter().any(|(target, ..)| target == index))
                .map(|(_, conjunct)| (*conjunct).clone())
                .collect::<Vec<_>>();
            let pending_local = conjuncts
                .iter()
                .filter(|conjunct| exists(conjunct).is_none())
                .map(|conjunct| (*conjunct).clone())
                .collect::<Vec<_>>();
            let local_where = crate::driver::predicate_push_down::combined(&local);
            // Go's Apply decorrelation starts from the preserved DataSource
            // after its ordinary local predicates. A candidate built from the
            // original SELECT may already include the semi predicate's
            // fallback selectivity (q4), while a scalar-subquery predicate
            // may only be represented by the candidate (q22). Prefer the
            // loaded-statistics local count, then Go's outer logical receipt,
            // and only then the physical candidate.
            let local_outer_rows = crate::driver::access::select_predicate_stats_rows(
                select,
                local_where.as_ref(),
                catalog,
                current_db,
                outer,
            );
            let candidate_outer_rows = delivered.candidate.as_ref().map(|candidate| {
                tidb_planner::candidate_cost::evaluate(
                    candidate,
                    &tidb_planner::candidate_cost::CostEnv::default(),
                    tidb_planner::task_type::TaskType::Root,
                )
                .rows
            });
            let initial_outer_rows = local_outer_rows
                .or(outer_rows_hint)
                .or(candidate_outer_rows);
            let mut logical_outer_rows = initial_outer_rows;
            let mut final_residual = local_where.clone();
            let mut physical_outer_scope = outer.clone();
            let mut consumed_outer_projection = false;
            for (position, (_, prepared, not)) in prepared.into_iter().enumerate() {
                let physical_join = tidb_ast::Join {
                    left: tidb_ast::JoinNode::Join(Box::new(outer_from.clone())),
                    right: Some(tidb_ast::JoinNode::Join(Box::new(
                        prepared
                            .select
                            .from
                            .as_ref()
                            .expect("prepared SELECT has FROM")
                            .clone(),
                    ))),
                    tp: tidb_ast::JoinType::Cross,
                    straight: false,
                    on: crate::driver::predicate_push_down::combined(
                        &prepared
                            .conditions
                            .iter()
                            .map(|condition| (*condition).clone())
                            .collect::<Vec<_>>(),
                    ),
                    using: Vec::new(),
                    natural: false,
                    explicit_parens: false,
                };
                let mut physical_select = select.clone();
                physical_select.from = Some(physical_join.clone());
                physical_select.where_clause = local_where.clone();
                let offered = crate::driver::predicate_push_down::offered_conjuncts(
                    physical_select.where_clause.as_ref(),
                );
                let pushdown = crate::driver::predicate_push_down::plan(
                    &physical_join,
                    physical_select.where_clause.as_ref(),
                    catalog,
                    current_db,
                    // `NOT EXISTS` becomes Go's `AntiSemiJoin`, which derives
                    // no `is not null` from its own conditions.
                    not,
                );
                let wanted = crate::driver::leaf_demand::LeafDemand::of_select(&physical_select);
                let output_wanted =
                    crate::driver::leaf_demand::LeafDemand::of_select_output(&physical_select);
                let Some(rows) = crate::driver::join_reorder::row_source_for_join_type(
                    &physical_join,
                    physical_select.where_clause.as_ref(),
                    catalog,
                    current_db,
                    ctx,
                    not,
                ) else {
                    break;
                };
                let local_join_rows =
                    crate::driver::join_search::estimated_rows(&physical_join, Some(&rows));
                let next_outer_rows = logical_outer_rows
                    .or_else(|| local_join_rows.map(|estimated| estimated.left))
                    .map(|left| left * crate::plan_trace::SELECTIVITY_FACTOR);
                let demand = crate::driver::leaf_demand::FromDemand {
                    offered: &offered,
                    pushdown: Some(&pushdown),
                    columns: Some(&wanted),
                    // `wanted` IS the statement-wide walk (`of_select`), so
                    // it answers both questions for this rewritten select.
                    all_names: Some(&wanted),
                    output_columns: Some(&output_wanted),
                    rows: Some(&rows),
                    join_hints: None,
                    physical_source_names: false,
                    plan_columns: &[],
                    runtime_lookup: None,
                };
                let (planned, planned_scope, planned_delivered) =
                    crate::driver::from::build_semi_join(
                        &physical_join,
                        source,
                        physical_outer_scope,
                        delivered,
                        catalog,
                        current_db,
                        ctx,
                        trace.as_deref_mut(),
                        Some(&physical_select),
                        demand,
                        not,
                        if position == 0 {
                            pending_local.clone()
                        } else {
                            Vec::new()
                        },
                        (position == 0).then_some(outer_projection).flatten(),
                        logical_outer_rows,
                        initial_outer_rows,
                    )?;
                consumed_outer_projection |= position == 0 && outer_projection.is_some();
                source = planned;
                physical_outer_scope = planned_scope;
                delivered = planned_delivered;
                logical_outer_rows = next_outer_rows;
                final_residual = rows.residual_where();
            }
            return Ok(DecorrelatedWhere {
                source,
                scope: physical_outer_scope,
                residual: final_residual,
                delivered,
                semi_join_count,
                consumed_outer_projection,
            });
        }
    }

    let mut semi_join_count = 0;
    for conjunct in conjuncts {
        let Some((query, not)) = exists(conjunct) else {
            residual.push(conjunct.clone());
            continue;
        };
        let Some(prepared) = prepare(query, outer, catalog, current_db, ctx)? else {
            residual.push(conjunct.clone());
            continue;
        };

        let (inner, built_scope, _) = build_join(
            prepared
                .select
                .from
                .as_ref()
                .expect("prepared SELECT has FROM"),
            catalog,
            current_db,
            ctx,
            trace.as_deref_mut(),
            None,
            crate::driver::leaf_demand::FromDemand::none(),
            &tidb_planner::physical_property::PhysicalProperty::default(),
        )?;
        debug_assert_eq!(built_scope.width(), prepared.inner_scope.width());

        let conditions = prepared
            .conditions
            .iter()
            .map(|condition| {
                rewrite_expr_resolved(
                    condition,
                    &ScopeResolver {
                        scope: &prepared.combined,
                    },
                )
                .map_err(|error| DriverError::Exec(ExecError::Eval(error)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let equal_mask = crate::hash_join::split_equi(&conditions, outer.width()).equal_mask;
        let kind = if not {
            JoinKind::AntiSemi
        } else {
            JoinKind::Semi
        };
        let meta = ExecutorMeta::new(
            source.schema().clone(),
            7,
            source.init_cap(),
            source.max_chunk_size(),
        );
        source = Box::new(JoinExec::new(
            meta,
            kind,
            conditions,
            source,
            inner,
            ctx.clone(),
            ctx.statement_memory(),
        ));
        semi_join_count += 1;
        delivered.clear();
        if let Some(trace) = trace.as_deref_mut() {
            trace
                .semi_join(
                    &prepared.conditions,
                    &prepared.combined,
                    current_db,
                    &equal_mask,
                    not,
                )
                .map_err(|()| DriverError::unsupported("failed to record a semi join plan"))?;
            source = trace.meter(source);
        }
    }

    Ok(DecorrelatedWhere {
        source,
        scope: outer.clone(),
        residual: crate::driver::predicate_push_down::combined(&residual),
        delivered,
        semi_join_count,
        consumed_outer_projection: false,
    })
}

struct Prepared<'a> {
    select: &'a tidb_ast::SelectStmt,
    inner_scope: FromScope,
    combined: FromScope,
    conditions: Vec<&'a tidb_ast::Expr>,
}

fn exists(expr: &tidb_ast::Expr) -> Option<(&tidb_ast::QueryStmt, bool)> {
    match expr {
        tidb_ast::Expr::Paren(inner) => exists(inner),
        tidb_ast::Expr::Exists { subquery, not } => Some((subquery, *not)),
        _ => None,
    }
}

fn prepare<'a>(
    query: &'a tidb_ast::QueryStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<Prepared<'a>>, DriverError> {
    let tidb_ast::QueryStmt::Select(select) = query else {
        return Ok(None);
    };
    if select.kind != tidb_ast::SelectStatementKind::Select
        || select.with.is_some()
        || !select.hints.is_empty()
        || select.from.is_none()
        || select.where_clause.is_none()
        || !select.group_by.is_empty()
        || select.rollup
        || select.having.is_some()
        || !select.windows.is_empty()
        || select.limit.is_some()
        || select.lock.is_some()
        || select.into_outfile.is_some()
        || select.fields.fields().iter().any(|field| match field {
            tidb_ast::SelectField::Expr { expr, .. } => {
                expr.has_aggregate_flag() || expr.has_window_flag()
            }
            tidb_ast::SelectField::Wildcard { .. } => false,
        })
    {
        return Ok(None);
    }
    let where_clause = select.where_clause.as_ref().expect("checked above");
    if crate::driver::subquery::expr_has_subquery(where_clause) {
        return Ok(None);
    }

    let (_, inner_scope, _) = build_join(
        select.from.as_ref().expect("checked above"),
        catalog,
        current_db,
        ctx,
        None,
        None,
        crate::driver::leaf_demand::FromDemand::none(),
        &tidb_planner::physical_property::PhysicalProperty::default(),
    )?;
    let mut all_correlated = Vec::new();
    crate::driver::subquery::collect_correlated_columns_query(
        query,
        outer,
        catalog,
        current_db,
        &mut all_correlated,
        ctx,
    );
    let where_correlated =
        crate::driver::subquery::collect_correlated_columns_expr(where_clause, &inner_scope, outer);
    if all_correlated.is_empty()
        || all_correlated
            .iter()
            .any(|column| !where_correlated.contains(column))
    {
        return Ok(None);
    }

    let combined = combined_scope(outer, &inner_scope);
    let mut conditions = Vec::new();
    crate::plan_trace::collect_and(where_clause, &mut conditions);
    for condition in &conditions {
        if rewrite_expr_resolved(condition, &ScopeResolver { scope: &combined }).is_err() {
            return Ok(None);
        }
    }
    Ok(Some(Prepared {
        select,
        inner_scope,
        combined,
        conditions,
    }))
}

fn combined_scope(outer: &FromScope, inner: &FromScope) -> FromScope {
    let mut combined = outer.clone();
    let offset = outer.width();
    combined
        .coalesced
        .extend(inner.coalesced.iter().map(|column| column + offset));
    combined.star.clear();
    for table in &inner.tables {
        let mut table = table.clone();
        table.offset += offset;
        combined.tables.push(table);
    }
    combined
}
