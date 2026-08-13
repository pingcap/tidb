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
    pub(crate) residual: Option<tidb_ast::Expr>,
}

/// Replaces each eligible top-level EXISTS conjunct with a semi join and
/// returns the conjuncts that still need the ordinary Selection/Apply path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn decorrelate_where(
    mut source: Box<dyn Executor>,
    outer: &FromScope,
    predicate: &tidb_ast::Expr,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<DecorrelatedWhere, DriverError> {
    let mut conjuncts = Vec::new();
    crate::plan_trace::collect_and(predicate, &mut conjuncts);
    let mut residual = Vec::new();

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
        residual: crate::driver::predicate_push_down::combined(&residual),
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
