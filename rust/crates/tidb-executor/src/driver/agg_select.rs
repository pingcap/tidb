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

//! The aggregate `SELECT` pipeline, one named stage per planner phase.
//!
//! Go's `PlanBuilder.buildSelect` builds a grouped query as a fixed chain of
//! logical operators -- `DataSource -> Selection(WHERE) -> Aggregation ->
//! Apply* -> Selection(HAVING) -> Window -> Sort -> Limit -> Projection` --
//! and every clause is lowered against whichever operator's output it reads.
//! This module keeps that chain explicit: [`run_aggregate_select`] calls the
//! stages in plan order and [`AggPipelineState`] carries the aggregation's
//! growing output columns (names/types/functions) plus each clause's hoisted
//! remainder between them.

use super::*;

/// The aggregation's output columns and each clause's hoisted remainder, as
/// they grow across the pipeline's stages.
///
/// Go threads the same information through `PlanBuilder`'s `aggMapper` /
/// `windowMap` / `correlatedAggMap` side tables: an expression in any clause
/// is rewritten to READ an aggregation output column, and the column itself is
/// appended here as the rewriting discovers it. `names`/`types`/`agg_funcs`
/// are index-parallel and always describe the current top-of-plan schema.
#[derive(Default)]
struct AggPipelineState {
    /// The grouped column names, which `GROUPING()` arguments resolve against
    /// and which `HAVING`/`ORDER BY` may reference even when the select list
    /// does not project them.
    group_by_names: Vec<String>,
    /// The aggregate functions, index-parallel with `names`/`types`.
    agg_funcs: Vec<AggFunc>,
    /// The aggregation output column names, in output order.
    names: Vec<String>,
    /// The aggregation output column types, in output order.
    types: Vec<FieldType>,
    /// `GROUPING()` calls hoisted into aggregation output columns.
    grouping_specs: Vec<GroupingSpec>,
    /// The window calls hoisted out of the select list / `ORDER BY`.
    window_calls: Vec<crate::window::WindowCall>,
    /// The first output index the window stage appends its columns at.
    window_base: usize,
    /// Where each select field's value comes from, in field order.
    slots: Vec<OutputSlot>,
    /// The name a select field forces onto its output column when the column
    /// it reads is SHARED with another field (a hoisted window value, or a
    /// grouped column the window stage already carried out).
    slot_names: Vec<Option<String>>,
    /// The hoisted expression for every select field a correlated subquery
    /// reaches into (see `OutputSlot::Expr`), in the order they were found.
    post_agg_exprs: Vec<tidb_ast::Expr>,
    /// Correlated subqueries to apply above the aggregation, as
    /// `(subquery, output name, value type)`.
    applies: Vec<(CorrelatedSubquery, String, FieldType)>,
    /// `HAVING` with its aggregates hoisted, over the aggregation's output.
    having_expr: Option<tidb_ast::Expr>,
    /// `ORDER BY` with its aggregates hoisted, as `(expr, desc)`.
    order_by_exprs: Vec<(tidb_ast::Expr, bool)>,
}

/// A resolver over the aggregation's CURRENT output columns, which is what
/// `HAVING`, `ORDER BY` and the final projection are rewritten against (Go's
/// `Aggregation.Schema()`).
fn agg_output_resolver(state: &AggPipelineState) -> AggOutputResolver {
    AggOutputResolver {
        names: state.names.clone(),
        types: state.types.clone(),
    }
}

/// Runs an aggregate `SELECT` (`GROUP BY` and/or aggregate select fields)
/// through [`HashAggExec`].
///
/// Faithful scope (deferred items documented): `COUNT`/`SUM` (Go models
/// `COUNT(*)` as the literal-`1` argument, which counts every row identically);
/// any non-aggregate select field becomes a `FIRST_ROW` carrier (Go's planner
/// does the same; `ONLY_FULL_GROUP_BY` validation is deferred); `DISTINCT`
/// and other aggregate functions are rejected as unsupported. `WITH ROLLUP`
/// runs through [`run_rollup_aggregate`] (plain-column grouping only).
/// `HAVING` and `ORDER BY` run over the aggregation's output, as in Go: an
/// aggregate appearing only in those clauses is appended as a hidden output
/// column and trimmed by a final projection. `GROUPING()` rides the same
/// hidden-column path ([`add_grouping_column`]) but is filled in by the
/// rollup pass rather than aggregated.
pub(crate) fn run_aggregate_select(
    select: &tidb_ast::SelectStmt,
    from_source: Option<Box<dyn Executor>>,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let mut state = AggPipelineState {
        group_by_names: group_by_display_names(select),
        ..AggPipelineState::default()
    };

    // Stage 1: hoist window calls out of the select list / ORDER BY.
    let hoisted = hoist_window_calls(select, &mut state, resolver)?;
    let select = hoisted.as_ref().unwrap_or(select);

    // Stage 2: lower the select list into the aggregation.
    lower_select_fields(select, &mut state, resolver, catalog, current_db, ctx)?;

    // Stage 3: lower HAVING / ORDER BY against the aggregation's output.
    hoist_having_and_order_by(select, &mut state, resolver, catalog, current_db, ctx)?;

    // Stage 4: carry every column an Apply's subquery reads out of the
    // aggregation. Runs after every clause has been walked, so it covers
    // select-field, HAVING and ORDER BY subqueries in one pass.
    carry_apply_columns(&mut state, resolver)?;

    // Stage 5: Source -> Selection(WHERE) -> Aggregation.
    let root = build_aggregation(select, from_source, &mut state, resolver, ctx)?;

    // Stage 6: the Apply chain above the aggregation.
    let root = build_apply_chain(root, &mut state, catalog, current_db, ctx)?;

    // Stage 7: HAVING over the aggregation's (+ Applies') output rows.
    let out_schema = root.schema().clone();
    let root = build_having_stage(root, &state, &out_schema, ctx)?;

    // Stage 8: the window operator, between HAVING and ORDER BY.
    state.window_base = state.names.len();
    let (root, out_schema) = build_window_stage(root, out_schema, &mut state, ctx)?;

    // ORDER BY and the final projection resolve against the WIDENED output.
    let agg_resolver = agg_output_resolver(&state);

    // Stage 9: ORDER BY, then LIMIT.
    let root = build_order_and_limit(root, &out_schema, &agg_resolver, select, &state, ctx)?;

    // Stage 10: the select list's own columns.
    let root = build_final_projection(root, select, &agg_resolver, &mut state, ctx)?;

    // Stage 11: DISTINCT, then drain the plan.
    distinct_and_drain(root, select, &mut state, ctx)
}

/// Stage 0: the grouped column names.
///
/// Mirrors Go's `resolveGbyExprs` / `buildAggregation` group-item naming: a
/// positional `GROUP BY 1` names the same column a literal `GROUP BY a`
/// would, so `GROUPING()` and `HAVING` see it the same way.
fn group_by_display_names(select: &tidb_ast::SelectStmt) -> Vec<String> {
    select
        .group_by
        .iter()
        .filter_map(|item| {
            // A positional `GROUP BY 1` names the same column a literal
            // `GROUP BY a` would, so GROUPING() must see it the same way.
            let resolved = resolve_group_by_position(&item.expr, select.fields.fields()).ok()?;
            match resolved.as_ref() {
                tidb_ast::Expr::Column(path) => path.last().cloned(),
                _ => None,
            }
        })
        .collect()
}

/// Stage 1 (hoist): pull window calls out of the select list / `ORDER BY`.
///
/// Mirrors Go's `buildWindowFunctions` placement: window functions over a
/// grouped query compute over the aggregation's OUTPUT rows (Go plans
/// `Aggregation -> Selection(HAVING) -> Window -> Sort`), so every expression
/// inside a window call -- `RANK() OVER (ORDER BY SUM(v))` -- is hoisted into
/// the aggregation first and the call is left reading that output column. The
/// display names the later stages use still come from the ORIGINAL field text,
/// so the column is named as written.
///
/// Returns the rewritten `SELECT` when the query has windows at all; the
/// caller keeps reading the original otherwise.
fn hoist_window_calls(
    select: &tidb_ast::SelectStmt,
    state: &mut AggPipelineState,
    resolver: &ScopeResolver<'_>,
) -> Result<Option<tidb_ast::SelectStmt>, DriverError> {
    if !crate::window::select_has_window(select) {
        return Ok(None);
    }
    // `ORDER BY <window alias>` names a value the window stage computes,
    // not an aggregation output column, so the alias is resolved to its
    // window expression BEFORE hoisting -- the hoist then leaves the same
    // computed column behind in both places.
    let mut aliased = select.clone();
    for item in &mut aliased.order_by {
        let tidb_ast::Expr::Column(path) = &item.expr else {
            continue;
        };
        let [name] = path.as_slice() else { continue };
        let projected = select.fields.fields().iter().find_map(|field| match field {
            SelectField::Expr {
                expr,
                alias: Some(alias),
            } if alias.eq_ignore_ascii_case(name)
                && !crate::window::windows_in(expr).is_empty() =>
            {
                Some(expr.clone())
            }
            _ => None,
        });
        if let Some(expr) = projected {
            item.expr = expr;
        }
    }
    let select = &aliased;
    let mut hoist_funcs = Vec::new();
    let mut hoist_names = Vec::new();
    let mut hoist_types = Vec::new();
    let mut hoist_specs = Vec::new();
    let (calls, rewritten) = crate::window::hoist_windows(select, |expr| {
        substitute_aggregates(
            expr,
            &mut hoist_funcs,
            &mut hoist_names,
            &mut hoist_types,
            &mut hoist_specs,
            &state.group_by_names,
            resolver,
        )
    })?;
    state.agg_funcs = hoist_funcs;
    state.names = hoist_names;
    state.types = hoist_types;
    state.grouping_specs = hoist_specs;
    state.window_calls = calls;
    Ok(Some(rewritten))
}

/// Stage 2 (aggregate-build): lower the select list into aggregate functions,
/// output columns and per-field slots.
///
/// Mirrors Go's `buildAggregation`: every aggregate becomes an
/// `AggFuncDesc` on the Aggregation operator, a plain field rides a
/// `FIRST_ROW` carrier, `GROUPING()` becomes a column the rollup pass fills
/// in, and a hoisted window value or correlated subquery leaves a slot that
/// the final projection evaluates over the aggregation's output.
fn lower_select_fields(
    select: &tidb_ast::SelectStmt,
    state: &mut AggPipelineState,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    for field in select.fields.fields() {
        let SelectField::Expr { expr, alias } = field else {
            return Err(DriverError::Unsupported(
                "`*` is not supported in an aggregate SELECT",
            ));
        };
        let display = alias.clone().unwrap_or_else(|| expr.restore());
        // A hoisted window call: its value is appended above the aggregation,
        // so the field reads that column rather than any aggregate.
        if let Some(index) = hoisted_window_index(expr) {
            state.slots.push(OutputSlot::Window(index));
            state.slot_names.push(Some(display));
            continue;
        }
        // A value the window hoist already carried out of the aggregation --
        // a grouped column, or an aggregate the window's own spec named
        // (`SUM(v)` beside `RANK() OVER (ORDER BY SUM(v))`) -- is REUSED
        // rather than carried twice: two columns of the same name in the
        // window stage's scope would be ambiguous there. Both are addressed
        // by the same text the hoist stored, a column's name or an
        // aggregate's restored form.
        if !state.window_calls.is_empty() {
            let name = match expr {
                tidb_ast::Expr::Column(path) => path.last().cloned().unwrap_or_default(),
                _ => display.clone(),
            };
            if let Some(index) = state
                .names
                .iter()
                .position(|have| have.eq_ignore_ascii_case(&name))
            {
                state.slots.push(OutputSlot::Agg(index));
                state.slot_names.push(Some(alias.clone().unwrap_or(name)));
                continue;
            }
        }
        // A window value inside a LARGER expression (`RANK() OVER (...) +
        // 1`): Go evaluates it in the projection ABOVE the window operator,
        // which is the final projection this path already builds for a
        // hoisted correlated subquery. The aggregates AROUND the window call
        // are hoisted into the aggregation the same way, so what is left is
        // an expression over the aggregation's output plus the window's own
        // computed column.
        if expr_has_hoisted_window(expr) {
            let hoisted = substitute_aggregates(
                expr,
                &mut state.agg_funcs,
                &mut state.names,
                &mut state.types,
                &mut state.grouping_specs,
                &state.group_by_names,
                resolver,
            )?;
            state
                .slots
                .push(OutputSlot::Expr(state.post_agg_exprs.len()));
            state.slot_names.push(None);
            state.post_agg_exprs.push(hoisted);
            continue;
        }
        // A correlated subquery in an aggregate select list reads the GROUPED
        // value, so it runs once per OUTPUT row rather than per source row --
        // Go's Apply sits above the aggregation for the same reason. It may
        // sit inside a larger expression (`SUM(v) + (SELECT ...)`); the
        // aggregates around it are hoisted the same way HAVING's are.
        let (hoisted, found) = extract_and_hoist_subquery(
            expr,
            resolver.scope,
            catalog,
            current_db,
            &mut state.applies,
            &mut state.agg_funcs,
            &mut state.names,
            &mut state.types,
            &mut state.grouping_specs,
            &state.group_by_names,
            resolver,
            ctx,
        )?;
        if found {
            state
                .slots
                .push(OutputSlot::Expr(state.post_agg_exprs.len()));
            state.slot_names.push(None);
            state.post_agg_exprs.push(hoisted);
            continue;
        }
        state.slots.push(OutputSlot::Agg(state.names.len()));
        state.slot_names.push(None);
        match expr {
            // Both aggregate shapes lower through the same builder, which
            // knows GROUP_CONCAT's separator and DISTINCT.
            tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. } => {
                let (func, ftype) = build_agg_func(expr, resolver)?;
                state.agg_funcs.push(func);
                state.names.push(display);
                state.types.push(ftype);
            }
            // GROUPING() is not an aggregate: it reads the grouping set the
            // output row came from, so it becomes an output column the rollup
            // pass fills in rather than an expression over the row.
            other if grouping_call_args(other).is_some() => {
                let args = grouping_call_args(other).unwrap_or_default();
                let (_, index) = add_grouping_column(
                    args,
                    display,
                    &mut state.agg_funcs,
                    &mut state.names,
                    &mut state.types,
                    &mut state.grouping_specs,
                    &state.group_by_names,
                )?;
                // The call text may already have a column -- hoisted out of a
                // window's PARTITION BY, or written twice -- in which case the
                // slot reserved above points past it.
                if let Some(slot) = state.slots.last_mut() {
                    *slot = OutputSlot::Agg(index);
                }
            }
            other if expr_has_grouping(other) => {
                // Go evaluates `GROUPING(a) + 1` over the projection above the
                // aggregation; this seed has no such projection for select
                // fields, so only a bare GROUPING() field is supported.
                return Err(DriverError::Unsupported(
                    "GROUPING() nested inside a larger select expression is not supported yet",
                ));
            }
            other => {
                // A plain field in an aggregate query rides FIRST_ROW.
                let rewritten = rewrite_expr_resolved(other, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                let t = rewritten
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                state.agg_funcs.push(AggFunc {
                    kind: AggKind::FirstRow,
                    arg: Some(rewritten),
                    extra_args: Vec::new(),
                    distinct: false,
                    order_by: Vec::new(),
                });
                state.names.push(match other {
                    tidb_ast::Expr::Column(path) => {
                        path.last().cloned().unwrap_or_else(|| other.restore())
                    }
                    _ => display,
                });
                state.types.push(t);
            }
        }
    }
    Ok(())
}

/// Stage 3 (having / order-by hoist): lower `HAVING` and `ORDER BY` against
/// the aggregation's output.
///
/// Mirrors Go's `resolveHavingAndOrderBy` + `buildProjection4Having`: an
/// aggregate appearing only in these clauses is appended as a hidden
/// aggregation output column and trimmed again by the final projection.
fn hoist_having_and_order_by(
    select: &tidb_ast::SelectStmt,
    state: &mut AggPipelineState,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    // HAVING / ORDER BY: a correlated subquery is hoisted the same way a
    // select field's is (Apply placeholder + aggregate hoisting); whatever
    // aggregates remain become aggregation output columns.
    let having_expr = match &select.having {
        Some(having) => {
            let (expr, found) = extract_and_hoist_subquery(
                having,
                resolver.scope,
                catalog,
                current_db,
                &mut state.applies,
                &mut state.agg_funcs,
                &mut state.names,
                &mut state.types,
                &mut state.grouping_specs,
                &state.group_by_names,
                resolver,
                ctx,
            )?;
            // A found subquery's remainder is already hoisted; otherwise
            // (no subquery at all, or an uncorrelated one left for the fold
            // pass) HAVING's aggregates still need hoisting, exactly as
            // before a subquery could appear here at all.
            let expr = if found {
                expr
            } else {
                substitute_aggregates(
                    &expr,
                    &mut state.agg_funcs,
                    &mut state.names,
                    &mut state.types,
                    &mut state.grouping_specs,
                    &state.group_by_names,
                    resolver,
                )?
            };
            Some(expr)
        }
        None => None,
    };
    let mut order_by_exprs = Vec::with_capacity(select.order_by.len());
    for item in &select.order_by {
        // A bare integer ORDER BY item is a 1-based output position on the
        // aggregate path too (the plain path resolves it in
        // `substitute_output_aliases`; without this the position fell through
        // as a CONSTANT and the sort was silently dropped).
        let item_expr = if matches!(item.expr, tidb_ast::Expr::Int(_)) {
            substitute_output_aliases(&item.expr, select.fields.fields(), true)?
        } else {
            item.expr.clone()
        };
        let (expr, found) = extract_and_hoist_subquery(
            &item_expr,
            resolver.scope,
            catalog,
            current_db,
            &mut state.applies,
            &mut state.agg_funcs,
            &mut state.names,
            &mut state.types,
            &mut state.grouping_specs,
            &state.group_by_names,
            resolver,
            ctx,
        )?;
        let expr = if found {
            expr
        } else {
            substitute_aggregates(
                &expr,
                &mut state.agg_funcs,
                &mut state.names,
                &mut state.types,
                &mut state.grouping_specs,
                &state.group_by_names,
                resolver,
            )?
        };
        order_by_exprs.push((expr, item.desc));
    }

    state.having_expr = having_expr;
    state.order_by_exprs = order_by_exprs;
    Ok(())
}

/// Stage 4 (apply carriers): carry every column a correlated subquery reads
/// out of the aggregation.
///
/// Mirrors Go's `buildApply` requirement that an Apply's correlated columns
/// come from its outer child's schema: the outer row here is the GROUP row,
/// so a grouped column the select list does not project rides the same hidden
/// `FIRST_ROW` carrier `HAVING`'s aggregates use.
fn carry_apply_columns(
    state: &mut AggPipelineState,
    resolver: &ScopeResolver<'_>,
) -> Result<(), DriverError> {
    // An Apply binds its correlated columns from the AGGREGATION's output row,
    // so every column such a subquery reads must be carried out of the
    // aggregation. A grouped column the select list does not project rides the
    // same hidden FIRST_ROW carrier HAVING's aggregates use. This runs after
    // every clause has been walked, so it covers select-field, HAVING and
    // ORDER BY subqueries in one pass.
    for (correlated, _, _) in &state.applies {
        for path in &correlated.columns {
            let Some(name) = path.last() else { continue };
            if state
                .names
                .iter()
                .any(|have| have.eq_ignore_ascii_case(name))
            {
                continue;
            }
            let carrier = rewrite_expr_resolved(&tidb_ast::Expr::Column(path.clone()), resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            let ftype = carrier
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            state.agg_funcs.push(AggFunc {
                kind: AggKind::FirstRow,
                arg: Some(carrier),
                extra_args: Vec::new(),
                distinct: false,
                order_by: Vec::new(),
            });
            state.names.push(name.clone());
            state.types.push(ftype);
        }
    }
    Ok(())
}

/// Stage 5 (aggregate exec): build `Source -> Selection(WHERE) ->
/// Aggregation`.
///
/// Mirrors Go's `buildSelection` + `buildAggregation` operator construction,
/// including the `WITH ROLLUP` Expand path ([`run_rollup_aggregate`]).
fn build_aggregation(
    select: &tidb_ast::SelectStmt,
    from_source: Option<Box<dyn Executor>>,
    state: &mut AggPipelineState,
    resolver: &ScopeResolver<'_>,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    // GROUP BY expressions (legacy ASC/DESC direction ignored, as in MySQL 8).
    // A bare integer is a 1-based output position, resolved against the
    // SELECT list the same way ORDER BY's is -- see
    // `resolve_group_by_position`.
    let mut group_by = Vec::with_capacity(select.group_by.len());
    for item in &select.group_by {
        let resolved = resolve_group_by_position(&item.expr, select.fields.fields())?;
        group_by.push(
            rewrite_expr_resolved(resolved.as_ref(), resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }

    // Source (+ WHERE), as in the plain path.
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match from_source {
        Some(exec) => {
            let schema = exec.schema().clone();
            (exec, schema)
        }
        None => (
            Box::new(TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
                1,
            )),
            Schema::new(vec![]),
        ),
    };
    if let Some(predicate) = &select.where_clause {
        let pred = rewrite_expr_resolved(predicate, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        source = Box::new(SelectionExec::new(
            ExecutorMeta::new(source_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![pred],
            source,
            ctx.clone(),
        ));
    }

    // The aggregation output schema.
    let out_columns: Vec<Column> = state
        .types
        .iter()
        .enumerate()
        .map(|(i, ft)| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let out_schema = Schema::new(out_columns);

    let root: Box<dyn Executor> = if select.rollup {
        run_rollup_aggregate(
            source,
            &group_by,
            &state.agg_funcs,
            &out_schema,
            &state.types,
            &state.grouping_specs,
            ctx,
        )?
    } else {
        Box::new(HashAggExec::new(
            ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
            group_by,
            std::mem::take(&mut state.agg_funcs),
            source,
            ctx.clone(),
        ))
    };
    Ok(root)
}

/// Stage 6 (apply chain): every correlated subquery found above (select
/// fields, `HAVING`, `ORDER BY`) becomes an Apply over the aggregation's
/// output rows.
///
/// Mirrors Go's `buildApply` placement ABOVE the Aggregation: the outer row is
/// the GROUP row, so each subquery sees the grouped value and runs once per
/// group rather than once per source row, and `HAVING`/`ORDER BY` can then
/// read the appended column like any other aggregation output.
fn build_apply_chain(
    root: Box<dyn Executor>,
    state: &mut AggPipelineState,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let mut root = root;
    // Every correlated subquery found above (select fields, HAVING, ORDER BY)
    // becomes an Apply over the aggregation's output rows here, BEFORE HAVING
    // filters and ORDER BY sorts: the outer row is the GROUP row, so each
    // subquery sees the grouped value and runs once per group rather than
    // once per source row, and HAVING/ORDER BY can then read the appended
    // column like any other aggregation output.
    for (correlated, display, value_type) in std::mem::take(&mut state.applies) {
        let outer_scope = FromScope {
            tables: vec![FromTable {
                name: String::new(),
                database: None,
                columns: state
                    .names
                    .iter()
                    .cloned()
                    .zip(state.types.iter().cloned())
                    .collect(),
                offset: 0,
            }],
        };
        state.types.push(value_type);
        state.names.push(display);
        let columns: Vec<Column> = state
            .types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        // The callback outlives this borrow of the catalog, so it owns a
        // snapshot (see ApplyExec::new).
        let inner_catalog = catalog.clone();
        let inner_db = current_db.to_owned();
        let inner_ctx = ctx.clone();
        let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
            run_correlated_subquery(
                &correlated,
                values,
                &outer_scope,
                &inner_catalog,
                &inner_db,
                &inner_ctx,
            )
            .map_err(|e| match e {
                DriverError::Exec(exec) => exec,
                DriverError::SubqueryReturnsMoreThanOneRow => {
                    ExecError::SubqueryReturnsMoreThanOneRow
                }
                other => ExecError::Unsupported(driver_error_text(&other)),
            })
        });
        root = Box::new(crate::apply::ApplyExec::new(
            ExecutorMeta::new(Schema::new(columns), 7, INIT_CAP, MAX_CHUNK_SIZE),
            root,
            runner,
        ));
    }
    Ok(root)
}

/// Stage 7 (having): filter the aggregation's (+ Applies') output rows.
///
/// Mirrors Go's Selection above the Aggregation. Built after the Applies, so
/// the predicate can read an `__apply_N` column by name exactly like an
/// aggregate output.
fn build_having_stage(
    root: Box<dyn Executor>,
    state: &AggPipelineState,
    out_schema: &Schema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let mut root = root;
    let agg_resolver = agg_output_resolver(state);
    let out_schema = out_schema.clone();
    if let Some(having) = &state.having_expr {
        let predicate = rewrite_expr_resolved(having, &agg_resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        root = Box::new(SelectionExec::new(
            ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
            vec![predicate],
            root,
            ctx.clone(),
        ));
    }
    Ok(root)
}

/// Stage 8 (window): compute the hoisted window values between `HAVING` and
/// `ORDER BY`, exactly where Go plans the Window operator.
///
/// The rows it sees are the surviving GROUP rows (with any Apply-appended
/// subquery columns), and the sort below then orders the already-computed
/// window values. The synthetic `__window_<i>` names are kept in the state so
/// `ORDER BY` rewriting resolves them; the final projection puts the field's
/// own written text back on the visible column.
fn build_window_stage(
    root: Box<dyn Executor>,
    out_schema: Schema,
    state: &mut AggPipelineState,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, Schema), DriverError> {
    let mut root = root;
    let mut out_schema = out_schema;
    if !state.window_calls.is_empty() {
        let scope = FromScope {
            tables: vec![FromTable {
                name: String::new(),
                database: None,
                columns: state
                    .names
                    .iter()
                    .cloned()
                    .zip(state.types.iter().cloned())
                    .collect(),
                offset: 0,
            }],
        };
        let rows = drain_executor_rows(root, &state.types)?;
        let (rows, scope_with_windows) =
            crate::window::compute_windows(&state.window_calls, rows, &scope, ctx)?;
        // The synthetic `__window_<i>` names are kept here so the ORDER BY /
        // HAVING rewriting resolves them; the final projection puts the
        // field's own written text back on the visible column.
        for (name, field_type) in scope_with_windows
            .column_list()
            .into_iter()
            .skip(state.window_base)
        {
            state.names.push(name);
            state.types.push(field_type);
        }
        let columns: Vec<Column> = state
            .types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        out_schema = Schema::new(columns);
        root = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(out_schema.clone(), 0, INIT_CAP, MAX_CHUNK_SIZE),
            rows,
        ));
        // ORDER BY resolves against the WIDENED output, so an `ORDER BY` over
        // a window value reads the computed column.
    }
    Ok((root, out_schema))
}

/// Stage 9 (order / limit): `Sort` over the aggregation's (widened) output,
/// then `Limit`.
///
/// Mirrors Go's `buildSort` + `buildLimit` above the Window operator.
fn build_order_and_limit(
    root: Box<dyn Executor>,
    out_schema: &Schema,
    agg_resolver: &AggOutputResolver,
    select: &tidb_ast::SelectStmt,
    state: &AggPipelineState,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let mut root = root;
    let out_schema = out_schema.clone();
    if !state.order_by_exprs.is_empty() {
        let mut by_items = Vec::with_capacity(state.order_by_exprs.len());
        for (expr, desc) in &state.order_by_exprs {
            by_items.push(SortByItem {
                expr: rewrite_expr_resolved(expr, agg_resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
                desc: *desc,
            });
        }
        root = Box::new(SortExec::new(
            ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
            by_items,
            root,
            ctx.clone(),
        ));
    }
    if let Some(limit) = &select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        let limit_schema = root.schema().clone();
        root = Box::new(LimitExec::new(
            ExecutorMeta::new(limit_schema, 4, INIT_CAP, MAX_CHUNK_SIZE),
            offset,
            count,
            root,
        ));
    }
    Ok(root)
}

/// Stage 10 (final projection): trim the aggregation's output down to the
/// select list's own columns, in field order.
///
/// Mirrors Go's Projection above the Window/Aggregation: the aggregates and
/// carriers `HAVING`/`ORDER BY` needed but nothing selected are trimmed here,
/// and a select field that hoisted a correlated subquery is evaluated as the
/// full expression (generalized from a plain column read to
/// `rewrite_expr_resolved` so `SUM(v) + (SELECT ...)`-shaped fields can be
/// more than one column). A window column always needs the projection, if only
/// to put the field's written text back on it in place of the synthetic name.
fn build_final_projection(
    root: Box<dyn Executor>,
    select: &tidb_ast::SelectStmt,
    agg_resolver: &AggOutputResolver,
    state: &mut AggPipelineState,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let mut root = root;
    // The select list's own columns, in field order: the aggregates and
    // carriers HAVING/ORDER BY needed but nothing selected are trimmed here,
    // and a select field that hoisted a correlated subquery is evaluated as
    // the full expression (Go's final projection over the aggregation's
    // schema, generalized from a plain column read to `rewrite_expr_resolved`
    // so `SUM(v) + (SELECT ...)`-shaped fields can be more than one column).
    // A window column always needs the projection, if only to put the
    // field's written text back on it in place of the synthetic name.
    let has_expr_slot = state
        .slots
        .iter()
        .any(|slot| matches!(slot, OutputSlot::Expr(_)));
    if has_expr_slot || !state.window_calls.is_empty() {
        let visible: Vec<Expression> = state
            .slots
            .iter()
            .map(|slot| match slot {
                OutputSlot::Agg(index) => {
                    let mut col = Column::new((*index + 1) as i64, state.types[*index].clone());
                    col.index = *index as i64;
                    Ok(Expression::Column(col))
                }
                OutputSlot::Window(k) => {
                    let index = state.window_base + k;
                    let mut col = Column::new((index + 1) as i64, state.types[index].clone());
                    col.index = index as i64;
                    Ok(Expression::Column(col))
                }
                OutputSlot::Expr(index) => {
                    rewrite_expr_resolved(&state.post_agg_exprs[*index], agg_resolver)
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))
                }
            })
            .collect::<Result<_, DriverError>>()?;
        let visible_schema: Vec<Column> = visible
            .iter()
            .enumerate()
            .map(|(out, expr)| {
                let mut col = Column::new(
                    (out + 1) as i64,
                    expr.static_type()
                        .cloned()
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
                );
                col.index = out as i64;
                col
            })
            .collect();
        let field_names: Vec<String> = select
            .fields
            .fields()
            .iter()
            .map(|field| match field {
                SelectField::Expr { expr, alias } => {
                    alias.clone().unwrap_or_else(|| expr.restore())
                }
                _ => String::new(),
            })
            .collect();
        let field_types: Vec<FieldType> = visible_schema
            .iter()
            .map(|c| {
                c.ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
            })
            .collect();
        root = Box::new(ProjectionExec::new(
            ExecutorMeta::new(Schema::new(visible_schema), 5, INIT_CAP, MAX_CHUNK_SIZE),
            visible,
            root,
            ctx.clone(),
        ));
        state.names = field_names;
        state.types = field_types;
    } else {
        let sources: Vec<usize> = state
            .slots
            .iter()
            .map(|slot| match slot {
                OutputSlot::Agg(index) => *index,
                OutputSlot::Window(k) => state.window_base + k,
                OutputSlot::Expr(_) => unreachable!("no Expr slot when !has_expr_slot"),
            })
            .collect();
        if !sources.iter().copied().eq(0..state.types.len()) {
            let visible: Vec<Expression> = sources
                .iter()
                .map(|&i| {
                    let mut col = Column::new((i + 1) as i64, state.types[i].clone());
                    col.index = i as i64;
                    Expression::Column(col)
                })
                .collect();
            let visible_schema: Vec<Column> = sources
                .iter()
                .enumerate()
                .map(|(out, &i)| {
                    let mut col = Column::new((out + 1) as i64, state.types[i].clone());
                    col.index = out as i64;
                    col
                })
                .collect();
            root = Box::new(ProjectionExec::new(
                ExecutorMeta::new(Schema::new(visible_schema), 5, INIT_CAP, MAX_CHUNK_SIZE),
                visible,
                root,
                ctx.clone(),
            ));
            let projected_names: Vec<String> = state
                .slot_names
                .iter()
                .zip(&sources)
                .map(|(forced, &i)| forced.clone().unwrap_or_else(|| state.names[i].clone()))
                .collect();
            let projected_types: Vec<FieldType> =
                sources.iter().map(|&i| state.types[i].clone()).collect();
            state.names = projected_names;
            state.types = projected_types;
        }
    }
    Ok(root)
}

/// Stage 11 (distinct + drain): `SELECT DISTINCT` over the aggregate result,
/// then pull every row out of the plan.
///
/// Mirrors Go's `buildDistinct` above the projection.
fn distinct_and_drain(
    root: Box<dyn Executor>,
    select: &tidb_ast::SelectStmt,
    state: &mut AggPipelineState,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let mut root = root;
    let ret_types: Vec<FieldType> = state.types.clone();

    // `SELECT DISTINCT` over an aggregate result deduplicates the output
    // rows, the same buildDistinct step the plain path applies.
    if select.distinct {
        let columns: Vec<Column> = ret_types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        let schema = Schema::new(columns);
        root = Box::new(distinct_over(root, &schema, ctx));
    }

    root.open()?;
    let mut req = root.new_chunk();
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    loop {
        root.next(&mut req)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            let row = req.get_row(r);
            let values = ret_types
                .iter()
                .enumerate()
                .map(|(c, ft)| row.get_datum(c, ft))
                .collect();
            rows.push(values);
        }
    }
    root.close()?;
    Ok((
        std::mem::take(&mut state.names)
            .into_iter()
            .zip(ret_types)
            .collect(),
        rows,
    ))
}
