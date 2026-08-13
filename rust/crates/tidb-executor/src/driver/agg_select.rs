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
//! logical operators -- `DataSource -> Selection(WHERE) -> Apply* ->
//! Aggregation -> Apply* -> Selection(HAVING) -> Window -> Sort -> Limit ->
//! Projection` -- and every clause is lowered against whichever operator's
//! output it reads. This module keeps that chain explicit:
//! [`run_aggregate_select`] calls the stages in plan order and
//! [`AggPipelineState`] carries the aggregation's growing output columns
//! (names/types/functions) plus each clause's hoisted remainder between them.
//!
//! The two Apply positions are not a duplication. The one BELOW the
//! aggregation runs a subquery once per SOURCE row, for an aggregate's own
//! argument ([`hoist_pre_agg_subqueries`]); the one ABOVE it runs once per
//! GROUP row, for a select field / `HAVING` / `ORDER BY`
//! ([`build_apply_chain`]). Which one a subquery belongs to is decided by
//! where it is written, exactly as Go's rewriter decides it by which plan is
//! current when it reaches the subquery.

use super::*;
use tidb_expr::Columns;

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
    /// `FIRST_ROW` outputs whose source expression is a `GROUP BY` item.
    /// `WITH ROLLUP` replaces these derived grouping values with NULL for a
    /// subtotal, without nulling the raw columns the expression read.
    rollup_group_carriers: Vec<(usize, usize)>,
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
    /// Correlated subqueries found inside an aggregate's ARGUMENT, to apply
    /// BELOW the aggregation -- one per appended source column, in the order
    /// the columns are appended. See [`hoist_pre_agg_subqueries`].
    pre_agg_applies: Vec<(CorrelatedSubquery, FieldType)>,
    /// The source scope widened by the [`Self::pre_agg_applies`] columns: the
    /// scope every source-row expression (an aggregate's argument above all)
    /// resolves against once the Apply chain below the aggregation exists.
    pre_agg_scope: Option<FromScope>,
    /// `HAVING` with its aggregates hoisted, over the aggregation's output.
    having_expr: Option<tidb_ast::Expr>,
    /// `ORDER BY` with its aggregates hoisted, as `(expr, desc)`.
    order_by_exprs: Vec<(tidb_ast::Expr, bool)>,
}

/// Go `LogicalAggregation.ResetHintIfConflicted` plus the root-only portion
/// of `getEnforcedStreamAggs`: this driver has no query-block property graph,
/// so it honors only an unqualified `STREAM_AGG()` on this SELECT and supplies
/// the required order with a local sort.
fn force_stream_aggregation(select: &tidb_ast::SelectStmt, ctx: &crate::StmtContext) -> bool {
    use tidb_ast::HintKind;

    let mut hash = false;
    let mut stream = false;
    for hint in &select.hints {
        if !matches!(hint.kind, HintKind::Nullary { qb_name: None }) {
            continue;
        }
        if hint.name.eq_ignore_ascii_case("hash_agg") {
            hash = true;
        } else if hint.name.eq_ignore_ascii_case("stream_agg") {
            stream = true;
        }
    }
    if hash && stream {
        ctx.append_warning(1815, "Optimizer aggregation hints are conflicted");
        return false;
    }
    stream
}

/// A resolver over the aggregation's CURRENT output columns, which is what
/// `HAVING`, `ORDER BY` and the final projection are rewritten against (Go's
/// `Aggregation.Schema()`).
fn agg_output_resolver(state: &AggPipelineState, ctx: &crate::StmtContext) -> AggOutputResolver {
    AggOutputResolver {
        names: state.names.clone(),
        types: state.types.clone(),
        zone: ctx.session_zone(),
    }
}

/// Runs an aggregate `SELECT` (`GROUP BY` and/or aggregate select fields)
/// through [`HashAggExec`].
///
/// Faithful scope (deferred items documented): `COUNT`/`SUM` (Go models
/// `COUNT(*)` as the literal-`1` argument, which counts every row identically);
/// any non-aggregate select field becomes a `FIRST_ROW` carrier (Go's planner
/// does the same, once [`super::only_full_group_by`] has established the
/// group determines a value for it); `DISTINCT`
/// and other aggregate functions are rejected as unsupported. `WITH ROLLUP`
/// runs through [`run_rollup_aggregate`].
/// `HAVING` and `ORDER BY` run over the aggregation's output, as in Go: an
/// aggregate appearing only in those clauses is appended as a hidden output
/// column and trimmed by a final projection. `GROUPING()` rides the same
/// hidden-column path ([`add_grouping_column`]) but is filled in by the
/// rollup pass rather than aggregated.
///
/// `traced_select` is the statement as written (this pipeline rewrites the
/// one it executes), and `trace` records the two operators EXPLAIN reports
/// for an aggregate query -- the `HashAgg` this chain is built around and the
/// `LIMIT` above it. The stages between them (Apply, HAVING, Window, the
/// final projection) are executors the plan recorder has never printed, so
/// they stay out of the trace rather than widening what EXPLAIN reports.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_aggregate_select(
    select: &tidb_ast::SelectStmt,
    traced_select: &tidb_ast::SelectStmt,
    from_source: Option<Box<dyn Executor>>,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<SelectMeta, DriverError> {
    // Stage 0: ONLY_FULL_GROUP_BY. Go runs this before the GROUP BY
    // expressions are rewritten, on the clauses AS WRITTEN, which is also the
    // only point where the select list still distinguishes a bare column from
    // the FIRST_ROW carrier the stages below turn it into.
    super::only_full_group_by::check_only_full_group_by(select, resolver.scope, ctx)?;
    // ... and then the DISTINCT rule, in Go's order: `checkOnlyFullGroupBy`
    // runs in `buildSelect`, `checkOrderByInDistinct` later in `buildSort`.
    super::only_full_group_by::check_order_by_in_distinct(select, resolver.scope, ctx)?;

    let mut state = AggPipelineState {
        group_by_names: group_by_display_names(select, resolver),
        ..AggPipelineState::default()
    };

    // Stage 1a: hoist correlated subqueries out of the aggregates' ARGUMENTS,
    // which the Apply chain in stage 5b appends per SOURCE row. Runs before
    // every stage that lowers an aggregate, so the arguments those stages
    // build already read the appended column.
    let pre_agg = hoist_pre_agg_subqueries(select, &mut state, resolver, catalog, current_db, ctx)?;
    let select = pre_agg.as_ref().unwrap_or(select);
    // Source-row expressions resolve against the WIDENED scope from here on;
    // the aggregation sits above the Apply chain, so its input row carries the
    // appended columns. `resolver` stays the narrow source scope, which is
    // what the `WHERE` below the Applies reads.
    let widened_scope = state.pre_agg_scope.clone();
    let widened = ScopeResolver {
        scope: widened_scope.as_ref().unwrap_or(resolver.scope),
    };
    let source_resolver = &widened;

    // Stage 1: hoist window calls out of the select list / ORDER BY.
    let hoisted = hoist_window_calls(select, &mut state, source_resolver, ctx)?;
    let select = hoisted.as_ref().unwrap_or(select);

    // Stage 2: lower the select list into the aggregation.
    lower_select_fields(
        select,
        &mut state,
        source_resolver,
        catalog,
        current_db,
        ctx,
    )?;

    // Stage 3: lower HAVING / ORDER BY against the aggregation's output.
    hoist_having_and_order_by(
        select,
        &mut state,
        source_resolver,
        catalog,
        current_db,
        ctx,
    )?;

    // Stage 4: carry every column an Apply's subquery reads out of the
    // aggregation. Runs after every clause has been walked, so it covers
    // select-field, HAVING and ORDER BY subqueries in one pass.
    carry_apply_columns(&mut state, source_resolver)?;

    // Stage 5: Source -> Selection(WHERE) -> Aggregation.
    let root = build_aggregation(
        select,
        traced_select,
        from_source,
        &mut state,
        resolver,
        catalog,
        current_db,
        ctx,
        trace.as_deref_mut(),
    )?;

    // Stage 6: the Apply chain above the aggregation.
    let root = build_apply_chain(root, &mut state, catalog, current_db, ctx)?;

    // Stage 7: HAVING over the aggregation's (+ Applies') output rows.
    let out_schema = root.schema().clone();
    let root = build_having_stage(root, &state, &out_schema, ctx)?;

    // Stage 8: the window operator, between HAVING and ORDER BY.
    state.window_base = state.names.len();
    let (root, out_schema) = build_window_stage(root, out_schema, &mut state, ctx)?;

    // ORDER BY and the final projection resolve against the WIDENED output.
    let agg_resolver = agg_output_resolver(&state, ctx);

    // Stage 8b: `SELECT DISTINCT`'s dedup, BELOW the sort and the limit.
    //
    // Go `buildSelect` builds `Projection -> Distinct -> Sort -> Limit`
    // (`logical_plan_builder.go:4528-4602`), and `buildDistinct(p, oldLen)`
    // groups by the select list's own columns while carrying every other
    // column of the projection through a `FIRST_ROW` -- so the `ORDER BY`
    // carriers survive the dedup and the sort above can still read them.
    //
    // Running the dedup ABOVE the limit instead lets the limit truncate rows
    // the dedup would have collapsed: `select distinct a from t group by a, b
    // order by a limit 2` answered `1` where Go answers `1;2`.
    //
    // The dedup key is the agg-output column each select field reads. A field
    // that is a computed EXPRESSION has no such column until stage 10
    // evaluates it -- Go dedups on the projected column, which does not exist
    // here yet -- so that shape keeps the old order and its own bug, rather
    // than deduplicating on the wrong key. Documented, and narrower than what
    // it replaces.
    let dedup_keys: Option<Vec<usize>> = select
        .distinct
        .then(|| {
            state
                .slots
                .iter()
                .map(|slot| match slot {
                    OutputSlot::Agg(index) => Some(*index),
                    OutputSlot::Window(k) => Some(state.window_base + k),
                    OutputSlot::Expr(_) => None,
                })
                .collect::<Option<Vec<usize>>>()
        })
        .flatten();
    let deduplicated = dedup_keys.is_some();
    let root = match &dedup_keys {
        Some(keys) => {
            let schema = root.schema().clone();
            Box::new(distinct_over(root, &schema, keys, ctx)) as Box<dyn Executor>
        }
        None => root,
    };

    // Stage 9: ORDER BY, then LIMIT.
    let root = build_order_and_limit(
        root,
        &out_schema,
        &agg_resolver,
        select,
        traced_select,
        resolver,
        current_db,
        &state,
        deduplicated,
        ctx,
        trace.as_deref_mut(),
    )?;

    // Stage 10: the select list's own columns.
    let root = build_final_projection(root, select, &agg_resolver, &mut state, ctx)?;

    // Stage 11: DISTINCT (for the shape stage 8b declined), then drain.
    distinct_and_drain(
        root,
        select.distinct && !deduplicated,
        &mut state,
        ctx,
        trace,
    )
}

/// Stage 0: the grouped column names.
///
/// Mirrors Go's `resolveGbyExprs` / `buildAggregation` group-item naming: a
/// positional `GROUP BY 1` names the same column a literal `GROUP BY a`
/// would, so `GROUPING()` and `HAVING` see it the same way.
fn group_by_display_names(
    select: &tidb_ast::SelectStmt,
    resolver: &ScopeResolver<'_>,
) -> Vec<String> {
    select
        .group_by
        .iter()
        .filter_map(|item| {
            // A positional `GROUP BY 1` names the same column a literal
            // `GROUP BY a` would, so GROUPING() must see it the same way.
            let resolved = resolve_group_by_item(&item.expr, &select.fields, resolver).ok()?;
            match resolved.as_ref() {
                tidb_ast::Expr::Column(path) => path.last().cloned(),
                _ => None,
            }
        })
        .collect()
}

/// Stage 1a (pre-aggregation Apply hoist): pull every correlated subquery out
/// of an aggregate's ARGUMENT, so the aggregate reads a column an Apply BELOW
/// the aggregation appends per SOURCE row.
///
/// This is the placement Go's expression rewriter produces and `EXPLAIN`
/// confirms: for `SELECT SUM((SELECT id FROM emp WHERE emp.dept_id =
/// dept.id)) FROM dept`, TiDB prints `HashAgg <- Projection <- Apply(dept,
/// MaxOneRow(inner))`. The subquery is correlated to the OUTER query's source
/// row, and `buildAggregation` runs on the plan the rewriter left behind --
/// which by then is the Apply, not the DataSource. The consequences, all
/// captured from Go, are what make the placement observable:
///
/// * `SELECT COUNT((SELECT id FROM emp WHERE emp.dept_id = dept.id AND
///   emp.id = 10)) FROM dept` is `1`: the subquery runs three times, yields
///   NULL twice, and `COUNT` skips exactly those. Neither a single evaluation
///   nor a NULL-dropping filter can produce that number.
/// * With `GROUP BY`, the Apply still runs per SOURCE row: after adding a
///   second `eng` department the grouped `SUM` goes from `eng|2` to `eng|3`,
///   which only happens if both source rows ran the subquery and the
///   aggregate summed both results.
///
/// The correlated columns therefore bind from the SOURCE row, which is the
/// whole difference from [`carry_apply_columns`] / [`build_apply_chain`]
/// (Applies ABOVE the aggregation, binding from the grouped output row).
///
/// Returns the rewritten `SELECT` when anything was hoisted; the caller keeps
/// reading the original otherwise. A subquery inside a shape
/// [`extract_correlated_subquery`] does not reach (a `CASE` arm, a function
/// call) is left in place for [`build_agg_func`]'s refusal.
fn hoist_pre_agg_subqueries(
    select: &tidb_ast::SelectStmt,
    state: &mut AggPipelineState,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<tidb_ast::SelectStmt>, DriverError> {
    let mut rewritten = select.clone();
    let mut hoister = PreAggApplyHoister {
        outer: resolver.scope,
        catalog,
        current_db,
        ctx,
        base: resolver.scope.width(),
        found: Vec::new(),
        error: None,
    };
    // Every clause whose aggregates read source rows: the select list, plus
    // HAVING / ORDER BY, whose aggregates are hoisted into the same
    // aggregation by stage 3.
    for field in rewritten.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            hoister.visit(expr);
        }
    }
    if let Some(having) = rewritten.having.as_mut() {
        hoister.visit(having);
    }
    for item in &mut rewritten.order_by {
        hoister.visit(&mut item.expr);
    }
    if let Some(error) = hoister.error {
        return Err(error);
    }
    if hoister.found.is_empty() {
        return Ok(None);
    }
    // The scope the aggregate arguments now resolve against: the source's own
    // columns plus one appended column per hoisted subquery, named exactly the
    // placeholder the extraction left behind.
    let mut widened = resolver.scope.clone();
    for (_, value_type) in &hoister.found {
        let offset = widened.width();
        widened.tables.push(FromTable {
            name: String::new(),
            database: None,
            columns: vec![(format!("__apply_{offset}"), value_type.clone())],
            offset,
            func_deps: Default::default(),
        });
    }
    state.pre_agg_applies = hoister.found;
    state.pre_agg_scope = Some(widened);
    Ok(Some(rewritten))
}

/// Replaces every correlated subquery in an aggregate's argument with the
/// placeholder column an Apply below the aggregation will append.
struct PreAggApplyHoister<'a> {
    /// The SOURCE scope the subqueries are correlated to.
    outer: &'a FromScope,
    catalog: &'a Catalog,
    current_db: &'a str,
    ctx: &'a crate::StmtContext,
    /// The row offset the first appended column lands at.
    base: usize,
    found: Vec<(CorrelatedSubquery, FieldType)>,
    error: Option<DriverError>,
}

impl PreAggApplyHoister<'_> {
    fn visit(&mut self, expr: &mut tidb_ast::Expr) {
        tidb_ast::Visitable::accept(expr, self);
    }

    /// Hoists every correlated subquery out of one aggregate argument.
    ///
    /// The loop is what lets an argument hold more than one
    /// (`SUM(sub1 + sub2)`): each pass extracts the first one it reaches and
    /// leaves a placeholder, so the next pass sees the next. It stops as soon
    /// as a pass finds nothing correlated, which is also how an UNCORRELATED
    /// subquery (folded elsewhere) leaves the loop.
    fn hoist_arg(&mut self, arg: &mut tidb_ast::Expr) -> Result<(), DriverError> {
        while expr_has_subquery(arg) {
            let index = self.base + self.found.len();
            let mut correlated = None;
            let rewritten = extract_correlated_subquery(
                arg,
                self.outer,
                self.catalog,
                self.current_db,
                index,
                &mut correlated,
                self.ctx,
            )?;
            let Some(correlated) = correlated else {
                return Ok(());
            };
            let value_type = if matches!(correlated.kind, SubqueryKind::Scalar) {
                subquery_result_type(
                    &correlated,
                    self.outer,
                    self.catalog,
                    self.current_db,
                    self.ctx,
                )
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
            } else {
                FieldType::new(FieldTypeCode::LongLong)
            };
            self.found.push((correlated, value_type));
            *arg = rewritten;
        }
        Ok(())
    }
}

impl tidb_ast::Visitor for PreAggApplyHoister<'_> {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        if self.error.is_some() {
            return true;
        }
        let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
            return false;
        };
        match expr {
            // The aggregate's own arguments read SOURCE rows, which is what
            // makes a correlated subquery there an Apply below the
            // aggregation. Its children are not walked further: an aggregate
            // cannot nest inside another one's argument.
            tidb_ast::Expr::Aggregate { args, .. } | tidb_ast::Expr::GroupConcat { args, .. } => {
                for arg in args.iter_mut() {
                    if let Err(error) = self.hoist_arg(arg) {
                        self.error = Some(error);
                        break;
                    }
                }
                true
            }
            // A subquery that is NOT inside an aggregate's argument belongs to
            // the per-GROUP Apply above the aggregation (stage 6), and an
            // aggregate INSIDE such a subquery is that subquery's own, over
            // its own rows -- so this stage does not descend into either.
            tidb_ast::Expr::Subquery(_)
            | tidb_ast::Expr::Exists { .. }
            | tidb_ast::Expr::InSubquery { .. }
            | tidb_ast::Expr::CompareSubquery { .. } => true,
            _ => false,
        }
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        self.error.is_none()
    }
}

/// Stage 5b: the Apply chain BELOW the aggregation, one per correlated
/// subquery [`hoist_pre_agg_subqueries`] took out of an aggregate's argument.
///
/// Mirrors Go's `Apply` under the `HashAgg`: the outer row is the SOURCE row
/// (post-`WHERE`), so the subquery runs once per source row and its value --
/// NULL included -- reaches the accumulator as an ordinary column.
fn build_pre_agg_applies(
    source: Box<dyn Executor>,
    state: &mut AggPipelineState,
    scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let mut source = source;
    let mut scope = scope.clone();
    for (correlated, value_type) in std::mem::take(&mut state.pre_agg_applies) {
        let offset = scope.width();
        // Each Apply binds its correlated columns from the row BELOW it, which
        // is the source row plus whatever the Applies before it appended.
        let inner_scope = scope.clone();
        scope.tables.push(FromTable {
            name: String::new(),
            database: None,
            columns: vec![(format!("__apply_{offset}"), value_type)],
            offset,
            func_deps: Default::default(),
        });
        let columns: Vec<Column> = scope
            .column_list()
            .iter()
            .enumerate()
            .map(|(i, (_, ft))| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        let cache_columns = correlated_column_indices(&correlated, &inner_scope)?;
        // The callback outlives this borrow of the catalog, so it owns a
        // snapshot (see ApplyExec::new).
        let inner_catalog = catalog.clone();
        let inner_db = current_db.to_owned();
        let inner_ctx = ctx.clone();
        let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
            run_correlated_subquery(
                &correlated,
                values,
                &inner_scope,
                &inner_catalog,
                &inner_db,
                &inner_ctx,
            )
            .map_err(|e| match e {
                DriverError::Exec(exec) => exec,
                DriverError::SubqueryReturnsMoreThanOneRow => {
                    ExecError::SubqueryReturnsMoreThanOneRow
                }
                other => ExecError::unsupported(driver_error_text(&other)),
            })
        });
        source = Box::new(
            crate::apply::ApplyExec::new(
                ExecutorMeta::new(Schema::new(columns), 7, INIT_CAP, MAX_CHUNK_SIZE),
                source,
                runner,
                ctx.statement_memory(),
                // This Apply sits UNDER the aggregation -- its outer side is
                // the post-`WHERE` source row -- so Go's deselected-default-
                // row case cannot arise here either.
                None,
            )
            .with_cache(
                ctx.apply_cache_capacity(),
                cache_columns,
                ctx.session_zone(),
            ),
        );
    }
    Ok(source)
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
    ctx: &crate::StmtContext,
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
            tidb_expr::Columns::div_precision_increment(ctx),
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
    for (field_index, field) in select.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr, alias } = field else {
            return Err(DriverError::unsupported(
                "`*` is not supported in an aggregate SELECT",
            ));
        };
        let display = alias.clone().unwrap_or_else(|| {
            crate::driver::default_field_display_name(&select.fields, field_index, expr)
        });
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
                tidb_expr::Columns::div_precision_increment(ctx),
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
                let (func, ftype) = build_agg_func(
                    expr,
                    resolver,
                    tidb_expr::Columns::div_precision_increment(ctx),
                )?;
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
                // Go evaluates expressions around GROUPING() in the
                // projection above aggregation. The same aggregate
                // substitution used for `AVG(a) / 2` makes the grouping
                // column an ordinary input to that projection.
                let hoisted = substitute_aggregates(
                    other,
                    &mut state.agg_funcs,
                    &mut state.names,
                    &mut state.types,
                    &mut state.grouping_specs,
                    &state.group_by_names,
                    resolver,
                    tidb_expr::Columns::div_precision_increment(ctx),
                )?;
                if let Some(slot) = state.slots.last_mut() {
                    *slot = OutputSlot::Expr(state.post_agg_exprs.len());
                }
                if let Some(name) = state.slot_names.last_mut() {
                    *name = Some(display);
                }
                state.post_agg_exprs.push(hoisted);
            }
            // An aggregate inside a LARGER expression (`IF(1=1, COUNT(*), 0)`,
            // `AVG(a) / 2`, `CASE WHEN COUNT(*) > 2 ...`): Go's
            // `buildProjection` evaluates it ABOVE the aggregation, over the
            // aggregate results. Hoisting the aggregates and leaving the rest
            // as a post-aggregation expression is that projection -- the same
            // path a hoisted window value and a correlated subquery already
            // take, so this is not a new stage.
            other if other.has_aggregate_flag() => {
                let hoisted = substitute_aggregates(
                    other,
                    &mut state.agg_funcs,
                    &mut state.names,
                    &mut state.types,
                    &mut state.grouping_specs,
                    &state.group_by_names,
                    resolver,
                    tidb_expr::Columns::div_precision_increment(ctx),
                )?;
                if let Some(slot) = state.slots.last_mut() {
                    *slot = OutputSlot::Expr(state.post_agg_exprs.len());
                }
                if let Some(name) = state.slot_names.last_mut() {
                    *name = Some(display);
                }
                state.post_agg_exprs.push(hoisted);
            }
            other => {
                // A plain field in an aggregate query rides FIRST_ROW.
                let rewritten = rewrite_expr_resolved(other, resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                let t = rewritten
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                let output_index = state.agg_funcs.len();
                state.agg_funcs.push(AggFunc {
                    kind: AggKind::FirstRow,
                    arg: Some(rewritten),
                    extra_args: Vec::new(),
                    distinct: false,
                    order_by: Vec::new(),
                    arg_orig_name: String::new(),
                });
                // The aggregation's own column keeps the COLUMN's name, which
                // is the namespace `HAVING` and `GROUP BY` resolve against
                // (`SELECT b AS x ... GROUP BY b HAVING b > 0` is legal in Go
                // because the group item matched `b`, not the alias). The
                // OUTPUT name is a separate question, and it is the written
                // alias -- Go's `buildProjection` names the projected column
                // `field.AsName` whenever there is one, aggregate or not.
                // Without this the alias was LOST for exactly this shape:
                // `select b as x from ht group by b` reported the header `b`
                // where TiDB reports `x`.
                if let Some(name) = state.slot_names.last_mut() {
                    *name = Some(display.clone());
                }
                state.names.push(match other {
                    tidb_ast::Expr::Column(path) => {
                        path.last().cloned().unwrap_or_else(|| other.restore())
                    }
                    _ => display,
                });
                state.types.push(t);
                if select.rollup {
                    let item_text = other.restore();
                    if let Some(group_position) = select.group_by.iter().position(|item| {
                        resolve_group_by_item(&item.expr, &select.fields, resolver)
                            .is_ok_and(|resolved| resolved.restore() == item_text)
                    }) {
                        state
                            .rollup_group_carriers
                            .push((output_index, group_position));
                    }
                }
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
/// Go's `havingWindowAndOrderbyExprResolver` for `HAVING`: the clause is
/// resolved against the AGGREGATION's output, not the source rows, so it may
/// name a grouped column, a select-list output (its own alias included) or an
/// aggregate -- and nothing else.
///
/// An ungrouped source column is `ErrUnknownColumn` naming the `having
/// clause`, in EVERY `sql_mode`: captured from TiDB, `SELECT k, count(*) FROM
/// gg GROUP BY k HAVING v > 0` reports 1054 with `ONLY_FULL_GROUP_BY` both on
/// and off. That is why this is a name-resolution rule of its own rather than
/// part of [`super::only_full_group_by`], whose `ORDER BY` counterpart is
/// mode-gated and does admit a functionally dependent column.
fn check_having_names(
    having: &tidb_ast::Expr,
    state: &AggPipelineState,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let known = |name: &str| {
        state
            .names
            .iter()
            .chain(state.group_by_names.iter())
            .any(|candidate| candidate.eq_ignore_ascii_case(name))
    };
    for path in super::only_full_group_by::bare_columns(having) {
        let name = path.last().cloned().unwrap_or_default();
        // `__apply_N` is the placeholder a correlated subquery leaves behind,
        // and a hoisted window value is computed above the aggregation; both
        // are made real later and neither is a source column.
        if name.starts_with("__apply_") || crate::window::is_window_column(&name) {
            continue;
        }
        if !known(&name) {
            return Err(DriverError::UnknownColumnInClause {
                column: name,
                clause: "having clause".to_owned(),
            });
        }
    }
    // A SUBQUERY DOES NOT LAUNDER THE REFERENCE. `bare_columns` stops at a
    // subquery because its columns name the subquery's own `FROM`; the ones it
    // CORRELATES to are this clause's, and they answer to the same rule.
    //
    // Go gets there without a second pass:
    // `havingWindowAndOrderbyExprResolver.Enter` returns `skipChildren` for
    // `*ast.SubqueryExpr`/`*ast.ExistsSubqueryExpr`, so the correlated name is
    // resolved LATER, when the subquery is built -- against the outer plan,
    // which at `HAVING` time is the AGGREGATION's output. A column the
    // aggregation does not carry is nowhere to be found, and the clause it is
    // reported under is still `having clause`. Captured from real TiDB, on
    // `ht(a, b)` and `hs(x, y)`:
    //
    // ```text
    // select a from ht group by a having (select y from hs where hs.x = ht.b) > 0;
    //   [planner:1054]Unknown column 'ht.b' in 'having clause'
    // select a from ht group by a having exists (select 1 from hs where hs.x = ht.b);
    //   [planner:1054]Unknown column 'ht.b' in 'having clause'
    // select a from ht group by a having a in (select x from hs where hs.y = ht.b);
    //   [planner:1054]Unknown column 'ht.b' in 'having clause'
    // select max(b) from ht having (select y from hs where hs.x = ht.b) > 0;
    //   [planner:1054]Unknown column 'ht.b' in 'having clause'
    // select a, b from ht having (select y from hs where hs.x = ht.b) > 0;  -- 1|1
    // select a from ht group by a having (select y from hs where hs.x = ht.a) > 0;  -- 1
    // ```
    //
    // The last two are why this is the aggregation's OUTPUT and not the group
    // keys: `b` in the select list makes `ht.b` reachable with no `GROUP BY`
    // at all.
    //
    // The name is reported AS WRITTEN (`ht.b`, not `b`), which is Go's
    // `ErrUnknownColumn.GenWithStackByArgs(v.Name, ...)` over a
    // `*ast.ColumnName`.
    let mut correlated = Vec::new();
    for query in having_subqueries(having) {
        crate::driver::subquery::collect_correlated_columns_query(
            &query,
            resolver.scope,
            catalog,
            current_db,
            &mut correlated,
            ctx,
        );
    }
    for path in correlated {
        let Some(name) = path.last() else { continue };
        if name.starts_with("__apply_") || crate::window::is_window_column(name) {
            continue;
        }
        if !known(name) {
            return Err(DriverError::UnknownColumnInClause {
                column: path.join("."),
                clause: "having clause".to_owned(),
            });
        }
    }
    Ok(())
}

/// Every subquery body directly under a `HAVING` expression.
///
/// This is the set `havingWindowAndOrderbyExprResolver.Enter` skips
/// (`*ast.SubqueryExpr`, `*ast.ExistsSubqueryExpr`, and the subquery operand
/// of `IN`/`ANY`/`ALL`), collected so their correlated names can be checked
/// where Go checks them later. A subquery nested INSIDE one of these is not
/// visited: its correlations are the middle query's outer scope, not this
/// clause's, and the middle query reports its own.
pub(crate) fn having_subqueries(expr: &tidb_ast::Expr) -> Vec<QueryStmt> {
    struct Collector {
        found: Vec<QueryStmt>,
    }
    impl tidb_ast::Visitor for Collector {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() else {
                return false;
            };
            match expr {
                tidb_ast::Expr::Subquery(subquery)
                | tidb_ast::Expr::Exists { subquery, .. }
                | tidb_ast::Expr::InSubquery { subquery, .. }
                | tidb_ast::Expr::CompareSubquery { subquery, .. } => {
                    self.found.push((**subquery).clone());
                    // The operand beside the subquery (`expr IN (...)`'s left
                    // side) is a plain expression `bare_columns` already
                    // walked, so stopping here loses nothing.
                    true
                }
                _ => false,
            }
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut collector = Collector { found: Vec::new() };
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut collector);
    collector.found
}

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
            // A `HAVING` name the aggregation's output does not carry may be
            // a select-list alias for one that it does (`SELECT dept AS x
            // ... GROUP BY x HAVING x = 'a'`). Go reaches the select list
            // only in that order, so a name the output HAS keeps its meaning.
            let known = |name: &str| {
                state
                    .names
                    .iter()
                    .chain(state.group_by_names.iter())
                    .any(|candidate| candidate.eq_ignore_ascii_case(name))
            };
            let having =
                &substitute_output_aliases_where(having, select.fields.fields(), false, &known)?;
            check_having_names(having, state, resolver, catalog, current_db, ctx)?;
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
                    tidb_expr::Columns::div_precision_increment(ctx),
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
        let item_expr = if is_positional_field(&item.expr) {
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
                tidb_expr::Columns::div_precision_increment(ctx),
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
                arg_orig_name: String::new(),
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
#[allow(clippy::too_many_arguments)]
fn build_aggregation(
    select: &tidb_ast::SelectStmt,
    traced_select: &tidb_ast::SelectStmt,
    from_source: Option<Box<dyn Executor>>,
    state: &mut AggPipelineState,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<Box<dyn Executor>, DriverError> {
    let qualify = Qualifier {
        db: current_db,
        scope: resolver.scope,
    };
    // GROUP BY expressions (legacy ASC/DESC direction ignored, as in MySQL 8).
    // A bare integer is a 1-based output position, resolved against the
    // SELECT list the same way ORDER BY's is -- see
    // `resolve_group_by_position`.
    let mut group_by = Vec::with_capacity(select.group_by.len());
    for item in &select.group_by {
        let resolved = resolve_group_by_item(&item.expr, &select.fields, resolver)?;
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
    // Predicate push-down, exactly as the plain path does it: offer the scan
    // the conjuncts it can apply itself and keep only the residual in a
    // `Selection`. Go pushes a `Selection` through to the `DataSource` in
    // `rule_predicate_push_down`, which runs on the logical plan whether or
    // not an `Aggregation` sits above it -- and the aggregate shapes are
    // exactly the ones that most need it, because they drag every source row
    // across the seam and then return one row.
    let (executed_where, pushed_where) = super::access::negotiate_scan_filter(
        select,
        resolver.scope,
        &mut source,
        ctx,
        trace.as_deref_mut(),
    );
    let mut explained_where = trace.is_some().then_some(pushed_where);
    if let Some(predicate) = &executed_where {
        let mut pred = rewrite_expr_resolved(predicate, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        refine_comparisons(&mut pred, ctx);
        if let Some(explained) = &mut explained_where {
            explained.push(pred.clone());
        }
        source = Box::new(SelectionExec::new(
            ExecutorMeta::new(source_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![pred],
            source,
            ctx.clone(),
            ctx.statement_memory(),
        ));
    }
    // The `Selection` is RECORDED whenever the statement wrote a `WHERE`,
    // whether or not an executor survived above the scan: Go prints one
    // `Selection` for both halves, and this tier prints no `cop[tikv]` task
    // to distinguish them.
    if select.where_clause.is_some() {
        if let Some(trace) = trace.as_deref_mut() {
            if let Some(written) = &traced_select.where_clause {
                trace.selection(
                    written,
                    explained_where.as_deref(),
                    &qualify,
                    select_stats_selectivity(select, catalog, current_db, resolver.scope),
                );
                source = trace.meter(source);
            }
        }
    }

    // Stage 5b: the Applies for the aggregates' own arguments, between the
    // WHERE and the aggregation -- Go's `Apply` under the `HashAgg`.
    let mut source =
        build_pre_agg_applies(source, state, resolver.scope, catalog, current_db, ctx)?;

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

    let force_stream = force_stream_aggregation(traced_select, ctx);
    let root: Box<dyn Executor> = if select.rollup {
        run_rollup_aggregate(
            source,
            &group_by,
            &state.agg_funcs,
            &out_schema,
            &state.types,
            RollupOutputMetadata {
                grouping_specs: &state.grouping_specs,
                group_carriers: &state.rollup_group_carriers,
            },
            ctx,
        )?
    } else if force_stream {
        let input_meta = ExecutorMeta::new(source.schema().clone(), 2, INIT_CAP, MAX_CHUNK_SIZE);
        if !group_by.is_empty() {
            let by_items = group_by
                .iter()
                .cloned()
                .map(|expr| SortByItem { expr, desc: false })
                .collect();
            source = Box::new(SortExec::new(
                input_meta,
                by_items,
                source,
                ctx.clone(),
                ctx.statement_memory(),
            ));
            if let Some(trace) = trace.as_deref_mut() {
                trace.stream_agg_sort(&traced_select.group_by, &qualify);
                source = trace.meter(source);
            }
        }
        Box::new(StreamAggExec::new(
            ExecutorMeta::new(
                out_schema.clone(),
                if group_by.is_empty() { 2 } else { 3 },
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            group_by,
            std::mem::take(&mut state.agg_funcs),
            source,
            ctx.clone(),
            ctx.statement_memory(),
        ))
    } else {
        Box::new(HashAggExec::new(
            ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
            group_by,
            std::mem::take(&mut state.agg_funcs),
            source,
            ctx.clone(),
            ctx.statement_memory(),
        ))
    };
    let root = match trace {
        Some(trace) => {
            if force_stream && !select.rollup {
                trace.stream_agg(traced_select, &qualify);
            } else {
                trace.hash_agg(traced_select, &qualify);
            }
            trace.meter(root)
        }
        None => root,
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
        // Go's apply deselects the aggregation's default row (see
        // `ApplyExec::new`), and only a SCALAR subquery's apply is the left
        // outer join whose mismatch pads NULL.
        let miss_match = matches!(correlated.kind, SubqueryKind::Scalar).then_some(Datum::Null);
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
                func_deps: Default::default(),
            }],
            zone: ctx.session_zone(),
            tidb_info_len: ctx.tidb_info_len(),
            ..FromScope::default()
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
        let cache_columns = correlated_column_indices(&correlated, &outer_scope)?;
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
                other => ExecError::unsupported(driver_error_text(&other)),
            })
        });
        root = Box::new(
            crate::apply::ApplyExec::new(
                ExecutorMeta::new(Schema::new(columns), 7, INIT_CAP, MAX_CHUNK_SIZE),
                root,
                runner,
                ctx.statement_memory(),
                miss_match,
            )
            .with_cache(
                ctx.apply_cache_capacity(),
                cache_columns,
                ctx.session_zone(),
            ),
        );
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
    let agg_resolver = agg_output_resolver(state, ctx);
    let out_schema = out_schema.clone();
    if let Some(having) = &state.having_expr {
        let predicate = rewrite_expr_resolved(having, &agg_resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        root = Box::new(SelectionExec::new(
            ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
            vec![predicate],
            root,
            ctx.clone(),
            ctx.statement_memory(),
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
                func_deps: Default::default(),
            }],
            zone: ctx.session_zone(),
            tidb_info_len: ctx.tidb_info_len(),
            ..FromScope::default()
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
/// then `Limit` -- fused into one `TopN` when both are present, exactly as
/// the plain path fuses them.
///
/// Mirrors Go's `buildSort` + `buildLimit` above the Window operator, plus
/// `topn_push_down`. Go's `TopN` cannot push through a `LogicalAggregation`
/// (`LogicalAggregation` inherits `BaseLogicalPlan.PushDownTopN`, which
/// attaches it on top), so Go's TopN also lands here, above the aggregate:
/// captured `Projection_7|2.00|root` over `TopN_10|2.00|root||test.t.a,
/// offset:0, count:2` over the two-phase `HashAgg`.
///
/// `SELECT DISTINCT` fuses only once stage 8b has deduplicated BELOW this: a
/// bounded sort may not discard rows a dedup above it would have collapsed.
#[allow(clippy::too_many_arguments)]
fn build_order_and_limit(
    root: Box<dyn Executor>,
    out_schema: &Schema,
    agg_resolver: &AggOutputResolver,
    select: &tidb_ast::SelectStmt,
    traced_select: &tidb_ast::SelectStmt,
    resolver: &ScopeResolver<'_>,
    current_db: &str,
    state: &AggPipelineState,
    deduplicated: bool,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<Box<dyn Executor>, DriverError> {
    let mut root = root;
    let out_schema = out_schema.clone();
    let qualify = Qualifier {
        db: current_db,
        scope: resolver.scope,
    };
    let mut fused_topn = false;
    if !state.order_by_exprs.is_empty() {
        let mut by_items = Vec::with_capacity(state.order_by_exprs.len());
        for (expr, desc) in &state.order_by_exprs {
            by_items.push(SortByItem {
                expr: rewrite_expr_resolved(expr, agg_resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
                desc: *desc,
            });
        }
        // A bounded sort may only discard rows the dedup has ALREADY
        // collapsed. Stage 8b puts the dedup below this for every shape it
        // can key, and only the shape it declined still needs the guard.
        let fused_limit = if select.distinct && !deduplicated {
            None
        } else {
            select.limit.as_ref()
        };
        if let Some(limit) = fused_limit {
            let count = eval_limit_bound(&limit.count)?;
            let offset = match &limit.offset {
                Some(expr) => eval_limit_bound(expr)?,
                None => 0,
            };
            root = Box::new(TopNExec::new(
                ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
                by_items,
                root,
                ctx.clone(),
                offset,
                count,
                ctx.statement_memory(),
            ));
            fused_topn = true;
            if let Some(trace) = trace.as_deref_mut() {
                trace.topn(&traced_select.order_by, &qualify, offset, count);
                root = trace.meter(root);
            }
        } else {
            root = Box::new(SortExec::new(
                ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
                by_items,
                root,
                ctx.clone(),
                ctx.statement_memory(),
            ));
        }
    }
    if let Some(limit) = select.limit.as_ref().filter(|_| !fused_topn) {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        let limit_schema = root.schema().clone();
        root = Box::new(LimitExec::new(
            ExecutorMeta::new(limit_schema, 4, limit_init_cap(count), MAX_CHUNK_SIZE),
            offset,
            count,
            root,
        ));
        if let Some(trace) = trace {
            trace.limit(offset, count);
            root = trace.meter(root);
        }
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
            .enumerate()
            .map(|(field_index, field)| match field {
                SelectField::Expr { expr, alias } => alias.clone().unwrap_or_else(|| {
                    crate::driver::default_field_display_name(&select.fields, field_index, expr)
                }),
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
            let projected_types: Vec<FieldType> =
                sources.iter().map(|&i| state.types[i].clone()).collect();
            state.names = output_names(state, &sources);
            state.types = projected_types;
        } else {
            // No projection was NEEDED -- the aggregation's columns already
            // are the select list, in order -- but the names it reports are
            // still the field's own, not the aggregation's. This is where the
            // written alias used to be lost: `state.names` here holds the
            // COLUMN name a plain grouped field rides under.
            state.names = output_names(state, &sources);
        }
    }
    Ok(root)
}

/// The name each select field reports, over the aggregation's columns.
///
/// Go's `buildProjection` names a projected column `field.AsName` when the
/// field was written with one and its own derived name otherwise, which is
/// what `slot_names` records; `state.names` holds the AGGREGATION's column
/// names, which a grouped plain column keeps under the COLUMN's name so that
/// `HAVING` and `GROUP BY` still resolve against it.
fn output_names(state: &AggPipelineState, sources: &[usize]) -> Vec<String> {
    state
        .slot_names
        .iter()
        .zip(sources)
        .map(|(forced, &i)| {
            forced
                .clone()
                .unwrap_or_else(|| state.names.get(i).cloned().unwrap_or_default())
        })
        .collect()
}

/// Stage 11 (distinct + drain): `SELECT DISTINCT` over the aggregate result,
/// then pull every row out of the plan.
///
/// Mirrors Go's `buildDistinct` above the projection.
fn distinct_and_drain(
    root: Box<dyn Executor>,
    dedup: bool,
    state: &mut AggPipelineState,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
) -> Result<SelectMeta, DriverError> {
    let mut root = root;
    let ret_types: Vec<FieldType> = state.types.clone();

    // `SELECT DISTINCT` over an aggregate result deduplicates the output
    // rows, the same buildDistinct step the plain path applies.
    if dedup {
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
        let all: Vec<usize> = (0..schema.columns.len()).collect();
        root = Box::new(distinct_over(root, &schema, &all, ctx));
    }

    // Plain `EXPLAIN`: the pipeline is recorded, then dropped undrained.
    if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
        return Ok((
            std::mem::take(&mut state.names)
                .into_iter()
                .zip(ret_types)
                .collect(),
            Vec::new(),
        ));
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
            rows.push(req.get_row(r).get_datum_row(&ret_types));
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
