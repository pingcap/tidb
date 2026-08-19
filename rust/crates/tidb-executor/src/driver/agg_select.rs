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
    /// Canonical forms of the resolved GROUP BY expressions. Go's Expand
    /// stage matches GROUPING() arguments against this expression set, not
    /// merely against source-column names.
    group_by_exprs: Vec<String>,
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
    /// The physical aggregate was reordered to TiKV's partial grouped-SUM
    /// schema and therefore needs the select-order projection above it.
    partial_grouped_sum: bool,
    /// Where each physical grouped-StreamAgg state writes in the logical
    /// aggregation schema. TiKV puts aggregate functions before FIRST_ROW
    /// group carriers; the executor writes them directly into select order so
    /// Go's aggregation needs no restoring Projection above it.
    grouped_stream_output_positions: Option<Vec<usize>>,
    /// A top-level grouped partial StreamAgg retains Go's visible restoring
    /// Projection. Derived-table projection elimination uses
    /// [`Self::grouped_stream_output_positions`] instead.
    partial_grouped_stream_reordered: bool,
}

/// Go `LogicalAggregation.ResetHintIfConflicted` plus the root-only portion
/// of `getEnforcedStreamAggs`: an unqualified `STREAM_AGG()` supplies the
/// required group-key order with a local sort when the source does not.
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
        ctx.append_warning_once_parts(1815, "Optimizer aggregation hints are conflicted");
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
        constant_context: ctx.clone(),
        zone: ctx.session_zone(),
        no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
        div_precision_increment: ctx.div_precision_increment(),
    }
}

/// Whether Go's `EliminateAggregation` can replace this grouped query block
/// with a projection over one unique source row per group.
///
/// This first port deliberately owns the shape needed by TPCC condition 09:
/// one base table, plain-column group keys covering a non-null unique key,
/// and a select list made only of carried columns and single-column `SUM`s.
/// Every unsupported aggregate or clause fails closed and keeps the ordinary
/// aggregation pipeline.
pub(super) fn aggregation_can_be_eliminated(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> bool {
    if select.rollup
        || select.distinct
        || select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || !select.windows.is_empty()
    {
        return false;
    }
    let Some(table) = crate::driver::access::single_kv_table(&select.from, catalog, current_db)
    else {
        return false;
    };
    let column_offset = |path: &[String]| {
        let name = path.last()?;
        table
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
    };
    let Some(group_offsets) = select
        .group_by
        .iter()
        .map(|item| match &item.expr {
            tidb_ast::Expr::Column(path) => column_offset(path),
            _ => None,
        })
        .collect::<Option<std::collections::BTreeSet<_>>>()
    else {
        return false;
    };
    let key_is_covered =
        |key: &[usize]| !key.is_empty() && key.iter().all(|offset| group_offsets.contains(offset));
    let primary_is_covered = table
        .pk_handle_offset()
        .is_some_and(|offset| group_offsets.contains(&offset))
        || key_is_covered(table.common_handle_offsets());
    let unique_index_is_covered = table.plan_indexes().any(|index| {
        index.unique
            && index
                .prefix_lengths
                .iter()
                .all(|length| *length == crate::ddl::index_prefix::UNSPECIFIED_LENGTH)
            && key_is_covered(&index.column_offsets)
            && index.column_offsets.iter().all(|offset| {
                table.columns[*offset]
                    .field_type
                    .has_flag(FieldTypeFlags::NOT_NULL)
            })
    });
    if !primary_is_covered && !unique_index_is_covered {
        return false;
    }
    select.fields.fields().iter().all(|field| match field {
        tidb_ast::SelectField::Expr {
            expr: tidb_ast::Expr::Column(_),
            ..
        } => true,
        tidb_ast::SelectField::Expr {
            expr:
                tidb_ast::Expr::Aggregate {
                    name,
                    distinct: false,
                    args,
                },
            ..
        } => {
            name.eq_ignore_ascii_case("SUM")
                && matches!(args.as_slice(), [tidb_ast::Expr::Column(path)]
                if column_offset(path).is_some_and(|offset| {
                    table.columns[offset].field_type.code() == FieldTypeCode::NewDecimal
                }))
        }
        _ => false,
    })
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
/// runs through [`run_rollup_aggregate`] (plain-column grouping only).
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
    semantic_select: &tidb_ast::SelectStmt,
    from_source: Option<Box<dyn Executor>>,
    resolver: &ScopeResolver<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    access_consumed_where: bool,
    logical_rows: Option<f64>,
    joined_logical_rows: Option<f64>,
    grouped_logical_rows: Option<f64>,
    grouped_stream_ordered: bool,
    grouped_stream_physical_order: Option<Vec<usize>>,
    derived_output: bool,
    physical_source_columns: bool,
    input_candidate: Option<tidb_planner::candidate_cost::Candidate>,
    mut output_delivered: Option<&mut super::from::Delivered>,
    deferred_exec: Option<&mut Option<Box<dyn Executor>>>,
    mut trace: Option<&mut PlanTrace>,
) -> Result<SelectMeta, DriverError> {
    // Stage 0: ONLY_FULL_GROUP_BY. Go runs this before the GROUP BY
    // expressions are rewritten, on the clauses AS WRITTEN, which is also the
    // only point where the select list still distinguishes a bare column from
    // the FIRST_ROW carrier the stages below turn it into.
    super::only_full_group_by::check_only_full_group_by(
        semantic_select,
        resolver.scope,
        catalog,
        current_db,
        ctx,
    )?;
    // ... and then the DISTINCT rule, in Go's order: `checkOnlyFullGroupBy`
    // runs in `buildSelect`, `checkOrderByInDistinct` later in `buildSort`.
    super::only_full_group_by::check_order_by_in_distinct(semantic_select, resolver.scope, ctx)?;

    let group_by_exprs = group_by_expression_texts(select, resolver);
    let mut state = AggPipelineState {
        group_by_names: group_by_display_names(select, resolver),
        group_by_exprs,
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
    let (root, aggregate_candidate) = build_aggregation(
        select,
        traced_select,
        from_source,
        &mut state,
        resolver,
        catalog,
        current_db,
        ctx,
        access_consumed_where,
        logical_rows,
        joined_logical_rows,
        grouped_logical_rows,
        grouped_stream_ordered,
        grouped_stream_physical_order,
        derived_output,
        physical_source_columns,
        input_candidate,
        trace.as_deref_mut(),
    )?;
    if let Some(delivered) = output_delivered.as_deref_mut() {
        delivered.candidate = aggregate_candidate;
    }

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
    let has_scalar_aggregate_projection = state
        .slots
        .iter()
        .any(|slot| matches!(slot, OutputSlot::Expr(_)));
    let root = build_final_projection(root, select, &agg_resolver, &mut state, ctx)?;
    if has_scalar_aggregate_projection {
        if let Some(trace) = trace.as_deref_mut() {
            trace.aggregate_projection(
                traced_select,
                &Qualifier {
                    db: current_db,
                    scope: resolver.scope,
                    catalog: Some(catalog),
                },
            );
        }
    }
    if state.partial_grouped_sum {
        if let Some(trace) = trace.as_deref_mut() {
            trace.grouped_sum_projection(
                traced_select,
                &Qualifier {
                    db: current_db,
                    scope: resolver.scope,
                    catalog: Some(catalog),
                },
            );
        }
    }
    if state.partial_grouped_stream_reordered {
        if let Some(trace) = trace.as_deref_mut() {
            trace.grouped_stream_output_projection(
                traced_select,
                &Qualifier {
                    db: current_db,
                    scope: resolver.scope,
                    catalog: Some(catalog),
                },
            );
        }
    }
    // Stage 11: DISTINCT (for the shape stage 8b declined), then drain.
    distinct_and_drain(
        root,
        select.distinct && !deduplicated,
        &mut state,
        ctx,
        deferred_exec,
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

/// The resolved `GROUP BY` expressions in the same canonical spelling used
/// when matching `GROUPING(...)` arguments. Public HAVING/ORDER BY name
/// resolution continues to use [`group_by_display_names`].
fn group_by_expression_texts(
    select: &tidb_ast::SelectStmt,
    resolver: &ScopeResolver<'_>,
) -> Vec<String> {
    select
        .group_by
        .iter()
        .filter_map(|item| {
            resolve_group_by_item(&item.expr, &select.fields, resolver)
                .ok()
                .map(|expr| expr.restore())
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
            &state.group_by_exprs,
            resolver,
            ctx.div_precision_increment(),
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
                &state.group_by_exprs,
                resolver,
                ctx.div_precision_increment(),
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
            &state.group_by_exprs,
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
                let (func, ftype) = build_agg_func(expr, resolver, ctx.div_precision_increment())?;
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
                    &state.group_by_exprs,
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
                // projection above aggregation. Hoist the grouping column and
                // leave the surrounding expression for that projection.
                let hoisted = substitute_aggregates(
                    other,
                    &mut state.agg_funcs,
                    &mut state.names,
                    &mut state.types,
                    &mut state.grouping_specs,
                    &state.group_by_exprs,
                    resolver,
                    ctx.div_precision_increment(),
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
                    &state.group_by_exprs,
                    resolver,
                    ctx.div_precision_increment(),
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
                    .map_err(|e| super::eval_error_in_clause(e, "field list"))?;
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
                &state.group_by_exprs,
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
                    &state.group_by_exprs,
                    resolver,
                    ctx.div_precision_increment(),
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
            &state.group_by_exprs,
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
                &state.group_by_exprs,
                resolver,
                ctx.div_precision_increment(),
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
fn integer_decimal_precision(field_type: &FieldType) -> Option<i64> {
    Some(match field_type.code() {
        FieldTypeCode::Tiny => 3,
        FieldTypeCode::Short => 5,
        FieldTypeCode::Int24 => 8,
        FieldTypeCode::Long => 10,
        FieldTypeCode::LongLong => 20,
        FieldTypeCode::Year => 4,
        _ => return None,
    })
}

#[derive(Clone, Copy)]
enum GlobalStreamAggPlan {
    Count,
    /// A global COUNT above a join/derived source whose Go ver2 cost is lower
    /// as a serial StreamAgg than as a concurrent HashAgg.
    CountComplex,
    CountDistinct,
    IntegerSum {
        precision: i64,
    },
}

struct GroupedSumPlan {
    group_func: usize,
    sum_func: usize,
    group_input: usize,
    sum_input: usize,
    group_type: FieldType,
    sum_type: FieldType,
}

struct GroupedStreamPartialPlan {
    group_offsets: Vec<usize>,
    group_types: Vec<FieldType>,
    functions: Vec<crate::remote_scan::PushdownAggregateFunction>,
    state_order: Vec<usize>,
    sources: Vec<GroupedPartialSource>,
}

/// Compares the two root aggregation implementations using Go's default
/// cost-model-ver2 inputs. Their child is identical, so its cost is zero in
/// both candidates; a global aggregate has one output row and no group key.
fn prefer_stream_agg_for_global_count(input_rows: f64) -> bool {
    let factors = tidb_planner::plan_cost_ver2::Ver2Factors::default();
    let cost_factors = tidb_planner::plan_cost_ver2::CostFactorVars::default();
    let session = tidb_planner::plan_cost_ver2::CostSessionOpts::default();
    let child_cost = tidb_planner::cost_usage::new_zero_cost_ver2(false);
    let stream = tidb_planner::plan_cost_ver2::stream_agg_cost(
        None,
        input_rows,
        1,
        &[],
        (&factors.tidb_cpu, cost_factors.stream_agg),
        &child_cost,
    );
    let hash = tidb_planner::plan_cost_ver2::hash_agg_cost(
        None,
        tidb_planner::plan_cost_ver2::HashAggInput {
            input_rows,
            output_rows: 1.0,
            output_row_size: tidb_planner::plan_cost_ver2::MIN_ROW_SIZE,
            num_agg_funcs: 1,
            child_can_provide_order: false,
        },
        &[],
        (&factors.tidb_cpu, &factors.tidb_mem, cost_factors.hash_agg),
        session.hashagg_final_concurrency,
        tidb_planner::task_type::TaskType::Root,
        &child_cost,
    );
    stream.value() < hash.value()
}

#[derive(Clone, Copy)]
enum GroupedPartialSource {
    Function(usize),
    Group(usize),
}

struct GroupedInputProjectionPlan {
    expressions: Vec<Expression>,
    group_positions: Vec<usize>,
    function_positions: Vec<usize>,
    injected_for_scalar: bool,
    has_ungrouped_carrier: bool,
    state_order: Vec<usize>,
}

/// Go `WrapCastForAggFuncs`: integer SUM consumes DECIMAL rather than folding
/// in integer width, and that scalar cast is what triggers
/// `InjectProjBelowAgg`.
fn grouped_projection_argument(function: &AggFunc) -> Option<Expression> {
    let argument = function.arg.as_ref()?.clone();
    if !matches!(function.kind, AggKind::Sum) {
        return Some(argument);
    }
    let source_type = argument.static_type()?;
    let Some(precision) = integer_decimal_precision(source_type) else {
        return Some(argument);
    };
    let source_flags = source_type.flags();
    let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    decimal_type.set_flen(precision);
    decimal_type.set_decimal(0);
    decimal_type.add_flags(
        FieldTypeFlags::BINARY
            | (source_flags & (FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL)),
    );
    Some(Expression::ScalarFunction(
        tidb_expr::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("cast_decimal"),
            decimal_type,
            vec![argument],
        ),
    ))
}

fn add_grouped_projection_expression(
    expressions: &mut Vec<Expression>,
    expression: &Expression,
) -> Option<usize> {
    match expression {
        Expression::Column(_) => {}
        Expression::ScalarFunction(function)
            if function.func_name.lowercase() == "cast_decimal"
                && function.args.len() == 1
                && matches!(function.args[0], Expression::Column(_)) => {}
        _ => return None,
    }
    if let Some(position) = expressions
        .iter()
        .position(|existing| existing.equal(expression))
    {
        return Some(position);
    }
    let position = expressions.len();
    expressions.push(expression.clone());
    Some(position)
}

fn grouped_projection_expression_text(
    expression: &Expression,
    qualify: &Qualifier<'_>,
    derived_aliases: &[String],
) -> Option<String> {
    match expression {
        Expression::Column(column) => {
            let offset = usize::try_from(column.index).ok()?;
            let derived = qualify.scope.tables.iter().any(|table| {
                (table.offset..table.offset + table.columns.len()).contains(&offset)
                    && derived_aliases
                        .iter()
                        .any(|alias| alias.eq_ignore_ascii_case(&table.name))
            });
            Some(if derived {
                format!("Column#{offset}")
            } else {
                crate::driver::from::qualified_scope_column(qualify.scope, qualify.db, offset)
            })
        }
        Expression::ScalarFunction(function)
            if function.func_name.lowercase() == "cast_decimal" && function.args.len() == 1 =>
        {
            let argument =
                grouped_projection_expression_text(&function.args[0], qualify, derived_aliases)?;
            let result_type = function.ret_type.as_ref()?;
            Some(format!(
                "cast({argument}, decimal({},{}) BINARY)",
                result_type.flen(),
                result_type.decimal()
            ))
        }
        _ => None,
    }
}

fn collect_derived_aliases(node: &tidb_ast::JoinNode, aliases: &mut Vec<String>) {
    match node {
        tidb_ast::JoinNode::Join(join) => {
            collect_derived_aliases(&join.left, aliases);
            if let Some(right) = &join.right {
                collect_derived_aliases(right, aliases);
            }
        }
        tidb_ast::JoinNode::Derived { alias, .. } => {
            if let Some(alias) = alias {
                aliases.push(alias.clone());
            }
        }
        tidb_ast::JoinNode::Table(_) => {}
    }
}

/// The projection below a complex grouped HashAgg or StreamAgg. Go's
/// `InjectProjBelowAgg` emits real aggregate arguments first, group items
/// next, and `FIRST_ROW` carriers last when either a scalar wrapper or an
/// ungrouped carrier requires the projection. Otherwise the compact
/// projection produced by column pruning keeps group keys first.
fn grouped_input_projection_plan(
    group_by: &[Expression],
    functions: &[AggFunc],
) -> Option<GroupedInputProjectionPlan> {
    let mut expressions = Vec::<Expression>::new();
    let function_arguments = functions
        .iter()
        .map(grouped_projection_argument)
        .collect::<Option<Vec<_>>>()?;
    let injected_for_scalar = function_arguments
        .iter()
        .any(|argument| matches!(argument, Expression::ScalarFunction(_)));
    let has_ungrouped_carrier =
        functions
            .iter()
            .zip(&function_arguments)
            .any(|(function, argument)| {
                matches!(function.kind, AggKind::FirstRow)
                    && !group_by.iter().any(|group| group.equal(argument))
            });
    let (group_positions, function_positions) = if injected_for_scalar || has_ungrouped_carrier {
        // Column pruning moves FIRST_ROW group carriers behind the real
        // aggregate functions. They still read the group-key projection
        // slot, but must not make that slot appear before SUM's injected
        // cast (Go condition 04: cast(SUM arg), MAX arg, group key), nor may
        // an ungrouped carrier precede SUM (TPCC condition 08).
        let mut function_positions = vec![None; function_arguments.len()];
        for (position, (function, argument)) in function_positions
            .iter_mut()
            .zip(functions.iter().zip(&function_arguments))
        {
            if !matches!(function.kind, AggKind::FirstRow)
                && !group_by.iter().any(|group| group.equal(argument))
            {
                *position = Some(add_grouped_projection_expression(
                    &mut expressions,
                    argument,
                )?);
            }
        }
        let group_positions = group_by
            .iter()
            .map(|group| add_grouped_projection_expression(&mut expressions, group))
            .collect::<Option<Vec<_>>>()?;
        for (position, argument) in function_positions.iter_mut().zip(&function_arguments) {
            if position.is_none() {
                *position =
                    if let Some(group) = group_by.iter().position(|group| group.equal(argument)) {
                        Some(group_positions[group])
                    } else {
                        Some(add_grouped_projection_expression(
                            &mut expressions,
                            argument,
                        )?)
                    };
            }
        }
        let function_positions = function_positions
            .into_iter()
            .map(|position| position.expect("every aggregate argument has a projection slot"))
            .collect();
        (group_positions, function_positions)
    } else {
        let group_positions = group_by
            .iter()
            .map(|group| add_grouped_projection_expression(&mut expressions, group))
            .collect::<Option<Vec<_>>>()?;
        let function_positions = function_arguments
            .iter()
            .map(|argument| add_grouped_projection_expression(&mut expressions, argument))
            .collect::<Option<Vec<_>>>()?;
        (group_positions, function_positions)
    };
    let state_order = functions
        .iter()
        .enumerate()
        .filter(|(_, function)| !matches!(function.kind, AggKind::FirstRow))
        .map(|(index, _)| index)
        .chain(
            functions
                .iter()
                .enumerate()
                .filter(|(_, function)| matches!(function.kind, AggKind::FirstRow))
                .map(|(index, _)| index),
        )
        .collect();
    Some(GroupedInputProjectionPlan {
        expressions,
        group_positions,
        function_positions,
        injected_for_scalar,
        has_ungrouped_carrier,
        state_order,
    })
}

/// Go's aggregation above an injected projection reads the projection's
/// internal columns, not the source names the SQL wrote. Build the EXPLAIN
/// payload from the same positions installed into the executor below.
fn grouped_stream_physical_aggregate_info(
    group_positions: &[usize],
    function_positions: &[usize],
    functions: &[AggFunc],
) -> Option<String> {
    let mut groups = group_positions
        .iter()
        .map(|position| format!("Column#{position}"))
        .collect::<Vec<_>>();
    groups.sort();
    let functions = functions
        .iter()
        .zip(function_positions)
        .enumerate()
        .map(|(output, (function, input))| {
            let name = match &function.kind {
                AggKind::Count => "count",
                AggKind::FinalCount | AggKind::Sum => "sum",
                AggKind::FirstRow => "firstrow",
                AggKind::Min => "min",
                AggKind::Max => "max",
                AggKind::Avg => "avg",
                _ => return None,
            };
            Some(format!(
                "funcs:{name}({}Column#{input})->Column#{output}",
                if function.distinct { "distinct " } else { "" }
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(format!(
        "group by:{}, {}",
        groups.join(", "),
        functions.join(", ")
    ))
}

/// The grouped StreamAgg split used by TPCC condition queries whose ordered
/// table path already delivers the group keys. The accepted function set is
/// exactly the algebra TiKV can merge losslessly at the root today.
fn grouped_stream_partial_plan(
    select: &tidb_ast::SelectStmt,
    state: &AggPipelineState,
    group_by: &[Expression],
    has_pre_agg_applies: bool,
    grouped_stream_ordered: bool,
    source_has_no_residual_filter: bool,
) -> Option<GroupedStreamPartialPlan> {
    let order_by_matches_groups = select.order_by.is_empty()
        || (select.order_by.len() == select.group_by.len()
            && select
                .order_by
                .iter()
                .zip(&select.group_by)
                .all(|(order, group)| !order.desc && order.expr == group.expr));
    if !grouped_stream_ordered
        || !source_has_no_residual_filter
        || select.rollup
        || select.distinct
        || select.having.is_some()
        || !order_by_matches_groups
        || select.limit.is_some()
        || !state.window_calls.is_empty()
        || has_pre_agg_applies
        || group_by.is_empty()
        || state.agg_funcs.is_empty()
    {
        return None;
    }
    let mut group_offsets = Vec::with_capacity(group_by.len());
    let mut group_types = Vec::with_capacity(group_by.len());
    for group in group_by {
        group_offsets.push(
            group
                .as_column()
                .and_then(|column| usize::try_from(column.index).ok())?,
        );
        group_types.push(group.static_type()?.clone());
    }
    let mut functions = Vec::with_capacity(state.agg_funcs.len());
    let mut function_states = Vec::new();
    let mut carrier_states = Vec::new();
    for (state_index, (function, output_type)) in
        state.agg_funcs.iter().zip(&state.types).enumerate()
    {
        if function.distinct || !function.extra_args.is_empty() || !function.order_by.is_empty() {
            return None;
        }
        let input_offset = function
            .arg
            .as_ref()
            .and_then(Expression::as_column)
            .and_then(|column| usize::try_from(column.index).ok());
        let kind = match function.kind {
            AggKind::FirstRow => {
                let group = group_offsets
                    .iter()
                    .position(|offset| Some(*offset) == input_offset)?;
                carrier_states.push((state_index, GroupedPartialSource::Group(group)));
                continue;
            }
            AggKind::Count => {
                if input_offset.is_none()
                    && !matches!(
                        function.arg.as_ref(),
                        Some(Expression::Constant(constant))
                            if matches!(constant.value, Datum::Int(1) | Datum::UInt(1))
                    )
                {
                    return None;
                }
                crate::remote_scan::PushdownAggregateKind::Count
            }
            AggKind::Sum if input_offset.is_some() => {
                crate::remote_scan::PushdownAggregateKind::Sum
            }
            AggKind::Min if input_offset.is_some() => {
                crate::remote_scan::PushdownAggregateKind::Min
            }
            AggKind::Max if input_offset.is_some() => {
                crate::remote_scan::PushdownAggregateKind::Max
            }
            _ => return None,
        };
        functions.push(crate::remote_scan::PushdownAggregateFunction {
            kind,
            input_offset,
            output_type: output_type.clone(),
        });
        function_states.push((
            state_index,
            GroupedPartialSource::Function(functions.len() - 1),
        ));
    }
    if functions.is_empty() {
        return None;
    }
    // Go orders FIRST_ROW carriers by their physical source-column offset,
    // independently of the SELECT-list order. The final projection maps
    // these states back to the visible derived-table columns.
    carrier_states.sort_by_key(|(state_index, _)| {
        state.agg_funcs[*state_index]
            .arg
            .as_ref()
            .and_then(Expression::as_column)
            .map_or(i64::MAX, |column| column.index)
    });
    let ordered_states = if super::derived_agg_pruning::has_pruned_row_count(select) {
        // LogicalAggregation.PruneColumns removed the last explicit
        // aggregate and appended COUNT(1) after the surviving FIRST_ROW
        // carriers. The cop task still emits its partial COUNT before the
        // group tuple, but the root aggregation preserves that logical append
        // order while reading each state from its physical source slot.
        carrier_states.extend(function_states);
        carrier_states
    } else {
        // Go builds written aggregate functions first, then appends the
        // source-column FIRST_ROW carriers.
        function_states.extend(carrier_states);
        function_states
    };
    let (state_order, sources): (Vec<_>, Vec<_>) = ordered_states.into_iter().unzip();
    Some(GroupedStreamPartialPlan {
        group_offsets,
        group_types,
        functions,
        state_order,
        sources,
    })
}

/// Installs one grouped partial aggregation and rewires the root aggregate
/// states to consume TiKV's function-first, group-key-last output schema.
fn accept_grouped_partial(
    source: &mut Box<dyn Executor>,
    state: &mut AggPipelineState,
    group_by: &mut Vec<Expression>,
    plan: GroupedStreamPartialPlan,
    derived_output: bool,
    streamed: bool,
) -> Option<usize> {
    let GroupedStreamPartialPlan {
        group_offsets,
        group_types,
        functions,
        state_order,
        sources,
    } = plan;
    let aggregate_count = functions.len();
    let aggregate = PushdownPartialAggregate::Grouped {
        group_offsets,
        group_types,
        functions: functions.clone(),
        streamed,
    };
    if !source
        .table_access()
        .is_some_and(|access| access.accept_partial_aggregate(&aggregate))
    {
        return None;
    }

    let old_funcs = std::mem::take(&mut state.agg_funcs);
    state.agg_funcs = state_order
        .iter()
        .map(|index| old_funcs[*index].clone())
        .collect();
    if derived_output && streamed {
        // Go's projection elimination maps a derived relation directly onto
        // the aggregation schema. Keep the logical names/types/slots and
        // scatter physical states into them.
        state.grouped_stream_output_positions = Some(state_order);
    } else {
        // HashAgg writes states in physical function-first order, so a
        // derived output needs the same mapping as a top-level SELECT. Its
        // restoring Projection is executable bookkeeping for Go's eliminated
        // projection and is deliberately absent from the trace.
        let old_names = std::mem::take(&mut state.names);
        state.names = state_order
            .iter()
            .map(|index| old_names[*index].clone())
            .collect();
        let old_types = std::mem::take(&mut state.types);
        state.types = state_order
            .iter()
            .map(|index| old_types[*index].clone())
            .collect();
        let mut old_to_new = vec![0; state_order.len()];
        for (new, old) in state_order.iter().copied().enumerate() {
            old_to_new[old] = new;
        }
        for slot in &mut state.slots {
            if let OutputSlot::Agg(index) = slot {
                *index = old_to_new[*index];
            }
        }
        state.partial_grouped_stream_reordered =
            !derived_output && !state_order.iter().copied().eq(0..state_order.len());
    }

    for (function, source_kind) in state.agg_funcs.iter_mut().zip(&sources) {
        let source_column = match source_kind {
            GroupedPartialSource::Function(index) => *index,
            GroupedPartialSource::Group(index) => aggregate_count + *index,
        };
        let mut partial = source.schema().columns[source_column].clone();
        partial.index = source_column as i64;
        function.arg = Some(Expression::Column(partial));
        if matches!(source_kind, GroupedPartialSource::Function(index)
            if matches!(functions[*index].kind, crate::remote_scan::PushdownAggregateKind::Count))
        {
            function.kind = AggKind::FinalCount;
        }
    }
    group_by.clear();
    for index in aggregate_count..source.schema().columns.len() {
        let mut group = source.schema().columns[index].clone();
        group.index = index as i64;
        group_by.push(Expression::Column(group));
    }
    Some(aggregate_count)
}

/// Inserts the accepted TiKV partial aggregation below its reader so whole-tree
/// costing sees the same `scan -> cop agg -> reader -> final agg` shape as Go.
fn pushed_grouped_partial_candidate(
    candidate: tidb_planner::candidate_cost::Candidate,
    output_rows: f64,
    output_row_size: f64,
    num_agg_funcs: usize,
    group_items: &[bool],
    streamed: bool,
) -> tidb_planner::candidate_cost::Candidate {
    use tidb_planner::candidate_cost::{Candidate, RowSize};

    let Candidate::Reader {
        child,
        rows: input_rows,
        kind,
        ..
    } = candidate
    else {
        return candidate;
    };
    let aggregate = if streamed {
        Candidate::StreamAgg {
            child,
            input_rows,
            output_rows,
            row_size: RowSize::Fixed(output_row_size),
            num_agg_funcs,
            group_items: group_items.to_vec(),
        }
    } else {
        Candidate::HashAgg {
            child,
            input: tidb_planner::plan_cost_ver2::HashAggInput {
                input_rows,
                output_rows,
                output_row_size,
                num_agg_funcs,
                child_can_provide_order: false,
            },
            group_items: group_items.to_vec(),
        }
    };
    Candidate::Reader {
        child: Box::new(aggregate),
        rows: output_rows,
        row_size: RowSize::Fixed(output_row_size),
        kind,
    }
}

/// The same decomposable function set as grouped StreamAgg, but with no
/// ordering requirement. This is Go's cop HashAgg split.
fn grouped_hash_partial_plan(
    select: &tidb_ast::SelectStmt,
    state: &AggPipelineState,
    group_by: &[Expression],
    has_pre_agg_applies: bool,
    grouped_stream_ordered: bool,
    source_has_no_residual_filter: bool,
) -> Option<GroupedStreamPartialPlan> {
    if grouped_stream_ordered || !select.order_by.is_empty() {
        return None;
    }
    grouped_stream_partial_plan(
        select,
        state,
        group_by,
        has_pre_agg_applies,
        true,
        source_has_no_residual_filter,
    )
}

/// The bounded grouped aggregate TiKV can execute as a partial HashAgg.
///
/// Go chooses this split for TPCC delivery: one plain group-key carrier and
/// one non-DISTINCT SUM over a scan whose complete predicate is already in
/// its ranges.  Keep the contract structural so another statement with the
/// same physical shape receives the same plan, while HAVING/ORDER/LIMIT and
/// every richer aggregate remain on the ordinary root path.
fn grouped_sum_plan(
    select: &tidb_ast::SelectStmt,
    state: &AggPipelineState,
    group_by: &[Expression],
    has_pre_agg_applies: bool,
    scan_consumed_where: bool,
) -> Option<GroupedSumPlan> {
    if select.rollup
        || select.distinct
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || !state.window_calls.is_empty()
        || has_pre_agg_applies
        || !scan_consumed_where
        || group_by.len() != 1
        || state.agg_funcs.len() != 2
        || state.slots.len() != 2
        || select.fields.fields().len() != 2
    {
        return None;
    }
    let group_input = group_by[0]
        .as_column()
        .and_then(|column| usize::try_from(column.index).ok())?;
    let mut group_func = None;
    let mut sum_func = None;
    for (index, func) in state.agg_funcs.iter().enumerate() {
        if func.distinct || !func.extra_args.is_empty() || !func.order_by.is_empty() {
            return None;
        }
        let input = func
            .arg
            .as_ref()
            .and_then(Expression::as_column)
            .and_then(|column| usize::try_from(column.index).ok())?;
        match &func.kind {
            AggKind::FirstRow if input == group_input => group_func = Some(index),
            AggKind::Sum => sum_func = Some((index, input)),
            _ => return None,
        }
    }
    let group_func = group_func?;
    let (sum_func, sum_input) = sum_func?;
    if !state.slots.iter().all(
        |slot| matches!(slot, OutputSlot::Agg(index) if *index == group_func || *index == sum_func),
    ) {
        return None;
    }
    Some(GroupedSumPlan {
        group_func,
        sum_func,
        group_input,
        sum_input,
        group_type: state.types.get(group_func)?.clone(),
        sum_type: state.types.get(sum_func)?.clone(),
    })
}

/// Source-table identities behind a derived relation's physical columns.
/// Aggregate outputs deliberately remain `None` and therefore print as
/// internal `Column#N` values.
fn physical_source_column_names(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
) -> Vec<Option<String>> {
    let Some(from) = &select.from else {
        return vec![None; scope.width()];
    };
    (0..scope.width())
        .map(|offset| {
            let path = scope.qualified_path(offset)?;
            let [.., relation, column] = path.as_slice() else {
                return None;
            };
            let column = crate::driver::merge_decision::RelColumn {
                relation: relation.clone(),
                column: column.clone(),
            };
            crate::driver::merge_decision::physical_column_trace_name(
                &from.left, &column, catalog, current_db,
            )
            .or_else(|| {
                from.right.as_ref().and_then(|right| {
                    crate::driver::merge_decision::physical_column_trace_name(
                        right, &column, catalog, current_db,
                    )
                })
            })
        })
        .collect()
}

/// Builds the executable projection that replaces a grouped aggregation once
/// [`aggregation_can_be_eliminated`] proved every group contains at most one
/// source row. `FIRST_ROW(x)` becomes `x`; decimal `SUM(x)` becomes the same
/// widened DECIMAL cast Go installs during aggregation elimination.
fn eliminated_aggregation_projection(
    select: &tidb_ast::SelectStmt,
    state: &mut AggPipelineState,
    catalog: &Catalog,
    current_db: &str,
    has_pre_agg_applies: bool,
) -> Option<Vec<Expression>> {
    if has_pre_agg_applies || !aggregation_can_be_eliminated(select, catalog, current_db) {
        return None;
    }
    let mut expressions = Vec::with_capacity(state.agg_funcs.len());
    let mut result_types = state.types.clone();
    for (index, function) in state.agg_funcs.iter().enumerate() {
        if function.distinct || !function.extra_args.is_empty() || !function.order_by.is_empty() {
            return None;
        }
        let argument = function.arg.as_ref()?.clone();
        match function.kind {
            AggKind::FirstRow => expressions.push(argument),
            AggKind::Sum if argument.static_type()?.code() == FieldTypeCode::NewDecimal => {
                let result = super::agg_build::sum_result_type(argument.static_type());
                result_types[index] = result.clone();
                expressions.push(Expression::ScalarFunction(
                    tidb_expr::scalar_function::ScalarFunction::new(
                        tidb_ast::CiString::new("cast_decimal"),
                        result,
                        vec![argument],
                    ),
                ));
            }
            _ => return None,
        }
    }
    state.types = result_types;
    Some(expressions)
}

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
    access_consumed_where: bool,
    logical_rows: Option<f64>,
    joined_logical_rows: Option<f64>,
    grouped_logical_rows: Option<f64>,
    grouped_stream_ordered: bool,
    grouped_stream_physical_order: Option<Vec<usize>>,
    derived_output: bool,
    physical_source_columns: bool,
    input_candidate: Option<tidb_planner::candidate_cost::Candidate>,
    mut trace: Option<&mut PlanTrace>,
) -> Result<
    (
        Box<dyn Executor>,
        Option<tidb_planner::candidate_cost::Candidate>,
    ),
    DriverError,
> {
    let qualify = Qualifier {
        db: current_db,
        scope: resolver.scope,
        catalog: Some(catalog),
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
                .map_err(|e| super::eval_error_in_clause(e, "group statement"))?,
        );
    }
    // Go's StreamAgg property matching removes equality-fixed index prefixes
    // and places those fixed group keys after the ordered suffix. This changes
    // neither group identity nor visible output, but it does define the
    // physical group tuple and the aggregate carrier layout.
    let physical_group_order = grouped_stream_physical_order
        .as_ref()
        .and_then(|source_offsets| {
            source_offsets
                .iter()
                .map(|source_offset| {
                    group_by.iter().position(|group| {
                        group
                            .as_column()
                            .and_then(|column| usize::try_from(column.index).ok())
                            == Some(*source_offset)
                    })
                })
                .collect::<Option<Vec<_>>>()
        })
        .filter(|order| order.len() == group_by.len())
        .unwrap_or_else(|| (0..group_by.len()).collect());
    let reordered_groups = !physical_group_order.iter().copied().eq(0..group_by.len());
    if reordered_groups {
        group_by = physical_group_order
            .iter()
            .map(|index| group_by[*index].clone())
            .collect();
    }
    let physical_trace_select;
    let traced_select = if reordered_groups && traced_select.group_by.len() == group_by.len() {
        physical_trace_select = {
            let mut rewritten = traced_select.clone();
            rewritten.group_by = physical_group_order
                .iter()
                .map(|index| traced_select.group_by[*index].clone())
                .collect();
            let mut fields = rewritten.fields.fields().to_vec();
            let carrier_positions = fields
                .iter()
                .enumerate()
                .filter_map(|(index, field)| match field {
                    tidb_ast::SelectField::Expr {
                        expr: tidb_ast::Expr::Column(path),
                        ..
                    } if traced_select.group_by.iter().any(|group| {
                        matches!(&group.expr, tidb_ast::Expr::Column(group_path) if group_path == path)
                    }) => Some(index),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut carriers = carrier_positions
                .iter()
                .map(|index| fields[*index].clone())
                .collect::<Vec<_>>();
            carriers.sort_by_key(|field| match field {
                tidb_ast::SelectField::Expr {
                    expr: tidb_ast::Expr::Column(path),
                    ..
                } => resolver.resolve(path).map_or(usize::MAX, |column| column.0),
                _ => usize::MAX,
            });
            for (position, field) in carrier_positions.into_iter().zip(carriers) {
                fields[position] = field;
            }
            rewritten.fields = fields.into();
            rewritten
        };
        &physical_trace_select
    } else {
        traced_select
    };
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
        access_consumed_where,
        trace.as_deref_mut(),
    );
    let mut explained_where = trace.is_some().then_some(pushed_where);
    let scan_consumed_where = select.where_clause.is_some() && executed_where.is_none();
    if let Some(predicate) = &executed_where {
        // A correlated `EXISTS` conjunct becomes a SEMI JOIN before the
        // predicate is rewritten -- Go's `rule_decorrelate` reaching the
        // same shape under an `Aggregation` as under a plain SELECT. The
        // plain path runs this too (`run_select_traced_with_delivery`);
        // without it here, an aggregate over a correlated EXISTS reached
        // the expression rewriter with the subquery still in the tree and
        // failed as an unsupported form.
        //
        // A semi join emits LEFT rows only, so the source schema and the
        // resolver's scope are the same afterwards and every downstream
        // stage -- the group keys, the aggregate arguments, the HAVING --
        // resolves exactly as it did before.
        let decorrelated = super::decorrelate_exists::decorrelate_where(
            source,
            resolver.scope,
            predicate,
            catalog,
            current_db,
            ctx,
            trace.as_deref_mut(),
        )?;
        source = decorrelated.source;
        if let Some(residual) = decorrelated.residual {
            let mut pred = rewrite_expr_resolved(&residual, resolver)
                .map_err(|e| super::eval_error_in_clause(e, "where clause"))?;
            refine_comparisons(&mut pred, ctx)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
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
    }
    // The `Selection` is RECORDED whenever the statement wrote a `WHERE`,
    // whether or not an executor survived above the scan: Go prints one
    // `Selection` for both halves, and this tier prints no `cop[tikv]` task
    // to distinguish them.
    if !access_consumed_where {
        if let Some(predicate) = select.where_clause.as_ref() {
            if let Some(trace) = trace.as_deref_mut() {
                if let Some(written) = &traced_select.where_clause {
                    let stats =
                        select_stats_selectivity(select, catalog, current_db, resolver.scope);
                    if physical_source_columns {
                        let mut physical = rewrite_expr_resolved(predicate, resolver)
                            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                        refine_comparisons(&mut physical, ctx)
                            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                        let column_names = physical_source_column_names(
                            traced_select,
                            resolver.scope,
                            catalog,
                            current_db,
                        );
                        if !trace.physical_selection_with_columns(
                            &physical,
                            written,
                            stats,
                            &column_names,
                        ) {
                            trace.refuse(
                                "a pruned derived aggregation's Selection is not printable yet",
                            );
                        }
                    } else {
                        trace.selection(written, explained_where.as_deref(), &qualify, stats);
                    }
                    source = trace.meter(source);
                }
            }
        }
    }

    // Stage 5b: the Applies for the aggregates' own arguments, between the
    // WHERE and the aggregation -- Go's `Apply` under the `HashAgg`.
    let has_pre_agg_applies = !state.pre_agg_applies.is_empty();
    let mut source =
        build_pre_agg_applies(source, state, resolver.scope, catalog, current_db, ctx)?;
    let force_stream = force_stream_aggregation(traced_select, ctx) && !select.rollup;
    let aggregation_elimination = (!force_stream)
        .then(|| {
            eliminated_aggregation_projection(
                select,
                state,
                catalog,
                current_db,
                has_pre_agg_applies,
            )
        })
        .flatten();

    // Go's physical optimizer chooses a root StreamAgg for the two Sysbench
    // range families once the range has been fully consumed by a table/index
    // scan. A global aggregate has no ordering requirement, so Go also
    // enumerates root StreamAgg for COUNT(DISTINCT) above a join and lets the
    // cost comparison decide. Keep this deliberately narrow: one direct
    // global COUNT/SUM without an Apply.
    let complex_global_count = select.from.is_some()
        && crate::driver::access::single_kv_table(&select.from, catalog, current_db).is_none()
        && state.agg_funcs.len() == 1
        && matches!(state.agg_funcs[0].kind, AggKind::Count);
    let complex_stream_agg =
        complex_global_count && joined_logical_rows.is_none_or(prefer_stream_agg_for_global_count);
    let stream_plan = if !force_stream
        && !select.rollup
        && group_by.is_empty()
        && !has_pre_agg_applies
        && (complex_stream_agg || (!complex_global_count && scan_consumed_where))
        && state.agg_funcs.len() == 1
        && select.fields.fields().len() == 1
    {
        let func = &state.agg_funcs[0];
        if !func.extra_args.is_empty() || !func.order_by.is_empty() {
            None
        } else {
            match (&func.kind, func.arg.as_ref(), func.distinct) {
                (AggKind::Count, Some(_), false) => Some(if complex_global_count {
                    GlobalStreamAggPlan::CountComplex
                } else {
                    GlobalStreamAggPlan::Count
                }),
                (AggKind::Count, Some(_), true) => Some(GlobalStreamAggPlan::CountDistinct),
                (AggKind::Sum, Some(argument), false) => argument
                    .static_type()
                    .and_then(integer_decimal_precision)
                    .map(|precision| GlobalStreamAggPlan::IntegerSum { precision }),
                _ => None,
            }
        }
    } else {
        None
    };

    // Go's coster splits the high-cardinality Sysbench range aggregates into
    // a TiKV partial stage and a root final stage. The access source owns the
    // estimate and accepts only when that same decision applies; accepting
    // also changes its output schema to the one partial-result column.
    let partial_stream_agg = stream_plan.is_some_and(|plan| {
        let Some(input_offset) = state.agg_funcs[0]
            .arg
            .as_ref()
            .and_then(Expression::as_column)
            .and_then(|column| usize::try_from(column.index).ok())
        else {
            return false;
        };
        let output_type = state.types[0].clone();
        let aggregate = match plan {
            GlobalStreamAggPlan::Count => PushdownPartialAggregate::Count {
                input_offset,
                output_type,
            },
            GlobalStreamAggPlan::CountComplex | GlobalStreamAggPlan::CountDistinct => return false,
            GlobalStreamAggPlan::IntegerSum { .. } => PushdownPartialAggregate::Sum {
                input_offset,
                output_type,
            },
        };
        source
            .table_access()
            .is_some_and(|access| access.accept_partial_aggregate(&aggregate))
    });

    // The TPCC delivery grouped SUM is the corresponding HashAgg split. TiKV
    // returns partial aggregate functions before group keys, so acceptance
    // changes the physical root aggregation to [SUM, FIRST_ROW(group)] and
    // remaps the select slots; stage 10 then builds the real [group, SUM]
    // projection Go has above that final aggregation.
    let partial_grouped_sum = !force_stream
        && aggregation_elimination.is_none()
        && grouped_sum_plan(
            select,
            state,
            &group_by,
            has_pre_agg_applies,
            scan_consumed_where,
        )
        .is_some_and(|plan| {
            let aggregate = PushdownPartialAggregate::GroupBySum {
                group_offset: plan.group_input,
                sum_offset: plan.sum_input,
                sum_type: plan.sum_type.clone(),
                group_type: plan.group_type.clone(),
            };
            if !source
                .table_access()
                .is_some_and(|access| access.accept_partial_aggregate(&aggregate))
            {
                return false;
            }

            let old_funcs = std::mem::take(&mut state.agg_funcs);
            state.agg_funcs = vec![
                old_funcs[plan.sum_func].clone(),
                old_funcs[plan.group_func].clone(),
            ];
            let old_names = std::mem::take(&mut state.names);
            state.names = vec![
                old_names[plan.sum_func].clone(),
                old_names[plan.group_func].clone(),
            ];
            let old_types = std::mem::take(&mut state.types);
            state.types = vec![
                old_types[plan.sum_func].clone(),
                old_types[plan.group_func].clone(),
            ];
            for slot in &mut state.slots {
                if let OutputSlot::Agg(index) = slot {
                    *index = if *index == plan.sum_func {
                        0
                    } else {
                        debug_assert_eq!(*index, plan.group_func);
                        1
                    };
                }
            }

            let mut partial_sum = source.schema().columns[0].clone();
            partial_sum.index = 0;
            let mut partial_group = source.schema().columns[1].clone();
            partial_group.index = 1;
            state.agg_funcs[0].arg = Some(Expression::Column(partial_sum));
            state.agg_funcs[1].arg = Some(Expression::Column(partial_group.clone()));
            group_by.clear();
            group_by.push(Expression::Column(partial_group));
            state.partial_grouped_sum = true;
            true
        });

    // An unordered scan can run the same decomposable functions in a TiKV
    // partial HashAgg. The final root HashAgg merges the function-first
    // partial rows.
    let partial_grouped_hash_agg_funcs =
        (!force_stream && aggregation_elimination.is_none() && !partial_grouped_sum)
            .then(|| {
                grouped_hash_partial_plan(
                    select,
                    state,
                    &group_by,
                    has_pre_agg_applies,
                    grouped_stream_ordered,
                    executed_where.is_none(),
                )
            })
            .flatten()
            .and_then(|plan| {
                accept_grouped_partial(
                    &mut source,
                    state,
                    &mut group_by,
                    plan,
                    derived_output,
                    false,
                )
            });
    let partial_grouped_hash = partial_grouped_hash_agg_funcs.is_some();

    // An ordered grouped scan can run all decomposable functions at TiKV and
    // stream the partial groups through the reader. The final root stage
    // keeps the same grouping order; COUNT alone changes kind because it must
    // sum per-region partial counts rather than count partial rows.
    let partial_grouped_stream_agg_funcs = (!force_stream
        && aggregation_elimination.is_none()
        && !partial_grouped_sum
        && !partial_grouped_hash)
        .then(|| {
            grouped_stream_partial_plan(
                select,
                state,
                &group_by,
                has_pre_agg_applies,
                grouped_stream_ordered,
                executed_where.is_none(),
            )
        })
        .flatten()
        .and_then(|plan| {
            accept_grouped_partial(
                &mut source,
                state,
                &mut group_by,
                plan,
                derived_output,
                true,
            )
        });
    let partial_grouped_stream = partial_grouped_stream_agg_funcs.is_some();

    // A join/derived source cannot push this package to one scan. Go still
    // injects a real Projection below the ordered root StreamAgg so the
    // aggregate reads a compact, position-stable row. Rewriting the physical
    // expressions to those projected columns keeps the trace and executor on
    // the same operator boundary.
    let mut source_slot = Some(source);
    let mut grouped_input_projection_trace: Option<(Vec<String>, bool)> = None;
    let mut grouped_stream_physical_agg_trace = None;
    let mut derived_aliases = Vec::new();
    if let Some(from) = &select.from {
        collect_derived_aliases(&from.left, &mut derived_aliases);
        if let Some(right) = &from.right {
            collect_derived_aliases(right, &mut derived_aliases);
        }
    }
    let grouped_stream_extra_first_rows = state
        .agg_funcs
        .iter()
        .filter(|function| matches!(function.kind, AggKind::FirstRow))
        .filter_map(|function| function.arg.as_ref())
        .filter(|argument| !group_by.iter().any(|group| group.equal(argument)))
        .filter_map(|argument| {
            grouped_projection_expression_text(argument, &qualify, &derived_aliases)
        })
        .collect::<Vec<_>>();
    let grouped_input_projection = if !force_stream
        && aggregation_elimination.is_none()
        && !partial_grouped_sum
        && !partial_grouped_hash
        && !partial_grouped_stream
        && !physical_source_columns
        && crate::driver::access::single_kv_table(&select.from, catalog, current_db).is_none()
    {
        grouped_input_projection_plan(&group_by, &state.agg_funcs).is_some_and(|plan| {
            let GroupedInputProjectionPlan {
                expressions,
                group_positions,
                mut function_positions,
                injected_for_scalar,
                has_ungrouped_carrier,
                state_order,
            } = plan;
            // Go's post-optimization rule injects a projection for either
            // physical aggregation when an argument/group item is scalar.
            // The ordered StreamAgg path additionally uses this compact
            // projection for the carrier layout it already models.
            if !grouped_stream_ordered && !injected_for_scalar && !has_ungrouped_carrier {
                return false;
            }
            if !grouped_stream_ordered && !state_order.iter().copied().eq(0..state_order.len()) {
                let old_funcs = std::mem::take(&mut state.agg_funcs);
                state.agg_funcs = state_order
                    .iter()
                    .map(|index| old_funcs[*index].clone())
                    .collect();
                let old_names = std::mem::take(&mut state.names);
                state.names = state_order
                    .iter()
                    .map(|index| old_names[*index].clone())
                    .collect();
                let old_types = std::mem::take(&mut state.types);
                state.types = state_order
                    .iter()
                    .map(|index| old_types[*index].clone())
                    .collect();
                let mut old_to_new = vec![0; state_order.len()];
                for (new, old) in state_order.iter().copied().enumerate() {
                    old_to_new[old] = new;
                }
                for slot in &mut state.slots {
                    if let OutputSlot::Agg(index) = slot {
                        *index = old_to_new[*index];
                    }
                }
                function_positions = state_order
                    .iter()
                    .map(|index| function_positions[*index])
                    .collect();
            }
            let Some(trace_expressions) = expressions
                .iter()
                .map(|expression| {
                    grouped_projection_expression_text(expression, &qualify, &derived_aliases)
                })
                .collect::<Option<Vec<_>>>()
            else {
                return false;
            };
            if injected_for_scalar {
                grouped_stream_physical_agg_trace = grouped_stream_physical_aggregate_info(
                    &group_positions,
                    &function_positions,
                    &state.agg_funcs,
                );
            }
            let columns = expressions
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    let mut column = Column::new(
                        (index + 1) as i64,
                        expression
                            .static_type()
                            .expect("projection eligibility requires typed columns")
                            .clone(),
                    );
                    column.index = index as i64;
                    column
                })
                .collect::<Vec<_>>();
            let projection_schema = Schema::new(columns.clone());
            let child = source_slot.take().expect("source is installed once");
            source_slot = Some(Box::new(ProjectionExec::new(
                ExecutorMeta::new(projection_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
                expressions,
                child,
                ctx.clone(),
            )));
            for (group, position) in group_by.iter_mut().zip(group_positions) {
                *group = Expression::Column(columns[position].clone());
            }
            for (function, position) in state.agg_funcs.iter_mut().zip(function_positions) {
                function.arg = Some(Expression::Column(columns[position].clone()));
            }
            grouped_input_projection_trace = Some((trace_expressions, injected_for_scalar));
            true
        })
    } else {
        false
    };
    let mut source = source_slot.expect("source remains installed");

    // Build the aggregation schema after a partial grouped SUM has reordered
    // its physical outputs.
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
    let hash_agg_trace = trace
        .is_some()
        .then(|| {
            let column_names = grouped_input_projection_trace.as_ref().map_or_else(
                || physical_source_column_names(traced_select, resolver.scope, catalog, current_db),
                |(expressions, _)| expressions.iter().cloned().map(Some).collect(),
            );
            state
                .agg_funcs
                .iter()
                .all(|function| {
                    matches!(
                        &function.kind,
                        crate::hash_agg::AggKind::FirstRow | crate::hash_agg::AggKind::Sum
                    )
                })
                .then(|| (group_by.clone(), state.agg_funcs.clone(), column_names))
        })
        .flatten();

    let candidate_output_rows = if group_by.is_empty() {
        Some(1.0)
    } else {
        grouped_logical_rows
    };
    // A root final aggregation above a pushed TiKV partial aggregation reads
    // partial groups, not the base rows consumed by the coprocessor stage.
    // Go's attached plan therefore uses the partial aggregation's output
    // cardinality as the final HashAgg/StreamAgg input cardinality.
    let candidate_input_rows = if partial_grouped_hash || partial_grouped_stream {
        candidate_output_rows
    } else {
        joined_logical_rows.or(logical_rows)
    };
    // A normal physical aggregation owns a new logical schema and Go drops
    // the data source HistColl there. Dynamic IndexJoin probes are different:
    // `index_probe_candidate` rebuilds their aggregate from the data source
    // and deliberately applies `grouped_aggregation_avg_row_size`.
    let candidate_row_size = crate::access_cost::schema_avg_row_size(&state.types);
    let candidate_num_agg_funcs = state.agg_funcs.len();
    let candidate_group_items = group_by
        .iter()
        .map(|expression| matches!(expression, Expression::ScalarFunction(_)))
        .collect::<Vec<_>>();
    let input_candidate = match (input_candidate, candidate_output_rows) {
        (Some(child), Some(output_rows)) => {
            if let Some(num_agg_funcs) = partial_grouped_hash_agg_funcs {
                Some(pushed_grouped_partial_candidate(
                    child,
                    output_rows,
                    candidate_row_size,
                    num_agg_funcs,
                    &candidate_group_items,
                    false,
                ))
            } else if let Some(num_agg_funcs) = partial_grouped_stream_agg_funcs {
                Some(pushed_grouped_partial_candidate(
                    child,
                    output_rows,
                    candidate_row_size,
                    num_agg_funcs,
                    &candidate_group_items,
                    true,
                ))
            } else {
                Some(child)
            }
        }
        (child, _) => child,
    };

    if force_stream && !group_by.is_empty() {
        let by_items = group_by
            .iter()
            .cloned()
            .map(|expr| SortByItem { expr, desc: false })
            .collect();
        source = Box::new(SortExec::new(
            ExecutorMeta::new(source.schema().clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
            by_items,
            source,
            ctx.clone(),
            ctx.statement_memory(),
        ));
        if let Some(trace) = trace.as_deref_mut() {
            trace.enforced_stream_agg_sort(&traced_select.group_by, &qualify);
            source = trace.meter(source);
        }
    }

    let root: Box<dyn Executor> = if let Some(expressions) = &aggregation_elimination {
        Box::new(ProjectionExec::new(
            ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
            expressions.clone(),
            source,
            ctx.clone(),
        ))
    } else if select.rollup {
        run_rollup_aggregate(
            source,
            &group_by,
            &state.agg_funcs,
            &out_schema,
            &state.types,
            &state.grouping_specs,
            ctx,
        )?
    } else if force_stream {
        let agg_funcs = std::mem::take(&mut state.agg_funcs);
        if group_by.is_empty() {
            Box::new(StreamAggExec::new(
                ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
                agg_funcs,
                source,
                ctx.clone(),
            ))
        } else {
            let output_positions = (0..agg_funcs.len()).collect();
            Box::new(crate::hash_agg::GroupedStreamAggExec::new(
                ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
                group_by,
                agg_funcs,
                output_positions,
                source,
                ctx.clone(),
            ))
        }
    } else if let Some(stream_plan) = stream_plan {
        let mut agg_funcs = std::mem::take(&mut state.agg_funcs);
        if partial_stream_agg {
            let mut partial = source.schema().columns[0].clone();
            partial.index = 0;
            agg_funcs[0].arg = Some(Expression::Column(partial));
            if matches!(stream_plan, GlobalStreamAggPlan::Count) {
                agg_funcs[0].kind = AggKind::FinalCount;
            }
        } else if let GlobalStreamAggPlan::IntegerSum { precision } = stream_plan {
            let argument = agg_funcs[0]
                .arg
                .take()
                .expect("integer SUM eligibility requires one argument");
            let source_flags = argument.static_type().map_or(0, FieldType::flags);
            let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
            decimal_type.set_flen(precision);
            decimal_type.set_decimal(0);
            decimal_type.add_flags(
                FieldTypeFlags::BINARY
                    | (source_flags & (FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL)),
            );
            let cast = Expression::ScalarFunction(tidb_expr::scalar_function::ScalarFunction::new(
                tidb_ast::CiString::new("cast_decimal"),
                decimal_type.clone(),
                vec![argument],
            ));
            let mut projected_column = Column::new(1, decimal_type.clone());
            projected_column.index = 0;
            source = Box::new(ProjectionExec::new(
                ExecutorMeta::new(
                    Schema::new(vec![projected_column.clone()]),
                    1,
                    INIT_CAP,
                    MAX_CHUNK_SIZE,
                ),
                vec![cast],
                source,
                ctx.clone(),
            ));
            agg_funcs[0].arg = Some(Expression::Column(projected_column));
        }
        Box::new(StreamAggExec::new(
            ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
            agg_funcs,
            source,
            ctx.clone(),
        ))
    } else if grouped_stream_ordered && !partial_grouped_sum {
        let output_positions = state
            .grouped_stream_output_positions
            .take()
            .unwrap_or_else(|| (0..state.agg_funcs.len()).collect());
        Box::new(crate::hash_agg::GroupedStreamAggExec::new(
            ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
            group_by,
            std::mem::take(&mut state.agg_funcs),
            output_positions,
            source,
            ctx.clone(),
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
            if let Some(expressions) = aggregation_elimination.as_deref() {
                if !trace.scan_reader() {
                    trace.refuse("aggregation elimination child is not a bare scan");
                }
                let column_names = physical_source_column_names(
                    traced_select,
                    resolver.scope,
                    catalog,
                    current_db,
                );
                if !trace.aggregation_elimination_projection(expressions, &column_names) {
                    trace.refuse("aggregation elimination projection is not printable yet");
                }
            } else if force_stream {
                trace.enforced_stream_agg(traced_select, &qualify, grouped_logical_rows);
            } else if let Some(stream_plan) = stream_plan {
                if partial_stream_agg {
                    if !trace.partial_stream_agg(
                        traced_select,
                        &qualify,
                        matches!(stream_plan, GlobalStreamAggPlan::IntegerSum { .. }),
                    ) {
                        trace.refuse("partial StreamAgg child is not a bare table/index scan");
                    }
                } else if !matches!(
                    stream_plan,
                    GlobalStreamAggPlan::CountComplex | GlobalStreamAggPlan::CountDistinct
                ) && !trace.scan_reader_or_point_get()
                {
                    trace.refuse("global StreamAgg child is not a point get or bare scan");
                }
                if !partial_stream_agg {
                    if let GlobalStreamAggPlan::IntegerSum { precision } = stream_plan {
                        trace.sum_cast_projection(traced_select, &qualify, precision);
                    }
                }
                trace.stream_agg(
                    traced_select,
                    &qualify,
                    partial_stream_agg
                        || matches!(stream_plan, GlobalStreamAggPlan::IntegerSum { .. }),
                );
            } else if partial_grouped_sum {
                if !trace.partial_grouped_sum(traced_select, &qualify, grouped_logical_rows) {
                    trace.refuse("partial grouped SUM child is not a bare table scan");
                }
                trace.final_grouped_sum_hash_agg(traced_select, &qualify);
            } else if partial_grouped_hash {
                if !trace.partial_grouped_hash_agg(traced_select, &qualify, grouped_logical_rows) {
                    trace.refuse("partial grouped HashAgg child is not a supported scan");
                }
                trace.final_grouped_hash_agg(traced_select, &qualify);
            } else if partial_grouped_stream {
                if !trace.partial_grouped_stream_agg(traced_select, &qualify) {
                    trace.refuse("partial grouped StreamAgg child is not a bare scan");
                }
                trace.final_grouped_stream_agg(traced_select, &qualify);
            } else if grouped_stream_ordered {
                if let Some((expressions, injected_for_scalar)) =
                    grouped_input_projection_trace.as_ref()
                {
                    trace.grouped_input_projection(expressions, *injected_for_scalar);
                }
                trace.grouped_stream_agg(
                    traced_select,
                    &qualify,
                    grouped_input_projection,
                    grouped_logical_rows,
                    grouped_stream_physical_agg_trace.as_deref(),
                    &grouped_stream_extra_first_rows,
                );
            } else {
                if let Some((expressions, injected_for_scalar)) =
                    grouped_input_projection_trace.as_ref()
                {
                    trace.grouped_input_projection(expressions, *injected_for_scalar);
                }
                if let Some(info) = grouped_stream_physical_agg_trace.as_deref() {
                    trace.physical_hash_agg(info, grouped_logical_rows);
                } else if let Some((group_by, functions, column_names)) = hash_agg_trace.as_ref() {
                    trace.hash_agg_first_row_sum(
                        traced_select,
                        &qualify,
                        group_by,
                        functions,
                        column_names,
                        grouped_logical_rows,
                    );
                } else {
                    trace.hash_agg(traced_select, &qualify, grouped_logical_rows);
                }
            }
            trace.meter(root)
        }
        None => root,
    };
    let candidate = match (input_candidate, candidate_input_rows, candidate_output_rows) {
        (Some(child), Some(input_rows), _) if aggregation_elimination.is_some() => {
            let exprs = aggregation_elimination
                .as_ref()
                .expect("the guarded eliminated aggregation has a projection")
                .iter()
                .map(|expression| matches!(expression, Expression::ScalarFunction(_)))
                .collect();
            Some(tidb_planner::candidate_cost::Candidate::Projection {
                child: Box::new(child),
                input_rows,
                exprs,
            })
        }
        (Some(child), Some(input_rows), Some(output_rows))
            if aggregation_elimination.is_none()
                && !select.rollup
                && !partial_stream_agg
                && !partial_grouped_sum =>
        {
            let row_size = tidb_planner::candidate_cost::RowSize::Fixed(candidate_row_size);
            if force_stream
                || stream_plan.is_some()
                || grouped_stream_ordered
                || partial_grouped_stream
            {
                Some(tidb_planner::candidate_cost::Candidate::StreamAgg {
                    child: Box::new(child),
                    input_rows,
                    output_rows,
                    row_size,
                    num_agg_funcs: candidate_num_agg_funcs,
                    group_items: candidate_group_items,
                })
            } else {
                Some(tidb_planner::candidate_cost::Candidate::HashAgg {
                    child: Box::new(child),
                    input: tidb_planner::plan_cost_ver2::HashAggInput {
                        input_rows,
                        output_rows,
                        output_row_size: candidate_row_size,
                        num_agg_funcs: candidate_num_agg_funcs,
                        child_can_provide_order: false,
                    },
                    group_items: candidate_group_items,
                })
            }
        }
        _ => None,
    };
    Ok((root, candidate))
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
            ..FromScope::for_statement(ctx)
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
            .map_err(|e| super::eval_error_in_clause(e, "having clause"))?;
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
            ..FromScope::for_statement(ctx)
        };
        let rows = drain_executor_rows(root, &state.types, &ctx.statement_memory())?;
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
        catalog: None,
    };
    let mut fused_topn = false;
    if !state.order_by_exprs.is_empty() {
        let mut by_items = Vec::with_capacity(state.order_by_exprs.len());
        for (expr, desc) in &state.order_by_exprs {
            by_items.push(SortByItem {
                expr: rewrite_expr_resolved(expr, agg_resolver)
                    .map_err(|e| super::eval_error_in_clause(e, "order clause"))?,
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
    deferred_exec: Option<&mut Option<Box<dyn Executor>>>,
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

    if let Some(deferred) = deferred_exec {
        *deferred = Some(root);
        return Ok((
            std::mem::take(&mut state.names)
                .into_iter()
                .zip(ret_types)
                .collect(),
            Vec::new(),
        ));
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
        next_executor(root.as_mut(), &mut req, &ctx.statement_memory())?;
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
