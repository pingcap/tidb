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

//! The tree-level halves of the four rules this crate runs: predicate
//! pushdown, column pruning, TopN pushdown and key-info building.
//!
//! Every operator's LOCAL half already lives on the operator (see
//! `super::<operator>`); what lives here is the RECURSION and the
//! child-replacement that Go writes inline in each `PredicatePushDown` /
//! `PruneColumns` / `PushDownTopN` method body. None of it recurses: each
//! function is one [`super::fold::fold_owned`] call.
//!
//! # The stash discipline
//!
//! Several operators compute at DESCEND time a value that only ASCEND needs —
//! `LogicalJoin`'s `ret`, `LogicalSelection`'s unpushable conditions,
//! `LogicalJoin`'s parent used-column set. The fold does not carry it, because
//! it does not need to: descends happen in DFS pre-order and ascends in
//! post-order, which is exactly a stack discipline. Each rewriter keeps its
//! own `Vec` and pushes in `descend`, pops in `ascend`.

use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::normal_form::{expr_from_schema, split_cnf_items};
use tidb_expr::expr_util::predicates::{contains, is_mutable_effects_expr};
use tidb_expr::expr_util::substitute::{column_substitute, SubstituteOptions};
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{compose_cnf_condition, extract_columns};

use crate::base_arms;
use crate::cardinality::derive_stats::estimate_cols_ndv_with_matched_len;
use crate::cardinality::join::{
    estimate_full_join_row_count, FullJoinRowCountInput, JoinKeyEstimate,
};
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanError;
use crate::stats_info::StatsInfo;

use super::fold::{fold_owned, Descend, OwnedRewrite, RewriteFailure};
use super::rule::{
    add_selection, apply_predicate_simplification, conds_to_table_dual, RuleContext,
};
use super::schema_producer;
use super::{
    BaseLogicalPlan, LogicalExpand, LogicalLimit, LogicalMaxOneRow, LogicalPlan, LogicalProjection,
    LogicalSort, LogicalTableDual, LogicalTopN, LogicalUnionAll, LogicalUnionScan,
};

/// The schema an operator effectively exposes, materialized.
///
/// Go's `LogicalSchemaProducer.Schema()` MEMOISES the child's schema into the
/// operator; here the memo is skipped and the answer is recomputed, which is
/// the same value. See [`schema_producer::materialized_schema`].
fn effective_schema(node: &LogicalPlan) -> Schema {
    node.schema().cloned().unwrap_or_default()
}

/// The schemas of `node`'s children, in child order.
fn child_schemas(node: &LogicalPlan) -> Vec<Schema> {
    node.children().iter().map(effective_schema).collect()
}

/// Go `getAllJoinLeaf`: DataSource, Aggregation, and Projection each start a
/// new leaf schema; every other operator contributes the leaves of its children.
fn all_join_leaf_schemas(node: &LogicalPlan) -> Vec<Schema> {
    match node {
        LogicalPlan::DataSource(_) | LogicalPlan::Aggregation(_) | LogicalPlan::Projection(_) => {
            vec![effective_schema(node)]
        }
        _ => node
            .children()
            .iter()
            .flat_map(all_join_leaf_schemas)
            .collect(),
    }
}

/// Histogram-backed `Selectivity` for the equality shapes whose complete
/// inputs already live in [`StatsInfo`]. Go estimates a column/constant
/// equality as one value out of the column NDV (and an IN list as its number
/// of values out of that NDV). Returning `None` keeps genuinely pseudo tables
/// on `pseudoSelectivity`; it must not overwrite loaded NDVs with the pseudo
/// 1/1000 equality rate.
fn analyzed_filter_selectivity(table_stats: &StatsInfo, conditions: &[Expression]) -> Option<f64> {
    if table_stats.col_ndvs().is_empty() {
        return None;
    }
    if table_stats.row_count() == 0.0 || conditions.is_empty() {
        return Some(1.0);
    }

    let mut selectivity = 1.0_f64;
    let mut recognized = false;
    for condition in conditions {
        let Expression::ScalarFunction(function) = condition else {
            selectivity *= crate::cost_factors::SELECTION_FACTOR;
            continue;
        };
        let (column, values) = match (function.func_name.lowercase(), function.args.as_slice()) {
            (
                "eq" | "nulleq",
                [Expression::Column(column), Expression::Constant(_)]
                | [Expression::Constant(_), Expression::Column(column)],
            ) => (column, 1_usize),
            ("in", [Expression::Column(column), values @ ..])
                if !values.is_empty()
                    && values
                        .iter()
                        .all(|value| matches!(value, Expression::Constant(_))) =>
            {
                (column, values.len())
            }
            _ => {
                selectivity *= crate::cost_factors::SELECTION_FACTOR;
                continue;
            }
        };
        let ndv = table_stats.col_ndv(column.unique_id);
        if ndv > 0.0 {
            selectivity *= (values as f64 / ndv).min(1.0);
            recognized = true;
        } else {
            selectivity *= crate::cost_factors::SELECTION_FACTOR;
        }
    }
    if recognized {
        Some(selectivity.max(1.0 / table_stats.row_count().max(1.0)))
    } else {
        Some(selectivity)
    }
}

#[derive(Clone, Copy)]
struct PseudoRangeSelectivityNode {
    mask: u64,
    selectivity: f64,
    preferred_path: bool,
    num_columns: usize,
    id: i64,
}

fn covered_condition_mask(conditions: &[Expression], access: &[Expression]) -> u64 {
    conditions
        .iter()
        .enumerate()
        .fold(0_u64, |mask, (offset, condition)| {
            if contains(access, condition) {
                mask | (1_u64 << offset)
            } else {
                mask
            }
        })
}

/// Go `LogicalJoin.getProj`: make a child projection that initially exposes
/// the child's complete schema through identity expressions.
pub(crate) fn ensure_join_projection(
    ctx: &RuleContext<'_>,
    child: &mut LogicalPlan,
) -> Result<(), PlanError> {
    if matches!(child, LogicalPlan::Projection(_)) {
        return Ok(());
    }
    let schema = child
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("LogicalJoin.getProj: child has no schema"))?;
    let expressions = schema
        .columns
        .iter()
        .cloned()
        .map(Expression::Column)
        .collect();
    let output_names = child.output_names().to_vec();
    let query_block_offset = child.base().base.query_block_offset();
    let owned = std::mem::replace(child, LogicalPlan::TableDual(LogicalTableDual::default()));
    let mut projection = LogicalProjection::new(
        BaseLogicalPlan::new(ctx.allocator, "Projection", query_block_offset),
        expressions,
    );
    projection.base.base.set_schema(Some(schema));
    projection.base.base.set_output_names(output_names);
    projection.base.set_children(vec![owned]);
    *child = LogicalPlan::Projection(projection);
    Ok(())
}

/// Go `LogicalProjection.AppendExpr`, used only by `updateEQCond`.
pub(crate) fn append_join_projection_expr(
    ctx: &RuleContext<'_>,
    child: &mut LogicalPlan,
    expression: Expression,
) -> Result<Column, PlanError> {
    if let Expression::Column(column) = expression {
        return Ok(column);
    }
    let LogicalPlan::Projection(projection) = child else {
        return Err(PlanError::internal(
            "LogicalProjection.AppendExpr: join child is not a projection",
        ));
    };
    let schema = projection
        .base
        .base
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("LogicalProjection.AppendExpr: missing schema"))?;
    let expression = column_substitute(
        &expression,
        &schema,
        &projection.exprs,
        &SubstituteOptions::new(ctx.builder),
    );
    let ret_type = expression.static_type().cloned().ok_or_else(|| {
        PlanError::internal("LogicalProjection.AppendExpr: expression has no static type")
    })?;
    let column = Column::new(ctx.column_allocator.alloc(), ret_type);
    projection.exprs.push(expression);
    let mut schema = schema;
    schema.columns.push(column.clone());
    projection.base.base.set_schema(Some(schema));
    Ok(column)
}

/// Go `logicalop.InjectExpr`: make sure `plan` is a projection and append a
/// non-column expression in the child's expression space.
pub(crate) fn inject_join_expression(
    ctx: &RuleContext<'_>,
    mut plan: LogicalPlan,
    expression: Expression,
) -> Result<(LogicalPlan, Column), PlanError> {
    if let Expression::Column(column) = expression {
        return Ok((plan, column));
    }
    ensure_join_projection(ctx, &mut plan)?;
    let column = append_join_projection_expr(ctx, &mut plan, expression)?;
    Ok((plan, column))
}

fn build_join_equality(
    ctx: &RuleContext<'_>,
    left: Column,
    right: Column,
) -> Result<ScalarFunction, PlanError> {
    let expression = ctx
        .builder
        .new_function(
            "eq",
            Some(tidb_expr::expr_util::builder::tiny_int_type()),
            vec![Expression::Column(left), Expression::Column(right)],
        )
        .map_err(|error| PlanError::internal(error.to_string()))?;
    let Expression::ScalarFunction(function) = expression else {
        return Err(PlanError::internal(
            "LogicalJoin.updateEQCond: equality is not a scalar function",
        ));
    };
    Ok(function)
}

/// Go `LogicalJoin.updateEQCond`'s normal-equality half.
///
/// Predicate pushdown can leave `expr(left) = expr(right)` in
/// `OtherConditions`. Physical joins require `column = column`, so Go moves
/// those conditions into `EqualConditions`, materializing either expression
/// under a child projection when necessary. This is also the prerequisite for
/// `JoinKeyTypeCastRewriter`: its input is the pair of DOUBLE projection
/// columns created here, never a bare INT/VARCHAR comparison.
fn update_join_equal_conditions(
    ctx: &RuleContext<'_>,
    join: &mut super::LogicalJoin,
) -> Result<(), PlanError> {
    let [left_schema, right_schema] = child_schemas(&LogicalPlan::Join(join.clone()))
        .try_into()
        .map_err(|_| PlanError::internal("LogicalJoin.updateEQCond needs two children"))?;

    let mut extracted = Vec::new();
    let mut remove = vec![false; join.other_conditions.len()];
    for index in (0..join.other_conditions.len()).rev() {
        let condition = &join.other_conditions[index];
        let Expression::ScalarFunction(function) = condition else {
            continue;
        };
        if function.func_name.lowercase() != "eq" || super::join::is_eq_cond_from_in(condition) {
            continue;
        }
        let [left, right] = function.args.as_slice() else {
            continue;
        };
        let pair = if expr_from_schema(left, &left_schema) && expr_from_schema(right, &right_schema)
        {
            Some((left.clone(), right.clone()))
        } else if expr_from_schema(left, &right_schema) && expr_from_schema(right, &left_schema) {
            Some((right.clone(), left.clone()))
        } else {
            None
        };
        if let Some(pair) = pair {
            remove[index] = true;
            extracted.push(pair);
        }
    }
    if extracted.is_empty() {
        return Ok(());
    }
    join.other_conditions = std::mem::take(&mut join.other_conditions)
        .into_iter()
        .enumerate()
        .filter_map(|(index, condition)| (!remove[index]).then_some(condition))
        .collect();

    let mut left_projection = extracted
        .iter()
        .any(|(left, _)| !matches!(left, Expression::Column(_)));
    let mut right_projection = extracted
        .iter()
        .any(|(_, right)| !matches!(right, Expression::Column(_)));
    if left_projection {
        ensure_join_projection(ctx, &mut join.base.children_mut()[0])?;
    }
    if right_projection {
        ensure_join_projection(ctx, &mut join.base.children_mut()[1])?;
    }

    for (left_expression, right_expression) in extracted {
        let keep_as_other =
            is_mutable_effects_expr(&left_expression) || is_mutable_effects_expr(&right_expression);
        let mut left_key = if left_projection {
            append_join_projection_expr(ctx, &mut join.base.children_mut()[0], left_expression)?
        } else {
            let Expression::Column(column) = left_expression else {
                unreachable!("projection need was computed from every extracted key")
            };
            column
        };
        let mut right_key = if right_projection {
            append_join_projection_expr(ctx, &mut join.base.children_mut()[1], right_expression)?
        } else {
            let Expression::Column(column) = right_expression else {
                unreachable!("projection need was computed from every extracted key")
            };
            column
        };

        let mut equality = build_join_equality(ctx, left_key.clone(), right_key.clone())?;
        if !matches!(equality.args.first(), Some(Expression::Column(_))) {
            if !left_projection {
                ensure_join_projection(ctx, &mut join.base.children_mut()[0])?;
                left_projection = true;
            }
            left_key = append_join_projection_expr(
                ctx,
                &mut join.base.children_mut()[0],
                equality.args[0].clone(),
            )?;
        } else if let Expression::Column(column) = &equality.args[0] {
            left_key = column.clone();
        }
        if !matches!(equality.args.get(1), Some(Expression::Column(_))) {
            if !right_projection {
                ensure_join_projection(ctx, &mut join.base.children_mut()[1])?;
                right_projection = true;
            }
            right_key = append_join_projection_expr(
                ctx,
                &mut join.base.children_mut()[1],
                equality.args[1].clone(),
            )?;
        } else if let Expression::Column(column) = &equality.args[1] {
            right_key = column.clone();
        }
        if !matches!(
            equality.args.as_slice(),
            [Expression::Column(_), Expression::Column(_)]
        ) {
            equality = build_join_equality(ctx, left_key, right_key)?;
        }
        if keep_as_other {
            join.other_conditions
                .push(Expression::ScalarFunction(equality));
        } else {
            join.equal_conditions.push(equality);
        }
    }
    Ok(())
}

fn update_join_equal_conditions_in_plan(
    ctx: &RuleContext<'_>,
    plan: &mut LogicalPlan,
) -> Result<bool, PlanError> {
    match plan {
        LogicalPlan::Join(join) => {
            let mut updated = join.clone();
            update_join_equal_conditions(ctx, &mut updated)?;
            *join = updated;
            Ok(true)
        }
        LogicalPlan::Apply(apply) => {
            let mut updated = apply.join.clone();
            update_join_equal_conditions(ctx, &mut updated)?;
            apply.join = updated;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Go `cardinality.Selectivity` over the pseudo column/index histograms that
/// `statistics.PseudoTable` creates. This is intentionally distinct from
/// `pseudoSelectivity`: that coarse fallback is used only when the histogram
/// collection has no columns/indexes (or more than 63 predicates). A normal
/// unanalyzed table still builds ranges, so `k >= 1 AND k <= 3` is one
/// bounded range (`1/pseudoBetweenRate`), not two unrelated `1/3` guesses.
fn pseudo_range_filter_selectivity(
    source: &super::data_source::DataSource,
    table_stats: &StatsInfo,
    conditions: &[Expression],
    schema: &Schema,
) -> Option<f64> {
    if conditions.is_empty() || table_stats.row_count() == 0.0 {
        return Some(1.0);
    }
    if conditions.len() > 63 {
        return None;
    }

    let rows = table_stats.row_count();
    let mut nodes = Vec::new();
    for column in &schema.columns {
        let field_type = column.ret_type.as_ref()?;
        let (access, _) =
            crate::ranger::detacher::detach_conds_for_column(conditions, column, true);
        if access.is_empty() {
            continue;
        }
        let range_result = crate::ranger::ranger::build_column_range(
            &access,
            field_type,
            crate::ranger::checker::UNSPECIFIED_LENGTH,
            0,
        )
        .ok()?;
        let mask = covered_condition_mask(conditions, &range_result.access_conds);
        if mask == 0 {
            continue;
        }
        let count =
            crate::ranger::stats_bridge::pseudo_count_by_column_ranges(&range_result.ranges, rows);
        nodes.push(PseudoRangeSelectivityNode {
            mask,
            selectivity: count / rows,
            preferred_path: source
                .handle_cols
                .iter()
                .any(|handle| handle.unique_id == column.unique_id),
            num_columns: 1,
            id: column.unique_id,
        });
    }

    for index in source
        .indexes
        .iter()
        .filter(|index| index.is_public && !index.is_multi_valued && !index.is_columnar)
    {
        let resolved = index
            .columns
            .iter()
            .map_while(|index_column| {
                source
                    .schema_column_for_index_column(index_column)
                    .cloned()
                    .map(|column| (column, index_column.length))
            })
            .collect::<Vec<_>>();
        if resolved.is_empty() {
            continue;
        }
        let index_columns = resolved
            .iter()
            .map(|(column, _)| column.clone())
            .collect::<Vec<_>>();
        let lengths = resolved
            .iter()
            .map(|(_, length)| *length)
            .collect::<Vec<_>>();
        let detached = crate::ranger::detacher::detach_cond_and_build_range_for_index(
            conditions,
            &index_columns,
            &lengths,
            0,
        )
        .ok()?;
        let mask = covered_condition_mask(conditions, &detached.access_conds);
        if mask == 0 {
            continue;
        }
        let unique_columns =
            (index.unique && resolved.len() == index.columns.len()).then_some(index.columns.len());
        let count = crate::ranger::stats_bridge::pseudo_count_by_index_ranges(
            &detached.ranges,
            rows,
            unique_columns,
        );
        nodes.push(PseudoRangeSelectivityNode {
            mask,
            selectivity: count / rows,
            preferred_path: true,
            num_columns: index.columns.len(),
            id: index.id,
        });
    }

    // Go `GetUsableSetsByGreedy`: primary/index paths outrank plain-column
    // nodes, then coverage, fewer columns, lower selectivity, and stable id.
    nodes.sort_by_key(|node| (node.preferred_path, node.id));
    let mut remaining = (1_u64 << conditions.len()) - 1;
    let mut selectivity = 1.0_f64;
    loop {
        let mut best: Option<PseudoRangeSelectivityNode> = None;
        for node in &nodes {
            if node.mask == 0 || node.mask & remaining != node.mask {
                continue;
            }
            let better = best.is_none_or(|incumbent| {
                node.preferred_path > incumbent.preferred_path
                    || (node.preferred_path == incumbent.preferred_path
                        && (node.mask.count_ones() > incumbent.mask.count_ones()
                            || (node.mask.count_ones() == incumbent.mask.count_ones()
                                && (node.num_columns < incumbent.num_columns
                                    || (node.num_columns == incumbent.num_columns
                                        && node.selectivity < incumbent.selectivity)))))
            });
            if better {
                best = Some(*node);
            }
        }
        let Some(best) = best else {
            break;
        };
        remaining &= !best.mask;
        selectivity *= best.selectivity;
    }
    if remaining != 0 {
        // Go applies the minimum default ONCE to all still-uncovered CNF
        // items, rather than multiplying 0.8 once per item.
        selectivity *= crate::cost_factors::SELECTION_FACTOR;
    }
    Some(selectivity.max(1.0 / rows.max(1.0)))
}

/// Replaces `node`'s OWN schema, when it has one.
///
/// An operator with no schema of its own reads its child's; writing there
/// would be writing into the child, which Go never does from these rules.
fn set_own_schema(node: &mut LogicalPlan, schema: Schema) {
    if node.base().base.schema().is_some() {
        node.base_mut().base.set_schema(Some(schema));
    }
}

// ***************************************************************************
// Predicate pushdown — Go rule #13, `PPDSolver`
// ***************************************************************************

/// What an operator's `descend` left for its `ascend`, and which of Go's three
/// tail shapes that `ascend` takes.
enum PendingPredicates {
    /// Go's `BaseLogicalPlan.PredicatePushDown` tail
    /// (`base_logical_plan.go:128`): each child's leftover becomes a
    /// `LogicalSelection` above it via `logicalop.AddSelection`, and the
    /// carried vector is what this node reports upward.
    AddSelection(Vec<Expression>),
    /// The tail `LogicalProjection`, `LogicalUnionScan` and `LogicalSequence`
    /// take: the child's leftover keeps travelling upward, joined with what
    /// this node could not push.
    PassThrough(Vec<Expression>),
    /// `LogicalSelection.PredicatePushDown`'s own tail
    /// (`logical_selection.go:96`), which either absorbs the leftover into its
    /// own conditions, collapses to a `LogicalTableDual`, or disappears.
    Selection(Vec<Expression>),
}

struct PredicatePushDown<'a, 'ctx> {
    ctx: &'a RuleContext<'ctx>,
    failure: RewriteFailure,
    stash: Vec<PendingPredicates>,
}

impl OwnedRewrite for PredicatePushDown<'_, '_> {
    type Down = Vec<Expression>;
    type Up = Vec<Expression>;

    #[allow(clippy::too_many_lines)]
    fn descend(
        &mut self,
        node: &mut LogicalPlan,
        predicates: Vec<Expression>,
    ) -> Descend<Self::Down, Self::Up> {
        let child_count = node.children().len();
        let schemas = child_schemas(node);
        let all_join_leaf = all_join_leaf_schemas(node);
        let own_schema = effective_schema(node);
        let names = node.base().base.output_names().to_vec();
        let query_block_offset = node.base().base.query_block_offset();
        match node {
            // Go `LogicalSelection.PredicatePushDown` (`logical_selection.go:96`).
            LogicalPlan::Selection(op) => {
                let conditions = std::mem::take(&mut op.conditions);
                op.conditions = apply_predicate_simplification(self.ctx, conditions, false, None);
                let (can_push, cannot_push) =
                    super::LogicalSelection::split_set_get_var_func(&op.conditions);
                self.stash.push(PendingPredicates::Selection(cannot_push));
                let mut down = can_push;
                down.extend(predicates);
                Descend::Children(vec![down])
            }
            // Go `LogicalProjection.PredicatePushDown` (`logical_projection.go:82`).
            LogicalPlan::Projection(op) => {
                let opts = SubstituteOptions::new(self.ctx.builder);
                let (can_push, cannot_push) =
                    op.break_down_predicates(&predicates, &own_schema, &opts);
                self.stash.push(PendingPredicates::PassThrough(cannot_push));
                Descend::Children(vec![can_push])
            }
            // Go `LogicalJoin.PredicatePushDown` (`logical_join.go:171`).
            LogicalPlan::Join(op) => {
                let opts = SubstituteOptions::new(self.ctx.builder);
                let left_schema = schemas.first().unwrap_or(&own_schema);
                let right_schema = schemas.get(1).unwrap_or(&own_schema);
                let split = op.predicate_push_down_local(
                    predicates,
                    left_schema,
                    right_schema,
                    &all_join_leaf,
                    &opts,
                    |conds, propagate_constant, valid| {
                        super::rule::apply_predicate_simplification_for_join(
                            self.ctx,
                            conds,
                            left_schema,
                            right_schema,
                            propagate_constant,
                            valid,
                        )
                    },
                    |join_conditions,
                     filter_conditions,
                     outer_schema,
                     inner_schema,
                     null_sensitive,
                     valid| {
                        super::rule_predicate_simplification::propagate_constant_for_outer_join(
                            self.ctx,
                            join_conditions,
                            filter_conditions,
                            outer_schema,
                            inner_schema,
                            null_sensitive,
                            valid,
                        )
                    },
                );
                if let Some(conds) = &split.dual_conditions {
                    if let Some(dual) =
                        conds_to_table_dual(self.ctx, conds, Some(&own_schema), query_block_offset)
                    {
                        *node = dual;
                        return Descend::Stop(Vec::new());
                    }
                }
                self.stash.push(PendingPredicates::AddSelection(split.ret));
                Descend::Children(vec![split.left_cond, split.right_cond])
            }
            // Go method promotion: `LogicalApply` embeds `LogicalJoin`, so
            // `LogicalJoin.PredicatePushDown` wins over the embedded
            // `BaseLogicalPlan` method even though logical_apply.go does not
            // spell out another override.
            LogicalPlan::Apply(op) => {
                let opts = SubstituteOptions::new(self.ctx.builder);
                let left_schema = schemas.first().unwrap_or(&own_schema);
                let right_schema = schemas.get(1).unwrap_or(&own_schema);
                let split = op.join.predicate_push_down_local(
                    predicates,
                    left_schema,
                    right_schema,
                    &all_join_leaf,
                    &opts,
                    |conds, propagate_constant, valid| {
                        super::rule::apply_predicate_simplification_for_join(
                            self.ctx,
                            conds,
                            left_schema,
                            right_schema,
                            propagate_constant,
                            valid,
                        )
                    },
                    |join_conditions,
                     filter_conditions,
                     outer_schema,
                     inner_schema,
                     null_sensitive,
                     valid| {
                        super::rule_predicate_simplification::propagate_constant_for_outer_join(
                            self.ctx,
                            join_conditions,
                            filter_conditions,
                            outer_schema,
                            inner_schema,
                            null_sensitive,
                            valid,
                        )
                    },
                );
                if let Some(conds) = &split.dual_conditions {
                    if let Some(dual) =
                        conds_to_table_dual(self.ctx, conds, Some(&own_schema), query_block_offset)
                    {
                        *node = dual;
                        return Descend::Stop(Vec::new());
                    }
                }
                self.stash.push(PendingPredicates::AddSelection(split.ret));
                Descend::Children(vec![split.left_cond, split.right_cond])
            }
            // Go `LogicalAggregation.PredicatePushDown`
            // (`logical_aggregation.go:113`).
            LogicalPlan::Aggregation(op) => {
                let (to_push, ret) = op.split_cond_for_aggregation(&predicates);
                self.stash.push(PendingPredicates::AddSelection(ret));
                Descend::Children(vec![to_push])
            }
            // Go `LogicalWindow.PredicatePushDown` (`logical_window.go:112`).
            LogicalPlan::Window(op) => {
                let (can_push, cannot_push) = op.predicate_push_down(&predicates);
                self.stash
                    .push(PendingPredicates::AddSelection(cannot_push));
                Descend::Children(vec![can_push])
            }
            // Go `LogicalUnionScan.PredicatePushDown`
            // (`logical_union_scan.go:70`).
            LogicalPlan::UnionScan(op) => {
                let split = LogicalUnionScan::predicate_push_down(&predicates);
                op.conditions = predicates;
                self.stash
                    .push(PendingPredicates::PassThrough(split.with_virtual_column));
                Descend::Children(vec![split.without_virtual_column])
            }
            // Go `LogicalUnionAll.PredicatePushDown`
            // (`logical_union_all.go:66`): every branch gets the whole set and
            // nothing is retained above.
            LogicalPlan::UnionAll(_) | LogicalPlan::PartitionUnionAll(_) => {
                self.stash.push(PendingPredicates::AddSelection(
                    LogicalUnionAll::predicate_push_down_local(),
                ));
                Descend::Children(vec![predicates; child_count])
            }
            // Go `LogicalLimit.PredicatePushDown` (`logical_limit.go:73`) and
            // `LogicalMaxOneRow.PredicatePushDown`
            // (`logical_max_one_row.go:60`): both forbid every condition.
            LogicalPlan::Limit(_) => {
                self.stash.push(PendingPredicates::AddSelection(
                    LogicalLimit::predicate_push_down(predicates),
                ));
                Descend::Children(vec![Vec::new()])
            }
            LogicalPlan::MaxOneRow(_) => {
                self.stash.push(PendingPredicates::AddSelection(
                    LogicalMaxOneRow::predicate_push_down(predicates),
                ));
                Descend::Children(vec![Vec::new()])
            }
            // Go `LogicalExpand.PredicatePushDown` (`logical_expand.go:75`).
            LogicalPlan::Expand(op) => {
                let retained = op.predicate_push_down(predicates);
                self.stash.push(PendingPredicates::AddSelection(retained));
                Descend::Children(vec![Vec::new()])
            }
            // Go `LogicalSequence.PredicatePushDown` (`logical_sequence.go:60`):
            // only the LAST child, which is the main query, sees them.
            LogicalPlan::Sequence(_) => {
                self.stash.push(PendingPredicates::PassThrough(Vec::new()));
                let mut downs = vec![Vec::new(); child_count];
                if let Some(index) = super::LogicalSequence::predicate_push_down_child(child_count)
                {
                    if let Some(slot) = downs.get_mut(index) {
                        *slot = predicates;
                    }
                }
                Descend::Children(downs)
            }
            // Go `DataSource.PredicatePushDown` (`logical_datasource.go:135`).
            //
            // Go splits `predicates` with
            // `expression.PushDownExprs(pushDownCtx, predicates, kv.UnSpecified)`.
            // The TiKV expression whitelist is the dependency-closed half of
            // that decision; session blacklist entries are applied later by
            // the live executor when it negotiates the scan. Keep unsupported
            // expressions above the source and let supported expressions grow
            // the DataSource ranges and statistics used by physical planning.
            LogicalPlan::DataSource(op) => {
                let (pushable, not_pushable): (Vec<_>, Vec<_>) =
                    predicates.into_iter().partition(|predicate| {
                        crate::pushdown::can_exprs_push_down_tikv(std::slice::from_ref(predicate))
                    });
                Descend::Stop(op.predicate_push_down_local(pushable, not_pushable))
            }
            // Go `LogicalTableDual.PredicatePushDown`
            // (`logical_table_dual.go:73`).
            LogicalPlan::TableDual(_) => {
                Descend::Stop(LogicalTableDual::predicate_push_down(predicates))
            }
            // Go `LogicalMemTable.PredicatePushDown` (`logical_mem_table.go:62`).
            LogicalPlan::MemTable(op) => {
                let (remained, _has_extractor) = op.predicate_push_down(predicates);
                Descend::Stop(remained)
            }
            // Go `LogicalShow.PredicatePushDown` (`logical_show.go:118`).
            LogicalPlan::Show(op) => {
                Descend::Stop(op.predicate_push_down(&own_schema, &names, predicates))
            }
            // Go `LogicalCTE.PredicatePushDown` (`logical_cte.go:96`). The
            // predicates still remain above this reference, while a resolved
            // CNF copy is accumulated on the shared class so its seed can be
            // optimized once against the DNF of every consumer.
            LogicalPlan::CTE(op) => {
                use super::cte::CtePredicatePushDown;

                let decision = op.predicate_push_down(&predicates);
                if let Some(class) = &op.cte {
                    let mut class = class.borrow_mut();
                    let recorded = match decision {
                        CtePredicatePushDown::Unsupported => None,
                        CtePredicatePushDown::RecordAlwaysTrue => {
                            Some(Expression::Constant(Constant::new_one()))
                        }
                        CtePredicatePushDown::Record(predicates) => compose_cnf_condition(
                            predicates
                                .into_iter()
                                .map(|predicate| {
                                    super::rule_util::resolve_expr_and_replace(
                                        &predicate,
                                        &class.column_map,
                                    )
                                })
                                .collect(),
                        ),
                    };
                    if let Some(recorded) = recorded {
                        class.push_down_predicates.push(recorded);
                    }
                }
                Descend::Stop(predicates)
            }
            // Go's base body: everything goes to `children[0]`, nothing comes
            // back up.
            base_arms![
                Sort,
                TopN,
                Lock,
                CTETable,
                TiKVSingleGather,
                TableScan,
                IndexScan,
                ShowDDLJobs,
            ] => {
                self.stash.push(PendingPredicates::AddSelection(Vec::new()));
                if child_count == 0 {
                    self.stash.pop();
                    return Descend::Stop(predicates);
                }
                let mut downs = vec![Vec::new(); child_count];
                downs[0] = predicates;
                Descend::Children(downs)
            }
        }
    }

    fn ascend(
        &mut self,
        mut node: LogicalPlan,
        child_ups: Vec<Self::Up>,
    ) -> (LogicalPlan, Self::Up) {
        let Some(pending) = self.stash.pop() else {
            return (node, Vec::new());
        };
        let query_block_offset = node.base().base.query_block_offset();
        match pending {
            PendingPredicates::AddSelection(ret) => {
                let children = node.base_mut().take_children();
                let rebuilt = children
                    .into_iter()
                    .zip(child_ups)
                    .map(|(child, leftover)| {
                        add_selection(self.ctx, child, leftover, query_block_offset)
                    })
                    .collect();
                node.set_children(rebuilt);
                match update_join_equal_conditions_in_plan(self.ctx, &mut node) {
                    Ok(true) => {
                        // Go calls `BuildKeyInfoPortal(p)` immediately after
                        // `updateEQCond`; the inserted projections changed the
                        // schemas and key identities this join consumes.
                        node = build_key_info_portal(node);
                    }
                    Ok(false) => {}
                    Err(error) => self.failure.record(error),
                }
                (node, ret)
            }
            PendingPredicates::PassThrough(mut extra) => {
                let mut up: Vec<Expression> = child_ups.into_iter().flatten().collect();
                up.append(&mut extra);
                (node, up)
            }
            PendingPredicates::Selection(cannot_push) => {
                let mut ret: Vec<Expression> = child_ups.into_iter().flatten().collect();
                ret.extend(cannot_push);
                let own_schema = effective_schema(&node);
                if ret.is_empty() {
                    // Go: `p.Conditions = p.Conditions[:0]; return nil, child`.
                    let mut children = node.base_mut().take_children();
                    if children.is_empty() {
                        return (node, Vec::new());
                    }
                    let child = children.remove(0);
                    node.dismantle();
                    return (child, Vec::new());
                }
                let simplified = apply_predicate_simplification(self.ctx, ret, true, None);
                if let Some(dual) = conds_to_table_dual(
                    self.ctx,
                    &simplified,
                    Some(&own_schema),
                    query_block_offset,
                ) {
                    node.dismantle();
                    return (dual, Vec::new());
                }
                if let LogicalPlan::Selection(op) = &mut node {
                    op.conditions = simplified;
                }
                (node, Vec::new())
            }
        }
    }
}

/// Go `base.LogicalPlan.PredicatePushDown(predicates)` over a whole tree, Go
/// rule #13's body.
///
/// Returns the rewritten plan, the predicates that could not be pushed at all,
/// and the first failure any operator recorded — see [`super::fold`] for why
/// the failure travels beside the plan rather than instead of it.
#[must_use]
pub fn predicate_push_down(
    ctx: &RuleContext<'_>,
    plan: LogicalPlan,
    predicates: Vec<Expression>,
) -> (LogicalPlan, Vec<Expression>, Option<PlanError>) {
    let mut rewrite = PredicatePushDown {
        ctx,
        failure: RewriteFailure::default(),
        stash: Vec::new(),
    };
    let (plan, remaining) = fold_owned(&mut rewrite, plan, predicates);
    (plan, remaining, rewrite.failure.take())
}

// ***************************************************************************
// Column pruning — Go rules #1 and #29, `rule.ColumnPruner`
// ***************************************************************************

/// What an operator's `descend` left for its `ascend`.
enum PendingColumns {
    /// Nothing to do on the way up.
    Nothing,
    /// `LogicalJoin` / `LogicalApply`: re-merge the two child schemas and
    /// inline-project down to the parent's set.
    MergeSchema(Vec<Column>),
    /// `LogicalLimit`: rebuild this operator's schema from the pruned child's.
    RebuildFromChild(Vec<Column>),
    /// `LogicalProjection`: replace this node by its child when every
    /// projected expression was pruned away.
    ProjectionEmptied,
    /// `LogicalWindow` / `LogicalTopN`: rebuild the schema from the pruned
    /// child plus the columns snapshotted on the way down — the parent's set
    /// for a TopN, this window's own result columns for a window.
    RebuildWithOwnColumns(Vec<Column>),
    /// `LogicalUnionAll`: add Go's identity projection above a child that
    /// retained condition-only columns after the union schema was pruned.
    RepairUnionChildren(bool),
}

struct PruneColumns<'a, 'ctx> {
    ctx: &'a RuleContext<'ctx>,
    failure: RewriteFailure,
    stash: Vec<PendingColumns>,
}

impl OwnedRewrite for PruneColumns<'_, '_> {
    type Down = Vec<Column>;
    type Up = ();

    #[allow(clippy::too_many_lines)]
    fn descend(
        &mut self,
        node: &mut LogicalPlan,
        parent_used_cols: Vec<Column>,
    ) -> Descend<Self::Down, Self::Up> {
        let child_count = node.children().len();
        let schemas = child_schemas(node);
        let mut own_schema = effective_schema(node);
        let empty = Schema::default();
        match node {
            // Go `LogicalSelection.PruneColumns` (`logical_selection.go:127`).
            LogicalPlan::Selection(op) => {
                self.stash.push(PendingColumns::Nothing);
                Descend::Children(vec![op.child_used_cols(&parent_used_cols)])
            }
            // Go `LogicalProjection.PruneColumns` (`logical_projection.go:105`).
            LogicalPlan::Projection(op) => {
                let (child_used, emptied) =
                    op.prune_columns_local(&parent_used_cols, &mut own_schema);
                set_own_schema(node, own_schema);
                self.stash.push(if emptied {
                    PendingColumns::ProjectionEmptied
                } else {
                    PendingColumns::Nothing
                });
                Descend::Children(vec![child_used])
            }
            // Go `LogicalJoin.PruneColumns` (`logical_join.go:339`).
            LogicalPlan::Join(op) => {
                let (left, right) = op.extract_used_cols(
                    &parent_used_cols,
                    schemas.first().unwrap_or(&empty),
                    schemas.get(1).unwrap_or(&empty),
                );
                self.stash
                    .push(PendingColumns::MergeSchema(parent_used_cols));
                Descend::Children(vec![left, right])
            }
            // Go `LogicalApply.PruneColumns` (`logical_apply.go:118`).
            LogicalPlan::Apply(op) => {
                let (mut left, right) = op.prune_columns_local(
                    &parent_used_cols,
                    schemas.first().unwrap_or(&empty),
                    schemas.get(1).unwrap_or(&empty),
                );
                let _outer_count = op.widen_outer_used_cols(&mut left);
                self.stash
                    .push(PendingColumns::MergeSchema(parent_used_cols));
                Descend::Children(vec![left, right])
            }
            // Go `LogicalAggregation.PruneColumns` (`logical_aggregation.go:113`).
            LogicalPlan::Aggregation(op) => {
                let child_used = op.prune_columns_local(&parent_used_cols, &mut own_schema);
                set_own_schema(node, own_schema);
                self.stash.push(PendingColumns::Nothing);
                Descend::Children(vec![child_used])
            }
            // Go `LogicalSort.PruneColumns` (`logical_sort.go:66`).
            LogicalPlan::Sort(op) => {
                self.stash.push(PendingColumns::Nothing);
                Descend::Children(vec![op.prune_columns_local(&parent_used_cols)])
            }
            // Go `LogicalTopN.PruneColumns` (`logical_top_n.go:79`).
            LogicalPlan::TopN(op) => {
                let child_used = op.prune_columns_local(&parent_used_cols);
                self.stash
                    .push(PendingColumns::RebuildWithOwnColumns(parent_used_cols));
                Descend::Children(vec![child_used])
            }
            // Go `LogicalLimit.PruneColumns` (`logical_limit.go:85`), whose
            // schema is rebuilt from the pruned child on the way up.
            LogicalPlan::Limit(_) => {
                self.stash
                    .push(PendingColumns::RebuildFromChild(parent_used_cols.clone()));
                Descend::Children(vec![parent_used_cols])
            }
            // Go `LogicalWindow.PruneColumns` (`logical_window.go:352`).
            LogicalPlan::Window(op) => {
                let window_columns = op.get_window_result_columns(&own_schema).to_vec();
                let child_used = op.prune_columns_local(&parent_used_cols, &own_schema);
                self.stash
                    .push(PendingColumns::RebuildWithOwnColumns(window_columns));
                Descend::Children(vec![child_used])
            }
            // Go `LogicalExpand.PruneColumns` (`logical_expand.go:95`).
            LogicalPlan::Expand(op) => {
                let widened = op.prune_columns_local(&parent_used_cols);
                LogicalExpand::prune_schema(&mut own_schema, &widened);
                set_own_schema(node, own_schema);
                self.stash.push(PendingColumns::Nothing);
                Descend::Children(vec![widened])
            }
            // Go `LogicalUnionScan.PruneColumns` (`logical_union_scan.go:88`).
            LogicalPlan::UnionScan(op) => {
                let child_used = op.prune_columns_local(&parent_used_cols, &own_schema);
                self.stash
                    .push(PendingColumns::RebuildFromChild(parent_used_cols));
                Descend::Children(vec![child_used])
            }
            // Go `LogicalLock.PruneColumns` (`logical_lock.go:76`).
            LogicalPlan::Lock(op) => {
                self.stash.push(PendingColumns::Nothing);
                Descend::Children(vec![op.prune_columns_local(&parent_used_cols)])
            }
            // Go `LogicalUnionAll.PruneColumns` (`logical_union_all.go:59`).
            LogicalPlan::UnionAll(_) | LogicalPlan::PartitionUnionAll(_) => {
                let pruning =
                    LogicalUnionAll::prune_columns_local(&parent_used_cols, &mut own_schema);
                set_own_schema(node, own_schema);
                self.stash
                    .push(PendingColumns::RepairUnionChildren(pruning.has_been_used));
                Descend::Children(vec![pruning.child_used_cols; child_count])
            }
            // Go `LogicalMemTable.PruneColumns` (`logical_mem_table.go:80`).
            LogicalPlan::MemTable(op) => {
                op.prune_columns(&mut own_schema, &parent_used_cols);
                set_own_schema(node, own_schema);
                Descend::Stop(())
            }
            // Go `LogicalTableDual.PruneColumns` (`logical_table_dual.go:80`).
            LogicalPlan::TableDual(_) => {
                LogicalTableDual::prune_columns(&mut own_schema, &parent_used_cols);
                set_own_schema(node, own_schema);
                Descend::Stop(())
            }
            // Go `DataSource.PruneColumns` (`logical_datasource.go:200`).
            //
            // NARROWING: when pruning empties the schema Go forces one handle
            // column back in, which needs the catalogue's handle definition;
            // the local half reports the emptied case and the repair is the
            // catalogue-owning batch's.
            LogicalPlan::DataSource(op) => {
                op.prune_columns_local(&parent_used_cols, &mut own_schema);
                set_own_schema(node, own_schema);
                Descend::Stop(())
            }
            // Go `LogicalCTE.PruneColumns` (`logical_cte.go:132`), whose whole
            // body is `return p, nil` — the seed is optimized as its own plan.
            LogicalPlan::CTE(_) => Descend::Stop(()),
            // Go `LogicalSequence.PruneColumns` (`logical_sequence.go:70`):
            // only the last child is pruned.
            LogicalPlan::Sequence(_) => {
                self.stash.push(PendingColumns::Nothing);
                let mut downs: Vec<Vec<Column>> = schemas
                    .iter()
                    .map(|schema| schema.columns.clone())
                    .collect();
                if let Some(index) = super::LogicalSequence::prune_columns_child(child_count) {
                    if let Some(slot) = downs.get_mut(index) {
                        *slot = parent_used_cols;
                    }
                }
                Descend::Children(downs)
            }
            // Go's base body (`base_logical_plan.go:171`): forward the set to
            // `children[0]` unchanged.
            base_arms![
                MaxOneRow,
                CTETable,
                TiKVSingleGather,
                TableScan,
                IndexScan,
                Show,
                ShowDDLJobs,
            ] => {
                if child_count == 0 {
                    return Descend::Stop(());
                }
                self.stash.push(PendingColumns::Nothing);
                let mut downs = vec![Vec::new(); child_count];
                downs[0] = parent_used_cols;
                Descend::Children(downs)
            }
        }
    }

    fn ascend(
        &mut self,
        mut node: LogicalPlan,
        _child_ups: Vec<Self::Up>,
    ) -> (LogicalPlan, Self::Up) {
        let Some(pending) = self.stash.pop() else {
            return (node, ());
        };
        let schemas = child_schemas(&node);
        match pending {
            PendingColumns::Nothing => (node, ()),
            PendingColumns::ProjectionEmptied => {
                // Go: "If its columns are all pruned, we directly use its
                // child." (`logical_projection.go:139`)
                let mut children = node.base_mut().take_children();
                if children.is_empty() {
                    return (node, ());
                }
                let child = children.remove(0);
                node.dismantle();
                (child, ())
            }
            PendingColumns::MergeSchema(parent_used_cols) => {
                // Go `p.MergeSchema()` then `p.InlineProjection(parentUsedCols)`.
                let mut merged = Vec::new();
                for schema in &schemas {
                    merged.extend(schema.columns.iter().cloned());
                }
                let mut schema = Schema::new(merged);
                schema_producer::inline_projection(&mut schema, &parent_used_cols);
                set_own_schema(&mut node, schema);
                (node, ())
            }
            PendingColumns::RebuildFromChild(parent_used_cols) => {
                let child_schema = schemas.first().cloned().unwrap_or_default();
                if let LogicalPlan::Limit(op) = &mut node {
                    op.prune_columns_local(&parent_used_cols, &child_schema);
                } else {
                    let mut schema = child_schema;
                    schema_producer::inline_projection(&mut schema, &parent_used_cols);
                    set_own_schema(&mut node, schema);
                }
                (node, ())
            }
            PendingColumns::RebuildWithOwnColumns(snapshot) => {
                let child_schema = schemas.first().cloned().unwrap_or_default();
                let rebuilt = match &mut node {
                    LogicalPlan::TopN(op) => {
                        Some(op.rebuild_schema_after_pruning(&snapshot, &child_schema))
                    }
                    LogicalPlan::Window(op) => {
                        Some(op.rebuild_schema_after_pruning(&child_schema, &snapshot))
                    }
                    // Only those two operators ever stash this shape.
                    other => {
                        debug_assert!(false, "unexpected rebuild for {}", other.base().base.tp());
                        None
                    }
                };
                if let Some(schema) = rebuilt {
                    set_own_schema(&mut node, schema);
                }
                (node, ())
            }
            PendingColumns::RepairUnionChildren(has_been_used) => {
                if !has_been_used {
                    return (node, ());
                }
                let Some(schema) = node.schema().cloned() else {
                    return (node, ());
                };
                let query_block_offset = node.base().base.query_block_offset();
                let children = node.base_mut().take_children();
                let children = children
                    .into_iter()
                    .map(|child| {
                        if !LogicalUnionAll::child_needs_pruning_projection(
                            schema.columns.len(),
                            child
                                .schema()
                                .map_or(0, |child_schema| child_schema.columns.len()),
                        ) {
                            return child;
                        }
                        let expressions = schema
                            .columns
                            .iter()
                            .cloned()
                            .map(Expression::Column)
                            .collect();
                        let mut projection = LogicalProjection::new(
                            BaseLogicalPlan::new(
                                self.ctx.allocator,
                                LogicalProjection::TYPE,
                                query_block_offset,
                            ),
                            expressions,
                        );
                        projection.base.base.set_schema(Some(schema.clone()));
                        projection.base.set_children(vec![child]);
                        LogicalPlan::Projection(projection)
                    })
                    .collect();
                node.set_children(children);
                (node, ())
            }
        }
    }
}

/// Go `base.LogicalPlan.PruneColumns(parentUsedCols)` over a whole tree, Go
/// rules #1 and #29's body.
#[must_use]
pub fn prune_columns(
    ctx: &RuleContext<'_>,
    plan: LogicalPlan,
    parent_used_cols: Vec<Column>,
) -> (LogicalPlan, Option<PlanError>) {
    let mut rewrite = PruneColumns {
        ctx,
        failure: RewriteFailure::default(),
        stash: Vec::new(),
    };
    let (plan, ()) = fold_owned(&mut rewrite, plan, parent_used_cols);
    (plan, rewrite.failure.take())
}

// ***************************************************************************
// TopN pushdown — Go rule #21, `PushDownTopNOptimizer`
// ***************************************************************************

/// What an operator's `descend` left for its `ascend`.
enum PendingTopN {
    /// Nothing to re-attach; the TopN travelled into the child, or there was
    /// none.
    Nothing,
    /// Re-attach this TopN ABOVE the node on the way up — Go's
    /// `topN.AttachChild(p)`.
    Reattach(Box<LogicalTopN>),
    /// Go `topnEliminated` WITH order (`logical_join.go:456-463`): an outer
    /// join absorbed a by-item TopN whose inner side is unique, so only the
    /// ORDER survives above the join as a plain `LogicalSort`.
    ReattachAsSort(Vec<ByItems>),
    /// Go bodies that RETURN THE CHILD (`LogicalLimit.PushDownTopN`,
    /// `LogicalSort.PushDownTopN`): this operator disappears; an optional
    /// TopN re-attaches above the surviving child.
    ReplaceWithChild(Option<Box<LogicalTopN>>),
}

struct PushDownTopN<'a> {
    stash: Vec<PendingTopN>,
    allocator: &'a crate::plan_base::PlanIdAllocator,
    /// Go `SCtx().GetExprCtx()`: the builder the projection arm substitutes
    /// and constant-folds by-items through.
    builder: &'a dyn tidb_expr::expr_util::builder::FunctionBuilder,
}

impl OwnedRewrite for PushDownTopN<'_> {
    type Down = Option<Box<LogicalTopN>>;
    type Up = ();

    fn descend(
        &mut self,
        node: &mut LogicalPlan,
        topn: Self::Down,
    ) -> Descend<Self::Down, Self::Up> {
        let child_count = node.children().len();
        match node {
            // Go `LogicalSort.PushDownTopN` (`logical_sort.go:88`): a Sort with
            // a TopN above it is ABSORBED by that TopN, which takes the Sort's
            // order when it has none of its own.
            LogicalPlan::Sort(op) => {
                // "if topN == nil" the base body keeps the sort; otherwise the
                // SORT ITSELF disappears — its order merges into an incoming
                // limit-shaped TopN, and a real TopN simply swallows it
                // ("If a TopN is pushed down, this sort is useless.").
                match topn {
                    None => {
                        self.stash.push(PendingTopN::Nothing);
                        Descend::Children(vec![None])
                    }
                    Some(mut incoming) => {
                        if incoming.is_limit() {
                            incoming.by_items = op.by_items.clone();
                        }
                        // Go returns `ls.Children()[0].PushDownTopN(topN)`:
                        // the Sort disappears and the TopN keeps travelling
                        // through the child instead of being reattached to an
                        // untouched subtree.
                        self.stash.push(PendingTopN::ReplaceWithChild(None));
                        Descend::Children(vec![Some(incoming)])
                    }
                }
            }
            // Go `LogicalLimit.PushDownTopN` (`logical_limit.go:106`): the
            // limit CONVERTS to a TopN and travels INTO the child; only an
            // incoming TopN from above re-attaches here. The limit operator
            // itself never survives.
            LogicalPlan::Limit(op) => {
                let converted = op.convert_to_topn(self.allocator);
                // Go returns the CONVERTED-and-pushed CHILD in every case —
                // the limit operator itself never survives.
                self.stash.push(PendingTopN::ReplaceWithChild(topn));
                Descend::Children(vec![Some(Box::new(converted)); child_count.max(1)])
            }
            // Go `LogicalUnionAll.PushDownTopN` (`logical_union_all.go:159`): a
            // COPY that keeps `count + offset` rows enters each branch, and the
            // original stays above.
            LogicalPlan::UnionAll(_) | LogicalPlan::PartitionUnionAll(_) => match topn {
                Some(topn) => {
                    let per_child =
                        LogicalUnionAll::push_down_topn_for_child(&topn, self.allocator);
                    self.stash.push(PendingTopN::Reattach(topn));
                    Descend::Children(vec![Some(Box::new(per_child)); child_count])
                }
                None => {
                    self.stash.push(PendingTopN::Nothing);
                    Descend::Children(vec![None; child_count])
                }
            },
            // Go `LogicalLock.PushDownTopN` (`logical_lock.go:96`): a lock TiDB
            // does not implement lets the TopN through.
            LogicalPlan::Lock(_) => {
                let pushes = super::LogicalLock::pushes_topn_into_child(topn.is_some());
                if pushes {
                    self.stash.push(PendingTopN::Nothing);
                    Descend::Children(vec![topn])
                } else {
                    self.stash
                        .push(topn.map_or(PendingTopN::Nothing, PendingTopN::Reattach));
                    Descend::Children(vec![None])
                }
            }
            // Go `LogicalMemTable.PushDownTopN` (`logical_mem_table.go:114`):
            // the TopN is ALWAYS re-attached; only hints travel inward.
            //
            // NARROWING: the hints the local half computes are handed to the
            // mem-table's predicate extractor in Go
            // (`LogicalMemTable.Extractor.SetLimit` / `SetDesc`), and this
            // crate's `LogicalMemTable` does not carry a live extractor. The
            // hint is computed and DISCARDED, which costs a coprocessor-side
            // row limit and never changes the result.
            LogicalPlan::MemTable(op) => {
                if let Some(topn) = &topn {
                    let _hints = op.push_down_topn(topn);
                }
                self.stash
                    .push(topn.map_or(PendingTopN::Nothing, PendingTopN::Reattach));
                // Ascend must run to re-attach above this leaf.
                Descend::Children(Vec::new())
            }
            // Go `LogicalCTE.PushDownTopN` (`logical_cte.go:139`): a TopN never
            // enters a CTE.
            LogicalPlan::CTE(_) => {
                self.stash
                    .push(topn.map_or(PendingTopN::Nothing, PendingTopN::Reattach));
                // Ascend must run to re-attach above this node.
                Descend::Children(Vec::new())
            }
            // Go's base body, `pushDownTopNForBaseLogicalPlan`
            // (`logical_plans_misc.go:110`): every child is rewritten with NO
            // TopN, and the incoming one is re-attached above this node.
            //
            // Go `LogicalProjection.PushDownTopN` (`logical_projection.go:183`):
            // by-items substitute through the projection's expressions and the
            // TopN continues BELOW — the projection stays where it is. Set-var
            // projections and non-immutable orders take the base body.
            LogicalPlan::Projection(op) => {
                let Some(incoming) = topn else {
                    self.stash.push(PendingTopN::Nothing);
                    return Descend::Children(vec![None; child_count.max(1)]);
                };
                if op
                    .exprs
                    .iter()
                    .any(tidb_expr::evaluator::has_get_set_var_func)
                {
                    self.stash.push(PendingTopN::Reattach(incoming));
                    return Descend::Children(vec![None; child_count]);
                }
                let own_schema = op.base.base.schema().cloned().unwrap_or_default();
                let opts = SubstituteOptions::new(self.builder);
                let mut substituted_items = Vec::with_capacity(incoming.by_items.len());
                for item in &incoming.by_items {
                    let substituted = tidb_expr::expr_util::fold::fold_constant(
                        &tidb_expr::expr_util::substitute::column_substitute(
                            &item.expr,
                            &own_schema,
                            &op.exprs,
                            &opts,
                        ),
                        &tidb_expr::NoColumns,
                        &opts,
                    );
                    // "if the order-by expression is un-deterministic like
                    // 'order by rand()', stop pushing down."
                    if !tidb_expr::expr_util::predicates::is_immutable_func(&substituted) {
                        self.stash.push(PendingTopN::Reattach(incoming));
                        return Descend::Children(vec![None; child_count]);
                    }
                    substituted_items.push(substituted);
                }
                // A column with ID 0 that only THIS projection produces cannot
                // enter the child; keep the TopN above.
                let child_schema = op
                    .base
                    .children()
                    .first()
                    .and_then(super::LogicalPlan::schema);
                let blocked_by_projection = substituted_items.iter().any(|expr| {
                    extract_columns(expr).iter().any(|col| {
                        col.id == 0
                            && own_schema.contains(col)
                            && !child_schema.is_some_and(|schema| schema.contains(col))
                    })
                });
                if blocked_by_projection {
                    self.stash.push(PendingTopN::Reattach(incoming));
                    return Descend::Children(vec![None; child_count]);
                }
                // Drop meaningless constant sort items.
                let mut pushed = *incoming;
                pushed.by_items = pushed
                    .by_items
                    .into_iter()
                    .zip(substituted_items)
                    .filter_map(|(mut item, substituted)| {
                        if matches!(substituted, Expression::Constant(_)) {
                            return None;
                        }
                        item.expr = substituted;
                        Some(item)
                    })
                    .collect();
                self.stash.push(PendingTopN::Nothing);
                Descend::Children(vec![Some(Box::new(pushed)); child_count])
            }
            // Go `LogicalJoin.PushDownTopN` (`logical_join.go:428`): a TopN
            // over an OUTER join descends into the PRESERVED side — the other
            // side and every non-outer join type take the base body. When the
            // inner side is unique on the join key, each preserved-side row
            // keeps its own output row, so the offset travels down too and the
            // join ELIMINATES the TopN (a by-item-only sort stays above).
            LogicalPlan::Join(op) => {
                let outer_idx = match op.join_type {
                    LogicalJoinType::LeftOuter
                    | LogicalJoinType::LeftOuterSemi
                    | LogicalJoinType::AntiLeftOuterSemi => Some(0usize),
                    LogicalJoinType::RightOuter => Some(1usize),
                    LogicalJoinType::Inner | LogicalJoinType::Semi | LogicalJoinType::AntiSemi => {
                        None
                    }
                };
                let Some(outer_idx) = outer_idx else {
                    self.stash.push(match topn {
                        Some(inner_topn) => PendingTopN::Reattach(inner_topn),
                        None => PendingTopN::Nothing,
                    });
                    return Descend::Children(vec![None; child_count]);
                };
                let Some(topn) = topn else {
                    self.stash.push(PendingTopN::Nothing);
                    return Descend::Children(vec![None; child_count]);
                };
                let topn = *topn;
                let children = op.base.children();
                let child_schema = children
                    .get(outer_idx)
                    .and_then(super::LogicalPlan::schema)
                    .map(|schema| schema.to_owned());
                // "for _, by := range topN.ByItems": every by-item column must
                // survive in the preserved child's schema, else the TopN
                // cannot enter that child and just re-attaches above.
                let by_item_cols_fit_child = child_schema.as_ref().is_some_and(|schema| {
                    topn.by_items.iter().all(|item| {
                        extract_columns(&item.expr)
                            .iter()
                            .all(|col| schema.contains(col))
                    })
                });
                if !by_item_cols_fit_child {
                    self.stash.push(PendingTopN::Reattach(Box::new(topn)));
                    if child_count == 0 {
                        return Descend::Stop(());
                    }
                    return Descend::Children(vec![None; child_count]);
                }
                let inner_idx = 1 - outer_idx;
                let (left_keys, right_keys, _is_null_eq, has_null_eq) = op.get_join_keys();
                let inner_keys = if outer_idx == 0 {
                    right_keys
                } else {
                    left_keys
                };
                let inner_unique = children
                    .get(inner_idx)
                    .and_then(super::LogicalPlan::schema)
                    .is_some_and(|schema| {
                        schema.is_unique(true, &inner_keys)
                            || (!has_null_eq && schema.is_unique(false, &inner_keys))
                    });
                let (count, offset) = if inner_unique {
                    (topn.count, topn.offset)
                } else {
                    (topn.count + topn.offset, 0)
                };
                let mut pushed = topn.clone();
                pushed.count = count;
                pushed.offset = offset;
                self.stash
                    .push(if inner_unique && topn.by_items.is_empty() {
                        PendingTopN::Nothing
                    } else if inner_unique {
                        PendingTopN::ReattachAsSort(topn.by_items.clone())
                    } else {
                        PendingTopN::Reattach(Box::new(topn))
                    });
                if child_count == 0 {
                    return Descend::Stop(());
                }
                let mut downs: Vec<Option<Box<LogicalTopN>>> = vec![None; child_count];
                if let Some(slot) = downs.get_mut(outer_idx) {
                    *slot = Some(Box::new(pushed));
                }
                Descend::Children(downs)
            }
            base_arms![
                Selection,
                Apply,
                TopN,
                Aggregation,
                Window,
                Expand,
                UnionScan,
                MaxOneRow,
                Sequence,
                CTETable,
                TiKVSingleGather,
                TableScan,
                IndexScan,
                DataSource,
                TableDual,
                Show,
                ShowDDLJobs,
            ] => {
                self.stash
                    .push(topn.map_or(PendingTopN::Nothing, PendingTopN::Reattach));
                // An EMPTY Children vector still parks the node, so ascend
                // runs and re-attaches the TopN above a leaf.
                Descend::Children(vec![None; child_count])
            }
        }
    }

    fn ascend(
        &mut self,
        mut node: LogicalPlan,
        _child_ups: Vec<Self::Up>,
    ) -> (LogicalPlan, Self::Up) {
        match self.stash.pop() {
            Some(PendingTopN::Reattach(topn)) => (topn.attach_child(node, self.allocator), ()),
            Some(PendingTopN::ReattachAsSort(by_items)) => {
                // "Add a sort if the topN has order by items."
                let mut sort = LogicalSort::new(
                    BaseLogicalPlan::new(
                        self.allocator,
                        LogicalSort::TYPE,
                        node.query_block_offset(),
                    ),
                    by_items,
                );
                sort.base.set_children(vec![node]);
                (LogicalPlan::Sort(sort), ())
            }
            // Go bodies that return the child: swap this operator out for its
            // single child, optionally with a TopN re-attached above it.
            Some(PendingTopN::ReplaceWithChild(topn)) => {
                let mut children = node.base_mut().take_children();
                let child = children.pop().unwrap_or(node);
                match topn {
                    Some(topn) => (topn.attach_child(child, self.allocator), ()),
                    None => (child, ()),
                }
            }
            Some(PendingTopN::Nothing) | None => (node, ()),
        }
    }
}

/// Test convenience: the rule body with the preserving test builder.
#[cfg(test)]
#[must_use]
pub(crate) fn push_down_topn(plan: LogicalPlan, topn: Option<LogicalTopN>) -> LogicalPlan {
    let allocator = crate::plan_base::PlanIdAllocator::new();
    push_down_topn_with_builder(&super::rule_tests::TEST_BUILDER, &allocator, plan, topn)
}

/// Go `base.LogicalPlan.PushDownTopN(topN)` over a whole tree, Go rule #21's
/// body.
#[must_use]
pub fn push_down_topn_with_builder(
    builder: &dyn tidb_expr::expr_util::builder::FunctionBuilder,
    allocator: &crate::plan_base::PlanIdAllocator,
    plan: LogicalPlan,
    topn: Option<LogicalTopN>,
) -> LogicalPlan {
    let mut rewrite = PushDownTopN {
        stash: Vec::new(),
        allocator,
        builder,
    };
    let (plan, ()) = fold_owned(&mut rewrite, plan, topn.map(Box::new));
    plan
}

// ***************************************************************************
// Key info — Go rule #3, `rule.BuildKeySolver`
// ***************************************************************************

struct BuildKeyInfo;

impl OwnedRewrite for BuildKeyInfo {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): ()) -> Descend<(), ()> {
        Descend::Children(vec![(); node.children().len()])
    }

    fn ascend(&mut self, mut node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
        let child_schemas = child_schemas(&node);
        let mut self_schema = effective_schema(&node);
        node.build_key_info(&mut self_schema, &child_schemas);
        set_own_schema(&mut node, self_schema);
        (node, ())
    }
}

/// Go `ruleutil.BuildKeyInfoPortal(lp)` (`rule/util/misc.go:222`): the
/// post-order recursion that Go rule #3's whole body is.
///
/// Go's `childSchemaSlicePool` is a `sync.Pool` reuse of the per-node child
/// schema slice; it is an allocation optimization with no semantic content and
/// is not modelled.
#[must_use]
pub fn build_key_info_portal(plan: LogicalPlan) -> LogicalPlan {
    let (plan, ()) = fold_owned(&mut BuildKeyInfo, plan, ());
    plan
}

/// Go `expression.SplitCNFItems` over a predicate list, flattened.
///
/// Used by the aggregation split; kept here rather than on the operator
/// because it is the driver's flattening, not the operator's decision.
#[must_use]
pub fn split_cnf(predicates: &[Expression]) -> Vec<Expression> {
    predicates.iter().flat_map(split_cnf_items).collect()
}

// ---------------------------------------------------------------------------
// Go `RecursiveDeriveStats`: the post-order statistics derivation driver.
// ---------------------------------------------------------------------------

/// Go `BaseLogicalPlan.RecursiveDeriveStats(colGroups)`
/// (`base_logical_plan.go`): derive every child bottom-up, then hand this
/// node's `DeriveStats` the children's profiles, schemas, and reload flags.
///
/// The recursion is [`fold_owned`], so a 60k-deep chain derives without
/// touching the host stack. `Down` is Go's `cumColGroups` — each node passes
/// `ExtractColGroups`' answer to every child — and `Up` is Go's
/// `(*StatsInfo, bool)` return, with the error travelling in the
/// [`RewriteFailure`] slot as the module header prescribes.
///
/// # Fidelity boundaries, each named at its arm
///
/// * Operators whose Go `DeriveStats` override is not yet ported REFUSE with
///   an error naming the Go symbol, rather than falling back to the base
///   body. The base body would silently adopt the child's row count — for
///   the two scans that means fabricating an estimate, so they refuse until
///   the access-path/selectivity machinery lands. Refusing is the safe
///   direction.
/// * `LogicalCTE` refuses by construction: Go derives a CTE's stats by
///   running `utilfuncp.DoOptimize` over the seed and reading the OPTIMIZED
///   seed's physical plan (`logical_cte.go`), which no stats-only walk can
///   reproduce honestly.
/// * `SessionVars.TiDBOptJoinReorderThreshold` arrives as a parameter;
///   `DefTiDBOptJoinReorderThreshold` is `0`, which is what
///   [`LogicalPlan::recursive_derive_stats`] passes.
struct DeriveStatsFold {
    /// The first failure, per the module header's first-failure discipline.
    failure: RewriteFailure,
    /// Go `SCtx().GetSessionVars().TiDBOptJoinReorderThreshold`, read by
    /// `cardinality.EstimateFullJoinRowCount`.
    join_reorder_threshold: i32,
}

/// What one `ascend` arm decided.
enum StatsOutcome {
    /// The operator's own override ran (or refused).
    Done(Result<(StatsInfo, bool), PlanError>),
    /// Go inherits `BaseLogicalPlan.DeriveStats`; run the enum's base body.
    Base,
}

/// A refusal naming the unported Go symbol. Deriving THROUGH an operator
/// whose override is missing would fabricate a row count silently, which is
/// the wrong-answer direction; a named error is the safe one.
fn unported_stats(go_symbol: &str) -> Result<(StatsInfo, bool), PlanError> {
    Err(PlanError::internal(format!(
        "recursive_derive_stats: {go_symbol} is not ported; \
         deriving through it would fabricate a row count"
    )))
}

/// The arity failure Go cannot reach (its callers guarantee child count); the
/// ported operators answer `None` there, which becomes a loud error here.
fn stats_arity(go_symbol: &str) -> PlanError {
    PlanError::internal(format!(
        "recursive_derive_stats: {go_symbol} saw the wrong child count"
    ))
}

/// Go `cardinality.EstimateColsNDVWithMatchedLen(cols, schema, profile)` over
/// the join keys, packaged as the [`JoinKeyEstimate`] the ported
/// [`estimate_full_join_row_count`] consumes.
///
/// The body is NOT restated: the existing port in
/// [`crate::cardinality::derive_stats`] is called directly — since the
/// `StatsInfo` unification there is only one profile type and one column-id
/// spelling, so the former u64 conversion detour is gone.
fn join_key_estimate(keys: &[tidb_expr::column::Column], profile: &StatsInfo) -> JoinKeyEstimate {
    let ids: Vec<i64> = keys.iter().map(|col| col.unique_id).collect();
    let (ndv, matched_len) = estimate_cols_ndv_with_matched_len(&ids, profile);
    JoinKeyEstimate {
        ndv,
        matched_len,
        key_len: ids.len(),
    }
}

impl OwnedRewrite for DeriveStatsFold {
    /// Go's `cumColGroups`, one copy per child.
    type Down = Vec<Vec<tidb_expr::column::Column>>;
    /// Go's `(*property.StatsInfo, bool)` return, plus the node's
    /// materialized schema. Go's `LogicalSchemaProducer.Schema()` MEMOISES a
    /// schema-less operator's answer into the node; this walk must not write
    /// schemas, so the memo travels UP the fold instead — otherwise asking a
    /// 60k-deep schema-less chain for its schema would recurse the host
    /// stack.
    type Up = (StatsInfo, bool, Schema);

    fn descend(
        &mut self,
        node: &mut LogicalPlan,
        down: Self::Down,
    ) -> Descend<Self::Down, Self::Up> {
        if self.failure.is_failed() {
            // Dead walk: the fold pads missing downs with Default, and every
            // ascend below short-circuits.
            return Descend::Children(Vec::new());
        }
        // Go: `cumColGroups := p.self.ExtractColGroups(colGroups)`, handed to
        // EVERY child.
        let cum = node.extract_col_groups(&down);
        Descend::Children(vec![cum; node.children().len()])
    }

    fn ascend(
        &mut self,
        mut node: LogicalPlan,
        child_ups: Vec<Self::Up>,
    ) -> (LogicalPlan, Self::Up) {
        let dead = || {
            (
                StatsInfo::new(0.0, std::iter::empty()),
                true,
                Schema::default(),
            )
        };
        if self.failure.is_failed() {
            return (node, dead());
        }
        let child_stats: Vec<StatsInfo> = child_ups
            .iter()
            .map(|(stats, _, _)| stats.clone())
            .collect();
        let reloads: Vec<bool> = child_ups.iter().map(|(_, reload, _)| *reload).collect();
        let child_schemas: Vec<Schema> = child_ups
            .iter()
            .map(|(_, _, schema)| schema.clone())
            .collect();
        // Go `p.self.Schema()`, memoised: own schema, else the single
        // child's, carried up by the fold rather than recomputed by a
        // recursive walk.
        let self_schema = schema_producer::materialized_schema(
            node.base().base.schema(),
            &child_schemas.iter().collect::<Vec<_>>(),
        );

        let outcome = match &mut node {
            // -- Overrides that exist in this port -------------------------
            LogicalPlan::DataSource(op) => match op.table_stats.clone() {
                // Go `initStats` (`core/stats.go`) always attaches at least
                // the pseudo table before DeriveStats runs; a source with no
                // `table_stats` means the builder skipped that step.
                None => StatsOutcome::Done(Err(PlanError::internal(
                    "recursive_derive_stats: DataSource.table_stats is absent; \
                     Go's initStats attaches at least the pseudo table first",
                ))),
                Some(table_stats) => {
                    if let Some(stats) = op.base.base.stats_info().cloned() {
                        StatsOutcome::Done(Ok((stats, op.all_conds.is_empty())))
                    } else {
                        // Go `deriveStats4DataSource`: `ds.stats =
                        // deriveStatsByFilter(ds, ds.PushedDownConds, nil)`.
                        let schema_ids: Vec<i64> = self_schema
                            .columns
                            .iter()
                            .map(|col| col.unique_id)
                            .collect();
                        let resolve = |unique_id: i64| {
                            let position = schema_ids.iter().position(|id| *id == unique_id)?;
                            let column = op.columns.get(position)?;
                            Some(crate::cardinality::pseudo::PseudoColumn {
                                lower_name: column.name.to_lowercase(),
                                unique_key_flag: false,
                            })
                        };
                        let range_selectivity = if op.table_scan_penalty.pseudo_stats {
                            pseudo_range_filter_selectivity(
                                op,
                                &table_stats,
                                &op.pushed_down_conds,
                                &self_schema,
                            )
                        } else {
                            analyzed_filter_selectivity(&table_stats, &op.pushed_down_conds)
                        };
                        let stats = range_selectivity.map_or_else(
                            || {
                                crate::cardinality::pseudo::derive_stats_by_filter_pseudo(
                                    &table_stats,
                                    &op.pushed_down_conds,
                                    &resolve,
                                    &[],
                                    crate::cost_factors::SELECTION_FACTOR,
                                    crate::cardinality::derive_stats::DEF_SCALE_NDV_SKEW_RATIO,
                                )
                            },
                            |selectivity| {
                                table_stats.scale(
                                    selectivity,
                                    crate::cardinality::derive_stats::DEF_SCALE_NDV_SKEW_RATIO,
                                )
                            },
                        );
                        op.base.base.set_stats(Some(stats.clone()));
                        StatsOutcome::Done(Ok((stats, op.all_conds.is_empty())))
                    }
                }
            },
            LogicalPlan::Selection(op) => StatsOutcome::Done(
                op.derive_stats(&child_stats, &reloads)
                    .ok_or_else(|| stats_arity("LogicalSelection.DeriveStats")),
            ),
            LogicalPlan::Projection(op) => StatsOutcome::Done(
                op.derive_stats(&child_stats, &self_schema, &reloads)
                    .ok_or_else(|| stats_arity("LogicalProjection.DeriveStats")),
            ),
            LogicalPlan::Join(op) => {
                // Go `logical_join.go` DeriveStats: EqualCondOutCnt is
                // computed from the children BEFORE the per-join-type
                // branches, with `is_cartesian = (0 == len(p.EqualConditions))`
                // and nil NA-key slices.
                match (child_stats.first(), child_stats.get(1)) {
                    (Some(left), Some(right)) => {
                        let (left_keys, right_keys, _, _) = op.get_join_keys();
                        let input = FullJoinRowCountInput {
                            left_row_count: left.row_count(),
                            right_row_count: right.row_count(),
                            is_cartesian: op.equal_conditions.is_empty(),
                            left_join_keys: join_key_estimate(&left_keys, left),
                            right_join_keys: join_key_estimate(&right_keys, right),
                            // Go passes `nil, nil` for the NA keys here.
                            left_non_equi_keys: join_key_estimate(&[], left),
                            right_non_equi_keys: join_key_estimate(&[], right),
                            join_reorder_threshold: self.join_reorder_threshold,
                        };
                        let equal_cond_out_cnt = estimate_full_join_row_count(&input);
                        StatsOutcome::Done(
                            op.derive_stats(
                                &child_stats,
                                &self_schema,
                                equal_cond_out_cnt,
                                &reloads,
                            )
                            .ok_or_else(|| stats_arity("LogicalJoin.DeriveStats")),
                        )
                    }
                    _ => StatsOutcome::Done(Err(stats_arity("LogicalJoin.DeriveStats"))),
                }
            }
            LogicalPlan::Apply(op) => {
                if op.needs_lateral_row_count_estimate() {
                    // The first two `IsLateral` branches of Go's body; see
                    // the operator's own doc for what they need.
                    StatsOutcome::Done(unported_stats(
                        "LogicalApply.DeriveStats' IsLateral row-count branches (logical_apply.go)",
                    ))
                } else {
                    let outer_len = child_schemas
                        .first()
                        .map_or(0, |schema| schema.columns.len());
                    StatsOutcome::Done(
                        op.derive_stats(&child_stats, &self_schema, outer_len, None, &reloads)
                            .ok_or_else(|| stats_arity("LogicalApply.DeriveStats")),
                    )
                }
            }
            LogicalPlan::Aggregation(op) => StatsOutcome::Done(
                op.derive_stats(&child_stats, &self_schema, &reloads)
                    .ok_or_else(|| stats_arity("LogicalAggregation.DeriveStats")),
            ),
            LogicalPlan::Limit(op) => StatsOutcome::Done(
                op.derive_stats(&child_stats, &reloads)
                    .ok_or_else(|| stats_arity("LogicalLimit.DeriveStats")),
            ),
            LogicalPlan::UnionAll(op) => {
                StatsOutcome::Done(Ok(op.derive_stats(&child_stats, &self_schema, &reloads)))
            }
            // Go `LogicalPartitionUnionAll` embeds `LogicalUnionAll` and
            // inherits its DeriveStats (`logical_partition_union_all.go`).
            LogicalPlan::PartitionUnionAll(op) => StatsOutcome::Done(Ok(op
                .union_all
                .derive_stats(&child_stats, &self_schema, &reloads))),
            LogicalPlan::Window(op) => StatsOutcome::Done(
                op.derive_stats(&child_stats, &self_schema, &reloads)
                    .ok_or_else(|| stats_arity("LogicalWindow.DeriveStats")),
            ),
            LogicalPlan::MaxOneRow(op) => {
                StatsOutcome::Done(Ok(op.derive_stats(&self_schema, &reloads)))
            }
            LogicalPlan::Sequence(op) => StatsOutcome::Done(
                op.derive_stats(&child_stats, &reloads)
                    .ok_or_else(|| stats_arity("LogicalSequence.DeriveStats")),
            ),
            LogicalPlan::TableDual(op) => {
                StatsOutcome::Done(Ok(op.derive_stats(&self_schema, &reloads)))
            }
            LogicalPlan::MemTable(op) => {
                // Go builds `statistics.PseudoTable(p.TableInfo, ..)` and
                // reads its `RealtimeCount`, which is `PseudoRowCount = 10000`
                // (`statistics/table.go:42`) for every pseudo table.
                const PSEUDO_ROW_COUNT: f64 = 10_000.0;
                StatsOutcome::Done(Ok(op.derive_stats(
                    &self_schema,
                    &reloads,
                    PSEUDO_ROW_COUNT,
                )))
            }
            LogicalPlan::Show(op) => {
                StatsOutcome::Done(Ok(op.derive_stats(&self_schema, &reloads)))
            }
            LogicalPlan::ShowDDLJobs(op) => {
                StatsOutcome::Done(Ok(op.derive_stats(&self_schema, &reloads)))
            }

            LogicalPlan::TopN(op) => StatsOutcome::Done(
                op.derive_stats(&child_stats, &reloads)
                    .ok_or_else(|| stats_arity("LogicalTopN.DeriveStats")),
            ),
            LogicalPlan::CTETable(op) => {
                StatsOutcome::Done(op.derive_stats(&reloads).ok_or_else(|| {
                    // Go carries a nil SeedStat into a later nil-deref;
                    // failing here is the loud spelling of the same absence.
                    PlanError::internal(
                        "LogicalCTETable.DeriveStats: SeedStat is nil — the owning \
                         LogicalCTE has not derived (or its stats path is refused)",
                    )
                }))
            }

            LogicalPlan::CTE(op) => {
                let class = op
                    .cte
                    .clone()
                    .ok_or_else(|| PlanError::internal("LogicalCTE.DeriveStats: CTEClass is nil"));
                StatsOutcome::Done(class.and_then(|class| {
                    let class = class.borrow();
                    let seed_plan = class.seed_part_physical_plan.as_deref().ok_or_else(|| {
                        PlanError::internal("LogicalCTE.DeriveStats: seed physical plan is nil")
                    })?;
                    let seed_stats = seed_plan.stats_info().cloned().ok_or_else(|| {
                        PlanError::internal("LogicalCTE.DeriveStats: seed stats are nil")
                    })?;
                    let seed_schema = class
                        .seed_part_logical_plan
                        .as_deref()
                        .and_then(LogicalPlan::schema)
                        .cloned()
                        .ok_or_else(|| {
                            PlanError::internal("LogicalCTE.DeriveStats: seed schema is nil")
                        })?;
                    let recursive = match (
                        class.recursive_part_physical_plan.as_deref(),
                        class.recursive_part_logical_plan.as_deref(),
                    ) {
                        (Some(plan), Some(logical)) => Some((
                            plan.stats_info().cloned().ok_or_else(|| {
                                PlanError::internal(
                                    "LogicalCTE.DeriveStats: recursive stats are nil",
                                )
                            })?,
                            logical.schema().cloned().ok_or_else(|| {
                                PlanError::internal(
                                    "LogicalCTE.DeriveStats: recursive schema is nil",
                                )
                            })?,
                        )),
                        (None, None) => None,
                        _ => {
                            return Err(PlanError::internal(
                                "LogicalCTE.DeriveStats: recursive logical/physical plans disagree",
                            ));
                        }
                    };
                    drop(class);
                    Ok(op.derive_stats(
                        &seed_stats,
                        &seed_schema,
                        recursive.as_ref().map(|(stats, schema)| (stats, schema)),
                        &self_schema,
                        None,
                        &reloads,
                    ))
                }))
            }
            // -- Go overrides NOT yet ported: refuse, never fall through ---
            LogicalPlan::TableScan(_) => StatsOutcome::Done(unported_stats(
                "deriveStats4LogicalTableScan (core/stats.go): needs \
                 deriveStatsByFilter and ranger.BuildTableRange — the \
                 access-path/selectivity machinery",
            )),
            LogicalPlan::IndexScan(_) => StatsOutcome::Done(unported_stats(
                "deriveStats4LogicalIndexScan (core/stats.go): needs \
                 deriveStatsByFilter, ranger.FullRange and \
                 util.IndexInfo2Cols — the access-path/selectivity machinery",
            )),
            // -- Go inherits the base body ---------------------------------
            // `logical_expand.go:133` says so explicitly; the others have no
            // DeriveStats in their files.
            LogicalPlan::Sort(_)
            | LogicalPlan::Lock(_)
            | LogicalPlan::UnionScan(_)
            | LogicalPlan::TiKVSingleGather(_)
            | LogicalPlan::Expand(_) => StatsOutcome::Base,
        };

        let result = match outcome {
            StatsOutcome::Done(result) => result,
            StatsOutcome::Base => {
                node.derive_stats(&child_stats, &self_schema, &child_schemas, &reloads)
            }
        };
        match result {
            Ok((stats, reload)) => (node, (stats, reload, self_schema)),
            Err(error) => {
                self.failure.record(error);
                (node, dead())
            }
        }
    }
}

/// Go `RecursiveDeriveStats(colGroups)` at tree level: derives and WRITES the
/// statistics onto every node (each operator's body calls its own
/// `set_stats`), and returns the root's `(profile, reload)` answer.
pub fn recursive_derive_stats(
    plan: LogicalPlan,
    col_groups: Vec<Vec<tidb_expr::column::Column>>,
    join_reorder_threshold: i32,
) -> (LogicalPlan, Result<(StatsInfo, bool), PlanError>) {
    let mut fold = DeriveStatsFold {
        failure: RewriteFailure::default(),
        join_reorder_threshold,
    };
    let (plan, (stats, reload, _schema)) = fold_owned(&mut fold, plan, col_groups);
    match fold.failure.take() {
        Some(error) => (plan, Err(error)),
        None => (plan, Ok((stats, reload))),
    }
}
