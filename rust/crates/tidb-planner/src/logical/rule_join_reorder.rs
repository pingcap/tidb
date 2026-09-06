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

//! Pinned Go `pkg/planner/core/rule_join_reorder*.go` legacy solver.

use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use tidb_expr::column::Column;
use tidb_expr::expr_util::normal_form::expr_from_schema;
use tidb_expr::expr_util::predicates::{check_non_deterministic, is_mutable_effects_expr};
use tidb_expr::expr_util::substitute::{column_substitute, SubstituteOptions};
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::{merge_schema, Schema};
use tidb_expr::simple_expr::extract_columns;

use super::projection::LogicalProjection;
use super::rule::{LogicalOptRule, RuleContext};
use super::selection::LogicalSelection;
use super::{BaseLogicalPlan, LogicalPlan, PlanError};
use crate::find_best_task::LogicalJoinType;
use crate::joinorder::JoinMethodHint;

#[derive(Clone, Debug)]
struct JoinTypeWithExtMsg {
    join_type: LogicalJoinType,
    outer_bind_conditions: Vec<Expression>,
}

#[derive(Clone, Debug, Default)]
struct BasicJoinGroupInfo {
    equal_edges: Vec<ScalarFunction>,
    other_conditions: Vec<Expression>,
    join_types: Vec<JoinTypeWithExtMsg>,
    null_extended_columns: Option<Schema>,
    join_method_hints: BTreeMap<i32, JoinMethodHint>,
}

#[derive(Clone, Debug, Default)]
struct JoinGroupResult {
    group: Vec<LogicalPlan>,
    has_outer_join: bool,
    join_order_hints: Vec<Rc<crate::plan_builder::from::JoinHints>>,
    info: BasicJoinGroupInfo,
    column_expressions: BTreeMap<i64, Expression>,
}

#[derive(Clone, Debug)]
struct JoinReorderNode {
    plan: LogicalPlan,
    cumulative_cost: f64,
}

/// Go `JoinReOrderSolver`.
pub struct JoinReOrderSolver;

impl LogicalOptRule for JoinReOrderSolver {
    fn optimize(
        &self,
        context: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let result = if context.advanced_join_reorder {
            crate::joinorder::optimize(context, plan.clone())
        } else {
            optimize_recursive(context, plan.clone())
        };
        match result {
            Ok(plan) => Ok((plan, false)),
            Err(error) => Err((plan, error)),
        }
    }

    fn name(&self) -> &'static str {
        "join_reorder"
    }
}

fn optimize_recursive(
    context: &RuleContext<'_>,
    mut plan: LogicalPlan,
) -> Result<LogicalPlan, PlanError> {
    if matches!(plan, LogicalPlan::CTE(_)) {
        return Ok(plan);
    }
    if matches!(plan, LogicalPlan::Join(_)) {
        let original_join_root = plan.clone();
        let original_schema = plan.schema().cloned();
        let original_names = plan.output_names().to_vec();
        let result = extract_join_group(context, &mut plan);
        if result.group.len() > 1 {
            let attempt = (|| {
                let mut group = Vec::with_capacity(result.group.len());
                for child in result.group.iter().cloned() {
                    group.push(optimize_recursive(context, child)?);
                }
                let all_inner = result
                    .info
                    .join_types
                    .iter()
                    .all(|join_type| join_type.join_type == LogicalJoinType::Inner);
                let use_greedy = !all_inner
                    || i32::try_from(group.len()).unwrap_or(i32::MAX)
                        > context.join_reorder_threshold;
                let mut solver = LegacyGroupSolver::new(context, result.info.clone(), all_inner);
                let (leading_hint, conflicting_leading) =
                    check_and_generate_leading_hint(&result.join_order_hints);
                if conflicting_leading {
                    set_hint_warning(
                        context,
                        "We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid",
                    );
                }
                if let Some(leading_hint) = leading_hint {
                    if use_greedy {
                        let original_conditions = solver.info.other_conditions.clone();
                        match solver.generate_leading_join_group(
                            group.clone(),
                            &leading_hint,
                            result.has_outer_join,
                        )? {
                            Some(remaining) => group = remaining,
                            None => {
                                solver.info.other_conditions = original_conditions;
                                set_hint_warning(
                                    context,
                                    "leading hint is inapplicable, check if the leading hint table is valid",
                                );
                            }
                        }
                    } else {
                        set_hint_warning(
                            context,
                            "leading hint is inapplicable for the DP join reorder algorithm",
                        );
                    }
                }
                let reordered = if use_greedy {
                    solver.solve_greedy(group)?
                } else {
                    solver.solve_dp(group)?
                };
                restore_schema_if_changed(
                    context,
                    reordered,
                    original_schema.as_ref(),
                    &original_names,
                    &result.column_expressions,
                )
            })();
            return match attempt {
                Ok(plan) => Ok(plan),
                Err(_) if context.join_reorder_through_proj => {
                    let fallback_context = without_projection_inline(context);
                    optimize_recursive(&fallback_context, original_join_root)
                }
                Err(error) => Err(error),
            };
        }
        if result.group.len() == 1 && !result.join_order_hints.is_empty() {
            set_hint_warning(
                context,
                "leading hint is inapplicable, check the join type or the join algorithm hint",
            );
        }
    }
    let children = plan.children().to_vec();
    let mut optimized = Vec::with_capacity(children.len());
    for child in children {
        optimized.push(optimize_recursive(context, child)?);
    }
    plan.set_children(optimized);
    Ok(plan)
}

fn without_projection_inline<'a>(context: &RuleContext<'a>) -> RuleContext<'a> {
    RuleContext {
        allocator: context.allocator,
        column_allocator: context.column_allocator,
        builder: context.builder,
        use_plan_cache: context.use_plan_cache,
        plan_cache_marker: context.plan_cache_marker,
        allow_derive_topn: context.allow_derive_topn,
        allow_agg_push_down: context.allow_agg_push_down,
        disabled_rules: context.disabled_rules.clone(),
        statistics_load: context.statistics_load,
        partition_pruning: context.partition_pruning,
        opt_index_prune_threshold: context.opt_index_prune_threshold,
        always_keep_join_key: context.always_keep_join_key,
        enable_unsafe_substitute: context.enable_unsafe_substitute,
        enable_semi_join_rewrite: context.enable_semi_join_rewrite,
        enable_no_decorrelate_in_select: context.enable_no_decorrelate_in_select,
        join_reorder_threshold: context.join_reorder_threshold,
        advanced_join_reorder: context.advanced_join_reorder,
        cartesian_join_order_threshold: context.cartesian_join_order_threshold,
        join_reorder_through_proj: false,
        join_reorder_through_sel: context.join_reorder_through_sel,
        outer_join_reorder: context.outer_join_reorder,
        advanced_join_hint: context.advanced_join_hint,
        hint_warning_sink: context.hint_warning_sink,
    }
}

fn extract_join_group(context: &RuleContext<'_>, plan: &mut LogicalPlan) -> JoinGroupResult {
    if let LogicalPlan::Selection(selection) = plan {
        if context.join_reorder_through_sel
            && !selection.conditions.iter().any(|condition| {
                is_mutable_effects_expr(condition) || check_non_deterministic(condition)
            })
        {
            if let [child @ LogicalPlan::Join(_)] = selection.base.children_mut().as_mut_slice() {
                let mut result = extract_join_group(context, child);
                let conditions = crate::joinorder::substitute_cols_in_exprs(
                    &selection.conditions,
                    &result.column_expressions,
                );
                result.info.other_conditions.extend(conditions);
                return result;
            }
        }
        return single_group(plan.clone());
    }
    if let LogicalPlan::Projection(projection) = plan {
        if context.join_reorder_through_proj {
            if let Some(result) = try_inline_projection(context, projection) {
                return result;
            }
        }
        return single_group(plan.clone());
    }

    let LogicalPlan::Join(join) = plan else {
        return single_group(plan.clone());
    };
    let mut join_order_hints = Vec::new();
    let current_leading = if join.prefer_join_order {
        join.hint_info.clone()
    } else if join.internal_prefer_join_order {
        join.internal_hint_info.clone()
    } else {
        None
    };
    if let Some(hint) = current_leading.as_ref() {
        join_order_hints.push(Rc::clone(hint));
    }
    let is_outer = matches!(
        join.join_type,
        LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter
    );
    let reorderable = !(join.prefer_join_type != 0 && !context.advanced_join_hint)
        && !join.straight_join
        && matches!(
            join.join_type,
            LogicalJoinType::Inner | LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter
        )
        && (!is_outer || !join.equal_conditions.is_empty())
        && !join
            .equal_conditions
            .iter()
            .any(|condition| condition.func_name.lowercase() == "nulleq")
        && (context.outer_join_reorder || !is_outer);
    if !reorderable {
        if !join_order_hints.is_empty() {
            join.hint_info = None;
            join.internal_hint_info = None;
        }
        return JoinGroupResult {
            group: vec![plan.clone()],
            join_order_hints,
            ..JoinGroupResult::default()
        };
    }

    let join_snapshot = join.clone();
    let [left, right] = join.base.children_mut().as_mut_slice() else {
        return single_group(plan.clone());
    };
    let mut left_has_hint = false;
    let mut right_has_hint = false;
    let mut method_hints = BTreeMap::new();
    if context.advanced_join_hint && join_snapshot.prefer_join_type != 0 {
        if let Some(hint_info) = join_snapshot.hint_info.as_ref() {
            if join_snapshot.left_prefer_join_type != 0 {
                method_hints.insert(
                    left.id(),
                    JoinMethodHint {
                        prefer_join_method: join_snapshot.left_prefer_join_type,
                        hint_info: Rc::clone(hint_info),
                    },
                );
                left_has_hint = true;
            }
            if join_snapshot.right_prefer_join_type != 0 {
                method_hints.insert(
                    right.id(),
                    JoinMethodHint {
                        prefer_join_method: join_snapshot.right_prefer_join_type,
                        hint_info: Rc::clone(hint_info),
                    },
                );
                right_has_hint = true;
            }
        }
    }

    let left_preserved = current_leading
        .as_ref()
        .is_some_and(|hint| derived_table_in_leading_hint(left, hint));
    let mut left_result = if join_snapshot.join_type != LogicalJoinType::RightOuter
        && !left_has_hint
        && !left_preserved
    {
        extract_join_group(context, left)
    } else {
        single_group(left.clone())
    };
    if join_snapshot.join_type == LogicalJoinType::LeftOuter
        && crate::joinorder::outer_join_side_filters_touch_multiple_leaves(
            &join_snapshot,
            &left_result.group,
            &left_result.column_expressions,
            true,
        )
    {
        return single_group(plan.clone());
    }

    let right_preserved = current_leading
        .as_ref()
        .is_some_and(|hint| derived_table_in_leading_hint(right, hint));
    let mut right_result = if join_snapshot.join_type != LogicalJoinType::LeftOuter
        && !right_has_hint
        && !right_preserved
    {
        extract_join_group(context, right)
    } else {
        single_group(right.clone())
    };
    if join_snapshot.join_type == LogicalJoinType::RightOuter
        && crate::joinorder::outer_join_side_filters_touch_multiple_leaves(
            &join_snapshot,
            &right_result.group,
            &right_result.column_expressions,
            false,
        )
    {
        return single_group(plan.clone());
    }

    let mut result = JoinGroupResult::default();
    result.group.append(&mut left_result.group);
    result.group.append(&mut right_result.group);
    result.has_outer_join = is_outer || left_result.has_outer_join || right_result.has_outer_join;
    result.join_order_hints.append(&mut join_order_hints);
    result
        .join_order_hints
        .append(&mut left_result.join_order_hints);
    result
        .join_order_hints
        .append(&mut right_result.join_order_hints);
    result
        .info
        .equal_edges
        .append(&mut left_result.info.equal_edges);
    result
        .info
        .equal_edges
        .append(&mut right_result.info.equal_edges);
    result
        .info
        .other_conditions
        .append(&mut left_result.info.other_conditions);
    result
        .info
        .other_conditions
        .append(&mut right_result.info.other_conditions);
    result
        .info
        .join_types
        .append(&mut left_result.info.join_types);
    result
        .info
        .join_types
        .append(&mut right_result.info.join_types);
    result.info.join_method_hints.append(&mut method_hints);
    result
        .info
        .join_method_hints
        .append(&mut left_result.info.join_method_hints);
    result
        .info
        .join_method_hints
        .append(&mut right_result.info.join_method_hints);
    result
        .column_expressions
        .append(&mut left_result.column_expressions);
    result
        .column_expressions
        .append(&mut right_result.column_expressions);

    let mut null_columns = BTreeMap::new();
    for schema in [
        left_result.info.null_extended_columns.as_ref(),
        right_result.info.null_extended_columns.as_ref(),
        (join_snapshot.join_type == LogicalJoinType::LeftOuter)
            .then(|| right.schema())
            .flatten(),
        (join_snapshot.join_type == LogicalJoinType::RightOuter)
            .then(|| left.schema())
            .flatten(),
    ]
    .into_iter()
    .flatten()
    {
        for column in &schema.columns {
            null_columns
                .entry(column.unique_id)
                .or_insert_with(|| column.clone());
        }
    }
    if !null_columns.is_empty() {
        result.info.null_extended_columns = Some(Schema::new(null_columns.into_values().collect()));
    }

    result
        .info
        .equal_edges
        .extend(join_snapshot.equal_conditions.clone());
    let mut local_conditions = join_snapshot.other_conditions.clone();
    local_conditions.extend(join_snapshot.left_conditions.clone());
    local_conditions.extend(join_snapshot.right_conditions.clone());
    for _ in &join_snapshot.equal_conditions {
        result.info.join_types.push(JoinTypeWithExtMsg {
            join_type: join_snapshot.join_type,
            outer_bind_conditions: is_outer
                .then(|| local_conditions.clone())
                .unwrap_or_default(),
        });
    }
    if !is_outer {
        result.info.other_conditions.extend(local_conditions);
    }
    if !result.column_expressions.is_empty() {
        result.info.equal_edges = crate::joinorder::substitute_cols_in_eq_edges(
            &result.info.equal_edges,
            &result.column_expressions,
        );
        result.info.other_conditions = crate::joinorder::substitute_cols_in_exprs(
            &result.info.other_conditions,
            &result.column_expressions,
        );
        for join_type in &mut result.info.join_types {
            join_type.outer_bind_conditions = crate::joinorder::substitute_cols_in_exprs(
                &join_type.outer_bind_conditions,
                &result.column_expressions,
            );
        }
    }
    result
}

fn single_group(plan: LogicalPlan) -> JoinGroupResult {
    JoinGroupResult {
        group: vec![plan],
        ..JoinGroupResult::default()
    }
}

fn derived_table_in_leading_hint(
    plan: &LogicalPlan,
    hints: &crate::plan_builder::from::JoinHints,
) -> bool {
    if plan.query_block_offset() <= 1 {
        return false;
    }
    let Some(alias) = crate::plan_builder::from::extract_table_alias(plan.output_names()) else {
        return false;
    };
    fn contains(elements: &[tidb_ast::LeadingElement], database: &str, table: &str) -> bool {
        elements.iter().any(|element| match element {
            tidb_ast::LeadingElement::Table(hint) => {
                hint.name.eq_ignore_ascii_case(table)
                    && hint
                        .db_name
                        .as_deref()
                        .is_none_or(|db| db == "*" || db.eq_ignore_ascii_case(database))
            }
            tidb_ast::LeadingElement::Group(group) => contains(group, database, table),
        })
    }
    hints
        .leading
        .as_deref()
        .is_some_and(|leading| contains(leading, &alias.db_name, &alias.table_name))
}

fn try_inline_projection(
    context: &RuleContext<'_>,
    projection: &mut LogicalProjection,
) -> Option<JoinGroupResult> {
    if projection.proj4_expand
        || projection.exprs.iter().any(|expression| {
            extract_columns(expression).is_empty()
                || !inlineable_projection_expression(expression)
                || is_mutable_effects_expr(expression)
                || check_non_deterministic(expression)
                || expression.is_correlated()
        })
    {
        return None;
    }
    let [child @ LogicalPlan::Join(_)] = projection.base.children_mut().as_mut_slice() else {
        return None;
    };
    let mut child_result = extract_join_group(context, child);
    if !can_inline_projection(projection, &child_result) {
        return None;
    }
    let schema = projection.base.base.schema()?;
    let mut mappings = BTreeMap::new();
    for (output, expression) in schema.columns.iter().zip(&projection.exprs) {
        let rewritten = crate::joinorder::substitute_cols_in_expr(
            expression.clone(),
            &child_result.column_expressions,
        );
        if matches!(&rewritten, Expression::Column(column) if column.unique_id == output.unique_id)
        {
            continue;
        }
        mappings.insert(output.unique_id, rewritten);
    }
    for (id, expression) in child_result.column_expressions {
        mappings.entry(id).or_insert(expression);
    }
    child_result.column_expressions = mappings;
    Some(child_result)
}

fn inlineable_projection_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Column(_) => true,
        Expression::ScalarFunction(function) => function
            .get_args()
            .iter()
            .all(inlineable_projection_expression),
        Expression::Constant(constant) => constant.deferred_expr.is_none(),
        _ => false,
    }
}

fn can_inline_projection(projection: &LogicalProjection, child_result: &JoinGroupResult) -> bool {
    let mut leaf_by_column = BTreeMap::new();
    for (leaf_index, leaf) in child_result.group.iter().enumerate() {
        let Some(schema) = leaf.schema() else {
            return false;
        };
        for column in &schema.columns {
            if leaf_by_column
                .insert(column.unique_id, leaf_index)
                .is_some_and(|previous| previous != leaf_index)
            {
                return false;
            }
        }
    }
    for expression in &projection.exprs {
        let rewritten = crate::joinorder::substitute_cols_in_expr(
            expression.clone(),
            &child_result.column_expressions,
        );
        let columns = extract_columns(&rewritten);
        if columns.is_empty() {
            return false;
        }
        if child_result
            .info
            .null_extended_columns
            .as_ref()
            .is_some_and(|schema| columns.iter().any(|column| schema.contains(column)))
        {
            return false;
        }
        let Some(first_leaf) = leaf_by_column.get(&columns[0].unique_id).copied() else {
            return false;
        };
        if columns
            .iter()
            .any(|column| leaf_by_column.get(&column.unique_id).copied() != Some(first_leaf))
        {
            return false;
        }
    }
    true
}

fn restore_schema_if_changed(
    context: &RuleContext<'_>,
    plan: LogicalPlan,
    original_schema: Option<&Schema>,
    original_names: &[tidb_datatype::FieldName],
    column_expressions: &BTreeMap<i64, Expression>,
) -> Result<LogicalPlan, PlanError> {
    let Some(original_schema) = original_schema else {
        return Ok(plan);
    };
    if plan
        .schema()
        .is_some_and(|schema| schema.equal(original_schema))
    {
        return Ok(plan);
    }
    let current_schema = plan
        .schema()
        .ok_or_else(|| PlanError::internal("join reorder result has no schema"))?;
    let mut expressions = Vec::with_capacity(original_schema.columns.len());
    for column in &original_schema.columns {
        if let Some(expression) = column_expressions.get(&column.unique_id) {
            expressions.push(expression.clone());
        } else if current_schema.contains(column) {
            expressions.push(Expression::Column(column.clone()));
        } else {
            return Err(PlanError::internal(
                "join reorder: schema restore mapping missing after projection inlining",
            ));
        }
    }
    let offset = plan.query_block_offset();
    let mut projection = LogicalProjection::new(
        BaseLogicalPlan::new(context.allocator, LogicalProjection::TYPE, offset),
        expressions,
    );
    projection
        .base
        .base
        .set_schema(Some(original_schema.clone()));
    projection
        .base
        .base
        .set_output_names(original_names.to_vec());
    projection.base.set_children(vec![plan]);
    Ok(LogicalPlan::Projection(projection))
}

fn set_hint_warning(context: &RuleContext<'_>, message: &str) {
    if let Some(sink) = context.hint_warning_sink {
        sink.set_hint_warning(message);
    }
}

fn check_and_generate_leading_hint(
    hints: &[Rc<crate::plan_builder::from::JoinHints>],
) -> (Option<Rc<crate::plan_builder::from::JoinHints>>, bool) {
    let Some(first) = hints.first() else {
        return (None, false);
    };
    let conflicting = hints.windows(2).any(|pair| !Rc::ptr_eq(&pair[0], &pair[1]));
    if conflicting {
        (None, true)
    } else {
        (Some(Rc::clone(first)), false)
    }
}

struct LegacyGroupSolver<'a, 'ctx> {
    context: &'a RuleContext<'ctx>,
    info: BasicJoinGroupInfo,
    all_inner: bool,
    leading_join_group: Option<LogicalPlan>,
}

impl<'a, 'ctx> LegacyGroupSolver<'a, 'ctx> {
    fn new(context: &'a RuleContext<'ctx>, info: BasicJoinGroupInfo, all_inner: bool) -> Self {
        Self {
            context,
            info,
            all_inner,
            leading_join_group: None,
        }
    }

    fn generate_leading_join_group(
        &mut self,
        plans: Vec<LogicalPlan>,
        hints: &crate::plan_builder::from::JoinHints,
        has_outer_join: bool,
    ) -> Result<Option<Vec<LogicalPlan>>, PlanError> {
        let Some(elements) = hints.leading.as_deref() else {
            return Ok(None);
        };
        let original = plans.clone();
        let (leading, remaining, applicable) =
            self.build_leading_tree(elements, plans, has_outer_join)?;
        if !applicable {
            return Ok(None);
        }
        let Some(leading) = leading else {
            return Ok(None);
        };
        self.leading_join_group = Some(leading);
        let _ = original;
        Ok(Some(remaining))
    }

    fn build_leading_tree(
        &mut self,
        elements: &[tidb_ast::LeadingElement],
        mut available: Vec<LogicalPlan>,
        has_outer_join: bool,
    ) -> Result<(Option<LogicalPlan>, Vec<LogicalPlan>, bool), PlanError> {
        if elements.is_empty() {
            return Ok((None, available, false));
        }
        let original = available.clone();
        let mut current = None;
        for element in elements {
            let next = match element {
                tidb_ast::LeadingElement::Table(table) => {
                    let Some(index) = available
                        .iter()
                        .position(|plan| hinted_plan_matches(plan, table))
                    else {
                        return Ok((None, original, false));
                    };
                    available.remove(index)
                }
                tidb_ast::LeadingElement::Group(group) => {
                    let (nested, remaining, applicable) =
                        self.build_leading_tree(group, available, has_outer_join)?;
                    if !applicable {
                        return Ok((None, original, false));
                    }
                    available = remaining;
                    let Some(nested) = nested else {
                        return Ok((None, original, false));
                    };
                    nested
                }
            };
            current = match current {
                None => Some(next),
                Some(left) => {
                    let Some(joined) = self.connect_leading_nodes(left, next, has_outer_join)?
                    else {
                        return Ok((None, original, false));
                    };
                    Some(joined)
                }
            };
        }
        Ok((current, available, true))
    }

    fn connect_leading_nodes(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        has_outer_join: bool,
    ) -> Result<Option<LogicalPlan>, PlanError> {
        let (left, right, edges, join_type) = self.connection(&left, &right);
        if has_outer_join && edges.is_empty() && !self.has_other_join_condition(&left, &right) {
            return Ok(None);
        }
        let (join, remaining) = self.make_join(
            left,
            right,
            &edges,
            join_type,
            self.info.other_conditions.clone(),
        )?;
        self.info.other_conditions = remaining;
        Ok(Some(join))
    }

    fn generate_nodes(&self, plans: Vec<LogicalPlan>) -> Result<Vec<JoinReorderNode>, PlanError> {
        plans
            .into_iter()
            .map(|plan| {
                let (plan, result) = crate::logical::rewrite::recursive_derive_stats(
                    plan,
                    Vec::new(),
                    self.context.join_reorder_threshold,
                );
                result?;
                let cumulative_cost = crate::joinorder::cumulative_cost_by_children(&plan)?;
                Ok(JoinReorderNode {
                    plan,
                    cumulative_cost,
                })
            })
            .collect()
    }

    fn solve_greedy(&mut self, plans: Vec<LogicalPlan>) -> Result<LogicalPlan, PlanError> {
        let mut nodes = self.generate_nodes(plans)?;
        nodes.sort_by(|left, right| left.cumulative_cost.total_cmp(&right.cumulative_cost));
        let join_node_count = nodes.len();
        if let Some(leading) = self.leading_join_group.clone() {
            let mut leading_nodes = self.generate_nodes(vec![leading])?;
            leading_nodes.append(&mut nodes);
            nodes = leading_nodes;
        }
        let mut cartesian_group = Vec::new();
        while !nodes.is_empty() {
            let mut current = nodes.remove(0);
            loop {
                let mut best: Option<(usize, JoinReorderNode, Vec<Expression>, bool)> = None;
                let mut best_cost = f64::MAX;
                let mut best_is_cartesian = false;
                let mut any_valid: Option<(usize, JoinReorderNode, Vec<Expression>)> = None;
                for (index, candidate) in nodes.iter().enumerate() {
                    let Some((join, remaining, cartesian)) =
                        self.check_connection_and_make_join(&current.plan, &candidate.plan)?
                    else {
                        continue;
                    };
                    if cartesian && self.context.cartesian_join_order_threshold <= 0.0 {
                        continue;
                    }
                    let joined = self.derive_join(join)?;
                    let cost = self.join_cost(&joined, &current, candidate)?;
                    any_valid = Some((
                        index,
                        JoinReorderNode {
                            plan: joined.clone(),
                            cumulative_cost: cost,
                        },
                        remaining.clone(),
                    ));
                    let better = if !best_is_cartesian && cartesian {
                        cost * self.context.cartesian_join_order_threshold < best_cost
                    } else if best_is_cartesian && !cartesian {
                        cost < best_cost * self.context.cartesian_join_order_threshold
                    } else {
                        cost < best_cost
                    };
                    if better {
                        best_cost = cost;
                        best_is_cartesian = cartesian;
                        best = Some((
                            index,
                            JoinReorderNode {
                                plan: joined,
                                cumulative_cost: cost,
                            },
                            remaining,
                            cartesian,
                        ));
                    }
                }
                let picked = best
                    .map(|(index, node, remaining, _)| (index, node, remaining))
                    .or(any_valid);
                let Some((index, node, remaining)) = picked else {
                    break;
                };
                current = node;
                nodes.remove(index);
                self.info.other_conditions = remaining;
            }
            if join_node_count > 0 && nodes.len() == join_node_count {
                set_hint_warning(
                    self.context,
                    "leading hint is inapplicable, check if the leading hint table has join conditions with other tables",
                );
            }
            cartesian_group.push(current.plan);
        }
        self.make_bushy_join(cartesian_group)
    }

    fn solve_dp(&mut self, plans: Vec<LogicalPlan>) -> Result<LogicalPlan, PlanError> {
        let equal_edges = self.info.equal_edges.clone();
        let mut adjacency = vec![Vec::new(); plans.len()];
        let mut graph_edges = Vec::with_capacity(equal_edges.len());
        for (edge_index, edge) in equal_edges.iter().enumerate() {
            let [left, right] = edge.get_args() else {
                return Err(PlanError::internal("join reorder dp: malformed eq edge"));
            };
            let left_index = find_node_for_columns(&plans, &extract_columns(left))?;
            let right_index = find_node_for_columns(&plans, &extract_columns(right))?;
            if left_index == right_index {
                return Err(PlanError::internal(
                    "join reorder dp: eq edge doesn't connect two join-group nodes",
                ));
            }
            adjacency[left_index].push(right_index);
            adjacency[right_index].push(left_index);
            graph_edges.push((left_index, right_index, edge_index));
        }
        let nodes = self.generate_nodes(plans)?;
        let mut non_equal = Vec::new();
        for condition in self.info.other_conditions.clone() {
            let mut mask = 0_u64;
            for column in extract_columns(&condition) {
                mask |= 1_u64 << find_node_for_column_nodes(&nodes, &column)?;
            }
            non_equal.push((mask, condition));
        }

        let mut visited = vec![false; nodes.len()];
        let mut components = Vec::new();
        let mut remaining_non_equal = non_equal;
        for start in 0..nodes.len() {
            if visited[start] {
                continue;
            }
            let mut queue = VecDeque::from([start]);
            visited[start] = true;
            let mut component = Vec::new();
            while let Some(node) = queue.pop_front() {
                component.push(node);
                for &next in &adjacency[node] {
                    if !visited[next] {
                        visited[next] = true;
                        queue.push_back(next);
                    }
                }
            }
            let component_mask = component
                .iter()
                .fold(0_u64, |mask, index| mask | (1_u64 << index));
            let mut local_non_equal = Vec::new();
            remaining_non_equal.retain(|(mask, condition)| {
                if mask & component_mask == *mask {
                    local_non_equal.push((*mask, condition.clone()));
                    false
                } else {
                    true
                }
            });
            components.push(self.dp_component(
                &nodes,
                &component,
                &graph_edges,
                &local_non_equal,
            )?);
        }
        self.info.other_conditions = remaining_non_equal
            .into_iter()
            .map(|(_, condition)| condition)
            .collect();
        self.make_bushy_join(components)
    }

    fn dp_component(
        &mut self,
        nodes: &[JoinReorderNode],
        component: &[usize],
        graph_edges: &[(usize, usize, usize)],
        non_equal: &[(u64, Expression)],
    ) -> Result<LogicalPlan, PlanError> {
        if component.len() == 1 {
            return Ok(nodes[component[0]].plan.clone());
        }
        if component.len() >= usize::BITS as usize {
            return Err(PlanError::internal(
                "too many nodes for legacy DP join reorder",
            ));
        }
        let mut old_to_local = BTreeMap::new();
        for (local, old) in component.iter().copied().enumerate() {
            old_to_local.insert(old, local);
        }
        let full = (1_usize << component.len()) - 1;
        let mut best: Vec<Option<JoinReorderNode>> = vec![None; full + 1];
        for (local, old) in component.iter().copied().enumerate() {
            best[1 << local] = Some(nodes[old].clone());
        }
        for subset in 1..=full {
            if subset.count_ones() == 1 {
                continue;
            }
            let mut left = (subset - 1) & subset;
            while left > 0 {
                let right = subset ^ left;
                if left <= right {
                    if let (Some(left_node), Some(right_node)) =
                        (best[left].clone(), best[right].clone())
                    {
                        let mut edge_indices = Vec::new();
                        for &(old_left, old_right, edge_index) in graph_edges {
                            let (Some(&local_left), Some(&local_right)) =
                                (old_to_local.get(&old_left), old_to_local.get(&old_right))
                            else {
                                continue;
                            };
                            let crosses = left & (1 << local_left) != 0
                                && right & (1 << local_right) != 0
                                || left & (1 << local_right) != 0 && right & (1 << local_left) != 0;
                            if crosses {
                                edge_indices.push(edge_index);
                            }
                        }
                        if edge_indices.is_empty() {
                            left = (left - 1) & subset;
                            continue;
                        }
                        let global_mask = component
                            .iter()
                            .enumerate()
                            .filter(|(local, _)| subset & (1 << local) != 0)
                            .fold(0_u64, |mask, (_, old)| mask | (1_u64 << old));
                        let left_global = component
                            .iter()
                            .enumerate()
                            .filter(|(local, _)| left & (1 << local) != 0)
                            .fold(0_u64, |mask, (_, old)| mask | (1_u64 << old));
                        let right_global = global_mask ^ left_global;
                        let other_conditions = non_equal
                            .iter()
                            .filter(|(mask, _)| {
                                mask & global_mask == *mask
                                    && mask & left_global != 0
                                    && mask & right_global != 0
                            })
                            .map(|(_, condition)| condition.clone())
                            .collect();
                        let joined = self.new_join_with_edge_indices(
                            left_node.plan.clone(),
                            right_node.plan.clone(),
                            &edge_indices,
                            other_conditions,
                        )?;
                        let joined = self.derive_join(joined)?;
                        let cost = self.join_cost(&joined, &left_node, &right_node)?;
                        if best[subset]
                            .as_ref()
                            .is_none_or(|old| cost < old.cumulative_cost)
                        {
                            best[subset] = Some(JoinReorderNode {
                                plan: joined,
                                cumulative_cost: cost,
                            });
                        }
                    }
                }
                left = (left - 1) & subset;
            }
        }
        best[full]
            .take()
            .map(|node| node.plan)
            .ok_or_else(|| PlanError::internal("legacy DP join reorder found no connected plan"))
    }

    fn new_join_with_edge_indices(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        indices: &[usize],
        other_conditions: Vec<Expression>,
    ) -> Result<LogicalPlan, PlanError> {
        let (left, right, equal_conditions) = self.materialize_equal_edges(left, right, indices)?;
        self.new_join(
            left,
            right,
            equal_conditions,
            other_conditions,
            Vec::new(),
            Vec::new(),
            LogicalJoinType::Inner,
        )
    }

    fn materialize_equal_edges(
        &mut self,
        mut left: LogicalPlan,
        mut right: LogicalPlan,
        indices: &[usize],
    ) -> Result<(LogicalPlan, LogicalPlan, Vec<ScalarFunction>), PlanError> {
        let mut equal_conditions = Vec::with_capacity(indices.len());
        for &index in indices {
            let edge = self
                .info
                .equal_edges
                .get(index)
                .cloned()
                .ok_or_else(|| PlanError::internal("join edge index out of range"))?;
            let [first, second] = edge.get_args() else {
                return Err(PlanError::internal("malformed join equality"));
            };
            let left_schema = left
                .schema()
                .ok_or_else(|| PlanError::internal("join left child has no schema"))?;
            let right_schema = right
                .schema()
                .ok_or_else(|| PlanError::internal("join right child has no schema"))?;
            let Some((mut left_expr, mut right_expr, _)) =
                crate::joinorder::align_join_edge_args(first, second, left_schema, right_schema)
            else {
                return Err(PlanError::internal(
                    "join reorder dp: eq edge doesn't connect left/right plans",
                ));
            };
            if !matches!(left_expr, Expression::Column(_)) {
                let (new_left, column) = self.inject_expression(left, left_expr)?;
                left = new_left;
                left_expr = Expression::Column(column);
            }
            if !matches!(right_expr, Expression::Column(_)) {
                let (new_right, column) = self.inject_expression(right, right_expr)?;
                right = new_right;
                right_expr = Expression::Column(column);
            }
            let mut rewritten = edge;
            rewritten.args = vec![left_expr, right_expr];
            rewritten.invalidate_cached_arguments();
            equal_conditions.push(rewritten);
        }
        Ok((left, right, equal_conditions))
    }

    fn inject_expression(
        &mut self,
        mut plan: LogicalPlan,
        expression: Expression,
    ) -> Result<(LogicalPlan, Column), PlanError> {
        if let Expression::Column(column) = expression {
            return Ok((plan, column));
        }
        let original_id = plan.id();
        crate::logical::rewrite::ensure_join_projection(self.context, &mut plan)?;
        let projection_id = plan.id();
        let LogicalPlan::Projection(projection) = &plan else {
            return Err(PlanError::internal(
                "join expression injection did not produce a projection",
            ));
        };
        let schema = projection
            .base
            .base
            .schema()
            .cloned()
            .ok_or_else(|| PlanError::internal("join projection has no schema"))?;
        let substituted = column_substitute(
            &expression,
            &schema,
            &projection.exprs,
            &SubstituteOptions::new(self.context.builder),
        );
        if !is_mutable_effects_expr(&substituted) && !check_non_deterministic(&substituted) {
            if let Some((index, _)) = projection
                .exprs
                .iter()
                .enumerate()
                .find(|(_, existing)| existing.equal(&substituted))
            {
                return Ok((plan, schema.columns[index].clone()));
            }
        }
        let column = crate::logical::rewrite::append_join_projection_expr(
            self.context,
            &mut plan,
            expression,
        )?;
        if original_id != projection_id && !self.info.join_method_hints.contains_key(&projection_id)
        {
            if let Some(hint) = self.info.join_method_hints.get(&original_id).cloned() {
                self.info.join_method_hints.insert(projection_id, hint);
            }
        }
        Ok((plan, column))
    }

    fn check_connection_and_make_join(
        &mut self,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> Result<Option<(LogicalPlan, Vec<Expression>, bool)>, PlanError> {
        let (actual_left, actual_right, used, selected_join_type) = self.connection(left, right);
        let has_other = self.has_other_join_condition(&actual_left, &actual_right);
        let cartesian = used.is_empty() && !has_other;
        if cartesian && (!self.all_inner || self.context.cartesian_join_order_threshold <= 0.0) {
            return Ok(None);
        }
        let (join, remaining) = self.make_join(
            actual_left,
            actual_right,
            &used,
            selected_join_type,
            self.info.other_conditions.clone(),
        )?;
        Ok(Some((join, remaining, cartesian)))
    }

    fn connection(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> (LogicalPlan, LogicalPlan, Vec<usize>, JoinTypeWithExtMsg) {
        let mut used = Vec::new();
        let mut selected_join_type = JoinTypeWithExtMsg {
            join_type: LogicalJoinType::Inner,
            outer_bind_conditions: Vec::new(),
        };
        let mut actual_left = left.clone();
        let mut actual_right = right.clone();
        for (index, edge) in self.info.equal_edges.iter().enumerate() {
            let [first, second] = edge.get_args() else {
                continue;
            };
            let Some((_, _, swapped)) = crate::joinorder::align_join_edge_args(
                first,
                second,
                left.schema().unwrap_or(&Schema::default()),
                right.schema().unwrap_or(&Schema::default()),
            ) else {
                continue;
            };
            selected_join_type =
                self.info
                    .join_types
                    .get(index)
                    .cloned()
                    .unwrap_or(JoinTypeWithExtMsg {
                        join_type: LogicalJoinType::Inner,
                        outer_bind_conditions: Vec::new(),
                    });
            if swapped && selected_join_type.join_type != LogicalJoinType::Inner {
                actual_left = right.clone();
                actual_right = left.clone();
            } else {
                actual_left = left.clone();
                actual_right = right.clone();
            }
            used.push(index);
        }
        (actual_left, actual_right, used, selected_join_type)
    }

    fn has_other_join_condition(&self, left: &LogicalPlan, right: &LogicalPlan) -> bool {
        let (Some(left_schema), Some(right_schema)) = (left.schema(), right.schema()) else {
            return false;
        };
        let Some(merged) = merge_schema(Some(left_schema), Some(right_schema)) else {
            return false;
        };
        self.info.other_conditions.iter().any(|condition| {
            expr_from_schema(condition, &merged)
                && !expr_from_schema(condition, left_schema)
                && !expr_from_schema(condition, right_schema)
                && !self
                    .info
                    .null_extended_columns
                    .as_ref()
                    .is_some_and(|schema| {
                        extract_columns(condition)
                            .iter()
                            .any(|column| schema.contains(column))
                    })
        })
    }

    fn make_join(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        edge_indices: &[usize],
        join_type: JoinTypeWithExtMsg,
        input_conditions: Vec<Expression>,
    ) -> Result<(LogicalPlan, Vec<Expression>), PlanError> {
        let (left, right, equal_conditions) =
            self.materialize_equal_edges(left, right, edge_indices)?;
        let left_schema = left
            .schema()
            .cloned()
            .ok_or_else(|| PlanError::internal("join left child has no schema"))?;
        let right_schema = right
            .schema()
            .cloned()
            .ok_or_else(|| PlanError::internal("join right child has no schema"))?;
        let merged = merge_schema(Some(&left_schema), Some(&right_schema)).unwrap_or_default();
        let mut remaining = Vec::new();
        let mut left_conditions = Vec::new();
        let mut right_conditions = Vec::new();
        let mut other_conditions = Vec::new();
        classify_conditions(
            input_conditions,
            &left_schema,
            &right_schema,
            &merged,
            &mut remaining,
            &mut left_conditions,
            &mut right_conditions,
            &mut other_conditions,
        );
        if join_type.join_type != LogicalJoinType::Inner {
            remaining.append(&mut other_conditions);
            remaining.append(&mut left_conditions);
            remaining.append(&mut right_conditions);
        }
        let mut bind_remaining = Vec::new();
        let mut bind_left = Vec::new();
        let mut bind_right = Vec::new();
        let mut bind_other = Vec::new();
        classify_conditions(
            join_type.outer_bind_conditions,
            &left_schema,
            &right_schema,
            &merged,
            &mut bind_remaining,
            &mut bind_left,
            &mut bind_right,
            &mut bind_other,
        );
        let _ = bind_remaining;
        let plan = self.new_join(
            left,
            right,
            equal_conditions,
            other_conditions.into_iter().chain(bind_other).collect(),
            left_conditions.into_iter().chain(bind_left).collect(),
            right_conditions.into_iter().chain(bind_right).collect(),
            join_type.join_type,
        )?;
        Ok((plan, remaining))
    }

    fn new_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        equal_conditions: Vec<ScalarFunction>,
        other_conditions: Vec<Expression>,
        left_conditions: Vec<Expression>,
        right_conditions: Vec<Expression>,
        join_type: LogicalJoinType,
    ) -> Result<LogicalPlan, PlanError> {
        let mut join = crate::joinorder::new_cartesian_join(self.context, join_type, left, right)?;
        crate::joinorder::set_new_join_with_hint(&mut join, &self.info.join_method_hints);
        join.equal_conditions = equal_conditions;
        join.other_conditions = other_conditions;
        join.left_conditions = left_conditions;
        join.right_conditions = right_conditions;
        if join_type == LogicalJoinType::Inner {
            for (side, conditions) in [
                (0, std::mem::take(&mut join.left_conditions)),
                (1, std::mem::take(&mut join.right_conditions)),
            ] {
                if conditions.is_empty() {
                    continue;
                }
                let child = join.base.children_mut()[side].clone();
                let offset = child.query_block_offset();
                let schema = child.schema().cloned();
                let names = child.output_names().to_vec();
                let mut selection = LogicalSelection::new(
                    BaseLogicalPlan::new(self.context.allocator, LogicalSelection::TYPE, offset),
                    conditions,
                );
                selection.base.base.set_schema(schema);
                selection.base.base.set_output_names(names);
                selection.base.set_children(vec![child]);
                join.base.children_mut()[side] = LogicalPlan::Selection(selection);
            }
        }
        Ok(LogicalPlan::Join(join))
    }

    fn derive_join(&self, plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
        let (plan, result) = crate::logical::rewrite::recursive_derive_stats(
            plan,
            Vec::new(),
            self.context.join_reorder_threshold,
        );
        result?;
        Ok(plan)
    }

    fn join_cost(
        &self,
        joined: &LogicalPlan,
        left: &JoinReorderNode,
        right: &JoinReorderNode,
    ) -> Result<f64, PlanError> {
        let row_count = joined
            .base()
            .base
            .stats_info()
            .ok_or_else(|| PlanError::internal("join has no statistics"))?
            .row_count();
        Ok(row_count + left.cumulative_cost + right.cumulative_cost)
    }

    fn make_bushy_join(&mut self, mut plans: Vec<LogicalPlan>) -> Result<LogicalPlan, PlanError> {
        if plans.is_empty() {
            return Err(PlanError::internal("empty cartesian join group"));
        }
        while plans.len() > 1 {
            let mut next = Vec::with_capacity(plans.len().div_ceil(2));
            let mut iter = plans.into_iter();
            while let Some(left) = iter.next() {
                let Some(right) = iter.next() else {
                    next.push(left);
                    break;
                };
                let mut join = crate::joinorder::new_cartesian_join(
                    self.context,
                    LogicalJoinType::Inner,
                    left,
                    right,
                )?;
                crate::joinorder::set_new_join_with_hint(&mut join, &self.info.join_method_hints);
                let schema = join.base.base.schema().cloned().unwrap_or_default();
                self.info.other_conditions.retain(|condition| {
                    if extract_columns(condition)
                        .iter()
                        .all(|column| schema.contains(column))
                    {
                        join.other_conditions.push(condition.clone());
                        false
                    } else {
                        true
                    }
                });
                next.push(LogicalPlan::Join(join));
            }
            plans = next;
        }
        let mut result = plans.pop().unwrap();
        if !self.info.other_conditions.is_empty() {
            let offset = result.query_block_offset();
            let schema = result.schema().cloned();
            let names = result.output_names().to_vec();
            let mut selection = LogicalSelection::new(
                BaseLogicalPlan::new(self.context.allocator, LogicalSelection::TYPE, offset),
                std::mem::take(&mut self.info.other_conditions),
            );
            selection.base.base.set_schema(schema);
            selection.base.base.set_output_names(names);
            selection.base.set_children(vec![result]);
            result = LogicalPlan::Selection(selection);
        }
        Ok(result)
    }
}

fn classify_conditions(
    conditions: Vec<Expression>,
    left_schema: &Schema,
    right_schema: &Schema,
    merged_schema: &Schema,
    remaining: &mut Vec<Expression>,
    left: &mut Vec<Expression>,
    right: &mut Vec<Expression>,
    other: &mut Vec<Expression>,
) {
    for condition in conditions {
        if !is_mutable_effects_expr(&condition)
            && expr_from_schema(&condition, left_schema)
            && !expr_from_schema(&condition, right_schema)
        {
            left.push(condition);
        } else if !is_mutable_effects_expr(&condition)
            && expr_from_schema(&condition, right_schema)
            && !expr_from_schema(&condition, left_schema)
        {
            right.push(condition);
        } else if expr_from_schema(&condition, merged_schema) {
            other.push(condition);
        } else {
            remaining.push(condition);
        }
    }
}

fn find_node_for_columns(plans: &[LogicalPlan], columns: &[Column]) -> Result<usize, PlanError> {
    let Some(first) = columns.first() else {
        return Err(PlanError::internal(
            "join reorder dp: eq edge has empty column list",
        ));
    };
    let index = plans
        .iter()
        .position(|plan| plan.schema().is_some_and(|schema| schema.contains(first)))
        .ok_or_else(|| PlanError::internal("unknown column in join reorder"))?;
    if columns.iter().any(|column| {
        !plans[index]
            .schema()
            .is_some_and(|schema| schema.contains(column))
    }) {
        return Err(PlanError::internal(
            "join reorder: columns span multiple nodes",
        ));
    }
    Ok(index)
}

fn find_node_for_column_nodes(
    nodes: &[JoinReorderNode],
    column: &Column,
) -> Result<usize, PlanError> {
    nodes
        .iter()
        .position(|node| {
            node.plan
                .schema()
                .is_some_and(|schema| schema.contains(column))
        })
        .ok_or_else(|| PlanError::internal("unknown column in join reorder"))
}

fn hinted_plan_matches(plan: &LogicalPlan, table: &tidb_ast::HintTable) -> bool {
    let Some(alias) = crate::plan_builder::from::extract_table_alias(plan.output_names()) else {
        return false;
    };
    let database_matches = table
        .db_name
        .as_deref()
        .is_none_or(|database| database == "*" || database.eq_ignore_ascii_case(&alias.db_name));
    let table_matches = table.name.eq_ignore_ascii_case(&alias.table_name);
    let query_block_matches = table.qb_name.as_deref().is_none_or(|query_block| {
        query_block
            .strip_prefix("sel_")
            .and_then(|offset| offset.parse::<i32>().ok())
            .filter(|offset| *offset > 0)
            .is_none_or(|offset| offset == plan.query_block_offset())
    });
    database_matches && table_matches && query_block_matches
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::data_source::DataSource;
    use crate::plan_base::PlanIdAllocator;
    use crate::stats_info::StatsInfo;
    use tidb_ast::CiString;
    use tidb_datatype::{
        Datum, FieldName, FieldNameMetadata, FieldType, FieldTypeCode, IdentifierMetadata,
    };
    use tidb_expr::constant::Constant;

    fn column(id: i64) -> Column {
        let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        column.id = id;
        column
    }

    fn field_name(table: &str) -> FieldName {
        FieldName::new(FieldNameMetadata {
            table: IdentifierMetadata::new(table),
            original_table: IdentifierMetadata::new(table),
            column: IdentifierMetadata::new("a"),
            original_column: IdentifierMetadata::new("a"),
            ..FieldNameMetadata::default()
        })
    }

    fn data_source(allocator: &PlanIdAllocator, id: i64, table: &str, rows: f64) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, DataSource::TYPE, 1);
        base.base.set_schema(Some(Schema::new(vec![column(id)])));
        base.base.set_output_names(vec![field_name(table)]);
        base.base
            .set_stats(Some(StatsInfo::new(rows, [(id, rows)])));
        let mut source = DataSource::new(base, id, table);
        source.table_stats = Some(StatsInfo::new(rows, [(id, rows)]));
        LogicalPlan::DataSource(source)
    }

    fn inner_join(
        context: &RuleContext<'_>,
        left: LogicalPlan,
        right: LogicalPlan,
        equality: Option<(i64, i64)>,
    ) -> LogicalPlan {
        let mut join =
            crate::joinorder::new_cartesian_join(context, LogicalJoinType::Inner, left, right)
                .unwrap();
        if let Some((left, right)) = equality {
            join.equal_conditions.push(ScalarFunction::new(
                CiString::new("eq"),
                FieldType::new(FieldTypeCode::Tiny),
                vec![
                    Expression::Column(column(left)),
                    Expression::Column(column(right)),
                ],
            ));
        }
        LogicalPlan::Join(join)
    }

    fn shape(plan: &LogicalPlan) -> String {
        match plan {
            LogicalPlan::DataSource(source) => source.table_name.clone(),
            LogicalPlan::Join(join) => {
                let [left, right] = join.base.children() else {
                    return "bad-join".to_owned();
                };
                format!("({},{})", shape(left), shape(right))
            }
            LogicalPlan::Projection(projection) => shape(&projection.base.children()[0]),
            LogicalPlan::Selection(selection) => shape(&selection.base.children()[0]),
            _ => plan.tp().to_owned(),
        }
    }

    #[test]
    fn legacy_dp_builds_the_pinned_bushy_tree_for_all_cartesian_components() {
        let allocator = PlanIdAllocator::new();
        let mut context = crate::logical::rule_tests::test_context(&allocator);
        context.advanced_join_reorder = false;
        context.join_reorder_threshold = 10;
        let a = data_source(&allocator, 1, "a", 100.0);
        let b = data_source(&allocator, 2, "b", 100.0);
        let c = data_source(&allocator, 3, "c", 100.0);
        let d = data_source(&allocator, 4, "d", 100.0);
        let ab = inner_join(&context, a, b, None);
        let abc = inner_join(&context, ab, c, None);
        let root = inner_join(&context, abc, d, None);

        let (optimized, changed) = JoinReOrderSolver.optimize(&context, root).unwrap();
        assert!(!changed);
        assert_eq!(shape(&optimized), "((a,b),(c,d))");
    }

    #[test]
    fn projection_inline_uses_real_leaf_local_expressions_and_rejects_cross_leaf_ones() {
        let allocator = PlanIdAllocator::new();
        let mut context = crate::logical::rule_tests::test_context(&allocator);
        context.join_reorder_through_proj = true;
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let child = inner_join(&context, left, right, Some((1, 2)));
        let mut base = BaseLogicalPlan::new(&allocator, LogicalProjection::TYPE, 1);
        base.base.set_schema(Some(Schema::new(vec![column(10)])));
        base.base.set_output_names(vec![field_name("dt")]);
        base.set_children(vec![child]);
        let mut projection = LogicalProjection::new(
            base,
            vec![Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![Expression::Column(column(1)), Expression::Column(column(1))],
            ))],
        );
        let result = try_inline_projection(&context, &mut projection).unwrap();
        assert_eq!(result.group.len(), 2);
        assert!(result.column_expressions.contains_key(&10));

        projection.exprs[0] = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Column(column(1)), Expression::Column(column(2))],
        ));
        assert!(try_inline_projection(&context, &mut projection).is_none());
    }

    #[test]
    fn conflicting_leading_hints_use_shared_owner_identity() {
        let first = Rc::new(crate::plan_builder::from::JoinHints::default());
        let same = Rc::clone(&first);
        assert!(check_and_generate_leading_hint(&[first.clone(), same])
            .0
            .is_some());
        let different = Rc::new(crate::plan_builder::from::JoinHints::default());
        let (hint, conflicting) = check_and_generate_leading_hint(&[first, different]);
        assert!(hint.is_none());
        assert!(conflicting);
    }

    #[test]
    fn legacy_injection_reuses_only_deterministic_equivalent_expressions() {
        let allocator = PlanIdAllocator::new();
        let context = crate::logical::rule_tests::test_context(&allocator);
        let mut solver = LegacyGroupSolver::new(&context, BasicJoinGroupInfo::default(), true);
        let plan = data_source(&allocator, 1, "t", 1.0);
        let deterministic = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![
                Expression::Column(column(1)),
                Expression::Constant(Constant::new(
                    Datum::Int(1),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        ));
        let (plan, first) = solver
            .inject_expression(plan, deterministic.clone())
            .unwrap();
        let (plan, second) = solver.inject_expression(plan, deterministic).unwrap();
        assert_eq!(first.unique_id, second.unique_id);
        let LogicalPlan::Projection(projection) = &plan else {
            panic!("injection must build a projection")
        };
        assert_eq!(projection.exprs.len(), 2);

        let random = || {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                FieldType::new(FieldTypeCode::Double),
                vec![
                    Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new("rand"),
                        FieldType::new(FieldTypeCode::Double),
                        Vec::new(),
                    )),
                    Expression::Column(column(1)),
                ],
            ))
        };
        let (plan, first_random) = solver.inject_expression(plan, random()).unwrap();
        let (plan, second_random) = solver.inject_expression(plan, random()).unwrap();
        assert_ne!(first_random.unique_id, second_random.unique_id);
        let LogicalPlan::Projection(projection) = plan else {
            panic!("injection must keep the projection")
        };
        assert_eq!(projection.exprs.len(), 4);
    }
}
