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

//! Go `pkg/planner/core/rule_decorrelate.go`, Go rule #5
//! (`DecorrelateSolver`).
//!
//! The rule walks the logical tree top-down and rewrites a `LogicalApply`
//! into a `LogicalJoin` whenever the correlated inner plan can be lifted out.
//! The arms implemented here, in Go's order:
//!
//! * an uncorrelated apply becomes the embedded join directly;
//! * `NoDecorrelate` and every condition-carrying case fall through to the
//!   children walk;
//! * an inner `Selection` has its conditions decorrelated and attached as
//!   join conditions;
//! * an inner `MaxOneRow` over a max-one-row child is peeled;
//! * an inner `Sort` is dropped (it cannot change the subquery's result);
//! * an inner `Limit` is peeled for the semi/anti join family when the apply
//!   carries no conditions and the offset is zero.
//!
//! The aggregation pull-up and projection arms are documented narrowings; see
//! [`DecorrelateSolver::optimize`].

use std::collections::BTreeSet;

use tidb_expression_aggregation::names as agg_names;
use tidb_expression_aggregation::AggFuncDesc;
use tidb_expr::expr_util::substitute::{column_substitute_all, SubstituteOptions};
use tidb_expr::expression::Expression;
use tidb_expr::schema::{merge_schema, Schema};
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};
use tidb_exprctx::NoColumns;

use tidb_planner_core_base::LogicalJoinType;

use crate::logical::aggregation::LogicalAggregation;
use crate::logical::apply::LogicalApply;
use crate::logical::projection::LogicalProjection;
use crate::logical::plan_context::{LogicalOptRule, RuleContext};
use crate::logical::BaseLogicalPlan;
use crate::logical::{LogicalPlan};
use tidb_planner_core_base::{PlanError};

/// Which aggregation arm fired.
#[derive(Clone, Debug)]
enum PullUpAggregation {
    /// Go's `NoOptimize` tail: the apply is left correlated.
    NotFired,
    /// The whole aggregation moved above the apply; the caller optimizes the
    /// apply and makes it the aggregation's child.
    Above,
    /// The correlated equalities became join keys; the caller optimizes the
    /// apply and keeps the aggregation as its inner child. `default_values`
    /// is Go's `aggDefaultValueMap` over the aggregation's output columns (a
    /// COUNT is 0 over an empty group), and `having_conds` the join
    /// conditions Go moved off the join so a HAVING-filtered group answers
    /// NULL while an empty group answers the default.
    Equalities {
        default_values: Vec<(tidb_expr::column::Column, tidb_expr::constant::Constant)>,
        having_conds: Vec<Expression>,
    },
}

/// Go `DecorrelateSolver`.
pub struct DecorrelateSolver;

impl DecorrelateSolver {
    /// Go `(*DecorrelateSolver).optimize` (`rule_decorrelate.go:253`).
    fn optimize(
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
        group_by_column: &mut BTreeSet<i64>,
    ) -> Result<LogicalPlan, PlanError> {
        if let LogicalPlan::Aggregation(aggregation) = &plan {
            for item in &aggregation.group_by_items {
                for column in extract_columns(item) {
                    group_by_column.insert(column.unique_id);
                }
            }
        }

        let mut plan = plan;
        if let Some(pruned) = Self::prune_redundant_apply(ctx, &mut plan, group_by_column) {
            return Ok(pruned);
        }
        match plan {
            LogicalPlan::Apply(apply) => Self::optimize_apply(ctx, apply, group_by_column),
            plan => Self::optimize_children(ctx, plan, group_by_column),
        }
    }

    /// Go `pruneRedundantApply` (`rule_decorrelate.go:143`): a Selection
    /// whose conditions simplify to TRUE over a LEFT OUTER (SEMI) Apply needs
    /// neither -- `WHERE 1 = 1 OR EXISTS(...)` reads only the outer side. The
    /// Selection and the whole chain of directly stacked Applies give way to
    /// the bottom outer child, unless an enclosing GROUP BY reads a column the
    /// Applies produced. It runs before the NO_DECORRELATE check, as in Go.
    fn prune_redundant_apply(
        ctx: &RuleContext<'_>,
        plan: &mut LogicalPlan,
        group_by_column: &BTreeSet<i64>,
    ) -> Option<LogicalPlan> {
        let LogicalPlan::Selection(selection) = &*plan else {
            return None;
        };
        let Some(LogicalPlan::Apply(apply)) = plan.children().first() else {
            return None;
        };
        if !matches!(
            apply.join.join_type,
            LogicalJoinType::LeftOuter | LogicalJoinType::LeftOuterSemi
        ) || apply.is_lateral
        {
            return None;
        }
        let simplified = crate::logical::plan_context::apply_predicate_simplification(
            ctx,
            selection.conditions.clone(),
            true,
            None,
        );
        let true_selection = match simplified.as_slice() {
            [] => true,
            [only] => super::rule_predicate_simplification::is_true_predicate(ctx, only),
            _ => false,
        };
        if !true_selection {
            return None;
        }
        let top_apply = &plan.children()[0];
        let mut bottom = top_apply;
        while let LogicalPlan::Apply(_) = bottom {
            bottom = &bottom.children()[0];
        }
        if let (Some(apply_schema), Some(child_schema)) = (top_apply.schema(), bottom.schema()) {
            let contains = |schema: &Schema, id: i64| {
                schema.columns.iter().any(|column| column.unique_id == id)
            };
            if group_by_column
                .iter()
                .any(|&id| contains(apply_schema, id) && !contains(child_schema, id))
            {
                return None;
            }
        }
        let mut node = plan.base_mut().take_children().swap_remove(0);
        while let LogicalPlan::Apply(_) = node {
            node = node.base_mut().take_children().swap_remove(0);
        }
        Some(node)
    }

    /// Rewrites one Apply node.
    ///
    /// Keep this value-heavy transformation out of the recursive tree walker.
    /// Go recurses with interface pointers; Rust's `LogicalPlan` is a wide
    /// value enum, so combining every rewrite temporary with the recursion
    /// made each tree level reserve a large stack frame.
    fn optimize_apply(
        ctx: &RuleContext<'_>,
        mut apply: LogicalApply,
        group_by_column: &mut BTreeSet<i64>,
    ) -> Result<LogicalPlan, PlanError> {
        let outer_schema = apply
            .base()
            .children()
            .first()
            .and_then(LogicalPlan::schema)
            .cloned()
            .unwrap_or_default();
        let inner_schema = apply
            .base()
            .children()
            .get(1)
            .and_then(LogicalPlan::schema)
            .cloned()
            .unwrap_or_default();
        // Go `coreusage.ExtractCorColumnsBySchema4LogicalPlan(innerPlan,
        // outerSchema)`: the INNER TREE walk, which resolves the columns
        // this apply's own outer side supplies. `LogicalApply::
        // extract_correlated_cols` is the different question
        // `PruneColumns` asks (which columns reach FURTHER out).
        let mut children = apply.base_mut().take_children().into_iter();
        let outer = children.next();
        let mut inner = children.next();
        match (outer, inner.as_mut()) {
            (Some(outer), Some(inner_plan)) => {
                apply.cor_cols =
                    super::super::expression_rewriter::extract_cor_columns_by_schema_4_logical_plan(
                        inner_plan,
                        &outer_schema,
                    );
                let inner = inner.expect("the inner child was just matched");
                apply.base_mut().set_children(vec![outer, inner]);
            }
            (outer, _) => {
                let mut restored = Vec::new();
                if let Some(outer) = outer {
                    restored.push(outer);
                }
                if let Some(inner) = inner {
                    restored.push(inner);
                }
                apply.base_mut().set_children(restored);
            }
        }
        if apply.cor_cols.is_empty() {
            // Go: "If the inner plan is non-correlated, the apply will be
            // simplified to join." An apply-decorrelated anti-semi keeps the
            // source's `* cost.SelectionFactor` estimate (TPC-DS q16: the
            // NOT EXISTS anti-semi = 1537.74 = the semi's 1922.18 * 0.8 on go
            // master nightly); only OuterJoinToSemiJoin's LEFT-JOIN +
            // IS-NULL conversion inherits the outer join's unscaled stats.
            return Self::optimize_children(ctx, LogicalPlan::Join(apply.join), group_by_column);
        }
        if apply.no_decorrelate {
            return Self::optimize_children(ctx, LogicalPlan::Apply(apply), group_by_column);
        }
        let opts = SubstituteOptions::new(ctx.builder);
        let inner = apply.base().children()[1].clone();
        match inner {
            LogicalPlan::Selection(mut selection) => {
                let conditions: Vec<_> = selection
                    .conditions
                    .iter()
                    .map(|condition| condition.decorrelate(Some(&outer_schema)))
                    .collect();
                apply
                    .join
                    .attach_on_conds(&conditions, &outer_schema, &inner_schema, &opts);
                let Some(child) = selection.base.take_children().into_iter().next() else {
                    return Err(PlanError::internal("selection has no child"));
                };
                let outer = apply
                    .base_mut()
                    .take_children()
                    .into_iter()
                    .next()
                    .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                apply.base_mut().set_children(vec![outer, child]);
                return Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column);
            }
            LogicalPlan::MaxOneRow(max_one_row) => {
                let max_one_row_child = max_one_row
                    .base
                    .children()
                    .first()
                    .filter(|child| child.max_one_row())
                    .cloned();
                if let Some(child) = max_one_row_child {
                    let outer = apply
                        .base_mut()
                        .take_children()
                        .into_iter()
                        .next()
                        .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                    apply.base_mut().set_children(vec![outer, child]);
                    return Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column);
                }
            }
            LogicalPlan::Projection(projection) => {
                if let Some(wrapper) =
                    Self::pull_up_projection(ctx, &mut apply, &outer_schema, projection)?
                {
                    let optimized =
                        Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column)?;
                    return Ok(match wrapper {
                        Some(mut proj) => {
                            proj.base.set_children(vec![optimized]);
                            LogicalPlan::Projection(proj)
                        }
                        None => optimized,
                    });
                }
            }
            LogicalPlan::Sort(mut sort) => {
                let Some(child) = sort.base.take_children().into_iter().next() else {
                    return Err(PlanError::internal("sort has no child"));
                };
                let outer = apply
                    .base_mut()
                    .take_children()
                    .into_iter()
                    .next()
                    .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                apply.base_mut().set_children(vec![outer, child]);
                return Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column);
            }
            LogicalPlan::Limit(mut limit) => {
                let semi_family = matches!(
                    apply.join.join_type,
                    LogicalJoinType::Semi
                        | LogicalJoinType::LeftOuterSemi
                        | LogicalJoinType::AntiSemi
                        | LogicalJoinType::AntiLeftOuterSemi
                );
                let has_conditions = !apply.join.equal_conditions.is_empty()
                    || !apply.join.left_conditions.is_empty()
                    || !apply.join.right_conditions.is_empty()
                    || !apply.join.other_conditions.is_empty();
                if semi_family && !has_conditions && limit.offset == 0 {
                    let Some(child) = limit.base.take_children().into_iter().next() else {
                        return Err(PlanError::internal("limit has no child"));
                    };
                    let outer = apply
                        .base_mut()
                        .take_children()
                        .into_iter()
                        .next()
                        .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                    apply.base_mut().set_children(vec![outer, child]);
                    return Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column);
                }
            }
            LogicalPlan::Aggregation(mut aggregation) => {
                match Self::pull_up_aggregation(&mut apply, &outer_schema, &mut aggregation)? {
                    PullUpAggregation::NotFired => {}
                    PullUpAggregation::Above => {
                        let optimized =
                            Self::optimize(ctx, LogicalPlan::Apply(apply), group_by_column)?;
                        aggregation.base.set_children(vec![optimized]);
                        return Ok(LogicalPlan::Aggregation(aggregation));
                    }
                    PullUpAggregation::Equalities {
                        default_values,
                        having_conds,
                    } => {
                        let query_block_offset = apply.base().base.query_block_offset();
                        let outer = apply
                            .base_mut()
                            .take_children()
                            .into_iter()
                            .next()
                            .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
                        apply
                            .base_mut()
                            .set_children(vec![outer, LogicalPlan::Aggregation(aggregation)]);
                        if default_values.is_empty() {
                            return Self::optimize(
                                ctx,
                                LogicalPlan::Apply(apply),
                                group_by_column,
                            );
                        }
                        let apply_schema = apply.base().base.schema().cloned().unwrap_or_default();
                        let new_projection = |exprs: Vec<Expression>, schema: Schema| {
                            let base = crate::logical::BaseLogicalPlan::new(
                                ctx.allocator,
                                LogicalProjection::TYPE,
                                query_block_offset,
                            );
                            let mut projection =
                                LogicalPlan::Projection(LogicalProjection::new(base, exprs));
                            projection.base_mut().base.set_schema(Some(schema));
                            projection
                        };
                        let column_exprs = |schema: &Schema| -> Vec<Expression> {
                            schema.columns.iter().cloned().map(Expression::Column).collect()
                        };
                        let ifnull_of = |column: &tidb_expr::column::Column,
                                         value: &tidb_expr::constant::Constant|
                         -> Result<Expression, PlanError> {
                            ctx.builder
                                .new_function(
                                    "ifnull",
                                    column.ret_type.clone(),
                                    vec![
                                        Expression::Column(column.clone()),
                                        Expression::Constant(value.clone()),
                                    ],
                                )
                                .map_err(|error| PlanError::internal(error.to_string()))
                        };
                        let plan = if having_conds.is_empty() {
                            // Go :494: `ifnull(agg, default)` for each
                            // defaulted output over the apply's own schema.
                            let mut exprs = column_exprs(&apply_schema);
                            for (column, value) in &default_values {
                                let position = apply_schema.column_index(column);
                                if position >= 0 {
                                    exprs[position as usize] = ifnull_of(column, value)?;
                                }
                            }
                            let mut projection = new_projection(exprs, apply_schema.clone());
                            projection
                                .base_mut()
                                .set_children(vec![LogicalPlan::Apply(apply)]);
                            projection
                        } else {
                            // Go :510: materialise the HAVING condition once,
                            // then NULL every inner column when it is false.
                            let mut default_exprs = column_exprs(&apply_schema);
                            let mut default_schema = apply_schema.clone();
                            let mut value_schema = Schema::default();
                            let mut value_exprs = Vec::with_capacity(default_values.len());
                            for (column, value) in &default_values {
                                let position = apply_schema.column_index(column);
                                let folded = ifnull_of(column, value)?;
                                if position >= 0 {
                                    default_exprs[position as usize] = folded.clone();
                                }
                                value_schema.append(std::iter::once(column.clone()));
                                value_exprs.push(folded);
                            }
                            let opts = SubstituteOptions::new(ctx.builder);
                            let having_items: Vec<Expression> = having_conds
                                .iter()
                                .map(|condition| {
                                    tidb_expr::expr_util::substitute::column_substitute(
                                        condition,
                                        &value_schema,
                                        &value_exprs,
                                        &opts,
                                    )
                                })
                                .collect();
                            let having_expr =
                                tidb_expr::simple_expr::compose_cnf_condition(having_items)
                                    .ok_or_else(|| PlanError::internal("empty HAVING"))?;
                            let mut having_col = tidb_expr::column::Column::new(
                                ctx.column_allocator.alloc(),
                                having_expr
                                    .static_type()
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        tidb_datatype::FieldType::new(
                                            tidb_datatype::FieldTypeCode::LongLong,
                                        )
                                    }),
                            );
                            having_col.index = default_schema.len() as i64;
                            default_exprs.push(having_expr);
                            default_schema.append(std::iter::once(having_col.clone()));
                            let mut default_projection =
                                new_projection(default_exprs, default_schema.clone());
                            default_projection
                                .base_mut()
                                .set_children(vec![LogicalPlan::Apply(apply)]);
                            let mut exprs = column_exprs(&Schema::new(
                                default_schema.columns[..apply_schema.len()].to_vec(),
                            ));
                            let outer_len = outer_schema.len();
                            for index in outer_len..apply_schema.len() {
                                let mut ret_type = apply_schema.columns[index]
                                    .ret_type
                                    .clone()
                                    .unwrap_or_else(|| {
                                        tidb_datatype::FieldType::new(
                                            tidb_datatype::FieldTypeCode::LongLong,
                                        )
                                    });
                                ret_type.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                                let null_value = Expression::Constant(
                                    tidb_expr::constant::Constant::new(
                                        tidb_datatype::Datum::Null,
                                        ret_type.clone(),
                                    ),
                                );
                                exprs[index] = ctx
                                    .builder
                                    .new_function(
                                        "if",
                                        Some(ret_type),
                                        vec![
                                            Expression::Column(having_col.clone()),
                                            exprs[index].clone(),
                                            null_value,
                                        ],
                                    )
                                    .map_err(|error| PlanError::internal(error.to_string()))?;
                            }
                            let mut projection = new_projection(exprs, apply_schema.clone());
                            projection.base_mut().set_children(vec![default_projection]);
                            projection
                        };
                        return Self::optimize(ctx, plan, group_by_column);
                    }
                }
            }
            _ => {}
        }
        Self::optimize_children(ctx, LogicalPlan::Apply(apply), group_by_column)
    }

    /// Go `DecorrelateSolver.optimize`'s aggregation arm
    /// (`rule_decorrelate.go:441`), in Go's own two-step order:
    ///
    /// 1. `apply.CanPullUpAgg() && agg.CanPullUp()` moves the WHOLE
    ///    aggregation above the apply, grouping it by the outer key and
    ///    carrying every outer column through `firstrow()`;
    /// 2. otherwise, the equalities in the aggregation's child `Selection`
    ///    that contain this apply's correlated column become join keys, and
    ///    their INNER column is appended to the grouping (with a `firstrow()`
    ///    carrier when the aggregation does not already output it).
    ///
    /// Both leave the apply uncorrelated, so the recursion converts it to a
    /// join.
    ///
    /// # Narrowing
    ///
    /// Go's `aggDefaultValueMap` arm (a scalar `COUNT`/`BIT_AND`/`BIT_OR`/
    /// `BIT_XOR` aggregation) needs the `ifnull`/HAVING projections and is not
    /// ported, so step 2 is skipped when a default value would apply. The
    /// apply simply stays correlated, which is a valid plan.
    fn pull_up_aggregation(
        apply: &mut LogicalApply,
        outer_schema: &Schema,
        aggregation: &mut LogicalAggregation,
    ) -> Result<PullUpAggregation, PlanError> {
        // Go's first branch: the aggregation is ungrouped and every argument
        // becomes NULL over a NULL child row, and the apply has no conditions
        // while the outer side has a key.
        let child_schema = aggregation
            .base
            .children()
            .first()
            .and_then(LogicalPlan::schema)
            .cloned()
            .unwrap_or_default();
        if apply.can_pull_up_agg(outer_schema) && aggregation.can_pull_up(&child_schema) {
            let Some(agg_child) = aggregation.base.children().first().cloned() else {
                return Ok(PullUpAggregation::NotFired);
            };
            // Go's `agg.SetSchema(apply.Schema())` reads the apply's schema
            // BEFORE `apply.SetSchema(applySchema)`, which is
            // `MergeSchema(outer, aggregation)` — the outer columns followed
            // by the aggregation's own outputs. Rebuild it from the children
            // instead of the stored schema, because column pruning can leave
            // the stored one stale relative to the aggregation.
            let aggregation_schema = aggregation.base.base.schema().cloned().unwrap_or_default();
            let outer = apply
                .base_mut()
                .take_children()
                .into_iter()
                .next()
                .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
            apply
                .base_mut()
                .set_children(vec![outer, agg_child.clone()]);
            let key_columns = outer_schema.pk_or_uk.first().cloned().unwrap_or_default();
            aggregation.group_by_items = key_columns
                .iter()
                .cloned()
                .map(Expression::Column)
                .collect();
            let mut new_funcs =
                Vec::with_capacity(outer_schema.len() + aggregation.agg_funcs.len());
            let mut outer_columns = Vec::with_capacity(outer_schema.len());
            for column in &outer_schema.columns {
                let first_row = AggFuncDesc::new(
                    &NoColumns,
                    agg_names::FIRST_ROW,
                    vec![Expression::Column(column.clone())],
                    false,
                )
                .map_err(|error| PlanError::internal(error.to_string()))?;
                let mut carried = column.clone();
                carried.ret_type = Some(first_row.ret_type().clone());
                outer_columns.push(carried);
                new_funcs.push(first_row);
            }
            let outer_columns_for_schema = outer_columns.clone();
            let inner_schema = agg_child.schema().cloned().unwrap_or_default();
            let mut apply_schema =
                merge_schema(Some(&Schema::new(outer_columns)), Some(&inner_schema))
                    .ok_or_else(|| PlanError::internal("aggregation pull-up has no schema"))?;
            for column in aggregation.get_group_by_cols() {
                if !apply_schema.contains(&column) {
                    apply_schema.append(std::iter::once(column));
                }
            }
            // Go `util.ResetNotNullFlag(apply.Schema(), outerLen, end)`: the
            // NULL-extended inner side can always be NULL.
            for column in &mut apply_schema.columns[outer_schema.len()..] {
                if let Some(field_type) = column.ret_type.as_mut() {
                    let mut cloned = field_type.clone();
                    cloned.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                    *field_type = cloned;
                }
            }
            apply.base_mut().base.set_schema(Some(apply_schema.clone()));
            for func in &aggregation.agg_funcs.clone() {
                let mut args = Vec::with_capacity(func.args().len());
                for arg in func.args() {
                    match arg {
                        Expression::Column(column) => {
                            let index = apply_schema.column_index(column);
                            if index == -1 {
                                args.push(arg.clone());
                            } else {
                                args.push(Expression::Column(
                                    apply_schema.columns[index as usize].clone(),
                                ));
                            }
                        }
                        Expression::ScalarFunction(function) => {
                            let mut cloned = function.clone();
                            if let Some(field_type) = cloned.ret_type.as_mut() {
                                let mut stripped = field_type.clone();
                                stripped.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                                *field_type = stripped;
                            }
                            args.push(Expression::ScalarFunction(cloned));
                        }
                        _ => args.push(arg.clone()),
                    }
                }
                let descriptor = AggFuncDesc::new(&NoColumns, func.name(), args, func.has_distinct)
                    .map_err(|error| PlanError::internal(error.to_string()))?;
                new_funcs.push(descriptor);
            }
            aggregation.agg_funcs = new_funcs;
            let aggregation_output = merge_schema(
                Some(&Schema::new(outer_columns_for_schema)),
                Some(&aggregation_schema),
            )
            .ok_or_else(|| PlanError::internal("aggregation pull-up has no output schema"))?;
            aggregation.base.base.set_schema(Some(aggregation_output));
            return Ok(PullUpAggregation::Above);
        }

        if apply.join.join_type != LogicalJoinType::LeftOuter {
            return Ok(PullUpAggregation::NotFired);
        }

        let Some(LogicalPlan::Selection(mut selection)) =
            aggregation.base.children().first().cloned()
        else {
            return Ok(PullUpAggregation::NotFired);
        };
        let apply_schema = apply.base().base.schema().cloned().unwrap_or_default();
        let mut eq_cond_with_cor_col = Vec::new();
        let mut remained = Vec::new();
        for condition in &selection.conditions {
            match apply.de_cor_col_from_eq_expr(condition, &apply_schema) {
                Some(Expression::ScalarFunction(function)) => {
                    eq_cond_with_cor_col.push(function);
                }
                _ => remained.push(condition.clone()),
            }
        }
        if eq_cond_with_cor_col.is_empty() {
            return Ok(PullUpAggregation::NotFired);
        }
        let original = std::mem::replace(&mut selection.conditions, remained);
        aggregation
            .base
            .set_children(vec![LogicalPlan::Selection(selection)]);
        apply.cor_cols =
            super::super::expression_rewriter::extract_cor_columns_by_schema_4_logical_plan(
                &mut LogicalPlan::Aggregation(aggregation.clone()),
                outer_schema,
            );
        if !apply.cor_cols.is_empty() {
            // There is another correlated column this arm cannot pull up, so
            // Go restores the selection and leaves the apply alone.
            if let Some(LogicalPlan::Selection(mut restored)) =
                aggregation.base.children().first().cloned()
            {
                restored.conditions = original;
                aggregation
                    .base
                    .set_children(vec![LogicalPlan::Selection(restored)]);
            }
            apply.cor_cols =
                super::super::expression_rewriter::extract_cor_columns_by_schema_4_logical_plan(
                    &mut LogicalPlan::Aggregation(aggregation.clone()),
                    outer_schema,
                );
            return Ok(PullUpAggregation::NotFired);
        }
        // Go `aggDefaultValueMap` (`rule_decorrelate.go:122`): COUNT, BIT_OR
        // and BIT_XOR answer 0, BIT_AND all ones, over an empty input.
        let agg_schema = aggregation.base.base.schema().cloned().unwrap_or_default();
        let mut default_values = Vec::new();
        for (index, func) in aggregation.agg_funcs.iter().enumerate() {
            let default = match func.name().to_ascii_lowercase().as_str() {
                "bit_or" | "bit_xor" | "count" => {
                    tidb_expr::constant::Constant::new_zero()
                }
                "bit_and" => tidb_expr::constant::Constant::new(
                    tidb_datatype::Datum::UInt(u64::MAX),
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                        .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED),
                ),
                _ => continue,
            };
            if let Some(column) = agg_schema.columns.get(index) {
                default_values.push((column.clone(), default));
            }
        }
        // Conditions pulled up from above the aggregation (typically HAVING)
        // would make "no matching group" ambiguous once attached to the join:
        // an empty group takes the default, a group the HAVING filtered out
        // takes NULL. Go moves them into a projection instead.
        let mut having_conds = Vec::new();
        let join = &mut apply.join;
        if !default_values.is_empty()
            && !(join.equal_conditions.is_empty()
                && join.left_conditions.is_empty()
                && join.right_conditions.is_empty()
                && join.other_conditions.is_empty()
                && join.na_eq_conditions.is_empty())
        {
            having_conds.extend(
                std::mem::take(&mut join.equal_conditions)
                    .into_iter()
                    .map(Expression::ScalarFunction),
            );
            having_conds.extend(
                std::mem::take(&mut join.na_eq_conditions)
                    .into_iter()
                    .map(Expression::ScalarFunction),
            );
            having_conds.append(&mut join.left_conditions);
            having_conds.append(&mut join.right_conditions);
            having_conds.append(&mut join.other_conditions);
        }
        let mut group_by_cols = Schema::new(aggregation.get_group_by_cols());
        let mut schema = aggregation.base.base.schema().cloned().unwrap_or_default();
        for condition in &eq_cond_with_cor_col {
            // Go takes the INNER column from the equal condition's right
            // argument, which `DeCorColFromEqExpr` normalised.
            let Some(Expression::Column(inner_column)) = condition.get_args().get(1).cloned()
            else {
                continue;
            };
            if !schema.contains(&inner_column) {
                let first_row = AggFuncDesc::new(
                    &NoColumns,
                    agg_names::FIRST_ROW,
                    vec![Expression::Column(inner_column.clone())],
                    false,
                )
                .map_err(|error| PlanError::internal(error.to_string()))?;
                let mut carried = inner_column.clone();
                carried.ret_type = Some(first_row.ret_type().clone());
                aggregation.agg_funcs.push(first_row);
                schema.append(std::iter::once(carried));
            }
            if !group_by_cols.contains(&inner_column) {
                aggregation
                    .group_by_items
                    .push(Expression::Column(inner_column.clone()));
                group_by_cols.append(std::iter::once(inner_column));
            }
        }
        apply.join.equal_conditions.extend(eq_cond_with_cor_col);
        aggregation.base.base.set_schema(Some(schema));
        // Go: "The selection may be useless, check and remove it."
        if let Some(LogicalPlan::Selection(selection)) = aggregation.base.children().first() {
            if selection.conditions.is_empty() {
                let child = selection.base.children().first().cloned();
                if let Some(child) = child {
                    aggregation.base.set_children(vec![child]);
                }
            }
        }
        Ok(PullUpAggregation::Equalities {
            default_values,
            having_conds,
        })
    }

    /// Go `DecorrelateSolver.optimize`'s projection arm
    /// (`rule_decorrelate.go:314`): substitute the projection's outputs into
    /// the apply's join conditions, decorrelate both the projection's
    /// expressions and the conditions, and drop the projection between the
    /// apply and its child. For a non-semi apply Go then re-attaches the
    /// projection ABOVE the optimized apply, with the outer child's columns
    /// prepended.
    ///
    /// Returns the plan to optimize next plus the projection to re-attach, or
    /// `None` when Go would take its `NoOptimize` tail.
    fn pull_up_projection(
        ctx: &RuleContext<'_>,
        apply: &mut LogicalApply,
        outer_schema: &Schema,
        projection: LogicalProjection,
    ) -> Result<Option<Option<LogicalProjection>>, PlanError> {
        if apply.join.join_type == LogicalJoinType::LeftOuter
            && skip_decorrelate_projection_for_left_outer_apply(apply, &projection)
        {
            return Ok(None);
        }
        let proj_schema = projection.base.base.schema().cloned().unwrap_or_default();
        let opts = SubstituteOptions::new(ctx.builder);
        // Go `ColumnSubstituteAll`: all-or-nothing substitution of the
        // projection's outputs into every join condition. NAEQ conditions are
        // not touched, exactly as Go's own helper.
        let mut left_conditions = Vec::with_capacity(apply.join.left_conditions.len());
        for condition in &apply.join.left_conditions {
            let (failed, replaced) =
                column_substitute_all(condition, &proj_schema, &projection.exprs, &opts);
            if failed {
                return Ok(None);
            }
            left_conditions.push(replaced);
        }
        let mut right_conditions = Vec::with_capacity(apply.join.right_conditions.len());
        for condition in &apply.join.right_conditions {
            let (failed, replaced) =
                column_substitute_all(condition, &proj_schema, &projection.exprs, &opts);
            if failed {
                return Ok(None);
            }
            right_conditions.push(replaced);
        }
        let mut other_conditions = Vec::with_capacity(apply.join.other_conditions.len());
        for condition in &apply.join.other_conditions {
            let (failed, replaced) =
                column_substitute_all(condition, &proj_schema, &projection.exprs, &opts);
            if failed {
                return Ok(None);
            }
            other_conditions.push(replaced);
        }
        let mut equal_conditions = Vec::with_capacity(apply.join.equal_conditions.len());
        for condition in &apply.join.equal_conditions {
            let (failed, replaced) = column_substitute_all(
                &Expression::ScalarFunction(condition.clone()),
                &proj_schema,
                &projection.exprs,
                &opts,
            );
            if failed {
                return Ok(None);
            }
            let Expression::ScalarFunction(replaced) = replaced else {
                return Ok(None);
            };
            equal_conditions.push(replaced);
        }
        apply.join.left_conditions = left_conditions
            .iter()
            .map(|condition| condition.decorrelate(Some(outer_schema)))
            .collect();
        apply.join.right_conditions = right_conditions
            .iter()
            .map(|condition| condition.decorrelate(Some(outer_schema)))
            .collect();
        apply.join.other_conditions = other_conditions
            .iter()
            .map(|condition| condition.decorrelate(Some(outer_schema)))
            .collect();
        apply.join.equal_conditions = equal_conditions
            .iter()
            .map(|condition| {
                let decorrelated =
                    Expression::ScalarFunction(condition.clone()).decorrelate(Some(outer_schema));
                match decorrelated {
                    Expression::ScalarFunction(function) => function,
                    _ => condition.clone(),
                }
            })
            .collect();
        let decorrelated_exprs = projection
            .exprs
            .iter()
            .map(|expr| expr.decorrelate(Some(outer_schema)))
            .collect::<Vec<_>>();
        let Some(inner) = projection.base.children().first().cloned() else {
            return Ok(None);
        };
        let outer = apply
            .base_mut()
            .take_children()
            .into_iter()
            .next()
            .ok_or_else(|| PlanError::internal("apply has no outer child"))?;
        apply
            .base_mut()
            .set_children(vec![outer.clone(), inner.clone()]);
        let semi_family = matches!(
            apply.join.join_type,
            LogicalJoinType::Semi
                | LogicalJoinType::LeftOuterSemi
                | LogicalJoinType::AntiSemi
                | LogicalJoinType::AntiLeftOuterSemi
        );
        if semi_family {
            return Ok(Some(None));
        }
        let apply_schema = apply.base().base.schema().cloned().unwrap_or_default();
        let outer_schema_of_apply = outer.schema().cloned().unwrap_or_default();
        // Go appends `Column2Exprs(outerPlan.Schema().Columns)` to the
        // projection's own expressions and keeps `apply.Schema()` as the
        // output. Column pruning may already have removed outer columns from
        // `apply.Schema()`, so building one expression per OUTPUT column keeps
        // the two lists the same length: an outer column projects itself, and
        // each remaining (inner) output column takes the next decorrelated
        // expression in order.
        let mut wrapper_exprs = Vec::with_capacity(apply_schema.len());
        let mut inner_exprs = decorrelated_exprs.into_iter();
        for column in &apply_schema.columns {
            if outer_schema_of_apply.contains(column) {
                wrapper_exprs.push(Expression::Column(column.clone()));
            } else if let Some(expr) = inner_exprs.next() {
                wrapper_exprs.push(expr);
            } else {
                return Ok(None);
            }
        }
        if inner_exprs.next().is_some() {
            return Ok(None);
        }
        // Go reuses the inner `proj` itself as the node above the apply, so
        // no new plan id is drawn.
        let mut wrapper = LogicalProjection::new(
            BaseLogicalPlan::with_id(
                projection.base.base.id(),
                LogicalProjection::TYPE,
                apply.base().base.query_block_offset(),
            ),
            wrapper_exprs,
        );
        wrapper.base.base.set_schema(Some(apply_schema));
        apply
            .base_mut()
            .base
            .set_schema(merge_schema(outer.schema(), inner.schema()));
        Ok(Some(Some(wrapper)))
    }

    /// Go's `NoOptimize` tail: recurse into every child.
    fn optimize_children(
        ctx: &RuleContext<'_>,
        mut plan: LogicalPlan,
        group_by_column: &mut BTreeSet<i64>,
    ) -> Result<LogicalPlan, PlanError> {
        // CTE's logical optimization is independent.
        if matches!(plan, LogicalPlan::CTE(_)) {
            return Ok(plan);
        }
        let children = plan.base_mut().take_children();
        let mut new_children = Vec::with_capacity(children.len());
        for child in children {
            new_children.push(Self::optimize(ctx, child, group_by_column)?);
        }
        plan.set_children(new_children);
        Ok(plan)
    }
}

/// Go `skipDecorrelateProjectionForLeftOuterApply` (`rule_decorrelate.go:573`):
/// a projection over a left-outer apply must NOT be pulled up when every
/// output is a constant (the outer join would have to produce NULL instead),
/// or when every output reads only the OUTER side (the projection would then
/// be evaluated on unmatched rows and break the left-outer NULL extension).
fn skip_decorrelate_projection_for_left_outer_apply(
    apply: &LogicalApply,
    projection: &LogicalProjection,
) -> bool {
    let all_const = !projection.exprs.is_empty()
        && projection
            .exprs
            .iter()
            .all(|expr| extract_cor_columns(expr).is_empty() && extract_columns(expr).is_empty());
    if all_const {
        return true;
    }
    let outer_schema = apply
        .base()
        .children()
        .first()
        .and_then(LogicalPlan::schema)
        .cloned()
        .unwrap_or_default();
    // Go `outerPlan.Schema().ColumnsIndices(cols) != nil`: true when every
    // plain column of the expression is an outer one, vacuously so for an
    // expression with none (a correlated column only, as a lifted
    // `sum(outer.a)` reads), whose value the join cannot null-extend either.
    projection.exprs.iter().any(|expr| {
        extract_columns(expr)
            .iter()
            .all(|column| outer_schema.contains(column))
    })
}

impl LogicalOptRule for DecorrelateSolver {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let recovery = plan.clone();
        Self::optimize(ctx, plan, &mut BTreeSet::new())
            .map(|plan| (plan, false))
            .map_err(|error| (recovery, error))
    }

    fn name(&self) -> &'static str {
        "decorrelate"
    }
}
