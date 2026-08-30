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

//! Go `pkg/planner/core/rule/rule_outer_join_to_semi_join.go`.

use std::collections::BTreeSet;

use tidb_datatype::{Datum, FieldTypeFlags};
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::extract::is_col_op_col;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::find_best_task::LogicalJoinType;
use crate::logical::{BaseLogicalPlan, LogicalJoin, LogicalPlan, LogicalProjection};
use crate::plan_base::PlanError;

use super::rule::{LogicalOptRule, RuleContext};

/// Go `OuterJoinToSemiJoin`.
#[derive(Debug)]
pub struct OuterJoinToSemiJoin;

impl LogicalOptRule for OuterJoinToSemiJoin {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        match rewrite(ctx, plan) {
            Ok(result) => Ok(result),
            Err((plan, error)) => Err((plan, error)),
        }
    }

    fn name(&self) -> &'static str {
        "outer_join_to_semi_join"
    }
}

#[allow(clippy::result_large_err)]
fn rewrite(
    ctx: &RuleContext<'_>,
    mut plan: LogicalPlan,
) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
    if matches!(plan, LogicalPlan::Selection(_)) {
        return rewrite_selection(ctx, plan);
    }
    let children = std::mem::take(plan.base_mut().children_mut());
    let mut rewritten = Vec::with_capacity(children.len());
    let mut changed = false;
    for child in children {
        match rewrite(ctx, child) {
            Ok((child, child_changed)) => {
                rewritten.push(child);
                changed |= child_changed;
            }
            Err((child, error)) => {
                rewritten.push(child);
                plan.set_children(rewritten);
                return Err((plan, error));
            }
        }
    }
    plan.set_children(rewritten);
    Ok((plan, changed))
}

#[allow(clippy::result_large_err)]
fn rewrite_selection(
    ctx: &RuleContext<'_>,
    plan: LogicalPlan,
) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
    let LogicalPlan::Selection(mut selection) = plan else {
        unreachable!()
    };
    let parent_schema = selection.base.base.schema().cloned().or_else(|| {
        selection
            .base
            .children()
            .first()
            .and_then(LogicalPlan::schema)
            .cloned()
    });
    let parent_names = if selection.base.base.output_names().is_empty() {
        selection
            .base
            .children()
            .first()
            .map_or_else(Vec::new, |child| child.output_names().to_vec())
    } else {
        selection.base.base.output_names().to_vec()
    };
    let Some(child) = selection.base.children_mut().pop() else {
        return Ok((LogicalPlan::Selection(selection), false));
    };

    match child {
        LogicalPlan::Join(join) => {
            match convert(
                ctx,
                join.clone(),
                &selection.conditions,
                parent_schema.as_ref(),
                parent_names,
            ) {
                Ok(Some(converted)) => {
                    rewrite(ctx, converted).map(|(plan, changed)| (plan, true | changed))
                }
                Ok(None) => {
                    selection.base.set_children(vec![LogicalPlan::Join(join)]);
                    let child = selection.base.children_mut().pop().unwrap();
                    match rewrite(ctx, child) {
                        Ok((child, changed)) => {
                            selection.base.set_children(vec![child]);
                            Ok((LogicalPlan::Selection(selection), changed))
                        }
                        Err((child, error)) => {
                            selection.base.set_children(vec![child]);
                            Err((LogicalPlan::Selection(selection), error))
                        }
                    }
                }
                Err(error) => {
                    selection.base.set_children(vec![LogicalPlan::Join(join)]);
                    Err((LogicalPlan::Selection(selection), error))
                }
            }
        }
        LogicalPlan::Projection(mut projection) if valid_projection(&projection) => {
            let Some(projection_child) = projection.base.children_mut().pop() else {
                selection
                    .base
                    .set_children(vec![LogicalPlan::Projection(projection)]);
                return Ok((LogicalPlan::Selection(selection), false));
            };
            if let LogicalPlan::Join(join) = projection_child {
                match convert(
                    ctx,
                    join.clone(),
                    &selection.conditions,
                    parent_schema.as_ref(),
                    parent_names,
                ) {
                    Ok(Some(converted)) => {
                        rewrite(ctx, converted).map(|(plan, changed)| (plan, true | changed))
                    }
                    Ok(None) => {
                        projection.base.set_children(vec![LogicalPlan::Join(join)]);
                        selection
                            .base
                            .set_children(vec![LogicalPlan::Projection(projection)]);
                        let child = selection.base.children_mut().pop().unwrap();
                        match rewrite(ctx, child) {
                            Ok((child, changed)) => {
                                selection.base.set_children(vec![child]);
                                Ok((LogicalPlan::Selection(selection), changed))
                            }
                            Err((child, error)) => {
                                selection.base.set_children(vec![child]);
                                Err((LogicalPlan::Selection(selection), error))
                            }
                        }
                    }
                    Err(error) => {
                        projection.base.set_children(vec![LogicalPlan::Join(join)]);
                        selection
                            .base
                            .set_children(vec![LogicalPlan::Projection(projection)]);
                        Err((LogicalPlan::Selection(selection), error))
                    }
                }
            } else {
                projection.base.set_children(vec![projection_child]);
                match rewrite(ctx, LogicalPlan::Projection(projection)) {
                    Ok((child, changed)) => {
                        selection.base.set_children(vec![child]);
                        Ok((LogicalPlan::Selection(selection), changed))
                    }
                    Err((child, error)) => {
                        selection.base.set_children(vec![child]);
                        Err((LogicalPlan::Selection(selection), error))
                    }
                }
            }
        }
        child => match rewrite(ctx, child) {
            Ok((child, changed)) => {
                selection.base.set_children(vec![child]);
                Ok((LogicalPlan::Selection(selection), changed))
            }
            Err((child, error)) => {
                selection.base.set_children(vec![child]);
                Err((LogicalPlan::Selection(selection), error))
            }
        },
    }
}

fn valid_projection(projection: &LogicalProjection) -> bool {
    let Some(schema) = projection.base.base.schema() else {
        return false;
    };
    schema.columns.len() == projection.exprs.len()
        && schema
            .columns
            .iter()
            .zip(&projection.exprs)
            .all(|(column, expression)| Expression::Column(column.clone()).equal(expression))
}

fn join_condition_null_rejects(
    join: &LogicalJoin,
    inner_ids: &BTreeSet<i64>,
    is_null_column: i64,
) -> bool {
    let rejects = |expression: &Expression| {
        let Expression::ScalarFunction(function) = expression else {
            return false;
        };
        let Some((left, right)) = is_col_op_col(function) else {
            return false;
        };
        (inner_ids.contains(&left.unique_id) && left.unique_id == is_null_column)
            || (inner_ids.contains(&right.unique_id) && right.unique_id == is_null_column)
    };
    join.equal_conditions
        .iter()
        .filter(|condition| condition.func_name.lowercase() != "nulleq")
        .any(|condition| rejects(&Expression::ScalarFunction(condition.clone())))
        || join.other_conditions.iter().any(|condition| {
            matches!(condition, Expression::ScalarFunction(function)
                if matches!(function.func_name.lowercase(), "gt" | "ge" | "le" | "lt" | "ne"))
                && rejects(condition)
        })
}

#[allow(clippy::result_large_err)]
fn convert(
    ctx: &RuleContext<'_>,
    mut join: LogicalJoin,
    selection_conditions: &[Expression],
    parent_schema: Option<&Schema>,
    parent_names: Vec<tidb_datatype::FieldName>,
) -> Result<Option<LogicalPlan>, PlanError> {
    if selection_conditions.len() != 1
        || (join.equal_conditions.is_empty() && join.other_conditions.is_empty())
    {
        return Ok(None);
    }
    let outer_index = match join.join_type {
        LogicalJoinType::LeftOuter => 0,
        LogicalJoinType::RightOuter => 1,
        _ => return Ok(None),
    };
    let Expression::ScalarFunction(is_null) = &selection_conditions[0] else {
        return Ok(None);
    };
    let [Expression::Column(is_null_column)] = is_null.get_args() else {
        return Ok(None);
    };
    if is_null.func_name.lowercase() != "isnull" {
        return Ok(None);
    }
    let Some(outer_schema) = join
        .base
        .children()
        .get(outer_index)
        .and_then(LogicalPlan::schema)
        .cloned()
    else {
        return Ok(None);
    };
    let inner_index = 1 ^ outer_index;
    let Some(inner_schema) = join
        .base
        .children()
        .get(inner_index)
        .and_then(LogicalPlan::schema)
        .cloned()
    else {
        return Ok(None);
    };
    let inner_ids = inner_schema
        .columns
        .iter()
        .map(|column| column.unique_id)
        .collect::<BTreeSet<_>>();
    let inner_non_null = inner_schema
        .retrieve_column(is_null_column)
        .and_then(|column| column.get_static_type())
        .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL));
    if !join_condition_null_rejects(&join, &inner_ids, is_null_column.unique_id) && !inner_non_null
    {
        return Ok(None);
    }

    if join.join_type == LogicalJoinType::RightOuter {
        for condition in &mut join.equal_conditions {
            let args = condition.args.clone();
            let expression = ctx
                .builder
                .new_function(
                    condition.func_name.lowercase(),
                    condition.ret_type.clone(),
                    vec![args[1].clone(), args[0].clone()],
                )
                .map_err(|error| PlanError::internal(error.to_string()))?;
            let Expression::ScalarFunction(rebuilt) = expression else {
                return Err(PlanError::internal(
                    "join equality did not rebuild as a scalar function",
                ));
            };
            *condition = rebuilt;
        }
        join.base.children_mut().swap(0, 1);
        std::mem::swap(&mut join.left_conditions, &mut join.right_conditions);
    }
    join.join_type = LogicalJoinType::AntiSemi;
    join.base.base.set_schema(Some(outer_schema.clone()));
    let outer_names = join
        .base
        .children()
        .first()
        .map_or_else(Vec::new, |child| child.output_names().to_vec());
    join.base.base.set_output_names(outer_names);
    let join = LogicalPlan::Join(join);

    let Some(parent_schema) = parent_schema else {
        return Ok(Some(join));
    };
    if parent_schema
        .columns
        .iter()
        .all(|column| outer_schema.contains(column))
    {
        return Ok(Some(join));
    }
    let expressions = parent_schema
        .columns
        .iter()
        .map(|column| {
            if inner_ids.contains(&column.unique_id) {
                let mut field_type = column.get_static_type().cloned().unwrap_or_else(|| {
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Tiny)
                });
                field_type.del_flags(FieldTypeFlags::NOT_NULL);
                Expression::Constant(Constant::new(Datum::Null, field_type))
            } else {
                Expression::Column(column.clone())
            }
        })
        .collect();
    let mut base = BaseLogicalPlan::new(
        ctx.allocator,
        LogicalProjection::TYPE,
        join.base().base.query_block_offset(),
    );
    base.base.set_schema(Some(parent_schema.clone()));
    base.base.set_output_names(parent_names);
    let mut projection = LogicalPlan::Projection(LogicalProjection::new(base, expressions));
    projection.set_children(vec![join]);
    Ok(Some(projection))
}
