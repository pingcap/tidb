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

//! Go `pkg/planner/core/rule/rule_join_key_type_cast.go`.
//!
//! The rule recognizes the pair of `CAST(column AS DOUBLE)` projections that
//! implicit comparison coercion placed under a join. For signed non-BIGINT
//! integer versus string equality it publishes integer-valued projection
//! columns instead and adds Go's round-trip guard below the string projection.
//! This makes the physical index-join ranger see the original integer key
//! without changing the result for strings such as `"1.5"`.

use crate::find_best_task::LogicalJoinType;
use crate::logical::projection::LogicalProjection;
use crate::logical::rule::{LogicalOptRule, RuleContext};
use crate::logical::{BaseLogicalPlan, LogicalPlan, LogicalSelection};
use crate::plan_base::PlanError;
use tidb_datatype::{EvalType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::aggregation::wrap_cast::{wrap_with_cast_as_int, wrap_with_cast_as_real};
use tidb_expr::column::Column;
use tidb_expr::expr_util::builder::tiny_int_type;
use tidb_expr::expression::Expression;

/// Go `JoinKeyTypeCastRewriter`.
#[derive(Debug)]
pub struct JoinKeyTypeCastRewriter;

#[derive(Clone)]
struct ProjectionCast {
    original: Column,
}

fn projection_cast(projection: &LogicalProjection, output: &Column) -> Option<ProjectionCast> {
    let schema = projection.base.base.schema()?;
    let offset = schema
        .columns
        .iter()
        .position(|column| column.unique_id == output.unique_id)?;
    let Expression::ScalarFunction(cast) = projection.exprs.get(offset)? else {
        return None;
    };
    if !matches!(
        cast.func_name.lowercase(),
        "cast" | "cast_real" | "cast_float" | "cast_double"
    ) || cast.ret_type.as_ref()?.eval_type() != EvalType::Real
        || cast.args.len() != 1
    {
        return None;
    }
    let Expression::Column(original) = &cast.args[0] else {
        return None;
    };
    Some(ProjectionCast {
        original: original.clone(),
    })
}

fn is_signed_non_bigint(column: &Column) -> bool {
    column.ret_type.as_ref().is_some_and(|field_type| {
        field_type.eval_type() == EvalType::Int
            && !field_type.has_flag(FieldTypeFlags::UNSIGNED)
            && field_type.code() != FieldTypeCode::LongLong
    })
}

fn is_string(column: &Column) -> bool {
    column
        .ret_type
        .as_ref()
        .is_some_and(|field_type| field_type.eval_type().is_string_kind())
}

fn append_projection_expression(
    projection: &mut LogicalProjection,
    expression: Expression,
    column: Column,
) -> Result<(), PlanError> {
    let mut schema =
        projection.base.base.schema().cloned().ok_or_else(|| {
            PlanError::internal("join-key type-cast projection has no output schema")
        })?;
    projection.exprs.push(expression);
    schema.columns.push(column);
    projection.base.base.set_schema(Some(schema));
    Ok(())
}

fn install_guard(
    ctx: &RuleContext<'_>,
    projection: &mut LogicalProjection,
    conditions: Vec<Expression>,
    query_block_offset: i32,
) -> Result<(), PlanError> {
    if conditions.is_empty() {
        return Ok(());
    }
    let children = projection.base.take_children();
    let [child] = <Vec<LogicalPlan> as TryInto<[LogicalPlan; 1]>>::try_into(children).map_err(
        |children| {
            projection.base.set_children(children);
            PlanError::internal("join-key type-cast projection has the wrong child count")
        },
    )?;
    let mut selection = LogicalPlan::Selection(LogicalSelection::new(
        BaseLogicalPlan::new(ctx.allocator, "Selection", query_block_offset),
        conditions,
    ));
    selection.set_children(vec![child]);
    projection.base.set_children(vec![selection]);
    Ok(())
}

fn rewrite_join(ctx: &RuleContext<'_>, plan: &mut LogicalPlan) -> Result<bool, PlanError> {
    let LogicalPlan::Join(join) = plan else {
        return Ok(false);
    };
    if join.equal_conditions.is_empty() || join.base.children().len() != 2 {
        return Ok(false);
    }

    let preserved_child = match join.join_type {
        LogicalJoinType::LeftOuter
        | LogicalJoinType::AntiSemi
        | LogicalJoinType::LeftOuterSemi
        | LogicalJoinType::AntiLeftOuterSemi => Some(0),
        LogicalJoinType::RightOuter => Some(1),
        LogicalJoinType::Inner | LogicalJoinType::Semi => None,
    };
    let query_block_offset = join.base.base.query_block_offset();
    let (left, right) = join.base.children_mut().split_at_mut(1);
    let (LogicalPlan::Projection(left), LogicalPlan::Projection(right)) =
        (&mut left[0], &mut right[0])
    else {
        return Ok(false);
    };

    let mut left_guards = Vec::new();
    let mut right_guards = Vec::new();
    let mut changed = false;
    for equality in &mut join.equal_conditions {
        if equality.func_name.lowercase() == "nulleq" || equality.args.len() != 2 {
            continue;
        }
        let (Expression::Column(left_key), Expression::Column(right_key)) =
            (&equality.args[0], &equality.args[1])
        else {
            continue;
        };
        if left_key
            .ret_type
            .as_ref()
            .is_none_or(|field_type| field_type.eval_type() != EvalType::Real)
            || right_key
                .ret_type
                .as_ref()
                .is_none_or(|field_type| field_type.eval_type() != EvalType::Real)
        {
            continue;
        }
        let Some(left_cast) = projection_cast(left, left_key) else {
            continue;
        };
        let Some(right_cast) = projection_cast(right, right_key) else {
            continue;
        };
        let (integer_side, integer_cast, string_side, string_cast) =
            if is_signed_non_bigint(&left_cast.original) && is_string(&right_cast.original) {
                (0, left_cast, 1, right_cast)
            } else if is_string(&left_cast.original) && is_signed_non_bigint(&right_cast.original) {
                (1, right_cast, 0, left_cast)
            } else {
                continue;
            };
        if preserved_child == Some(string_side) {
            continue;
        }

        let integer_type =
            integer_cast.original.ret_type.clone().ok_or_else(|| {
                PlanError::internal("join-key type-cast integer column has no type")
            })?;
        let integer_output = Column::new(integer_cast.original.unique_id, integer_type.clone());
        let string_as_int = wrap_with_cast_as_int(
            Expression::Column(string_cast.original.clone()),
            Some(&integer_type),
        )
        .map_err(|error| PlanError::internal(format!("{error:?}")))?;
        let string_output = Column::new(
            ctx.column_allocator.alloc(),
            string_as_int
                .static_type()
                .cloned()
                .ok_or_else(|| PlanError::internal("join-key type-cast result has no type"))?,
        );
        let guard_left = wrap_with_cast_as_real(
            wrap_with_cast_as_int(
                Expression::Column(string_cast.original.clone()),
                Some(&integer_type),
            )
            .map_err(|error| PlanError::internal(format!("{error:?}")))?,
        )
        .map_err(|error| PlanError::internal(format!("{error:?}")))?;
        let guard_right = wrap_with_cast_as_real(Expression::Column(string_cast.original))
            .map_err(|error| PlanError::internal(format!("{error:?}")))?;
        let guard = ctx
            .builder
            .new_function("eq", Some(tiny_int_type()), vec![guard_left, guard_right])
            .map_err(|error| PlanError::internal(error.to_string()))?;
        let rewritten = ctx
            .builder
            .new_function(
                &equality.func_name.lowercase(),
                Some(tiny_int_type()),
                if integer_side == 0 {
                    vec![
                        Expression::Column(integer_output.clone()),
                        Expression::Column(string_output.clone()),
                    ]
                } else {
                    vec![
                        Expression::Column(string_output.clone()),
                        Expression::Column(integer_output.clone()),
                    ]
                },
            )
            .map_err(|error| PlanError::internal(error.to_string()))?;
        let Expression::ScalarFunction(rewritten) = rewritten else {
            return Err(PlanError::internal(
                "join-key type-cast equality did not build a scalar function",
            ));
        };

        if integer_side == 0 {
            append_projection_expression(
                left,
                Expression::Column(integer_cast.original),
                integer_output,
            )?;
            append_projection_expression(right, string_as_int, string_output)?;
        } else {
            append_projection_expression(
                right,
                Expression::Column(integer_cast.original),
                integer_output,
            )?;
            append_projection_expression(left, string_as_int, string_output)?;
        }
        if string_side == 0 {
            left_guards.push(guard);
        } else {
            right_guards.push(guard);
        }
        *equality = rewritten;
        changed = true;
    }

    install_guard(ctx, left, left_guards, query_block_offset)?;
    install_guard(ctx, right, right_guards, query_block_offset)?;
    Ok(changed)
}

fn rewrite_tree(ctx: &RuleContext<'_>, plan: &mut LogicalPlan) -> Result<bool, PlanError> {
    let mut changed = false;
    for child in plan.base_mut().children_mut() {
        changed |= rewrite_tree(ctx, child)?;
    }
    changed |= rewrite_join(ctx, plan)?;
    Ok(changed)
}

impl LogicalOptRule for JoinKeyTypeCastRewriter {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        mut plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        match rewrite_tree(ctx, &mut plan) {
            Ok(changed) => Ok((plan, changed)),
            Err(error) => Err((plan, error)),
        }
    }

    fn name(&self) -> &'static str {
        "join_key_type_cast"
    }
}
