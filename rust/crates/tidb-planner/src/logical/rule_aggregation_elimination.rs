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

//! Go `AggregationEliminator`
//! (`pkg/planner/core/rule_aggregation_elimination.go`), Go rule #6.
//!
//! An aggregation grouped by a strong unique key has at most one input row
//! per group. Go replaces that aggregation with a projection whose expressions
//! preserve each aggregate's one-row semantics. This rule must run after
//! `BuildKeySolver`: making the decision anywhere below the logical planner
//! loses keys carried through projections, joins, and derived tables and can
//! reverse later physical-join choices.

use crate::base_arms;
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanError;
use tidb_datatype::{Datum, FieldType, FieldTypeFlags};
use tidb_expr::aggregation::{names, AggFuncDesc, AggFunctionMode};
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::builder::tiny_int_type;
use tidb_expr::expression::Expression;
use tidb_expr::simple_expr::compose_dnf_condition;

use super::fold::{fold_owned, Descend, OwnedRewrite, RewriteFailure};
use super::projection::LogicalProjection;
use super::rule::{LogicalOptRule, RuleContext};
use super::{BaseLogicalPlan, LogicalAggregation, LogicalPlan};

fn covers_key(columns: &[tidb_expr::column::Column], key: &[tidb_expr::column::Column]) -> bool {
    key.iter().all(|key_column| {
        columns
            .iter()
            .any(|column| column.unique_id == key_column.unique_id)
    })
}

fn eliminate_distinct(aggregation: &mut LogicalAggregation) {
    let Some(child_schema) = aggregation
        .base
        .children()
        .first()
        .and_then(LogicalPlan::schema)
    else {
        return;
    };
    for function in &mut aggregation.agg_funcs {
        if !function.has_distinct {
            continue;
        }
        let Some(columns) = function
            .args()
            .iter()
            .map(|argument| argument.as_column().cloned())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        if child_schema
            .pk_or_uk
            .iter()
            .chain(&child_schema.nullable_uk)
            .any(|key| covers_key(&columns, key))
        {
            function.has_distinct = false;
        }
    }
}

fn wrap_cast(
    ctx: &RuleContext<'_>,
    argument: Expression,
    target: &FieldType,
) -> Result<Expression, PlanError> {
    if argument
        .static_type()
        .is_some_and(|source| source.equal(target))
    {
        return Ok(argument);
    }
    ctx.builder
        .build_cast(argument, Some(target.clone()), false)
        .map_err(|error| PlanError::internal(error.to_string()))
}

fn rewrite_count(
    ctx: &RuleContext<'_>,
    arguments: &[Expression],
    target: &FieldType,
) -> Result<Expression, PlanError> {
    let mut null_tests = Vec::with_capacity(arguments.len());
    for argument in arguments {
        if argument
            .static_type()
            .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL))
        {
            null_tests.push(Expression::Constant(Constant::new_zero()));
        } else {
            null_tests.push(
                ctx.builder
                    .new_function("isnull", Some(tiny_int_type()), vec![argument.clone()])
                    .map_err(|error| PlanError::internal(error.to_string()))?,
            );
        }
    }
    let any_null = compose_dnf_condition(null_tests)
        .unwrap_or_else(|| Expression::Constant(Constant::new_zero()));
    ctx.builder
        .new_function(
            "if",
            Some(target.clone()),
            vec![
                any_null,
                Expression::Constant(Constant::new_zero()),
                Expression::Constant(Constant::new_one()),
            ],
        )
        .map_err(|error| PlanError::internal(error.to_string()))
}

fn rewrite_bit_function(
    ctx: &RuleContext<'_>,
    function_name: &str,
    argument: Expression,
    target: &FieldType,
) -> Result<Expression, PlanError> {
    let signed = tidb_expr::aggregation::wrap_cast::wrap_with_cast_as_int(argument, None)
        .map_err(|error| PlanError::internal(format!("cannot build integer cast: {error:?}")))?;
    let cast = wrap_cast(ctx, signed, target)?;
    let fallback = if function_name == names::BIT_AND {
        Expression::Constant(Constant::new(Datum::UInt(u64::MAX), target.clone()))
    } else {
        Expression::Constant(Constant::new_zero())
    };
    ctx.builder
        .new_function("ifnull", Some(target.clone()), vec![cast, fallback])
        .map_err(|error| PlanError::internal(error.to_string()))
}

fn is_binary_literal(expression: &Expression) -> bool {
    matches!(expression, Expression::Constant(constant) if matches!(constant.value, Datum::BinaryLiteral(_)))
}

fn rewrite_aggregate(
    ctx: &RuleContext<'_>,
    function: &AggFuncDesc,
) -> Result<Option<Expression>, PlanError> {
    let Some(first_argument) = function.args().first().cloned() else {
        return Ok(None);
    };
    let expression = match function.name() {
        names::COUNT => {
            if function.mode == AggFunctionMode::Final
                && function.args().len() == 1
                && first_argument
                    .static_type()
                    .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL))
            {
                wrap_cast(ctx, first_argument, function.ret_type())?
            } else {
                rewrite_count(ctx, function.args(), function.ret_type())?
            }
        }
        names::MAX | names::MIN => {
            if is_binary_literal(&first_argument) {
                return Ok(None);
            }
            wrap_cast(ctx, first_argument, function.ret_type())?
        }
        names::SUM | names::SUM_INT | names::AVG | names::FIRST_ROW => {
            wrap_cast(ctx, first_argument, function.ret_type())?
        }
        names::BIT_AND | names::BIT_OR | names::BIT_XOR => {
            rewrite_bit_function(ctx, function.name(), first_argument, function.ret_type())?
        }
        // Go rejects GROUP_CONCAT before attempting a rewrite, because the
        // projection would bypass group_concat_max_len truncation.
        names::GROUP_CONCAT => return Ok(None),
        _ => return Ok(None),
    };
    Ok(Some(expression))
}

fn aggregation_projection(
    ctx: &RuleContext<'_>,
    aggregation: &LogicalAggregation,
) -> Result<Option<Vec<Expression>>, PlanError> {
    if aggregation
        .agg_funcs
        .iter()
        .any(|function| function.name() == names::GROUP_CONCAT)
    {
        return Ok(None);
    }
    let group_by_columns = aggregation.get_group_by_cols();
    let covered = aggregation
        .base
        .children()
        .first()
        .and_then(LogicalPlan::schema)
        .is_some_and(|schema| {
            schema
                .pk_or_uk
                .iter()
                .any(|key| covers_key(&group_by_columns, key))
        });
    if !covered {
        return Ok(None);
    }
    aggregation
        .agg_funcs
        .iter()
        .map(|function| rewrite_aggregate(ctx, function))
        .collect()
}

fn is_distinct_only_aggregation(aggregation: &LogicalAggregation) -> bool {
    !aggregation.group_by_items.is_empty()
        && aggregation.agg_funcs.iter().all(|function| {
            function.name() == names::FIRST_ROW
                && !function.has_distinct
                && function.order_by_items.is_empty()
                && function.args().len() == 1
        })
        && aggregation
            .base
            .children()
            .first()
            .is_some_and(|child| !crate::expression_rewriter::has_limit(child))
}

struct AggregationEliminate<'a, 'ctx> {
    ctx: &'a RuleContext<'ctx>,
    failure: RewriteFailure,
}

impl OwnedRewrite for AggregationEliminate<'_, '_> {
    type Down = ();
    type Up = bool;

    fn descend(&mut self, node: &mut LogicalPlan, (): ()) -> Descend<(), bool> {
        match node {
            base_arms![
                Selection,
                Projection,
                Join,
                Apply,
                Aggregation,
                Sort,
                Limit,
                TopN,
                UnionAll,
                PartitionUnionAll,
                Window,
                CTE,
                CTETable,
                MaxOneRow,
                Lock,
                Sequence,
                UnionScan,
                TiKVSingleGather,
                TableScan,
                IndexScan,
                DataSource,
                TableDual,
                Expand,
                MemTable,
                Show,
                ShowDDLJobs,
                Todo,
            ] => Descend::Children((0..node.children().len()).map(|_| ()).collect()),
        }
    }

    fn ascend(&mut self, mut node: LogicalPlan, child_ups: Vec<bool>) -> (LogicalPlan, bool) {
        let child_changed = child_ups.into_iter().any(|changed| changed);

        if let LogicalPlan::Apply(apply) = &mut node {
            let semi = matches!(
                apply.join.join_type,
                LogicalJoinType::Semi
                    | LogicalJoinType::AntiSemi
                    | LogicalJoinType::LeftOuterSemi
                    | LogicalJoinType::AntiLeftOuterSemi
            );
            if semi
                && apply
                    .base()
                    .children()
                    .get(1)
                    .is_some_and(|child| matches!(child, LogicalPlan::Aggregation(aggregation) if is_distinct_only_aggregation(aggregation)))
            {
                let mut children = apply.base_mut().take_children();
                let mut inner = children.remove(1);
                let replacement = inner
                    .base_mut()
                    .take_children()
                    .into_iter()
                    .next()
                    .unwrap_or(inner);
                children.push(replacement);
                apply.base_mut().set_children(children);
                return (node, true);
            }
        }

        let LogicalPlan::Aggregation(mut aggregation) = node else {
            return (node, child_changed);
        };
        eliminate_distinct(&mut aggregation);
        let expressions = match aggregation_projection(self.ctx, &aggregation) {
            Ok(Some(expressions)) => expressions,
            Ok(None) => return (LogicalPlan::Aggregation(aggregation), child_changed),
            Err(error) => {
                self.failure.record(error);
                return (LogicalPlan::Aggregation(aggregation), child_changed);
            }
        };

        let query_block_offset = aggregation.base.base.query_block_offset();
        let schema = aggregation.base.base.schema().cloned();
        let output_names = aggregation.base.base.output_names().to_vec();
        let children = aggregation.base.take_children();
        if children.len() != 1 {
            return (LogicalPlan::Aggregation(aggregation), child_changed);
        }
        let mut base = BaseLogicalPlan::new(
            self.ctx.allocator,
            LogicalProjection::TYPE,
            query_block_offset,
        );
        base.base.set_schema(schema);
        base.base.set_output_names(output_names);
        base.set_children(children);
        (
            LogicalPlan::Projection(LogicalProjection::new(base, expressions)),
            // Go returns the children's `planChanged` value here; replacing
            // the aggregation itself does not set it.
            child_changed,
        )
    }
}

/// Go `AggregationEliminator` (`rule_aggregation_elimination.go:276`).
#[derive(Debug)]
pub struct AggregationEliminator;

impl LogicalOptRule for AggregationEliminator {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let mut rewrite = AggregationEliminate {
            ctx,
            failure: RewriteFailure::default(),
        };
        let result = fold_owned(&mut rewrite, plan, ());
        match rewrite.failure.take() {
            Some(error) => Err((result.0, error)),
            None => Ok(result),
        }
    }

    fn name(&self) -> &'static str {
        "aggregation_eliminate"
    }
}
