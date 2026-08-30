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

//! Go `pkg/planner/core/rule_aggregation_skew_rewrite.go`.

use std::collections::HashSet;

use tidb_expr::aggregation::{names, AggFuncDesc, AggFunctionMode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::extract_columns;
use tidb_expr::NoColumns;

use super::rule::{LogicalOptRule, RuleContext};
use super::{BaseLogicalPlan, LogicalAggregation, LogicalPlan, LogicalProjection, PlanError};

/// Go `SkewDistinctAggRewriter`.
pub struct SkewDistinctAggRewriter;

fn is_qualified(function: &AggFuncDesc) -> bool {
    if function.mode != AggFunctionMode::Complete
        || !function.order_by_items.is_empty()
        || function.args().len() > 1
        || function
            .args()
            .iter()
            .any(|argument| !matches!(argument, Expression::Column(_) | Expression::Constant(_)))
    {
        return false;
    }

    match function.name() {
        names::FIRST_ROW | names::COUNT | names::SUM | names::MAX | names::MIN => true,
        names::AVG => function.has_distinct,
        names::BIT_AND | names::BIT_OR | names::BIT_XOR => false,
        _ => false,
    }
}

fn fresh_column(context: &RuleContext<'_>, field_type: tidb_datatype::FieldType) -> Column {
    Column::new(context.column_allocator.alloc(), field_type)
}

struct PreparedRewrite {
    original_top_schema: Schema,
    rewritten_top_schema: Schema,
    bottom_schema: Schema,
    top_functions: Vec<AggFuncDesc>,
    bottom_functions: Vec<AggFuncDesc>,
    bottom_group_by: Vec<Expression>,
    projection_expressions: Option<Vec<Expression>>,
}

fn prepare_rewrite(
    context: &RuleContext<'_>,
    aggregation: &LogicalAggregation,
) -> Option<PreparedRewrite> {
    if aggregation.group_by_items.is_empty() {
        return None;
    }

    let mut distinct_count = 0;
    let mut distinct_columns = Vec::new();
    for function in &aggregation.agg_funcs {
        if function.has_distinct {
            distinct_count += 1;
            distinct_columns.extend(function.args().iter().cloned());
        }
        if distinct_count > 1 || !is_qualified(function) {
            return None;
        }
    }
    if distinct_count != 1 {
        return None;
    }

    let mut bottom_group_by = aggregation.group_by_items.clone();
    bottom_group_by.extend(distinct_columns);

    let top_schema = aggregation.base.base.schema().cloned()?;
    if top_schema.columns.len() != aggregation.agg_funcs.len() {
        return None;
    }
    let mut rewritten_top_schema = top_schema.clone();
    let mut bottom_schema = Schema::new(Vec::with_capacity(top_schema.columns.len()));
    let mut top_functions = Vec::with_capacity(aggregation.agg_funcs.len());
    let mut bottom_functions = Vec::with_capacity(aggregation.agg_funcs.len());

    let mut group_columns = Vec::new();
    let mut first_row_columns = HashSet::new();
    for item in &aggregation.group_by_items {
        let columns = extract_columns(item);
        first_row_columns.extend(columns.iter().map(|column| column.unique_id));
        group_columns.extend(columns);
    }

    let mut count_indexes = Vec::new();
    for (index, function) in aggregation.agg_funcs.iter().enumerate() {
        let mut new_function = function.clone();
        if function.has_distinct {
            if function.args().len() != 1 {
                return None;
            }
            let argument = function.args()[0].clone();
            let first_row =
                AggFuncDesc::new(&NoColumns, names::FIRST_ROW, vec![argument.clone()], false)
                    .ok()?;
            let output = match argument {
                Expression::Column(column) => column,
                _ => fresh_column(context, first_row.ret_type().clone()),
            };
            bottom_functions.push(first_row);
            bottom_schema.columns.push(output);

            new_function.has_distinct = false;
            top_functions.push(new_function);
            continue;
        }

        let argument = new_function.args().first()?.clone();
        let argument_column = match argument {
            Expression::Column(column) => Some(column),
            _ => None,
        };
        bottom_functions.push(new_function.clone());

        let aggregate_column = if new_function.name() == names::FIRST_ROW {
            let column = argument_column?;
            first_row_columns.remove(&column.unique_id);
            column
        } else {
            fresh_column(context, new_function.ret_type().clone())
        };
        bottom_schema.columns.push(aggregate_column.clone());

        if new_function.name() == names::COUNT {
            count_indexes.push(index);
            let sum = AggFuncDesc::new(
                &NoColumns,
                names::SUM,
                vec![Expression::Column(aggregate_column)],
                false,
            )
            .ok()?;
            rewritten_top_schema.columns[index] = fresh_column(context, sum.ret_type().clone());
            top_functions.push(sum);
        } else {
            let mut top_function = function.clone();
            top_function.base.args = vec![Expression::Column(aggregate_column)];
            top_functions.push(top_function);
        }
    }

    for column in group_columns {
        if first_row_columns.contains(&column.unique_id) {
            let first_row = AggFuncDesc::new(
                &NoColumns,
                names::FIRST_ROW,
                vec![Expression::Column(column.clone())],
                false,
            )
            .ok()?;
            bottom_functions.push(first_row);
            bottom_schema.columns.push(column);
        }
    }

    let projection_expressions = if count_indexes.is_empty() {
        None
    } else {
        let mut expressions: Vec<Expression> = rewritten_top_schema
            .columns
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect();
        for index in count_indexes {
            let target = top_schema.columns[index].ret_type.clone()?;
            if !expressions[index]
                .static_type()
                .is_some_and(|source| source.equal(&target))
            {
                expressions[index] = context
                    .builder
                    .build_cast(expressions[index].clone(), Some(target), false)
                    .ok()?;
            }
        }
        Some(expressions)
    };

    Some(PreparedRewrite {
        original_top_schema: top_schema,
        rewritten_top_schema,
        bottom_schema,
        top_functions,
        bottom_functions,
        bottom_group_by,
        projection_expressions,
    })
}

fn commit_rewrite(
    context: &RuleContext<'_>,
    mut aggregation: LogicalAggregation,
    prepared: PreparedRewrite,
) -> LogicalPlan {
    let query_block_offset = aggregation.base.base.query_block_offset();
    let children = aggregation.base.take_children();
    let mut bottom = LogicalAggregation::new(
        BaseLogicalPlan::new(
            context.allocator,
            LogicalAggregation::TYPE,
            query_block_offset,
        ),
        prepared.bottom_functions,
        prepared.bottom_group_by,
    );
    bottom.prefer_agg_type = aggregation.prefer_agg_type;
    bottom.base.set_children(children);
    bottom.base.base.set_schema(Some(prepared.bottom_schema));

    let mut top = LogicalAggregation::new(
        BaseLogicalPlan::new(
            context.allocator,
            LogicalAggregation::TYPE,
            query_block_offset,
        ),
        prepared.top_functions,
        aggregation.group_by_items,
    );
    top.prefer_agg_to_cop = aggregation.prefer_agg_to_cop;
    top.base
        .set_children(vec![LogicalPlan::Aggregation(bottom)]);
    top.base
        .base
        .set_schema(Some(prepared.rewritten_top_schema));

    let Some(projection_expressions) = prepared.projection_expressions else {
        return LogicalPlan::Aggregation(top);
    };
    let mut projection = LogicalProjection::new(
        BaseLogicalPlan::new(
            context.allocator,
            LogicalProjection::TYPE,
            query_block_offset,
        ),
        projection_expressions,
    );
    projection
        .base
        .set_children(vec![LogicalPlan::Aggregation(top)]);
    projection
        .base
        .base
        .set_schema(Some(prepared.original_top_schema));
    LogicalPlan::Projection(projection)
}

fn rewrite(context: &RuleContext<'_>, mut plan: LogicalPlan) -> LogicalPlan {
    let children = plan
        .base_mut()
        .take_children()
        .into_iter()
        .map(|child| rewrite(context, child))
        .collect();
    plan.base_mut().set_children(children);
    match plan {
        LogicalPlan::Aggregation(aggregation) => match prepare_rewrite(context, &aggregation) {
            Some(prepared) => commit_rewrite(context, aggregation, prepared),
            None => LogicalPlan::Aggregation(aggregation),
        },
        other => other,
    }
}

impl LogicalOptRule for SkewDistinctAggRewriter {
    fn optimize(
        &self,
        context: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok((rewrite(context, plan), false))
    }

    fn name(&self) -> &'static str {
        "skew_distinct_agg_rewrite"
    }
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::constant::Constant;

    use crate::logical::rule::LogicalOptRule;
    use crate::logical::rule_tests::test_context;
    use crate::logical::{LogicalTableDual, PlanIdAllocator};

    use super::*;

    fn column(id: i64) -> Column {
        Column::new(id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn function(name: &str, argument: Expression, distinct: bool) -> AggFuncDesc {
        AggFuncDesc::new(&NoColumns, name, vec![argument], distinct).expect("aggregate descriptor")
    }

    fn aggregation(
        allocator: &PlanIdAllocator,
        functions: Vec<AggFuncDesc>,
        group_by: Vec<Expression>,
    ) -> LogicalPlan {
        let schema = Schema::new(
            functions
                .iter()
                .enumerate()
                .map(|(index, function)| {
                    Column::new(100 + index as i64, function.ret_type().clone())
                })
                .collect(),
        );
        let mut child_base = BaseLogicalPlan::new(allocator, LogicalTableDual::TYPE, 0);
        child_base
            .base
            .set_schema(Some(Schema::new(vec![column(1), column(2)])));
        let child = LogicalPlan::TableDual(LogicalTableDual::new(child_base, 1));
        let mut base = BaseLogicalPlan::new(allocator, LogicalAggregation::TYPE, 0);
        base.base.set_schema(Some(schema));
        let mut aggregation = LogicalAggregation::new(base, functions, group_by);
        aggregation.base.set_children(vec![child]);
        LogicalPlan::Aggregation(aggregation)
    }

    #[test]
    fn distinct_constant_rewrite_uses_a_real_bottom_schema_column() {
        let allocator = PlanIdAllocator::new();
        let context = test_context(&allocator);
        let plan = aggregation(
            &allocator,
            vec![function(
                names::COUNT,
                Expression::Constant(Constant::new_one()),
                true,
            )],
            vec![Expression::Column(column(2))],
        );
        let (plan, changed) = SkewDistinctAggRewriter
            .optimize(&context, plan)
            .expect("rewrite succeeds");
        assert!(!changed, "pinned Go keeps planChanged false");

        let LogicalPlan::Aggregation(top) = plan else {
            panic!("no ordinary COUNT means no cast Projection");
        };
        assert_eq!(top.agg_funcs[0].name(), names::COUNT);
        assert!(!top.agg_funcs[0].has_distinct);
        let [LogicalPlan::Aggregation(bottom)] = top.base.children() else {
            panic!("expected the bottom Aggregation");
        };
        assert_eq!(bottom.agg_funcs[0].name(), names::FIRST_ROW);
        assert!(matches!(
            bottom.agg_funcs[0].args(),
            [Expression::Constant(_)]
        ));
        assert_eq!(bottom.base.base.schema().map(Schema::len), Some(2));
        assert_eq!(bottom.agg_funcs[1].name(), names::FIRST_ROW);
        assert_eq!(bottom.group_by_items.len(), 2);
    }

    #[test]
    fn ordinary_count_is_sum_above_count_with_output_cast() {
        let allocator = PlanIdAllocator::new();
        let context = test_context(&allocator);
        let mut plan = aggregation(
            &allocator,
            vec![
                function(names::COUNT, Expression::Column(column(1)), false),
                function(names::COUNT, Expression::Column(column(2)), true),
            ],
            vec![Expression::Column(column(1))],
        );
        let LogicalPlan::Aggregation(aggregation) = &mut plan else {
            unreachable!();
        };
        aggregation.prefer_agg_type = 7;
        aggregation.prefer_agg_to_cop = true;

        let (plan, _) = SkewDistinctAggRewriter
            .optimize(&context, plan)
            .expect("rewrite succeeds");
        let LogicalPlan::Projection(projection) = plan else {
            panic!("the split ordinary COUNT needs an output cast Projection");
        };
        assert!(matches!(projection.exprs[0], Expression::ScalarFunction(_)));
        let [LogicalPlan::Aggregation(top)] = projection.base.children() else {
            panic!("expected the top Aggregation");
        };
        assert_eq!(
            top.agg_funcs
                .iter()
                .map(AggFuncDesc::name)
                .collect::<Vec<_>>(),
            vec![names::SUM, names::COUNT]
        );
        assert!(top.prefer_agg_to_cop);
        assert_eq!(top.prefer_agg_type, 0);
        let [LogicalPlan::Aggregation(bottom)] = top.base.children() else {
            panic!("expected the bottom Aggregation");
        };
        assert_eq!(
            bottom
                .agg_funcs
                .iter()
                .map(AggFuncDesc::name)
                .collect::<Vec<_>>(),
            vec![names::COUNT, names::FIRST_ROW, names::FIRST_ROW]
        );
        assert_eq!(bottom.prefer_agg_type, 7);
        assert!(!bottom.prefer_agg_to_cop);
    }

    #[test]
    fn inapplicable_aggregate_keeps_its_original_plan_identity() {
        let allocator = PlanIdAllocator::new();
        let context = test_context(&allocator);
        let plan = aggregation(
            &allocator,
            vec![function(names::COUNT, Expression::Column(column(1)), true)],
            Vec::new(),
        );
        let original_id = plan.id();
        let (plan, _) = SkewDistinctAggRewriter
            .optimize(&context, plan)
            .expect("refusal succeeds");
        assert_eq!(plan.id(), original_id);
        assert!(matches!(plan, LogicalPlan::Aggregation(_)));
    }
}
