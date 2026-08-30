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

//! Go `pkg/planner/core/rule_semi_join_rewrite.go` and
//! `logicalop.LogicalJoin.SemiJoinRewrite`.

use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode, BaseFuncDesc};
use tidb_expr::expr_util::substitute::SubstituteOptions;
use tidb_expr::expression::Expression;
use tidb_expr::schema::{merge_schema, Schema};

use crate::expression_rewriter::PREFER_REWRITE_SEMI_JOIN;
use crate::find_best_task::LogicalJoinType;
use crate::logical::aggregation::AGG_FUNC_FIRST_ROW;

use super::rule::{LogicalOptRule, RuleContext};
use super::{
    BaseLogicalPlan, LogicalAggregation, LogicalPlan, LogicalProjection, LogicalSelection,
    PlanError,
};

/// Go `SemiJoinRewriter`.
pub struct SemiJoinRewriter;

fn rewrite_join(context: &RuleContext<'_>, mut join: super::LogicalJoin) -> LogicalPlan {
    if !matches!(
        join.join_type,
        LogicalJoinType::Semi | LogicalJoinType::LeftOuterSemi
    ) || (join.prefer_join_type & PREFER_REWRITE_SEMI_JOIN == 0
        && !context.enable_semi_join_rewrite)
    {
        return LogicalPlan::Join(join);
    }

    // Go consumes this hint bit at this rule even when a later applicability
    // check refuses the rewrite.
    join.prefer_join_type &= !PREFER_REWRITE_SEMI_JOIN;
    if join.join_type == LogicalJoinType::LeftOuterSemi {
        if let Some(sink) = context.hint_warning_sink {
            sink.set_hint_warning("SEMI_JOIN_REWRITE() is inapplicable for LeftOuterSemiJoin.");
        }
        return LogicalPlan::Join(join);
    }
    if !join.left_conditions.is_empty() || !join.other_conditions.is_empty() {
        if let Some(sink) = context.hint_warning_sink {
            sink.set_hint_warning(
                "SEMI_JOIN_REWRITE() is inapplicable for SemiJoin with left conditions or other conditions.",
            );
        }
        return LogicalPlan::Join(join);
    }

    // Go's implementation indexes the right argument as a column below. Keep
    // the original join intact if an independently-constructed Rust plan does
    // not satisfy that planner invariant.
    let Some(inner_columns) = join
        .equal_conditions
        .iter()
        .map(|condition| match condition.get_args().get(1) {
            Some(Expression::Column(column)) => Some(column.clone()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
    else {
        return LogicalPlan::Join(join);
    };

    let mut children = join.base.take_children();
    if children.len() != 2 {
        join.base.set_children(children);
        return LogicalPlan::Join(join);
    }
    let right = children.pop().expect("two join children");
    let left = children.pop().expect("two join children");
    let left_schema = left.schema().cloned().unwrap_or_default();
    let right_offset = right.query_block_offset();

    let right = if join.right_conditions.is_empty() {
        right
    } else {
        let mut selection = LogicalSelection::new(
            BaseLogicalPlan::new(context.allocator, LogicalSelection::TYPE, right_offset),
            join.right_conditions.clone(),
        );
        selection.base.set_children(vec![right]);
        LogicalPlan::Selection(selection)
    };

    let mut agg_funcs = Vec::with_capacity(join.equal_conditions.len());
    let mut group_by_items = Vec::with_capacity(join.equal_conditions.len());
    let mut output_columns = Vec::with_capacity(join.equal_conditions.len());
    for inner_column in inner_columns {
        let expression = Expression::Column(inner_column.clone());
        let ret_type = inner_column.ret_type.clone().unwrap_or_else(|| {
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Unspecified)
        });
        agg_funcs.push(AggFuncDesc {
            base: BaseFuncDesc::from_parts(
                AGG_FUNC_FIRST_ROW.to_owned(),
                vec![expression.clone()],
                ret_type,
            ),
            mode: AggFunctionMode::Complete,
            has_distinct: false,
            order_by_items: Vec::new(),
            grouping_id: 0,
        });
        group_by_items.push(expression);
        output_columns.push(inner_column);
    }

    let mut aggregation = LogicalAggregation::new(
        BaseLogicalPlan::new(context.allocator, LogicalAggregation::TYPE, right_offset),
        agg_funcs,
        group_by_items,
    );
    aggregation.base.set_children(vec![right]);
    let mut aggregation_schema = Schema::new(output_columns);
    aggregation.build_self_key_info(&mut aggregation_schema);
    aggregation
        .base
        .base
        .set_schema(Some(aggregation_schema.clone()));

    let query_block_offset = join.base.base.query_block_offset();
    let mut inner_join = super::LogicalJoin::new(
        BaseLogicalPlan::new(
            context.allocator,
            super::LogicalJoin::TYPE,
            query_block_offset,
        ),
        LogicalJoinType::Inner,
    );
    inner_join.hint_info = join.hint_info;
    inner_join.internal_hint_info = join.internal_hint_info;
    inner_join.prefer_join_type = join.prefer_join_type;
    inner_join.prefer_join_order = join.prefer_join_order;
    inner_join.internal_prefer_join_order = join.internal_prefer_join_order;
    inner_join
        .base
        .set_children(vec![left, LogicalPlan::Aggregation(aggregation)]);
    let merged = merge_schema(Some(&left_schema), Some(&aggregation_schema)).unwrap_or_default();
    inner_join.base.base.set_schema(Some(merged));
    inner_join.attach_on_conds(
        &join
            .equal_conditions
            .into_iter()
            .map(Expression::ScalarFunction)
            .collect::<Vec<_>>(),
        &left_schema,
        &aggregation_schema,
        &SubstituteOptions::new(context.builder),
    );

    let projection_exprs = left_schema
        .columns
        .iter()
        .cloned()
        .map(Expression::Column)
        .collect();
    let mut projection = LogicalProjection::new(
        BaseLogicalPlan::new(
            context.allocator,
            LogicalProjection::TYPE,
            query_block_offset,
        ),
        projection_exprs,
    );
    projection
        .base
        .set_children(vec![LogicalPlan::Join(inner_join)]);
    projection.base.base.set_schema(Some(left_schema));
    LogicalPlan::Projection(projection)
}

fn rewrite(context: &RuleContext<'_>, mut plan: LogicalPlan) -> LogicalPlan {
    if matches!(plan, LogicalPlan::CTE(_)) {
        return plan;
    }
    let children = plan
        .base_mut()
        .take_children()
        .into_iter()
        .map(|child| rewrite(context, child))
        .collect();
    plan.base_mut().set_children(children);
    match plan {
        LogicalPlan::Join(join) => rewrite_join(context, join),
        other => other,
    }
}

impl LogicalOptRule for SemiJoinRewriter {
    fn optimize(
        &self,
        context: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        // Pinned Go keeps `planChanged` false.
        Ok((rewrite(context, plan), false))
    }

    fn name(&self) -> &'static str {
        "semi_join_rewrite"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::scalar_function::ScalarFunction;

    use crate::logical::rule::{HintWarningSink, LogicalOptRule};
    use crate::logical::rule_tests::test_context;
    use crate::logical::{LogicalJoin, LogicalTableDual, PlanIdAllocator};

    use super::*;

    fn column(id: i64) -> Column {
        let mut column = Column::default();
        column.id = id;
        column.unique_id = id;
        column.ret_type = Some(FieldType::new(FieldTypeCode::Long));
        column
    }

    fn schema(ids: &[i64]) -> Schema {
        Schema::new(ids.iter().copied().map(column).collect())
    }

    fn dual(allocator: &PlanIdAllocator, ids: &[i64]) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, LogicalTableDual::TYPE, 0);
        base.base.set_schema(Some(schema(ids)));
        LogicalPlan::TableDual(LogicalTableDual::new(base, 1))
    }

    fn equal(left: i64, right: i64) -> ScalarFunction {
        ScalarFunction::new(
            CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(column(left)),
                Expression::Column(column(right)),
            ],
        )
    }

    fn join(
        allocator: &PlanIdAllocator,
        join_type: LogicalJoinType,
        prefer_join_type: u32,
    ) -> LogicalPlan {
        let left = dual(allocator, &[1]);
        let right = dual(allocator, &[2]);
        let mut join = LogicalJoin::new(
            BaseLogicalPlan::new(allocator, LogicalJoin::TYPE, 0),
            join_type,
        );
        join.prefer_join_type = prefer_join_type;
        join.equal_conditions = vec![equal(1, 2)];
        join.base.set_children(vec![left, right]);
        join.base.base.set_schema(Some(schema(&[1])));
        LogicalPlan::Join(join)
    }

    #[test]
    fn hinted_semi_join_becomes_projection_over_inner_join_and_grouping_aggregation() {
        let allocator = PlanIdAllocator::new();
        let context = test_context(&allocator);
        let (plan, changed) = SemiJoinRewriter
            .optimize(
                &context,
                join(&allocator, LogicalJoinType::Semi, PREFER_REWRITE_SEMI_JOIN),
            )
            .expect("rewrite succeeds");
        assert!(!changed, "pinned Go keeps planChanged false");

        let LogicalPlan::Projection(projection) = plan else {
            panic!("expected the identity Projection");
        };
        assert!(matches!(
            projection.exprs.as_slice(),
            [Expression::Column(column)] if column.unique_id == 1
        ));
        let [LogicalPlan::Join(inner_join)] = projection.base.children() else {
            panic!("expected an inner Join below the Projection");
        };
        assert_eq!(inner_join.join_type, LogicalJoinType::Inner);
        assert_eq!(inner_join.prefer_join_type & PREFER_REWRITE_SEMI_JOIN, 0);
        let [_, LogicalPlan::Aggregation(aggregation)] = inner_join.base.children() else {
            panic!("expected a grouping Aggregation as the inner child");
        };
        assert!(matches!(
            aggregation.group_by_items.as_slice(),
            [Expression::Column(column)] if column.unique_id == 2
        ));
        assert_eq!(aggregation.agg_funcs.len(), 1);
        assert_eq!(aggregation.agg_funcs[0].base.name, AGG_FUNC_FIRST_ROW);
    }

    #[derive(Default)]
    struct Warnings(Mutex<Vec<String>>);

    impl HintWarningSink for Warnings {
        fn set_hint_warning(&self, message: &str) {
            self.0
                .lock()
                .expect("warning lock")
                .push(message.to_owned());
        }
    }

    #[test]
    fn left_outer_semi_refusal_consumes_hint_and_emits_go_warning() {
        let allocator = PlanIdAllocator::new();
        let warnings = Warnings::default();
        let mut context = test_context(&allocator);
        context.hint_warning_sink = Some(&warnings);
        let (plan, changed) = SemiJoinRewriter
            .optimize(
                &context,
                join(
                    &allocator,
                    LogicalJoinType::LeftOuterSemi,
                    PREFER_REWRITE_SEMI_JOIN,
                ),
            )
            .expect("rule refuses without error");
        assert!(!changed);
        let LogicalPlan::Join(join) = plan else {
            panic!("refusal must preserve the Join");
        };
        assert_eq!(join.join_type, LogicalJoinType::LeftOuterSemi);
        assert_eq!(join.prefer_join_type & PREFER_REWRITE_SEMI_JOIN, 0);
        assert_eq!(
            *warnings.0.lock().expect("warning lock"),
            vec!["SEMI_JOIN_REWRITE() is inapplicable for LeftOuterSemiJoin."]
        );
    }
}
