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

//! Go `pkg/planner/core/rule/rule_max_min_eliminate.go`.

use tidb_datatype::{FieldTypeCode, FieldTypeFlags};
use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expr_util::substitute::{build_not_null_expr, SubstituteOptions};
use tidb_expr::expression::Expression;
use tidb_expr::schema::{merge_schema, Schema};
use tidb_expr::simple_expr::extract_columns;

use super::aggregation::{AGG_FUNC_MAX, AGG_FUNC_MIN};
use super::rule::{LogicalOptRule, RuleContext};
use super::{
    BaseLogicalPlan, LogicalAggregation, LogicalJoin, LogicalLimit, LogicalPlan, LogicalSelection,
    LogicalSort,
};
use crate::access_path::{AccessPathStore, DataSourceAccessPath};
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanError;

fn check_column_can_use_index(
    plan: &LogicalPlan,
    column: &Column,
    mut conditions: Vec<Expression>,
) -> bool {
    match plan {
        LogicalPlan::Selection(selection) => {
            conditions.extend(selection.conditions.iter().cloned());
            selection
                .base
                .children()
                .first()
                .is_some_and(|child| check_column_can_use_index(child, column, conditions))
        }
        LogicalPlan::DataSource(source) => {
            source
                .all_possible_access_paths
                .iter()
                .any(|path| match path {
                    DataSourceAccessPath::Table(path) => {
                        if path.store() != AccessPathStore::TiKv {
                            return false;
                        }
                        if source.handle_is_int {
                            return source.handle_cols.first().is_some_and(|handle| {
                                handle.unique_id == column.unique_id
                                    && crate::ranger::detacher::detach_conds_for_column(
                                        &conditions,
                                        column,
                                        true,
                                    )
                                    .1
                                    .is_empty()
                            });
                        }
                        if source.common_handle_cols.is_empty() {
                            return false;
                        }
                        crate::ranger::detacher::detach_cond_and_build_range_for_index(
                            &conditions,
                            &source.common_handle_cols,
                            &source.common_handle_lens,
                            0,
                        )
                        .is_ok_and(|result| {
                            result.remained_conds.is_empty()
                                && (0..=result.eq_cond_count).any(|offset| {
                                    source.common_handle_cols.get(offset).is_some_and(
                                        |index_column| index_column.unique_id == column.unique_id,
                                    )
                                })
                        })
                    }
                    DataSourceAccessPath::Index(path) => {
                        let Some(index) = source
                            .indexes
                            .iter()
                            .find(|index| index.id == path.candidate().index_id)
                        else {
                            return false;
                        };
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
                            return false;
                        }
                        let columns = resolved
                            .iter()
                            .map(|(column, _)| column.clone())
                            .collect::<Vec<_>>();
                        let lengths = resolved
                            .iter()
                            .map(|(_, length)| *length)
                            .collect::<Vec<_>>();
                        crate::ranger::detacher::detach_cond_and_build_range_for_index(
                            &conditions,
                            &columns,
                            &lengths,
                            0,
                        )
                        .is_ok_and(|result| {
                            result.remained_conds.is_empty()
                                && (0..=result.eq_cond_count).any(|offset| {
                                    columns.get(offset).is_some_and(|index_column| {
                                        index_column.unique_id == column.unique_id
                                    })
                                })
                        })
                    }
                    DataSourceAccessPath::IndexMerge => false,
                })
        }
        _ => false,
    }
}

fn clone_subplan(ctx: &RuleContext<'_>, plan: &LogicalPlan) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::Selection(selection) => {
            // Go `cloneSubPlans` indexes `p.Children()[0]` unconditionally.
            let child = clone_subplan(ctx, &selection.base.children()[0])?;
            let mut cloned = LogicalPlan::Selection(LogicalSelection::new(
                BaseLogicalPlan::new(
                    ctx.allocator,
                    LogicalSelection::TYPE,
                    selection.base.base.query_block_offset(),
                ),
                selection.conditions.clone(),
            ));
            cloned.set_children(vec![child]);
            Some(cloned)
        }
        LogicalPlan::DataSource(source) => {
            let mut cloned = source.clone();
            let mut base = BaseLogicalPlan::new(
                ctx.allocator,
                source.base.base.tp(),
                source.base.base.query_block_offset(),
            );
            base.base.set_schema(source.base.base.schema().cloned());
            base.base
                .set_output_names(source.base.base.output_names().to_vec());
            cloned.base = base;
            Some(LogicalPlan::DataSource(cloned))
        }
        _ => None,
    }
}

fn eliminate_single(
    ctx: &RuleContext<'_>,
    mut aggregation: LogicalAggregation,
) -> Result<LogicalAggregation, PlanError> {
    // Go `eliminateSingleMaxMin` reads `agg.AggFuncs[0]`, `f.Args[0]`, and
    // `agg.Children()[0]`, so a malformed aggregation panics instead of
    // reporting an internal error.
    let function = &aggregation.agg_funcs[0];
    let argument = function.args()[0].clone();
    let mut child = aggregation
        .base
        .take_children()
        .into_iter()
        .next()
        .expect("max/min elimination requires an aggregation child");

    if !extract_columns(&argument).is_empty() {
        if !argument
            .static_type()
            .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL))
        {
            let predicate =
                build_not_null_expr(argument.clone(), &SubstituteOptions::new(ctx.builder))
                    .map_err(|error| {
                        PlanError::internal(format!(
                            "cannot build max/min not-null predicate: {error:?}"
                        ))
                    })?;
            let mut selection = LogicalPlan::Selection(LogicalSelection::new(
                BaseLogicalPlan::new(
                    ctx.allocator,
                    LogicalSelection::TYPE,
                    aggregation.base.base.query_block_offset(),
                ),
                vec![predicate],
            ));
            selection.set_children(vec![child]);
            child = selection;
        }
        let mut sort = LogicalPlan::Sort(LogicalSort::new(
            BaseLogicalPlan::new(
                ctx.allocator,
                LogicalSort::TYPE,
                aggregation.base.base.query_block_offset(),
            ),
            vec![ByItems::new(argument, function.name() == AGG_FUNC_MAX)],
        ));
        sort.set_children(vec![child]);
        child = sort;
    }

    let mut limit = LogicalPlan::Limit(LogicalLimit::new(
        BaseLogicalPlan::new(
            ctx.allocator,
            LogicalLimit::TYPE,
            aggregation.base.base.query_block_offset(),
        ),
        0,
        1,
    ));
    limit.set_children(vec![child]);
    aggregation.base.set_children(vec![limit]);
    Ok(aggregation)
}

fn split_aggregations(
    ctx: &RuleContext<'_>,
    aggregation: &LogicalAggregation,
) -> Option<Vec<LogicalAggregation>> {
    // Go `splitAggFuncAndCheckIndices` indexes `agg.Children()[0]`,
    // `f.Args[0]`, and `agg.Schema().Columns[i]` unconditionally.
    let child = &aggregation.base.children()[0];
    for function in &aggregation.agg_funcs {
        let Expression::Column(column) = &function.args()[0] else {
            return None;
        };
        if !check_column_can_use_index(child, column, Vec::new()) {
            return None;
        }
    }

    let schema = aggregation.base.base.schema()?;
    let mut split = Vec::with_capacity(aggregation.agg_funcs.len());
    for (offset, function) in aggregation.agg_funcs.iter().enumerate() {
        let output = schema.columns[offset].clone();
        let mut new_aggregation = LogicalPlan::Aggregation(LogicalAggregation::new(
            {
                let mut base = BaseLogicalPlan::new(
                    ctx.allocator,
                    LogicalAggregation::TYPE,
                    aggregation.base.base.query_block_offset(),
                );
                base.base
                    .set_schema(Some(Schema::new(vec![output.clone()])));
                base
            },
            vec![function.clone()],
            Vec::new(),
        ));
        new_aggregation.set_children(vec![clone_subplan(ctx, child)?]);
        let new_aggregation = new_aggregation.prune_columns(ctx, &[output]).ok()?;
        let LogicalPlan::Aggregation(new_aggregation) = new_aggregation else {
            return None;
        };
        split.push(new_aggregation);
    }
    Some(split)
}

fn compose_by_inner_join(
    ctx: &RuleContext<'_>,
    mut aggregations: Vec<LogicalAggregation>,
) -> Option<LogicalPlan> {
    let first = aggregations
        .first()
        .expect("compose requires at least one aggregation");
    let query_block_offset = first.base.base.query_block_offset();
    let mut plan = LogicalPlan::Aggregation(aggregations.remove(0));
    for aggregation in aggregations {
        let right = LogicalPlan::Aggregation(aggregation);
        let schema = merge_schema(plan.schema(), right.schema());
        let mut base = BaseLogicalPlan::new(ctx.allocator, LogicalJoin::TYPE, query_block_offset);
        base.base.set_schema(schema);
        let mut join = LogicalPlan::Join(LogicalJoin::new(base, LogicalJoinType::Inner));
        join.set_children(vec![plan, right]);
        plan = join;
    }
    Some(plan)
}

fn eliminate(ctx: &RuleContext<'_>, mut plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
    if matches!(plan, LogicalPlan::CTE(_)) {
        return Ok(plan);
    }
    let children = plan.base_mut().take_children();
    let mut rewritten = Vec::with_capacity(children.len());
    for child in children {
        rewritten.push(eliminate(ctx, child)?);
    }
    plan.set_children(rewritten);

    let LogicalPlan::Aggregation(aggregation) = plan else {
        return Ok(plan);
    };
    if !aggregation.group_by_items.is_empty() || aggregation.agg_funcs.is_empty() {
        return Ok(LogicalPlan::Aggregation(aggregation));
    }
    if aggregation
        .agg_funcs
        .iter()
        .any(|function| !matches!(function.name(), AGG_FUNC_MAX | AGG_FUNC_MIN))
    {
        return Ok(LogicalPlan::Aggregation(aggregation));
    }
    if aggregation.get_used_cols().iter().any(|column| {
        column.get_static_type().is_some_and(|field_type| {
            matches!(field_type.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
        })
    }) {
        return Ok(LogicalPlan::Aggregation(aggregation));
    }
    if aggregation.agg_funcs.len() == 1 {
        return eliminate_single(ctx, aggregation).map(LogicalPlan::Aggregation);
    }
    let Some(split) = split_aggregations(ctx, &aggregation) else {
        return Ok(LogicalPlan::Aggregation(aggregation));
    };
    let eliminated = split
        .into_iter()
        .map(|aggregation| eliminate_single(ctx, aggregation))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(compose_by_inner_join(ctx, eliminated)
        .unwrap_or_else(|| unreachable!("a multiple aggregate split is non-empty")))
}

/// Go `MaxMinEliminator`.
#[derive(Debug)]
pub struct MaxMinEliminator;

impl LogicalOptRule for MaxMinEliminator {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let recovery = plan.clone();
        eliminate(ctx, plan)
            .map(|plan| (plan, false))
            .map_err(|error| (recovery, error))
    }

    fn name(&self) -> &'static str {
        "max_min_eliminate"
    }
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode, BaseFuncDesc};
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;
    use tidb_expr::schema::Schema;

    use super::*;
    use crate::logical::rule_tests::test_context;
    use crate::logical::DataSource;
    use crate::plan_base::PlanIdAllocator;

    fn column(id: i64, nullable: bool) -> Column {
        let mut field_type = FieldType::new(FieldTypeCode::Long);
        if !nullable {
            field_type.add_flags(FieldTypeFlags::NOT_NULL);
        }
        Column::new(id, field_type)
    }

    fn aggregation(
        allocator: &PlanIdAllocator,
        functions: Vec<AggFuncDesc>,
        child: Option<LogicalPlan>,
    ) -> LogicalPlan {
        let outputs = (0..functions.len())
            .map(|offset| column(100 + offset as i64, false))
            .collect::<Vec<_>>();
        let mut base = BaseLogicalPlan::new(allocator, LogicalAggregation::TYPE, 0);
        base.base.set_schema(Some(Schema::new(outputs)));
        let mut plan =
            LogicalPlan::Aggregation(LogicalAggregation::new(base, functions, Vec::new()));
        if let Some(child) = child {
            plan.set_children(vec![child]);
        }
        plan
    }

    fn max(column: Column) -> AggFuncDesc {
        AggFuncDesc {
            base: BaseFuncDesc {
                name: AGG_FUNC_MAX.to_owned(),
                args: vec![Expression::Column(column)],
                ret_type: FieldType::new(FieldTypeCode::Long),
            },
            mode: AggFunctionMode::Complete,
            has_distinct: false,
            order_by_items: Vec::new(),
            grouping_id: 0,
        }
    }

    #[test]
    fn max_min_eliminate_skips_empty_scalar_aggregation() {
        let allocator = PlanIdAllocator::new();
        let plan = aggregation(&allocator, Vec::new(), None);
        let (plan, changed) = MaxMinEliminator
            .optimize(&test_context(&allocator), plan)
            .expect("empty aggregate stays valid");
        assert!(!changed);
        assert!(matches!(plan, LogicalPlan::Aggregation(_)));
    }

    #[test]
    fn nullable_single_max_becomes_not_null_sort_limit_under_aggregation() {
        let allocator = PlanIdAllocator::new();
        let argument = column(1, true);
        let mut source_base = BaseLogicalPlan::new(&allocator, DataSource::TYPE, 0);
        source_base
            .base
            .set_schema(Some(Schema::new(vec![argument.clone()])));
        let source = LogicalPlan::DataSource(DataSource {
            base: source_base,
            ..DataSource::default()
        });
        let plan = aggregation(&allocator, vec![max(argument)], Some(source));

        let (plan, changed) = MaxMinEliminator
            .optimize(&test_context(&allocator), plan)
            .expect("single max is eliminated");
        assert!(!changed);
        let LogicalPlan::Aggregation(aggregation) = &plan else {
            panic!("aggregation must remain")
        };
        let [LogicalPlan::Limit(limit)] = aggregation.base.children() else {
            panic!("aggregation child must be limit")
        };
        assert_eq!((limit.offset, limit.count), (0, 1));
        assert!(matches!(limit.base.children(), [LogicalPlan::Sort(_)]));
        assert!(matches!(
            limit.base.children()[0].children(),
            [LogicalPlan::Selection(_)]
        ));
        plan.dismantle();
    }
}
