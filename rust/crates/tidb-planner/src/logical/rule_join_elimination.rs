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

//! Go `pkg/planner/core/rule_join_elimination.go`.

use std::collections::HashSet;

use tidb_datatype::{Datum, FieldTypeFlags};
use tidb_expr::aggregation::names;
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::extract::find_upper_bound;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::extract_columns;

use crate::access_path::DataSourceAccessPath;
use crate::expression_rewriter::{
    extract_cor_columns_by_schema_4_logical_plan, extract_correlated_cols_4_logical_plan,
};
use crate::find_best_task::LogicalJoinType;

use super::rule::{LogicalOptRule, RuleContext};
use super::window::{BoundType, FrameType};
use super::{
    BaseLogicalPlan, LogicalAggregation, LogicalJoin, LogicalPlan, LogicalProjection, PlanError,
};

/// Go `OuterJoinEliminator`.
pub struct OuterJoinEliminator;

fn columns_from(expressions: impl IntoIterator<Item = Expression>) -> Vec<Column> {
    expressions
        .into_iter()
        .flat_map(|expression| extract_columns(&expression))
        .collect()
}

fn append_unique(target: &mut Vec<Column>, columns: impl IntoIterator<Item = Column>) {
    let mut seen = HashSet::new();
    for column in columns {
        if seen.insert(column.unique_id) {
            target.push(column);
        }
    }
}

fn null_extended_projection(
    context: &RuleContext<'_>,
    join: &LogicalJoin,
    outer: LogicalPlan,
) -> LogicalPlan {
    let Some(join_schema) = join.base.base.schema().cloned() else {
        return outer;
    };
    let Some(outer_schema) = outer.schema() else {
        return outer;
    };
    let mut all_from_outer = true;
    let expressions = join_schema
        .columns
        .iter()
        .map(|column| {
            if outer_schema.contains(column) {
                Expression::Column(column.clone())
            } else {
                all_from_outer = false;
                let mut field_type = column
                    .ret_type
                    .clone()
                    .expect("logical schema column has RetType");
                field_type.del_flags(FieldTypeFlags::NOT_NULL);
                Expression::Constant(Constant::new(Datum::Null, field_type))
            }
        })
        .collect();
    if all_from_outer {
        return outer;
    }
    let mut base = BaseLogicalPlan::new(
        context.allocator,
        LogicalProjection::TYPE,
        join.base.base.query_block_offset(),
    );
    base.base.set_schema(Some(join_schema));
    base.base
        .set_output_names(join.base.base.output_names().to_vec());
    base.set_children(vec![outer]);
    LogicalPlan::Projection(LogicalProjection::new(base, expressions))
}

fn extract_inner_join_keys(join: &LogicalJoin, inner_index: usize) -> (Schema, HashSet<i64>) {
    let mut keys = Vec::with_capacity(join.equal_conditions.len());
    let mut null_eq = HashSet::new();
    let inner_schema = join
        .base
        .children()
        .get(inner_index)
        .and_then(LogicalPlan::schema);
    for condition in &join.equal_conditions {
        let Some(column) = condition.args.iter().find_map(|argument| {
            let Expression::Column(column) = argument else {
                return None;
            };
            inner_schema
                .is_some_and(|schema| schema.contains(column))
                .then_some(column)
        }) else {
            continue;
        };
        keys.push(column.clone());
        if condition.func_name.lowercase() == "nulleq" {
            null_eq.insert(column.unique_id);
        }
    }
    (Schema::new(keys), null_eq)
}

fn is_col_eq_one(expression: &Expression, target: &Column) -> bool {
    let Expression::ScalarFunction(function) = expression else {
        return false;
    };
    if function.func_name.lowercase() != "eq" || function.args.len() != 2 {
        return false;
    }
    let matches = |column: &Expression, constant: &Expression| {
        matches!(column, Expression::Column(column) if column.unique_id == target.unique_id)
            && matches!(constant, Expression::Constant(constant) if constant.value == Datum::Int(1))
    };
    matches(&function.args[0], &function.args[1]) || matches(&function.args[1], &function.args[0])
}

fn row_number_partition_is_unique(inner: &LogicalPlan, join_keys: &Schema) -> bool {
    let LogicalPlan::Selection(selection) = inner else {
        return false;
    };
    if selection.conditions.is_empty() {
        return false;
    }
    let [LogicalPlan::Window(window)] = selection.base.children() else {
        return false;
    };
    if window.window_func_descs.len() != 1 || window.window_func_descs[0].base.name != "row_number"
    {
        return false;
    }
    let Some(frame) = &window.frame else {
        return false;
    };
    if frame.frame_type != FrameType::Rows
        || frame.start.as_ref().map(|bound| bound.bound_type) != Some(BoundType::CurrentRow)
        || frame.end.as_ref().map(|bound| bound.bound_type) != Some(BoundType::CurrentRow)
    {
        return false;
    }
    let Some(schema) = window.base.base.schema() else {
        return false;
    };
    let [row_number] = window.get_window_result_columns(schema) else {
        return false;
    };
    let bounded = selection.conditions.iter().any(|condition| {
        is_col_eq_one(condition, row_number)
            || find_upper_bound(condition).is_some_and(|(column, upper)| {
                column.unique_id == row_number.unique_id && upper <= 1
            })
    });
    bounded
        && window
            .get_partition_by_cols()
            .iter()
            .all(|column| join_keys.contains(column))
}

fn join_keys_contain_schema_key(
    inner: &LogicalPlan,
    join_keys: &Schema,
    null_eq: &HashSet<i64>,
) -> bool {
    if row_number_partition_is_unique(inner, join_keys) {
        return true;
    }
    let Some(schema) = inner.schema() else {
        return false;
    };
    if schema
        .pk_or_uk
        .iter()
        .any(|key| key.iter().all(|column| join_keys.contains(column)))
    {
        return true;
    }
    schema.nullable_uk.iter().any(|key| {
        key.iter()
            .all(|column| join_keys.contains(column) && !null_eq.contains(&column.unique_id))
    })
}

fn join_keys_contain_unique_index(
    inner: &LogicalPlan,
    join_keys: &Schema,
    null_eq: &HashSet<i64>,
) -> bool {
    let LogicalPlan::DataSource(source) = inner else {
        return false;
    };
    source.all_possible_access_paths.iter().any(|path| {
        let DataSourceAccessPath::Index(path) = path else {
            return false;
        };
        let index_id = path.candidate().index_id;
        let Some(index) = source
            .indexes
            .iter()
            .find(|index| index.id == index_id && index.unique && !index.columns.is_empty())
        else {
            return false;
        };
        index.columns.iter().all(|index_column| {
            let Some(column) = source.table_columns.get(index_column.offset) else {
                return false;
            };
            join_keys.contains(column)
                && (column
                    .ret_type
                    .as_ref()
                    .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL))
                    || !null_eq.contains(&column.unique_id))
        })
    })
}

fn try_eliminate(
    context: &RuleContext<'_>,
    mut join: LogicalJoin,
    agg_columns: &[Column],
    parent_columns: &[Column],
) -> (LogicalPlan, bool) {
    let inner_index = match join.join_type {
        LogicalJoinType::LeftOuter => 1,
        LogicalJoinType::RightOuter => 0,
        _ => return (LogicalPlan::Join(join), false),
    };
    let outer_index = 1 ^ inner_index;
    let children = join.base.children();
    let Some(inner) = children.get(inner_index) else {
        return (LogicalPlan::Join(join), false);
    };
    let Some(outer) = children.get(outer_index) else {
        return (LogicalPlan::Join(join), false);
    };
    if matches!(inner, LogicalPlan::TableDual(dual) if dual.row_count == 0) {
        let mut children = join.base.take_children();
        let outer = children.swap_remove(outer_index);
        return (null_extended_projection(context, &join, outer), true);
    }
    let Some(outer_schema) = outer.schema() else {
        return (LogicalPlan::Join(join), false);
    };
    if parent_columns
        .iter()
        .any(|column| !outer_schema.contains(column))
    {
        return (LogicalPlan::Join(join), false);
    }
    if !agg_columns.is_empty() {
        let Some(inner_schema) = inner.schema() else {
            return (LogicalPlan::Join(join), false);
        };
        if agg_columns
            .iter()
            .all(|column| !inner_schema.contains(column))
        {
            let mut children = join.base.take_children();
            return (children.swap_remove(outer_index), true);
        }
    }
    let (join_keys, null_eq) = extract_inner_join_keys(&join, inner_index);
    if join_keys_contain_schema_key(inner, &join_keys, &null_eq)
        || join_keys_contain_unique_index(inner, &join_keys, &null_eq)
    {
        let mut children = join.base.take_children();
        return (children.swap_remove(outer_index), true);
    }
    (LogicalPlan::Join(join), false)
}

fn duplicate_agnostic_columns(aggregation: &LogicalAggregation) -> Vec<Column> {
    let mut columns = Vec::new();
    for function in &aggregation.agg_funcs {
        if !function.has_distinct
            && !matches!(
                function.name(),
                names::FIRST_ROW | names::MAX | names::MIN | names::APPROX_COUNT_DISTINCT
            )
        {
            return Vec::new();
        }
        for argument in function.args() {
            columns.extend(extract_columns(argument));
        }
    }
    columns
}

fn required_columns(
    context: &RuleContext<'_>,
    plan: &LogicalPlan,
    incoming: &[Column],
) -> Vec<Column> {
    match plan {
        LogicalPlan::Apply(apply) => {
            if context.enable_no_decorrelate_in_select {
                let Some(left) = apply.join.base.children().first() else {
                    return Vec::new();
                };
                let Some(left_schema) = left.schema() else {
                    return Vec::new();
                };
                let mut columns = incoming
                    .iter()
                    .filter(|column| left_schema.contains(column))
                    .cloned()
                    .collect();
                if let Some(right) = apply.join.base.children().get(1) {
                    append_unique(
                        &mut columns,
                        extract_cor_columns_by_schema_4_logical_plan(right, left_schema)
                            .into_iter()
                            .map(|column| column.column),
                    );
                }
                columns
            } else {
                plan.schema()
                    .map_or_else(Vec::new, |schema| schema.columns.clone())
            }
        }
        LogicalPlan::Projection(projection) => {
            let mut columns = columns_from(projection.exprs.clone());
            if context.enable_no_decorrelate_in_select {
                if let Some(child) = projection.base.children().first() {
                    append_unique(
                        &mut columns,
                        extract_correlated_cols_4_logical_plan(child)
                            .into_iter()
                            .map(|column| column.column),
                    );
                }
            }
            columns
        }
        LogicalPlan::Aggregation(aggregation) => {
            let mut columns = columns_from(aggregation.group_by_items.clone());
            for function in &aggregation.agg_funcs {
                columns.extend(columns_from(function.args().to_vec()));
                columns.extend(columns_from(
                    function.order_by_items.iter().map(|item| item.expr.clone()),
                ));
            }
            columns
        }
        LogicalPlan::Join(join) => {
            let mut columns = plan
                .schema()
                .map_or_else(Vec::new, |schema| schema.columns.clone());
            columns.extend(columns_from(
                join.equal_conditions
                    .iter()
                    .cloned()
                    .map(Expression::ScalarFunction),
            ));
            columns.extend(columns_from(join.left_conditions.clone()));
            columns.extend(columns_from(join.right_conditions.clone()));
            columns.extend(columns_from(join.other_conditions.clone()));
            columns.extend(columns_from(
                join.na_eq_conditions
                    .iter()
                    .cloned()
                    .map(Expression::ScalarFunction),
            ));
            columns
        }
        _ => plan
            .schema()
            .map_or_else(Vec::new, |schema| schema.columns.clone()),
    }
}

fn optimize_recursive(
    context: &RuleContext<'_>,
    mut plan: LogicalPlan,
    agg_columns: Vec<Column>,
    parent_columns: Vec<Column>,
) -> LogicalPlan {
    if matches!(plan, LogicalPlan::CTE(_)) {
        return plan;
    }
    loop {
        let LogicalPlan::Join(join) = plan else {
            break;
        };
        let (rewritten, eliminated) = try_eliminate(context, join, &agg_columns, &parent_columns);
        plan = rewritten;
        if !eliminated {
            break;
        }
    }

    let child_parent_columns = required_columns(context, &plan, &parent_columns);
    let child_agg_columns = match &plan {
        LogicalPlan::Aggregation(aggregation) => duplicate_agnostic_columns(aggregation),
        _ => agg_columns,
    };
    let children = plan
        .base_mut()
        .take_children()
        .into_iter()
        .map(|child| {
            optimize_recursive(
                context,
                child,
                child_agg_columns.clone(),
                child_parent_columns.clone(),
            )
        })
        .collect();
    plan.base_mut().set_children(children);
    plan
}

impl LogicalOptRule for OuterJoinEliminator {
    fn optimize(
        &self,
        context: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok((
            optimize_recursive(context, plan, Vec::new(), Vec::new()),
            false,
        ))
    }

    fn name(&self) -> &'static str {
        "outer_join_eliminate"
    }
}
