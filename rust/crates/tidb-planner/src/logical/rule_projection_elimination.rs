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

//! Go `ProjectionEliminator` (`pkg/planner/core/rule_eliminate_projection.go`).

use std::collections::BTreeMap;

use tidb_datatype::FieldTypeFlags;
use tidb_expr::column::Column;
use tidb_expr::expr_util::predicates::exprs_has_side_effects;
use tidb_expr::expr_util::substitute::SubstituteOptions;
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::schema::{merge_schema, Schema};

use crate::base_arms;
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanError;

use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::rule::{LogicalOptRule, RuleContext};
use super::LogicalPlan;

type ColumnReplacements = BTreeMap<Vec<u8>, Column>;

fn column_key(column: &Column) -> Vec<u8> {
    let mut column = column.clone();
    column.hash_code().to_vec()
}

fn resolve_column(origin: &Column, replace: &ColumnReplacements) -> (Column, bool) {
    let Some(replacement) = replace.get(&column_key(origin)) else {
        return (origin.clone(), false);
    };
    let mut replacement = replacement.clone();
    replacement.ret_type.clone_from(&origin.ret_type);
    replacement.in_operand = origin.in_operand;
    (replacement, true)
}

fn resolve_expression(origin: &Expression, replace: &ColumnReplacements) -> (Expression, bool) {
    match origin {
        Expression::Column(column) => {
            let (column, changed) = resolve_column(column, replace);
            (Expression::Column(column), changed)
        }
        Expression::CorrelatedColumn(correlated) => {
            let mut correlated = correlated.clone();
            let (column, changed) = resolve_column(&correlated.column, replace);
            correlated.column = column;
            (Expression::CorrelatedColumn(correlated), changed)
        }
        Expression::ScalarFunction(function) => {
            let mut function = function.clone();
            let mut changed = false;
            function.args = function
                .args
                .into_iter()
                .map(|argument| {
                    let (argument, argument_changed) = resolve_expression(&argument, replace);
                    changed |= argument_changed;
                    argument
                })
                .collect();
            if changed {
                function.invalidate_cached_arguments();
            }
            (Expression::ScalarFunction(function), changed)
        }
        Expression::Constant(constant) => (Expression::Constant(constant.clone()), false),
    }
}

fn resolve_scalar_function(
    function: ScalarFunction,
    replace: &ColumnReplacements,
) -> ScalarFunction {
    let (Expression::ScalarFunction(function), _) =
        resolve_expression(&Expression::ScalarFunction(function), replace)
    else {
        unreachable!("a scalar function remains a scalar function")
    };
    function
}

fn replace_operator_expressions(plan: &mut LogicalPlan, replace: &ColumnReplacements) {
    match plan {
        LogicalPlan::Selection(selection) => {
            for expression in &mut selection.conditions {
                *expression = resolve_expression(expression, replace).0;
            }
        }
        LogicalPlan::Projection(projection) => {
            for expression in &mut projection.exprs {
                *expression = resolve_expression(expression, replace).0;
            }
        }
        LogicalPlan::Join(join) => replace_join_expressions(join, replace),
        LogicalPlan::Apply(apply) => {
            replace_join_expressions(&mut apply.join, replace);
            for correlated in &mut apply.cor_cols {
                correlated.column = resolve_column(&correlated.column, replace).0;
            }
        }
        LogicalPlan::Aggregation(aggregation) => {
            for function in &mut aggregation.agg_funcs {
                for argument in &mut function.base.args {
                    *argument = resolve_expression(argument, replace).0;
                }
                for item in &mut function.order_by_items {
                    item.expr = resolve_expression(&item.expr, replace).0;
                }
            }
            for expression in &mut aggregation.group_by_items {
                *expression = resolve_expression(expression, replace).0;
            }
        }
        LogicalPlan::Sort(sort) => {
            for item in &mut sort.by_items {
                item.expr = resolve_expression(&item.expr, replace).0;
            }
        }
        LogicalPlan::TopN(topn) => {
            for item in &mut topn.by_items {
                item.expr = resolve_expression(&item.expr, replace).0;
            }
        }
        LogicalPlan::Window(window) => {
            for descriptor in &mut window.window_func_descs {
                for argument in &mut descriptor.base.args {
                    *argument = resolve_expression(argument, replace).0;
                }
            }
            for item in &mut window.partition_by {
                item.col = resolve_column(&item.col, replace).0;
            }
            for item in &mut window.order_by {
                item.col = resolve_column(&item.col, replace).0;
            }
            if let Some(frame) = &mut window.frame {
                for bound in [frame.start.as_mut(), frame.end.as_mut()]
                    .into_iter()
                    .flatten()
                {
                    for expression in &mut bound.calc_funcs {
                        *expression = resolve_expression(expression, replace).0;
                    }
                    for expression in &mut bound.compare_cols {
                        *expression = resolve_expression(expression, replace).0;
                    }
                }
            }
        }
        base_arms![
            Limit,
            UnionAll,
            PartitionUnionAll,
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
        ] => {}
    }
}

fn replace_join_expressions(join: &mut super::LogicalJoin, replace: &ColumnReplacements) {
    for condition in &mut join.equal_conditions {
        *condition = resolve_scalar_function(condition.clone(), replace);
    }
    for expressions in [
        &mut join.left_conditions,
        &mut join.right_conditions,
        &mut join.other_conditions,
    ] {
        for expression in expressions {
            *expression = resolve_expression(expression, replace).0;
        }
    }
}

fn replace_schema_columns(plan: &mut LogicalPlan, replace: &ColumnReplacements) {
    let Some(mut schema) = plan.base().base.schema().cloned() else {
        return;
    };
    for column in &mut schema.columns {
        *column = resolve_column(column, replace).0;
    }
    plan.base_mut().base.set_schema(Some(schema));
}

fn apply_schema(plan: &LogicalPlan, join_type: LogicalJoinType) -> Option<Schema> {
    let left = plan.children().first()?.schema()?;
    match join_type {
        LogicalJoinType::Semi | LogicalJoinType::AntiSemi => Some(left.clone()),
        LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi => {
            let mut schema = left.clone();
            schema.columns.push(plan.schema()?.columns.last()?.clone());
            Some(schema)
        }
        LogicalJoinType::Inner | LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter => {
            let mut schema = merge_schema(Some(left), plan.children().get(1)?.schema())?;
            let left_len = left.len();
            let total = schema.len();
            match join_type {
                LogicalJoinType::LeftOuter => {
                    crate::plan_builder::from::reset_not_null_flag(&mut schema, left_len, total);
                }
                LogicalJoinType::RightOuter => {
                    crate::plan_builder::from::reset_not_null_flag(&mut schema, 0, left_len);
                }
                _ => {}
            }
            Some(schema)
        }
    }
}

fn expression_type_mut(expression: &mut Expression) -> Option<&mut tidb_datatype::FieldType> {
    match expression {
        Expression::Column(column) => column.ret_type.as_mut(),
        Expression::Constant(constant) => constant.ret_type.as_mut(),
        Expression::CorrelatedColumn(correlated) => correlated.column.ret_type.as_mut(),
        Expression::ScalarFunction(function) => function.ret_type.as_mut(),
    }
}

fn preserve_not_null_flag(source: &Expression, target: &mut Expression) {
    let not_null = source
        .static_type()
        .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL));
    let Some(target_type) = expression_type_mut(target) else {
        return;
    };
    if not_null {
        target_type.add_flags(FieldTypeFlags::NOT_NULL);
    } else {
        target_type.del_flags(FieldTypeFlags::NOT_NULL);
    }
}

fn merge_adjacent_projections(
    mut projection: super::LogicalProjection,
    builder: &dyn tidb_expr::expr_util::builder::FunctionBuilder,
) -> super::LogicalProjection {
    let mut children = projection.base.take_children();
    let Some(child_plan) = children.pop() else {
        projection.base.set_children(children);
        return projection;
    };
    let LogicalPlan::Projection(mut child) = child_plan else {
        children.push(child_plan);
        projection.base.set_children(children);
        return projection;
    };
    if !children.is_empty() || exprs_has_side_effects(&child.exprs) {
        children.push(LogicalPlan::Projection(child));
        projection.base.set_children(children);
        return projection;
    }
    let child_schema = child.base.base.schema().cloned().unwrap_or_default();
    let options = SubstituteOptions::new(builder);
    for expression in &mut projection.exprs {
        let replaced =
            super::rule_util::replace_column_of_expr(expression, &child.exprs, &child_schema);
        let mut folded =
            tidb_expr::expr_util::fold::fold_constant(&replaced, &tidb_expr::NoColumns, &options);
        preserve_not_null_flag(&replaced, &mut folded);
        *expression = folded;
    }
    projection.base.set_children(child.base.take_children());
    projection
}

struct ProjectionRewrite<'a> {
    builder: &'a dyn tidb_expr::expr_util::builder::FunctionBuilder,
    replace: ColumnReplacements,
    can_eliminate: Vec<bool>,
}

impl OwnedRewrite for ProjectionRewrite<'_> {
    type Down = bool;
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, can_eliminate: bool) -> Descend<bool, ()> {
        if matches!(node, LogicalPlan::CTE(_)) {
            return Descend::Stop(());
        }
        self.can_eliminate.push(can_eliminate);
        let child_flag = match node {
            LogicalPlan::UnionAll(_) => false,
            LogicalPlan::Aggregation(_) | LogicalPlan::Projection(_) | LogicalPlan::Window(_) => {
                true
            }
            base_arms![
                Selection,
                Join,
                Apply,
                Sort,
                Limit,
                TopN,
                PartitionUnionAll,
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
            ] => can_eliminate,
            LogicalPlan::CTE(_) => unreachable!("CTE stopped above"),
        };
        Descend::Children(vec![child_flag; node.children().len()])
    }

    fn ascend(&mut self, mut node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
        let can_eliminate = self
            .can_eliminate
            .pop()
            .unwrap_or_else(|| unreachable!("every descended node records its flag"));

        if let LogicalPlan::Apply(apply) = &node {
            if let Some(schema) = apply_schema(&node, apply.join.join_type) {
                node.base_mut().base.set_schema(Some(schema));
            }
        } else {
            replace_schema_columns(&mut node, &self.replace);
        }
        replace_operator_expressions(&mut node, &self.replace);

        if let LogicalPlan::Projection(projection) = node {
            let mut projection = merge_adjacent_projections(projection, self.builder);
            if can_eliminate && projection.can_be_eliminated_loose() {
                if let Some(schema) = projection.base.base.schema() {
                    for (column, expression) in schema.columns.iter().zip(projection.exprs.iter()) {
                        let Expression::Column(replacement) = expression else {
                            unreachable!("loose elimination admitted only columns")
                        };
                        self.replace.insert(column_key(column), replacement.clone());
                    }
                }
                if let Some(child) = projection.base.take_children().pop() {
                    return (child, ());
                }
            }
            return (LogicalPlan::Projection(projection), ());
        }
        (node, ())
    }
}

/// Go `ProjectionEliminator.eliminate` over the complete logical tree.
#[must_use]
pub fn eliminate_projections(
    plan: LogicalPlan,
    builder: &dyn tidb_expr::expr_util::builder::FunctionBuilder,
) -> LogicalPlan {
    let mut rewrite = ProjectionRewrite {
        builder,
        replace: BTreeMap::new(),
        can_eliminate: Vec::new(),
    };
    fold_owned(&mut rewrite, plan, false).0
}

/// Go `ProjectionEliminator`.
#[derive(Debug)]
pub struct ProjectionEliminator;

impl LogicalOptRule for ProjectionEliminator {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok((eliminate_projections(plan, ctx.builder), false))
    }

    fn name(&self) -> &'static str {
        "projection_eliminate"
    }
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::expression::Expression;
    use tidb_expr::schema::Schema;

    use super::eliminate_projections;
    use crate::logical::aggregation::LogicalAggregation;
    use crate::logical::projection::LogicalProjection;
    use crate::logical::table_dual::LogicalTableDual;
    use crate::logical::union_all::LogicalUnionAll;
    use crate::logical::{BaseLogicalPlan, LogicalPlan};

    fn column(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn base(id: i32, tp: &str, columns: &[i64]) -> BaseLogicalPlan {
        let mut base = BaseLogicalPlan::with_id(id, tp, 0);
        base.base.set_schema(Some(Schema::new(
            columns.iter().copied().map(column).collect(),
        )));
        base
    }

    fn dual(id: i32, output: i64) -> LogicalPlan {
        LogicalPlan::TableDual(LogicalTableDual::new(
            base(id, LogicalTableDual::TYPE, &[output]),
            1,
        ))
    }

    fn projection(id: i32, output: i64, expression: Expression, child: LogicalPlan) -> LogicalPlan {
        let mut base = base(id, LogicalProjection::TYPE, &[output]);
        base.set_children(vec![child]);
        LogicalPlan::Projection(LogicalProjection::new(base, vec![expression]))
    }

    #[test]
    fn aggregate_eliminates_column_projection_and_rewrites_columns() {
        let child = projection(2, 2, Expression::Column(column(1)), dual(1, 1));
        let mut aggregate_base = base(3, LogicalAggregation::TYPE, &[2]);
        aggregate_base.set_children(vec![child]);
        let aggregate = LogicalPlan::Aggregation(LogicalAggregation::new(
            aggregate_base,
            Vec::new(),
            vec![Expression::Column(column(2))],
        ));

        let LogicalPlan::Aggregation(aggregate) =
            eliminate_projections(aggregate, &crate::logical::rule_tests::TEST_BUILDER)
        else {
            panic!("the aggregate remains the root")
        };
        assert!(matches!(
            aggregate.base.children(),
            [LogicalPlan::TableDual(_)]
        ));
        let Expression::Column(group_by) = &aggregate.group_by_items[0] else {
            panic!("the group-by remains a column")
        };
        assert_eq!(group_by.unique_id, 1);
        assert_eq!(
            aggregate.base.base.schema().unwrap().columns[0].unique_id,
            1
        );
    }

    #[test]
    fn union_all_blocks_projection_elimination_in_its_children() {
        let child = projection(2, 2, Expression::Column(column(1)), dual(1, 1));
        let mut union_base = base(3, LogicalUnionAll::TYPE, &[2]);
        union_base.set_children(vec![child]);
        let union = LogicalPlan::UnionAll(LogicalUnionAll::new(union_base));

        let LogicalPlan::UnionAll(union) =
            eliminate_projections(union, &crate::logical::rule_tests::TEST_BUILDER)
        else {
            panic!("the union remains the root")
        };
        assert!(matches!(
            union.base.children(),
            [LogicalPlan::Projection(_)]
        ));
    }

    #[test]
    fn root_projection_is_not_eliminated() {
        let root = projection(2, 2, Expression::Column(column(1)), dual(1, 1));
        assert!(matches!(
            eliminate_projections(root, &crate::logical::rule_tests::TEST_BUILDER),
            LogicalPlan::Projection(_)
        ));
    }

    #[test]
    fn adjacent_projection_is_composed_before_elimination() {
        let constant = Expression::Constant(Constant::new(
            Datum::new_int(7),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let child = projection(2, 2, constant, dual(1, 1));
        let root = projection(3, 3, Expression::Column(column(2)), child);

        let LogicalPlan::Projection(root) =
            eliminate_projections(root, &crate::logical::rule_tests::TEST_BUILDER)
        else {
            panic!("the root projection is retained")
        };
        assert!(matches!(root.base.children(), [LogicalPlan::TableDual(_)]));
        let [Expression::Constant(constant)] = root.exprs.as_slice() else {
            panic!("the composed expression is the child's constant")
        };
        assert_eq!(constant.value, Datum::new_int(7));
    }
}
