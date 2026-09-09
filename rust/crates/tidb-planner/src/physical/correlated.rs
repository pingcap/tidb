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

//! Go physical ExtractCorrelatedCols and coreusage.ExtractCorColumnsBySchema.
//! Access conditions participate even when the correlated column occurs only
//! in a reader-owned scan, not in a Selection above it.
use super::*;
use tidb_expr::expression::Expression;
use tidb_expr::simple_expr::extract_cor_columns;

fn expressions<'a>(plan: &'a PhysicalPlan) -> Vec<&'a Expression> {
    let mut expressions = Vec::new();
    fn hash<'a>(join: &'a PhysicalHashJoin, out: &mut Vec<&'a Expression>) {
        for fun in join
            .equal_conditions
            .iter()
            .chain(&join.na_equal_conditions)
        {
            out.extend(fun.args.iter());
        }
        base(&join.join, out);
    }
    fn base<'a>(join: &'a BasePhysicalJoin, out: &mut Vec<&'a Expression>) {
        out.extend(join.left_conditions.iter());
        out.extend(join.right_conditions.iter());
        out.extend(join.other_conditions.iter());
    }
    match plan {
        PhysicalPlan::TableScan(op) => {
            expressions.extend(op.access_conditions.iter());
            expressions.extend(op.late_materialization_filter_conditions.iter());
        }
        PhysicalPlan::IndexScan(op) => expressions.extend(op.access_conditions.iter()),
        PhysicalPlan::Selection(op) => expressions.extend(op.conditions.iter()),
        PhysicalPlan::Projection(op) => expressions.extend(op.exprs.iter()),
        PhysicalPlan::HashJoin(op) => hash(op, &mut expressions),
        PhysicalPlan::MergeJoin(op) => base(&op.join, &mut expressions),
        PhysicalPlan::IndexJoin(op) => {
            base(&op.join, &mut expressions);
            if let Some(filters) = &op.compare_filters {
                expressions.extend(filters.op_args.iter());
            }
        }
        PhysicalPlan::Apply(op) => hash(&op.hash_join, &mut expressions),
        PhysicalPlan::Sort(op) => expressions.extend(op.by_items.iter().map(|item| &item.expr)),
        PhysicalPlan::TopN(op) => expressions.extend(op.by_items.iter().map(|item| &item.expr)),
        PhysicalPlan::HashAgg(op) => {
            expressions.extend(op.group_by_items.iter());
            for fun in &op.agg_funcs {
                expressions.extend(fun.base.args.iter());
            }
        }
        PhysicalPlan::StreamAgg(op) => {
            expressions.extend(op.group_by_items.iter());
            for fun in &op.agg_funcs {
                expressions.extend(fun.base.args.iter());
            }
        }
        _ => {}
    }
    expressions
}
pub(super) fn extract(plan: &PhysicalPlan) -> Vec<CorrelatedColumn> {
    let mut result = expressions(plan)
        .into_iter()
        .flat_map(extract_cor_columns)
        .collect::<Vec<_>>();
    if let PhysicalPlan::Apply(op) = plan {
        if let Some(schema) = op
            .hash_join
            .base
            .children()
            .first()
            .and_then(PhysicalPlan::schema)
        {
            result.retain(|col| !schema.contains(&col.column));
        }
    }
    match plan {
        PhysicalPlan::TableReader(op) => {
            if let Some(p) = &op.table_plan {
                result.extend(p.extract_correlated_cols_recursive());
            }
        }
        PhysicalPlan::IndexReader(op) => {
            if let Some(p) = &op.index_plan {
                result.extend(p.extract_correlated_cols_recursive());
            }
        }
        PhysicalPlan::IndexLookUpReader(op) => {
            for p in [&op.index_plan, &op.table_plan].into_iter().flatten() {
                result.extend(p.extract_correlated_cols_recursive());
            }
        }
        _ => {}
    }
    result
}
fn visit_expression(expr: &mut Expression, visit: &mut impl FnMut(&mut CorrelatedColumn)) {
    match expr {
        Expression::CorrelatedColumn(col) => visit(col),
        Expression::ScalarFunction(fun) => {
            for expr in &mut fun.args {
                visit_expression(expr, visit);
            }
        }
        _ => {}
    }
}
fn visit_base(join: &mut BasePhysicalJoin, visit: &mut impl FnMut(&mut CorrelatedColumn)) {
    for expr in join
        .left_conditions
        .iter_mut()
        .chain(&mut join.right_conditions)
        .chain(&mut join.other_conditions)
    {
        visit_expression(expr, visit);
    }
}
fn visit_hash(join: &mut PhysicalHashJoin, visit: &mut impl FnMut(&mut CorrelatedColumn)) {
    for fun in join
        .equal_conditions
        .iter_mut()
        .chain(&mut join.na_equal_conditions)
    {
        for expr in &mut fun.args {
            visit_expression(expr, visit);
        }
    }
    visit_base(&mut join.join, visit);
}
fn visit(plan: &mut PhysicalPlan, callback: &mut impl FnMut(&mut CorrelatedColumn)) {
    match plan {
        PhysicalPlan::IndexJoin(op) => {
            visit_base(&mut op.join, callback);
            if let Some(filters) = &mut op.compare_filters {
                for expr in &mut filters.op_args { visit_expression(expr, callback); }
            }
        }
        PhysicalPlan::TableScan(op) => {
            for e in op
                .access_conditions
                .iter_mut()
                .chain(&mut op.late_materialization_filter_conditions)
            {
                visit_expression(e, callback);
            }
        }
        PhysicalPlan::IndexScan(op) => {
            for e in &mut op.access_conditions {
                visit_expression(e, callback);
            }
        }
        PhysicalPlan::Selection(op) => {
            for e in &mut op.conditions {
                visit_expression(e, callback);
            }
        }
        PhysicalPlan::Projection(op) => {
            for e in &mut op.exprs {
                visit_expression(e, callback);
            }
        }
        PhysicalPlan::HashJoin(op) => visit_hash(op, callback),
        PhysicalPlan::MergeJoin(op) => visit_base(&mut op.join, callback),
        PhysicalPlan::Apply(op) => {
            let hidden = op
                .hash_join
                .base
                .children()
                .first()
                .and_then(PhysicalPlan::schema)
                .map(|s| s.columns.iter().map(|c| c.unique_id).collect::<Vec<_>>())
                .unwrap_or_default();
            visit_hash(&mut op.hash_join, &mut |col| {
                if !hidden.contains(&col.column.unique_id) {
                    callback(col);
                }
            });
        }
        PhysicalPlan::Sort(op) => {
            for item in &mut op.by_items {
                visit_expression(&mut item.expr, callback);
            }
        }
        PhysicalPlan::TopN(op) => {
            for item in &mut op.by_items {
                visit_expression(&mut item.expr, callback);
            }
        }
        PhysicalPlan::HashAgg(op) => {
            for e in &mut op.group_by_items {
                visit_expression(e, callback);
            }
            for fun in &mut op.agg_funcs {
                for e in &mut fun.base.args {
                    visit_expression(e, callback);
                }
            }
        }
        PhysicalPlan::StreamAgg(op) => {
            for e in &mut op.group_by_items {
                visit_expression(e, callback);
            }
            for fun in &mut op.agg_funcs {
                for e in &mut fun.base.args {
                    visit_expression(e, callback);
                }
            }
        }
        PhysicalPlan::TableReader(op) => {
            if let Some(p) = &mut op.table_plan {
                visit(p, callback);
            }
        }
        PhysicalPlan::IndexReader(op) => {
            if let Some(p) = &mut op.index_plan {
                visit(p, callback);
            }
        }
        PhysicalPlan::IndexLookUpReader(op) => {
            for p in [&mut op.index_plan, &mut op.table_plan]
                .into_iter()
                .flatten()
            {
                visit(p, callback);
            }
        }
        _ => {}
    }
    for child in plan.base_mut().children_mut() {
        visit(child, callback);
    }
}
impl PhysicalPlan {
    /// Go coreusage.ExtractCorrelatedCols4PhysicalPlan, node then children.
    pub fn extract_correlated_cols_recursive(&self) -> Vec<CorrelatedColumn> {
        let mut result = Vec::new();
        let mut stack = vec![self];
        while let Some(node) = stack.pop() {
            result.extend(node.extract_correlated_cols());
            stack.extend(node.children().iter().rev());
        }
        result
    }
    /// Go ExtractCorColumnsBySchema4PhysicalPlan. Rebind a caller-owned inner
    /// tree to fresh datum cells, returning one slot per outer schema column.
    /// Clone the plan before this call when retaining a prepared definition.
    pub fn bind_correlated_columns(&mut self, schema: &Schema) -> Vec<CorrelatedColumn> {
        let mut slots = vec![None; schema.len()];
        visit(self, &mut |cor| {
            let index = schema.column_index(&cor.column);
            if index < 0 {
                return;
            }
            let index = index as usize;
            let slot = slots[index].get_or_insert_with(|| {
                let mut column = schema.columns[index].clone();
                column.index = index as i64;
                CorrelatedColumn::new(column)
            });
            cor.data = slot.data.clone();
        });
        slots.into_iter().flatten().collect()
    }
}
