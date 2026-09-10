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

//! Bind execution indexes without changing planner column identities.
//! Go: `core/resolve_indices.go` and `operator/physicalop/*::ResolveIndices`.

use std::collections::BTreeMap;

use tidb_expr::aggregation::{AggFuncDesc, ByItems};
use tidb_expr::column::Column;
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::schema::{merge_schema, Schema};
use tidb_expr::simple_expr::resolve_indices_in_place;
use tidb_util::disjointset::SimpleIntSet;

use super::{
    BasePhysicalPlan, PhysicalHashJoin, PhysicalIndexJoin, PhysicalMergeJoin, PhysicalPlan,
};
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanError;

fn bind(expr: &mut Expression, schema: &Schema) -> Result<(), PlanError> {
    resolve_indices_in_place(expr, schema).map_err(|error| PlanError::internal(error.to_string()))
}

fn bind_exprs(exprs: &mut [Expression], schema: &Schema) -> Result<(), PlanError> {
    for expr in exprs {
        bind(expr, schema)?;
    }
    Ok(())
}

fn bind_column(column: &mut Column, schema: &Schema) -> Result<(), PlanError> {
    let index = schema.column_index(column);
    if index < 0 {
        return Err(PlanError::internal(format!(
            "Can't find column {} in schema",
            column.unique_id
        )));
    }
    column.index = index as i64;
    Ok(())
}

fn child_schema(base: &BasePhysicalPlan, index: usize) -> Result<&Schema, PlanError> {
    base.children()
        .get(index)
        .and_then(PhysicalPlan::schema)
        .ok_or_else(|| {
            PlanError::internal(format!(
                "{} has no child schema at {index}",
                base.base.explain_id(false)
            ))
        })
}

fn bind_by_items(items: &mut [ByItems], schema: &Schema) -> Result<(), PlanError> {
    for item in items {
        bind(&mut item.expr, schema)?;
    }
    Ok(())
}

fn bind_aggregation(
    functions: &mut [AggFuncDesc],
    group_by: &mut [Expression],
    schema: &Schema,
) -> Result<(), PlanError> {
    for function in functions {
        bind_exprs(&mut function.base.args, schema)?;
        bind_by_items(&mut function.order_by_items, schema)?;
    }
    bind_exprs(group_by, schema)
}

// Selection alone permits Go's duplicate virtual-expression fallback.
fn bind_virtual(expr: &mut Expression, schema: &Schema) -> bool {
    match expr {
        Expression::Column(column) => {
            if let Some(resolved) = column.resolve_indices_by_virtual_expr(schema) {
                *column = resolved;
                true
            } else {
                false
            }
        }
        Expression::ScalarFunction(function) => function
            .args
            .iter_mut()
            .all(|arg| bind_virtual(arg, schema)),
        Expression::Constant(_) | Expression::CorrelatedColumn(_) => true,
    }
}

fn bind_virtual_columns(columns: &mut [Column], schema: &Schema) -> Result<(), PlanError> {
    for column in columns {
        if let Some(expr) = &mut column.virtual_expr {
            bind(expr, schema)?;
        }
    }
    Ok(())
}

// Go's inline projection is an ordered subsequence, not ColumnIndex: duplicate
// UniqueIDs must select distinct occurrences in the input row.
fn bind_inline_projection(base: &mut BasePhysicalPlan) -> Result<(), PlanError> {
    let input = child_schema(base, 0)?;
    let mut output = base.base.schema().cloned().unwrap_or_else(|| input.clone());
    let mut next = 0;
    for column in &mut output.columns {
        let relative = input.columns[next..]
            .iter()
            .position(|candidate| candidate.unique_id == column.unique_id)
            .ok_or_else(|| {
                PlanError::internal(format!(
                    "Some columns of {} cannot find the reference from its child(ren)",
                    base.base.explain_id(false)
                ))
            })?;
        next += relative;
        column.index = next as i64;
        next += 1;
    }
    base.base.set_schema(Some(Schema::new(output.columns)));
    Ok(())
}

fn bind_equalities(
    conditions: &mut [ScalarFunction],
    left_keys: &mut Vec<Column>,
    right_keys: &mut Vec<Column>,
    left: &Schema,
    right: &Schema,
) -> Result<(), PlanError> {
    left_keys.clear();
    right_keys.clear();
    for condition in conditions {
        let [Expression::Column(left_key), Expression::Column(right_key)] =
            condition.args.as_mut_slice()
        else {
            return Err(PlanError::internal(
                "physical join equality requires two column arguments",
            ));
        };
        bind_column(left_key, left)?;
        bind_column(right_key, right)?;
        left_keys.push(left_key.clone());
        right_keys.push(right_key.clone());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn resolve_join_predicates_and_output(
    base: &mut BasePhysicalPlan,
    join_type: LogicalJoinType,
    left_conditions: &mut [Expression],
    right_conditions: &mut [Expression],
    other_conditions: &mut [Expression],
) -> Result<(), PlanError> {
    let left = child_schema(base, 0)?;
    let right = child_schema(base, 1)?;
    bind_exprs(left_conditions, left)?;
    bind_exprs(right_conditions, right)?;
    let merged = merge_schema(Some(left), Some(right)).expect("both input schemas exist");
    bind_exprs(other_conditions, &merged)?;
    let mut output = base
        .base
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("join has no output schema"))?;
    let indicator = matches!(
        join_type,
        LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi
    );
    let count = output
        .len()
        .checked_sub(usize::from(indicator))
        .ok_or_else(|| PlanError::internal("outer semi join has no match column"))?;
    let mut marked = vec![false; merged.len()];
    for column in &mut output.columns[..count] {
        let at = merged
            .columns
            .iter()
            .enumerate()
            .position(|(at, candidate)| candidate.unique_id == column.unique_id && !marked[at])
            .ok_or_else(|| {
                PlanError::internal(format!(
                    "Some columns of {} cannot find the reference from its child(ren)",
                    base.base.explain_id(false)
                ))
            })?;
        column.index = at as i64;
        marked[at] = true;
    }
    base.base.set_schema(Some(Schema::new(output.columns)));
    Ok(())
}

impl PhysicalHashJoin {
    fn resolve_indices_itself(&mut self) -> Result<(), PlanError> {
        let left = child_schema(&self.base, 0)?;
        let right = child_schema(&self.base, 1)?;
        if self.equal_conditions.is_empty() {
            for column in &mut self.left_join_keys {
                bind_column(column, left)?;
            }
            for column in &mut self.right_join_keys {
                bind_column(column, right)?;
            }
        } else {
            bind_equalities(
                &mut self.equal_conditions,
                &mut self.left_join_keys,
                &mut self.right_join_keys,
                left,
                right,
            )?;
        }
        for condition in &mut self.na_equal_conditions {
            let [left_arg, right_arg] = condition.args.as_mut_slice() else {
                return Err(PlanError::internal(
                    "physical join equality requires two arguments",
                ));
            };
            bind(left_arg, left)?;
            bind(right_arg, right)?;
        }
        resolve_join_predicates_and_output(
            &mut self.base,
            self.join_type,
            &mut self.left_conditions,
            &mut self.right_conditions,
            &mut self.other_conditions,
        )
    }
}

impl PhysicalIndexJoin {
    fn resolve_indices_itself(&mut self) -> Result<(), PlanError> {
        if self.inner_child_idx > 1 {
            return Err(PlanError::internal("invalid IndexJoin inner child index"));
        }
        let outer = child_schema(&self.base, 1 - self.inner_child_idx)?;
        let inner = child_schema(&self.base, self.inner_child_idx)?;
        for col in self
            .outer_join_keys
            .iter_mut()
            .chain(&mut self.outer_hash_keys)
        {
            bind_column(col, outer)?;
        }
        for col in self
            .inner_join_keys
            .iter_mut()
            .chain(&mut self.inner_hash_keys)
        {
            bind_column(col, inner)?;
        }
        if let Some(filters) = &mut self.compare_filters {
            bind_exprs(&mut filters.args, outer)?;
        }
        // Go resolves duplicate outputs as an ordered subsequence of L + R.
        resolve_join_predicates_and_output(
            &mut self.base,
            self.join_type,
            &mut self.left_conditions,
            &mut self.right_conditions,
            &mut self.other_conditions,
        )?;
        let merged = merge_schema(
            Some(child_schema(&self.base, 0)?),
            Some(child_schema(&self.base, 1)?),
        )
        .expect("both schemas exist");
        let mut output = self.base.base.schema().cloned().expect("resolved above");
        let count = output.len()
            - usize::from(matches!(
                self.join_type,
                LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi
            ));
        let mut next = 0;
        for column in &mut output.columns[..count] {
            let offset = merged.columns[next..]
                .iter()
                .position(|c| c.unique_id == column.unique_id)
                .ok_or_else(|| {
                    PlanError::internal("IndexJoin output is not a child subsequence")
                })?
                + next;
            column.index = offset as i64;
            next = offset + 1;
        }
        self.base.base.set_schema(Some(output));
        Ok(())
    }
}

impl PhysicalMergeJoin {
    fn resolve_indices_itself(&mut self) -> Result<(), PlanError> {
        let join = &mut *self;
        let left = child_schema(&join.base, 0)?;
        let right = child_schema(&join.base, 1)?;
        for key in &mut join.left_join_keys {
            bind_column(key, left)?;
        }
        for key in &mut join.right_join_keys {
            bind_column(key, right)?;
        }
        resolve_join_predicates_and_output(
            &mut join.base,
            join.join_type,
            &mut join.left_conditions,
            &mut join.right_conditions,
            &mut join.other_conditions,
        )
    }
}

impl PhysicalPlan {
    /// Go resolves children first, then binds expressions against their final schemas.
    pub fn resolve_indices(&mut self) -> Result<(), PlanError> {
        enum Step {
            Enter(PhysicalPlan),
            Exit(PhysicalPlan, usize),
        }
        let root = std::mem::replace(self, Self::TableDual(Default::default()));
        let mut work = vec![Step::Enter(root)];
        let mut resolved = Vec::new();
        let mut result = Ok(());
        while let Some(step) = work.pop() {
            match step {
                Step::Enter(mut node) => {
                    let children = node.base_mut().take_children();
                    work.push(Step::Exit(node, children.len()));
                    work.extend(children.into_iter().rev().map(Step::Enter));
                }
                Step::Exit(mut node, child_count) => {
                    node.set_children(resolved.split_off(resolved.len() - child_count));
                    // Reassemble every node after an error without binding later siblings.
                    if result.is_ok() {
                        result = node.resolve_indices_itself();
                    }
                    resolved.push(node);
                }
            }
        }
        *self = resolved.pop().expect("resolved root");
        result
    }

    fn resolve_indices_itself(&mut self) -> Result<(), PlanError> {
        match self {
            Self::Projection(op) => {
                let input = child_schema(&op.base, 0)?;
                bind_exprs(&mut op.exprs, input)?;
                if let Some(Self::Projection(child)) = op.base.children().first() {
                    let mut first_output = BTreeMap::new();
                    let mut union = SimpleIntSet::new(input.len() as isize);
                    for (index, expr) in child.exprs.iter().enumerate() {
                        if let Expression::Column(column) = expr {
                            let first = *first_output.entry(column.index).or_insert(index);
                            union.union(first as isize, index as isize);
                        }
                    }
                    for expr in &mut op.exprs {
                        if let Expression::Column(column) = expr {
                            column.index = union.find_root(column.index as isize) as i64;
                        }
                    }
                }
            }
            Self::Selection(op) => {
                if op.conditions.is_empty() {
                    return Ok(());
                }
                let input = child_schema(&op.base, 0)?;
                for expr in &mut op.conditions {
                    if let Err(error) = bind(expr, input) {
                        if !bind_virtual(expr, input) {
                            return Err(error);
                        }
                    }
                }
            }
            Self::Sort(op) => bind_by_items(&mut op.by_items, child_schema(&op.base, 0)?)?,
            Self::NominalSort(op) => {
                let input = child_schema(&op.base, 0)?;
                for item in &mut op.by_items {
                    bind_column(&mut item.col, input)?;
                }
            }
            Self::HashAgg(op) => bind_aggregation(
                &mut op.agg_funcs,
                &mut op.group_by_items,
                child_schema(&op.base, 0)?,
            )?,
            Self::StreamAgg(op) => bind_aggregation(
                &mut op.agg_funcs,
                &mut op.group_by_items,
                child_schema(&op.base, 0)?,
            )?,
            Self::Limit(op) => {
                let input = child_schema(&op.base, 0)?;
                for item in &mut op.partition_by {
                    bind_column(&mut item.col, input)?;
                }
                bind_inline_projection(&mut op.base)?;
            }
            Self::TopN(op) => {
                let input = child_schema(&op.base, 0)?;
                bind_by_items(&mut op.by_items, input)?;
                for item in &mut op.partition_by {
                    bind_column(&mut item.col, input)?;
                }
                bind_inline_projection(&mut op.base)?;
            }
            Self::HashJoin(op) => op.resolve_indices_itself()?,
            Self::MergeJoin(op) => op.resolve_indices_itself()?,
            Self::IndexJoin(op) => op.resolve_indices_itself()?,
            Self::Apply(op) => {
                op.hash_join.resolve_indices_itself()?;
                let left = child_schema(&op.hash_join.base, 0)?;
                let right = child_schema(&op.hash_join.base, 1)?;
                let mut dedup = BTreeMap::new();
                for mut column in std::mem::take(&mut op.outer_schema) {
                    bind_column(&mut column.column, left)?;
                    dedup.insert(column.column.unique_id, column);
                }
                op.outer_schema = dedup.into_values().collect();
                let merged =
                    merge_schema(Some(left), Some(right)).expect("both input schemas exist");
                for condition in op
                    .hash_join
                    .equal_conditions
                    .iter_mut()
                    .chain(&mut op.hash_join.na_equal_conditions)
                {
                    bind_exprs(&mut condition.args, &merged)?;
                }
            }
            Self::Lock(op) => {
                let input = child_schema(&op.base, 0)?;
                for columns in op.tbl_id_to_handle_cols.values_mut() {
                    for column in columns {
                        bind_column(column, input)?;
                    }
                }
                for column in op.tbl_id_to_phys_tbl_id_col.values_mut() {
                    bind_column(column, input)?;
                }
            }
            Self::TableReader(op) => {
                let mut schema = op
                    .base
                    .base
                    .schema()
                    .cloned()
                    .ok_or_else(|| PlanError::internal("TableReader has no output schema"))?;
                let binding_schema = schema.clone();
                bind_virtual_columns(&mut schema.columns, &binding_schema)?;
                op.base.base.set_schema(Some(schema));
                if let Some(plan) = &mut op.table_plan {
                    plan.resolve_indices()?;
                }
            }
            Self::IndexReader(op) => {
                let plan = op
                    .index_plan
                    .as_mut()
                    .ok_or_else(|| PlanError::internal("IndexReader has no index plan"))?;
                plan.resolve_indices()?;
                let input = plan
                    .schema()
                    .ok_or_else(|| PlanError::internal("IndexReader index plan has no schema"))?;
                for column in &mut op.output_columns {
                    if let Err(error) = bind_column(column, input) {
                        *column = column.resolve_indices_by_virtual_expr(input).ok_or(error)?;
                    }
                }
            }
            Self::IndexLookUpReader(op) => {
                if let Some(plan) = &mut op.table_plan {
                    let input = op.base.base.schema().ok_or_else(|| {
                        PlanError::internal("IndexLookUpReader has no output schema")
                    })?;
                    let mut schema = plan.schema().cloned().ok_or_else(|| {
                        PlanError::internal("IndexLookUpReader table plan has no schema")
                    })?;
                    bind_virtual_columns(&mut schema.columns, input)?;
                    plan.base_mut().base.set_schema(Some(schema));
                    plan.resolve_indices()?;
                }
                if let Some(plan) = &mut op.index_plan {
                    plan.resolve_indices()?;
                }
            }
            Self::TableScan(op) => {
                let mut schema = op
                    .base
                    .base
                    .schema()
                    .cloned()
                    .ok_or_else(|| PlanError::internal("TableScan has no schema"))?;
                for (index, column) in schema.columns.iter_mut().enumerate() {
                    column.index = index as i64;
                }
                op.base.base.set_schema(Some(schema));
            }
            Self::Window(op) => {
                let input = child_schema(&op.base, 0)?.clone();
                let mut schema = op
                    .base
                    .base
                    .schema()
                    .cloned()
                    .ok_or_else(|| PlanError::internal("Window has no output schema"))?;
                let pass_through = schema
                    .len()
                    .checked_sub(op.window_func_descs.len())
                    .ok_or_else(|| {
                        PlanError::internal("Window schema is missing function columns")
                    })?;
                for column in &mut schema.columns[..pass_through] {
                    bind_column(column, &input)?;
                }
                op.base.base.set_schema(Some(schema));
                for item in op.partition_by.iter_mut().chain(&mut op.order_by) {
                    bind_column(&mut item.col, &input)?;
                }
                for function in &mut op.window_func_descs {
                    bind_exprs(&mut function.base.args, &input)?;
                }
                if let Some(frame) = &mut op.frame {
                    for bound in frame.start.iter_mut().chain(frame.end.iter_mut()) {
                        bind_exprs(&mut bound.calc_funcs, &input)?;
                        bind_exprs(&mut bound.compare_cols, &input)?;
                    }
                }
            }
            Self::CTE(op) => {
                op.seed_plan.resolve_indices()?;
                if let Some(plan) = &mut op.recursive_plan {
                    plan.resolve_indices()?;
                }
            }
            Self::IndexMergeReader(op) => {
                for plan in &mut op.partial_plans_raw {
                    plan.resolve_indices()?;
                }
                if let Some(plan) = &mut op.table_plan {
                    plan.resolve_indices()?;
                }
            }
            Self::Dml(op) => {
                if let Some(plan) = &mut op.select_plan {
                    plan.resolve_indices()?;
                    let schema = plan
                        .schema()
                        .ok_or_else(|| PlanError::internal("DML source has no schema"))?;
                    for expression in op.update_expressions.iter_mut().flatten() {
                        bind(expression, schema)?;
                    }
                }
            }
            Self::IndexScan(_)
            | Self::TableSample(_)
            | Self::MemTable(_)
            | Self::PointGet(_)
            | Self::BatchPointGet(_)
            | Self::LocalIndexLookUp(_)
            | Self::TableDual(_)
            | Self::MaxOneRow(_)
            | Self::CTETable(_)
            | Self::Show(_)
            | Self::ShowDDLJobs(_)
            | Self::UnionAll(_)
            | Self::Sequence(_) => {}
        }
        Ok(())
    }
}
