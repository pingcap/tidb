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

//! Bottom-up `ExtractFD` dispatch from pinned Go logical operators.

use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};
use tidb_funcdep::{ColSet, FdSet};

use super::aggregation::AGG_FUNC_FIRST_ROW;
use super::LogicalPlan;
use crate::find_best_task::LogicalJoinType;

#[derive(Default)]
struct FdExtractionContext {
    next_column_id: i64,
    projected_expression_ids: std::collections::HashMap<Vec<u8>, i64>,
}

impl FdExtractionContext {
    fn for_plan(plan: &LogicalPlan) -> Self {
        fn collect(plan: &LogicalPlan, context: &mut FdExtractionContext) {
            for child in plan.children() {
                collect(child, context);
            }
            if let Some(schema) = plan.schema() {
                for column in &schema.columns {
                    context.next_column_id = context.next_column_id.max(column.unique_id + 1);
                }
                if let LogicalPlan::Projection(projection) = plan {
                    if projection.fd_expression_ids_registered {
                        for (expression, output) in projection.exprs.iter().zip(&schema.columns) {
                            let mut expression = expression.clone();
                            context
                                .projected_expression_ids
                                .insert(expression.hash_code().to_vec(), output.unique_id);
                        }
                    }
                }
            }
            match plan {
                LogicalPlan::DataSource(source) => {
                    for column in &source.table_columns {
                        context.next_column_id = context.next_column_id.max(column.unique_id + 1);
                    }
                }
                LogicalPlan::Join(join) => {
                    for column in join.full_schema.iter().flat_map(|schema| &schema.columns) {
                        context.next_column_id = context.next_column_id.max(column.unique_id + 1);
                    }
                }
                LogicalPlan::Apply(apply) => {
                    for column in apply
                        .join
                        .full_schema
                        .iter()
                        .flat_map(|schema| &schema.columns)
                    {
                        context.next_column_id = context.next_column_id.max(column.unique_id + 1);
                    }
                }
                _ => {}
            }
        }

        let mut context = Self {
            next_column_id: 1,
            ..Self::default()
        };
        collect(plan, &mut context);
        context
    }

    fn alloc_column_id(&mut self) -> i64 {
        let id = self.next_column_id;
        self.next_column_id += 1;
        id
    }
}

fn schema_columns(schema: Option<&Schema>) -> ColSet {
    ColSet::new(
        schema
            .into_iter()
            .flat_map(|schema| schema.columns.iter())
            .map(|column| column.unique_id),
    )
}

fn fd_object_id(
    context: &mut FdExtractionContext,
    fds: &mut FdSet,
    expression: &Expression,
) -> Option<i64> {
    match expression {
        Expression::Column(column) => Some(column.unique_id),
        Expression::ScalarFunction(_) => {
            let mut expression = expression.clone();
            let hash = expression.hash_code().to_vec();
            if let Some(id) = fds.registered_unique_id(&hash) {
                return Some(id);
            }
            let mut next = context.alloc_column_id();
            while fds.all_cols().has(next)
                || fds.hash_code_to_unique_id.values().any(|id| *id == next)
            {
                next = context.alloc_column_id();
            }
            fds.register_unique_id(hash, next);
            Some(next)
        }
        _ => None,
    }
}

fn base_dependencies(plan: &LogicalPlan, context: &mut FdExtractionContext) -> FdSet {
    let mut result = FdSet::new();
    for child in plan.children() {
        result.add_from(&child.extract_fd_with_context(context));
    }
    result
}

fn add_not_null_facts(fds: &mut FdSet, conditions: &[Expression]) {
    let mut not_null = ColSet::default();
    for condition in conditions {
        for column in extract_columns(condition) {
            if tidb_funcdep::null_reject::is_null_rejected(condition, column.unique_id) {
                not_null.insert(column.unique_id);
            }
        }
    }
    fds.make_not_null(not_null);
}

fn add_constant_facts(
    context: &mut FdExtractionContext,
    fds: &mut FdSet,
    conditions: &[Expression],
) {
    let mut constants = ColSet::default();
    for expression in
        tidb_expr::expr_util::extract_constant_eq_columns_or_scalar(Vec::new(), conditions)
    {
        if let Some(id) = fd_object_id(context, fds, &expression) {
            constants.insert(id);
        }
    }
    fds.add_constants(constants);
}

fn add_equivalence_facts(
    context: &mut FdExtractionContext,
    fds: &mut FdSet,
    conditions: &[Expression],
) -> Vec<(ColSet, ColSet)> {
    let mut equivalences = Vec::new();
    for [left, right] in tidb_expr::expr_util::extract_equivalence_columns(Vec::new(), conditions) {
        if let (Some(left), Some(right)) = (
            fd_object_id(context, fds, &left),
            fd_object_id(context, fds, &right),
        ) {
            let left = ColSet::new([left]);
            let right = ColSet::new([right]);
            fds.add_equivalence(left.clone(), right.clone());
            equivalences.push((left, right));
        }
    }
    equivalences
}

fn add_condition_facts(
    context: &mut FdExtractionContext,
    fds: &mut FdSet,
    conditions: &[Expression],
) -> Vec<(ColSet, ColSet)> {
    add_not_null_facts(fds, conditions);
    add_constant_facts(context, fds, conditions);
    add_equivalence_facts(context, fds, conditions)
}

fn expression_columns(expressions: &[Expression]) -> ColSet {
    ColSet::new(
        expressions
            .iter()
            .flat_map(extract_columns)
            .map(|column| column.unique_id),
    )
}

fn expression_determinants(expression: &Expression) -> ColSet {
    ColSet::new(
        extract_columns(expression)
            .into_iter()
            .map(|column| column.unique_id)
            .chain(
                extract_cor_columns(expression)
                    .into_iter()
                    .map(|column| column.column.unique_id),
            ),
    )
}

fn expression_is_null_rejected_by_schema(expression: &Expression, schema: Option<&Schema>) -> bool {
    schema.is_some_and(|schema| {
        let columns = schema
            .columns
            .iter()
            .map(|column| column.unique_id)
            .collect::<Vec<_>>();
        tidb_funcdep::null_reject::is_null_rejected_by(expression, &columns)
    })
}

fn literal_is_false(expression: &Expression) -> bool {
    let Expression::Constant(constant) = expression else {
        return false;
    };
    constant.deferred_expr.is_none()
        && constant.param_marker.is_none()
        && constant.value.to_bool().is_ok_and(|value| value.value == 0)
}

impl LogicalPlan {
    /// Go `LogicalPlan.ExtractFD`: derive the dependency set bottom-up from
    /// the concrete logical operator rather than an exchange-local shortcut.
    #[must_use]
    pub fn extract_fd(&self) -> FdSet {
        let mut context = FdExtractionContext::for_plan(self);
        self.extract_fd_with_context(&mut context)
    }

    fn extract_fd_with_context(&self, context: &mut FdExtractionContext) -> FdSet {
        match self {
            Self::DataSource(source) => {
                let mut fds = FdSet::new();
                let all_columns =
                    ColSet::new(source.table_columns.iter().map(|column| column.unique_id));
                if source.pk_is_handle {
                    let primary = source
                        .columns
                        .iter()
                        .zip(&source.table_columns)
                        .filter(|(metadata, _)| metadata.is_primary_key)
                        .map(|(_, column)| column.unique_id)
                        .collect::<Vec<_>>();
                    if !primary.is_empty() {
                        let primary = ColSet::new(primary);
                        fds.add_strict(primary.clone(), all_columns.clone());
                        fds.make_not_null(primary);
                    }
                }
                if source.fd_latest_index_lookup_failed {
                    return fds;
                }
                if let Some(schema) = self.schema() {
                    let (strict_keys, lax_keys) = source.index_keys(schema);
                    for key in strict_keys {
                        let key = ColSet::new(key.iter().map(|column| column.unique_id));
                        fds.add_strict(key.clone(), all_columns.clone());
                        fds.make_not_null(key);
                    }
                    for key in lax_keys {
                        fds.add_lax(
                            ColSet::new(key.iter().map(|column| column.unique_id)),
                            all_columns.clone(),
                        );
                    }
                }
                add_condition_facts(context, &mut fds, &source.all_conds);
                let mut declared_not_null = ColSet::default();
                for (metadata, column) in source.columns.iter().zip(&source.table_columns) {
                    if metadata.is_not_null {
                        declared_not_null.insert(column.unique_id);
                    }
                    if let Some(generated) = column.virtual_expr.as_deref() {
                        fds.add_strict(
                            ColSet::new(
                                extract_columns(generated).iter().map(|base| base.unique_id),
                            ),
                            ColSet::new([column.unique_id]),
                        );
                    }
                }
                fds.make_not_null(declared_not_null);
                fds
            }
            Self::Selection(selection) => {
                let mut fds = base_dependencies(self, context);
                let schema = match self.children().first() {
                    Some(Self::Join(join)) => join.full_schema.as_ref().or_else(|| self.schema()),
                    _ => self.schema(),
                };
                add_condition_facts(context, &mut fds, &selection.conditions);
                fds.project_cols(&schema_columns(schema));
                fds
            }
            Self::Projection(projection) => {
                let mut fds = base_dependencies(self, context);
                let Some(schema) = self.schema() else {
                    return fds;
                };
                let mut projected = schema_columns(Some(schema));
                let mut expression_not_null = ColSet::default();
                for (expression, output) in projection.exprs.iter().zip(&schema.columns) {
                    match expression {
                        Expression::Column(input) if input.unique_id != output.unique_id => {
                            fds.add_equivalence(
                                ColSet::new([input.unique_id]),
                                ColSet::new([output.unique_id]),
                            );
                        }
                        Expression::CorrelatedColumn(_) => {}
                        Expression::Constant(_) => {
                            let mut expression = expression.clone();
                            let hash = expression.hash_code().to_vec();
                            let id = fds.registered_unique_id(&hash).unwrap_or(output.unique_id);
                            fds.register_unique_id(hash, id);
                            fds.add_constants(ColSet::new([id]));
                        }
                        Expression::ScalarFunction(_) => {
                            let mut expression = expression.clone();
                            let hash = expression.hash_code().to_vec();
                            if tidb_expr::expr_util::check_non_deterministic(&expression) {
                                if fds.registered_unique_id(&hash).is_none() {
                                    fds.register_unique_id(hash, 0);
                                }
                                continue;
                            }
                            let id = fds.registered_unique_id(&hash).unwrap_or(output.unique_id);
                            fds.register_unique_id(hash, id);
                            let determinants = expression_determinants(&expression);
                            projected.union_with(&determinants);
                            if expression_is_null_rejected_by_schema(&expression, self.schema())
                                || determinants.subset_of(&fds.not_null_cols)
                            {
                                expression_not_null.insert(id);
                            }
                            fds.add_strict(determinants, ColSet::new([id]));
                            if id != output.unique_id {
                                fds.add_equivalence(
                                    ColSet::new([id]),
                                    ColSet::new([output.unique_id]),
                                );
                            }
                        }
                        _ => {}
                    }
                }
                fds.make_not_null(expression_not_null);
                projected.union_with(&fds.group_by_cols);
                fds.project_cols(&projected);
                fds
            }
            Self::Join(join) => {
                let Some(left) = self.children().first() else {
                    return FdSet::new();
                };
                let Some(right) = self.children().get(1) else {
                    return left.extract_fd_with_context(context);
                };
                let mut left_fd = left.extract_fd_with_context(context);
                let right_fd = right.extract_fd_with_context(context);
                let mut conditions = join
                    .equal_conditions
                    .iter()
                    .cloned()
                    .map(Expression::ScalarFunction)
                    .collect::<Vec<_>>();
                conditions.extend(join.other_conditions.iter().cloned());
                match join.join_type {
                    LogicalJoinType::Inner => {
                        left_fd.make_cartesian_product(&right_fd);
                        add_condition_facts(context, &mut left_fd, &conditions);
                        left_fd.not_null_cols.union_with(&right_fd.not_null_cols);
                        for (hash, id) in right_fd.hash_code_to_unique_id {
                            left_fd.hash_code_to_unique_id.entry(hash).or_insert(id);
                        }
                        left_fd.group_by_cols.union_with(&right_fd.group_by_cols);
                        left_fd.has_agg_built |= right_fd.has_agg_built;
                        left_fd
                    }
                    LogicalJoinType::Semi => {
                        add_not_null_facts(&mut left_fd, &conditions);
                        add_constant_facts(context, &mut left_fd, &join.left_conditions);
                        left_fd
                    }
                    LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter => {
                        let (
                            mut outer_fd,
                            inner_fd,
                            outer,
                            inner,
                            outer_conditions,
                            inner_conditions,
                        ) = if join.join_type == LogicalJoinType::LeftOuter {
                            (
                                left_fd,
                                right_fd,
                                left,
                                right,
                                &join.left_conditions,
                                &join.right_conditions,
                            )
                        } else {
                            (
                                right_fd,
                                left_fd,
                                right,
                                left,
                                &join.right_conditions,
                                &join.left_conditions,
                            )
                        };
                        let outer_cols = schema_columns(outer.schema());
                        let inner_cols = schema_columns(inner.schema());
                        let mut all_conditions = conditions;
                        all_conditions.extend(inner_conditions.iter().cloned());
                        all_conditions.extend(outer_conditions.iter().cloned());
                        let mut filter_fd = FdSet::new();
                        let equivalences =
                            add_condition_facts(context, &mut filter_fd, &all_conditions);

                        let mut across = 0usize;
                        let mut outer_equivalent = ColSet::default();
                        for (left, right) in equivalences {
                            if left.subset_of(&outer_cols) && right.subset_of(&inner_cols) {
                                across += 1;
                                outer_equivalent.union_with(&left);
                            } else if left.subset_of(&inner_cols) && right.subset_of(&outer_cols) {
                                across += 1;
                                outer_equivalent.union_with(&right);
                            }
                        }
                        let mut options = tidb_funcdep::OuterJoinOptions::default();
                        options.skip_rule_331 = across == 0
                            || expression_columns(outer_conditions)
                                .union(&expression_columns(&join.other_conditions))
                                .intersects(&outer_cols.difference(&outer_equivalent));
                        options.only_inner_filter = join.equal_conditions.is_empty()
                            && outer_conditions.is_empty()
                            && join.other_conditions.is_empty();
                        options.inner_is_false = options.only_inner_filter
                            && inner_conditions.iter().any(literal_is_false);
                        outer_fd.make_outer_join(
                            &inner_fd,
                            &filter_fd,
                            &outer_cols,
                            &inner_cols,
                            options,
                        );
                        outer_fd
                    }
                    _ => FdSet::new(),
                }
            }
            Self::Apply(apply) => {
                let Some(left) = self.children().first() else {
                    return FdSet::new();
                };
                let Some(right) = self.children().get(1) else {
                    return left.extract_fd_with_context(context);
                };
                let mut left_fd = left.extract_fd_with_context(context);
                let right_fd = right.extract_fd_with_context(context);
                let mut conditions = apply
                    .join
                    .equal_conditions
                    .iter()
                    .cloned()
                    .map(Expression::ScalarFunction)
                    .collect::<Vec<_>>();
                conditions.extend(apply.join.other_conditions.iter().cloned());
                let correlated_equivalences = right
                    .schema()
                    .into_iter()
                    .flat_map(|schema| &schema.columns)
                    .filter(|column| column.correlated_col_unique_id != 0)
                    .map(|column| (column.correlated_col_unique_id, column.unique_id))
                    .collect::<Vec<_>>();
                match apply.join.join_type {
                    LogicalJoinType::Inner => {
                        left_fd.make_cartesian_product(&right_fd);
                        add_condition_facts(context, &mut left_fd, &conditions);
                        for (outer, inner) in correlated_equivalences {
                            left_fd.add_equivalence(ColSet::new([outer]), ColSet::new([inner]));
                        }
                        left_fd.not_null_cols.union_with(&right_fd.not_null_cols);
                        for (hash, id) in right_fd.hash_code_to_unique_id {
                            left_fd.hash_code_to_unique_id.entry(hash).or_insert(id);
                        }
                        left_fd.group_by_cols.union_with(&right_fd.group_by_cols);
                        left_fd.has_agg_built |= right_fd.has_agg_built;
                        left_fd
                    }
                    LogicalJoinType::Semi => {
                        add_not_null_facts(&mut left_fd, &conditions);
                        add_constant_facts(context, &mut left_fd, &apply.join.left_conditions);
                        for (outer, inner) in correlated_equivalences {
                            left_fd.add_equivalence(ColSet::new([outer]), ColSet::new([inner]));
                        }
                        left_fd
                    }
                    LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter => {
                        let (
                            mut outer_fd,
                            inner_fd,
                            outer,
                            inner,
                            outer_conditions,
                            inner_conditions,
                        ) = if apply.join.join_type == LogicalJoinType::LeftOuter {
                            (
                                left_fd,
                                right_fd,
                                left,
                                right,
                                &apply.join.left_conditions,
                                &apply.join.right_conditions,
                            )
                        } else {
                            (
                                right_fd,
                                left_fd,
                                right,
                                left,
                                &apply.join.right_conditions,
                                &apply.join.left_conditions,
                            )
                        };
                        let outer_cols = schema_columns(outer.schema());
                        let inner_cols = schema_columns(inner.schema());
                        conditions.extend(inner_conditions.iter().cloned());
                        conditions.extend(outer_conditions.iter().cloned());
                        let mut filter_fd = FdSet::new();
                        let mut equivalences =
                            add_condition_facts(context, &mut filter_fd, &conditions);
                        for (outer, inner) in correlated_equivalences {
                            let outer = ColSet::new([outer]);
                            let inner = ColSet::new([inner]);
                            filter_fd.add_equivalence(outer.clone(), inner.clone());
                            equivalences.push((outer, inner));
                        }

                        let mut across = 0usize;
                        let mut outer_equivalent = ColSet::default();
                        for (left, right) in equivalences {
                            if left.subset_of(&outer_cols) && right.subset_of(&inner_cols) {
                                across += 1;
                                outer_equivalent.union_with(&left);
                            } else if left.subset_of(&inner_cols) && right.subset_of(&outer_cols) {
                                across += 1;
                                outer_equivalent.union_with(&right);
                            }
                        }
                        let mut options = tidb_funcdep::OuterJoinOptions::default();
                        options.skip_rule_331 = across == 0
                            || expression_columns(outer_conditions)
                                .union(&expression_columns(&apply.join.other_conditions))
                                .intersects(&outer_cols.difference(&outer_equivalent));
                        options.only_inner_filter = apply.join.equal_conditions.is_empty()
                            && outer_conditions.is_empty()
                            && apply.join.other_conditions.is_empty();
                        options.inner_is_false = options.only_inner_filter
                            && inner_conditions.iter().any(literal_is_false);
                        outer_fd.make_outer_join(
                            &inner_fd,
                            &filter_fd,
                            &outer_cols,
                            &inner_cols,
                            options,
                        );
                        outer_fd
                    }
                    _ => FdSet::new(),
                }
            }
            Self::Aggregation(aggregation) => {
                let mut fds = base_dependencies(self, context);
                let mut group_by = ColSet::default();
                let mut group_by_outputs = ColSet::default();
                let mut expression_not_null = ColSet::default();
                for expression in &aggregation.group_by_items {
                    match expression {
                        Expression::Column(column) => group_by.insert(column.unique_id),
                        Expression::ScalarFunction(_) => {
                            let mut expression_for_hash = expression.clone();
                            let hash = expression_for_hash.hash_code().to_vec();
                            let id = if let Some(id) = fds.registered_unique_id(&hash) {
                                id
                            } else if let Some(id) =
                                context.projected_expression_ids.get(&hash).copied()
                            {
                                fds.register_unique_id(hash, id);
                                id
                            } else {
                                fd_object_id(context, &mut fds, expression)
                                    .expect("a scalar group-by expression has an extended column")
                            };
                            group_by.insert(id);
                            let determinants = expression_determinants(expression);
                            group_by_outputs.union_with(&determinants);
                            if expression_is_null_rejected_by_schema(expression, self.schema())
                                || determinants.subset_of(&fds.not_null_cols)
                            {
                                expression_not_null.insert(id);
                            }
                            fds.add_strict(determinants, ColSet::new([id]));
                        }
                        _ => {}
                    }
                }
                if group_by.is_empty() {
                    group_by.insert(0);
                }
                if let Some(schema) = self.schema() {
                    for (function, output) in aggregation.agg_funcs.iter().zip(&schema.columns) {
                        if function.name() != AGG_FUNC_FIRST_ROW {
                            fds.add_strict(group_by.clone(), ColSet::new([output.unique_id]));
                        }
                    }
                    if !aggregation.group_by_items.is_empty() {
                        fds.project_cols(
                            &schema_columns(Some(schema))
                                .union(&group_by_outputs)
                                .union(&group_by),
                        );
                        fds.make_not_null(expression_not_null);
                    }
                }
                fds.group_by_cols = group_by;
                fds.has_agg_built = true;
                fds
            }
            Self::UnionAll(_) | Self::PartitionUnionAll(_) => {
                let child_fds = self
                    .children()
                    .iter()
                    .map(|child| child.extract_fd_with_context(context))
                    .collect::<Vec<_>>();
                let refs = child_fds.iter().collect::<Vec<_>>();
                let mut result = FdSet::new();
                let mut not_null = schema_columns(self.schema());
                for child in &child_fds {
                    not_null.intersection_with(&child.not_null_cols);
                }
                result.make_not_null(not_null);
                for class in tidb_funcdep::find_common_equiv_classes(&refs) {
                    result.add_equivalence_union(class);
                }
                result
            }
            _ => base_dependencies(self, context),
        }
    }
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode, BaseFuncDesc};
    use tidb_expr::column::{Column, CorrelatedColumn};
    use tidb_expr::constant::Constant;
    use tidb_expr::scalar_function::ScalarFunction;

    use super::*;
    use crate::logical::aggregation::LogicalAggregation;
    use crate::logical::apply::LogicalApply;
    use crate::logical::data_source::{DataSource, DataSourceColumn};
    use crate::logical::join::LogicalJoin;
    use crate::logical::projection::LogicalProjection;
    use crate::logical::selection::LogicalSelection;
    use crate::logical::union_all::LogicalUnionAll;
    use crate::logical::BaseLogicalPlan;
    use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};

    fn column(id: i64) -> Column {
        Column::new(id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn schema(ids: &[i64]) -> Schema {
        Schema::new(ids.iter().copied().map(column).collect())
    }

    fn scalar(name: &str, args: Vec<Expression>) -> ScalarFunction {
        ScalarFunction::new(
            CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        )
    }

    fn aggregate(name: &str, args: Vec<Expression>) -> AggFuncDesc {
        AggFuncDesc {
            base: BaseFuncDesc {
                name: name.to_owned(),
                args,
                ret_type: FieldType::new(FieldTypeCode::LongLong),
            },
            mode: AggFunctionMode::Complete,
            has_distinct: false,
            order_by_items: Vec::new(),
            grouping_id: 0,
        }
    }

    fn data_source(ids: &[i64], primary: usize) -> LogicalPlan {
        let mut source = DataSource::new(BaseLogicalPlan::default(), 1, "t");
        source.table_columns = ids.iter().copied().map(column).collect();
        source.columns = ids
            .iter()
            .enumerate()
            .map(|(index, _)| DataSourceColumn {
                is_primary_key: index == primary,
                is_not_null: index == primary,
                ..DataSourceColumn::default()
            })
            .collect();
        source.pk_is_handle = true;
        source.base.base.set_schema(Some(schema(ids)));
        LogicalPlan::DataSource(source)
    }

    fn keyless_data_source(ids: &[i64]) -> LogicalPlan {
        let mut source = DataSource::new(BaseLogicalPlan::default(), 1, "t");
        source.table_columns = ids.iter().copied().map(column).collect();
        source.columns = ids.iter().map(|_| DataSourceColumn::default()).collect();
        source.base.base.set_schema(Some(schema(ids)));
        LogicalPlan::DataSource(source)
    }

    fn nullable_unique_source(with_not_null_filter: bool) -> LogicalPlan {
        let mut source = DataSource::new(BaseLogicalPlan::default(), 1, "t");
        source.table_columns = vec![column(1), column(2), column(3)];
        source.columns = vec![
            DataSourceColumn {
                id: 1,
                name: "a".to_owned(),
                ..DataSourceColumn::default()
            },
            DataSourceColumn {
                id: 2,
                name: "b".to_owned(),
                is_not_null: true,
                ..DataSourceColumn::default()
            },
            DataSourceColumn {
                id: 3,
                name: "c".to_owned(),
                ..DataSourceColumn::default()
            },
        ];
        source.indexes = vec![SourceIndex {
            id: 1,
            name: "bc".to_owned(),
            columns: vec![
                SourceIndexColumn {
                    name: "b".to_owned(),
                    offset: 1,
                    length: -1,
                },
                SourceIndexColumn {
                    name: "c".to_owned(),
                    offset: 2,
                    length: -1,
                },
            ],
            unique: true,
            is_public: true,
            is_visible: true,
            ..SourceIndex::default()
        }];
        if with_not_null_filter {
            source.all_conds.push(Expression::ScalarFunction(scalar(
                "eq",
                vec![
                    Expression::Column(column(3)),
                    Expression::Constant(Constant::new_one()),
                ],
            )));
        }
        source.base.base.set_schema(Some(schema(&[1, 2, 3])));
        LogicalPlan::DataSource(source)
    }

    #[test]
    fn inner_join_extract_fd_feeds_equivalent_partition_keys() {
        let left = data_source(&[1, 2], 0);
        let right = data_source(&[10, 11], 0);
        let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
        join.base.set_children(vec![left, right]);
        join.base.base.set_schema(Some(schema(&[1, 2, 10, 11])));
        join.equal_conditions.push(ScalarFunction::new(
            CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(column(1)),
                Expression::Column(column(10)),
            ],
        ));

        let fd = LogicalPlan::Join(join).extract_fd();
        assert!(fd.closure_of_equivalence(&ColSet::new([1])).has(10));
        assert!(fd.in_closure(&ColSet::new([10]), &ColSet::new([2])));
    }

    #[test]
    fn semi_join_keeps_only_go_source_facts() {
        let left = data_source(&[1, 2], 0);
        let right = data_source(&[10, 11], 0);
        let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::Semi);
        join.base.set_children(vec![left, right]);
        join.base.base.set_schema(Some(schema(&[1, 2])));
        join.equal_conditions.push(scalar(
            "eq",
            vec![
                Expression::Column(column(1)),
                Expression::Column(column(10)),
            ],
        ));
        join.left_conditions.push(Expression::ScalarFunction(scalar(
            "eq",
            vec![
                Expression::Column(column(2)),
                Expression::Constant(Constant::new_one()),
            ],
        )));

        let fd = LogicalPlan::Join(join).extract_fd();
        assert!(!fd.closure_of_equivalence(&ColSet::new([1])).has(10));
        assert!(fd.constant_cols().has(2));
    }

    #[test]
    fn projection_derives_scalar_and_constant_dependencies() {
        let child = data_source(&[1, 2], 0);
        let mut projection = LogicalProjection::new(
            BaseLogicalPlan::default(),
            vec![
                Expression::Column(column(1)),
                Expression::ScalarFunction(scalar(
                    "plus",
                    vec![
                        Expression::Column(column(2)),
                        Expression::Constant(Constant::new_one()),
                    ],
                )),
                Expression::Constant(Constant::new_one()),
            ],
        );
        projection.base.set_children(vec![child]);
        projection.base.base.set_schema(Some(schema(&[1, 5, 6])));

        let fd = LogicalPlan::Projection(projection).extract_fd();
        assert!(fd.in_closure(&ColSet::new([2]), &ColSet::new([5])));
        assert!(fd.in_closure(&ColSet::new([1]), &ColSet::new([5])));
        assert!(fd.constant_cols().has(6));
    }

    #[test]
    fn scalar_null_rejection_uses_the_whole_go_operator_schema() {
        let expression = Expression::ScalarFunction(scalar(
            "coalesce",
            vec![Expression::Column(column(1)), Expression::Column(column(2))],
        ));
        let schema = schema(&[1, 2]);

        assert!(expression_is_null_rejected_by_schema(
            &expression,
            Some(&schema)
        ));
        assert!(!tidb_funcdep::null_reject::is_null_rejected(&expression, 1));
        assert!(!tidb_funcdep::null_reject::is_null_rejected(&expression, 2));
    }

    #[test]
    fn scalar_projection_keeps_correlated_column_as_a_determinant() {
        let child = data_source(&[1, 2], 0);
        let correlated = Expression::CorrelatedColumn(CorrelatedColumn {
            column: column(10),
            data: None,
        });
        let mut projection = LogicalProjection::new(
            BaseLogicalPlan::default(),
            vec![Expression::ScalarFunction(scalar(
                "plus",
                vec![Expression::Column(column(2)), correlated],
            ))],
        );
        projection.base.set_children(vec![child]);
        projection.base.base.set_schema(Some(schema(&[5])));

        let fd = LogicalPlan::Projection(projection).extract_fd();
        assert!(fd.in_closure(&ColSet::new([2, 10]), &ColSet::new([5])));
        assert!(!fd.in_closure(&ColSet::new([2]), &ColSet::new([5])));
    }

    #[test]
    fn datasource_filter_promotes_a_nullable_unique_key() {
        let lax = nullable_unique_source(false).extract_fd();
        assert!(lax.closure_of_lax(&ColSet::new([2, 3])).has(1));
        assert!(!lax.closure_of_strict(&ColSet::new([2, 3])).has(1));

        let strict = nullable_unique_source(true).extract_fd();
        assert!(strict.closure_of_strict(&ColSet::new([2, 3])).has(1));
        assert!(strict.constant_cols().has(3));

        let mut latest_dropped = nullable_unique_source(false);
        let LogicalPlan::DataSource(source) = &mut latest_dropped else {
            unreachable!("helper returns a data source")
        };
        source.fd_latest_public_index_ids = Some(std::collections::BTreeSet::new());
        let latest_dropped = latest_dropped.extract_fd();
        assert!(!latest_dropped.closure_of_lax(&ColSet::new([2, 3])).has(1));

        let mut lookup_failed = nullable_unique_source(true);
        let LogicalPlan::DataSource(source) = &mut lookup_failed else {
            unreachable!("helper returns a data source")
        };
        source.pk_is_handle = true;
        source.columns[0].is_primary_key = true;
        source.columns[0].is_not_null = true;
        source.fd_latest_index_lookup_failed = true;
        let lookup_failed = lookup_failed.extract_fd();
        assert!(lookup_failed.in_closure(&ColSet::new([1]), &ColSet::new([2, 3])));
        assert!(!lookup_failed.closure_of_lax(&ColSet::new([2, 3])).has(1));
        assert!(!lookup_failed.constant_cols().has(3));
    }

    #[test]
    fn condition_facts_use_the_complete_go_extractors() {
        let mut fds = FdSet::new();
        let one = Expression::Constant(Constant::new_one());
        let conditions = vec![
            Expression::ScalarFunction(scalar(
                "nulleq",
                vec![
                    Expression::Column(column(1)),
                    Expression::CorrelatedColumn(CorrelatedColumn {
                        column: column(10),
                        data: None,
                    }),
                ],
            )),
            Expression::ScalarFunction(scalar(
                "in",
                vec![Expression::Column(column(2)), one.clone(), one],
            )),
            Expression::ScalarFunction(scalar(
                "in",
                vec![Expression::Column(column(3)), Expression::Column(column(4))],
            )),
        ];

        let mut context = FdExtractionContext {
            next_column_id: 11,
            ..FdExtractionContext::default()
        };
        add_condition_facts(&mut context, &mut fds, &conditions);

        assert!(fds.constant_cols().has(1));
        assert!(fds.constant_cols().has(2));
        assert!(fds.closure_of_equivalence(&ColSet::new([3])).has(4));

        let mut constant_filter = FdSet::new();
        add_condition_facts(
            &mut context,
            &mut constant_filter,
            &[Expression::Constant(Constant::new_zero())],
        );
        assert!(constant_filter.not_null_cols().is_empty());
    }

    #[test]
    fn aggregation_adds_only_real_aggregate_outputs() {
        let child = data_source(&[1, 2], 0);
        let mut aggregation = LogicalAggregation::new(
            BaseLogicalPlan::default(),
            vec![
                aggregate("sum", vec![Expression::Column(column(1))]),
                aggregate("firstrow", vec![Expression::Column(column(2))]),
            ],
            vec![Expression::Column(column(2))],
        );
        aggregation.base.set_children(vec![child]);
        aggregation.base.base.set_schema(Some(schema(&[5, 2])));

        let fd = LogicalPlan::Aggregation(aggregation).extract_fd();
        assert_eq!(fd.group_by_cols().to_string(), "(2)");
        assert!(fd.in_closure(&ColSet::new([2]), &ColSet::new([5])));
        assert!(fd.has_agg_built());
    }

    #[test]
    fn scalar_group_by_keeps_correlated_column_as_a_determinant() {
        let child = data_source(&[1, 2], 0);
        let group_by = Expression::ScalarFunction(scalar(
            "plus",
            vec![
                Expression::Column(column(2)),
                Expression::CorrelatedColumn(CorrelatedColumn {
                    column: column(10),
                    data: None,
                }),
            ],
        ));
        let mut aggregation = LogicalAggregation::new(
            BaseLogicalPlan::default(),
            vec![aggregate("sum", vec![Expression::Column(column(1))])],
            vec![group_by],
        );
        aggregation.base.set_children(vec![child]);
        aggregation.base.base.set_schema(Some(schema(&[5])));

        let fd = LogicalPlan::Aggregation(aggregation).extract_fd();
        let (extended, present) = fd.group_by_cols().next(0);
        assert!(present, "scalar group-by extended column");
        assert!(fd.in_closure(&ColSet::new([2, 10]), &ColSet::new([extended])));
        assert!(!fd.in_closure(&ColSet::new([2]), &ColSet::new([extended])));
    }

    #[test]
    fn scalar_group_by_reuses_the_upper_projection_column_id() {
        let child = data_source(&[1, 2, 3], 0);
        let scalar_group_by = Expression::ScalarFunction(scalar(
            "plus",
            vec![Expression::Column(column(2)), Expression::Column(column(3))],
        ));
        let mut aggregation = LogicalAggregation::new(
            BaseLogicalPlan::default(),
            vec![aggregate("sum", vec![Expression::Column(column(1))])],
            vec![scalar_group_by.clone()],
        );
        aggregation.base.set_children(vec![child]);
        aggregation.base.base.set_schema(Some(schema(&[5])));

        let mut projection =
            LogicalProjection::new(BaseLogicalPlan::default(), vec![scalar_group_by]);
        projection.fd_expression_ids_registered = true;
        projection
            .base
            .set_children(vec![LogicalPlan::Aggregation(aggregation)]);
        projection.base.base.set_schema(Some(schema(&[6])));

        let fd = LogicalPlan::Projection(projection).extract_fd();
        assert!(fd.group_by_cols().has(6));
        assert!(fd.in_closure(&ColSet::new([2, 3]), &ColSet::new([6])));
    }

    #[test]
    fn scalar_group_by_allocates_a_separate_id_when_the_go_flag_is_off() {
        let child = data_source(&[1, 2, 3], 0);
        let scalar_group_by = Expression::ScalarFunction(scalar(
            "plus",
            vec![Expression::Column(column(2)), Expression::Column(column(3))],
        ));
        let mut aggregation = LogicalAggregation::new(
            BaseLogicalPlan::default(),
            vec![aggregate("sum", vec![Expression::Column(column(1))])],
            vec![scalar_group_by.clone()],
        );
        aggregation.base.set_children(vec![child]);
        aggregation.base.base.set_schema(Some(schema(&[5])));

        let mut projection =
            LogicalProjection::new(BaseLogicalPlan::default(), vec![scalar_group_by]);
        projection
            .base
            .set_children(vec![LogicalPlan::Aggregation(aggregation)]);
        projection.base.base.set_schema(Some(schema(&[6])));

        let fd = LogicalPlan::Projection(projection).extract_fd();
        assert!(!fd.group_by_cols().has(6));
        assert!(fd.in_closure(fd.group_by_cols(), &ColSet::new([6])));
    }

    #[test]
    fn selection_and_union_preserve_the_source_equivalence_class() {
        let selected = || {
            let child = data_source(&[1, 2], 0);
            let mut selection = LogicalSelection::new(
                BaseLogicalPlan::default(),
                vec![Expression::ScalarFunction(scalar(
                    "eq",
                    vec![Expression::Column(column(1)), Expression::Column(column(2))],
                ))],
            );
            selection.base.set_children(vec![child]);
            selection.base.base.set_schema(Some(schema(&[1, 2])));
            LogicalPlan::Selection(selection)
        };
        let mut union = LogicalUnionAll::default();
        union.base.set_children(vec![selected(), selected()]);
        union.base.base.set_schema(Some(schema(&[1, 2])));

        let fd = LogicalPlan::UnionAll(union).extract_fd();
        assert_eq!(
            fd.closure_of_equivalence(&ColSet::new([1])).to_string(),
            "(1,2)"
        );
        assert!(fd.not_null_cols().has(1));
        assert!(fd.not_null_cols().has(2));
    }

    #[test]
    fn outer_join_keeps_inner_key_and_builds_the_combined_key() {
        let left = data_source(&[1, 2], 0);
        let right = data_source(&[10, 11], 0);
        let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::LeftOuter);
        join.base.set_children(vec![left, right]);
        join.base.base.set_schema(Some(schema(&[1, 2, 10, 11])));

        let fd = LogicalPlan::Join(join).extract_fd();
        assert!(fd.in_closure(&ColSet::new([10]), &ColSet::new([11])));
        assert!(fd.in_closure(&ColSet::new([1, 10]), &ColSet::new([2, 11])));
        assert!(!fd.not_null_cols().has(10));
    }

    #[test]
    fn outer_join_rule_331_uses_original_equality_pairs() {
        let left = keyless_data_source(&[1, 2]);
        let right = keyless_data_source(&[10, 11]);
        let mut join = LogicalJoin::new(BaseLogicalPlan::default(), LogicalJoinType::LeftOuter);
        join.base.set_children(vec![left, right]);
        join.base.base.set_schema(Some(schema(&[1, 2, 10, 11])));
        join.equal_conditions.push(scalar(
            "eq",
            vec![
                Expression::Column(column(2)),
                Expression::Column(column(10)),
            ],
        ));
        join.left_conditions.push(Expression::ScalarFunction(scalar(
            "eq",
            vec![Expression::Column(column(1)), Expression::Column(column(2))],
        )));

        let fd = LogicalPlan::Join(join).extract_fd();
        assert!(!fd.in_closure(&ColSet::new([1, 2]), &ColSet::new([10])));
    }

    #[test]
    fn apply_connects_reprojected_correlated_columns() {
        let left = data_source(&[1, 2], 0);
        let mut right = data_source(&[10, 11], 0);
        let mut right_schema = right.schema().expect("schema").clone();
        right_schema.columns[0].correlated_col_unique_id = 1;
        right.base_mut().base.set_schema(Some(right_schema));
        let mut apply = LogicalApply::new(BaseLogicalPlan::default(), LogicalJoinType::Inner);
        apply.join.base.set_children(vec![left, right]);
        apply
            .join
            .base
            .base
            .set_schema(Some(schema(&[1, 2, 10, 11])));

        let fd = LogicalPlan::Apply(apply).extract_fd();
        assert!(fd.closure_of_equivalence(&ColSet::new([1])).has(10));
    }

    #[test]
    fn outer_apply_hides_correlation_until_the_inner_side_is_null_rejected() {
        let left = data_source(&[1, 2], 0);
        let mut right = data_source(&[10, 11], 0);
        let mut right_schema = right.schema().expect("schema").clone();
        right_schema.columns[0].correlated_col_unique_id = 1;
        right.base_mut().base.set_schema(Some(right_schema));
        let mut apply = LogicalApply::new(BaseLogicalPlan::default(), LogicalJoinType::LeftOuter);
        apply.join.base.set_children(vec![left, right]);
        apply
            .join
            .base
            .base
            .set_schema(Some(schema(&[1, 2, 10, 11])));

        let mut fd = LogicalPlan::Apply(apply).extract_fd();
        assert!(!fd.closure_of_equivalence(&ColSet::new([1])).has(10));
        fd.make_not_null(ColSet::new([10]));
        assert!(fd.closure_of_equivalence(&ColSet::new([1])).has(10));
    }
}
