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

//! Go logicalop.ExtractFD and planner/util/funcdep_misc.go. Inputs are the
//! resolved logical tree, not a second walk of written SQL.

use super::{DataSource, LogicalJoin, LogicalPlan};
use crate::expression_rewriter::ColumnIdAllocator;
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanError;
use std::collections::{HashMap, HashSet};
use tidb_datatype::FieldTypeFlags;
use tidb_expr::column::Column;
use tidb_expr::expr_util::{
    check_non_deterministic, extract_constant_eq_columns_or_scalar, extract_equivalence_columns,
};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};
use tidb_funcdep::{ColSet, FdSet, OuterJoinOptions};

/// GetLatestIndexInfo: None means unchanged; Some contains the current public IDs.
pub type LatestIndexes<'a> = dyn Fn(i64) -> Result<Option<HashSet<i64>>, PlanError> + 'a;

/// Statement inputs used by Go's FD extraction.
pub struct FdContext<'a> {
    /// The same column allocator used by PlanBuilder, never a fresh counter.
    pub column_ids: &'a ColumnIdAllocator,
    /// Go MapHashCode2UniqueID4ExtendedCol.
    pub extended_columns: HashMap<Vec<u8>, i64>,
    /// Go SessionVars.ConnectionID; zero denotes a planner-only context.
    pub connection_id: u64,
    /// Go SessionVars.IsIsolation(ReadCommitted).
    pub read_committed: bool,
    /// Domain-owned metadata lookup for live RC or locking reads.
    pub latest_indexes: Option<&'a LatestIndexes<'a>>,
}

impl<'a> FdContext<'a> {
    /// A planner-only context (Go's ConnectionID == 0).
    pub fn new(column_ids: &'a ColumnIdAllocator) -> Self {
        Self {
            column_ids,
            extended_columns: HashMap::new(),
            connection_id: 0,
            read_committed: false,
            latest_indexes: None,
        }
    }
}

fn schema(plan: &LogicalPlan) -> Result<&Schema, PlanError> {
    plan.schema()
        .ok_or_else(|| PlanError::internal("FD extraction requires a resolved schema"))
}
fn ids(columns: &[Column]) -> ColSet {
    ColSet::of(columns.iter().map(|column| column.unique_id))
}
fn references(expressions: &[Expression]) -> ColSet {
    ColSet::of(
        expressions
            .iter()
            .flat_map(extract_columns)
            .map(|column| column.unique_id),
    )
}
fn not_null(conditions: &[Expression]) -> ColSet {
    let mut result = ColSet::default();
    for condition in conditions {
        for column in extract_columns(condition) {
            if tidb_funcdep::null_reject::is_null_rejected(condition, column.unique_id) {
                result.insert(column.unique_id);
            }
        }
    }
    result
}
fn object_id(expression: &Expression, fds: &mut FdSet, ctx: &FdContext<'_>) -> i64 {
    if let Expression::Column(column) = expression {
        return column.unique_id;
    }
    let hash = expression.clone().hash_code().to_vec();
    *fds.hash_code_to_unique_id
        .entry(hash)
        .or_insert_with(|| ctx.column_ids.alloc())
}
fn constants(conditions: &[Expression], fds: &mut FdSet, ctx: &FdContext<'_>) -> ColSet {
    ColSet::of(
        extract_constant_eq_columns_or_scalar(Vec::new(), conditions)
            .iter()
            .map(|expr| object_id(expr, fds, ctx)),
    )
}
fn equivalences(
    conditions: &[Expression],
    fds: &mut FdSet,
    ctx: &FdContext<'_>,
) -> Vec<(ColSet, ColSet)> {
    extract_equivalence_columns(Vec::new(), conditions)
        .iter()
        .map(|[left, right]| {
            (
                ColSet::of([object_id(left, fds, ctx)]),
                ColSet::of([object_id(right, fds, ctx)]),
            )
        })
        .collect()
}
fn apply_conditions(fds: &mut FdSet, conditions: &[Expression], ctx: &FdContext<'_>) {
    let not_null = not_null(conditions);
    let constants = constants(conditions, fds, ctx);
    let equivalents = equivalences(conditions, fds, ctx);
    fds.make_not_null(not_null);
    fds.add_constants(constants);
    for (left, right) in equivalents {
        fds.add_equivalence(left, right);
    }
}

fn data_source(source: &DataSource, ctx: &FdContext<'_>) -> Result<FdSet, PlanError> {
    let columns = &source.table_columns;
    if columns.is_empty() && !source.columns.is_empty() {
        return Err(PlanError::internal(
            "DataSource TblCols are missing before FD extraction",
        ));
    }
    let all = ids(columns);
    let mut fds = FdSet::new();
    if source.pk_is_handle {
        let key = ColSet::of(
            columns
                .iter()
                .filter(|col| {
                    col.ret_type
                        .as_ref()
                        .is_some_and(|ft| ft.flags() & FieldTypeFlags::PRI_KEY != 0)
                })
                .map(|col| col.unique_id),
        );
        fds.add_strict(key.clone(), all.clone());
        fds.make_not_null(key);
    }
    let latest = if ctx.connection_id > 0 && (ctx.read_committed || source.is_for_update_read) {
        let lookup = ctx
            .latest_indexes
            .ok_or_else(|| PlanError::internal("FD latest-index metadata lookup is missing"))?;
        match lookup(source.table_id) {
            Ok(latest) => latest,
            // Go keeps just the int-PK dependencies when domain lookup fails.
            Err(_) => return Ok(fds),
        }
    } else {
        None
    };
    for index in &source.indexes {
        if !index.is_public
            || (source.is_for_update_read
                && latest.as_ref().is_some_and(|set| !set.contains(&index.id)))
        {
            continue;
        }
        let mut key = ColSet::default();
        let mut definite = true;
        for part in &index.columns {
            let column = columns
                .get(part.offset)
                .ok_or_else(|| PlanError::internal("FD index offset is outside TblCols"))?;
            key.insert(column.unique_id);
            definite &= column
                .ret_type
                .as_ref()
                .is_some_and(|ft| ft.flags() & FieldTypeFlags::NOT_NULL != 0);
        }
        if index.primary || (index.unique && definite) {
            fds.add_strict(key.clone(), all.clone());
            fds.make_not_null(key);
        } else if index.unique {
            fds.add_lax(key, all.clone());
        }
    }
    apply_conditions(&mut fds, &source.all_conds, ctx);
    let mut definite = ColSet::default();
    for column in columns {
        if let Some(expression) = &column.virtual_expr {
            fds.add_strict(
                ColSet::of(extract_columns(expression).iter().map(|col| col.unique_id)),
                ColSet::of([column.unique_id]),
            );
        }
        if column
            .ret_type
            .as_ref()
            .is_some_and(|ft| ft.flags() & FieldTypeFlags::NOT_NULL != 0)
        {
            definite.insert(column.unique_id);
        }
    }
    fds.make_not_null(definite);
    Ok(fds)
}

/// Go's three join extraction bodies; Apply supplies extra correlations.
pub fn join(join: &LogicalJoin, ctx: &FdContext<'_>, apply: bool) -> Result<FdSet, PlanError> {
    use LogicalJoinType::*;
    if !matches!(join.join_type, Inner | LeftOuter | RightOuter | Semi) {
        return Ok(FdSet::new());
    }
    let [left, right] = join.base.children() else {
        return Err(PlanError::internal("FD join requires two children"));
    };
    let extra = if apply {
        schema(right)?
            .columns
            .iter()
            .filter(|column| column.correlated_col_unique_id != 0)
            .map(|column| {
                (
                    ColSet::of([column.correlated_col_unique_id]),
                    ColSet::of([column.unique_id]),
                )
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let mut conditions = join
        .equal_conditions
        .iter()
        .cloned()
        .map(Expression::ScalarFunction)
        .collect::<Vec<_>>();
    conditions.extend(join.other_conditions.iter().cloned());
    if join.join_type == Inner {
        // Go derives the right child before the left child for inner joins.
        let right = extract(right, ctx)?;
        let mut fds = extract(left, ctx)?;
        fds.make_cartesian_product(&right);
        apply_conditions(&mut fds, &conditions, ctx);
        for (left, right) in extra {
            fds.add_equivalence(left, right);
        }
        fds.not_null_cols.union_with(&right.not_null_cols);
        for (hash, id) in right.hash_code_to_unique_id {
            fds.hash_code_to_unique_id.entry(hash).or_insert(id);
        }
        fds.group_by_cols.union_with(&right.group_by_cols);
        fds.has_agg_built |= right.has_agg_built;
        return Ok(fds);
    }
    let mut outer = extract(left, ctx)?;
    let mut inner = extract(right, ctx)?;
    if join.join_type == Semi {
        let constants = constants(&join.left_conditions, &mut outer, ctx);
        for (left, right) in extra {
            outer.add_equivalence(left, right);
        }
        outer.make_not_null(not_null(&conditions));
        outer.add_constants(constants);
        return Ok(outer);
    }
    let mut outer_cols = ids(&schema(left)?.columns);
    let mut inner_cols = ids(&schema(right)?.columns);
    let (outer_conditions, inner_conditions) = if join.join_type == RightOuter {
        std::mem::swap(&mut outer, &mut inner);
        std::mem::swap(&mut outer_cols, &mut inner_cols);
        (&join.right_conditions, &join.left_conditions)
    } else {
        (&join.left_conditions, &join.right_conditions)
    };
    conditions.extend(inner_conditions.iter().cloned());
    conditions.extend(outer_conditions.iter().cloned());
    let mut filter = FdSet::new();
    let consts = constants(&conditions, &mut filter, ctx);
    let mut equivs = equivalences(&conditions, &mut filter, ctx);
    filter.add_constants(consts);
    equivs.extend(extra);
    let mut outer_equivalents = ColSet::default();
    let mut across = false;
    for (left, right) in equivs {
        filter.add_equivalence(left.clone(), right.clone());
        if left.subset_of(&outer_cols) && right.subset_of(&inner_cols) {
            across = true;
            outer_equivalents.union_with(&left);
        } else if left.subset_of(&inner_cols) && right.subset_of(&outer_cols) {
            across = true;
            outer_equivalents.union_with(&right);
        }
    }
    filter.make_not_null(not_null(&conditions));
    let only_inner_filter = join.equal_conditions.is_empty()
        && outer_conditions.is_empty()
        && join.other_conditions.is_empty();
    let outer_references = references(outer_conditions).union(&references(&join.other_conditions));
    let options = OuterJoinOptions {
        skip_rule_331: !across
            || outer_references.intersects(&outer_cols.difference(&outer_equivalents)),
        only_inner_filter,
        inner_is_false: only_inner_filter
            && inner_conditions.iter().any(|expr| {
                let Expression::Constant(value) = expr else {
                    return false;
                };
                value
                    .literal_value()
                    .is_some_and(|value| tidb_expr::truthy_of(value).ok() == Some(Some(false)))
            }),
    };
    outer.make_outer_join(&inner, &filter, &outer_cols, &inner_cols, options);
    Ok(outer)
}

/// Derive from the current tree. No AST reconstruction or cross-statement cache.
pub fn extract(plan: &LogicalPlan, ctx: &FdContext<'_>) -> Result<FdSet, PlanError> {
    match plan {
        LogicalPlan::DataSource(source) => return data_source(source, ctx),
        LogicalPlan::Join(operator) => return join(operator, ctx, false),
        LogicalPlan::Apply(operator) => return join(&operator.join, ctx, true),
        _ => {}
    }
    let children = plan
        .children()
        .iter()
        .map(|child| extract(child, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    if matches!(
        plan,
        LogicalPlan::UnionAll(_) | LogicalPlan::PartitionUnionAll(_)
    ) {
        let mut result = FdSet::new();
        let mut not_null = ids(&schema(plan)?.columns);
        for child in &children {
            not_null.intersection_with(&child.not_null_cols);
        }
        result.make_not_null(not_null);
        for equiv in tidb_funcdep::find_common_equiv_classes(&children.iter().collect::<Vec<_>>()) {
            result.add_equivalence_union(equiv);
        }
        return Ok(result);
    }
    let mut fds = FdSet::new();
    for child in &children {
        fds.add_from(child);
    }
    match plan {
        LogicalPlan::Selection(operator) => {
            apply_conditions(&mut fds, &operator.conditions, ctx);
            let visible = match plan.children().first() {
                Some(LogicalPlan::Join(join)) => join.full_schema.as_ref().unwrap_or(schema(plan)?),
                _ => schema(plan)?,
            };
            fds.project_cols(&ids(&visible.columns));
        }
        LogicalPlan::Projection(operator) => {
            let outputs = &schema(plan)?.columns;
            if outputs.len() != operator.exprs.len() {
                return Err(PlanError::internal(
                    "FD projection schema/expressions differ",
                ));
            }
            let mut visible = ids(outputs);
            let mut not_null = ColSet::default();
            for (expression, output) in operator.exprs.iter().zip(outputs) {
                match expression {
                    Expression::Column(column) => {
                        if column.unique_id != output.unique_id {
                            fds.add_equivalence(
                                ColSet::of([column.unique_id]),
                                ColSet::of([output.unique_id]),
                            );
                        }
                    }
                    Expression::CorrelatedColumn(_) => {}
                    Expression::Constant(_) => {
                        let hash = expression.clone().hash_code().to_vec();
                        let id = *fds
                            .hash_code_to_unique_id
                            .entry(hash)
                            .or_insert(output.unique_id);
                        fds.add_constants(ColSet::of([id]));
                    }
                    Expression::ScalarFunction(_) => {
                        let hash = expression.clone().hash_code().to_vec();
                        if check_non_deterministic(expression) {
                            // Go registers the zero-valued local ID on this branch.
                            fds.hash_code_to_unique_id.entry(hash).or_insert(0);
                            continue;
                        }
                        let id = if let Some(id) = fds.hash_code_to_unique_id.get(&hash).copied() {
                            fds.add_equivalence(ColSet::of([id]), ColSet::of([output.unique_id]));
                            id
                        } else {
                            fds.hash_code_to_unique_id.insert(hash, output.unique_id);
                            output.unique_id
                        };
                        let determinants = scalar_dependencies(expression);
                        visible.union_with(&determinants);
                        if tidb_funcdep::null_reject::is_null_rejected_by(
                            expression,
                            &outputs.iter().map(|col| col.unique_id).collect::<Vec<_>>(),
                        ) || determinants.subset_of(&fds.not_null_cols)
                        {
                            not_null.insert(id);
                        }
                        fds.add_strict(determinants, ColSet::of([id]));
                    }
                }
            }
            fds.make_not_null(not_null);
            fds.project_cols(&visible.union(&fds.group_by_cols));
        }
        LogicalPlan::Aggregation(operator) => {
            let outputs = &schema(plan)?.columns;
            let mut groups = ColSet::default();
            let mut dependencies = ColSet::default();
            let mut not_null = ColSet::default();
            for expression in &operator.group_by_items {
                match expression {
                    Expression::Column(column) => {
                        groups.insert(column.unique_id);
                    }
                    Expression::ScalarFunction(_) => {
                        let hash = expression.clone().hash_code().to_vec();
                        let id = *fds
                            .hash_code_to_unique_id
                            .entry(hash.clone())
                            .or_insert_with(|| {
                                ctx.extended_columns
                                    .get(&hash)
                                    .copied()
                                    .unwrap_or_else(|| ctx.column_ids.alloc())
                            });
                        groups.insert(id);
                        let determinants = scalar_dependencies(expression);
                        dependencies.union_with(&determinants);
                        if tidb_funcdep::null_reject::is_null_rejected_by(
                            expression,
                            &outputs.iter().map(|col| col.unique_id).collect::<Vec<_>>(),
                        ) || determinants.subset_of(&fds.not_null_cols)
                        {
                            not_null.insert(id);
                        }
                        fds.add_strict(determinants, ColSet::of([id]));
                    }
                    _ => {}
                }
            }
            if operator.group_by_items.is_empty() {
                groups.insert(0);
            } else {
                fds.project_cols(&ids(outputs).union(&dependencies).union(&groups));
            }
            for (function, output) in operator.agg_funcs.iter().zip(outputs) {
                if function.base.name != "firstrow" {
                    fds.add_strict(groups.clone(), ColSet::of([output.unique_id]));
                }
            }
            if !operator.group_by_items.is_empty() {
                fds.make_not_null(not_null);
            }
            fds.group_by_cols = groups;
            fds.has_agg_built = true;
        }
        LogicalPlan::Todo(_) => {
            return Err(PlanError::internal(
                "FD extraction for an unported logical operator",
            ))
        }
        _ => {}
    }
    Ok(fds)
}

fn scalar_dependencies(expression: &Expression) -> ColSet {
    let mut result = ColSet::of(
        extract_columns(expression)
            .iter()
            .map(|column| column.unique_id),
    );
    for column in extract_cor_columns(expression) {
        result.insert(column.column.unique_id);
    }
    result
}
