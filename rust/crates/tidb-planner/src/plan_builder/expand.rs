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

//! `GROUP BY ... WITH ROLLUP`: the `LogicalExpand` build.
//!
//! Go sources, by symbol:
//!
//! | Rust | Go `logical_plan_builder.go` |
//! | --- | --- |
//! | [`PlanBuilder::build_expand`] | `buildExpand` (:144) |
//! | [`PlanBuilder::replace_grouping_func`] | `replaceGroupingFunc` (:1723) + `resolveGroupingTraverseAction` (:1700) |
//! | [`PlanBuilder::implicit_project_grouping_set_cols`] | `implicitProjectGroupingSetCols` (:1734) |
//! | [`build_expand_field_name`] | `buildExpandFieldName` (:1509) |
//! | [`deduplicate_gby_expression`] | `expression.DeduplicateGbyExpression` |
//! | [`restore_gby_expression`] | `expression.RestoreGbyExpression` |
//! | [`rollup_grouping_sets`] | `expression.RollupGroupingSets` |
//! | [`distinct_size`] | `expression.GroupingSets.DistinctSize` |
//!
//! # What ROLLUP actually is, once
//!
//! `GROUP BY a, b, c WITH ROLLUP` reports the four groupings `{}`, `{a}`,
//! `{a,b}`, `{a,b,c}`. Go does NOT compute four aggregates: it REPLICATES each
//! source row once per grouping set, NULLing the columns that set does not
//! group by, tags the copy with a `gid`, and then runs ONE aggregation grouped
//! additionally by `gid`. [`crate::logical::expand::LogicalExpand`] is the
//! replicator, and this file is the only thing in the crate that builds one.
//!
//! `tidb-executor`'s `driver/grouping.rs:221` `run_rollup_aggregate` solves the
//! same problem by re-running the aggregate once per prefix at RUNTIME. That
//! decision does not transfer — the IR wants the operator — and this file does
//! not use it. What DOES transfer is `grouping.rs:65-164`
//! (`replace_grouping_expr`, `grouping_call_args`, `grouping_arg_positions`),
//! the `GROUPING()` argument resolution, which is
//! [`PlanBuilder::replace_grouping_func`] here over
//! [`crate::logical::expand::LogicalExpand::resolve_grouping_func_args_in_group_by`].
//!
//! # Narrowings, by exact blocking Go symbol
//!
//! * `expression.AdjustNullabilityFromGroupingSets(rollupGroupingSets,
//!   expandSchema)`. A grouping column becomes NULLABLE across the Expand,
//!   because the sets that do not group by it project NULL. Reproduced inline
//!   below by clearing [`tidb_datatype::FieldTypeFlags::NOT_NULL`] on every
//!   column whose unique id appears in any grouping set — which is the whole
//!   content of that function.
//! * `expression.GroupingSets`/`GroupingSet`/`GroupingExprs` are not
//!   transcreated; [`crate::logical::expand::RollupGroupingSet`]'s own header
//!   records why, and [`distinct_size`] is the one method of that file the
//!   builder must supply, ported here against the column-id sets.
//! * `expression.Expression.CanonicalHashCode`, which
//!   `TrySubstituteExprWithGroupingSetCol` compares with. The operator's
//!   header already names this: a COMMUTED group-by expression is not
//!   recognised, a false negative that leaves the expression unsubstituted.

use std::collections::{BTreeMap, BTreeSet};

use tidb_datatype::{
    FieldName, FieldNameMetadata, FieldType, FieldTypeCode, FieldTypeFlags, IdentifierMetadata,
};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

use super::catalog::TableSource;
use super::{snapshot_schema_and_names, BlockExpand, PlanBuilder};
use crate::logical::expand::{GroupingMode, LogicalExpand, RollupGroupingSet};
use crate::logical::projection::LogicalProjection;
use crate::logical::rule::flags;
use crate::logical::LogicalPlan;
use crate::plan_base::PlanError;

/// Go `expression.DeduplicateGbyExpression(gbyItems)`: the distinct group-by
/// expressions, plus for each ORIGINAL item the position of its representative.
///
/// `group by a, b, a` has two distinct expressions and the reference positions
/// `[0, 1, 0]`, which is what lets [`restore_gby_expression`] put the rebuilt
/// column back in every place the expression was written.
#[must_use]
pub fn deduplicate_gby_expression(gby_items: &[Expression]) -> (Vec<Expression>, Vec<usize>) {
    let mut distinct: Vec<Expression> = Vec::with_capacity(gby_items.len());
    let mut positions = Vec::with_capacity(gby_items.len());
    for item in gby_items {
        let position = distinct
            .iter()
            .position(|existing| crate::logical::schema_producer::expressions_equal(existing, item))
            .unwrap_or_else(|| {
                distinct.push(item.clone());
                distinct.len() - 1
            });
        positions.push(position);
    }
    (distinct, positions)
}

/// Go `expression.RestoreGbyExpression(distinctGbyCols, gbyExprsRefPos)`: the
/// group-by item list rebuilt over the projected columns.
#[must_use]
pub fn restore_gby_expression(
    distinct_cols: &[Column],
    ref_positions: &[usize],
) -> Vec<Expression> {
    ref_positions
        .iter()
        .filter_map(|position| distinct_cols.get(*position))
        .cloned()
        .map(Expression::Column)
        .collect()
}

/// Go `expression.RollupGroupingSets(newGbyItems)`: `<a,b,c>` becomes
/// `{}, {a}, {a,b}, {a,b,c}` — every PREFIX of the written group-by list.
#[must_use]
pub fn rollup_grouping_sets(gby_items: &[Expression]) -> Vec<RollupGroupingSet> {
    let ids: Vec<i64> = gby_items
        .iter()
        .filter_map(|item| match item {
            Expression::Column(column) => Some(column.unique_id),
            _ => None,
        })
        .collect();
    (0..=ids.len())
        .map(|length| RollupGroupingSet::new(ids[..length].iter().copied()))
        .collect()
}

/// Go `expression.GroupingSets.DistinctSize()`: the number of DISTINCT
/// grouping sets, each set's grouping id, and per column the ids it is present
/// in.
///
/// The rollup sets are prefixes and so are always distinct, but the shape is
/// Go's and [`LogicalExpand::gen_level_projections`] reads all three fields.
#[must_use]
pub fn distinct_size(
    sets: &[RollupGroupingSet],
    distinct_cols: &[Column],
    mode: GroupingMode,
) -> (i64, Vec<u64>, BTreeMap<i64, BTreeSet<u64>>) {
    let mut seen: Vec<&BTreeSet<i64>> = Vec::with_capacity(sets.len());
    let mut ids = Vec::with_capacity(sets.len());
    let mut id_to_gids: BTreeMap<i64, BTreeSet<u64>> = BTreeMap::new();
    for (offset, set) in sets.iter().enumerate() {
        if !seen.contains(&&set.col_ids) {
            seen.push(&set.col_ids);
        }
        // In `ModeNumericSet` the id IS the set's index; otherwise it is the
        // bitmask, which is what `GenerateGroupingIDModeBitAnd` recomputes.
        let id = match mode {
            GroupingMode::NumericSet => offset as u64,
            _ => bit_and_id(set, distinct_cols),
        };
        ids.push(id);
        for col_id in &set.col_ids {
            id_to_gids.entry(*col_id).or_default().insert(id);
        }
    }
    (seen.len() as i64, ids, id_to_gids)
}

/// [`LogicalExpand::generate_grouping_id_mode_bit_and`] before the operator
/// exists to call it on.
fn bit_and_id(set: &RollupGroupingSet, distinct_cols: &[Column]) -> u64 {
    let mut res = 0_u64;
    for column in distinct_cols.iter().rev() {
        res <<= 1;
        if set.col_ids.contains(&column.unique_id) {
            res |= 1;
        }
    }
    res
}

/// Go `buildExpandFieldName(ctx, expr, name, genName)`
/// (`logical_plan_builder.go:1509`).
///
/// Three shapes, and the prefix is the whole point: a generated column takes
/// its own `gid_`/`gpos_` label, a projected COLUMN keeps its origin names but
/// gets `ex_`-prefixed visible ones (because its nullability changed and it is
/// no longer the same column), and anything else is `ex_` plus the restored
/// expression.
#[must_use]
pub fn build_expand_field_name(
    expr_text: &str,
    origin: Option<&FieldName>,
    gen_name: &str,
) -> FieldName {
    if !gen_name.is_empty() {
        return FieldName::new(FieldNameMetadata {
            column: IdentifierMetadata::new(expr_text),
            ..FieldNameMetadata::default()
        });
    }
    match origin {
        Some(origin) => FieldName::new(FieldNameMetadata {
            table: IdentifierMetadata::new(format!("ex_{}", origin.names.table.original)),
            original_table: origin.names.original_table.clone(),
            column: IdentifierMetadata::new(format!("ex_{}", origin.names.column.original)),
            original_column: origin.names.original_column.clone(),
            database: origin.names.database.clone(),
        }),
        None => FieldName::new(FieldNameMetadata {
            column: IdentifierMetadata::new(format!("ex_{expr_text}")),
            ..FieldNameMetadata::default()
        }),
    }
}

/// Go's `gid` / `gpos` type: `LONGLONG UNSIGNED NOT NULL`.
fn grouping_id_type() -> FieldType {
    let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
    ret_type.set_flags(ret_type.flags() | FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL);
    ret_type
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildExpand(p, gbyItems)` (`logical_plan_builder.go:144`).
    ///
    /// Builds TWO operators: a projection that materialises every distinct
    /// group-by expression as a fresh column (so that a rolled-up `a+b` has
    /// something to be NULLed), and the `LogicalExpand` above it. Returns the
    /// Expand and Go's `newGbyItems` — the group-by list rewritten onto those
    /// fresh columns, which is what [`PlanBuilder::build_aggregation`] then
    /// groups by.
    ///
    /// Registers the Expand in [`PlanBuilder::current_block_expand`], which is
    /// Go's `b.currentBlockExpand = expand` and what
    /// [`PlanBuilder::replace_grouping_func`] and
    /// [`PlanBuilder::implicit_project_grouping_set_cols`] read.
    ///
    /// # Errors
    ///
    /// None on this path; the signature matches its Go sibling.
    pub fn build_expand(
        &mut self,
        plan: LogicalPlan,
        gby_items: Vec<Expression>,
    ) -> Result<(LogicalPlan, Vec<Expression>), PlanError> {
        self.opt_flag |= flags::RESOLVE_EXPAND;
        let (schema, mut names) = snapshot_schema_and_names(&plan);

        // `:149` the projection below: the child's own columns, then one fresh
        // column per DISTINCT group-by expression.
        let (distinct_gby_exprs, ref_positions) = deduplicate_gby_expression(&gby_items);
        let mut proj_exprs: Vec<Expression> = schema
            .columns
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect();
        let mut proj_columns = schema.columns.clone();
        let mut distinct_gby_cols = Vec::with_capacity(distinct_gby_exprs.len());
        let mut distinct_gby_col_names = Vec::with_capacity(distinct_gby_exprs.len());
        let mut distinct_gby_field_names = Vec::with_capacity(distinct_gby_exprs.len());

        for expr in &distinct_gby_exprs {
            proj_exprs.push(expr.clone());
            let origin = match expr {
                Expression::Column(column) => schema
                    .columns
                    .iter()
                    .position(|candidate| candidate.unique_id == column.unique_id)
                    .and_then(|index| names.get(index)),
                _ => None,
            };
            let name = build_expand_field_name(&expression_text(expr), origin, "");
            // `:174` "since we will change the nullability of source col, proj
            // it with a new col id."
            let ret_type = expr
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            let mut column = Column::new(self.column_ids.alloc(), ret_type);
            column.orig_name = name.display_name();
            column.index = proj_columns.len() as i64;
            distinct_gby_col_names.push(name.names.column.original.clone());
            distinct_gby_field_names.push(name.clone());
            names.push(name);
            proj_columns.push(column.clone());
            distinct_gby_cols.push(column);
        }

        let mut projection = LogicalProjection::new(self.base(LogicalProjection::TYPE), proj_exprs);
        projection.proj4_expand = true;
        projection.base.set_children(vec![plan]);
        projection
            .base
            .base
            .set_schema(Some(Schema::new(proj_columns.clone())));
        projection.base.base.set_output_names(names.clone());

        let new_gby_items = restore_gby_expression(&distinct_gby_cols, &ref_positions);
        let grouping_sets = rollup_grouping_sets(&new_gby_items);

        // `:196` the Expand's own schema is the projection's, with the
        // grouping columns made NULLABLE — the `AdjustNullabilityFromGroupingSets`
        // narrowing named in this module's header.
        let grouping_col_ids: BTreeSet<i64> = grouping_sets
            .iter()
            .flat_map(|set| set.col_ids.iter().copied())
            .collect();
        let mut expand_columns = proj_columns;
        for column in &mut expand_columns {
            if !grouping_col_ids.contains(&column.unique_id) {
                continue;
            }
            if let Some(ret_type) = column.ret_type.as_mut() {
                ret_type.set_flags(ret_type.flags() & !FieldTypeFlags::NOT_NULL);
            }
        }

        let mut expand = LogicalExpand::new(self.base(LogicalExpand::TYPE));
        // `:210` "if we want to use bitAnd for the quick computation of
        // grouping function, then the maximum capacity of num of grouping is
        // about 64."
        let mode = if grouping_sets.len() > 64 {
            GroupingMode::NumericSet
        } else {
            GroupingMode::BitAnd
        };
        expand.grouping_mode = Some(mode);
        let (size, ids, id_to_gids) = distinct_size(&grouping_sets, &distinct_gby_cols, mode);
        expand.distinct_size = size;
        expand.rollup_grouping_ids = ids;
        expand.rollup_id_to_gids = id_to_gids;
        expand.rollup_grouping_sets = grouping_sets;
        expand.distinct_group_by_col = distinct_gby_cols.clone();
        expand.distinct_gby_col_names = distinct_gby_col_names;
        let distinct_gby_exprs_for_block = distinct_gby_exprs.clone();
        expand.distinct_gby_exprs = distinct_gby_exprs;

        // `:219` the generated `gid`, and `gpos` when two grouping sets are
        // duplicates of each other.
        let has_duplicate_grouping_set =
            expand.rollup_grouping_sets.len() as i64 != expand.distinct_size;
        let mut gid = Column::new(self.column_ids.alloc(), grouping_id_type());
        gid.orig_name = "gid".to_owned();
        gid.index = expand_columns.len() as i64;
        expand_columns.push(gid.clone());
        expand.extra_grouping_col_names.push("gid".to_owned());
        let gid_name = build_expand_field_name("gid", None, "gid_");
        names.push(gid_name);
        expand.gid_name = Some("gid_".to_owned());
        expand.gid = Some(Box::new(gid.clone()));
        let mut gpos_column = None;
        if has_duplicate_grouping_set {
            let mut gpos = Column::new(self.column_ids.alloc(), grouping_id_type());
            gpos.orig_name = "gpos".to_owned();
            gpos.index = expand_columns.len() as i64;
            expand_columns.push(gpos.clone());
            expand.extra_grouping_col_names.push("gpos".to_owned());
            names.push(build_expand_field_name("gpos", None, "gpos_"));
            expand.gpos_name = Some("gpos_".to_owned());
            expand.gpos = Some(Box::new(gpos.clone()));
            gpos_column = Some(gpos);
        }

        expand
            .base
            .set_children(vec![LogicalPlan::Projection(projection)]);
        expand
            .base
            .base
            .set_schema(Some(Schema::new(expand_columns)));
        expand.base.base.set_output_names(names);

        // `:245` "register current rollup Expand operator in current select
        // block."
        self.current_block_expand = Some(BlockExpand {
            grouping_id_col: Some(gid),
            grouping_pos_col: gpos_column,
            distinct_group_by_cols: distinct_gby_cols,
            distinct_group_by_names: distinct_gby_field_names,
            distinct_group_by_exprs: distinct_gby_exprs_for_block,
        });
        Ok((LogicalPlan::Expand(expand), new_gby_items))
    }

    /// Go `replaceGroupingFunc(expr)` (`logical_plan_builder.go:1723`), over
    /// `resolveGroupingTraverseAction` (`:1700`).
    ///
    /// Two jobs on one traversal, and Go's comment at `:1707` is the reason
    /// the ORDER matters: a scalar function tries to substitute itself as a
    /// WHOLE group-by expression first (`select a+1 ... group by a+1` resolves
    /// `a+1` to the projected column `c`), and only if that fails does it
    /// recurse into its arguments (`select a+1 ... group by a` resolves to
    /// `a'+1` over the projected `a'`).
    ///
    /// A block with no Expand returns the expression untouched, which is Go's
    /// first line.
    #[must_use]
    pub fn replace_grouping_func(&self, expr: Expression) -> Expression {
        let Some(block) = &self.current_block_expand else {
            return expr;
        };
        replace_grouping_expr(&expr, block)
    }

    /// [`Self::replace_grouping_func`] over every `ByItems` of an already-built
    /// sort, which is where `buildSortWithCheck` (`:2424`) applies it.
    #[must_use]
    pub fn replace_grouping_func_in_sort(&self, plan: LogicalPlan) -> LogicalPlan {
        let LogicalPlan::Sort(mut sort) = plan else {
            return plan;
        };
        for item in &mut sort.by_items {
            item.expr = self.replace_grouping_func(item.expr.clone());
        }
        LogicalPlan::Sort(sort)
    }

    /// Go `implicitProjectGroupingSetCols(projSchema, projNames, projExprs)`
    /// (`logical_plan_builder.go:1734`): a grouping column the select list did
    /// not project is projected ANYWAY, then `gid` and `gpos`.
    ///
    /// They must survive the projection because a later `ORDER BY a+1` over
    /// `group by a+1 with rollup` resolves to the grouping-set column, not to
    /// the source one. Anything unused is removed again by column pruning,
    /// which is Go's own note.
    pub fn implicit_project_grouping_set_cols(
        &self,
        columns: &mut Vec<Column>,
        names: &mut Vec<FieldName>,
        exprs: &mut Vec<Expression>,
    ) {
        let Some(block) = &self.current_block_expand else {
            return;
        };
        let projected: BTreeSet<i64> = columns.iter().map(|column| column.unique_id).collect();
        for (index, grouping_col) in block.distinct_group_by_cols.iter().enumerate() {
            if projected.contains(&grouping_col.unique_id) {
                continue;
            }
            columns.push(grouping_col.clone());
            exprs.push(Expression::Column(grouping_col.clone()));
            names.push(
                block
                    .distinct_group_by_names
                    .get(index)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        for (column, gen_name) in [
            (block.grouping_id_col.as_ref(), "gid_"),
            (block.grouping_pos_col.as_ref(), "gpos_"),
        ] {
            let Some(column) = column else { continue };
            columns.push(column.clone());
            exprs.push(Expression::Column(column.clone()));
            names.push(build_expand_field_name(&column.orig_name, None, gen_name));
        }
    }
}

/// The recursive half of `resolveGroupingTraverseAction.Transform` (`:1700`).
fn replace_grouping_expr(expr: &Expression, block: &BlockExpand) -> Expression {
    match expr {
        // "column: resolve it to the grouping set col if any."
        Expression::Column(_) => {
            substitute_grouping_col(expr, block).unwrap_or_else(|| expr.clone())
        }
        // "constant just keep it real: select 1 from t group by a, b with rollup."
        Expression::Constant(_) => expr.clone(),
        Expression::ScalarFunction(function) => {
            if let Some(substituted) = substitute_grouping_col(expr, block) {
                return substituted;
            }
            let mut function = function.clone();
            for arg in &mut function.args {
                *arg = replace_grouping_expr(arg, block);
            }
            Expression::ScalarFunction(function)
        }
        other => other.clone(),
    }
}

/// [`LogicalExpand::try_substitute_expr_with_grouping_set_col`] against the
/// block record, which is what the builder holds rather than the operator.
fn substitute_grouping_col(expr: &Expression, block: &BlockExpand) -> Option<Expression> {
    let position = block.distinct_group_by_exprs.iter().position(|candidate| {
        crate::logical::schema_producer::expressions_equal(expr, candidate)
    })?;
    block
        .distinct_group_by_cols
        .get(position)
        .cloned()
        .map(Expression::Column)
}

/// Go `expr.StringWithCtx(ctx, errors.RedactLogDisable)`, which
/// `buildExpandFieldName` uses to NAME an expression column.
fn expression_text(expr: &Expression) -> String {
    match expr {
        Expression::Column(column) if !column.orig_name.is_empty() => column.orig_name.clone(),
        other => format!("{other:?}"),
    }
}
