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

//! The aggregate partial/final split: Go `BuildFinalModeAggregation`
//! (`pkg/planner/core/operator/physicalop/base_physical_agg.go:600`) and its
//! helpers `RemoveUnnecessaryFirstRow` (`:460`), `genFirstRowAggForGroupBy`
//! (`:441`), and `computePartialCursorOffset` (`:507`).
//!
//! Go splits one complete aggregation into a PARTIAL half that the
//! coprocessor runs next to the scan and a FINAL half that merges partial
//! results at the root. The split rewrites the descriptors: `avg` becomes
//! `count`+`sum` below and a division above, a distinct argument becomes a
//! pushed group-by column, and the final half's arguments become the
//! partial half's output schema columns.
//!
//! Go keys `firstRowFuncMap` by descriptor POINTER; descriptors here are
//! values, so the map is `partial index -> final index`, carried alongside
//! the two halves and consumed by [`remove_unnecessary_first_row`] exactly
//! where Go consumes the pointer map.

use std::collections::HashMap;

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::aggregation::{
    names, need_count, need_value, AggFuncDesc, AggFunctionMode, ByItems,
};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::Columns;
use tidb_expr::schema::Schema;

use crate::expression_rewriter::ColumnIdAllocator;

/// Go `AggInfo` (`base_physical_agg.go:592`): the descriptor triple either
/// half of a split carries.
#[derive(Clone, Debug, Default)]
pub struct AggInfo {
    /// Go `AggFuncs`.
    pub agg_funcs: Vec<AggFuncDesc>,
    /// Go `GroupByItems`.
    pub group_by_items: Vec<Expression>,
    /// Go `Schema`.
    pub schema: Schema,
}

/// The split's product: both halves plus the firstrow pairing Go returns as
/// `firstRowFuncMap` (partial-index -> final-index here, pointer map there).
#[derive(Debug)]
pub struct FinalModeSplit {
    /// The half pushed toward the source.
    pub partial: AggInfo,
    /// The half kept at the root.
    pub final_agg: AggInfo,
    /// Which partial `firstrow` merges into which final `firstrow`.
    pub first_row_func_map: HashMap<usize, usize>,
}

/// Go `genFirstRowAggForGroupBy` (`:441`): one `firstrow(item)` per group-by
/// item, for the TiDB-cop case whose executor does not output group-by
/// values.
pub fn gen_first_row_agg_for_group_by(
    ctx: &impl Columns,
    group_by_items: &[Expression],
) -> Result<Vec<AggFuncDesc>, tidb_expr::aggregation::AggDescError> {
    let mut agg_funcs = Vec::with_capacity(group_by_items.len());
    for group_by in group_by_items {
        agg_funcs.push(AggFuncDesc::new(
            ctx,
            names::FIRST_ROW,
            vec![group_by.clone()],
            false,
        )?);
    }
    Ok(agg_funcs)
}

/// Go `computePartialCursorOffset` (`:507`).
fn compute_partial_cursor_offset(name: &str) -> usize {
    let mut offset = 0;
    if need_count(name) {
        offset += 1;
    }
    if need_value(name) {
        offset += 1;
    }
    if name == names::APPROX_COUNT_DISTINCT {
        offset += 1;
    }
    offset
}

/// Go `RemoveUnnecessaryFirstRow` (`:460`): a partial `firstrow(expr)` whose
/// argument already IS a group-by item is dropped — its value arrives through
/// the group-by schema — and the FINAL firstrow's argument is redirected to
/// the final group-by column. Returns the surviving partial functions;
/// `final_agg_funcs` is mutated exactly where Go writes through
/// `firstRowFuncMap`.
pub fn remove_unnecessary_first_row(
    final_agg_funcs: &mut [AggFuncDesc],
    final_gby_items: &[Expression],
    partial_agg_funcs: Vec<AggFuncDesc>,
    partial_gby_items: &[Expression],
    partial_schema: &mut Schema,
    first_row_func_map: &HashMap<usize, usize>,
) -> Vec<AggFuncDesc> {
    let mut partial_cursor = 0;
    let mut new_agg_funcs = Vec::with_capacity(partial_agg_funcs.len());
    for (partial_index, agg_func) in partial_agg_funcs.into_iter().enumerate() {
        if agg_func.name() == names::FIRST_ROW {
            let mut can_optimize = false;
            for (j, gby_expr) in partial_gby_items.iter().enumerate() {
                if j >= final_gby_items.len() {
                    // After distinct push, the partial group-by can be longer
                    // than the final one (`select a, count(distinct a)`); the
                    // root task's firstrow cannot be removed.
                    break;
                }
                // A constant group-by item is skipped: for
                // `SELECT DISTINCT SQRT(1)` the `firstrow(SQRT(1))` must
                // stay.
                if matches!(gby_expr, Expression::Constant(_)) {
                    continue;
                }
                if gby_expr.equal(&agg_func.base.args[0]) {
                    can_optimize = true;
                    if let Some(final_index) = first_row_func_map.get(&partial_index) {
                        final_agg_funcs[*final_index].base.args[0] =
                            final_gby_items[j].clone();
                    }
                    break;
                }
            }
            if can_optimize {
                partial_schema.columns.remove(partial_cursor);
                continue;
            }
        }
        partial_cursor += compute_partial_cursor_offset(agg_func.name());
        new_agg_funcs.push(agg_func);
    }
    new_agg_funcs
}

/// Go's `types.NewFieldType(mysql.TypeLonglong)` count column: `Flen` 21,
/// binary charset and collation.
fn count_column_type() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(21);
    ft.set_charset_name("binary");
    ft.set_collation_name("binary");
    ft
}

/// Go's `mysql.TypeString` sketch column for `approx_count_distinct`:
/// binary charset/collation plus `NotNullFlag`.
fn approx_count_distinct_column_type() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::String);
    ft.set_charset_name("binary");
    ft.set_collation_name("binary");
    ft.set_flags(ft.flags() | tidb_datatype::FieldTypeFlags::NOT_NULL);
    ft
}

/// The mutable split state Go's `getDistinctExpr` closure captures.
struct DistinctState<'a> {
    partial: &'a mut AggInfo,
    partial_gby_schema: &'a mut Schema,
    partial_cursor: &'a mut usize,
}

/// Go `getDistinctExpr` (`:668`): route one distinct argument through the
/// partial group-by, allocating a column for it if it is not one already,
/// and — outside cop, or for group_concat order-by items — a `firstrow` to
/// carry it.
fn get_distinct_expr(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    state: &mut DistinctState<'_>,
    distinct_arg: &Expression,
    only_add_first_row: bool,
    partial_is_cop: bool,
) -> Expression {
    let mut ret: Option<Expression> = None;
    // 1. An argument already in the group-by list, with the exact type,
    //    reuses that group-by column.
    for (j, gby_expr) in state.partial.group_by_items.iter().enumerate() {
        let types_equal = match (gby_expr.static_type(), distinct_arg.static_type()) {
            (Some(left), Some(right)) => left.equal(right),
            _ => false,
        };
        if gby_expr.equal(distinct_arg) && types_equal {
            ret = Some(Expression::Column(
                state.partial_gby_schema.columns[j].clone(),
            ));
            break;
        }
    }
    if ret.is_none() {
        let gby_col = if let Expression::Column(col) = distinct_arg {
            col.clone()
        } else {
            Column::new(
                alloc.alloc(),
                distinct_arg
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
            )
        };
        // 2. Add the group-by item if needed.
        if !only_add_first_row {
            state.partial.group_by_items.push(distinct_arg.clone());
            state.partial_gby_schema.append([gby_col.clone()]);
            ret = Some(Expression::Column(gby_col.clone()));
        }
        // 3. Add `firstrow()` if needed: a cop partial outputs group-by
        //    values through its schema, so the firstrow is redundant there;
        //    group_concat order-by items always take one.
        if !partial_is_cop || only_add_first_row {
            let first_row = AggFuncDesc::new(
                ctx,
                names::FIRST_ROW,
                vec![distinct_arg.clone()],
                false,
            )
            .unwrap_or_else(|error| {
                // Go: `panic("NewAggFuncDesc FirstRow meets error: " + ...)`.
                panic!("NewAggFuncDesc FirstRow meets error: {error:?}")
            });
            let mut new_col = gby_col;
            new_col.ret_type = Some(first_row.base.ret_type.clone());
            state.partial.agg_funcs.push(first_row);
            state.partial.schema.append([new_col.clone()]);
            if only_add_first_row {
                ret = Some(Expression::Column(new_col));
            }
            *state.partial_cursor += 1;
        }
    }
    ret.expect("getDistinctExpr always resolves through one of its three steps")
}

/// Go `BuildFinalModeAggregation` (`:600`). `None` is Go's
/// `partial == nil, final == original` return: the aggregation must run in
/// ONE phase (group_concat with order-by but no distinct, or a failed
/// avg-split type inference).
///
/// `partial_is_cop` and `is_mpp_task` carry Go's meanings; the live callers
/// in this port pass `(true, false)`, but every branch both flags gate is
/// ported.
#[allow(clippy::too_many_lines)] // Go's own comment: "This for loop is ugly".
pub fn build_final_mode_aggregation(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    original: &AggInfo,
    partial_is_cop: bool,
    is_mpp_task: bool,
) -> Option<FinalModeSplit> {
    let mut first_row_func_map: HashMap<usize, usize> =
        HashMap::with_capacity(original.agg_funcs.len());
    let mut partial = AggInfo {
        agg_funcs: Vec::with_capacity(original.agg_funcs.len()),
        group_by_items: original.group_by_items.clone(),
        schema: Schema::default(),
    };
    let mut partial_cursor = 0usize;
    let mut final_agg = AggInfo {
        agg_funcs: Vec::with_capacity(original.agg_funcs.len()),
        group_by_items: Vec::with_capacity(original.group_by_items.len()),
        schema: original.schema.clone(),
    };

    let mut partial_gby_schema = Schema::default();
    // Add group-by columns: an expression that is not already a column gets a
    // fresh plan column carrying its type.
    for gby_expr in &partial.group_by_items {
        let gby_col = if let Expression::Column(col) = gby_expr {
            col.clone()
        } else {
            Column::new(
                alloc.alloc(),
                gby_expr
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
            )
        };
        partial_gby_schema.append([gby_col.clone()]);
        final_agg.group_by_items.push(Expression::Column(gby_col));
    }

    for (i, agg_func) in original.agg_funcs.iter().enumerate() {
        // Go builds `finalAggFunc` field by field; the base fields land at
        // the end of the loop body exactly as Go's trailing assignments do.
        let mut final_name = agg_func.name().to_owned();
        let mut final_mode = AggFunctionMode::Complete;
        let mut final_has_distinct = false;
        let mut final_order_by_items = agg_func.order_by_items.clone();
        let mut final_grouping_id = 0;
        let mut args: Vec<Expression> = Vec::with_capacity(agg_func.base.args.len());
        if agg_func.has_distinct {
            // eg: SELECT COUNT(DISTINCT a), SUM(b) FROM t GROUP BY c —
            // the cop half groups by (c, a) and the root keeps the distinct
            // aggregate over the pushed column.
            let mut state = DistinctState {
                partial: &mut partial,
                partial_gby_schema: &mut partial_gby_schema,
                partial_cursor: &mut partial_cursor,
            };
            let arg_count = agg_func.base.args.len();
            for (j, distinct_arg) in agg_func.base.args.iter().enumerate() {
                // The last arg of group_concat is the separator: it goes to
                // the final half untouched.
                if agg_func.name() == names::GROUP_CONCAT && j + 1 == arg_count {
                    args.push(distinct_arg.clone());
                    continue;
                }
                args.push(get_distinct_expr(
                    ctx,
                    alloc,
                    &mut state,
                    distinct_arg,
                    false,
                    partial_is_cop,
                ));
            }

            let mut by_items = Vec::with_capacity(agg_func.order_by_items.len());
            for by_item in &agg_func.order_by_items {
                by_items.push(ByItems::new(
                    get_distinct_expr(
                        ctx,
                        alloc,
                        &mut state,
                        &by_item.expr,
                        true,
                        partial_is_cop,
                    ),
                    by_item.desc,
                ));
            }

            if is_mpp_task && agg_func.grouping_id > 0 {
                // Keep the groupingID, else the split final aggregate loses
                // its grouping info.
                final_grouping_id = agg_func.grouping_id;
            }

            final_order_by_items = by_items;
            final_has_distinct = agg_func.has_distinct;
            // If the original mode is already partial (an Agg above a
            // PartitionUnion), the final becomes Partial2.
            if agg_func.mode == AggFunctionMode::Complete {
                final_mode = AggFunctionMode::Complete;
            } else if matches!(
                agg_func.mode,
                AggFunctionMode::Partial1 | AggFunctionMode::Partial2
            ) {
                final_mode = AggFunctionMode::Partial2;
            }
        } else {
            if agg_func.name() == names::GROUP_CONCAT && !agg_func.order_by_items.is_empty() {
                // group_concat with order-by but without distinct runs in one
                // phase only.
                return None;
            }
            if need_count(&final_name) {
                if is_mpp_task && final_name == names::COUNT {
                    // For MPP the final count() merges by sum().
                    final_name = names::SUM.to_owned();
                } else {
                    partial
                        .schema
                        .append([Column::new(alloc.alloc(), count_column_type())]);
                    args.push(Expression::Column(
                        partial.schema.columns[partial_cursor].clone(),
                    ));
                    partial_cursor += 1;
                }
            }
            if final_name == names::APPROX_COUNT_DISTINCT {
                partial.schema.append([Column::new(
                    alloc.alloc(),
                    approx_count_distinct_column_type(),
                )]);
                args.push(Expression::Column(
                    partial.schema.columns[partial_cursor].clone(),
                ));
                partial_cursor += 1;
            }
            if need_value(&final_name) {
                partial.schema.append([Column::new(
                    alloc.alloc(),
                    original.schema.columns[i]
                        .ret_type
                        .clone()
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
                )]);
                args.push(Expression::Column(
                    partial.schema.columns[partial_cursor].clone(),
                ));
                partial_cursor += 1;
            }
            if agg_func.name() == names::AVG {
                let mut cnt_agg = agg_func.clone();
                cnt_agg.base.name = names::COUNT.to_owned();
                if cnt_agg.base.type_infer(ctx).is_err() {
                    // must not happen (Go's comment) — one phase.
                    return None;
                }
                partial.schema.columns[partial_cursor - 2].ret_type =
                    Some(cnt_agg.base.ret_type.clone());
                // Deep clone, to avoid sharing the arguments.
                let mut sum_agg = agg_func.clone();
                sum_agg.base.name = names::SUM.to_owned();
                if sum_agg
                    .base
                    .type_infer_4_avg_sum(&agg_func.base.ret_type)
                    .is_err()
                {
                    return None;
                }
                partial.schema.columns[partial_cursor - 1].ret_type =
                    Some(sum_agg.base.ret_type.clone());
                partial.agg_funcs.push(cnt_agg);
                partial.agg_funcs.push(sum_agg);
            } else if agg_func.name() == names::APPROX_COUNT_DISTINCT
                || agg_func.name() == names::GROUP_CONCAT
            {
                let mut new_agg_func = agg_func.clone();
                new_agg_func.base.ret_type = partial.schema.columns[partial_cursor - 1]
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                partial.agg_funcs.push(new_agg_func);
                if agg_func.name() == names::GROUP_CONCAT {
                    // Append the trailing separator arg.
                    args.push(
                        agg_func.base.args[agg_func.base.args.len() - 1].clone(),
                    );
                }
            } else {
                // Other descriptors just split into two identical halves.
                let partial_func_desc = agg_func.clone();
                if agg_func.name() == names::FIRST_ROW {
                    first_row_func_map.insert(partial.agg_funcs.len(), i);
                }
                partial.agg_funcs.push(partial_func_desc);
            }

            if agg_func.mode == AggFunctionMode::Complete {
                final_mode = AggFunctionMode::Final;
            } else if matches!(
                agg_func.mode,
                AggFunctionMode::Partial1 | AggFunctionMode::Partial2
            ) {
                final_mode = AggFunctionMode::Partial2;
            }
        }

        final_agg.agg_funcs.push(AggFuncDesc {
            base: tidb_expr::aggregation::BaseFuncDesc {
                name: final_name,
                args,
                ret_type: agg_func.base.ret_type.clone(),
            },
            mode: final_mode,
            has_distinct: final_has_distinct,
            order_by_items: final_order_by_items,
            grouping_id: final_grouping_id,
        });
    }
    partial
        .schema
        .append(partial_gby_schema.columns.iter().cloned());
    if partial_is_cop {
        for func in &mut partial.agg_funcs {
            func.mode = AggFunctionMode::Partial1;
        }
    }
    Some(FinalModeSplit {
        partial,
        final_agg,
        first_row_func_map,
    })
}

/// Go `ContainVirtualColumn` (`pkg/expression/util.go:1635`): a column
/// backed by a generating expression cannot be pushed.
fn contain_virtual_column(exprs: &[Expression]) -> bool {
    exprs.iter().any(|expr| match expr {
        Expression::Column(col) => col.virtual_expr.is_some(),
        Expression::ScalarFunction(func) => contain_virtual_column(func.get_args()),
        _ => false,
    })
}

/// Go `ContainCorrelatedColumn` (`pkg/expression/util.go:1652`). Note Go's
/// switch does NOT look inside Constants — neither does this.
fn contain_correlated_column(exprs: &[Expression]) -> bool {
    exprs.iter().any(|expr| match expr {
        Expression::CorrelatedColumn(_) => true,
        Expression::ScalarFunction(func) => contain_correlated_column(func.get_args()),
        _ => false,
    })
}

/// Go `CheckAggCanPushCop` (`base_physical_agg.go:522`) for the TiKV store.
///
/// Ported checks, in Go's order: virtual/correlated columns in arguments,
/// `CheckAggPushDown` (with the default empty push-down blacklist Go reads
/// from the session), argument pushability, order-by-item pushability, and
/// the same two group-by checks. Go's final `AggFuncToPBExpr != nil` probe
/// narrows: the PB conversion layer is unported, and for TiKV every function
/// admitted by `CheckAggPushDown` has a PB mapping, so the probe cannot
/// refuse anything the earlier checks admitted. Go's refusal WARNING
/// (`Aggregation can not be pushed to ...`) narrows with the same unported
/// statement-context channel the other attach refusals name.
#[must_use]
pub fn check_agg_can_push_cop_tikv(
    agg_funcs: &[AggFuncDesc],
    group_by_items: &[Expression],
) -> bool {
    let blacklist = HashMap::new();
    for agg_func in agg_funcs {
        if contain_virtual_column(&agg_func.base.args)
            || contain_correlated_column(&agg_func.base.args)
        {
            return false;
        }
        if !tidb_expr::aggregation::check_agg_push_down(
            agg_func,
            tidb_expr::infer_pushdown::PushDownStore::TiKv,
            &blacklist,
        ) {
            return false;
        }
        // Go passes `aggFunc.Name == ast.AggFuncSum` as `canEnumPush`, a
        // TiFlash-only enum carve-out (`canExprPushDown`); for TiKV the
        // plain check is the same function.
        if !crate::pushdown::can_exprs_push_down_tikv(&agg_func.base.args) {
            return false;
        }
        if !agg_func.order_by_items.is_empty() {
            let exprs: Vec<Expression> = agg_func
                .order_by_items
                .iter()
                .map(|item| item.expr.clone())
                .collect();
            if !crate::pushdown::can_exprs_push_down_tikv(&exprs) {
                return false;
            }
        }
    }
    if contain_virtual_column(group_by_items) {
        return false;
    }
    crate::pushdown::can_exprs_push_down_tikv(group_by_items)
}

/// Go `BasePhysicalAgg.NewPartialAggregate` (`base_physical_agg.go:279`) for
/// a TiKV cop task: `(None, plan)` is Go's `(nil, p.Self)` — the aggregate
/// stays whole — and `(Some(partial), final)` is the split, the partial
/// keeping the original plan's id and stats (Go mutates `p` into it) and the
/// final sharing those stats above it.
///
/// The `copTaskType == kv.TiDB` firstrow-appending arm is not reachable: the
/// only caller hands TiKV cop tasks. Go's expression context is consulted
/// ONLY for argument types during `TypeInfer` on this path, which is why a
/// column-free context is exact here.
pub fn new_partial_aggregate(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    plan: crate::physical::PhysicalPlan,
) -> Result<
    (Option<crate::physical::PhysicalPlan>, crate::physical::PhysicalPlan),
    crate::plan_base::PlanError,
> {
    use crate::physical::PhysicalPlan;
    let (agg_funcs, group_by_items, is_stream) = match &plan {
        PhysicalPlan::HashAgg(agg) => (&agg.agg_funcs, &agg.group_by_items, false),
        PhysicalPlan::StreamAgg(agg) => (&agg.agg_funcs, &agg.group_by_items, true),
        _ => {
            return Err(crate::plan_base::PlanError::internal(
                "NewPartialAggregate over a non-aggregate plan",
            ))
        }
    };
    if !check_agg_can_push_cop_tikv(agg_funcs, group_by_items) {
        return Ok((None, plan));
    }
    let original = AggInfo {
        agg_funcs: agg_funcs.clone(),
        group_by_items: group_by_items.clone(),
        schema: plan
            .schema()
            .cloned()
            .unwrap_or_default(),
    };
    let Some(mut split) = build_final_mode_aggregation(ctx, alloc, &original, true, false)
    else {
        return Ok((None, plan));
    };
    // A stream aggregate whose split grew the group-by (a pushed distinct
    // argument) cannot keep its order contract: stay whole.
    if is_stream && split.partial.group_by_items.len() != split.final_agg.group_by_items.len()
    {
        return Ok((None, plan));
    }
    // Remove unnecessary FirstRow.
    let partial_funcs = std::mem::take(&mut split.partial.agg_funcs);
    split.partial.agg_funcs = remove_unnecessary_first_row(
        &mut split.final_agg.agg_funcs,
        &split.final_agg.group_by_items,
        partial_funcs,
        &split.partial.group_by_items,
        &mut split.partial.schema,
        &split.first_row_func_map,
    );
    // Go mutates `p` into the partial half (same plan id, same stats) and
    // Init's a NEW final of the same kind above it, with
    // `ExpectedCnt: math.MaxFloat64` and `p`'s stats. Plan ids on this port
    // follow the TopN-push precedent: both halves carry the original id.
    let mut partial = plan.clone();
    let mut final_plan = plan;
    match (&mut partial, &mut final_plan) {
        (PhysicalPlan::HashAgg(part), PhysicalPlan::HashAgg(fin)) => {
            part.agg_funcs = split.partial.agg_funcs;
            part.group_by_items = split.partial.group_by_items;
            part.base.base.set_schema(Some(split.partial.schema));
            fin.agg_funcs = split.final_agg.agg_funcs;
            fin.group_by_items = split.final_agg.group_by_items;
            fin.base.base.set_schema(Some(split.final_agg.schema));
        }
        (PhysicalPlan::StreamAgg(part), PhysicalPlan::StreamAgg(fin)) => {
            part.agg_funcs = split.partial.agg_funcs;
            part.group_by_items = split.partial.group_by_items;
            part.base.base.set_schema(Some(split.partial.schema));
            fin.agg_funcs = split.final_agg.agg_funcs;
            fin.group_by_items = split.final_agg.group_by_items;
            fin.base.base.set_schema(Some(split.final_agg.schema));
        }
        _ => unreachable!("both halves clone from the same variant"),
    }
    Ok((Some(partial), final_plan))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_expr::{SessionTimeZone, ZonedNoColumns};

    fn ctx() -> ZonedNoColumns {
        ZonedNoColumns(SessionTimeZone::utc())
    }

    fn bigint_col(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn agg(name: &str, arg: &Column, distinct: bool) -> AggFuncDesc {
        AggFuncDesc::new(
            &ctx(),
            name,
            vec![Expression::Column(arg.clone())],
            distinct,
        )
        .expect("descriptor")
    }

    /// `select count(b) from t group by a`, cop split: the partial half
    /// keeps `count(b)` in Partial1 mode, its schema is the count column
    /// then the group-by column, and the final half counts THE COUNT COLUMN
    /// in Final mode over the aliased group-by column.
    #[test]
    fn a_plain_count_splits_into_partial1_below_and_final_above() {
        let a = bigint_col(1);
        let b = bigint_col(2);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc(); // ids 1, 2 are taken by the test columns
        alloc.alloc();
        let count = agg(names::COUNT, &b, false);
        let original = AggInfo {
            schema: Schema::new(vec![Column::new(
                10,
                count.base.ret_type.clone(),
            )]),
            agg_funcs: vec![count],
            group_by_items: vec![Expression::Column(a.clone())],
        };
        let split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false)
            .expect("two-phase");

        assert_eq!(split.partial.agg_funcs.len(), 1);
        assert_eq!(split.partial.agg_funcs[0].name(), names::COUNT);
        assert_eq!(split.partial.agg_funcs[0].mode, AggFunctionMode::Partial1);
        // Schema: [count-out, group-by a].
        assert_eq!(split.partial.schema.columns.len(), 2);
        assert_eq!(split.partial.schema.columns[1].unique_id, a.unique_id);

        assert_eq!(split.final_agg.agg_funcs.len(), 1);
        let final_count = &split.final_agg.agg_funcs[0];
        assert_eq!(final_count.name(), names::COUNT);
        assert_eq!(final_count.mode, AggFunctionMode::Final);
        // Its argument IS the partial count output column.
        let Expression::Column(arg) = &final_count.base.args[0] else {
            panic!("final count must read the partial output column");
        };
        assert_eq!(arg.unique_id, split.partial.schema.columns[0].unique_id);
        // The final group-by is the group-by COLUMN, reused as-is.
        let Expression::Column(gby) = &split.final_agg.group_by_items[0] else {
            panic!("column group-by stays a column");
        };
        assert_eq!(gby.unique_id, a.unique_id);
    }

    /// `avg(b)` splits into partial `count(b)` + `sum(b)`; the final avg
    /// reads both partial output columns (Go's NeedCount + NeedValue pair).
    #[test]
    fn an_avg_splits_into_count_plus_sum_below() {
        let b = bigint_col(1);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc();
        let avg = agg(names::AVG, &b, false);
        let original = AggInfo {
            schema: Schema::new(vec![Column::new(10, avg.base.ret_type.clone())]),
            agg_funcs: vec![avg],
            group_by_items: Vec::new(),
        };
        let split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false)
            .expect("two-phase");
        let names_below: Vec<&str> = split
            .partial
            .agg_funcs
            .iter()
            .map(AggFuncDesc::name)
            .collect();
        assert_eq!(names_below, [names::COUNT, names::SUM]);
        assert_eq!(split.final_agg.agg_funcs[0].name(), names::AVG);
        assert_eq!(split.final_agg.agg_funcs[0].base.args.len(), 2);
    }

    /// `select a, count(b) group by a`: the partial `firstrow(a)` duplicates
    /// the group-by output, so `remove_unnecessary_first_row` drops it, its
    /// schema column goes too, and the FINAL firstrow's argument is
    /// redirected to the final group-by column (Go `:460`'s example).
    #[test]
    fn a_group_by_first_row_is_removed_and_redirected() {
        let a = bigint_col(1);
        let b = bigint_col(2);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc();
        alloc.alloc();
        let first_row = agg(names::FIRST_ROW, &a, false);
        let count = agg(names::COUNT, &b, false);
        let original = AggInfo {
            schema: Schema::new(vec![
                Column::new(10, first_row.base.ret_type.clone()),
                Column::new(11, count.base.ret_type.clone()),
            ]),
            agg_funcs: vec![first_row, count],
            group_by_items: vec![Expression::Column(a.clone())],
        };
        let mut split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false)
            .expect("two-phase");
        assert_eq!(split.partial.agg_funcs.len(), 2);
        let schema_before = split.partial.schema.columns.len();

        let partial_funcs = std::mem::take(&mut split.partial.agg_funcs);
        let kept = remove_unnecessary_first_row(
            &mut split.final_agg.agg_funcs,
            &split.final_agg.group_by_items,
            partial_funcs,
            &split.partial.group_by_items,
            &mut split.partial.schema,
            &split.first_row_func_map,
        );
        // Only the count survives below.
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].name(), names::COUNT);
        assert_eq!(split.partial.schema.columns.len(), schema_before - 1);
        // The final firstrow now reads the final group-by column.
        let final_first_row = &split.final_agg.agg_funcs[0];
        assert_eq!(final_first_row.name(), names::FIRST_ROW);
        assert!(final_first_row.base.args[0].equal(&split.final_agg.group_by_items[0]));
    }

    /// `count(distinct a) group by c`, cop split (Go's own worked example at
    /// `:657`): the cop half groups by (c, a) with NO aggregate functions,
    /// and the root keeps `count(distinct ...)` over the pushed column.
    #[test]
    fn a_distinct_count_pushes_its_argument_into_the_partial_group_by() {
        let a = bigint_col(1);
        let c = bigint_col(2);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc();
        alloc.alloc();
        let count = agg(names::COUNT, &a, true);
        let original = AggInfo {
            schema: Schema::new(vec![Column::new(10, count.base.ret_type.clone())]),
            agg_funcs: vec![count],
            group_by_items: vec![Expression::Column(c.clone())],
        };
        let split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false)
            .expect("two-phase");
        // A cop partial outputs group-by values through its schema: no
        // firstrow is added, so no partial functions at all.
        assert!(split.partial.agg_funcs.is_empty());
        assert_eq!(split.partial.group_by_items.len(), 2);
        assert!(split.partial.group_by_items[1].equal(&Expression::Column(a.clone())));
        let final_count = &split.final_agg.agg_funcs[0];
        assert!(final_count.has_distinct);
        assert_eq!(final_count.mode, AggFunctionMode::Complete);
        // Its argument is the group-by column for a, not a itself... which
        // for a plain column IS the same column reference.
        assert!(final_count.base.args[0].equal(&Expression::Column(a)));
    }

    /// group_concat with order-by but WITHOUT distinct runs in one phase
    /// only: Go returns `(nil, original)`, here `None`.
    #[test]
    fn a_sorted_group_concat_without_distinct_refuses_to_split() {
        let a = bigint_col(1);
        let b = bigint_col(2);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc();
        alloc.alloc();
        let mut concat = agg(names::GROUP_CONCAT, &a, false);
        concat
            .order_by_items
            .push(ByItems::new(Expression::Column(b), false));
        let original = AggInfo {
            schema: Schema::new(vec![Column::new(10, concat.base.ret_type.clone())]),
            agg_funcs: vec![concat],
            group_by_items: Vec::new(),
        };
        assert!(
            build_final_mode_aggregation(&ctx(), &alloc, &original, true, false).is_none()
        );
    }
}
