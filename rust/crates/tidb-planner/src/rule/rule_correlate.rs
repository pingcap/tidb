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

//! Go `pkg/planner/core/rule_correlate.go` (`CorrelateSolver`) and the
//! logical-subtree clone of `plan_clone_utils.go` it depends on.

use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::column::{Column, CorrelatedColumn};
use tidb_expr::expr_util::extract::is_col_op_col;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::extract_cor_columns;
use tidb_planner_core_base::{LogicalJoinType, PlanError};
use tidb_planner_core_operator_logicalop::apply::LogicalApply;
use tidb_planner_core_operator_logicalop::join::LogicalJoin;
use tidb_planner_core_operator_logicalop::limit::LogicalLimit;
use tidb_planner_core_operator_logicalop::plan_context::{LogicalOptRule, RuleContext};
use tidb_planner_core_operator_logicalop::{BaseLogicalPlan, LogicalPlan, LogicalSelection};
use tidb_planner_util::path::AccessPath;

/// Go `CorrelateSolver`: turns a non-correlated semi join the rewriter marked
/// `PreferCorrelate` back into a correlated Apply, so the inner side can use
/// an index lookup driven by the outer row.
pub struct CorrelateSolver;

impl CorrelateSolver {
    /// Go `(*CorrelateSolver).correlate`.
    fn correlate(ctx: &RuleContext<'_>, mut p: LogicalPlan) -> Result<(LogicalPlan, bool), PlanError> {
        if matches!(p, LogicalPlan::CTE(_)) {
            return Ok((p, false));
        }

        let mut plan_changed = false;
        let children = std::mem::take(p.base_mut().children_mut());
        let mut new_children = Vec::with_capacity(children.len());
        for child in children {
            let (np, changed) = Self::correlate(ctx, child)?;
            plan_changed = plan_changed || changed;
            new_children.push(np);
        }
        p.base_mut().set_children(new_children);

        let LogicalPlan::Join(join) = &p else {
            return Ok((p, plan_changed));
        };
        if !is_semi_join(join.join_type) || !join.prefer_correlate {
            return Ok((p, plan_changed));
        }
        if join.equal_conditions.is_empty() {
            return Ok((p, plan_changed));
        }
        if !join.na_eq_conditions.is_empty()
            || !join.left_conditions.is_empty()
            || !join.other_conditions.is_empty()
        {
            return Ok((p, plan_changed));
        }

        let left_schema = join.base.children()[0].schema().cloned().unwrap_or_default();
        let right_schema = join.base.children()[1].schema().cloned().unwrap_or_default();

        // A NULL on either side of a LeftOuterSemi / AntiLeftOuterSemi key
        // answers NULL, which the correlated filter cannot.
        if matches!(
            join.join_type,
            LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi
        ) {
            for eq_cond in &join.equal_conditions {
                let Some((left_col, right_col)) = join_key_columns(eq_cond, &left_schema, &right_schema)
                else {
                    return Ok((p, plan_changed));
                };
                if !has_not_null_flag(left_col) || !has_not_null_flag(right_col) {
                    return Ok((p, plan_changed));
                }
            }
        }

        let mut sel_conds =
            Vec::with_capacity(join.equal_conditions.len() + join.right_conditions.len());
        let mut cor_cols = Vec::with_capacity(join.equal_conditions.len());
        for eq_cond in &join.equal_conditions {
            let Some((cond, cor_col)) =
                Self::build_correlated_cond(ctx, eq_cond, &left_schema, &right_schema)?
            else {
                return Ok((p, plan_changed));
            };
            sel_conds.push(cond);
            cor_cols.push(cor_col);
        }
        sel_conds.extend(join.right_conditions.iter().cloned());

        let Some(cloned_inner) = clone_logical_subtree(ctx, &join.base.children()[1]) else {
            return Ok((p, plan_changed));
        };
        let cloned_inner = lift_data_source_conds(ctx, cloned_inner);

        let query_block_offset = join.base.base.query_block_offset();
        let mut sel = LogicalSelection::new(
            BaseLogicalPlan::new(ctx.allocator, LogicalSelection::TYPE, query_block_offset),
            sel_conds,
        );
        sel.base.set_children(vec![cloned_inner]);

        let Ok((_, mut inner_plan)) = LogicalPlan::Selection(sel).predicate_push_down(ctx, Vec::new())
        else {
            return Ok((p, plan_changed));
        };

        reset_stats_for_correlated_ds(&mut inner_plan);

        if !has_limit(&inner_plan) {
            let mut limit = LogicalLimit::new(
                BaseLogicalPlan::new(ctx.allocator, LogicalLimit::TYPE, query_block_offset),
                0,
                1,
            );
            limit.base.set_children(vec![inner_plan]);
            inner_plan = LogicalPlan::Limit(limit);
        }

        let LogicalPlan::Join(mut join) = p else {
            unreachable!("the plan was matched as a join above");
        };
        let mut base = BaseLogicalPlan::new(ctx.allocator, LogicalApply::TYPE, query_block_offset);
        base.base.set_schema_shared(join.base.base.schema_shared().cloned());
        base.base
            .set_output_names_shared(join.base.base.output_names_shared().cloned());
        let left = join.base.children_mut().swap_remove(0);
        base.set_children(vec![left, inner_plan]);
        let ap = LogicalApply {
            join: LogicalJoin {
                base,
                join_type: join.join_type,
                hint_info: join.hint_info,
                prefer_join_type: join.prefer_join_type,
                prefer_join_order: join.prefer_join_order,
                left_prefer_join_type: join.left_prefer_join_type,
                right_prefer_join_type: join.right_prefer_join_type,
                ..LogicalJoin::default()
            },
            cor_cols,
            ..LogicalApply::default()
        };
        Ok((LogicalPlan::Apply(ap), true))
    }

    /// Go `(*CorrelateSolver).buildCorrelatedCond`: `rightCol <op>
    /// CorCol(leftCol)` for one join key.
    fn build_correlated_cond(
        ctx: &RuleContext<'_>,
        eq_cond: &ScalarFunction,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<Option<(Expression, CorrelatedColumn)>, PlanError> {
        let Some((left_col, right_col)) = join_key_columns(eq_cond, left_schema, right_schema) else {
            return Ok(None);
        };
        let cor_col = CorrelatedColumn::new(left_col.clone());
        let cond = ctx
            .builder
            .new_function(
                eq_cond.func_name.lowercase(),
                Some(FieldType::new(FieldTypeCode::Tiny)),
                vec![
                    Expression::Column(right_col.clone()),
                    Expression::CorrelatedColumn(cor_col.clone()),
                ],
            )
            .map_err(|error| PlanError::internal(error.to_string()))?;
        Ok(Some((cond, cor_col)))
    }
}

/// Go `JoinType.IsSemiJoin()`.
const fn is_semi_join(join_type: LogicalJoinType) -> bool {
    matches!(
        join_type,
        LogicalJoinType::Semi
            | LogicalJoinType::AntiSemi
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiLeftOuterSemi
    )
}

fn has_not_null_flag(column: &Column) -> bool {
    column
        .ret_type
        .as_ref()
        .is_some_and(|field_type| field_type.has_flag(FieldTypeFlags::NOT_NULL))
}

/// The outer and the inner column of a `col op col` join key, whichever
/// order the condition names them in.
fn join_key_columns<'a>(
    eq_cond: &ScalarFunction,
    left_schema: &'a Schema,
    right_schema: &'a Schema,
) -> Option<(&'a Column, &'a Column)> {
    let (col0, col1) = is_col_op_col(eq_cond)?;
    match (left_schema.retrieve_column(col0), right_schema.retrieve_column(col1)) {
        (Some(left_col), Some(right_col)) => Some((left_col, right_col)),
        _ => Some((
            left_schema.retrieve_column(col1)?,
            right_schema.retrieve_column(col0)?,
        )),
    }
}

/// Go `cloneLogicalSubtree`: a copy of the subtree under fresh plan ids, or
/// None when it holds an operator the clone does not cover.
fn clone_logical_subtree(ctx: &RuleContext<'_>, p: &LogicalPlan) -> Option<LogicalPlan> {
    if !matches!(
        p,
        LogicalPlan::DataSource(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Selection(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Aggregation(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::TopN(_)
    ) {
        return None;
    }
    // Go `cloneWithChildren`.
    let children = p
        .children()
        .iter()
        .map(|child| clone_logical_subtree(ctx, child))
        .collect::<Option<Vec<_>>>()?;
    let mut clone = p.clone_shallow();
    // Go `NewBaseLogicalPlan`: a new id and none of the derived state; the
    // schema and names are the copied operator's own.
    let source = &p.base().base;
    let mut base = BaseLogicalPlan::new(ctx.allocator, source.tp(), source.query_block_offset());
    base.base.set_schema_shared(source.schema_shared().cloned());
    base.base
        .set_output_names_shared(source.output_names_shared().cloned());
    match &mut clone {
        LogicalPlan::DataSource(_) => base.base.set_stats(source.stats_info().cloned()),
        LogicalPlan::Join(join) => join.prefer_correlate = false,
        _ => {}
    }
    base.set_children(children);
    *clone.base_mut() = base;
    Some(clone)
}

/// Go `liftDataSourceConds`: the conditions predicate push-down left in each
/// DataSource go back into a Selection above it, so the push-down that
/// follows sees them again beside the correlated conditions.
fn lift_data_source_conds(ctx: &RuleContext<'_>, mut p: LogicalPlan) -> LogicalPlan {
    let children = std::mem::take(p.base_mut().children_mut());
    let children = children
        .into_iter()
        .map(|child| lift_data_source_conds(ctx, child))
        .collect();
    p.base_mut().set_children(children);

    let LogicalPlan::DataSource(ds) = &mut p else {
        return p;
    };
    if ds.all_conds.is_empty() {
        return p;
    }
    let conditions = std::mem::take(&mut ds.all_conds);
    ds.pushed_down_conds = Vec::new();
    let mut sel = LogicalSelection::new(
        BaseLogicalPlan::new(
            ctx.allocator,
            LogicalSelection::TYPE,
            ds.base.base.query_block_offset(),
        ),
        conditions,
    );
    sel.base.set_children(vec![p]);
    LogicalPlan::Selection(sel)
}

/// Go `resetStatsForCorrelatedDS`: a DataSource that now filters on a
/// correlated column starts its access paths over, and it and every operator
/// above it drop their statistics, so both are derived again with the
/// correlated conditions in place.
fn reset_stats_for_correlated_ds(p: &mut LogicalPlan) -> bool {
    let mut has_correlated = false;

    if let LogicalPlan::DataSource(ds) = p {
        has_correlated = ds
            .all_conds
            .iter()
            .any(|cond| !extract_cor_columns(cond).is_empty());
        if has_correlated {
            ds.all_possible_access_paths = ds
                .all_possible_access_paths
                .iter()
                .map(fresh_access_path)
                .collect();
            ds.possible_access_paths = None;
        }
    }

    for child in p.base_mut().children_mut() {
        if reset_stats_for_correlated_ds(child) {
            has_correlated = true;
        }
    }

    if has_correlated {
        p.set_stats(None);
    }
    has_correlated
}

/// Go `freshAccessPath`: the path as enumeration made it, without anything
/// statistics derivation filled in.
fn fresh_access_path(src: &AccessPath) -> AccessPath {
    AccessPath {
        index: src.index.clone(),
        store_type: src.store_type,
        is_int_handle_path: src.is_int_handle_path,
        is_common_handle_path: src.is_common_handle_path,
        forced: src.forced,
        force_keep_order: src.force_keep_order,
        force_no_keep_order: src.force_no_keep_order,
        is_uk_shard_index_path: src.is_uk_shard_index_path,
        index_lookup_push_down_by: src.index_lookup_push_down_by,
        noncacheable_reason: src.noncacheable_reason.clone(),
        ..AccessPath::default()
    }
}

/// Go `hasLimit`.
fn has_limit(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Limit(_)) || plan.children().iter().any(has_limit)
}

impl LogicalOptRule for CorrelateSolver {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let recovery = plan.clone();
        Self::correlate(ctx, plan).map_err(|error| (recovery, error))
    }

    fn name(&self) -> &'static str {
        "correlate"
    }
}
