//! Go `pkg/planner/core/rule_aggregation_push_down.go`, Go rule #19
//! (`AggregationPushDownSolver`).
//!
//! Three transformations, in the order Go's `aggPushDown` reaches them for an
//! `Aggregation` node:
//!
//! 1. push the aggregation's functions into both sides of a JOIN child
//!    (`tidb_opt_agg_push_down` gated), splitting functions by child and
//!    treating every join-condition column as a group-by column of the pushed
//!    partial aggregation;
//! 2. cross a PROJECTION child unconditionally, substituting the projection's
//!    expressions into the group-by items and the aggregate arguments and
//!    dropping the projection from the tree;
//! 3. push a two-phase (partial/final) aggregation through a UNION ALL or
//!    PARTITION UNION child — the partition-union arm is NOT gated by
//!    `tidb_opt_agg_push_down`, mirroring Go's own asymmetry.
//!
//! Go shares `*AggFuncDesc` pointers between the parent aggregation and the
//! pushed-down children (`decompose` mutates the parent's descriptors into
//! final mode through the collected pointers). The port models that aliasing
//! with INDICES into the parent's `agg_funcs`: [`make_new_agg`] mutates the
//! indexed descriptors in place (mode → final, args → the partial outputs) and
//! clones the result into the child, so the parent keeps final-mode
//! descriptors exactly as Go leaves them.

use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::aggregation::{names, AggFuncDesc, AggFunctionMode, ByItems};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::extract::{
    extract_columns_from_col_op_col, extract_columns_map_from_expressions,
};
use tidb_expr::expr_util::exprs_has_side_effects;
use tidb_expr::expr_util::substitute::{column_substitute, column_substitute_impl, SubstituteOptions};
use tidb_expr::expression::Expression;
use tidb_expr::schema::{merge_schema, Schema};
use tidb_expr::simple_expr::extract_columns;
use tidb_expr::NoColumns;

use crate::final_mode_agg::{build_final_mode_aggregation, AggInfo};
use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanError;

use super::rule::{LogicalOptRule, RuleContext};
use super::{
    BaseLogicalPlan, LogicalAggregation, LogicalJoin, LogicalPlan, LogicalProjection,
    LogicalUnionAll,
};

/// Go `isDecomposableWithJoin` (`rule_aggregation_push_down.go:44`).
///
/// An aggregation function F is decomposable when
/// `F(S1 ∪ S2) == F2(F1(S1), F1(S2))`; max/min/firstrow always, sum/count
/// only without DISTINCT, never avg/group-concat/variance/stddev/approx.
fn is_decomposable_with_join(fun: &AggFuncDesc) -> bool {
    if !fun.order_by_items.is_empty() {
        return false;
    }
    match fun.name() {
        names::AVG
        | names::GROUP_CONCAT
        | names::VAR_POP
        | names::JSON_ARRAYAGG
        | names::JSON_OBJECTAGG
        | names::STDDEV_POP
        | names::VAR_SAMP
        | names::APPROX_PERCENTILE
        | names::STDDEV_SAMP => false,
        names::MAX | names::MIN | names::FIRST_ROW => true,
        names::SUM | names::COUNT => !fun.has_distinct,
        _ => false,
    }
}

/// Go `isDecomposableWithUnion` (`rule_aggregation_push_down.go:61`). Note the
/// asymmetry with [`is_decomposable_with_join`]: avg and
/// approx-count-distinct decompose across a union, and var_samp/stddev fall to
/// the default `false`.
fn is_decomposable_with_union(fun: &AggFuncDesc) -> bool {
    if !fun.order_by_items.is_empty() {
        return false;
    }
    match fun.name() {
        names::GROUP_CONCAT
        | names::VAR_POP
        | names::JSON_ARRAYAGG
        | names::APPROX_PERCENTILE
        | names::JSON_OBJECTAGG => false,
        names::MAX | names::MIN | names::FIRST_ROW => true,
        names::SUM | names::COUNT | names::AVG | names::APPROX_COUNT_DISTINCT => true,
        _ => false,
    }
}

/// Go `getAggFuncChildIdx` (`rule_aggregation_push_down.go:79`): 0 left,
/// 1 right, -1 both, 2 neither (`count(*)`, `sum(1)`).
fn get_agg_func_child_idx(agg_func: &AggFuncDesc, l_schema: &Schema, r_schema: &Schema) -> i32 {
    let mut from_left = false;
    let mut from_right = false;
    let cols = extract_columns_map_from_expressions(None, agg_func.args());
    for col in cols.values() {
        if l_schema.contains(col) {
            from_left = true;
        }
        if r_schema.contains(col) {
            from_right = true;
        }
    }
    if from_left && from_right {
        -1
    } else if from_left {
        0
    } else if from_right {
        1
    } else {
        2
    }
}

/// Go `checkAllArgsColumn` (`rule_aggregation_push_down.go:317`): every
/// argument is a bare column (`count(a)` yes, `count(*)`/`sum(a+1)` no).
fn check_all_args_column(fun: &AggFuncDesc) -> bool {
    fun.args()
        .iter()
        .all(|arg| matches!(arg, Expression::Column(_)))
}

/// Go `collectAggFuncs` (`rule_aggregation_push_down.go:103`), keyed by INDEX
/// into the aggregation's own `agg_funcs` — see this module's header for the
/// pointer-aliasing note. `None` is Go's `valid = false`.
fn collect_agg_funcs(
    funcs: &[AggFuncDesc],
    l_schema: &Schema,
    r_schema: &Schema,
    join_type: LogicalJoinType,
) -> Option<(Vec<usize>, Vec<usize>)> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for (offset, agg_func) in funcs.iter().enumerate() {
        if !is_decomposable_with_join(agg_func) {
            return None;
        }
        match get_agg_func_child_idx(agg_func, l_schema, r_schema) {
            0 => {
                if join_type == LogicalJoinType::RightOuter && !check_all_args_column(agg_func) {
                    return None;
                }
                left.push(offset);
            }
            1 => {
                if join_type == LogicalJoinType::LeftOuter && !check_all_args_column(agg_func) {
                    return None;
                }
                right.push(offset);
            }
            // constant arguments: either side is fine, ideally the hash build
            // side
            2 => match join_type {
                LogicalJoinType::LeftOuter => left.push(offset),
                LogicalJoinType::RightOuter => right.push(offset),
                _ => right.push(offset),
            },
            _ => return None,
        }
    }
    Some((left, right))
}

/// Go `addGbyCol` (`rule_aggregation_push_down.go:198`): dedup by column
/// identity. Go's `Column.Equal` is `UniqueID` equality.
fn add_gby_col(gby_cols: &mut Vec<Column>, col: Column) {
    if !gby_cols.iter().any(|gby| gby.unique_id == col.unique_id) {
        gby_cols.push(col);
    }
}

/// Go `collectGbyCols` (`rule_aggregation_push_down.go:146`): group-by items
/// are appended verbatim (no dedup), every join-condition column goes through
/// the deduplicating [`add_gby_col`].
fn collect_gby_cols(
    agg: &LogicalAggregation,
    l_schema: &Schema,
    join: &LogicalJoin,
) -> (Vec<Column>, Vec<Column>) {
    let mut left_gby_cols: Vec<Column> = Vec::new();
    let mut right_gby_cols: Vec<Column> = Vec::new();
    for gby_expr in &agg.group_by_items {
        for col in extract_columns(gby_expr) {
            if l_schema.contains(&col) {
                left_gby_cols.push(col);
            } else {
                right_gby_cols.push(col);
            }
        }
    }
    for eq_func in &join.equal_conditions {
        if let Some((l, r)) = extract_columns_from_col_op_col(eq_func) {
            add_gby_col(&mut left_gby_cols, l.clone());
            add_gby_col(&mut right_gby_cols, r.clone());
        }
    }
    for left_cond in &join.left_conditions {
        for col in extract_columns(left_cond) {
            add_gby_col(&mut left_gby_cols, col);
        }
    }
    for right_cond in &join.right_conditions {
        for col in extract_columns(right_cond) {
            add_gby_col(&mut right_gby_cols, col);
        }
    }
    for other_cond in &join.other_conditions {
        for col in extract_columns(other_cond) {
            if l_schema.contains(&col) {
                add_gby_col(&mut left_gby_cols, col);
            } else {
                add_gby_col(&mut right_gby_cols, col);
            }
        }
    }
    (left_gby_cols, right_gby_cols)
}

/// Go `checkValidJoin` (`rule_aggregation_push_down.go:215`).
fn check_valid_join(join: &LogicalJoin) -> bool {
    matches!(
        join.join_type,
        LogicalJoinType::Inner | LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter
    )
}

/// Go `checkAnyCountAndSum` (`rule_aggregation_push_down.go:306`), over the
/// INDEXED descriptors.
fn check_any_count_and_sum(funcs: &[AggFuncDesc], idxs: &[usize]) -> bool {
    idxs.iter()
        .any(|&i| funcs[i].name() == names::SUM || funcs[i].name() == names::COUNT)
}

/// Go `decompose` (`rule_aggregation_push_down.go:221`): turn `agg_func` into
/// its FINAL mode over the partial output column appended to `schema`. When
/// the partial aggregation sits on the null-generating side of an outer join,
/// the final function's argument copy loses the NOT-NULL flag — the schema
/// column keeps it, exactly as Go clears the flag only on the argument copy.
fn decompose(
    ctx: &RuleContext<'_>,
    agg_func: &mut AggFuncDesc,
    schema: &mut Schema,
    null_generating: bool,
) {
    let ret_type = agg_func.ret_type().clone();
    let output = Column::new(ctx.column_allocator.alloc(), ret_type.clone());
    schema.append(std::iter::once(output.clone()));
    let arg = if null_generating {
        let mut arg_type = ret_type;
        arg_type.del_flags(FieldTypeFlags::NOT_NULL);
        let mut arg = output;
        arg.ret_type = Some(arg_type);
        arg
    } else {
        output
    };
    agg_func.base.args = vec![Expression::Column(arg)];
    agg_func.mode = AggFunctionMode::Final;
}

/// Go `makeNewAgg` (`rule_aggregation_push_down.go:330`): one FINAL-mode
/// descriptor per collected function (through [`decompose`]) plus one
/// `firstrow` per group-by column.
#[allow(clippy::too_many_arguments)]
fn make_new_agg(
    ctx: &RuleContext<'_>,
    funcs: &mut [AggFuncDesc],
    func_idxs: &[usize],
    gby_cols: &[Column],
    prefer_agg_type: u32,
    prefer_agg_to_cop: bool,
    block_offset: i32,
    null_generating: bool,
) -> Result<LogicalAggregation, PlanError> {
    let base = BaseLogicalPlan::new(ctx.allocator, LogicalAggregation::TYPE, block_offset);
    let mut agg = LogicalAggregation::new(
        base,
        Vec::new(),
        gby_cols
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect::<Vec<Expression>>(),
    );
    agg.prefer_agg_type = prefer_agg_type;
    agg.prefer_agg_to_cop = prefer_agg_to_cop;
    let agg_len = func_idxs.len() + gby_cols.len();
    let mut new_agg_func_descs = Vec::with_capacity(agg_len);
    let mut schema = Schema::new(Vec::with_capacity(agg_len));
    for &i in func_idxs {
        // Go clones the descriptor BEFORE `decompose` mutates the original:
        // the pushed-down child keeps the ORIGINAL arguments in its original
        // mode, while the parent's descriptor becomes FinalMode over the
        // partial output column ("we can confuse" partial and complete mode
        // here, as Go's own comment says).
        let child_copy = funcs[i].clone();
        decompose(ctx, &mut funcs[i], &mut schema, null_generating);
        new_agg_func_descs.push(child_copy);
    }
    for gby_col in gby_cols {
        let first_row = AggFuncDesc::new(
            &NoColumns,
            names::FIRST_ROW,
            vec![Expression::Column(gby_col.clone())],
            false,
        )
        .map_err(|error| PlanError::internal(error.to_string()))?;
        let mut new_col = gby_col.clone();
        new_col.ret_type = Some(first_row.ret_type().clone());
        new_agg_func_descs.push(first_row);
        schema.append(std::iter::once(new_col));
    }
    agg.agg_funcs = new_agg_func_descs;
    agg.base.base.set_schema(Some(schema));
    Ok(agg)
}

/// Go `getDefaultValues` (`rule_aggregation_push_down.go:293`): one value per
/// aggregate for the outer-join NULL row; `None` when any function cannot
/// produce one (evaluation error or the avg/group_concat arm).
fn get_default_values(agg: &LogicalAggregation, child_schema: &Schema) -> Option<Vec<Datum>> {
    let mut default_values = Vec::with_capacity(agg.agg_funcs.len());
    for agg_func in &agg.agg_funcs {
        match agg_func.eval_null_value_in_outer_join(&NoColumns, child_schema) {
            Ok((value, true)) => default_values.push(value),
            // Go treats an evaluation error as "no default value".
            _ => return None,
        }
    }
    Some(default_values)
}

/// Go `tryToPushDownAgg` (`rule_aggregation_push_down.go:253`), for ONE join
/// path. `Ok(None)` is Go's `return child, nil` (no pushdown). When the outer
/// join defaults apply to this side, `join.default_values` is REPLACED — with
/// the computed values, or with an empty vec (Go's nil) when no default exists
/// and the pushdown is abandoned. The two sides' conditions are mutually
/// exclusive, so at most one call replaces the slice, in Go's order.
#[allow(clippy::too_many_arguments)]
fn try_to_push_down_agg(
    ctx: &RuleContext<'_>,
    funcs: &mut [AggFuncDesc],
    func_idxs: &[usize],
    gby_cols: &[Column],
    child: &LogicalPlan,
    child_idx: usize,
    join: &mut LogicalJoin,
    prefer_agg_type: u32,
    prefer_agg_to_cop: bool,
    block_offset: i32,
) -> Result<Option<LogicalPlan>, PlanError> {
    let all_first_row = func_idxs
        .iter()
        .all(|&i| funcs[i].name() == names::FIRST_ROW);
    if all_first_row {
        return Ok(None);
    }
    // If the join is multiway-join, we forbid pushing down.
    if matches!(child, LogicalPlan::Join(_)) {
        return Ok(None);
    }
    // If the pushed aggregation is grouped by unique key, it's no need to
    // push it down.
    let child_schema = child.schema_or_empty();
    let tmp_schema = Schema::new(gby_cols.to_vec());
    for key in &child_schema.pk_or_uk {
        if tmp_schema.columns_indices(key).is_some() {
            return Ok(None);
        }
    }
    let join_type = join.join_type;
    let null_generating = (join_type == LogicalJoinType::LeftOuter && child_idx == 1)
        || (join_type == LogicalJoinType::RightOuter && child_idx == 0);
    let agg = make_new_agg(
        ctx,
        funcs,
        func_idxs,
        gby_cols,
        prefer_agg_type,
        prefer_agg_to_cop,
        block_offset,
        null_generating,
    )?;
    let mut pushed = LogicalPlan::Aggregation(agg);
    pushed.set_children(vec![child.clone()]);
    // If agg has no group-by item, it will return a default value, which may
    // cause some bugs. So here we add a group-by item forcely.
    let LogicalPlan::Aggregation(agg) = &mut pushed else {
        return Ok(None);
    };
    if agg.group_by_items.is_empty() {
        agg.group_by_items = vec![Expression::Constant(Constant::new(
            Datum::Int(0),
            FieldType::new(FieldTypeCode::Long),
        ))];
    }
    if (child_idx == 0 && join_type == LogicalJoinType::RightOuter)
        || (child_idx == 1 && join_type == LogicalJoinType::LeftOuter)
    {
        match get_default_values(agg, &child_schema) {
            Some(values) => join.default_values = values,
            None => {
                join.default_values = Vec::new();
                return Ok(None);
            }
        }
    }
    Ok(Some(pushed))
}

/// Go `splitPartialAgg` (`rule_aggregation_push_down.go:362`): rewrite `agg`
/// into its FINAL mode in place and return the PARTIAL half to push. `None`
/// means Go's `partial == nil` — the aggregation is untouched.
fn split_partial_agg(ctx: &RuleContext<'_>, agg: &mut LogicalAggregation) -> Option<LogicalAggregation> {
    let original = AggInfo {
        agg_funcs: agg.agg_funcs.clone(),
        group_by_items: agg.group_by_items.clone(),
        schema: agg.base.base.schema().cloned()?,
    };
    let split =
        build_final_mode_aggregation(&NoColumns, ctx.column_allocator, &original, false, false)?;
    let final_agg = split.final_agg;
    agg.base.base.set_schema(Some(final_agg.schema.clone()));
    agg.agg_funcs = final_agg.agg_funcs;
    agg.group_by_items = final_agg.group_by_items;

    let mut pushed = LogicalAggregation::new(
        BaseLogicalPlan::new(
            ctx.allocator,
            LogicalAggregation::TYPE,
            agg.base.base.query_block_offset(),
        ),
        split.partial.agg_funcs,
        split.partial.group_by_items,
    );
    pushed.prefer_agg_type = agg.prefer_agg_type;
    pushed.prefer_agg_to_cop = agg.prefer_agg_to_cop;
    pushed.base.base.set_schema(Some(split.partial.schema));
    Some(pushed)
}

/// Go `pushAggCrossUnion` (`rule_aggregation_push_down.go:388`): build the
/// per-union-child aggregation by substituting the union's schema columns for
/// the child's, then either keep it or (when a child unique key is covered by
/// the group-by) collapse it into a projection.
fn push_agg_cross_union(
    ctx: &RuleContext<'_>,
    agg: &LogicalAggregation,
    union_schema: &Schema,
    union_child: LogicalPlan,
) -> Result<LogicalPlan, PlanError> {
    let child_schema = union_child.schema_or_empty();
    let child_exprs: Vec<Expression> = child_schema
        .columns
        .iter()
        .cloned()
        .map(Expression::Column)
        .collect();
    let base = BaseLogicalPlan::new(
        ctx.allocator,
        LogicalAggregation::TYPE,
        agg.base.base.query_block_offset(),
    );
    let mut new_agg = LogicalAggregation::new(base, Vec::new(), Vec::new());
    new_agg.prefer_agg_type = agg.prefer_agg_type;
    new_agg.prefer_agg_to_cop = agg.prefer_agg_to_cop;
    new_agg.base.base.set_schema(Some(
        agg.base
            .base
            .schema()
            .cloned()
            .ok_or_else(|| PlanError::internal("union pushdown aggregation has no schema"))?,
    ));
    let opts = SubstituteOptions::new(ctx.builder);
    for agg_func in &agg.agg_funcs {
        let mut new_func = agg_func.clone();
        let new_args = agg_func
            .args()
            .iter()
            .map(|arg| column_substitute(arg, union_schema, &child_exprs, &opts))
            .collect();
        new_func.base.args = new_args;
        new_agg.agg_funcs.push(new_func);
    }
    for gby_expr in &agg.group_by_items {
        let new_expr = column_substitute(gby_expr, union_schema, &child_exprs, &opts);
        new_agg.group_by_items.push(new_expr);
        // TODO(upstream): if there is a duplicated first_row function, we can
        // delete it. Go builds this firstrow from the ORIGINAL group-by item
        // and copies the leading aggregate's mode.
        let mut first_row = AggFuncDesc::new(&NoColumns, names::FIRST_ROW, vec![gby_expr.clone()], false)
            .map_err(|error| PlanError::internal(error.to_string()))?;
        first_row.mode = agg
            .agg_funcs
            .first()
            .map(|func| func.mode)
            .unwrap_or(AggFunctionMode::Partial1);
        new_agg.agg_funcs.push(first_row);
    }
    let tmp_schema = Schema::new(new_agg.get_group_by_cols());
    for key in &child_schema.pk_or_uk {
        if tmp_schema.columns_indices(key).is_some() {
            if let Some(expressions) = convert_agg_to_proj(ctx, &new_agg)? {
                let mut proj_base = BaseLogicalPlan::new(
                    ctx.allocator,
                    LogicalProjection::TYPE,
                    new_agg.base.base.query_block_offset(),
                );
                proj_base.base.set_schema(new_agg.base.base.schema().cloned());
                proj_base
                    .base
                    .set_output_names(new_agg.base.base.output_names().to_vec());
                proj_base.set_children(vec![union_child]);
                return Ok(LogicalPlan::Projection(LogicalProjection::new(
                    proj_base,
                    expressions,
                )));
            }
            break;
        }
    }
    new_agg.base.set_children(vec![union_child]);
    Ok(LogicalPlan::Aggregation(new_agg))
}

/// Go `tryAggPushDownForUnion` (`rule_aggregation_push_down.go:446`). The
/// bool answers Go's "did anything change": `false` leaves the union
/// untouched (decomposability refusal or split refusal).
fn try_agg_push_down_for_union(
    ctx: &RuleContext<'_>,
    agg: &mut LogicalAggregation,
    union: LogicalUnionAll,
) -> Result<(LogicalUnionAll, bool), PlanError> {
    if agg
        .agg_funcs
        .iter()
        .any(|func| !is_decomposable_with_union(func))
    {
        return Ok((union, false));
    }
    let Some(mut pushed) = split_partial_agg(ctx, agg) else {
        return Ok((union, false));
    };
    // Update the agg mode for the pushed down aggregation.
    for func in &mut pushed.agg_funcs {
        if func.mode == AggFunctionMode::Complete {
            func.mode = AggFunctionMode::Partial1;
        } else if func.mode == AggFunctionMode::Final {
            func.mode = AggFunctionMode::Partial2;
        }
    }
    let mut union = union;
    let union_schema = union
        .base
        .base
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("union has no schema"))?;
    let children = union.base.take_children();
    let mut new_children = Vec::with_capacity(children.len());
    for child in children {
        new_children.push(push_agg_cross_union(ctx, &pushed, &union_schema, child)?);
    }
    let schema = Schema::new(new_children[0].schema_or_empty().columns);
    union.base.base.set_schema(Some(schema));
    union.base.set_children(new_children);
    Ok((union, true))
}

/// Go `rewriteExpr`/`ConvertAggToProj` (`rule_aggregation_elimination.go`):
/// the per-function rewrite. `None` is Go's `ok = false`.
fn convert_agg_to_proj(
    ctx: &RuleContext<'_>,
    agg: &LogicalAggregation,
) -> Result<Option<Vec<Expression>>, PlanError> {
    let mut expressions = Vec::with_capacity(agg.agg_funcs.len());
    for func in &agg.agg_funcs {
        match super::rule_aggregation_elimination::rewrite_aggregate(ctx, func)? {
            Some(expression) => expressions.push(expression),
            None => return Ok(None),
        }
    }
    Ok(Some(expressions))
}

/// Go `CheckCanConvertAggToProj` (`rule_aggregation_elimination.go`): refuse
/// when an aggregate argument can be NULL-filled by the outer join's inner
/// side — the projection rewrite would mis-count those NULL rows.
fn check_can_convert_agg_to_proj(agg: &LogicalAggregation, child: &LogicalPlan) -> bool {
    let LogicalPlan::Join(join) = child else {
        return true;
    };
    let may_null_child = match join.join_type {
        LogicalJoinType::LeftOuter => join.base.children().get(1),
        LogicalJoinType::RightOuter => join.base.children().first(),
        _ => return true,
    };
    let Some(may_null_child) = may_null_child else {
        return true;
    };
    let may_null_schema = may_null_child.schema_or_empty();
    for func in &agg.agg_funcs {
        let may_null_cols = extract_columns_map_from_expressions(
            Some(&|column: &Column| may_null_schema.contains(column)),
            func.args(),
        );
        if !may_null_cols.is_empty() {
            return false;
        }
    }
    true
}

/// Go `tryToEliminateAggregation` (`rule_aggregation_elimination.go:52`) with
/// the checker's `oldAggEliminationCheck` flag made explicit. `child` is the
/// aggregation's (current) child — Go reads it live through the plan pointer.
fn try_to_eliminate_aggregation(
    ctx: &RuleContext<'_>,
    agg: &LogicalAggregation,
    child: &LogicalPlan,
    old_check: bool,
) -> Result<Option<Vec<Expression>>, PlanError> {
    if agg
        .agg_funcs
        .iter()
        .any(|func| func.name() == names::GROUP_CONCAT)
    {
        return Ok(None);
    }
    let schema_by_groupby = Schema::new(agg.get_group_by_cols());
    let child_schema = child.schema_or_empty();
    let covered_by_unique_key = child_schema
        .pk_or_uk
        .iter()
        .any(|key| schema_by_groupby.columns_indices(key).is_some());
    if covered_by_unique_key {
        if old_check && !check_can_convert_agg_to_proj(agg, child) {
            return Ok(None);
        }
        return convert_agg_to_proj(ctx, agg);
    }
    Ok(None)
}

/// Builds Go's `proj.SetChildren(agg.Children()[0])` / `SetSchema(agg.Schema())`
/// projection, carrying the aggregation's output names like the aggregation
/// elimination fold does.
fn elimination_projection(
    ctx: &RuleContext<'_>,
    agg: &LogicalAggregation,
    expressions: Vec<Expression>,
    child: LogicalPlan,
) -> LogicalPlan {
    let mut base = BaseLogicalPlan::new(
        ctx.allocator,
        LogicalProjection::TYPE,
        agg.base.base.query_block_offset(),
    );
    base.base.set_schema(agg.base.base.schema().cloned());
    base.base
        .set_output_names(agg.base.base.output_names().to_vec());
    base.set_children(vec![child]);
    LogicalPlan::Projection(LogicalProjection::new(base, expressions))
}

/// The expression's evaluation type, Go `expr.GetType(ectx).EvalType()`. An
/// untyped expression cannot be compared, so the crossing refuses it.
fn eval_type_of(expr: &Expression) -> Option<EvalType> {
    expr.static_type().map(|field_type| field_type.eval_type())
}

/// What Go's PROJECTION arm leaves behind: either the crossing succeeded (the
/// aggregation keeps the projection's child; the projection is dropped) or it
/// refused (the untouched projection stays the aggregation's child).
#[allow(clippy::large_enum_variant)]
enum CrossOutcome {
    Crossed(LogicalPlan),
    Kept(LogicalProjection),
}

/// Go `aggPushDown`'s PROJECTION arm (`rule_aggregation_push_down.go:529`):
/// substitute the projection's expressions into the group-by items, the
/// aggregate arguments, and the aggregate order-by items; any substitution
/// failure, side effect, or eval-type mismatch aborts the whole crossing.
fn cross_projection(
    ctx: &RuleContext<'_>,
    agg: &mut LogicalAggregation,
    mut projection: LogicalProjection,
) -> Result<CrossOutcome, PlanError> {
    let projection_schema = projection
        .base
        .base
        .schema()
        .cloned()
        .ok_or_else(|| PlanError::internal("projection has no schema"))?;
    let projection_expressions = projection.exprs.clone();
    let Some(projection_child) = projection.base.take_children().into_iter().next() else {
        return Ok(CrossOutcome::Kept(projection));
    };
    let opts = SubstituteOptions::new(ctx.builder);
    let mut no_side_effects = true;

    let mut new_gby_items = Vec::with_capacity(agg.group_by_items.len());
    for gby_item in &agg.group_by_items {
        let outcome =
            column_substitute_impl(gby_item, &projection_schema, &projection_expressions, true, &opts);
        if outcome.has_fail {
            no_side_effects = false;
            break;
        }
        new_gby_items.push(outcome.expr);
        if exprs_has_side_effects(&new_gby_items) {
            no_side_effects = false;
            break;
        }
    }

    // Go tracks per-function old/new argument lists and old/new order-by
    // lists; `None` marks Go's nil placeholder for a function without
    // order-by.
    let mut old_args_per_func: Vec<Vec<Expression>> = Vec::new();
    let mut new_args_per_func: Vec<Vec<Expression>> = Vec::new();
    let mut old_order_per_func: Vec<Vec<ByItems>> = Vec::new();
    let mut new_order_per_func: Vec<Option<Vec<ByItems>>> = Vec::new();
    if no_side_effects {
        for agg_func in &agg.agg_funcs {
            let old_args = agg_func.args().to_vec();
            let mut new_args = Vec::with_capacity(old_args.len());
            let mut args_failed = false;
            for arg in &old_args {
                let outcome = column_substitute_impl(
                    arg,
                    &projection_schema,
                    &projection_expressions,
                    true,
                    &opts,
                );
                if outcome.has_fail {
                    args_failed = true;
                    break;
                }
                new_args.push(outcome.expr);
            }
            if args_failed || exprs_has_side_effects(&new_args) {
                no_side_effects = false;
                break;
            }
            old_args_per_func.push(old_args);
            new_args_per_func.push(new_args);
            if agg_func.order_by_items.is_empty() {
                old_order_per_func.push(Vec::new());
                new_order_per_func.push(None);
            } else {
                old_order_per_func.push(agg_func.order_by_items.clone());
                let mut one_agg_order_items = Vec::with_capacity(agg_func.order_by_items.len());
                let mut order_failed = false;
                for (offset, oby) in agg_func.order_by_items.iter().enumerate() {
                    let outcome = column_substitute_impl(
                        &oby.expr,
                        &projection_schema,
                        &projection_expressions,
                        true,
                        &opts,
                    );
                    if outcome.has_fail {
                        order_failed = true;
                        break;
                    }
                    one_agg_order_items.push(ByItems::new(
                        outcome.expr,
                        agg_func.order_by_items[offset].desc,
                    ));
                }
                let order_side_effects = !order_failed
                    && exprs_has_side_effects(
                        &one_agg_order_items
                            .iter()
                            .map(|item| item.expr.clone())
                            .collect::<Vec<Expression>>(),
                    );
                if order_failed || order_side_effects {
                    no_side_effects = false;
                    break;
                }
                new_order_per_func.push(Some(one_agg_order_items));
            }
        }
    }
    if no_side_effects {
        // A substitution happened — check the eval type compatibility.
        'types: for (offset, funcs_args) in old_args_per_func.iter().enumerate() {
            for (arg_offset, old_arg) in funcs_args.iter().enumerate() {
                let (Some(old_type), Some(new_type)) = (
                    eval_type_of(old_arg),
                    eval_type_of(&new_args_per_func[offset][arg_offset]),
                ) else {
                    no_side_effects = false;
                    break 'types;
                };
                if old_type != new_type {
                    no_side_effects = false;
                    break 'types;
                }
            }
            for (item_offset, new_item) in new_order_per_func[offset]
                .iter()
                .flatten()
                .enumerate()
            {
                let old_expr = &old_order_per_func[offset][item_offset].expr;
                let (Some(old_type), Some(new_type)) =
                    (eval_type_of(old_expr), eval_type_of(&new_item.expr))
                else {
                    no_side_effects = false;
                    break 'types;
                };
                if old_type != new_type {
                    no_side_effects = false;
                    break 'types;
                }
            }
        }
    }
    if !no_side_effects {
        projection.base.set_children(vec![projection_child]);
        return Ok(CrossOutcome::Kept(projection));
    }
    agg.group_by_items = new_gby_items;
    for (offset, agg_func) in agg.agg_funcs.iter_mut().enumerate() {
        agg_func.base.args = new_args_per_func[offset].clone();
        if let Some(items) = new_order_per_func[offset].clone() {
            agg_func.order_by_items = items;
        }
    }
    Ok(CrossOutcome::Crossed(projection_child))
}

/// Go `util.ResetNotNullFlag(schema, start, end)` (`pkg/planner/util/null_misc.go`):
/// clear the NOT-NULL flag on the merged join schema's columns `[start, end)`
/// — the null-filling side of an outer join can emit NULLs for columns its
/// child declared NOT NULL.
fn reset_not_null_flag(schema: &mut Schema, start: usize, end: usize) {
    for column in schema.columns.get_mut(start..end).into_iter().flatten() {
        if let Some(field_type) = column.ret_type.as_mut() {
            field_type.del_flags(FieldTypeFlags::NOT_NULL);
        }
    }
}

/// Go `aggPushDown`'s JOIN arm (`rule_aggregation_push_down.go:497`): the
/// split was valid; push one partial aggregation into each side, rebuild the
/// join's children/schema/keys, then run Go's post-push elimination re-checks.
fn push_down_over_join(
    ctx: &RuleContext<'_>,
    mut agg: LogicalAggregation,
    mut join: LogicalJoin,
) -> Result<LogicalPlan, PlanError> {
    let mut funcs = std::mem::take(&mut agg.agg_funcs);
    let join_children = join.base.take_children();
    if join_children.len() != 2 {
        return Err(PlanError::internal("join pushdown expects two children"));
    }
    let mut join_children = join_children.into_iter();
    let l_child = join_children.next().unwrap_or_else(|| unreachable!());
    let r_child = join_children.next().unwrap_or_else(|| unreachable!());
    let l_schema = l_child.schema_or_empty();
    let r_schema = r_child.schema_or_empty();
    let Some((left_idxs, right_idxs)) =
        collect_agg_funcs(&funcs, &l_schema, &r_schema, join.join_type)
    else {
        // Go: splitAggFuncsAndGbyCols not valid — nothing happens.
        agg.agg_funcs = funcs;
        join.base.set_children(vec![l_child, r_child]);
        agg.base.set_children(vec![LogicalPlan::Join(join)]);
        return Ok(LogicalPlan::Aggregation(agg));
    };
    let (left_gby, right_gby) = collect_gby_cols(&agg, &l_schema, &join);
    let block_offset = agg.base.base.query_block_offset();
    let prefer_agg_type = agg.prefer_agg_type;
    let prefer_agg_to_cop = agg.prefer_agg_to_cop;

    // If there exist count or sum functions in one join path, we can't push
    // any aggregate function into the OTHER path.
    let right_invalid = check_any_count_and_sum(&funcs, &left_idxs);
    let left_invalid = check_any_count_and_sum(&funcs, &right_idxs);
    let new_r = if right_invalid {
        r_child
    } else {
        match try_to_push_down_agg(
            ctx,
            &mut funcs,
            &right_idxs,
            &right_gby,
            &r_child,
            1,
            &mut join,
            prefer_agg_type,
            prefer_agg_to_cop,
            block_offset,
        )? {
            Some(pushed) => pushed,
            None => r_child,
        }
    };
    let new_l = if left_invalid {
        l_child
    } else {
        match try_to_push_down_agg(
            ctx,
            &mut funcs,
            &left_idxs,
            &left_gby,
            &l_child,
            0,
            &mut join,
            prefer_agg_type,
            prefer_agg_to_cop,
            block_offset,
        )? {
            Some(pushed) => pushed,
            None => l_child,
        }
    };

    // join.SetChildren + SetSchema(MergeSchema(...)) + ResetNotNullFlag +
    // BuildKeyInfoPortal, in Go's order.
    let l_len = new_l.schema_or_empty().columns.len();
    let l_schema_after = new_l.schema_or_empty();
    let r_schema_after = new_r.schema_or_empty();
    let mut schema = merge_schema(new_l.schema(), new_r.schema())
        .ok_or_else(|| PlanError::internal("join schema merge failed"))?;
    match join.join_type {
        LogicalJoinType::LeftOuter => {
            let end = schema.columns.len();
            reset_not_null_flag(&mut schema, l_len, end);
        }
        LogicalJoinType::RightOuter => {
            reset_not_null_flag(&mut schema, 0, l_len);
        }
        _ => {}
    }
    join.build_key_info(&mut schema, &[l_schema_after, r_schema_after]);
    join.base.base.set_schema(Some(schema));
    join.base.set_children(vec![new_l, new_r]);
    let mut join_plan = LogicalPlan::Join(join);

    // count(a) -> ifnull(col#x, 0, 1) in rewriteExpr of agg function, since
    // col#x is already the final pushed-down aggregation's result, we don't
    // need to take every row as count 1 — Go re-checks the outer aggregation
    // with the STRICTER elimination gate here.
    let outer_eliminated = try_to_eliminate_aggregation(ctx, &agg, &join_plan, true)?;

    // Combine the aggregation elimination logic below since the pushed aggs'
    // child key info has changed; Go re-checks BOTH pushed aggregations with
    // the default gate.
    let mut changed = false;
    let mut rebuilt = join_plan.base_mut().take_children();
    let mut new_l = rebuilt.remove(0);
    let mut new_r = rebuilt.remove(0);
    for child in [&mut new_l, &mut new_r] {
        let LogicalPlan::Aggregation(pushed_agg) = child else {
            continue;
        };
        let Some(pushed_child) = pushed_agg.base.children().first().cloned() else {
            continue;
        };
        if let Some(expressions) = try_to_eliminate_aggregation(ctx, pushed_agg, &pushed_child, false)? {
            let grand = pushed_agg
                .base
                .take_children()
                .into_iter()
                .next()
                .ok_or_else(|| PlanError::internal("pushed aggregation has no child"))?;
            *child = elimination_projection(ctx, pushed_agg, expressions, grand);
            changed = true;
        }
    }
    if changed {
        // Go: join.SetChildren(lChild, rChild); BuildKeyInfoPortal(join).
        let l_schema_after = new_l.schema_or_empty();
        let r_schema_after = new_r.schema_or_empty();
        let LogicalPlan::Join(join) = &mut join_plan else {
            return Err(PlanError::internal("join pushdown lost its join"));
        };
        let mut schema = join.base.base.schema().cloned().unwrap_or_default();
        join.build_key_info(&mut schema, &[l_schema_after, r_schema_after]);
        join.base.base.set_schema(Some(schema));
    }
    join_plan.set_children(vec![new_l, new_r]);

    if let Some(expressions) = outer_eliminated {
        Ok(elimination_projection(ctx, &agg, expressions, join_plan))
    } else {
        agg.agg_funcs = funcs;
        agg.base.set_children(vec![join_plan]);
        Ok(LogicalPlan::Aggregation(agg))
    }
}

/// Go `aggPushDown` (`rule_aggregation_push_down.go:480`).
fn agg_push_down(ctx: &RuleContext<'_>, plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
    let mut plan = plan;
    if let LogicalPlan::Aggregation(mut agg) = plan {
        let initial_child = agg
            .base
            .children()
            .first()
            .ok_or_else(|| PlanError::internal("aggregation has no child"))?;
        let eliminated = try_to_eliminate_aggregation(ctx, &agg, initial_child, false)?;
        if let Some(expressions) = eliminated {
            let child = agg
                .base
                .take_children()
                .into_iter()
                .next()
                .ok_or_else(|| PlanError::internal("aggregation has no child"))?;
            plan = elimination_projection(ctx, &agg, expressions, child);
        } else {
            let child = agg
                .base
                .take_children()
                .into_iter()
                .next()
                .ok_or_else(|| PlanError::internal("aggregation has no child"))?;
            plan = match child {
                LogicalPlan::Join(join)
                    if check_valid_join(&join) && ctx.allow_agg_push_down =>
                {
                    push_down_over_join(ctx, agg, join)?
                }
                LogicalPlan::Projection(projection) => {
                    let mut agg = agg;
                    match cross_projection(ctx, &mut agg, projection)? {
                        CrossOutcome::Crossed(new_child) => {
                            agg.base.set_children(vec![new_child]);
                        }
                        CrossOutcome::Kept(projection) => {
                            agg.base
                                .set_children(vec![LogicalPlan::Projection(projection)]);
                        }
                    }
                    LogicalPlan::Aggregation(agg)
                }
                LogicalPlan::UnionAll(union) if ctx.allow_agg_push_down => {
                    let mut agg = agg;
                    let (union, _) = try_agg_push_down_for_union(ctx, &mut agg, union)?;
                    agg.base.set_children(vec![LogicalPlan::UnionAll(union)]);
                    LogicalPlan::Aggregation(agg)
                }
                LogicalPlan::PartitionUnionAll(mut partition_union) => {
                    let mut agg = agg;
                    let (union, _) =
                        try_agg_push_down_for_union(ctx, &mut agg, partition_union.union_all)?;
                    partition_union.union_all = union;
                    agg.base
                        .set_children(vec![LogicalPlan::PartitionUnionAll(partition_union)]);
                    LogicalPlan::Aggregation(agg)
                }
                other => {
                    let mut agg = agg;
                    agg.base.set_children(vec![other]);
                    LogicalPlan::Aggregation(agg)
                }
            };
        }
    }
    let children = plan.base_mut().take_children();
    if !children.is_empty() {
        let new_children = children
            .into_iter()
            .map(|child| agg_push_down(ctx, child))
            .collect::<Result<Vec<_>, PlanError>>()?;
        plan.set_children(new_children);
    }
    Ok(plan)
}

/// Go `AggregationPushDownSolver.Optimize` (`rule_aggregation_push_down.go:440`):
/// Go always reports `planChanged = false`.
#[derive(Debug)]
pub struct AggregationPushDownSolver;

impl LogicalOptRule for AggregationPushDownSolver {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let recovery = plan.clone();
        agg_push_down(ctx, plan)
            .map(|plan| (plan, false))
            .map_err(|error| (recovery, error))
    }

    fn name(&self) -> &'static str {
        "aggregation_push_down"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::data_source::DataSource;
    use crate::logical::LogicalPartitionUnionAll;
    use crate::logical::rule_tests::test_context;
    use crate::plan_base::PlanIdAllocator;
    use crate::stats_info::StatsInfo;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldName, FieldNameMetadata, IdentifierMetadata};
    use tidb_expr::aggregation::BaseFuncDesc;
    use tidb_expr::expression::ScalarFunction;

    fn column(id: i64) -> Column {
        Column::new(id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn not_null_column(id: i64) -> Column {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        field_type.add_flags(FieldTypeFlags::NOT_NULL);
        Column::new(id, field_type)
    }

    fn field_name(table: &str) -> FieldName {
        FieldName::new(FieldNameMetadata {
            table: IdentifierMetadata::new(table),
            original_table: IdentifierMetadata::new(table),
            column: IdentifierMetadata::new("a"),
            original_column: IdentifierMetadata::new("a"),
            ..FieldNameMetadata::default()
        })
    }

    fn data_source(allocator: &PlanIdAllocator, id: i64, table: &str, rows: f64) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, DataSource::TYPE, 1);
        base.base.set_schema(Some(Schema::new(vec![column(id)])));
        base.base.set_output_names(vec![field_name(table)]);
        base.base
            .set_stats(Some(StatsInfo::new(rows, [(id, rows)])));
        let mut source = DataSource::new(base, id, table);
        source.table_stats = Some(StatsInfo::new(rows, [(id, rows)]));
        LogicalPlan::DataSource(source)
    }

    fn agg_func(name: &str, arg: Column) -> AggFuncDesc {
        AggFuncDesc {
            base: BaseFuncDesc {
                name: name.to_owned(),
                args: vec![Expression::Column(arg)],
                ret_type: FieldType::new(FieldTypeCode::LongLong),
            },
            mode: AggFunctionMode::Complete,
            has_distinct: false,
            order_by_items: Vec::new(),
            grouping_id: 0,
        }
    }

    fn aggregation_node(
        allocator: &PlanIdAllocator,
        funcs: Vec<AggFuncDesc>,
        gby: Vec<Expression>,
        child: LogicalPlan,
    ) -> LogicalPlan {
        let outputs = (0..funcs.len())
            .map(|offset| column(100 + offset as i64))
            .collect::<Vec<Column>>();
        let mut base = BaseLogicalPlan::new(allocator, LogicalAggregation::TYPE, 1);
        base.base.set_schema(Some(Schema::new(outputs)));
        let mut plan = LogicalPlan::Aggregation(LogicalAggregation::new(base, funcs, gby));
        plan.set_children(vec![child]);
        plan
    }

    fn join_node(
        allocator: &PlanIdAllocator,
        join_type: LogicalJoinType,
        left: LogicalPlan,
        right: LogicalPlan,
        equality: Option<(i64, i64)>,
    ) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, LogicalJoin::TYPE, 1);
        let schema = merge_schema(left.schema(), right.schema())
            .unwrap_or_else(|| Schema::new(Vec::new()));
        base.base.set_schema(Some(schema));
        let mut join = LogicalJoin::new(base, join_type);
        if let Some((l, r)) = equality {
            join.equal_conditions.push(ScalarFunction::new(
                CiString::new("eq"),
                FieldType::new(FieldTypeCode::Tiny),
                vec![Expression::Column(column(l)), Expression::Column(column(r))],
            ));
        }
        join.base.set_children(vec![left, right]);
        LogicalPlan::Join(join)
    }

    fn union_node(
        allocator: &PlanIdAllocator,
        partitioned: bool,
        children: Vec<LogicalPlan>,
        schema_columns: Vec<Column>,
    ) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(
            allocator,
            if partitioned {
                LogicalPartitionUnionAll::TYPE
            } else {
                LogicalUnionAll::TYPE
            },
            1,
        );
        base.base.set_schema(Some(Schema::new(schema_columns)));
        let mut union_all = LogicalUnionAll::new(base);
        union_all.base.set_children(children);
        if partitioned {
            LogicalPlan::PartitionUnionAll(LogicalPartitionUnionAll { union_all })
        } else {
            LogicalPlan::UnionAll(union_all)
        }
    }

    fn projection_node(
        allocator: &PlanIdAllocator,
        expression: Expression,
        child: LogicalPlan,
    ) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, LogicalProjection::TYPE, 1);
        base.base.set_schema(Some(Schema::new(vec![column(100)])));
        base.base.set_output_names(vec![field_name("p")]);
        let mut projection = LogicalProjection::new(base, vec![expression]);
        projection.base.set_children(vec![child]);
        LogicalPlan::Projection(projection)
    }

    fn shape(plan: &LogicalPlan) -> String {
        match plan {
            LogicalPlan::DataSource(source) => source.table_name.clone(),
            LogicalPlan::Join(join) => format!(
                "({},{})",
                shape(&join.base.children()[0]),
                shape(&join.base.children()[1])
            ),
            LogicalPlan::Aggregation(agg) => format!("Agg({})", shape(&agg.base.children()[0])),
            LogicalPlan::Projection(projection) => {
                format!("Proj({})", shape(&projection.base.children()[0]))
            }
            LogicalPlan::UnionAll(union_all) => {
                let inner: Vec<String> = union_all.base.children().iter().map(shape).collect();
                format!("Union({})", inner.join(","))
            }
            LogicalPlan::PartitionUnionAll(partition_union) => {
                let inner: Vec<String> = partition_union
                    .union_all
                    .base
                    .children()
                    .iter()
                    .map(shape)
                    .collect();
                format!("PUnion({})", inner.join(","))
            }
            other => other.tp().to_owned(),
        }
    }

    fn agg_over_join_tree(
        allocator: &PlanIdAllocator,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: LogicalJoinType,
        left_column: i64,
        right_column: i64,
    ) -> LogicalPlan {
        let join = join_node(
            allocator,
            join_type,
            left,
            right,
            Some((left_column, right_column)),
        );
        aggregation_node(
            allocator,
            vec![
                agg_func(names::MAX, column(left_column)),
                agg_func(names::MAX, column(right_column)),
            ],
            Vec::new(),
            join,
        )
    }

    fn single_sum_over_join_tree(
        allocator: &PlanIdAllocator,
        left: LogicalPlan,
        right: LogicalPlan,
        left_column: i64,
        right_column: i64,
    ) -> LogicalPlan {
        let join = join_node(
            allocator,
            LogicalJoinType::Inner,
            left,
            right,
            Some((left_column, right_column)),
        );
        aggregation_node(
            allocator,
            vec![agg_func(names::SUM, column(left_column))],
            Vec::new(),
            join,
        )
    }

    #[test]
    fn max_pushes_into_both_sides_of_an_inner_join_with_the_flag_on() {
        let allocator = PlanIdAllocator::new();
        let mut context = test_context(&allocator);
        context.allow_agg_push_down = true;
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let plan = agg_over_join_tree(
            &allocator, left, right, LogicalJoinType::Inner, 1, 2,
        );

        let (optimized, changed) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert!(!changed);
        assert_eq!(shape(&optimized), "Agg((Agg(a),Agg(b)))");

        let LogicalPlan::Aggregation(agg) = &optimized else {
            panic!("root must stay an aggregation");
        };
        // The parent's descriptors become FINAL mode over the partial outputs;
        // the pushed-down children keep the ORIGINAL arguments in COMPLETE
        // mode (Go decompose's clone-first ordering).
        assert_eq!(agg.agg_funcs[0].name(), names::MAX);
        assert_eq!(agg.agg_funcs[0].mode, AggFunctionMode::Final);
        assert_eq!(agg.agg_funcs[1].mode, AggFunctionMode::Final);
        let LogicalPlan::Join(join) = &agg.base.children()[0] else {
            panic!("aggregation must sit on the join");
        };
        for side in join.base.children() {
            let LogicalPlan::Aggregation(pushed) = side else {
                panic!("each join side must be wrapped in an aggregation");
            };
            assert_eq!(pushed.agg_funcs.len(), 2);
            // [max(original arg, Complete), firstrow(group-by column)]
            assert_eq!(pushed.agg_funcs[0].mode, AggFunctionMode::Complete);
            assert_eq!(pushed.agg_funcs[1].name(), names::FIRST_ROW);
            // The join-condition columns became the pushed group-by columns.
            assert_eq!(pushed.group_by_items.len(), 1);
        }
    }

    #[test]
    fn sum_pushes_into_one_side_and_blocks_the_other() {
        // Go checkAnyCountAndSum: a sum/count in ONE join path forbids
        // pushing any aggregate into the OTHER path. sum(col1) on the left
        // blocks the right path; the empty right side allows the left push.
        let allocator = PlanIdAllocator::new();
        let mut context = test_context(&allocator);
        context.allow_agg_push_down = true;
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let plan = single_sum_over_join_tree(&allocator, left, right, 1, 2);

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert_eq!(shape(&optimized), "Agg((Agg(a),b))");
    }

    #[test]
    fn flag_off_leaves_the_join_child_untouched() {
        let allocator = PlanIdAllocator::new();
        let context = test_context(&allocator);
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let plan = agg_over_join_tree(
            &allocator, left, right, LogicalJoinType::Inner, 1, 2,
        );

        let (optimized, changed) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert!(!changed);
        assert_eq!(shape(&optimized), "Agg((a,b))");
        let LogicalPlan::Aggregation(agg) = &optimized else {
            panic!("root must stay an aggregation");
        };
        assert_eq!(agg.agg_funcs[0].mode, AggFunctionMode::Complete);
        let LogicalPlan::Join(join) = &agg.base.children()[0] else {
            panic!("aggregation must sit on the join");
        };
        assert!(join.default_values.is_empty());
    }

    #[test]
    fn count_over_left_outer_join_records_default_values_for_the_null_side() {
        let allocator = PlanIdAllocator::new();
        let mut context = test_context(&allocator);
        context.allow_agg_push_down = true;
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let join = join_node(
            &allocator,
            LogicalJoinType::LeftOuter,
            left,
            right,
            Some((1, 2)),
        );
        let plan = aggregation_node(
            &allocator,
            vec![agg_func(names::COUNT, not_null_column(2))],
            Vec::new(),
            join,
        );

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert_eq!(shape(&optimized), "Agg((a,Agg(b)))");
        let LogicalPlan::Aggregation(agg) = &optimized else {
            panic!("root must stay an aggregation");
        };
        // count's final-mode argument is the partial output column, with the
        // NOT-NULL flag cleared because the right side is null-generating.
        let arg = agg.agg_funcs[0].args()[0].as_column().unwrap();
        assert!(!arg
            .ret_type
            .as_ref()
            .is_some_and(|ft| ft.has_flag(FieldTypeFlags::NOT_NULL)));
        let LogicalPlan::Join(join) = &agg.base.children()[0] else {
            panic!("aggregation must sit on the join");
        };
        // Go EvalNullValueInOuterJoin: count over a schema column nulls to
        // NULL (the pushed aggregation's count + its group-by firstrow), and
        // the join records both as the outer-join defaults.
        assert_eq!(join.default_values, vec![Datum::Null, Datum::Null]);
    }

    #[test]
    fn group_by_covered_by_child_unique_key_blocks_pushdown() {
        let allocator = PlanIdAllocator::new();
        let mut context = test_context(&allocator);
        context.allow_agg_push_down = true;
        let left = data_source(&allocator, 1, "a", 10.0);
        let mut right = data_source(&allocator, 2, "b", 10.0);
        if let LogicalPlan::DataSource(source) = &mut right {
            let mut schema = source.base.base.schema().cloned().unwrap();
            schema.set_keys(vec![vec![column(2)]]);
            source.base.base.set_schema(Some(schema));
        }
        let join = join_node(
            &allocator,
            LogicalJoinType::Inner,
            left,
            right,
            Some((1, 2)),
        );
        let plan = aggregation_node(
            &allocator,
            vec![agg_func(names::COUNT, column(2))],
            vec![Expression::Column(column(2))],
            join,
        );

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        // The group-by column covers the right child's unique key, so the
        // pushdown is refused on both sides.
        assert_eq!(shape(&optimized), "Agg((a,b))");
    }

    #[test]
    fn avg_blocks_the_join_arm() {
        let allocator = PlanIdAllocator::new();
        let mut context = test_context(&allocator);
        context.allow_agg_push_down = true;
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let join = join_node(
            &allocator,
            LogicalJoinType::Inner,
            left,
            right,
            Some((1, 2)),
        );
        let plan = aggregation_node(
            &allocator,
            vec![agg_func(names::AVG, column(1))],
            Vec::new(),
            join,
        );

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert_eq!(shape(&optimized), "Agg((a,b))");
    }

    #[test]
    fn projection_crossing_drops_the_projection_even_with_the_flag_off() {
        let allocator = PlanIdAllocator::new();
        let context = test_context(&allocator);
        let source = data_source(&allocator, 1, "a", 10.0);
        let projection = projection_node(
            &allocator,
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![Expression::Column(column(1)), Expression::Column(column(1))],
            )),
            source,
        );
        let plan = aggregation_node(
            &allocator,
            vec![agg_func(names::SUM, column(100))],
            Vec::new(),
            projection,
        );

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert_eq!(shape(&optimized), "Agg(a)");
        let LogicalPlan::Aggregation(agg) = &optimized else {
            panic!("root must stay an aggregation");
        };
        // The aggregate's argument was substituted through the projection.
        assert!(matches!(agg.agg_funcs[0].args()[0], Expression::ScalarFunction(_)));
    }

    #[test]
    fn projection_crossing_refuses_a_side_effect_expression() {
        let allocator = PlanIdAllocator::new();
        let context = test_context(&allocator);
        let source = data_source(&allocator, 1, "a", 10.0);
        let projection = projection_node(
            &allocator,
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("sleep"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![Expression::Column(column(1))],
            )),
            source,
        );
        let plan = aggregation_node(
            &allocator,
            vec![agg_func(names::SUM, column(100))],
            Vec::new(),
            projection,
        );

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert_eq!(shape(&optimized), "Agg(Proj(a))");
        let LogicalPlan::Aggregation(agg) = &optimized else {
            panic!("root must stay an aggregation");
        };
        assert!(matches!(agg.agg_funcs[0].args()[0], Expression::Column(_)));
    }

    #[test]
    fn union_pushes_partial_aggregations_into_each_child_with_the_flag_on() {
        let allocator = PlanIdAllocator::new();
        let mut context = test_context(&allocator);
        context.allow_agg_push_down = true;
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let union = union_node(
            &allocator,
            false,
            vec![left, right],
            vec![column(100)],
        );
        let plan = aggregation_node(
            &allocator,
            vec![agg_func(names::COUNT, column(100))],
            Vec::new(),
            union,
        );

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert_eq!(shape(&optimized), "Agg(Union(Agg(a),Agg(b)))");
        let LogicalPlan::Aggregation(agg) = &optimized else {
            panic!("root must stay an aggregation");
        };
        assert_eq!(agg.agg_funcs[0].mode, AggFunctionMode::Final);
        let LogicalPlan::UnionAll(union_all) = &agg.base.children()[0] else {
            panic!("the aggregation must sit on the union");
        };
        // Each union child got its own partial aggregation with the union's
        // schema columns substituted by the child's.
        let expected_args = [1, 2];
        for (offset, child) in union_all.base.children().iter().enumerate() {
            let LogicalPlan::Aggregation(pushed) = child else {
                panic!("each union child must be wrapped in an aggregation");
            };
            assert_eq!(
                pushed.agg_funcs[0].args()[0].as_column().unwrap().unique_id,
                expected_args[offset]
            );
        }
    }

    #[test]
    fn non_decomposable_union_aggregate_blocks_the_union_arm() {
        // Go isDecomposableWithUnion: avg decomposes across a union, but
        // var_samp (and stddev/group_concat/...) fall to the default refusal.
        let allocator = PlanIdAllocator::new();
        let mut context = test_context(&allocator);
        context.allow_agg_push_down = true;
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let union = union_node(
            &allocator,
            false,
            vec![left, right],
            vec![column(100)],
        );
        let plan = aggregation_node(
            &allocator,
            vec![agg_func(names::VAR_SAMP, column(100))],
            Vec::new(),
            union,
        );

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert_eq!(shape(&optimized), "Agg(Union(a,b))");
    }

    #[test]
    fn partition_union_arm_ignores_the_flag() {
        let allocator = PlanIdAllocator::new();
        let context = test_context(&allocator);
        let left = data_source(&allocator, 1, "a", 10.0);
        let right = data_source(&allocator, 2, "b", 10.0);
        let union = union_node(
            &allocator,
            true,
            vec![left, right],
            vec![column(100)],
        );
        let plan = aggregation_node(
            &allocator,
            vec![agg_func(names::COUNT, column(100))],
            Vec::new(),
            union,
        );

        let (optimized, _) = AggregationPushDownSolver.optimize(&context, plan).unwrap();
        assert_eq!(shape(&optimized), "Agg(PUnion(Agg(a),Agg(b)))");
    }

    #[test]
    fn rule_name_matches_go() {
        assert_eq!(AggregationPushDownSolver.name(), "aggregation_push_down");
    }
}
