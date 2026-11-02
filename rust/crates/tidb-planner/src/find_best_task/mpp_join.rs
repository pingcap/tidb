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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `exhaustPhysicalPlans4LogicalJoin`'s MPP head and `tryToGetMppHashJoin`
//! (`pkg/planner/core/exhaust_physical_plans.go`): the TiFlash hash joins a
//! join enumerates before its TiDB-side candidates.

use tidb_hint::join_hint_flags;
use tidb_planner_core_base::LogicalJoinType;
use tidb_planner_core_operator_logicalop::{LogicalJoin, LogicalPlan};
use tidb_planner_core_operator_physicalop::{BasePhysicalPlan, PhysicalHashJoin, PhysicalPlan};
use tidb_planner_property::{
    choose_partition_keys, get_collate_id_by_name_for_partition, MppPartitionColumn, MppPartitionType,
    PhysicalProperty, TaskType,
};

use super::dispatch::DispatchContext;

/// What the MPP head decides before the TiDB-side join candidates.
pub(crate) enum MppJoinHead {
    /// Go returns at once: a broadcast property, or a flash property after
    /// the MPP joins (`return joins, true, nil`) or a blocked hint
    /// (`return nil, false, nil`).
    Return(Vec<PhysicalPlan>, bool),
    /// The MPP joins lead the candidate list; the prefer-join type may have
    /// been reset by a hint conflict.
    Continue(Vec<PhysicalPlan>),
}

fn raise(ctx: &DispatchContext<'_>, message: &str) {
    if let Some(sink) = ctx.mpp_warning_sink {
        sink.raise_mpp_warning(message);
    }
}

fn hint_warning(ctx: &DispatchContext<'_>, message: &str) {
    if let Some(sink) = ctx.mpp_warning_sink {
        sink.set_hint_warning(message);
    }
}

/// Go `hasMPPJoinHints`.
const fn has_mpp_join_hints(prefer: u32) -> bool {
    prefer & join_hint_flags::BC_JOIN > 0 || prefer & join_hint_flags::SHUFFLE_JOIN > 0
}

/// Go `isJoinHintSupportedInMPPMode`: TiFlash runs hash joins only, so a
/// join method hint other than the hash-join family and the MPP hints is
/// unsupported.
const fn is_join_hint_supported_in_mpp_mode(prefer: u32) -> bool {
    if prefer == 0 {
        return true;
    }
    let mpp_mask = join_hint_flags::SHUFFLE_JOIN ^ join_hint_flags::BC_JOIN;
    let hash_join_family = join_hint_flags::HASH_JOIN
        ^ join_hint_flags::LEFT_AS_HJ_BUILD
        ^ join_hint_flags::RIGHT_AS_HJ_BUILD
        ^ join_hint_flags::LEFT_AS_HJ_PROBE
        ^ join_hint_flags::RIGHT_AS_HJ_PROBE;
    (prefer & !hash_join_family & !mpp_mask).count_ones() < 1
}

/// Go `exhaustPhysicalPlans4LogicalJoin` up to `getHashJoins`.
/// `prefer_join_type` is the join's hint set, which a conflict clears.
pub(crate) fn mpp_join_head(
    op: &LogicalJoin,
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    prefer_join_type: &mut u32,
    ctx: &DispatchContext<'_>,
) -> MppJoinHead {
    if !is_join_hint_supported_in_mpp_mode(*prefer_join_type) {
        if has_mpp_join_hints(*prefer_join_type) {
            hint_warning(
                ctx,
                "The MPP join hints are in conflict, and you can only specify join method hints that are currently supported by MPP mode now",
            );
            *prefer_join_type = 0;
        } else {
            raise(
                ctx,
                "MPP mode may be blocked because you have used hint to specify a join algorithm which is not supported by mpp now.",
            );
            if prop.is_flash_prop() {
                return MppJoinHead::Return(Vec::new(), false);
            }
        }
    }
    if prop.mpp_partition_tp == MppPartitionType::Broadcast {
        return MppJoinHead::Return(Vec::new(), false);
    }
    let mut joins = Vec::new();
    if prop.index_join_prop.is_none() {
        if can_try_mpp_join_for_join(plan, ctx) {
            if has_mpp_join_hints(*prefer_join_type) {
                // Both shapes; the attachment picks the hinted one.
                joins.extend(try_to_get_mpp_hash_join(op, plan, prop, true, *prefer_join_type, ctx));
                joins.extend(try_to_get_mpp_hash_join(op, plan, prop, false, *prefer_join_type, ctx));
            } else {
                let use_bcj = prefer_mpp_bcj(op, plan, ctx);
                joins.extend(try_to_get_mpp_hash_join(op, plan, prop, use_bcj, *prefer_join_type, ctx));
            }
        } else {
            if *prefer_join_type & join_hint_flags::SHUFFLE_JOIN > 0 {
                hint_warning(ctx, "The join can not push down to the MPP side, the shuffle_join() hint is invalid");
            }
            if *prefer_join_type & join_hint_flags::BC_JOIN > 0 {
                hint_warning(ctx, "The join can not push down to the MPP side, the broadcast_join() hint is invalid");
            }
        }
        if prop.is_flash_prop() {
            return MppJoinHead::Return(joins, true);
        }
    }
    MppJoinHead::Continue(joins)
}

/// Go `canTryMPPJoinForJoin`: both sides reach TiFlash and TiFlash is an
/// isolation-read engine -- or the strict-SQL-mode removal of TiFlash hides
/// sources that do have TiFlash replicas.
fn can_try_mpp_join_for_join(plan: &LogicalPlan, ctx: &DispatchContext<'_>) -> bool {
    let has_tiflash = plan.base().has_tiflash();
    (has_tiflash && ctx.tiflash_in_isolation_read)
        || (ctx.tiflash_engine_removed_due_to_strict_sql_mode && has_tiflash_replica_for_mpp(plan, ctx))
}

/// Go `hasTiFlashReplicaForMPP`.
fn has_tiflash_replica_for_mpp(plan: &LogicalPlan, ctx: &DispatchContext<'_>) -> bool {
    use tidb_planner_core_operator_logicalop::data_source::{PREFER_TIFLASH, PREFER_TIKV};
    if let LogicalPlan::DataSource(ds) = plan {
        let prefer_tikv_only = ds.prefer_store_type & PREFER_TIKV != 0 && ds.prefer_store_type & PREFER_TIFLASH == 0;
        return ds.has_tiflash() && !prefer_tikv_only && ctx.mpp_allowed;
    }
    let children = plan.children();
    !children.is_empty() && children.iter().all(|child| has_tiflash_replica_for_mpp(child, ctx))
}

/// Go `checkChildFitBC`: a side small enough to broadcast.
fn check_child_fit_bc(child: &LogicalPlan, ctx: &DispatchContext<'_>) -> bool {
    use tidb_planner_cardinality::row_size::{get_avg_row_size, RowSizeColumn};
    use tidb_planner_property::stats_info::HistCollExt;
    let Some(stats) = child.stats_info() else {
        return false;
    };
    let session = &ctx.mpp_join;
    let count = stats.count();
    let Some(hist) = stats.hist_coll() else {
        return session.bcj_threshold_count == -1 || count < session.bcj_threshold_count;
    };
    let columns: Vec<_> = child
        .schema()
        .into_iter()
        .flat_map(|schema| &schema.columns)
        .map(|column| RowSizeColumn {
            stats: hist.column(column.unique_id),
            estimated_width: 0.0,
        })
        .collect();
    let avg = get_avg_row_size(&columns, hist.pseudo(), hist.realtime_count(), false, false, ctx.enable_chunk_rpc);
    session.bcj_threshold_size == -1 || avg * (count as f64) < session.bcj_threshold_size as f64
}

/// Go `preferMppBCJ`.
///
/// Under `tidb_prefer_broadcast_join_by_exchange_data_size` Go asks the MPP
/// client how many TiFlash stores there are; the planner has no MPP client,
/// which is Go's error answer (`GetMPPStoreCount` failing), so the
/// threshold checks below decide.
fn prefer_mpp_bcj(op: &LogicalJoin, plan: &LogicalPlan, ctx: &DispatchContext<'_>) -> bool {
    if op.equal_conditions.is_empty() && ctx.mpp_join.allow_cartesian_bcj == 2 {
        return true;
    }
    let [left, right] = plan.children() else {
        return false;
    };
    match op.join_type {
        LogicalJoinType::LeftOuter | LogicalJoinType::Semi | LogicalJoinType::AntiSemi => {
            check_child_fit_bc(right, ctx)
        }
        LogicalJoinType::RightOuter => check_child_fit_bc(left, ctx),
        _ => check_child_fit_bc(left, ctx) || check_child_fit_bc(right, ctx),
    }
}

/// Go `canExprsInJoinPushdown`.
fn can_exprs_in_join_pushdown(op: &LogicalJoin, ctx: &DispatchContext<'_>) -> bool {
    use tidb_expr::expression::Expression;
    use tidb_kv::store_type::StoreType;
    if op.equal_conditions.iter().any(|condition| condition.func_name.lowercase() == "nulleq") {
        return false;
    }
    let equal: Vec<Expression> = op
        .equal_conditions
        .iter()
        .cloned()
        .map(Expression::ScalarFunction)
        .collect();
    let pushable = |conditions: &[Expression]| {
        tidb_expr::infer_pushdown::can_exprs_push_down(conditions, StoreType::TiFlash, &ctx.expr_pushdown_blacklist)
    };
    pushable(&equal)
        && pushable(&op.left_conditions)
        && pushable(&op.right_conditions)
        && pushable(&op.other_conditions)
}

/// Go `LogicalJoin.GetPotentialPartitionKeys`.
fn potential_partition_keys(op: &LogicalJoin) -> (Vec<MppPartitionColumn>, Vec<MppPartitionColumn>) {
    let mut left = Vec::with_capacity(op.equal_conditions.len());
    let mut right = Vec::with_capacity(op.equal_conditions.len());
    for condition in &op.equal_conditions {
        // Go `CharsetAndCollation()` of a comparison: the collation derived
        // from its arguments when it was built.
        let Ok(derived) = tidb_expr::collation_derive::derive_collation(
            condition.func_name.lowercase(),
            &condition.args,
            tidb_datatype::EvalType::Int,
        ) else {
            return (Vec::new(), Vec::new());
        };
        let collate_id = get_collate_id_by_name_for_partition(&derived.collation);
        let Some((l, r)) = tidb_expr::expr_util::extract::extract_columns_from_col_op_col(condition) else {
            continue;
        };
        left.push(MppPartitionColumn { col: l.clone(), collate_id });
        right.push(MppPartitionColumn { col: r.clone(), collate_id });
    }
    (left, right)
}

/// Go `tryToGetMppHashJoin`.
fn try_to_get_mpp_hash_join(
    op: &LogicalJoin,
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    use_bcj: bool,
    prefer_join_type: u32,
    ctx: &DispatchContext<'_>,
) -> Vec<PhysicalPlan> {
    if !prop.is_sort_item_empty() {
        return Vec::new();
    }
    if prop.task_tp != TaskType::Root && prop.task_tp != TaskType::Mpp {
        return Vec::new();
    }
    let join_type_name = LogicalJoin::join_type_name(op.join_type);
    if !tidb_expr::infer_pushdown::is_push_down_enabled(
        &ctx.expr_pushdown_blacklist,
        join_type_name,
        tidb_kv::store_type::StoreType::TiFlash,
    ) {
        raise(
            ctx,
            &format!("MPP mode may be blocked because join type `{join_type_name}` is blocked by blacklist, check `table mysql.expr_pushdown_blacklist;` for more information."),
        );
        return Vec::new();
    }
    if op.equal_conditions.is_empty() {
        if !use_bcj {
            raise(ctx, "MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variables `tidb_broadcast_join_threshold_size` and `tidb_broadcast_join_threshold_count`.");
            return Vec::new();
        }
        if ctx.mpp_join.allow_cartesian_bcj == 0 {
            raise(ctx, "MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variable `tidb_opt_broadcast_cartesian_join`.");
            return Vec::new();
        }
    }
    if !op.left_conditions.is_empty() && op.join_type != LogicalJoinType::LeftOuter {
        raise(ctx, "MPP mode may be blocked because there is a join that is not `left join` but has left conditions, which is not supported by mpp now, see github.com/pingcap/tidb/issues/26090 for more information.");
        return Vec::new();
    }
    if !op.right_conditions.is_empty() && op.join_type != LogicalJoinType::RightOuter {
        raise(ctx, "MPP mode may be blocked because there is a join that is not `right join` but has right conditions, which is not supported by mpp now.");
        return Vec::new();
    }
    if prop.mpp_partition_tp == MppPartitionType::Broadcast {
        return Vec::new();
    }
    if !can_exprs_in_join_pushdown(op, ctx) {
        return Vec::new();
    }
    let [left, right] = plan.children() else {
        return Vec::new();
    };
    let count = |child: &LogicalPlan| child.stats_info().map_or(0, |stats| stats.count());
    let (count0, count1) = (count(left), count(right));

    let mut force_left_to_build =
        prefer_join_type & (join_hint_flags::LEFT_AS_HJ_BUILD | join_hint_flags::RIGHT_AS_HJ_PROBE) > 0;
    let mut force_right_to_build =
        prefer_join_type & (join_hint_flags::RIGHT_AS_HJ_BUILD | join_hint_flags::LEFT_AS_HJ_PROBE) > 0;
    if force_left_to_build && force_right_to_build {
        hint_warning(ctx, "Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are conflicts, please check the hints");
        force_left_to_build = false;
        force_right_to_build = false;
    }
    let mut preferred_build_index = 0;
    let mut fixed_build_side = false;
    let is_semi = matches!(
        op.join_type,
        LogicalJoinType::Semi
            | LogicalJoinType::AntiSemi
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiLeftOuterSemi
    );
    if op.join_type == LogicalJoinType::Inner {
        if count0 > count1 {
            preferred_build_index = 1;
        }
    } else if is_semi {
        if !use_bcj
            && !op.is_naaj()
            && !op.equal_conditions.is_empty()
            && matches!(op.join_type, LogicalJoinType::Semi | LogicalJoinType::AntiSemi)
        {
            // TiFlash builds either side of a non-null-aware, non-cross
            // semi/anti-semi join.
            preferred_build_index = 1;
            if !ctx.mpp_join.outer_join_fixed_build_side && count1 > count0 {
                preferred_build_index = 0;
            }
        } else {
            preferred_build_index = 1;
            fixed_build_side = true;
        }
    }
    if matches!(op.join_type, LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter) {
        if use_bcj || op.is_naaj() || op.equal_conditions.is_empty() || ctx.mpp_join.outer_join_fixed_build_side {
            if !ctx.mpp_join.outer_join_fixed_build_side {
                fixed_build_side = true;
            }
            if op.join_type == LogicalJoinType::LeftOuter {
                preferred_build_index = 1;
            }
        } else if count0 > count1 {
            preferred_build_index = 1;
        }
    }
    if force_left_to_build || force_right_to_build {
        let matched = (force_left_to_build && preferred_build_index == 0)
            || (force_right_to_build && preferred_build_index == 1);
        if !matched {
            if fixed_build_side {
                hint_warning(
                    ctx,
                    "Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints",
                );
            } else {
                preferred_build_index = 1 - preferred_build_index;
            }
        }
    }

    let mpp_prop = |partition_tp, partition_cols, expected_cnt, can_add_enforcer| PhysicalProperty {
        task_tp: TaskType::Mpp,
        expected_cnt,
        mpp_partition_tp: partition_tp,
        mpp_partition_cols: partition_cols,
        can_add_enforcer,
        cte_producer_status: prop.cte_producer_status,
        ..PhysicalProperty::default()
    };
    let mut children_props: [Option<PhysicalProperty>; 2] = [None, None];
    let (mut left_keys, mut right_keys) = potential_partition_keys(op);
    if use_bcj {
        children_props[preferred_build_index] =
            Some(mpp_prop(MppPartitionType::Broadcast, Vec::new(), f64::MAX, true));
        let join_rows = op.base.base.stats_info().map_or(0.0, tidb_planner_property::stats_info::StatsInfo::row_count);
        let mut expected_cnt = f64::MAX;
        if prop.expected_cnt < join_rows {
            let scale = prop.expected_cnt / join_rows;
            let target = if preferred_build_index == 1 { left } else { right };
            expected_cnt = target
                .stats_info()
                .map_or(0.0, tidb_planner_property::stats_info::StatsInfo::row_count)
                * scale;
        }
        children_props[1 - preferred_build_index] = Some(if prop.mpp_partition_tp == MppPartitionType::Hash {
            let hash_keys = if preferred_build_index == 1 { &left_keys } else { &right_keys };
            if prop.is_subset_of(hash_keys).is_none_or(|matches| matches.is_empty()) {
                return Vec::new();
            }
            mpp_prop(MppPartitionType::Hash, prop.mpp_partition_cols.clone(), expected_cnt, false)
        } else {
            mpp_prop(MppPartitionType::Any, Vec::new(), expected_cnt, false)
        });
    } else {
        if prop.mpp_partition_tp == MppPartitionType::Hash {
            let matches = match op.join_type {
                LogicalJoinType::Inner => prop
                    .is_subset_of(&left_keys)
                    .filter(|matches| !matches.is_empty())
                    .or_else(|| prop.is_subset_of(&right_keys)),
                // Only the right keys survive a right outer join; the left
                // ones are NULL-padded at random.
                LogicalJoinType::RightOuter => prop.is_subset_of(&right_keys),
                _ => prop.is_subset_of(&left_keys),
            };
            let Some(matches) = matches.filter(|matches| !matches.is_empty()) else {
                return Vec::new();
            };
            left_keys = choose_partition_keys(&left_keys, &matches);
            right_keys = choose_partition_keys(&right_keys, &matches);
        }
        children_props[0] = Some(mpp_prop(MppPartitionType::Hash, left_keys, f64::MAX, true));
        children_props[1] = Some(mpp_prop(MppPartitionType::Hash, right_keys, f64::MAX, true));
    }

    let (left_join_keys, right_join_keys, is_null_eq, _) = op.get_join_keys();
    let mut base = BasePhysicalPlan::new(ctx.allocator, op.base.base.tp(), op.base.base.query_block_offset());
    // An MPP join's cost is heavy; a limit above cannot stop it in time, so
    // the count is not scaled.
    base.base.set_stats(op.base.base.stats_info().cloned());
    base.base.set_schema(op.base.base.schema().cloned());
    base.set_children_req_props(children_props.into_iter().collect());
    vec![PhysicalPlan::HashJoin(PhysicalHashJoin {
        base,
        concurrency: ctx.mpp_join.tiflash_concurrency_factor as usize,
        join_type: op.join_type,
        inner_child_idx: preferred_build_index,
        use_outer_to_build: false,
        left_join_keys,
        right_join_keys,
        is_null_eq,
        equal_conditions: op.equal_conditions.clone(),
        na_equal_conditions: op.na_eq_conditions.clone(),
        left_conditions: op.left_conditions.clone(),
        right_conditions: op.right_conditions.clone(),
        other_conditions: op.other_conditions.clone(),
        default_values: op.default_values.clone(),
        store_tp: tidb_kv::store_type::StoreType::TiFlash,
        mpp_shuffle_join: !use_bcj,
    })]
}
