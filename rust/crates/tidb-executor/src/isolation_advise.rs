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

//! Go `pkg/sessiontxn/isolation`'s `AdviseOptimizeWithPlan`: the statement's
//! optimized plan tells its pessimistic provider whether the statement needs
//! a fresh for-update timestamp, before the statement's first read or lock.

use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_sessionctx::PessimisticTxnProvider;

/// Go `AdviseOptimizeWithPlan` for the statement's root `plan`.
/// `insert_replace_or_on_duplicate` is an INSERT root's `IsReplace ||
/// len(OnDuplicate) > 0`, which the read-committed provider reads.
pub(crate) fn advise_optimize_with_plan(
    ctx: &crate::StmtContext,
    plan: &PhysicalPlan,
    insert_replace_or_on_duplicate: bool,
) {
    let keep = match ctx.pessimistic_txn_provider() {
        None => return,
        Some(PessimisticTxnProvider::RepeatableRead) => not_need_get_latest_ts_from_pd(plan, false),
        Some(PessimisticTxnProvider::ReadCommitted { rc_write_check_ts }) => {
            plan_skip_get_tso_from_pd(plan, rc_write_check_ts, insert_replace_or_on_duplicate, false)
        }
    };
    if keep {
        // Read committed reads the kept timestamp under `RCCheckTS`
        // (`checkTSInWriteStmt`).
        let check_ts = matches!(
            ctx.pessimistic_txn_provider(),
            Some(PessimisticTxnProvider::ReadCommitted { .. })
        );
        crate::statement_store(ctx).advise_keep_for_update_ts(check_ts);
    }
}

/// Go `notNeedGetLatestTSFromPD`: `in_lock_or_write_stmt` means an ancestor
/// is an UPDATE, DELETE or `PhysicalLock`.
fn not_need_get_latest_ts_from_pd(plan: &PhysicalPlan, in_lock_or_write_stmt: bool) -> bool {
    match plan {
        PhysicalPlan::PointGet(point) => !in_lock_or_write_stmt || point.lock,
        PhysicalPlan::BatchPointGet(batch) => !in_lock_or_write_stmt || batch.lock,
        PhysicalPlan::Dml(root) => match root.go_operator.as_str() {
            "Insert" => root.select_plan.is_none(),
            _ => root
                .select_plan
                .as_deref()
                .is_some_and(|select| not_need_get_latest_ts_from_pd(select, true)),
        },
        _ => {
            let children = plan.children();
            let is_physical_lock = matches!(plan, PhysicalPlan::Lock(_));
            !children.is_empty()
                && children.iter().all(|child| {
                    not_need_get_latest_ts_from_pd(child, is_physical_lock || in_lock_or_write_stmt)
                })
        }
    }
}

/// Go `planSkipGetTsoFromPD`: the plans whose statement does not fetch the
/// newest timestamp under read committed.
fn plan_skip_get_tso_from_pd(
    plan: &PhysicalPlan,
    rc_write_check_ts: bool,
    insert_replace_or_on_duplicate: bool,
    in_lock_or_write_stmt: bool,
) -> bool {
    match plan {
        PhysicalPlan::PointGet(point) => rc_write_check_ts && (point.lock || in_lock_or_write_stmt),
        PhysicalPlan::Dml(root) => match root.go_operator.as_str() {
            "Insert" => root.select_plan.is_none() && !insert_replace_or_on_duplicate,
            _ => root.select_plan.as_deref().is_some_and(|select| {
                plan_skip_get_tso_from_pd(select, rc_write_check_ts, false, true)
            }),
        },
        _ => {
            let children = plan.children();
            let is_physical_lock = matches!(plan, PhysicalPlan::Lock(_));
            !children.is_empty()
                && children.iter().all(|child| {
                    plan_skip_get_tso_from_pd(
                        child,
                        rc_write_check_ts,
                        false,
                        is_physical_lock || in_lock_or_write_stmt,
                    )
                })
        }
    }
}
