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
//! optimized plan tells its provider whether the statement needs a fresh
//! for-update timestamp (pessimistic) or reads at `math.MaxUint64`
//! (optimistic), before the statement's first read or lock.

use tidb_planner_core_operator_physicalop::PhysicalPlan;

/// Go `executorBuilder`'s `txnManager.AdviseOptimizeWithPlan(plan)`: the
/// statement's provider advises; a kept timestamp goes to the statement's
/// snapshot handle.
pub(crate) fn advise_optimize_with_plan(
    ctx: &crate::StmtContext,
    plan: &PhysicalPlan,
    insert_replace_or_on_duplicate: bool,
) {
    let Some(advisor) = ctx.txn_plan_advisor() else {
        return;
    };
    if let Some(check_ts) = advisor.advise_optimize_with_plan(plan, insert_replace_or_on_duplicate) {
        crate::statement_store(ctx).advise_keep_for_update_ts(check_ts);
    }
    if advisor.optimize_with_max_ts() {
        ctx.set_priority_high_if_no_priority();
    }
    // Go `AdviseOptimizeWithPlanAndThenWarmUp`.
    advisor.advise_warmup();
}
