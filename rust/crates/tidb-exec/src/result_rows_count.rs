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

//! Result-row counting over the runtime stats collection from `stmtctx.go`.
//!
//! Source: `pkg/sessionctx/stmtctx/stmtctx.go:1445-1455`
//! (`GetResultRowsCount`).
//!
//! Go guards on a nil context, a nil `RuntimeStatsColl`, and a `PlanIDFunc`
//! that fails to extract the root plan ID from `sc.GetPlan()`; all three
//! zero-returning guards collapse to the two `Option` inputs here. The
//! planner hook itself (`PlanIDFunc`) and the statement context that owns
//! the collection stay outside this leaf.

use crate::runtime_stats::RuntimeStatsColl;

/// Go `StatementContext.GetResultRowsCount`: the number of result rows of
/// the root plan, or `0` when the collection or the plan ID is unavailable.
#[must_use]
pub fn get_result_rows_count(coll: Option<&RuntimeStatsColl>, plan_id: Option<i64>) -> i64 {
    let Some(coll) = coll else {
        return 0;
    };
    let Some(plan_id) = plan_id else {
        return 0;
    };
    coll.get_plan_act_rows(plan_id)
}
