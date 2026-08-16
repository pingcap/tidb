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

//! Source-contract tests for result-row counting over runtime stats.

use std::time::Duration;

use tidb_exec::result_rows_count::get_result_rows_count;
use tidb_exec::runtime_stats::RuntimeStatsColl;

#[test]
fn missing_collection_or_plan_id_counts_zero() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1445-1455. The nil
    // context/collection and the failed PlanIDFunc extraction all return 0.
    assert_eq!(get_result_rows_count(None, Some(1)), 0);

    let coll = RuntimeStatsColl::new(None);
    assert_eq!(get_result_rows_count(Some(&coll), None), 0);

    // An unknown plan ID reaches GetPlanActRows and still yields 0.
    assert_eq!(get_result_rows_count(Some(&coll), Some(42)), 0);
}

#[test]
fn root_plan_rows_are_reported_from_the_collection() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1445-1455 over
    // pkg/util/execdetails (RuntimeStatsColl.GetPlanActRows /
    // BasicRuntimeStats.Record).
    let mut coll = RuntimeStatsColl::new(None);
    let root_plan_id = 1;
    let child_plan_id = 2;

    let root_basic = coll
        .get_basic_runtime_stats(root_plan_id, true)
        .expect("init_new_executor_stats creates the basic stats");
    root_basic.record(Duration::from_millis(3), 100);
    root_basic.record(Duration::from_millis(2), 23);

    let child_basic = coll
        .get_basic_runtime_stats(child_plan_id, true)
        .expect("init_new_executor_stats creates the basic stats");
    child_basic.record(Duration::from_millis(1), 999);

    // Only the root plan's rows are the statement's result rows.
    assert_eq!(get_result_rows_count(Some(&coll), Some(root_plan_id)), 123);
    assert_eq!(get_result_rows_count(Some(&coll), Some(child_plan_id)), 999);
}
