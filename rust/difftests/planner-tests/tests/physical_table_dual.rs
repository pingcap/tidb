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

//! Dependency-closed vectors for PhysicalTableDual planning metadata.
//!
//! The Go anchor is `TestTableDual` at
//! `pkg/planner/core/casetest/cbotest/cbo_test.go:367`.

use tidb_planner::physical_table_dual::{find_best_task, PhysicalTableDualPlan};

#[test]
fn table_dual_explain_and_kind_match_source_plan_tree() {
    let plan = find_best_task(0, 0, false, false).unwrap();
    assert_eq!(plan.plan_type(), "Dual");
    assert_eq!(plan.query_block_offset(), 0);
    assert_eq!(plan.explain_info(), "rows:0");
}

#[test]
fn one_row_dual_accepts_required_sort_property() {
    assert_eq!(
        find_best_task(1, 12, false, true),
        Some(PhysicalTableDualPlan::init(1, 12))
    );
}

#[test]
fn multirow_dual_rejects_sort_and_all_duals_reject_index_join() {
    assert!(find_best_task(2, 0, false, true).is_none());
    assert!(find_best_task(0, 0, true, false).is_none());
}
