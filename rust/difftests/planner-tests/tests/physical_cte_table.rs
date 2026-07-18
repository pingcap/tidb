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

//! Dependency-closed vectors for PhysicalCTETable identity and eligibility.
//!
//! The Go anchor is `TestRedactExplain` at
//! `pkg/planner/core/tests/redact/redact_test.go:23`; its recursive CTE plan
//! tree includes `CTETable root Scan on CTE_0`.

use tidb_planner::physical_cte_table::PhysicalCteTable;

#[test]
fn explain_info_matches_cte_plan_tree_text() {
    let cte = PhysicalCteTable::new(0);
    assert_eq!(cte.explain_info(), "Scan on CTE_0");
}

#[test]
fn task_gates_match_source_rejection_order() {
    assert!(PhysicalCteTable::find_best_task(0, true, false).is_none());
    assert!(PhysicalCteTable::find_best_task(0, false, true).is_none());
    assert!(PhysicalCteTable::find_best_task(0, false, false).is_some());
}
