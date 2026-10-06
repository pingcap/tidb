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

//! Vectors for the wired PhysicalCTETable identity and eligibility.
//!
//! The Go anchor is `TestRedactExplain` at
//! `pkg/planner/core/tests/redact/redact_test.go:23`; its recursive CTE plan
//! tree includes `CTETable root Scan on CTE_0`.

use tidb_planner::logical::{BaseLogicalPlan, LogicalCTETable};
use tidb_planner::physical::{find_best_task_4_logical_cte_table, PhysicalPlan};
use tidb_planner::physical_property::{PhysicalProperty, SortItem};
use tidb_planner::plan_base::PlanIdAllocator;

fn logical_cte(allocator: &PlanIdAllocator) -> LogicalCTETable {
    LogicalCTETable {
        base: BaseLogicalPlan::new(allocator, LogicalCTETable::TYPE, 9),
        seed_stat: None,
        name: "cte0".to_owned(),
        id_for_storage: 7,
        seed_schema: None,
    }
}

#[test]
fn explain_info_matches_cte_plan_tree_text() {
    let allocator = PlanIdAllocator::new();
    let task = find_best_task_4_logical_cte_table(
        &logical_cte(&allocator),
        &PhysicalProperty::default(),
        &allocator,
    );
    let Some(PhysicalPlan::CTETable(cte)) = task.plan() else {
        panic!("a wired PhysicalCTETable, got {:?}", task.plan());
    };
    assert_eq!(cte.explain_info(), "Scan on CTE_7");
    assert_eq!(cte.base.base.query_block_offset(), 0);
}

#[test]
fn task_gates_match_source_rejection_order() {
    let allocator = PlanIdAllocator::new();
    let sorted = PhysicalProperty {
        sort_items: vec![SortItem::new(1, false)],
        ..PhysicalProperty::default()
    };
    assert!(
        find_best_task_4_logical_cte_table(&logical_cte(&allocator), &sorted, &allocator).invalid()
    );
    assert!(!find_best_task_4_logical_cte_table(
        &logical_cte(&allocator),
        &PhysicalProperty::default(),
        &allocator,
    )
    .invalid());
}
