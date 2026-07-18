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

//! Dependency-closed vectors for PhysicalMaxOneRow's planning contract.
//!
//! The Go anchor is `TestMaxOneRow` at
//! `pkg/executor/test/executor/executor_test.go:2157`; it proves the
//! user-visible subquery error that motivates this physical operator.

use tidb_planner::physical_max_one_row::{CteProducerStatus, PhysicalMaxOneRowPlan};

#[test]
fn unsupported_sort_or_flash_requirements_do_not_emit_a_plan() {
    let status = CteProducerStatus::new(1);
    assert!(PhysicalMaxOneRowPlan::exhaust(false, false, status, false).is_none());
    assert!(PhysicalMaxOneRowPlan::exhaust(true, true, status, false).is_none());
}

#[test]
fn supported_plan_requests_two_rows_and_forwards_property_fields() {
    let status = CteProducerStatus::new(42);
    let plan = PhysicalMaxOneRowPlan::exhaust(true, false, status, true).unwrap();
    assert_eq!(plan.expected_cnt(), 2);
    assert_eq!(plan.cte_producer_status().value(), 42);
    assert!(plan.no_cop_push_down());
}
