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

//! Dependency-closed vectors for physical SHOW planning metadata.
//!
//! The Go anchor is `TestShow` at `pkg/planner/core/planbuilder_test.go:63`.

use tidb_planner::physical_show::{PhysicalShowPlan, ShowPlanKind};

#[test]
fn regular_show_uses_pseudo_one_row_stats() {
    let plan = PhysicalShowPlan::init_show();
    assert_eq!(plan.kind(), ShowPlanKind::Show);
    assert_eq!(plan.row_count(), 1);
}

#[test]
fn ddl_jobs_show_keeps_job_number_and_gate_behavior() {
    let plan = PhysicalShowPlan::find_best_task(ShowPlanKind::DdlJobs, false, false, 12).unwrap();
    assert_eq!(plan.job_number(), Some(12));
    assert!(PhysicalShowPlan::find_best_task(ShowPlanKind::DdlJobs, false, true, 12).is_none());
}
