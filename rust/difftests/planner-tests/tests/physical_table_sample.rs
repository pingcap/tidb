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

//! Dependency-closed vectors for PhysicalTableSample initialization.
//!
//! The Go anchor is `TestTableSamplePlan` at
//! `pkg/executor/sample_test.go:111`; its EXPLAIN ANALYZE assertion requires
//! a `TableSample` physical plan.

use tidb_planner::physical_table_sample::PhysicalTableSamplePlan;

#[test]
fn explain_plan_uses_table_sample_type_and_one_row_pseudo_stats() {
    let plan = PhysicalTableSamplePlan::init(10, false, 0);
    assert_eq!(plan.plan_type(), "TableSample");
    assert_eq!(plan.row_count(), 1.0);
}

#[test]
fn sample_plan_preserves_table_id_desc_and_query_block_offset() {
    let plan = PhysicalTableSamplePlan::init(-8, true, 4);
    assert_eq!(plan.physical_table_id(), -8);
    assert!(plan.desc());
    assert_eq!(plan.query_block_offset(), 4);
}
