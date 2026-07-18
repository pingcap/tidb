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

//! Dependency-closed vectors for PhysicalUnionAll planning gates.
//!
//! The Go anchor is `TestMppUnionAll` at
//! `pkg/planner/core/casetest/mpp/mpp_test.go:446`.

use tidb_planner::physical_union_all::{exhaust_physical_union_all, PhysicalUnionAllPlan};

#[test]
fn mpp_union_all_keeps_union_type_and_mpp_candidate() {
    let plans = exhaust_physical_union_all(true, true, false, false, true, false, 0).unwrap();
    assert_eq!(plans, vec![PhysicalUnionAllPlan::init(true, 0)]);
    assert_eq!(plans[0].plan_type(), "Union");
    assert!(plans[0].mpp());
}

#[test]
fn root_union_all_emits_source_candidate_order() {
    let plans = exhaust_physical_union_all(false, true, false, false, true, true, 7).unwrap();
    assert_eq!(plans[0], PhysicalUnionAllPlan::init(false, 7));
    assert_eq!(plans[1], PhysicalUnionAllPlan::init(true, 7));
}

#[test]
fn sort_and_invalid_partition_requests_are_rejected() {
    assert!(exhaust_physical_union_all(false, true, true, false, true, false, 0).is_none());
    assert!(exhaust_physical_union_all(true, true, false, false, false, false, 0).is_none());
}
