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

//! Dependency-closed vectors for PhysicalUnionScan planning gates.
//!
//! The Go anchor is `TestDAGPlanBuilderUnionScan` at
//! `pkg/planner/core/casetest/dag/dag_test.go:274`.

use tidb_planner::physical_union_scan::{
    exhaust_physical_union_scan, PhysicalUnionScanPlan, UnionScanExhaustion,
};

#[test]
fn mpp_union_scan_is_rejected() {
    assert_eq!(
        exhaust_physical_union_scan(true, true, 0, 0, 0),
        UnionScanExhaustion::UnsupportedFlash
    );
}

#[test]
fn admitted_non_mpp_union_scan_preserves_metadata() {
    assert_eq!(
        exhaust_physical_union_scan(false, true, 2, 3, 1),
        UnionScanExhaustion::Planned(PhysicalUnionScanPlan::init(2, 3, 1))
    );
}
