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

//! Documentary gap port for `pkg/planner/core/casetest/flatplan`
//! (`pkg/planner.part4` item 188 on `origin/master`).
//!
//! `TestFlatPhysicalPlan` flattens a whole optimized physical plan through
//! `core.FlattenPhysicalPlan(p, false)` and pins, per main plan and per CTE,
//! the simplified `FlatOperator` record {Depth, Label, IsRoot, StoreType,
//! ReqType (physicalop.ReadReqType), TextTreeIndent, IsLastChild,
//! IsPhysicalPlan} — both in cascades and non-cascades modes — against the
//! `flat_plan_suite` book over `coretestsdk.MockSignedTable()` /
//! `MockUnsignedTable()`. This crate owns no `FlattenPhysicalPlan` and no
//! end-to-end `planner.Optimize`, so the port is recorded as a gap. The
//! family bootstrap `flatplan/main_test.go:30 TestMain` is skipped-reason:
//! bootstrap only (loads the book, zeroes async-commit clock-drift config,
//! goleak).

/// GO PORT of `pkg/planner/core/casetest/flatplan/flat_plan_test.go:69
/// TestFlatPhysicalPlan`.
///
/// Re-derived contract: for every suite SQL, parse → `planner.Optimize`
/// against the mocked two-table infoschema must succeed, and the flattened
/// tree of each main query plus every materialized CTE must equal the
/// recorded depth/label/root-ness/store-type/request-type/text-indent/
/// last-child/physicality tuples exactly (`testdata.OnRecord` shapes the
/// golden both before and after; dual-mode via RunTestUnderCascades).
#[test]
#[ignore = "go-parity-gap: FlattenPhysicalPlan operator-tree flattening with Label/ReadReqType metadata has no Rust counterpart; needs planner.Optimize end to end"]
fn flat_physical_plan_operator_records_main_and_ctes_golden() {}
