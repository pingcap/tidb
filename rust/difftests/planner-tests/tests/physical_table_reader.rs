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

//! Dependency-closed vectors for PhysicalTableReader metadata.
//!
//! Source anchors:
//! - `TestRequestTypeSupportedOff` at `pkg/planner/core/physical_plan_test.go:151`
//! - `TestTablePlansAndTablePlanInPhysicalTableReaderClone` at
//!   `pkg/planner/core/planbuilder_test.go:312`

use tidb_planner::physical_table_reader::{PhysicalTableReaderPlan, ReadReqType, StoreType};

#[test]
fn request_type_supported_off_keeps_table_reader_operator_shape() {
    let plan = PhysicalTableReaderPlan::init(Some("Sel([in(test.t.a, 1, 10, 20)])"), 0);
    assert_eq!(plan.plan_type(), "TableReader");
    assert_eq!(plan.explain_info(), "data:Sel([in(test.t.a, 1, 10, 20)])");
    assert_eq!(plan.operator_info(), "data:Sel([in(test.t.a, 1, 10, 20)])");
}

#[test]
fn table_reader_clone_rebuilds_flattened_plan_metadata() {
    let plan = PhysicalTableReaderPlan::init(Some("TableFullScan"), 0)
        .with_read_req_type(ReadReqType::Cop, 0)
        .with_store_type(StoreType::TiFlash)
        .with_table_plan_identity(42)
        .with_common_handle(true)
        .with_table_scan_partition_info_count(3);
    let cloned = plan.clone_plan();
    assert_eq!(cloned.table_plan_explain(), Some("TableFullScan"));
    assert_eq!(cloned.table_plans_len(), 1);
    assert_eq!(cloned.get_table_scans().len(), 1);
    assert_eq!(cloned.table_scan_count(), 1);
    assert!(cloned.table_plan_is_first_flattened());
    assert!(cloned.is_common_handle());
    assert_eq!(cloned.store_type(), StoreType::TiFlash);
    assert_eq!(cloned.memory_usage(), plan.memory_usage() - 3);
}

#[test]
fn multi_node_clone_preserves_derived_flattened_shape_and_scan_count() {
    let plan = PhysicalTableReaderPlan::init(Some("Selection"), 0)
        .with_table_shape(3, 2)
        .with_table_plan_identity(7)
        .with_table_scan_partition_info_count(1);
    let cloned = plan.clone_plan();
    assert_eq!(cloned.table_plans_len(), 3);
    assert_eq!(cloned.table_scan_count(), 2);
    assert!(!cloned.table_plan_is_first_flattened());
    assert_eq!(cloned.memory_usage(), plan.memory_usage() - 1);
}

#[test]
fn table_scan_error_and_normalized_explain_are_stable() {
    let invalid = PhysicalTableReaderPlan::init(None::<String>, 0);
    let error = invalid.get_table_scan().unwrap_err();
    assert_eq!(error.actual(), 0);
    assert_eq!(error.to_string(), "the count of table scan != 1");
    assert_eq!(invalid.explain_normalized_info(), "");
}

#[test]
fn mpp_reader_explain_includes_source_mpp_version() {
    let plan = PhysicalTableReaderPlan::init(Some("ExchangeSender"), 4)
        .with_read_req_type(ReadReqType::Mpp, 3);
    assert_eq!(plan.query_block_offset(), 4);
    assert_eq!(plan.read_req_name(), "mpp");
    assert_eq!(plan.explain_info(), "MppVersion: 3, data:ExchangeSender");
}
