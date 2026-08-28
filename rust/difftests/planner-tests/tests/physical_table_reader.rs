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

//! Dependency-closed vectors for the wired PhysicalTableReader tree.
//!
//! Source anchors:
//! - `TestRequestTypeSupportedOff` at `pkg/planner/core/physical_plan_test.go:151`
//! - `TestTablePlansAndTablePlanInPhysicalTableReaderClone` at
//!   `pkg/planner/core/planbuilder_test.go:312`

use tidb_planner::{
    access_path::{ResolvedTableDescriptor, ResolvedTableScanKind, TableScanExplainIdSuffix},
    physical::{
        BasePhysicalPlan, PhysicalHashJoin, PhysicalPlan, PhysicalTableReader, PhysicalTableScan,
    },
    physical_table_reader::{ReadReqType, StoreType},
    tikv_scan_spec::TiKvTableScanSpec,
};

fn table_scan(id: i32) -> PhysicalTableScan {
    let mut scan = PhysicalTableScan::init(id, 0, TiKvTableScanSpec::new(i64::from(id), vec![]));
    scan.resolved_descriptor = Some(ResolvedTableDescriptor::new(
        i64::from(id),
        false,
        ResolvedTableScanKind::Full,
        TableScanExplainIdSuffix::Omit,
    ));
    scan
}

#[test]
fn request_type_supported_off_keeps_wired_table_reader_shape() {
    let plan = PhysicalTableReader::from_table_scan(table_scan(1)).expect("resolved table scan");
    assert_eq!(plan.plan_type(), "TableReader");
    assert_eq!(plan.explain_info(), "data:TableFullScan");
    assert_eq!(plan.operator_info(), "data:TableFullScan");
    assert_eq!(plan.read_req_name(), "cop");
}

#[test]
fn table_reader_clone_rebuilds_the_owned_physical_tree() {
    let mut reader =
        PhysicalTableReader::from_table_scan(table_scan(42)).expect("resolved table scan");
    reader.store_type = StoreType::TiFlash;
    reader.read_req_type = ReadReqType::Cop;
    reader.is_common_handle = true;
    let plan = PhysicalPlan::TableReader(reader);
    let cloned = plan.clone_plan();
    assert_eq!(cloned.memory_usage(), plan.memory_usage());

    let PhysicalPlan::TableReader(original) = &plan else {
        unreachable!();
    };
    let PhysicalPlan::TableReader(cloned) = &cloned else {
        unreachable!();
    };
    assert_eq!(cloned.table_scans().len(), 1);
    assert_eq!(cloned.table_scan().unwrap().table_id, 42);
    assert!(!std::ptr::eq(
        original.table_plan.as_deref().unwrap(),
        cloned.table_plan.as_deref().unwrap(),
    ));
    assert!(cloned.is_common_handle());
    assert_eq!(cloned.store_type, StoreType::TiFlash);
    assert_eq!(cloned.read_req_type, ReadReqType::Cop);
}

#[test]
fn multi_scan_reader_reports_the_real_tree_cardinality() {
    let mut join_base = BasePhysicalPlan::with_id(7, "HashJoin", 0);
    join_base.set_children(vec![
        PhysicalPlan::TableScan(table_scan(1)),
        PhysicalPlan::TableScan(table_scan(2)),
    ]);
    let table_plan = PhysicalPlan::HashJoin(PhysicalHashJoin {
        base: join_base,
        ..PhysicalHashJoin::default()
    });
    let reader = PhysicalTableReader {
        base: BasePhysicalPlan::with_id(8, "TableReader", 0),
        table_plan: Some(Box::new(table_plan)),
        read_req_type: ReadReqType::Mpp,
        ..PhysicalTableReader::default()
    };

    assert_eq!(reader.table_scans().len(), 2);
    let error = reader.table_scan().unwrap_err();
    assert_eq!(error.actual(), 2);
    assert_eq!(error.to_string(), "the count of table scan != 1");
    assert_eq!(reader.explain_normalized_info(), "");
}
