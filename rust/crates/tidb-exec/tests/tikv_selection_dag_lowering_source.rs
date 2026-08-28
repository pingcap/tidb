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

//! Source-backed TiKV list-DAG Selection lowering vectors.

use prost::Message;
use tidb_distsql::EncodeType as DistSqlEncodeType;
use tidb_exec::dag_request::{
    construct_read_only_dag_req, DagRequestBuildError, DagRequestContext, TiKvScanPlan,
};
use tidb_expr::expression::Expression;
use tidb_planner::{
    physical_table_scan::PhysicalTableScanPlan,
    read_only_scan::{ConfiguredColumn, ConfiguredTable, ReadOnlyScanPlan},
    tikv_scan_spec::{ScanColumnInfo, TiKvTableScanSpec},
};
use tidb_proto::tipb::{DagRequest, ExecType, ExprType, ScalarFuncSig};

fn context() -> DagRequestContext {
    DagRequestContext::new("UTC", 0, 32, DistSqlEncodeType::Default)
}

fn configured_table() -> ConfiguredTable {
    ConfiguredTable::new(
        "test",
        "accounts",
        42,
        [
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_not_null("balance", 2),
            ConfiguredColumn::stored_not_null("version", 3),
        ],
    )
}

fn bounded_plan(sql: &str) -> ReadOnlyScanPlan {
    ReadOnlyScanPlan::lower(sql, &configured_table()).unwrap()
}

fn column(id: i64, flags: i32) -> ScanColumnInfo {
    ScanColumnInfo {
        column_id: id,
        tp: 8,
        collation: 63,
        column_len: 20,
        decimal: 0,
        flag: flags,
        pk_handle: flags == 3,
        ..ScanColumnInfo::default()
    }
}

fn scan_plan(columns: Vec<ScanColumnInfo>) -> PhysicalTableScanPlan {
    PhysicalTableScanPlan::init(1, 0, TiKvTableScanSpec::new(42, columns))
}

#[test]
fn table_scan_then_selection_preserves_all_six_signatures_and_projection() {
    let plan = bounded_plan(
        "SELECT id, balance, version FROM accounts \
         WHERE balance < -2 AND balance <= -1 AND balance > 0 \
           AND balance >= 1 AND balance = 2 AND balance != 3",
    );
    let cases = [
        ScalarFuncSig::LtInt,
        ScalarFuncSig::LeInt,
        ScalarFuncSig::GtInt,
        ScalarFuncSig::GeInt,
        ScalarFuncSig::EqInt,
        ScalarFuncSig::NeInt,
    ];
    let request = construct_read_only_dag_req(
        &context(),
        TiKvScanPlan::Table(plan.table_scan()),
        plan.selection(),
        &[0, 2],
    )
    .unwrap();

    assert_eq!(request.output_offsets, [0, 2]);
    assert_eq!(request.executors.len(), 2);
    assert_eq!(
        request.executors[0].tp,
        Some(ExecType::TypeTableScan as i32)
    );
    let selection_executor = &request.executors[1];
    assert_eq!(selection_executor.tp, Some(ExecType::TypeSelection as i32));
    assert_eq!(selection_executor.tbl_scan, None);
    assert_eq!(selection_executor.idx_scan, None);
    assert_eq!(selection_executor.executor_id.as_deref(), Some(""));
    assert_eq!(selection_executor.parent_idx, None);
    let conditions = &selection_executor.selection.as_ref().unwrap().conditions;
    assert_eq!(conditions.len(), cases.len());
    for (condition, signature) in conditions.iter().zip(cases) {
        assert_eq!(condition.tp, Some(ExprType::ScalarFunc as i32));
        assert_eq!(condition.sig, Some(signature as i32));
        assert_eq!(condition.children[0].tp, Some(ExprType::ColumnRef as i32));
        assert_eq!(
            condition.children[0].val.as_deref(),
            Some(&[0x80, 0, 0, 0, 0, 0, 0, 1][..])
        );
        assert_eq!(
            condition.children[0].field_type.as_ref().unwrap().flag,
            Some(1)
        );
        assert_eq!(condition.children[1].tp, Some(ExprType::Int64 as i32));
    }
    assert_eq!(
        DagRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
}

#[test]
fn literal_left_order_and_duplicate_projection_offsets_are_preserved() {
    let plan = bounded_plan("SELECT id, balance, version FROM accounts WHERE 7 < version");
    let request = construct_read_only_dag_req(
        &context(),
        TiKvScanPlan::Table(plan.table_scan()),
        plan.selection(),
        &[2, 0, 2],
    )
    .unwrap();
    assert_eq!(request.output_offsets, [2, 0, 2]);
    let condition = &request.executors[1].selection.as_ref().unwrap().conditions[0];
    assert_eq!(condition.children[0].tp, Some(ExprType::Int64 as i32));
    assert_eq!(condition.children[1].tp, Some(ExprType::ColumnRef as i32));
    assert_eq!(
        condition.children[1].field_type.as_ref().unwrap().flag,
        Some(1)
    );
}

#[test]
fn optional_selection_keeps_scan_only_context_and_requested_offsets() {
    let table = scan_plan(vec![column(1, 3), column(2, 1)]);
    let request =
        construct_read_only_dag_req(&context(), TiKvScanPlan::Table(&table), None, &[1]).unwrap();
    assert_eq!(request.executors.len(), 1);
    assert_eq!(request.output_offsets, [1]);
    assert_eq!(request.time_zone_name.as_deref(), Some("UTC"));
    assert_eq!(request.flags, Some(32));
}

#[test]
fn invalid_projection_condition_and_flags_fail_closed() {
    let table = scan_plan(vec![column(1, 3), column(2, 1)]);
    assert_eq!(
        construct_read_only_dag_req(&context(), TiKvScanPlan::Table(&table), None, &[2]),
        Err(DagRequestBuildError::OutputOffsetOutOfRange {
            offset: 2,
            width: 2,
        })
    );

    let outside_plan = bounded_plan("SELECT id FROM accounts WHERE balance = 1");
    let mut outside = outside_plan.selection().unwrap().clone();
    let Expression::ScalarFunction(function) = &mut outside.conditions[0] else {
        panic!("the bounded comparison must be a scalar function")
    };
    let Expression::Column(input_column) = &mut function.args[0] else {
        panic!("the left operand must be the scan column")
    };
    input_column.index = 2;
    assert_eq!(
        construct_read_only_dag_req(
            &context(),
            TiKvScanPlan::Table(outside_plan.table_scan()),
            Some(&outside),
            &[0],
        ),
        Err(DagRequestBuildError::ConditionInputOffsetOutOfRange {
            offset: 2,
            width: 2,
        })
    );

    let empty_plan = bounded_plan("SELECT id FROM accounts WHERE balance = 1");
    let mut empty = empty_plan.selection().unwrap().clone();
    empty.conditions.clear();
    assert_eq!(
        construct_read_only_dag_req(
            &context(),
            TiKvScanPlan::Table(empty_plan.table_scan()),
            Some(&empty),
            &[0],
        ),
        Err(DagRequestBuildError::EmptySelection)
    );

    let invalid_flags = scan_plan(vec![column(1, -1)]);
    let selection_plan = bounded_plan("SELECT balance FROM accounts WHERE balance = 1");
    assert_eq!(
        construct_read_only_dag_req(
            &context(),
            TiKvScanPlan::Table(&invalid_flags),
            selection_plan.selection(),
            &[0],
        ),
        Err(DagRequestBuildError::InvalidColumnFlags {
            offset: 0,
            flags: -1,
        })
    );
}
