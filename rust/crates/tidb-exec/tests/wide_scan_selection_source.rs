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

//! The wide SQL path's pushed predicates, lowered onto the bounded physical
//! Selection and encoded into a coprocessor DAG.

use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_distsql::EncodeType as DistSqlEncodeType;
use tidb_exec::dag_request::{construct_read_only_dag_req, DagRequestContext, TiKvScanPlan};
use tidb_exec::wide_scan_selection::{wide_scan_selection_plan, WideScanSelectionError};
use tidb_executor::scan_pushdown::{ScanComparison, ScanComparisonOp};
use tidb_planner::{
    physical_table_scan::PhysicalTableScanPlan,
    scan_pushdown::{ScanColumnInfo, TiKvTableScanSpec},
};
use tidb_proto::tipb::{DagRequest, ExecType, ExprType, ScalarFuncSig};

fn column_info(id: i64, flags: i32) -> ScanColumnInfo {
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

fn bigint() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn pushed(
    column_offset: u32,
    op: ScanComparisonOp,
    literal: Datum,
    column_on_left: bool,
) -> ScanComparison {
    ScanComparison {
        column_offset,
        column_type: bigint(),
        op,
        literal,
        column_on_left,
    }
}

/// The pushed half of a wide-path `WHERE` really travels in the request: the
/// encoded DAG carries a Selection executor beside the scan, with one
/// condition per pushed conjunct.
#[test]
fn a_wide_path_pushed_predicate_is_carried_by_the_encoded_dag() {
    let table = PhysicalTableScanPlan::init(
        1,
        0,
        TiKvTableScanSpec::new(42, vec![column_info(1, 3), column_info(2, 1)]),
    );
    // `SELECT a, b FROM t WHERE a > 5 AND 10 > b`.
    let comparisons = vec![
        pushed(0, ScanComparisonOp::Gt, Datum::Int(5), true),
        pushed(1, ScanComparisonOp::Gt, Datum::Int(10), false),
    ];
    let selection = wide_scan_selection_plan(&comparisons).unwrap();
    let request = construct_read_only_dag_req(
        &DagRequestContext::new("UTC", 0, 32, DistSqlEncodeType::Default),
        TiKvScanPlan::Table(&table),
        Some(&selection),
        &[0, 1],
    )
    .unwrap();

    // Decoding the bytes that would go on the wire, not the in-memory value.
    let decoded = DagRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.executors.len(), 2);
    assert_eq!(decoded.executors[0].tp, Some(ExecType::TypeTableScan as i32));
    let selection_executor = &decoded.executors[1];
    assert_eq!(selection_executor.tp, Some(ExecType::TypeSelection as i32));
    let conditions = &selection_executor.selection.as_ref().unwrap().conditions;
    assert_eq!(conditions.len(), 2, "one condition per pushed conjunct");

    // `a > 5`: the column stays on the left, as written.
    assert_eq!(conditions[0].sig, Some(ScalarFuncSig::GtInt as i32));
    assert_eq!(
        conditions[0].children[0].tp,
        Some(ExprType::ColumnRef as i32)
    );
    assert_eq!(conditions[0].children[1].tp, Some(ExprType::Int64 as i32));
    // `10 > b`: the literal stays on the left, so the operand order the
    // statement was written in survives into the request.
    assert_eq!(conditions[1].sig, Some(ScalarFuncSig::GtInt as i32));
    assert_eq!(conditions[1].children[0].tp, Some(ExprType::Int64 as i32));
    assert_eq!(
        conditions[1].children[1].tp,
        Some(ExprType::ColumnRef as i32)
    );
}

/// The wide path applies the same narrow acceptance test the bounded binder
/// does, independently: a shape TiKV's signed-BIGINT Selection cannot
/// evaluate is refused here rather than encoded.
#[test]
fn the_wide_lowering_refuses_everything_outside_the_signed_bigint_shape() {
    assert_eq!(
        wide_scan_selection_plan(&[]),
        Err(WideScanSelectionError::NoConditions)
    );
    let mut unsigned = pushed(0, ScanComparisonOp::Eq, Datum::Int(1), true);
    unsigned.column_type = bigint();
    unsigned.column_type.add_flags(FieldTypeFlags::UNSIGNED);
    assert_eq!(
        wide_scan_selection_plan(&[unsigned]),
        Err(WideScanSelectionError::UnsupportedColumnType { offset: 0 })
    );
    let mut narrow = pushed(3, ScanComparisonOp::Eq, Datum::Int(1), true);
    narrow.column_type = FieldType::new(FieldTypeCode::Long);
    assert_eq!(
        wide_scan_selection_plan(&[narrow]),
        Err(WideScanSelectionError::UnsupportedColumnType { offset: 3 })
    );
    assert_eq!(
        wide_scan_selection_plan(&[pushed(
            2,
            ScanComparisonOp::Eq,
            Datum::Bytes(b"x".to_vec()),
            true
        )]),
        Err(WideScanSelectionError::UnsupportedLiteral { offset: 2 })
    );
}
