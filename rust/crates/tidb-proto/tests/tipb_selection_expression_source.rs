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

//! Exact TiPB wire vectors for the bounded Selection expression projection.

use prost::Message;
use tidb_proto::tipb::{ExecType, Executor, Expr, ExprType, FieldType, ScalarFuncSig, Selection};

#[test]
fn bounded_selection_contract_keeps_upstream_numeric_values() {
    assert_eq!(ExecType::TypeSelection as i32, 2);
    assert_eq!(ExprType::Null as i32, 0);
    assert_eq!(ExprType::Int64 as i32, 1);
    assert_eq!(ExprType::ColumnRef as i32, 201);
    assert_eq!(ExprType::ScalarFunc as i32, 10_000);
    assert_eq!(ScalarFuncSig::Unspecified as i32, 0);
    assert_eq!(ScalarFuncSig::LtInt as i32, 100);
    assert_eq!(ScalarFuncSig::LeInt as i32, 110);
    assert_eq!(ScalarFuncSig::GtInt as i32, 120);
    assert_eq!(ScalarFuncSig::GeInt as i32, 130);
    assert_eq!(ScalarFuncSig::EqInt as i32, 140);
    assert_eq!(ScalarFuncSig::NeInt as i32, 150);
}

#[test]
fn selection_executor_and_nonnullable_defaults_keep_exact_wire_tags() {
    let literal = Expr {
        tp: Some(ExprType::Int64 as i32),
        val: Some(vec![0x80, 0, 0, 0, 0, 0, 0, 1]),
        children: Vec::new(),
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: None,
        has_distinct: Some(false),
    };
    let executor = Executor {
        tp: Some(ExecType::TypeSelection as i32),
        tbl_scan: None,
        idx_scan: None,
        selection: Some(Selection {
            conditions: vec![literal],
        }),
        executor_id: Some(String::new()),
        parent_idx: None,
    };
    let expected = vec![
        0x08, 0x02, // Executor.tp = TypeSelection (field 1).
        0x22, 0x12, // Executor.selection (field 4), 18-byte payload.
        0x0a, 0x10, // Selection.conditions (field 1), 16-byte Expr.
        0x08, 0x01, // Expr.tp = Int64 (field 1).
        0x12, 0x08, 0x80, 0, 0, 0, 0, 0, 0, 1, // Expr.val (field 2).
        0x20, 0, // Expr.sig = Unspecified (field 4, present at zero).
        0x38, 0, // Expr.has_distinct (field 7, present at false).
        0x52, 0, // Executor.executor_id (field 10, present and empty).
    ];
    assert_eq!(executor.encode_to_vec(), expected);
    assert_eq!(Executor::decode(expected.as_slice()).unwrap(), executor);
}

#[test]
fn field_type_preserves_every_upstream_scalar_field_presence() {
    let field_type = FieldType {
        tp: Some(8),
        flag: Some(0),
        flen: Some(20),
        decimal: Some(0),
        collate: Some(-63),
        charset: Some("binary".to_owned()),
        elems: Vec::new(),
        array: Some(false),
    };
    let encoded = field_type.encode_to_vec();
    assert_eq!(FieldType::decode(encoded.as_slice()).unwrap(), field_type);
    assert!(encoded.windows(2).any(|bytes| bytes == [0x10, 0]));
    assert!(encoded.windows(2).any(|bytes| bytes == [0x20, 0]));
    assert!(encoded.windows(2).any(|bytes| bytes == [0x40, 0]));
}
