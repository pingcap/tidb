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

//! Exact TiPB wire vectors for the complete upstream expression contract.

use prost::Message;
use tidb_proto::tipb::{ExecType, Executor, Expr, ExprType, FieldType, ScalarFuncSig, Selection};

#[test]
fn complete_contract_retains_master_executor_and_expression_fields() {
    // Go master pins executor.proto's ExplainForConnection (executor field 26,
    // connection_id field 1) and expression.proto's rpn_args_len (field 6).
    let executor = [0x08, 0x15, 0xd2, 0x01, 0x02, 0x08, 0x2a];
    assert_eq!(
        Executor::decode(executor.as_slice())
            .unwrap()
            .encode_to_vec(),
        executor
    );
    let expression = [0x30, 0x03];
    assert_eq!(
        Expr::decode(expression.as_slice()).unwrap().encode_to_vec(),
        expression
    );
}

#[test]
fn complete_contract_recognizes_the_master_executor_vocabulary() {
    assert!(ExecType::try_from(21).is_ok());
}

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
    assert_eq!(ScalarFuncSig::InString as i32, 4004);
    assert_eq!(ScalarFuncSig::RegexpLikeSig as i32, 4313);
}

#[test]
fn selection_executor_and_nonnullable_defaults_keep_exact_wire_tags() {
    for mode in 0..=4 {
        let expr = Expr {
            agg_func_mode: Some(mode),
            ..Default::default()
        };
        let wire = expr.encode_to_vec();
        assert_eq!(wire, [0x48, mode as u8]);
        assert_eq!(
            Expr::decode(wire.as_slice()).unwrap().agg_func_mode,
            Some(mode)
        );
    }

    let literal = Expr {
        tp: Some(ExprType::Int64 as i32),
        val: Some(vec![0x80, 0, 0, 0, 0, 0, 0, 1]),
        children: Vec::new(),
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: None,
        has_distinct: Some(false),
        agg_func_mode: None,
        ..Default::default()
    };
    let executor = Executor {
        tp: Some(ExecType::TypeSelection as i32),
        tbl_scan: None,
        idx_scan: None,
        selection: Some(Box::new(Selection {
            conditions: vec![literal],
            ..Default::default()
        })),
        aggregation: None,
        top_n: None,
        limit: None,
        executor_id: Some(String::new()),
        parent_idx: None,
        exchange_sender: None,
        ..Default::default()
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

#[test]
fn complete_expression_enum_keeps_value_list_distinct_from_null() {
    let value = ExprType::try_from(151).expect("upstream ValueList is a wire kind");
    assert_ne!(value, ExprType::Null);
    let expr = Expr {
        tp: Some(151),
        ..Default::default()
    };
    assert_eq!(
        Expr::decode(expr.encode_to_vec().as_slice()).unwrap().tp(),
        value
    );
}
