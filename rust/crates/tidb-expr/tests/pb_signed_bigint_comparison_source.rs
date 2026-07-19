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

//! Source-backed signed-`BIGINT` expression-to-TiPB vectors.

use tidb_ast::BinaryOp;
use tidb_expr::pb_comparison::{
    signed_bigint_comparison_to_pb, PbComparisonError, SignedBigIntPbOperand,
};
use tidb_proto::tipb::{ExprType, ScalarFuncSig};

fn column(offset: usize, flags: u32) -> SignedBigIntPbOperand {
    SignedBigIntPbOperand::Column { offset, flags }
}

#[test]
fn all_six_signed_comparisons_preserve_source_signatures_and_children() {
    let cases = [
        (BinaryOp::Lt, ScalarFuncSig::LtInt),
        (BinaryOp::Le, ScalarFuncSig::LeInt),
        (BinaryOp::Gt, ScalarFuncSig::GtInt),
        (BinaryOp::Ge, ScalarFuncSig::GeInt),
        (BinaryOp::Eq, ScalarFuncSig::EqInt),
        (BinaryOp::Ne, ScalarFuncSig::NeInt),
    ];
    for (operator, signature) in cases {
        let expression = signed_bigint_comparison_to_pb(
            operator,
            column(1, 3),
            SignedBigIntPbOperand::Literal(-7),
        )
        .unwrap();
        assert_eq!(expression.tp, Some(ExprType::ScalarFunc as i32));
        assert_eq!(expression.sig, Some(signature as i32));
        assert_eq!(expression.val, None);
        assert_eq!(expression.has_distinct, Some(false));
        assert_eq!(expression.children.len(), 2);
        assert_eq!(expression.children[0].tp, Some(ExprType::ColumnRef as i32));
        assert_eq!(expression.children[1].tp, Some(ExprType::Int64 as i32));
    }
}

#[test]
fn dag_column_offset_and_literal_use_go_comparable_integer_bytes() {
    let expression = signed_bigint_comparison_to_pb(
        BinaryOp::Gt,
        column(0, 1),
        SignedBigIntPbOperand::Literal(i64::MIN),
    )
    .unwrap();
    assert_eq!(
        expression.children[0].val.as_deref(),
        Some(&[0x80, 0, 0, 0, 0, 0, 0, 0][..])
    );
    assert_eq!(
        expression.children[1].val.as_deref(),
        Some(&[0, 0, 0, 0, 0, 0, 0, 0][..])
    );

    let column_type = expression.children[0].field_type.as_ref().unwrap();
    assert_eq!(column_type.tp, Some(8));
    assert_eq!(column_type.flag, Some(1));
    assert_eq!(column_type.flen, Some(20));
    assert_eq!(column_type.decimal, Some(0));
    assert_eq!(column_type.collate, Some(-63));
    assert_eq!(column_type.charset.as_deref(), Some("binary"));
    assert_eq!(column_type.array, Some(false));

    let literal_type = expression.children[1].field_type.as_ref().unwrap();
    assert_eq!(literal_type.flag, Some(129));
    assert_eq!(literal_type.flen, Some(20));
}

#[test]
fn operand_order_is_not_rewritten() {
    let expression = signed_bigint_comparison_to_pb(
        BinaryOp::Lt,
        SignedBigIntPbOperand::Literal(1),
        column(2, 3),
    )
    .unwrap();
    assert_eq!(expression.children[0].tp, Some(ExprType::Int64 as i32));
    assert_eq!(expression.children[1].tp, Some(ExprType::ColumnRef as i32));
    assert_eq!(
        expression.children[0].field_type.as_ref().unwrap().flen,
        Some(1)
    );
    assert_eq!(
        expression.children[1].field_type.as_ref().unwrap().flag,
        Some(3)
    );
}

#[test]
fn unsupported_operator_fails_closed() {
    assert_eq!(
        signed_bigint_comparison_to_pb(
            BinaryOp::NullEq,
            column(0, 1),
            SignedBigIntPbOperand::Literal(1),
        ),
        Err(PbComparisonError::UnsupportedOperator(BinaryOp::NullEq))
    );
}
