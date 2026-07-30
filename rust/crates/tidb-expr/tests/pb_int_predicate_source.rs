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

//! Source-backed integer-domain predicate-to-TiPB vectors.

use tidb_ast::BinaryOp;
use tidb_expr::pb_predicate::{
    bigint_column_field_type, int_comparison_to_pb, int_in_to_pb, int_is_null_to_pb,
    is_int_family_type, is_unsigned, logical_not_to_pb, logical_or_to_pb, IntPbOperand,
    PbPredicateError,
};
use tidb_proto::tipb::{ExprType, ScalarFuncSig};

fn column(offset: usize, flags: u32) -> IntPbOperand {
    IntPbOperand::Column {
        offset,
        field_type: bigint_column_field_type(flags),
    }
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
        let expression =
            int_comparison_to_pb(operator, column(1, 3), IntPbOperand::Literal(-7)).unwrap();
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
    let expression =
        int_comparison_to_pb(BinaryOp::Gt, column(0, 1), IntPbOperand::Literal(i64::MIN)).unwrap();
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
    let expression =
        int_comparison_to_pb(BinaryOp::Lt, IntPbOperand::Literal(1), column(2, 3)).unwrap();
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
        int_comparison_to_pb(BinaryOp::NullEq, column(0, 1), IntPbOperand::Literal(1),),
        Err(PbPredicateError::UnsupportedOperator(BinaryOp::NullEq))
    );
}

/// Go's `getBaseCmpType` answers `ETInt` for every integer type, which is why
/// one signature family covers the whole family; `BIT` is deliberately not in
/// it, because Go gates hybrid-type pushdown separately.
#[test]
fn the_int_family_is_exactly_gos_eval_int_column_types() {
    // TINYINT, SMALLINT, LONG, LONGLONG, INT24, YEAR.
    for mysql_type in [1, 2, 3, 8, 9, 13] {
        assert!(is_int_family_type(mysql_type), "type {mysql_type}");
    }
    // DECIMAL, DOUBLE, VARCHAR, DATETIME, BIT, JSON.
    for mysql_type in [246, 5, 15, 12, 16, 245] {
        assert!(!is_int_family_type(mysql_type), "type {mysql_type}");
    }
}

/// The `UNSIGNED` flag is read from the column's own flags, because that is
/// the only place TiKV learns a comparison is unsigned: the signature is the
/// same either way.
#[test]
fn unsignedness_travels_in_the_column_flags_not_the_signature() {
    const UNSIGNED_FLAG: u32 = 32;
    assert!(is_unsigned(UNSIGNED_FLAG | 1));
    assert!(!is_unsigned(1));
    let signed =
        int_comparison_to_pb(BinaryOp::Gt, column(0, 1), IntPbOperand::Literal(5)).unwrap();
    let unsigned = int_comparison_to_pb(
        BinaryOp::Gt,
        column(0, 1 | UNSIGNED_FLAG),
        IntPbOperand::Literal(5),
    )
    .unwrap();
    assert_eq!(signed.sig, unsigned.sig, "one signature for both");
    assert_eq!(
        unsigned.children[0].field_type.as_ref().unwrap().flag,
        Some(1 | UNSIGNED_FLAG)
    );
}

/// `IS NULL` over an integer argument is Go's `IntIsNull`, and its result is
/// the boolean `BIGINT(1)` every predicate here returns.
#[test]
fn is_null_lowers_to_the_integer_signature_with_a_boolean_result() {
    let expression = int_is_null_to_pb(column(2, 0)).unwrap();
    assert_eq!(expression.tp, Some(ExprType::ScalarFunc as i32));
    assert_eq!(expression.sig, Some(ScalarFuncSig::IntIsNull as i32));
    assert_eq!(expression.children.len(), 1);
    assert_eq!(expression.children[0].tp, Some(ExprType::ColumnRef as i32));
    let result = expression.field_type.as_ref().unwrap();
    assert_eq!(result.tp, Some(8));
    assert_eq!(result.flen, Some(1));
}

/// `IN` puts the tested expression first and the list after it, with Go's
/// in-place constant de-duplication already applied.
#[test]
fn in_int_children_are_the_tested_column_then_the_deduplicated_list() {
    let expression = int_in_to_pb(
        column(0, 1),
        [
            IntPbOperand::Literal(3),
            IntPbOperand::Literal(1),
            IntPbOperand::Literal(3),
        ],
    )
    .unwrap();
    assert_eq!(expression.sig, Some(ScalarFuncSig::InInt as i32));
    assert_eq!(expression.children.len(), 3, "the duplicate 3 is gone");
    assert_eq!(expression.children[0].tp, Some(ExprType::ColumnRef as i32));
    // Order of the survivors is the source order, not sorted.
    assert_eq!(
        expression.children[1].val.as_deref(),
        Some(&[0x80, 0, 0, 0, 0, 0, 0, 3][..])
    );
    assert_eq!(
        expression.children[2].val.as_deref(),
        Some(&[0x80, 0, 0, 0, 0, 0, 0, 1][..])
    );
    // An empty list is not an `IN` at all.
    assert_eq!(
        int_in_to_pb(column(0, 1), []),
        Err(PbPredicateError::EmptyOperandList)
    );
}

/// `OR` folds left into TiKV's binary `LogicalOr`; one branch needs no node,
/// and no branch is an error rather than a vacuous condition.
#[test]
fn logical_or_folds_left_and_a_single_branch_is_returned_unchanged() {
    let branch = |value| {
        int_comparison_to_pb(BinaryOp::Eq, column(0, 1), IntPbOperand::Literal(value)).unwrap()
    };
    let single = logical_or_to_pb([branch(1)]).unwrap();
    assert_eq!(single.sig, Some(ScalarFuncSig::EqInt as i32));

    let chain = logical_or_to_pb([branch(1), branch(2), branch(3)]).unwrap();
    assert_eq!(chain.sig, Some(ScalarFuncSig::LogicalOr as i32));
    assert_eq!(chain.children[0].sig, Some(ScalarFuncSig::LogicalOr as i32));
    assert_eq!(chain.children[1].sig, Some(ScalarFuncSig::EqInt as i32));

    assert_eq!(
        logical_or_to_pb([]),
        Err(PbPredicateError::EmptyOperandList)
    );

    let negated = logical_not_to_pb(branch(1));
    assert_eq!(negated.sig, Some(ScalarFuncSig::UnaryNotInt as i32));
    assert_eq!(negated.children.len(), 1);
}
