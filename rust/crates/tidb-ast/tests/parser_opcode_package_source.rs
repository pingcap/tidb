// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete package-contract tests for `pkg/parser/opcode`.

use tidb_ast::{BinaryOp, Op, RestoreCtx, RestoreFlags, UnaryOp};

const SOURCE_ROWS: [(Op, &str, &str, bool); 32] = [
    (Op::LogicAnd, "and", "AND", true),
    (Op::LeftShift, "leftshift", "<<", false),
    (Op::RightShift, "rightshift", ">>", false),
    (Op::LogicOr, "or", "OR", true),
    (Op::GE, "ge", ">=", false),
    (Op::LE, "le", "<=", false),
    (Op::EQ, "eq", "=", false),
    (Op::NE, "ne", "!=", false),
    (Op::LT, "lt", "<", false),
    (Op::GT, "gt", ">", false),
    (Op::Plus, "plus", "+", false),
    (Op::Minus, "minus", "-", false),
    (Op::And, "bitand", "&", false),
    (Op::Or, "bitor", "|", false),
    (Op::Mod, "mod", "%", false),
    (Op::Xor, "bitxor", "^", false),
    (Op::Div, "div", "/", false),
    (Op::Mul, "mul", "*", false),
    (Op::Not, "not", "not ", true),
    (Op::Not2, "!", "!", false),
    (Op::BitNeg, "bitneg", "~", false),
    (Op::IntDiv, "intdiv", "DIV", true),
    (Op::LogicXor, "xor", "XOR", true),
    (Op::NullEQ, "nulleq", "<=>", false),
    (Op::In, "in", "IN", true),
    (Op::Like, "like", "LIKE", true),
    (Op::Case, "case", "CASE", true),
    (Op::Regexp, "regexp", "REGEXP", true),
    (Op::IsNull, "isnull", "IS NULL", true),
    (Op::IsTruth, "istrue", "IS TRUE", true),
    (Op::IsFalsity, "isfalse", "IS FALSE", true),
    (Op::Binary, "binary", "BINARY", true),
];

#[test]
fn every_source_value_name_literal_and_keyword_bit_is_exact() {
    assert_eq!(Op::ALL, SOURCE_ROWS.map(|row| row.0));

    for (index, (op, name, literal, is_keyword)) in SOURCE_ROWS.into_iter().enumerate() {
        assert_eq!(op.value(), i32::try_from(index).unwrap() + 1);
        assert_eq!(op.name(), name);
        assert_eq!(op.to_string(), name);
        assert_eq!(op.literal(), literal);
        assert_eq!(op.is_keyword(), is_keyword);
        assert_eq!(Op::from_value(op.value()), op);
    }
}

#[test]
fn format_matches_every_source_table_entry_including_zero_value() {
    for (op, _, literal, _) in SOURCE_ROWS {
        let mut formatted = String::new();
        op.format(&mut formatted);
        assert_eq!(formatted, literal);
    }

    let mut zero = String::new();
    Op::default().format(&mut zero);
    assert_eq!(Op::default(), Op::INVALID);
    assert_eq!(Op::default().value(), 0);
    assert_eq!(Op::default().name(), "");
    assert_eq!(Op::default().to_string(), "");
    assert_eq!(zero, "");
    assert!(!Op::default().is_keyword());
}

#[test]
fn restore_transforms_only_keywords_through_the_shared_context() {
    for (op, _, literal, is_keyword) in SOURCE_ROWS {
        let mut upper = RestoreCtx::new(RestoreFlags::KEYWORD_UPPERCASE, String::new());
        op.restore(&mut upper);
        assert_eq!(
            upper.into_inner(),
            if is_keyword {
                literal.to_uppercase()
            } else {
                literal.to_owned()
            }
        );

        let mut lower = RestoreCtx::new(RestoreFlags::KEYWORD_LOWERCASE, String::new());
        op.restore(&mut lower);
        assert_eq!(
            lower.into_inner(),
            if is_keyword {
                literal.to_lowercase()
            } else {
                literal.to_owned()
            }
        );
    }

    let mut zero = RestoreCtx::new(RestoreFlags::DEFAULT, String::new());
    Op::INVALID.restore(&mut zero);
    assert_eq!(zero.into_inner(), "");
}

#[test]
fn values_outside_the_source_table_fail_fast_when_inspected() {
    for value in [-1, 33] {
        assert!(std::panic::catch_unwind(|| Op::from_value(value).name()).is_err());
        assert!(std::panic::catch_unwind(|| Op::from_value(value).literal()).is_err());
        assert!(std::panic::catch_unwind(|| Op::from_value(value).is_keyword()).is_err());
    }
}

#[test]
fn expression_operator_adapters_delegate_to_the_opcode_authority() {
    assert_eq!(UnaryOp::Plus.opcode(), Op::Plus);
    assert_eq!(UnaryOp::Minus.opcode(), Op::Minus);
    assert_eq!(UnaryOp::BitNeg.opcode(), Op::BitNeg);
    assert_eq!(UnaryOp::Not.opcode(), Op::Not2);
    assert_eq!(UnaryOp::NotKeyword.opcode(), Op::Not);

    let binary = [
        (BinaryOp::Plus, Op::Plus),
        (BinaryOp::Minus, Op::Minus),
        (BinaryOp::Mul, Op::Mul),
        (BinaryOp::Div, Op::Div),
        (BinaryOp::Mod, Op::Mod),
        (BinaryOp::IntDiv, Op::IntDiv),
        (BinaryOp::BitOr, Op::Or),
        (BinaryOp::BitAnd, Op::And),
        (BinaryOp::BitXor, Op::Xor),
        (BinaryOp::LeftShift, Op::LeftShift),
        (BinaryOp::RightShift, Op::RightShift),
        (BinaryOp::Eq, Op::EQ),
        (BinaryOp::NullEq, Op::NullEQ),
        (BinaryOp::Ge, Op::GE),
        (BinaryOp::Gt, Op::GT),
        (BinaryOp::Le, Op::LE),
        (BinaryOp::Lt, Op::LT),
        (BinaryOp::Ne, Op::NE),
        (BinaryOp::LogicAnd, Op::LogicAnd),
        (BinaryOp::LogicOr, Op::LogicOr),
        (BinaryOp::LogicXor, Op::LogicXor),
    ];
    for (adapter, op) in binary {
        assert_eq!(adapter.opcode(), op);
    }

    assert_eq!(UnaryOp::NotKeyword.canonical_literal(), "NOT ");
}
