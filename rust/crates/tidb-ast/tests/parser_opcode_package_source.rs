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

use std::io;

use tidb_ast::{BinaryOp, Op, RestoreCtx, RestoreFlags, UnaryOp};

const SOURCE_ROWS: [(Op, &str, &str, bool); 32] = [
    (Op::LogicAnd, "and", "AND", true),
    (Op::LeftShift, "leftshift", "<<", false),
    (Op::RightShift, "rightshift", ">>", false),
    (Op::LogicOr, "or", "OR", true),
    (Op::Ge, "ge", ">=", false),
    (Op::Le, "le", "<=", false),
    (Op::Eq, "eq", "=", false),
    (Op::Ne, "ne", "!=", false),
    (Op::Lt, "lt", "<", false),
    (Op::Gt, "gt", ">", false),
    (Op::Plus, "plus", "+", false),
    (Op::Minus, "minus", "-", false),
    (Op::BitAnd, "bitand", "&", false),
    (Op::BitOr, "bitor", "|", false),
    (Op::Mod, "mod", "%", false),
    (Op::BitXor, "bitxor", "^", false),
    (Op::Div, "div", "/", false),
    (Op::Mul, "mul", "*", false),
    (Op::NotKeyword, "not", "not ", true),
    (Op::NotSymbol, "!", "!", false),
    (Op::BitNeg, "bitneg", "~", false),
    (Op::IntDiv, "intdiv", "DIV", true),
    (Op::LogicXor, "xor", "XOR", true),
    (Op::NullEq, "nulleq", "<=>", false),
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
        assert_eq!(usize::from(op.value()), index + 1);
        assert_eq!(op.name(), name);
        assert_eq!(op.to_string(), name);
        assert_eq!(op.literal(), literal);
        assert_eq!(op.is_keyword(), is_keyword);
    }
}

#[test]
fn format_matches_every_source_table_entry_including_zero_value() {
    for (op, _, literal, _) in SOURCE_ROWS {
        let mut formatted = Vec::new();
        op.format(&mut formatted).unwrap();
        assert_eq!(formatted, literal.as_bytes());
    }

    let mut zero = Vec::new();
    Op::default().format(&mut zero).unwrap();
    assert_eq!(Op::default(), Op::Invalid);
    assert_eq!(Op::default().value(), 0);
    assert_eq!(Op::default().name(), "");
    assert_eq!(Op::default().to_string(), "");
    assert_eq!(zero, b"");
    assert!(!Op::default().is_keyword());
}

struct ErrorWriter;

impl io::Write for ErrorWriter {
    fn write(&mut self, _input: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("source writer failure"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn format_reports_writer_errors() {
    assert_eq!(
        Op::LogicAnd
            .format(&mut ErrorWriter)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
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

        let mut unchanged = RestoreCtx::new(RestoreFlags::from_bits(0), String::new());
        op.restore(&mut unchanged);
        assert_eq!(unchanged.into_inner(), literal);

        let mut upper_wins = RestoreCtx::new(
            RestoreFlags::KEYWORD_UPPERCASE | RestoreFlags::KEYWORD_LOWERCASE,
            String::new(),
        );
        op.restore(&mut upper_wins);
        assert_eq!(
            upper_wins.into_inner(),
            if is_keyword {
                literal.to_uppercase()
            } else {
                literal.to_owned()
            }
        );
    }

    let mut zero = RestoreCtx::new(RestoreFlags::DEFAULT, String::new());
    Op::Invalid.restore(&mut zero);
    assert_eq!(zero.into_inner(), "");
}

#[test]
fn expression_operator_adapters_delegate_to_the_opcode_authority() {
    assert_eq!(UnaryOp::Plus.opcode(), Op::Plus);
    assert_eq!(UnaryOp::Minus.opcode(), Op::Minus);
    assert_eq!(UnaryOp::BitNeg.opcode(), Op::BitNeg);
    assert_eq!(UnaryOp::Not.opcode(), Op::NotSymbol);
    assert_eq!(UnaryOp::NotKeyword.opcode(), Op::NotKeyword);

    let binary = [
        (BinaryOp::Plus, Op::Plus),
        (BinaryOp::Minus, Op::Minus),
        (BinaryOp::Mul, Op::Mul),
        (BinaryOp::Div, Op::Div),
        (BinaryOp::Mod, Op::Mod),
        (BinaryOp::IntDiv, Op::IntDiv),
        (BinaryOp::BitOr, Op::BitOr),
        (BinaryOp::BitAnd, Op::BitAnd),
        (BinaryOp::BitXor, Op::BitXor),
        (BinaryOp::LeftShift, Op::LeftShift),
        (BinaryOp::RightShift, Op::RightShift),
        (BinaryOp::Eq, Op::Eq),
        (BinaryOp::NullEq, Op::NullEq),
        (BinaryOp::Ge, Op::Ge),
        (BinaryOp::Gt, Op::Gt),
        (BinaryOp::Le, Op::Le),
        (BinaryOp::Lt, Op::Lt),
        (BinaryOp::Ne, Op::Ne),
        (BinaryOp::LogicAnd, Op::LogicAnd),
        (BinaryOp::LogicOr, Op::LogicOr),
        (BinaryOp::LogicXor, Op::LogicXor),
    ];
    for (adapter, op) in binary {
        assert_eq!(adapter.opcode(), op);
    }

    assert_eq!(UnaryOp::NotKeyword.canonical_literal(), "NOT ");
}
