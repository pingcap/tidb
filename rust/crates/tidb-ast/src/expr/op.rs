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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Unary/binary operators and the small operand-selector enums they read,
//! mirroring Go's operator handling in `pkg/parser/ast/expressions.go` over
//! `pkg/parser/opcode`.

use super::*;

/// [`Expr::WeightString`]'s own `AS` clause type — `CHARACTER` is a real
/// synonym for `CHAR`, collapsed to [`WeightStringType::Char`] at parse
/// time (see [`Expr::WeightString`]'s own doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightStringType {
    /// `CHAR` (or its `CHARACTER` synonym).
    Char,
    /// `BINARY`.
    Binary,
}

/// [`Expr::Trim`]'s own direction keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimDirection {
    /// `BOTH`.
    Both,
    /// `LEADING`.
    Leading,
    /// `TRAILING`.
    Trailing,
}

/// The right-hand side of an `IS` predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsTarget {
    /// `IS NULL`.
    Null,
    /// `IS TRUE`.
    True,
    /// `IS FALSE`.
    False,
    /// `IS UNKNOWN`.
    Unknown,
}

/// A prefix unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// Unary plus `+`.
    Plus,
    /// Unary minus `-`.
    Minus,
    /// Bitwise NOT `~`.
    BitNeg,
    /// Logical NOT via `!`.
    Not,
    /// Logical NOT via the `NOT` keyword.
    NotKeyword,
}

impl UnaryOp {
    /// Returns the single source opcode authority for this typed AST subset.
    pub const fn opcode(self) -> Op {
        match self {
            UnaryOp::Plus => Op::Plus,
            UnaryOp::Minus => Op::Minus,
            UnaryOp::BitNeg => Op::BitNeg,
            UnaryOp::Not => Op::Not2,
            UnaryOp::NotKeyword => Op::Not,
        }
    }

    /// Returns the canonical AST restore spelling for this unary adapter.
    pub fn canonical_literal(self) -> &'static str {
        match self.opcode() {
            Op::Not => "NOT ",
            op => op.literal(),
        }
    }

    pub(crate) fn restore(self) -> &'static str {
        self.canonical_literal()
    }
}

/// An infix binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    /// `+`.
    Plus,
    /// `-`.
    Minus,
    /// `*`.
    Mul,
    /// `/`.
    Div,
    /// `%` / `MOD`.
    Mod,
    /// `DIV` (integer division).
    IntDiv,
    /// `|`.
    BitOr,
    /// `&`.
    BitAnd,
    /// `^`.
    BitXor,
    /// `<<`.
    LeftShift,
    /// `>>`.
    RightShift,
    /// `=`.
    Eq,
    /// `<=>`.
    NullEq,
    /// `>=`.
    Ge,
    /// `>`.
    Gt,
    /// `<=`.
    Le,
    /// `<`.
    Lt,
    /// `!=` / `<>`.
    Ne,
    /// `AND` / `&&`.
    LogicAnd,
    /// `OR` / `||`.
    LogicOr,
    /// `XOR`.
    LogicXor,
}

impl BinaryOp {
    /// Returns the single source opcode authority for this typed AST subset.
    pub const fn opcode(self) -> Op {
        match self {
            BinaryOp::Plus => Op::Plus,
            BinaryOp::Minus => Op::Minus,
            BinaryOp::Mul => Op::Mul,
            BinaryOp::Div => Op::Div,
            BinaryOp::Mod => Op::Mod,
            BinaryOp::IntDiv => Op::IntDiv,
            BinaryOp::BitOr => Op::Or,
            BinaryOp::BitAnd => Op::And,
            BinaryOp::BitXor => Op::Xor,
            BinaryOp::LeftShift => Op::LeftShift,
            BinaryOp::RightShift => Op::RightShift,
            BinaryOp::Eq => Op::EQ,
            BinaryOp::NullEq => Op::NullEQ,
            BinaryOp::Ge => Op::GE,
            BinaryOp::Gt => Op::GT,
            BinaryOp::Le => Op::LE,
            BinaryOp::Lt => Op::LT,
            BinaryOp::Ne => Op::NE,
            BinaryOp::LogicAnd => Op::LogicAnd,
            BinaryOp::LogicOr => Op::LogicOr,
            BinaryOp::LogicXor => Op::LogicXor,
        }
    }

    /// The restore text, including surrounding spaces for keyword operators.
    pub(crate) fn restore(self) -> &'static str {
        match self.opcode() {
            Op::IntDiv => " DIV ",
            Op::LogicAnd => " AND ",
            Op::LogicOr => " OR ",
            Op::LogicXor => " XOR ",
            op => op.literal(),
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for WeightStringType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Char => {}
            Self::Binary => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for TrimDirection {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Both => {}
            Self::Leading => {}
            Self::Trailing => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IsTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Null => {}
            Self::True => {}
            Self::False => {}
            Self::Unknown => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for UnaryOp {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Plus => {}
            Self::Minus => {}
            Self::BitNeg => {}
            Self::Not => {}
            Self::NotKeyword => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BinaryOp {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Plus => {}
            Self::Minus => {}
            Self::Mul => {}
            Self::Div => {}
            Self::Mod => {}
            Self::IntDiv => {}
            Self::BitOr => {}
            Self::BitAnd => {}
            Self::BitXor => {}
            Self::LeftShift => {}
            Self::RightShift => {}
            Self::Eq => {}
            Self::NullEq => {}
            Self::Ge => {}
            Self::Gt => {}
            Self::Le => {}
            Self::Lt => {}
            Self::Ne => {}
            Self::LogicAnd => {}
            Self::LogicOr => {}
            Self::LogicXor => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
