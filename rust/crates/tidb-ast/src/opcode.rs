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

//! SQL operator identity and restore metadata from `pkg/parser/opcode`.

use std::{fmt, io};

use crate::{RestoreCtx, RestoreWriter};

#[derive(Clone, Copy)]
struct OpInfo {
    name: &'static str,
    literal: &'static str,
    is_keyword: bool,
}

const fn info(name: &'static str, literal: &'static str, is_keyword: bool) -> OpInfo {
    OpInfo {
        name,
        literal,
        is_keyword,
    }
}

const OPS: [OpInfo; 33] = [
    info("", "", false),
    info("and", "AND", true),
    info("leftshift", "<<", false),
    info("rightshift", ">>", false),
    info("or", "OR", true),
    info("ge", ">=", false),
    info("le", "<=", false),
    info("eq", "=", false),
    info("ne", "!=", false),
    info("lt", "<", false),
    info("gt", ">", false),
    info("plus", "+", false),
    info("minus", "-", false),
    info("bitand", "&", false),
    info("bitor", "|", false),
    info("mod", "%", false),
    info("bitxor", "^", false),
    info("div", "/", false),
    info("mul", "*", false),
    info("not", "not ", true),
    info("!", "!", false),
    info("bitneg", "~", false),
    info("intdiv", "DIV", true),
    info("xor", "XOR", true),
    info("nulleq", "<=>", false),
    info("in", "IN", true),
    info("like", "LIKE", true),
    info("case", "CASE", true),
    info("regexp", "REGEXP", true),
    info("isnull", "IS NULL", true),
    info("istrue", "IS TRUE", true),
    info("isfalse", "IS FALSE", true),
    info("binary", "BINARY", true),
];

/// One valid source SQL opcode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Op {
    /// Go's zero-value sentinel.
    #[default]
    Invalid = 0,
    /// Logical `AND`.
    LogicAnd,
    /// Bitwise left shift.
    LeftShift,
    /// Bitwise right shift.
    RightShift,
    /// Logical `OR`.
    LogicOr,
    /// Greater than or equal.
    Ge,
    /// Less than or equal.
    Le,
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Less than.
    Lt,
    /// Greater than.
    Gt,
    /// Addition or unary plus.
    Plus,
    /// Subtraction or unary minus.
    Minus,
    /// Bitwise AND.
    BitAnd,
    /// Bitwise OR.
    BitOr,
    /// Modulo.
    Mod,
    /// Bitwise XOR.
    BitXor,
    /// Division.
    Div,
    /// Multiplication.
    Mul,
    /// Keyword logical NOT.
    NotKeyword,
    /// Symbolic logical NOT (`!`).
    NotSymbol,
    /// Bitwise negation.
    BitNeg,
    /// Integer division.
    IntDiv,
    /// Logical XOR.
    LogicXor,
    /// NULL-safe equality.
    NullEq,
    /// Membership predicate.
    In,
    /// Pattern-match predicate.
    Like,
    /// CASE expression marker.
    Case,
    /// Regular-expression predicate.
    Regexp,
    /// `IS NULL` predicate.
    IsNull,
    /// `IS TRUE` predicate.
    IsTruth,
    /// `IS FALSE` predicate.
    IsFalsity,
    /// Unary `BINARY` cast marker.
    Binary,
}

impl Op {
    /// Every declared source opcode, in numeric order.
    pub const ALL: [Self; 32] = [
        Self::LogicAnd,
        Self::LeftShift,
        Self::RightShift,
        Self::LogicOr,
        Self::Ge,
        Self::Le,
        Self::Eq,
        Self::Ne,
        Self::Lt,
        Self::Gt,
        Self::Plus,
        Self::Minus,
        Self::BitAnd,
        Self::BitOr,
        Self::Mod,
        Self::BitXor,
        Self::Div,
        Self::Mul,
        Self::NotKeyword,
        Self::NotSymbol,
        Self::BitNeg,
        Self::IntDiv,
        Self::LogicXor,
        Self::NullEq,
        Self::In,
        Self::Like,
        Self::Case,
        Self::Regexp,
        Self::IsNull,
        Self::IsTruth,
        Self::IsFalsity,
        Self::Binary,
    ];

    /// Returns the source numeric value.
    #[must_use]
    pub const fn value(self) -> u8 {
        self as u8
    }

    fn info(self) -> &'static OpInfo {
        &OPS[usize::from(self.value())]
    }

    /// Returns the scalar-function name used by `Op.String()`.
    #[must_use]
    pub fn name(self) -> &'static str {
        self.info().name
    }

    /// Returns the literal emitted by source `Op.Format()`.
    #[must_use]
    pub fn literal(self) -> &'static str {
        self.info().literal
    }

    /// Writes the operator literal.
    pub fn format<W: io::Write + ?Sized>(self, writer: &mut W) -> io::Result<()> {
        writer.write_all(self.literal().as_bytes())
    }

    /// Returns whether restore treats this operator as a keyword.
    #[must_use]
    pub fn is_keyword(self) -> bool {
        self.info().is_keyword
    }

    /// Restores this operator through the shared restore context.
    pub fn restore<W: RestoreWriter>(self, context: &mut RestoreCtx<W>) {
        if self.is_keyword() {
            context.write_keyword(self.literal());
        } else {
            context.write_plain(self.literal());
        }
    }
}

impl fmt::Display for Op {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

impl crate::Visitable for Op {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visitor.leave(self)
    }
}
