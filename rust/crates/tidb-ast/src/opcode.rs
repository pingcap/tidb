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
//!
//! [`Op`] deliberately keeps the source integer representation instead of
//! using a Rust enum. Go's zero value is observable: `Op(0).String()` and
//! `Op(0).Format(...)` both produce an empty string. Values outside the
//! source table panic when inspected, just like indexing Go's `ops` array.

use std::fmt;

use crate::{RestoreCtx, RestoreWriter};

#[derive(Clone, Copy)]
struct OpInfo {
    name: &'static str,
    literal: &'static str,
    is_keyword: bool,
}

const EMPTY: OpInfo = OpInfo {
    name: "",
    literal: "",
    is_keyword: false,
};

const OPS: [OpInfo; 33] = [
    EMPTY,
    OpInfo {
        name: "and",
        literal: "AND",
        is_keyword: true,
    },
    OpInfo {
        name: "leftshift",
        literal: "<<",
        is_keyword: false,
    },
    OpInfo {
        name: "rightshift",
        literal: ">>",
        is_keyword: false,
    },
    OpInfo {
        name: "or",
        literal: "OR",
        is_keyword: true,
    },
    OpInfo {
        name: "ge",
        literal: ">=",
        is_keyword: false,
    },
    OpInfo {
        name: "le",
        literal: "<=",
        is_keyword: false,
    },
    OpInfo {
        name: "eq",
        literal: "=",
        is_keyword: false,
    },
    OpInfo {
        name: "ne",
        literal: "!=",
        is_keyword: false,
    },
    OpInfo {
        name: "lt",
        literal: "<",
        is_keyword: false,
    },
    OpInfo {
        name: "gt",
        literal: ">",
        is_keyword: false,
    },
    OpInfo {
        name: "plus",
        literal: "+",
        is_keyword: false,
    },
    OpInfo {
        name: "minus",
        literal: "-",
        is_keyword: false,
    },
    OpInfo {
        name: "bitand",
        literal: "&",
        is_keyword: false,
    },
    OpInfo {
        name: "bitor",
        literal: "|",
        is_keyword: false,
    },
    OpInfo {
        name: "mod",
        literal: "%",
        is_keyword: false,
    },
    OpInfo {
        name: "bitxor",
        literal: "^",
        is_keyword: false,
    },
    OpInfo {
        name: "div",
        literal: "/",
        is_keyword: false,
    },
    OpInfo {
        name: "mul",
        literal: "*",
        is_keyword: false,
    },
    OpInfo {
        name: "not",
        literal: "not ",
        is_keyword: true,
    },
    OpInfo {
        name: "!",
        literal: "!",
        is_keyword: false,
    },
    OpInfo {
        name: "bitneg",
        literal: "~",
        is_keyword: false,
    },
    OpInfo {
        name: "intdiv",
        literal: "DIV",
        is_keyword: true,
    },
    OpInfo {
        name: "xor",
        literal: "XOR",
        is_keyword: true,
    },
    OpInfo {
        name: "nulleq",
        literal: "<=>",
        is_keyword: false,
    },
    OpInfo {
        name: "in",
        literal: "IN",
        is_keyword: true,
    },
    OpInfo {
        name: "like",
        literal: "LIKE",
        is_keyword: true,
    },
    OpInfo {
        name: "case",
        literal: "CASE",
        is_keyword: true,
    },
    OpInfo {
        name: "regexp",
        literal: "REGEXP",
        is_keyword: true,
    },
    OpInfo {
        name: "isnull",
        literal: "IS NULL",
        is_keyword: true,
    },
    OpInfo {
        name: "istrue",
        literal: "IS TRUE",
        is_keyword: true,
    },
    OpInfo {
        name: "isfalse",
        literal: "IS FALSE",
        is_keyword: true,
    },
    OpInfo {
        name: "binary",
        literal: "BINARY",
        is_keyword: true,
    },
];

/// Source-faithful SQL opcode value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Op(i32);

#[allow(non_upper_case_globals)]
impl Op {
    /// Zero-value sentinel. Its name and literal are empty.
    pub const INVALID: Self = Self(0);
    /// Logical `AND`.
    pub const LogicAnd: Self = Self(1);
    /// Bitwise left shift.
    pub const LeftShift: Self = Self(2);
    /// Bitwise right shift.
    pub const RightShift: Self = Self(3);
    /// Logical `OR`.
    pub const LogicOr: Self = Self(4);
    /// Greater than or equal.
    pub const GE: Self = Self(5);
    /// Less than or equal.
    pub const LE: Self = Self(6);
    /// Equal.
    pub const EQ: Self = Self(7);
    /// Not equal.
    pub const NE: Self = Self(8);
    /// Less than.
    pub const LT: Self = Self(9);
    /// Greater than.
    pub const GT: Self = Self(10);
    /// Addition or unary plus.
    pub const Plus: Self = Self(11);
    /// Subtraction or unary minus.
    pub const Minus: Self = Self(12);
    /// Bitwise AND.
    pub const And: Self = Self(13);
    /// Bitwise OR.
    pub const Or: Self = Self(14);
    /// Modulo.
    pub const Mod: Self = Self(15);
    /// Bitwise XOR.
    pub const Xor: Self = Self(16);
    /// Division.
    pub const Div: Self = Self(17);
    /// Multiplication.
    pub const Mul: Self = Self(18);
    /// Keyword logical NOT, whose literal includes one trailing space.
    pub const Not: Self = Self(19);
    /// Symbolic logical NOT (`!`).
    pub const Not2: Self = Self(20);
    /// Bitwise negation.
    pub const BitNeg: Self = Self(21);
    /// Integer division.
    pub const IntDiv: Self = Self(22);
    /// Logical XOR.
    pub const LogicXor: Self = Self(23);
    /// NULL-safe equality.
    pub const NullEQ: Self = Self(24);
    /// Membership predicate.
    pub const In: Self = Self(25);
    /// Pattern-match predicate.
    pub const Like: Self = Self(26);
    /// CASE expression marker.
    pub const Case: Self = Self(27);
    /// Regular-expression predicate.
    pub const Regexp: Self = Self(28);
    /// `IS NULL` predicate.
    pub const IsNull: Self = Self(29);
    /// `IS TRUE` predicate.
    pub const IsTruth: Self = Self(30);
    /// `IS FALSE` predicate.
    pub const IsFalsity: Self = Self(31);
    /// Unary `BINARY` cast marker.
    pub const Binary: Self = Self(32);

    /// Every declared source opcode, in exact numeric order.
    pub const ALL: [Self; 32] = [
        Self::LogicAnd,
        Self::LeftShift,
        Self::RightShift,
        Self::LogicOr,
        Self::GE,
        Self::LE,
        Self::EQ,
        Self::NE,
        Self::LT,
        Self::GT,
        Self::Plus,
        Self::Minus,
        Self::And,
        Self::Or,
        Self::Mod,
        Self::Xor,
        Self::Div,
        Self::Mul,
        Self::Not,
        Self::Not2,
        Self::BitNeg,
        Self::IntDiv,
        Self::LogicXor,
        Self::NullEQ,
        Self::In,
        Self::Like,
        Self::Case,
        Self::Regexp,
        Self::IsNull,
        Self::IsTruth,
        Self::IsFalsity,
        Self::Binary,
    ];

    /// Constructs an opcode from the source integer representation.
    ///
    /// Zero preserves Go's empty zero value. Inspecting a value outside
    /// `0..=32` panics with the same fail-fast behavior as Go's table index.
    pub const fn from_value(value: i32) -> Self {
        Self(value)
    }

    /// Returns the exact source integer representation.
    pub const fn value(self) -> i32 {
        self.0
    }

    fn info(self) -> &'static OpInfo {
        &OPS[self.0 as usize]
    }

    /// Returns the source operator name used by `Op.String()`.
    pub fn name(self) -> &'static str {
        self.info().name
    }

    /// Returns the literal emitted by source `Op.Format()`.
    pub fn literal(self) -> &'static str {
        self.info().literal
    }

    /// Writes the source literal without transforming keyword case.
    ///
    /// Writer failures are deliberately ignored, matching Go's discarded
    /// `io.WriteString` result.
    pub fn format<W: fmt::Write + ?Sized>(self, writer: &mut W) {
        let _ = writer.write_str(self.literal());
    }

    /// Returns whether restore treats this operator as a keyword.
    pub fn is_keyword(self) -> bool {
        self.info().is_keyword
    }

    /// Restores this operator through the shared source restore context.
    pub fn restore<W: RestoreWriter>(self, ctx: &mut RestoreCtx<W>) {
        if self.is_keyword() {
            ctx.write_keyword(self.literal());
        } else {
            ctx.write_plain(self.literal());
        }
    }
}

impl fmt::Display for Op {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}
