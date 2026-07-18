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

//! Expression precedence levels, a direct copy of `pkg/parser/prec.go`. Higher
//! binds tighter.

/// No operator / lowest precedence.
pub const NONE: u8 = 0;
/// `OR`, `||`.
pub const OR: u8 = 1;
/// `XOR`.
pub const XOR: u8 = 2;
/// `AND`, `&&`.
pub const AND: u8 = 3;
/// `NOT` (prefix).
pub const NOT: u8 = 4;
/// `=`, `<=>`, `>=`, `>`, `<=`, `<`, `!=`, `<>`, and `IS`.
pub const COMPARISON: u8 = 5;
/// `LIKE`, `IN`, `BETWEEN`, `REGEXP`.
pub const PREDICATE: u8 = 6;
/// `|`.
pub const BIT_OR: u8 = 7;
/// `&`.
pub const BIT_AND: u8 = 8;
/// `<<`, `>>`.
pub const SHIFT: u8 = 9;
/// `+`, `-`.
pub const ADD_SUB: u8 = 10;
/// `*`, `/`, `%`, `DIV`, `MOD`.
pub const MUL_DIV: u8 = 11;
/// `^`.
pub const BIT_XOR: u8 = 12;
/// Unary `-`, `~`, `!`.
pub const UNARY: u8 = 13;
/// `COLLATE` — real TiDB's own table (`pkg/parser/prec.go`) has an
/// intervening `precConcat = 14` level for `||` under `PIPES_AS_CONCAT`
/// sql_mode, which this crate doesn't model (`||` is always `OR`, matching
/// [`tidb_ast::BinaryOp::LogicOr`]'s own doc), so that level is skipped
/// here rather than reserved unused. Binds TIGHTER than [`UNARY`] —
/// confirmed by reading `pkg/parser/expr_parser.go` directly: MySQL's own
/// documented example is `-1 COLLATE x` == `-(1 COLLATE x)`.
pub const COLLATE: u8 = 14;
