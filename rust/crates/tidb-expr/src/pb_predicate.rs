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

//! TiPB lowering for the integer-domain predicates TiKV evaluates: a
//! comparison, `IS NULL`, `IN`, and the `OR`/`NOT` composition of those.
//!
//! This is the dependency-closed part of Go `expression.PbConverter`
//! (`pkg/expression/expr_to_pb.go`): a resolved TiKV DAG column offset or
//! integer constant becomes a TiPB leaf, and one predicate becomes a
//! scalar-function node. Catalog binding, type coercion, pushdown admission,
//! conjunction splitting, and DAG executor construction remain with their
//! existing owners.
//!
//! # Why one signature family covers the whole integer column family
//!
//! Go picks a comparison's `tipb.ScalarFuncSig` in
//! `builtin_compare.go`'s `generateCmpSigs`, from the comparison's *evaluation
//! type* alone: every `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT` column
//! evaluates as `ETInt`, so all of them -- signed and unsigned -- lower to the
//! same six `*Int` signatures, and `WrapWithCastAsInt` inserts no cast because
//! the argument is already `ETInt`. Signedness and width live *only* in each
//! child's `FieldType`, which is why this module copies the column's real
//! declared type onto the `ColumnRef` leaf rather than assuming `BIGINT`.
//!
//! # `IS NOT NULL` and `NOT IN` are not signatures
//!
//! Go's expression rewriter spells them as `UnaryNot` over the positive form
//! (`expression_rewriter.go`'s `notToExpression`), so this module does too:
//! `col IS NOT NULL` is `UnaryNotInt(IntIsNull(col))`. There is no
//! `IsNotNull` signature to reach for.

use std::{error::Error, fmt};

use tidb_ast::BinaryOp;
use tidb_codec::encode_int;
use tidb_proto::tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

const MYSQL_TYPE_LONGLONG: i32 = 8;
const NOT_NULL_FLAG: u32 = 1;
const UNSIGNED_FLAG: u32 = 1 << 5;
const BINARY_FLAG: u32 = 1 << 7;
const IS_BOOLEAN_FLAG: u32 = 1 << 19;
const BINARY_COLLATION_PROTO_ID: i32 = -63;
const BINARY_CHARSET: &str = "binary";
const BIGINT_COLUMN_LENGTH: i32 = 20;

/// One already-resolved operand of an integer-domain TiKV predicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntPbOperand {
    /// A zero-based offset in the preceding scan executor's output, carrying
    /// the column's own declared type -- Go's `ToPBFieldType(column.RetType)`.
    Column {
        /// Zero-based DAG-basic column offset, not a catalog column ID.
        offset: usize,
        /// The column's TiPB field type, whose `flag` is what tells TiKV
        /// whether to compare the value as signed or unsigned.
        field_type: FieldType,
    },
    /// A signed integer literal after parser normalization.
    Literal(i64),
}

/// Whether an integer column's declared MySQL type is one this lowering
/// speaks: the whole `ETInt` family, which shares one signature set.
#[must_use]
pub const fn is_int_family_type(mysql_type: i32) -> bool {
    // MYSQL_TYPE_TINY, SHORT, LONG, LONGLONG, INT24, and YEAR: the types whose
    // Go `EvalType()` is `ETInt`. `BIT` is deliberately absent -- Go treats it
    // as a hybrid type whose pushdown is separately gated
    // (`infer_pushdown.go`'s `bit` switch), and `BOOL` is `TINYINT` already.
    matches!(mysql_type, 1 | 2 | 3 | 8 | 9 | 13)
}

/// Builds the TiPB field type of a `BIGINT` column with `flags`.
#[must_use]
pub fn bigint_column_field_type(flags: u32) -> FieldType {
    int_field_type(MYSQL_TYPE_LONGLONG, flags, BIGINT_COLUMN_LENGTH, 0)
}

/// Why a typed predicate cannot enter the bounded TiKV expression path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PbPredicateError {
    /// The operator needs a scalar signature outside the six ordinary
    /// integer comparisons admitted by this boundary.
    UnsupportedOperator(BinaryOp),
    /// TiPB encodes a DAG-basic column offset with Go's signed integer codec.
    ColumnOffsetOutOfRange(usize),
    /// `IN` needs at least one list element, and `OR` at least one branch.
    EmptyOperandList,
}

impl fmt::Display for PbPredicateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedOperator(operator) => {
                write!(
                    formatter,
                    "unsupported TiKV integer comparison operator: {operator:?}"
                )
            }
            Self::ColumnOffsetOutOfRange(offset) => {
                write!(formatter, "TiKV column offset does not fit i64: {offset}")
            }
            Self::EmptyOperandList => {
                formatter.write_str("a TiKV IN list and an OR chain each need an operand")
            }
        }
    }
}

impl Error for PbPredicateError {}

/// Lowers one source-ordered integer comparison into exact TiPB.
///
/// Operand order is never normalized: `1 < column` and `column > 1` remain
/// distinct expression trees, matching Go `scalarFuncToPBExpr`.
pub fn int_comparison_to_pb(
    operator: BinaryOp,
    left: IntPbOperand,
    right: IntPbOperand,
) -> Result<Expr, PbPredicateError> {
    let signature = comparison_signature(operator)?;
    Ok(boolean_scalar_func(
        signature,
        vec![operand_to_pb(left)?, operand_to_pb(right)?],
    ))
}

/// Lowers `column IS NULL` (Go `ScalarFuncSig_IntIsNull`).
pub fn int_is_null_to_pb(column: IntPbOperand) -> Result<Expr, PbPredicateError> {
    Ok(boolean_scalar_func(
        ScalarFuncSig::IntIsNull,
        vec![operand_to_pb(column)?],
    ))
}

/// Lowers `tested IN (list)` (Go `ScalarFuncSig_InInt`).
///
/// Go's `buildHashMapForConstArgs` de-duplicates the constant list in place
/// before the expression is ever converted, so the duplicates never reach the
/// wire; this does the same, preserving the order of the survivors.
pub fn int_in_to_pb(
    tested: IntPbOperand,
    list: impl IntoIterator<Item = IntPbOperand>,
) -> Result<Expr, PbPredicateError> {
    let mut children = vec![operand_to_pb(tested)?];
    let mut seen: Vec<IntPbOperand> = Vec::new();
    for element in list {
        if seen.contains(&element) {
            continue;
        }
        seen.push(element.clone());
        children.push(operand_to_pb(element)?);
    }
    if children.len() < 2 {
        return Err(PbPredicateError::EmptyOperandList);
    }
    Ok(boolean_scalar_func(ScalarFuncSig::InInt, children))
}

/// Composes branches with `OR` (Go `ScalarFuncSig_LogicalOr`).
///
/// TiKV's `LogicalOr` is binary, so a longer chain folds left, which is the
/// association Go's own left-associative `OR` parse produces. A single branch
/// needs no node at all and is returned unchanged.
///
/// Each branch must already evaluate to a boolean integer. Go additionally
/// wraps a non-boolean `OR` argument in `IsTruth` (`builtin_op.go`'s
/// `wrapWithIsTrue`); every branch this module accepts is a comparison,
/// `IS NULL`, `IN`, `NOT` or a nested `OR`, all of which Go already types as
/// `ETInt` with `IsBooleanFlag`, so the wrapper would be the identity.
pub fn logical_or_to_pb(
    branches: impl IntoIterator<Item = Expr>,
) -> Result<Expr, PbPredicateError> {
    branches
        .into_iter()
        .reduce(|left, right| boolean_scalar_func(ScalarFuncSig::LogicalOr, vec![left, right]))
        .ok_or(PbPredicateError::EmptyOperandList)
}

/// Negates a boolean-integer predicate (Go `ScalarFuncSig_UnaryNotInt`).
#[must_use]
pub fn logical_not_to_pb(child: Expr) -> Expr {
    boolean_scalar_func(ScalarFuncSig::UnaryNotInt, vec![child])
}

fn comparison_signature(operator: BinaryOp) -> Result<ScalarFuncSig, PbPredicateError> {
    let signature = match operator {
        BinaryOp::Lt => ScalarFuncSig::LtInt,
        BinaryOp::Le => ScalarFuncSig::LeInt,
        BinaryOp::Gt => ScalarFuncSig::GtInt,
        BinaryOp::Ge => ScalarFuncSig::GeInt,
        BinaryOp::Eq => ScalarFuncSig::EqInt,
        BinaryOp::Ne => ScalarFuncSig::NeInt,
        _ => return Err(PbPredicateError::UnsupportedOperator(operator)),
    };
    Ok(signature)
}

/// A scalar-function node whose result is MySQL's boolean `BIGINT(1)`: the
/// `ETInt` return type with `SetFlen(1)` that Go's `newBaseBuiltinFuncWithTp`
/// gives every comparison, `IS NULL`, `IN`, `NOT` and logical connective.
fn boolean_scalar_func(signature: ScalarFuncSig, children: Vec<Expr>) -> Expr {
    Expr {
        tp: Some(ExprType::ScalarFunc as i32),
        val: None,
        children,
        sig: Some(signature as i32),
        field_type: Some(int_field_type(
            MYSQL_TYPE_LONGLONG,
            BINARY_FLAG | IS_BOOLEAN_FLAG,
            1,
            0,
        )),
        // gogoproto nullable=false emits this field even at its default.
        has_distinct: Some(false),
    }
}

fn operand_to_pb(operand: IntPbOperand) -> Result<Expr, PbPredicateError> {
    let (tp, value, field_type) = match operand {
        IntPbOperand::Column { offset, field_type } => {
            let index = i64::try_from(offset)
                .map_err(|_| PbPredicateError::ColumnOffsetOutOfRange(offset))?;
            (ExprType::ColumnRef, encode_signed(index), field_type)
        }
        IntPbOperand::Literal(value) => (
            ExprType::Int64,
            encode_signed(value),
            int_field_type(
                MYSQL_TYPE_LONGLONG,
                NOT_NULL_FLAG | BINARY_FLAG,
                i32::try_from(value.to_string().len()).expect("i64 display width fits i32"),
                0,
            ),
        ),
    };
    Ok(Expr {
        tp: Some(tp as i32),
        val: Some(value),
        children: Vec::new(),
        // Upstream Expr.sig is gogoproto nullable=false.
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: Some(field_type),
        has_distinct: Some(false),
    })
}

fn encode_signed(value: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(8);
    encode_int(&mut encoded, value);
    encoded
}

/// Go `ToPBFieldType` for an integer column or constant: the seven fields it
/// copies, with the binary charset/collation every integer type carries.
#[must_use]
pub fn int_field_type(mysql_type: i32, flags: u32, flen: i32, decimal: i32) -> FieldType {
    FieldType {
        tp: Some(mysql_type),
        flag: Some(flags),
        flen: Some(flen),
        decimal: Some(decimal),
        collate: Some(BINARY_COLLATION_PROTO_ID),
        charset: Some(BINARY_CHARSET.to_owned()),
        elems: Vec::new(),
        // Upstream FieldType.array is gogoproto nullable=false.
        array: Some(false),
    }
}

/// Whether `flags` marks the column `UNSIGNED`, which decides whether a
/// negative comparison constant is a shape Go would have refined away.
#[must_use]
pub const fn is_unsigned(flags: u32) -> bool {
    flags & UNSIGNED_FLAG != 0
}
