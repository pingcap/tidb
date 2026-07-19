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

//! Bounded TiPB lowering for signed-`BIGINT` comparison conditions.
//!
//! This is the dependency-closed part of Go `expression.PbConverter`: a
//! resolved TiKV DAG column offset or signed integer constant becomes a TiPB
//! leaf, and one ordinary comparison becomes a scalar-function node. Catalog
//! binding, type coercion, pushdown admission, conjunction splitting, and DAG
//! executor construction remain with their existing owners.

use std::{error::Error, fmt};

use tidb_ast::BinaryOp;
use tidb_codec::encode_int;
use tidb_proto::tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

const MYSQL_TYPE_LONGLONG: i32 = 8;
const NOT_NULL_FLAG: u32 = 1;
const BINARY_FLAG: u32 = 1 << 7;
const IS_BOOLEAN_FLAG: u32 = 1 << 19;
const BINARY_COLLATION_PROTO_ID: i32 = -63;
const BINARY_CHARSET: &str = "binary";
const BIGINT_COLUMN_LENGTH: i32 = 20;

/// One already-resolved operand of a signed-`BIGINT` TiKV comparison.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SignedBigIntPbOperand {
    /// A zero-based offset in the preceding scan executor's output.
    Column {
        /// Zero-based DAG-basic column offset, not a catalog column ID.
        offset: usize,
        /// Exact TiDB field flags from the resolved catalog column.
        flags: u32,
    },
    /// A signed integer literal after parser normalization.
    Literal(i64),
}

/// Why a typed comparison cannot enter the bounded TiKV expression path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PbComparisonError {
    /// The operator needs a scalar signature outside the six ordinary
    /// signed-integer comparisons admitted by this boundary.
    UnsupportedOperator(BinaryOp),
    /// TiPB encodes a DAG-basic column offset with Go's signed integer codec.
    ColumnOffsetOutOfRange(usize),
}

impl fmt::Display for PbComparisonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedOperator(operator) => {
                write!(
                    formatter,
                    "unsupported TiKV BIGINT comparison operator: {operator:?}"
                )
            }
            Self::ColumnOffsetOutOfRange(offset) => {
                write!(formatter, "TiKV column offset does not fit i64: {offset}")
            }
        }
    }
}

impl Error for PbComparisonError {}

/// Lowers one source-ordered signed-`BIGINT` comparison into exact TiPB.
///
/// Operand order is never normalized: `1 < column` and `column > 1` remain
/// distinct expression trees, matching Go `scalarFuncToPBExpr`.
pub fn signed_bigint_comparison_to_pb(
    operator: BinaryOp,
    left: SignedBigIntPbOperand,
    right: SignedBigIntPbOperand,
) -> Result<Expr, PbComparisonError> {
    let signature = comparison_signature(operator)?;
    Ok(Expr {
        tp: Some(ExprType::ScalarFunc as i32),
        val: None,
        children: vec![operand_to_pb(left)?, operand_to_pb(right)?],
        sig: Some(signature as i32),
        field_type: Some(comparison_result_field_type()),
        // gogoproto nullable=false emits this field even at its default.
        has_distinct: Some(false),
    })
}

fn comparison_signature(operator: BinaryOp) -> Result<ScalarFuncSig, PbComparisonError> {
    let signature = match operator {
        BinaryOp::Lt => ScalarFuncSig::LtInt,
        BinaryOp::Le => ScalarFuncSig::LeInt,
        BinaryOp::Gt => ScalarFuncSig::GtInt,
        BinaryOp::Ge => ScalarFuncSig::GeInt,
        BinaryOp::Eq => ScalarFuncSig::EqInt,
        BinaryOp::Ne => ScalarFuncSig::NeInt,
        _ => return Err(PbComparisonError::UnsupportedOperator(operator)),
    };
    Ok(signature)
}

fn operand_to_pb(operand: SignedBigIntPbOperand) -> Result<Expr, PbComparisonError> {
    let (tp, value, field_type) = match operand {
        SignedBigIntPbOperand::Column { offset, flags } => {
            let offset = i64::try_from(offset)
                .map_err(|_| PbComparisonError::ColumnOffsetOutOfRange(offset))?;
            (
                ExprType::ColumnRef,
                encode_signed(offset),
                bigint_field_type(flags, BIGINT_COLUMN_LENGTH),
            )
        }
        SignedBigIntPbOperand::Literal(value) => (
            ExprType::Int64,
            encode_signed(value),
            bigint_field_type(
                NOT_NULL_FLAG | BINARY_FLAG,
                i32::try_from(value.to_string().len()).expect("i64 display width fits i32"),
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

fn bigint_field_type(flags: u32, flen: i32) -> FieldType {
    FieldType {
        tp: Some(MYSQL_TYPE_LONGLONG),
        flag: Some(flags),
        flen: Some(flen),
        decimal: Some(0),
        collate: Some(BINARY_COLLATION_PROTO_ID),
        charset: Some(BINARY_CHARSET.to_owned()),
        elems: Vec::new(),
        // Upstream FieldType.array is gogoproto nullable=false.
        array: Some(false),
    }
}

fn comparison_result_field_type() -> FieldType {
    bigint_field_type(BINARY_FLAG | IS_BOOLEAN_FLAG, 1)
}
