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

//! The wide SQL path's lowering of pushed scan predicates onto the
//! coprocessor's Selection.
//!
//! The driver splits a `WHERE` into the conjuncts a base-table scan applies
//! itself and the residual above it
//! ([`tidb_executor::scan_pushdown`]). Those pushed conjuncts are described
//! independently of how they are evaluated; this module turns one description
//! into the TiPB condition tree
//! [`crate::dag_request::construct_capped_read_only_dag_req_with_conditions`]
//! puts in the request, so a coprocessor request carries the predicate rather
//! than the whole table crossing the network.
//!
//! # What this lowering accepts, and why exactly that
//!
//! The integer domain, in full: a `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/
//! `BIGINT`/`YEAR` column, signed or unsigned, compared with an integer
//! constant; `IS [NOT] NULL` over such a column; `[NOT] IN` over an integer
//! constant list; and the `OR`/`NOT` composition of those. Go lowers all of
//! these through the same `*Int` signature family with no cast inserted (see
//! [`tidb_expr::pb_predicate`]), which is what makes the pushed form provably
//! the same predicate as the local one.
//!
//! Two refusals are deliberate and are not "not implemented yet":
//!
//! * **A negative or zero constant against an unsigned column.** Go does not
//!   send that comparison at all: `builtin_compare.go`'s
//!   `refineArgsByUnsignedFlag` rewrites it into a constant-versus-constant
//!   comparison whose truth value it already knows, and the rewrite depends on
//!   the column's `NOT NULL` flag and on the operator. Refusing keeps this
//!   lowering out of a rule it does not implement; the conjunct is still
//!   applied locally, so only wire volume is lost.
//! * **A non-integer constant** (`i = '5'`, `i > 1.5`). Go refines those too,
//!   through `RefineComparedConstant`, and the refined constant is not the one
//!   written in the source.
//!
//! # Why refusing is always safe and lowering wrong never is
//!
//! A conjunct this module refuses stays in the request as a description the
//! backend simply does not evaluate; the caller applies every pushed conjunct
//! to every row it emits anyway ([`tidb_executor::pushdown_scan`]), so a
//! refusal costs network and nothing else. A conjunct lowered *wrongly*,
//! though, would make the coprocessor drop a row the query selects -- and the
//! local filter cannot put back a row that never crossed the wire. Every
//! shape here therefore lowers to the signature Go itself sends for it, and
//! the row-level agreement of the two forms is proved against a real cluster
//! by `rust/scripts/run-realtikv-scan-pushdown.sh`.
//!
//! # What a caller must still do
//!
//! A coprocessor Selection filters the **snapshot** only. The session's
//! staged mutation buffer is merged into the read client-side and never
//! passed through TiKV, so a caller that lowers a predicate through here must
//! keep applying the full predicate to the merged staged rows. That is the
//! promise [`tidb_executor::table_access::TableAccess::accept_scan_filter`]
//! governs, and a source that cannot keep it must refuse the push-down.

use std::error::Error;
use std::fmt;

use tidb_datatype::Datum;
use tidb_executor::scan_pushdown::{ScanComparison, ScanComparisonOp, ScanPredicate};
use tidb_expr::pb_predicate::{
    int_comparison_to_pb, int_field_type, int_in_to_pb, int_is_null_to_pb, is_int_family_type,
    is_unsigned, logical_not_to_pb, logical_or_to_pb, IntPbOperand, PbPredicateError,
};
use tidb_planner::scan_pushdown::ScanColumnInfo;
use tidb_proto::tipb::Expr;

/// Why a pushed conjunct cannot become a coprocessor Selection condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WideScanSelectionError {
    /// Nothing was pushed, so there is no Selection to build.
    NoConditions,
    /// The described column offset is outside the scan's output.
    ColumnOffsetOutOfRange {
        /// The refused condition's scan input offset.
        offset: u32,
        /// The scan's output width.
        width: usize,
    },
    /// The compared column is outside the integer family this lowering speaks.
    UnsupportedColumnType {
        /// The refused column's scan input offset.
        offset: u32,
    },
    /// The constant is not an integer, or is one Go would have refined away
    /// against this column's signedness.
    UnsupportedLiteral {
        /// The refused condition's scan input offset.
        offset: u32,
    },
    /// The bounded expression owner rejected the assembled condition.
    Expression(PbPredicateError),
}

impl fmt::Display for WideScanSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoConditions => {
                formatter.write_str("no pushed conjunct to lower into a Selection")
            }
            Self::ColumnOffsetOutOfRange { offset, width } => write!(
                formatter,
                "scan input offset {offset} is outside scan width {width}"
            ),
            Self::UnsupportedColumnType { offset } => write!(
                formatter,
                "scan input offset {offset} is not an integer-family column"
            ),
            Self::UnsupportedLiteral { offset } => write!(
                formatter,
                "the constant compared with scan input offset {offset} is not an \
                 integer TiKV would compare unrefined"
            ),
            Self::Expression(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for WideScanSelectionError {}

impl From<PbPredicateError> for WideScanSelectionError {
    fn from(error: PbPredicateError) -> Self {
        Self::Expression(error)
    }
}

/// Lowers every pushed conjunct into one TiPB condition, in `WHERE` order.
///
/// TiKV's Selection ANDs its conditions, so the flattened top-level `AND` the
/// driver produced needs no `LogicalAnd` node of its own.
pub fn wide_scan_selection_conditions(
    predicates: &[ScanPredicate],
    columns: &[ScanColumnInfo],
) -> Result<Vec<Expr>, WideScanSelectionError> {
    if predicates.is_empty() {
        return Err(WideScanSelectionError::NoConditions);
    }
    predicates
        .iter()
        .map(|predicate| predicate_to_pb(predicate, columns))
        .collect()
}

/// Whether this lowering accepts `predicate` on its own -- the per-conjunct
/// admission test the request filter applies before anything is sent.
#[must_use]
pub fn accepts(predicate: &ScanPredicate, columns: &[ScanColumnInfo]) -> bool {
    predicate_to_pb(predicate, columns).is_ok()
}

fn predicate_to_pb(
    predicate: &ScanPredicate,
    columns: &[ScanColumnInfo],
) -> Result<Expr, WideScanSelectionError> {
    match predicate {
        ScanPredicate::Compare(comparison) => comparison_to_pb(comparison, columns),
        ScanPredicate::IsNull {
            column_offset,
            negated,
            ..
        } => {
            let is_null = int_is_null_to_pb(int_column_operand(*column_offset, columns)?)?;
            Ok(negate_if(is_null, *negated))
        }
        ScanPredicate::In {
            column_offset,
            literals,
            negated,
            ..
        } => {
            let offset = *column_offset;
            let flags = column_flags(offset, columns)?;
            let list = literals
                .iter()
                .map(|literal| int_literal_operand(offset, flags, literal))
                .collect::<Result<Vec<_>, _>>()?;
            let membership = int_in_to_pb(int_column_operand(offset, columns)?, list)?;
            Ok(negate_if(membership, *negated))
        }
        ScanPredicate::Or(branches) => {
            let branches = branches
                .iter()
                .map(|branch| predicate_to_pb(branch, columns))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(logical_or_to_pb(branches)?)
        }
        ScanPredicate::Not(inner) => Ok(logical_not_to_pb(predicate_to_pb(inner, columns)?)),
    }
}

fn negate_if(expression: Expr, negated: bool) -> Expr {
    if negated {
        logical_not_to_pb(expression)
    } else {
        expression
    }
}

fn comparison_to_pb(
    comparison: &ScanComparison,
    columns: &[ScanColumnInfo],
) -> Result<Expr, WideScanSelectionError> {
    let offset = comparison.column_offset;
    let flags = column_flags(offset, columns)?;
    let column = int_column_operand(offset, columns)?;
    let constant = int_literal_operand(offset, flags, &comparison.literal)?;
    // Operand order is preserved rather than canonicalized, matching what the
    // source protobuf encodes for `5 < a` versus `a > 5`.
    let (lhs, rhs) = if comparison.column_on_left {
        (column, constant)
    } else {
        (constant, column)
    };
    Ok(int_comparison_to_pb(
        comparison_op(comparison.op),
        lhs,
        rhs,
    )?)
}

fn column_flags(offset: u32, columns: &[ScanColumnInfo]) -> Result<u32, WideScanSelectionError> {
    let column =
        columns
            .get(offset as usize)
            .ok_or(WideScanSelectionError::ColumnOffsetOutOfRange {
                offset,
                width: columns.len(),
            })?;
    if !is_int_family_type(column.tp) {
        return Err(WideScanSelectionError::UnsupportedColumnType { offset });
    }
    u32::try_from(column.flag).map_err(|_| WideScanSelectionError::UnsupportedColumnType { offset })
}

/// The column leaf, carrying the scan descriptor's own declared type so TiKV
/// compares the value with the signedness and width the table declares.
fn int_column_operand(
    offset: u32,
    columns: &[ScanColumnInfo],
) -> Result<IntPbOperand, WideScanSelectionError> {
    let flags = column_flags(offset, columns)?;
    let column = &columns[offset as usize];
    Ok(IntPbOperand::Column {
        offset: offset as usize,
        field_type: int_field_type(column.tp, flags, column.column_len, column.decimal),
    })
}

/// One constant operand, refused unless it is an integer TiKV would compare
/// exactly as written against a column carrying `column_flags`.
fn int_literal_operand(
    offset: u32,
    column_flags: u32,
    literal: &Datum,
) -> Result<IntPbOperand, WideScanSelectionError> {
    let Datum::Int(value) = literal else {
        return Err(WideScanSelectionError::UnsupportedLiteral { offset });
    };
    // Go's `refineArgsByUnsignedFlag` returns the arguments untouched exactly
    // when the constant is strictly positive; at or below zero it rewrites the
    // comparison, so the form written here would not be the form Go sends.
    if is_unsigned(column_flags) && *value <= 0 {
        return Err(WideScanSelectionError::UnsupportedLiteral { offset });
    }
    Ok(IntPbOperand::Literal(*value))
}

const fn comparison_op(op: ScanComparisonOp) -> tidb_ast::BinaryOp {
    match op {
        ScanComparisonOp::Eq => tidb_ast::BinaryOp::Eq,
        ScanComparisonOp::Ne => tidb_ast::BinaryOp::Ne,
        ScanComparisonOp::Lt => tidb_ast::BinaryOp::Lt,
        ScanComparisonOp::Le => tidb_ast::BinaryOp::Le,
        ScanComparisonOp::Gt => tidb_ast::BinaryOp::Gt,
        ScanComparisonOp::Ge => tidb_ast::BinaryOp::Ge,
    }
}
