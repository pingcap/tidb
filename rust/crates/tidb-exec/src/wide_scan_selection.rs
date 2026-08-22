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
//! ([`tidb_executor::predicate_pushdown`]). Those pushed conjuncts are described
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
//! And one string shape: a `VARCHAR`/`CHAR`/`VAR_STRING`/blob column compared
//! with a string constant, which Go lowers through the `*String` signature
//! family. That comparison is only defined together with a collator, and TiKV
//! reads the collator off the comparison node's own field type; the collation
//! written there is derived from the operands by
//! [`tidb_expr::pb_predicate::string_comparison_to_pb`], which performs the
//! one derivation with a single answer -- a column against a literal, where
//! the column's collation wins -- and refuses every other shape. `IS NULL`,
//! `IN` additionally accepts a pushable string scalar (including nested calls)
//! against non-NULL string constants, deriving its collator from that tested
//! scalar exactly as Go does.
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
//! to every row it emits anyway ([`tidb_executor::remote_scan`]), so a
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

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::predicate_pushdown::{
    ScanColumnComparison, ScanComparison, ScanComparisonOp, ScanPredicate,
};
use tidb_expr::pb_predicate::{
    decimal_comparison_to_pb, int_comparison_to_pb, int_field_type, int_in_to_pb,
    int_is_null_to_pb, is_int_family_type, is_string_family_type, is_unsigned, logical_not_to_pb,
    logical_or_to_pb, string_comparison_to_pb, string_in_to_pb, string_like_to_pb,
    time_comparison_to_pb, DecimalPbOperand, IntPbOperand, PbPredicateError, StringPbOperand,
    TimePbOperand,
};
use tidb_planner::tikv_scan_spec::ScanColumnInfo;
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
    /// A described builtin call resolved a signature TiKV evaluates, but one of
    /// its operands is a leaf the push-down catalog will not encode -- a column
    /// whose collation this tier does not resolve, an offset outside the scan's
    /// output, or a constant Go would have folded into another type.
    UnsupportedBuiltinOperand,
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
            Self::UnsupportedBuiltinOperand => formatter
                .write_str("a pushed builtin call has an operand this lowering does not encode"),
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
    let mut conditions = Vec::new();
    for predicate in predicates {
        conditions.extend(predicate_to_conditions(predicate, columns)?);
    }
    Ok(conditions)
}

/// Whether this lowering accepts `predicate` on its own -- the per-conjunct
/// admission test the request filter applies before anything is sent.
#[must_use]
pub fn accepts(predicate: &ScanPredicate, columns: &[ScanColumnInfo]) -> bool {
    predicate_to_conditions(predicate, columns).is_ok()
}

fn predicate_to_conditions(
    predicate: &ScanPredicate,
    columns: &[ScanColumnInfo],
) -> Result<Vec<Expr>, WideScanSelectionError> {
    match predicate {
        ScanPredicate::And(branches) => {
            let mut conditions = Vec::new();
            for branch in branches {
                conditions.extend(predicate_to_conditions(branch, columns)?);
            }
            Ok(conditions)
        }
        other => Ok(vec![predicate_to_pb(other, columns)?]),
    }
}

fn predicate_to_pb(
    predicate: &ScanPredicate,
    columns: &[ScanColumnInfo],
) -> Result<Expr, WideScanSelectionError> {
    match predicate {
        ScanPredicate::Compare(comparison) => comparison_to_pb(comparison, columns),
        ScanPredicate::ColumnCompare(comparison) => column_comparison_to_pb(comparison, columns),
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
        ScanPredicate::ScalarIn {
            tested,
            literals,
            negated,
            collation,
        } => {
            let tested = tidb_expr::pushdown_catalog::to_pb(tested, &|offset| {
                scan_column_descriptor(offset, columns)
            })
            .ok_or(WideScanSelectionError::UnsupportedBuiltinOperand)?;
            let literals = literals
                .iter()
                .map(|literal| match literal {
                    Datum::String(value) => Ok(value.bytes().to_vec()),
                    Datum::Bytes(value) => Ok(value.clone()),
                    _ => Err(WideScanSelectionError::UnsupportedBuiltinOperand),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let membership = string_in_to_pb(tested, literals, collation.name())?;
            Ok(negate_if(membership, *negated))
        }
        ScanPredicate::Like {
            column_offset,
            pattern,
            escape,
            collation,
            ..
        } => {
            let column = columns.get(*column_offset as usize).ok_or(
                WideScanSelectionError::ColumnOffsetOutOfRange {
                    offset: *column_offset,
                    width: columns.len(),
                },
            )?;
            Ok(string_like_to_pb(
                string_column_operand(*column_offset, column)?,
                pattern.clone(),
                i64::from(*escape),
                collation.name(),
            )?)
        }
        // A resolved builtin call: the catalog that admitted it is also what
        // encodes it, including the implicit argument casts Go's
        // `newBaseBuiltinFuncWithTp` inserts. It refuses a leaf whose TiPB
        // field type this tier cannot build faithfully, which is a refusal to
        // send and never a wrong condition.
        ScanPredicate::Builtin(call) => tidb_expr::pushdown_catalog::to_pb(call, &|offset| {
            scan_column_descriptor(offset, columns)
        })
        .ok_or(WideScanSelectionError::UnsupportedBuiltinOperand),
        // Top-level ANDs are expanded into Selection.conditions by
        // `predicate_to_conditions`; an AND nested under OR/NOT is not a
        // description the driver produces.
        ScanPredicate::And(_) => Err(WideScanSelectionError::UnsupportedBuiltinOperand),
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

fn scan_column_descriptor(
    offset: u32,
    columns: &[ScanColumnInfo],
) -> Option<tidb_expr::pushdown_catalog::ColumnDescriptor> {
    let column = columns.get(offset as usize)?;
    // The scan descriptor states a column's collation as the PROTOCOL id --
    // already negated by `RewriteNewCollationIDIfNeeded` -- and states no
    // charset at all. Recover both from that one id so the scalar leaf and the
    // scanned column cannot disagree about their collator.
    let collation = tidb_datatype::proto_to_collation(column.collation);
    let charset = tidb_datatype::get_collation_by_name(&collation)
        .map_or_else(|_| "binary".to_owned(), |row| row.charset_name);
    Some(tidb_expr::pushdown_catalog::ColumnDescriptor {
        tp: column.tp,
        flag: u32::try_from(column.flag).ok()?,
        flen: column.column_len,
        decimal: column.decimal,
        charset,
        collation,
    })
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
    let column =
        columns
            .get(offset as usize)
            .ok_or(WideScanSelectionError::ColumnOffsetOutOfRange {
                offset,
                width: columns.len(),
            })?;
    if is_string_family_type(column.tp) {
        return string_comparison(comparison, column);
    }
    if column.tp == i32::from(FieldTypeCode::NewDecimal.mysql_type()) {
        return decimal_comparison(comparison, column);
    }
    if u8::try_from(column.tp)
        .ok()
        .map(FieldTypeCode::from_mysql_type)
        .is_some_and(|code| matches!(code, FieldTypeCode::Date | FieldTypeCode::Datetime))
    {
        return time_comparison(comparison, column);
    }
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

fn column_comparison_to_pb(
    comparison: &ScanColumnComparison,
    columns: &[ScanColumnInfo],
) -> Result<Expr, WideScanSelectionError> {
    let left = columns.get(comparison.left_offset as usize).ok_or(
        WideScanSelectionError::ColumnOffsetOutOfRange {
            offset: comparison.left_offset,
            width: columns.len(),
        },
    )?;
    let right = columns.get(comparison.right_offset as usize).ok_or(
        WideScanSelectionError::ColumnOffsetOutOfRange {
            offset: comparison.right_offset,
            width: columns.len(),
        },
    )?;
    if comparison.left_type.code() != comparison.right_type.code() {
        return Err(WideScanSelectionError::UnsupportedColumnType {
            offset: comparison.left_offset,
        });
    }
    let op = comparison_op(comparison.op);
    let code = comparison.left_type.code();
    if is_int_family_type(left.tp) && is_int_family_type(right.tp) {
        return Ok(int_comparison_to_pb(
            op,
            int_column_operand(comparison.left_offset, columns)?,
            int_column_operand(comparison.right_offset, columns)?,
        )?);
    }
    if code == FieldTypeCode::NewDecimal {
        return Ok(decimal_comparison_to_pb(
            op,
            DecimalPbOperand::Column {
                offset: comparison.left_offset as usize,
                field_type: scan_field_type(left, &comparison.left_type, comparison.left_offset)?,
            },
            DecimalPbOperand::Column {
                offset: comparison.right_offset as usize,
                field_type: scan_field_type(
                    right,
                    &comparison.right_type,
                    comparison.right_offset,
                )?,
            },
        )?);
    }
    if matches!(code, FieldTypeCode::Date | FieldTypeCode::Datetime) {
        return Ok(time_comparison_to_pb(
            op,
            TimePbOperand::Column {
                offset: comparison.left_offset as usize,
                field_type: scan_field_type(left, &comparison.left_type, comparison.left_offset)?,
            },
            TimePbOperand::Column {
                offset: comparison.right_offset as usize,
                field_type: scan_field_type(
                    right,
                    &comparison.right_type,
                    comparison.right_offset,
                )?,
            },
        )?);
    }
    // String column-to-column comparisons require Go's full collation
    // derivation (coercibility and repertoire). Keep this path fail-closed
    // until that package contract is ported; the in-process Selection still
    // evaluates the description exactly.
    Err(WideScanSelectionError::UnsupportedColumnType {
        offset: comparison.left_offset,
    })
}

fn decimal_comparison(
    comparison: &ScanComparison,
    column: &ScanColumnInfo,
) -> Result<Expr, WideScanSelectionError> {
    let offset = comparison.column_offset;
    if comparison.column_type.code() != FieldTypeCode::NewDecimal
        || comparison.literal_type.code() != FieldTypeCode::NewDecimal
    {
        return Err(WideScanSelectionError::UnsupportedColumnType { offset });
    }
    let Datum::Decimal(value) = &comparison.literal else {
        return Err(WideScanSelectionError::UnsupportedLiteral { offset });
    };
    let column_operand = DecimalPbOperand::Column {
        offset: offset as usize,
        field_type: scan_field_type(column, &comparison.column_type, offset)?,
    };
    let literal_operand = DecimalPbOperand::Literal {
        value: value.clone(),
        field_type: tidb_expr::pushdown_catalog::field_type_to_pb(&comparison.literal_type)
            .ok_or(WideScanSelectionError::UnsupportedLiteral { offset })?,
    };
    let (lhs, rhs) = if comparison.column_on_left {
        (column_operand, literal_operand)
    } else {
        (literal_operand, column_operand)
    };
    Ok(decimal_comparison_to_pb(
        comparison_op(comparison.op),
        lhs,
        rhs,
    )?)
}

fn time_comparison(
    comparison: &ScanComparison,
    column: &ScanColumnInfo,
) -> Result<Expr, WideScanSelectionError> {
    let offset = comparison.column_offset;
    if !matches!(
        comparison.column_type.code(),
        FieldTypeCode::Date | FieldTypeCode::Datetime
    ) || !matches!(
        comparison.literal_type.code(),
        FieldTypeCode::Date | FieldTypeCode::Datetime
    ) {
        return Err(WideScanSelectionError::UnsupportedColumnType { offset });
    }
    let Datum::Time(value) = &comparison.literal else {
        return Err(WideScanSelectionError::UnsupportedLiteral { offset });
    };
    let column_operand = TimePbOperand::Column {
        offset: offset as usize,
        field_type: scan_field_type(column, &comparison.column_type, offset)?,
    };
    let literal_operand = TimePbOperand::Literal {
        value: *value,
        field_type: tidb_expr::pushdown_catalog::field_type_to_pb(&comparison.literal_type)
            .ok_or(WideScanSelectionError::UnsupportedLiteral { offset })?,
    };
    let (lhs, rhs) = if comparison.column_on_left {
        (column_operand, literal_operand)
    } else {
        (literal_operand, column_operand)
    };
    Ok(time_comparison_to_pb(
        comparison_op(comparison.op),
        lhs,
        rhs,
    )?)
}

fn scan_field_type(
    column: &ScanColumnInfo,
    described: &FieldType,
    offset: u32,
) -> Result<tidb_proto::tipb::FieldType, WideScanSelectionError> {
    let mysql_type = u8::try_from(column.tp)
        .map_err(|_| WideScanSelectionError::UnsupportedColumnType { offset })?;
    let code = FieldTypeCode::from_mysql_type(mysql_type);
    if code != described.code() {
        return Err(WideScanSelectionError::UnsupportedColumnType { offset });
    }
    let flags = u32::try_from(column.flag)
        .map_err(|_| WideScanSelectionError::UnsupportedColumnType { offset })?;
    let collation = tidb_datatype::proto_to_collation(column.collation);
    let charset = tidb_datatype::get_collation_by_name(&collation)
        .map_or_else(|_| "binary".to_owned(), |row| row.charset_name);
    let field_type = FieldType::new(code)
        .with_flags(flags)
        .with_flen(i64::from(column.column_len))
        .with_decimal(i64::from(column.decimal))
        .with_charset_name(charset)
        .with_collation_name(collation);
    tidb_expr::pushdown_catalog::field_type_to_pb(&field_type)
        .ok_or(WideScanSelectionError::UnsupportedColumnType { offset })
}

/// A character-string column compared with a string constant.
///
/// The comparison's collation is derived by the expression owner from the two
/// operands, and it is the column's own -- the very collation the scan
/// descriptor already told the region to read this column with, since both are
/// recovered from that one protocol id. A comparison whose collation cannot be
/// derived that way is refused, not guessed: a wrong collator makes the region
/// drop a row the query selects, and the local filter cannot put back a row
/// that never crossed the wire.
fn string_comparison(
    comparison: &ScanComparison,
    column: &ScanColumnInfo,
) -> Result<Expr, WideScanSelectionError> {
    let offset = comparison.column_offset;
    // Only a character string. Go's `constantToPBExpr` splits `KindString`
    // (`ExprType_String`) from `KindBytes` (`ExprType_Bytes`), and only the
    // former is a leaf this path builds; a `Datum::Bytes` constant is refused
    // rather than sent under the other kind's tag.
    let Datum::String(literal) = &comparison.literal else {
        return Err(WideScanSelectionError::UnsupportedLiteral { offset });
    };
    let literal = literal.bytes();
    // The scan descriptor states the collation as the PROTOCOL id and states
    // no charset at all; both are recovered from that one id, exactly as the
    // builtin path does, so the operand and the column the coprocessor was
    // told to read cannot disagree about which collator applies.
    let column_operand = string_column_operand(offset, column)?;
    let constant = StringPbOperand::Literal(literal.to_vec());
    let (lhs, rhs) = if comparison.column_on_left {
        (column_operand, constant)
    } else {
        (constant, column_operand)
    };
    Ok(string_comparison_to_pb(
        comparison_op(comparison.op),
        lhs,
        rhs,
        comparison.collation.name(),
    )?)
}

fn string_column_operand(
    offset: u32,
    column: &ScanColumnInfo,
) -> Result<StringPbOperand, WideScanSelectionError> {
    if !is_string_family_type(column.tp) {
        return Err(WideScanSelectionError::UnsupportedColumnType { offset });
    }
    let collation = tidb_datatype::proto_to_collation(column.collation);
    let charset = tidb_datatype::get_collation_by_name(&collation)
        .map(|row| row.charset_name)
        .map_err(|_| WideScanSelectionError::UnsupportedColumnType { offset })?;
    Ok(StringPbOperand::Column {
        offset: offset as usize,
        mysql_type: column.tp,
        flags: u32::try_from(column.flag)
            .map_err(|_| WideScanSelectionError::UnsupportedColumnType { offset })?,
        flen: column.column_len,
        charset,
        collation,
    })
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
