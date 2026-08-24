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
use tidb_codec::{encode_decimal_fixed, encode_int, encode_uint};
use tidb_datatype::{Decimal, Time};
use tidb_proto::tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

const MYSQL_TYPE_LONGLONG: i32 = 8;
const NOT_NULL_FLAG: u32 = 1;
const UNSIGNED_FLAG: u32 = 1 << 5;
const BINARY_FLAG: u32 = 1 << 7;
const IS_BOOLEAN_FLAG: u32 = 1 << 19;
const BINARY_COLLATION_PROTO_ID: i32 = -63;
const BINARY_CHARSET: &str = "binary";
const UTF8MB4_CHARSET: &str = "utf8mb4";
const BIGINT_COLUMN_LENGTH: i32 = 20;
const MYSQL_TYPE_VAR_STRING: i32 = 253;
const UNSPECIFIED_LENGTH: i32 = -1;

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

/// One already-resolved operand of an exact DECIMAL comparison.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecimalPbOperand {
    /// A scan output column carrying its declared DECIMAL precision/scale.
    Column {
        /// Zero-based DAG-basic column offset.
        offset: usize,
        /// Go `ToPBFieldType(column.RetType)`.
        field_type: FieldType,
    },
    /// A folded decimal literal carrying its expression result type.
    Literal {
        /// Exact fixed-point value.
        value: Decimal,
        /// Go `ToPBFieldType(constant.RetType)`.
        field_type: FieldType,
    },
}

/// One already-resolved operand of a DATE/DATETIME comparison.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimePbOperand {
    /// A scan output column carrying its declared temporal type and FSP.
    Column {
        /// Zero-based DAG-basic column offset.
        offset: usize,
        /// Go `ToPBFieldType(column.RetType)`.
        field_type: FieldType,
    },
    /// A folded MySQL time literal carrying its comparison-domain type.
    Literal {
        /// Packed MySQL date/datetime value before wire encoding.
        value: Time,
        /// Go `ToPBFieldType(constant.RetType)`.
        field_type: FieldType,
    },
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

/// Whether a column's declared MySQL type is a character-string family whose
/// comparison this lowering speaks.
///
/// `VARCHAR`, `VAR_STRING`, `CHAR` and the four blob/text widths: the families
/// whose Go `EvalType()` is `ETString` and whose TiPB leaf needs nothing
/// beyond the seven fields `ToPBFieldType` copies. `ENUM` and `SET` also
/// evaluate as `ETString` and are deliberately absent -- their leaf needs the
/// `elems` list on the wire and Go gates them behind `IsPushDownEnabled` --
/// as are `BIT` and `JSON`, for the same reason `columnToPBExpr` refuses them.
#[must_use]
pub const fn is_string_family_type(mysql_type: i32) -> bool {
    // MYSQL_TYPE_TINY_BLOB, MEDIUM_BLOB, LONG_BLOB, BLOB, VAR_STRING, STRING,
    // and VARCHAR.
    matches!(mysql_type, 249 | 250 | 251 | 252 | 253 | 254 | 15)
}

/// One already-resolved operand of a string comparison.
///
/// The two spellings are not interchangeable: which side is the column is what
/// decides the comparison's collation, so the distinction is carried in the
/// type rather than recovered from a field type later.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StringPbOperand {
    /// A scan output column, with the charset and collation the scan
    /// descriptor declares for it. Its collation is `Coercibility` IMPLICIT,
    /// which is what makes it win the derivation against a literal.
    Column {
        /// Zero-based DAG-basic column offset, not a catalog column ID.
        offset: usize,
        /// The column's MySQL type byte.
        mysql_type: i32,
        /// The column's flags.
        flags: u32,
        /// The column's declared display width.
        flen: i32,
        /// The column's charset name (`utf8mb4`, `binary`).
        charset: String,
        /// The column's collation NAME -- what TiKV compares its values with.
        collation: String,
    },
    /// A string literal, carrying the connection charset and collation Go's
    /// parser gives one. Its collation is `Coercibility` COERCIBLE and so
    /// never wins a derivation against a column's.
    Literal(Vec<u8>),
}

/// Why a typed predicate cannot enter the bounded TiKV expression path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PbPredicateError {
    /// The operator needs a scalar signature outside the six ordinary
    /// integer comparisons admitted by this boundary.
    UnsupportedOperator(BinaryOp),
    /// A string comparison whose collation this lowering will not derive: not
    /// exactly one column against one literal, or a column in a charset whose
    /// repertoire rules Go's `inferCollation` applies and this does not.
    UnderivableStringCollation,
    /// TiPB encodes a DAG-basic column offset with Go's signed integer codec.
    ColumnOffsetOutOfRange(usize),
    /// `IN` needs at least one list element, and `OR` at least one branch.
    EmptyOperandList,
    /// A typed constant could not be encoded with Go's mem-comparable codec.
    InvalidLiteralEncoding,
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
            Self::UnderivableStringCollation => formatter
                .write_str("a string comparison whose collation this lowering does not derive"),
            Self::InvalidLiteralEncoding => {
                formatter.write_str("a typed TiKV comparison literal could not be encoded")
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

/// Lowers one source-ordered exact DECIMAL comparison into Go's TiPB shape.
pub fn decimal_comparison_to_pb(
    operator: BinaryOp,
    left: DecimalPbOperand,
    right: DecimalPbOperand,
) -> Result<Expr, PbPredicateError> {
    let signature = decimal_comparison_signature(operator)?;
    Ok(boolean_scalar_func(
        signature,
        vec![decimal_operand_to_pb(left)?, decimal_operand_to_pb(right)?],
    ))
}

/// Lowers one source-ordered DATE/DATETIME comparison into Go's TiPB shape.
pub fn time_comparison_to_pb(
    operator: BinaryOp,
    left: TimePbOperand,
    right: TimePbOperand,
) -> Result<Expr, PbPredicateError> {
    let signature = time_comparison_signature(operator)?;
    Ok(boolean_scalar_func(
        signature,
        vec![time_operand_to_pb(left)?, time_operand_to_pb(right)?],
    ))
}

/// Lowers one source-ordered string comparison into exact TiPB.
///
/// # Which collation TiKV uses, and why it is on the parent node
///
/// A string comparison is only defined together with a collator, and TiKV
/// reads that collator from the *comparison node's own* field type -- not
/// from either operand's. Go writes it there in `scalarFuncToPBExpr`:
/// `tp := *expr.RetType; tp.SetCollate(str1)` with `str1` the second half of
/// `expr.CharsetAndCollation()`, which for a comparison is the collation
/// `CheckAndDeriveCollationFromExprs` derived from the two arguments. The
/// return type stays `BIGINT(1)`; only its collation is overwritten. So the
/// node this builds carries the derived collation even though it returns an
/// integer, and getting that field wrong is a silently wrong answer computed
/// at the region rather than an error.
///
/// # The derivation this performs, and the one it refuses
///
/// Go's `inferCollation` aggregates coercibility over all arguments. This
/// lowering implements exactly the one case where that aggregation has a
/// single answer needing no repertoire reasoning: **one column against one
/// literal**. The column's collation is IMPLICIT (2) and the literal's is
/// COERCIBLE (3), lower wins, so the result is the column's own collation --
/// the very collation the scan descriptor already told the region to read
/// that column with, which is what makes the pushed comparison provably the
/// comparison the local evaluator would have made.
///
/// Everything else is refused rather than approximated: column-versus-column
/// (needs the full coercibility/repertoire aggregation), literal-versus-
/// literal (Go folds it at plan time and sends no comparison), and any column
/// charset other than `utf8mb4` or `binary` (`latin1`, `ascii`, `gbk` reach
/// `inferCollation`'s repertoire branches, and a literal that does not fit
/// the column's repertoire changes the derived answer).
pub fn string_comparison_to_pb(
    operator: BinaryOp,
    left: StringPbOperand,
    right: StringPbOperand,
    collation: &str,
) -> Result<Expr, PbPredicateError> {
    let signature = string_comparison_signature(operator)?;
    check_string_collation_is_sendable(&left, collation)?;
    check_string_collation_is_sendable(&right, collation)?;
    let collation = collation.to_owned();
    let children = vec![string_operand_to_pb(left)?, string_operand_to_pb(right)?];
    Ok(Expr {
        tp: Some(ExprType::ScalarFunc as i32),
        val: None,
        children,
        sig: Some(signature as i32),
        // Go `newBaseBuiltinFuncWithTp(..., ETInt, ...)` plus `SetFlen(1)`,
        // with the derived collation written over the return type's own.
        field_type: Some(FieldType {
            tp: Some(MYSQL_TYPE_LONGLONG),
            flag: Some(BINARY_FLAG | IS_BOOLEAN_FLAG),
            flen: Some(1),
            decimal: Some(0),
            collate: Some(tidb_datatype::collation_to_proto(&collation)),
            charset: Some(BINARY_CHARSET.to_owned()),
            elems: Vec::new(),
            array: Some(false),
        }),
        has_distinct: Some(false),
    })
}

/// Refuses an operand whose CHARSET this lowering cannot faithfully describe.
///
/// This used to DERIVE the comparison's collation here, by returning the
/// column operand's on the assumption that the other side was a literal and
/// so COERCIBLE. An explicit `COLLATE` is EXPLICIT and outranks the column,
/// so that inference sent TiKV a BINARY comparison for
/// `a LIKE 'aa' COLLATE utf8mb4_general_ci` over a `utf8mb4_bin` column. The
/// collation is now CARRIED from the description
/// (`tidb_executor::predicate_pushdown::ScanPredicate`), which takes it from
/// the built expression -- the same value Go writes onto the function's own
/// `FieldType` for `ExprToPB` to send.
///
/// What remains is the charset gate that inference also performed: a column
/// charset other than `utf8mb4` or `binary` (`latin1`, `ascii`, `gbk`)
/// reaches `inferCollation`'s repertoire branches, where a literal that does
/// not fit the column's repertoire changes the derived answer, so those are
/// refused rather than approximated. A literal operand carries no charset of
/// its own here and passes.
fn check_string_collation_is_sendable(
    operand: &StringPbOperand,
    collation: &str,
) -> Result<(), PbPredicateError> {
    if collation.is_empty() {
        return Err(PbPredicateError::UnderivableStringCollation);
    }
    match operand {
        StringPbOperand::Column { charset, .. } => {
            if charset != UTF8MB4_CHARSET && charset != BINARY_CHARSET {
                return Err(PbPredicateError::UnderivableStringCollation);
            }
            Ok(())
        }
        StringPbOperand::Literal(_) => Ok(()),
    }
}

fn string_operand_to_pb(operand: StringPbOperand) -> Result<Expr, PbPredicateError> {
    let (tp, value, field_type) = match operand {
        StringPbOperand::Column {
            offset,
            mysql_type,
            flags,
            flen,
            charset,
            collation,
        } => {
            let index = i64::try_from(offset)
                .map_err(|_| PbPredicateError::ColumnOffsetOutOfRange(offset))?;
            (
                ExprType::ColumnRef,
                encode_signed(index),
                FieldType {
                    tp: Some(mysql_type),
                    flag: Some(flags),
                    flen: Some(flen),
                    decimal: Some(0),
                    collate: Some(tidb_datatype::collation_to_proto(&collation)),
                    charset: Some(charset),
                    elems: Vec::new(),
                    array: Some(false),
                },
            )
        }
        // Go `constantToPBExpr`'s `KindString` arm: `tipb.ExprType_String`
        // with the raw bytes as `Val` -- no codec, unlike the integer leaf --
        // and the constant's own field type, which for a parsed literal is
        // the connection charset and collation.
        StringPbOperand::Literal(bytes) => {
            let (charset, collation) = crate::collation_derive::connection_charset_info();
            let flen = i32::try_from(bytes.len()).unwrap_or(UNSPECIFIED_LENGTH);
            (
                ExprType::String,
                bytes,
                FieldType {
                    tp: Some(MYSQL_TYPE_VAR_STRING),
                    flag: Some(NOT_NULL_FLAG),
                    flen: Some(flen),
                    decimal: Some(UNSPECIFIED_LENGTH),
                    collate: Some(tidb_datatype::collation_to_proto(collation)),
                    charset: Some(charset.to_owned()),
                    elems: Vec::new(),
                    array: Some(false),
                },
            )
        }
    };
    Ok(Expr {
        tp: Some(tp as i32),
        val: Some(value),
        children: Vec::new(),
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: Some(field_type),
        has_distinct: Some(false),
    })
}

/// Go `generateCmpSigs`' `types.ETString` arm.
fn string_comparison_signature(operator: BinaryOp) -> Result<ScalarFuncSig, PbPredicateError> {
    Ok(match operator {
        BinaryOp::Lt => ScalarFuncSig::LtString,
        BinaryOp::Le => ScalarFuncSig::LeString,
        BinaryOp::Gt => ScalarFuncSig::GtString,
        BinaryOp::Ge => ScalarFuncSig::GeString,
        BinaryOp::Eq => ScalarFuncSig::EqString,
        BinaryOp::Ne => ScalarFuncSig::NeString,
        // `NullEQ` has a `NullEQString` signature Go pushes, but the
        // description this lowering reads never carries `<=>`: a NULL-safe
        // comparison is not the "column versus non-NULL constant" shape the
        // scan filter splits out.
        _ => return Err(PbPredicateError::UnsupportedOperator(operator)),
    })
}

/// Lowers `column IS NULL` using the signature selected by its Go evaluation
/// type (`IntIsNull`, `DecimalIsNull`, `StringIsNull`, and so on).
pub fn int_is_null_to_pb(column: IntPbOperand) -> Result<Expr, PbPredicateError> {
    Ok(boolean_scalar_func(
        ScalarFuncSig::IntIsNull,
        vec![operand_to_pb(column)?],
    ))
}

/// Lowers `column IS NULL` for an already-built column reference. The
/// signature is deliberately supplied by the caller because TiKV dispatches
/// each evaluation family separately; Go chooses it in `isNullFunctionClass`.
pub fn is_null_to_pb(
    column: Expr,
    signature: ScalarFuncSig,
) -> Result<Expr, PbPredicateError> {
    Ok(boolean_scalar_func(
        signature,
        vec![column],
    ))
}

/// Lowers `string_column IS NULL` with the same `IntIsNull` signature Go
/// uses for every scalar type. The operand keeps its declared string
/// collation in the column reference, so TiKV sees the same descriptor as a
/// string comparison rather than an integer placeholder.
pub fn string_is_null_to_pb(column: StringPbOperand) -> Result<Expr, PbPredicateError> {
    Ok(boolean_scalar_func(
        ScalarFuncSig::IntIsNull,
        vec![string_operand_to_pb(column)?],
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

/// Lowers a string scalar tested by `IN` to Go's `InString` TiPB shape.
///
/// Go derives the function collation across the tested expression and its
/// constant list. For the accepted shape, every list item is a coercible SQL
/// string literal, so the tested expression's collation wins. Go then removes
/// duplicate constants with that collator before protobuf conversion; the
/// same collation keys and first-occurrence order are used here.
pub fn string_in_to_pb(
    tested: Expr,
    list: impl IntoIterator<Item = Vec<u8>>,
    collation_name: &str,
) -> Result<Expr, PbPredicateError> {
    let tested_type = tested
        .field_type
        .as_ref()
        .ok_or(PbPredicateError::UnderivableStringCollation)?;
    let tested_mysql_type = tested_type
        .tp
        .ok_or(PbPredicateError::UnderivableStringCollation)?;
    if !is_string_family_type(tested_mysql_type) {
        return Err(PbPredicateError::UnderivableStringCollation);
    }
    // The COMPARISON's collation, carried from the description rather than
    // read back off the tested operand's own type: an explicit `COLLATE` on a
    // list item outranks the tested expression's, and reading the operand
    // could not see that. See [`check_string_collation_is_sendable`].
    let collation = tidb_datatype::Collation::from_name(collation_name)
        .filter(|collation| *collation != tidb_datatype::Collation::Utf8Mb4ZhPinyinTiDbAsCs)
        .ok_or(PbPredicateError::UnderivableStringCollation)?;

    let mut children = vec![tested];
    let mut seen = Vec::<Vec<u8>>::new();
    for literal in list {
        let key = collation.key(&literal);
        if seen.contains(&key) {
            continue;
        }
        seen.push(key);
        children.push(string_operand_to_pb(StringPbOperand::Literal(literal))?);
    }
    if children.len() < 2 {
        return Err(PbPredicateError::EmptyOperandList);
    }
    Ok(boolean_scalar_func_with_collation(
        ScalarFuncSig::InString,
        children,
        &collation_name,
    ))
}

/// Lowers `string_column LIKE constant_pattern ESCAPE constant` to Go's
/// `LikeSig` shape. The accepted column/literal pair has the same single
/// unambiguous collation derivation as [`string_comparison_to_pb`].
pub fn string_like_to_pb(
    tested: StringPbOperand,
    pattern: Vec<u8>,
    escape: i64,
    collation: &str,
) -> Result<Expr, PbPredicateError> {
    let pattern = StringPbOperand::Literal(pattern);
    check_string_collation_is_sendable(&tested, collation)?;
    let children = vec![
        string_operand_to_pb(tested)?,
        string_operand_to_pb(pattern)?,
        operand_to_pb(IntPbOperand::Literal(escape))?,
    ];
    Ok(boolean_scalar_func_with_collation(
        ScalarFuncSig::LikeSig,
        children,
        &collation,
    ))
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

fn decimal_comparison_signature(operator: BinaryOp) -> Result<ScalarFuncSig, PbPredicateError> {
    Ok(match operator {
        BinaryOp::Lt => ScalarFuncSig::LtDecimal,
        BinaryOp::Le => ScalarFuncSig::LeDecimal,
        BinaryOp::Gt => ScalarFuncSig::GtDecimal,
        BinaryOp::Ge => ScalarFuncSig::GeDecimal,
        BinaryOp::Eq => ScalarFuncSig::EqDecimal,
        BinaryOp::Ne => ScalarFuncSig::NeDecimal,
        _ => return Err(PbPredicateError::UnsupportedOperator(operator)),
    })
}

fn time_comparison_signature(operator: BinaryOp) -> Result<ScalarFuncSig, PbPredicateError> {
    Ok(match operator {
        BinaryOp::Lt => ScalarFuncSig::LtTime,
        BinaryOp::Le => ScalarFuncSig::LeTime,
        BinaryOp::Gt => ScalarFuncSig::GtTime,
        BinaryOp::Ge => ScalarFuncSig::GeTime,
        BinaryOp::Eq => ScalarFuncSig::EqTime,
        BinaryOp::Ne => ScalarFuncSig::NeTime,
        _ => return Err(PbPredicateError::UnsupportedOperator(operator)),
    })
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

fn boolean_scalar_func_with_collation(
    signature: ScalarFuncSig,
    children: Vec<Expr>,
    collation: &str,
) -> Expr {
    let mut expression = boolean_scalar_func(signature, children);
    expression
        .field_type
        .as_mut()
        .expect("a scalar predicate has a result type")
        .collate = Some(tidb_datatype::collation_to_proto(collation));
    expression
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

fn decimal_operand_to_pb(operand: DecimalPbOperand) -> Result<Expr, PbPredicateError> {
    let (tp, value, field_type) = match operand {
        DecimalPbOperand::Column { offset, field_type } => {
            let index = i64::try_from(offset)
                .map_err(|_| PbPredicateError::ColumnOffsetOutOfRange(offset))?;
            (ExprType::ColumnRef, encode_signed(index), field_type)
        }
        DecimalPbOperand::Literal { value, field_type } => {
            let mut encoded = Vec::new();
            encode_decimal_fixed(&mut encoded, &value, 0, 0)
                .map_err(|_| PbPredicateError::InvalidLiteralEncoding)?;
            (ExprType::MysqlDecimal, encoded, field_type)
        }
    };
    Ok(leaf_expr(tp, value, field_type))
}

fn time_operand_to_pb(operand: TimePbOperand) -> Result<Expr, PbPredicateError> {
    let (tp, value, field_type) = match operand {
        TimePbOperand::Column { offset, field_type } => {
            let index = i64::try_from(offset)
                .map_err(|_| PbPredicateError::ColumnOffsetOutOfRange(offset))?;
            (ExprType::ColumnRef, encode_signed(index), field_type)
        }
        TimePbOperand::Literal { value, field_type } => {
            let packed = value
                .to_packed_uint()
                .map_err(|_| PbPredicateError::InvalidLiteralEncoding)?;
            let mut encoded = Vec::new();
            encode_uint(&mut encoded, packed);
            (ExprType::MysqlTime, encoded, field_type)
        }
    };
    Ok(leaf_expr(tp, value, field_type))
}

fn leaf_expr(tp: ExprType, value: Vec<u8>, field_type: FieldType) -> Expr {
    Expr {
        tp: Some(tp as i32),
        val: Some(value),
        children: Vec::new(),
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: Some(field_type),
        has_distinct: Some(false),
    }
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
