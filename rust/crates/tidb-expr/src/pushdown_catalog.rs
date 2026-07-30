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

//! The builtin-call push-down catalog: the single owner of "which
//! `tipb.ScalarFuncSig` is this call, and may TiKV evaluate it".
//!
//! # Why one table, and why it owns pushability
//!
//! Go answers the same question in two places that are read together and
//! never separately:
//!
//! * the per-family `getFunction` in `pkg/expression/builtin_*.go` picks a
//!   concrete `tipb.ScalarFuncSig` from the arguments' *evaluation types*
//!   (and, for `MOD`, their `UNSIGNED` flags), and calls
//!   `newBaseBuiltinFuncWithTp`, which wraps each argument in the implicit
//!   cast that makes it the type the chosen signature reads;
//! * `infer_pushdown.go`'s `scalarExprSupportedByTiKV` then answers whether
//!   that already-resolved signature may travel, keyed on the function name
//!   and -- for `ROUND`, `RAND`, `UNIX_TIMESTAMP` -- on the chosen signature
//!   itself.
//!
//! `pkg/expression/expr_to_pb.go`'s `scalarFuncToPBExpr` reads the resolved
//! signature off the function and asks `canFuncBePushed`; it never re-derives
//! either fact. This module is that pair as ONE table, so a caller cannot hold
//! a signature this engine resolves while consulting a different opinion about
//! whether it may be sent. [`resolve`] is the only entry point, and it returns
//! `None` for a signature TiKV does not evaluate: refusing and "not in the
//! catalog" are deliberately the same answer.
//!
//! # The selector is Go's own switch, as data
//!
//! Each [`BuiltinSignature`] row carries the *incoming* argument pattern it is
//! chosen for ([`BuiltinSignature::selector`]) and the eval types Go coerces
//! the arguments to ([`BuiltinSignature::arg_types`]). Rows are matched in
//! declaration order and the first match wins, which is exactly how Go's
//! `if`/`switch` chains read -- `MOD` tries `(Int, Int)` before falling to
//! `Decimal` and then `Real`, and `ROUND` tries `Int` and `Decimal` before
//! defaulting to `Real`. A `None` slot matches any type, which is how a
//! monomorphic function such as `SIN` (always `ETReal`) is spelled.
//!
//! # What the lowering refuses, and why refusing is safe
//!
//! [`to_pb`] builds the TiPB tree, inserting the same implicit casts
//! `newBaseBuiltinFuncWithTp` inserts. It refuses any leaf whose TiPB
//! `FieldType` this tier cannot build faithfully -- today a non-binary
//! collation (every string, `ENUM`, `SET` and JSON column) and any constant
//! that is not an integer in an integer-typed slot, because Go folds
//! `CAST(<int const> AS REAL)` at plan time and therefore sends a `Float64`
//! literal this tier does not encode.
//!
//! A refusal costs network only. The scan source applies every pushed
//! conjunct to every row it emits regardless
//! (`tidb_executor::scan_pushdown`), so a conjunct the store does not filter
//! is still filtered locally. A conjunct lowered *wrongly* would drop a row
//! the query selects, and no local pass can put back a row that never crossed
//! the wire -- which is why every row of the table below cites the Go
//! `getFunction` it was read from.

use tidb_datatype::{EvalType, FieldType, FieldTypeCode};
use tidb_proto::tipb::{Expr, ExprType};

/// The TiPB signature enum, re-exported so a caller that reads a resolved
/// signature off the catalog does not need its own TiPB dependency: the
/// catalog is the only place a signature is chosen.
pub use tidb_proto::tipb::ScalarFuncSig;

use crate::pb_predicate::int_field_type;

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;
/// Go `mysql.UnsignedFlag`.
const UNSIGNED_FLAG: u32 = 1 << 5;
/// Go `mysql.BinaryFlag`.
const BINARY_FLAG: u32 = 1 << 7;
/// Go `mysql.MaxIntWidth`.
const MAX_INT_WIDTH: i32 = 20;
/// Go `mysql.MaxRealWidth`.
const MAX_REAL_WIDTH: i32 = 23;
/// Go `types.UnspecifiedLength`.
const UNSPECIFIED_LENGTH: i32 = -1;
/// The `flen` Go's `newReturnFieldTypeForBaseBuiltinFunc` gives an `ETDecimal`
/// return type before any per-family adjustment.
const DECIMAL_RETURN_FLEN: i32 = 11;

/// One argument slot of a [`BuiltinSignature`]'s selector.
///
/// `None` in either field matches anything, which is how a monomorphic family
/// and a signedness-blind family are spelled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArgPattern {
    /// The argument's own evaluation type, before any implicit cast.
    pub eval: Option<EvalType>,
    /// Whether the argument's declared type carries `UNSIGNED`.
    pub unsigned: Option<bool>,
}

impl ArgPattern {
    /// A slot that matches any argument: Go's `getFunction` families that pick
    /// one signature whatever came in.
    const ANY: Self = Self {
        eval: None,
        unsigned: None,
    };

    /// A slot selected on the argument's evaluation type alone.
    const fn eval(eval: EvalType) -> Self {
        Self {
            eval: Some(eval),
            unsigned: None,
        }
    }

    /// A slot selected on evaluation type and signedness, as `MOD`'s integer
    /// signatures are.
    const fn int(unsigned: bool) -> Self {
        Self {
            eval: Some(EvalType::Int),
            unsigned: Some(unsigned),
        }
    }

    fn matches(self, argument: &PbScalar) -> bool {
        self.eval.is_none_or(|eval| eval == argument.eval_type())
            && self
                .unsigned
                .is_none_or(|unsigned| unsigned == argument.is_unsigned())
    }
}

/// One row of the catalog: a name, the argument pattern it is chosen for, the
/// signature Go resolves for it, and TiKV's verdict on that signature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuiltinSignature {
    /// The lowercase Go function name (`ScalarFunction.FuncName.L`).
    pub name: &'static str,
    /// The incoming-argument pattern this row is selected for. Its length is
    /// the call's arity, so an overloaded arity is separate rows.
    pub selector: &'static [ArgPattern],
    /// What Go's `newBaseBuiltinFuncWithTp` coerces each argument to. Same
    /// length as `selector`.
    pub arg_types: &'static [EvalType],
    /// The signature's result evaluation type.
    pub ret: EvalType,
    /// The `tipb.ScalarFuncSig` Go's `setPbCode` records.
    pub sig: ScalarFuncSig,
    /// Whether Go's `getFunction` copies the first argument's `UNSIGNED` flag
    /// onto the result type (`MOD` and `ROUND` do; the trigonometric family
    /// does not).
    pub ret_unsigned_from_first_arg: bool,
}

impl BuiltinSignature {
    /// The TiPB field type of this signature's result, which is Go's
    /// `newReturnFieldTypeForBaseBuiltinFunc` for `ret`.
    fn return_field_type(self, unsigned: bool) -> tidb_proto::tipb::FieldType {
        let flag = BINARY_FLAG | if unsigned { UNSIGNED_FLAG } else { 0 };
        match self.ret {
            EvalType::Int => int_field_type(
                FieldTypeCode::LongLong.mysql_type().into(),
                flag,
                MAX_INT_WIDTH,
                0,
            ),
            EvalType::Real => int_field_type(
                FieldTypeCode::Double.mysql_type().into(),
                flag,
                MAX_REAL_WIDTH,
                UNSPECIFIED_LENGTH,
            ),
            EvalType::Decimal => int_field_type(
                FieldTypeCode::NewDecimal.mysql_type().into(),
                flag,
                DECIMAL_RETURN_FLEN,
                0,
            ),
            // Every other return family needs the charset and collation
            // resolution this tier does not build; such a row is not in the
            // table, so this arm is unreachable through `resolve`.
            _ => int_field_type(0, flag, UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH),
        }
    }
}

/// The catalog, in Go's own per-family order.
///
/// Only signatures `infer_pushdown.go`'s `scalarExprSupportedByTiKV` admits are
/// listed: a family whose name that switch omits, and an overload the switch
/// excludes by signature (`ROUND` with a `frac` argument -- Go's comment cites
/// MySQL's special rounding behaviour), is absent rather than flagged, so
/// "not in the catalog" and "TiKV refuses it" are one answer.
pub const CATALOG: &[BuiltinSignature] = &[
    // `builtin_arithmetic.go` `arithmeticModFunctionClass.getFunction`: Real
    // wins over Decimal, Decimal over Int, and the integer case then splits
    // four ways on the two arguments' UNSIGNED flags.
    signature(
        "mod",
        &[ArgPattern::eval(EvalType::Real), ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::ModReal,
        true,
    ),
    signature(
        "mod",
        &[ArgPattern::ANY, ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::ModReal,
        true,
    ),
    signature(
        "mod",
        &[ArgPattern::eval(EvalType::Decimal), ArgPattern::ANY],
        &[EvalType::Decimal, EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::ModDecimal,
        true,
    ),
    signature(
        "mod",
        &[ArgPattern::ANY, ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal, EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::ModDecimal,
        true,
    ),
    signature(
        "mod",
        &[ArgPattern::int(true), ArgPattern::int(true)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::ModIntUnsignedUnsigned,
        true,
    ),
    signature(
        "mod",
        &[ArgPattern::int(true), ArgPattern::int(false)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::ModIntUnsignedSigned,
        true,
    ),
    signature(
        "mod",
        &[ArgPattern::int(false), ArgPattern::int(true)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::ModIntSignedUnsigned,
        true,
    ),
    signature(
        "mod",
        &[ArgPattern::int(false), ArgPattern::int(false)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::ModIntSignedSigned,
        true,
    ),
    // `builtin_math.go` `roundFunctionClass.getFunction`: Int and Decimal keep
    // their domain, everything else becomes Real. The two-argument `ROUND(x,
    // frac)` overload is deliberately absent -- `scalarExprSupportedByTiKV`
    // admits only the three no-frac signatures.
    signature(
        "round",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::RoundInt,
        true,
    ),
    signature(
        "round",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::RoundDec,
        true,
    ),
    signature(
        "round",
        &[ArgPattern::ANY],
        &[EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::RoundReal,
        true,
    ),
    // `builtin_math.go`, the `ETReal -> ETReal` trigonometric family, each of
    // which resolves one signature whatever came in.
    signature(
        "acos",
        &[ArgPattern::ANY],
        &[EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Acos,
        false,
    ),
    signature(
        "asin",
        &[ArgPattern::ANY],
        &[EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Asin,
        false,
    ),
    // `atanFunctionClass.getFunction` branches on arity, not on type.
    signature(
        "atan",
        &[ArgPattern::ANY],
        &[EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Atan1Arg,
        false,
    ),
    signature(
        "atan",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Atan2Args,
        false,
    ),
    signature(
        "atan2",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Atan2Args,
        false,
    ),
    signature(
        "cos",
        &[ArgPattern::ANY],
        &[EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Cos,
        false,
    ),
    signature(
        "cot",
        &[ArgPattern::ANY],
        &[EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Cot,
        false,
    ),
    signature(
        "sin",
        &[ArgPattern::ANY],
        &[EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Sin,
        false,
    ),
    // `piFunctionClass.getFunction`: no arguments, and `PI()` is a constant on
    // both sides of the wire.
    signature("pi", &[], &[], EvalType::Real, ScalarFuncSig::Pi, false),
    // `powFunctionClass.getFunction`. MySQL spells the same function `POW` and
    // `POWER`; TiDB registers both names on one class, so both are rows.
    signature(
        "pow",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Pow,
        false,
    ),
    signature(
        "power",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Pow,
        false,
    ),
];

/// A `const fn` row constructor, so the table above reads as data.
const fn signature(
    name: &'static str,
    selector: &'static [ArgPattern],
    arg_types: &'static [EvalType],
    ret: EvalType,
    sig: ScalarFuncSig,
    ret_unsigned_from_first_arg: bool,
) -> BuiltinSignature {
    BuiltinSignature {
        name,
        selector,
        arg_types,
        ret,
        sig,
        ret_unsigned_from_first_arg,
    }
}

/// The implicit cast Go's `newBaseBuiltinFuncWithTp` inserts to make an
/// argument of evaluation type `from` readable as `to`.
///
/// `None` means no cast is needed, which is what `WrapWithCastAsReal` and
/// friends do when the argument already evaluates as the target type. A source
/// family with no row here is one this tier refuses rather than guesses at.
const fn cast_signature(from: EvalType, to: EvalType) -> Option<Option<ScalarFuncSig>> {
    if matches!(
        (from, to),
        (EvalType::Int, EvalType::Int) | (EvalType::Real, EvalType::Real)
    ) {
        // Go returns the argument untouched, so no node is added at all.
        return Some(None);
    }
    Some(Some(match (from, to) {
        (EvalType::Real, EvalType::Int) => ScalarFuncSig::CastRealAsInt,
        (EvalType::Int, EvalType::Real) => ScalarFuncSig::CastIntAsReal,
        (EvalType::Decimal, EvalType::Int) => ScalarFuncSig::CastDecimalAsInt,
        (EvalType::Decimal, EvalType::Real) => ScalarFuncSig::CastDecimalAsReal,
        (EvalType::String, EvalType::Int) => ScalarFuncSig::CastStringAsInt,
        (EvalType::String, EvalType::Real) => ScalarFuncSig::CastStringAsReal,
        _ => return None,
    }))
}

/// One node of a described builtin call: the description a lowering reads,
/// carrying no evaluation behaviour of its own.
#[derive(Clone, Debug, PartialEq)]
pub enum PbScalar {
    /// A column of the scanned table, by zero-based scan output offset.
    Column {
        /// Zero-based offset in the scan's output row.
        offset: u32,
        /// The column's declared type.
        field_type: FieldType,
    },
    /// A signed integer constant, already folded.
    IntLiteral(i64),
    /// A resolved builtin call.
    Call {
        /// The catalog row [`resolve`] chose.
        signature: &'static BuiltinSignature,
        /// The call's arguments, in source order.
        args: Vec<PbScalar>,
    },
}

impl PbScalar {
    /// The node's evaluation type, which is what the selector matches on.
    #[must_use]
    pub fn eval_type(&self) -> EvalType {
        match self {
            Self::Column { field_type, .. } => field_type.eval_type(),
            Self::IntLiteral(_) => EvalType::Int,
            Self::Call { signature, .. } => signature.ret,
        }
    }

    /// Whether the node's declared type carries `UNSIGNED`.
    ///
    /// A folded integer literal is signed however large it is: the parser
    /// produces an unsigned constant only for a literal above `i64::MAX`,
    /// which does not fit [`PbScalar::IntLiteral`] and is therefore not
    /// describable here at all.
    #[must_use]
    pub fn is_unsigned(&self) -> bool {
        match self {
            Self::Column { field_type, .. } => field_type.is_unsigned(),
            Self::IntLiteral(_) => false,
            Self::Call { signature, args } => {
                signature.ret_unsigned_from_first_arg
                    && args.first().is_some_and(PbScalar::is_unsigned)
            }
        }
    }
}

/// The catalog row for a call of `name` over `args`, when TiKV evaluates it.
///
/// This is Go's per-family `getFunction` signature choice and
/// `scalarExprSupportedByTiKV`'s verdict on it, answered together: `None`
/// means either that no signature exists for this name and argument shape or
/// that TiKV does not evaluate the one that does. A caller must not
/// distinguish the two, because Go does not either -- `scalarFuncToPBExpr`
/// returns nil for both.
#[must_use]
pub fn resolve(name: &str, args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    CATALOG.iter().find(|candidate| {
        candidate.name == name
            && candidate.selector.len() == args.len()
            && candidate
                .selector
                .iter()
                .zip(args)
                .all(|(pattern, argument)| pattern.matches(argument))
    })
}

/// Describes a call of `name` over `args`, when the catalog resolves one.
#[must_use]
pub fn build_call(name: &str, args: Vec<PbScalar>) -> Option<PbScalar> {
    let signature = resolve(name, &args)?;
    Some(PbScalar::Call { signature, args })
}

/// The scan descriptor's own declaration of one output column: the four fields
/// Go's `ToPBFieldType` copies for a numeric or temporal column.
///
/// A lowering passes these rather than trusting the description's copy, so the
/// `ColumnRef` leaf carries the type the *coprocessor was told the scan
/// produces*. [`to_pb`] refuses when the two disagree in a way that would have
/// changed the resolved signature, because a signature chosen from one type and
/// sent against another is exactly the shape that returns wrong rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnDescriptor {
    /// The MySQL type byte.
    pub tp: i32,
    /// The column's flags, whose `UNSIGNED` bit selects a `MOD` signature.
    pub flag: u32,
    /// Display width.
    pub flen: i32,
    /// Scale.
    pub decimal: i32,
}

/// Lowers a described call into the TiPB expression Go's `ExprToPB` builds for
/// it, or `None` when a leaf is one this tier will not encode.
///
/// `columns` resolves a scan output offset into the scan descriptor's own
/// declaration of that column; an offset it does not know refuses the whole
/// condition, as does a column whose declared type would not have produced the
/// signature already resolved.
///
/// The implicit casts are inserted here rather than recorded in the
/// description, because they are a property of the chosen signature and the
/// catalog is the only thing that knows the signature.
#[must_use]
pub fn to_pb(
    scalar: &PbScalar,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
) -> Option<Expr> {
    match scalar {
        PbScalar::Column { offset, field_type } => {
            let declared = columns(*offset)?;
            let code = FieldTypeCode::from_mysql_type(u8::try_from(declared.tp).ok()?);
            let declared_type = FieldType::new(code).with_flags(declared.flag);
            // The signature was chosen from the description's type; sending it
            // against a differently-typed column would be a different function.
            if declared_type.eval_type() != field_type.eval_type()
                || declared_type.is_unsigned() != field_type.is_unsigned()
            {
                return None;
            }
            if declared_type.has_charset() {
                // A real collation id is what TiKV compares strings with, and
                // resolving one is a separate unit; guessing would change an
                // answer.
                return None;
            }
            Some(leaf(
                ExprType::ColumnRef,
                encode_signed(i64::from(*offset)),
                int_field_type(declared.tp, declared.flag, declared.flen, declared.decimal),
            ))
        }
        PbScalar::IntLiteral(value) => Some(leaf(
            ExprType::Int64,
            encode_signed(*value),
            int_field_type(
                FieldTypeCode::LongLong.mysql_type().into(),
                NOT_NULL_FLAG | BINARY_FLAG,
                i32::try_from(value.to_string().len()).expect("an i64 display width fits i32"),
                0,
            ),
        )),
        PbScalar::Call { signature, args } => {
            let mut children = Vec::with_capacity(args.len());
            for (argument, required) in args.iter().zip(signature.arg_types) {
                children.push(coerced_to_pb(argument, *required, columns)?);
            }
            Some(Expr {
                tp: Some(ExprType::ScalarFunc as i32),
                val: None,
                children,
                sig: Some(signature.sig as i32),
                field_type: Some(signature.return_field_type(scalar.is_unsigned())),
                has_distinct: Some(false),
            })
        }
    }
}

/// One argument, wrapped in the implicit cast the signature's slot needs.
fn coerced_to_pb(
    argument: &PbScalar,
    required: EvalType,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
) -> Option<Expr> {
    let cast = cast_signature(argument.eval_type(), required)?;
    let lowered = to_pb(argument, columns)?;
    let Some(cast) = cast else {
        return Some(lowered);
    };
    // A constant in a slot that needs a cast is refused rather than cast: Go
    // folds `CAST(<const>)` at plan time (`foldConstant` over a deterministic
    // function of constants) and sends the *folded* literal, whose TiPB
    // encoding is the target family's and not the source's.
    if matches!(argument, PbScalar::IntLiteral(_)) {
        return None;
    }
    let field_type = match required {
        EvalType::Int => int_field_type(
            FieldTypeCode::LongLong.mysql_type().into(),
            cast_flags(argument),
            MAX_INT_WIDTH,
            0,
        ),
        EvalType::Real => int_field_type(
            FieldTypeCode::Double.mysql_type().into(),
            cast_flags(argument),
            MAX_REAL_WIDTH,
            UNSPECIFIED_LENGTH,
        ),
        EvalType::Decimal => int_field_type(
            FieldTypeCode::NewDecimal.mysql_type().into(),
            cast_flags(argument),
            DECIMAL_RETURN_FLEN,
            0,
        ),
        _ => return None,
    };
    Some(Expr {
        tp: Some(ExprType::ScalarFunc as i32),
        val: None,
        children: vec![lowered],
        sig: Some(cast as i32),
        field_type: Some(field_type),
        has_distinct: Some(false),
    })
}

/// The flags `WrapWithCastAsReal`/`WrapWithCastAsInt` copy onto the cast's own
/// result type: the source's `UNSIGNED` and `NOT NULL` bits, plus the binary
/// charset flag every numeric type carries.
fn cast_flags(argument: &PbScalar) -> u32 {
    let mut flags = BINARY_FLAG;
    if argument.is_unsigned() {
        flags |= UNSIGNED_FLAG;
    }
    if let PbScalar::Column { field_type, .. } = argument {
        if field_type.has_flag(NOT_NULL_FLAG) {
            flags |= NOT_NULL_FLAG;
        }
    }
    flags
}

fn leaf(tp: ExprType, val: Vec<u8>, field_type: tidb_proto::tipb::FieldType) -> Expr {
    Expr {
        tp: Some(tp as i32),
        val: Some(val),
        children: Vec::new(),
        // Upstream `Expr.sig` is gogoproto nullable=false.
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: Some(field_type),
        has_distinct: Some(false),
    }
}

fn encode_signed(value: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(8);
    tidb_codec::encode_int(&mut encoded, value);
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(code: FieldTypeCode) -> PbScalar {
        PbScalar::Column {
            offset: 0,
            field_type: FieldType::new(code),
        }
    }

    fn unsigned_column(code: FieldTypeCode) -> PbScalar {
        PbScalar::Column {
            offset: 0,
            field_type: FieldType::new(code).with_unsigned(true),
        }
    }

    /// A descriptor provider that agrees with whatever the description says,
    /// which is the case a served scan produces; the disagreement case has its
    /// own test.
    fn descriptors(scalar: &PbScalar) -> impl Fn(u32) -> Option<ColumnDescriptor> + use<> {
        let mut declared = Vec::new();
        collect(scalar, &mut declared);
        move |offset| {
            declared
                .iter()
                .find(|(at, _)| *at == offset)
                .map(|(_, descriptor)| *descriptor)
        }
    }

    fn collect(scalar: &PbScalar, out: &mut Vec<(u32, ColumnDescriptor)>) {
        match scalar {
            PbScalar::Column { offset, field_type } => out.push((
                *offset,
                ColumnDescriptor {
                    tp: field_type.code().mysql_type().into(),
                    flag: field_type.flags(),
                    flen: i32::try_from(field_type.flen()).unwrap_or(UNSPECIFIED_LENGTH),
                    decimal: i32::try_from(field_type.decimal()).unwrap_or(UNSPECIFIED_LENGTH),
                },
            )),
            PbScalar::IntLiteral(_) => {}
            PbScalar::Call { args, .. } => args.iter().for_each(|arg| collect(arg, out)),
        }
    }

    fn lower(scalar: &PbScalar) -> Option<Expr> {
        to_pb(scalar, &descriptors(scalar))
    }

    /// `MOD` picks its signature from both arguments, Real before Decimal
    /// before Int, and the integer case splits on the two UNSIGNED flags --
    /// Go `arithmeticModFunctionClass.getFunction`, read as a table.
    #[test]
    fn mod_resolves_the_signature_gos_switch_would() {
        let cases: [(PbScalar, PbScalar, ScalarFuncSig); 7] = [
            (
                column(FieldTypeCode::Double),
                column(FieldTypeCode::LongLong),
                ScalarFuncSig::ModReal,
            ),
            (
                column(FieldTypeCode::LongLong),
                column(FieldTypeCode::Double),
                ScalarFuncSig::ModReal,
            ),
            (
                column(FieldTypeCode::NewDecimal),
                column(FieldTypeCode::LongLong),
                ScalarFuncSig::ModDecimal,
            ),
            (
                column(FieldTypeCode::LongLong),
                column(FieldTypeCode::NewDecimal),
                ScalarFuncSig::ModDecimal,
            ),
            (
                column(FieldTypeCode::LongLong),
                PbScalar::IntLiteral(7),
                ScalarFuncSig::ModIntSignedSigned,
            ),
            (
                unsigned_column(FieldTypeCode::LongLong),
                PbScalar::IntLiteral(7),
                ScalarFuncSig::ModIntUnsignedSigned,
            ),
            (
                unsigned_column(FieldTypeCode::LongLong),
                unsigned_column(FieldTypeCode::Long),
                ScalarFuncSig::ModIntUnsignedUnsigned,
            ),
        ];
        for (left, right, expected) in cases {
            let resolved = resolve("mod", &[left.clone(), right.clone()])
                .unwrap_or_else(|| panic!("mod({left:?}, {right:?}) resolves"));
            assert_eq!(resolved.sig, expected, "mod({left:?}, {right:?})");
        }
        // Decimal on both sides is still the Decimal signature, and Real beats
        // a Decimal partner.
        assert_eq!(
            resolve(
                "mod",
                &[
                    column(FieldTypeCode::NewDecimal),
                    column(FieldTypeCode::Double)
                ]
            )
            .unwrap()
            .sig,
            ScalarFuncSig::ModReal
        );
    }

    /// `ROUND` keeps an integer and a decimal argument in their own domain and
    /// makes everything else Real -- and the `frac` overload is not pushable
    /// at all, so it must not resolve.
    #[test]
    fn round_keeps_its_argument_domain_and_refuses_the_frac_overload() {
        for (argument, expected) in [
            (column(FieldTypeCode::LongLong), ScalarFuncSig::RoundInt),
            (column(FieldTypeCode::NewDecimal), ScalarFuncSig::RoundDec),
            (column(FieldTypeCode::Double), ScalarFuncSig::RoundReal),
            (column(FieldTypeCode::VarString), ScalarFuncSig::RoundReal),
        ] {
            assert_eq!(resolve("round", &[argument]).unwrap().sig, expected);
        }
        assert!(
            resolve(
                "round",
                &[column(FieldTypeCode::Double), PbScalar::IntLiteral(2)]
            )
            .is_none(),
            "ROUND with a frac argument is one of the signatures \
             scalarExprSupportedByTiKV excludes"
        );
    }

    /// A name the TiKV switch omits is simply absent, which is the same answer
    /// as a refusal by design.
    #[test]
    fn a_name_tikv_does_not_evaluate_is_not_in_the_catalog() {
        for name in ["tan", "truncate", "inet_aton", "str_to_date", "nonesuch"] {
            assert!(
                resolve(name, &[column(FieldTypeCode::LongLong)]).is_none(),
                "{name} is not a TiKV-pushable signature"
            );
        }
    }

    /// `ATAN` branches on arity alone, and `ATAN2` is the two-argument name.
    #[test]
    fn atan_resolves_by_arity() {
        let real = || column(FieldTypeCode::Double);
        assert_eq!(
            resolve("atan", &[real()]).unwrap().sig,
            ScalarFuncSig::Atan1Arg
        );
        assert_eq!(
            resolve("atan", &[real(), real()]).unwrap().sig,
            ScalarFuncSig::Atan2Args
        );
        assert_eq!(
            resolve("atan2", &[real(), real()]).unwrap().sig,
            ScalarFuncSig::Atan2Args
        );
        assert!(resolve("atan2", &[real()]).is_none());
    }

    /// An integer argument in a Real slot gets the `CastIntAsReal` node Go's
    /// `WrapWithCastAsReal` inserts; an argument already Real gets none.
    #[test]
    fn a_real_slot_wraps_an_integer_argument_in_the_cast_go_inserts() {
        let call = build_call("sin", vec![column(FieldTypeCode::LongLong)]).unwrap();
        let pb = lower(&call).unwrap();
        assert_eq!(pb.sig, Some(ScalarFuncSig::Sin as i32));
        assert_eq!(pb.children.len(), 1);
        assert_eq!(
            pb.children[0].sig,
            Some(ScalarFuncSig::CastIntAsReal as i32),
            "the integer column is cast to real, as newBaseBuiltinFuncWithTp does"
        );
        assert_eq!(pb.children[0].children.len(), 1);
        assert_eq!(
            pb.children[0].children[0].tp,
            Some(ExprType::ColumnRef as i32)
        );

        let already_real = build_call("sin", vec![column(FieldTypeCode::Double)]).unwrap();
        let pb = lower(&already_real).unwrap();
        assert_eq!(
            pb.children[0].tp,
            Some(ExprType::ColumnRef as i32),
            "an argument that already evaluates as Real is passed through uncast"
        );
    }

    /// The cast copies the source column's UNSIGNED bit, which is the only
    /// place TiKV learns the value's signedness from.
    #[test]
    fn the_inserted_cast_carries_the_columns_signedness() {
        let call = build_call("sin", vec![unsigned_column(FieldTypeCode::LongLong)]).unwrap();
        let pb = lower(&call).unwrap();
        let flags = pb.children[0].field_type.as_ref().unwrap().flag.unwrap();
        assert!(flags & UNSIGNED_FLAG != 0);
    }

    /// A constant is refused in a slot that would need a cast, because Go
    /// folds the cast away and sends a literal of the target family.
    #[test]
    fn a_constant_that_would_need_a_cast_is_refused() {
        let call = build_call(
            "atan2",
            vec![column(FieldTypeCode::Double), PbScalar::IntLiteral(1)],
        )
        .unwrap();
        assert!(
            lower(&call).is_none(),
            "Go sends the folded Float64 constant, not CastIntAsReal(1)"
        );
        // The same constant in an Int slot needs no cast and lowers.
        let call = build_call(
            "mod",
            vec![column(FieldTypeCode::LongLong), PbScalar::IntLiteral(7)],
        )
        .unwrap();
        assert!(lower(&call).is_some());
    }

    /// A column whose collation TiKV compares with is refused rather than
    /// guessed at.
    #[test]
    fn a_column_with_a_real_collation_is_refused_by_the_lowering() {
        let call = build_call("round", vec![column(FieldTypeCode::VarString)]).unwrap();
        assert!(
            lower(&call).is_none(),
            "a VARCHAR leaf needs the collation id resolution this tier lacks"
        );
    }

    /// `MOD` and `ROUND` copy the first argument's UNSIGNED flag onto the
    /// result; the trigonometric family does not.
    #[test]
    fn only_the_families_go_flags_propagate_unsignedness() {
        let unsigned_mod = build_call(
            "mod",
            vec![
                unsigned_column(FieldTypeCode::LongLong),
                PbScalar::IntLiteral(7),
            ],
        )
        .unwrap();
        assert!(unsigned_mod.is_unsigned());
        let sin = build_call("sin", vec![unsigned_column(FieldTypeCode::LongLong)]).unwrap();
        assert!(!sin.is_unsigned());
    }

    /// Every row of the table is well-formed: the selector and the coerced
    /// argument list agree in length, and the return family is one
    /// `return_field_type` builds faithfully.
    #[test]
    fn every_catalog_row_is_well_formed() {
        for row in CATALOG {
            assert_eq!(
                row.selector.len(),
                row.arg_types.len(),
                "{} selector and arg_types describe the same slots",
                row.name
            );
            assert!(
                matches!(row.ret, EvalType::Int | EvalType::Real | EvalType::Decimal),
                "{}: the return family needs a TiPB field type this tier builds",
                row.name
            );
            assert_ne!(row.sig, ScalarFuncSig::Unspecified, "{}", row.name);
        }
    }
}
