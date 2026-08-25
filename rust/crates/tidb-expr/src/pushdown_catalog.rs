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
//! collation (every string, `ENUM`, `SET` and JSON column) and constant
//! families other than the numeric literals encoded below.
//!
//! A refusal costs network only. The scan source applies every pushed
//! conjunct to every row it emits regardless
//! (`tidb_executor::predicate_pushdown`), so a conjunct the store does not filter
//! is still filtered locally. A conjunct lowered *wrongly* would drop a row
//! the query selects, and no local pass can put back a row that never crossed
//! the wire -- which is why every row of the table below cites the Go
//! `getFunction` it was read from.

use tidb_datatype::{collation_to_proto, Datum, Decimal, EvalType, FieldType, FieldTypeCode};
use tidb_proto::tipb::{Expr, ExprType};

use crate::expression::Expression;

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
/// The `flen` `convFunctionClass.getFunction` stamps on `CONV`'s result.
const CONV_RETURN_FLEN: i32 = 64;
/// Go `charset.CharsetBin` / `charset.CollationBin`.
const BINARY_CHARSET: &str = "binary";
const BINARY_COLLATION: &str = "binary";

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
    /// Whether the argument is a *binary* string -- Go `types.IsBinaryStr`,
    /// which is the whole selector of the string family's two spellings:
    /// `UpperUTF8` versus `Upper`, `CharLengthUTF8` versus `CharLength`,
    /// `Substring3ArgsUTF8` versus `Substring3Args`. Reading this from the
    /// argument's *collation* rather than from its MySQL type is the point:
    /// `VARCHAR(10) COLLATE binary` and `VARBINARY(10)` are the same answer to
    /// Go and must be the same answer here.
    pub binary_string: Option<bool>,
}

impl ArgPattern {
    /// A slot that matches any argument: Go's `getFunction` families that pick
    /// one signature whatever came in.
    const ANY: Self = Self {
        eval: None,
        unsigned: None,
        binary_string: None,
    };

    /// A slot selected on the argument's evaluation type alone.
    const fn eval(eval: EvalType) -> Self {
        Self {
            eval: Some(eval),
            unsigned: None,
            binary_string: None,
        }
    }

    /// A slot selected on evaluation type and signedness, as `MOD`'s integer
    /// signatures are.
    const fn int(unsigned: bool) -> Self {
        Self {
            eval: Some(EvalType::Int),
            unsigned: Some(unsigned),
            binary_string: None,
        }
    }

    /// A string slot selected on `types.IsBinaryStr`, as every two-spelling
    /// row of the string family is.
    const fn string(binary: bool) -> Self {
        Self {
            eval: Some(EvalType::String),
            unsigned: None,
            binary_string: Some(binary),
        }
    }

    /// A string slot of either spelling: `CONV` reads its argument as bytes
    /// whatever its collation, so it has one signature and one row.
    const fn any_string() -> Self {
        Self {
            eval: Some(EvalType::String),
            unsigned: None,
            binary_string: None,
        }
    }

    fn matches(self, argument: &PbScalar) -> bool {
        self.eval.is_none_or(|eval| eval == argument.eval_type())
            && self
                .unsigned
                .is_none_or(|unsigned| unsigned == argument.is_unsigned())
            && self
                .binary_string
                .is_none_or(|binary| binary == argument.is_binary_string())
    }
}

/// Where a signature's result charset and collation come from -- Go's
/// `deriveCollation` (`pkg/expression/collation.go`) reduced to the three
/// answers the pushable families actually take.
///
/// The collation on a result type is not decoration: TiKV compares and folds
/// case with it, so a wrong one returns wrong rows silently. Every variant
/// below therefore names the Go branch it is, and a family whose branch is not
/// one of these three has no row in the catalog at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetCollation {
    /// `binary`/`binary`: `deriveCollation`'s fallthrough for a non-`ETString`
    /// return, which is every numeric signature and `CHAR_LENGTH`.
    Numeric,
    /// The first argument's own charset and collation, with the first
    /// argument's `flen` and `SetBinFlagOrBinStr`: `deriveCollation`'s
    /// `ast.Upper`/`ast.Lower` (through `args...`, a single argument) and
    /// `ast.Substr`/`ast.Substring`/`ast.Mid` (through `args[0]`), followed by
    /// the `bf.tp.SetFlen(argType.GetFlen())` each `getFunction` performs.
    ///
    /// Both reduce to `inferCollation` over exactly ONE string expression,
    /// whose answer is that expression's own charset and collation -- which is
    /// why this family needs none of the coercibility aggregation a
    /// multi-operand derivation does, and is the reason it could be widened to
    /// without the `Coercibility`/`Repertoire` seam.
    FirstArgString,
    /// `@@character_set_connection`/`@@collation_connection` with `flen` 64:
    /// `convFunctionClass.getFunction`, which sets them itself rather than
    /// taking `deriveCollation`'s answer.
    ConnectionString,
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
    /// Where the result's charset and collation come from.
    pub ret_collation: RetCollation,
}

impl BuiltinSignature {
    /// The TiPB field type of this signature's result: Go's
    /// `newReturnFieldTypeForBaseBuiltinFunc` for `ret`, plus the per-family
    /// adjustment the family's own `getFunction` makes to it afterwards.
    ///
    /// `children` are the arguments already lowered, so a
    /// [`RetCollation::FirstArgString`] row reads the very field type that
    /// crosses the wire rather than a second, separately-derived copy of it.
    fn return_field_type(
        self,
        unsigned: bool,
        children: &[Expr],
    ) -> Option<tidb_proto::tipb::FieldType> {
        if self.ret == EvalType::String {
            return self.string_return_field_type(children);
        }
        let flag = BINARY_FLAG | if unsigned { UNSIGNED_FLAG } else { 0 };
        Some(match self.ret {
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
            // Every other return family (`ETDatetime`, `ETDuration`,
            // `ETJson`) needs a field type this tier does not build; such a
            // row is not in the table, so this arm is unreachable through
            // `resolve` and refuses rather than guesses.
            _ => return None,
        })
    }

    /// The result field type of an `ETString` signature: Go's
    /// `newReturnFieldTypeForBaseBuiltinFunc`'s `types.ETString` arm --
    /// `VAR_STRING`, `flen`/`decimal` unspecified, charset and collation from
    /// the derived `ExprCollation` -- with the family's own follow-up.
    fn string_return_field_type(self, children: &[Expr]) -> Option<tidb_proto::tipb::FieldType> {
        match self.ret_collation {
            // Unreachable through `resolve`: a string-returning row always
            // declares where its collation comes from, and
            // `every_catalog_row_is_well_formed` pins that.
            RetCollation::Numeric => None,
            RetCollation::ConnectionString => {
                let (charset, collation) = crate::collation_derive::connection_charset_info();
                Some(pb_field_type(
                    FieldTypeCode::VarString.mysql_type().into(),
                    0,
                    CONV_RETURN_FLEN,
                    UNSPECIFIED_LENGTH,
                    charset,
                    collation,
                ))
            }
            RetCollation::FirstArgString => {
                let argument = children.first()?.field_type.as_ref()?;
                let argument_flag = argument.flag.unwrap_or_default();
                let collation =
                    tidb_datatype::proto_to_collation(argument.collate.unwrap_or_default());
                let charset = argument.charset.clone()?;
                // Go `SetBinFlagOrBinStr(argType, bf.tp)`. Only `ETString`
                // arguments reach here (a non-string one would need the cast
                // into `ETString` this tier refuses), so `IsNonBinaryStr` is
                // the negation of `IsBinaryStr` and the second branch reduces
                // to the argument's own BINARY flag.
                let (charset, collation, flag) = if collation == BINARY_COLLATION {
                    // `types.SetBinChsClnFlag`.
                    (
                        BINARY_CHARSET.to_owned(),
                        BINARY_COLLATION.to_owned(),
                        BINARY_FLAG,
                    )
                } else {
                    (charset, collation, argument_flag & BINARY_FLAG)
                };
                Some(pb_field_type(
                    FieldTypeCode::VarString.mysql_type().into(),
                    flag,
                    argument.flen.unwrap_or(UNSPECIFIED_LENGTH),
                    UNSPECIFIED_LENGTH,
                    &charset,
                    &collation,
                ))
            }
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
    // `arithmeticPlusFunctionClass.getFunction` and
    // `arithmeticMinusFunctionClass.getFunction`: a Decimal operand selects
    // the Decimal family and the other argument is wrapped as Decimal.
    signature(
        "plus",
        &[ArgPattern::eval(EvalType::Decimal), ArgPattern::ANY],
        &[EvalType::Decimal, EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::PlusDecimal,
        false,
    ),
    signature(
        "plus",
        &[ArgPattern::ANY, ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal, EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::PlusDecimal,
        false,
    ),
    signature(
        "minus",
        &[ArgPattern::eval(EvalType::Decimal), ArgPattern::ANY],
        &[EvalType::Decimal, EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::MinusDecimal,
        false,
    ),
    signature(
        "minus",
        &[ArgPattern::ANY, ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal, EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::MinusDecimal,
        false,
    ),
    // `builtin_arithmetic.go` `arithmeticMultiplyFunctionClass.getFunction`:
    // two decimal operands stay decimal and use MultiplyDecimal. This is the
    // exact argument family used by TPC-H q6; other multiplication families
    // remain fail-closed until their signedness and cast matrices are ported.
    signature(
        "mul",
        &[
            ArgPattern::eval(EvalType::Decimal),
            ArgPattern::eval(EvalType::Decimal),
        ],
        &[EvalType::Decimal, EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::MultiplyDecimal,
        false,
    ),
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
    // --- The string family. ---
    //
    // Every row below takes an argument that is ALREADY `ETString`: a slot
    // that would need Go's `WrapWithCastAsString` has no row, because that
    // cast's target `FieldType` takes its `flen` from a per-source-type table
    // and its charset from the session, and sending a differently-shaped cast
    // than Go sends would change what TiKV reads. `CHAR_LENGTH(i)` over an
    // integer column is therefore refused here, which costs network only --
    // see the module doc on why a refusal is the safe direction.
    //
    // `builtin_string.go` `charLengthFunctionClass.getFunction`: the return is
    // `ETInt`, so `deriveCollation` never runs for it (`char_length` is not in
    // its switch and the fallthrough is binary/binary for a non-string
    // return).
    string_int_signature(
        "char_length",
        &[ArgPattern::string(true)],
        ScalarFuncSig::CharLength,
    ),
    string_int_signature(
        "char_length",
        &[ArgPattern::string(false)],
        ScalarFuncSig::CharLengthUtf8,
    ),
    // `upperFunctionClass` / `lowerFunctionClass`: one `ETString` argument,
    // `bf.tp.SetFlen(argTp.GetFlen())`, `SetBinFlagOrBinStr(argTp, bf.tp)`,
    // and a collation `deriveCollation`'s `ast.Upper`/`ast.Lower` case reads
    // off that same single argument.
    string_signature(
        "upper",
        &[ArgPattern::string(true)],
        &[EvalType::String],
        ScalarFuncSig::Upper,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "upper",
        &[ArgPattern::string(false)],
        &[EvalType::String],
        ScalarFuncSig::UpperUtf8,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "lower",
        &[ArgPattern::string(true)],
        &[EvalType::String],
        ScalarFuncSig::Lower,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "lower",
        &[ArgPattern::string(false)],
        &[EvalType::String],
        ScalarFuncSig::LowerUtf8,
        RetCollation::FirstArgString,
    ),
    // `substringFunctionClass.getFunction`, whose switch is arity crossed with
    // `types.IsBinaryStr(args[0])`. TiDB registers the one class under three
    // names (`builtin.go`: `ast.Substr`, `ast.Substring`, `ast.Mid`), each
    // with arity 2..3, and `scalarExprSupportedByTiKV` lists all three, so all
    // three are rows -- twelve in total, which is Go's four-way switch times
    // its three names.
    substring_signature("substr", 3, true, ScalarFuncSig::Substring3Args),
    substring_signature("substr", 3, false, ScalarFuncSig::Substring3ArgsUtf8),
    substring_signature("substr", 2, true, ScalarFuncSig::Substring2Args),
    substring_signature("substr", 2, false, ScalarFuncSig::Substring2ArgsUtf8),
    substring_signature("substring", 3, true, ScalarFuncSig::Substring3Args),
    substring_signature("substring", 3, false, ScalarFuncSig::Substring3ArgsUtf8),
    substring_signature("substring", 2, true, ScalarFuncSig::Substring2Args),
    substring_signature("substring", 2, false, ScalarFuncSig::Substring2ArgsUtf8),
    substring_signature("mid", 3, true, ScalarFuncSig::Substring3Args),
    substring_signature("mid", 3, false, ScalarFuncSig::Substring3ArgsUtf8),
    substring_signature("mid", 2, true, ScalarFuncSig::Substring2Args),
    substring_signature("mid", 2, false, ScalarFuncSig::Substring2ArgsUtf8),
    // `builtin_math.go` `convFunctionClass.getFunction`: one signature
    // whatever the argument's collation, with the connection charset and
    // `flen` 64 set on the result by hand.
    //
    // `scalarExprSupportedByTiKV`'s `ast.Conv` case refuses a first argument
    // that is a `CAST` over a hybrid type or a binary literal (Go issue
    // 51877). That shape cannot be built here at all -- `CAST` is not in this
    // catalog -- so the refusal is structural, and
    // `tikv_refuses_what_go_refuses` pins `conv(cast(bt as binary), i, i)`.
    // `builtin_json.go` `jsonMemberOfFunctionClass.getFunction`: both
    // operands are coerced to `ETJson` (`argTps := []types.EvalType{
    // types.ETJson, types.ETJson}`) and the result is `ETInt` with
    // `sig.setPbCode(tipb.ScalarFuncSig_JsonMemberOfSig)`. The candidate
    // argument arrives as any JSON-coercible literal or column -- Go wraps it
    // in the implicit cast `newBaseBuiltinFuncWithTp` inserts -- so its slot
    // matches anything and the lowering asks `coerced_to_pb` for the cast.
    // A JSON operand's TiPB leaf is one this tier does not build, so the
    // call refuses to encode and the scan source filters locally; refusing
    // there costs network only.
    signature(
        "json_memberof",
        &[ArgPattern::ANY, ArgPattern::eval(EvalType::Json)],
        &[EvalType::Json, EvalType::Json],
        EvalType::Int,
        ScalarFuncSig::JsonMemberOfSig,
        false,
    ),
    string_signature(
        "conv",
        &[ArgPattern::any_string(), ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::Int, EvalType::Int],
        ScalarFuncSig::Conv,
        RetCollation::ConnectionString,
    ),
];

/// A `const fn` row constructor for a numeric-returning family, so the table
/// above reads as data. Its result carries `binary`/`binary`, which is
/// `deriveCollation`'s answer for every non-`ETString` return.
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
        ret_collation: RetCollation::Numeric,
    }
}

/// A one-string-argument, integer-returning row: the `CHAR_LENGTH` pair.
const fn string_int_signature(
    name: &'static str,
    selector: &'static [ArgPattern],
    sig: ScalarFuncSig,
) -> BuiltinSignature {
    signature(
        name,
        selector,
        &[EvalType::String],
        EvalType::Int,
        sig,
        false,
    )
}

/// One row of `substringFunctionClass.getFunction`'s arity-by-binaryness
/// switch, under one of its three registered names.
const fn substring_signature(
    name: &'static str,
    arity: usize,
    binary: bool,
    sig: ScalarFuncSig,
) -> BuiltinSignature {
    // `argTps := []ETString, ETInt`, plus a third `ETInt` when the call has
    // three arguments.
    const BINARY_3: &[ArgPattern] = &[ArgPattern::string(true), ArgPattern::ANY, ArgPattern::ANY];
    const UTF8_3: &[ArgPattern] = &[ArgPattern::string(false), ArgPattern::ANY, ArgPattern::ANY];
    const BINARY_2: &[ArgPattern] = &[ArgPattern::string(true), ArgPattern::ANY];
    const UTF8_2: &[ArgPattern] = &[ArgPattern::string(false), ArgPattern::ANY];
    const TYPES_3: &[EvalType] = &[EvalType::String, EvalType::Int, EvalType::Int];
    const TYPES_2: &[EvalType] = &[EvalType::String, EvalType::Int];
    let (selector, arg_types): (&'static [ArgPattern], &'static [EvalType]) = match (arity, binary)
    {
        (3, true) => (BINARY_3, TYPES_3),
        (3, false) => (UTF8_3, TYPES_3),
        (_, true) => (BINARY_2, TYPES_2),
        (_, false) => (UTF8_2, TYPES_2),
    };
    string_signature(name, selector, arg_types, sig, RetCollation::FirstArgString)
}

/// A `const fn` row constructor for a string-returning family, which must say
/// where its result collation comes from. No such row ever propagates
/// `UNSIGNED`: Go's string `getFunction`s do not touch that flag.
const fn string_signature(
    name: &'static str,
    selector: &'static [ArgPattern],
    arg_types: &'static [EvalType],
    sig: ScalarFuncSig,
    ret_collation: RetCollation,
) -> BuiltinSignature {
    BuiltinSignature {
        name,
        selector,
        arg_types,
        ret: EvalType::String,
        sig,
        ret_unsigned_from_first_arg: false,
        ret_collation,
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
        (EvalType::Int, EvalType::Int)
            | (EvalType::Real, EvalType::Real)
            | (EvalType::Decimal, EvalType::Decimal)
            | (EvalType::String, EvalType::String)
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
    /// An exact decimal constant, already folded.
    DecimalLiteral {
        /// Constant value encoded with Go's natural precision and scale.
        value: Decimal,
        /// The planner-inferred literal type carried on the TiPB leaf.
        field_type: FieldType,
    },
    /// A double-precision constant, already folded.
    RealLiteral {
        /// Constant value encoded with TiDB's mem-comparable float codec.
        value: f64,
        /// The planner-inferred literal type carried on the TiPB leaf.
        field_type: FieldType,
    },
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
            Self::DecimalLiteral { .. } => EvalType::Decimal,
            Self::RealLiteral { .. } => EvalType::Real,
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
            Self::DecimalLiteral { field_type, .. } | Self::RealLiteral { field_type, .. } => {
                field_type.is_unsigned()
            }
            Self::Call { signature, args } => {
                signature.ret_unsigned_from_first_arg
                    && args.first().is_some_and(PbScalar::is_unsigned)
            }
        }
    }

    /// Whether the node is a binary string -- Go `types.IsBinaryStr`, the
    /// selector of the string family's two spellings.
    ///
    /// For a call this reads the result collation rule rather than the
    /// argument: `UPPER` over a `VARBINARY` returns binary (Go
    /// `SetBinFlagOrBinStr` -> `SetBinChsClnFlag`), so `UPPER(UPPER(b))`
    /// resolves the binary signature at both levels, while `CONV` always
    /// returns the connection charset and so is never binary.
    #[must_use]
    pub fn is_binary_string(&self) -> bool {
        match self {
            Self::Column { field_type, .. } => field_type.is_binary_string(),
            Self::IntLiteral(_) => false,
            Self::DecimalLiteral { .. } | Self::RealLiteral { .. } => false,
            Self::Call { signature, args } => match signature.ret_collation {
                RetCollation::Numeric | RetCollation::ConnectionString => false,
                RetCollation::FirstArgString => {
                    args.first().is_some_and(PbScalar::is_binary_string)
                }
            },
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

/// Describes one already-resolved executor expression in the same catalog
/// used by scan predicates. Returning `None` is the Rust equivalent of Go's
/// `CanExprsPushDownWithExtraInfo` / `AggFuncToPBExpr` refusal: the aggregate
/// stays wholly in the root task.
#[must_use]
pub fn from_expression(expression: &Expression) -> Option<PbScalar> {
    match expression {
        Expression::Column(column) => Some(PbScalar::Column {
            offset: u32::try_from(column.index).ok()?,
            field_type: column.get_static_type()?.clone(),
        }),
        Expression::Constant(constant) => match &constant.value {
            Datum::Int(value) => Some(PbScalar::IntLiteral(*value)),
            Datum::Decimal(value) => Some(PbScalar::DecimalLiteral {
                value: value.clone(),
                field_type: constant.ret_type.clone()?,
            }),
            Datum::Real(value) | Datum::Float32(value)
                if !(*value == 0.0 && value.is_sign_negative()) =>
            {
                Some(PbScalar::RealLiteral {
                    value: *value,
                    field_type: constant.ret_type.clone()?,
                })
            }
            _ => None,
        },
        Expression::ScalarFunction(function) => {
            let args = function
                .args
                .iter()
                .map(from_expression)
                .collect::<Option<Vec<_>>>()?;
            build_call(function.func_name.lowercase(), args)
        }
        Expression::CorrelatedColumn(_) => None,
    }
}

/// Lowers one executor expression and preserves its already-inferred result
/// field type. The catalog still owns signature choice and implicit casts;
/// the exact type matters for decimal aggregate arguments because TiKV reads
/// their precision and scale from the scalar node.
#[must_use]
pub fn expression_to_pb(
    expression: &Expression,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
) -> Option<Expr> {
    let described = from_expression(expression)?;
    let mut encoded = to_pb(&described, columns)?;
    encoded.field_type = Some(field_type_to_pb(expression.static_type()?)?);
    Some(encoded)
}

/// Go `ToPBFieldType` for a type used by this bounded expression closure.
#[must_use]
pub fn field_type_to_pb(field_type: &FieldType) -> Option<tidb_proto::tipb::FieldType> {
    if !leaf_column_family(field_type.code()) {
        return None;
    }
    Some(pb_field_type(
        i32::from(field_type.code().mysql_type()),
        field_type.flags(),
        i32::try_from(field_type.flen()).ok()?,
        i32::try_from(field_type.decimal()).ok()?,
        field_type.charset_name(),
        field_type.collation_name(),
    ))
}

/// The scan descriptor's own declaration of one output column: the four fields
/// Go's `ToPBFieldType` copies for a numeric or temporal column.
///
/// A lowering passes these rather than trusting the description's copy, so the
/// `ColumnRef` leaf carries the type the *coprocessor was told the scan
/// produces*. [`to_pb`] refuses when the two disagree in a way that would have
/// changed the resolved signature, because a signature chosen from one type and
/// sent against another is exactly the shape that returns wrong rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnDescriptor {
    /// The MySQL type byte.
    pub tp: i32,
    /// The column's flags, whose `UNSIGNED` bit selects a `MOD` signature.
    pub flag: u32,
    /// Display width.
    pub flen: i32,
    /// Scale.
    pub decimal: i32,
    /// The column's charset name -- Go `ft.GetCharset()`.
    pub charset: String,
    /// The column's collation NAME (`utf8mb4_bin`, `binary`, ...) -- Go
    /// `ft.GetCollate()`. This is what TiKV compares and case-folds the
    /// column's values with, and what selects the string family's
    /// binary-versus-UTF-8 spelling, so it is carried verbatim rather than
    /// reduced to a flag. A numeric or temporal column carries `binary`,
    /// exactly as Go's `FieldType` does.
    pub collation: String,
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
            let declared_type = FieldType::new(code)
                .with_flags(declared.flag)
                .with_collation_name(declared.collation.clone());
            // The signature was chosen from the description's type; sending it
            // against a differently-typed column would be a different
            // function. Binary-string-ness is checked alongside the eval type
            // and the sign because it is the third thing a signature is chosen
            // by: `UpperUTF8` sent against a `binary` column is `Upper`'s job.
            if declared_type.eval_type() != field_type.eval_type()
                || declared_type.is_unsigned() != field_type.is_unsigned()
                || declared_type.is_binary_string() != field_type.is_binary_string()
            {
                return None;
            }
            // A column family whose TiPB leaf needs more than the six fields
            // below: `ENUM`/`SET` carry their `elems` list, `BIT` and `JSON`
            // are separately gated by Go's `columnToPBExpr`, and
            // `GEOMETRY`/unspecified are refused there outright.
            if !leaf_column_family(code) {
                return None;
            }
            Some(leaf(
                ExprType::ColumnRef,
                encode_signed(i64::from(*offset)),
                pb_field_type(
                    declared.tp,
                    declared.flag,
                    declared.flen,
                    declared.decimal,
                    &declared.charset,
                    &declared.collation,
                ),
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
        PbScalar::DecimalLiteral { value, field_type } => {
            let mut encoded = Vec::new();
            tidb_codec::encode_decimal_fixed(&mut encoded, value, 0, 0).ok()?;
            Some(leaf(
                ExprType::MysqlDecimal,
                encoded,
                field_type_to_pb(field_type)?,
            ))
        }
        PbScalar::RealLiteral { value, field_type } => {
            let mut encoded = Vec::new();
            tidb_codec::encode_float(&mut encoded, *value);
            Some(leaf(
                ExprType::Float64,
                encoded,
                field_type_to_pb(field_type)?,
            ))
        }
        PbScalar::Call { signature, args } => {
            let mut children = Vec::with_capacity(args.len());
            for (argument, required) in args.iter().zip(signature.arg_types) {
                children.push(coerced_to_pb(argument, *required, columns)?);
            }
            let return_field_type = signature.return_field_type(scalar.is_unsigned(), &children)?;
            Some(Expr {
                tp: Some(ExprType::ScalarFunc as i32),
                val: None,
                children,
                sig: Some(signature.sig as i32),
                field_type: Some(return_field_type),
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
    // Go folds a deterministic cast over a constant and sends the target
    // literal family, not a ScalarFunc cast node.
    if let PbScalar::IntLiteral(value) = argument {
        if required == EvalType::Int {
            return to_pb(argument, columns);
        }
        return match required {
            EvalType::Real => {
                let mut encoded = Vec::new();
                tidb_codec::encode_float(&mut encoded, *value as f64);
                Some(leaf(
                    ExprType::Float64,
                    encoded,
                    int_field_type(
                        FieldTypeCode::Double.mysql_type().into(),
                        BINARY_FLAG | NOT_NULL_FLAG,
                        MAX_REAL_WIDTH,
                        UNSPECIFIED_LENGTH,
                    ),
                ))
            }
            EvalType::Decimal => {
                let decimal = Decimal::from_int(*value);
                let mut encoded = Vec::new();
                tidb_codec::encode_decimal_fixed(&mut encoded, &decimal, 0, 0).ok()?;
                Some(leaf(
                    ExprType::MysqlDecimal,
                    encoded,
                    int_field_type(
                        FieldTypeCode::NewDecimal.mysql_type().into(),
                        BINARY_FLAG | NOT_NULL_FLAG,
                        DECIMAL_RETURN_FLEN,
                        0,
                    ),
                ))
            }
            _ => None,
        };
    }
    let cast = cast_signature(argument.eval_type(), required)?;
    let lowered = to_pb(argument, columns)?;
    let Some(cast) = cast else {
        return Some(lowered);
    };
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

/// Go `ToPBFieldType` in full: the seven fields it copies, with the charset
/// carried verbatim and the collation put through
/// `collate.CollationToProto` -- the negation `RewriteNewCollationIDIfNeeded`
/// applies when new collations are enabled, which is how TiKV is told to use
/// the new collator rather than a byte comparison.
///
/// `elems` is always empty here because no `ENUM`/`SET` leaf reaches this
/// tier; [`leaf_column_family`] is what keeps that true.
fn pb_field_type(
    mysql_type: i32,
    flags: u32,
    flen: i32,
    decimal: i32,
    charset: &str,
    collation: &str,
) -> tidb_proto::tipb::FieldType {
    tidb_proto::tipb::FieldType {
        tp: Some(mysql_type),
        flag: Some(flags),
        flen: Some(flen),
        decimal: Some(decimal),
        collate: Some(collation_to_proto(collation)),
        charset: Some(charset.to_owned()),
        elems: Vec::new(),
        // Upstream FieldType.array is gogoproto nullable=false.
        array: Some(false),
    }
}

/// Whether a column of this family may become a TiPB `ColumnRef` leaf here.
///
/// Go `columnToPBExpr` refuses `SET`, `GEOMETRY` and unspecified outright, and
/// admits `BIT` and `ENUM` only behind `IsPushDownEnabled` switches this tier
/// does not read; an `ENUM`/`SET` leaf would additionally need its `elems`
/// list on the wire, and a `JSON` leaf the `ETJson` handling. All of them are
/// refused here instead, which costs network and never an answer.
const fn leaf_column_family(code: FieldTypeCode) -> bool {
    matches!(
        code,
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
            | FieldTypeCode::Float
            | FieldTypeCode::Double
            | FieldTypeCode::NewDecimal
            | FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::Timestamp
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::String
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
    )
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
                .map(|(_, descriptor)| descriptor.clone())
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
                    // The charset is derived from the collation, exactly as
                    // the served scan descriptor derives it: `tipb.ColumnInfo`
                    // states a collation id and no charset at all.
                    charset: tidb_datatype::get_collation_by_name(field_type.collation_name())
                        .map_or_else(|_| "binary".to_owned(), |row| row.charset_name),
                    collation: field_type.collation_name().to_owned(),
                },
            )),
            PbScalar::IntLiteral(_)
            | PbScalar::DecimalLiteral { .. }
            | PbScalar::RealLiteral { .. } => {}
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

    /// A constant in a typed slot is folded to the target literal family, as
    /// Go's `foldConstant` does before PB conversion.
    #[test]
    fn a_constant_that_needs_a_cast_is_folded_before_lowering() {
        let call = build_call(
            "atan2",
            vec![column(FieldTypeCode::Double), PbScalar::IntLiteral(1)],
        )
        .unwrap();
        let pb = lower(&call).expect("the folded Float64 literal lowers");
        assert_eq!(pb.children[1].tp, Some(ExprType::Float64 as i32));
        assert_eq!(pb.children[1].sig, Some(ScalarFuncSig::Unspecified as i32));
        // The same constant in an Int slot needs no cast and lowers.
        let call = build_call(
            "mod",
            vec![column(FieldTypeCode::LongLong), PbScalar::IntLiteral(7)],
        )
        .unwrap();
        assert!(lower(&call).is_some());

        let call = build_call(
            "minus",
            vec![PbScalar::IntLiteral(1), column(FieldTypeCode::NewDecimal)],
        )
        .unwrap();
        let pb = lower(&call).expect("the folded MysqlDecimal literal lowers");
        assert_eq!(pb.sig, Some(ScalarFuncSig::MinusDecimal as i32));
        assert_eq!(pb.children[0].tp, Some(ExprType::MysqlDecimal as i32));
    }

    /// A string column's leaf carries the column's OWN collation, put through
    /// `collate.CollationToProto` -- the id TiKV picks its collator from.
    /// Guessing this is the one mistake that returns wrong rows silently, so
    /// it is asserted as an exact protocol id and not as "some collation".
    #[test]
    fn a_string_leaf_carries_the_columns_own_collation_id() {
        for name in ["utf8mb4_bin", "utf8mb4_general_ci", "binary", "latin1_bin"] {
            let scalar = PbScalar::Column {
                offset: 0,
                field_type: FieldType::new(FieldTypeCode::VarString).with_collation_name(name),
            };
            let call = build_call("char_length", vec![scalar]).unwrap();
            let pb = lower(&call).unwrap();
            let leaf = pb.children[0].field_type.as_ref().unwrap();
            assert_eq!(
                leaf.collate,
                Some(tidb_datatype::collation_to_proto(name)),
                "{name}: the leaf must carry the column's own collation id"
            );
            assert_eq!(
                leaf.charset.as_deref(),
                Some(
                    tidb_datatype::get_collation_by_name(name)
                        .unwrap()
                        .charset_name
                        .as_str()
                ),
                "{name}: the charset the collation belongs to"
            );
        }
    }

    /// A column family whose TiPB leaf needs more than the fields this tier
    /// copies is refused: `ENUM`/`SET` carry an `elems` list, and `BIT`,
    /// `JSON` and `GEOMETRY` are gated or refused by Go's `columnToPBExpr`.
    #[test]
    fn a_leaf_family_go_gates_or_refuses_is_refused_here() {
        for code in [
            FieldTypeCode::Enum,
            FieldTypeCode::Set,
            FieldTypeCode::Bit,
            FieldTypeCode::Json,
            FieldTypeCode::Geometry,
        ] {
            assert!(
                !leaf_column_family(code),
                "{code:?} needs more than the six TiPB field-type fields this tier copies"
            );
        }
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

    /// A string column with an explicit collation, which is what the string
    /// family's signature is chosen by.
    fn string_column(code: FieldTypeCode, collation: &str) -> PbScalar {
        PbScalar::Column {
            offset: 0,
            field_type: FieldType::new(code).with_collation_name(collation),
        }
    }

    /// The whole binary-versus-UTF-8 switch of `builtin_string.go`, as one
    /// table: every string family resolves its spelling from
    /// `types.IsBinaryStr(args[0])` and from nothing else -- not from the
    /// MySQL type, which is why `VARCHAR COLLATE binary` and `VARBINARY`
    /// resolve the same signature.
    #[test]
    fn the_string_family_resolves_its_spelling_from_is_binary_str() {
        let cases: &[(&str, usize, ScalarFuncSig, ScalarFuncSig)] = &[
            (
                "char_length",
                1,
                ScalarFuncSig::CharLength,
                ScalarFuncSig::CharLengthUtf8,
            ),
            ("upper", 1, ScalarFuncSig::Upper, ScalarFuncSig::UpperUtf8),
            ("lower", 1, ScalarFuncSig::Lower, ScalarFuncSig::LowerUtf8),
            (
                "substr",
                2,
                ScalarFuncSig::Substring2Args,
                ScalarFuncSig::Substring2ArgsUtf8,
            ),
            (
                "substr",
                3,
                ScalarFuncSig::Substring3Args,
                ScalarFuncSig::Substring3ArgsUtf8,
            ),
            (
                "substring",
                3,
                ScalarFuncSig::Substring3Args,
                ScalarFuncSig::Substring3ArgsUtf8,
            ),
            (
                "mid",
                3,
                ScalarFuncSig::Substring3Args,
                ScalarFuncSig::Substring3ArgsUtf8,
            ),
        ];
        for &(name, arity, binary_sig, utf8_sig) in cases {
            for (collation, expected) in [
                ("binary", binary_sig),
                ("utf8mb4_bin", utf8_sig),
                ("utf8mb4_general_ci", utf8_sig),
            ] {
                for code in [FieldTypeCode::VarString, FieldTypeCode::String] {
                    let mut args = vec![string_column(code, collation)];
                    args.extend((1..arity).map(|position| {
                        PbScalar::IntLiteral(i64::try_from(position).expect("small"))
                    }));
                    let resolved = resolve(name, &args).unwrap_or_else(|| {
                        panic!("{name}/{arity} over {collation} {code:?} resolves")
                    });
                    assert_eq!(
                        resolved.sig, expected,
                        "{name}/{arity} over {collation} {code:?}"
                    );
                }
            }
        }
    }

    /// `CONV` has ONE signature whatever the argument's collation, and its
    /// result carries the connection charset with `flen` 64 --
    /// `convFunctionClass.getFunction` setting them by hand rather than taking
    /// `deriveCollation`'s answer.
    #[test]
    fn conv_is_collation_blind_and_returns_the_connection_charset() {
        for collation in ["binary", "utf8mb4_bin", "utf8mb4_general_ci"] {
            let call = build_call(
                "conv",
                vec![
                    string_column(FieldTypeCode::VarString, collation),
                    PbScalar::IntLiteral(10),
                    PbScalar::IntLiteral(2),
                ],
            )
            .unwrap_or_else(|| panic!("conv over {collation} resolves"));
            let pb = lower(&call).unwrap();
            assert_eq!(pb.sig, Some(ScalarFuncSig::Conv as i32));
            let ret = pb.field_type.as_ref().unwrap();
            let (charset, collation_name) = crate::collation_derive::connection_charset_info();
            assert_eq!(ret.charset.as_deref(), Some(charset));
            assert_eq!(
                ret.collate,
                Some(tidb_datatype::collation_to_proto(collation_name))
            );
            assert_eq!(ret.flen, Some(CONV_RETURN_FLEN));
            assert_eq!(ret.flag, Some(0), "CONV's result carries no BINARY flag");
        }
    }

    /// `UPPER`/`LOWER`/`SUBSTR` stamp the FIRST ARGUMENT's charset, collation
    /// and `flen` on their result -- Go's `deriveCollation` over that one
    /// argument, then `bf.tp.SetFlen(argType.GetFlen())` and
    /// `SetBinFlagOrBinStr`.
    #[test]
    fn the_first_arg_string_family_stamps_its_arguments_collation_on_the_result() {
        for name in ["upper", "lower", "substr"] {
            for collation in ["utf8mb4_bin", "utf8mb4_general_ci", "binary"] {
                let mut args = vec![PbScalar::Column {
                    offset: 0,
                    field_type: FieldType::new(FieldTypeCode::VarString)
                        .with_collation_name(collation)
                        .with_flen(40),
                }];
                if name == "substr" {
                    args.push(PbScalar::IntLiteral(2));
                }
                let call = build_call(name, args).unwrap();
                let pb = lower(&call).unwrap();
                let ret = pb.field_type.as_ref().unwrap();
                assert_eq!(
                    ret.tp,
                    Some(FieldTypeCode::VarString.mysql_type().into()),
                    "{name}: newReturnFieldTypeForBaseBuiltinFunc's ETString arm is VAR_STRING"
                );
                assert_eq!(ret.flen, Some(40), "{name}: SetFlen(argType.GetFlen())");
                assert_eq!(
                    ret.collate,
                    Some(tidb_datatype::collation_to_proto(collation)),
                    "{name} over {collation}"
                );
                // `SetBinFlagOrBinStr`: a binary-string argument sets
                // BINARY_FLAG and pins charset/collation to `binary`; a
                // non-binary one leaves the flag clear.
                if collation == "binary" {
                    assert_eq!(ret.flag, Some(BINARY_FLAG), "{name}: SetBinChsClnFlag");
                    assert_eq!(ret.charset.as_deref(), Some("binary"));
                } else {
                    assert_eq!(ret.flag, Some(0), "{name} over {collation}");
                }
            }
        }
    }

    /// A nested string call keeps resolving on the collation the INNER call
    /// returns, so `UPPER(LOWER(b))` over a binary column is the binary
    /// spelling twice and never mixes the two.
    #[test]
    fn a_nested_string_call_resolves_on_the_inner_calls_collation() {
        let inner = build_call(
            "lower",
            vec![string_column(FieldTypeCode::VarString, "binary")],
        )
        .unwrap();
        let outer = build_call("upper", vec![inner]).unwrap();
        let PbScalar::Call { signature, .. } = &outer else {
            panic!("a call");
        };
        assert_eq!(signature.sig, ScalarFuncSig::Upper);

        let inner = build_call(
            "lower",
            vec![string_column(
                FieldTypeCode::VarString,
                "utf8mb4_general_ci",
            )],
        )
        .unwrap();
        let outer = build_call("upper", vec![inner]).unwrap();
        let PbScalar::Call { signature, .. } = &outer else {
            panic!("a call");
        };
        assert_eq!(signature.sig, ScalarFuncSig::UpperUtf8);
    }

    /// A NON-string argument in a string slot is refused, loudly and on
    /// purpose. Go would insert `WrapWithCastAsString`, whose target
    /// `FieldType` takes its `flen` from a per-source-type table
    /// (`builtin_cast.go`'s `WrapWithCastAsString`) and its charset from the
    /// session; sending a differently-shaped cast than Go sends would change
    /// what TiKV reads, so this tier does not send one at all.
    #[test]
    fn a_non_string_argument_in_a_string_slot_is_refused() {
        for name in ["char_length", "upper", "lower", "conv"] {
            let mut args = vec![column(FieldTypeCode::LongLong)];
            if name == "conv" {
                args.push(PbScalar::IntLiteral(10));
                args.push(PbScalar::IntLiteral(2));
            }
            assert!(
                resolve(name, &args).is_none(),
                "{name} over an integer column needs WrapWithCastAsString, \
                 which this tier does not build"
            );
        }
        assert!(
            resolve("upper", &[column(FieldTypeCode::Json)]).is_none(),
            "a JSON argument needs the ETJson handling this tier does not build"
        );
    }

    /// The lowering refuses when the scan descriptor's collation disagrees
    /// with the collation the signature was chosen from: `UpperUTF8` sent
    /// against a column the coprocessor reads as `binary` is `Upper`'s job,
    /// and case-folds differently.
    #[test]
    fn a_descriptor_that_disagrees_about_the_collation_refuses_the_push() {
        let call = build_call(
            "upper",
            vec![string_column(FieldTypeCode::VarString, "utf8mb4_bin")],
        )
        .unwrap();
        assert!(lower(&call).is_some(), "agreeing descriptors lower");
        let binary_descriptor = |_offset| {
            Some(ColumnDescriptor {
                tp: FieldTypeCode::VarString.mysql_type().into(),
                flag: 0,
                flen: UNSPECIFIED_LENGTH,
                decimal: UNSPECIFIED_LENGTH,
                charset: "binary".to_owned(),
                collation: "binary".to_owned(),
            })
        };
        assert!(
            to_pb(&call, &binary_descriptor).is_none(),
            "the UTF-8 signature must not travel against a binary column"
        );
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
                matches!(
                    row.ret,
                    EvalType::Int | EvalType::Real | EvalType::Decimal | EvalType::String
                ),
                "{}: the return family needs a TiPB field type this tier builds",
                row.name
            );
            assert_eq!(
                row.ret == EvalType::String,
                row.ret_collation != RetCollation::Numeric,
                "{}: a string-returning row must say where its collation comes \
                 from, and a numeric one must not claim a derived collation",
                row.name
            );
            assert!(
                !row.ret_unsigned_from_first_arg || row.ret != EvalType::String,
                "{}: no string getFunction propagates UNSIGNED",
                row.name
            );
            assert_ne!(row.sig, ScalarFuncSig::Unspecified, "{}", row.name);
        }
    }
}
