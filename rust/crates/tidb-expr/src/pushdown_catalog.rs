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

//! The builtin-call catalog: "which `tipb.ScalarFuncSig` is this call", for
//! every builtin Go gives one.
//!
//! # What the table answers, and what it does not
//!
//! Go answers two questions in two places:
//!
//! * the per-family `getFunction` in `pkg/expression/builtin_*.go` picks a
//!   concrete `tipb.ScalarFuncSig` from the arguments' *evaluation types*
//!   (and, for `MOD`, their `UNSIGNED` flags), and calls
//!   `newBaseBuiltinFuncWithTp`, which wraps each argument in the implicit
//!   cast that makes it the type the chosen signature reads;
//! * `infer_pushdown.go`'s `canFuncBePushed` then answers whether that
//!   already-resolved signature may travel to a store, keyed on the function
//!   name and -- for `ROUND`, `RAND`, `UNIX_TIMESTAMP` and others -- on the
//!   chosen signature itself.
//!
//! This module is the first: [`resolve`] and [`pb_code`] are Go's
//! `Function.PbCode()`, and they answer for a call no store evaluates (`TAN`,
//! `ROUND` with a `frac` argument) exactly as for one every store does. The
//! second is [`crate::infer_pushdown`], which reads the signature from here.
//! The converter ([`expr_to_pb`], Go `PbConverter.ExprToPB`) applies Go
//! `scalarFuncToPBExpr`'s own gate -- a signature at all, and
//! `canFuncBePushed` for *some* store -- and whether one particular store
//! takes the expression is the caller's `can_exprs_push_down`, as it is the
//! Go planner's.
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
//! # What the lowering refuses
//!
//! [`to_pb`] builds the TiPB tree, inserting the same implicit casts
//! `newBaseBuiltinFuncWithTp` inserts. It refuses only leaves whose TiPB
//! `FieldType` or literal family this tier cannot build faithfully; concrete
//! string/ENUM/JSON metadata, including non-binary collations, is carried
//! through the descriptor.
//!
//! A conjunct lowered *wrongly* would drop a row the query selects, and no
//! local pass can put back a row that never crossed the wire -- which is why
//! every row of the table below cites the Go `getFunction` it was read from.

use tidb_datatype::{
    collation_to_proto, BinaryJSON, BinaryLiteral, Datum, Decimal, EvalType, FieldType,
    FieldTypeCode, MySqlDuration, Time, VectorFloat32,
};
use tidb_proto::tipb::{Expr, ExprType};

use crate::expression::Expression;

/// The TiPB signature enum, re-exported so a caller that reads a resolved
/// signature off the catalog does not need its own TiPB dependency: the
/// catalog is the only place a signature is chosen.
use tidb_proto::tipb::ScalarFuncSig;

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
/// Go `mysql.MaxDecimalWidth` / `MaxDecimalScale`, the implicit DECIMAL
/// cast's limits.
const MAX_DECIMAL_WIDTH_I32: i32 = 65;
const MAX_DECIMAL_SCALE_I32: i32 = 30;
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
    /// Aggregate all value branches using the native control-function inference.
    ControlBranches,
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
    /// The second argument's charset and collation, as `DATE_FORMAT`'s
    /// `deriveCollation` branch does. Go also stamps the result `flen` from
    /// the format mask, which is mirrored by the return-type builder below.
    SecondArgString,
    /// `@@character_set_connection`/`@@collation_connection` with `flen` 64:
    /// `convFunctionClass.getFunction`, which sets them itself rather than
    /// taking `deriveCollation`'s answer.
    ConnectionString,
    /// The function node's own result type -- Go `sf.RetType`, which is what
    /// `scalarFuncToPBExpr` sends -- carried on the call by
    /// `describe_function`. A call built without a node has none.
    FunctionType,
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
    /// The evaluation type argument `index` of an `arity`-argument call is
    /// cast to; `own` is that argument's own evaluation type. Fixed-shape
    /// rows read `arg_types`. The variadic conditional rows type their slots
    /// the way Go's `getFunction` does (`builtin_control.go`): a CASE WHEN
    /// condition and IF's first argument keep the type of the boolean wrapper
    /// built by `wrapWithIsTrue` (`args[i].GetType()`), and every value slot is the return type.
    #[must_use]
    pub fn arg_type_at(&self, index: usize, arity: usize, own: EvalType) -> Option<EvalType> {
        if self.name == "in" {
            return self.arg_types.first().copied();
        }
        if !self.arg_types.is_empty() {
            return self.arg_types.get(index).copied();
        }
        match self.name {
            "case" if index + 1 == arity && arity % 2 == 1 => Some(self.ret),
            "case" if index % 2 == 0 => Some(own),
            "case" => Some(self.ret),
            "if" if arity == 3 && index == 0 => Some(own),
            "if" if arity == 3 => Some(self.ret),
            "ifnull" if arity == 2 => Some(self.ret),
            "coalesce" => Some(self.ret),
            "concat" | "concat_ws" => Some(EvalType::String),
            "json_extract" if index == 0 => Some(EvalType::Json),
            "json_type" | "json_merge" | "json_merge_patch" | "json_merge_preserve" | "json_array" => {
                Some(EvalType::Json)
            }
            "json_unquote" => Some(EvalType::String),
            "json_valid" => Some(own),
            "json_length" | "json_remove" | "json_contains_path" | "json_search" | "json_keys" if index == 0 => {
                Some(EvalType::Json)
            }
            "json_length" | "json_remove" | "json_contains_path" | "json_search" | "json_keys" => {
                Some(EvalType::String)
            }
            "json_object" if index % 2 == 0 => Some(EvalType::String),
            "json_object" => Some(EvalType::Json),
            "json_set" | "json_insert" | "json_replace" | "json_array_append" | "json_array_insert"
                if index % 2 == 1 =>
            {
                Some(EvalType::String)
            }
            "json_set" | "json_insert" | "json_replace" | "json_array_append" | "json_array_insert" => {
                Some(EvalType::Json)
            }
            "json_contains" if index < 2 => Some(EvalType::Json),
            "json_contains" => Some(EvalType::String),
            "elt" if index == 0 => Some(EvalType::Int),
            "elt" => Some(EvalType::String),
            "field" => Some(match self.sig {
                ScalarFuncSig::FieldString => EvalType::String,
                ScalarFuncSig::FieldInt => EvalType::Int,
                _ => EvalType::Real,
            }),
            "json_extract" => Some(EvalType::String),
            // Go `resolveType4Extremum`'s `argTp`, which is the result type
            // of every GREATEST/LEAST signature.
            "greatest" | "least" => Some(self.ret),
            // Go `timeDiffFunctionClass.getArgEvalTp`: a time or datetime
            // operand keeps its type, any other is read as a string.
            "timediff" => Some(match own {
                EvalType::Duration | EvalType::Datetime | EvalType::Timestamp => own,
                _ => EvalType::String,
            }),
            "interval" => Some(match self.sig {
                ScalarFuncSig::IntervalInt => EvalType::Int,
                _ => EvalType::Real,
            }),
            "make_set" if index == 0 => Some(EvalType::Int),
            "make_set" => Some(EvalType::String),
            // Go `charFunctionClass.getFunction`: `ETInt` code points, then
            // the `ETString` charset name.
            "char_func" if index + 1 == arity => Some(EvalType::String),
            "char_func" => Some(EvalType::Int),
            "match_against" => Some(EvalType::String),
            _ => None,
        }
    }
}

impl BuiltinSignature {
    /// The TiPB field type of this signature's result: Go's
    /// `newReturnFieldTypeForBaseBuiltinFunc` for `ret`, plus the per-family
    /// adjustment the family's own `getFunction` makes to it afterwards.
    ///
    /// `children` are the arguments already lowered, so a
    /// [`RetCollation::FirstArgString`] and [`RetCollation::SecondArgString`]
    /// rows read the relevant argument field type that crosses the wire rather
    /// than a second, separately-derived copy of it.
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
                if matches!(self.name, "isnull" | "istrue_with_null") {
                    1
                } else {
                    MAX_INT_WIDTH
                },
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
            EvalType::Datetime => temporal_field_type(FieldTypeCode::Datetime, 0),
            EvalType::Timestamp => temporal_field_type(FieldTypeCode::Timestamp, 0),
            EvalType::Duration => temporal_field_type(FieldTypeCode::Duration, 0),
            EvalType::Json => temporal_field_type(FieldTypeCode::Json, 0),
            EvalType::VectorFloat32 => int_field_type(
                FieldTypeCode::VectorFloat32.mysql_type().into(),
                BINARY_FLAG,
                UNSPECIFIED_LENGTH,
                0,
            ),
            EvalType::String => unreachable!("string returns use string_return_field_type"),
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
            // Controls carry their already-inferred full field type on the call.
            RetCollation::ControlBranches => None,
            // The node's result type is on the call, not derived here.
            RetCollation::FunctionType => None,
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
                let argument = children[0].field_type.as_ref()?;
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
            RetCollation::SecondArgString => {
                let argument = children[1].field_type.as_ref()?;
                let argument_flag = argument.flag.unwrap_or_default();
                let collation =
                    tidb_datatype::proto_to_collation(argument.collate.unwrap_or_default());
                let charset = argument.charset.clone()?;
                let (charset, collation, flag) = if collation == BINARY_COLLATION {
                    (
                        BINARY_CHARSET.to_owned(),
                        BINARY_COLLATION.to_owned(),
                        BINARY_FLAG,
                    )
                } else {
                    (charset, collation, argument_flag & BINARY_FLAG)
                };
                // `dateFormatFunctionClass.getFunction` computes the worst
                // case output width from the format argument's declared
                // length: `(flen + 1) / 2 * 11`.
                let format_flen = argument.flen.unwrap_or(UNSPECIFIED_LENGTH);
                let flen = (format_flen + 1) / 2 * 11;
                Some(pb_field_type(
                    FieldTypeCode::VarString.mysql_type().into(),
                    flag,
                    flen,
                    UNSPECIFIED_LENGTH,
                    &charset,
                    &collation,
                ))
            }
        }
    }
}

/// Builds the default TiPB field metadata for one non-string evaluation
/// family.  Go's `newReturnFieldTypeForBaseBuiltinFunc` uses the same type
/// code/flags pair for these results; keeping this in one helper also makes
/// the temporal and JSON return arms use the full `ToPBFieldType` path.
fn temporal_field_type(code: FieldTypeCode, flags: u32) -> tidb_proto::tipb::FieldType {
    let field_type = FieldType::new(code).with_flags(flags);
    to_pb_field_type(&field_type).expect("supported temporal/json result type")
}

// DATE_ADD/SUB take Go's three arguments: the date, the interval and the
// constant unit (`addSubDateFunctionClass` declares the unit `ETString`).
// The unit changes the runtime operation and result metadata, but not the
// TiPB signature family.  Keep every Go overload here so an unsupported row
// cannot accidentally resolve merely because it shares a function name.
const DATE_ARGS_SS: &[EvalType] = &[EvalType::String, EvalType::String, EvalType::String];
const DATE_ARGS_SI: &[EvalType] = &[EvalType::String, EvalType::Int, EvalType::String];
const DATE_ARGS_SR: &[EvalType] = &[EvalType::String, EvalType::Real, EvalType::String];
const DATE_ARGS_SD: &[EvalType] = &[EvalType::String, EvalType::Decimal, EvalType::String];
const DATE_ARGS_IS: &[EvalType] = &[EvalType::Int, EvalType::String, EvalType::String];
const DATE_ARGS_II: &[EvalType] = &[EvalType::Int, EvalType::Int, EvalType::String];
const DATE_ARGS_IR: &[EvalType] = &[EvalType::Int, EvalType::Real, EvalType::String];
const DATE_ARGS_ID: &[EvalType] = &[EvalType::Int, EvalType::Decimal, EvalType::String];
const DATE_ARGS_RS: &[EvalType] = &[EvalType::Real, EvalType::String, EvalType::String];
const DATE_ARGS_RI: &[EvalType] = &[EvalType::Real, EvalType::Int, EvalType::String];
const DATE_ARGS_RR: &[EvalType] = &[EvalType::Real, EvalType::Real, EvalType::String];
const DATE_ARGS_RD: &[EvalType] = &[EvalType::Real, EvalType::Decimal, EvalType::String];
const DATE_ARGS_DS: &[EvalType] = &[EvalType::Decimal, EvalType::String, EvalType::String];
const DATE_ARGS_DI: &[EvalType] = &[EvalType::Decimal, EvalType::Int, EvalType::String];
const DATE_ARGS_DR: &[EvalType] = &[EvalType::Decimal, EvalType::Real, EvalType::String];
const DATE_ARGS_DD: &[EvalType] = &[EvalType::Decimal, EvalType::Decimal, EvalType::String];
const DATE_ARGS_TS: &[EvalType] = &[EvalType::Datetime, EvalType::String, EvalType::String];
const DATE_ARGS_TI: &[EvalType] = &[EvalType::Datetime, EvalType::Int, EvalType::String];
const DATE_ARGS_TR: &[EvalType] = &[EvalType::Datetime, EvalType::Real, EvalType::String];
const DATE_ARGS_TD: &[EvalType] = &[EvalType::Datetime, EvalType::Decimal, EvalType::String];
const DATE_ARGS_HS: &[EvalType] = &[EvalType::Duration, EvalType::String, EvalType::String];
const DATE_ARGS_HI: &[EvalType] = &[EvalType::Duration, EvalType::Int, EvalType::String];
const DATE_ARGS_HR: &[EvalType] = &[EvalType::Duration, EvalType::Real, EvalType::String];
const DATE_ARGS_HD: &[EvalType] = &[EvalType::Duration, EvalType::Decimal, EvalType::String];

const fn date_arg_types(from: EvalType, to: EvalType) -> &'static [EvalType] {
    match (from, to) {
        (EvalType::String, EvalType::String) => DATE_ARGS_SS,
        (EvalType::String, EvalType::Int) => DATE_ARGS_SI,
        (EvalType::String, EvalType::Real) => DATE_ARGS_SR,
        (EvalType::String, EvalType::Decimal) => DATE_ARGS_SD,
        (EvalType::Int, EvalType::String) => DATE_ARGS_IS,
        (EvalType::Int, EvalType::Int) => DATE_ARGS_II,
        (EvalType::Int, EvalType::Real) => DATE_ARGS_IR,
        (EvalType::Int, EvalType::Decimal) => DATE_ARGS_ID,
        (EvalType::Real, EvalType::String) => DATE_ARGS_RS,
        (EvalType::Real, EvalType::Int) => DATE_ARGS_RI,
        (EvalType::Real, EvalType::Real) => DATE_ARGS_RR,
        (EvalType::Real, EvalType::Decimal) => DATE_ARGS_RD,
        (EvalType::Decimal, EvalType::String) => DATE_ARGS_DS,
        (EvalType::Decimal, EvalType::Int) => DATE_ARGS_DI,
        (EvalType::Decimal, EvalType::Real) => DATE_ARGS_DR,
        (EvalType::Decimal, EvalType::Decimal) => DATE_ARGS_DD,
        (EvalType::Datetime, EvalType::String) => DATE_ARGS_TS,
        (EvalType::Datetime, EvalType::Int) => DATE_ARGS_TI,
        (EvalType::Datetime, EvalType::Real) => DATE_ARGS_TR,
        (EvalType::Datetime, EvalType::Decimal) => DATE_ARGS_TD,
        (EvalType::Duration, EvalType::String) => DATE_ARGS_HS,
        (EvalType::Duration, EvalType::Int) => DATE_ARGS_HI,
        (EvalType::Duration, EvalType::Real) => DATE_ARGS_HR,
        (EvalType::Duration, EvalType::Decimal) => DATE_ARGS_HD,
        _ => &[],
    }
}

const fn date_signature(
    from: EvalType,
    to: EvalType,
    ret: EvalType,
    sig: ScalarFuncSig,
) -> BuiltinSignature {
    BuiltinSignature {
        name: "date_arithmetic",
        selector: &[ArgPattern::ANY, ArgPattern::ANY],
        arg_types: date_arg_types(from, to),
        ret,
        sig,
        ret_unsigned_from_first_arg: false,
        // Non-string signatures ignore this field; string DATE_ADD/SUB
        // results use the connection collation selected by Go's fallback
        // derivation.
        ret_collation: RetCollation::ConnectionString,
    }
}

const DATE_ADD_SIGNATURES: &[BuiltinSignature] = &[
    date_signature(
        EvalType::String,
        EvalType::String,
        EvalType::String,
        ScalarFuncSig::AddDateStringString,
    ),
    date_signature(
        EvalType::String,
        EvalType::Int,
        EvalType::String,
        ScalarFuncSig::AddDateStringInt,
    ),
    date_signature(
        EvalType::String,
        EvalType::Real,
        EvalType::String,
        ScalarFuncSig::AddDateStringReal,
    ),
    date_signature(
        EvalType::String,
        EvalType::Decimal,
        EvalType::String,
        ScalarFuncSig::AddDateStringDecimal,
    ),
    date_signature(
        EvalType::Int,
        EvalType::String,
        EvalType::String,
        ScalarFuncSig::AddDateIntString,
    ),
    date_signature(
        EvalType::Int,
        EvalType::Int,
        EvalType::String,
        ScalarFuncSig::AddDateIntInt,
    ),
    date_signature(
        EvalType::Int,
        EvalType::Real,
        EvalType::String,
        ScalarFuncSig::AddDateIntReal,
    ),
    date_signature(
        EvalType::Int,
        EvalType::Decimal,
        EvalType::String,
        ScalarFuncSig::AddDateIntDecimal,
    ),
    date_signature(
        EvalType::Real,
        EvalType::String,
        EvalType::String,
        ScalarFuncSig::AddDateRealString,
    ),
    date_signature(
        EvalType::Real,
        EvalType::Int,
        EvalType::String,
        ScalarFuncSig::AddDateRealInt,
    ),
    date_signature(
        EvalType::Real,
        EvalType::Real,
        EvalType::String,
        ScalarFuncSig::AddDateRealReal,
    ),
    date_signature(
        EvalType::Real,
        EvalType::Decimal,
        EvalType::String,
        ScalarFuncSig::AddDateRealDecimal,
    ),
    date_signature(
        EvalType::Decimal,
        EvalType::String,
        EvalType::String,
        ScalarFuncSig::AddDateDecimalString,
    ),
    date_signature(
        EvalType::Decimal,
        EvalType::Int,
        EvalType::String,
        ScalarFuncSig::AddDateDecimalInt,
    ),
    date_signature(
        EvalType::Decimal,
        EvalType::Real,
        EvalType::String,
        ScalarFuncSig::AddDateDecimalReal,
    ),
    date_signature(
        EvalType::Decimal,
        EvalType::Decimal,
        EvalType::String,
        ScalarFuncSig::AddDateDecimalDecimal,
    ),
    date_signature(
        EvalType::Datetime,
        EvalType::String,
        EvalType::Datetime,
        ScalarFuncSig::AddDateDatetimeString,
    ),
    date_signature(
        EvalType::Datetime,
        EvalType::Int,
        EvalType::Datetime,
        ScalarFuncSig::AddDateDatetimeInt,
    ),
    date_signature(
        EvalType::Datetime,
        EvalType::Real,
        EvalType::Datetime,
        ScalarFuncSig::AddDateDatetimeReal,
    ),
    date_signature(
        EvalType::Datetime,
        EvalType::Decimal,
        EvalType::Datetime,
        ScalarFuncSig::AddDateDatetimeDecimal,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::String,
        EvalType::Duration,
        ScalarFuncSig::AddDateDurationString,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Int,
        EvalType::Duration,
        ScalarFuncSig::AddDateDurationInt,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Real,
        EvalType::Duration,
        ScalarFuncSig::AddDateDurationReal,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Decimal,
        EvalType::Duration,
        ScalarFuncSig::AddDateDurationDecimal,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::String,
        EvalType::Datetime,
        ScalarFuncSig::AddDateDurationStringDatetime,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Int,
        EvalType::Datetime,
        ScalarFuncSig::AddDateDurationIntDatetime,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Real,
        EvalType::Datetime,
        ScalarFuncSig::AddDateDurationRealDatetime,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Decimal,
        EvalType::Datetime,
        ScalarFuncSig::AddDateDurationDecimalDatetime,
    ),
];

const DATE_SUB_SIGNATURES: &[BuiltinSignature] = &[
    date_signature(
        EvalType::String,
        EvalType::String,
        EvalType::String,
        ScalarFuncSig::SubDateStringString,
    ),
    date_signature(
        EvalType::String,
        EvalType::Int,
        EvalType::String,
        ScalarFuncSig::SubDateStringInt,
    ),
    date_signature(
        EvalType::String,
        EvalType::Real,
        EvalType::String,
        ScalarFuncSig::SubDateStringReal,
    ),
    date_signature(
        EvalType::String,
        EvalType::Decimal,
        EvalType::String,
        ScalarFuncSig::SubDateStringDecimal,
    ),
    date_signature(
        EvalType::Int,
        EvalType::String,
        EvalType::String,
        ScalarFuncSig::SubDateIntString,
    ),
    date_signature(
        EvalType::Int,
        EvalType::Int,
        EvalType::String,
        ScalarFuncSig::SubDateIntInt,
    ),
    date_signature(
        EvalType::Int,
        EvalType::Real,
        EvalType::String,
        ScalarFuncSig::SubDateIntReal,
    ),
    date_signature(
        EvalType::Int,
        EvalType::Decimal,
        EvalType::String,
        ScalarFuncSig::SubDateIntDecimal,
    ),
    date_signature(
        EvalType::Real,
        EvalType::String,
        EvalType::String,
        ScalarFuncSig::SubDateRealString,
    ),
    date_signature(
        EvalType::Real,
        EvalType::Int,
        EvalType::String,
        ScalarFuncSig::SubDateRealInt,
    ),
    date_signature(
        EvalType::Real,
        EvalType::Real,
        EvalType::String,
        ScalarFuncSig::SubDateRealReal,
    ),
    date_signature(
        EvalType::Real,
        EvalType::Decimal,
        EvalType::String,
        ScalarFuncSig::SubDateRealDecimal,
    ),
    date_signature(
        EvalType::Decimal,
        EvalType::String,
        EvalType::String,
        ScalarFuncSig::SubDateDecimalString,
    ),
    date_signature(
        EvalType::Decimal,
        EvalType::Int,
        EvalType::String,
        ScalarFuncSig::SubDateDecimalInt,
    ),
    date_signature(
        EvalType::Decimal,
        EvalType::Real,
        EvalType::String,
        ScalarFuncSig::SubDateDecimalReal,
    ),
    date_signature(
        EvalType::Decimal,
        EvalType::Decimal,
        EvalType::String,
        ScalarFuncSig::SubDateDecimalDecimal,
    ),
    date_signature(
        EvalType::Datetime,
        EvalType::String,
        EvalType::Datetime,
        ScalarFuncSig::SubDateDatetimeString,
    ),
    date_signature(
        EvalType::Datetime,
        EvalType::Int,
        EvalType::Datetime,
        ScalarFuncSig::SubDateDatetimeInt,
    ),
    date_signature(
        EvalType::Datetime,
        EvalType::Real,
        EvalType::Datetime,
        ScalarFuncSig::SubDateDatetimeReal,
    ),
    date_signature(
        EvalType::Datetime,
        EvalType::Decimal,
        EvalType::Datetime,
        ScalarFuncSig::SubDateDatetimeDecimal,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::String,
        EvalType::Duration,
        ScalarFuncSig::SubDateDurationString,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Int,
        EvalType::Duration,
        ScalarFuncSig::SubDateDurationInt,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Real,
        EvalType::Duration,
        ScalarFuncSig::SubDateDurationReal,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Decimal,
        EvalType::Duration,
        ScalarFuncSig::SubDateDurationDecimal,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::String,
        EvalType::Datetime,
        ScalarFuncSig::SubDateDurationStringDatetime,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Int,
        EvalType::Datetime,
        ScalarFuncSig::SubDateDurationIntDatetime,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Real,
        EvalType::Datetime,
        ScalarFuncSig::SubDateDurationRealDatetime,
    ),
    date_signature(
        EvalType::Duration,
        EvalType::Decimal,
        EvalType::Datetime,
        ScalarFuncSig::SubDateDurationDecimalDatetime,
    ),
];

/// Resolves the Go-pushable DATE_ADD/SUB overloads.  Go's
/// `scalarExprSupportedByTiKV` admits the date-arithmetic function family;
/// `getFunction` then selects one of the concrete overloads below.  Keeping
/// that overload choice here is important because the protobuf signature also
/// determines the implicit argument casts and result field type.
fn resolve_date_arithmetic(name: &str, args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    let subtract = match name {
        "date_add" | "adddate" => false,
        "date_sub" | "subdate" => true,
        _ => return None,
    };
    let [first, second, PbScalar::StringLiteral { value: unit, .. }] = args else {
        return None;
    };
    let unit = std::str::from_utf8(unit).ok()?.to_ascii_uppercase();
    // Go normalizes TIMESTAMP and JSON date operands before selecting its
    // overload (`timestamp` behaves as `datetime`, JSON as `string`).
    let first_type = match first.eval_type() {
        EvalType::Timestamp => EvalType::Datetime,
        EvalType::Json => EvalType::String,
        other => other,
    };
    let second_type = match second.eval_type() {
        EvalType::Json => EvalType::String,
        other => other,
    };
    let candidates = if subtract {
        DATE_SUB_SIGNATURES
    } else {
        DATE_ADD_SIGNATURES
    };
    let arg_types = [first_type, second_type, EvalType::String];
    let mut matches = candidates
        .iter()
        .filter(|candidate| candidate.arg_types == arg_types);
    let first_match = matches.next()?;
    if first_match.ret == EvalType::Duration {
        let date_unit = matches!(
            unit.as_str(),
            "DAY" | "WEEK" | "MONTH" | "QUARTER" | "YEAR" | "YEAR_MONTH"
        );
        if date_unit {
            return candidates.iter().find(|candidate| {
                candidate.arg_types == arg_types && candidate.ret == EvalType::Datetime
            });
        }
    }
    Some(first_match)
}

/// Resolves Go's `unixTimestampFunctionClass` return domain.
///
/// Without an argument the signature is `UnixTimestampCurrent`. With one the
/// function has one wire family, but Go chooses
/// `UnixTimestampInt` when the resolved DATETIME argument has FSP 0 and
/// `UnixTimestampDec` otherwise.  A single `ANY -> DECIMAL` catalog row would
/// be observably wrong: it changes the result field type and the TiPB
/// signature for ordinary `DATETIME`/`DATE` columns.  Unknown precision (for
/// example a non-constant string or a computed child) follows Go's decimal
/// fallback.
fn resolve_unix_timestamp(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    let signature = match args {
        [] => ScalarFuncSig::UnixTimestampCurrent,
        [argument] => {
            let decimal = argument
                .static_field_type()
                .map(FieldType::decimal)
                .unwrap_or(i64::from(UNSPECIFIED_LENGTH));
            if decimal == 0 {
                ScalarFuncSig::UnixTimestampInt
            } else {
                ScalarFuncSig::UnixTimestampDec
            }
        }
        _ => return None,
    };
    CATALOG
        .iter()
        .find(|candidate| candidate.name == "unix_timestamp" && candidate.sig == signature)
}

/// Go `extractFunctionClass.getFunction`: a unit that is both a clock and a
/// date unit (`DAY_HOUR` and its kin) reads a datetime or timestamp value as
/// a datetime and a time value as a time; a clock unit reads the value as a
/// time, a date unit as a datetime. The two-sided units read any other value
/// as a string (`ExtractDatetimeFromString`).
fn resolve_extract(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    let [PbScalar::StringLiteral { value: unit, .. }, value] = args else {
        return None;
    };
    let unit = std::str::from_utf8(unit).ok()?;
    let is_clock_unit = tidb_datatype::is_clock_unit(unit);
    let is_date_unit = tidb_datatype::is_date_unit(unit);
    let signature = if is_clock_unit && is_date_unit {
        match value.eval_type() {
            EvalType::Datetime | EvalType::Timestamp => ScalarFuncSig::ExtractDatetime,
            EvalType::Duration => ScalarFuncSig::ExtractDuration,
            _ => ScalarFuncSig::ExtractDatetimeFromString,
        }
    } else if is_clock_unit {
        ScalarFuncSig::ExtractDuration
    } else {
        ScalarFuncSig::ExtractDatetime
    };
    CATALOG.iter().find(|candidate| candidate.name == "extract" && candidate.sig == signature)
}

const STR: ArgPattern = ArgPattern::eval(EvalType::String);
const INT: ArgPattern = ArgPattern::eval(EvalType::Int);
/// The selectors of a family with one signature per arity.
const ANY_0: &[ArgPattern] = &[];
const ANY_1: &[ArgPattern] = &[ArgPattern::ANY; 1];
const ANY_2: &[ArgPattern] = &[ArgPattern::ANY; 2];
const ANY_3: &[ArgPattern] = &[ArgPattern::ANY; 3];
const ANY_4: &[ArgPattern] = &[ArgPattern::ANY; 4];
const ANY_5: &[ArgPattern] = &[ArgPattern::ANY; 5];
const REGEXP_LIKE_SELECTOR_2: &[ArgPattern] = &[STR, STR];
const REGEXP_LIKE_SELECTOR_3: &[ArgPattern] = &[STR, STR, STR];
const REGEXP_LIKE_TYPES_2: &[EvalType] = &[EvalType::String, EvalType::String];
const REGEXP_LIKE_TYPES_3: &[EvalType] = &[EvalType::String, EvalType::String, EvalType::String];
const REGEXP_INSTR_SELECTOR_2: &[ArgPattern] = &[STR, STR];
const REGEXP_INSTR_SELECTOR_3: &[ArgPattern] = &[STR, STR, INT];
const REGEXP_INSTR_SELECTOR_4: &[ArgPattern] = &[STR, STR, INT, INT];
const REGEXP_INSTR_SELECTOR_5: &[ArgPattern] = &[STR, STR, INT, INT, INT];
const REGEXP_INSTR_SELECTOR_6: &[ArgPattern] = &[STR, STR, INT, INT, INT, STR];
const REGEXP_INSTR_TYPES_2: &[EvalType] = &[EvalType::String, EvalType::String];
const REGEXP_INSTR_TYPES_3: &[EvalType] = &[EvalType::String, EvalType::String, EvalType::Int];
const REGEXP_INSTR_TYPES_4: &[EvalType] =
    &[EvalType::String, EvalType::String, EvalType::Int, EvalType::Int];
const REGEXP_INSTR_TYPES_5: &[EvalType] = &[
    EvalType::String,
    EvalType::String,
    EvalType::Int,
    EvalType::Int,
    EvalType::Int,
];
const REGEXP_INSTR_TYPES_6: &[EvalType] = &[
    EvalType::String,
    EvalType::String,
    EvalType::Int,
    EvalType::Int,
    EvalType::Int,
    EvalType::String,
];
const REGEXP_SUBSTR_SELECTOR_2: &[ArgPattern] = &[STR, STR];
const REGEXP_SUBSTR_SELECTOR_3: &[ArgPattern] = &[STR, STR, INT];
const REGEXP_SUBSTR_SELECTOR_4: &[ArgPattern] = &[STR, STR, INT, INT];
const REGEXP_SUBSTR_SELECTOR_5: &[ArgPattern] = &[STR, STR, INT, INT, STR];
const REGEXP_SUBSTR_TYPES_2: &[EvalType] = &[EvalType::String, EvalType::String];
const REGEXP_SUBSTR_TYPES_3: &[EvalType] = &[EvalType::String, EvalType::String, EvalType::Int];
const REGEXP_SUBSTR_TYPES_4: &[EvalType] =
    &[EvalType::String, EvalType::String, EvalType::Int, EvalType::Int];
const REGEXP_SUBSTR_TYPES_5: &[EvalType] = &[
    EvalType::String,
    EvalType::String,
    EvalType::Int,
    EvalType::Int,
    EvalType::String,
];
const REGEXP_REPLACE_SELECTOR_3: &[ArgPattern] = &[STR, STR, STR];
const REGEXP_REPLACE_SELECTOR_4: &[ArgPattern] = &[STR, STR, STR, INT];
const REGEXP_REPLACE_SELECTOR_5: &[ArgPattern] = &[STR, STR, STR, INT, INT];
const REGEXP_REPLACE_SELECTOR_6: &[ArgPattern] = &[STR, STR, STR, INT, INT, STR];
const REGEXP_REPLACE_TYPES_3: &[EvalType] = &[EvalType::String, EvalType::String, EvalType::String];
const REGEXP_REPLACE_TYPES_4: &[EvalType] = &[
    EvalType::String,
    EvalType::String,
    EvalType::String,
    EvalType::Int,
];
const REGEXP_REPLACE_TYPES_5: &[EvalType] = &[
    EvalType::String,
    EvalType::String,
    EvalType::String,
    EvalType::Int,
    EvalType::Int,
];
const REGEXP_REPLACE_TYPES_6: &[EvalType] = &[
    EvalType::String,
    EvalType::String,
    EvalType::String,
    EvalType::Int,
    EvalType::Int,
    EvalType::String,
];

// Go `compareFunctionClass.generateCmpSigs` builds every comparison over two
// arguments already coerced to the comparison type (`newBaseBuiltinFuncWithTp`
// with `tp, tp`), so each comparison row selects on that shared type.
const CMP_INT_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Int),
    ArgPattern::eval(EvalType::Int),
];
const CMP_INT_TYPES: &[EvalType] = &[EvalType::Int, EvalType::Int];
const CMP_REAL_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Real),
    ArgPattern::eval(EvalType::Real),
];
const CMP_REAL_TYPES: &[EvalType] = &[EvalType::Real, EvalType::Real];
const CMP_DECIMAL_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Decimal),
    ArgPattern::eval(EvalType::Decimal),
];
const CMP_DECIMAL_TYPES: &[EvalType] = &[EvalType::Decimal, EvalType::Decimal];
const CMP_STRING_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::String),
    ArgPattern::eval(EvalType::String),
];
const CMP_STRING_TYPES: &[EvalType] = &[EvalType::String, EvalType::String];
const CMP_DATETIME_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Datetime),
    ArgPattern::eval(EvalType::Datetime),
];
const CMP_DATETIME_TYPES: &[EvalType] = &[EvalType::Datetime, EvalType::Datetime];
const CMP_TIMESTAMP_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Timestamp),
    ArgPattern::eval(EvalType::Timestamp),
];
const CMP_TIMESTAMP_TYPES: &[EvalType] = &[EvalType::Timestamp, EvalType::Timestamp];
const CMP_DATETIME_TIMESTAMP_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Datetime),
    ArgPattern::eval(EvalType::Timestamp),
];
const CMP_DATETIME_TIMESTAMP_TYPES: &[EvalType] = &[EvalType::Datetime, EvalType::Timestamp];
const CMP_TIMESTAMP_DATETIME_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Timestamp),
    ArgPattern::eval(EvalType::Datetime),
];
const CMP_TIMESTAMP_DATETIME_TYPES: &[EvalType] = &[EvalType::Timestamp, EvalType::Datetime];
const CMP_DURATION_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Duration),
    ArgPattern::eval(EvalType::Duration),
];
const CMP_DURATION_TYPES: &[EvalType] = &[EvalType::Duration, EvalType::Duration];
const CMP_JSON_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::Json),
    ArgPattern::eval(EvalType::Json),
];
const CMP_JSON_TYPES: &[EvalType] = &[EvalType::Json, EvalType::Json];
const CMP_VECTOR_FLOAT32_PAIR: &[ArgPattern] = &[
    ArgPattern::eval(EvalType::VectorFloat32),
    ArgPattern::eval(EvalType::VectorFloat32),
];
const CMP_VECTOR_FLOAT32_TYPES: &[EvalType] = &[EvalType::VectorFloat32, EvalType::VectorFloat32];

/// The catalog, in Go's own per-family order: every signature a Go
/// `getFunction` assigns with `setPbCode`, whichever stores evaluate it.
pub const CATALOG: &[BuiltinSignature] = &[
    // Go `groupingImplFunctionClass.getFunction`: the grouping id as an
    // `ETInt`, one signature.
    signature(
        "grouping",
        ANY_1,
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::GroupingSig,
        false,
    ),
    // Go `regexpLikeFunctionClass.getFunction` coerces both REGEXP operands
    // to ETString and assigns `RegexpLikeSig`.
    signature(
        "regexp",
        ANY_2,
        &[EvalType::String, EvalType::String],
        EvalType::Int,
        ScalarFuncSig::RegexpLikeSig,
        false,
    ),
    // Go `regexpLikeFunctionClass.getFunction`: `(expr, pat[, match_type])`,
    // every operand an `ETString`, one signature for both arities.
    signature(
        "regexp_like",
        REGEXP_LIKE_SELECTOR_2,
        REGEXP_LIKE_TYPES_2,
        EvalType::Int,
        ScalarFuncSig::RegexpLikeSig,
        false,
    ),
    signature(
        "regexp_like",
        REGEXP_LIKE_SELECTOR_3,
        REGEXP_LIKE_TYPES_3,
        EvalType::Int,
        ScalarFuncSig::RegexpLikeSig,
        false,
    ),
    // Go `regexpInStrFunctionClass.getFunction`: `argTp` is the prefix of
    // `(ETString, ETString, ETInt, ETInt, ETInt, ETString)` as long as the
    // call, and every arity resolves `RegexpInStrSig`.
    signature(
        "regexp_instr",
        REGEXP_INSTR_SELECTOR_2,
        REGEXP_INSTR_TYPES_2,
        EvalType::Int,
        ScalarFuncSig::RegexpInStrSig,
        false,
    ),
    signature(
        "regexp_instr",
        REGEXP_INSTR_SELECTOR_3,
        REGEXP_INSTR_TYPES_3,
        EvalType::Int,
        ScalarFuncSig::RegexpInStrSig,
        false,
    ),
    signature(
        "regexp_instr",
        REGEXP_INSTR_SELECTOR_4,
        REGEXP_INSTR_TYPES_4,
        EvalType::Int,
        ScalarFuncSig::RegexpInStrSig,
        false,
    ),
    signature(
        "regexp_instr",
        REGEXP_INSTR_SELECTOR_5,
        REGEXP_INSTR_TYPES_5,
        EvalType::Int,
        ScalarFuncSig::RegexpInStrSig,
        false,
    ),
    signature(
        "regexp_instr",
        REGEXP_INSTR_SELECTOR_6,
        REGEXP_INSTR_TYPES_6,
        EvalType::Int,
        ScalarFuncSig::RegexpInStrSig,
        false,
    ),
    // Go `regexpSubstrFunctionClass.getFunction`: the prefix of
    // `(ETString, ETString, ETInt, ETInt, ETString)`, an `ETString` result
    // whose `flen` is the first argument's.
    string_signature(
        "regexp_substr",
        REGEXP_SUBSTR_SELECTOR_2,
        REGEXP_SUBSTR_TYPES_2,
        ScalarFuncSig::RegexpSubstrSig,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "regexp_substr",
        REGEXP_SUBSTR_SELECTOR_3,
        REGEXP_SUBSTR_TYPES_3,
        ScalarFuncSig::RegexpSubstrSig,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "regexp_substr",
        REGEXP_SUBSTR_SELECTOR_4,
        REGEXP_SUBSTR_TYPES_4,
        ScalarFuncSig::RegexpSubstrSig,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "regexp_substr",
        REGEXP_SUBSTR_SELECTOR_5,
        REGEXP_SUBSTR_TYPES_5,
        ScalarFuncSig::RegexpSubstrSig,
        RetCollation::FirstArgString,
    ),
    // Go `regexpReplaceFunctionClass.getFunction`: the prefix of
    // `(ETString, ETString, ETString, ETInt, ETInt, ETString)`.
    string_signature(
        "regexp_replace",
        REGEXP_REPLACE_SELECTOR_3,
        REGEXP_REPLACE_TYPES_3,
        ScalarFuncSig::RegexpReplaceSig,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "regexp_replace",
        REGEXP_REPLACE_SELECTOR_4,
        REGEXP_REPLACE_TYPES_4,
        ScalarFuncSig::RegexpReplaceSig,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "regexp_replace",
        REGEXP_REPLACE_SELECTOR_5,
        REGEXP_REPLACE_TYPES_5,
        ScalarFuncSig::RegexpReplaceSig,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "regexp_replace",
        REGEXP_REPLACE_SELECTOR_6,
        REGEXP_REPLACE_TYPES_6,
        ScalarFuncSig::RegexpReplaceSig,
        RetCollation::FirstArgString,
    ),
    // Go isNullFunctionClass: Timestamp uses Time, JSON is cast to String.
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::IntIsNull,
        false,
    ),
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Int,
        ScalarFuncSig::DecimalIsNull,
        false,
    ),
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real],
        EvalType::Int,
        ScalarFuncSig::RealIsNull,
        false,
    ),
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::Datetime)],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::TimeIsNull,
        false,
    ),
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::Timestamp)],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::TimeIsNull,
        false,
    ),
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::Duration)],
        &[EvalType::Duration],
        EvalType::Int,
        ScalarFuncSig::DurationIsNull,
        false,
    ),
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::String)],
        &[EvalType::String],
        EvalType::Int,
        ScalarFuncSig::StringIsNull,
        false,
    ),
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::Json)],
        &[EvalType::String],
        EvalType::Int,
        ScalarFuncSig::StringIsNull,
        false,
    ),
    signature(
        "isnull",
        &[ArgPattern::eval(EvalType::VectorFloat32)],
        &[EvalType::VectorFloat32],
        EvalType::Int,
        ScalarFuncSig::VectorFloat32IsNull,
        false,
    ),
    // `arithmeticPlusFunctionClass.getFunction`,
    // `arithmeticMinusFunctionClass.getFunction` and
    // `arithmeticMultiplyFunctionClass.getFunction`: a Real operand selects
    // the Real family, which is asked before the Decimal one.
    signature(
        "plus",
        &[ArgPattern::eval(EvalType::Real), ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::PlusReal,
        false,
    ),
    signature(
        "plus",
        &[ArgPattern::ANY, ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::PlusReal,
        false,
    ),
    signature(
        "minus",
        &[ArgPattern::eval(EvalType::Real), ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::MinusReal,
        false,
    ),
    signature(
        "minus",
        &[ArgPattern::ANY, ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::MinusReal,
        false,
    ),
    signature(
        "mul",
        &[ArgPattern::eval(EvalType::Real), ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::MultiplyReal,
        false,
    ),
    signature(
        "mul",
        &[ArgPattern::ANY, ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::MultiplyReal,
        false,
    ),
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
    // two decimal operands stay decimal and use MultiplyDecimal.
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
    // `builtin_arithmetic.go` `arithmeticDivideFunctionClass.getFunction`:
    // the result of `/` is never integral — real operands divide as Real,
    // everything else (the planner already inserted the decimal casts, as
    // q34's `div(cast(hd_dep_count, decimal), cast(hd_vehicle_count,
    // decimal)))` shows) divides as Decimal. Without these rows a `/` nested
    // inside a conditional fails the PB conversion of the whole conditional.
    signature(
        "div",
        &[
            ArgPattern::eval(EvalType::Real),
            ArgPattern::eval(EvalType::Real),
        ],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::DivideReal,
        false,
    ),
    signature(
        "div",
        &[
            ArgPattern::eval(EvalType::Decimal),
            ArgPattern::eval(EvalType::Decimal),
        ],
        &[EvalType::Decimal, EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::DivideDecimal,
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
    // `sqrtFunctionClass`, `expFunctionClass`, `log2FunctionClass`,
    // `log10FunctionClass`, `radiansFunctionClass`, `degreesFunctionClass`:
    // one `ETReal` argument, an `ETReal` result.
    signature("sqrt", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Real, ScalarFuncSig::Sqrt, false),
    signature("exp", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Real, ScalarFuncSig::Exp, false),
    signature("log2", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Real, ScalarFuncSig::Log2, false),
    signature("log10", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Real, ScalarFuncSig::Log10, false),
    signature("radians", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Real, ScalarFuncSig::Radians, false),
    signature("degrees", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Real, ScalarFuncSig::Degrees, false),
    // `logFunctionClass` (`ast.Ln` too): `Log1Arg` over one `ETReal`,
    // `Log2Args` over two.
    signature("ln", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Real, ScalarFuncSig::Log1Arg, false),
    signature("log", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Real, ScalarFuncSig::Log1Arg, false),
    signature(
        "log",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Real, EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::Log2Args,
        false,
    ),
    // `signFunctionClass`: an `ETReal` argument, an `ETInt` result.
    signature("sign", &[ArgPattern::ANY], &[EvalType::Real], EvalType::Int, ScalarFuncSig::Sign, false),
    // Go `compareFunctionClass` for `ast.NullEQ`: the comparison families'
    // `NullEQ*` signatures.
    signature(
        "nulleq",
        CMP_INT_PAIR,
        CMP_INT_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqInt,
        false,
    ),
    signature(
        "nulleq",
        CMP_REAL_PAIR,
        CMP_REAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqReal,
        false,
    ),
    signature(
        "nulleq",
        CMP_DECIMAL_PAIR,
        CMP_DECIMAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqDecimal,
        false,
    ),
    signature(
        "nulleq",
        CMP_STRING_PAIR,
        CMP_STRING_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqString,
        false,
    ),
    signature(
        "nulleq",
        CMP_DATETIME_PAIR,
        CMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqTime,
        false,
    ),
    signature(
        "nulleq",
        CMP_TIMESTAMP_PAIR,
        CMP_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqTime,
        false,
    ),
    signature(
        "nulleq",
        CMP_DATETIME_TIMESTAMP_PAIR,
        CMP_DATETIME_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqTime,
        false,
    ),
    signature(
        "nulleq",
        CMP_TIMESTAMP_DATETIME_PAIR,
        CMP_TIMESTAMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqTime,
        false,
    ),
    // `bitAndFunctionClass`, `bitOrFunctionClass`, `bitXorFunctionClass`,
    // `leftShiftFunctionClass`, `rightShiftFunctionClass`: two `ETInt`
    // arguments, an unsigned `ETInt` result; `bitNegFunctionClass` one.
    signature("bitand", &[ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::BitAndSig, false),
    signature("bitor", &[ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::BitOrSig, false),
    signature("bitxor", &[ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::BitXorSig, false),
    signature("leftshift", &[ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::LeftShift, false),
    signature("rightshift", &[ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::RightShift, false),
    signature("bitneg", &[ArgPattern::ANY], &[EvalType::Int], EvalType::Int, ScalarFuncSig::BitNegSig, false),
    // `logicXorFunctionClass`: both arguments wrapped `IsTrue` (`build_call`),
    // `LogicalXor` over two `ETInt`.
    signature("xor", &[ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::LogicalXor, false),
    // `asciiFunctionClass`, `bitLengthFunctionClass`: one `ETString`, an
    // `ETInt` result; `strcmpFunctionClass` two.
    signature("ascii", &[ArgPattern::ANY], &[EvalType::String], EvalType::Int, ScalarFuncSig::Ascii, false),
    signature("bit_length", &[ArgPattern::ANY], &[EvalType::String], EvalType::Int, ScalarFuncSig::BitLength, false),
    signature(
        "strcmp",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::String],
        EvalType::Int,
        ScalarFuncSig::Strcmp,
        false,
    ),
    // `reverseFunctionClass` / `rightFunctionClass`: a binary (or BIT)
    // argument answers the byte form, any other the UTF-8 one.
    string_signature("reverse", &[ArgPattern::string(true)], &[EvalType::String], ScalarFuncSig::Reverse, RetCollation::FirstArgString),
    string_signature("reverse", &[ArgPattern::string(false)], &[EvalType::String], ScalarFuncSig::ReverseUtf8, RetCollation::FirstArgString),
    string_signature(
        "right",
        &[ArgPattern::string(true), ArgPattern::ANY],
        &[EvalType::String, EvalType::Int],
        ScalarFuncSig::Right,
        RetCollation::FirstArgString,
    ),
    string_signature(
        "right",
        &[ArgPattern::string(false), ArgPattern::ANY],
        &[EvalType::String, EvalType::Int],
        ScalarFuncSig::RightUtf8,
        RetCollation::FirstArgString,
    ),
    // `replaceFunctionClass`: three `ETString`s; `trimLeftFunctionClass`,
    // `trimRightFunctionClass`: one; `spaceFunctionClass`: one `ETInt`.
    string_signature(
        "replace",
        &[ArgPattern::ANY, ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::String, EvalType::String],
        ScalarFuncSig::Replace,
        RetCollation::FirstArgString,
    ),
    string_signature("ltrim", &[ArgPattern::ANY], &[EvalType::String], ScalarFuncSig::LTrim, RetCollation::FirstArgString),
    string_signature("rtrim", &[ArgPattern::ANY], &[EvalType::String], ScalarFuncSig::RTrim, RetCollation::FirstArgString),
    string_signature("space", &[ArgPattern::ANY], &[EvalType::Int], ScalarFuncSig::Space, RetCollation::ConnectionString),
    // The date builtins' function classes: `dayOfMonthFunctionClass` &c over
    // one `ETDatetime` (an `ETInt` result; MONTHNAME's an `ETString`),
    // FROM_DAYS over one `ETInt`, MAKEDATE over two, MAKETIME over
    // `ETInt, ETInt, ETReal`, TIME_TO_SEC over `ETDuration`. PERIOD_ADD and
    // PERIOD_DIFF set no PbCode, so Go never pushes them.
    signature("dayofmonth", &[ArgPattern::ANY], &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::DayOfMonth, false),
    signature("dayofweek", &[ArgPattern::ANY], &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::DayOfWeek, false),
    signature("dayofyear", &[ArgPattern::ANY], &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::DayOfYear, false),
    signature("weekofyear", &[ArgPattern::ANY], &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::WeekOfYear, false),
    signature("year", &[ArgPattern::ANY], &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::Year, false),
    string_signature("monthname", &[ArgPattern::ANY], &[EvalType::Datetime], ScalarFuncSig::MonthName, RetCollation::ConnectionString),
    signature("from_days", &[ArgPattern::ANY], &[EvalType::Int], EvalType::Datetime, ScalarFuncSig::FromDays, false),
    signature("makedate", &[ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Datetime, ScalarFuncSig::MakeDate, false),
    signature(
        "maketime",
        &[ArgPattern::ANY, ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Int, EvalType::Int, EvalType::Real],
        EvalType::Duration,
        ScalarFuncSig::MakeTime,
        false,
    ),
    signature("time_to_sec", &[ArgPattern::ANY], &[EvalType::Duration], EvalType::Int, ScalarFuncSig::TimeToSec, false),
    // `sha1FunctionClass`: one `ETString`; `sha2FunctionClass`: `ETString,
    // ETInt`; `uncompressedLengthFunctionClass`: one `ETString`, an `ETInt`
    // result; `likeFunctionClass`: `ETString, ETString, ETInt` (the escape).
    string_signature("sha1", &[ArgPattern::ANY], &[EvalType::String], ScalarFuncSig::Sha1, RetCollation::ConnectionString),
    string_signature(
        "sha2",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::Int],
        ScalarFuncSig::Sha2,
        RetCollation::ConnectionString,
    ),
    signature("uncompressed_length", &[ArgPattern::ANY], &[EvalType::String], EvalType::Int, ScalarFuncSig::UncompressedLength, false),
    signature(
        "like",
        &[ArgPattern::ANY, ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::String, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::LikeSig,
        false,
    ),
    // The vector function classes (`builtin_vec.go`): `ETVectorFloat32`
    // arguments; VEC_DIMS answers `ETInt`, VEC_AS_TEXT `ETString`, the
    // distances and the norm `ETReal`.
    signature("vec_dims", &[ArgPattern::ANY], &[EvalType::VectorFloat32], EvalType::Int, ScalarFuncSig::VecDimsSig, false),
    signature("vec_l2_norm", &[ArgPattern::ANY], &[EvalType::VectorFloat32], EvalType::Real, ScalarFuncSig::VecL2NormSig, false),
    string_signature("vec_as_text", &[ArgPattern::ANY], &[EvalType::VectorFloat32], ScalarFuncSig::VecAsTextSig, RetCollation::ConnectionString),
    signature(
        "vec_l1_distance",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::VectorFloat32, EvalType::VectorFloat32],
        EvalType::Real,
        ScalarFuncSig::VecL1DistanceSig,
        false,
    ),
    signature(
        "vec_l2_distance",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::VectorFloat32, EvalType::VectorFloat32],
        EvalType::Real,
        ScalarFuncSig::VecL2DistanceSig,
        false,
    ),
    signature(
        "vec_negative_inner_product",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::VectorFloat32, EvalType::VectorFloat32],
        EvalType::Real,
        ScalarFuncSig::VecNegativeInnerProductSig,
        false,
    ),
    signature(
        "vec_cosine_distance",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::VectorFloat32, EvalType::VectorFloat32],
        EvalType::Real,
        ScalarFuncSig::VecCosineDistanceSig,
        false,
    ),
    // `uuidFunctionClass`: no argument; `uuidTimestampFunctionClass` /
    // `uuidVersionFunctionClass`: one `ETString`, an `ETDecimal` / `ETInt`
    // result. `sysDateFunctionClass`: `SysDateWithoutFsp`, or
    // `SysDateWithFsp` over an `ETInt` fsp.
    string_signature("uuid", &[], &[], ScalarFuncSig::Uuid, RetCollation::ConnectionString),
    signature("uuid_timestamp", &[ArgPattern::ANY], &[EvalType::String], EvalType::Decimal, ScalarFuncSig::UuidTimestamp, false),
    signature("uuid_version", &[ArgPattern::ANY], &[EvalType::String], EvalType::Int, ScalarFuncSig::UuidVersion, false),
    signature("sysdate", &[], &[], EvalType::Datetime, ScalarFuncSig::SysDateWithoutFsp, false),
    signature("sysdate", &[ArgPattern::ANY], &[EvalType::Int], EvalType::Datetime, ScalarFuncSig::SysDateWithFsp, false),
    // `piFunctionClass.getFunction`: no arguments, and `PI()` is a constant on
    // both sides of the wire.
    signature("pi", &[], &[], EvalType::Real, ScalarFuncSig::Pi, false),
    // Go wrapWithIsTrue selects the truth signature from the argument domain;
    // nonnumeric domains use an implicit Real cast and NULL is preserved.
    signature(
        "istrue_with_null",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::IntIsTrueWithNull,
        false,
    ),
    signature(
        "istrue_with_null",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Int,
        ScalarFuncSig::DecimalIsTrueWithNull,
        false,
    ),
    signature(
        "istrue_with_null",
        &[ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real],
        EvalType::Int,
        ScalarFuncSig::RealIsTrueWithNull,
        false,
    ),
    // `unaryNotFunctionClass.getFunction`: a temporal argument is negated
    // as `ETInt`, a string one as `ETReal`, any other in its own type.
    signature(
        "not",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::UnaryNotInt,
        false,
    ),
    signature(
        "not",
        &[ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real],
        EvalType::Int,
        ScalarFuncSig::UnaryNotReal,
        false,
    ),
    signature(
        "not",
        &[ArgPattern::eval(EvalType::String)],
        &[EvalType::Real],
        EvalType::Int,
        ScalarFuncSig::UnaryNotReal,
        false,
    ),
    signature(
        "not",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Int,
        ScalarFuncSig::UnaryNotDecimal,
        false,
    ),
    signature(
        "not",
        &[ArgPattern::eval(EvalType::Json)],
        &[EvalType::Json],
        EvalType::Int,
        ScalarFuncSig::UnaryNotJson,
        false,
    ),
    signature(
        "not",
        &[ArgPattern::eval(EvalType::Datetime)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::UnaryNotInt,
        false,
    ),
    signature(
        "not",
        &[ArgPattern::eval(EvalType::Timestamp)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::UnaryNotInt,
        false,
    ),
    signature(
        "not",
        &[ArgPattern::eval(EvalType::Duration)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::UnaryNotInt,
        false,
    ),
    // The internal cast nodes the planner emits around columns whose type a
    // Real/Decimal slot does not accept (Go wraps the same operands with
    // `WrapWithCastAsReal`/`WrapWithCastAsDecimal`). The result copies the
    // first argument's UNSIGNED flag, exactly as Go's cast signatures do.
    signature(
        "cast_double",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Real,
        ScalarFuncSig::CastIntAsReal,
        true,
    ),
    signature(
        "cast_double",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Real,
        ScalarFuncSig::CastDecimalAsReal,
        true,
    ),
    signature(
        "cast_double",
        &[ArgPattern::eval(EvalType::String)],
        &[EvalType::String],
        EvalType::Real,
        ScalarFuncSig::CastStringAsReal,
        true,
    ),
    signature(
        "cast_decimal",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Decimal,
        ScalarFuncSig::CastIntAsDecimal,
        true,
    ),
    signature(
        "cast_decimal",
        &[ArgPattern::eval(EvalType::String)],
        &[EvalType::String],
        EvalType::Decimal,
        ScalarFuncSig::CastStringAsDecimal,
        true,
    ),
    // The integer arithmetic family, and the comparisons that compose it.
    // Go `arithmeticPlusFunctionClass.getFunction`: two ints are PlusInt with
    // the result unsigned when either operand is unsigned; the four
    // signedness pairs are four rows so the selector can see them. The
    // comparisons and the logical connectives are the shapes Go's rewriter
    // nests UNDER `or`/`and` when a whole predicate tree travels.
    signature(
        "plus",
        &[ArgPattern::int(false), ArgPattern::int(false)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::PlusInt,
        false,
    ),
    signature(
        "plus",
        &[ArgPattern::int(true), ArgPattern::int(false)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::PlusInt,
        false,
    ),
    signature(
        "plus",
        &[ArgPattern::int(false), ArgPattern::int(true)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::PlusInt,
        false,
    ),
    signature(
        "plus",
        &[ArgPattern::int(true), ArgPattern::int(true)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::PlusInt,
        false,
    ),
    // `arithmeticMinusFunctionClass.getFunction`: two ints are MinusInt
    // (the result's UNSIGNED flag comes from the planner's own result type,
    // which also honours NO_UNSIGNED_SUBTRACTION).
    signature(
        "minus",
        &[ArgPattern::int(false), ArgPattern::int(false)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::MinusInt,
        false,
    ),
    signature(
        "minus",
        &[ArgPattern::int(true), ArgPattern::int(false)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::MinusInt,
        false,
    ),
    signature(
        "minus",
        &[ArgPattern::int(false), ArgPattern::int(true)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::MinusInt,
        false,
    ),
    signature(
        "minus",
        &[ArgPattern::int(true), ArgPattern::int(true)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::MinusInt,
        false,
    ),
    // `arithmeticMultiplyFunctionClass.getFunction`: two ints are MultiplyInt,
    // or MultiplyIntUnsigned when either operand is unsigned.
    signature(
        "mul",
        &[ArgPattern::int(false), ArgPattern::int(false)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::MultiplyInt,
        false,
    ),
    signature(
        "mul",
        &[ArgPattern::int(true), ArgPattern::int(false)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::MultiplyIntUnsigned,
        false,
    ),
    signature(
        "mul",
        &[ArgPattern::int(false), ArgPattern::int(true)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::MultiplyIntUnsigned,
        false,
    ),
    signature(
        "mul",
        &[ArgPattern::int(true), ArgPattern::int(true)],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::MultiplyIntUnsigned,
        false,
    ),
    // `absFunctionClass.getFunction`: one signature per argument evaluation
    // type (every type outside Int/Decimal/Real evaluates as Real); the result
    // copies the argument's UNSIGNED flag.
    signature(
        "abs",
        &[ArgPattern::int(false)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::AbsInt,
        true,
    ),
    signature(
        "abs",
        &[ArgPattern::int(true)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::AbsUInt,
        true,
    ),
    signature(
        "abs",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Decimal,
        ScalarFuncSig::AbsDecimal,
        true,
    ),
    signature(
        "abs",
        &[ArgPattern::ANY],
        &[EvalType::Real],
        EvalType::Real,
        ScalarFuncSig::AbsReal,
        true,
    ),
    // `isTrueOrFalseFunctionClass.getFunction` for `IS TRUE` / `IS FALSE`
    // (Go `ast.IsTruthWithoutNull` / `ast.IsFalsity`, keepNull false): the
    // signature follows the argument domain, nonnumeric domains as Real.
    signature(
        "istrue",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::IntIsTrue,
        false,
    ),
    signature(
        "istrue",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Int,
        ScalarFuncSig::DecimalIsTrue,
        false,
    ),
    signature(
        "istrue",
        &[ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real],
        EvalType::Int,
        ScalarFuncSig::RealIsTrue,
        false,
    ),
    signature(
        "isfalse",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::IntIsFalse,
        false,
    ),
    signature(
        "isfalse",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Int,
        ScalarFuncSig::DecimalIsFalse,
        false,
    ),
    signature(
        "isfalse",
        &[ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real],
        EvalType::Int,
        ScalarFuncSig::RealIsFalse,
        false,
    ),
    // `md5FunctionClass` / `crc32FunctionClass`: one `ETString` argument.
    // The wire carries the planned function's own result type (MD5's
    // connection charset and flen 32; CRC32's unsigned flen 10).
    string_signature(
        "md5",
        &[ArgPattern::any_string()],
        &[EvalType::String],
        ScalarFuncSig::Md5,
        RetCollation::ConnectionString,
    ),
    signature(
        "crc32",
        &[ArgPattern::any_string()],
        &[EvalType::String],
        EvalType::Int,
        ScalarFuncSig::Crc32,
        false,
    ),
    // `lengthFunctionClass.getFunction`: the byte length of an ETString
    // argument, one signature whatever its collation.
    signature(
        "length",
        &[ArgPattern::any_string()],
        &[EvalType::String],
        EvalType::Int,
        ScalarFuncSig::Length,
        false,
    ),
    // Go `compareFunctionClass.getFunction` over ETDuration, ETJson and
    // ETVectorFloat32 (`genCompareFunctionClass`'s `*Duration`, `*Json` and
    // `*VectorFloat32` signatures); `scalarExprSupportedByTiKV` admits every
    // comparison.
    signature(
        "lt",
        CMP_DURATION_PAIR,
        CMP_DURATION_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtDuration,
        false,
    ),
    signature(
        "le",
        CMP_DURATION_PAIR,
        CMP_DURATION_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeDuration,
        false,
    ),
    signature(
        "gt",
        CMP_DURATION_PAIR,
        CMP_DURATION_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtDuration,
        false,
    ),
    signature(
        "ge",
        CMP_DURATION_PAIR,
        CMP_DURATION_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeDuration,
        false,
    ),
    signature(
        "eq",
        CMP_DURATION_PAIR,
        CMP_DURATION_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqDuration,
        false,
    ),
    signature(
        "ne",
        CMP_DURATION_PAIR,
        CMP_DURATION_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeDuration,
        false,
    ),
    signature(
        "nulleq",
        CMP_DURATION_PAIR,
        CMP_DURATION_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqDuration,
        false,
    ),
    signature(
        "lt",
        CMP_JSON_PAIR,
        CMP_JSON_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtJson,
        false,
    ),
    signature(
        "le",
        CMP_JSON_PAIR,
        CMP_JSON_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeJson,
        false,
    ),
    signature(
        "gt",
        CMP_JSON_PAIR,
        CMP_JSON_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtJson,
        false,
    ),
    signature(
        "ge",
        CMP_JSON_PAIR,
        CMP_JSON_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeJson,
        false,
    ),
    signature(
        "eq",
        CMP_JSON_PAIR,
        CMP_JSON_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqJson,
        false,
    ),
    signature(
        "ne",
        CMP_JSON_PAIR,
        CMP_JSON_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeJson,
        false,
    ),
    signature(
        "nulleq",
        CMP_JSON_PAIR,
        CMP_JSON_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqJson,
        false,
    ),
    signature(
        "lt",
        CMP_VECTOR_FLOAT32_PAIR,
        CMP_VECTOR_FLOAT32_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtVectorFloat32,
        false,
    ),
    signature(
        "le",
        CMP_VECTOR_FLOAT32_PAIR,
        CMP_VECTOR_FLOAT32_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeVectorFloat32,
        false,
    ),
    signature(
        "gt",
        CMP_VECTOR_FLOAT32_PAIR,
        CMP_VECTOR_FLOAT32_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtVectorFloat32,
        false,
    ),
    signature(
        "ge",
        CMP_VECTOR_FLOAT32_PAIR,
        CMP_VECTOR_FLOAT32_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeVectorFloat32,
        false,
    ),
    signature(
        "eq",
        CMP_VECTOR_FLOAT32_PAIR,
        CMP_VECTOR_FLOAT32_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqVectorFloat32,
        false,
    ),
    signature(
        "ne",
        CMP_VECTOR_FLOAT32_PAIR,
        CMP_VECTOR_FLOAT32_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeVectorFloat32,
        false,
    ),
    signature(
        "nulleq",
        CMP_VECTOR_FLOAT32_PAIR,
        CMP_VECTOR_FLOAT32_TYPES,
        EvalType::Int,
        ScalarFuncSig::NullEqVectorFloat32,
        false,
    ),
    // `inFunctionClass.getFunction`: the planner has cast every argument to
    // the comparison type, which selects the signature; the selector reads
    // the tested argument and `resolve_in` checks the rest share its type.
    signature(
        "in",
        &[ArgPattern::eval(EvalType::Int)],
        &[EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::InInt,
        false,
    ),
    signature(
        "in",
        &[ArgPattern::eval(EvalType::String)],
        &[EvalType::String],
        EvalType::Int,
        ScalarFuncSig::InString,
        false,
    ),
    signature(
        "in",
        &[ArgPattern::eval(EvalType::Real)],
        &[EvalType::Real],
        EvalType::Int,
        ScalarFuncSig::InReal,
        false,
    ),
    signature(
        "in",
        &[ArgPattern::eval(EvalType::Decimal)],
        &[EvalType::Decimal],
        EvalType::Int,
        ScalarFuncSig::InDecimal,
        false,
    ),
    signature(
        "in",
        &[ArgPattern::eval(EvalType::Datetime)],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::InTime,
        false,
    ),
    signature(
        "in",
        &[ArgPattern::eval(EvalType::Timestamp)],
        &[EvalType::Timestamp],
        EvalType::Int,
        ScalarFuncSig::InTime,
        false,
    ),
    signature(
        "in",
        &[ArgPattern::eval(EvalType::Duration)],
        &[EvalType::Duration],
        EvalType::Int,
        ScalarFuncSig::InDuration,
        false,
    ),
    signature(
        "in",
        &[ArgPattern::eval(EvalType::Json)],
        &[EvalType::Json],
        EvalType::Int,
        ScalarFuncSig::InJson,
        false,
    ),
    // `coalesceFunctionClass.getFunction`: the result type is inferred from
    // every argument (`InferType4ControlFuncs`) and selects the signature.
    signature("coalesce", &[], &[], EvalType::Int, ScalarFuncSig::CoalesceInt, false),
    signature("coalesce", &[], &[], EvalType::Real, ScalarFuncSig::CoalesceReal, false),
    signature("coalesce", &[], &[], EvalType::Decimal, ScalarFuncSig::CoalesceDecimal, false),
    string_signature(
        "coalesce",
        &[],
        &[],
        ScalarFuncSig::CoalesceString,
        RetCollation::ControlBranches,
    ),
    signature("coalesce", &[], &[], EvalType::Datetime, ScalarFuncSig::CoalesceTime, false),
    signature("coalesce", &[], &[], EvalType::Duration, ScalarFuncSig::CoalesceDuration, false),
    signature("coalesce", &[], &[], EvalType::Json, ScalarFuncSig::CoalesceJson, false),
    // Go `compareFunctionClass.generateCmpSigs`: one signature per
    // comparison type, `ETDatetime` and `ETTimestamp` sharing the `*Time`
    // family. `NullEQ` and the Duration/JSON families are absent because
    // `pb_to_expr` (Go `getSignatureByPB`) does not decode them here yet.
    signature(
        "lt",
        CMP_INT_PAIR,
        CMP_INT_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtInt,
        false,
    ),
    signature(
        "le",
        CMP_INT_PAIR,
        CMP_INT_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeInt,
        false,
    ),
    signature(
        "gt",
        CMP_INT_PAIR,
        CMP_INT_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtInt,
        false,
    ),
    signature(
        "ge",
        CMP_INT_PAIR,
        CMP_INT_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeInt,
        false,
    ),
    signature(
        "eq",
        CMP_INT_PAIR,
        CMP_INT_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqInt,
        false,
    ),
    signature(
        "ne",
        CMP_INT_PAIR,
        CMP_INT_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeInt,
        false,
    ),
    signature(
        "lt",
        CMP_REAL_PAIR,
        CMP_REAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtReal,
        false,
    ),
    signature(
        "le",
        CMP_REAL_PAIR,
        CMP_REAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeReal,
        false,
    ),
    signature(
        "gt",
        CMP_REAL_PAIR,
        CMP_REAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtReal,
        false,
    ),
    signature(
        "ge",
        CMP_REAL_PAIR,
        CMP_REAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeReal,
        false,
    ),
    signature(
        "eq",
        CMP_REAL_PAIR,
        CMP_REAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqReal,
        false,
    ),
    signature(
        "ne",
        CMP_REAL_PAIR,
        CMP_REAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeReal,
        false,
    ),
    signature(
        "lt",
        CMP_DECIMAL_PAIR,
        CMP_DECIMAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtDecimal,
        false,
    ),
    signature(
        "le",
        CMP_DECIMAL_PAIR,
        CMP_DECIMAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeDecimal,
        false,
    ),
    signature(
        "gt",
        CMP_DECIMAL_PAIR,
        CMP_DECIMAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtDecimal,
        false,
    ),
    signature(
        "ge",
        CMP_DECIMAL_PAIR,
        CMP_DECIMAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeDecimal,
        false,
    ),
    signature(
        "eq",
        CMP_DECIMAL_PAIR,
        CMP_DECIMAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqDecimal,
        false,
    ),
    signature(
        "ne",
        CMP_DECIMAL_PAIR,
        CMP_DECIMAL_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeDecimal,
        false,
    ),
    signature(
        "lt",
        CMP_STRING_PAIR,
        CMP_STRING_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtString,
        false,
    ),
    signature(
        "le",
        CMP_STRING_PAIR,
        CMP_STRING_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeString,
        false,
    ),
    signature(
        "gt",
        CMP_STRING_PAIR,
        CMP_STRING_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtString,
        false,
    ),
    signature(
        "ge",
        CMP_STRING_PAIR,
        CMP_STRING_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeString,
        false,
    ),
    signature(
        "eq",
        CMP_STRING_PAIR,
        CMP_STRING_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqString,
        false,
    ),
    signature(
        "ne",
        CMP_STRING_PAIR,
        CMP_STRING_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeString,
        false,
    ),
    signature(
        "lt",
        CMP_DATETIME_PAIR,
        CMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtTime,
        false,
    ),
    signature(
        "le",
        CMP_DATETIME_PAIR,
        CMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeTime,
        false,
    ),
    signature(
        "gt",
        CMP_DATETIME_PAIR,
        CMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtTime,
        false,
    ),
    signature(
        "ge",
        CMP_DATETIME_PAIR,
        CMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeTime,
        false,
    ),
    signature(
        "eq",
        CMP_DATETIME_PAIR,
        CMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqTime,
        false,
    ),
    signature(
        "ne",
        CMP_DATETIME_PAIR,
        CMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeTime,
        false,
    ),
    signature(
        "lt",
        CMP_TIMESTAMP_PAIR,
        CMP_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtTime,
        false,
    ),
    signature(
        "le",
        CMP_TIMESTAMP_PAIR,
        CMP_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeTime,
        false,
    ),
    signature(
        "gt",
        CMP_TIMESTAMP_PAIR,
        CMP_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtTime,
        false,
    ),
    signature(
        "ge",
        CMP_TIMESTAMP_PAIR,
        CMP_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeTime,
        false,
    ),
    signature(
        "eq",
        CMP_TIMESTAMP_PAIR,
        CMP_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqTime,
        false,
    ),
    signature(
        "ne",
        CMP_TIMESTAMP_PAIR,
        CMP_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeTime,
        false,
    ),
    signature(
        "lt",
        CMP_DATETIME_TIMESTAMP_PAIR,
        CMP_DATETIME_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtTime,
        false,
    ),
    signature(
        "le",
        CMP_DATETIME_TIMESTAMP_PAIR,
        CMP_DATETIME_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeTime,
        false,
    ),
    signature(
        "gt",
        CMP_DATETIME_TIMESTAMP_PAIR,
        CMP_DATETIME_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtTime,
        false,
    ),
    signature(
        "ge",
        CMP_DATETIME_TIMESTAMP_PAIR,
        CMP_DATETIME_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeTime,
        false,
    ),
    signature(
        "eq",
        CMP_DATETIME_TIMESTAMP_PAIR,
        CMP_DATETIME_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqTime,
        false,
    ),
    signature(
        "ne",
        CMP_DATETIME_TIMESTAMP_PAIR,
        CMP_DATETIME_TIMESTAMP_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeTime,
        false,
    ),
    signature(
        "lt",
        CMP_TIMESTAMP_DATETIME_PAIR,
        CMP_TIMESTAMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::LtTime,
        false,
    ),
    signature(
        "le",
        CMP_TIMESTAMP_DATETIME_PAIR,
        CMP_TIMESTAMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::LeTime,
        false,
    ),
    signature(
        "gt",
        CMP_TIMESTAMP_DATETIME_PAIR,
        CMP_TIMESTAMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::GtTime,
        false,
    ),
    signature(
        "ge",
        CMP_TIMESTAMP_DATETIME_PAIR,
        CMP_TIMESTAMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::GeTime,
        false,
    ),
    signature(
        "eq",
        CMP_TIMESTAMP_DATETIME_PAIR,
        CMP_TIMESTAMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::EqTime,
        false,
    ),
    signature(
        "ne",
        CMP_TIMESTAMP_DATETIME_PAIR,
        CMP_TIMESTAMP_DATETIME_TYPES,
        EvalType::Int,
        ScalarFuncSig::NeTime,
        false,
    ),
    signature(
        "or",
        &[
            ArgPattern::eval(EvalType::Int),
            ArgPattern::eval(EvalType::Int),
        ],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::LogicalOr,
        false,
    ),
    signature(
        "and",
        &[
            ArgPattern::eval(EvalType::Int),
            ArgPattern::eval(EvalType::Int),
        ],
        &[EvalType::Int, EvalType::Int],
        EvalType::Int,
        ScalarFuncSig::LogicalAnd,
        false,
    ),
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
    // `tidbToBinaryFunctionClass` / `tidbFromBinaryFunctionClass`: the
    // charset boundary `HandleBinaryLiteral` inserts. The node carries its
    // own result type (the target charset), so the call's metadata is read
    // from the expression the way a control function's is.
    string_signature(
        "to_binary",
        &[ArgPattern::any_string()],
        &[EvalType::String],
        ScalarFuncSig::ToBinary,
        RetCollation::ControlBranches,
    ),
    string_signature(
        "from_binary",
        &[ArgPattern::any_string()],
        &[EvalType::String],
        ScalarFuncSig::FromBinary,
        RetCollation::ControlBranches,
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
    // JSON leaves are carried with their concrete `MysqlJson` type and the
    // candidate value is wrapped by the normal implicit cast path below.
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
    // Go's date/time builtins.  Their signatures are selected from the
    // evaluation domain, while the scalar protobuf carries the concrete
    // temporal field type (including FSP) on each child.
    string_signature(
        "date_format",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Datetime, EvalType::String],
        ScalarFuncSig::DateFormatSig,
        RetCollation::SecondArgString,
    ),
    signature(
        "date",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Datetime,
        ScalarFuncSig::Date,
        false,
    ),
    signature(
        "hour",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::Hour,
        false,
    ),
    signature(
        "minute",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::Minute,
        false,
    ),
    signature(
        "second",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::Second,
        false,
    ),
    signature(
        "microsecond",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::MicroSecond,
        false,
    ),
    signature(
        "month",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::Month,
        false,
    ),
    signature(
        "week",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::WeekWithoutMode,
        false,
    ),
    signature(
        "datediff",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Datetime, EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::DateDiff,
        false,
    ),
    signature(
        "from_unixtime",
        &[ArgPattern::ANY],
        &[EvalType::Decimal],
        EvalType::Datetime,
        ScalarFuncSig::FromUnixTime1Arg,
        false,
    ),
    string_signature(
        "from_unixtime",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Decimal, EvalType::String],
        ScalarFuncSig::FromUnixTime2Arg,
        RetCollation::ConnectionString,
    ),
    signature(
        "timestampdiff",
        &[ArgPattern::ANY, ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::Datetime, EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::TimestampDiff,
        false,
    ),
    // Go `extractFunctionClass.getFunction` picks between these three by the
    // unit and the value's type (`resolve_extract`).
    signature(
        "extract",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::String],
        EvalType::Int,
        ScalarFuncSig::ExtractDatetimeFromString,
        false,
    ),
    signature(
        "extract",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::ExtractDatetime,
        false,
    ),
    signature(
        "extract",
        &[ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::String, EvalType::Duration],
        EvalType::Int,
        ScalarFuncSig::ExtractDuration,
        false,
    ),
    signature("unix_timestamp", ANY_0, &[], EvalType::Int, ScalarFuncSig::UnixTimestampCurrent, false),
    signature(
        "unix_timestamp",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Int,
        ScalarFuncSig::UnixTimestampInt,
        false,
    ),
    signature(
        "unix_timestamp",
        &[ArgPattern::ANY],
        &[EvalType::Datetime],
        EvalType::Decimal,
        ScalarFuncSig::UnixTimestampDec,
        false,
    ),
    // Go wraps a string argument in CAST(... AS DATETIME) before selecting
    // UnixTimestampDec.  The same row also admits a DATE/ TIMESTAMP column.
    signature(
        "unix_timestamp",
        &[ArgPattern::eval(EvalType::String)],
        &[EvalType::Datetime],
        EvalType::Decimal,
        ScalarFuncSig::UnixTimestampDec,
        false,
    ),
    //
    // GO's conditional family (inferType4ControlFuncs picks the signature by
    // the RESULT type; real upstream tipb numbers). `case` has a variable
    // arity (cond/value pairs + an optional else), so these rows carry an
    // EMPTY selector: the generic matcher above can never select them, and
    // [`resolve_conditional`] answers for this family instead.
    string_signature(
        "case",
        &[],
        &[],
        ScalarFuncSig::CaseWhenString,
        RetCollation::ControlBranches,
    ),
    string_signature(
        "if",
        &[],
        &[],
        ScalarFuncSig::IfString,
        RetCollation::ControlBranches,
    ),
    string_signature(
        "ifnull",
        &[],
        &[],
        ScalarFuncSig::IfNullString,
        RetCollation::ControlBranches,
    ),
    signature(
        "case",
        &[],
        &[],
        EvalType::Int,
        ScalarFuncSig::CaseWhenInt,
        false,
    ),
    signature(
        "case",
        &[],
        &[],
        EvalType::Real,
        ScalarFuncSig::CaseWhenReal,
        false,
    ),
    signature(
        "case",
        &[],
        &[],
        EvalType::Decimal,
        ScalarFuncSig::CaseWhenDecimal,
        false,
    ),
    signature(
        "case",
        &[],
        &[],
        EvalType::Datetime,
        ScalarFuncSig::CaseWhenTime,
        false,
    ),
    signature(
        "case",
        &[],
        &[],
        EvalType::Duration,
        ScalarFuncSig::CaseWhenDuration,
        false,
    ),
    signature(
        "case",
        &[],
        &[],
        EvalType::Json,
        ScalarFuncSig::CaseWhenJson,
        false,
    ),
    signature("if", &[], &[], EvalType::Int, ScalarFuncSig::IfInt, false),
    signature("if", &[], &[], EvalType::Real, ScalarFuncSig::IfReal, false),
    signature(
        "if",
        &[],
        &[],
        EvalType::Decimal,
        ScalarFuncSig::IfDecimal,
        false,
    ),
    signature(
        "if",
        &[],
        &[],
        EvalType::Datetime,
        ScalarFuncSig::IfTime,
        false,
    ),
    signature(
        "if",
        &[],
        &[],
        EvalType::Duration,
        ScalarFuncSig::IfDuration,
        false,
    ),
    signature("if", &[], &[], EvalType::Json, ScalarFuncSig::IfJson, false),
    signature(
        "ifnull",
        &[],
        &[],
        EvalType::Int,
        ScalarFuncSig::IfNullInt,
        false,
    ),
    signature(
        "ifnull",
        &[],
        &[],
        EvalType::Real,
        ScalarFuncSig::IfNullReal,
        false,
    ),
    signature(
        "ifnull",
        &[],
        &[],
        EvalType::Decimal,
        ScalarFuncSig::IfNullDecimal,
        false,
    ),
    signature(
        "ifnull",
        &[],
        &[],
        EvalType::Datetime,
        ScalarFuncSig::IfNullTime,
        false,
    ),
    signature(
        "ifnull",
        &[],
        &[],
        EvalType::Duration,
        ScalarFuncSig::IfNullDuration,
        false,
    ),
    signature(
        "ifnull",
        &[],
        &[],
        EvalType::Json,
        ScalarFuncSig::IfNullJson,
        false,
    ),
    // Every builtin whose `getFunction` picks one signature per arity
    // (`pkg/expression/builtin_*.go`), with the evaluation types it casts its
    // arguments to.
    string_signature("aes_decrypt", ANY_2, &[EvalType::String, EvalType::String], ScalarFuncSig::AesDecrypt, RetCollation::FunctionType),
    string_signature("aes_decrypt", ANY_3, &[EvalType::String, EvalType::String, EvalType::String], ScalarFuncSig::AesDecrypt, RetCollation::FunctionType),
    string_signature("aes_encrypt", ANY_2, &[EvalType::String, EvalType::String], ScalarFuncSig::AesEncrypt, RetCollation::FunctionType),
    string_signature("aes_encrypt", ANY_3, &[EvalType::String, EvalType::String, EvalType::String], ScalarFuncSig::AesEncrypt, RetCollation::FunctionType),
    string_signature("bin", ANY_1, &[EvalType::Int], ScalarFuncSig::Bin, RetCollation::FunctionType),
    string_signature("compress", ANY_1, &[EvalType::String], ScalarFuncSig::Compress, RetCollation::FunctionType),
    signature("convert_tz", ANY_3, &[EvalType::Datetime, EvalType::String, EvalType::String], EvalType::Datetime, ScalarFuncSig::ConvertTz, false),
    signature("current_time", ANY_1, &[EvalType::Int], EvalType::Duration, ScalarFuncSig::CurrentTime1Arg, false),
    signature("current_timestamp", ANY_1, &[EvalType::Int], EvalType::Datetime, ScalarFuncSig::NowWithArg, false),
    signature("curtime", ANY_1, &[EvalType::Int], EvalType::Duration, ScalarFuncSig::CurrentTime1Arg, false),
    signature("day", ANY_1, &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::DayOfMonth, false),
    string_signature("dayname", ANY_1, &[EvalType::Datetime], ScalarFuncSig::DayName, RetCollation::FunctionType),
    string_signature("decode", ANY_2, &[EvalType::String, EvalType::String], ScalarFuncSig::Decode, RetCollation::FunctionType),
    string_signature("encode", ANY_2, &[EvalType::String, EvalType::String], ScalarFuncSig::Encode, RetCollation::FunctionType),
    string_signature("export_set", ANY_3, &[EvalType::Int, EvalType::String, EvalType::String], ScalarFuncSig::ExportSet3Arg, RetCollation::FunctionType),
    signature("find_in_set", ANY_2, &[EvalType::String, EvalType::String], EvalType::Int, ScalarFuncSig::FindInSet, false),
    string_signature("from_base64", ANY_1, &[EvalType::String], ScalarFuncSig::FromBase64, RetCollation::FunctionType),
    string_signature("get_format", ANY_2, &[EvalType::String, EvalType::String], ScalarFuncSig::GetFormat, RetCollation::FunctionType),
    signature("ilike", ANY_3, &[EvalType::String, EvalType::String, EvalType::Int], EvalType::Int, ScalarFuncSig::IlikeSig, false),
    string_signature("inet6_aton", ANY_1, &[EvalType::String], ScalarFuncSig::Inet6Aton, RetCollation::FunctionType),
    string_signature("inet6_ntoa", ANY_1, &[EvalType::String], ScalarFuncSig::Inet6Ntoa, RetCollation::FunctionType),
    signature("inet_aton", ANY_1, &[EvalType::String], EvalType::Int, ScalarFuncSig::InetAton, false),
    string_signature("inet_ntoa", ANY_1, &[EvalType::Int], ScalarFuncSig::InetNtoa, RetCollation::FunctionType),
    signature("is_ipv4", ANY_1, &[EvalType::String], EvalType::Int, ScalarFuncSig::IsIPv4, false),
    signature("is_ipv4_compat", ANY_1, &[EvalType::String], EvalType::Int, ScalarFuncSig::IsIPv4Compat, false),
    signature("is_ipv4_mapped", ANY_1, &[EvalType::String], EvalType::Int, ScalarFuncSig::IsIPv4Mapped, false),
    signature("is_ipv6", ANY_1, &[EvalType::String], EvalType::Int, ScalarFuncSig::IsIPv6, false),
    signature("is_uuid", ANY_1, &[EvalType::String], EvalType::Int, ScalarFuncSig::IsUuid, false),
    signature("json_depth", ANY_1, &[EvalType::Json], EvalType::Int, ScalarFuncSig::JsonDepthSig, false),
    string_signature("json_pretty", ANY_1, &[EvalType::Json], ScalarFuncSig::JsonPrettySig, RetCollation::FunctionType),
    string_signature("json_quote", ANY_1, &[EvalType::String], ScalarFuncSig::JsonQuoteSig, RetCollation::FunctionType),
    signature("json_storage_free", ANY_1, &[EvalType::Json], EvalType::Int, ScalarFuncSig::JsonStorageFreeSig, false),
    signature("json_storage_size", ANY_1, &[EvalType::Json], EvalType::Int, ScalarFuncSig::JsonStorageSizeSig, false),
    signature("last_day", ANY_1, &[EvalType::Datetime], EvalType::Datetime, ScalarFuncSig::LastDay, false),
    signature("last_insert_id", ANY_1, &[EvalType::Int], EvalType::Int, ScalarFuncSig::LastInsertIdWithId, false),
    signature("localtime", ANY_1, &[EvalType::Int], EvalType::Datetime, ScalarFuncSig::NowWithArg, false),
    signature("localtimestamp", ANY_1, &[EvalType::Int], EvalType::Datetime, ScalarFuncSig::NowWithArg, false),
    signature("now", ANY_1, &[EvalType::Int], EvalType::Datetime, ScalarFuncSig::NowWithArg, false),
    signature("octet_length", ANY_1, &[EvalType::String], EvalType::Int, ScalarFuncSig::Length, false),
    signature("ord", ANY_1, &[EvalType::String], EvalType::Int, ScalarFuncSig::Ord, false),
    string_signature("password", ANY_1, &[EvalType::String], ScalarFuncSig::Password, RetCollation::FunctionType),
    signature("quarter", ANY_1, &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::Quarter, false),
    string_signature("quote", ANY_1, &[EvalType::String], ScalarFuncSig::Quote, RetCollation::FunctionType),
    string_signature("repeat", ANY_2, &[EvalType::String, EvalType::Int], ScalarFuncSig::Repeat, RetCollation::FunctionType),
    signature("sec_to_time", ANY_1, &[EvalType::Real], EvalType::Duration, ScalarFuncSig::SecToTime, false),
    string_signature("sha", ANY_1, &[EvalType::String], ScalarFuncSig::Sha1, RetCollation::FunctionType),
    string_signature("substring_index", ANY_3, &[EvalType::String, EvalType::String, EvalType::Int], ScalarFuncSig::SubstringIndex, RetCollation::FunctionType),
    signature("tan", ANY_1, &[EvalType::Real], EvalType::Real, ScalarFuncSig::Tan, false),
    signature("tidb_shard", ANY_1, &[EvalType::Int], EvalType::Int, ScalarFuncSig::TiDbShard, false),
    signature("time", ANY_1, &[EvalType::String], EvalType::Duration, ScalarFuncSig::Time, false),
    string_signature("time_format", ANY_2, &[EvalType::Duration, EvalType::String], ScalarFuncSig::TimeFormat, RetCollation::FunctionType),
    signature("timestamp", ANY_1, &[EvalType::String], EvalType::Datetime, ScalarFuncSig::Timestamp1Arg, false),
    signature("timestamp", ANY_2, &[EvalType::String, EvalType::String], EvalType::Datetime, ScalarFuncSig::Timestamp2Args, false),
    string_signature("to_base64", ANY_1, &[EvalType::String], ScalarFuncSig::ToBase64, RetCollation::FunctionType),
    signature("to_days", ANY_1, &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::ToDays, false),
    signature("to_seconds", ANY_1, &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::ToSeconds, false),
    string_signature("trim", ANY_1, &[EvalType::String], ScalarFuncSig::Trim1Arg, RetCollation::FunctionType),
    string_signature("trim", ANY_2, &[EvalType::String, EvalType::String], ScalarFuncSig::Trim2Args, RetCollation::FunctionType),
    string_signature("trim", ANY_3, &[EvalType::String, EvalType::String, EvalType::Int], ScalarFuncSig::Trim3Args, RetCollation::FunctionType),
    string_signature("uncompress", ANY_1, &[EvalType::String], ScalarFuncSig::Uncompress, RetCollation::FunctionType),
    string_signature("unhex", ANY_1, &[EvalType::String], ScalarFuncSig::UnHex, RetCollation::FunctionType),
    signature("utc_time", ANY_1, &[EvalType::Int], EvalType::Duration, ScalarFuncSig::UtcTimeWithArg, false),
    signature("utc_timestamp", ANY_1, &[EvalType::Int], EvalType::Datetime, ScalarFuncSig::UtcTimestampWithArg, false),
    // The clock and session-state classes called without an argument
    // (`nowFunctionClass`, `currentTimeFunctionClass`, `utcTimeFunctionClass`,
    // `utcTimestampFunctionClass`, `lastInsertIDFunctionClass`,
    // `rowCountFunctionClass`): one signature each.
    signature("current_time", ANY_0, &[], EvalType::Duration, ScalarFuncSig::CurrentTime0Arg, false),
    signature("current_timestamp", ANY_0, &[], EvalType::Datetime, ScalarFuncSig::NowWithoutArg, false),
    signature("curtime", ANY_0, &[], EvalType::Duration, ScalarFuncSig::CurrentTime0Arg, false),
    signature("last_insert_id", ANY_0, &[], EvalType::Int, ScalarFuncSig::LastInsertId, false),
    signature("localtime", ANY_0, &[], EvalType::Datetime, ScalarFuncSig::NowWithoutArg, false),
    signature("localtimestamp", ANY_0, &[], EvalType::Datetime, ScalarFuncSig::NowWithoutArg, false),
    signature("now", ANY_0, &[], EvalType::Datetime, ScalarFuncSig::NowWithoutArg, false),
    signature("row_count", ANY_0, &[], EvalType::Int, ScalarFuncSig::RowCount, false),
    signature("utc_time", ANY_0, &[], EvalType::Duration, ScalarFuncSig::UtcTimeWithoutArg, false),
    signature("utc_timestamp", ANY_0, &[], EvalType::Datetime, ScalarFuncSig::UtcTimestampWithoutArg, false),
    // `convertFunctionClass` and `timestampAddFunctionClass.getFunction`.
    string_signature("convert", ANY_2, &[EvalType::String, EvalType::String], ScalarFuncSig::Convert, RetCollation::FunctionType),
    string_signature("timestampadd", ANY_3, &[EvalType::String, EvalType::Real, EvalType::Datetime], ScalarFuncSig::TimestampAdd, RetCollation::FunctionType),
    string_signature("uuid_v4", ANY_0, &[], ScalarFuncSig::UuiDv4, RetCollation::FunctionType),
    string_signature("uuid_v7", ANY_0, &[], ScalarFuncSig::UuiDv7, RetCollation::FunctionType),
    signature("vitess_hash", ANY_1, &[EvalType::Int], EvalType::Int, ScalarFuncSig::VitessHash, false),
    signature("weekday", ANY_1, &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::WeekDay, false),
    signature("yearweek", ANY_1, &[EvalType::Datetime], EvalType::Int, ScalarFuncSig::YearWeekWithoutMode, false),
    signature("yearweek", ANY_2, &[EvalType::Datetime, EvalType::Int], EvalType::Int, ScalarFuncSig::YearWeekWithMode, false),
    // `weekFunctionClass.getFunction`: a mode argument selects `WeekWithMode`.
    signature("week", ANY_2, &[EvalType::Datetime, EvalType::Int], EvalType::Int, ScalarFuncSig::WeekWithMode, false),
    // `exportSetFunctionClass.getFunction`: one signature per arity.
    string_signature("export_set", ANY_4, &[EvalType::Int, EvalType::String, EvalType::String, EvalType::String], ScalarFuncSig::ExportSet4Arg, RetCollation::FunctionType),
    string_signature("export_set", ANY_5, &[EvalType::Int, EvalType::String, EvalType::String, EvalType::String, EvalType::Int], ScalarFuncSig::ExportSet5Arg, RetCollation::FunctionType),
    // `anyValueFunctionClass.getFunction`: the argument in its own
    // evaluation type, and its type as the result.
    signature("any_value", &[ArgPattern::eval(EvalType::Int)], &[EvalType::Int], EvalType::Int, ScalarFuncSig::IntAnyValue, true),
    signature("any_value", &[ArgPattern::eval(EvalType::Real)], &[EvalType::Real], EvalType::Real, ScalarFuncSig::RealAnyValue, true),
    signature("any_value", &[ArgPattern::eval(EvalType::Decimal)], &[EvalType::Decimal], EvalType::Decimal, ScalarFuncSig::DecimalAnyValue, true),
    signature("any_value", &[ArgPattern::eval(EvalType::Datetime)], &[EvalType::Datetime], EvalType::Datetime, ScalarFuncSig::TimeAnyValue, false),
    signature("any_value", &[ArgPattern::eval(EvalType::Timestamp)], &[EvalType::Timestamp], EvalType::Timestamp, ScalarFuncSig::TimeAnyValue, false),
    signature("any_value", &[ArgPattern::eval(EvalType::Duration)], &[EvalType::Duration], EvalType::Duration, ScalarFuncSig::DurationAnyValue, false),
    signature("any_value", &[ArgPattern::eval(EvalType::Json)], &[EvalType::Json], EvalType::Json, ScalarFuncSig::JsonAnyValue, false),
    signature("any_value", &[ArgPattern::eval(EvalType::VectorFloat32)], &[EvalType::VectorFloat32], EvalType::VectorFloat32, ScalarFuncSig::VectorFloat32AnyValue, false),
    string_signature("any_value", &[ArgPattern::eval(EvalType::String)], &[EvalType::String], ScalarFuncSig::StringAnyValue, RetCollation::FunctionType),
    // `charLengthFunctionClass.getFunction` under its other name.
    string_int_signature("character_length", &[ArgPattern::string(true)], ScalarFuncSig::CharLength),
    string_int_signature("character_length", &[ArgPattern::string(false)], ScalarFuncSig::CharLengthUtf8),
    // `lowerFunctionClass` / `upperFunctionClass.getFunction` under their
    // other names.
    string_signature("lcase", &[ArgPattern::string(true)], &[EvalType::String], ScalarFuncSig::Lower, RetCollation::FirstArgString),
    string_signature("lcase", &[ArgPattern::string(false)], &[EvalType::String], ScalarFuncSig::LowerUtf8, RetCollation::FirstArgString),
    string_signature("ucase", &[ArgPattern::string(true)], &[EvalType::String], ScalarFuncSig::Upper, RetCollation::FirstArgString),
    string_signature("ucase", &[ArgPattern::string(false)], &[EvalType::String], ScalarFuncSig::UpperUtf8, RetCollation::FirstArgString),
    // `leftFunctionClass.getFunction`: a binary string selects `Left`.
    string_signature("left", &[ArgPattern::string(true), ArgPattern::ANY], &[EvalType::String, EvalType::Int], ScalarFuncSig::Left, RetCollation::FirstArgString),
    string_signature("left", &[ArgPattern::string(false), ArgPattern::ANY], &[EvalType::String, EvalType::Int], ScalarFuncSig::LeftUtf8, RetCollation::FirstArgString),
    // `lpadFunctionClass` / `rpadFunctionClass.getFunction`: a binary
    // string or a binary pad selects the byte signature.
    string_signature("lpad", &[ArgPattern::string(true), ArgPattern::ANY, ArgPattern::ANY], &[EvalType::String, EvalType::Int, EvalType::String], ScalarFuncSig::Lpad, RetCollation::FunctionType),
    string_signature("lpad", &[ArgPattern::ANY, ArgPattern::ANY, ArgPattern::string(true)], &[EvalType::String, EvalType::Int, EvalType::String], ScalarFuncSig::Lpad, RetCollation::FunctionType),
    string_signature("lpad", ANY_3, &[EvalType::String, EvalType::Int, EvalType::String], ScalarFuncSig::LpadUtf8, RetCollation::FunctionType),
    string_signature("rpad", &[ArgPattern::string(true), ArgPattern::ANY, ArgPattern::ANY], &[EvalType::String, EvalType::Int, EvalType::String], ScalarFuncSig::Rpad, RetCollation::FunctionType),
    string_signature("rpad", &[ArgPattern::ANY, ArgPattern::ANY, ArgPattern::string(true)], &[EvalType::String, EvalType::Int, EvalType::String], ScalarFuncSig::Rpad, RetCollation::FunctionType),
    string_signature("rpad", ANY_3, &[EvalType::String, EvalType::Int, EvalType::String], ScalarFuncSig::RpadUtf8, RetCollation::FunctionType),
    // `formatFunctionClass.getFunction`: an `ETInt` or `ETDecimal` number is
    // formatted as Decimal, any other as Real; a locale selects
    // `FormatWithLocale`.
    string_signature("format", &[ArgPattern::eval(EvalType::Decimal), ArgPattern::ANY], &[EvalType::Decimal, EvalType::Int], ScalarFuncSig::Format, RetCollation::FunctionType),
    string_signature("format", &[ArgPattern::eval(EvalType::Int), ArgPattern::ANY], &[EvalType::Decimal, EvalType::Int], ScalarFuncSig::Format, RetCollation::FunctionType),
    string_signature("format", ANY_2, &[EvalType::Real, EvalType::Int], ScalarFuncSig::Format, RetCollation::FunctionType),
    string_signature("format", &[ArgPattern::eval(EvalType::Decimal), ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Decimal, EvalType::Int, EvalType::String], ScalarFuncSig::FormatWithLocale, RetCollation::FunctionType),
    string_signature("format", &[ArgPattern::eval(EvalType::Int), ArgPattern::ANY, ArgPattern::ANY], &[EvalType::Decimal, EvalType::Int, EvalType::String], ScalarFuncSig::FormatWithLocale, RetCollation::FunctionType),
    string_signature("format", ANY_3, &[EvalType::Real, EvalType::Int, EvalType::String], ScalarFuncSig::FormatWithLocale, RetCollation::FunctionType),
    // `octFunctionClass.getFunction`: an `ETInt` argument selects `OctInt`,
    // any other is read as a string.
    string_signature("oct", &[ArgPattern::eval(EvalType::Int)], &[EvalType::Int], ScalarFuncSig::OctInt, RetCollation::FunctionType),
    string_signature("oct", ANY_1, &[EvalType::String], ScalarFuncSig::OctString, RetCollation::FunctionType),
    // `roundFunctionClass.getFunction` with a fraction argument: the same
    // argument domains as the one-argument rows.
    signature("round", &[ArgPattern::eval(EvalType::Int), ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::RoundWithFracInt, true),
    signature("round", &[ArgPattern::eval(EvalType::Decimal), ArgPattern::ANY], &[EvalType::Decimal, EvalType::Int], EvalType::Decimal, ScalarFuncSig::RoundWithFracDec, true),
    signature("round", ANY_2, &[EvalType::Real, EvalType::Int], EvalType::Real, ScalarFuncSig::RoundWithFracReal, true),
    // `truncateFunctionClass.getFunction`: Int and Decimal keep their domain,
    // an unsigned Int its own signature, any other truncates as Real.
    signature("truncate", &[ArgPattern::int(true), ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::TruncateUint, true),
    signature("truncate", &[ArgPattern::int(false), ArgPattern::ANY], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::TruncateInt, true),
    signature("truncate", &[ArgPattern::eval(EvalType::Decimal), ArgPattern::ANY], &[EvalType::Decimal, EvalType::Int], EvalType::Decimal, ScalarFuncSig::TruncateDecimal, true),
    signature("truncate", ANY_2, &[EvalType::Real, EvalType::Int], EvalType::Real, ScalarFuncSig::TruncateReal, true),
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

/// Whether Go has a `CastXAsY` signature for a cast between these eval types.
#[must_use]
pub fn cast_pair_has_signature(from: EvalType, to: EvalType) -> bool {
    cast_node_signature(from, to).is_some()
}

/// The signature of a cast NODE: a cast between equal eval types is still a
/// node (`CastIntAsInt` for `CAST(x AS SIGNED)` over an integer), unlike the
/// implicit cast the catalog skips when the types already agree.
fn cast_node_signature(from: EvalType, to: EvalType) -> Option<ScalarFuncSig> {
    match cast_signature(from, to)? {
        Some(signature) => Some(signature),
        None => Some(match to {
            EvalType::Int => ScalarFuncSig::CastIntAsInt,
            EvalType::Real => ScalarFuncSig::CastRealAsReal,
            EvalType::Decimal => ScalarFuncSig::CastDecimalAsDecimal,
            EvalType::String => ScalarFuncSig::CastStringAsString,
            EvalType::Datetime | EvalType::Timestamp => ScalarFuncSig::CastTimeAsTime,
            EvalType::Duration => ScalarFuncSig::CastDurationAsDuration,
            EvalType::Json => ScalarFuncSig::CastJsonAsJson,
            EvalType::VectorFloat32 => ScalarFuncSig::CastVectorFloat32AsVectorFloat32,
        }),
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
            | (EvalType::Datetime, EvalType::Datetime)
            | (EvalType::Timestamp, EvalType::Timestamp)
            | (EvalType::Timestamp, EvalType::Datetime)
            | (EvalType::VectorFloat32, EvalType::VectorFloat32)
            | (EvalType::Duration, EvalType::Duration)
            | (EvalType::Json, EvalType::Json)
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
        (EvalType::Int, EvalType::String) => ScalarFuncSig::CastIntAsString,
        (EvalType::Int, EvalType::Decimal) => ScalarFuncSig::CastIntAsDecimal,
        (EvalType::Int, EvalType::Datetime | EvalType::Timestamp) => ScalarFuncSig::CastIntAsTime,
        (EvalType::Int, EvalType::Duration) => ScalarFuncSig::CastIntAsDuration,
        (EvalType::Int, EvalType::Json) => ScalarFuncSig::CastIntAsJson,
        (EvalType::Real, EvalType::String) => ScalarFuncSig::CastRealAsString,
        (EvalType::Real, EvalType::Decimal) => ScalarFuncSig::CastRealAsDecimal,
        (EvalType::Real, EvalType::Datetime | EvalType::Timestamp) => ScalarFuncSig::CastRealAsTime,
        (EvalType::Real, EvalType::Duration) => ScalarFuncSig::CastRealAsDuration,
        (EvalType::Real, EvalType::Json) => ScalarFuncSig::CastRealAsJson,
        (EvalType::Decimal, EvalType::String) => ScalarFuncSig::CastDecimalAsString,
        (EvalType::Decimal, EvalType::Datetime | EvalType::Timestamp) => {
            ScalarFuncSig::CastDecimalAsTime
        }
        (EvalType::Decimal, EvalType::Duration) => ScalarFuncSig::CastDecimalAsDuration,
        (EvalType::Decimal, EvalType::Json) => ScalarFuncSig::CastDecimalAsJson,
        (EvalType::String, EvalType::Decimal) => ScalarFuncSig::CastStringAsDecimal,
        (EvalType::String, EvalType::Datetime | EvalType::Timestamp) => {
            ScalarFuncSig::CastStringAsTime
        }
        (EvalType::String, EvalType::Duration) => ScalarFuncSig::CastStringAsDuration,
        (EvalType::String, EvalType::Json) => ScalarFuncSig::CastStringAsJson,
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Int) => ScalarFuncSig::CastTimeAsInt,
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Real) => ScalarFuncSig::CastTimeAsReal,
        (EvalType::Datetime | EvalType::Timestamp, EvalType::String) => {
            ScalarFuncSig::CastTimeAsString
        }
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Decimal) => {
            ScalarFuncSig::CastTimeAsDecimal
        }
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Datetime | EvalType::Timestamp) => {
            ScalarFuncSig::CastTimeAsTime
        }
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Duration) => {
            ScalarFuncSig::CastTimeAsDuration
        }
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Json) => ScalarFuncSig::CastTimeAsJson,
        (EvalType::Duration, EvalType::Int) => ScalarFuncSig::CastDurationAsInt,
        (EvalType::Duration, EvalType::Real) => ScalarFuncSig::CastDurationAsReal,
        (EvalType::Duration, EvalType::String) => ScalarFuncSig::CastDurationAsString,
        (EvalType::Duration, EvalType::Decimal) => ScalarFuncSig::CastDurationAsDecimal,
        (EvalType::Duration, EvalType::Datetime | EvalType::Timestamp) => {
            ScalarFuncSig::CastDurationAsTime
        }
        (EvalType::Duration, EvalType::Json) => ScalarFuncSig::CastDurationAsJson,
        (EvalType::Json, EvalType::Int) => ScalarFuncSig::CastJsonAsInt,
        (EvalType::Json, EvalType::Real) => ScalarFuncSig::CastJsonAsReal,
        (EvalType::Json, EvalType::String) => ScalarFuncSig::CastJsonAsString,
        (EvalType::Json, EvalType::Decimal) => ScalarFuncSig::CastJsonAsDecimal,
        (EvalType::Json, EvalType::Datetime | EvalType::Timestamp) => ScalarFuncSig::CastJsonAsTime,
        (EvalType::Json, EvalType::Duration) => ScalarFuncSig::CastJsonAsDuration,
        (EvalType::VectorFloat32, EvalType::String) => ScalarFuncSig::CastVectorFloat32AsString,
        _ => return None,
    }))
}

/// All coercions the protobuf encoder can insert around catalog arguments.
/// Keep decoding tied to the same cast selection table as encoding.
pub(crate) fn implicit_cast_signatures() -> impl Iterator<Item = ScalarFuncSig> {
    const TYPES: [EvalType; 9] = [
        EvalType::Int,
        EvalType::Real,
        EvalType::Decimal,
        EvalType::String,
        EvalType::Datetime,
        EvalType::Timestamp,
        EvalType::Duration,
        EvalType::Json,
        EvalType::VectorFloat32,
    ];
    TYPES.into_iter().flat_map(|from| {
        TYPES
            .into_iter()
            .filter_map(move |to| cast_signature(from, to).flatten())
    })
}

/// Go's resolved control-function result, retained through encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct ControlMetadata {
    field_type: FieldType,
    collation: crate::expr_collation::ExprCollation,
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
    /// A SQL `NULL` constant. Go emits `ExprType_Null` with an empty value;
    /// the field type remains the planner's inferred type so a surrounding
    /// builtin can apply the same implicit cast selection as Go.
    NullLiteral {
        /// Planner-inferred type carried on the TiPB leaf.
        field_type: FieldType,
    },
    /// A signed integer constant, already folded, with the constant's own
    /// type (Go `Constant.GetType`).
    IntLiteral {
        /// The value.
        value: i64,
        /// The constant's type.
        field_type: FieldType,
    },
    /// An unsigned integer constant, already folded. Go uses a distinct
    /// `ExprType_Uint64` leaf so values above `math.MaxInt64` do not wrap.
    UIntLiteral {
        /// The exact unsigned value.
        value: u64,
        /// Planner-inferred literal type, including the unsigned flag.
        field_type: FieldType,
    },
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
    /// A single-precision datum, distinct from its inferred SQL result type.
    Float32Literal {
        /// Widened storage of the exact f32 value.
        value: f64,
        /// Planner-inferred literal type.
        field_type: FieldType,
    },
    /// A folded string/bytes literal.  Go sends both `KindString` and
    /// `KindBinaryLiteral` as a raw `ExprType_String` leaf; the field type
    /// carries the collation and binary flag.
    StringLiteral {
        /// Raw literal bytes.
        value: Vec<u8>,
        /// Planner-inferred literal type.
        field_type: FieldType,
    },
    /// A byte-preserving `KindBytes` literal. Go encodes this as the distinct
    /// TiPB `ExprType_Bytes` leaf rather than `ExprType_String`.
    BytesLiteral {
        /// Raw octets.
        value: Vec<u8>,
        /// Planner-inferred literal type.
        field_type: FieldType,
    },
    /// A MySQL BIT literal (`KindMysqlBit`).
    BitLiteral {
        /// Raw, width-preserving bit payload.
        value: BinaryLiteral,
        /// Planner-inferred literal type.
        field_type: FieldType,
    },
    /// A MySQL ENUM literal (`KindMysqlEnum`).
    EnumLiteral {
        /// One-based ENUM element number.
        value: u64,
        /// Planner-inferred literal type and element metadata.
        field_type: FieldType,
    },
    /// A folded MySQL DATE/DATETIME/TIMESTAMP literal.
    TimeLiteral {
        /// Packed MySQL time value before TiPB encoding.
        value: Time,
        /// Planner-inferred literal type.
        field_type: FieldType,
    },
    /// A folded MySQL TIME literal.
    DurationLiteral {
        /// Duration value before TiPB encoding.
        value: MySqlDuration,
        /// Planner-inferred literal type.
        field_type: FieldType,
    },
    /// A folded binary JSON literal, refused by Go encodeDatum and this encoder.
    JsonLiteral {
        /// Exact type-code-plus-payload representation.
        value: BinaryJSON,
        /// Planner-inferred literal type.
        field_type: FieldType,
    },
    /// A TiDB vector literal (`KindVectorFloat32`).
    VectorLiteral {
        /// Exact vector payload.
        value: VectorFloat32,
        /// Planner-inferred literal type.
        field_type: FieldType,
    },
    /// A resolved builtin call.
    Call {
        /// The catalog row [`resolve`] chose.
        signature: &'static BuiltinSignature,
        /// The call's arguments, in source order.
        args: Vec<PbScalar>,
        /// Result metadata inferred once by the control-function constructor.
        control: Option<Box<ControlMetadata>>,
        /// The described function's own result type (Go's `RetType`, with
        /// its collation forced in), which `scalarFuncToPBExpr` sends for
        /// every function node; `None` for a call built from a name alone,
        /// which takes the catalog's result type.
        ret_type: Option<FieldType>,
        /// Go `expr.Function.metadata()`, marshalled: the function node's
        /// `Val`. `None` for a function with none (`baseBuiltinFunc.metadata`).
        metadata: Option<Vec<u8>>,
    },
    /// A cast node the planner built (Go's single `ast.Cast` function): its
    /// signature is chosen by the argument's and the target's eval types and
    /// it is sent with the target's exact field type, so an implicit cast
    /// `newBaseBuiltinFuncWithTp` already placed is lowered as built.
    Cast {
        /// The `CastXAsY` signature.
        signature: ScalarFuncSig,
        /// The converted argument.
        arg: Box<PbScalar>,
        /// The cast's result type.
        field_type: FieldType,
    },
}

impl PbScalar {
    /// An integer literal of the literal's own type (Go
    /// `types.DefaultTypeForValue`).
    #[must_use]
    pub fn int_literal(value: i64) -> Self {
        Self::IntLiteral {
            value,
            field_type: FieldType::new(FieldTypeCode::LongLong)
                .with_flen(value.to_string().len() as i64)
                .with_decimal(0)
                .with_flags(NOT_NULL_FLAG | BINARY_FLAG),
        }
    }
}

impl PbScalar {
    /// Go `Expression.GetType()`: the declared field metadata a leaf, a cast
    /// or a described function node carries. A call built without a node has
    /// none and derives its result type from the catalog.
    fn static_field_type(&self) -> Option<&FieldType> {
        match self {
            Self::Column { field_type, .. }
            | Self::NullLiteral { field_type }
            | Self::IntLiteral { field_type, .. }
            | Self::UIntLiteral { field_type, .. }
            | Self::DecimalLiteral { field_type, .. }
            | Self::RealLiteral { field_type, .. }
            | Self::Float32Literal { field_type, .. }
            | Self::StringLiteral { field_type, .. }
            | Self::BytesLiteral { field_type, .. }
            | Self::BitLiteral { field_type, .. }
            | Self::EnumLiteral { field_type, .. }
            | Self::TimeLiteral { field_type, .. }
            | Self::DurationLiteral { field_type, .. }
            | Self::JsonLiteral { field_type, .. }
            | Self::VectorLiteral { field_type, .. } => Some(field_type),
            Self::Call {
                control: Some(control),
                ..
            } => Some(&control.field_type),
            Self::Cast { field_type, .. } => Some(field_type),
            Self::Call { ret_type, .. } => ret_type.as_ref(),
        }
    }

    /// The node's evaluation type, which is what the selector matches on.
    #[must_use]
    pub fn eval_type(&self) -> EvalType {
        match self {
            Self::Column { field_type, .. } => field_type.eval_type(),
            Self::NullLiteral { field_type } => field_type.eval_type(),
            Self::IntLiteral { .. } => EvalType::Int,
            Self::UIntLiteral { field_type, .. } => field_type.eval_type(),
            Self::DecimalLiteral { .. } => EvalType::Decimal,
            Self::RealLiteral { .. } | Self::Float32Literal { .. } => EvalType::Real,
            Self::StringLiteral { .. } => EvalType::String,
            Self::BytesLiteral { .. } => EvalType::String,
            Self::BitLiteral { field_type, .. } => field_type.eval_type(),
            Self::EnumLiteral { field_type, .. } => field_type.eval_type(),
            Self::TimeLiteral { field_type, .. } => field_type.eval_type(),
            Self::DurationLiteral { .. } => EvalType::Duration,
            Self::JsonLiteral { .. } => EvalType::Json,
            Self::VectorLiteral { .. } => EvalType::VectorFloat32,
            // A described function node answers from its own result type,
            // a call built without one from its signature's.
            Self::Call { signature, .. } => self
                .static_field_type()
                .map_or(signature.ret, FieldType::eval_type),
            Self::Cast { field_type, .. } => field_type.eval_type(),
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
            Self::NullLiteral { field_type } => field_type.is_unsigned(),
            Self::IntLiteral { .. } => false,
            Self::UIntLiteral { field_type, .. }
            | Self::DecimalLiteral { field_type, .. }
            | Self::RealLiteral { field_type, .. }
            | Self::Float32Literal { field_type, .. } => field_type.is_unsigned(),
            Self::StringLiteral { field_type, .. }
            | Self::BytesLiteral { field_type, .. }
            | Self::BitLiteral { field_type, .. }
            | Self::EnumLiteral { field_type, .. }
            | Self::TimeLiteral { field_type, .. }
            | Self::DurationLiteral { field_type, .. }
            | Self::JsonLiteral { field_type, .. }
            | Self::VectorLiteral { field_type, .. } => field_type.is_unsigned(),
            Self::Call {
                signature,
                args,
                control,
                ret_type,
                ..
            } => {
                if let Some(control) = control {
                    return control.field_type.is_unsigned();
                }
                // Go reads the function's own `RetType` (`plus`, `minus`,
                // `mul` and `intdiv` are unsigned when EITHER operand is).
                if let Some(ret_type) = ret_type {
                    return ret_type.is_unsigned();
                }
                signature.ret_unsigned_from_first_arg
                    && args.first().is_some_and(PbScalar::is_unsigned)
            }
            Self::Cast { field_type, .. } => field_type.is_unsigned(),
        }
    }

    /// Whether the node is a binary string -- Go `types.IsBinaryStr`, the
    /// selector of the string family's two spellings.
    ///
    /// A described function node answers from its own result type, as Go's
    /// `args[i].GetType()` does. A call built without a node reads the result
    /// collation rule rather than the argument: `UPPER` over a `VARBINARY`
    /// returns binary (Go `SetBinFlagOrBinStr` -> `SetBinChsClnFlag`), so
    /// `UPPER(UPPER(b))` resolves the binary signature at both levels, while
    /// `CONV` always returns the connection charset and so is never binary.
    #[must_use]
    pub fn is_binary_string(&self) -> bool {
        match self {
            Self::Column { field_type, .. } => field_type.is_binary_string(),
            Self::NullLiteral { .. } => false,
            Self::IntLiteral { .. } => false,
            Self::UIntLiteral { .. }
            | Self::DecimalLiteral { .. }
            | Self::RealLiteral { .. }
            | Self::Float32Literal { .. } => false,
            Self::StringLiteral { field_type, .. } | Self::BytesLiteral { field_type, .. } => {
                field_type.is_binary_string()
            }
            Self::BitLiteral { .. }
            | Self::EnumLiteral { .. }
            | Self::TimeLiteral { .. }
            | Self::DurationLiteral { .. }
            | Self::JsonLiteral { .. }
            | Self::VectorLiteral { .. } => false,
            Self::Call {
                ret_type: Some(ret_type),
                control: None,
                ..
            } => ret_type.is_binary_string(),
            Self::Call {
                signature,
                args,
                control,
                ..
            } => match signature.ret_collation {
                RetCollation::ControlBranches => control
                    .as_ref()
                    .is_some_and(|metadata| metadata.field_type.is_binary_string()),
                RetCollation::Numeric
                | RetCollation::ConnectionString
                | RetCollation::FunctionType => false,
                RetCollation::FirstArgString => {
                    args.first().is_some_and(PbScalar::is_binary_string)
                }
                RetCollation::SecondArgString => {
                    args.get(1).is_some_and(PbScalar::is_binary_string)
                }
            },
            Self::Cast { field_type, .. } => field_type.is_binary_string(),
        }
    }
}

/// Reconstruct metadata in the native expression model, without evaluating
/// arguments or encoding/decoding protobufs. Literal repertoire and column
/// coercibility must survive branch aggregation.
fn metadata_expression(scalar: &PbScalar) -> Option<Expression> {
    use crate::{column::Column, constant::Constant, scalar_function::ScalarFunction};
    use tidb_ast::CiString;
    let datum = match scalar {
        PbScalar::Column { offset, field_type } => {
            let mut column = Column::new(0, field_type.clone());
            column.index = i64::from(*offset);
            return Some(Expression::Column(column));
        }
        PbScalar::Cast {
            arg, field_type, ..
        } => {
            let argument = metadata_expression(arg)?;
            let collation =
                crate::collation_derive::derive_collation("cast", &[argument.clone()], field_type.eval_type())
                    .ok()?;
            let mut expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("cast"),
                field_type.clone(),
                vec![argument],
            ));
            crate::collation_derive::apply_derived_collation(&mut expression, &collation);
            return Some(expression);
        }
        PbScalar::Call {
            signature,
            args,
            control,
            ret_type,
            ..
        } => {
            if let Some(control) = control {
                // Only the immutable metadata is needed by an enclosing inference.
                let mut expression = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(signature.name),
                    control.field_type.clone(),
                    Vec::new(),
                ));
                crate::collation_derive::apply_derived_collation(
                    &mut expression,
                    &control.collation,
                );
                return Some(expression);
            }
            let arguments = args
                .iter()
                .map(metadata_expression)
                .collect::<Option<Vec<_>>>()?;
            let field = match ret_type {
                Some(field) => field.clone(),
                None => {
                    let children = arguments
                        .iter()
                        .map(|arg| {
                            Some(Expr {
                                field_type: Some(to_pb_field_type(arg.static_type()?)?),
                                ..Default::default()
                            })
                        })
                        .collect::<Option<Vec<_>>>()?;
                    crate::distsql_builtin::pb_type_to_field_type(
                        &signature.return_field_type(scalar.is_unsigned(), &children)?,
                    )
                }
            };
            let collation = crate::collation_derive::derive_collation(
                signature.name,
                &arguments,
                signature.ret,
            )
            .ok()?;
            let mut expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(signature.name),
                field,
                arguments,
            ));
            crate::collation_derive::apply_derived_collation(&mut expression, &collation);
            return Some(expression);
        }
        PbScalar::IntLiteral { value, .. } => Datum::Int(*value),
        PbScalar::NullLiteral { .. } => Datum::Null,
        PbScalar::UIntLiteral { value, .. } => Datum::UInt(*value),
        PbScalar::DecimalLiteral { value, .. } => Datum::Decimal(value.clone()),
        PbScalar::RealLiteral { value, .. } => Datum::Real(*value),
        PbScalar::Float32Literal { value, .. } => Datum::Float32(*value),
        PbScalar::StringLiteral { value, field_type } => {
            Datum::new_collation_string_datum(value.clone(), field_type.collation())
        }
        PbScalar::BytesLiteral { value, .. } => Datum::Bytes(value.clone()),
        PbScalar::BitLiteral { value, .. } => Datum::Bit(value.clone()),
        PbScalar::EnumLiteral { value, field_type } => Datum::Enum(
            tidb_datatype::parse_enum_value(&field_type.elems_snapshot(), *value).ok()?,
            field_type.collation(),
        ),
        PbScalar::TimeLiteral { value, .. } => Datum::Time(*value),
        PbScalar::DurationLiteral { value, .. } => Datum::Duration(*value),
        PbScalar::JsonLiteral { value, .. } => Datum::Json(value.clone()),
        PbScalar::VectorLiteral { value, .. } => Datum::VectorFloat32(value.clone()),
    };
    Some(Expression::Constant(Constant::new(
        datum,
        scalar.static_field_type()?.clone(),
    )))
}

/// Go infers the complete result type from every value branch, then derives
/// collation with the function's SQL name before choosing its typed signature.
fn control_expression(name: &str, args: &[PbScalar]) -> Option<Expression> {
    let arguments = args
        .iter()
        .map(metadata_expression)
        .collect::<Option<Vec<_>>>()?;
    let values = match name {
        "if" if arguments.len() == 3 => arguments[1..].to_vec(),
        "ifnull" if arguments.len() == 2 => arguments.clone(),
        "coalesce" if !arguments.is_empty() => arguments.clone(),
        "case" if arguments.len() >= 2 => arguments
            .iter()
            .enumerate()
            .filter(|(index, _)| index % 2 == 1 || index + 1 == arguments.len())
            .map(|(_, arg)| arg.clone())
            .collect(),
        _ => return None,
    };
    let field = crate::rewriter::infer_type_4_control_funcs(name, &values)?;
    let collation =
        crate::collation_derive::derive_collation(name, &arguments, field.eval_type()).ok()?;
    let mut expression = Expression::ScalarFunction(crate::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new(name),
        field,
        arguments,
    ));
    crate::collation_derive::apply_derived_collation(&mut expression, &collation);
    Some(expression)
}

/// Resolves GO's conditional family (`case`/`if`/`ifnull`).
///
/// GO records each conditional's PbCode from the RESULT type that
/// `inferType4ControlFuncs` derives from all value branches. `case` takes a
/// variable arity (flattened cond/value pairs with an optional trailing else, exactly GO's PB argument order), so
/// the static matcher cannot answer for it.
fn resolve_conditional(name: &str, args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    match (name, args.len()) {
        ("case", n) if n >= 2 => {}
        ("if", 3) => {}
        ("ifnull", 2) => {}
        ("coalesce", n) if n >= 1 => {}
        _ => return None,
    }
    let inferred = control_expression(name, args)?;
    resolve_control_type(name, inferred.static_type()?.eval_type())
}

fn resolve_control_type(name: &str, domain: EvalType) -> Option<&'static BuiltinSignature> {
    let suffix = match domain {
        EvalType::Int => "Int",
        EvalType::Real => "Real",
        EvalType::Decimal => "Decimal",
        EvalType::String => "String",
        EvalType::Datetime | EvalType::Timestamp => "Time",
        EvalType::Duration => "Duration",
        EvalType::Json => "Json",
        _ => return None,
    };
    let sig = match (name, suffix) {
        ("case", "Int") => ScalarFuncSig::CaseWhenInt,
        ("case", "Real") => ScalarFuncSig::CaseWhenReal,
        ("case", "Decimal") => ScalarFuncSig::CaseWhenDecimal,
        ("case", "String") => ScalarFuncSig::CaseWhenString,
        ("case", "Time") => ScalarFuncSig::CaseWhenTime,
        ("case", "Duration") => ScalarFuncSig::CaseWhenDuration,
        ("case", "Json") => ScalarFuncSig::CaseWhenJson,
        ("if", "Int") => ScalarFuncSig::IfInt,
        ("if", "Real") => ScalarFuncSig::IfReal,
        ("if", "Decimal") => ScalarFuncSig::IfDecimal,
        ("if", "String") => ScalarFuncSig::IfString,
        ("if", "Time") => ScalarFuncSig::IfTime,
        ("if", "Duration") => ScalarFuncSig::IfDuration,
        ("if", "Json") => ScalarFuncSig::IfJson,
        ("ifnull", "Int") => ScalarFuncSig::IfNullInt,
        ("ifnull", "Real") => ScalarFuncSig::IfNullReal,
        ("ifnull", "Decimal") => ScalarFuncSig::IfNullDecimal,
        ("ifnull", "String") => ScalarFuncSig::IfNullString,
        ("ifnull", "Time") => ScalarFuncSig::IfNullTime,
        ("ifnull", "Duration") => ScalarFuncSig::IfNullDuration,
        ("ifnull", "Json") => ScalarFuncSig::IfNullJson,
        ("coalesce", "Int") => ScalarFuncSig::CoalesceInt,
        ("coalesce", "Real") => ScalarFuncSig::CoalesceReal,
        ("coalesce", "Decimal") => ScalarFuncSig::CoalesceDecimal,
        ("coalesce", "String") => ScalarFuncSig::CoalesceString,
        ("coalesce", "Time") => ScalarFuncSig::CoalesceTime,
        ("coalesce", "Duration") => ScalarFuncSig::CoalesceDuration,
        ("coalesce", "Json") => ScalarFuncSig::CoalesceJson,
        _ => return None,
    };
    CATALOG
        .iter()
        .find(|candidate| candidate.name == name && candidate.sig == sig)
}

/// The catalog row for a call of `name` over `args`: Go's per-family
/// `getFunction` signature choice for arguments of these types. `None` means
/// Go builds no signature for this name and argument shape.
///
/// Which store evaluates the signature is not answered here: that is Go
/// `canFuncBePushed`, [`crate::infer_pushdown`].
#[must_use]
pub fn resolve(name: &str, args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    if let Some(signature) = resolve_conditional(name, args) {
        return Some(signature);
    }
    if let Some(signature) = resolve_date_arithmetic(name, args) {
        return Some(signature);
    }
    if name == "unix_timestamp" {
        return resolve_unix_timestamp(args);
    }
    if name == "extract" {
        return resolve_extract(args);
    }
    if name == "in" {
        return resolve_in(args);
    }
    if name == "concat" {
        return resolve_concat(args);
    }
    if name == "elt" {
        return resolve_elt(args);
    }
    if name == "rand" {
        return resolve_rand(args);
    }
    if name == "field" {
        return resolve_field(args);
    }
    if name == "hex" {
        return resolve_hex(args);
    }
    if name == "unaryminus" {
        let [arg] = args else {
            return None;
        };
        // The result an argument's own type gives; `from_expression` reads
        // the planned result type over it.
        return resolve_unary_minus(arg.eval_type());
    }
    if matches!(name, "ceil" | "ceiling" | "floor") {
        let [arg] = args else {
            return None;
        };
        let arg = arg.eval_type();
        // The result an argument's own type gives; `from_expression` reads
        // the planned result type over it.
        let ret = match arg {
            EvalType::Int | EvalType::Decimal => arg,
            _ => EvalType::Real,
        };
        return resolve_ceil_floor(name, arg, ret);
    }
    if name == "json_extract" {
        return resolve_json_extract(args);
    }
    if let Some(signature) = resolve_json(name, args) {
        return Some(signature);
    }
    if name == "concat_ws" {
        return resolve_concat_ws(args);
    }
    if name == "timediff" {
        return resolve_time_diff(args);
    }
    if name == "interval" {
        return resolve_interval(args);
    }
    if name == "make_set" {
        return resolve_make_set(args);
    }
    if name == "char_func" {
        return resolve_char(args);
    }
    if name == "match_against" {
        return resolve_match_against(args);
    }
    if matches!(name, "istrue" | "isfalse" | "istrue_with_null") {
        return resolve_is_true_or_false(name, args);
    }
    if matches!(
        name,
        "addtime"
            | "subtime"
            | "intdiv"
            | "greatest"
            | "least"
            | "str_to_date"
            | "instr"
            | "locate"
            | "position"
            | "insert_func"
    ) {
        // These families read their arguments' field types, or the collation
        // derived from them, rather than the evaluation types alone.
        let args = args
            .iter()
            .map(metadata_expression)
            .collect::<Option<Vec<_>>>()?;
        return match name {
            "addtime" | "subtime" => resolve_add_sub_time(name, &args),
            "intdiv" => resolve_int_divide(&args),
            "greatest" | "least" => {
                resolve_extremum(name, crate::rewriter::result_type::gl_signature(&args)?)
            }
            "str_to_date" => resolve_str_to_date(
                crate::rewriter::result_type::str_to_date_return_type(&args)?.code(),
            ),
            "insert_func" => resolve_insert(
                args.len(),
                crate::collation_derive::derive_collation(name, &args, EvalType::String)
                    .ok()?
                    .collation
                    == BINARY_COLLATION,
            ),
            _ => resolve_locate(
                name,
                args.len(),
                crate::collation_derive::derive_collation(name, &args, EvalType::Int)
                    .ok()?
                    .collation
                    == BINARY_COLLATION,
            ),
        };
    }
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

/// Go `concatFunctionClass.getFunction`: every argument `ETString`, one
/// signature (`Concat`) whatever the arity.
fn resolve_concat(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static CONCAT: BuiltinSignature = string_signature(
        "concat",
        &[],
        &[],
        ScalarFuncSig::Concat,
        RetCollation::FirstArgString,
    );
    (!args.is_empty() && args.iter().all(|arg| arg.eval_type() == EvalType::String))
        .then_some(&CONCAT)
}

/// Go `ceilFunctionClass` / `floorFunctionClass.getFunction` over
/// `getEvalTp4FloorAndCeil`: an `ETInt` argument answers `*IntToInt`, an
/// `ETDecimal` one `*DecToInt` or `*DecToDec` by the result type, any other
/// `*Real`.
pub(crate) fn resolve_ceil_floor(name: &str, arg: EvalType, ret: EvalType) -> Option<&'static BuiltinSignature> {
    const fn ceil_floor(name: &'static str, arg: &'static [EvalType], ret: EvalType, sig: ScalarFuncSig) -> BuiltinSignature {
        signature(name, &[], arg, ret, sig, true)
    }
    static CEIL: [BuiltinSignature; 4] = [
        ceil_floor("ceil", &[EvalType::Int], EvalType::Int, ScalarFuncSig::CeilIntToInt),
        ceil_floor("ceil", &[EvalType::Decimal], EvalType::Int, ScalarFuncSig::CeilDecToInt),
        ceil_floor("ceil", &[EvalType::Decimal], EvalType::Decimal, ScalarFuncSig::CeilDecToDec),
        ceil_floor("ceil", &[EvalType::Real], EvalType::Real, ScalarFuncSig::CeilReal),
    ];
    static FLOOR: [BuiltinSignature; 4] = [
        ceil_floor("floor", &[EvalType::Int], EvalType::Int, ScalarFuncSig::FloorIntToInt),
        ceil_floor("floor", &[EvalType::Decimal], EvalType::Int, ScalarFuncSig::FloorDecToInt),
        ceil_floor("floor", &[EvalType::Decimal], EvalType::Decimal, ScalarFuncSig::FloorDecToDec),
        ceil_floor("floor", &[EvalType::Real], EvalType::Real, ScalarFuncSig::FloorReal),
    ];
    let family = if name == "floor" { &FLOOR } else { &CEIL };
    Some(match (arg, ret) {
        (EvalType::Int, EvalType::Int) => &family[0],
        (EvalType::Decimal, EvalType::Int) => &family[1],
        (EvalType::Decimal, EvalType::Decimal) => &family[2],
        (EvalType::Int | EvalType::Decimal, _) => return None,
        _ => &family[3],
    })
}

/// Go `randFunctionClass.getFunction`: `RAND()` and a constant seed, which
/// seeds the generator once at build, are `Rand`; a seed read per row is
/// `RandWithSeedFirstGen`.
fn resolve_rand(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static RAND: [BuiltinSignature; 3] = [
        signature("rand", &[], &[], EvalType::Real, ScalarFuncSig::Rand, false),
        signature("rand", &[], &[EvalType::Int], EvalType::Real, ScalarFuncSig::Rand, false),
        signature("rand", &[], &[EvalType::Int], EvalType::Real, ScalarFuncSig::RandWithSeedFirstGen, false),
    ];
    match args {
        [] => Some(&RAND[0]),
        [PbScalar::Column { .. } | PbScalar::Call { .. } | PbScalar::Cast { .. }] => Some(&RAND[2]),
        [_] => Some(&RAND[1]),
        _ => None,
    }
}

/// Go `eltFunctionClass.getFunction`: an `ETInt` index, then `ETString`s.
fn resolve_elt(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static ELT: BuiltinSignature =
        string_signature("elt", &[], &[], ScalarFuncSig::Elt, RetCollation::ControlBranches);
    (args.len() >= 2).then_some(&ELT)
}

/// Go `fieldFunctionClass.getFunction`: all `ETString` arguments compare as
/// strings, all `ETInt` as integers, any other mix as reals.
fn resolve_field(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static FIELD: [BuiltinSignature; 3] = [
        signature("field", &[], &[], EvalType::Int, ScalarFuncSig::FieldString, false),
        signature("field", &[], &[], EvalType::Int, ScalarFuncSig::FieldInt, false),
        signature("field", &[], &[], EvalType::Int, ScalarFuncSig::FieldReal, false),
    ];
    if args.len() < 2 {
        return None;
    }
    Some(if args.iter().all(|arg| arg.eval_type() == EvalType::String) {
        &FIELD[0]
    } else if args.iter().all(|arg| arg.eval_type() == EvalType::Int) {
        &FIELD[1]
    } else {
        &FIELD[2]
    })
}

/// Go `hexFunctionClass.getFunction`: a string, temporal or JSON argument
/// answers `HexStrArg` over `ETString`, a numeric one `HexIntArg` over
/// `ETInt`.
fn resolve_hex(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static HEX: [BuiltinSignature; 2] = [
        string_signature("hex", &[], &[EvalType::String], ScalarFuncSig::HexStrArg, RetCollation::ConnectionString),
        string_signature("hex", &[], &[EvalType::Int], ScalarFuncSig::HexIntArg, RetCollation::ConnectionString),
    ];
    let [arg] = args else {
        return None;
    };
    match arg.eval_type() {
        EvalType::String | EvalType::Datetime | EvalType::Timestamp | EvalType::Duration | EvalType::Json => {
            Some(&HEX[0])
        }
        EvalType::Int | EvalType::Real | EvalType::Decimal => Some(&HEX[1]),
        EvalType::VectorFloat32 => None,
    }
}

/// Go `unaryMinusFunctionClass.getFunction`: the signature follows the
/// result type it picks (an `ETInt` that may overflow, or an unsigned one,
/// answers `ETDecimal`), one argument of that type.
pub(crate) fn resolve_unary_minus(ret: EvalType) -> Option<&'static BuiltinSignature> {
    static UNARY_MINUS: [BuiltinSignature; 3] = [
        signature("unaryminus", &[], &[EvalType::Int], EvalType::Int, ScalarFuncSig::UnaryMinusInt, false),
        signature("unaryminus", &[], &[EvalType::Decimal], EvalType::Decimal, ScalarFuncSig::UnaryMinusDecimal, false),
        signature("unaryminus", &[], &[EvalType::Real], EvalType::Real, ScalarFuncSig::UnaryMinusReal, false),
    ];
    Some(match ret {
        EvalType::Int => &UNARY_MINUS[0],
        EvalType::Decimal => &UNARY_MINUS[1],
        EvalType::Real => &UNARY_MINUS[2],
        _ => return None,
    })
}

/// Go `jsonExtractFunctionClass.getFunction`: an `ETJson` document and one
/// or more `ETString` paths, one signature (`JsonExtractSig`).
fn resolve_json_extract(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static JSON_EXTRACT: BuiltinSignature = signature(
        "json_extract",
        &[],
        &[],
        EvalType::Json,
        ScalarFuncSig::JsonExtractSig,
        false,
    );
    (args.len() >= 2).then_some(&JSON_EXTRACT)
}

/// The JSON builtins' function classes (`builtin_json.go`): one signature
/// each over its arity, JSON_VALID's by its argument's type; the argument
/// types per position are [`BuiltinSignature::arg_type_at`]'s.
fn resolve_json(name: &str, args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    const fn json(name: &'static str, ret: EvalType, sig: ScalarFuncSig) -> BuiltinSignature {
        signature(name, &[], &[], ret, sig, false)
    }
    static JSON: [BuiltinSignature; 21] = [
        json("json_type", EvalType::String, ScalarFuncSig::JsonTypeSig),
        json("json_unquote", EvalType::String, ScalarFuncSig::JsonUnquoteSig),
        json("json_length", EvalType::Int, ScalarFuncSig::JsonLengthSig),
        json("json_object", EvalType::Json, ScalarFuncSig::JsonObjectSig),
        json("json_array", EvalType::Json, ScalarFuncSig::JsonArraySig),
        json("json_merge", EvalType::Json, ScalarFuncSig::JsonMergeSig),
        json("json_set", EvalType::Json, ScalarFuncSig::JsonSetSig),
        json("json_insert", EvalType::Json, ScalarFuncSig::JsonInsertSig),
        json("json_remove", EvalType::Json, ScalarFuncSig::JsonRemoveSig),
        json("json_contains", EvalType::Int, ScalarFuncSig::JsonContainsSig),
        json("json_valid", EvalType::Int, ScalarFuncSig::JsonValidJsonSig),
        json("json_valid", EvalType::Int, ScalarFuncSig::JsonValidStringSig),
        json("json_replace", EvalType::Json, ScalarFuncSig::JsonReplaceSig),
        json("json_array_append", EvalType::Json, ScalarFuncSig::JsonArrayAppendSig),
        json("json_array_insert", EvalType::Json, ScalarFuncSig::JsonArrayInsertSig),
        json("json_merge_patch", EvalType::Json, ScalarFuncSig::JsonMergePatchSig),
        json("json_merge_preserve", EvalType::Json, ScalarFuncSig::JsonMergePreserveSig),
        json("json_contains_path", EvalType::Int, ScalarFuncSig::JsonContainsPathSig),
        json("json_search", EvalType::Json, ScalarFuncSig::JsonSearchSig),
        json("json_keys", EvalType::Json, ScalarFuncSig::JsonKeysSig),
        json("json_keys", EvalType::Json, ScalarFuncSig::JsonKeys2ArgsSig),
    ];
    static JSON_VALID_OTHERS: BuiltinSignature = json("json_valid", EvalType::Int, ScalarFuncSig::JsonValidOthersSig);
    let arity = args.len();
    let index = match name {
        "json_type" if arity == 1 => 0,
        "json_unquote" if arity == 1 => 1,
        "json_length" if matches!(arity, 1 | 2) => 2,
        "json_object" if arity % 2 == 0 => 3,
        "json_array" => 4,
        "json_merge" if arity >= 2 => 5,
        "json_set" if arity >= 3 && arity % 2 == 1 => 6,
        "json_insert" if arity >= 3 && arity % 2 == 1 => 7,
        "json_remove" if arity >= 2 => 8,
        "json_contains" if matches!(arity, 2 | 3) => 9,
        "json_replace" if arity >= 3 && arity % 2 == 1 => 12,
        "json_array_append" if arity >= 3 && arity % 2 == 1 => 13,
        "json_array_insert" if arity >= 3 && arity % 2 == 1 => 14,
        "json_merge_patch" if arity >= 2 => 15,
        "json_merge_preserve" if arity >= 2 => 16,
        "json_contains_path" if arity >= 3 => 17,
        "json_search" if arity >= 3 => 18,
        "json_keys" if arity == 1 => 19,
        "json_keys" if arity == 2 => 20,
        "json_valid" if arity == 1 => match args[0].eval_type() {
            EvalType::Json => 10,
            EvalType::String => 11,
            _ => return Some(&JSON_VALID_OTHERS),
        },
        _ => return None,
    };
    Some(&JSON[index])
}

/// Go `concatWSFunctionClass.getFunction`: every argument `ETString`, the
/// separator first, one signature (`ConcatWS`) whatever the arity.
fn resolve_concat_ws(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static CONCAT_WS: BuiltinSignature = string_signature(
        "concat_ws",
        &[],
        &[],
        ScalarFuncSig::ConcatWs,
        RetCollation::FirstArgString,
    );
    (args.len() >= 2 && args.iter().all(|arg| arg.eval_type() == EvalType::String))
        .then_some(&CONCAT_WS)
}

/// Go `inFunctionClass.getFunction`: every argument is read as the tested
/// argument's evaluation type, which selects the signature.
fn resolve_in(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    let [tested, _, ..] = args else {
        return None;
    };
    CATALOG.iter().find(|candidate| {
        candidate.name == "in"
            && candidate
                .selector
                .first()
                .is_some_and(|pattern| pattern.matches(tested))
    })
}

/// Go `arithmeticIntDivideFunctionClass.getFunction`: two operands whose
/// numeric context (`numericContextResultType`) is `ETInt` divide as
/// integers, any other pair as decimals; the result is `ETInt` either way.
fn resolve_int_divide(args: &[Expression]) -> Option<&'static BuiltinSignature> {
    static INT_DIVIDE: [BuiltinSignature; 2] = [
        signature("intdiv", &[], &[EvalType::Int, EvalType::Int], EvalType::Int, ScalarFuncSig::IntDivideInt, false),
        signature(
            "intdiv",
            &[],
            &[EvalType::Decimal, EvalType::Decimal],
            EvalType::Int,
            ScalarFuncSig::IntDivideDecimal,
            false,
        ),
    ];
    let [left, right] = args else {
        return None;
    };
    let integer = |arg| crate::builtin_arithmetic::numeric_context_result_type(arg) == EvalType::Int;
    Some(if integer(left) && integer(right) {
        &INT_DIVIDE[0]
    } else {
        &INT_DIVIDE[1]
    })
}

/// Go `instrFunctionClass` / `locateFunctionClass.getFunction` (LOCATE and
/// POSITION share the latter): two `ETString`s and LOCATE's optional `ETInt`
/// start; the function's derived collation being `binary` selects the byte
/// signature over the multibyte one.
fn resolve_locate(name: &str, arity: usize, binary: bool) -> Option<&'static BuiltinSignature> {
    const TYPES_2: &[EvalType] = &[EvalType::String, EvalType::String];
    const TYPES_3: &[EvalType] = &[EvalType::String, EvalType::String, EvalType::Int];
    const fn locate(name: &'static str, arg_types: &'static [EvalType], sig: ScalarFuncSig) -> BuiltinSignature {
        signature(name, &[], arg_types, EvalType::Int, sig, false)
    }
    static INSTR: [BuiltinSignature; 2] = [
        locate("instr", TYPES_2, ScalarFuncSig::InstrUtf8),
        locate("instr", TYPES_2, ScalarFuncSig::Instr),
    ];
    static LOCATE: [BuiltinSignature; 4] = [
        locate("locate", TYPES_2, ScalarFuncSig::Locate2ArgsUtf8),
        locate("locate", TYPES_2, ScalarFuncSig::Locate2Args),
        locate("locate", TYPES_3, ScalarFuncSig::Locate3ArgsUtf8),
        locate("locate", TYPES_3, ScalarFuncSig::Locate3Args),
    ];
    static POSITION: [BuiltinSignature; 2] = [
        locate("position", TYPES_2, ScalarFuncSig::Locate2ArgsUtf8),
        locate("position", TYPES_2, ScalarFuncSig::Locate2Args),
    ];
    let family: &'static [BuiltinSignature] = match (name, arity) {
        ("instr", 2) => &INSTR,
        ("locate", 2) => &LOCATE[..2],
        ("locate", 3) => &LOCATE[2..],
        ("position", 2) => &POSITION,
        _ => return None,
    };
    Some(&family[usize::from(binary)])
}

/// Go `greatestFunctionClass` / `leastFunctionClass.getFunction` over
/// `resolveType4Extremum`'s answer: every argument and the result are
/// `argTp`, which with the string compare mode and the time kind selects the
/// signature. Go builds none over vectors.
fn resolve_extremum(
    name: &str,
    extremum: crate::builtin_ext::GlSignature,
) -> Option<&'static BuiltinSignature> {
    use crate::builtin_ext::GlCmpStringMode;
    static GREATEST: [BuiltinSignature; 11] = [
        signature("greatest", &[], &[], EvalType::Int, ScalarFuncSig::GreatestInt, false),
        signature("greatest", &[], &[], EvalType::Real, ScalarFuncSig::GreatestReal, false),
        signature("greatest", &[], &[], EvalType::Decimal, ScalarFuncSig::GreatestDecimal, false),
        string_signature("greatest", &[], &[], ScalarFuncSig::GreatestString, RetCollation::FunctionType),
        string_signature("greatest", &[], &[], ScalarFuncSig::GreatestCmpStringAsDate, RetCollation::FunctionType),
        string_signature("greatest", &[], &[], ScalarFuncSig::GreatestCmpStringAsTime, RetCollation::FunctionType),
        signature("greatest", &[], &[], EvalType::Duration, ScalarFuncSig::GreatestDuration, false),
        signature("greatest", &[], &[], EvalType::Datetime, ScalarFuncSig::GreatestDate, false),
        signature("greatest", &[], &[], EvalType::Datetime, ScalarFuncSig::GreatestTime, false),
        signature("greatest", &[], &[], EvalType::Timestamp, ScalarFuncSig::GreatestDate, false),
        signature("greatest", &[], &[], EvalType::Timestamp, ScalarFuncSig::GreatestTime, false),
    ];
    static LEAST: [BuiltinSignature; 11] = [
        signature("least", &[], &[], EvalType::Int, ScalarFuncSig::LeastInt, false),
        signature("least", &[], &[], EvalType::Real, ScalarFuncSig::LeastReal, false),
        signature("least", &[], &[], EvalType::Decimal, ScalarFuncSig::LeastDecimal, false),
        string_signature("least", &[], &[], ScalarFuncSig::LeastString, RetCollation::FunctionType),
        string_signature("least", &[], &[], ScalarFuncSig::LeastCmpStringAsDate, RetCollation::FunctionType),
        string_signature("least", &[], &[], ScalarFuncSig::LeastCmpStringAsTime, RetCollation::FunctionType),
        signature("least", &[], &[], EvalType::Duration, ScalarFuncSig::LeastDuration, false),
        signature("least", &[], &[], EvalType::Datetime, ScalarFuncSig::LeastDate, false),
        signature("least", &[], &[], EvalType::Datetime, ScalarFuncSig::LeastTime, false),
        signature("least", &[], &[], EvalType::Timestamp, ScalarFuncSig::LeastDate, false),
        signature("least", &[], &[], EvalType::Timestamp, ScalarFuncSig::LeastTime, false),
    ];
    let family = match name {
        "greatest" => &GREATEST,
        "least" => &LEAST,
        _ => return None,
    };
    Some(match (extremum.arg_type, extremum.cmp_string_mode, extremum.ret_date) {
        (EvalType::Int, ..) => &family[0],
        (EvalType::Real, ..) => &family[1],
        (EvalType::Decimal, ..) => &family[2],
        (EvalType::String, GlCmpStringMode::Directly, _) => &family[3],
        (EvalType::String, GlCmpStringMode::AsDate, _) => &family[4],
        (EvalType::String, GlCmpStringMode::AsDatetime, _) => &family[5],
        (EvalType::Duration, ..) => &family[6],
        (EvalType::Datetime, _, true) => &family[7],
        (EvalType::Datetime, _, false) => &family[8],
        (EvalType::Timestamp, _, true) => &family[9],
        (EvalType::Timestamp, _, false) => &family[10],
        (EvalType::Json | EvalType::VectorFloat32, ..) => return None,
    })
}

/// Go `strToDateFunctionClass.getFunction`: the result type `getRetTp` reads
/// from the format selects the signature over two `ETString`s.
fn resolve_str_to_date(ret: FieldTypeCode) -> Option<&'static BuiltinSignature> {
    const TYPES: &[EvalType] = &[EvalType::String, EvalType::String];
    static STR_TO_DATE: [BuiltinSignature; 3] = [
        signature("str_to_date", &[], TYPES, EvalType::Datetime, ScalarFuncSig::StrToDateDate, false),
        signature("str_to_date", &[], TYPES, EvalType::Datetime, ScalarFuncSig::StrToDateDatetime, false),
        signature("str_to_date", &[], TYPES, EvalType::Duration, ScalarFuncSig::StrToDateDuration, false),
    ];
    Some(match ret {
        FieldTypeCode::Date => &STR_TO_DATE[0],
        FieldTypeCode::Datetime => &STR_TO_DATE[1],
        FieldTypeCode::Duration => &STR_TO_DATE[2],
        _ => return None,
    })
}

/// Go `addTimeFunctionClass` / `subTimeFunctionClass.getFunction` over
/// `getBf4TimeAddSub`. The first argument's MySQL type gives its slot and the
/// result: a datetime or timestamp is a datetime, a time a time, a date a
/// datetime read into a string result, any other a string. The second's gives
/// its slot (a datetime or time is read as a time, any other as a string) and,
/// with the first's, the signature: a time adds, a datetime or timestamp
/// answers NULL, any other is parsed from its string.
fn resolve_add_sub_time(name: &str, args: &[Expression]) -> Option<&'static BuiltinSignature> {
    const fn time_add_sub(
        name: &'static str,
        arg_types: &'static [EvalType],
        ret: EvalType,
        sig: ScalarFuncSig,
    ) -> BuiltinSignature {
        BuiltinSignature {
            name,
            selector: &[],
            arg_types,
            ret,
            sig,
            ret_unsigned_from_first_arg: false,
            ret_collation: match ret {
                EvalType::String => RetCollation::FunctionType,
                _ => RetCollation::Numeric,
            },
        }
    }
    static ADD_TIME: [BuiltinSignature; 16] = [
        time_add_sub("addtime", &[EvalType::Datetime, EvalType::Duration], EvalType::Datetime, ScalarFuncSig::AddDatetimeAndDuration),
        time_add_sub("addtime", &[EvalType::Datetime, EvalType::Duration], EvalType::Datetime, ScalarFuncSig::AddTimeDateTimeNull),
        time_add_sub("addtime", &[EvalType::Datetime, EvalType::String], EvalType::Datetime, ScalarFuncSig::AddTimeDateTimeNull),
        time_add_sub("addtime", &[EvalType::Datetime, EvalType::String], EvalType::Datetime, ScalarFuncSig::AddDatetimeAndString),
        time_add_sub("addtime", &[EvalType::Datetime, EvalType::Duration], EvalType::String, ScalarFuncSig::AddDateAndDuration),
        time_add_sub("addtime", &[EvalType::Datetime, EvalType::Duration], EvalType::String, ScalarFuncSig::AddTimeStringNull),
        time_add_sub("addtime", &[EvalType::Datetime, EvalType::String], EvalType::String, ScalarFuncSig::AddTimeStringNull),
        time_add_sub("addtime", &[EvalType::Datetime, EvalType::String], EvalType::String, ScalarFuncSig::AddDateAndString),
        time_add_sub("addtime", &[EvalType::Duration, EvalType::Duration], EvalType::Duration, ScalarFuncSig::AddDurationAndDuration),
        time_add_sub("addtime", &[EvalType::Duration, EvalType::Duration], EvalType::Duration, ScalarFuncSig::AddTimeDurationNull),
        time_add_sub("addtime", &[EvalType::Duration, EvalType::String], EvalType::Duration, ScalarFuncSig::AddTimeDurationNull),
        time_add_sub("addtime", &[EvalType::Duration, EvalType::String], EvalType::Duration, ScalarFuncSig::AddDurationAndString),
        time_add_sub("addtime", &[EvalType::String, EvalType::Duration], EvalType::String, ScalarFuncSig::AddStringAndDuration),
        time_add_sub("addtime", &[EvalType::String, EvalType::Duration], EvalType::String, ScalarFuncSig::AddTimeStringNull),
        time_add_sub("addtime", &[EvalType::String, EvalType::String], EvalType::String, ScalarFuncSig::AddTimeStringNull),
        time_add_sub("addtime", &[EvalType::String, EvalType::String], EvalType::String, ScalarFuncSig::AddStringAndString),
    ];
    static SUB_TIME: [BuiltinSignature; 16] = [
        time_add_sub("subtime", &[EvalType::Datetime, EvalType::Duration], EvalType::Datetime, ScalarFuncSig::SubDatetimeAndDuration),
        time_add_sub("subtime", &[EvalType::Datetime, EvalType::Duration], EvalType::Datetime, ScalarFuncSig::SubTimeDateTimeNull),
        time_add_sub("subtime", &[EvalType::Datetime, EvalType::String], EvalType::Datetime, ScalarFuncSig::SubTimeDateTimeNull),
        time_add_sub("subtime", &[EvalType::Datetime, EvalType::String], EvalType::Datetime, ScalarFuncSig::SubDatetimeAndString),
        time_add_sub("subtime", &[EvalType::Datetime, EvalType::Duration], EvalType::String, ScalarFuncSig::SubDateAndDuration),
        time_add_sub("subtime", &[EvalType::Datetime, EvalType::Duration], EvalType::String, ScalarFuncSig::SubTimeStringNull),
        time_add_sub("subtime", &[EvalType::Datetime, EvalType::String], EvalType::String, ScalarFuncSig::SubTimeStringNull),
        time_add_sub("subtime", &[EvalType::Datetime, EvalType::String], EvalType::String, ScalarFuncSig::SubDateAndString),
        time_add_sub("subtime", &[EvalType::Duration, EvalType::Duration], EvalType::Duration, ScalarFuncSig::SubDurationAndDuration),
        time_add_sub("subtime", &[EvalType::Duration, EvalType::Duration], EvalType::Duration, ScalarFuncSig::SubTimeDurationNull),
        time_add_sub("subtime", &[EvalType::Duration, EvalType::String], EvalType::Duration, ScalarFuncSig::SubTimeDurationNull),
        time_add_sub("subtime", &[EvalType::Duration, EvalType::String], EvalType::Duration, ScalarFuncSig::SubDurationAndString),
        time_add_sub("subtime", &[EvalType::String, EvalType::Duration], EvalType::String, ScalarFuncSig::SubStringAndDuration),
        time_add_sub("subtime", &[EvalType::String, EvalType::Duration], EvalType::String, ScalarFuncSig::SubTimeStringNull),
        time_add_sub("subtime", &[EvalType::String, EvalType::String], EvalType::String, ScalarFuncSig::SubTimeStringNull),
        time_add_sub("subtime", &[EvalType::String, EvalType::String], EvalType::String, ScalarFuncSig::SubStringAndString),
    ];
    let family = match name {
        "addtime" => &ADD_TIME,
        "subtime" => &SUB_TIME,
        _ => return None,
    };
    let [first, second] = args else {
        return None;
    };
    let first = match first.static_type()?.code() {
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => 0,
        FieldTypeCode::Date => 1,
        FieldTypeCode::Duration => 2,
        _ => 3,
    };
    let second = match second.static_type()?.code() {
        FieldTypeCode::Duration => 0,
        FieldTypeCode::Datetime => 1,
        FieldTypeCode::Timestamp => 2,
        _ => 3,
    };
    Some(&family[first * 4 + second])
}

/// Go `timeDiffFunctionClass.getFunction`: each argument is a time, a
/// datetime (or timestamp) or, for any other type, a string, and the pair
/// selects the signature; a time against a datetime answers NULL.
fn resolve_time_diff(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    const fn time_diff(sig: ScalarFuncSig) -> BuiltinSignature {
        signature("timediff", &[], &[], EvalType::Duration, sig, false)
    }
    static TIME_DIFF: [BuiltinSignature; 9] = [
        time_diff(ScalarFuncSig::DurationDurationTimeDiff),
        time_diff(ScalarFuncSig::NullTimeDiff),
        time_diff(ScalarFuncSig::DurationStringTimeDiff),
        time_diff(ScalarFuncSig::NullTimeDiff),
        time_diff(ScalarFuncSig::TimeTimeTimeDiff),
        time_diff(ScalarFuncSig::TimeStringTimeDiff),
        time_diff(ScalarFuncSig::StringDurationTimeDiff),
        time_diff(ScalarFuncSig::StringTimeTimeDiff),
        time_diff(ScalarFuncSig::StringStringTimeDiff),
    ];
    let [left, right] = args else {
        return None;
    };
    let class = |arg: &PbScalar| match arg.eval_type() {
        EvalType::Duration => 0,
        EvalType::Datetime | EvalType::Timestamp => 1,
        _ => 2,
    };
    Some(&TIME_DIFF[class(left) * 3 + class(right)])
}

/// Go `intervalFunctionClass.getFunction`: all-`ETInt` arguments compare as
/// integers, any other mix as reals.
fn resolve_interval(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static INTERVAL: [BuiltinSignature; 2] = [
        signature("interval", &[], &[], EvalType::Int, ScalarFuncSig::IntervalInt, false),
        signature("interval", &[], &[], EvalType::Int, ScalarFuncSig::IntervalReal, false),
    ];
    if args.len() < 2 {
        return None;
    }
    Some(if args.iter().all(|arg| arg.eval_type() == EvalType::Int) {
        &INTERVAL[0]
    } else {
        &INTERVAL[1]
    })
}

/// Go `makeSetFunctionClass.getFunction`: an `ETInt` bit set, then
/// `ETString`s.
fn resolve_make_set(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static MAKE_SET: BuiltinSignature =
        string_signature("make_set", &[], &[], ScalarFuncSig::MakeSet, RetCollation::FunctionType);
    (args.len() >= 2).then_some(&MAKE_SET)
}

/// Go `charFunctionClass.getFunction`: `ETInt` code points, then the
/// `ETString` charset name.
fn resolve_char(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static CHAR: BuiltinSignature =
        string_signature("char_func", &[], &[], ScalarFuncSig::Char, RetCollation::FunctionType);
    (args.len() >= 2).then_some(&CHAR)
}

/// Go `ftsMysqlMatchAgainstFunctionClass.getFunction`: the `ETString` query,
/// then the `ETString` columns it is matched against; an `ETReal` score.
fn resolve_match_against(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    static MATCH_AGAINST: BuiltinSignature =
        signature("match_against", &[], &[], EvalType::Real, ScalarFuncSig::FtsMatchExpression, false);
    (args.len() >= 2).then_some(&MATCH_AGAINST)
}

/// Go `insertFunctionClass.getFunction`: the function's derived collation
/// being `binary` selects the byte signature over the multibyte one.
fn resolve_insert(arity: usize, binary: bool) -> Option<&'static BuiltinSignature> {
    const TYPES: &[EvalType] = &[EvalType::String, EvalType::Int, EvalType::Int, EvalType::String];
    static INSERT: [BuiltinSignature; 2] = [
        string_signature("insert_func", &[], TYPES, ScalarFuncSig::InsertUtf8, RetCollation::FunctionType),
        string_signature("insert_func", &[], TYPES, ScalarFuncSig::Insert, RetCollation::FunctionType),
    ];
    (arity == 4).then(|| &INSERT[usize::from(binary)])
}

/// Go `isTrueOrFalseFunctionClass.getFunction`: a temporal, JSON or string
/// argument is read as `ETReal`, an integer, real or decimal one in its own
/// type. The vector signatures have no pb code.
fn resolve_is_true_or_false(name: &str, args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    let [argument] = args else {
        return None;
    };
    let arg_tp = match argument.eval_type() {
        EvalType::Timestamp | EvalType::Datetime | EvalType::Duration | EvalType::Json | EvalType::String => {
            EvalType::Real
        }
        own => own,
    };
    CATALOG
        .iter()
        .find(|candidate| candidate.name == name && candidate.arg_types == [arg_tp])
}

/// The signature Go's `getFunction` chose for the function node `expression`
/// over its described arguments. A family whose choice reads the node -- its
/// result type, its derived collation, its arguments' field types -- is
/// answered from the node; every other from the arguments, as [`resolve`].
fn node_signature(
    expression: &Expression,
    function: &crate::scalar_function::ScalarFunction,
    args: &[PbScalar],
) -> Option<&'static BuiltinSignature> {
    let name = function.func_name.lowercase();
    match name {
        "unaryminus" => resolve_unary_minus(expression.static_type()?.eval_type()),
        "ceil" | "ceiling" | "floor" => {
            let [arg] = args else {
                return None;
            };
            resolve_ceil_floor(name, arg.eval_type(), expression.static_type()?.eval_type())
        }
        "addtime" | "subtime" => resolve_add_sub_time(name, &function.args),
        "intdiv" => resolve_int_divide(&function.args),
        "greatest" | "least" => resolve_extremum(name, function.extremum_signature()?),
        "str_to_date" => resolve_str_to_date(expression.static_type()?.code()),
        "instr" | "locate" | "position" => resolve_locate(
            name,
            args.len(),
            function.collation.charset_and_collation().1 == BINARY_COLLATION,
        ),
        "insert_func" => resolve_insert(
            args.len(),
            function.collation.charset_and_collation().1 == BINARY_COLLATION,
        ),
        _ => resolve(name, args),
    }
}

/// Describes a call of `name` over `args`, when the catalog resolves one.
#[must_use]
pub fn build_call(name: &str, args: Vec<PbScalar>) -> Option<PbScalar> {
    build_call_with(name, args, |args| resolve(name, args))
}

/// [`build_call`] with the signature of a non-control family chosen by
/// `resolve`, over the arguments as the call carries them.
/// Go `wrapWithIsTrue(ctx, true, arg, false)` over the conditions of `if`,
/// `case`, `and`, `or` and `xor`, as their `getFunction` applies it: an
/// integer condition stays, any other is read through `is_true`. Casting
/// directly to Int would lose nonzero fractions.
fn wrap_conditions(
    name: &str,
    args: Vec<PbScalar>,
    is_true: impl Fn(PbScalar) -> Option<PbScalar>,
) -> Option<Vec<PbScalar>> {
    let arity = args.len();
    args.into_iter()
        .enumerate()
        .map(|(index, argument)| {
            let condition = (name == "if" && arity == 3 && index == 0)
                || (name == "case" && index % 2 == 0 && index + 1 < arity)
                || (matches!(name, "and" | "or" | "xor") && arity == 2);
            if condition && argument.eval_type() != EvalType::Int {
                is_true(argument)
            } else {
                Some(argument)
            }
        })
        .collect()
}

fn build_call_with(
    name: &str,
    args: Vec<PbScalar>,
    resolve: impl FnOnce(&[PbScalar]) -> Option<&'static BuiltinSignature>,
) -> Option<PbScalar> {
    let args = wrap_conditions(name, args, |condition| {
        build_call("istrue_with_null", vec![condition])
    })?;
    let (signature, control) = if matches!(name, "case" | "if" | "ifnull" | "coalesce") {
        let expression = control_expression(name, &args)?;
        let field_type = expression.static_type()?.clone();
        let signature = resolve_control_type(name, field_type.eval_type())?;
        let collation = crate::expr_collation::ExprCollation {
            coer: crate::collation_derive::coercibility_of(&expression),
            repe: crate::collation_derive::repertoire_of(&expression),
            charset: field_type.charset_name().to_owned(),
            collation: field_type.collation_name().to_owned(),
        };
        (
            signature,
            Some(Box::new(ControlMetadata {
                field_type,
                collation,
            })),
        )
    } else {
        (resolve(&args)?, None)
    };
    Some(PbScalar::Call {
        signature,
        args,
        control,
        ret_type: None,
        metadata: None,
    })
}

/// Go `PbConverter.ExprToPB` without a statement: the expression's TiPB
/// description, or `None` where Go returns a nil `*tipb.Expr`. Like Go's
/// converter it answers for no store in particular; whether TiKV or TiFlash
/// takes the expression is `crate::infer_pushdown::can_exprs_push_down`.
/// This context-free entry point accepts strict literals only and reads no
/// blacklist. Execution-time parameter and correlated values, and the
/// statement's blacklist, belong to [`expr_to_pb`].
#[must_use]
pub fn from_expression(expression: &Expression) -> Option<PbScalar> {
    from_expression_with_context(expression, &crate::NoColumns, false)
        .ok()
        .flatten()
}

/// Go `PbConverter.ExprToPB`: read constants and correlated columns from the
/// current evaluation context, without cloning or rewriting the expression.
pub fn expr_to_pb(
    expression: &Expression,
    context: &dyn crate::Columns,
) -> Result<Option<PbScalar>, crate::EvalError> {
    from_expression_with_context(expression, context, true)
}

/// What Go `NewPBConverter(client, ctx)` is handed: the statement's
/// `exprctx.EvalContext`, whose constants, parameters and correlated bindings
/// `ExprToPB` reads ([`crate::Columns`]), plus the expression pushdown
/// blacklist every conversion is gated by. Go reads the blacklist from the
/// global `DefaultExprPushDownBlacklist`; this tier captures it per statement,
/// so the statement's context answers it.
pub trait PbConverterContext: crate::Columns + Send + Sync {}

impl<T: crate::Columns + Send + Sync + ?Sized> PbConverterContext for T {}

/// Go `PbConverter.conOrCorColToPBExpr`: every deferred constant (a parameter
/// marker or a value bound at execution) and every correlated column under
/// `expression` becomes the constant `context` holds for it now. The caller
/// binds a request-owned copy, so the retained plan expression is untouched
/// for the next Open.
pub fn con_or_cor_col_to_pb_expr(
    expression: &mut Expression,
    context: &dyn crate::Columns,
) -> Result<(), crate::EvalError> {
    use crate::constant::Constant;
    match expression {
        Expression::Constant(constant) if constant.literal_value().is_none() => {
            let value = constant.eval_in(context)?;
            let field_type = if constant.param_marker.is_some() {
                tidb_datatype::infer_param_type_from_datum(&value)
            } else {
                constant
                    .ret_type
                    .clone()
                    .ok_or(crate::EvalError::Unsupported(
                        "deferred aggregate constant has no result type",
                    ))?
            };
            *constant = Constant::new(value, field_type);
        }
        Expression::CorrelatedColumn(column) => {
            let field_type =
                column
                    .column
                    .ret_type
                    .clone()
                    .ok_or(crate::EvalError::Unsupported(
                        "correlated aggregate column has no result type",
                    ))?;
            *expression = Expression::Constant(Constant::new(column.eval(), field_type));
        }
        Expression::ScalarFunction(function) => {
            for argument in &mut function.args {
                con_or_cor_col_to_pb_expr(argument, context)?;
            }
        }
        Expression::Column(_) | Expression::Constant(_) | Expression::ScalarSubQuery(_) => {}
    }
    Ok(())
}

fn from_expression_with_context(
    expression: &Expression,
    context: &dyn crate::Columns,
    bind_values: bool,
) -> Result<Option<PbScalar>, crate::EvalError> {
    match expression {
        Expression::Column(column) => Ok(u32::try_from(column.index)
            .ok()
            .zip(column.get_static_type())
            .map(|(offset, field_type)| PbScalar::Column {
                offset,
                field_type: field_type.clone(),
            })),
        Expression::Constant(constant) => {
            let value = if bind_values {
                constant.eval_in(context)?
            } else if let Some(value) = constant.literal_value() {
                value.clone()
            } else {
                return Ok(None);
            };
            // Go Constant.GetType(ctx) infers a marker's type from the current
            // datum, independently of its planning-time RetType.
            let parameter_type = (bind_values && constant.param_marker.is_some())
                .then(|| tidb_datatype::infer_param_type_from_datum(&value));
            Ok(describe_literal(
                value,
                parameter_type.as_ref().or(constant.ret_type.as_ref()),
            ))
        }
        Expression::CorrelatedColumn(column) if bind_values => Ok(describe_literal(
            column.eval(),
            column.column.ret_type.as_ref(),
        )),
        Expression::ScalarFunction(function) => {
            let mut args = Vec::with_capacity(function.args.len());
            for argument in &function.args {
                let Some(argument) = from_expression_with_context(argument, context, bind_values)?
                else {
                    return Ok(None);
                };
                args.push(argument);
            }
            let Some(mut described) = describe_function(expression, function, args) else {
                return Ok(None);
            };
            // Go `scalarFuncToPBExpr`: a function with no signature code, or
            // one `canFuncBePushed(.., kv.UnSpecified)` refuses -- no store
            // lists it, or the blacklist names it -- has no TiPB form. The
            // signature is read off the described call, so the check follows
            // the arguments here; either order refuses alike.
            let signature = match &described {
                PbScalar::Call { signature, .. } => signature.sig,
                PbScalar::Cast { signature, .. } => *signature,
                _ => ScalarFuncSig::Unspecified,
            };
            if !crate::infer_pushdown::scalar_function_can_be_pushed(
                expression,
                function,
                signature,
                tidb_kv::store_type::StoreType::Unspecified,
                context.expr_pushdown_blacklist(),
            ) {
                return Ok(None);
            }
            if let PbScalar::Call { metadata, .. } = &mut described {
                let Some(encoded) = function_metadata(function) else {
                    return Ok(None);
                };
                *metadata = encoded;
            }
            Ok(Some(described))
        }
        // Go `PbConverter.ExprToPB` has no arm for `ScalarSubQueryExpr`.
        Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => Ok(None),
    }
}

/// Go `expr.Function.metadata()` as `scalarFuncToPBExpr` marshals it into the
/// function node's `Val`: `Some(None)` for a function with none, and `None`
/// where Go's `proto.Marshal` fails and the function is not converted -- a
/// `GROUPING` whose metadata was never set marshals no required `mode`.
/// (A cast's `InUnionMetadata` is written where the cast node is encoded.)
fn function_metadata(function: &crate::scalar_function::ScalarFunction) -> Option<Option<Vec<u8>>> {
    use prost::Message as _;
    if function.func_name.lowercase() != "grouping" {
        return Some(None);
    }
    let metadata = function.grouping_metadata().ok()?;
    let mode = match metadata.mode() {
        crate::grouping::GroupingMode::BitAnd => tidb_proto::tipb::GroupingMode::ModeBitAnd,
        crate::grouping::GroupingMode::NumericCmp => tidb_proto::tipb::GroupingMode::ModeNumericCmp,
        crate::grouping::GroupingMode::NumericSet => tidb_proto::tipb::GroupingMode::ModeNumericSet,
    };
    Some(Some(
        tidb_proto::tipb::GroupingFunctionMetadata {
            mode: mode as i32,
            grouping_marks: metadata
                .grouping_marks()
                .iter()
                .map(|mark| tidb_proto::tipb::GroupingMark {
                    grouping_nums: mark.iter().copied().collect(),
                })
                .collect(),
        }
        .encode_to_vec(),
    ))
}

/// The TiPB form of the scalar function `expression` over its described
/// arguments: the signature Go's `getFunction` chose for it and the result
/// type its node carries.
fn describe_function(
    expression: &Expression,
    function: &crate::scalar_function::ScalarFunction,
    mut args: Vec<PbScalar>,
) -> Option<PbScalar> {
    let name = function.func_name.lowercase();
    if name.starts_with("cast_") && !name.ends_with("_in_union") && args.len() == 1 {
        let field = expression.static_type()?;
        let signature = cast_node_signature(args[0].eval_type(), field.eval_type())?;
        return Some(PbScalar::Cast {
            signature,
            arg: Box::new(args.pop().expect("one argument")),
            field_type: field.clone(),
        });
    }
    let mut described = build_call_with(name, args, |args| {
        node_signature(expression, function, args)
    });
    if let Some(PbScalar::Call {
        signature,
        control: Some(control),
        ..
    }) = &mut described
    {
        // Go's encoder reads the resolved function, including explicit
        // result metadata, rather than inferring it again from children.
        let field = expression.static_type()?;
        let resolved = resolve_control_type(function.func_name.lowercase(), field.eval_type())?;
        *signature = resolved;
        control.field_type = field.clone();
        control.collation = crate::expr_collation::ExprCollation {
            coer: crate::collation_derive::coercibility_of(expression),
            repe: crate::collation_derive::repertoire_of(expression),
            charset: field.charset_name().to_owned(),
            collation: field.collation_name().to_owned(),
        };
    }
    // Go `scalarFuncToPBExpr` sends every function node's own
    // `RetType`, with the function's collation forced into it -- a
    // CAST's target width included, which a catalog default would
    // lose.
    if let Some(PbScalar::Call {
        control: None,
        ret_type,
        ..
    }) = &mut described
    {
        // A function built without a result type keeps the catalog's.
        if let Some(field) = expression.static_type() {
            let mut field = field.clone();
            if tidb_datatype::new_collation_enabled() {
                let (_, collation) = function.collation.charset_and_collation();
                if !collation.is_empty() {
                    field.set_collation_name(collation);
                }
            }
            *ret_type = Some(field);
        }
    }
    // `to_binary` / `from_binary` carry their own result type (the
    // charset they convert to), which the encoder sends as is.
    if matches!(function.func_name.lowercase(), "to_binary" | "from_binary") {
        if let Some(PbScalar::Call { control, .. }) = &mut described {
            let field = expression.static_type()?;
            *control = Some(Box::new(ControlMetadata {
                field_type: field.clone(),
                collation: crate::expr_collation::ExprCollation {
                    coer: crate::collation_derive::coercibility_of(expression),
                    repe: crate::collation_derive::repertoire_of(expression),
                    charset: field.charset_name().to_owned(),
                    collation: field.collation_name().to_owned(),
                },
            }));
        }
    }
    described
}

/// Go `ScalarFunction.Function.PbCode()`: the signature of the function
/// itself, whatever its arguments are. An argument with no TiPB form of its
/// own stands as a value of its type, which is all a signature reads of it.
#[must_use]
pub fn pb_code(expression: &Expression) -> ScalarFuncSig {
    let Expression::ScalarFunction(function) = expression else {
        return ScalarFuncSig::Unspecified;
    };
    // The signature the function's class set (`setPbCode`), when its class
    // built it.
    if let Some(signature) = function.pb_signature() {
        return signature;
    }
    let stand_in = |field_type: FieldType| PbScalar::Column {
        offset: 0,
        field_type,
    };
    let args = function
        .args
        .iter()
        .map(|argument| {
            from_expression(argument).or_else(|| argument.static_type().cloned().map(stand_in))
        })
        .collect::<Option<Vec<_>>>()
        .and_then(|args| {
            // A condition's `istrue_with_null` is an argument like any other:
            // the vector one has no TiPB form, and stands as its bigint.
            wrap_conditions(function.func_name.lowercase(), args, |condition| {
                build_call("istrue_with_null", vec![condition])
                    .or_else(|| Some(stand_in(FieldType::new(FieldTypeCode::LongLong))))
            })
        });
    match args.and_then(|args| describe_function(expression, function, args)) {
        Some(PbScalar::Call { signature, .. }) => signature.sig,
        Some(PbScalar::Cast { signature, .. }) => signature,
        _ => ScalarFuncSig::Unspecified,
    }
}

fn describe_literal(value: Datum, field_type: Option<&FieldType>) -> Option<PbScalar> {
    match &value {
        Datum::Null => Some(PbScalar::NullLiteral {
            field_type: field_type?.clone(),
        }),
        Datum::Int(value) => Some(match field_type {
            Some(field_type) => PbScalar::IntLiteral {
                value: *value,
                field_type: field_type.clone(),
            },
            None => PbScalar::int_literal(*value),
        }),
        Datum::UInt(value) => Some(PbScalar::UIntLiteral {
            value: *value,
            field_type: field_type?.clone(),
        }),
        Datum::Decimal(value) => Some(PbScalar::DecimalLiteral {
            value: value.clone(),
            field_type: field_type?.clone(),
        }),
        Datum::Real(value) if !(*value == 0.0 && value.is_sign_negative()) => {
            Some(PbScalar::RealLiteral {
                value: *value,
                field_type: field_type?.clone(),
            })
        }
        Datum::Float32(value) if !(*value == 0.0 && value.is_sign_negative()) => {
            Some(PbScalar::Float32Literal {
                value: *value,
                field_type: field_type?.clone(),
            })
        }
        Datum::String(value) => Some(PbScalar::StringLiteral {
            value: value.bytes().to_vec(),
            field_type: field_type?.clone(),
        }),
        Datum::Bytes(value) => Some(PbScalar::BytesLiteral {
            value: value.clone(),
            field_type: field_type?.clone(),
        }),
        Datum::BinaryLiteral(value) => Some(PbScalar::StringLiteral {
            value: value.as_bytes().to_vec(),
            field_type: field_type?.clone(),
        }),
        Datum::Bit(value) => Some(PbScalar::BitLiteral {
            value: value.clone(),
            field_type: field_type?.clone(),
        }),
        Datum::Enum(value, _) => Some(PbScalar::EnumLiteral {
            value: value.value(),
            field_type: field_type?.clone(),
        }),
        Datum::Time(value) => Some(PbScalar::TimeLiteral {
            value: *value,
            field_type: field_type?.clone(),
        }),
        Datum::Duration(value) => Some(PbScalar::DurationLiteral {
            value: *value,
            field_type: field_type?.clone(),
        }),
        // Go encodeDatum has no KindMysqlJSON arm.
        Datum::Json(_) => None,
        Datum::VectorFloat32(value) => Some(PbScalar::VectorLiteral {
            value: value.clone(),
            field_type: field_type?.clone(),
        }),
        _ => None,
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
    expression_to_pb_in(expression, columns, &tidb_datatype::SessionTimeZone::utc())
}

/// Encode expression literals in the statement's timezone.
#[must_use]
pub fn expression_to_pb_in(
    expression: &Expression,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Expr> {
    described_to_pb_in(expression, from_expression(expression)?, columns, zone)
}

/// [`expression_to_pb_in`] as Go's `PbConverter.ExprToPB` over a statement:
/// a deferred constant or correlated column under `expression` is the value
/// `context` holds for it now (`conOrCorColToPBExpr`).
pub fn expression_to_pb_in_context(
    expression: &Expression,
    context: &dyn crate::Columns,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Expr> {
    let described = from_expression_with_context(expression, context, true).ok().flatten()?;
    described_to_pb_in(expression, described, columns, zone)
}

fn described_to_pb_in(
    expression: &Expression,
    described: PbScalar,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Expr> {
    let mut encoded = to_pb_in(&described, columns, zone)?;
    let mut field_type = expression.static_type()?.clone();
    // Go `scalarFuncToPBExpr`: "put collation information into the RetType
    // enforcedly and push it down" -- a function's own collation, which a
    // non-string result does not carry on its type, is the one TiKV runs it
    // under.
    if let Expression::ScalarFunction(function) = expression {
        if tidb_datatype::new_collation_enabled() {
            let (_, collation) = function.collation.charset_and_collation();
            if !collation.is_empty() {
                field_type.set_collation_name(collation);
            }
        }
    }
    encoded.field_type = Some(to_pb_field_type(&field_type)?);
    Some(encoded)
}

/// Go `expression.PushDownContext` as the aggregate and window lowering
/// read it: the converter, the evaluation context's rendering, and
/// `group_concat_max_len`.
pub trait PushDownContext {
    /// Go `PbConverter().ExprToPB`.
    fn expr_to_pb(&self, expr: &Expression) -> Option<Expr>;
    /// Go `expr.StringWithCtx(ctx.EvalCtx(), errors.RedactLogDisable)`.
    fn expr_string(&self, expr: &Expression) -> String;
    /// Go `GetGroupConcatMaxLen`.
    fn group_concat_max_len(&self) -> u64;
}

/// Go `ToPBFieldTypeWithCheck`.
///
/// # Errors
/// A TiFlash type whose decimal is invalid, or a type with no PB form.
pub fn to_pb_field_type_with_check(
    ft: &FieldType,
    store_type: tidb_kv::store_type::StoreType,
) -> Result<tidb_proto::tipb::FieldType, String> {
    if store_type == tidb_kv::store_type::StoreType::TiFlash && !ft.is_decimal_valid() {
        return Err(format!(
            "{ft} can not be pushed to TiFlash because it contains invalid decimal('{}','{}').",
            ft.flen(),
            ft.decimal()
        ));
    }
    to_pb_field_type(ft).ok_or_else(|| format!("{ft} has no PB field type"))
}

/// Go `ToPBFieldType` for a type used by this bounded expression closure.
#[must_use]
pub fn to_pb_field_type(field_type: &FieldType) -> Option<tidb_proto::tipb::FieldType> {
    if !leaf_column_family(field_type.code()) {
        return None;
    }
    let mut result = pb_field_type(
        i32::from(field_type.code().mysql_type()),
        field_type.flags(),
        // Go `ToPBFieldType`: `int32(ft.GetFlen())` and `int32(ft.GetDecimal())`.
        field_type.flen() as i32,
        field_type.decimal() as i32,
        field_type.charset_name(),
        field_type.collation_name(),
    );
    result.elems = field_type
        .elems_snapshot()
        .into_iter()
        .map(|elem| elem.to_string())
        .collect();
    result.array = Some(field_type.is_array());
    Some(result)
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
    /// ENUM/SET element names copied by Go's `ToPBFieldType`.
    pub elems: Vec<String>,
    /// Whether the descriptor carries TiDB's ARRAY marker.
    pub array: bool,
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
    to_pb_in(scalar, columns, &tidb_datatype::SessionTimeZone::utc())
}

/// Encode a described expression using the statement's timezone recursively.
#[must_use]
pub fn to_pb_in(
    scalar: &PbScalar,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
    zone: &tidb_datatype::SessionTimeZone,
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
            let pb_type = FieldType::new(code)
                .with_flags(declared.flag)
                .with_flen(i64::from(declared.flen))
                .with_decimal(i64::from(declared.decimal))
                .with_charset_name(declared.charset.clone())
                .with_collation_name(declared.collation.clone())
                .with_elems(declared.elems.clone())
                .with_array(declared.array);
            Some(leaf(
                ExprType::ColumnRef,
                encode_signed(i64::from(*offset)),
                to_pb_field_type(&pb_type)?,
            ))
        }
        PbScalar::NullLiteral { field_type } => Some(Expr {
            tp: Some(ExprType::Null as i32),
            val: None,
            children: Vec::new(),
            sig: Some(ScalarFuncSig::Unspecified as i32),
            field_type: Some(to_pb_field_type(field_type)?),
            has_distinct: Some(false),
            agg_func_mode: None,
            ..Default::default()
        }),
        PbScalar::IntLiteral { value, field_type } => Some(leaf(
            ExprType::Int64,
            encode_signed(*value),
            to_pb_field_type(field_type)?,
        )),
        PbScalar::UIntLiteral { value, field_type } => {
            let mut encoded = Vec::new();
            tidb_codec::encode_uint(&mut encoded, *value);
            Some(leaf(
                ExprType::Uint64,
                encoded,
                to_pb_field_type(field_type)?,
            ))
        }
        PbScalar::DecimalLiteral { value, field_type } => {
            let mut encoded = Vec::new();
            tidb_codec::encode_decimal_fixed(&mut encoded, value, 0, 0).ok()?;
            Some(leaf(
                ExprType::MysqlDecimal,
                encoded,
                to_pb_field_type(field_type)?,
            ))
        }
        PbScalar::RealLiteral { value, field_type }
        | PbScalar::Float32Literal { value, field_type } => {
            if *value == 0.0 && value.is_sign_negative() {
                return None;
            }
            let mut encoded = Vec::new();
            tidb_codec::encode_float(&mut encoded, *value);
            Some(leaf(
                if matches!(scalar, PbScalar::Float32Literal { .. }) {
                    ExprType::Float32
                } else {
                    ExprType::Float64
                },
                encoded,
                to_pb_field_type(field_type)?,
            ))
        }
        PbScalar::StringLiteral { value, field_type } => Some(leaf(
            ExprType::String,
            value.clone(),
            to_pb_field_type(field_type)?,
        )),
        PbScalar::BytesLiteral { value, field_type } => Some(leaf(
            ExprType::Bytes,
            value.clone(),
            to_pb_field_type(field_type)?,
        )),
        PbScalar::BitLiteral { value, field_type } => Some(leaf(
            ExprType::MysqlBit,
            value.as_bytes().to_vec(),
            to_pb_field_type(field_type)?,
        )),
        PbScalar::EnumLiteral { value, field_type } => {
            let mut encoded = Vec::new();
            tidb_codec::encode_uint(&mut encoded, *value);
            Some(leaf(
                ExprType::MysqlEnum,
                encoded,
                to_pb_field_type(field_type)?,
            ))
        }
        PbScalar::TimeLiteral { value, field_type } => {
            let mut encoded = Vec::new();
            let kind = match field_type.code() {
                FieldTypeCode::Timestamp => tidb_datatype::TimeType::Timestamp,
                FieldTypeCode::Date => tidb_datatype::TimeType::Date,
                _ => tidb_datatype::TimeType::DateTime,
            };
            tidb_codec::encode_mysql_time(zone, *value, Some(kind), &mut encoded).ok()?;
            Some(leaf(
                ExprType::MysqlTime,
                encoded,
                to_pb_field_type(field_type)?,
            ))
        }
        PbScalar::DurationLiteral { value, field_type } => Some(leaf(
            ExprType::MysqlDuration,
            encode_signed(value.nanoseconds()),
            to_pb_field_type(field_type)?,
        )),
        PbScalar::JsonLiteral { .. } => None,
        PbScalar::VectorLiteral { value, field_type } => Some(leaf(
            ExprType::TiDbVectorFloat32,
            value.serialize(),
            to_pb_field_type(field_type)?,
        )),
        PbScalar::Cast {
            signature,
            arg,
            field_type,
        } => Some(cast_expr(*signature, to_pb_in(arg, columns, zone)?, to_pb_field_type(field_type)?)),
        PbScalar::Call {
            signature,
            args,
            control,
            ret_type,
            metadata,
        } => {
            let control_field = control.as_ref().map(|metadata| &metadata.field_type);
            let mut children = Vec::with_capacity(args.len());
            for (index, argument) in args.iter().enumerate() {
                let required = signature.arg_type_at(index, args.len(), argument.eval_type())?;
                // Go newBaseBuiltinFuncWithFieldTypes aligns decimal and temporal
                // precision even when the argument already has that eval type.
                if let Some(field) = control_field.filter(|_| {
                    matches!(
                        required,
                        EvalType::Decimal
                            | EvalType::Datetime
                            | EvalType::Timestamp
                            | EvalType::Duration
                    )
                }) {
                    let mut child = to_pb_in(argument, columns, zone)?;
                    let target = to_pb_field_type(field)?;
                    if child.field_type.as_ref() != Some(&target) {
                        let cast = match (argument.eval_type(), required) {
                            (EvalType::Decimal, EvalType::Decimal) => {
                                ScalarFuncSig::CastDecimalAsDecimal
                            }
                            (
                                EvalType::Datetime | EvalType::Timestamp,
                                EvalType::Datetime | EvalType::Timestamp,
                            ) => ScalarFuncSig::CastTimeAsTime,
                            (EvalType::Duration, EvalType::Duration) => {
                                ScalarFuncSig::CastDurationAsDuration
                            }
                            (source, target) => cast_signature(source, target)??,
                        };
                        child = cast_expr(cast, child, target);
                    }
                    children.push(child);
                } else {
                    children.push(coerced_to_pb(argument, required, columns, zone)?);
                }
            }
            let return_field_type = if let Some(field) = control_field.or(ret_type.as_ref()) {
                to_pb_field_type(field)?
            } else {
                signature.return_field_type(scalar.is_unsigned(), &children)?
            };
            Some(Expr {
                tp: Some(ExprType::ScalarFunc as i32),
                val: metadata.clone(),
                children,
                sig: Some(signature.sig as i32),
                field_type: Some(return_field_type),
                has_distinct: Some(false),
                agg_func_mode: None,
                ..Default::default()
            })
        }
    }
}

/// One argument, wrapped in the implicit cast the signature's slot needs.
fn coerced_to_pb(
    argument: &PbScalar,
    required: EvalType,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Expr> {
    // Go folds a deterministic cast over a constant and sends the target
    // literal family, not a ScalarFunc cast node.
    if let PbScalar::IntLiteral { value, .. } = argument {
        if required == EvalType::Int {
            return to_pb_in(argument, columns, zone);
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
            // Unlike numeric constants, Go keeps an integer child and emits
            // the corresponding temporal/JSON cast node.  Let the generic
            // path below build that node with the target metadata.
            EvalType::String
            | EvalType::Datetime
            | EvalType::Timestamp
            | EvalType::Duration
            | EvalType::Json => {
                // Fall through after the constant-specialization block.
                let cast = cast_signature(argument.eval_type(), required)?;
                let Some(cast) = cast else {
                    return to_pb_in(argument, columns, zone);
                };
                let field_type = cast_target_field_type(argument, required)?;
                return Some(cast_expr(cast, to_pb_in(argument, columns, zone)?, field_type));
            }
            _ => None,
        };
    }
    let cast = cast_signature(argument.eval_type(), required)?;
    let lowered = to_pb_in(argument, columns, zone)?;
    let Some(cast) = cast else {
        return Some(lowered);
    };
    // Go `WrapWithCastAsInt` / `WrapWithCastAsDecimal` size the implicit
    // cast from the argument's own type: a BIGINT becomes DECIMAL(20,0), not
    // a default-width DECIMAL that would clamp its large values.
    let source = lowered.field_type.as_ref();
    let source_code = source
        .and_then(|field| u8::try_from(field.tp()).ok())
        .map(FieldTypeCode::from_mysql_type);
    let source_flen = source.map_or(UNSPECIFIED_LENGTH, |field| field.flen());
    let source_decimal = source.map_or(UNSPECIFIED_LENGTH, |field| field.decimal());
    let field_type = match required {
        EvalType::Int => int_field_type(
            FieldTypeCode::LongLong.mysql_type().into(),
            cast_flags(argument),
            source_flen,
            0,
        ),
        EvalType::Real => int_field_type(
            FieldTypeCode::Double.mysql_type().into(),
            cast_flags(argument),
            MAX_REAL_WIDTH,
            UNSPECIFIED_LENGTH,
        ),
        EvalType::Decimal => {
            let (mut flen, decimal) = if argument.eval_type() == EvalType::Int {
                let width = source_code.map_or(i64::from(MAX_INT_WIDTH), |code| {
                    crate::wrap_cast::minimal_decimal_len_for_holding_integer(code)
                });
                (i32::try_from(width).unwrap_or(MAX_INT_WIDTH), 0)
            } else {
                (
                    source_flen.min(MAX_DECIMAL_WIDTH_I32),
                    source_decimal.min(MAX_DECIMAL_SCALE_I32),
                )
            };
            if flen == UNSPECIFIED_LENGTH || flen > MAX_DECIMAL_WIDTH_I32 {
                flen = MAX_DECIMAL_WIDTH_I32;
            }
            int_field_type(
                FieldTypeCode::NewDecimal.mysql_type().into(),
                cast_flags(argument),
                flen,
                decimal,
            )
        }
        _ => cast_target_field_type(argument, required)?,
    };
    Some(Expr {
        tp: Some(ExprType::ScalarFunc as i32),
        val: None,
        children: vec![lowered],
        sig: Some(cast as i32),
        field_type: Some(field_type),
        has_distinct: Some(false),
        agg_func_mode: None,
        ..Default::default()
    })
}

/// Builds the target metadata used by Go's temporal, duration, string and
/// JSON implicit casts.  These targets are part of the wire contract: TiKV
/// uses the FSP and JSON charset/flag when evaluating the cast, so emitting a
/// generic binary field type would not be equivalent to `WrapWithCastAsTime`
/// or `WrapWithCastAsJSON`.
fn cast_target_field_type(
    argument: &PbScalar,
    required: EvalType,
) -> Option<tidb_proto::tipb::FieldType> {
    let source = argument.static_field_type();
    let source_decimal = source.map(FieldType::decimal).unwrap_or(0);
    let source_eval = source
        .map(FieldType::eval_type)
        .unwrap_or_else(|| argument.eval_type());
    let fsp = match required {
        EvalType::Datetime | EvalType::Timestamp => match source_eval {
            EvalType::Int => 0,
            EvalType::String | EvalType::Real | EvalType::Json => 6,
            EvalType::Datetime | EvalType::Timestamp | EvalType::Duration | EvalType::Decimal => {
                source_decimal.clamp(0, 6)
            }
            _ => return None,
        },
        EvalType::Duration => match source.map(FieldType::code) {
            Some(FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp) => {
                source_decimal.clamp(0, 6)
            }
            Some(_) | None => 6,
        },
        _ => 0,
    };
    let field_type = match required {
        EvalType::String => {
            let (charset, collation) = crate::collation_derive::connection_charset_info();
            FieldType::new(FieldTypeCode::VarString)
                .with_flen(i64::from(UNSPECIFIED_LENGTH))
                .with_decimal(i64::from(UNSPECIFIED_LENGTH))
                .with_charset_name(charset)
                .with_collation_name(collation)
        }
        EvalType::Datetime => FieldType::new(FieldTypeCode::Datetime)
            .with_flags(BINARY_FLAG)
            .with_flen(19 + if fsp > 0 { fsp + 1 } else { 0 })
            .with_decimal(fsp)
            .with_charset_name(BINARY_CHARSET)
            .with_collation_name(BINARY_COLLATION),
        EvalType::Timestamp => FieldType::new(FieldTypeCode::Timestamp)
            .with_flags(BINARY_FLAG)
            .with_flen(19 + if fsp > 0 { fsp + 1 } else { 0 })
            .with_decimal(fsp)
            .with_charset_name(BINARY_CHARSET)
            .with_collation_name(BINARY_COLLATION),
        EvalType::Duration => FieldType::new(FieldTypeCode::Duration)
            .with_flags(BINARY_FLAG)
            .with_flen(10 + if fsp > 0 { fsp + 1 } else { 0 })
            .with_decimal(fsp)
            .with_charset_name(BINARY_CHARSET)
            .with_collation_name(BINARY_COLLATION),
        EvalType::Json => FieldType::new(FieldTypeCode::Json)
            .with_flags(BINARY_FLAG)
            .with_flen(12_582_912)
            .with_decimal(UNSPECIFIED_LENGTH.into())
            .with_charset_name("utf8mb4")
            .with_collation_name("utf8mb4_bin"),
        _ => return None,
    };
    to_pb_field_type(&field_type)
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

/// Go `scalarFuncToPBExpr` of a builtin cast: its `metadata()` is
/// `InUnionMetadata{InUnion: false}` (an in-union cast is not lowered here),
/// encoded as the expression's `val`.
fn cast_expr(signature: ScalarFuncSig, child: Expr, field_type: tidb_proto::tipb::FieldType) -> Expr {
    use prost::Message;
    Expr {
        tp: Some(ExprType::ScalarFunc as i32),
        val: Some(tidb_proto::tipb::InUnionMetadata { in_union: false }.encode_to_vec()),
        children: vec![child],
        // Upstream `Expr.sig` and `has_distinct` are gogoproto nullable=false.
        sig: Some(signature as i32),
        field_type: Some(field_type),
        has_distinct: Some(false),
        agg_func_mode: None,
        ..Default::default()
    }
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
        agg_func_mode: None,
        ..Default::default()
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
/// `elems` and `array` are copied as-is, matching Go's `ToPBFieldType`.
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
/// admits `BIT`, `ENUM`, JSON, temporal and vector leaves. The caller supplies
/// the already-resolved push-down metadata, so this helper only enforces the
/// type-family boundary; blacklist switches remain the planner's concern.
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
            | FieldTypeCode::NewDate
            | FieldTypeCode::Datetime
            | FieldTypeCode::Timestamp
            | FieldTypeCode::Duration
            | FieldTypeCode::Bit
            | FieldTypeCode::Enum
            | FieldTypeCode::Json
            | FieldTypeCode::VectorFloat32
            | FieldTypeCode::Null
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

    #[test]
    fn pb_contract_control_builder_aggregates_all_value_branches() {
        let text = PbScalar::StringLiteral {
            value: b"fallback".to_vec(),
            field_type: FieldType::new(FieldTypeCode::VarString).with_flen(8),
        };
        let column = PbScalar::Column {
            offset: 0,
            field_type: FieldType::new(FieldTypeCode::Varchar)
                .with_flen(30)
                .with_collation_name("utf8mb4_general_ci"),
        };
        for (name, sig) in [
            ("if", ScalarFuncSig::IfString),
            ("case", ScalarFuncSig::CaseWhenString),
            ("ifnull", ScalarFuncSig::IfNullString),
        ] {
            for branches in [
                vec![column.clone(), text.clone()],
                vec![text.clone(), column.clone()],
            ] {
                let mut args = branches;
                if name != "ifnull" {
                    args.insert(0, PbScalar::int_literal(1));
                }
                let call = build_call(name, args)
                    .expect("Go derives string control metadata from all value branches");
                let pb = lower(&call).unwrap();
                assert_eq!(pb.sig(), sig);
                let field = pb.field_type.as_ref().unwrap();
                assert_eq!(
                    field.collate(),
                    tidb_datatype::collation_to_proto("utf8mb4_general_ci")
                );
                assert_eq!(field.flen(), 30);
                crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
                let nested = build_call("ifnull", vec![text.clone(), call]).unwrap();
                assert_eq!(
                    lower(&nested).unwrap().field_type.unwrap().collate(),
                    field.collate()
                );
            }
        }
        for name in ["if", "case", "ifnull"] {
            let mut args = vec![
                PbScalar::int_literal(1),
                PbScalar::RealLiteral {
                    value: 1.5,
                    field_type: FieldType::new(FieldTypeCode::Double),
                },
            ];
            if name != "ifnull" {
                args.insert(0, PbScalar::int_literal(0));
            }
            let call = build_call(name, args).unwrap();
            assert_eq!(call.eval_type(), EvalType::Real);
            let pb = lower(&call).unwrap();
            let expr = crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
            assert_eq!(
                expr.eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
                    .unwrap(),
                Datum::Real(if name == "ifnull" { 1.0 } else { 1.5 })
            );
        }
    }

    fn column(code: FieldTypeCode) -> PbScalar {
        PbScalar::Column {
            offset: 0,
            field_type: FieldType::new(code),
        }
    }

    #[test]
    fn pb_contract_controls_preserve_cast_precision_and_string_domains() {
        let decimal = |value: &str, precision, scale| PbScalar::DecimalLiteral {
            value: Decimal::parse_mysql(value).0,
            field_type: FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(precision)
                .with_decimal(scale),
        };
        let call = build_call(
            "if",
            vec![
                PbScalar::int_literal(1),
                decimal("1.2", 2, 1),
                decimal("1.234", 4, 3),
            ],
        )
        .unwrap();
        let pb = lower(&call).unwrap();
        assert_eq!(pb.field_type.as_ref().unwrap().decimal(), 3);
        assert_eq!(pb.children[1].sig(), ScalarFuncSig::CastDecimalAsDecimal);
        assert_eq!(pb.children[1].field_type.as_ref().unwrap().decimal(), 3);
        let value = crate::distsql_builtin::pb_to_expr(&pb, &[])
            .unwrap()
            .eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
            .unwrap();
        assert_eq!(value.sql_string().unwrap(), "1.200");
        let text = PbScalar::StringLiteral {
            value: b"fallback".to_vec(),
            field_type: FieldType::new(FieldTypeCode::VarString),
        };
        let call = build_call(
            "if",
            vec![PbScalar::int_literal(1), PbScalar::int_literal(42), text],
        )
        .unwrap();
        let pb = lower(&call).unwrap();
        assert_eq!(pb.sig(), ScalarFuncSig::IfString);
        let value = crate::distsql_builtin::pb_to_expr(&pb, &[])
            .unwrap()
            .eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
            .unwrap();
        assert_eq!(value.sql_bytes().unwrap(), b"42");
    }

    #[test]
    fn control_conditions_use_go_typed_truth_wrappers() {
        for name in ["case", "if"] {
            for (code, signature) in [
                (FieldTypeCode::Double, ScalarFuncSig::RealIsTrueWithNull),
                (
                    FieldTypeCode::NewDecimal,
                    ScalarFuncSig::DecimalIsTrueWithNull,
                ),
                (FieldTypeCode::VarString, ScalarFuncSig::RealIsTrueWithNull),
            ] {
                let call = build_call(
                    name,
                    vec![
                        column(code),
                        PbScalar::int_literal(1),
                        PbScalar::int_literal(0),
                    ],
                )
                .unwrap();
                let pb = lower(&call).unwrap();
                assert_eq!(pb.children[0].sig(), signature);
                assert_eq!(pb.children[0].field_type.as_ref().unwrap().flen(), 1);
                if code == FieldTypeCode::VarString {
                    assert_eq!(
                        pb.children[0].children[0].sig(),
                        ScalarFuncSig::CastStringAsReal
                    );
                }
                if code == FieldTypeCode::Double {
                    let expression = crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
                    for (value, expected) in [
                        (Datum::Real(0.5), 1),
                        (Datum::Real(0.0), 0),
                        (Datum::Null, 0),
                    ] {
                        let row = tidb_chunk::mutrow::MutRow::mut_row_from_datums(&[value]);
                        assert_eq!(
                            expression.eval(&crate::NoColumns, row.to_row()).unwrap(),
                            Datum::Int(expected)
                        );
                    }
                }
            }
            let call = build_call(
                name,
                vec![
                    column(FieldTypeCode::LongLong),
                    PbScalar::int_literal(1),
                    PbScalar::int_literal(0),
                ],
            )
            .unwrap();
            assert_eq!(lower(&call).unwrap().children[0].tp(), ExprType::ColumnRef);
        }
        for name in ["and", "or"] {
            let call = build_call(
                name,
                vec![column(FieldTypeCode::Double), PbScalar::int_literal(1)],
            )
            .unwrap();
            let pb = lower(&call).unwrap();
            assert_eq!(pb.children[0].sig(), ScalarFuncSig::RealIsTrueWithNull);
            let expression = crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
            let row = tidb_chunk::mutrow::MutRow::mut_row_from_datums(&[Datum::Real(0.5)]);
            assert_eq!(
                expression.eval(&crate::NoColumns, row.to_row()).unwrap(),
                Datum::Int(1)
            );
        }
    }

    /// Go `getEvalTp4FloorAndCeil`: an `ETInt` argument answers `*IntToInt`,
    /// an `ETDecimal` one `*DecToInt` while `flen - decimal <= 18` (the
    /// planned result is BIGINT) and `*DecToDec` past it, anything else
    /// `*Real`. A real TiKV selects its evaluator by this code.
    #[test]
    fn result_typed_builtins_lower_to_the_signature_go_picks_for_their_result() {
        let decimal = |flen: i64, frac: i64| {
            let mut field_type = FieldType::new(FieldTypeCode::NewDecimal);
            field_type.set_flen(flen);
            field_type.set_decimal(frac);
            field_type
        };
        let long = FieldType::new(FieldTypeCode::LongLong);
        let double = FieldType::new(FieldTypeCode::Double);
        let mut unsigned = long.clone();
        unsigned.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        for (name, arg, ret, sig) in [
            ("ceil", long.clone(), long.clone(), ScalarFuncSig::CeilIntToInt),
            ("ceil", decimal(10, 2), long.clone(), ScalarFuncSig::CeilDecToInt),
            ("ceil", decimal(30, 2), decimal(29, 0), ScalarFuncSig::CeilDecToDec),
            ("ceiling", double.clone(), double.clone(), ScalarFuncSig::CeilReal),
            ("floor", long.clone(), long.clone(), ScalarFuncSig::FloorIntToInt),
            ("floor", decimal(10, 2), long.clone(), ScalarFuncSig::FloorDecToInt),
            ("floor", decimal(30, 2), decimal(29, 0), ScalarFuncSig::FloorDecToDec),
            ("floor", double.clone(), double.clone(), ScalarFuncSig::FloorReal),
            // Go `unaryMinusFunctionClass`: the signature follows the result
            // type it picks; an unsigned BIGINT negates to DECIMAL.
            ("unaryminus", long.clone(), long.clone(), ScalarFuncSig::UnaryMinusInt),
            ("unaryminus", unsigned.clone(), decimal(20, 0), ScalarFuncSig::UnaryMinusDecimal),
            ("unaryminus", double.clone(), double.clone(), ScalarFuncSig::UnaryMinusReal),
        ] {
            let mut column = crate::column::Column::new(1, arg);
            column.index = 0;
            let expression = Expression::ScalarFunction(crate::scalar_function::ScalarFunction::new(
                tidb_ast::CiString::new(name),
                ret,
                vec![Expression::Column(column)],
            ));
            let Some(PbScalar::Call { signature, .. }) = from_expression(&expression) else {
                panic!("{name} does not lower");
            };
            assert_eq!(signature.sig, sig, "{name}");
        }
    }

    #[test]
    fn ifnull_string_column_literal_uses_go_signature_and_column_collation() {
        let mut column_type = FieldType::new(FieldTypeCode::Varchar);
        column_type.set_collation_name("utf8mb4_general_ci");
        let column = PbScalar::Column {
            offset: 0,
            field_type: column_type,
        };
        let literal = PbScalar::StringLiteral {
            value: b"fallback".to_vec(),
            field_type: FieldType::new(FieldTypeCode::VarString),
        };
        let call = build_call("ifnull", vec![column.clone(), literal.clone()])
            .expect("Go supports IFNULL(string column, string literal)");
        assert!(matches!(
            &call,
            PbScalar::Call { signature, .. }
                if signature.sig == ScalarFuncSig::IfNullString
                    && signature.arg_type_at(0, 2, EvalType::String) == Some(EvalType::String)
        ));
        let encoded = to_pb(&call, &descriptors(&call)).unwrap();
        assert_eq!(encoded.sig, Some(ScalarFuncSig::IfNullString as i32));
        assert_eq!(
            encoded.field_type.as_ref().unwrap().collate,
            Some(tidb_datatype::collation_to_proto("utf8mb4_general_ci"))
        );

        // Column coercibility wins regardless of branch position.
        let reverse = build_call("ifnull", vec![literal, column]).unwrap();
        assert_eq!(
            lower(&reverse).unwrap().field_type.unwrap().collate,
            encoded.field_type.as_ref().unwrap().collate
        );
    }

    #[test]
    fn is_null_catalog_preserves_go_families_casts_and_boolean_width() {
        for (code, signature, cast) in [
            (FieldTypeCode::LongLong, ScalarFuncSig::IntIsNull, None),
            (FieldTypeCode::Bit, ScalarFuncSig::IntIsNull, None),
            (
                FieldTypeCode::NewDecimal,
                ScalarFuncSig::DecimalIsNull,
                None,
            ),
            (FieldTypeCode::Double, ScalarFuncSig::RealIsNull, None),
            (FieldTypeCode::Date, ScalarFuncSig::TimeIsNull, None),
            (FieldTypeCode::Datetime, ScalarFuncSig::TimeIsNull, None),
            (FieldTypeCode::Timestamp, ScalarFuncSig::TimeIsNull, None),
            (FieldTypeCode::Duration, ScalarFuncSig::DurationIsNull, None),
            (FieldTypeCode::Varchar, ScalarFuncSig::StringIsNull, None),
            (
                FieldTypeCode::Json,
                ScalarFuncSig::StringIsNull,
                Some(ScalarFuncSig::CastJsonAsString),
            ),
            (
                FieldTypeCode::VectorFloat32,
                ScalarFuncSig::VectorFloat32IsNull,
                None,
            ),
        ] {
            let described =
                build_call("isnull", vec![column(code)]).expect("Go IS NULL evaluation family");
            let encoded = to_pb(&described, &descriptors(&described)).unwrap();
            assert_eq!(encoded.sig, Some(signature as i32), "{code:?}");
            assert_eq!(
                encoded.children[0].sig,
                Some(cast.map_or(0, |cast| cast as i32)),
                "{code:?}"
            );
            assert_eq!(encoded.field_type.as_ref().unwrap().tp, Some(8));
            assert_eq!(encoded.field_type.as_ref().unwrap().flen, Some(1));
            assert_eq!(encoded.field_type.as_ref().unwrap().decimal, Some(0));
            assert_eq!(encoded.field_type.as_ref().unwrap().flag, Some(BINARY_FLAG));
        }
        assert!(build_call("isnull", Vec::new()).is_none());
        assert!(build_call(
            "isnull",
            vec![PbScalar::int_literal(1), PbScalar::int_literal(2)]
        )
        .is_none());
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
                    charset: tidb_charset::get_collation_by_name(field_type.collation_name())
                        .map_or_else(|_| "binary".to_owned(), |row| row.charset_name),
                    collation: field_type.collation_name().to_owned(),
                    elems: field_type
                        .elems_snapshot()
                        .into_iter()
                        .map(|elem| elem.to_string())
                        .collect(),
                    array: field_type.is_array(),
                },
            )),
            PbScalar::IntLiteral { .. }
            | PbScalar::NullLiteral { .. }
            | PbScalar::UIntLiteral { .. }
            | PbScalar::DecimalLiteral { .. }
            | PbScalar::RealLiteral { .. }
            | PbScalar::Float32Literal { .. }
            | PbScalar::StringLiteral { .. }
            | PbScalar::BytesLiteral { .. }
            | PbScalar::BitLiteral { .. }
            | PbScalar::EnumLiteral { .. }
            | PbScalar::TimeLiteral { .. }
            | PbScalar::DurationLiteral { .. }
            | PbScalar::JsonLiteral { .. }
            | PbScalar::VectorLiteral { .. } => {}
            PbScalar::Call { args, .. } => args.iter().for_each(|arg| collect(arg, out)),
            PbScalar::Cast { arg, .. } => collect(arg, out),
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
                PbScalar::int_literal(7),
                ScalarFuncSig::ModIntSignedSigned,
            ),
            (
                unsigned_column(FieldTypeCode::LongLong),
                PbScalar::int_literal(7),
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
    /// makes everything else Real. The `frac` overload resolves as Go's
    /// `RoundWithFrac*`, which `scalarExprSupportedByTiKV` then excludes.
    #[test]
    fn round_keeps_its_argument_domain_and_tikv_refuses_the_frac_overload() {
        use crate::infer_pushdown::{scalar_expr_supported_by_tikv, PushDownPolicy};

        for (argument, expected) in [
            (column(FieldTypeCode::LongLong), ScalarFuncSig::RoundInt),
            (column(FieldTypeCode::NewDecimal), ScalarFuncSig::RoundDec),
            (column(FieldTypeCode::Double), ScalarFuncSig::RoundReal),
            (column(FieldTypeCode::VarString), ScalarFuncSig::RoundReal),
        ] {
            let sig = resolve("round", &[argument]).unwrap().sig;
            assert_eq!(sig, expected);
            assert!(scalar_expr_supported_by_tikv(&PushDownPolicy::new("round", sig)));
        }
        for (argument, expected) in [
            (column(FieldTypeCode::LongLong), ScalarFuncSig::RoundWithFracInt),
            (column(FieldTypeCode::NewDecimal), ScalarFuncSig::RoundWithFracDec),
            (column(FieldTypeCode::Double), ScalarFuncSig::RoundWithFracReal),
        ] {
            let sig = resolve("round", &[argument, PbScalar::int_literal(2)])
                .unwrap()
                .sig;
            assert_eq!(sig, expected);
            assert!(
                !scalar_expr_supported_by_tikv(&PushDownPolicy::new("round", sig)),
                "ROUND with a frac argument is one of the signatures \
                 scalarExprSupportedByTiKV excludes"
            );
        }
    }

    /// A name the TiKV switch omits still has the signature Go's
    /// `getFunction` gives it: the catalog answers `PbCode`, and the store
    /// lists answer admission.
    #[test]
    fn a_name_tikv_does_not_evaluate_keeps_its_signature() {
        use crate::infer_pushdown::{
            scalar_expr_supported_by_flash, scalar_expr_supported_by_tikv, PushDownPolicy,
        };

        for (name, args, expected, flash) in [
            (
                "tan",
                vec![column(FieldTypeCode::Double)],
                ScalarFuncSig::Tan,
                false,
            ),
            (
                "truncate",
                vec![column(FieldTypeCode::LongLong), PbScalar::int_literal(1)],
                ScalarFuncSig::TruncateInt,
                true,
            ),
            (
                "inet_aton",
                vec![column(FieldTypeCode::VarString)],
                ScalarFuncSig::InetAton,
                true,
            ),
        ] {
            let sig = resolve(name, &args).unwrap().sig;
            assert_eq!(sig, expected, "{name}");
            let policy = PushDownPolicy::new(name, sig);
            assert!(!scalar_expr_supported_by_tikv(&policy), "{name}");
            assert_eq!(scalar_expr_supported_by_flash(&policy), flash, "{name}");
        }
        assert!(resolve("nonesuch", &[column(FieldTypeCode::LongLong)]).is_none());
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
            vec![column(FieldTypeCode::Double), PbScalar::int_literal(1)],
        )
        .unwrap();
        let pb = lower(&call).expect("the folded Float64 literal lowers");
        assert_eq!(pb.children[1].tp, Some(ExprType::Float64 as i32));
        assert_eq!(pb.children[1].sig, Some(ScalarFuncSig::Unspecified as i32));
        // The same constant in an Int slot needs no cast and lowers.
        let call = build_call(
            "mod",
            vec![column(FieldTypeCode::LongLong), PbScalar::int_literal(7)],
        )
        .unwrap();
        assert!(lower(&call).is_some());

        let call = build_call(
            "minus",
            vec![PbScalar::int_literal(1), column(FieldTypeCode::NewDecimal)],
        )
        .unwrap();
        let pb = lower(&call).expect("the folded MysqlDecimal literal lowers");
        assert_eq!(pb.sig, Some(ScalarFuncSig::MinusDecimal as i32));
        assert_eq!(pb.children[0].tp, Some(ExprType::MysqlDecimal as i32));
    }

    /// Temporal/JSON families use the same concrete signatures and cast
    /// metadata as Go's `newBaseBuiltinFuncWithTp`.  In particular,
    /// `UNIX_TIMESTAMP(string)` must carry a DATETIME cast target (including
    /// binary temporal metadata), while DATE_ADD/SUB and JSON modification
    /// calls must select their exact upstream signature numbers.
    #[test]
    fn temporal_and_json_families_lower_with_go_signatures_and_casts() {
        // Go's `ast.TimeUnitExpr` constant: DATE_ADD/SUB's third argument.
        let unit = |name: &str| PbScalar::StringLiteral {
            value: name.as_bytes().to_vec(),
            field_type: FieldType::new(FieldTypeCode::Varchar),
        };
        let cases = [
            (
                build_call(
                    "date_add",
                    vec![
                        column(FieldTypeCode::Datetime),
                        PbScalar::int_literal(1),
                        unit("YEAR"),
                    ],
                )
                .unwrap(),
                ScalarFuncSig::AddDateDatetimeInt,
            ),
            (
                build_call(
                    "date_add",
                    vec![
                        column(FieldTypeCode::VarString),
                        PbScalar::int_literal(1),
                        unit("SECOND"),
                    ],
                )
                .unwrap(),
                ScalarFuncSig::AddDateStringInt,
            ),
            (
                build_call(
                    "date_sub",
                    vec![
                        column(FieldTypeCode::Duration),
                        PbScalar::Column {
                            offset: 1,
                            field_type: FieldType::new(FieldTypeCode::VarString),
                        },
                        unit("HOUR"),
                    ],
                )
                .unwrap(),
                ScalarFuncSig::SubDateDurationString,
            ),
            (
                build_call(
                    "json_replace",
                    vec![
                        column(FieldTypeCode::Json),
                        column(FieldTypeCode::Json),
                        column(FieldTypeCode::Json),
                        column(FieldTypeCode::Json),
                        column(FieldTypeCode::Json),
                    ],
                )
                .unwrap(),
                ScalarFuncSig::JsonReplaceSig,
            ),
        ];
        for (call, expected) in cases {
            let pb = lower(&call).unwrap_or_else(|| {
                panic!("the Go-compatible temporal/JSON call {expected:?} lowers")
            });
            assert_eq!(pb.sig, Some(expected as i32));
        }

        let unix = build_call("unix_timestamp", vec![column(FieldTypeCode::VarString)]).unwrap();
        let pb = lower(&unix).expect("UNIX_TIMESTAMP(string) lowers through CAST AS DATETIME");
        assert_eq!(pb.sig, Some(ScalarFuncSig::UnixTimestampDec as i32));
        assert_eq!(
            pb.children[0].sig,
            Some(ScalarFuncSig::CastStringAsTime as i32)
        );
        let cast_type = pb.children[0].field_type.as_ref().unwrap();
        assert_eq!(
            cast_type.tp,
            Some(FieldTypeCode::Datetime.mysql_type().into())
        );
        assert_eq!(cast_type.decimal, Some(6));
        assert_eq!(
            cast_type.collate,
            Some(tidb_datatype::collation_to_proto("binary"))
        );

        // Go selects the integer result/signature for a DATETIME with FSP 0;
        // the decimal row above is only for the string/unknown-precision
        // case.  Keeping this assertion prevents a broad `ANY -> DECIMAL`
        // row from silently regressing the ordinary timestamp path.
        let unix = build_call(
            "unix_timestamp",
            vec![PbScalar::Column {
                offset: 0,
                field_type: FieldType::new(FieldTypeCode::Datetime).with_decimal(0),
            }],
        )
        .unwrap();
        let pb = lower(&unix).expect("UNIX_TIMESTAMP(datetime) lowers");
        assert_eq!(pb.sig, Some(ScalarFuncSig::UnixTimestampInt as i32));
    }

    /// Go `extractFunctionClass.getFunction` (`builtin_time.go`): a clock
    /// unit reads a time and a date unit a datetime, whatever the value; a
    /// unit that is both follows the value, and a value that is neither gets
    /// `ExtractDatetimeFromString`.
    #[test]
    fn extract_resolves_its_signature_from_the_unit_and_the_value() {
        for (unit, value, want) in [
            ("YEAR", FieldTypeCode::Datetime, Some(ScalarFuncSig::ExtractDatetime)),
            ("YEAR", FieldTypeCode::Duration, Some(ScalarFuncSig::ExtractDatetime)),
            ("YEAR_MONTH", FieldTypeCode::VarString, Some(ScalarFuncSig::ExtractDatetime)),
            ("HOUR", FieldTypeCode::Datetime, Some(ScalarFuncSig::ExtractDuration)),
            ("HOUR_MICROSECOND", FieldTypeCode::VarString, Some(ScalarFuncSig::ExtractDuration)),
            ("DAY_HOUR", FieldTypeCode::Datetime, Some(ScalarFuncSig::ExtractDatetime)),
            ("DAY_HOUR", FieldTypeCode::Date, Some(ScalarFuncSig::ExtractDatetime)),
            ("DAY_SECOND", FieldTypeCode::Timestamp, Some(ScalarFuncSig::ExtractDatetime)),
            ("DAY_MINUTE", FieldTypeCode::Duration, Some(ScalarFuncSig::ExtractDuration)),
            ("DAY_MICROSECOND", FieldTypeCode::VarString, Some(ScalarFuncSig::ExtractDatetimeFromString)),
            ("DAY_HOUR", FieldTypeCode::LongLong, Some(ScalarFuncSig::ExtractDatetimeFromString)),
        ] {
            let args = [
                PbScalar::StringLiteral {
                    value: unit.as_bytes().to_vec(),
                    field_type: FieldType::new(FieldTypeCode::Varchar),
                },
                column(value),
            ];
            let got = resolve("extract", &args).map(|signature| signature.sig);
            assert_eq!(got, want, "extract({unit} from {value:?})");
        }
        // The unit is the grammar's keyword, never a column.
        assert!(resolve("extract", &[column(FieldTypeCode::Varchar), column(FieldTypeCode::Datetime)]).is_none());
    }

    #[test]
    fn date_format_uses_the_format_argument_collation_and_width() {
        let format = PbScalar::Column {
            offset: 1,
            field_type: FieldType::new(FieldTypeCode::VarString)
                .with_collation_name("utf8mb4_general_ci")
                .with_flen(8),
        };
        let call =
            build_call("date_format", vec![column(FieldTypeCode::Datetime), format]).unwrap();
        let pb = lower(&call).expect("DATE_FORMAT is a TiKV-pushable signature");
        let result = pb.field_type.as_ref().expect("result type is encoded");
        assert_eq!(
            result.collate,
            Some(tidb_datatype::collation_to_proto("utf8mb4_general_ci"))
        );
        assert_eq!(result.charset.as_deref(), Some("utf8mb4"));
        assert_eq!(result.flen, Some(44), "(8 + 1) / 2 * 11, as Go computes");
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
                    tidb_charset::get_collation_by_name(name)
                        .unwrap()
                        .charset_name
                        .as_str()
                ),
                "{name}: the charset the collation belongs to"
            );
        }
    }

    #[test]
    fn concrete_go_literal_kinds_keep_their_tipb_leaf_types() {
        let null = PbScalar::NullLiteral {
            field_type: FieldType::new(FieldTypeCode::Null),
        };
        let null_pb = lower(&null).unwrap();
        assert_eq!(null_pb.tp, Some(ExprType::Null as i32));
        assert!(null_pb.val.is_none());

        let unsigned = PbScalar::UIntLiteral {
            value: u64::MAX,
            field_type: FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
        };
        assert_eq!(lower(&unsigned).unwrap().tp, Some(ExprType::Uint64 as i32));

        let bytes = PbScalar::BytesLiteral {
            value: vec![0xff, 0x00],
            field_type: FieldType::new(FieldTypeCode::Blob),
        };
        assert_eq!(lower(&bytes).unwrap().tp, Some(ExprType::Bytes as i32));

        let bit = PbScalar::BitLiteral {
            value: BinaryLiteral::from_uint(3, None),
            field_type: FieldType::new(FieldTypeCode::Bit),
        };
        assert_eq!(lower(&bit).unwrap().tp, Some(ExprType::MysqlBit as i32));

        let enum_value = PbScalar::EnumLiteral {
            value: 2,
            field_type: FieldType::new(FieldTypeCode::Enum).with_elems(["red", "green"]),
        };
        assert_eq!(
            lower(&enum_value).unwrap().tp,
            Some(ExprType::MysqlEnum as i32)
        );

        let vector = PbScalar::VectorLiteral {
            value: VectorFloat32::must_create([1.0, 2.0]),
            field_type: FieldType::new(FieldTypeCode::VectorFloat32),
        };
        assert_eq!(
            lower(&vector).unwrap().tp,
            Some(ExprType::TiDbVectorFloat32 as i32)
        );
    }

    /// The leaf family follows Go's `columnToPBExpr`: `SET` and `GEOMETRY`
    /// remain refused, while the concrete BIT/ENUM/JSON families carry their
    /// full metadata on the wire.
    #[test]
    fn a_leaf_family_matches_go_column_to_pb() {
        for code in [FieldTypeCode::Set, FieldTypeCode::Geometry] {
            assert!(!leaf_column_family(code), "{code:?} is refused by Go");
        }
        for code in [FieldTypeCode::Bit, FieldTypeCode::Enum, FieldTypeCode::Json] {
            assert!(leaf_column_family(code), "{code:?} is admitted by Go");
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
                PbScalar::int_literal(7),
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
                        PbScalar::int_literal(i64::try_from(position).expect("small"))
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
                    PbScalar::int_literal(10),
                    PbScalar::int_literal(2),
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
                    args.push(PbScalar::int_literal(2));
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
                args.push(PbScalar::int_literal(10));
                args.push(PbScalar::int_literal(2));
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
                elems: Vec::new(),
                array: false,
            })
        };
        assert!(
            to_pb(&call, &binary_descriptor).is_none(),
            "the UTF-8 signature must not travel against a binary column"
        );
    }

    /// Go resolves one signature per regexp builtin whatever the arity, with
    /// `argTp` the prefix of the family's declared operand types.
    #[test]
    fn the_regexp_family_resolves_one_signature_per_arity() {
        let string = || PbScalar::StringLiteral {
            value: b"x".to_vec(),
            field_type: FieldType::new(FieldTypeCode::VarString),
        };
        let int = || PbScalar::int_literal(1);
        let cases: &[(&str, Vec<PbScalar>, ScalarFuncSig)] = &[
            ("regexp_like", vec![string(), string()], ScalarFuncSig::RegexpLikeSig),
            (
                "regexp_like",
                vec![string(), string(), string()],
                ScalarFuncSig::RegexpLikeSig,
            ),
            ("regexp_instr", vec![string(), string()], ScalarFuncSig::RegexpInStrSig),
            (
                "regexp_instr",
                vec![string(), string(), int(), int(), int(), string()],
                ScalarFuncSig::RegexpInStrSig,
            ),
            (
                "regexp_substr",
                vec![string(), string(), int(), int(), string()],
                ScalarFuncSig::RegexpSubstrSig,
            ),
            (
                "regexp_replace",
                vec![string(), string(), string()],
                ScalarFuncSig::RegexpReplaceSig,
            ),
            (
                "regexp_replace",
                vec![string(), string(), string(), int(), int(), string()],
                ScalarFuncSig::RegexpReplaceSig,
            ),
        ];
        for (name, args, expected) in cases {
            let row = resolve(name, args)
                .unwrap_or_else(|| panic!("{name}/{} is a catalog row", args.len()));
            assert_eq!(row.sig, *expected, "{name}/{}", args.len());
        }
        // A miss in the positional slots is a refusal, never a guess.
        assert!(resolve("regexp_substr", &[string(), string(), string()]).is_none());
        assert!(resolve("regexp_replace", &[string(), string()]).is_none());
    }

    /// Go `compareFunctionClass.generateCmpSigs`: a comparison over operands
    /// already coerced to its comparison type resolves that type's signature.
    /// A strict `UPDATE t SET v = 9 WHERE int_col = 'abc'` plans the Selection
    /// `eq(cast(int_col, double), cast("abc", double))` in `cop[tikv]`, and Go
    /// sends it whole: TiKV's truncation error is then the statement's error
    /// (`Truncated incorrect INTEGER value: 'abc'`, captured against TiKV).
    #[test]
    fn comparisons_resolve_their_comparison_type_signature() {
        let cast = |arg: PbScalar, signature, code| PbScalar::Cast {
            signature,
            arg: Box::new(arg),
            field_type: FieldType::new(code),
        };
        let call = build_call(
            "eq",
            vec![
                cast(
                    column(FieldTypeCode::LongLong),
                    ScalarFuncSig::CastIntAsReal,
                    FieldTypeCode::Double,
                ),
                cast(
                    PbScalar::StringLiteral {
                        value: b"abc".to_vec(),
                        field_type: FieldType::new(FieldTypeCode::VarString),
                    },
                    ScalarFuncSig::CastStringAsReal,
                    FieldTypeCode::Double,
                ),
            ],
        )
        .unwrap();
        let pb = lower(&call).expect("a Real comparison lowers");
        assert_eq!(pb.sig, Some(ScalarFuncSig::EqReal as i32));
        assert_eq!(
            pb.children
                .iter()
                .map(|child| child.sig)
                .collect::<Vec<_>>(),
            [
                Some(ScalarFuncSig::CastIntAsReal as i32),
                Some(ScalarFuncSig::CastStringAsReal as i32),
            ]
        );

        for (name, left, right, signature) in [
            (
                "lt",
                FieldTypeCode::LongLong,
                FieldTypeCode::LongLong,
                ScalarFuncSig::LtInt,
            ),
            (
                "le",
                FieldTypeCode::Double,
                FieldTypeCode::Double,
                ScalarFuncSig::LeReal,
            ),
            (
                "gt",
                FieldTypeCode::NewDecimal,
                FieldTypeCode::NewDecimal,
                ScalarFuncSig::GtDecimal,
            ),
            (
                "ge",
                FieldTypeCode::VarString,
                FieldTypeCode::VarString,
                ScalarFuncSig::GeString,
            ),
            (
                "ne",
                FieldTypeCode::Datetime,
                FieldTypeCode::Date,
                ScalarFuncSig::NeTime,
            ),
            (
                "eq",
                FieldTypeCode::Timestamp,
                FieldTypeCode::Datetime,
                ScalarFuncSig::EqTime,
            ),
        ] {
            let call = build_call(name, vec![column(left), column(right)])
                .unwrap_or_else(|| panic!("{name}({left:?}, {right:?}) resolves"));
            let PbScalar::Call { signature: row, .. } = &call else {
                panic!("a comparison is a call");
            };
            assert_eq!(row.sig, signature, "{name}({left:?}, {right:?})");
        }
        // Go only compares operands of one comparison type; mixed operands
        // were already wrapped in casts by the planner.
        assert!(build_call(
            "eq",
            vec![
                column(FieldTypeCode::LongLong),
                column(FieldTypeCode::Double)
            ],
        )
        .is_none());
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
                    EvalType::Int
                        | EvalType::Real
                        | EvalType::Decimal
                        | EvalType::String
                        | EvalType::Datetime
                        | EvalType::Timestamp
                        | EvalType::Duration
                        | EvalType::Json
                        | EvalType::VectorFloat32
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
