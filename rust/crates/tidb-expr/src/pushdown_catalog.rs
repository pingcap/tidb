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
//! `newBaseBuiltinFuncWithTp` inserts. It refuses only leaves whose TiPB
//! `FieldType` or literal family this tier cannot build faithfully; concrete
//! string/ENUM/JSON metadata, including non-binary collations, is carried
//! through the descriptor instead of forcing a local scan; `SET` remains
//! refused for predicate leaves because Go's `columnToPBExpr` refuses it too.
//!
//! A refusal costs network only. The scan source applies every pushed
//! conjunct to every row it emits regardless
//! (`tidb_executor::predicate_pushdown`), so a conjunct the store does not filter
//! is still filtered locally. A conjunct lowered *wrongly* would drop a row
//! the query selects, and no local pass can put back a row that never crossed
//! the wire -- which is why every row of the table below cites the Go
//! `getFunction` it was read from.

use tidb_datatype::{
    collation_to_proto, BinaryJSON, BinaryLiteral, Datum, Decimal, EvalType, FieldType,
    FieldTypeCode, MySqlDuration, Time, VectorFloat32,
};
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
    /// The second argument's charset and collation, as `DATE_FORMAT`'s
    /// `deriveCollation` branch does. Go also stamps the result `flen` from
    /// the format mask, which is mirrored by the return-type builder below.
    SecondArgString,
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
            EvalType::Datetime => temporal_field_type(FieldTypeCode::Datetime, 0),
            EvalType::Timestamp => temporal_field_type(FieldTypeCode::Timestamp, 0),
            EvalType::Duration => temporal_field_type(FieldTypeCode::Duration, 0),
            EvalType::Json => temporal_field_type(FieldTypeCode::Json, 0),
            // Vector-returning builtins are not in the TiKV catalog yet.
            EvalType::String => unreachable!("string returns use string_return_field_type"),
            EvalType::VectorFloat32 => return None,
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
    field_type_to_pb(&field_type).expect("supported temporal/json result type")
}

// DATE_ADD/SUB are named `date_add_<unit>`/`date_sub_<unit>` by the rewriter.
// Their unit changes the runtime operation and result metadata, but not the
// TiPB signature family.  Keep every Go overload here so an unsupported row
// cannot accidentally resolve merely because it shares a function prefix.
const DATE_ARGS_SS: &[EvalType] = &[EvalType::String, EvalType::String];
const DATE_ARGS_SI: &[EvalType] = &[EvalType::String, EvalType::Int];
const DATE_ARGS_SR: &[EvalType] = &[EvalType::String, EvalType::Real];
const DATE_ARGS_SD: &[EvalType] = &[EvalType::String, EvalType::Decimal];
const DATE_ARGS_IS: &[EvalType] = &[EvalType::Int, EvalType::String];
const DATE_ARGS_II: &[EvalType] = &[EvalType::Int, EvalType::Int];
const DATE_ARGS_IR: &[EvalType] = &[EvalType::Int, EvalType::Real];
const DATE_ARGS_ID: &[EvalType] = &[EvalType::Int, EvalType::Decimal];
const DATE_ARGS_RS: &[EvalType] = &[EvalType::Real, EvalType::String];
const DATE_ARGS_RI: &[EvalType] = &[EvalType::Real, EvalType::Int];
const DATE_ARGS_RR: &[EvalType] = &[EvalType::Real, EvalType::Real];
const DATE_ARGS_RD: &[EvalType] = &[EvalType::Real, EvalType::Decimal];
const DATE_ARGS_DS: &[EvalType] = &[EvalType::Decimal, EvalType::String];
const DATE_ARGS_DI: &[EvalType] = &[EvalType::Decimal, EvalType::Int];
const DATE_ARGS_DR: &[EvalType] = &[EvalType::Decimal, EvalType::Real];
const DATE_ARGS_DD: &[EvalType] = &[EvalType::Decimal, EvalType::Decimal];
const DATE_ARGS_TS: &[EvalType] = &[EvalType::Datetime, EvalType::String];
const DATE_ARGS_TI: &[EvalType] = &[EvalType::Datetime, EvalType::Int];
const DATE_ARGS_TR: &[EvalType] = &[EvalType::Datetime, EvalType::Real];
const DATE_ARGS_TD: &[EvalType] = &[EvalType::Datetime, EvalType::Decimal];
const DATE_ARGS_HS: &[EvalType] = &[EvalType::Duration, EvalType::String];
const DATE_ARGS_HI: &[EvalType] = &[EvalType::Duration, EvalType::Int];
const DATE_ARGS_HR: &[EvalType] = &[EvalType::Duration, EvalType::Real];
const DATE_ARGS_HD: &[EvalType] = &[EvalType::Duration, EvalType::Decimal];

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
    let subtract = name.starts_with("date_sub_");
    if !subtract && !name.starts_with("date_add_") {
        return None;
    }
    let [first, second] = args else {
        return None;
    };
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
    let mut matches = candidates
        .iter()
        .filter(|candidate| candidate.arg_types == [first_type, second_type]);
    let first_match = matches.next()?;
    if first_match.ret == EvalType::Duration {
        let unit = name.rsplit('_').next()?.to_ascii_uppercase();
        let date_unit = matches!(
            unit.as_str(),
            "DAY" | "WEEK" | "MONTH" | "QUARTER" | "YEAR" | "YEAR_MONTH"
        );
        if date_unit && unit != "DAY_MICROSECOND" {
            return candidates.iter().find(|candidate| {
                candidate.arg_types == [first_type, second_type]
                    && candidate.ret == EvalType::Datetime
            });
        }
    }
    Some(first_match)
}

/// Resolves Go's `unixTimestampFunctionClass` return domain.
///
/// The function has one wire family for an argument, but Go chooses
/// `UnixTimestampInt` when the resolved DATETIME argument has FSP 0 and
/// `UnixTimestampDec` otherwise.  A single `ANY -> DECIMAL` catalog row would
/// be observably wrong: it changes the result field type and the TiPB
/// signature for ordinary `DATETIME`/`DATE` columns.  Unknown precision (for
/// example a non-constant string or a computed child) follows Go's decimal
/// fallback.
fn resolve_unix_timestamp(args: &[PbScalar]) -> Option<&'static BuiltinSignature> {
    let [argument] = args else {
        // `unix_timestamp()` is deliberately not pushed by Go:
        // `scalarExprSupportedByTiKV` rejects the Current signature because
        // it reads the TiDB session clock rather than a coprocessor row.
        return None;
    };
    let decimal = argument
        .static_field_type()
        .map(FieldType::decimal)
        .unwrap_or(i64::from(UNSPECIFIED_LENGTH));
    let signature = if decimal == 0 {
        ScalarFuncSig::UnixTimestampInt
    } else {
        ScalarFuncSig::UnixTimestampDec
    };
    CATALOG.iter().find(|candidate| {
        candidate.name == "unix_timestamp"
            && candidate.sig == signature
            && candidate.selector.len() == 1
    })
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
    // JSON modification functions use alternating JSON/path/JSON operands.
    signature(
        "json_replace",
        &[
            ArgPattern::ANY,
            ArgPattern::ANY,
            ArgPattern::ANY,
            ArgPattern::ANY,
            ArgPattern::ANY,
        ],
        &[
            EvalType::Json,
            EvalType::String,
            EvalType::Json,
            EvalType::String,
            EvalType::Json,
        ],
        EvalType::Json,
        ScalarFuncSig::JsonReplaceSig,
        false,
    ),
    signature(
        "json_array_append",
        &[
            ArgPattern::ANY,
            ArgPattern::ANY,
            ArgPattern::ANY,
            ArgPattern::ANY,
            ArgPattern::ANY,
        ],
        &[
            EvalType::Json,
            EvalType::String,
            EvalType::Json,
            EvalType::String,
            EvalType::Json,
        ],
        EvalType::Json,
        ScalarFuncSig::JsonArrayAppendSig,
        false,
    ),
    signature(
        "json_merge_patch",
        &[ArgPattern::ANY, ArgPattern::ANY, ArgPattern::ANY],
        &[EvalType::Json, EvalType::Json, EvalType::Json],
        EvalType::Json,
        ScalarFuncSig::JsonMergePatchSig,
        false,
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
            | (EvalType::Datetime, EvalType::Datetime)
            | (EvalType::Timestamp, EvalType::Timestamp)
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
    /// A SQL `NULL` constant. Go emits `ExprType_Null` with an empty value;
    /// the field type remains the planner's inferred type so a surrounding
    /// builtin can apply the same implicit cast selection as Go.
    NullLiteral {
        /// Planner-inferred type carried on the TiPB leaf.
        field_type: FieldType,
    },
    /// A signed integer constant, already folded.
    IntLiteral(i64),
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
    /// A folded binary JSON literal.
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
    },
}

impl PbScalar {
    /// Returns the declared field metadata carried by a leaf. Calls derive
    /// their result type from the catalog, while the legacy signed integer
    /// literal uses Go's fixed BIGINT representation and therefore has no
    /// separate source `FieldType` here.
    fn static_field_type(&self) -> Option<&FieldType> {
        match self {
            Self::Column { field_type, .. }
            | Self::NullLiteral { field_type }
            | Self::UIntLiteral { field_type, .. }
            | Self::DecimalLiteral { field_type, .. }
            | Self::RealLiteral { field_type, .. }
            | Self::StringLiteral { field_type, .. }
            | Self::BytesLiteral { field_type, .. }
            | Self::BitLiteral { field_type, .. }
            | Self::EnumLiteral { field_type, .. }
            | Self::TimeLiteral { field_type, .. }
            | Self::DurationLiteral { field_type, .. }
            | Self::JsonLiteral { field_type, .. }
            | Self::VectorLiteral { field_type, .. } => Some(field_type),
            Self::IntLiteral(_) | Self::Call { .. } => None,
        }
    }

    /// The node's evaluation type, which is what the selector matches on.
    #[must_use]
    pub fn eval_type(&self) -> EvalType {
        match self {
            Self::Column { field_type, .. } => field_type.eval_type(),
            Self::NullLiteral { field_type } => field_type.eval_type(),
            Self::IntLiteral(_) => EvalType::Int,
            Self::UIntLiteral { field_type, .. } => field_type.eval_type(),
            Self::DecimalLiteral { .. } => EvalType::Decimal,
            Self::RealLiteral { .. } => EvalType::Real,
            Self::StringLiteral { .. } => EvalType::String,
            Self::BytesLiteral { .. } => EvalType::String,
            Self::BitLiteral { field_type, .. } => field_type.eval_type(),
            Self::EnumLiteral { field_type, .. } => field_type.eval_type(),
            Self::TimeLiteral { field_type, .. } => field_type.eval_type(),
            Self::DurationLiteral { .. } => EvalType::Duration,
            Self::JsonLiteral { .. } => EvalType::Json,
            Self::VectorLiteral { .. } => EvalType::VectorFloat32,
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
            Self::NullLiteral { field_type } => field_type.is_unsigned(),
            Self::IntLiteral(_) => false,
            Self::UIntLiteral { field_type, .. }
            | Self::DecimalLiteral { field_type, .. }
            | Self::RealLiteral { field_type, .. } => field_type.is_unsigned(),
            Self::StringLiteral { field_type, .. }
            | Self::BytesLiteral { field_type, .. }
            | Self::BitLiteral { field_type, .. }
            | Self::EnumLiteral { field_type, .. }
            | Self::TimeLiteral { field_type, .. }
            | Self::DurationLiteral { field_type, .. }
            | Self::JsonLiteral { field_type, .. }
            | Self::VectorLiteral { field_type, .. } => field_type.is_unsigned(),
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
            Self::NullLiteral { .. } => false,
            Self::IntLiteral(_) => false,
            Self::UIntLiteral { .. } | Self::DecimalLiteral { .. } | Self::RealLiteral { .. } => {
                false
            }
            Self::StringLiteral { field_type, .. } | Self::BytesLiteral { field_type, .. } => {
                field_type.is_binary_string()
            }
            Self::BitLiteral { .. }
            | Self::EnumLiteral { .. }
            | Self::TimeLiteral { .. }
            | Self::DurationLiteral { .. }
            | Self::JsonLiteral { .. }
            | Self::VectorLiteral { .. } => false,
            Self::Call { signature, args } => match signature.ret_collation {
                RetCollation::Numeric | RetCollation::ConnectionString => false,
                RetCollation::FirstArgString => {
                    args.first().is_some_and(PbScalar::is_binary_string)
                }
                RetCollation::SecondArgString => {
                    args.get(1).is_some_and(PbScalar::is_binary_string)
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
    if let Some(signature) = resolve_date_arithmetic(name, args) {
        return Some(signature);
    }
    if name == "unix_timestamp" {
        return resolve_unix_timestamp(args);
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
            Datum::Null => Some(PbScalar::NullLiteral {
                field_type: constant.ret_type.clone()?,
            }),
            Datum::Int(value) => Some(PbScalar::IntLiteral(*value)),
            Datum::UInt(value) => Some(PbScalar::UIntLiteral {
                value: *value,
                field_type: constant.ret_type.clone()?,
            }),
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
            Datum::String(value) => Some(PbScalar::StringLiteral {
                value: value.bytes().to_vec(),
                field_type: constant.ret_type.clone()?,
            }),
            Datum::Bytes(value) => Some(PbScalar::BytesLiteral {
                value: value.clone(),
                field_type: constant.ret_type.clone()?,
            }),
            Datum::BinaryLiteral(value) => Some(PbScalar::StringLiteral {
                value: value.as_bytes().to_vec(),
                field_type: constant.ret_type.clone()?,
            }),
            Datum::Bit(value) => Some(PbScalar::BitLiteral {
                value: value.clone(),
                field_type: constant.ret_type.clone()?,
            }),
            Datum::Enum(value, _) => Some(PbScalar::EnumLiteral {
                value: value.value(),
                field_type: constant.ret_type.clone()?,
            }),
            Datum::Time(value) => Some(PbScalar::TimeLiteral {
                value: *value,
                field_type: constant.ret_type.clone()?,
            }),
            Datum::Duration(value) => Some(PbScalar::DurationLiteral {
                value: *value,
                field_type: constant.ret_type.clone()?,
            }),
            Datum::Json(value) => Some(PbScalar::JsonLiteral {
                value: value.clone(),
                field_type: constant.ret_type.clone()?,
            }),
            Datum::VectorFloat32(value) => Some(PbScalar::VectorLiteral {
                value: value.clone(),
                field_type: constant.ret_type.clone()?,
            }),
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
    let mut result = pb_field_type(
        i32::from(field_type.code().mysql_type()),
        field_type.flags(),
        i32::try_from(field_type.flen()).ok()?,
        i32::try_from(field_type.decimal()).ok()?,
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
                field_type_to_pb(&pb_type)?,
            ))
        }
        PbScalar::NullLiteral { field_type } => Some(Expr {
            tp: Some(ExprType::Null as i32),
            val: None,
            children: Vec::new(),
            sig: Some(ScalarFuncSig::Unspecified as i32),
            field_type: Some(field_type_to_pb(field_type)?),
            has_distinct: Some(false),
        }),
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
        PbScalar::UIntLiteral { value, field_type } => {
            let mut encoded = Vec::new();
            tidb_codec::encode_uint(&mut encoded, *value);
            Some(leaf(
                ExprType::Uint64,
                encoded,
                field_type_to_pb(field_type)?,
            ))
        }
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
        PbScalar::StringLiteral { value, field_type } => Some(leaf(
            ExprType::String,
            value.clone(),
            field_type_to_pb(field_type)?,
        )),
        PbScalar::BytesLiteral { value, field_type } => Some(leaf(
            ExprType::Bytes,
            value.clone(),
            field_type_to_pb(field_type)?,
        )),
        PbScalar::BitLiteral { value, field_type } => Some(leaf(
            ExprType::MysqlBit,
            value.as_bytes().to_vec(),
            field_type_to_pb(field_type)?,
        )),
        PbScalar::EnumLiteral { value, field_type } => {
            let mut encoded = Vec::new();
            tidb_codec::encode_uint(&mut encoded, *value);
            Some(leaf(
                ExprType::MysqlEnum,
                encoded,
                field_type_to_pb(field_type)?,
            ))
        }
        PbScalar::TimeLiteral { value, field_type } => {
            let packed = value.to_packed_uint().ok()?;
            let mut encoded = Vec::new();
            tidb_codec::encode_uint(&mut encoded, packed);
            Some(leaf(
                ExprType::MysqlTime,
                encoded,
                field_type_to_pb(field_type)?,
            ))
        }
        PbScalar::DurationLiteral { value, field_type } => Some(leaf(
            ExprType::MysqlDuration,
            encode_signed(value.nanoseconds()),
            field_type_to_pb(field_type)?,
        )),
        PbScalar::JsonLiteral { value, field_type } => Some(leaf(
            ExprType::MysqlJson,
            value.encoded(),
            field_type_to_pb(field_type)?,
        )),
        PbScalar::VectorLiteral { value, field_type } => Some(leaf(
            ExprType::TiDbVectorFloat32,
            value.serialize(),
            field_type_to_pb(field_type)?,
        )),
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
            // Unlike numeric constants, Go keeps an integer child and emits
            // the corresponding temporal/JSON cast node.  Let the generic
            // path below build that node with the target metadata.
            EvalType::Datetime | EvalType::Timestamp | EvalType::Duration | EvalType::Json => {
                // Fall through after the constant-specialization block.
                let cast = cast_signature(argument.eval_type(), required)?;
                let Some(cast) = cast else {
                    return to_pb(argument, columns);
                };
                let field_type = cast_target_field_type(argument, required)?;
                return Some(Expr {
                    tp: Some(ExprType::ScalarFunc as i32),
                    val: None,
                    children: vec![to_pb(argument, columns)?],
                    sig: Some(cast as i32),
                    field_type: Some(field_type),
                    has_distinct: Some(false),
                });
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
        _ => cast_target_field_type(argument, required)?,
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
    field_type_to_pb(&field_type)
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
                    elems: field_type
                        .elems_snapshot()
                        .into_iter()
                        .map(|elem| elem.to_string())
                        .collect(),
                    array: field_type.is_array(),
                },
            )),
            PbScalar::IntLiteral(_)
            | PbScalar::NullLiteral { .. }
            | PbScalar::UIntLiteral { .. }
            | PbScalar::DecimalLiteral { .. }
            | PbScalar::RealLiteral { .. }
            | PbScalar::StringLiteral { .. }
            | PbScalar::BytesLiteral { .. }
            | PbScalar::BitLiteral { .. }
            | PbScalar::EnumLiteral { .. }
            | PbScalar::TimeLiteral { .. }
            | PbScalar::DurationLiteral { .. }
            | PbScalar::JsonLiteral { .. }
            | PbScalar::VectorLiteral { .. } => {}
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

    /// Temporal/JSON families use the same concrete signatures and cast
    /// metadata as Go's `newBaseBuiltinFuncWithTp`.  In particular,
    /// `UNIX_TIMESTAMP(string)` must carry a DATETIME cast target (including
    /// binary temporal metadata), while DATE_ADD/SUB and JSON modification
    /// calls must select their exact upstream signature numbers.
    #[test]
    fn temporal_and_json_families_lower_with_go_signatures_and_casts() {
        let cases = [
            (
                build_call(
                    "date_add_year",
                    vec![column(FieldTypeCode::Datetime), PbScalar::IntLiteral(1)],
                )
                .unwrap(),
                ScalarFuncSig::AddDateDatetimeInt,
            ),
            (
                build_call(
                    "date_add_second",
                    vec![column(FieldTypeCode::VarString), PbScalar::IntLiteral(1)],
                )
                .unwrap(),
                ScalarFuncSig::AddDateStringInt,
            ),
            (
                build_call(
                    "date_sub_hour",
                    vec![
                        column(FieldTypeCode::Duration),
                        PbScalar::Column {
                            offset: 1,
                            field_type: FieldType::new(FieldTypeCode::VarString),
                        },
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
                    tidb_datatype::get_collation_by_name(name)
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
                elems: Vec::new(),
                array: false,
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
                    EvalType::Int
                        | EvalType::Real
                        | EvalType::Decimal
                        | EvalType::String
                        | EvalType::Datetime
                        | EvalType::Timestamp
                        | EvalType::Duration
                        | EvalType::Json
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
