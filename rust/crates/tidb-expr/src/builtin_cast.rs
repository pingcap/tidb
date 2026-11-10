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

//! `pkg/expression/builtin_cast.go`: the cast function classes and
//! `BuildCastFunction`.

use crate::builtin::{
    is_binary_literal, new_base_builtin_func, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::expr_collation::{Coercibility, ExprCollation, Repertoire};
use crate::expression::{propagate_type, Expression};
use crate::rewriter::adjust_ret_ft_for_cast_string;
use crate::scalar_function::ScalarFunction;
use crate::wrap_cast::type_of;
use crate::EvalError;
use tidb_datatype::{EvalType, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `charset.CharsetBin`.
const CHARSET_BIN: &str = "binary";
/// Go `charset.CharsetASCII`.
const CHARSET_ASCII: &str = "ascii";

/// The signature a cast class sets for an argument of `arg_tp`, from the
/// codes of its int, real, decimal, time, duration, JSON and string
/// signatures. A vector argument's signature sets none.
fn sig_of_arg(arg_tp: EvalType, sigs: [ScalarFuncSig; 7]) -> ScalarFuncSig {
    let [int, real, decimal, time, duration, json, string] = sigs;
    match arg_tp {
        EvalType::Int => int,
        EvalType::Real => real,
        EvalType::Decimal => decimal,
        EvalType::Datetime | EvalType::Timestamp => time,
        EvalType::Duration => duration,
        EvalType::Json => json,
        EvalType::String => string,
        EvalType::VectorFloat32 => ScalarFuncSig::Unspecified,
    }
}

/// Go `castAsIntFunctionClass`.
pub(crate) struct CastAsIntFunctionClass {
    pub(crate) tp: FieldType,
}

impl FunctionClass for CastAsIntFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func(ctx, "cast", args, self.tp.clone())?;
        let arg_tp = type_of(&bf.args[0]);
        bf.pb_code = if arg_tp.is_hybrid() || is_binary_literal(&bf.args[0]) {
            ScalarFuncSig::CastIntAsInt
        } else {
            sig_of_arg(
                arg_tp.eval_type(),
                [
                    ScalarFuncSig::CastIntAsInt,
                    ScalarFuncSig::CastRealAsInt,
                    ScalarFuncSig::CastDecimalAsInt,
                    ScalarFuncSig::CastTimeAsInt,
                    ScalarFuncSig::CastDurationAsInt,
                    ScalarFuncSig::CastJsonAsInt,
                    ScalarFuncSig::CastStringAsInt,
                ],
            )
        };
        Ok(bf)
    }
}

/// Go `castAsRealFunctionClass`.
pub(crate) struct CastAsRealFunctionClass {
    pub(crate) tp: FieldType,
}

impl FunctionClass for CastAsRealFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func(ctx, "cast", args, self.tp.clone())?;
        if is_binary_literal(&bf.args[0]) {
            bf.pb_code = ScalarFuncSig::CastRealAsReal;
            return Ok(bf);
        }
        let arg_tp = type_of(&bf.args[0]);
        let arg_tp = if arg_tp.is_hybrid() {
            EvalType::Int
        } else {
            arg_tp.eval_type()
        };
        if arg_tp == EvalType::Decimal {
            propagate_type(&mut bf.args[0], EvalType::Real);
        }
        bf.pb_code = sig_of_arg(
            arg_tp,
            [
                ScalarFuncSig::CastIntAsReal,
                ScalarFuncSig::CastRealAsReal,
                ScalarFuncSig::CastDecimalAsReal,
                ScalarFuncSig::CastTimeAsReal,
                ScalarFuncSig::CastDurationAsReal,
                ScalarFuncSig::CastJsonAsReal,
                ScalarFuncSig::CastStringAsReal,
            ],
        );
        Ok(bf)
    }
}

/// Go `castAsDecimalFunctionClass`.
pub(crate) struct CastAsDecimalFunctionClass {
    pub(crate) tp: FieldType,
}

impl FunctionClass for CastAsDecimalFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func(ctx, "cast", args, self.tp.clone())?;
        if is_binary_literal(&bf.args[0]) {
            bf.pb_code = ScalarFuncSig::CastDecimalAsDecimal;
            return Ok(bf);
        }
        let arg_tp = type_of(&bf.args[0]);
        let arg_tp = if arg_tp.is_hybrid() {
            EvalType::Int
        } else {
            arg_tp.eval_type()
        };
        bf.pb_code = sig_of_arg(
            arg_tp,
            [
                ScalarFuncSig::CastIntAsDecimal,
                ScalarFuncSig::CastRealAsDecimal,
                ScalarFuncSig::CastDecimalAsDecimal,
                ScalarFuncSig::CastTimeAsDecimal,
                ScalarFuncSig::CastDurationAsDecimal,
                ScalarFuncSig::CastJsonAsDecimal,
                ScalarFuncSig::CastStringAsDecimal,
            ],
        );
        Ok(bf)
    }
}

/// Go `newBaseBuiltinCastFunc4String`: a cast to a charset the statement
/// names is EXPLICIT in it; any other derives its collation as a builtin
/// does.
fn new_base_builtin_cast_func_4_string(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
    tp: FieldType,
    is_explicit_charset: bool,
) -> Result<BaseBuiltinFunc, EvalError> {
    if !is_explicit_charset {
        return new_base_builtin_func(ctx, func_name, args, tp);
    }
    let mut collation = crate::expr_collation::CollationInfo::default();
    collation.set_charset_and_collation(tp.charset_name(), tp.collation_name());
    collation.set_coercibility(Coercibility::EXPLICIT);
    collation.set_explicit_charset(true);
    collation.set_repertoire(if tp.charset_name() == CHARSET_ASCII {
        Repertoire::ASCII
    } else {
        Repertoire::UNICODE
    });
    let bf = BaseBuiltinFunc {
        args,
        tp,
        pb_code: ScalarFuncSig::Unspecified,
        collation,
        sig: crate::builtin::SigFields::None,
    };
    Ok(bf)
}

/// Go `castAsStringFunctionClass`.
pub(crate) struct CastAsStringFunctionClass {
    pub(crate) tp: FieldType,
    pub(crate) is_explicit_charset: bool,
}

impl FunctionClass for CastAsStringFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_cast_func_4_string(
            ctx,
            "cast",
            args,
            self.tp.clone(),
            self.is_explicit_charset,
        )?;
        let arg_ft = type_of(&bf.args[0]);
        if arg_ft.is_hybrid() {
            // A BIT cast to a non-binary charset is cast to a binary string
            // first, so the bytes meet `from_binary` below.
            if arg_ft.code() != FieldTypeCode::Bit || self.tp.charset_name() == CHARSET_BIN {
                bf.pb_code = ScalarFuncSig::CastStringAsString;
                return Ok(bf);
            }
            let mut binary = FieldType::new(FieldTypeCode::String);
            binary.set_charset_name(CHARSET_BIN);
            binary.set_collation_name(CHARSET_BIN);
            binary.add_flags(FieldTypeFlags::BINARY);
            let bit = bf.args.remove(0);
            bf.args.push(build_cast_function(ctx, bit, binary)?);
        }
        let arg_ft = type_of(&bf.args[0]);
        adjust_ret_ft_for_cast_string(&mut bf.tp, &arg_ft);
        bf.pb_code = match arg_ft.eval_type() {
            EvalType::VectorFloat32 => ScalarFuncSig::CastVectorFloat32AsString,
            EvalType::String => {
                let ec = ExprCollation {
                    charset: self.tp.charset_name().to_owned(),
                    collation: self.tp.collation_name().to_owned(),
                    ..ExprCollation::default()
                };
                let arg = bf.args.remove(0);
                bf.args.push(crate::convert_charset::handle_binary_literal(
                    ctx, arg, &ec, "cast", true,
                ));
                ScalarFuncSig::CastStringAsString
            }
            arg_tp => sig_of_arg(
                arg_tp,
                [
                    ScalarFuncSig::CastIntAsString,
                    ScalarFuncSig::CastRealAsString,
                    ScalarFuncSig::CastDecimalAsString,
                    ScalarFuncSig::CastTimeAsString,
                    ScalarFuncSig::CastDurationAsString,
                    ScalarFuncSig::CastJsonAsString,
                    ScalarFuncSig::CastStringAsString,
                ],
            ),
        };
        Ok(bf)
    }
}

/// Go `castAsTimeFunctionClass`.
pub(crate) struct CastAsTimeFunctionClass {
    pub(crate) tp: FieldType,
}

impl FunctionClass for CastAsTimeFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func(ctx, "cast", args, self.tp.clone())?;
        bf.pb_code = sig_of_arg(
            type_of(&bf.args[0]).eval_type(),
            [
                ScalarFuncSig::CastIntAsTime,
                ScalarFuncSig::CastRealAsTime,
                ScalarFuncSig::CastDecimalAsTime,
                ScalarFuncSig::CastTimeAsTime,
                ScalarFuncSig::CastDurationAsTime,
                ScalarFuncSig::CastJsonAsTime,
                ScalarFuncSig::CastStringAsTime,
            ],
        );
        Ok(bf)
    }
}

/// Go `castAsDurationFunctionClass`.
pub(crate) struct CastAsDurationFunctionClass {
    pub(crate) tp: FieldType,
}

impl FunctionClass for CastAsDurationFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func(ctx, "cast", args, self.tp.clone())?;
        bf.pb_code = sig_of_arg(
            type_of(&bf.args[0]).eval_type(),
            [
                ScalarFuncSig::CastIntAsDuration,
                ScalarFuncSig::CastRealAsDuration,
                ScalarFuncSig::CastDecimalAsDuration,
                ScalarFuncSig::CastTimeAsDuration,
                ScalarFuncSig::CastDurationAsDuration,
                ScalarFuncSig::CastJsonAsDuration,
                ScalarFuncSig::CastStringAsDuration,
            ],
        );
        Ok(bf)
    }
}

/// Go `castAsJSONFunctionClass`.
pub(crate) struct CastAsJsonFunctionClass {
    pub(crate) tp: FieldType,
}

impl FunctionClass for CastAsJsonFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func(ctx, "cast", args, self.tp.clone())?;
        let arg_tp = type_of(&bf.args[0]).eval_type();
        bf.pb_code = sig_of_arg(
            arg_tp,
            [
                ScalarFuncSig::CastIntAsJson,
                ScalarFuncSig::CastRealAsJson,
                ScalarFuncSig::CastDecimalAsJson,
                ScalarFuncSig::CastTimeAsJson,
                ScalarFuncSig::CastDurationAsJson,
                ScalarFuncSig::CastJsonAsJson,
                ScalarFuncSig::CastStringAsJson,
            ],
        );
        // The text is parsed as a JSON document unless a caller clears the
        // flag later (`DisableParseJSONFlag4Expr`).
        if arg_tp == EvalType::String {
            bf.tp.add_flags(FieldTypeFlags::PARSE_TO_JSON);
        }
        Ok(bf)
    }
}

/// Go `castAsArrayFunctionClass`.
pub(crate) struct CastAsArrayFunctionClass {
    pub(crate) tp: FieldType,
}

impl FunctionClass for CastAsArrayFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        // `verifyArgs`.
        if type_of(&args[0]).eval_type() != EvalType::Json {
            return Err(EvalError::Json(crate::JsonError::InvalidTypeForJson {
                argument: 1,
                function: "cast_as_array",
            }));
        }
        let array_type = self.tp.array_type();
        if matches!(
            array_type.code(),
            FieldTypeCode::Year | FieldTypeCode::Json | FieldTypeCode::Float | FieldTypeCode::NewDecimal
        ) {
            return Err(EvalError::NotImplemented(
                format!("CAST-ing data to array of {}", array_type.source_string()).into(),
            ));
        }
        if array_type.eval_type() == EvalType::String
            && !matches!(array_type.charset_name(), "utf8mb4" | CHARSET_BIN)
        {
            return Err(EvalError::NotImplemented(
                "specifying charset for multi-valued index".into(),
            ));
        }
        if array_type.eval_type() == EvalType::String
            && array_type.flen() == tidb_datatype::UNSPECIFIED_LENGTH
        {
            return Err(EvalError::NotImplemented(
                "CAST-ing data to array of char/binary BLOBs with unspecified length".into(),
            ));
        }
        new_base_builtin_func(ctx, "cast", args, self.tp.clone())
    }
}

/// Go `castAsVectorFloat32FunctionClass`.
pub(crate) struct CastAsVectorFloat32FunctionClass {
    pub(crate) tp: FieldType,
}

impl FunctionClass for CastAsVectorFloat32FunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func(ctx, "cast", args, self.tp.clone())?;
        if type_of(&bf.args[0]).eval_type() == EvalType::VectorFloat32 {
            bf.pb_code = ScalarFuncSig::CastVectorFloat32AsVectorFloat32;
        }
        Ok(bf)
    }
}

/// The conversion the evaluator's row and batch paths run for a cast to
/// `target`, by the name its conversion table is keyed with.
pub(crate) fn cast_target_name(target: &FieldType) -> Result<&'static str, EvalError> {
    let unsigned = target.flags() & FieldTypeFlags::UNSIGNED != 0;
    if target.is_array() {
        return Ok("array");
    }
    Ok(match target.code() {
        FieldTypeCode::Year => "year",
        FieldTypeCode::Date | FieldTypeCode::NewDate => "date",
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => "datetime",
        FieldTypeCode::Duration => "time",
        FieldTypeCode::NewDecimal => "decimal",
        // A FLOAT target is its own signature: Go's float32 cast overflows
        // differently from the double one (`CAST('1e300' AS FLOAT)` errors
        // 1690 where the DOUBLE cast answers 1e+300).
        FieldTypeCode::Float => "float",
        FieldTypeCode::Double => "double",
        FieldTypeCode::Json => "json",
        FieldTypeCode::VectorFloat32 => "vector",
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Bit => {
            if unsigned {
                "unsigned"
            } else {
                "signed"
            }
        }
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::Enum
        | FieldTypeCode::Set
        // Go's `TypeNull` is `ETString`, so `castAsStringFunctionClass`
        // builds its cast (`castCollationForIn` over an `IN (NULL)` member).
        | FieldTypeCode::Null => {
            if target.charset_name() == CHARSET_BIN {
                "binary"
            } else {
                "char"
            }
        }
        _ => return Err(EvalError::Unsupported("this cast target is not ported")),
    })
}

/// Go `BuildCastFunction`.
pub fn build_cast_function(
    ctx: &dyn BuildContext,
    expr: Expression,
    tp: FieldType,
) -> Result<Expression, EvalError> {
    build_cast_function_with_check(ctx, expr, tp, false, false)
}

/// Go `BuildCastFunctionWithCheck`: the cast of `expr` to `tp`, folded
/// unless it is a cast to JSON, whose flags may still be reset.
pub fn build_cast_function_with_check(
    ctx: &dyn BuildContext,
    expr: Expression,
    mut tp: FieldType,
    in_union: bool,
    is_explicit_charset: bool,
) -> Result<Expression, EvalError> {
    if !type_of(&expr).has_flag(FieldTypeFlags::NOT_NULL) {
        tp.del_flag(FieldTypeFlags::NOT_NULL);
    }
    let expr =
        try_push_cast_into_control_function_for_hybrid_type(ctx, expr, &tp);
    let f = match tp.eval_type() {
        EvalType::Int => CastAsIntFunctionClass { tp }.get_function(ctx, vec![expr])?,
        EvalType::Decimal => CastAsDecimalFunctionClass { tp }.get_function(ctx, vec![expr])?,
        EvalType::Real => CastAsRealFunctionClass { tp }.get_function(ctx, vec![expr])?,
        EvalType::Datetime | EvalType::Timestamp => {
            CastAsTimeFunctionClass { tp }.get_function(ctx, vec![expr])?
        }
        EvalType::Duration => CastAsDurationFunctionClass { tp }.get_function(ctx, vec![expr])?,
        EvalType::Json if tp.is_array() => {
            CastAsArrayFunctionClass { tp }.get_function(ctx, vec![expr])?
        }
        EvalType::Json => CastAsJsonFunctionClass { tp }.get_function(ctx, vec![expr])?,
        EvalType::VectorFloat32 => {
            CastAsVectorFloat32FunctionClass { tp }.get_function(ctx, vec![expr])?
        }
        EvalType::String => {
            let source = type_of(&expr);
            if source.code() == FieldTypeCode::Bit {
                tp.set_flen((source.flen() + 7) / 8);
            }
            CastAsStringFunctionClass {
                tp,
                is_explicit_charset,
            }
            .get_function(ctx, vec![expr])?
        }
    };
    cast_target_name(&f.tp)?;
    let is_json = f.tp.eval_type() == EvalType::Json;
    let mut res = Expression::ScalarFunction(ScalarFunction::from_cast_func(f, in_union));
    // We do not fold CAST if the eval type of this scalar function is
    // ETJson since we may reset the flag of the field type of CastAsJson
    // later which would affect the evaluation of it.
    if !is_json {
        ctx.fold_constant(&mut res);
    }
    Ok(res)
}

/// Go `TryPushCastIntoControlFunctionForHybridType`: an IF, CASE or ELT with
/// an ENUM or SET branch is rebuilt over its branches cast to the INT or
/// REAL `tp`, so the branch's number is read where the unpushed function
/// would return its name.
#[must_use]
pub fn try_push_cast_into_control_function_for_hybrid_type(
    ctx: &dyn BuildContext,
    expr: Expression,
    tp: &FieldType,
) -> Expression {
    let Expression::ScalarFunction(sf) = &expr else {
        return expr;
    };
    let wrap_cast_func: &dyn Fn(Expression) -> Result<Expression, EvalError> =
        match tp.eval_type() {
            EvalType::Int => &|expr| crate::wrap_cast::wrap_with_cast_as_int(ctx, expr, Some(tp)),
            EvalType::Real => &|expr| crate::wrap_cast::wrap_with_cast_as_real(ctx, expr),
            _ => return expr,
        };
    // todo: compatible with mysql control function using bit type. issue 24725
    let is_hybrid = |expr: &Expression| {
        let ft = type_of(expr);
        ft.is_hybrid() && ft.code() != FieldTypeCode::Bit
    };
    let name = sf.func_name.lowercase();
    let len = sf.args.len();
    let branches: Vec<usize> = match name {
        "if" => vec![1, 2],
        "case" => {
            let mut branches: Vec<usize> = (1..len).step_by(2).collect();
            if len % 2 == 1 {
                branches.push(len - 1);
            }
            branches
        }
        "elt" => (1..len).collect(),
        _ => return expr,
    };
    if !branches.iter().any(|&i| is_hybrid(&sf.args[i])) {
        return expr;
    }
    let mut args = sf.args.clone();
    for i in branches {
        let Ok(wrapped) = wrap_cast_func(args[i].clone()) else {
            return expr;
        };
        args[i] = wrapped;
    }
    let Some(class) = crate::builtin::funcs(name) else {
        return expr;
    };
    let Ok(f) = class.get_function(ctx, args) else {
        return expr;
    };
    let ret_type = f.get_ret_tp();
    Expression::ScalarFunction(crate::new_function::scalar_function_of(name, ret_type, f))
}
