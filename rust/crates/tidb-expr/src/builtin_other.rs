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

//! `pkg/expression/builtin_other.go`: `IN`, `ROW`, `BIT_COUNT`, `SETVAR` and
//! `GETPARAM`.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::builtin_miscellaneous::function_class;
use crate::expr_util::predicates::disable_parse_json_flag_4_expr;
use crate::expression::Expression;
use crate::wrap_cast::type_of;
use crate::EvalError;
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `inFunctionClass`.
pub(crate) struct InFunctionClass;

impl InFunctionClass {
    /// Go `inFunctionClass.verifyArgs`: a negative integer constant can
    /// never equal a BIT value, so it leaves the list.
    fn verify_args(
        ctx: &dyn BuildContext,
        mut args: Vec<Expression>,
    ) -> Result<Vec<Expression>, EvalError> {
        if type_of(&args[0]).code() == FieldTypeCode::Bit {
            let columns = ctx.columns().unwrap_or(&crate::NoColumns);
            let over_optimized = crate::expr_util::maybe_over_optimized_4_plan_cache(
                columns.use_plan_cache(),
                &args,
            );
            args.retain(|arg| match arg {
                Expression::Constant(constant) => match constant.value {
                    Datum::Int(value) if value < 0 => {
                        if over_optimized {
                            columns.set_skip_plan_cache(&format!("Bit Column in ({value})"));
                        }
                        false
                    }
                    _ => true,
                },
                _ => true,
            });
        }
        if args.len() < 2 {
            return Err(EvalError::WrongParameterCount("in"));
        }
        Ok(args)
    }
}

impl FunctionClass for InFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let args = Self::verify_args(ctx, args)?;
        let arg_tp = type_of(&args[0]).eval_type();
        let arg_tps = vec![arg_tp; args.len()];
        let mut bf = new_base_builtin_func_with_tp(ctx, "in", args, EvalType::Int, &arg_tps)?;
        for arg in &mut bf.args[1..] {
            disable_parse_json_flag_4_expr(arg);
        }
        bf.tp.set_flen(1);
        build_hash_map_for_const_args(ctx, &mut bf)?;
        bf.pb_code = match type_of(&bf.args[0]).eval_type() {
            EvalType::Int => ScalarFuncSig::InInt,
            EvalType::String => ScalarFuncSig::InString,
            EvalType::Real => ScalarFuncSig::InReal,
            EvalType::Decimal => ScalarFuncSig::InDecimal,
            EvalType::Datetime | EvalType::Timestamp => ScalarFuncSig::InTime,
            EvalType::Duration => ScalarFuncSig::InDuration,
            EvalType::Json => ScalarFuncSig::InJson,
            EvalType::VectorFloat32 => ScalarFuncSig::Unspecified,
        };
        Ok(bf)
    }
}

/// What one `builtinIn*Sig.hashSet` is keyed by.
#[derive(PartialEq, Eq, Hash)]
enum InKey {
    /// `builtinInIntSig`: the evaluated int64, whatever its sign flag.
    Int(i64),
    /// `builtinInStringSig`: the collation key; `builtinInDecimalSig`:
    /// `MyDecimal.ToHashKey`.
    Bytes(Vec<u8>),
    /// `builtinInRealSig`.
    Real(u64),
    /// `builtinInTimeSig`: `CoreTime`, without the type and the fsp.
    Time(tidb_datatype::CoreTime),
    /// `builtinInDurationSig`: `Duration.Duration`.
    Duration(i64),
}

/// The key Go's `Eval<Type>` result of a list constant hashes to.
fn in_key(
    value: &Datum,
    collator: tidb_datatype::Collator,
) -> Result<Option<InKey>, EvalError> {
    Ok(Some(match value {
        Datum::Int(value) => InKey::Int(*value),
        Datum::UInt(value) => InKey::Int(*value as i64),
        Datum::String(value) => InKey::Bytes(collator.key(value.bytes())),
        Datum::Bytes(value) => InKey::Bytes(collator.key(value)),
        // Go's map compares float64 keys with `==`: the two zeros are one.
        Datum::Real(value) | Datum::Float32(value) => InKey::Real((value + 0.0).to_bits()),
        Datum::Decimal(value) => InKey::Bytes(
            value
                .to_hash_key()
                .map_err(|tidb_datatype::DecimalCodecError::BadNumber| {
                    EvalError::Conversion(tidb_datatype::types_errors::ERR_BAD_NUMBER.clone())
                })?
                .0,
        ),
        Datum::Time(value) => InKey::Time(value.core_time()),
        Datum::Duration(value) => InKey::Duration(value.nanoseconds()),
        _ => return Ok(None),
    }))
}

/// Go `builtinIn*Sig.buildHashMapForConstArgs`, the part that rebuilds the
/// argument list: of the strict constants in the list only the first of
/// each value and one NULL stay, and a constant of this execution is
/// evaluated so that a wrong type fails the build. `builtinInJSONSig` and
/// `builtinInVectorFloat32Sig` have no hash map.
fn build_hash_map_for_const_args(
    ctx: &dyn BuildContext,
    bf: &mut BaseBuiltinFunc,
) -> Result<(), EvalError> {
    use crate::expression::ConstLevel;
    if matches!(
        type_of(&bf.args[0]).eval_type(),
        EvalType::Json | EvalType::VectorFloat32
    ) {
        return Ok(());
    }
    let collator = tidb_datatype::get_collator(bf.collation.charset_and_collation().1);
    let mut hash_set = std::collections::HashSet::with_capacity(bf.args.len() - 1);
    let mut has_null = false;
    let mut args = std::mem::take(&mut bf.args).into_iter();
    bf.args.extend(args.next());
    for arg in args {
        let level = arg.const_level();
        if level == ConstLevel::NONE {
            bf.args.push(arg);
            continue;
        }
        let value = ctx.eval_constant(&arg)?;
        if level == ConstLevel::ONLY_IN_CONTEXT {
            bf.args.push(arg);
            continue;
        }
        let unique = match &value {
            Datum::Null => !std::mem::replace(&mut has_null, true),
            value => in_key(value, collator)?.is_none_or(|key| hash_set.insert(key)),
        };
        if unique {
            bf.args.push(arg);
        }
    }
    Ok(())
}

/// Go `rowFunctionClass`.
pub(crate) struct RowFunctionClass;

impl FunctionClass for RowFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps: Vec<EvalType> = args.iter().map(|arg| type_of(arg).eval_type()).collect();
        new_base_builtin_func_with_tp(ctx, "row", args, EvalType::String, &arg_tps)
    }
}

function_class!(
    /// Go `bitCountFunctionClass`.
    BitCountFunctionClass, Int, [Int], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(2);
    }
);

/// Go `setVarFunctionClass`.
pub(crate) struct SetVarFunctionClass;

impl FunctionClass for SetVarFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tp = match type_of(&args[1]).eval_type() {
            EvalType::Timestamp | EvalType::Duration | EvalType::Json => EvalType::String,
            tp => tp,
        };
        if !matches!(
            arg_tp,
            EvalType::String | EvalType::Real | EvalType::Decimal | EvalType::Int | EvalType::Datetime
        ) {
            return Err(EvalError::Unsupported("unexpected types.EvalType"));
        }
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "setvar", args, arg_tp, &[EvalType::String, arg_tp])?;
        let flen = type_of(&bf.args[1]).flen();
        bf.tp.set_flen_under_limit(flen);
        Ok(bf)
    }
}

/// Go `getVarFunctionClass` with its five embedders (`getStringVarFunctionClass`,
/// `getIntVarFunctionClass`, `getRealVarFunctionClass`,
/// `getDecimalVarFunctionClass`, `getTimeVarFunctionClass`): `ret_tp` is the
/// embedder, `tp` the variable's declared type.
pub(crate) struct GetVarFunctionClass {
    ret_tp: EvalType,
    tp: FieldType,
}

impl FunctionClass for GetVarFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "getvar", args, self.ret_tp, &[EvalType::String])?;
        match self.ret_tp {
            EvalType::Int => {
                bf.tp.set_flen(self.tp.flen());
                bf.tp.set_raw_flags(self.tp.raw_flags());
            }
            EvalType::Real => bf.tp.set_flen(self.tp.flen()),
            EvalType::Decimal => bf.tp.set_flen_under_limit(self.tp.flen()),
            EvalType::Datetime => {
                if self.tp.code() == FieldTypeCode::Datetime {
                    let mut fsp = self.tp.flen() - MAX_DATETIME_WIDTH_NO_FSP;
                    if fsp > 0 {
                        fsp -= 1;
                    }
                    bf.set_decimal_and_flen_for_datetime(fsp);
                } else {
                    bf.set_decimal_and_flen_for_date();
                }
            }
            _ => {
                bf.tp.set_flen(self.tp.flen());
                if !self.tp.charset_name().is_empty() {
                    bf.tp.set_charset_name(self.tp.charset_name());
                    bf.tp.set_collation_name(self.tp.collation_name());
                }
            }
        }
        Ok(bf)
    }
}

/// Go `mysql.MaxDatetimeWidthNoFsp`.
const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;

/// Go `BuildGetVarFunction`.
pub(crate) fn build_get_var_function(
    ctx: &dyn BuildContext,
    expr: Expression,
    ret_type: FieldType,
) -> Result<Expression, EvalError> {
    let ret_tp = match ret_type.eval_type() {
        tp @ (EvalType::Int | EvalType::Decimal | EvalType::Real | EvalType::Datetime) => tp,
        _ => EvalType::String,
    };
    let fc = GetVarFunctionClass { ret_tp, tp: ret_type.clone() };
    let f = fc.get_function(ctx, vec![expr])?;
    Ok(convert_readonly_var_to_const(
        ctx,
        crate::new_function::scalar_function_of("getvar", ret_type, f),
    ))
}

/// Go `convertReadonlyVarToConst`: a user variable the statement only reads
/// is the constant it holds.
fn convert_readonly_var_to_const(
    ctx: &dyn BuildContext,
    get_var: crate::scalar_function::ScalarFunction,
) -> Expression {
    let var_name = match get_var.args.first() {
        Some(Expression::Constant(arg0)) if arg0.deferred_expr.is_none() => {
            String::from_utf8_lossy(arg0.value.go_bytes()).into_owned()
        }
        _ => return Expression::ScalarFunction(get_var),
    };
    let Some(eval_ctx) = ctx.columns().filter(|eval_ctx| eval_ctx.is_readonly_user_var(&var_name))
    else {
        return Expression::ScalarFunction(get_var);
    };
    let ret_type = get_var.ret_type.clone();
    let get_var = Expression::ScalarFunction(get_var);
    let Ok(mut v) = ctx.eval_constant(&get_var) else {
        return get_var;
    };
    if matches!(eval_ctx.get_uservar(&var_name), Some(Datum::BinaryLiteral(_))) {
        v = Datum::BinaryLiteral(v.go_bytes().to_vec().into());
    }
    let mut constant = crate::constant::Constant::default();
    constant.value = v;
    constant.ret_type = ret_type;
    Expression::Constant(constant)
}

/// Go `valuesFunctionClass`: `offset` is the column's position in the
/// insert row, `tp` its type.
pub(crate) struct ValuesFunctionClass {
    offset: usize,
    tp: FieldType,
}

impl FunctionClass for ValuesFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        if !args.is_empty() {
            return Err(EvalError::WrongParameterCount("values"));
        }
        let mut bf = crate::builtin::new_base_builtin_func(ctx, "values", args, self.tp.clone())?;
        bf.sig = crate::builtin::SigFields::Values { offset: self.offset };
        Ok(bf)
    }
}

/// Go `NewValuesFunc`.
pub(crate) fn new_values_func(
    ctx: &dyn BuildContext,
    offset: usize,
    ret_tp: FieldType,
) -> Result<crate::scalar_function::ScalarFunction, EvalError> {
    let fc = ValuesFunctionClass { offset, tp: ret_tp.clone() };
    let bt = fc.get_function(ctx, Vec::new())?;
    Ok(crate::scalar_function::ScalarFunction::from_builtin_func(
        tidb_ast::CiString::new("values"),
        ret_tp,
        bt,
    ))
}

function_class!(
    /// Go `getParamFunctionClass`.
    GetParamFunctionClass, String, [Int], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(65_535);
    }
);
