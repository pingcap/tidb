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
use tidb_datatype::{Datum, EvalType, FieldTypeCode};
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

function_class!(
    /// Go `getParamFunctionClass`.
    GetParamFunctionClass, String, [Int], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(65_535);
    }
);
