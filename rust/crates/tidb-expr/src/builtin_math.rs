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

//! `pkg/expression/builtin_math.go`: the math function classes.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::expression::Expression;
use crate::wrap_cast::type_of;
use crate::EvalError;
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `types.UnspecifiedLength`.
const UNSPECIFIED_LENGTH: i64 = -1;
/// Go `mysql.MaxIntWidth`.
const MAX_INT_WIDTH: i64 = 20;
/// Go `mysql.MaxDecimalScale`.
const MAX_DECIMAL_SCALE: i64 = 30;

/// A Go function class whose `getFunction` is `newBaseBuiltinFuncWithTp`
/// over fixed evaluation types and one signature.
macro_rules! fixed_function_class {
    ($(#[$doc:meta])* $class:ident, $ret:ident, [$($arg:ident),*], $sig:ident) => {
        $(#[$doc])*
        pub(crate) struct $class {
            pub(crate) func_name: &'static str,
        }

        impl FunctionClass for $class {
            fn get_function(
                &self,
                ctx: &dyn BuildContext,
                args: Vec<Expression>,
            ) -> Result<BaseBuiltinFunc, EvalError> {
                let arg_tps = [$(EvalType::$arg),*];
                let mut bf =
                    new_base_builtin_func_with_tp(ctx, self.func_name, args, EvalType::$ret, &arg_tps)?;
                bf.pb_code = ScalarFuncSig::$sig;
                Ok(bf)
            }
        }
    };
}

fixed_function_class!(
    /// Go `log2FunctionClass`.
    Log2FunctionClass, Real, [Real], Log2
);
fixed_function_class!(
    /// Go `log10FunctionClass`.
    Log10FunctionClass, Real, [Real], Log10
);
fixed_function_class!(
    /// Go `powFunctionClass`.
    PowFunctionClass, Real, [Real, Real], Pow
);
fixed_function_class!(
    /// Go `signFunctionClass`.
    SignFunctionClass, Int, [Real], Sign
);
fixed_function_class!(
    /// Go `sqrtFunctionClass`.
    SqrtFunctionClass, Real, [Real], Sqrt
);
fixed_function_class!(
    /// Go `acosFunctionClass`.
    AcosFunctionClass, Real, [Real], Acos
);
fixed_function_class!(
    /// Go `asinFunctionClass`.
    AsinFunctionClass, Real, [Real], Asin
);
fixed_function_class!(
    /// Go `cosFunctionClass`.
    CosFunctionClass, Real, [Real], Cos
);
fixed_function_class!(
    /// Go `cotFunctionClass`.
    CotFunctionClass, Real, [Real], Cot
);
fixed_function_class!(
    /// Go `degreesFunctionClass`.
    DegreesFunctionClass, Real, [Real], Degrees
);
fixed_function_class!(
    /// Go `expFunctionClass`.
    ExpFunctionClass, Real, [Real], Exp
);
fixed_function_class!(
    /// Go `radiansFunctionClass`.
    RadiansFunctionClass, Real, [Real], Radians
);
fixed_function_class!(
    /// Go `sinFunctionClass`.
    SinFunctionClass, Real, [Real], Sin
);
fixed_function_class!(
    /// Go `tanFunctionClass`.
    TanFunctionClass, Real, [Real], Tan
);

/// The evaluation type `ABS` and `ROUND` read their first argument in: its
/// own when that is an integer or a decimal, a double otherwise.
fn int_decimal_or_real(arg: &Expression) -> EvalType {
    match type_of(arg).eval_type() {
        arg_tp @ (EvalType::Int | EvalType::Decimal) => arg_tp,
        _ => EvalType::Real,
    }
}

/// Go `absFunctionClass`.
pub(crate) struct AbsFunctionClass;

impl FunctionClass for AbsFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_field_tp = type_of(&args[0]);
        let arg_tp = int_decimal_or_real(&args[0]);
        let unsigned = arg_field_tp.has_flag(FieldTypeFlags::UNSIGNED);
        let mut bf = new_base_builtin_func_with_tp(ctx, "abs", args, arg_tp, &[arg_tp])?;
        if unsigned {
            bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        }
        if arg_tp == EvalType::Real {
            let (flen, decimal) =
                tidb_mysql::util::default_field_length_and_decimal(tidb_mysql::types::TypeDouble);
            bf.tp.set_flen(flen);
            bf.tp.set_decimal(i64::from(decimal));
        } else {
            bf.tp.set_flen_under_limit(arg_field_tp.flen());
            bf.tp.set_decimal_under_limit(arg_field_tp.decimal());
        }
        bf.pb_code = match arg_tp {
            EvalType::Int if unsigned => ScalarFuncSig::AbsUInt,
            EvalType::Int => ScalarFuncSig::AbsInt,
            EvalType::Decimal => ScalarFuncSig::AbsDecimal,
            _ => ScalarFuncSig::AbsReal,
        };
        Ok(bf)
    }
}

/// Go `roundFunctionClass`.
pub(crate) struct RoundFunctionClass;

impl FunctionClass for RoundFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tp = int_decimal_or_real(&args[0]);
        let arg_tps = [arg_tp, EvalType::Int];
        let with_frac = args.len() > 1;
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "round", args, arg_tp, &arg_tps[..=usize::from(with_frac)])?;
        // Go reads `args[0]` after `newBaseBuiltinFuncWithTp` cast it in place.
        let arg_field_tp = type_of(&bf.args[0]);
        if arg_field_tp.has_flag(FieldTypeFlags::UNSIGNED) {
            bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        }
        if arg_tp == EvalType::Decimal {
            bf.tp.set_flen_under_limit(arg_field_tp.flen());
            bf.tp
                .set_decimal_under_limit(calculate_decimal_4_round_and_truncate(ctx, &bf.args, arg_tp));
            if bf.tp.decimal() != UNSPECIFIED_LENGTH {
                if arg_field_tp.decimal() != UNSPECIFIED_LENGTH {
                    let decimal_delta = bf.tp.decimal() - arg_field_tp.decimal();
                    bf.tp.set_flen_under_limit(bf.tp.flen() + decimal_delta.max(0));
                } else {
                    bf.tp.set_flen_under_limit(arg_field_tp.flen() + bf.tp.decimal());
                }
            }
        }
        bf.pb_code = match (with_frac, arg_tp) {
            (true, EvalType::Int) => ScalarFuncSig::RoundWithFracInt,
            (true, EvalType::Decimal) => ScalarFuncSig::RoundWithFracDec,
            (true, _) => ScalarFuncSig::RoundWithFracReal,
            (false, EvalType::Int) => ScalarFuncSig::RoundInt,
            (false, EvalType::Decimal) => ScalarFuncSig::RoundDec,
            (false, _) => ScalarFuncSig::RoundReal,
        };
        Ok(bf)
    }
}

/// Go `calculateDecimal4RoundAndTruncate`: the result's scale for a decimal
/// or real `ROUND` / `TRUNCATE`.
fn calculate_decimal_4_round_and_truncate(
    ctx: &dyn BuildContext,
    args: &[Expression],
    ret_type: EvalType,
) -> i64 {
    if ret_type == EvalType::Int || args.len() <= 1 {
        return 0;
    }
    let Some(second_const) = folded_constant(ctx, &args[1]) else {
        return type_of(&args[0]).decimal();
    };
    match crate::arg_eval_type::eval_int(&second_const) {
        Ok(Some(arg_dec)) if arg_dec >= 0 => arg_dec.min(MAX_DECIMAL_SCALE),
        _ => 0,
    }
}

/// The value of an argument Go would hold as a `*Constant`. Go asks
/// `arg.(*Constant)` of a tree `NewFunction` has already folded, and
/// `foldConstant` leaves a subtree whose evaluation fails unfolded; on this
/// unfolded tree that is `folds_to_constant` plus a successful evaluation.
fn folded_constant(ctx: &dyn BuildContext, arg: &Expression) -> Option<Datum> {
    if !crate::constant_fold::folds_to_constant(arg) {
        return None;
    }
    ctx.eval_constant(arg).ok()
}

/// Go `getEvalTp4FloorAndCeil`: the `(retTp, argTp)` of `FLOOR` / `CEIL`.
fn get_eval_tp_4_floor_and_ceil(arg: &Expression) -> (EvalType, EvalType) {
    let field_tp = type_of(arg);
    match field_tp.eval_type() {
        EvalType::Int => (EvalType::Int, EvalType::Int),
        // len(math.MaxInt64) - 1
        EvalType::Decimal if field_tp.flen() - field_tp.decimal() > MAX_INT_WIDTH - 2 => {
            (EvalType::Decimal, EvalType::Decimal)
        }
        EvalType::Decimal => (EvalType::Int, EvalType::Decimal),
        _ => (EvalType::Real, EvalType::Real),
    }
}

/// Go `setFlag4FloorAndCeil`.
fn set_flag_4_floor_and_ceil(tp: &mut FieldType, arg: &Expression) {
    let field_tp = type_of(arg);
    if matches!(
        field_tp.code(),
        FieldTypeCode::Long | FieldTypeCode::LongLong | FieldTypeCode::NewDecimal
    ) && field_tp.has_flag(FieldTypeFlags::UNSIGNED)
    {
        tp.add_flags(FieldTypeFlags::UNSIGNED);
    }
}

/// The shared body of Go `ceilFunctionClass.getFunction` and
/// `floorFunctionClass.getFunction`, which differ in their signatures only:
/// `sigs` is `[IntToInt, IntToDec, DecToInt, DecToDec, Real]`.
fn floor_or_ceil(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
    sigs: [ScalarFuncSig; 5],
) -> Result<BaseBuiltinFunc, EvalError> {
    let (ret_tp, arg_tp) = get_eval_tp_4_floor_and_ceil(&args[0]);
    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, ret_tp, &[arg_tp])?;
    // Go reads `args[0]` after `newBaseBuiltinFuncWithTp` cast it in place.
    set_flag_4_floor_and_ceil(&mut bf.tp, &bf.args[0]);
    if ret_tp == EvalType::Decimal {
        bf.tp.set_flen_under_limit(type_of(&bf.args[0]).flen());
        bf.tp.set_decimal(0);
    }
    let [int_to_int, int_to_dec, dec_to_int, dec_to_dec, real] = sigs;
    bf.pb_code = match (arg_tp, ret_tp) {
        (EvalType::Int, EvalType::Int) => int_to_int,
        (EvalType::Int, _) => int_to_dec,
        (EvalType::Decimal, EvalType::Int) => dec_to_int,
        (EvalType::Decimal, _) => dec_to_dec,
        _ => real,
    };
    Ok(bf)
}

/// Go `ceilFunctionClass`.
pub(crate) struct CeilFunctionClass {
    pub(crate) func_name: &'static str,
}

impl FunctionClass for CeilFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        use ScalarFuncSig::{CeilDecToDec, CeilDecToInt, CeilIntToDec, CeilIntToInt, CeilReal};
        let sigs = [CeilIntToInt, CeilIntToDec, CeilDecToInt, CeilDecToDec, CeilReal];
        floor_or_ceil(ctx, self.func_name, args, sigs)
    }
}

/// Go `floorFunctionClass`.
pub(crate) struct FloorFunctionClass;

impl FunctionClass for FloorFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        use ScalarFuncSig::{FloorDecToDec, FloorDecToInt, FloorIntToDec, FloorIntToInt, FloorReal};
        let sigs = [FloorIntToInt, FloorIntToDec, FloorDecToInt, FloorDecToDec, FloorReal];
        floor_or_ceil(ctx, "floor", args, sigs)
    }
}

/// Go `logFunctionClass`.
pub(crate) struct LogFunctionClass {
    pub(crate) func_name: &'static str,
}

impl FunctionClass for LogFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = [EvalType::Real; 2];
        let args_len = args.len();
        let mut bf =
            new_base_builtin_func_with_tp(ctx, self.func_name, args, EvalType::Real, &arg_tps[..args_len])?;
        bf.pb_code = if args_len == 1 {
            ScalarFuncSig::Log1Arg
        } else {
            ScalarFuncSig::Log2Args
        };
        Ok(bf)
    }
}

/// Go `randFunctionClass`.
pub(crate) struct RandFunctionClass;

impl FunctionClass for RandFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = [EvalType::Int];
        let args_len = args.len();
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "rand", args, EvalType::Real, &arg_tps[..args_len])?;
        // Go seeds `builtinRandSig`'s generator from a constant argument
        // here; this port's generator is seeded at the first evaluation.
        bf.pb_code = match bf.args.first() {
            None => ScalarFuncSig::Rand,
            Some(seed) if folded_constant(ctx, seed).is_some() => ScalarFuncSig::Rand,
            Some(_) => ScalarFuncSig::RandWithSeedFirstGen,
        };
        Ok(bf)
    }
}

/// Go `convFunctionClass`.
pub(crate) struct ConvFunctionClass;

impl FunctionClass for ConvFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = [EvalType::String, EvalType::Int, EvalType::Int];
        let mut bf = new_base_builtin_func_with_tp(ctx, "conv", args, EvalType::String, &arg_tps)?;
        let (charset, collate) = ctx.connection_charset_info();
        bf.tp.set_charset_name(charset);
        bf.tp.set_collation_name(collate);
        bf.tp.set_flen(64);
        bf.pb_code = ScalarFuncSig::Conv;
        Ok(bf)
    }
}

/// Go `crc32FunctionClass`.
pub(crate) struct Crc32FunctionClass;

impl FunctionClass for Crc32FunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "crc32", args, EvalType::Int, &[EvalType::String])?;
        bf.tp.set_flen(10);
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        bf.pb_code = ScalarFuncSig::Crc32;
        Ok(bf)
    }
}

/// Go `atanFunctionClass`.
pub(crate) struct AtanFunctionClass {
    pub(crate) func_name: &'static str,
}

impl FunctionClass for AtanFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = [EvalType::Real; 2];
        let args_len = args.len();
        let mut bf =
            new_base_builtin_func_with_tp(ctx, self.func_name, args, EvalType::Real, &arg_tps[..args_len])?;
        bf.pb_code = if args_len == 1 {
            ScalarFuncSig::Atan1Arg
        } else {
            ScalarFuncSig::Atan2Args
        };
        Ok(bf)
    }
}

/// Go `piFunctionClass`.
pub(crate) struct PiFunctionClass;

impl FunctionClass for PiFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func_with_tp(ctx, "pi", args, EvalType::Real, &[])?;
        bf.tp.set_decimal(6);
        bf.tp.set_flen(8);
        bf.pb_code = ScalarFuncSig::Pi;
        Ok(bf)
    }
}

/// Go `truncateFunctionClass`.
pub(crate) struct TruncateFunctionClass;

impl FunctionClass for TruncateFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut arg_tp = type_of(&args[0]).eval_type();
        if arg_tp.is_string_kind() {
            arg_tp = EvalType::Real;
        }
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "truncate", args, arg_tp, &[arg_tp, EvalType::Int])?;
        // Go reads `args[0]` after `newBaseBuiltinFuncWithTp` cast it in place.
        let arg_field_tp = type_of(&bf.args[0]);
        if arg_tp == EvalType::Decimal {
            bf.tp
                .set_decimal_under_limit(calculate_decimal_4_round_and_truncate(ctx, &bf.args, arg_tp));
            bf.tp
                .set_flen_under_limit(arg_field_tp.flen() - arg_field_tp.decimal() + bf.tp.decimal());
        }
        let unsigned = arg_field_tp.has_flag(FieldTypeFlags::UNSIGNED);
        if unsigned {
            bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        }
        // `arg_tp` is an integer, a decimal or a double: Go's `default` arm
        // (`errIncorrectArgs`) has no evaluation type left to reach it.
        bf.pb_code = match arg_tp {
            EvalType::Int if unsigned => ScalarFuncSig::TruncateUint,
            EvalType::Int => ScalarFuncSig::TruncateInt,
            EvalType::Decimal => ScalarFuncSig::TruncateDecimal,
            _ => ScalarFuncSig::TruncateReal,
        };
        Ok(bf)
    }
}
