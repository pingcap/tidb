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

//! `pkg/expression/builtin_op.go`: the logic, bit, truth-test, unary and
//! `IS NULL` function classes, and `wrapWithIsTrue` (`expression.go`).

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::expr_util::traits::is_logical_op;
use crate::expression::Expression;
use crate::wrap_cast::type_of;
use crate::EvalError;
use tidb_datatype::{Datum, EvalType, FieldTypeCode, FieldTypeFlags};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `wrapWithIsTrue`: `arg` wrapped in `istrue` unless it is an integer.
/// `keep_null` selects `istrue_with_null`, which answers NULL for a NULL
/// `arg`; `wrap_for_int` wraps an integer that is not a logical operator's
/// result as well.
pub(crate) fn wrap_with_is_true(
    ctx: &dyn BuildContext,
    keep_null: bool,
    arg: Expression,
    wrap_for_int: bool,
) -> Result<Expression, EvalError> {
    if type_of(&arg).eval_type() == EvalType::Int {
        if !wrap_for_int {
            return Ok(arg);
        }
        if let Expression::ScalarFunction(child) = &arg {
            if is_logical_op(child.func_name.lowercase()) {
                return Ok(arg);
            }
        }
    }
    let fc = if keep_null {
        &IS_TRUTH_WITH_NULL
    } else {
        &IS_TRUTH_WITHOUT_NULL
    };
    let f = fc.get_function(ctx, vec![arg])?;
    let mut sf = Expression::ScalarFunction(crate::new_function::scalar_function_of(
        fc.func_name,
        tidb_datatype::FieldType::new(FieldTypeCode::Unspecified),
        f,
    ));
    ctx.fold_constant(&mut sf);
    Ok(sf)
}

/// Go `logic{And,Or,Xor}FunctionClass.getFunction`: both arguments read as
/// truth values, the result one digit wide.
fn logic_function(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
    pb_code: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    let args = args
        .into_iter()
        .map(|arg| wrap_with_is_true(ctx, true, arg, false))
        .collect::<Result<Vec<_>, _>>()?;
    let mut bf = new_base_builtin_func_with_tp(
        ctx,
        func_name,
        args,
        EvalType::Int,
        &[EvalType::Int, EvalType::Int],
    )?;
    bf.pb_code = pb_code;
    bf.tp.set_flen(1);
    Ok(bf)
}

/// Go `logicAndFunctionClass`.
pub(crate) struct LogicAndFunctionClass;

impl FunctionClass for LogicAndFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        logic_function(ctx, "and", args, ScalarFuncSig::LogicalAnd)
    }
}

/// Go `logicOrFunctionClass`.
pub(crate) struct LogicOrFunctionClass;

impl FunctionClass for LogicOrFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        logic_function(ctx, "or", args, ScalarFuncSig::LogicalOr)
    }
}

/// Go `logicXorFunctionClass`.
pub(crate) struct LogicXorFunctionClass;

impl FunctionClass for LogicXorFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        logic_function(ctx, "xor", args, ScalarFuncSig::LogicalXor)
    }
}

/// Go `bit{And,Or,Xor}FunctionClass` and `{left,right}ShiftFunctionClass`:
/// two integers to an unsigned integer.
pub(crate) struct BitOpFunctionClass {
    func_name: &'static str,
    pb_code: ScalarFuncSig,
}

/// Go `bitAndFunctionClass`.
pub(crate) static BIT_AND: BitOpFunctionClass = BitOpFunctionClass {
    func_name: "bitand",
    pb_code: ScalarFuncSig::BitAndSig,
};
/// Go `bitOrFunctionClass`.
pub(crate) static BIT_OR: BitOpFunctionClass = BitOpFunctionClass {
    func_name: "bitor",
    pb_code: ScalarFuncSig::BitOrSig,
};
/// Go `bitXorFunctionClass`.
pub(crate) static BIT_XOR: BitOpFunctionClass = BitOpFunctionClass {
    func_name: "bitxor",
    pb_code: ScalarFuncSig::BitXorSig,
};
/// Go `leftShiftFunctionClass`.
pub(crate) static LEFT_SHIFT: BitOpFunctionClass = BitOpFunctionClass {
    func_name: "leftshift",
    pb_code: ScalarFuncSig::LeftShift,
};
/// Go `rightShiftFunctionClass`.
pub(crate) static RIGHT_SHIFT: BitOpFunctionClass = BitOpFunctionClass {
    func_name: "rightshift",
    pb_code: ScalarFuncSig::RightShift,
};

impl FunctionClass for BitOpFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            self.func_name,
            args,
            EvalType::Int,
            &[EvalType::Int, EvalType::Int],
        )?;
        bf.pb_code = self.pb_code;
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        Ok(bf)
    }
}

/// Go `isTrueOrFalseFunctionClass`.
pub(crate) struct IsTrueOrFalseFunctionClass {
    func_name: &'static str,
    /// Go `op == opcode.IsFalsity`.
    falsity: bool,
    /// Go `keepNull`: a NULL argument answers NULL, not 0.
    keep_null: bool,
}

/// Go `funcs[ast.IsTruthWithoutNull]`.
pub(crate) static IS_TRUTH_WITHOUT_NULL: IsTrueOrFalseFunctionClass = IsTrueOrFalseFunctionClass {
    func_name: "istrue",
    falsity: false,
    keep_null: false,
};
/// Go `funcs[ast.IsTruthWithNull]`.
pub(crate) static IS_TRUTH_WITH_NULL: IsTrueOrFalseFunctionClass = IsTrueOrFalseFunctionClass {
    func_name: "istrue_with_null",
    falsity: false,
    keep_null: true,
};
/// Go `funcs[ast.IsFalsity]`.
pub(crate) static IS_FALSITY: IsTrueOrFalseFunctionClass = IsTrueOrFalseFunctionClass {
    func_name: "isfalse",
    falsity: true,
    keep_null: false,
};

impl FunctionClass for IsTrueOrFalseFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut arg_tp = type_of(&args[0]).eval_type();
        if matches!(
            arg_tp,
            EvalType::Timestamp
                | EvalType::Datetime
                | EvalType::Duration
                | EvalType::Json
                | EvalType::String
        ) {
            arg_tp = EvalType::Real;
        }
        let mut bf =
            new_base_builtin_func_with_tp(ctx, self.func_name, args, EvalType::Int, &[arg_tp])?;
        bf.tp.set_flen(1);
        use ScalarFuncSig::*;
        bf.pb_code = match (self.falsity, arg_tp, self.keep_null) {
            (false, EvalType::Real, true) => RealIsTrueWithNull,
            (false, EvalType::Real, false) => RealIsTrue,
            (false, EvalType::Decimal, true) => DecimalIsTrueWithNull,
            (false, EvalType::Decimal, false) => DecimalIsTrue,
            (false, EvalType::Int, true) => IntIsTrueWithNull,
            (false, EvalType::Int, false) => IntIsTrue,
            (true, EvalType::Real, true) => RealIsFalseWithNull,
            (true, EvalType::Real, false) => RealIsFalse,
            (true, EvalType::Decimal, true) => DecimalIsFalseWithNull,
            (true, EvalType::Decimal, false) => DecimalIsFalse,
            (true, EvalType::Int, true) => IntIsFalseWithNull,
            (true, EvalType::Int, false) => IntIsFalse,
            // `builtinVectorFloat32Is{True,False}Sig` set no pb code.
            (_, EvalType::VectorFloat32, _) => Unspecified,
            (_, other, _) => {
                return Err(EvalError::Raw(format!(
                    "unexpected types.EvalType {}",
                    other.as_str()
                )))
            }
        };
        Ok(bf)
    }
}

/// Go `bitNegFunctionClass`.
pub(crate) struct BitNegFunctionClass;

impl FunctionClass for BitNegFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "bitneg", args, EvalType::Int, &[EvalType::Int])?;
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        bf.pb_code = ScalarFuncSig::BitNegSig;
        Ok(bf)
    }
}

/// Go `unaryNotFunctionClass`.
pub(crate) struct UnaryNotFunctionClass;

impl FunctionClass for UnaryNotFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut arg_tp = type_of(&args[0]).eval_type();
        if matches!(arg_tp, EvalType::Timestamp | EvalType::Datetime | EvalType::Duration) {
            arg_tp = EvalType::Int;
        } else if arg_tp == EvalType::String {
            arg_tp = EvalType::Real;
        }
        let mut bf = new_base_builtin_func_with_tp(ctx, "not", args, EvalType::Int, &[arg_tp])?;
        bf.tp.set_flen(1);
        bf.pb_code = match arg_tp {
            EvalType::Real => ScalarFuncSig::UnaryNotReal,
            EvalType::Decimal => ScalarFuncSig::UnaryNotDecimal,
            EvalType::Int => ScalarFuncSig::UnaryNotInt,
            EvalType::Json => {
                ctx.append_warning(&crate::expression_errors::ERR_JSON_IN_BOOLEAN_CONTEXT);
                ScalarFuncSig::UnaryNotJson
            }
            other => {
                return Err(EvalError::Raw(format!(
                    "{} is not supported for unary not operator",
                    other.as_str()
                )))
            }
        };
        Ok(bf)
    }
}

/// Go `unaryMinusFunctionClass`.
pub(crate) struct UnaryMinusFunctionClass;

impl UnaryMinusFunctionClass {
    /// Go `handleIntOverflow`: whether negating the integer constant leaves
    /// `BIGINT`.
    fn handle_int_overflow(arg: &crate::constant::Constant, unsigned: bool) -> bool {
        // Go reads the datum's integer bits by the type's sign.
        let bits = match arg.value {
            Datum::Int(value) => value,
            Datum::UInt(value) => value.cast_signed(),
            _ => return false,
        };
        if unsigned {
            // -math.MinInt64 is 9223372036854775808; a larger value has no
            // negation in BIGINT.
            bits.cast_unsigned() > 1_u64 << 63
        } else {
            bits == i64::MIN
        }
    }

    /// Go `typeInfer`'s overflow answer: an integer constant whose negation
    /// overflows is negated as a decimal.
    fn type_infer_overflow(arg_expr: &Expression) -> bool {
        let tp = type_of(arg_expr);
        match arg_expr {
            Expression::Constant(arg) if tp.eval_type() == EvalType::Int => {
                Self::handle_int_overflow(arg, tp.is_unsigned())
            }
            _ => false,
        }
    }
}

impl FunctionClass for UnaryMinusFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        const FUNC_NAME: &str = "unaryminus";
        let arg_expr_tp = type_of(&args[0]);
        let int_overflow = Self::type_infer_overflow(&args[0]);
        let is_column = matches!(
            args[0],
            Expression::Column(_) | Expression::CorrelatedColumn(_)
        );
        let build = |args, tp: EvalType| new_base_builtin_func_with_tp(ctx, FUNC_NAME, args, tp, &[tp]);

        let eval_type = arg_expr_tp.eval_type();
        let mut bf = match eval_type {
            EvalType::Int => {
                let mut bf = if int_overflow {
                    let mut bf = build(args, EvalType::Decimal)?;
                    bf.pb_code = ScalarFuncSig::UnaryMinusDecimal;
                    bf
                } else {
                    let mut bf = build(args, EvalType::Int)?;
                    bf.pb_code = ScalarFuncSig::UnaryMinusInt;
                    bf
                };
                bf.tp.set_decimal(0);
                bf
            }
            EvalType::Real => {
                let mut bf = build(args, EvalType::Real)?;
                bf.pb_code = ScalarFuncSig::UnaryMinusReal;
                bf
            }
            _ => {
                let as_decimal = eval_type == EvalType::Decimal
                    || matches!(
                        arg_expr_tp.code(),
                        FieldTypeCode::Date
                            | FieldTypeCode::NewDate
                            | FieldTypeCode::Datetime
                            | FieldTypeCode::Timestamp
                            | FieldTypeCode::Duration
                    );
                if as_decimal {
                    let mut bf = build(args, EvalType::Decimal)?;
                    bf.tp.set_decimal_under_limit(arg_expr_tp.decimal());
                    bf.pb_code = ScalarFuncSig::UnaryMinusDecimal;
                    bf
                } else {
                    let mut bf = build(args, EvalType::Real)?;
                    bf.pb_code = ScalarFuncSig::UnaryMinusReal;
                    bf
                }
            }
        };
        if is_column
            && (eval_type == EvalType::Decimal
                || (eval_type == EvalType::Int && !int_overflow && !arg_expr_tp.is_unsigned()))
        {
            // Column types already reserve their signed display width, so
            // unary minus does not widen them.
            bf.tp.set_flen_under_limit(arg_expr_tp.flen());
        } else {
            bf.tp.set_flen_under_limit(arg_expr_tp.flen() + 1);
        }
        Ok(bf)
    }
}

/// Go `isNullFunctionClass`.
pub(crate) struct IsNullFunctionClass;

impl FunctionClass for IsNullFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut arg_tp = type_of(&args[0]).eval_type();
        if arg_tp == EvalType::Timestamp {
            arg_tp = EvalType::Datetime;
        } else if arg_tp == EvalType::Json {
            arg_tp = EvalType::String;
        }
        let mut bf = new_base_builtin_func_with_tp(ctx, "isnull", args, EvalType::Int, &[arg_tp])?;
        bf.tp.set_flen(1);
        bf.pb_code = match arg_tp {
            EvalType::Int => ScalarFuncSig::IntIsNull,
            EvalType::Decimal => ScalarFuncSig::DecimalIsNull,
            EvalType::Real => ScalarFuncSig::RealIsNull,
            EvalType::Datetime => ScalarFuncSig::TimeIsNull,
            EvalType::Duration => ScalarFuncSig::DurationIsNull,
            EvalType::String => ScalarFuncSig::StringIsNull,
            EvalType::VectorFloat32 => ScalarFuncSig::VectorFloat32IsNull,
            other => {
                return Err(EvalError::Raw(format!(
                    "{} is not supported for ISNULL()",
                    other.as_str()
                )))
            }
        };
        Ok(bf)
    }
}
