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

//! `pkg/expression/builtin_control.go`: the `CASE`, `IF` and `IFNULL`
//! function classes and the type the control functions infer.

use crate::builtin::{
    new_base_builtin_func_with_field_types, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::builtin_op::wrap_with_is_true;
use crate::collation_derive::check_and_derive_collation_from_exprs_with_connection;
use crate::expression::Expression;
use crate::wrap_cast::{set_bin_chs_cln_flag, type_of};
use crate::EvalError;
use tidb_datatype::{
    agg_field_type, aggregate_eval_type, EvalType, FieldType, FieldTypeCode, FieldTypeFlags,
    UNSPECIFIED_LENGTH,
};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `mysql.MaxRealWidth`.
const MAX_REAL_WIDTH: i64 = 23;
/// Go `mysql.MaxDatetimeWidthNoFsp`: `YYYY-MM-DD HH:MM:SS`.
const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;
/// Go `mysql.DefaultCharset`.
const DEFAULT_CHARSET: &str = "utf8mb4";
/// Go `mysql.DefaultCollationName`.
const DEFAULT_COLLATION_NAME: &str = "utf8mb4_bin";
/// Go `charset.CollationBin`.
const COLLATION_BIN: &str = "binary";

/// Go `maxlen`: `-1` is an unknown length, as an expression's is.
fn maxlen(lhs_flen: i64, rhs_flen: i64) -> i64 {
    if lhs_flen < 0 || rhs_flen < 0 {
        return MAX_REAL_WIDTH;
    }
    lhs_flen.max(rhs_flen)
}

/// Go `setFlenFromArgs`.
fn set_flen_from_args(eval_type: EvalType, result_field_type: &mut FieldType, arg_tps: &[FieldType]) {
    if eval_type == EvalType::Decimal || eval_type == EvalType::Int {
        let mut max_arg_flen = 0;
        for arg_tp in arg_tps {
            let flag_len = i64::from(!arg_tp.is_unsigned());
            let mut flen = arg_tp.flen() - flag_len;
            if arg_tp.decimal() != UNSPECIFIED_LENGTH {
                flen -= arg_tp.decimal();
            }
            max_arg_flen = maxlen(max_arg_flen, flen);
        }
        // `+ 1` accounts for the fields of length -1.
        let result_flen = max_arg_flen + result_field_type.decimal() + 1;
        result_field_type.set_flen_under_limit(result_flen);
    } else if eval_type == EvalType::String {
        let mut max_len = 0;
        for arg_tp in arg_tps {
            match arg_tp.code() {
                FieldTypeCode::Tiny => max_len = maxlen(4, max_len),
                FieldTypeCode::Short => max_len = maxlen(6, max_len),
                FieldTypeCode::Int24 => max_len = maxlen(9, max_len),
                FieldTypeCode::Long => max_len = maxlen(11, max_len),
                FieldTypeCode::LongLong => max_len = maxlen(20, max_len),
                _ => {
                    let arg_flen = arg_tp.flen();
                    if arg_flen == UNSPECIFIED_LENGTH {
                        result_field_type.set_flen(UNSPECIFIED_LENGTH);
                        return;
                    }
                    max_len = maxlen(arg_flen, max_len);
                }
            }
        }
        result_field_type.set_flen(max_len);
    } else {
        let mut max_len = 0;
        for arg_tp in arg_tps {
            max_len = maxlen(arg_tp.flen(), max_len);
        }
        result_field_type.set_flen(max_len);
    }
}

/// Go `setDecimalFromArgs`.
fn set_decimal_from_args(
    eval_type: EvalType,
    result_field_type: &mut FieldType,
    arg_tps: &[FieldType],
) {
    if eval_type == EvalType::Int {
        result_field_type.set_decimal(0);
        return;
    }
    let mut max_decimal = 0;
    for arg_tp in arg_tps {
        if arg_tp.decimal() == UNSPECIFIED_LENGTH {
            result_field_type.set_decimal(UNSPECIFIED_LENGTH);
            return;
        }
        max_decimal = max_decimal.max(arg_tp.decimal());
    }
    result_field_type.set_decimal_under_limit(max_decimal);
}

/// Go `types.IsString`.
fn is_string(tp: FieldTypeCode) -> bool {
    matches!(
        tp,
        FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Unspecified
    )
}

/// Go `types.IsBinaryStr`.
fn is_binary_str(ft: &FieldType) -> bool {
    ft.collation_name() == COLLATION_BIN && is_string(ft.code())
}

/// Go `types.IsNonBinaryStr`.
fn is_non_binary_str(ft: &FieldType) -> bool {
    ft.collation_name() != COLLATION_BIN && is_string(ft.code())
}

/// Go `addCollateAndCharsetAndFlagFromArgs`.
///
/// # Panics
///
/// As Go does, for a function that is not a control function or is handed
/// the wrong number of arguments.
fn add_collate_and_charset_and_flag_from_args(
    ctx: &dyn BuildContext,
    func_name: &str,
    eval_type: EvalType,
    result_field_type: &mut FieldType,
    args: &[Expression],
) -> Result<(), EvalError> {
    let connection = ctx.connection_charset_info();
    let derive = |result_field_type: &mut FieldType| -> Result<(), EvalError> {
        let ec = check_and_derive_collation_from_exprs_with_connection(
            func_name, eval_type, args, connection,
        )?;
        result_field_type.set_collation_name(ec.collation);
        result_field_type.set_charset_name(ec.charset);
        Ok(())
    };
    match func_name {
        "if" | "ifnull" | "lead" | "lag" => {
            assert_eq!(args.len(), 2, "unexpected length of args for if/ifnull/lead/lag");
            let (lhs, rhs) = (type_of(&args[0]), type_of(&args[1]));
            if is_non_binary_str(&lhs) && !is_binary_str(&rhs) {
                derive(result_field_type)?;
                result_field_type.set_flag(0);
                if lhs.has_flag(FieldTypeFlags::BINARY) || !is_non_binary_str(&rhs) {
                    result_field_type.add_flags(FieldTypeFlags::BINARY);
                }
            } else if is_non_binary_str(&rhs) && !is_binary_str(&lhs) {
                derive(result_field_type)?;
                result_field_type.set_flag(0);
                if rhs.has_flag(FieldTypeFlags::BINARY) || !is_non_binary_str(&lhs) {
                    result_field_type.add_flags(FieldTypeFlags::BINARY);
                }
            } else if is_binary_str(&lhs) || is_binary_str(&rhs) || !eval_type.is_string_kind() {
                set_bin_chs_cln_flag(result_field_type);
            } else {
                result_field_type.set_charset_name(DEFAULT_CHARSET);
                result_field_type.set_collation_name(DEFAULT_COLLATION_NAME);
                result_field_type.set_flag(0);
            }
        }
        "case" => {
            assert!(!args.is_empty(), "unexpected length 0 of args for casewhen");
            derive(result_field_type)?;
            if args.iter().map(type_of).any(|tp| {
                tp.has_flag(FieldTypeFlags::BINARY) || !is_non_binary_str(&tp)
            }) {
                result_field_type.add_flags(FieldTypeFlags::BINARY);
            }
        }
        "coalesce" => {
            let arg_types: Vec<FieldType> = args.iter().map(type_of).collect();
            let non_binary_str_exist = arg_types.iter().any(is_non_binary_str);
            let binary_str_exist = arg_types.iter().any(is_binary_str);
            if !binary_str_exist && non_binary_str_exist {
                derive(result_field_type)?;
                result_field_type.set_flag(0);
                if arg_types.iter().any(|arg_type| !is_string(arg_type.code())) {
                    result_field_type.add_flags(FieldTypeFlags::BINARY);
                }
            } else if binary_str_exist || !eval_type.is_string_kind() {
                set_bin_chs_cln_flag(result_field_type);
            } else {
                result_field_type.set_charset_name(DEFAULT_CHARSET);
                result_field_type.set_collation_name(DEFAULT_COLLATION_NAME);
                result_field_type.set_flag(0);
            }
        }
        _ => panic!("unexpected function: {func_name}"),
    }
    Ok(())
}

/// Go `types.TryToFixFlenOfDatetime`.
fn try_to_fix_flen_of_datetime(result_tp: &mut FieldType) {
    if result_tp.code() == FieldTypeCode::Datetime {
        result_tp.set_flen(MAX_DATETIME_WIDTH_NO_FSP);
        if result_tp.decimal() > 0 {
            result_tp.set_flen(result_tp.flen() + result_tp.decimal() + 1);
        }
    }
}

/// Go `InferType4ControlFuncs`: the result type of `IF`, `IFNULL`, `NULLIF`,
/// `CASE`, `COALESCE`, `LEAD` and `LAG`.
///
/// # Panics
///
/// As Go does, when `args` is empty.
pub(crate) fn infer_type_4_control_funcs(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: &[Expression],
) -> Result<FieldType, EvalError> {
    let args_num = args.len();
    assert!(args_num != 0, "unexpected length 0 of args");
    let (null_fields, not_null_fields): (Vec<FieldType>, Vec<FieldType>) = args
        .iter()
        .map(type_of)
        .partition(|tp| tp.code() == FieldTypeCode::Null);
    if null_fields.len() == args_num {
        // Every field is TypeNull.
        let mut result_field_type = null_fields[0].clone();
        result_field_type.del_flag(FieldTypeFlags::NOT_NULL);
        result_field_type.set_code(FieldTypeCode::Null);
        result_field_type.set_flen(0);
        result_field_type.set_decimal(0);
        set_bin_chs_cln_flag(&mut result_field_type);
        return Ok(result_field_type);
    }
    let mut result_field_type = if not_null_fields.len() == 1 {
        not_null_fields[0].clone()
    } else {
        let mut result_field_type = agg_field_type(&not_null_fields);
        let mut temp_flag = 0;
        let eval_type = aggregate_eval_type(&not_null_fields, &mut temp_flag);
        result_field_type.set_flag(temp_flag);
        set_decimal_from_args(eval_type, &mut result_field_type, &not_null_fields);
        add_collate_and_charset_and_flag_from_args(
            ctx,
            func_name,
            eval_type,
            &mut result_field_type,
            args,
        )?;
        set_flen_from_args(eval_type, &mut result_field_type, &not_null_fields);
        result_field_type
    };
    // A NULL argument unsets the result's NotNullFlag.
    if !null_fields.is_empty() {
        result_field_type.del_flag(FieldTypeFlags::NOT_NULL);
    }
    let result_eval_type = result_field_type.eval_type();
    // Fix the decimal for int and string.
    if result_eval_type == EvalType::Int {
        result_field_type.set_decimal(0);
    } else if result_eval_type == EvalType::String {
        result_field_type.set_decimal(UNSPECIFIED_LENGTH);
    }
    // Fix the type for enum and set.
    if matches!(result_field_type.code(), FieldTypeCode::Enum | FieldTypeCode::Set) {
        match result_eval_type {
            EvalType::Int => result_field_type.set_code(FieldTypeCode::LongLong),
            EvalType::String => result_field_type.set_code(FieldTypeCode::Varchar),
            _ => {}
        }
    }
    // Fix the flen for datetime.
    try_to_fix_flen_of_datetime(&mut result_field_type);
    Ok(result_field_type)
}

/// Go `caseWhenFunctionClass`.
pub(crate) struct CaseWhenFunctionClass;

impl FunctionClass for CaseWhenFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        mut args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let l = args.len();
        // Fill in each `THEN` clause parameter type.
        let mut then_args: Vec<Expression> = args.iter().skip(1).step_by(2).cloned().collect();
        if l % 2 == 1 {
            then_args.push(args[l - 1].clone());
        }
        let mut field_tp = infer_type_4_control_funcs(ctx, "case", &then_args)?;
        // The NotNullFlag is off: when every WHEN clause is false, the
        // result of the CASE is NULL.
        field_tp.del_flag(FieldTypeFlags::NOT_NULL);
        let tp = field_tp.eval_type();

        let mut arg_tps = Vec::with_capacity(l);
        for i in (0..l - 1).step_by(2) {
            let condition = std::mem::replace(&mut args[i], Expression::Constant(Default::default()));
            args[i] = wrap_with_is_true(ctx, true, condition, false)?;
            arg_tps.push(type_of(&args[i]));
            arg_tps.push(field_tp.clone());
        }
        if l % 2 == 1 {
            arg_tps.push(field_tp.clone());
        }
        let mut bf = new_base_builtin_func_with_field_types(ctx, "case", args, tp, &arg_tps)?;
        field_tp.set_charset_name(bf.tp.charset_name().to_owned());
        field_tp.set_collation_name(bf.tp.collation_name().to_owned());
        bf.tp = field_tp;
        bf.pb_code = match tp {
            EvalType::Int => {
                bf.tp.set_decimal(0);
                ScalarFuncSig::CaseWhenInt
            }
            EvalType::Real => ScalarFuncSig::CaseWhenReal,
            EvalType::Decimal => ScalarFuncSig::CaseWhenDecimal,
            EvalType::String => {
                bf.tp.set_decimal(UNSPECIFIED_LENGTH);
                ScalarFuncSig::CaseWhenString
            }
            EvalType::Datetime | EvalType::Timestamp => ScalarFuncSig::CaseWhenTime,
            EvalType::Duration => ScalarFuncSig::CaseWhenDuration,
            EvalType::Json => ScalarFuncSig::CaseWhenJson,
            // Go sets no pb code for `builtinCaseWhenVectorFloat32Sig`.
            EvalType::VectorFloat32 => ScalarFuncSig::Unspecified,
        };
        Ok(bf)
    }
}

/// Go `ifFunctionClass`.
pub(crate) struct IfFunctionClass;

impl FunctionClass for IfFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        mut args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut ret_tp = infer_type_4_control_funcs(ctx, "if", &args[1..])?;
        let eval_tps = ret_tp.eval_type();
        let condition = std::mem::replace(&mut args[0], Expression::Constant(Default::default()));
        args[0] = wrap_with_is_true(ctx, true, condition, false)?;
        let arg_tps = [type_of(&args[0]), ret_tp.clone(), ret_tp.clone()];
        let bf = new_base_builtin_func_with_field_types(ctx, "if", args, eval_tps, &arg_tps)?;
        ret_tp.add_flags(bf.tp.flags());
        Ok(BaseBuiltinFunc {
            tp: ret_tp,
            pb_code: match eval_tps {
                EvalType::Int => ScalarFuncSig::IfInt,
                EvalType::Real => ScalarFuncSig::IfReal,
                EvalType::Decimal => ScalarFuncSig::IfDecimal,
                EvalType::String => ScalarFuncSig::IfString,
                EvalType::Datetime | EvalType::Timestamp => ScalarFuncSig::IfTime,
                EvalType::Duration => ScalarFuncSig::IfDuration,
                EvalType::Json => ScalarFuncSig::IfJson,
                // Go sets no pb code for `builtinIfVectorFloat32Sig`.
                EvalType::VectorFloat32 => ScalarFuncSig::Unspecified,
            },
            ..bf
        })
    }
}

/// Go `ifNullFunctionClass`.
pub(crate) struct IfNullFunctionClass;

impl FunctionClass for IfNullFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let (lhs, rhs) = (type_of(&args[0]), type_of(&args[1]));
        let mut ret_tp = infer_type_4_control_funcs(ctx, "ifnull", &args)?;
        ret_tp.add_flags((lhs.flags() | rhs.flags()) & FieldTypeFlags::NOT_NULL);
        if lhs.code() == FieldTypeCode::Null && rhs.code() == FieldTypeCode::Null {
            ret_tp.set_code(FieldTypeCode::Null);
            ret_tp.set_flen(0);
            ret_tp.set_decimal(0);
            set_bin_chs_cln_flag(&mut ret_tp);
        }
        let eval_tps = ret_tp.eval_type();
        let arg_tps = [ret_tp.clone(), ret_tp.clone()];
        let bf = new_base_builtin_func_with_field_types(ctx, "ifnull", args, eval_tps, &arg_tps)?;
        Ok(BaseBuiltinFunc {
            tp: ret_tp,
            pb_code: match eval_tps {
                EvalType::Int => ScalarFuncSig::IfNullInt,
                EvalType::Real => ScalarFuncSig::IfNullReal,
                EvalType::Decimal => ScalarFuncSig::IfNullDecimal,
                EvalType::String => ScalarFuncSig::IfNullString,
                EvalType::Datetime | EvalType::Timestamp => ScalarFuncSig::IfNullTime,
                EvalType::Duration => ScalarFuncSig::IfNullDuration,
                EvalType::Json => ScalarFuncSig::IfNullJson,
                // Go sets no pb code for `builtinIfNullVectorFloat32Sig`.
                EvalType::VectorFloat32 => ScalarFuncSig::Unspecified,
            },
            ..bf
        })
    }
}
