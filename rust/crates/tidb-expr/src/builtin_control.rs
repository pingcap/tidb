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
pub(crate) fn is_binary_str(ft: &FieldType) -> bool {
    ft.collation_name() == COLLATION_BIN && is_string(ft.code())
}

/// Go `types.IsNonBinaryStr`.
pub(crate) fn is_non_binary_str(ft: &FieldType) -> bool {
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
pub fn infer_type_4_control_funcs(
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
            EvalType::VectorFloat32 => {
                bf.kernel = Some(crate::scalar_function::Kernel::Case(EvalType::VectorFloat32));
                ScalarFuncSig::Unspecified
            }
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
            kernel: (eval_tps == EvalType::VectorFloat32)
                .then_some(crate::scalar_function::Kernel::If(EvalType::VectorFloat32)),
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
            kernel: (eval_tps == EvalType::VectorFloat32)
                .then_some(crate::scalar_function::Kernel::IfNull(EvalType::VectorFloat32)),
            ..bf
        })
    }
}

#[cfg(test)]
mod infer_type_4_control_funcs_tests {
    use super::*;
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags, UNSPECIFIED_LENGTH};

    /// The inference over `args` as a function class runs it.
    fn infer_type_4_control_funcs(name: &str, args: &[Expression]) -> Option<FieldType> {
        super::infer_type_4_control_funcs(&crate::builtin::UnfoldedBuildContext, name, args).ok()
    }
    use crate::constant::Constant;
    use tidb_datatype::Datum;

    fn arg(code: FieldTypeCode, flen: i64, decimal: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(decimal);
        Expression::Constant(Constant::new(Datum::Null, ft))
    }

    fn typed(code: FieldTypeCode) -> Expression {
        arg(code, UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH)
    }

    /// The measured Rank-A case: a string branch beside an integer one is a
    /// STRING result, not an integer one. Go, via `goeval`:
    /// `IF(1,1,'a')` -> `STR:1`.
    #[test]
    fn an_integer_branch_beside_a_string_one_is_a_string_result() {
        let ft = infer_type_4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::LongLong, 1, 0),
                arg(FieldTypeCode::VarString, 1, UNSPECIFIED_LENGTH),
            ],
        )
        .expect("both branches are typed");
        assert_eq!(ft.eval_type(), EvalType::String);
        // The ETString arm of `setFlenFromArgs`: the LONGLONG branch is 20
        // wide whatever display width it declared.
        assert_eq!(ft.flen(), 20);
        assert_eq!(ft.decimal(), UNSPECIFIED_LENGTH);
    }

    /// Go's `len(notNullFields) == 1` shortcut copies the ONE typed branch
    /// whole -- no re-derivation at all.
    ///
    /// The width is the boundary that shows it: an UNSIGNED `int(10)` spends
    /// no digit on a sign, so running the merge over a one-element list
    /// instead would put one back (`setFlenFromArgs` re-adds a sign digit
    /// unconditionally) and report `int(11)`. A SIGNED branch, or a DECIMAL
    /// one, round-trips through the merge unchanged and would agree either
    /// way -- which is why this case is unsigned.
    #[test]
    fn one_typed_branch_beside_nulls_is_that_branch_exactly() {
        let mut unsigned = FieldType::new(FieldTypeCode::Long);
        unsigned.set_flen(10);
        unsigned.set_decimal(0);
        unsigned.add_flags(FieldTypeFlags::UNSIGNED);
        let ft = infer_type_4_control_funcs(
            "ifnull",
            &[
                typed(FieldTypeCode::Null),
                Expression::Constant(Constant::new(Datum::Null, unsigned)),
            ],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Long);
        assert_eq!((ft.flen(), ft.decimal()), (10, 0));
    }

    // Go TestIssue46475: COALESCE(NULL, DATE, NULL) must preserve the lone
    // typed branch as DATE instead of aggregating the NULL branches into a
    // string result.
    #[test]
    fn test_issue_46475() {
        let ft = infer_type_4_control_funcs(
            "coalesce",
            &[
                typed(FieldTypeCode::Null),
                typed(FieldTypeCode::Date),
                typed(FieldTypeCode::Null),
            ],
        )
        .expect("all COALESCE branches have static types");
        assert_eq!(ft.code(), FieldTypeCode::Date);
    }

    /// The ENUM/SET rewrite's ONLY reachable path: one enum branch beside a
    /// NULL one, where `AggFieldType` never runs. Recorded witness:
    /// `tests/integrationtest/r/expression/misc.result:911` runs
    /// `select if(A, null,b)=1 from t` over `b enum("b")`.
    #[test]
    fn a_lone_enum_branch_reports_the_varchar_its_signature_returns() {
        let ft = infer_type_4_control_funcs(
            "if",
            &[typed(FieldTypeCode::Null), arg(FieldTypeCode::Enum, 1, 0)],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Varchar);
    }

    /// `IF` never re-adds NOT NULL: `newReturnFieldTypeForBaseBuiltinFunc`
    /// gives `bf.tp` only `mysql.BinaryFlag`, so two NOT NULL branches still
    /// make a nullable result -- if all when-clauses are false there is
    /// nothing to return.
    ///
    /// This is also the boundary for Go's `var tempFlag uint`: starting that
    /// mask at the MERGED flags instead of at zero would let `AggFieldType`'s
    /// `mergeTypeFlag` carry NOT NULL through, and `IF` has no tail to undo
    /// it. `IFNULL` and `COALESCE` put NOT NULL back in their own
    /// `getFunction` tails, not here.
    #[test]
    fn two_not_null_branches_do_not_make_an_if_not_null() {
        let not_null = |code| {
            let mut ft = FieldType::new(code);
            ft.set_flen(1);
            ft.set_decimal(0);
            ft.add_flags(FieldTypeFlags::NOT_NULL);
            Expression::Constant(Constant::new(Datum::Null, ft))
        };
        let branches = [
            not_null(FieldTypeCode::LongLong),
            not_null(FieldTypeCode::LongLong),
        ];
        let ft = infer_type_4_control_funcs("if", &branches).expect("typed");
        assert!(!ft.has_flag(FieldTypeFlags::NOT_NULL));
    }

    /// `setFlenFromArgs` branches on `AggregateEvalType`'s answer, NOT on the
    /// merged FIELD type's eval type, and the two genuinely differ: every
    /// TEMPORAL eval type is `IsStringKind`, so a DURATION pair merges to a
    /// DURATION field type whose `AggregateEvalType` is `ETString`.
    ///
    /// The ETString arm gives up on the width as soon as one branch has none;
    /// the trailing arm this would otherwise take just maxes the widths and
    /// reports the other branch's. Reading the eval type off the merged type
    /// would agree on every NUMERIC pair, which is why the boundary is
    /// temporal.
    #[test]
    fn the_width_arm_is_chosen_by_the_aggregate_eval_type() {
        let ft = infer_type_4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::Duration, 10, 0),
                arg(FieldTypeCode::Duration, UNSPECIFIED_LENGTH, 0),
            ],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Duration);
        assert_eq!(ft.flen(), UNSPECIFIED_LENGTH);
    }

    /// Every branch NULL: a NULL column, width and scale zeroed.
    #[test]
    fn every_branch_null_is_a_null_column() {
        let ft = infer_type_4_control_funcs(
            "ifnull",
            &[typed(FieldTypeCode::Null), typed(FieldTypeCode::Null)],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Null);
        assert_eq!((ft.flen(), ft.decimal()), (0, 0));
        assert_eq!(ft.charset_name(), "binary");
    }

    /// An ENUM pair merges to a STRING eval type, and Go then REWRITES the
    /// reported type to VARCHAR because `builtinIfStringSig` is what runs.
    #[test]
    fn an_enum_pair_reports_the_varchar_its_signature_returns() {
        let ft = infer_type_4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::Enum, 1, 0),
                arg(FieldTypeCode::Enum, 1, 0),
            ],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::Varchar);
        assert_eq!(ft.decimal(), UNSPECIFIED_LENGTH);
    }

    /// `setDecimalFromArgs` widens the scale to the widest branch, which is
    /// what makes `IFNULL(1, 1.5)` print `1.0` and not `1`. Go, via `gorun`:
    /// `RS:1|1.0|1.0|1|1|1`.
    #[test]
    fn a_decimal_branch_widens_the_integer_branchs_scale() {
        let ft = infer_type_4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::LongLong, 1, 0),
                arg(FieldTypeCode::NewDecimal, 2, 1),
            ],
        )
        .expect("typed");
        assert_eq!(ft.code(), FieldTypeCode::NewDecimal);
        // `setFlenFromArgs`' decimal arm: each branch's INTEGRAL width is its
        // flen less one sign digit less its own scale, so both branches here
        // contribute 0, and the merged scale plus one sign digit are added
        // back -- `0 + 1 + 1`.
        assert_eq!((ft.flen(), ft.decimal()), (2, 1));
    }

    /// A DATETIME result takes its canonical width, never a branch's.
    #[test]
    fn a_datetime_result_takes_its_canonical_width() {
        let ft = infer_type_4_control_funcs(
            "ifnull",
            &[
                arg(FieldTypeCode::Datetime, 3, 0),
                arg(FieldTypeCode::Datetime, 3, 0),
            ],
        )
        .expect("typed");
        assert_eq!((ft.code(), ft.flen()), (FieldTypeCode::Datetime, 19));
    }
}
