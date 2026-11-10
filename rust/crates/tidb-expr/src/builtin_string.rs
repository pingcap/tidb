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

//! `pkg/expression/builtin_string.go`: the string function classes.
//!
//! Go reads `args[i].GetType()` after `newBaseBuiltinFuncWithTp` cast the
//! slice in place, so every class here reads `bf.args[i]` unless Go reads
//! the type before the build.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass, SigFields,
    WeightStringPadding,
};
use crate::builtin_control::{is_binary_str, is_non_binary_str};
use crate::builtin_miscellaneous::{function_class, set_connection_charset};
use crate::expr_collation::{Coercibility, Repertoire};
use crate::expression::Expression;
use crate::wrap_cast::{set_bin_chs_cln_flag, type_of, MAX_BLOB_WIDTH};
use crate::EvalError;
use tidb_datatype::{
    Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags, UNSPECIFIED_LENGTH,
};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `SetBinFlagOrBinStr`.
pub(crate) fn set_bin_flag_or_bin_str(arg_tp: &FieldType, res_tp: &mut FieldType) {
    let non_enum_or_set = !matches!(arg_tp.code(), FieldTypeCode::Enum | FieldTypeCode::Set);
    if is_binary_str(arg_tp) {
        set_bin_chs_cln_flag(res_tp);
    } else if arg_tp.has_flag(FieldTypeFlags::BINARY)
        || (!is_non_binary_str(arg_tp) && non_enum_or_set)
    {
        res_tp.add_flags(FieldTypeFlags::BINARY);
    }
}

/// Go `addBinFlag`.
fn add_bin_flag(tp: &mut FieldType) {
    let arg_tp = tp.clone();
    set_bin_flag_or_bin_str(&arg_tp, tp);
}

/// Go `Constant.EvalInt` of an argument `newBaseBuiltinFuncWithTp` cast to
/// an integer: `None` is Go's `isNull || err != nil`.
fn eval_int_constant(ctx: &dyn BuildContext, arg: &Expression) -> Option<i64> {
    match ctx.eval_constant(arg) {
        Ok(Datum::Int(value)) => Some(value),
        Ok(Datum::UInt(value)) => Some(value as i64),
        _ => None,
    }
}

/// A class whose `getFunction` is written out: the struct, and the trait
/// header around the body.
macro_rules! string_class {
    ($(#[$doc:meta])* $class:ident, |$name:ident, $ctx:ident, $args:ident| $body:block) => {
        $(#[$doc])*
        pub(crate) struct $class {
            pub(crate) func_name: &'static str,
        }

        impl FunctionClass for $class {
            fn get_function(
                &self,
                $ctx: &dyn BuildContext,
                $args: Vec<Expression>,
            ) -> Result<BaseBuiltinFunc, EvalError> {
                let $name = self.func_name;
                $body
            }
        }
    };
}
pub(crate) use string_class;

function_class!(
    /// Go `lengthFunctionClass`.
    LengthFunctionClass, Int, [String], Length, |bf, _ctx| {
        bf.tp.set_flen(10);
    }
);

function_class!(
    /// Go `asciiFunctionClass`.
    AsciiFunctionClass, Int, [String], Ascii, |bf, _ctx| {
        bf.tp.set_flen(3);
    }
);

string_class!(
    /// Go `concatFunctionClass`.
    ConcatFunctionClass, |func_name, ctx, args| {
        let arg_tps = vec![EvalType::String; args.len()];
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
        add_bin_flag(&mut bf.tp);
        bf.tp.set_flen(0);
        for i in 0..bf.args.len() {
            let arg_flen = type_of(&bf.args[i]).flen();
            if arg_flen < 0 {
                bf.tp.set_flen(MAX_BLOB_WIDTH);
            }
            bf.tp.set_flen(bf.tp.flen() + arg_flen);
        }
        if bf.tp.flen() >= MAX_BLOB_WIDTH {
            bf.tp.set_flen(MAX_BLOB_WIDTH);
        }
        bf.pb_code = ScalarFuncSig::Concat;
        Ok(bf)
    }
);

string_class!(
    /// Go `concatWSFunctionClass`.
    ConcatWSFunctionClass, |func_name, ctx, args| {
        let arg_tps = vec![EvalType::String; args.len()];
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
        bf.tp.set_flen(0);
        add_bin_flag(&mut bf.tp);
        // skip separator param
        for i in 1..bf.args.len() {
            let arg_flen = type_of(&bf.args[i]).flen();
            if arg_flen < 0 {
                bf.tp.set_flen(MAX_BLOB_WIDTH);
            }
            bf.tp.set_flen(bf.tp.flen() + arg_flen);
        }
        // add separator
        let seps_len = bf.args.len() as i64 - 2;
        bf.tp
            .set_flen(bf.tp.flen() + seps_len * type_of(&bf.args[0]).flen());
        if bf.tp.flen() >= MAX_BLOB_WIDTH {
            bf.tp.set_flen(MAX_BLOB_WIDTH);
        }
        bf.pb_code = ScalarFuncSig::ConcatWs;
        Ok(bf)
    }
);

/// The classes whose result is as wide as the first argument and is binary
/// when it is: `sig` for a binary first argument, `utf8_sig` otherwise.
fn first_arg_shaped(
    ctx: &dyn BuildContext,
    func_name: &'static str,
    args: Vec<Expression>,
    arg_tps: &[EvalType],
    sig: ScalarFuncSig,
    utf8_sig: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, arg_tps)?;
    let arg_type = type_of(&bf.args[0]);
    bf.tp.set_flen(arg_type.flen());
    set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
    bf.pb_code = if is_binary_str(&arg_type) {
        sig
    } else {
        utf8_sig
    };
    Ok(bf)
}

string_class!(
    /// Go `leftFunctionClass`.
    LeftFunctionClass, |func_name, ctx, args| {
        first_arg_shaped(
            ctx,
            func_name,
            args,
            &[EvalType::String, EvalType::Int],
            ScalarFuncSig::Left,
            ScalarFuncSig::LeftUtf8,
        )
    }
);

string_class!(
    /// Go `rightFunctionClass`.
    RightFunctionClass, |func_name, ctx, args| {
        first_arg_shaped(
            ctx,
            func_name,
            args,
            &[EvalType::String, EvalType::Int],
            ScalarFuncSig::Right,
            ScalarFuncSig::RightUtf8,
        )
    }
);

function_class!(
    /// Go `repeatFunctionClass`.
    RepeatFunctionClass, String, [String, Int], Repeat, |bf, _ctx| {
        bf.tp.set_flen(MAX_BLOB_WIDTH);
        let arg_type = type_of(&bf.args[0]);
        set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
    }
);

string_class!(
    /// Go `lowerFunctionClass`.
    LowerFunctionClass, |func_name, ctx, args| {
        first_arg_shaped(
            ctx,
            func_name,
            args,
            &[EvalType::String],
            ScalarFuncSig::Lower,
            ScalarFuncSig::LowerUtf8,
        )
    }
);

string_class!(
    /// Go `reverseFunctionClass`.
    ReverseFunctionClass, |func_name, ctx, args| {
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::String,
            &[EvalType::String],
        )?;
        let arg_tp = type_of(&bf.args[0]);
        bf.tp.set_flen(arg_tp.flen());
        add_bin_flag(&mut bf.tp);
        bf.pb_code = if is_binary_str(&arg_tp) || arg_tp.code() == FieldTypeCode::Bit {
            ScalarFuncSig::Reverse
        } else {
            ScalarFuncSig::ReverseUtf8
        };
        Ok(bf)
    }
);

function_class!(
    /// Go `spaceFunctionClass`.
    SpaceFunctionClass, String, [Int], Space, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(MAX_BLOB_WIDTH);
    }
);

string_class!(
    /// Go `upperFunctionClass`.
    UpperFunctionClass, |func_name, ctx, args| {
        first_arg_shaped(
            ctx,
            func_name,
            args,
            &[EvalType::String],
            ScalarFuncSig::Upper,
            ScalarFuncSig::UpperUtf8,
        )
    }
);

function_class!(
    /// Go `strcmpFunctionClass`.
    StrcmpFunctionClass, Int, [String, String], Strcmp, |bf, _ctx| {
        bf.tp.set_flen(2);
        set_bin_chs_cln_flag(&mut bf.tp);
    }
);

/// Go `replaceFunctionClass.fixLength`.
fn fix_length(args: &[Expression]) -> i64 {
    let mut char_len = type_of(&args[0]).flen();
    let old_str_len = type_of(&args[1]).flen();
    let diff = type_of(&args[2]).flen() - old_str_len;
    if diff > 0 && old_str_len > 0 {
        char_len += (char_len / old_str_len) * diff;
    }
    char_len
}

function_class!(
    /// Go `replaceFunctionClass`.
    ReplaceFunctionClass, String, [String, String, String], Replace, |bf, _ctx| {
        bf.tp.set_flen(fix_length(&bf.args));
        for i in 0..bf.args.len() {
            let arg_type = type_of(&bf.args[i]);
            set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
        }
    }
);

string_class!(
    /// Go `convertFunctionClass`.
    ConvertFunctionClass, |func_name, ctx, args| {
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::String,
            &[EvalType::String, EvalType::String],
        )?;
        // `args[1]` is limited by parser to be a constant string.
        let Expression::Constant(charset_arg) = &bf.args[1] else {
            return Err(EvalError::Conversion(
                crate::expression_errors::ERR_INCORRECT_ARGS
                    .gen_with_stack_by_args(&["charset".into()]),
            ));
        };
        let transcoding_name = charset_arg.value.sql_string().unwrap_or_default();
        let charset = transcoding_name.to_lowercase();
        bf.tp.set_charset_name(&charset);
        // The string has the default collation for the character set.
        let Ok(collation) = tidb_charset::charset::get_default_collation(&charset) else {
            return Err(EvalError::Conversion(
                crate::expression_errors::ERR_UNKNOWN_CHARACTER_SET
                    .gen_with_stack_by_args(&[transcoding_name.into()]),
            ));
        };
        bf.tp.set_collation_name(&collation);
        // convert function should always derive to CoercibilityImplicit
        bf.collation.set_coercibility(Coercibility::IMPLICIT);
        bf.collation.set_repertoire(if charset == "ascii" {
            Repertoire::ASCII
        } else {
            Repertoire::UNICODE
        });
        // Result will be a binary string if converts charset to BINARY.
        if is_binary_str(&bf.tp) {
            set_bin_chs_cln_flag(&mut bf.tp);
        } else {
            bf.tp.del_flag(FieldTypeFlags::BINARY);
        }
        bf.tp.set_flen(MAX_BLOB_WIDTH);
        bf.pb_code = ScalarFuncSig::Convert;
        Ok(bf)
    }
);

string_class!(
    /// Go `substringFunctionClass`.
    SubstringFunctionClass, |func_name, ctx, args| {
        let (arg_tps, sig, utf8_sig): (&[EvalType], _, _) = if args.len() == 3 {
            (
                &[EvalType::String, EvalType::Int, EvalType::Int],
                ScalarFuncSig::Substring3Args,
                ScalarFuncSig::Substring3ArgsUtf8,
            )
        } else {
            (
                &[EvalType::String, EvalType::Int],
                ScalarFuncSig::Substring2Args,
                ScalarFuncSig::Substring2ArgsUtf8,
            )
        };
        first_arg_shaped(ctx, func_name, args, arg_tps, sig, utf8_sig)
    }
);

function_class!(
    /// Go `substringIndexFunctionClass`.
    SubstringIndexFunctionClass, String, [String, String, Int], SubstringIndex, |bf, _ctx| {
        let arg_type = type_of(&bf.args[0]);
        bf.tp.set_flen(arg_type.flen());
        set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
    }
);

/// Go's `bf.collation == charset.CollationBin`.
fn use_binary(bf: &BaseBuiltinFunc) -> bool {
    bf.collation.charset_and_collation().1 == "binary"
}

string_class!(
    /// Go `locateFunctionClass`.
    LocateFunctionClass, |func_name, ctx, args| {
        let has_start_pos = args.len() == 3;
        let arg_tps: &[EvalType] = if has_start_pos {
            &[EvalType::String, EvalType::String, EvalType::Int]
        } else {
            &[EvalType::String, EvalType::String]
        };
        let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Int, arg_tps)?;
        // Locate is multibyte safe.
        bf.pb_code = match (has_start_pos, use_binary(&bf)) {
            (true, true) => ScalarFuncSig::Locate3Args,
            (true, false) => ScalarFuncSig::Locate3ArgsUtf8,
            (false, true) => ScalarFuncSig::Locate2Args,
            (false, false) => ScalarFuncSig::Locate2ArgsUtf8,
        };
        Ok(bf)
    }
);

string_class!(
    /// Go `hexFunctionClass`.
    HexFunctionClass, |func_name, ctx, args| {
        match type_of(&args[0]).eval_type() {
            EvalType::String
            | EvalType::Datetime
            | EvalType::Timestamp
            | EvalType::Duration
            | EvalType::Json => {
                let mut bf = new_base_builtin_func_with_tp(
                    ctx,
                    func_name,
                    args,
                    EvalType::String,
                    &[EvalType::String],
                )?;
                bf.tp.set_flen(UNSPECIFIED_LENGTH);
                let arg_len = type_of(&bf.args[0]).flen();
                // Use UTF8MB4 as default.
                if arg_len != UNSPECIFIED_LENGTH {
                    bf.tp.set_flen(arg_len * 4 * 2);
                }
                bf.pb_code = ScalarFuncSig::HexStrArg;
                Ok(bf)
            }
            EvalType::Int | EvalType::Real | EvalType::Decimal => {
                let mut bf = new_base_builtin_func_with_tp(
                    ctx,
                    func_name,
                    args,
                    EvalType::String,
                    &[EvalType::Int],
                )?;
                bf.tp.set_flen(UNSPECIFIED_LENGTH);
                let arg_len = type_of(&bf.args[0]).flen();
                if arg_len != UNSPECIFIED_LENGTH {
                    bf.tp.set_flen(arg_len * 2);
                }
                set_connection_charset(&mut bf, ctx);
                bf.pb_code = ScalarFuncSig::HexIntArg;
                Ok(bf)
            }
            _ => Err(EvalError::Unsupported(
                "Hex invalid args, need int or string",
            )),
        }
    }
);

string_class!(
    /// Go `unhexFunctionClass`.
    UnhexFunctionClass, |func_name, ctx, args| {
        let arg_type = type_of(&args[0]);
        let mut ret_flen = match arg_type.eval_type() {
            // Use UTF8MB4 as default charset, so there're (flen * 4 + 1) / 2 byte-pairs.
            EvalType::String
            | EvalType::Datetime
            | EvalType::Timestamp
            | EvalType::Duration
            | EvalType::Json => (arg_type.flen() * 4 + 1) / 2,
            // For number value, there're (flen + 1) / 2 byte-pairs.
            EvalType::Int | EvalType::Real | EvalType::Decimal => (arg_type.flen() + 1) / 2,
            _ => {
                return Err(EvalError::Unsupported(
                    "Unhex invalid args, need int or string",
                ))
            }
        };
        if arg_type.flen() == UNSPECIFIED_LENGTH {
            ret_flen = UNSPECIFIED_LENGTH;
        }
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::String,
            &[EvalType::String],
        )?;
        bf.tp.set_flen(ret_flen);
        set_bin_chs_cln_flag(&mut bf.tp);
        bf.pb_code = ScalarFuncSig::UnHex;
        Ok(bf)
    }
);

string_class!(
    /// Go `trimFunctionClass`.
    TrimFunctionClass, |func_name, ctx, args| {
        let (arg_tps, sig): (&[EvalType], _) = match args.len() {
            1 => (&[EvalType::String], ScalarFuncSig::Trim1Arg),
            2 => (&[EvalType::String, EvalType::String], ScalarFuncSig::Trim2Args),
            3 => (
                &[EvalType::String, EvalType::String, EvalType::Int],
                ScalarFuncSig::Trim3Args,
            ),
            _ => return Err(EvalError::WrongParameterCount(func_name)),
        };
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, arg_tps)?;
        let arg_type = type_of(&bf.args[0]);
        // Go's two-argument arm sets no flen.
        if bf.args.len() != 2 {
            bf.tp.set_flen(arg_type.flen());
        }
        set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
        bf.pb_code = sig;
        Ok(bf)
    }
);

function_class!(
    /// Go `lTrimFunctionClass`.
    LTrimFunctionClass, String, [String], LTrim, |bf, _ctx| {
        let arg_type = type_of(&bf.args[0]);
        bf.tp.set_flen(arg_type.flen());
        set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
    }
);

function_class!(
    /// Go `rTrimFunctionClass`.
    RTrimFunctionClass, String, [String], RTrim, |bf, _ctx| {
        let arg_type = type_of(&bf.args[0]);
        bf.tp.set_flen(arg_type.flen());
        set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
    }
);

/// Go `getFlen4LpadAndRpad`.
fn get_flen_4_lpad_and_rpad(ctx: &dyn BuildContext, arg: &Expression) -> i64 {
    if !matches!(arg, Expression::Constant(_)) {
        return MAX_BLOB_WIDTH;
    }
    match eval_int_constant(ctx, arg) {
        Some(length) if length <= MAX_BLOB_WIDTH => length,
        _ => MAX_BLOB_WIDTH,
    }
}

/// Go `lpadFunctionClass.getFunction` and `rpadFunctionClass.getFunction`.
fn pad_function(
    ctx: &dyn BuildContext,
    func_name: &'static str,
    args: Vec<Expression>,
    sig: ScalarFuncSig,
    utf8_sig: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    let mut bf = new_base_builtin_func_with_tp(
        ctx,
        func_name,
        args,
        EvalType::String,
        &[EvalType::String, EvalType::Int, EvalType::String],
    )?;
    bf.tp.set_flen(get_flen_4_lpad_and_rpad(ctx, &bf.args[1]));
    add_bin_flag(&mut bf.tp);
    if is_binary_str(&type_of(&bf.args[0])) || is_binary_str(&type_of(&bf.args[2])) {
        bf.pb_code = sig;
        return Ok(bf);
    }
    bf.tp.set_flen(bf.tp.flen() * 4);
    if bf.tp.flen() > MAX_BLOB_WIDTH {
        bf.tp.set_flen(MAX_BLOB_WIDTH);
    }
    bf.pb_code = utf8_sig;
    Ok(bf)
}

string_class!(
    /// Go `lpadFunctionClass`.
    LpadFunctionClass, |func_name, ctx, args| {
        pad_function(ctx, func_name, args, ScalarFuncSig::Lpad, ScalarFuncSig::LpadUtf8)
    }
);

string_class!(
    /// Go `rpadFunctionClass`.
    RpadFunctionClass, |func_name, ctx, args| {
        pad_function(ctx, func_name, args, ScalarFuncSig::Rpad, ScalarFuncSig::RpadUtf8)
    }
);

function_class!(
    /// Go `bitLengthFunctionClass`.
    BitLengthFunctionClass, Int, [String], BitLength, |bf, _ctx| {
        bf.tp.set_flen(10);
    }
);

string_class!(
    /// Go `charFunctionClass`.
    CharFunctionClass, |func_name, ctx, args| {
        let mut arg_tps = vec![EvalType::Int; args.len() - 1];
        arg_tps.push(EvalType::String);
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
        // The last argument represents the charset name after "using".
        let last = bf.args.last().expect("char_func has its charset argument");
        if !matches!(last, Expression::Constant(_)) {
            return Err(EvalError::Conversion(
                crate::expression_errors::ERR_INCORRECT_ARGS.clone(),
            ));
        }
        match ctx.eval_constant(last)? {
            // Use the default charset binary if it is nil.
            Datum::Null => set_bin_chs_cln_flag(&mut bf.tp),
            charset_name => {
                let charset_name = charset_name.sql_string().unwrap_or_default();
                bf.tp.set_charset_name(&charset_name);
                let default_collate = tidb_charset::charset::get_default_collation(&charset_name)
                    .map_err(|err| {
                        EvalError::Unsupported(Box::leak(err.to_string().into_boxed_str()))
                    })?;
                bf.tp.set_collation_name(&default_collate);
            }
        }
        bf.tp.set_flen(4 * (bf.args.len() as i64 - 1));
        bf.pb_code = ScalarFuncSig::Char;
        Ok(bf)
    }
);

string_class!(
    /// Go `charLengthFunctionClass`.
    CharLengthFunctionClass, |func_name, ctx, args| {
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::Int,
            &[EvalType::String],
        )?;
        bf.pb_code = if is_binary_str(&type_of(&bf.args[0])) {
            ScalarFuncSig::CharLength
        } else {
            ScalarFuncSig::CharLengthUtf8
        };
        Ok(bf)
    }
);

function_class!(
    /// Go `findInSetFunctionClass`.
    FindInSetFunctionClass, Int, [String, String], FindInSet, |bf, _ctx| {
        bf.tp.set_flen(3);
    }
);

string_class!(
    /// Go `fieldFunctionClass`.
    FieldFunctionClass, |func_name, ctx, args| {
        let (mut is_all_string, mut is_all_number) = (true, true);
        for arg in &args {
            let arg_tp = type_of(arg).eval_type();
            is_all_string = is_all_string && arg_tp == EvalType::String;
            is_all_number = is_all_number && arg_tp == EvalType::Int;
        }
        let (arg_tp, sig) = if is_all_string {
            (EvalType::String, ScalarFuncSig::FieldString)
        } else if is_all_number {
            (EvalType::Int, ScalarFuncSig::FieldInt)
        } else {
            (EvalType::Real, ScalarFuncSig::FieldReal)
        };
        let arg_tps = vec![arg_tp; args.len()];
        let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Int, &arg_tps)?;
        bf.pb_code = sig;
        Ok(bf)
    }
);

/// Go `makeSetFunctionClass.getFlen`.
fn make_set_flen(ctx: &dyn BuildContext, args: &[Expression]) -> i64 {
    let arg_flen = |i: usize| type_of(&args[i]).flen();
    if matches!(args[0], Expression::Constant(_)) {
        if let Some(bits) = eval_int_constant(ctx, &args[0]) {
            let (mut flen, mut count) = (0, 0);
            for i in 1..args.len() {
                // Go's `1 << uint(i-1)` is 0 once the shift reaches the word
                // width.
                let bit = u32::try_from(i - 1)
                    .ok()
                    .and_then(|shift| 1_i64.checked_shl(shift))
                    .unwrap_or(0);
                if bits & bit != 0 {
                    flen += arg_flen(i);
                    count += 1;
                }
            }
            if count > 0 {
                flen += count - 1;
            }
            return flen;
        }
    }
    (1..args.len()).map(arg_flen).sum::<i64>() + args.len() as i64 - 1 - 1
}

string_class!(
    /// Go `makeSetFunctionClass`.
    MakeSetFunctionClass, |func_name, ctx, args| {
        let mut arg_tps = vec![EvalType::String; args.len()];
        arg_tps[0] = EvalType::Int;
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
        add_bin_flag(&mut bf.tp);
        bf.tp.set_flen(make_set_flen(ctx, &bf.args));
        if bf.tp.flen() > MAX_BLOB_WIDTH {
            bf.tp.set_flen(MAX_BLOB_WIDTH);
        }
        bf.pb_code = ScalarFuncSig::MakeSet;
        Ok(bf)
    }
);

string_class!(
    /// Go `octFunctionClass`.
    OctFunctionClass, |func_name, ctx, args| {
        // Go `IsBinaryLiteral(args[0])`.
        let is_binary_literal = matches!(
            &args[0],
            Expression::Constant(constant) if matches!(constant.value, Datum::BinaryLiteral(_))
        );
        let (arg_tp, sig) =
            if is_binary_literal || type_of(&args[0]).eval_type() == EvalType::Int {
                (EvalType::Int, ScalarFuncSig::OctInt)
            } else {
                (EvalType::String, ScalarFuncSig::OctString)
            };
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &[arg_tp])?;
        set_connection_charset(&mut bf, ctx);
        bf.tp.set_flen(64);
        bf.tp.set_decimal(UNSPECIFIED_LENGTH);
        bf.pb_code = sig;
        Ok(bf)
    }
);

function_class!(
    /// Go `ordFunctionClass`.
    OrdFunctionClass, Int, [String], Ord, |bf, _ctx| {
        bf.tp.set_flen(10);
    }
);

function_class!(
    /// Go `quoteFunctionClass`.
    QuoteFunctionClass, String, [String], Quote, |bf, _ctx| {
        let arg_type = type_of(&bf.args[0]);
        set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
        let flen = arg_type.flen();
        bf.tp.set_flen(if flen == UNSPECIFIED_LENGTH {
            UNSPECIFIED_LENGTH
        } else {
            2 * flen + 2
        });
        if bf.tp.flen() > MAX_BLOB_WIDTH {
            bf.tp.set_flen(MAX_BLOB_WIDTH);
        }
    }
);

function_class!(
    /// Go `binFunctionClass`.
    BinFunctionClass, String, [Int], Bin, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(64);
    }
);

string_class!(
    /// Go `eltFunctionClass`.
    EltFunctionClass, |func_name, ctx, args| {
        let mut arg_tps = vec![EvalType::String; args.len()];
        arg_tps[0] = EvalType::Int;
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
        for i in 1..bf.args.len() {
            let arg_type = type_of(&bf.args[i]);
            if is_binary_str(&arg_type) {
                set_bin_chs_cln_flag(&mut bf.tp);
            }
            let flen = arg_type.flen();
            if flen == UNSPECIFIED_LENGTH || flen > bf.tp.flen() {
                bf.tp.set_flen(flen);
            }
        }
        bf.pb_code = ScalarFuncSig::Elt;
        Ok(bf)
    }
);

string_class!(
    /// Go `exportSetFunctionClass`.
    ExportSetFunctionClass, |func_name, ctx, args| {
        let mut arg_tps = vec![EvalType::Int, EvalType::String, EvalType::String];
        if args.len() > 3 {
            arg_tps.push(EvalType::String);
        }
        if args.len() > 4 {
            arg_tps.push(EvalType::Int);
        }
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
        // Calculate the flen as MySQL does.
        let l = type_of(&bf.args[2]).flen().max(type_of(&bf.args[1]).flen());
        let sep_l = if bf.args.len() > 3 {
            type_of(&bf.args[3]).flen()
        } else {
            1
        };
        bf.tp.set_flen((l * 64 + sep_l * 63) * 4);
        bf.pb_code = match bf.args.len() {
            3 => ScalarFuncSig::ExportSet3Arg,
            4 => ScalarFuncSig::ExportSet4Arg,
            _ => ScalarFuncSig::ExportSet5Arg,
        };
        Ok(bf)
    }
);

string_class!(
    /// Go `formatFunctionClass`.
    FormatFunctionClass, |func_name, ctx, args| {
        let arg_tp = type_of(&args[0]).eval_type();
        let mut arg_tps = vec![
            if arg_tp == EvalType::Decimal || arg_tp == EvalType::Int {
                EvalType::Decimal
            } else {
                EvalType::Real
            },
            EvalType::Int,
        ];
        if args.len() == 3 {
            arg_tps.push(EvalType::String);
        }
        let mut bf =
            new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
        set_connection_charset(&mut bf, ctx);
        bf.tp.set_flen(MAX_BLOB_WIDTH);
        bf.pb_code = if bf.args.len() == 3 {
            ScalarFuncSig::FormatWithLocale
        } else {
            ScalarFuncSig::Format
        };
        Ok(bf)
    }
);

function_class!(
    /// Go `fromBase64FunctionClass`.
    FromBase64FunctionClass, String, [String], FromBase64, |bf, _ctx| {
        // The calculation of flen is the same as MySQL.
        let flen = type_of(&bf.args[0]).flen();
        if flen == UNSPECIFIED_LENGTH {
            bf.tp.set_flen(UNSPECIFIED_LENGTH);
        } else {
            bf.tp.set_flen(flen * 3);
            if bf.tp.flen() > MAX_BLOB_WIDTH {
                bf.tp.set_flen(MAX_BLOB_WIDTH);
            }
        }
        set_bin_chs_cln_flag(&mut bf.tp);
    }
);

/// Go `base64NeededEncodedLength`, on a 64-bit `int`.
pub(crate) fn base64_needed_encoded_length(n: i64) -> i64 {
    // Returns -1 indicate the result will overflow.
    if n > 6_827_690_988_321_067_803 {
        return -1;
    }
    let length = (n + 2) / 3 * 4;
    length + (length - 1) / 76
}

function_class!(
    /// Go `toBase64FunctionClass`.
    ToBase64FunctionClass, String, [String], ToBase64, |bf, ctx| {
        set_connection_charset(bf, ctx);
        let flen = type_of(&bf.args[0]).flen();
        bf.tp.set_flen(if flen == UNSPECIFIED_LENGTH {
            UNSPECIFIED_LENGTH
        } else {
            base64_needed_encoded_length(flen)
        });
    }
);

string_class!(
    /// Go `insertFunctionClass`.
    InsertFunctionClass, |func_name, ctx, args| {
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::String,
            &[EvalType::String, EvalType::Int, EvalType::Int, EvalType::String],
        )?;
        bf.tp.set_flen(MAX_BLOB_WIDTH);
        add_bin_flag(&mut bf.tp);
        bf.pb_code = if is_binary_str(&bf.tp) {
            ScalarFuncSig::Insert
        } else {
            ScalarFuncSig::InsertUtf8
        };
        Ok(bf)
    }
);

string_class!(
    /// Go `instrFunctionClass`.
    InstrFunctionClass, |func_name, ctx, args| {
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::Int,
            &[EvalType::String, EvalType::String],
        )?;
        bf.tp.set_flen(11);
        bf.pb_code = if use_binary(&bf) {
            ScalarFuncSig::Instr
        } else {
            ScalarFuncSig::InstrUtf8
        };
        Ok(bf)
    }
);

function_class!(
    /// Go `loadFileFunctionClass`.
    LoadFileFunctionClass, String, [String], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::load_file), |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(64);
    }
);

function_class!(
    /// Go `translateFunctionClass`: `builtinTranslateBinarySig` and
    /// `builtinTranslateUTF8Sig` have no pb code.
    TranslateFunctionClass, String, [String, String, String], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::translate), |bf, _ctx| {
        let arg_type = type_of(&bf.args[0]);
        bf.tp.set_flen(arg_type.flen());
        set_bin_flag_or_bin_str(&arg_type, &mut bf.tp);
    }
);

string_class!(
    /// Go `weightStringFunctionClass`. `verifyArgs` reads the padding and
    /// the length off the constant second and third arguments.
    WeightStringFunctionClass, |func_name, ctx, args| {
        let incorrect_type = |argument: String| {
            EvalError::Conversion(
                crate::expression_errors::ERR_INCORRECT_TYPE
                    .gen_with_stack_by_args(&[argument.into(), func_name.into()]),
            )
        };
        let text = |arg: &Expression| {
            crate::scalar_function::string_with_ctx(
                arg,
                ctx.columns().unwrap_or(&tidb_exprctx::NoColumns),
            )
            .unwrap_or_default()
        };
        if !matches!(args.len(), 1 | 3) {
            return Err(EvalError::Conversion(
                crate::expression_errors::ERR_INCORRECT_PARAMETER_COUNT
                    .gen_with_stack_by_args(&[func_name.into()]),
            ));
        }
        let numeric = type_of(&args[0]).code().is_type_numeric();
        let mut padding = WeightStringPadding::None;
        let mut length = 0;
        if args.len() == 3 {
            let (Expression::Constant(c1), EvalType::String) =
                (&args[1], type_of(&args[1]).eval_type())
            else {
                return Err(incorrect_type(text(&args[1])));
            };
            match c1.value.sql_string().unwrap_or_default().as_str() {
                "CHAR" => padding = WeightStringPadding::AsChar,
                "BINARY" => padding = WeightStringPadding::AsBinary,
                other => return Err(incorrect_type(other.to_owned())),
            }
            if type_of(&args[2]).eval_type() != EvalType::Int {
                return Err(incorrect_type(text(&args[2])));
            }
            let Expression::Constant(c2) = &args[2] else {
                return Err(incorrect_type(text(&args[1])));
            };
            length = match c2.value {
                Datum::Int(value) => value,
                Datum::UInt(value) => value as i64,
                _ => 0,
            };
            if length == 0 {
                return Err(incorrect_type(text(&args[2])));
            }
        }
        let arg_tps = [EvalType::String, EvalType::String, EvalType::Int];
        let arg_len = args.len();
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            func_name,
            args,
            EvalType::String,
            &arg_tps[..arg_len],
        )?;
        set_bin_chs_cln_flag(&mut bf.tp);
        // A numeric argument is `weightStringPaddingNull`, which `AS BINARY`
        // alone replaces.
        let kernel;
        (bf.sig, kernel) = if numeric && padding != WeightStringPadding::AsBinary {
            (SigFields::WeightStringNull, crate::scalar_function::Kernel::Row(crate::scalar_function::class_kernels::weight_string_null))
        } else {
            (SigFields::WeightString { padding, length }, crate::scalar_function::Kernel::Row(crate::scalar_function::class_kernels::weight_string))
        };
        Ok(bf.evaluated_by(kernel))
    }
);
