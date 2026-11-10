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

//! `pkg/expression/builtin_json.go`: the JSON function classes.
//! `jsonSumCRC32FunctionClass` carries its array type and is built by the
//! rewriter's `JSON_SUM_CRC32` arm, as Go's
//! `BuildJSONSumCrc32FunctionWithCheck` builds it.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::builtin_ext::json::{verify_json_args_type, verify_json_schema_valid_constants};
use crate::builtin_miscellaneous::set_connection_charset;
use crate::expr_util::disable_parse_json_flag_4_expr;
use crate::expression::Expression;
use crate::wrap_cast::{type_of, MAX_BLOB_WIDTH};
use crate::EvalError;
use tidb_datatype::{EvalType, FieldTypeFlags};
use tidb_exprctx::context::JsonError;
use tidb_proto::tipb::ScalarFuncSig;

/// The JSON document followed by `rest` for every later argument.
fn document_then(args: &[Expression], rest: EvalType) -> Vec<EvalType> {
    let mut arg_tps = vec![rest; args.len()];
    if let Some(first) = arg_tps.first_mut() {
        *first = EvalType::Json;
    }
    arg_tps
}

/// The JSON document followed by (path, value) pairs.
fn document_then_path_value_pairs(args: &[Expression]) -> Vec<EvalType> {
    let mut arg_tps = vec![EvalType::Json];
    for _ in (1..args.len().saturating_sub(1)).step_by(2) {
        arg_tps.extend([EvalType::String, EvalType::Json]);
    }
    arg_tps
}

/// Go's `jsonSetFunctionClass`, `jsonInsertFunctionClass`,
/// `jsonReplaceFunctionClass`, `jsonArrayInsertFunctionClass` and
/// `jsonArrayAppendFunctionClass` `getFunction`: the document, then
/// (path, value) pairs whose values keep a string a string.
fn json_modify_function(
    ctx: &dyn BuildContext,
    func_name: &'static str,
    args: Vec<Expression>,
    sig: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    if args.len() & 1 != 1 {
        return Err(EvalError::WrongParameterCount(func_name));
    }
    let arg_tps = document_then_path_value_pairs(&args);
    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::Json, &arg_tps)?;
    for value in bf.args.iter_mut().skip(2).step_by(2) {
        disable_parse_json_flag_4_expr(value);
    }
    bf.pb_code = sig;
    Ok(bf)
}

/// A class whose `getFunction` is one `newBaseBuiltinFuncWithTp` over
/// argument types computed from the arguments, after `verifyArgs`.
macro_rules! json_class {
    ($(#[$doc:meta])* $class:ident, $_name:literal, |$ctx:ident, $args:ident| $body:block) => {
        $(#[$doc])*
        pub(crate) struct $class;

        impl FunctionClass for $class {
            fn get_function(
                &self,
                $ctx: &dyn BuildContext,
                $args: Vec<Expression>,
            ) -> Result<BaseBuiltinFunc, EvalError> {
                $body
            }
        }
    };
}

/// `newBaseBuiltinFuncWithTp` followed by the signature's pb code.
fn with_sig(
    ctx: &dyn BuildContext,
    func_name: &'static str,
    args: Vec<Expression>,
    ret: EvalType,
    arg_tps: &[EvalType],
    sig: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, ret, arg_tps)?;
    bf.pb_code = sig;
    Ok(bf)
}

json_class!(
    /// Go `jsonTypeFunctionClass`.
    JsonTypeFunctionClass, "json_type", |ctx, args| {
        verify_json_args_type("json_type", true, &args, Some(&[0]))?;
        let mut bf = with_sig(
            ctx, "json_type", args, EvalType::String, &[EvalType::Json], ScalarFuncSig::JsonTypeSig,
        )?;
        set_connection_charset(&mut bf, ctx);
        bf.tp.set_flen(51); // flen of JSON_TYPE is length of UNSIGNED INTEGER.
        bf.tp.add_flags(FieldTypeFlags::BINARY);
        Ok(bf)
    }
);

json_class!(
    /// Go `jsonExtractFunctionClass`.
    JsonExtractFunctionClass, "json_extract", |ctx, args| {
        verify_json_args_type("json_extract", true, &args, Some(&[0]))?;
        let arg_tps = document_then(&args, EvalType::String);
        with_sig(ctx, "json_extract", args, EvalType::Json, &arg_tps, ScalarFuncSig::JsonExtractSig)
    }
);

json_class!(
    /// Go `jsonUnquoteFunctionClass`.
    JsonUnquoteFunctionClass, "json_unquote", |ctx, args| {
        verify_json_args_type("json_unquote", false, &args, Some(&[0]))?;
        let mut bf = with_sig(
            ctx, "json_unquote", args, EvalType::String, &[EvalType::String],
            ScalarFuncSig::JsonUnquoteSig,
        )?;
        bf.tp.set_flen(type_of(&bf.args[0]).flen());
        bf.tp.add_flags(FieldTypeFlags::BINARY);
        disable_parse_json_flag_4_expr(&mut bf.args[0]);
        Ok(bf)
    }
);

json_class!(
    /// Go `jsonSetFunctionClass`.
    JsonSetFunctionClass, "json_set", |ctx, args| {
        json_modify_function(ctx, "json_set", args, ScalarFuncSig::JsonSetSig)
    }
);

json_class!(
    /// Go `jsonInsertFunctionClass`.
    JsonInsertFunctionClass, "json_insert", |ctx, args| {
        json_modify_function(ctx, "json_insert", args, ScalarFuncSig::JsonInsertSig)
    }
);

json_class!(
    /// Go `jsonReplaceFunctionClass`.
    JsonReplaceFunctionClass, "json_replace", |ctx, args| {
        json_modify_function(ctx, "json_replace", args, ScalarFuncSig::JsonReplaceSig)
    }
);

json_class!(
    /// Go `jsonArrayAppendFunctionClass`: its `verifyArgs` is the parity
    /// check alone.
    JsonArrayAppendFunctionClass, "json_array_append", |ctx, args| {
        json_modify_function(ctx, "json_array_append", args, ScalarFuncSig::JsonArrayAppendSig)
    }
);

json_class!(
    /// Go `jsonArrayInsertFunctionClass`.
    JsonArrayInsertFunctionClass, "json_array_insert", |ctx, args| {
        json_modify_function(ctx, "json_array_insert", args, ScalarFuncSig::JsonArrayInsertSig)
    }
);

json_class!(
    /// Go `jsonRemoveFunctionClass`.
    JsonRemoveFunctionClass, "json_remove", |ctx, args| {
        let arg_tps = document_then(&args, EvalType::String);
        with_sig(ctx, "json_remove", args, EvalType::Json, &arg_tps, ScalarFuncSig::JsonRemoveSig)
    }
);

/// Go's `jsonMergeFunctionClass`, `jsonMergePatchFunctionClass` and
/// `jsonMergePreserveFunctionClass` `getFunction`: every argument a
/// document.
fn json_merge_function(
    ctx: &dyn BuildContext,
    func_name: &'static str,
    args: Vec<Expression>,
    sig: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    verify_json_args_type(func_name, true, &args, None)?;
    let arg_tps = vec![EvalType::Json; args.len()];
    with_sig(ctx, func_name, args, EvalType::Json, &arg_tps, sig)
}

json_class!(
    /// Go `jsonMergeFunctionClass`.
    JsonMergeFunctionClass, "json_merge", |ctx, args| {
        json_merge_function(ctx, "json_merge", args, ScalarFuncSig::JsonMergeSig)
    }
);

json_class!(
    /// Go `jsonMergePatchFunctionClass`.
    JsonMergePatchFunctionClass, "json_merge_patch", |ctx, args| {
        json_merge_function(ctx, "json_merge_patch", args, ScalarFuncSig::JsonMergePatchSig)
    }
);

json_class!(
    /// Go `jsonMergePreserveFunctionClass`.
    JsonMergePreserveFunctionClass, "json_merge_preserve", |ctx, args| {
        json_merge_function(ctx, "json_merge_preserve", args, ScalarFuncSig::JsonMergePreserveSig)
    }
);

json_class!(
    /// Go `jsonObjectFunctionClass`.
    JsonObjectFunctionClass, "json_object", |ctx, args| {
        if args.len() & 1 != 0 {
            return Err(EvalError::WrongParameterCount("json_object"));
        }
        let mut arg_tps = Vec::with_capacity(args.len());
        for key in args.iter().step_by(2) {
            let key_tp = type_of(key);
            if key_tp.eval_type() == EvalType::String && key_tp.charset_name() == "binary" {
                return Err(EvalError::Json(JsonError::InvalidCharset));
            }
            arg_tps.extend([EvalType::String, EvalType::Json]);
        }
        let mut bf = with_sig(
            ctx, "json_object", args, EvalType::Json, &arg_tps, ScalarFuncSig::JsonObjectSig,
        )?;
        for value in bf.args.iter_mut().skip(1).step_by(2) {
            disable_parse_json_flag_4_expr(value);
        }
        Ok(bf)
    }
);

json_class!(
    /// Go `jsonArrayFunctionClass`.
    JsonArrayFunctionClass, "json_array", |ctx, args| {
        let arg_tps = vec![EvalType::Json; args.len()];
        let mut bf = with_sig(
            ctx, "json_array", args, EvalType::Json, &arg_tps, ScalarFuncSig::JsonArraySig,
        )?;
        for value in &mut bf.args {
            disable_parse_json_flag_4_expr(value);
        }
        Ok(bf)
    }
);

json_class!(
    /// Go `jsonContainsPathFunctionClass`.
    JsonContainsPathFunctionClass, "json_contains_path", |ctx, args| {
        verify_json_args_type("json_contains_path", true, &args, Some(&[0]))?;
        let arg_tps = document_then(&args, EvalType::String);
        with_sig(
            ctx, "json_contains_path", args, EvalType::Int, &arg_tps,
            ScalarFuncSig::JsonContainsPathSig,
        )
    }
);

json_class!(
    /// Go `jsonMemberOfFunctionClass`.
    JsonMemberOfFunctionClass, "json_memberof", |ctx, args| {
        verify_json_args_type("member of", true, &args, Some(&[1]))?;
        let mut bf = with_sig(
            ctx, "json_memberof", args, EvalType::Int, &[EvalType::Json, EvalType::Json],
            ScalarFuncSig::JsonMemberOfSig,
        )?;
        disable_parse_json_flag_4_expr(&mut bf.args[0]);
        Ok(bf)
    }
);

json_class!(
    /// Go `jsonContainsFunctionClass`.
    JsonContainsFunctionClass, "json_contains", |ctx, args| {
        verify_json_args_type("json_contains", true, &args, Some(&[0, 1]))?;
        let mut arg_tps = vec![EvalType::Json, EvalType::Json];
        if args.len() == 3 {
            arg_tps.push(EvalType::String);
        }
        with_sig(ctx, "json_contains", args, EvalType::Int, &arg_tps, ScalarFuncSig::JsonContainsSig)
    }
);

json_class!(
    /// Go `jsonOverlapsFunctionClass`.
    JsonOverlapsFunctionClass, "json_overlaps", |ctx, args| {
        verify_json_args_type("json_overlaps", true, &args, Some(&[0, 1]))?;
        new_base_builtin_func_with_tp(
            ctx, "json_overlaps", args, EvalType::Int, &[EvalType::Json, EvalType::Json],
        )
        .map(|bf| bf.evaluated_by(crate::scalar_function::Kernel::Method(crate::scalar_function::class_kernels::json_overlaps)))
    }
);

json_class!(
    /// Go `jsonValidFunctionClass`.
    JsonValidFunctionClass, "json_valid", |ctx, args| {
        let arg_type = type_of(&args[0]).eval_type();
        let sig = match arg_type {
            EvalType::Json => ScalarFuncSig::JsonValidJsonSig,
            EvalType::String => ScalarFuncSig::JsonValidStringSig,
            _ => ScalarFuncSig::JsonValidOthersSig,
        };
        with_sig(ctx, "json_valid", args, EvalType::Int, &[arg_type], sig)
    }
);

json_class!(
    /// Go `jsonPrettyFunctionClass`.
    JsonPrettyFunctionClass, "json_pretty", |ctx, args| {
        let mut bf = with_sig(
            ctx, "json_pretty", args, EvalType::String, &[EvalType::Json],
            ScalarFuncSig::JsonPrettySig,
        )?;
        bf.tp.add_flags(FieldTypeFlags::BINARY);
        bf.tp.set_flen(MAX_BLOB_WIDTH * 4);
        Ok(bf)
    }
);

json_class!(
    /// Go `jsonQuoteFunctionClass`.
    JsonQuoteFunctionClass, "json_quote", |ctx, args| {
        if type_of(&args[0]).eval_type() != EvalType::String {
            return Err(EvalError::Json(JsonError::IncorrectType {
                argument: 1,
                function: "json_quote",
            }));
        }
        let mut bf = with_sig(
            ctx, "json_quote", args, EvalType::String, &[EvalType::String],
            ScalarFuncSig::JsonQuoteSig,
        )?;
        disable_parse_json_flag_4_expr(&mut bf.args[0]);
        bf.tp.add_flags(FieldTypeFlags::BINARY);
        bf.tp.set_flen(type_of(&bf.args[0]).flen() * 6 + 2);
        Ok(bf)
    }
);

json_class!(
    /// Go `jsonSearchFunctionClass`.
    JsonSearchFunctionClass, "json_search", |ctx, args| {
        verify_json_args_type("json_search", true, &args, Some(&[0]))?;
        let arg_tps = document_then(&args, EvalType::String);
        with_sig(ctx, "json_search", args, EvalType::Json, &arg_tps, ScalarFuncSig::JsonSearchSig)
    }
);

json_class!(
    /// Go `jsonStorageFreeFunctionClass`.
    JsonStorageFreeFunctionClass, "json_storage_free", |ctx, args| {
        with_sig(
            ctx, "json_storage_free", args, EvalType::Int, &[EvalType::Json],
            ScalarFuncSig::JsonStorageFreeSig,
        )
    }
);

json_class!(
    /// Go `jsonStorageSizeFunctionClass`.
    JsonStorageSizeFunctionClass, "json_storage_size", |ctx, args| {
        with_sig(
            ctx, "json_storage_size", args, EvalType::Int, &[EvalType::Json],
            ScalarFuncSig::JsonStorageSizeSig,
        )
    }
);

json_class!(
    /// Go `jsonDepthFunctionClass`.
    JsonDepthFunctionClass, "json_depth", |ctx, args| {
        with_sig(
            ctx, "json_depth", args, EvalType::Int, &[EvalType::Json], ScalarFuncSig::JsonDepthSig,
        )
    }
);

json_class!(
    /// Go `jsonKeysFunctionClass`.
    JsonKeysFunctionClass, "json_keys", |ctx, args| {
        verify_json_args_type("json_keys", true, &args, Some(&[0]))?;
        let arg_tps = document_then(&args, EvalType::String);
        let sig = if args.len() == 2 {
            ScalarFuncSig::JsonKeys2ArgsSig
        } else {
            ScalarFuncSig::JsonKeysSig
        };
        with_sig(ctx, "json_keys", args, EvalType::Json, &arg_tps, sig)
    }
);

json_class!(
    /// Go `jsonLengthFunctionClass`.
    JsonLengthFunctionClass, "json_length", |ctx, args| {
        let arg_tps = document_then(&args, EvalType::String);
        with_sig(ctx, "json_length", args, EvalType::Int, &arg_tps, ScalarFuncSig::JsonLengthSig)
    }
);

json_class!(
    /// Go `jsonSchemaValidFunctionClass`.
    JsonSchemaValidFunctionClass, "json_schema_valid", |ctx, args| {
        verify_json_args_type("json_schema_valid", true, &args, Some(&[0, 1]))?;
        verify_json_schema_valid_constants(&args)?;
        new_base_builtin_func_with_tp(
            ctx, "json_schema_valid", args, EvalType::Int, &[EvalType::Json, EvalType::Json],
        )
        .map(|bf| bf.evaluated_by(crate::scalar_function::Kernel::Row(crate::scalar_function::class_kernels::json_schema_valid)))
    }
);
