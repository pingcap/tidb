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

//! `pkg/expression/builtin_info.go`: the information function classes.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::builtin_miscellaneous::{function_class, set_connection_charset};
use crate::expression::Expression;
use crate::wrap_cast::type_of;
use crate::EvalError;
use tidb_datatype::{EvalType, FieldTypeFlags};
use tidb_proto::tipb::ScalarFuncSig;

function_class!(
    /// Go `databaseFunctionClass`.
    DatabaseFunctionClass, String, [], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(64);
    }
);

function_class!(
    /// Go `currentUserFunctionClass`.
    CurrentUserFunctionClass, String, [], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(64);
    }
);

function_class!(
    /// Go `currentRoleFunctionClass`.
    CurrentRoleFunctionClass, String, [], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(64);
    }
);

function_class!(
    /// Go `currentResourceGroupFunctionClass`.
    CurrentResourceGroupFunctionClass, String, [], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(64);
    }
);

function_class!(
    /// Go `userFunctionClass`.
    UserFunctionClass, String, [], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(64);
    }
);

function_class!(
    /// Go `versionFunctionClass`.
    VersionFunctionClass, String, [], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(64);
    }
);

function_class!(
    /// Go `foundRowsFunctionClass`.
    FoundRowsFunctionClass, Int, [], Unspecified, |bf, _ctx| {
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
    }
);

function_class!(
    /// Go `connectionIDFunctionClass`.
    ConnectionIdFunctionClass, Int, [], Unspecified, |bf, _ctx| {
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
    }
);

/// Go `lastInsertIDFunctionClass`.
pub(crate) struct LastInsertIdFunctionClass;

impl FunctionClass for LastInsertIdFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let args_len = args.len();
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            "last_insert_id",
            args,
            EvalType::Int,
            &[EvalType::Int][..args_len],
        )?;
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        bf.pb_code = if args_len == 1 {
            ScalarFuncSig::LastInsertIdWithId
        } else {
            ScalarFuncSig::LastInsertId
        };
        Ok(bf)
    }
}

function_class!(
    /// Go `tidbVersionFunctionClass`.
    TidbVersionFunctionClass, String, [], Unspecified, |bf, _ctx| {
        let flen = tidb_printer::get_tidb_info().len();
        bf.tp.set_flen(i64::try_from(flen).unwrap_or(i64::MAX));
    }
);

function_class!(
    /// Go `tidbIsDDLOwnerFunctionClass`.
    TidbIsDdlOwnerFunctionClass, Int, [], Unspecified
);

/// The evaluation types of `args`, for the Go classes that take each
/// argument as it is.
fn eval_types(args: &[Expression]) -> Vec<EvalType> {
    args.iter().map(|arg| type_of(arg).eval_type()).collect()
}

/// Go `benchmarkFunctionClass`. The expression keeps its own evaluation
/// type, so no cast is wrapped around it.
pub(crate) struct BenchmarkFunctionClass;

impl FunctionClass for BenchmarkFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let same_eval_type = type_of(&args[1]).eval_type();
        new_base_builtin_func_with_tp(ctx, "benchmark", args, EvalType::Int, &[EvalType::Int, same_eval_type])
    }
}

/// Go `charsetFunctionClass` and `collationFunctionClass`: a 64-character
/// string in the connection charset over the argument as it is.
fn charset_or_collation(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
) -> Result<BaseBuiltinFunc, EvalError> {
    let arg_tps = eval_types(&args);
    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
    set_connection_charset(&mut bf, ctx);
    bf.tp.set_flen(64);
    Ok(bf)
}

/// Go `charsetFunctionClass`.
pub(crate) struct CharsetFunctionClass;

impl FunctionClass for CharsetFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        charset_or_collation(ctx, "charset", args)
    }
}

/// Go `coercibilityFunctionClass`.
pub(crate) struct CoercibilityFunctionClass;

impl FunctionClass for CoercibilityFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = eval_types(&args);
        new_base_builtin_func_with_tp(ctx, "coercibility", args, EvalType::Int, &arg_tps)
    }
}

/// Go `collationFunctionClass`.
pub(crate) struct CollationFunctionClass;

impl FunctionClass for CollationFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        charset_or_collation(ctx, "collation", args)
    }
}

function_class!(
    /// Go `rowCountFunctionClass`.
    RowCountFunctionClass, Int, [], RowCount
);

function_class!(
    /// Go `tidbMVCCInfoFunctionClass`.
    TidbMvccInfoFunctionClass, String, [String], Unspecified
);

function_class!(
    /// Go `tidbDecodeKeyFunctionClass`.
    TidbDecodeKeyFunctionClass, String, [String], Unspecified
);

/// Go `tidbRowChecksumFunctionClass`: every build is refused with
/// `ErrNotSupportedYet.GenWithStack(...)`, which REPLACES the 1235 message
/// rather than formatting it -- the checksum exists only as the fast point
/// plan's select field (`tryExtractRowChecksumColumn`).
pub(crate) struct TidbRowChecksumFunctionClass;

impl FunctionClass for TidbRowChecksumFunctionClass {
    fn get_function(
        &self,
        _: &dyn BuildContext,
        _: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        use tidb_terror::{TerrorClass, TerrorError};
        // Go `ErrNotSupportedYet`: `ClassExpression.NewStd(1235)`.
        static ERR_NOT_SUPPORTED_YET: std::sync::LazyLock<TerrorError> =
            std::sync::LazyLock::new(|| {
                tidb_dbterror::ErrClass(TerrorClass::Expression).new_std(1235)
            });
        Err(EvalError::Conversion(ERR_NOT_SUPPORTED_YET.gen_with_stack(
            "FUNCTION tidb_row_checksum can only be used as a select field in a fast point plan",
        )))
    }
}

/// Go `tidbEncodeRecordKeyClass` (`leading == 2`) and
/// `tidbEncodeIndexKeyClass` (`leading == 3`): the database, table and index
/// names are strings, and every key value keeps its own evaluation type.
pub(crate) struct TidbEncodeKeyClass {
    pub(crate) func_name: &'static str,
    pub(crate) leading: usize,
}

impl FunctionClass for TidbEncodeKeyClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let mut eval_tps = vec![EvalType::String; self.leading];
        eval_tps.extend(args[self.leading..].iter().map(|arg| type_of(arg).eval_type()));
        new_base_builtin_func_with_tp(ctx, self.func_name, args, EvalType::String, &eval_tps)
    }
}

/// Go `tidbDecodeSQLDigestsFunctionClass`. The PROCESS privilege Go checks
/// here is checked by the rewriter, which holds the session.
pub(crate) struct TidbDecodeSqlDigestsFunctionClass;

impl FunctionClass for TidbDecodeSqlDigestsFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = [EvalType::String, EvalType::Int];
        let args_len = args.len();
        new_base_builtin_func_with_tp(
            ctx,
            "tidb_decode_sql_digests",
            args,
            EvalType::String,
            &arg_tps[..args_len],
        )
    }
}

function_class!(
    /// Go `tidbEncodeSQLDigestFunctionClass`.
    TidbEncodeSqlDigestFunctionClass, String, [String], Unspecified
);

function_class!(
    /// Go `tidbDecodePlanFunctionClass`.
    TidbDecodePlanFunctionClass, String, [String], Unspecified
);

function_class!(
    /// Go `nextValFunctionClass`.
    NextValFunctionClass, Int, [String], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(10);
    }
);

function_class!(
    /// Go `lastValFunctionClass`.
    LastValFunctionClass, Int, [String], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(10);
    }
);

function_class!(
    /// Go `setValFunctionClass`.
    SetValFunctionClass, Int, [String, Int], Unspecified, |bf, _ctx| {
        // Go reads `args[1]` after `newBaseBuiltinFuncWithTp` cast it in place.
        let flen = type_of(&bf.args[1]).flen();
        bf.tp.set_flen(flen);
    }
);

function_class!(
    /// Go `formatBytesFunctionClass`.
    FormatBytesFunctionClass, String, [Real], Unspecified, |bf, ctx| {
        set_connection_charset(bf, ctx);
    }
);

function_class!(
    /// Go `formatNanoTimeFunctionClass`.
    FormatNanoTimeFunctionClass, String, [Real], Unspecified, |bf, ctx| {
        set_connection_charset(bf, ctx);
    }
);
