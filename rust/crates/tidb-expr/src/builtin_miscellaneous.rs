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

//! `pkg/expression/builtin_miscellaneous.go`: the miscellaneous function
//! classes.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::expression::Expression;
use crate::wrap_cast::{set_bin_chs_cln_flag, type_of};
use crate::EvalError;
use tidb_datatype::{EvalType, FieldTypeFlags};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `types.UnspecifiedLength`.
const UNSPECIFIED_LENGTH: i64 = -1;
/// Go `mysql.DefaultCharset`.
const DEFAULT_CHARSET: &str = "utf8mb4";
/// Go `mysql.DefaultCollationName`.
const DEFAULT_COLLATION_NAME: &str = "utf8mb4_bin";

/// A Go function class whose `getFunction` is `newBaseBuiltinFuncWithTp`
/// over fixed evaluation types, an optional adjustment of the result type
/// and one signature (`Unspecified` where Go sets none).
macro_rules! function_class {
    ($(#[$doc:meta])* $class:ident, $ret:ident, [$($arg:ident),*], $sig:ident) => {
        function_class!($(#[$doc])* $class, $ret, [$($arg),*], $sig, |_bf, _ctx| {});
    };
    ($(#[$doc:meta])* $class:ident, $ret:ident, [$($arg:ident),*], $sig:ident, |$bf:ident, $ctx:ident| $body:block) => {
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
                let arg_tps: [EvalType; 0 $(+ { let _ = EvalType::$arg; 1 })*] = [$(EvalType::$arg),*];
                let mut bf =
                    new_base_builtin_func_with_tp(ctx, self.func_name, args, EvalType::$ret, &arg_tps)?;
                {
                    let $bf = &mut bf;
                    let $ctx = ctx;
                    $body
                }
                bf.pb_code = ScalarFuncSig::$sig;
                Ok(bf)
            }
        }
    };
}
pub(crate) use function_class;

/// The connection charset and collation on a string result, as each Go
/// class sets them from `ctx.GetCharsetInfo()`.
pub(crate) fn set_connection_charset(bf: &mut BaseBuiltinFunc, ctx: &dyn BuildContext) {
    let (charset, collate) = ctx.connection_charset_info();
    bf.tp.set_charset_name(charset);
    bf.tp.set_collation_name(collate);
}

function_class!(
    /// Go `sleepFunctionClass`.
    SleepFunctionClass, Int, [Real], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(21);
    }
);

function_class!(
    /// Go `lockFunctionClass`.
    LockFunctionClass, Int, [String, Int], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `releaseLockFunctionClass`.
    ReleaseLockFunctionClass, Int, [String], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

/// Go `anyValueFunctionClass.getFunction` and `nameConstFunctionClass
/// .getFunction` share this: the result is the value argument's type, and
/// the signature is chosen by its evaluation type.
fn value_typed_function(
    ctx: &dyn BuildContext,
    func_name: &str,
    args: Vec<Expression>,
    value: usize,
    sigs: [ScalarFuncSig; 8],
) -> Result<BaseBuiltinFunc, EvalError> {
    let value_tp = type_of(&args[value]).clone();
    let arg_tp = value_tp.eval_type();
    let arg_tps = [EvalType::String, arg_tp];
    let mut bf =
        new_base_builtin_func_with_tp(ctx, func_name, args, arg_tp, &arg_tps[1 - value..])?;
    let flags = bf.tp.raw_flags();
    bf.tp = value_tp;
    if value == 0 {
        // Go `anyValueFunctionClass`: `ft.AddFlag(bf.tp.GetFlag())`.
        bf.tp.add_raw_flags(flags);
    }
    let [decimal, duration, int, json, vector, real, string, time] = sigs;
    bf.pb_code = match arg_tp {
        EvalType::Decimal => decimal,
        EvalType::Duration => duration,
        EvalType::Int => {
            bf.tp.set_decimal(0);
            int
        }
        EvalType::Json => json,
        EvalType::VectorFloat32 => vector,
        EvalType::Real => real,
        EvalType::String => {
            bf.tp.set_decimal(UNSPECIFIED_LENGTH);
            string
        }
        EvalType::Datetime | EvalType::Timestamp => {
            bf.tp.set_charset_name(DEFAULT_CHARSET);
            bf.tp.set_collation_name(DEFAULT_COLLATION_NAME);
            bf.tp.set_raw_flags(0);
            time
        }
    };
    Ok(bf)
}

/// Go `anyValueFunctionClass`.
pub(crate) struct AnyValueFunctionClass;

impl FunctionClass for AnyValueFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        use ScalarFuncSig::{
            DecimalAnyValue, DurationAnyValue, IntAnyValue, JsonAnyValue, RealAnyValue,
            StringAnyValue, TimeAnyValue, VectorFloat32AnyValue,
        };
        let sigs = [
            DecimalAnyValue, DurationAnyValue, IntAnyValue, JsonAnyValue, VectorFloat32AnyValue,
            RealAnyValue, StringAnyValue, TimeAnyValue,
        ];
        value_typed_function(ctx, "any_value", args, 0, sigs)
    }
}

function_class!(
    /// Go `inetAtonFunctionClass`.
    InetAtonFunctionClass, Int, [String], InetAton, |bf, _ctx| {
        bf.tp.set_flen(21);
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
    }
);

function_class!(
    /// Go `inetNtoaFunctionClass`.
    InetNtoaFunctionClass, String, [Int], InetNtoa, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(93);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `inet6AtonFunctionClass`.
    Inet6AtonFunctionClass, String, [String], Inet6Aton, |bf, _ctx| {
        bf.tp.set_flen(16);
        set_bin_chs_cln_flag(&mut bf.tp);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `inet6NtoaFunctionClass`.
    Inet6NtoaFunctionClass, String, [String], Inet6Ntoa, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(117);
        bf.tp.set_decimal(0);
    }
);

function_class!(
    /// Go `isFreeLockFunctionClass`.
    IsFreeLockFunctionClass, Int, [String], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `isIPv4FunctionClass`.
    IsIPv4FunctionClass, Int, [String], IsIPv4, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `isIPv4CompatFunctionClass`.
    IsIPv4CompatFunctionClass, Int, [String], IsIPv4Compat, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `isIPv4MappedFunctionClass`.
    IsIPv4MappedFunctionClass, Int, [String], IsIPv4Mapped, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `isIPv6FunctionClass`.
    IsIPv6FunctionClass, Int, [String], IsIPv6, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `isUsedLockFunctionClass`.
    IsUsedLockFunctionClass, Int, [String], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

function_class!(
    /// Go `isUUIDFunctionClass`.
    IsUUIDFunctionClass, Int, [String], IsUuid, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

/// Go `nameConstFunctionClass`.
pub(crate) struct NameConstFunctionClass;

impl FunctionClass for NameConstFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        // Go's `builtinNameConst*Sig` set no signature code.
        value_typed_function(ctx, "name_const", args, 1, [ScalarFuncSig::Unspecified; 8])
    }
}

function_class!(
    /// Go `releaseAllLocksFunctionClass`.
    ReleaseAllLocksFunctionClass, Int, [], Unspecified, |bf, _ctx| {
        bf.tp.set_flen(1);
    }
);

/// Go `defaultFunctionClass` (`"DEFAULT"`) and `uuidShortFunctionClass`
/// (`"UUID_SHORT"`): the names are registered so the parser recognizes them,
/// and building one is `ErrFunctionNotExists`.
pub(crate) struct FunctionNotExistsClass {
    pub(crate) name: &'static str,
}

impl FunctionClass for FunctionNotExistsClass {
    fn get_function(
        &self,
        _: &dyn BuildContext,
        _: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        Err(EvalError::FunctionNotExists(self.name.to_owned()))
    }
}

function_class!(
    /// Go `uuidFunctionClass`.
    UuidFunctionClass, String, [], Uuid, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(36);
    }
);

function_class!(
    /// Go `uuidv4FunctionClass`.
    Uuidv4FunctionClass, String, [], UuiDv4, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(36);
    }
);

function_class!(
    /// Go `uuidv7FunctionClass`.
    Uuidv7FunctionClass, String, [], UuiDv7, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(36);
    }
);

function_class!(
    /// Go `uuidVersionFunctionClass`.
    UuidVersionFunctionClass, Int, [String], UuidVersion, |bf, _ctx| {
        bf.tp.set_flen(10);
    }
);

function_class!(
    /// Go `uuidTimestampFunctionClass`.
    UuidTimestampFunctionClass, Decimal, [String], UuidTimestamp, |bf, _ctx| {
        bf.tp.set_flen(18);
        bf.tp.set_decimal_under_limit(6);
    }
);

function_class!(
    /// Go `vitessHashFunctionClass`.
    VitessHashFunctionClass, Int, [Int], VitessHash, |bf, _ctx| {
        bf.tp.set_flen(20);
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        set_bin_chs_cln_flag(&mut bf.tp);
    }
);

/// Go `uuidToBinFunctionClass`.
pub(crate) struct UuidToBinFunctionClass;

impl FunctionClass for UuidToBinFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = [EvalType::String, EvalType::Int];
        let args_len = args.len();
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "uuid_to_bin", args, EvalType::String, &arg_tps[..args_len])?;
        bf.tp.set_flen(16);
        set_bin_chs_cln_flag(&mut bf.tp);
        bf.tp.set_decimal(0);
        Ok(bf)
    }
}

/// Go `binToUUIDFunctionClass`.
pub(crate) struct BinToUuidFunctionClass;

impl FunctionClass for BinToUuidFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = [EvalType::String, EvalType::Int];
        let args_len = args.len();
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "bin_to_uuid", args, EvalType::String, &arg_tps[..args_len])?;
        set_connection_charset(&mut bf, ctx);
        bf.tp.set_flen(32);
        bf.tp.set_decimal(0);
        Ok(bf)
    }
}

function_class!(
    /// Go `tidbShardFunctionClass`.
    TidbShardFunctionClass, Int, [Int], TiDbShard, |bf, _ctx| {
        bf.tp.set_flen(4);
        bf.tp.add_flags(FieldTypeFlags::UNSIGNED);
        set_bin_chs_cln_flag(&mut bf.tp);
    }
);
