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

//! `pkg/expression/builtin_encryption.go`: the encryption function
//! classes.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::builtin_miscellaneous::{function_class, set_connection_charset};
use crate::expression::Expression;
use crate::wrap_cast::{set_bin_chs_cln_flag, type_of, MAX_BLOB_WIDTH};
use crate::EvalError;
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

/// Go `aes.BlockSize`.
const AES_BLOCK_SIZE: i64 = 16;
/// Go `mysql.PWDHashLen`.
const PWD_HASH_LEN: i64 = 40;

/// Go `aesDecryptFunctionClass.getFunction` and `aesEncryptFunctionClass
/// .getFunction`: every argument is a string, the result is binary, and
/// the statement's block encryption mode chooses the signature. A build
/// without a statement reads the default mode.
fn aes_function(
    ctx: &dyn BuildContext,
    func_name: &'static str,
    args: Vec<Expression>,
    flen: fn(i64) -> i64,
    sig: ScalarFuncSig,
    iv_sig: ScalarFuncSig,
) -> Result<BaseBuiltinFunc, EvalError> {
    let arg_tps = vec![EvalType::String; args.len()];
    let mut bf = new_base_builtin_func_with_tp(ctx, func_name, args, EvalType::String, &arg_tps)?;
    bf.tp.set_flen(flen(type_of(&bf.args[0]).flen())); // At most.
    set_bin_chs_cln_flag(&mut bf.tp);
    let mode = ctx
        .columns()
        .map(|columns| columns.block_encryption_mode())
        .unwrap_or_default();
    bf.pb_code = if mode.iv_required() {
        if bf.args.len() != 3 {
            return Err(EvalError::WrongParameterCount(func_name));
        }
        iv_sig
    } else {
        sig
    };
    Ok(bf)
}

/// Go `aesDecryptFunctionClass`.
pub(crate) struct AesDecryptFunctionClass;

impl FunctionClass for AesDecryptFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        aes_function(
            ctx,
            "aes_decrypt",
            args,
            |flen| flen,
            ScalarFuncSig::AesDecrypt,
            ScalarFuncSig::AesDecryptIv,
        )
    }
}

/// Go `aesEncryptFunctionClass`.
pub(crate) struct AesEncryptFunctionClass;

impl FunctionClass for AesEncryptFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        aes_function(
            ctx,
            "aes_encrypt",
            args,
            |flen| AES_BLOCK_SIZE * (flen / AES_BLOCK_SIZE + 1),
            ScalarFuncSig::AesEncrypt,
            ScalarFuncSig::AesEncryptIv,
        )
    }
}

function_class!(
    /// Go `decodeFunctionClass`.
    DecodeFunctionClass, String, [String, String], Decode, |bf, _ctx| {
        // Go reads `args[0]` after `newBaseBuiltinFuncWithTp` cast it in place.
        let flen = type_of(&bf.args[0]).flen();
        bf.tp.set_flen(flen);
    }
);

function_class!(
    /// Go `encodeFunctionClass`.
    EncodeFunctionClass, String, [String, String], Encode, |bf, _ctx| {
        // Go reads `args[0]` after `newBaseBuiltinFuncWithTp` cast it in place.
        let flen = type_of(&bf.args[0]).flen();
        bf.tp.set_flen(flen);
    }
);

function_class!(
    /// Go `passwordFunctionClass`.
    PasswordFunctionClass, String, [String], Password, |bf, _ctx| {
        bf.tp.set_flen(PWD_HASH_LEN + 1);
    }
);

function_class!(
    /// Go `randomBytesFunctionClass`.
    RandomBytesFunctionClass, String, [Int], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::random_bytes), |bf, _ctx| {
        bf.tp.set_flen(1024); // Max allowed random bytes
        set_bin_chs_cln_flag(&mut bf.tp);
    }
);

function_class!(
    /// Go `md5FunctionClass`.
    Md5FunctionClass, String, [String], Md5, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(32);
    }
);

function_class!(
    /// Go `sha1FunctionClass`.
    Sha1FunctionClass, String, [String], Sha1, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(40);
    }
);

function_class!(
    /// Go `sha2FunctionClass`.
    Sha2FunctionClass, String, [String, Int], Sha2, |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(128); // sha512
    }
);

function_class!(
    /// Go `sm3FunctionClass`.
    Sm3FunctionClass, String, [String], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::sm3), |bf, ctx| {
        set_connection_charset(bf, ctx);
        bf.tp.set_flen(40);
    }
);

function_class!(
    /// Go `compressFunctionClass`.
    CompressFunctionClass, String, [String], Compress, |bf, _ctx| {
        let src_len = type_of(&bf.args[0]).flen();
        let compress_bound =
            (src_len + (src_len >> 12) + (src_len >> 14) + (src_len >> 25) + 13).min(MAX_BLOB_WIDTH);
        bf.tp.set_flen(compress_bound);
        set_bin_chs_cln_flag(&mut bf.tp);
    }
);

function_class!(
    /// Go `uncompressFunctionClass`.
    UncompressFunctionClass, String, [String], Uncompress, |bf, _ctx| {
        bf.tp.set_flen(MAX_BLOB_WIDTH);
        set_bin_chs_cln_flag(&mut bf.tp);
    }
);

function_class!(
    /// Go `uncompressedLengthFunctionClass`.
    UncompressedLengthFunctionClass, Int, [String], UncompressedLength, |bf, _ctx| {
        bf.tp.set_flen(10);
    }
);

function_class!(
    /// Go `validatePasswordStrengthFunctionClass`.
    ValidatePasswordStrengthFunctionClass, Int, [String], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::validate_password_strength), |bf, _ctx| {
        bf.tp.set_flen(21);
    }
);
