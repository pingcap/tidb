// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_encryption_vec.go`: column staging before cryptographic operations.

use super::{vec_eval, ScalarFunction};
use crate::{builtin_ext::crypto, Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

mod digest;
mod zlib;

/// Go encryption signatures' `vectorized` methods.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        AesEncrypt
            | AesEncryptIv
            | AesDecrypt
            | AesDecryptIv
            | Md5
            | Sha1
            | Sha2
            | Compress
            | Uncompress
            | UncompressedLength
            | Encode
            | Decode
    ) || matches!(
        function.func_name.lowercase(),
        "password" | "random_bytes" | "sm3" | "validate_password_strength"
    )
}

/// Go `builtin{Aes,MD5,SHA1,SHA2,Compress,Password,...}Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    match function.pb_code() {
        Md5 => return digest::unary(function, ctx, input, result, crypto::md5_bytes),
        Sha1 => return digest::unary(function, ctx, input, result, crypto::sha1_bytes),
        Sha2 => return digest::sha2(function, ctx, input, result),
        Compress => return zlib::compress(function, ctx, input, result),
        Uncompress => return zlib::uncompress(function, ctx, input, result),
        UncompressedLength => return zlib::length(function, ctx, input, result),
        _ if function.func_name.lowercase() == "sm3" => {
            return digest::unary(function, ctx, input, result, tidb_parser::auth::sm3_hash);
        }
        _ => {}
    }
    if let Some(mode) = function.aes_mode() {
        let iv = mode.iv_required();
        if !iv && input.num_rows() == 0 {
            result.reset_for_eval_type(EvalType::String);
            return Ok(());
        }
        let mut buffers = Vec::new();
        for arg in function.args.iter().take(if iv { 3 } else { 2 }) {
            buffers.push(vec_eval::argument(arg, EvalType::String, ctx, input)?);
        }
        result.reserve_string(input.num_rows());
        let name = if matches!(
            function.pb_code(),
            ScalarFuncSig::AesEncrypt | ScalarFuncSig::AesEncryptIv
        ) {
            "aes_encrypt"
        } else {
            "aes_decrypt"
        };
        for row in 0..input.num_rows() {
            let get = |index: usize| {
                Ok(vec_eval::value(
                    &buffers[index],
                    row,
                    EvalType::String,
                    function.args[index].static_type(),
                ))
            };
            let value = if iv {
                crypto::aes_iv(name, mode, get)?
            } else {
                crypto::aes(name, mode, function.args.len(), get, ctx)?
            };
            vec_eval::put(result, row, EvalType::String, &value)?;
        }
        return Ok(());
    }
    if function.func_name.lowercase() == "validate_password_strength" {
        // Go `vecEvalInt`: no length shortcut, unlike `evalInt`.
        let user = crypto::password_validation_user(ctx)?;
        let vars = tidb_expropt::SessionVarsPropReader
            .get_session_vars(ctx)
            .map_err(crate::builtin_ext::misc::opt_prop_error)?;
        let buffer = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
        result.resize_int64(input.num_rows(), false);
        result.merge_nulls(&[&buffer]);
        let globals = crypto::EvalGlobalVars(&*vars);
        let enabled = crypto::password_validation_enabled(&globals)?;
        for row in 0..input.num_rows() {
            if result.is_null(row) {
                continue;
            }
            let value = if enabled {
                crypto::validate_password_strength_with_user(
                    &vec_eval::value(
                        &buffer,
                        row,
                        EvalType::String,
                        function.args[0].static_type(),
                    ),
                    &globals,
                    user.as_ref(),
                )?
            } else {
                Datum::Int(0)
            };
            vec_eval::put(result, row, EvalType::Int, &value)?;
        }
        return Ok(());
    }
    let n = input.num_rows();
    let name = function.func_name.lowercase();
    // RANDOM_BYTES reads an int length. The other remaining arguments are strings.
    let arg_type = if name == "random_bytes" {
        EvalType::Int
    } else {
        EvalType::String
    };
    let mut bufs = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        bufs.push(vec_eval::argument(arg, arg_type, ctx, input)?);
    }
    let cell = |buf: usize, i: usize| {
        vec_eval::value(&bufs[buf], i, arg_type, function.args[buf].static_type())
    };
    result.reserve_string(n);
    for i in 0..n {
        if bufs.iter().any(|buf| buf.is_null(i)) {
            // Go `builtinPasswordSig.vecEvalString`: NULL hashes to "".
            if name == "password" {
                result.append_bytes(b"");
            } else {
                result.append_null();
            }
            continue;
        }
        let value = match function.pb_code() {
            Encode => crypto::sql_encode(&cell(0, i), &cell(1, i))?,
            Decode => crypto::sql_decode(&cell(0, i), &cell(1, i))?,
            _ => match name {
                "password" => crypto::password_hash(&cell(0, i), ctx)?,
                "random_bytes" => crypto::random_bytes(&cell(0, i))?,
                _ => unreachable!("not an encryption kernel: {name}"),
            },
        };
        vec_eval::put(result, i, EvalType::String, &value)?;
    }
    Ok(())
}
