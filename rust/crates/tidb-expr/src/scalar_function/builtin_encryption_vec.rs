// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_encryption_vec.go`: column staging before cryptographic operations.

use super::{vec_eval, ScalarFunction};
use crate::{builtin_ext::crypto, Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

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
    use ScalarFuncSig::*;
    let n = input.num_rows();
    let name = function.func_name.lowercase();
    // Go's argument columns: RANDOM_BYTES reads an int length, SHA2 an int
    // hash length; every other argument is a string.
    let arg_type = |index: usize| match (name, function.pb_code(), index) {
        ("random_bytes", _, 0) | (_, Sha2, 1) => EvalType::Int,
        _ => EvalType::String,
    };
    let mut bufs = Vec::with_capacity(function.args.len());
    for (index, arg) in function.args.iter().enumerate() {
        bufs.push(vec_eval::argument(arg, arg_type(index), ctx, input)?);
    }
    let cell = |buf: usize, i: usize| {
        vec_eval::value(&bufs[buf], i, arg_type(buf), function.args[buf].static_type())
    };
    // Go `builtinUncompressedLengthSig.vecEvalInt`: NULLs merged into an int
    // result, the rest computed in place.
    if function.pb_code() == UncompressedLength {
        result.resize_int64(n, false);
        result.merge_nulls(&[&bufs[0]]);
        for i in 0..n {
            if result.is_null(i) {
                continue;
            }
            let length = crypto::uncompressed_length(&cell(0, i), ctx)?;
            vec_eval::put(result, i, EvalType::Int, &length)?;
        }
        return Ok(());
    }
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
            Md5 => crypto::hash_unary::<md5::Md5>(&cell(0, i))?,
            Sha1 => crypto::hash_unary::<sha1::Sha1>(&cell(0, i))?,
            Sha2 => crypto::sha2_hash(&cell(0, i), &cell(1, i))?,
            Compress => crypto::compress(&cell(0, i))?,
            Uncompress => crypto::uncompress(&cell(0, i), ctx)?,
            Encode => crypto::sql_encode(&cell(0, i), &cell(1, i))?,
            Decode => crypto::sql_decode(&cell(0, i), &cell(1, i))?,
            _ => match name {
                "password" => crypto::password_hash(&cell(0, i), ctx)?,
                "random_bytes" => crypto::random_bytes(&cell(0, i))?,
                "sm3" => crypto::sm3_hash(&cell(0, i))?,
                _ => unreachable!("not an encryption kernel: {name}"),
            },
        };
        vec_eval::put(result, i, EvalType::String, &value)?;
    }
    Ok(())
}
