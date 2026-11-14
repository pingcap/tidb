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
        let user = crypto::password_validation_user(ctx);
        let buffer = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
        result.resize_int64(input.num_rows(), false);
        result.merge_nulls(&[&buffer]);
        let enabled = crypto::password_validation_enabled(ctx)?;
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
                    ctx,
                    user.as_ref(),
                )?
            } else {
                Datum::Int(0)
            };
            vec_eval::put(result, row, EvalType::Int, &value)?;
        }
        return Ok(());
    }
    vec_eval::map_arguments(function, ctx, input, result, None, |values| {
        if function.func_name.lowercase() == "password" && values[0].is_null() {
            return Ok(Datum::new_string_datum(""));
        }
        function.pb_builtin.eval_values(function, values, ctx)
    })
}
