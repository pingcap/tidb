// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_json_vec.go`: typed document, path, and value columns.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

/// Go JSON signature `vectorized` methods (JSON_VALID/SCHEMA_VALID are scalar).
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        JsonTypeSig
            | JsonUnquoteSig
            | JsonLengthSig
            | JsonObjectSig
            | JsonArraySig
            | JsonMergeSig
            | JsonSetSig
            | JsonInsertSig
            | JsonRemoveSig
            | JsonContainsSig
            | JsonMemberOfSig
            | JsonReplaceSig
            | JsonArrayAppendSig
            | JsonMergePatchSig
            | JsonExtractSig
            | JsonMergePreserveSig
            | JsonArrayInsertSig
            | JsonContainsPathSig
            | JsonDepthSig
            | JsonKeysSig
            | JsonKeys2ArgsSig
            | JsonPrettySig
            | JsonQuoteSig
            | JsonSearchSig
            | JsonStorageFreeSig
            | JsonStorageSizeSig
    ) || matches!(
        function.func_name.lowercase(),
        "json_overlaps" | "json_sum_crc32"
    )
}

/// Go `builtinJSON*Sig.vecEval*`, including `vecJSONModify`'s path-before-value order.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    if function.pb_code() == JsonObjectSig {
        return object(function, ctx, input, result);
    }
    let order: Vec<_> = match function.pb_code() {
        JsonSetSig | JsonInsertSig | JsonReplaceSig => std::iter::once(0)
            .chain((1..function.args.len()).step_by(2))
            .chain((2..function.args.len()).step_by(2))
            .collect(),
        _ => (0..function.args.len()).collect(),
    };
    vec_eval::map_arguments_in_order(function, ctx, input, result, None, &order, |values| {
        function.pb_builtin.eval_values(function, values, ctx)
    })
}

/// Go `builtinJSONObjectSig.vecEvalJSON`: check all key columns before
/// constructing any result document, with the first key's NULL error first.
fn object(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let mut buffers = Vec::with_capacity(function.args.len());
    for (i, arg) in function.args.iter().enumerate() {
        buffers.push(vec_eval::argument(
            arg,
            if i % 2 == 0 {
                EvalType::String
            } else {
                EvalType::Json
            },
            ctx,
            input,
        )?);
    }
    result.reserve_json(input.num_rows());
    for i in (0..function.args.len()).step_by(2) {
        for row in 0..input.num_rows() {
            if buffers[i].is_null(row) {
                return Err(EvalError::Conversion(
                    tidb_datatype::types_errors::ERR_JSON_DOCUMENT_NULL_KEY.clone(),
                ));
            }
        }
    }
    for row in 0..input.num_rows() {
        let values = buffers
            .iter()
            .enumerate()
            .map(|(i, buffer)| {
                vec_eval::value(
                    buffer,
                    row,
                    if i % 2 == 0 {
                        EvalType::String
                    } else {
                        EvalType::Json
                    },
                    function.args[i].static_type(),
                )
            })
            .collect();
        let value = function.pb_builtin.eval_values(function, values, ctx)?;
        vec_eval::put(result, row, EvalType::Json, &value)?;
    }
    Ok(())
}
