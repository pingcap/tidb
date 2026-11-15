// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_vec_vec.go`: vector columns and vector datatype operations.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{EvalType, VectorFloat32};
use tidb_proto::tipb::ScalarFuncSig;

/// Go vector builtin `vectorized` methods.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        VecDimsSig
            | VecL1DistanceSig
            | VecL2DistanceSig
            | VecNegativeInnerProductSig
            | VecCosineDistanceSig
            | VecL2NormSig
            | VecAsTextSig
    ) || function.func_name.lowercase() == "vec_from_text"
}

/// Go `builtinVec{Dims,L1Distance,L2Distance,NegativeInnerProduct,CosineDistance,L2Norm,FromText,AsText}Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let n = input.num_rows();
    if function.func_name.lowercase() == "vec_from_text" {
        return from_text(function, ctx, input, result);
    }
    let col1 = vec_eval::argument(&function.args[0], EvalType::VectorFloat32, ctx, input)?;
    match function.pb_code() {
        VecDimsSig => {
            result.resize_int64(n, false);
            result.merge_nulls(&[&col1]);
            for i in 0..n {
                if result.is_null(i) {
                    continue;
                }
                let length = col1.get_vector_float32(i).len() as i64;
                result.with_int64s_mut(|res| res[i] = length);
            }
        }
        VecAsTextSig => {
            result.reserve_string(n);
            for i in 0..n {
                if col1.is_null(i) {
                    result.append_null();
                    continue;
                }
                result.append_bytes(col1.get_vector_float32(i).to_string().as_bytes());
            }
        }
        VecL2NormSig => {
            result.resize_float64(n, false);
            result.merge_nulls(&[&col1]);
            for i in 0..n {
                if result.is_null(i) {
                    continue;
                }
                let d = col1.get_vector_float32(i).l2_norm();
                put_distance(result, i, d);
            }
        }
        sig => {
            let operation = match sig {
                VecL1DistanceSig => VectorFloat32::l1_distance,
                VecL2DistanceSig => VectorFloat32::l2_distance,
                VecNegativeInnerProductSig => VectorFloat32::negative_inner_product,
                VecCosineDistanceSig => VectorFloat32::cosine_distance,
                _ => unreachable!("not a vector kernel: {sig:?}"),
            };
            let col2 = vec_eval::argument(&function.args[1], EvalType::VectorFloat32, ctx, input)?;
            result.resize_float64(n, false);
            result.merge_nulls(&[&col1, &col2]);
            for i in 0..n {
                if result.is_null(i) {
                    continue;
                }
                let x = col1.get_vector_float32(i);
                let y = col2.get_vector_float32(i);
                let d = operation(&x, &y).map_err(|error| EvalError::Vector(error.to_string()))?;
                put_distance(result, i, d);
            }
        }
    }
    Ok(())
}

/// Go: a NaN distance is NULL.
fn put_distance(result: &mut Column, i: usize, d: f64) {
    if d.is_nan() {
        result.set_null(i, true);
        return;
    }
    result.with_float64s_mut(|res| res[i] = d);
}

/// Go `builtinVecFromTextSig.vecEvalVectorFloat32`.
fn from_text(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let n = input.num_rows();
    let buf = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
    let flen = function
        .get_static_type()
        .expect("vector return type")
        .flen();
    result.reserve_vector_float32(n);
    for i in 0..n {
        if buf.is_null(i) {
            result.append_null();
            continue;
        }
        let bytes = buf.get_bytes(i);
        let text = std::str::from_utf8(bytes.as_ref())
            .map_err(|error| EvalError::Vector(error.to_string()))?;
        let vector =
            VectorFloat32::parse(text).map_err(|error| EvalError::Vector(error.to_string()))?;
        vector
            .check_dims_fit_column(usize::try_from(flen).ok())
            .map_err(|error| EvalError::Vector(error.to_string()))?;
        result.append_vector_float32(&vector);
    }
    Ok(())
}
