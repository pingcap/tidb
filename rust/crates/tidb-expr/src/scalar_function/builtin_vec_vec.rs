// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_vec_vec.go`: vector columns and vector datatype operations.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::Datum;
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
    vec_eval::map_arguments(function, ctx, input, result, None, |values| {
        let value = function.pb_builtin.eval_values(function, values, ctx)?;
        if let Datum::VectorFloat32(vector) = &value {
            let flen = function
                .get_static_type()
                .expect("vector return type")
                .flen();
            vector
                .check_dims_fit_column(usize::try_from(flen).ok())
                .map_err(|error| EvalError::Vector(error.to_string()))?;
        }
        Ok(value)
    })
}
