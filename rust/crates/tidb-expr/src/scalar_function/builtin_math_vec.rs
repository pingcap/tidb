// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_math_vec.go`: argument columns and in-place numeric kernels.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_proto::tipb::ScalarFuncSig;

/// Go math `vectorized` methods, keyed by the signature selected at build.
pub(super) fn supported(sig: ScalarFuncSig) -> bool {
    use ScalarFuncSig::*;
    matches!(
        sig,
        AbsInt
            | AbsUInt
            | AbsReal
            | AbsDecimal
            | Sqrt
            | Exp
            | Log1Arg
            | Log2Args
            | Log2
            | Log10
            | Radians
            | Degrees
            | Sign
            | CeilIntToInt
            | CeilIntToDec
            | CeilDecToInt
            | CeilDecToDec
            | CeilReal
            | FloorIntToInt
            | FloorIntToDec
            | FloorDecToInt
            | FloorDecToDec
            | FloorReal
            | Acos
            | Asin
            | Atan1Arg
            | Atan2Args
            | Cos
            | Cot
            | Sin
            | Tan
            | Pow
            | Pi
            | Crc32
            | RoundInt
            | RoundReal
            | RoundDec
            | RoundWithFracInt
            | RoundWithFracReal
            | RoundWithFracDec
            | TruncateInt
            | TruncateUint
            | TruncateReal
            | TruncateDecimal
            | Rand
            | RandWithSeedFirstGen
    )
}

/// Go `builtin{Abs,Round,Floor,Ceil,Truncate,Log,Sqrt,...}Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let sig = function.pb_code();
    if sig == Rand {
        result.resize_float64(input.num_rows(), false);
        for row in 0..input.num_rows() {
            vec_eval::put(
                result,
                row,
                tidb_datatype::EvalType::Real,
                &super::pb_builtin::rand_next(function, ctx)?,
            )?;
        }
        return Ok(());
    }
    let reuse = match sig {
        Pow => Some(1),
        Sign | CeilIntToDec | CeilDecToInt | FloorIntToDec | FloorDecToInt | Pi | Conv | Crc32
        | Rand | RandWithSeedFirstGen => None,
        _ => Some(0),
    };
    vec_eval::map_arguments(function, ctx, input, result, reuse, |values| match sig {
        Rand | RandWithSeedFirstGen => crate::math_fn::eval_rand_values(
            &values,
            ctx,
            Some(std::ptr::from_ref(function) as usize),
            sig == Rand,
        ),
        Conv => crate::math_fn::conv(&values),
        _ => function
            .pb_builtin
            .eval_values(function, values, ctx)
            .map_err(|error| super::math_overflow_error(function, error, ctx)),
    })
}
