// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_vec_vec.go`: vector columns and vector datatype operations.

use super::{ScalarFunction, vec_eval};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{EvalType, VectorFloat32, VectorFloat32Ref, peek_vector_float32};
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
    if function.pb_code() == VecAsTextSig {
        return vec_eval::with_argument_column(
            &function.args[0],
            EvalType::VectorFloat32,
            ctx,
            input,
            |column, selection| {
                let cells = column.raw_cells();
                let mut text = String::new();
                result.reserve_string(n);
                for row in 0..n {
                    let row = selection.map_or(row, |selection| selection[row]);
                    if cells.is_null(row) {
                        result.append_null();
                    } else {
                        text.clear();
                        decoded(cells.get(row)).append_text(&mut text);
                        result.append_bytes(text.as_bytes());
                    }
                }
            },
        );
    }

    if matches!(function.pb_code(), VecDimsSig | VecL2NormSig) {
        return vec_eval::with_argument_column(
            &function.args[0],
            EvalType::VectorFloat32,
            ctx,
            input,
            |column, selection| {
                let cells = column.raw_cells();
                let rows = (0..n).map(|row| selection.map_or(row, |selection| selection[row]));
                match function.pb_code() {
                    VecDimsSig => result.write_int64_rows(rows.map(|row| {
                        if cells.is_null(row) {
                            return None;
                        }
                        let length = peek_vector_float32(cells.get(row)).unwrap_or_else(|error| {
                            panic!("invalid VectorFloat32 chunk cell: {error}")
                        });
                        Some((length / 4 - 1) as i64)
                    })),
                    VecL2NormSig => {
                        result.resize_float64(n, false);
                        result.with_fixed_cells_mut::<8, _>(|output, bitmap| {
                            for (i, row) in rows.enumerate() {
                                if cells.is_null(row) {
                                    bitmap[i / 8] &= !(1 << (i % 8));
                                } else {
                                    write_distance(
                                        &mut output[i],
                                        &mut bitmap[i / 8],
                                        1 << (i % 8),
                                        decoded(cells.get(row)).l2_norm(),
                                    );
                                }
                            }
                        });
                    }
                    sig => unreachable!("not a unary vector kernel: {sig:?}"),
                }
            },
        );
    }

    let left = vec_eval::read_argument(&function.args[0], EvalType::VectorFloat32, ctx, input)?;
    let right = vec_eval::read_argument(&function.args[1], EvalType::VectorFloat32, ctx, input)?;
    let (left, left_selection) = left.source();
    let (right, right_selection) = right.source();
    let selected = left_selection.is_some() || right_selection.is_some();
    result.resize_float64(n, false);
    if !selected {
        result.merge_nulls(&[left, right]);
    }
    let left = left.raw_cells();
    let right = right.raw_cells();
    let rows = (0..n).map(|row| {
        (
            left_selection.map_or(row, |selection| selection[row]),
            right_selection.map_or(row, |selection| selection[row]),
        )
    });
    let sig = function.pb_code();
    result.with_fixed_cells_mut::<8, _>(|output, bitmap| {
        if selected {
            for (i, (left_row, right_row)) in rows.clone().enumerate() {
                if left.is_null(left_row) || right.is_null(right_row) {
                    bitmap[i / 8] &= !(1 << (i % 8));
                }
            }
        }
        for (i, (cell, (left_row, right_row))) in output.iter_mut().zip(rows).enumerate() {
            let present = &mut bitmap[i / 8];
            let bit = 1 << (i % 8);
            if *present & bit == 0 {
                continue;
            }
            let x = decoded(left.get(left_row));
            let y = decoded(right.get(right_row));
            let distance = match sig {
                VecL1DistanceSig => x.l1_distance(&y),
                VecL2DistanceSig => x.l2_distance(&y),
                VecNegativeInnerProductSig => x.negative_inner_product(&y),
                VecCosineDistanceSig => x.cosine_distance(&y),
                _ => unreachable!("not a binary vector kernel: {sig:?}"),
            }
            .map_err(|error| EvalError::Vector(error.to_string()))?;
            write_distance(cell, present, bit, distance);
        }
        Ok(())
    })
}

#[inline]
fn decoded(bytes: &[u8]) -> VectorFloat32Ref<'_> {
    VectorFloat32Ref::from_encoded(bytes)
        .unwrap_or_else(|error| panic!("invalid VectorFloat32 chunk cell: {error}"))
}

/// Go: a NaN distance is NULL.
#[inline]
fn write_distance(cell: &mut [u8; 8], present: &mut u8, bit: u8, distance: f64) {
    if distance.is_nan() {
        *present &= !bit;
    } else {
        *cell = distance.to_ne_bytes();
    }
}

/// Go `builtinVecFromTextSig.vecEvalVectorFloat32`.
fn from_text(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let n = input.num_rows();
    let flen = function
        .get_static_type()
        .expect("vector return type")
        .flen();
    let dimensions = usize::try_from(flen).ok();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let get = cells.variable_cells();
            result.reserve_vector_float32(n);
            for row in 0..n {
                let row = selection.map_or(row, |selection| selection[row]);
                if cells.is_null(row) {
                    result.append_null();
                    continue;
                }
                let text = std::str::from_utf8(get(row))
                    .map_err(|error| EvalError::Vector(error.to_string()))?;
                let vector = VectorFloat32::parse(text)
                    .map_err(|error| EvalError::Vector(error.to_string()))?;
                vector.check_dims_fit_column(dimensions)
                    .map_err(|error| EvalError::Vector(error.to_string()))?;
                result.append_vector_float32(&vector);
            }
            Ok(())
        },
    )?
}
