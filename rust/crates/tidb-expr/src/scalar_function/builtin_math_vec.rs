// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_math_vec.go`: argument columns and in-place numeric kernels.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{DecimalError, EvalType, FieldType};
use tidb_proto::tipb::ScalarFuncSig;

mod real;

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
    if real::supported(sig) {
        return real::eval(function, ctx, input, result);
    }
    if sig == Crc32 {
        return vec_eval::with_argument_column(
            &function.args[0],
            EvalType::String,
            ctx,
            input,
            |column, selection| {
                let cells = column.raw_cells();
                let get = cells.variable_cells();
                let hash = |row| (!cells.is_null(row)).then(|| i64::from(crc32fast::hash(get(row))));
                if let Some(selection) = selection {
                    result.write_int64_rows(selection.iter().copied().map(hash));
                } else {
                    result.write_int64_rows((0..column.rows()).map(hash));
                }
            },
        );
    }
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
    if matches!(sig, CeilDecToInt | FloorDecToInt) {
        return dec_to_int(function, sig == CeilDecToInt, ctx, input, result);
    }
    if let [arg] = function.args.as_slice() {
        let domain = arg.static_type().map(FieldType::eval_type);
        match (sig, domain) {
            (AbsUInt | RoundInt | CeilIntToInt | FloorIntToInt, Some(EvalType::Int)) => {
                return vec_eval::eval_into(arg, EvalType::Int, ctx, input, result);
            }
            (AbsInt, Some(EvalType::Int)) => {
                vec_eval::eval_into(arg, EvalType::Int, ctx, input, result)?;
                return result.try_map_fixed_rows(|cell| {
                    let value = i64::from_ne_bytes(cell);
                    let abs = value
                        .checked_abs()
                        .ok_or_else(|| EvalError::DataOutOfRange {
                            value: "BIGINT",
                            expression: format!("abs({value})"),
                        })?;
                    Ok(Some(abs.to_ne_bytes()))
                });
            }
            (AbsReal | CeilReal | FloorReal | RoundReal | Sqrt, Some(EvalType::Real)) => {
                vec_eval::eval_into(arg, EvalType::Real, ctx, input, result)?;
                match sig {
                    AbsReal => result.map_float64_rows(|value| Some(value.abs())),
                    CeilReal => result.map_float64_rows(|value| Some(value.ceil())),
                    FloorReal => result.map_float64_rows(|value| Some(value.floor())),
                    RoundReal => result.map_float64_rows(|value| {
                        Some(crate::math_fn::go_round_float(value, 0))
                    }),
                    Sqrt => result.map_float64_rows(|value| {
                        if value < 0.0 {
                            None
                        } else {
                            Some(value.sqrt())
                        }
                    }),
                    _ => unreachable!("real unary math signature"),
                }
                return Ok(());
            }
            (Sign, Some(EvalType::Real)) => return sign(function, ctx, input, result),
            _ => {}
        }
    }
    let reuse = match sig {
        Pow => Some(1),
        Sign | CeilIntToDec | CeilDecToInt | FloorIntToDec | FloorDecToInt | Pi | Conv
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

/// Go `builtin{Ceil,Floor}DecToIntSig.vecEvalInt` adjusts a truncated result
/// one step away from zero when required by its rounding direction.
fn dec_to_int(
    function: &ScalarFunction,
    ceiling: bool,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let n = input.num_rows();
    let mut overflow = false;
    vec_eval::with_argument_column(&function.args[0], EvalType::Decimal, ctx, input, |column, sel| {
        result.write_int64_rows((0..n).map(|row| {
            let row = sel.map_or(row, |sel| sel[row]);
            if column.is_null(row) {
                return None;
            }
            if overflow {
                // Go initialized the untouched result rows to zero and merged
                // the source null bitmap before attempting any conversions.
                return Some(0);
            }
            let decimal = column.get_my_decimal(row);
            let (mut value, error) = decimal.to_int();
            match error {
                Some(DecimalError::Overflow) => overflow = true,
                Some(DecimalError::Truncated) if ceiling && !decimal.is_negative() => {
                    value = value.wrapping_add(1);
                }
                Some(DecimalError::Truncated) if !ceiling && decimal.is_negative() => {
                    value = value.wrapping_sub(1);
                }
                _ => {}
            }
            Some(value)
        }));
    })?;
    if overflow {
        return Err(EvalError::Conversion(tidb_datatype::types_errors::ERR_OVERFLOW.clone()));
    }
    Ok(())
}

fn sign(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(&function.args[0], EvalType::Real, ctx, input, |column, sel| {
        let cells = column.raw_cells();
        result.write_int64_rows((0..input.num_rows()).map(|row| {
            let row = sel.map_or(row, |sel| sel[row]);
            (!cells.is_null(row)).then(|| {
                let value = f64::from_ne_bytes(
                    cells.get(row).try_into().expect("Float64 cell width"),
                );
                crate::math_fn::sign_of_real(value)
            })
        }));
    })
}
