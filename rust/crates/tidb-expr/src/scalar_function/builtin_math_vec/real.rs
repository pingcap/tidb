// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Packed DOUBLE kernels from pkg/expression/builtin_math_vec.go.

use super::{ScalarFunction, vec_eval};
use crate::math_fn::{GO_MATH_FMA, go_exp_log, go_trig};
use crate::{Columns, EvalError};
use std::f64::consts::{FRAC_PI_2, PI};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn supported(sig: ScalarFuncSig) -> bool {
    use ScalarFuncSig::*;
    matches!(
        sig,
        Acos | Asin
            | Atan1Arg
            | Atan2Args
            | Cos
            | Cot
            | Degrees
            | Exp
            | Log1Arg
            | Log2
            | Log2Args
            | Log10
            | Pow
            | Radians
            | Sin
            | Tan
    )
}

pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    output: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let sig = function.pb_code();
    match sig {
        Atan2Args => binary::<false>(function, ctx, input, output, |y, x| {
            Ok(Some(go_trig::go_atan2(y, x)))
        }),
        Log2Args => binary::<false>(function, ctx, input, output, |base, value| {
            if base <= 0.0 || base == 1.0 || value <= 0.0 {
                warn_invalid_log(ctx);
                Ok(None)
            } else {
                Ok(Some(
                    go_exp_log::log_positive::<GO_MATH_FMA>(value)
                        / go_exp_log::log_positive::<GO_MATH_FMA>(base),
                ))
            }
        }),
        Pow => binary::<true>(function, ctx, input, output, |base, exponent| {
            let power = go_exp_log::pow::<GO_MATH_FMA>(base, exponent);
            if power.is_finite() {
                Ok(Some(power))
            } else {
                Err(EvalError::DataOutOfRange {
                    value: "DOUBLE",
                    expression: format!("pow({}, {})", format_arg(base), format_arg(exponent)),
                })
            }
        }),
        _ => unary(function, sig, ctx, input, output),
    }
}

fn unary(
    function: &ScalarFunction,
    sig: ScalarFuncSig,
    ctx: &dyn Columns,
    input: &Chunk,
    output: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    macro_rules! apply {
        ($compute:expr) => {
            unary_rows::<false>(function, ctx, input, output, $compute)
        };
    }
    match sig {
        Radians => unary_rows::<true>(function, ctx, input, output, |x| Ok(Some(x * (PI / 180.0)))),
        Degrees => unary_rows::<true>(function, ctx, input, output, |x| Ok(Some((x * 180.0) / PI))),
        Sin => apply!(|x| Ok(Some(go_trig::go_sin(x)))),
        Cos => apply!(|x| Ok(Some(go_trig::go_cos(x)))),
        Tan => apply!(|x| Ok(Some(go_trig::go_tan(x)))),
        Atan1Arg => apply!(|x| Ok(Some(go_trig::go_atan(x)))),
        Asin => {
            apply!(|x| { Ok((!outside_unit_interval(x)).then(|| go_trig::asin::<GO_MATH_FMA>(x))) })
        }
        Acos => apply!(|x| {
            Ok((!outside_unit_interval(x)).then(|| FRAC_PI_2 - go_trig::asin::<GO_MATH_FMA>(x)))
        }),
        Log1Arg => apply!(|x| logarithm(ctx, x, go_exp_log::log_positive::<GO_MATH_FMA>)),
        Log2 => apply!(|x| logarithm(ctx, x, go_exp_log::log2::<GO_MATH_FMA>)),
        Log10 => apply!(|x| logarithm(ctx, x, go_exp_log::log10::<GO_MATH_FMA>)),
        Cot => apply!(|x| {
            let tangent = go_trig::tan::<GO_MATH_FMA>(x);
            let value = tangent.recip();
            if value.is_finite() {
                return Ok(Some(value));
            }
            if tangent == 0.0 {
                return Err(cot_overflow(x));
            }
            // Go's vector kernel keeps the original cell in this case.
            Ok(Some(x))
        }),
        Exp => apply!(|x| {
            if GO_MATH_FMA && x.abs() < 700.0 {
                return Ok(Some(go_exp_log::exp_arm64_normal(x)));
            }
            let value = go_exp_log::exp::<GO_MATH_FMA>(x);
            if value.is_finite() {
                return Ok(Some(value));
            }
            let arg = super::super::string_with_ctx(&function.args[0], ctx)
                .unwrap_or_else(|| format_arg(x));
            Err(EvalError::DataOutOfRange {
                value: "DOUBLE",
                expression: format!("exp({arg})"),
            })
        }),
        _ => unreachable!("unary real math signature"),
    }
}

#[inline]
fn outside_unit_interval(value: f64) -> bool {
    value < -1.0 || value > 1.0
}

fn warn_invalid_log(ctx: &dyn Columns) {
    ctx.append_go_warning(
        &crate::expression_errors::ERR_INVALID_ARGUMENT_FOR_LOGARITHM,
        "Invalid argument for logarithm",
    );
}

#[inline]
fn logarithm(
    ctx: &dyn Columns,
    argument: f64,
    compute: impl Fn(f64) -> f64,
) -> Result<Option<f64>, EvalError> {
    if argument <= 0.0 {
        warn_invalid_log(ctx);
        Ok(None)
    } else {
        Ok(Some(compute(argument)))
    }
}

fn format_arg(value: f64) -> String {
    tidb_goruntime::go_strconv::format_float(value, b'f', -1, 64)
}

#[cold]
fn cot_overflow(value: f64) -> EvalError {
    EvalError::DataOutOfRange {
        value: "DOUBLE",
        expression: format!("cot({})", format_arg(value)),
    }
}

fn unary_rows<const ANGLE: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    output: &mut Column,
    compute: impl Fn(f64) -> Result<Option<f64>, EvalError>,
) -> Result<(), EvalError> {
    if input.sel().is_some() {
        return vec_eval::with_argument_column(
            &function.args[0],
            EvalType::Real,
            ctx,
            input,
            |source, selection| {
                let source = source.raw_cells();
                let values = source.fixed_cells();
                output.resize_float64(input.num_rows(), false);
                output.with_fixed_cells_mut(|cells, bitmap| {
                    for (row, dest) in cells.iter_mut().enumerate() {
                        let src = selection.map_or(row, |selection| selection[row]);
                        if !source.is_null(src) {
                            if let Some(value) = compute(f64::from_ne_bytes(values[src]))? {
                                *dest = value.to_ne_bytes();
                                continue;
                            }
                        }
                        bitmap[row >> 3] &= !(1 << (row & 7));
                    }
                    Ok(())
                })
            },
        )?;
    }

    vec_eval::eval_into(&function.args[0], EvalType::Real, ctx, input, output)?;
    if ANGLE && output.null_count() == 0 {
        return output.with_fixed_cells_mut(|cells, _| {
            for cell in cells {
                let value = compute(f64::from_ne_bytes(*cell))?.expect("angle conversion");
                *cell = value.to_ne_bytes();
            }
            Ok(())
        });
    }
    output.try_map_fixed_rows(|cell| Ok(compute(f64::from_ne_bytes(cell))?.map(f64::to_ne_bytes)))
}

fn binary<const REUSE_RIGHT: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    output: &mut Column,
    compute: impl Fn(f64, f64) -> Result<Option<f64>, EvalError>,
) -> Result<(), EvalError> {
    if input.sel().is_some() {
        return vec_eval::with_argument_pair(
            &function.args[0],
            &function.args[1],
            EvalType::Real,
            ctx,
            input,
            |left, right| {
                let left = left.cells();
                let right = right.cells();
                let (left, right) = (left.fixed::<8>(), right.fixed::<8>());
                output.resize_float64(input.num_rows(), false);
                output.with_fixed_cells_mut(|cells, bitmap| {
                    for (row, dest) in cells.iter_mut().enumerate() {
                        if let (Some(&left), Some(&right)) = (left(row), right(row)) {
                            if let Some(value) =
                                compute(f64::from_ne_bytes(left), f64::from_ne_bytes(right))?
                            {
                                *dest = value.to_ne_bytes();
                                continue;
                            }
                        }
                        bitmap[row >> 3] &= !(1 << (row & 7));
                    }
                    Ok(())
                })
            },
        );
    }
    let other = if REUSE_RIGHT {
        let left = vec_eval::read_argument(&function.args[0], EvalType::Real, ctx, input)?;
        vec_eval::eval_into(&function.args[1], EvalType::Real, ctx, input, output)?;
        left
    } else {
        vec_eval::eval_into(&function.args[0], EvalType::Real, ctx, input, output)?;
        vec_eval::read_argument(&function.args[1], EvalType::Real, ctx, input)?
    }
    .into_contiguous();
    output.merge_nulls(&[&other]);
    let other = other.raw_cells();
    output.with_fixed_cells_mut(|cells, bitmap| {
        for (row, (result, &input)) in cells.iter_mut().zip(other.fixed_cells()).enumerate() {
            let bit = 1 << (row & 7);
            let mask = &mut bitmap[row >> 3];
            if *mask & bit == 0 {
                continue;
            }
            let in_result = f64::from_ne_bytes(*result);
            let separate = f64::from_ne_bytes(input);
            let computed = if REUSE_RIGHT {
                compute(separate, in_result)?
            } else {
                compute(in_result, separate)?
            };
            if let Some(value) = computed {
                *result = value.to_ne_bytes();
            } else {
                *mask &= !bit;
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        NoColumns,
        column::Column as ExpressionColumn,
        evaluator::{EvaluatorError, EvaluatorSuite},
        expression::Expression,
        new_function::new_function_base,
    };
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn eval_real(name: &str, value: f64) -> Result<f64, EvaluatorError> {
        let field = FieldType::new(FieldTypeCode::Double);
        let mut column = ExpressionColumn::new(1, field.clone());
        column.index = 0;
        column.orig_name = "x".to_owned();
        let function = new_function_base(
            &NoColumns,
            name,
            field.clone(),
            vec![Expression::Column(column)],
        )
        .unwrap();
        let suite = EvaluatorSuite::new(vec![function], false);
        assert!(suite.vectorizable());
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&field), 1);
        let mut output = Chunk::new_chunk_with_capacity(&[field], 1);
        input.append_float64(0, value);
        suite.run(&NoColumns, &mut input, &mut output)?;
        let column = output.column(0);
        assert!(!column.is_null(0));
        Ok(column.get_float64(0))
    }

    #[test]
    fn vector_exp_overflow_names_the_expression() {
        let error = eval_real("exp", 1_000.0).unwrap_err();
        let EvaluatorError::Eval(EvalError::DataOutOfRange { value, expression }) = error else {
            panic!("unexpected overflow error: {error:?}");
        };
        assert_eq!(value, "DOUBLE");
        assert_eq!(expression, "exp(x)");
    }

    #[test]
    fn vector_real_operations_keep_go_special_values() {
        assert_eq!(
            eval_real("sin", f64::INFINITY).unwrap().to_bits(),
            crate::math_fn::GO_NAN.to_bits()
        );
        let signaling_nan = f64::from_bits(0x7ff0_0000_0000_0011);
        let quiet_nan_bits = 0x7ff8_0000_0000_0011;
        assert_eq!(
            eval_real("atan", signaling_nan).unwrap().to_bits(),
            quiet_nan_bits
        );
        let subnormal = f64::from_bits(1);
        assert_eq!(eval_real("cot", subnormal).unwrap().to_bits(), 1);
        assert_eq!(eval_real("log2", subnormal).unwrap(), -1074.0);
    }
}
