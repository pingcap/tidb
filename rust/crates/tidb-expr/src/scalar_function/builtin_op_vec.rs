// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_op_vec.go`: logical, unary, and NULL/truth predicates.

use super::{Kernel, ScalarFunction, vec_eval};
use crate::{Columns, EvalError};
use tidb_ast::{BinaryOp, UnaryOp};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{EvalType, MyDecimal};

/// Go `builtinLogic{And,Or,Xor}Sig.vecEvalInt`.
pub(super) fn logic(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let Kernel::Logic(op) = function.pb_builtin.kernel else {
        unreachable!()
    };
    let before_left = ctx.warning_count();
    vec_eval::eval_into(&function.args[0], EvalType::Int, ctx, input, result)?;
    let before_right = ctx.warning_count();
    let right = vec_eval::read_argument(&function.args[1], EvalType::Int, ctx, input);
    if op != BinaryOp::LogicXor && (right.is_err() || ctx.warning_count() > before_right) {
        ctx.truncate_warnings(before_left);
        return super::builtin_control_vec::fallback(function, ctx, input, result);
    }
    let right = right?;
    match op {
        BinaryOp::LogicAnd => logic_columns(right, result, |a, b, pa, pb| {
            (a & b, (pa & pb) | (pa & !a) | (pb & !b))
        }),
        BinaryOp::LogicOr => logic_columns(right, result, |a, b, pa, pb| {
            let value = (pa & a) | (pb & b);
            (value, (pa & pb) | value)
        }),
        BinaryOp::LogicXor => logic_columns(right, result, |a, b, pa, pb| (a ^ b, pa & pb)),
        _ => unreachable!(),
    }
    Ok(())
}

// The left operand occupies the result column, as in Go's logical kernels.
fn logic_columns(
    right: vec_eval::ReadArgument<'_>,
    result: &mut Column,
    logic: impl Fn(bool, bool, bool, bool) -> (bool, bool),
) {
    let (right, selection) = right.source();
    let right = right.raw_cells();
    let values = right.fixed_cells::<8>();
    result.with_fixed_cells_mut(|cells, bitmap| {
        if let Some(selection) = selection {
            for (row, (cell, &source)) in cells.iter_mut().zip(selection).enumerate() {
                let bit = 1 << (row & 7);
                let mask = &mut bitmap[row >> 3];
                let (value, present) = logic(
                    u64::from_ne_bytes(*cell) != 0,
                    u64::from_ne_bytes(values[source]) != 0,
                    *mask & bit != 0,
                    !right.is_null(source),
                );
                if present {
                    *cell = i64::from(value).to_ne_bytes();
                    *mask |= bit;
                } else {
                    *mask &= !bit;
                }
            }
            return;
        }
        for (((out, mask), values), &pb) in cells
            .chunks_mut(8)
            .zip(bitmap)
            .zip(values.chunks(8))
            .zip(right.not_null_bitmap())
        {
            let pa = *mask;
            if pa & pb == u8::MAX {
                for (cell, &value) in out.iter_mut().zip(values) {
                    let (value, _) = logic(
                        u64::from_ne_bytes(*cell) != 0,
                        u64::from_ne_bytes(value) != 0,
                        true,
                        true,
                    );
                    *cell = i64::from(value).to_ne_bytes();
                }
            } else {
                for (index, (cell, &value)) in out.iter_mut().zip(values).enumerate() {
                    let bit = 1 << index;
                    let (value, present) = logic(
                        u64::from_ne_bytes(*cell) != 0,
                        u64::from_ne_bytes(value) != 0,
                        pa & bit != 0,
                        pb & bit != 0,
                    );
                    if present {
                        *cell = i64::from(value).to_ne_bytes();
                        *mask |= bit;
                    } else {
                        *mask &= !bit;
                    }
                }
            }
        }
    });
}

/// Go's bitwise signatures evaluate both Int64 operands, merge nulls, and
/// update every packed row, including the hidden values of null rows.
pub(super) fn bitwise(op: BinaryOp, result: &mut Column, right: &Column) {
    fn update(result: &mut Column, right: &Column, calculate: impl Fn(u64, u64) -> u64) {
        let source = right.raw_cells();
        result.with_fixed_cells_mut(|cells, _| {
            for (cell, other) in cells.iter_mut().zip(source.fixed_cells()) {
                *cell =
                    calculate(u64::from_ne_bytes(*cell), u64::from_ne_bytes(*other)).to_ne_bytes();
            }
        });
    }
    match op {
        BinaryOp::BitAnd => update(result, right, |a, b| a & b),
        BinaryOp::BitOr => update(result, right, |a, b| a | b),
        BinaryOp::BitXor => update(result, right, |a, b| a ^ b),
        BinaryOp::LeftShift => update(result, right, crate::ops::shift_left),
        BinaryOp::RightShift => update(result, right, crate::ops::shift_right),
        _ => unreachable!(),
    }
}

/// Go `builtin*Is{Null,True,False}Sig.vecEvalInt`, `builtinUnaryNot*Sig.vecEvalInt`.
pub(super) fn predicate(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let domain = match function.pb_builtin.kernel {
        Kernel::IsNull(t) | Kernel::Truth { domain: t, .. } => t,
        _ => unreachable!(),
    };
    if domain == EvalType::Int {
        vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
        match function.pb_builtin.kernel {
            Kernel::IsNull(_) => result.with_fixed_cells_mut(|cells, bitmap| {
                for (group, mask) in cells.chunks_mut(8).zip(bitmap) {
                    let present = *mask;
                    for (bit, cell) in group.iter_mut().enumerate() {
                        *cell = i64::from(present & (1 << bit) == 0).to_ne_bytes();
                    }
                    *mask = ((1_u16 << group.len()) - 1) as u8;
                }
            }),
            Kernel::Truth { negate, keep_null, falsity, .. } => {
                let invert = negate ^ falsity;
                result.with_fixed_cells_mut(|cells, bitmap| {
                    for (group, mask) in cells.chunks_mut(8).zip(bitmap) {
                        let present = *mask;
                        for (bit, cell) in group.iter_mut().enumerate() {
                            if present & (1 << bit) != 0 {
                                let value = u64::from_ne_bytes(*cell) != 0;
                                *cell = i64::from(value ^ invert).to_ne_bytes();
                            } else if !keep_null {
                                *cell = [0; 8];
                                *mask |= 1 << bit;
                            }
                        }
                    }
                });
            }
            _ => unreachable!(),
        }
        return Ok(());
    }
    if matches!(function.pb_builtin.kernel, Kernel::IsNull(_)) {
        return vec_eval::with_argument_column(
            &function.args[0],
            domain,
            ctx,
            input,
            |source, selection| {
                let source = source.raw_cells();
                result.resize_int64(input.num_rows(), false);
                result.with_fixed_cells_mut(|cells, _| {
                    if let Some(selection) = selection {
                        for (cell, &row) in cells.iter_mut().zip(selection) {
                            *cell = i64::from(source.is_null(row)).to_ne_bytes();
                        }
                    } else {
                        for (group, &present) in cells.chunks_mut(8).zip(source.not_null_bitmap()) {
                            let mut missing = !present;
                            while missing != 0 {
                                let bit = missing.trailing_zeros() as usize;
                                let Some(cell) = group.get_mut(bit) else {
                                    break;
                                };
                                *cell = 1_i64.to_ne_bytes();
                                missing &= missing - 1;
                            }
                        }
                    }
                });
            },
        );
    }
    let Kernel::Truth {
        negate,
        keep_null,
        falsity,
        ..
    } = function.pb_builtin.kernel
    else {
        unreachable!()
    };
    let invert = negate ^ falsity;
    vec_eval::with_argument_column(
        &function.args[0],
        domain,
        ctx,
        input,
        |source, selection| {
            let source = source.raw_cells();
            result.resize_int64(input.num_rows(), false);
            match domain {
                EvalType::Real => {
                    truth_rows(
                        result,
                        &source,
                        selection,
                        invert,
                        keep_null,
                        |raw| {
                            // Both signs of zero are false. NaNs and infinities are true.
                            u64::from_ne_bytes(*raw) << 1 != 0
                        },
                        |_| [0; 8],
                    )
                }
                EvalType::Decimal => truth_rows(
                    result,
                    &source,
                    selection,
                    invert,
                    keep_null,
                    |raw| !MyDecimal::packed_is_zero(raw),
                    |_| [0; 8],
                ),
                _ => unreachable!("vector truth uses numeric columns"),
            }
        },
    )
}

fn truth_rows<const WIDTH: usize>(
    result: &mut Column,
    source: &tidb_chunk::column::RawCells<'_>,
    selection: Option<&[usize]>,
    invert: bool,
    keep_null: bool,
    is_true: impl Fn(&[u8; WIDTH]) -> bool,
    null_payload: impl Fn(&[u8; WIDTH]) -> [u8; 8],
) {
    let values = source.fixed_cells();
    result.with_fixed_cells_mut(|cells, bitmap| {
        if let Some(selection) = selection {
            for (row, (cell, &index)) in cells.iter_mut().zip(selection).enumerate() {
                if source.is_null(index) {
                    if keep_null {
                        bitmap[row >> 3] &= !(1 << (row & 7));
                        *cell = null_payload(&values[index]);
                    }
                } else {
                    *cell = i64::from(is_true(&values[index]) ^ invert).to_ne_bytes();
                }
            }
            return;
        }
        let presence = source.not_null_bitmap();
        if keep_null {
            bitmap.copy_from_slice(presence);
        }
        for ((cells, values), &present) in cells.chunks_mut(8).zip(values.chunks(8)).zip(presence) {
            if present == u8::MAX {
                for (cell, value) in cells.iter_mut().zip(values) {
                    *cell = i64::from(is_true(value) ^ invert).to_ne_bytes();
                }
            } else {
                for (bit, (cell, value)) in cells.iter_mut().zip(values).enumerate() {
                    if present & (1 << bit) != 0 {
                        *cell = i64::from(is_true(value) ^ invert).to_ne_bytes();
                    } else if keep_null {
                        *cell = null_payload(value);
                    }
                }
            }
        }
    });
}

/// Go `builtinUnaryMinus{Int,Real,Decimal}Sig.vecEval*`, `builtinBitNegSig.vecEvalInt`.
pub(super) fn unary(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let Kernel::Unary(op) = function.pb_builtin.kernel else {
        unreachable!()
    };
    let domain = function.get_static_type().expect("unary type").eval_type();
    let unsigned = op == UnaryOp::Minus
        && domain == EvalType::Int
        && function.args[0]
            .static_type()
            .is_some_and(|field| field.is_unsigned());
    if op == UnaryOp::Minus && (input.sel().is_some() || input.num_rows() <= 64) {
        match domain {
            EvalType::Int => {
                return project_negation(
                    function,
                    ctx,
                    input,
                    result,
                    domain,
                    |result, rows| result.resize_int64(rows, true),
                    |cell, present| {
                        if present {
                            *cell = negate_int(*cell, unsigned)?;
                        }
                        Ok(())
                    },
                );
            }
            EvalType::Real => {
                return project_negation(
                    function,
                    ctx,
                    input,
                    result,
                    domain,
                    |result, rows| result.resize_float64(rows, true),
                    |cell, _| {
                        negate_real(cell);
                        Ok(())
                    },
                );
            }
            EvalType::Decimal => {
                return project_negation(
                    function,
                    ctx,
                    input,
                    result,
                    domain,
                    |result, rows| result.resize_decimal(rows, true),
                    |cell, present| {
                        if present {
                            MyDecimal::negate_packed(cell);
                        }
                        Ok(())
                    },
                );
            }
            _ => (),
        }
    }
    vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
    match (op, domain) {
        (UnaryOp::BitNeg, EvalType::Int) => {
            result.with_fixed_cells_mut(|cells, _| {
                for cell in cells {
                    *cell = (!u64::from_ne_bytes(*cell)).to_ne_bytes();
                }
            });
            return Ok(());
        }
        (UnaryOp::Minus, EvalType::Int) => {
            return if unsigned {
                result.try_map_fixed_rows(|cell| negate_int(cell, true).map(Some))
            } else {
                result.try_map_fixed_rows(|cell| negate_int(cell, false).map(Some))
            };
        }
        (UnaryOp::Minus, EvalType::Real) => {
            result.with_fixed_cells_mut(|cells, _| {
                for cell in cells {
                    negate_real(cell);
                }
            });
            return Ok(());
        }
        (UnaryOp::Minus, EvalType::Decimal) => {
            result.with_fixed_cells_mut(|cells, bitmap| {
                for (row, cell) in cells.iter_mut().enumerate() {
                    if bitmap[row >> 3] & (1 << (row & 7)) != 0 {
                        MyDecimal::negate_packed(cell);
                    }
                }
            });
            return Ok(());
        }
        _ => {}
    }
    unreachable!("fixed-width unary signature")
}

#[inline]
fn negate_real(cell: &mut [u8; 8]) {
    *cell = (u64::from_ne_bytes(*cell) ^ (1 << 63)).to_ne_bytes();
}

#[inline]
fn negate_int(cell: [u8; 8], unsigned: bool) -> Result<[u8; 8], EvalError> {
    let bits = u64::from_ne_bytes(cell);
    let value = bits as i64;
    if if unsigned {
        bits > (1 << 63)
    } else {
        value == i64::MIN
    } {
        return Err(negation_overflow(bits, unsigned));
    }
    Ok(value.wrapping_neg().to_ne_bytes())
}

#[cold]
fn negation_overflow(bits: u64, unsigned: bool) -> EvalError {
    EvalError::DataOutOfRange {
        value: "BIGINT",
        expression: if unsigned {
            format!("-{bits}")
        } else {
            format!("-{}", bits as i64)
        },
    }
}

fn project_negation<const WIDTH: usize>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    domain: EvalType,
    resize: impl FnOnce(&mut Column, usize),
    negate: impl Fn(&mut [u8; WIDTH], bool) -> Result<(), EvalError>,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        domain,
        ctx,
        input,
        |source, selection| {
            let source = source.raw_cells();
            let packed = source.fixed_cells();
            let rows = input.num_rows();
            // Both paths overwrite every cell and presence bit.
            if result.type_size() != WIDTH as i64 || result.raw_cells().data_len() / WIDTH != rows {
                resize(result, rows);
            }
            result.with_fixed_cells_mut(|cells, bitmap| {
                let mut failure = None;
                if let Some(selection) = selection {
                    bitmap.fill(0);
                    for (row, (dest, &index)) in cells.iter_mut().zip(selection).enumerate() {
                        let present = !source.is_null(index);
                        *dest = packed[index];
                        bitmap[row >> 3] |= u8::from(present) << (row & 7);
                        if let Err(error) = negate(dest, present) {
                            failure = Some((row + 1, error));
                            break;
                        }
                    }
                } else {
                    bitmap.copy_from_slice(source.not_null_bitmap());
                    'groups: for (group, ((output, input), &present)) in cells
                        .chunks_mut(8)
                        .zip(packed.chunks(8))
                        .zip(bitmap.iter())
                        .enumerate()
                    {
                        for (bit, (dest, &src)) in output.iter_mut().zip(input).enumerate() {
                            *dest = src;
                            if let Err(error) = negate(dest, present & (1 << bit) != 0) {
                                failure = Some((group * 8 + bit + 1, error));
                                break 'groups;
                            }
                        }
                    }
                }
                if let Some((start, error)) = failure {
                    // Go copies the operand column before negating it. Rows
                    // after an overflow keep their original values and nulls.
                    if let Some(selection) = selection {
                        for (offset, (dest, &index)) in cells[start..]
                            .iter_mut()
                            .zip(&selection[start..])
                            .enumerate()
                        {
                            let row = start + offset;
                            *dest = packed[index];
                            bitmap[row >> 3] |= u8::from(!source.is_null(index)) << (row & 7);
                        }
                    } else {
                        cells[start..].copy_from_slice(&packed[start..]);
                    }
                    return Err(error);
                }
                Ok(())
            })
        },
    )?
}

#[cfg(test)]
mod tests {
    use crate::{
        NoColumns, column::Column as ExpressionColumn, evaluator::EvaluatorSuite,
        expression::Expression, new_function::new_function_base,
    };
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeCode, MyDecimal};

    #[test]
    fn decimal_predicates_and_negation_use_all_stored_words() {
        let decimal = FieldType::new(FieldTypeCode::NewDecimal);
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut signed_zero = MyDecimal::from_string(b"0.000").0.to_raw_bytes();
        signed_zero[3] = 1;
        let mut with_unused_word = signed_zero;
        with_unused_word[39] = 1;
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&decimal), 2);
        for packed in [signed_zero, with_unused_word] {
            input.append_my_decimal(0, &MyDecimal::from_raw_bytes(packed).unwrap());
        }

        let make = |name, output| {
            let mut column = ExpressionColumn::new(1, decimal.clone());
            column.index = 0;
            new_function_base(&NoColumns, name, output, vec![Expression::Column(column)]).unwrap()
        };
        let suite = EvaluatorSuite::new(
            vec![
                make("not", integer.clone()),
                make("unaryminus", decimal.clone()),
            ],
            false,
        );
        let mut output = Chunk::new_chunk_with_capacity(&[integer, decimal], 2);
        suite.run(&NoColumns, &mut input, &mut output).unwrap();
        assert_eq!(output.column(0).get_int64(0), 1);
        assert_eq!(output.column(0).get_int64(1), 0);
        assert_eq!(output.column(1).get_raw(0).as_ref(), &signed_zero);
        with_unused_word[3] = 0;
        assert_eq!(output.column(1).get_raw(1).as_ref(), &with_unused_word);
    }
}
