// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_control_vec_generated.go`, `builtin_compare_vec_generated.go`:
//! speculative column evaluation with the owning signature's row fallback.

use super::{Kernel, ScalarFunction, vec_eval};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{EvalType, MYDECIMAL_STRUCT_SIZE};

/// Go `builtin{If,IfNull,CaseWhen,Coalesce}*Sig.fallbackEval*`.
pub(super) fn fallback(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let domain = function
        .get_static_type()
        .expect("control type")
        .eval_type();
    crate::chunk_executor::allocate(domain, input.num_rows(), result);
    fallback_rows(function, ctx, input, result, domain)
}

fn fallback_rows(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    domain: EvalType,
) -> Result<(), EvalError> {
    for i in 0..input.num_rows() {
        vec_eval::put(result, i, domain, &function.eval(ctx, input.get_row(i))?)?;
    }
    Ok(())
}

/// Go `builtin{If,IfNull,CaseWhen,Coalesce}*Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let kernel = function.pb_builtin.kernel;
    let domain = function
        .get_static_type()
        .expect("control type")
        .eval_type();
    let variable = matches!(
        domain,
        EvalType::String | EvalType::Json | EvalType::VectorFloat32
    );
    let reuse = if variable {
        None
    } else {
        match kernel {
            Kernel::If(_) => Some(1),
            Kernel::IfNull(_) => Some(0),
            _ => None,
        }
    };
    let coalesce_fixed = !variable && matches!(kernel, Kernel::Coalesce(_));
    if coalesce_fixed {
        let rows = input.num_rows();
        match domain {
            EvalType::Int => result.resize_int64(rows, true),
            EvalType::Real => result.resize_float64(rows, true),
            EvalType::Decimal => result.resize_decimal(rows, true),
            EvalType::Datetime | EvalType::Timestamp => result.resize_time(rows, true),
            EvalType::Duration => result.resize_go_duration(rows, true),
            _ => unreachable!(),
        }
    }
    let mut bookmark = ctx.warning_count();
    let mut buffers = if coalesce_fixed {
        Vec::new()
    } else {
        Vec::with_capacity(function.args.len())
    };
    for (index, argument) in function.args.iter().enumerate() {
        let condition = match kernel {
            Kernel::If(_) => index == 0,
            Kernel::Case(_) => index % 2 == 0 && index + 1 < function.args.len(),
            _ => false,
        };
        let arg_type = if condition { EvalType::Int } else { domain };
        let evaluation = if reuse == Some(index) {
            vec_eval::eval_into(argument, arg_type, ctx, input, result).map(|()| None)
        } else {
            vec_eval::read_argument(argument, arg_type, ctx, input).map(Some)
        };
        // IF's condition and IFNULL's first argument are not speculative.
        let unconditional = index == 0 && matches!(kernel, Kernel::If(_) | Kernel::IfNull(_));
        let evaluated = if unconditional {
            let evaluated = evaluation?;
            bookmark = ctx.warning_count();
            evaluated
        } else {
            match evaluation {
                Ok(evaluated) if ctx.warning_count() <= bookmark => evaluated,
                _ => {
                    ctx.truncate_warnings(bookmark);
                    if coalesce_fixed {
                        // Go's fixed COALESCE fallback writes into the
                        // result it has already populated from earlier
                        // arguments. A row error can leave later cells intact.
                        return fallback_rows(function, ctx, input, result, domain);
                    }
                    return fallback(function, ctx, input, result);
                }
            }
        };
        if coalesce_fixed {
            let cells = evaluated.as_ref().expect("COALESCE argument").cells();
            copy_fixed_argument(result, domain, |_, is_null| is_null, &cells);
        } else {
            buffers.push(evaluated);
        }
    }
    if coalesce_fixed {
        return Ok(());
    }
    let argument = |index: usize| buffers[index].as_ref().expect("argument buffer").cells();
    match reuse {
        Some(1) => {
            let condition = argument(0);
            let predicate = condition.fixed::<8>();
            copy_fixed_argument(
                result,
                domain,
                |row, _| !predicate(row).is_some_and(nonzero),
                &argument(2),
            );
            return Ok(());
        }
        Some(0) => {
            copy_fixed_argument(result, domain, |_, is_null| is_null, &argument(1));
            return Ok(());
        }
        _ => {}
    }
    let cells: Vec<_> = (0..buffers.len()).map(argument).collect();
    let rows = input.num_rows();
    crate::chunk_executor::allocate(domain, rows, result);
    match kernel {
        Kernel::If(_) => {
            let condition = cells[0].fixed::<8>();
            write_rows(result, domain, rows, |row| {
                let index = if condition(row).is_some_and(nonzero) {
                    1
                } else {
                    2
                };
                cells[index].bytes(row)
            });
        }
        Kernel::IfNull(_) => write_rows(result, domain, rows, |row| {
            cells[0].bytes(row).or_else(|| cells[1].bytes(row))
        }),
        Kernel::Case(_) => {
            let pairs = cells.chunks_exact(2);
            let otherwise = pairs.remainder().first();
            let branches: Vec<_> = pairs.map(|pair| (pair[0].fixed::<8>(), &pair[1])).collect();
            write_rows(result, domain, rows, |row| {
                branches
                    .iter()
                    .find(|(condition, _)| condition(row).is_some_and(nonzero))
                    .map(|(_, value)| *value)
                    .or(otherwise)
                    .and_then(|value| value.bytes(row))
            });
        }
        Kernel::Coalesce(_) => write_rows(result, domain, rows, |row| {
            cells.iter().find_map(|input| input.bytes(row))
        }),
        _ => unreachable!(),
    }
    Ok(())
}

fn nonzero(value: &[u8; 8]) -> bool {
    i64::from_ne_bytes(*value) != 0
}

fn write_rows<'a>(
    result: &mut Column,
    domain: EvalType,
    rows: usize,
    select: impl Fn(usize) -> Option<&'a [u8]>,
) {
    match domain {
        EvalType::String | EvalType::Json | EvalType::VectorFloat32 => result
            .append_byte_rows((0..rows).map(|row| select(row).map(|cell| (cell.len(), [cell])))),
        EvalType::Decimal => copy_fixed_rows::<MYDECIMAL_STRUCT_SIZE>(result, |_, _| true, select),
        _ => copy_fixed_rows::<8>(result, |_, _| true, select),
    }
}

fn copy_fixed_argument(
    result: &mut Column,
    domain: EvalType,
    replace: impl Fn(usize, bool) -> bool,
    source: &vec_eval::ReadArgumentCells<'_>,
) {
    if domain == EvalType::Decimal {
        let cells = source.fixed::<MYDECIMAL_STRUCT_SIZE>();
        copy_fixed_rows::<MYDECIMAL_STRUCT_SIZE>(result, replace, |row| {
            cells(row).map(|cell| cell.as_slice())
        });
    } else {
        let cells = source.fixed::<8>();
        copy_fixed_rows::<8>(result, replace, |row| {
            cells(row).map(|cell| cell.as_slice())
        });
    }
}

/// Go's control signatures select cells after typed argument evaluation.
/// Copy the carrier without reinterpreting the branch's field metadata.
fn copy_fixed_rows<'a, const N: usize>(
    result: &mut Column,
    replace: impl Fn(usize, bool) -> bool,
    source: impl Fn(usize) -> Option<&'a [u8]>,
) {
    result.with_fixed_cells_mut::<N, _>(|cells, bitmap| {
        for (row, cell) in cells.iter_mut().enumerate() {
            let bit = 1 << (row & 7);
            let mask = &mut bitmap[row >> 3];
            if !replace(row, *mask & bit == 0) {
                continue;
            }
            if let Some(value) = source(row) {
                cell.copy_from_slice(value);
                *mask |= bit;
            } else {
                *mask &= !bit;
            }
        }
    });
}
