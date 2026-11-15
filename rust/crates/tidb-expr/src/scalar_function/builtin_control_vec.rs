// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_control_vec_generated.go`, `builtin_compare_vec_generated.go`:
//! speculative column evaluation with the owning signature's row fallback.

use super::{vec_eval, Kernel, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};

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
        crate::chunk_executor::allocate(domain, input.num_rows(), result);
        result.set_nulls(0, input.num_rows(), true);
    }
    let mut bookmark = ctx.warning_count();
    let mut buffers = Vec::with_capacity(function.args.len());
    for (index, argument) in function.args.iter().enumerate() {
        let condition = match kernel {
            Kernel::If(_) => index == 0,
            Kernel::Case(_) => index % 2 == 0 && index + 1 < function.args.len(),
            _ => false,
        };
        let arg_type = if condition { EvalType::Int } else { domain };
        let mut buffer = Column::default();
        let destination = if reuse == Some(index) {
            &mut *result
        } else {
            &mut buffer
        };
        let evaluation = vec_eval::eval_into(argument, arg_type, ctx, input, destination);
        // IF's condition and IFNULL's first argument are not speculative.
        let unconditional = index == 0 && matches!(kernel, Kernel::If(_) | Kernel::IfNull(_));
        if unconditional {
            evaluation?;
            bookmark = ctx.warning_count();
        } else if evaluation.is_err() || ctx.warning_count() > bookmark {
            ctx.truncate_warnings(bookmark);
            return fallback(function, ctx, input, result);
        }
        if coalesce_fixed {
            for i in 0..input.num_rows() {
                if result.is_null(i) && !buffer.is_null(i) {
                    let value = vec_eval::value(&buffer, i, domain, argument.static_type());
                    vec_eval::put(result, i, domain, &value)?;
                }
            }
        }
        buffers.push(buffer);
    }
    if coalesce_fixed {
        return Ok(());
    }
    if reuse.is_none() {
        crate::chunk_executor::allocate(domain, input.num_rows(), result);
    }
    for i in 0..input.num_rows() {
        let buffer = |index| {
            if reuse == Some(index) {
                &*result
            } else {
                &buffers[index]
            }
        };
        let condition = |index| !buffer(index).is_null(i) && buffer(index).get_int64(i) != 0;
        let selected = match kernel {
            Kernel::If(_) => Some(if condition(0) { 1 } else { 2 }),
            Kernel::IfNull(_) => Some(if buffer(0).is_null(i) { 1 } else { 0 }),
            Kernel::Case(_) => (0..function.args.len() / 2)
                .find(|j| condition(j * 2))
                .map(|j| j * 2 + 1)
                .or_else(|| (function.args.len() % 2 != 0).then_some(function.args.len() - 1)),
            Kernel::Coalesce(_) => (0..function.args.len()).find(|&j| !buffer(j).is_null(i)),
            _ => unreachable!(),
        };
        let value = selected.map_or(Datum::Null, |index| {
            vec_eval::value(buffer(index), i, domain, function.args[index].static_type())
        });
        vec_eval::put(result, i, domain, &value)?;
    }
    Ok(())
}
