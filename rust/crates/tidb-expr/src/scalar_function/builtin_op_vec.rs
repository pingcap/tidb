// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_op_vec.go`: logical, unary, and NULL/truth predicates.

use super::{vec_eval, Kernel, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_ast::BinaryOp;
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};

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
    let right = vec_eval::argument(&function.args[1], EvalType::Int, ctx, input);
    if op != BinaryOp::LogicXor && (right.is_err() || ctx.warning_count() > before_right) {
        ctx.truncate_warnings(before_left);
        return super::builtin_control_vec::fallback(function, ctx, input, result);
    }
    let right = right?;
    for i in 0..input.num_rows() {
        let a = (!result.is_null(i)).then(|| result.get_int64(i) != 0);
        let b = (!right.is_null(i)).then(|| right.get_int64(i) != 0);
        let value = match (op, a, b) {
            (BinaryOp::LogicAnd, Some(false), _) | (BinaryOp::LogicAnd, _, Some(false)) => {
                Datum::Int(0)
            }
            (BinaryOp::LogicOr, Some(true), _) | (BinaryOp::LogicOr, _, Some(true)) => {
                Datum::Int(1)
            }
            (_, None, _) | (_, _, None) => Datum::Null,
            (BinaryOp::LogicXor, Some(a), Some(b)) => Datum::Int(i64::from(a != b)),
            (_, _, Some(b)) => Datum::Int(i64::from(b)),
        };
        vec_eval::put(result, i, EvalType::Int, &value)?;
    }
    Ok(())
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
    let mut buffer = Column::default();
    if domain == EvalType::Int {
        vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
    } else {
        vec_eval::eval_into(&function.args[0], domain, ctx, input, &mut buffer)?;
        result.resize_int64(input.num_rows(), false);
    }
    for i in 0..input.num_rows() {
        let source = if domain == EvalType::Int {
            &*result
        } else {
            &buffer
        };
        let value = match function.pb_builtin.kernel {
            Kernel::IsNull(_) => Datum::Int(i64::from(source.is_null(i))),
            Kernel::Truth {
                negate,
                keep_null,
                falsity,
                ..
            } => {
                let value = vec_eval::value(source, i, domain, function.args[0].static_type());
                match crate::truthy_of(&value)? {
                    None if keep_null => Datum::Null,
                    None => Datum::Int(0),
                    Some(value) => Datum::Int(i64::from(value ^ negate ^ falsity)),
                }
            }
            _ => unreachable!(),
        };
        vec_eval::put(result, i, EvalType::Int, &value)?;
    }
    Ok(())
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
    vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
    for i in 0..input.num_rows() {
        if result.is_null(i) {
            continue;
        }
        let value = vec_eval::value(result, i, domain, function.args[0].static_type());
        let value =
            crate::ops::eval_unary(op, value, crate::ops::Operand::Expr(&function.args[0]), ctx)?;
        vec_eval::put(result, i, domain, &value)?;
    }
    Ok(())
}
