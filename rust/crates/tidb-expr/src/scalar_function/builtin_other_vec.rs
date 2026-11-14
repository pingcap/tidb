// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_other_vec.go`: typed user-variable columns, parameters and bits.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};

/// Go variable and BIT_COUNT `vectorized` methods. EvaluatorSuite separately
/// disables vectorization of trees with user-variable side effects, as Go does.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    match function.func_name.lowercase() {
        "bit_count" | "getparam" | "row" => true,
        "getvar" | "setvar" => function.get_static_type().is_some_and(|field| {
            matches!(
                field.eval_type(),
                EvalType::Int | EvalType::Real | EvalType::Decimal | EvalType::String
            )
        }),
        _ => false,
    }
}

/// Go `builtin{BitCount,GetParamString,Set*Var,Get*Var}Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let name = function.func_name.lowercase();
    if name == "row" {
        panic!("builtinRowSig.vecEvalString() should never be called.");
    }
    if name == "bit_count" {
        if let Err(error) =
            vec_eval::eval_into(&function.args[0], EvalType::Int, ctx, input, result)
        {
            if matches!(
                error,
                EvalError::DataOutOfRange { .. } | EvalError::IntOverflow
            ) || matches!(&error, EvalError::Conversion(error) if error.code() == tidb_terror::TerrorCode::new(1690))
            {
                return super::builtin_control_vec::fallback(function, ctx, input, result);
            }
            return Err(error);
        }
        for row in 0..input.num_rows() {
            if !result.is_null(row) {
                let count = result.get_int64(row).count_ones();
                vec_eval::put(result, row, EvalType::Int, &Datum::Int(i64::from(count)))?;
            }
        }
        return Ok(());
    }
    let field = function.get_static_type().expect("variable result type");
    vec_eval::map_arguments(function, ctx, input, result, None, |values| {
        if name == "getparam" {
            return super::class_kernels::getparam(&values, ctx);
        }
        if values.iter().any(Datum::is_null) {
            return Ok(Datum::Null);
        }
        let variable = tidb_goruntime::GoString::from_bytes(
            crate::arg_eval_type::eval_string(&values[0])?.expect("not NULL"),
        )
        .to_utf8_lossy_go()
        .to_lowercase();
        if name == "setvar" {
            let value = match field.eval_type() {
                EvalType::Int => Datum::Int(values[1].get_int64()),
                EvalType::String => Datum::new_collation_string_datum(
                    crate::arg_eval_type::eval_string(&values[1])?.expect("not NULL"),
                    tidb_charset::Collation::from_name(ctx.connection_charset_info().1)
                        .unwrap_or(tidb_charset::Collation::DEFAULT),
                ),
                _ => values[1].clone(),
            };
            ctx.set_uservar(&variable, value.clone());
            return Ok(value);
        }
        let kind = match field.eval_type() {
            EvalType::Int if field.is_unsigned() => "uint",
            EvalType::Int => "int",
            EvalType::Real => "real",
            EvalType::Decimal => "decimal",
            _ => "string",
        };
        super::uservar_as_kind(kind, ctx.get_uservar(&variable).unwrap_or(Datum::Null), ctx)
    })
}
