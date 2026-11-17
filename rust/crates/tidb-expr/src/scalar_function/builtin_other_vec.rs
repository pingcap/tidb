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
    let domain = function
        .get_static_type()
        .expect("variable result type")
        .eval_type();
    match name {
        "getparam" => get_param_string(function, ctx, input, result),
        "setvar" => set_var(function, domain, ctx, input, result),
        _ => get_var(function, domain, ctx, input, result),
    }
}

/// Go `strings.ToLower(buf0.GetString(i))`.
fn variable_name(column: &Column, row: usize) -> String {
    tidb_goruntime::GoString::from_bytes(column.get_bytes(row).as_ref().to_vec())
        .to_utf8_lossy_go()
        .to_lowercase()
}

/// Go `builtinGetParamStringSig.vecEvalString`.
fn get_param_string(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let n = input.num_rows();
    let idx = vec_eval::argument(&function.args[0], EvalType::Int, ctx, input)?;
    result.reserve_string(n);
    for i in 0..n {
        if idx.is_null(i) {
            result.append_null();
            continue;
        }
        let index = usize::try_from(idx.get_int64(i))
            .map_err(|_| EvalError::ParamIndexExceedParamCounts)?;
        let value = ctx.get_param_value(index)?;
        // Go: a value `ToString` cannot render is NULL.
        match value.sql_string() {
            Ok(text) => result.append_bytes(text.as_bytes()),
            Err(_) => result.append_null(),
        }
    }
    Ok(())
}

/// Go `builtinSet{String,Int,Real,Decimal}VarSig.vecEval*`.
fn set_var(
    function: &ScalarFunction,
    domain: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let vars = tidb_expropt::SessionVarsPropReader
        .get_session_vars(ctx)
        .map_err(crate::builtin_ext::misc::opt_prop_error)?;
    let n = input.num_rows();
    let buf0 = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
    let buf1 = vec_eval::argument(&function.args[1], domain, ctx, input)?;
    crate::chunk_executor::allocate(domain, n, result);
    let collation = tidb_charset::Collation::from_name(ctx.connection_charset_info().1)
        .unwrap_or(tidb_charset::Collation::DEFAULT);
    for i in 0..n {
        if buf0.is_null(i) || buf1.is_null(i) {
            vec_eval::put(result, i, domain, &Datum::Null)?;
            continue;
        }
        let var_name = variable_name(&buf0, i);
        let res = match domain {
            EvalType::String => {
                Datum::new_collation_string_datum(buf1.get_bytes(i).as_ref().to_vec(), collation)
            }
            _ => vec_eval::value(&buf1, i, domain, None),
        };
        vars.set_user_var_val(&tidb_hack::go_to_lower(&var_name), res.clone());
        vec_eval::put(result, i, domain, &res)?;
    }
    Ok(())
}

/// Go `builtinGet{String,Int,Real,Decimal}VarSig.vecEval*`.
fn get_var(
    function: &ScalarFunction,
    domain: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let n = input.num_rows();
    let field = function.get_static_type().expect("variable result type");
    let kind = match domain {
        EvalType::Int if field.is_unsigned() => "uint",
        EvalType::Int => "int",
        EvalType::Real => "real",
        EvalType::Decimal => "decimal",
        _ => "string",
    };
    let buf0 = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
    crate::chunk_executor::allocate(domain, n, result);
    for i in 0..n {
        if buf0.is_null(i) {
            vec_eval::put(result, i, domain, &Datum::Null)?;
            continue;
        }
        let var_name = variable_name(&buf0, i);
        // Go `GetUserVarVal`, then `ToString` / `GetInt64` / `ToFloat64` /
        // `ToDecimal`; an unset variable is NULL.
        let value = match ctx.get_uservar(&var_name) {
            Some(value) => super::uservar_as_kind(kind, value, ctx)?,
            None => Datum::Null,
        };
        vec_eval::put(result, i, domain, &value)?;
    }
    Ok(())
}
