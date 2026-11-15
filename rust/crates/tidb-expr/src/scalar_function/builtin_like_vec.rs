// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_like_vec.go`: complete argument columns and a private pattern.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::Datum;
use tidb_proto::tipb::ScalarFuncSig;

/// Go `builtinLikeSig.vectorized`.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    matches!(
        function.pb_code(),
        ScalarFuncSig::LikeSig | ScalarFuncSig::IlikeSig
    )
}

/// Go `builtinLikeSig.vecEvalInt`: compile each row's pattern locally,
/// without mutating the scalar signature's shared cached pattern.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    if function.pb_code() == ScalarFuncSig::IlikeSig {
        return ilike(function, ctx, input, result);
    }
    use tidb_datatype::EvalType;
    let n = input.num_rows();
    let buf_val = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
    let buf_pattern = vec_eval::argument(&function.args[1], EvalType::String, ctx, input)?;
    let buf_escape = vec_eval::argument(&function.args[2], EvalType::Int, ctx, input)?;
    result.resize_int64(n, false);
    result.merge_nulls(&[&buf_val, &buf_pattern, &buf_escape]);
    for i in 0..n {
        if result.is_null(i) {
            continue;
        }
        // Go compiles into its own `collator().Pattern()`, never `b.pattern`.
        let pattern = crate::like::CompiledLikePattern::new(
            &buf_pattern.get_bytes(i),
            buf_escape.get_int64(i) as u8,
            function.derived_collation(),
        );
        let matched = i64::from(pattern.is_match(&buf_val.get_bytes(i)));
        result.with_int64s_mut(|i64s| i64s[i] = matched);
    }
    Ok(())
}

/// Go `builtinIlikeSig.vecEvalInt`, `buildStringParam`, and `getEscape`:
/// contextual constants are read once and a constant NULL stops later arguments.
fn ilike(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use crate::expression::ConstLevel;
    use tidb_chunk::row::Row;
    use tidb_datatype::EvalType;
    let mut buffers = Vec::with_capacity(2);
    for arg in &function.args[..2] {
        if arg.const_level() >= ConstLevel::ONLY_IN_CONTEXT {
            let value = {
                super::eval_numeric_row(arg, ctx, Row::empty(), EvalType::String)
            }
            .map_err(crate::regexp::err_regexp_wrapping)?;
            if value.is_null() {
                result.reserve_string(input.num_rows());
                for _ in 0..input.num_rows() {
                    result.append_null();
                }
                return Ok(());
            }
            let mut buffer = Column::default();
            buffer.reserve_string(input.num_rows());
            for row in 0..input.num_rows() {
                vec_eval::put(&mut buffer, row, EvalType::String, &value)?;
            }
            buffers.push(buffer);
        } else {
            buffers.push(
                vec_eval::argument(arg, EvalType::String, ctx, input)
                    .map_err(crate::regexp::err_regexp_wrapping)?,
            );
        }
    }
    if function.args[2].const_level() != ConstLevel::STRICT {
        return Err(EvalError::Raw("escape should be const".into()));
    }
    let escape = {
        super::eval_numeric_row(&function.args[2], ctx, Row::empty(), EvalType::Int)?
    };
    if escape.is_null() {
        result.reserve_string(input.num_rows());
        for _ in 0..input.num_rows() {
            result.append_null();
        }
        return Ok(());
    }
    let escape = crate::arg_eval_type::eval_int(&escape)?.expect("not NULL") as u8;
    result.resize_int64(input.num_rows(), false);
    result.merge_nulls(&[&buffers[0], &buffers[1]]);
    for row in 0..input.num_rows() {
        if result.is_null(row) {
            continue;
        }
        let bytes = buffers[1].get_bytes(row);
        let pattern =
            crate::like::CompiledIlikePattern::new(&bytes, escape, function.derived_collation());
        vec_eval::put(
            result,
            row,
            EvalType::Int,
            &Datum::Int(i64::from(pattern.is_match(&buffers[0].get_bytes(row)))),
        )?;
    }
    Ok(())
}
