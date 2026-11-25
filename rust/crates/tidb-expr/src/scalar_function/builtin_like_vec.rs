// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_like_vec.go`: complete argument evaluation and batch-local patterns.

use super::{ScalarFunction, vec_eval};
use crate::{
    Columns, Datum, EvalError,
    expression::{ConstLevel, Expression},
};
use tidb_chunk::{chunk::Chunk, column::Column, row::Row};
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

/// Go `builtinLikeSig.vectorized`.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    matches!(
        function.pb_code(),
        ScalarFuncSig::LikeSig | ScalarFuncSig::IlikeSig
    )
}

/// Go `builtinLikeSig.vecEvalInt`: use a private pattern for each batch.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    if function.pb_code() == ScalarFuncSig::IlikeSig {
        return ilike(function, ctx, input, result);
    }
    let values = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let patterns = vec_eval::read_argument(&function.args[1], EvalType::String, ctx, input)?;
    let escapes = vec_eval::read_argument(&function.args[2], EvalType::Int, ctx, input)?;
    let values = values.cells();
    let patterns = patterns.cells();
    let escapes = escapes.cells();
    let collation = function.derived_collation();
    let mut cache: Option<((_, _), crate::like::CompiledLikePattern)> = None;
    result.write_int64_rows((0..input.num_rows()).map(|row| {
        let value = values.bytes(row)?;
        let pattern = patterns.bytes(row)?;
        let escape = escapes.int64(row)? as u8;
        let key = (pattern, escape);
        let (last, compiled) = cache.get_or_insert_with(|| {
            (
                key,
                crate::like::CompiledLikePattern::new(pattern, escape, collation),
            )
        });
        if *last != key {
            compiled.recompile(pattern, escape);
            *last = key;
        }
        Some(i64::from(compiled.is_match(value)))
    }));
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
    let rows = input.num_rows();
    let Some(values) = ilike_argument(&function.args[0], ctx, input)? else {
        fill_nulls(result, rows);
        return Ok(());
    };
    let Some(patterns) = ilike_argument(&function.args[1], ctx, input)? else {
        fill_nulls(result, rows);
        return Ok(());
    };
    if function.args[2].const_level() != ConstLevel::STRICT {
        return Err(EvalError::Raw("escape should be const".into()));
    }
    let escape = super::eval_numeric_row(&function.args[2], ctx, Row::empty(), EvalType::Int)?;
    let Some(escape) = crate::arg_eval_type::eval_int(&escape)? else {
        fill_nulls(result, rows);
        return Ok(());
    };
    let escape = escape as u8;
    let values = values.values()?;
    let patterns = patterns.values()?;
    let collation = function.derived_collation();
    let mut cache: Option<(_, crate::like::CompiledIlikePattern)> = None;
    let mut scratch = Vec::new();
    result.write_int64_rows((0..rows).map(|row| {
        let value = values.get(row)?;
        let pattern = patterns.get(row)?;
        let (last, compiled) = cache.get_or_insert_with(|| {
            (
                pattern,
                crate::like::CompiledIlikePattern::new(pattern, escape, collation),
            )
        });
        if *last != pattern {
            compiled.recompile(pattern, escape, &mut scratch);
            *last = pattern;
        }
        Some(i64::from(
            compiled.is_match_with_buffer(value, &mut scratch),
        ))
    }));
    Ok(())
}

enum IlikeArgument<'a> {
    Constant(Datum),
    Column(vec_eval::ReadArgument<'a>),
}

impl IlikeArgument<'_> {
    fn values(&self) -> Result<IlikeCells<'_>, EvalError> {
        match self {
            Self::Constant(value) => {
                crate::arg_eval_type::eval_string_bytes(value).map(IlikeCells::Constant)
            }
            Self::Column(argument) => Ok(IlikeCells::Column(argument.cells())),
        }
    }
}

enum IlikeCells<'a> {
    Constant(Option<&'a [u8]>),
    Column(vec_eval::ReadArgumentCells<'a>),
}

impl IlikeCells<'_> {
    fn get(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Constant(value) => *value,
            Self::Column(cells) => cells.bytes(row),
        }
    }
}

fn ilike_argument<'a>(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &'a Chunk,
) -> Result<Option<IlikeArgument<'a>>, EvalError> {
    if expression.const_level() >= ConstLevel::ONLY_IN_CONTEXT {
        let value = super::eval_numeric_row(expression, ctx, Row::empty(), EvalType::String)
            .map_err(crate::regexp::err_regexp_wrapping)?;
        return Ok((!value.is_null()).then_some(IlikeArgument::Constant(value)));
    }
    vec_eval::read_argument(expression, EvalType::String, ctx, input)
        .map(|argument| Some(IlikeArgument::Column(argument)))
        .map_err(crate::regexp::err_regexp_wrapping)
}

fn fill_nulls(result: &mut Column, rows: usize) {
    result.reserve_string(rows);
    for _ in 0..rows {
        result.append_null();
    }
}
