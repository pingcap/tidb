// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_miscellaneous_vec.go`: addresses, identifiers and delegated columns.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

mod address;
mod uuid;

/// Go miscellaneous signatures' `vectorized` methods.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        InetAton
            | InetNtoa
            | Inet6Aton
            | Inet6Ntoa
            | IsIPv4
            | IsIPv6
            | IsIPv4Compat
            | IsIPv4Mapped
            | IsUuid
            | Uuid
            | UuiDv4
            | UuiDv7
            | UuidVersion
            | UuidTimestamp
            | VitessHash
            | DecimalAnyValue
            | DurationAnyValue
            | IntAnyValue
            | JsonAnyValue
            | RealAnyValue
            | StringAnyValue
            | TimeAnyValue
    ) || matches!(
        function.func_name.lowercase(),
        "name_const" | "uuid_v4" | "uuid_v7" | "uuid_to_bin" | "bin_to_uuid" | "sleep"
    )
}

/// Go `builtin{NameConst,AnyValue,UUID,Inet,Sleep,...}Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let sig = function.pb_code();
    if sig == InetAton {
        return inet_aton(function, ctx, input, result);
    }
    match sig {
        UuidVersion => return uuid::version(function, ctx, input, result),
        UuidTimestamp => return uuid::timestamp(function, ctx, input, result),
        VitessHash => return vitess_hash(function, ctx, input, result),
        _ => {}
    }
    if let Some(result) = address::eval(sig, function, ctx, input, result) {
        return result;
    }
    let name = function.func_name.lowercase();
    let domain = function
        .get_static_type()
        .expect("misc result type")
        .eval_type();
    if matches!(name, "any_value" | "name_const")
        || matches!(
            function.pb_code(),
            ScalarFuncSig::DecimalAnyValue
                | ScalarFuncSig::DurationAnyValue
                | ScalarFuncSig::IntAnyValue
                | ScalarFuncSig::JsonAnyValue
                | ScalarFuncSig::RealAnyValue
                | ScalarFuncSig::StringAnyValue
                | ScalarFuncSig::TimeAnyValue
        )
    {
        return vec_eval::eval_into(
            &function.args[usize::from(name == "name_const")],
            domain,
            ctx,
            input,
            result,
        );
    }
    if matches!(
        function.pb_code(),
        ScalarFuncSig::Uuid | ScalarFuncSig::UuiDv4 | ScalarFuncSig::UuiDv7
    ) {
        result.reserve_string_with_size_hint(input.num_rows(), 36);
        for row in 0..input.num_rows() {
            let value = function.pb_builtin.eval_values(function, Vec::new(), ctx)?;
            vec_eval::put(result, row, domain, &value)?;
        }
        return Ok(());
    }
    if name == "sleep" {
        let buffer = vec_eval::argument(&function.args[0], EvalType::Real, ctx, input)?;
        result.resize_int64(input.num_rows(), false);
        result.with_int64s_mut(|values| values.fill(0));
        for row in 0..input.num_rows() {
            let value = super::class_kernels::sleep(
                &[vec_eval::value(
                    &buffer,
                    row,
                    EvalType::Real,
                    function.args[0].static_type(),
                )],
                ctx,
            )?;
            if value == Datum::Int(1) {
                result.with_int64s_mut(|values| values[row..].fill(1));
                return Ok(());
            }
        }
        return Ok(());
    }
    let n = input.num_rows();
    let arg_type = |index: usize| if index == 1 { EvalType::Int } else { EvalType::String };
    let mut bufs = Vec::with_capacity(function.args.len());
    for (index, arg) in function.args.iter().enumerate() {
        bufs.push(vec_eval::argument(arg, arg_type(index), ctx, input)?);
    }
    let cell = |buf: usize, i: usize| {
        vec_eval::value(&bufs[buf], i, arg_type(buf), function.args[buf].static_type())
    };
    // Go's fixed-width results: the argument's NULLs merged into the result,
    // every other row computed in place.
    if domain != EvalType::String {
        crate::chunk_executor::allocate(domain, n, result);
        result.merge_nulls(&[&bufs[0]]);
        for i in 0..n {
            if result.is_null(i) {
                continue;
            }
            let value = function.pb_builtin.eval_values(function, vec![cell(0, i)], ctx)?;
            vec_eval::put(result, i, domain, &value)?;
        }
        return Ok(());
    }
    // Go's string results: a NULL argument appends NULL; `UUID_TO_BIN` /
    // `BIN_TO_UUID`'s NULL swap flag swaps nothing.
    result.reserve_string(n);
    for i in 0..n {
        if bufs[0].is_null(i) {
            result.append_null();
            continue;
        }
        let mut values = vec![cell(0, i)];
        if bufs.len() == 2 {
            values.push(if bufs[1].is_null(i) { Datum::Int(0) } else { cell(1, i) });
        }
        let value = function.pb_builtin.eval_values(function, values, ctx)?;
        vec_eval::put(result, i, EvalType::String, &value)?;
    }
    Ok(())
}

/// Go `builtinInetAtonSig.vecEvalInt`: unlike its row form, a malformed
/// address is NULL without an error.
fn inet_aton(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let get = cells.variable_cells();
            let parse = |row| {
                if cells.is_null(row) {
                    return None;
                }
                crate::builtin_ext::compare2::inet_aton_bytes(get(row)).map(|value| value as i64)
            };
            if let Some(selection) = selection {
                result.write_int64_rows(selection.iter().copied().map(parse));
            } else {
                result.write_int64_rows((0..input.num_rows()).map(parse));
            }
        },
    )
}

/// Go `builtinVitessHashSig.vecEvalInt`.
fn vitess_hash(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Int,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let values = cells.fixed_cells::<8>();
            let hash = |row| {
                (!cells.is_null(row)).then(|| {
                    tidb_vitess::hash_uint64(u64::from_ne_bytes(values[row])) as i64
                })
            };
            if let Some(selection) = selection {
                result.write_int64_rows(selection.iter().copied().map(hash));
            } else {
                result.write_int64_rows((0..column.rows()).map(hash));
            }
        },
    )
}
