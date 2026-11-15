// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_miscellaneous_vec.go`: addresses, identifiers and delegated columns.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

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
    use ScalarFuncSig::*;
    let n = input.num_rows();
    if function.pb_code() == InetAton {
        return inet_aton(function, ctx, input, result);
    }
    if function.pb_code() == Inet6Aton {
        return inet6_aton(function, ctx, input, result);
    }
    let arg_type = |index: usize| match (function.pb_code(), index) {
        (InetNtoa | VitessHash, 0) => EvalType::Int,
        (_, 1) => EvalType::Int,
        _ => EvalType::String,
    };
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
    let n = input.num_rows();
    let buf = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
    result.resize_int64(n, false);
    result.merge_nulls(&[&buf]);
    for i in 0..n {
        if result.is_null(i) {
            continue;
        }
        let ip_addr = buf.get_bytes(i);
        let ip_addr = ip_addr.as_ref();
        if ip_addr.is_empty() || ip_addr[ip_addr.len() - 1] == b'.' {
            result.set_null(i, true);
            continue;
        }
        let mut byte_result: u64 = 0;
        let mut res: u64 = 0;
        let mut dot_count = 0;
        // Go ranges over the string's runes; every byte of a multi-byte rune
        // is an illegal character either way.
        for c in String::from_utf8_lossy(ip_addr).chars() {
            if c.is_ascii_digit() {
                byte_result = byte_result * 10 + u64::from(c as u8 - b'0');
                if byte_result > 255 {
                    result.set_null(i, true);
                    break;
                }
            } else if c == '.' {
                dot_count += 1;
                if dot_count > 3 {
                    result.set_null(i, true);
                    break;
                }
                res = (res << 8) + byte_result;
                byte_result = 0;
            } else {
                result.set_null(i, true);
                break;
            }
        }
        if !result.is_null(i) {
            if dot_count == 1 {
                res <<= 16;
            }
            if dot_count == 2 {
                res <<= 8;
            }
            let value = ((res << 8) + byte_result) as i64;
            result.with_int64s_mut(|i64s| i64s[i] = value);
        }
    }
    Ok(())
}

/// Go `builtinInet6AtonSig.vecEvalString`: unlike its row form, an empty or
/// unparseable address is NULL without an error.
fn inet6_aton(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let n = input.num_rows();
    let buf = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
    result.reserve_string(n);
    for i in 0..n {
        if buf.is_null(i) {
            result.append_null();
            continue;
        }
        let bytes = buf.get_bytes(i);
        // IP syntax is ASCII: an invalid UTF-8 byte is the same parse failure.
        let text = String::from_utf8_lossy(bytes.as_ref());
        match crate::builtin_ext::compare2::inet6_aton_text(&text) {
            Some(address) => vec_eval::put(result, i, EvalType::String, &address)?,
            None => result.append_null(),
        }
    }
    Ok(())
}
