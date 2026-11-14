// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_string_vec{,_generated}.go`.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

/// Go string signature `vectorized` methods.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        Length
            | CharLength
            | CharLengthUtf8
            | Lower
            | LowerUtf8
            | Upper
            | UpperUtf8
            | Substring2Args
            | Substring3Args
            | Substring2ArgsUtf8
            | Substring3ArgsUtf8
            | ToBinary
            | FromBinary
            | Ascii
            | BitLength
            | Reverse
            | ReverseUtf8
            | Replace
            | Strcmp
            | Elt
            | FieldInt
            | FieldReal
            | FieldString
            | HexStrArg
            | HexIntArg
            | LTrim
            | RTrim
            | Right
            | RightUtf8
            | Space
            | Concat
            | ConcatWs
            | Bin
            | SubstringIndex
            | ToBase64
            | FromBase64
            | Quote
            | Ord
            | Repeat
            | FindInSet
            | Left
            | LeftUtf8
            | Lpad
            | LpadUtf8
            | Rpad
            | RpadUtf8
            | Locate2Args
            | Locate2ArgsUtf8
            | Locate3Args
            | Locate3ArgsUtf8
            | Instr
            | InstrUtf8
            | UnHex
            | OctInt
            | OctString
            | ExportSet3Arg
            | ExportSet4Arg
            | ExportSet5Arg
            | MakeSet
            | Format
            | FormatWithLocale
            | Insert
            | InsertUtf8
            | Trim1Arg
            | Trim2Args
            | Trim3Args
            | Convert
            | Char
    ) || function.func_name.lowercase() == "translate"
}

/// Go `builtin*StringSig.vecEval*`: fixed-arity argument columns and the
/// signature's string operation; special streaming signatures dispatch below.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    match function.pb_code() {
        Lower | Upper => {
            return vec_eval::eval_into(&function.args[0], EvalType::String, ctx, input, result)
        }
        Reverse | ReverseUtf8 => return reverse(function, ctx, input, result),
        Concat => return concat(function, ctx, input, result),
        ConcatWs => return concat_ws(function, ctx, input, result),
        FindInSet => return find_in_set(function, ctx, input, result),
        Elt => return elt(function, ctx, input, result),
        Convert | Char => return charset(function, ctx, input, result),
        _ => {}
    }
    let reuse = matches!(function.pb_code(), Locate3Args | Locate3ArgsUtf8).then_some(2);
    let order: Vec<_> = match function.pb_code() {
        Format => vec![1, 0],
        FormatWithLocale => vec![1, 2, 0],
        _ => (0..function.args.len()).collect(),
    };
    vec_eval::map_arguments_in_order(function, ctx, input, result, reuse, &order, |values| {
        match function.pb_code() {
            Trim1Arg | Trim2Args | Trim3Args => trim(&values),
            FindInSet => crate::builtin_ext::find_in_set_with_collation(
                &values,
                function.derived_collation(),
            ),
            _ => function.pb_builtin.eval_values(function, values, ctx),
        }
    })
}

/// Go `builtinReverseSig.vecEvalString`, `builtinReverseUTF8Sig.vecEvalString`:
/// SetRaw copies into the existing cell, including Go's truncation on invalid UTF8.
fn reverse(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::eval_into(&function.args[0], EvalType::String, ctx, input, result)?;
    for i in 0..input.num_rows() {
        if result.is_null(i) {
            continue;
        }
        let bytes = result.get_bytes(i).as_ref().to_vec();
        let reversed = if function.pb_code() == ScalarFuncSig::Reverse {
            bytes.into_iter().rev().collect::<Vec<_>>()
        } else {
            tidb_goruntime::GoString::from_bytes(bytes)
                .to_utf8_lossy_go()
                .chars()
                .rev()
                .collect::<String>()
                .into_bytes()
        };
        result.set_raw(i, &reversed);
    }
    Ok(())
}

/// Go `builtinConcatSig.vecEvalString`: append each argument column's bytes
/// before evaluating the next, including packet warnings in that order.
fn concat(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let mut values = vec![Vec::<u8>::new(); input.num_rows()];
    let mut nulls = vec![false; input.num_rows()];
    result.reserve_string(input.num_rows());
    for arg in &function.args {
        let buffer = vec_eval::argument(arg, EvalType::String, ctx, input)?;
        for i in 0..input.num_rows() {
            if nulls[i] {
                continue;
            }
            if buffer.is_null(i) {
                nulls[i] = true;
                continue;
            }
            let bytes = buffer.get_bytes(i);
            if (values[i].len() + bytes.len()) as u64 > ctx.max_allowed_packet() {
                crate::expression_errors::handle_allowed_packet_overflowed(ctx, "concat")?;
                nulls[i] = true;
            } else {
                values[i].extend_from_slice(&bytes);
            }
        }
    }
    for (i, bytes) in values.into_iter().enumerate() {
        if nulls[i] {
            result.append_null();
        } else {
            result.append_bytes(&bytes);
        }
    }
    Ok(())
}

/// Go `builtinEltSig.vecEvalString`: evaluate a selected argument's whole
/// column on its first occurrence, cache it for the remaining input rows.
fn elt(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let positions = vec_eval::argument(&function.args[0], EvalType::Int, ctx, input)?;
    let mut buffers = vec![None; function.args.len()];
    result.reserve_string(input.num_rows());
    for i in 0..input.num_rows() {
        let position = positions.get_int64(i);
        if positions.is_null(i) || position < 1 || position as usize >= function.args.len() {
            result.append_null();
            continue;
        }
        let j = position as usize;
        if buffers[j].is_none() {
            buffers[j] = Some(vec_eval::argument(
                &function.args[j],
                EvalType::String,
                ctx,
                input,
            )?);
        }
        let buffer = buffers[j].as_ref().unwrap();
        if buffer.is_null(i) {
            result.append_null();
        } else {
            result.append_bytes(&buffer.get_bytes(i));
        }
    }
    Ok(())
}

/// Go `builtinTrim{1,2,3}ArgSig.vecEvalString` per-cell trim.
fn trim(values: &[Datum]) -> Result<Datum, EvalError> {
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let remove = values
        .get(1)
        .cloned()
        .unwrap_or_else(|| Datum::new_string_datum(" "));
    let direction = match values.get(2).map(Datum::get_int64) {
        Some(2) => tidb_ast::TrimDirection::Leading,
        Some(3) => tidb_ast::TrimDirection::Trailing,
        _ => tidb_ast::TrimDirection::Both,
    };
    Ok(crate::string_fn::trim_value(
        crate::coerce::coerce_str_bytes(&values[0])?,
        crate::coerce::coerce_str_bytes(&remove)?,
        direction,
        matches!(values[0], Datum::Bytes(_)),
    ))
}

/// Go `builtinConvertSig.vecEvalString` and `builtinCharSig.vecEvalString`:
/// the charset was selected at build; its syntax argument is not evaluated.
fn charset(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let convert = function.pb_code() == ScalarFuncSig::Convert;
    let field = function.get_static_type().expect("charset result");
    let mut buffers = Vec::new();
    for arg in &function.args[..function.args.len() - 1] {
        buffers.push(vec_eval::argument(
            arg,
            if convert {
                EvalType::String
            } else {
                EvalType::Int
            },
            ctx,
            input,
        )?);
    }
    result.reserve_string(input.num_rows());
    for i in 0..input.num_rows() {
        let value = if convert {
            if buffers[0].is_null(i) {
                Datum::Null
            } else {
                crate::convert_charset::convert_using(
                    &vec_eval::value(
                        &buffers[0],
                        i,
                        EvalType::String,
                        function.args[0].static_type(),
                    ),
                    function.args[0].static_type().unwrap(),
                    field.charset_name(),
                )?
            }
        } else {
            let mut values: Vec<_> = buffers
                .iter()
                .enumerate()
                .map(|(j, b)| vec_eval::value(b, i, EvalType::Int, function.args[j].static_type()))
                .collect();
            values.push(Datum::new_string_datum(field.charset_name()));
            crate::string_fn::char_func_with_context(&values, ctx)?
        };
        vec_eval::put(result, i, EvalType::String, &value)?;
    }
    Ok(())
}

/// Go `builtinConcatWSSig.vecEvalString`: stage every argument, then check
/// packet limits argument-major. The source adds separator length at row i>1.
fn concat_ws(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let mut buffers = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        buffers.push(vec_eval::argument(arg, EvalType::String, ctx, input)?);
    }
    let n = input.num_rows();
    let mut nulls: Vec<bool> = (0..n).map(|i| buffers[0].is_null(i)).collect();
    let mut lengths = vec![0_usize; n];
    let mut strings: Vec<Vec<Vec<u8>>> = vec![Vec::new(); n];
    for buffer in &buffers[1..] {
        for i in 0..n {
            if nulls[i] || buffer.is_null(i) {
                continue;
            }
            let text = buffer.get_bytes(i);
            lengths[i] += text.len();
            if i > 1 {
                lengths[i] += buffers[0].get_bytes(i).len();
            }
            if lengths[i] as u64 > ctx.max_allowed_packet() {
                crate::expression_errors::handle_allowed_packet_overflowed(ctx, "concat_ws")?;
                nulls[i] = true;
                continue;
            }
            strings[i].push(text.as_ref().to_vec());
        }
    }
    result.reserve_string(n);
    for i in 0..n {
        if nulls[i] {
            result.append_null();
        } else {
            result.append_bytes(&strings[i].join(buffers[0].get_bytes(i).as_ref()));
        }
    }
    Ok(())
}

/// Go `builtinFindInSetSig.vecEvalInt`: contextual constant lists use the
/// signature's lookup cache; nonconstant lists evaluate as a second column.
fn find_in_set(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let needle = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
    result.resize_int64(input.num_rows(), false);
    result.merge_nulls(&[&needle]);
    let collation = function.derived_collation();
    if function.args[1].const_level() >= crate::expression::ConstLevel::ONLY_IN_CONTEXT {
        let lookup = function
            .find_in_set_cache
            .get_or_init_cache(ctx.context_id(), || {
                let _scope = crate::chunk_executor::enter_vec_eval(false);
                let list = function.args[1].eval(ctx, tidb_chunk::row::Row::empty())?;
                crate::builtin_ext::build_find_in_set_lookup(&list, collation)
            })?;
        for row in 0..input.num_rows() {
            let value = crate::builtin_ext::find_in_set_lookup(
                &vec_eval::value(
                    &needle,
                    row,
                    EvalType::String,
                    function.args[0].static_type(),
                ),
                &lookup,
                collation,
            )?;
            vec_eval::put(result, row, EvalType::Int, &value)?;
        }
        return Ok(());
    }
    let list = vec_eval::argument(&function.args[1], EvalType::String, ctx, input)?;
    result.merge_nulls(&[&list]);
    for row in 0..input.num_rows() {
        if result.is_null(row) {
            continue;
        }
        let values = [&needle, &list]
            .iter()
            .enumerate()
            .map(|(i, buffer)| {
                vec_eval::value(
                    buffer,
                    row,
                    EvalType::String,
                    function.args[i].static_type(),
                )
            })
            .collect::<Vec<_>>();
        let value = crate::builtin_ext::find_in_set_with_collation(&values, collation)?;
        vec_eval::put(result, row, EvalType::Int, &value)?;
    }
    Ok(())
}
