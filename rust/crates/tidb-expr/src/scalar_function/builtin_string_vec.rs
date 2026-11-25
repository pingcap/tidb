// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_string_vec{,_generated}.go`.

use super::{vec_eval, ScalarFunction};
use crate::expression::Expression;
use crate::string_signature::StrUnits;
use crate::{Columns, EvalError};
use memchr::{memchr, memchr_iter, memmem};
use std::borrow::Cow;
use tidb_chunk::{
    chunk::Chunk,
    column::{ByteRow, Column, RawCells},
};
use tidb_datatype::{CollationCollator, Datum, EvalType, FieldType};
use tidb_goruntime::go_hex;
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
        Length | CharLength | CharLengthUtf8 | Ascii | BitLength | Ord
            if argument_types_match(function, &[EvalType::String]) =>
        {
            return string_integer_result(function, ctx, input, result);
        }
        Lower | Upper => {
            return vec_eval::eval_into(&function.args[0], EvalType::String, ctx, input, result)
        }
        LowerUtf8 if argument_types_match(function, &[EvalType::String]) => {
            return case_convert::<false>(function, ctx, input, result);
        }
        UpperUtf8 if argument_types_match(function, &[EvalType::String]) => {
            return case_convert::<true>(function, ctx, input, result);
        }
        Reverse | ReverseUtf8 => return reverse(function, ctx, input, result),
        HexStrArg | UnHex if argument_types_match(function, &[EvalType::String]) => {
            return hex_bytes(function, ctx, input, result);
        }
        HexIntArg if argument_types_match(function, &[EvalType::Int]) => {
            return hex_integers(function, ctx, input, result);
        }
        Space if argument_types_match(function, &[EvalType::Int]) => {
            return space(function, ctx, input, result);
        }
        Quote if argument_types_match(function, &[EvalType::String]) => {
            return quote(function, ctx, input, result);
        }
        Concat => return concat(function, ctx, input, result),
        ConcatWs => return concat_ws(function, ctx, input, result),
        FindInSet => return find_in_set(function, ctx, input, result),
        Locate2Args | Instr => {
            return string_search::<true, false, false>(function, ctx, input, result);
        }
        Locate3Args => return string_search::<true, false, true>(function, ctx, input, result),
        Locate2ArgsUtf8 | InstrUtf8 => {
            return string_search_text::<false>(function, ctx, input, result);
        }
        Locate3ArgsUtf8 => return string_search_text::<true>(function, ctx, input, result),
        FieldInt => return field_numeric::<false>(function, ctx, input, result),
        FieldReal => return field_numeric::<true>(function, ctx, input, result),
        FieldString => return field_string(function, ctx, input, result),
        MakeSet => return make_set(function, ctx, input, result),
        ExportSet3Arg | ExportSet4Arg | ExportSet5Arg => {
            return export_set(function, ctx, input, result);
        }
        Elt => return elt(function, ctx, input, result),
        Repeat => return repeat(function, ctx, input, result),
        Lpad | Rpad => return pad::<false>(function, ctx, input, result),
        LpadUtf8 | RpadUtf8 => return pad::<true>(function, ctx, input, result),
        Insert => return insert::<false>(function, ctx, input, result),
        InsertUtf8 => return insert::<true>(function, ctx, input, result),
        Unspecified if function.func_name.lowercase() == "translate" => {
            if function
                .args
                .iter()
                .any(|arg| arg.static_type().is_some_and(FieldType::is_binary_string))
            {
                return translate::<true>(function, ctx, input, result);
            }
            return translate::<false>(function, ctx, input, result);
        }
        Replace if argument_types_match(function, &[EvalType::String; 3]) => {
            return replace(function, ctx, input, result);
        }
        Trim1Arg | Trim2Args | Trim3Args => {
            let types = [EvalType::String, EvalType::String, EvalType::Int];
            let arity = match function.pb_code() {
                Trim1Arg => 1,
                Trim2Args => 2,
                _ => 3,
            };
            if argument_types_match(function, &types[..arity]) {
                return trim_cells(function, ctx, input, result);
            }
        }
        Left | LeftUtf8 | Right | RightUtf8 | Substring2Args | Substring2ArgsUtf8 | Substring3Args
        | Substring3ArgsUtf8 => {
            if let Some(args) = slice_arguments(function) {
                return slice_strings(function.pb_code(), args, ctx, input, result);
            }
        }
        SubstringIndex
            if function
                .args
                .iter()
                .map(|arg| arg.static_type().map(FieldType::eval_type))
                .eq([Some(EvalType::String), Some(EvalType::String), Some(EvalType::Int)]) =>
        {
            return substring_index(function, ctx, input, result);
        }
        Convert | Char => return charset(function, ctx, input, result),
        Format | FormatWithLocale => return format_number(function, ctx, input, result),
        _ => {}
    }
    let order: Vec<_> = (0..function.args.len()).collect();
    vec_eval::map_arguments_in_order(function, ctx, input, result, None, &order, |values| {
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

/// Go's unary string kernels that return an integer per row.
fn string_integer_result(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let argument = &function.args[0];
    let signature = function.pb_code();
    let num_rows = input.num_rows();
    vec_eval::with_argument_column(argument, EvalType::String, ctx, input, |column, selection| {
        let rows = (0..num_rows).map(|row| selection.map_or(row, |selection| selection[row]));
        let cells = column.raw_cells();
        if matches!(signature, Length | CharLength | BitLength) {
            if selection.is_none() {
                result.resize_int64(num_rows, false);
                result.merge_nulls(&[column]);
                result.with_fixed_cells_mut(|values, bitmap| {
                    if signature == BitLength {
                        cells.write_variable_lengths::<8>(values, bitmap);
                    } else {
                        cells.write_variable_lengths::<1>(values, bitmap);
                    }
                });
                return;
            }
            let scale = if signature == BitLength { 8 } else { 1 };
            result.write_int64_rows(rows.map(|row| {
                (!column.is_null(row)).then(|| column.get_raw_length(row) as i64 * scale)
            }));
            return;
        }

        let bytes_at = cells.variable_cells();
        let bitmap = cells.not_null_bitmap();
        let values = rows.map(|row| (bitmap[row >> 3] & (1 << (row & 7)) != 0).then(|| bytes_at(row)));
        match signature {
            Ascii => result.write_int64_rows(values.map(|value| {
                value.map(|bytes| i64::from(bytes.first().copied().unwrap_or(0)))
            })),
            CharLengthUtf8 => result.write_int64_rows(values.map(|value| {
                value.map(|bytes| crate::build::go_utf8_rune_count(bytes) as i64)
            })),
            Ord => {
                let charset = argument
                    .static_type()
                    .expect("typed string argument")
                    .charset_name();
                let encoding = tidb_charset::find_encoding(charset);
                result.write_int64_rows(values.map(|value| {
                    value.map(|bytes| crate::string_fn::ord_in_encoding(bytes, encoding))
                }));
            }
            _ => unreachable!("unary string integer signature"),
        }
    })
}

fn case_convert<const UPPER: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let arg = &function.args[0];
    let field = arg.static_type().expect("typed case-conversion argument");
    let encoding = tidb_charset::find_encoding(field.charset_name());
    let rows = input.num_rows();
    vec_eval::with_argument_column(arg, EvalType::String, ctx, input, |column, selection| {
        let cells = column.raw_cells();
        if selection.is_none() && column.null_count() == 0 && cells.is_ascii() {
            column.copy_construct_to(result);
            result.map_ascii_case::<UPPER>();
            return;
        }

        let bytes_at = cells.variable_cells();
        result.reserve_string(rows);
        let append: Result<(), std::convert::Infallible> = result.try_append_byte_rows(rows, |row, out| {
            let row = selection.map_or(row, |selection| selection[row]);
            if cells.is_null(row) {
                return Ok(false);
            }
            let bytes = bytes_at(row);
            let text = match simdutf8::basic::from_utf8(bytes) {
                Ok(text) => Cow::Borrowed(text),
                Err(_) => Cow::Owned(tidb_goruntime::decode_go_utf8_lossy(bytes)),
            };
            let output = if UPPER {
                encoding.to_upper_cow(&text)
            } else {
                encoding.to_lower_cow(&text)
            };
            out.append(output.as_bytes());
            Ok(true)
        });
        match append {
            Ok(()) => {}
            Err(never) => match never {},
        }
    })
}

fn argument_types_match(function: &ScalarFunction, types: &[EvalType]) -> bool {
    function
        .args
        .iter()
        .map(|arg| arg.static_type().map(FieldType::eval_type))
        .eq(types.iter().copied().map(Some))
}

fn hex_bytes(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let decode = function.pb_code() == ScalarFuncSig::UnHex;
    let rows = input.num_rows();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            result.reserve_string(rows);
            let rows = (0..rows).map(|row| selection.map_or(row, |selection| selection[row]));
            if cells.data_len() == 0 {
                result.append_byte_rows(
                    rows.map(|row| (!cells.is_null(row)).then_some((0, std::iter::empty()))),
                );
                return;
            }
            let mut buffer = Vec::new();
            for row in rows {
                if cells.is_null(row) {
                    result.append_null();
                    continue;
                }
                let bytes = cells.get(row);
                if decode {
                    if !crate::string_fn::decode_sql_hex(bytes, &mut buffer) {
                        result.append_null();
                        continue;
                    }
                } else {
                    buffer.clear();
                    go_hex::append_uppercase(&mut buffer, bytes);
                }
                result.append_bytes(&buffer);
            }
        },
    )
}

fn hex_integers(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let rows = input.num_rows();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Int,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let values = cells.fixed_cells::<8>();
            let mut buffer = Vec::with_capacity(16);
            result.reserve_string(rows);
            for row in 0..rows {
                let row = selection.map_or(row, |selection| selection[row]);
                if cells.is_null(row) {
                    result.append_null();
                    continue;
                }
                let bits = u64::from_ne_bytes(values[row]);
                let bytes = bits.to_be_bytes();
                let leading_digits = (bits.leading_zeros() as usize / 4).min(15);
                buffer.clear();
                go_hex::append_uppercase(&mut buffer, &bytes[leading_digits / 2..]);
                result.append_bytes(&buffer[leading_digits % 2..]);
            }
        },
    )
}

fn slice_arguments(
    function: &ScalarFunction,
) -> Option<(&Expression, &Expression, Option<&Expression>)> {
    use ScalarFuncSig::*;
    let (value, position, length) = match function.args.as_slice() {
        [value, position, length]
            if matches!(function.pb_code(), Substring3Args | Substring3ArgsUtf8) =>
        {
            (value, position, Some(length))
        }
        [value, position]
            if !matches!(function.pb_code(), Substring3Args | Substring3ArgsUtf8) =>
        {
            (value, position, None)
        }
        _ => return None,
    };
    if value.static_type().map(FieldType::eval_type) == Some(EvalType::String)
        && std::iter::once(position)
            .chain(length)
            .all(|arg| arg.static_type().map(FieldType::eval_type) == Some(EvalType::Int))
    {
        Some((value, position, length))
    } else {
        None
    }
}

/// Go's typed LEFT, RIGHT and SUBSTRING loops over bytes or UTF-8 units.
fn slice_strings(
    signature: ScalarFuncSig,
    args: (&Expression, &Expression, Option<&Expression>),
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let (value, position, length) = args;
    let value = vec_eval::read_argument(value, EvalType::String, ctx, input)?;
    let position = vec_eval::read_argument(position, EvalType::Int, ctx, input)?;
    let length = length
        .map(|arg| vec_eval::read_argument(arg, EvalType::Int, ctx, input))
        .transpose()?;
    let value = value.cells();
    let position = position.cells();
    let length = length.as_ref().map(vec_eval::ReadArgument::cells);
    let binary = matches!(signature, Left | Right | Substring2Args | Substring3Args);
    let left = matches!(signature, Left | LeftUtf8);
    let take = left || matches!(signature, Right | RightUtf8);
    let rows = input.num_rows();
    result.reserve_string(rows);
    for row in 0..rows {
        let (Some(bytes), Some(position)) = (value.bytes(row), position.int64(row)) else {
            result.append_null();
            continue;
        };
        let length = match length.as_ref().map(|column| column.int64(row)) {
            Some(None) => {
                result.append_null();
                continue;
            }
            length => length.flatten(),
        };
        let units = StrUnits::from_bytes(Cow::Borrowed(bytes), binary);
        let selected = if take {
            crate::string_fn::take_string_unit_slice(&units, position.max(0) as usize, left)
        } else {
            crate::string_fn::substring_slice(&units, position, length)
        };
        result.append_bytes(selected);
    }
    Ok(())
}

/// Go `builtinSubstringIndexSig.vecEvalString`, reading and writing typed cells.
fn substring_index(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let value = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
    let delim = vec_eval::argument(&function.args[1], EvalType::String, ctx, input)?;
    let count_arg = &function.args[2];
    let count = vec_eval::argument(count_arg, EvalType::Int, ctx, input)?;
    let unsigned = count_arg.static_type().is_some_and(FieldType::is_unsigned);
    let rows = input.num_rows();
    result.reserve_string(rows);
    for row in 0..rows {
        if value.is_null(row) || delim.is_null(row) || count.is_null(row) {
            result.append_null();
            continue;
        }
        let value = value.get_bytes(row);
        let delim = delim.get_bytes(row);
        let bytes = crate::string_fn::substring_index_byte_slice(
            &value,
            &delim,
            count.get_int64(row),
            unsigned,
        );
        result.append_bytes(bytes);
    }
    Ok(())
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
    if function.pb_code() == ScalarFuncSig::Reverse {
        result.reverse_var_cells(input.num_rows());
        return Ok(());
    }
    let mut reversed = String::new();
    for i in 0..input.num_rows() {
        if result.is_null(i) {
            continue;
        }
        let bytes = result.get_bytes(i);
        reversed.clear();
        reversed.extend(crate::string_signature::go_utf8_text(&bytes).chars().rev());
        drop(bytes);
        result.set_raw(i, reversed.as_bytes());
    }
    Ok(())
}

/// Go `builtinReplaceSig.vecEvalString` over three typed byte columns.
fn replace(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let value = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let from = vec_eval::read_argument(&function.args[1], EvalType::String, ctx, input)?;
    let to = vec_eval::read_argument(&function.args[2], EvalType::String, ctx, input)?;
    let (value, from, to) = (value.cells(), from.cells(), to.cells());
    let mut output = Vec::new();
    result.reserve_string(input.num_rows());
    for row in 0..input.num_rows() {
        let (Some(value), Some(from), Some(to)) = (value.bytes(row), from.bytes(row), to.bytes(row)) else {
            result.append_null();
            continue;
        };
        let bytes = if crate::string_fn::replace_bytes_into(value, from, to, &mut output) {
            &output
        } else {
            value
        };
        result.append_bytes(bytes);
    }
    Ok(())
}

/// Go evaluates all the argument columns, then joins selected non-NULL
/// values. The bit mask can select only the first 64 string arguments.
fn make_set(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let bits = vec_eval::read_argument(&function.args[0], EvalType::Int, ctx, input)?;
    let mut arguments = Vec::with_capacity((function.args.len() - 1).min(64));
    for (index, arg) in function.args[1..].iter().enumerate() {
        let argument = StringArgument::read(arg, ctx, input)?;
        if index < 64 {
            arguments.push(argument);
        }
    }
    let rows = input.num_rows();
    if let [StringArgument::Rows(value)] = arguments.as_slice() {
        let (value, value_sel) = value.source();
        let (masks, masks_sel) = bits.source();
        if rows != 0
            && value_sel.is_none()
            && masks_sel.is_none()
            && value.rows() == rows
            && masks.rows() == rows
            && masks
                .raw_cells()
                .fixed_cells::<8>()
                .iter()
                .all(|&mask| u64::from_ne_bytes(mask) & 1 != 0)
            && masks.null_count() == 0
            && value.null_count() == 0
        {
            value.copy_construct_to(result);
            return Ok(());
        }
    }
    let cells: Vec<_> = arguments.iter().map(StringArgument::cells).collect();
    let bits = bits.cells();
    let bits = bits.fixed::<8>();
    let mask = u64::MAX.checked_shr((64 - cells.len()) as u32).unwrap_or(0);
    result.reserve_string(rows);
    if let [text] = cells.as_slice() {
        result.append_byte_rows((0..rows).map(|row| {
            let bytes = if u64::from_ne_bytes(*bits(row)?) & 1 != 0 {
                text.bytes(row).unwrap_or_default()
            } else {
                b""
            };
            Some((bytes.len(), std::iter::once(bytes)))
        }));
        return Ok(());
    }
    let mut selected = [b"".as_slice(); 64];
    result.try_append_byte_rows(rows, |row, output| {
        let Some(&bits) = bits(row) else {
            return Ok(false);
        };
        let mut bits = u64::from_ne_bytes(bits) & mask;
        let mut count = 0;
        let mut width = 0;
        while bits != 0 {
            let index = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            if let Some(bytes) = cells[index].bytes(row) {
                selected[count] = bytes;
                count += 1;
                width += bytes.len();
            }
        }
        output.reserve(width + count.saturating_sub(1));
        if let Some((first, tail)) = selected[..count].split_first() {
            output.append(first);
            for bytes in tail {
                output.append(b",");
                output.append(bytes);
            }
        }
        Ok(true)
    })
}

/// Go's EXPORT_SET uses the same fragment construction for all three arities.
fn export_set(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let bits = vec_eval::read_argument(&function.args[0], EvalType::Int, ctx, input)?;
    let on = StringArgument::read(&function.args[1], ctx, input)?;
    let off = StringArgument::read(&function.args[2], ctx, input)?;
    let separator = function
        .args
        .get(3)
        .map(|arg| StringArgument::read(arg, ctx, input))
        .transpose()?;
    let count = function
        .args
        .get(4)
        .map(|arg| vec_eval::read_argument(arg, EvalType::Int, ctx, input))
        .transpose()?;
    let bits = bits.cells();
    let bits = bits.fixed::<8>();
    let (on, off) = (on.cells(), off.cells());
    let separator = separator.as_ref().map(StringArgument::cells);
    let count = count.as_ref().map(vec_eval::ReadArgument::cells);
    let count = count.as_ref().map(|cells| cells.fixed::<8>());
    let rows = input.num_rows();
    result.reserve_string(rows);
    result.append_byte_rows((0..rows).map(|row| {
        let bits = i64::from_ne_bytes(*bits(row)?);
        let count = match &count {
            Some(count) => i64::from_ne_bytes(*count(row)?),
            None => 64,
        };
        if matches!(count, 0 | 1) {
            if separator.as_ref().is_some_and(|sep| sep.is_null(row)) {
                return None;
            }
            let value = if count == 0 {
                if on.is_null(row) || off.is_null(row) {
                    return None;
                }
                b"".as_slice()
            } else {
                let (selected, other) = if bits & 1 != 0 {
                    (&on, &off)
                } else {
                    (&off, &on)
                };
                if other.is_null(row) {
                    return None;
                }
                selected.bytes(row)?
            };
            return Some(crate::string_fn::export_set_fragments(
                1, value, b"", b"", count,
            ));
        }
        let on = on.bytes(row)?;
        let off = off.bytes(row)?;
        let separator = match &separator {
            Some(separator) => separator.bytes(row)?,
            None => b",",
        };
        Some(crate::string_fn::export_set_fragments(
            bits, on, off, separator, count,
        ))
    }));
    Ok(())
}

/// Go's TRANSLATE signatures map bytes or runes and build a common mapping
/// once when both mapping expressions have a single value for the batch.
fn translate<const BINARY: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let source = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let from = StringArgument::read(&function.args[1], ctx, input)?;
    let to = StringArgument::read(&function.args[2], ctx, input)?;
    let rows = input.num_rows();
    let source = source.cells();
    result.reserve_string(rows);
    let mut table = TranslateTable {
        byte_keys: [None; 256],
        rune_keys: std::collections::HashMap::new(),
    };
    let mut output = Vec::new();
    if let (StringArgument::Literal(from), StringArgument::Literal(to)) = (&from, &to) {
        let (Some(from), Some(to)) = (from.as_raw_bytes(), to.as_raw_bytes()) else {
            result.append_n_nulls(rows);
            return Ok(());
        };
        let byte_output = table.build::<BINARY>(from, to);
        for row in 0..rows {
            if let Some(source) = source.bytes(row) {
                result.append_bytes(table.apply::<BINARY>(source, byte_output, &mut output));
            } else {
                result.append_null();
            }
        }
        return Ok(());
    }
    let (from, to) = (from.cells(), to.cells());
    for row in 0..rows {
        let (Some(source), Some(from), Some(to)) =
            (source.bytes(row), from.bytes(row), to.bytes(row))
        else {
            result.append_null();
            continue;
        };
        let byte_output = table.build::<BINARY>(from, to);
        result.append_bytes(table.apply::<BINARY>(source, byte_output, &mut output));
    }
    Ok(())
}

/// Go's FIELD evaluates candidates in argument order, even after a match,
/// and compares the typed values from those evaluations.
fn field_numeric<const REAL: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let domain = if REAL { EvalType::Real } else { EvalType::Int };
    let needle = vec_eval::read_argument(&function.args[0], domain, ctx, input)?;
    let (needle_column, needle_selection) = needle.source();
    let needle_packed = needle_selection.is_none();
    let needle_has_nulls = needle_column.null_count() != 0;
    let needle_raw = needle_column.raw_cells();
    let needle_cells = needle.cells();
    let needle_at = needle_cells.fixed::<8>();
    let rows = input.num_rows();
    result.resize_int64(rows, false);
    result.with_fixed_cells_mut(|values, _| values.fill([0; 8]));

    let equal = |a: &[u8; 8], b: &[u8; 8]| {
        if REAL {
            f64::from_ne_bytes(*a) == f64::from_ne_bytes(*b)
        } else {
            a == b
        }
    };
    for (index, arg) in function.args.iter().enumerate().skip(1) {
        let index = (index as i64).to_ne_bytes();
        if let Expression::Constant(literal) = arg {
            if literal.deferred_expr.is_none() && rows != 0 {
                let value =
                    literal.eval_typed_as_on_row(ctx, tidb_chunk::row::Row::empty(), domain)?;
                if value.is_null() {
                    continue;
                }
                // GetInt64 also exposes the raw bits of a typed Real datum.
                let candidate = value.get_int64().to_ne_bytes();
                result.with_fixed_cells_mut(|values, _| {
                    if needle_packed && !needle_has_nulls {
                        for (out, a) in values.iter_mut().zip(needle_raw.fixed_cells::<8>()) {
                            if (*out == [0; 8]) & equal(a, &candidate) {
                                *out = index;
                            }
                        }
                    } else {
                        for (row, out) in values.iter_mut().enumerate() {
                            if *out == [0; 8]
                                && needle_at(row).is_some_and(|a| equal(a, &candidate))
                            {
                                *out = index;
                            }
                        }
                    }
                });
                continue;
            }
        }

        let candidate = vec_eval::read_argument(arg, domain, ctx, input)?;
        let (column, selection) = candidate.source();
        result.with_fixed_cells_mut(|values, _| {
            if needle_packed && selection.is_none() {
                let candidate_raw = column.raw_cells();
                let a = needle_raw.fixed_cells::<8>();
                let b = candidate_raw.fixed_cells::<8>();
                if !needle_has_nulls && column.null_count() == 0 {
                    for ((out, a), b) in values.iter_mut().zip(a).zip(b) {
                        if (*out == [0; 8]) & equal(a, b) {
                            *out = index;
                        }
                    }
                } else {
                    let valid = needle_raw
                        .not_null_bitmap()
                        .iter()
                        .zip(candidate_raw.not_null_bitmap());
                    for (((out, a), b), (&left, &right)) in values
                        .chunks_mut(8)
                        .zip(a.chunks(8))
                        .zip(b.chunks(8))
                        .zip(valid)
                    {
                        let valid = left & right;
                        for (bit, ((out, a), b)) in out.iter_mut().zip(a).zip(b).enumerate() {
                            if (*out == [0; 8]) & (valid & (1 << bit) != 0) & equal(a, b) {
                                *out = index;
                            }
                        }
                    }
                }
            } else {
                let candidate_cells = candidate.cells();
                let candidate_at = candidate_cells.fixed::<8>();
                for (row, out) in values.iter_mut().enumerate() {
                    if *out == [0; 8] {
                        if let (Some(a), Some(b)) = (needle_at(row), candidate_at(row)) {
                            if equal(a, b) {
                                *out = index;
                            }
                        }
                    }
                }
            }
        });
    }
    Ok(())
}

fn field_string(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use tidb_charset::Collation::*;
    let collation = function.derived_collation();
    match collation {
        Binary | Utf8Mb40900Bin => field_string_with(function, ctx, input, result, field_bytes_eq),
        AsciiBin | Latin1Bin | Utf8Bin | Utf8Mb4Bin => {
            field_string_with(function, ctx, input, result, pad_space_eq)
        }
        _ => field_string_with(function, ctx, input, result, |a, b| {
            collation.compare(a, b).is_eq()
        }),
    }
}

// Two overlapping words cover the payload of a short binary key.
fn field_bytes_eq(a: &[u8], b: &[u8]) -> bool {
    let len = a.len();
    if len != b.len() {
        return false;
    }
    if (8..=16).contains(&len) {
        a.first_chunk::<8>() == b.first_chunk::<8>() && a.last_chunk::<8>() == b.last_chunk::<8>()
    } else {
        a == b
    }
}

// Equal strings under PAD SPACE can differ only by extra spaces on the
// longer string. Compare the common prefix without scanning both suffixes.
fn pad_space_eq(a: &[u8], b: &[u8]) -> bool {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let (prefix, suffix) = long.split_at(short.len());
    field_bytes_eq(short, prefix) && suffix.iter().all(|&byte| byte == b' ')
}

fn field_string_with(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    equal: impl Fn(&[u8], &[u8]) -> bool,
) -> Result<(), EvalError> {
    let needle = StringArgument::read(&function.args[0], ctx, input)?;
    let needle_column = match &needle {
        StringArgument::Rows(arg) if arg.source().1.is_none() => Some(arg.source().0),
        _ => None,
    };
    let needle_has_nulls = needle_column.is_some_and(|column| column.null_count() != 0);
    let needle_raw = needle_column.map(Column::raw_cells);
    let needle = needle.cells();
    result.resize_int64(input.num_rows(), false);
    result.with_fixed_cells_mut(|values, _| values.fill([0; 8]));
    for (index, arg) in function.args.iter().enumerate().skip(1) {
        let candidate = StringArgument::read(arg, ctx, input)?;
        let index = (index as i64).to_ne_bytes();
        if let (Some(a), StringArgument::Rows(b)) = (&needle_raw, &candidate) {
            let (b, selection) = b.source();
            if selection.is_none() {
                let b_has_nulls = b.null_count() != 0;
                let b = b.raw_cells();
                let (a_bytes, b_bytes) = (a.variable_cells(), b.variable_cells());
                result.with_fixed_cells_mut(|values, _| {
                    if !needle_has_nulls && !b_has_nulls {
                        for (row, out) in values.iter_mut().enumerate() {
                            if *out == [0; 8] && equal(a_bytes(row), b_bytes(row)) {
                                *out = index;
                            }
                        }
                    } else {
                        for (row, out) in values.iter_mut().enumerate() {
                            if *out == [0; 8]
                                && !a.is_null(row)
                                && !b.is_null(row)
                                && equal(a_bytes(row), b_bytes(row))
                            {
                                *out = index;
                            }
                        }
                    }
                });
                continue;
            }
        }
        let candidate = candidate.cells();
        result.with_fixed_cells_mut(|values, _| {
            for (row, out) in values.iter_mut().enumerate() {
                if *out == [0; 8] {
                    if let (Some(a), Some(b)) = (needle.bytes(row), candidate.bytes(row)) {
                        if equal(a, b) {
                            *out = index;
                        }
                    }
                }
            }
        });
    }
    Ok(())
}

/// Go's vector LOCATE and INSTR search the original bytes, or the bytes
/// after strings.ToLower for case-insensitive UTF-8 signatures.
fn string_search_text<const START: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    if tidb_datatype::is_ci_collation(function.derived_collation().name()) {
        string_search::<false, true, START>(function, ctx, input, result)
    } else {
        string_search::<false, false, START>(function, ctx, input, result)
    }
}

fn string_search<const BINARY: bool, const CI: bool, const START: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let first = StringArgument::read(&function.args[0], ctx, input)?;
    let second = StringArgument::read(&function.args[1], ctx, input)?;
    let (needle, hay) = if matches!(
        function.pb_code(),
        ScalarFuncSig::Instr | ScalarFuncSig::InstrUtf8
    ) {
        (&second, &first)
    } else {
        (&first, &second)
    };
    let positions = if START {
        let arg = &function.args[2];
        if matches!(arg, Expression::Column(_))
            && arg
                .static_type()
                .is_some_and(|field| field.eval_type() == EvalType::Int && !field.is_hybrid())
        {
            Some(vec_eval::read_argument(arg, EvalType::Int, ctx, input)?)
        } else {
            // Go evaluates the third argument into the result. Keep that
            // buffer for evaluated expressions and their partial results.
            vec_eval::eval_into(arg, EvalType::Int, ctx, input, result)?;
            None
        }
    } else {
        None
    };

    let mut hay_lower = Vec::new();
    if !CI {
        if let (StringArgument::Rows(needle), StringArgument::Rows(hay)) = (needle, hay) {
            let (needle, needle_sel) = needle.source();
            let (hay, hay_sel) = hay.source();
            if needle_sel.is_none() && hay_sel.is_none() {
                let nullable = needle.null_count() != 0 || hay.null_count() != 0;
                let (needle, hay) = (needle.raw_cells(), hay.raw_cells());
                let (needle_valid, hay_valid) = (needle.not_null_bitmap(), hay.not_null_bitmap());
                let (needle, hay) = (needle.variable_cells(), hay.variable_cells());
                write_search_results::<START>(
                    input.num_rows(),
                    result,
                    positions.as_ref(),
                    |row, position| {
                        if nullable
                            && needle_valid[row / 8] & hay_valid[row / 8] & (1 << (row % 8)) == 0
                        {
                            return None;
                        }
                        let (needle, hay) = (needle(row), hay(row));
                        let len = search_length::<BINARY>(needle, START);
                        Some(search_position::<BINARY, false>(
                            hay,
                            len,
                            needle,
                            position,
                            &mut hay_lower,
                            |hay| index_string_bytes(hay, needle),
                        ))
                    },
                );
                return Ok(());
            }
        }
    }

    let (needle, hay) = (needle.cells(), hay.cells());
    let mut literal_lower = Vec::new();
    let literal = if let StringCells::Literal(Some(bytes)) = needle {
        let lowered = if CI {
            lowercase_search_bytes(bytes, &mut literal_lower)
        } else {
            bytes
        };
        Some((
            search_length::<BINARY>(bytes, START),
            memmem::Finder::new(lowered),
        ))
    } else {
        None
    };
    let mut needle_lower = Vec::new();
    write_search_results::<START>(
        input.num_rows(),
        result,
        positions.as_ref(),
        |row, position| {
            let hay = hay.bytes(row)?;
            let bytes = needle.bytes(row)?;
            let (len, finder) = if let Some((len, finder)) = &literal {
                (*len, finder)
            } else {
                let len = search_length::<BINARY>(bytes, START);
                let bytes = if CI {
                    lowercase_search_bytes(bytes, &mut needle_lower)
                } else {
                    bytes
                };
                return Some(search_position::<BINARY, CI>(
                    hay,
                    len,
                    bytes,
                    position,
                    &mut hay_lower,
                    |hay| index_string_bytes(hay, bytes),
                ));
            };
            Some(search_position::<BINARY, CI>(
                hay,
                len,
                finder.needle(),
                position,
                &mut hay_lower,
                |hay| finder.find(hay),
            ))
        },
    );
    Ok(())
}

fn write_search_results<const START: bool>(
    rows: usize,
    result: &mut Column,
    positions: Option<&vec_eval::ReadArgument<'_>>,
    mut lookup: impl FnMut(usize, Option<i64>) -> Option<i64>,
) {
    if let Some(positions) = positions {
        let (column, selection) = positions.source();
        if selection.is_none() && column.null_count() == 0 {
            let raw = column.raw_cells();
            result.write_int64_rows(
                (0..rows)
                    .zip(raw.fixed_cells::<8>())
                    .map(|(row, &position)| lookup(row, Some(i64::from_ne_bytes(position)))),
            );
            return;
        }
        let positions = positions.cells();
        let position_at = positions.fixed::<8>();
        result.write_int64_rows((0..rows).map(|row| {
            let &position = position_at(row)?;
            lookup(row, Some(i64::from_ne_bytes(position)))
        }));
    } else if START {
        result.with_fixed_cells_mut::<8, _>(|values, bitmap| {
            for (row, value) in values.iter_mut().enumerate() {
                let bit = 1 << (row & 7);
                if bitmap[row >> 3] & bit != 0 {
                    if let Some(found) = lookup(row, Some(i64::from_ne_bytes(*value))) {
                        *value = found.to_ne_bytes();
                    } else {
                        bitmap[row >> 3] &= !bit;
                    }
                }
            }
        });
    } else {
        result.write_int64_rows((0..rows).map(|row| lookup(row, None)));
    }
}

#[inline]
fn index_string_bytes(hay: &[u8], needle: &[u8]) -> Option<usize> {
    match needle {
        [_, _] => index_short_bytes(hay, needle.first_chunk::<2>().unwrap()),
        [_, _, _] => index_short_bytes(hay, needle.first_chunk::<3>().unwrap()),
        [_, _, _, _] => index_short_bytes(hay, needle.first_chunk::<4>().unwrap()),
        _ if hay.starts_with(needle) => Some(0),
        _ => memmem::find(hay, needle),
    }
}

#[inline]
fn index_short_bytes<const N: usize>(hay: &[u8], needle: &[u8; N]) -> Option<usize> {
    if hay.first_chunk::<N>()? == needle {
        Some(0)
    } else {
        index_short_tail(hay, needle)
    }
}

fn index_short_tail<const N: usize>(hay: &[u8], needle: &[u8; N]) -> Option<usize> {
    let end = hay.len() - N + 1;
    for (misses, offset) in memchr_iter(needle[0], &hay[1..end]).enumerate() {
        let index = offset + 1;
        if hay[index..].first_chunk::<N>() == Some(needle) {
            return Some(index);
        }
        // Switch to a multi-byte filter when the first byte is common.
        if misses >= 3 {
            return memmem::find(&hay[index + 1..], needle).map(|offset| offset + index + 1);
        }
    }
    None
}

fn search_length<const BINARY: bool>(bytes: &[u8], has_start: bool) -> usize {
    if !BINARY && has_start && !bytes.is_ascii() {
        crate::build::go_utf8_rune_count(bytes)
    } else {
        bytes.len()
    }
}

#[inline]
fn search_position<const BINARY: bool, const CI: bool>(
    hay: &[u8],
    needle_len: usize,
    needle: &[u8],
    position: Option<i64>,
    lower: &mut Vec<u8>,
    find: impl FnOnce(&[u8]) -> Option<usize>,
) -> i64 {
    let text;
    let mut start = 0;
    let mut hay = hay;
    if let Some(position) = position {
        start = position.wrapping_sub(1);
        let Ok(offset) = usize::try_from(start) else {
            return 0;
        };
        if BINARY {
            let Some(suffix) = hay.get(offset..) else {
                return 0;
            };
            hay = suffix;
        } else if hay.is_ascii() {
            if hay
                .len()
                .checked_sub(offset)
                .is_none_or(|left| left < needle_len)
            {
                return 0;
            }
            hay = &hay[offset..];
        } else {
            // Go uses string([]rune(hay)[offset:]), normalizing invalid bytes
            // in the suffix even for a case-sensitive collation.
            text = crate::string_signature::go_utf8_text(hay);
            if text
                .chars()
                .count()
                .checked_sub(offset)
                .is_none_or(|left| left < needle_len)
            {
                return 0;
            }
            let offset = text
                .char_indices()
                .nth(offset)
                .map_or(text.len(), |(index, _)| index);
            hay = &text.as_bytes()[offset..];
        }
    }
    if (position.is_some() || CI) && needle.is_empty() {
        return start + 1;
    }
    if CI {
        hay = lowercase_search_bytes(hay, lower);
    }
    find(hay).map_or(0, |offset| {
        let offset = if BINARY || offset == 0 || hay[..offset].is_ascii() {
            offset
        } else {
            crate::build::go_utf8_rune_count(&hay[..offset])
        };
        start + offset as i64 + 1
    })
}

fn lowercase_search_bytes<'a>(bytes: &'a [u8], lower: &'a mut Vec<u8>) -> &'a [u8] {
    lower.clear();
    if bytes.is_ascii() {
        lower.extend_from_slice(bytes);
        lower.make_ascii_lowercase();
    } else if let Some(text) = simdutf8::basic::from_utf8(bytes)
        .ok()
        .filter(|_| memchr(0xf4, bytes).is_none())
    {
        lower.extend_from_slice(tidb_mysql::to_lowercase_cow(text).as_bytes());
    } else {
        // Invalid bytes or characters in the escape range use the encoding
        // expected by the Go case mapping implementation.
        use tidb_hack::raw_bytes::{bytes_to_str, str_to_bytes};
        let input = bytes_to_str(bytes);
        let text = tidb_mysql::to_lowercase_cow(&input);
        lower.extend_from_slice(&str_to_bytes(&text));
    }
    lower
}

/// Go evaluates the precision and locale columns before the number column.
fn format_number(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use crate::string_fn::{append_format_number, append_format_real, format_locale};

    let precision = vec_eval::read_argument(&function.args[1], EvalType::Int, ctx, input)?;
    let locale = function
        .args
        .get(2)
        .map(|arg| StringArgument::read(arg, ctx, input))
        .transpose()?;
    let domain = if crate::wrap_cast::type_of(&function.args[0]).eval_type() == EvalType::Decimal {
        EvalType::Decimal
    } else {
        EvalType::Real
    };
    let number = vec_eval::read_argument(&function.args[0], domain, ctx, input)?;
    let (number, precision) = (number.cells(), precision.cells());
    let precision = precision.fixed::<8>();
    let locale = locale.as_ref().map(StringArgument::cells);
    let default_style = tidb_mysql::locale::locale_format_style("en_US").0;

    let rows = input.num_rows();
    let mut digits = Vec::new();
    let mut formatted = Vec::new();
    result.reserve_string(rows);
    result.try_append_byte_rows(rows, |row, output| {
        let (Some(number), Some(&precision)) = (number.bytes(row), precision(row)) else {
            return Ok(false);
        };
        let locale_text;
        let style = if let Some(locales) = &locale {
            locale_text = locales
                .bytes(row)
                .map(tidb_hack::raw_bytes::bytes_to_str);
            format_locale(locale_text.as_deref(), ctx)
        } else {
            default_style
        };
        digits.clear();
        if domain == EvalType::Decimal {
            tidb_datatype::MyDecimal::from_raw_bytes(
                number.try_into().expect("FORMAT decimal cell width"),
            )
            .expect("chunk decimal cell holds a valid MyDecimal")
            .append_result_string_bytes(&mut digits);
        } else {
            let value = f64::from_ne_bytes(number.try_into().expect("FORMAT real cell width"));
            append_format_real(value, &mut digits);
        }
        formatted.clear();
        append_format_number(
            &mut digits,
            i64::from_ne_bytes(precision),
            style,
            &mut formatted,
        );
        output.append(&formatted);
        Ok(true)
    })
}

enum StringArgument<'a> {
    Literal(Datum),
    Rows(vec_eval::ReadArgument<'a>),
}

impl<'a> StringArgument<'a> {
    fn read(arg: &Expression, ctx: &dyn Columns, input: &'a Chunk) -> Result<Self, EvalError> {
        if let Expression::Constant(literal) = arg {
            if literal.deferred_expr.is_none() && input.num_rows() != 0 {
                return literal
                    .eval_typed_as_on_row(ctx, tidb_chunk::row::Row::empty(), EvalType::String)
                    .map(Self::Literal);
            }
        }
        vec_eval::read_argument(arg, EvalType::String, ctx, input).map(Self::Rows)
    }

    fn cells(&self) -> StringCells<'_> {
        match self {
            Self::Literal(value) => StringCells::Literal(value.as_raw_bytes()),
            Self::Rows(value) => StringCells::Rows(value.cells()),
        }
    }
}

enum StringCells<'a> {
    Literal(Option<&'a [u8]>),
    Rows(vec_eval::ReadArgumentCells<'a>),
}

impl StringCells<'_> {
    #[inline]
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Literal(bytes) => bytes.is_none(),
            Self::Rows(cells) => cells.is_null(row),
        }
    }

    #[inline]
    fn bytes(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Literal(bytes) => *bytes,
            Self::Rows(cells) => cells.bytes(row),
        }
    }
}

struct TranslateTable {
    // None preserves the input. Some(None) deletes it.
    byte_keys: [Option<Option<char>>; 256],
    rune_keys: std::collections::HashMap<char, Option<char>>,
}

impl TranslateTable {
    fn build<const BINARY: bool>(&mut self, from: &[u8], to: &[u8]) -> bool {
        self.byte_keys.fill(None);
        self.rune_keys.clear();
        let byte_output = BINARY || to.is_ascii();
        if BINARY || (byte_output && from.is_ascii()) {
            for (index, &key) in from.iter().enumerate() {
                self.byte_keys[usize::from(key)]
                    .get_or_insert_with(|| to.get(index).copied().map(char::from));
            }
        } else {
            let mut to = tidb_goruntime::go_chars(to);
            for key in tidb_goruntime::go_chars(from) {
                let replacement = to.next();
                if let Some(entry) = self.byte_keys.get_mut(key as usize) {
                    entry.get_or_insert(replacement);
                } else {
                    self.rune_keys.entry(key).or_insert(replacement);
                }
            }
        }
        byte_output
    }

    fn apply<'a, const BINARY: bool>(
        &self,
        source: &[u8],
        byte_output: bool,
        output: &'a mut Vec<u8>,
    ) -> &'a [u8] {
        output.clear();
        output.reserve(source.len());
        if BINARY || (byte_output && source.is_ascii()) {
            output.extend(source.iter().filter_map(|&byte| {
                match self.byte_keys[usize::from(byte)] {
                    None => Some(byte),
                    Some(replacement) => replacement.map(|ch| ch as u8),
                }
            }));
        } else {
            let mut encoded = [0; 4];
            for key in tidb_goruntime::go_chars(source) {
                let value = if let Some(&value) = self.byte_keys.get(key as usize) {
                    value
                } else {
                    self.rune_keys.get(&key).copied()
                };
                if let Some(ch) = value.unwrap_or(Some(key)) {
                    output.extend_from_slice(ch.encode_utf8(&mut encoded).as_bytes());
                }
            }
        }
        output
    }
}

/// Go's INSERT vector signatures check packet size only after finding an
/// in-range position, then copy the two retained pieces around the replacement.
fn insert<const UTF8: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let source = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let rows = input.num_rows();
    // Plain constants already broadcast the result of one typed evaluation.
    // Keep that value directly and let deferred expressions evaluate as columns.
    let constants = match &function.args[1..] {
        [Expression::Constant(position), Expression::Constant(length), Expression::Constant(replacement)]
            if [position, length, replacement]
                .iter()
                .all(|arg| arg.deferred_expr.is_none()) =>
        {
            Some((position, length, replacement))
        }
        _ => None,
    };
    if let Some((position, length, replacement)) = constants {
        use tidb_chunk::row::Row;
        if rows == 0 {
            result.reserve_string(0);
            return Ok(());
        }
        let position = position.eval_typed_as_on_row(ctx, Row::empty(), EvalType::Int)?;
        let length = length.eval_typed_as_on_row(ctx, Row::empty(), EvalType::Int)?;
        let replacement = replacement.eval_typed_as_on_row(ctx, Row::empty(), EvalType::String)?;
        if position.is_null() || length.is_null() || replacement.is_null() {
            result.reserve_string(rows);
            result.append_n_nulls(rows);
            return Ok(());
        }
        let position = position.get_int64();
        let length = length.get_int64();
        let replacement = replacement.as_raw_bytes().expect("typed string constant");
        let (column, selection) = source.source();
        if selection.is_none()
            && column.rows() == rows
            && insert_positions_out_of_range(column, std::iter::repeat_n(position, rows))
            && column.null_count() == 0
        {
            column.copy_construct_to(result);
            return Ok(());
        }
        let source = source.cells();
        let packet = ctx.max_allowed_packet();
        result.reserve_string(rows);
        return result.try_append_byte_rows(rows, |row, output| {
            let Some(source) = source.bytes(row) else {
                return Ok(false);
            };
            insert_row::<UTF8>(source, position, length, replacement, packet, ctx, output)
        });
    }
    let position = vec_eval::read_argument(&function.args[1], EvalType::Int, ctx, input)?;
    let length = vec_eval::read_argument(&function.args[2], EvalType::Int, ctx, input)?;
    let replacement = vec_eval::read_argument(&function.args[3], EvalType::String, ctx, input)?;
    if copy_insert_noops([&source, &position, &length, &replacement], rows, result) {
        return Ok(());
    }
    let (source, position, length, replacement) = (
        source.cells(),
        position.cells(),
        length.cells(),
        replacement.cells(),
    );
    let position = position.fixed::<8>();
    let length = length.fixed::<8>();
    let packet = ctx.max_allowed_packet();
    result.reserve_string(rows);
    result.try_append_byte_rows(rows, |row, output| {
        let (Some(source), Some(&position), Some(&length), Some(replacement)) = (
            source.bytes(row),
            position(row),
            length(row),
            replacement.bytes(row),
        ) else {
            return Ok(false);
        };
        insert_row::<UTF8>(
            source,
            i64::from_ne_bytes(position),
            i64::from_ne_bytes(length),
            replacement,
            packet,
            ctx,
            output,
        )
    })
}

#[inline]
fn insert_row<const UTF8: bool>(
    source: &[u8],
    position: i64,
    length: i64,
    replacement: &[u8],
    packet: u64,
    ctx: &dyn Columns,
    output: &mut ByteRow<'_>,
) -> Result<bool, EvalError> {
    let length = usize::try_from(length).unwrap_or(usize::MAX);
    let text;
    let parts = if position <= 0 || position as u64 > source.len() as u64 {
        None
    } else {
        let start = position as usize - 1;
        if !UTF8 || source.is_ascii() {
            let end = start.saturating_add(length).min(source.len());
            Some((&source[..start], &source[end..]))
        } else {
            text = crate::string_signature::go_utf8_text(source);
            text.char_indices().nth(start).map(|(start, _)| {
                let tail = &text[start..];
                let end = tail
                    .char_indices()
                    .nth(length)
                    .map_or(tail.len(), |(i, _)| i);
                (&text.as_bytes()[..start], &tail.as_bytes()[end..])
            })
        }
    };
    if let Some((head, tail)) = parts {
        let size = head.len() + replacement.len() + tail.len();
        if size as u64 > packet {
            crate::expression_errors::handle_allowed_packet_overflowed(ctx, "insert")?;
            return Ok(false);
        }
        output.reserve(size);
        output.append(head);
        output.append(replacement);
        output.append(tail);
    } else {
        output.reserve(source.len());
        output.append(source);
    }
    Ok(true)
}

/// Byte length is an upper bound for both INSERT signatures. A whole batch
/// of out-of-range positions can copy the source column once.
fn copy_insert_noops(
    args: [&vec_eval::ReadArgument<'_>; 4],
    rows: usize,
    result: &mut Column,
) -> bool {
    let [
        (source, None),
        (position, None),
        (length, None),
        (replacement, None),
    ] = args.map(vec_eval::ReadArgument::source)
    else {
        return false;
    };
    if rows == 0
        || [source, position, length, replacement]
            .iter()
            .any(|col| col.rows() != rows)
    {
        return false;
    }
    let positions = position.raw_cells();
    if !insert_positions_out_of_range(
        source,
        positions
            .fixed_cells::<8>()
            .iter()
            .copied()
            .map(i64::from_ne_bytes),
    ) || [source, position, length, replacement]
        .iter()
        .any(|col| col.null_count() != 0)
    {
        return false;
    }
    source.copy_construct_to(result);
    true
}

fn insert_positions_out_of_range(source: &Column, positions: impl Iterator<Item = i64>) -> bool {
    let values = source.raw_cells();
    positions
        .enumerate()
        .all(|(row, pos)| pos <= 0 || pos as u64 > values.get(row).len() as u64)
}

/// Go `builtinConcatSig.vecEvalString`: append each argument column's bytes
/// before evaluating the next, including packet warnings in that order.
fn concat(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let rows = input.num_rows();
    let mut lengths = vec![Some(0_usize); rows];
    let mut args = Vec::with_capacity(function.args.len());
    result.reserve_string(rows);
    for arg in &function.args {
        let buffer = vec_eval::read_argument(arg, EvalType::String, ctx, input)?;
        let cells = buffer.cells();
        for (row, length) in lengths.iter_mut().enumerate() {
            let Some(current) = *length else {
                continue;
            };
            let Some(bytes) = cells.bytes(row) else {
                *length = None;
                continue;
            };
            let total = current.checked_add(bytes.len());
            if !total.is_some_and(|total| total as u64 <= ctx.max_allowed_packet()) {
                crate::expression_errors::handle_allowed_packet_overflowed(ctx, "concat")?;
                *length = None;
            } else {
                *length = total;
            }
        }
        drop(cells);
        args.push(buffer);
    }
    let columns: Vec<_> = args.iter().map(vec_eval::ReadArgument::cells).collect();
    result.append_byte_rows(lengths.into_iter().enumerate().map(|(row, length)| {
        length.map(|length| (length, columns.iter().map(move |column| column.bytes(row).unwrap())))
    }));
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
fn trim_cells(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let bytes = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let remove = function
        .args
        .get(1)
        .map(|arg| vec_eval::read_argument(arg, EvalType::String, ctx, input))
        .transpose()?;
    let direction = function
        .args
        .get(2)
        .map(|arg| vec_eval::read_argument(arg, EvalType::Int, ctx, input))
        .transpose()?;
    let bytes = bytes.cells();
    let remove = remove.as_ref().map(vec_eval::ReadArgument::cells);
    let direction = direction.as_ref().map(vec_eval::ReadArgument::cells);
    result.reserve_string(input.num_rows());
    result.append_byte_rows((0..input.num_rows()).map(|row| {
        let bytes = bytes.bytes(row)?;
        let remove = remove.as_ref().map_or(Some(b" ".as_slice()), |cell| cell.bytes(row))?;
        let direction = direction.as_ref().map_or(Some(1), |cell| cell.int64(row))?;
        let range = crate::string_fn::trim_range(bytes, remove, trim_direction(direction));
        Some((range.len(), std::iter::once(&bytes[range])))
    }));
    Ok(())
}

fn trim_direction(value: i64) -> tidb_ast::TrimDirection {
    match value {
        2 => tidb_ast::TrimDirection::Leading,
        3 => tidb_ast::TrimDirection::Trailing,
        _ => tidb_ast::TrimDirection::Both,
    }
}

fn trim(values: &[Datum]) -> Result<Datum, EvalError> {
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let remove = values
        .get(1)
        .cloned()
        .unwrap_or_else(|| Datum::new_string_datum(" "));
    let direction = trim_direction(values.get(2).map_or(1, Datum::get_int64));
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

/// Go `builtinRepeatSig.vecEvalString`: after the packet check a cell longer
/// than `flen / num` is a silent NULL, a check its row form does not have.
fn repeat(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let value = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let count = vec_eval::read_argument(&function.args[1], EvalType::Int, ctx, input)?;
    let values = value.cells();
    let counts = count.cells();
    let counts = counts.fixed::<8>();
    let flen = function.get_static_type().expect("REPEAT type").flen();
    let rows = input.num_rows();
    let (source, selection) = value.source();
    let (count, _) = count.source();
    let source_cells = source.raw_cells();
    let payload = source_cells.data_len() as u64;
    if payload == 0 {
        result.reserve_string(rows);
        result.append_byte_rows((0..rows).map(|row| {
            let row_index = selection.map_or(row, |selection| selection[row]);
            (!source_cells.is_null(row_index) && counts(row).is_some())
                .then_some((0, std::iter::empty()))
        }));
        return Ok(());
    }
    // If each count is at most one, the entire input payload bounds every result.
    let within_limits = payload <= ctx.max_allowed_packet().min(flen.max(0) as u64);
    let raw_counts = count.raw_cells();
    let raw_counts = raw_counts.fixed_cells::<8>();
    if within_limits
        && selection.is_none()
        && rows == source.rows()
        && raw_counts.iter().all(|&count| count == 1_i64.to_ne_bytes())
        && count.null_count() == 0
        && source.null_count() == 0
    {
        source.copy_construct_to(result);
        return Ok(());
    }
    result.reserve_string(rows);
    if within_limits && raw_counts.iter().all(|&count| i64::from_ne_bytes(count) <= 1) {
        result.append_byte_rows((0..rows).map(|row| {
            let value = values.bytes(row)?;
            let count = i64::from_ne_bytes(*counts(row)?);
            let bytes = if count == 1 { value } else { b"" };
            Some((bytes.len(), std::iter::once(bytes)))
        }));
        return Ok(());
    }
    let mut buffer = Vec::new();
    for row in 0..rows {
        let (Some(value), Some(&count)) = (values.bytes(row), counts(row)) else {
            result.append_null();
            continue;
        };
        let count = i64::from_ne_bytes(count);
        match crate::string_packet::repeat_output_len(value.len(), count, Some(flen), ctx)? {
            None => result.append_null(),
            Some(0) => result.append_bytes(b""),
            Some(width) if width == value.len() => result.append_bytes(value),
            Some(width) => {
                buffer.clear();
                buffer.reserve(width);
                buffer.extend_from_slice(value);
                while buffer.len() < width {
                    let copy = buffer.len().min(width - buffer.len());
                    buffer.extend_from_within(..copy);
                }
                result.append_bytes(&buffer);
            }
        }
    }
    Ok(())
}

/// Go's four LPAD/RPAD vector signatures read the same three typed arguments.
fn pad<const UTF8: bool>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let source = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let width = vec_eval::read_argument(&function.args[1], EvalType::Int, ctx, input)?;
    let pattern = vec_eval::read_argument(&function.args[2], EvalType::String, ctx, input)?;
    let left = matches!(
        function.pb_code(),
        ScalarFuncSig::Lpad | ScalarFuncSig::LpadUtf8
    );
    let packet = ctx.max_allowed_packet();
    let flen = function
        .get_static_type()
        .expect("LPAD/RPAD result type")
        .flen();
    let unit = if UTF8 { 4 } else { 1 };
    let limit = if UTF8 {
        flen.div_euclid(unit).min(crate::wrap_cast::MAX_BLOB_WIDTH)
    } else {
        flen
    };
    let rows = input.num_rows();
    if limit >= 0
        && pad_packed_rows::<UTF8>(
            &source,
            &width,
            &pattern,
            rows,
            (packet / unit as u64).min(limit as u64),
            left,
            result,
        )
    {
        return Ok(());
    }
    let (sources, widths, patterns) = (source.cells(), width.cells(), pattern.cells());
    let widths = widths.fixed::<8>();
    result.reserve_string(rows);
    result.try_append_byte_rows(rows, |row, output| {
        let (Some(source), Some(&width)) = (sources.bytes(row), widths(row)) else {
            return Ok(false);
        };
        let width = i64::from_ne_bytes(width);
        if (width as u64).wrapping_mul(unit as u64) > packet {
            crate::expression_errors::handle_allowed_packet_overflowed(
                ctx,
                if left { "lpad" } else { "rpad" },
            )?;
            return Ok(false);
        }
        let Some(pattern) = patterns.bytes(row).filter(|_| width >= 0 && width <= limit) else {
            return Ok(false);
        };
        let width = width as usize;
        if width != 0 {
            if UTF8 {
                pad_text(source, width, pattern, left, output)?;
            } else {
                pad_byte_string(source, width, pattern, left, output);
            }
        }
        Ok(true)
    })
}

/// A dense batch with one target width can copy binary prefixes in bulk.
/// When the target exceeds the source's byte width, an empty pattern yields
/// empty strings in either signature because rune count cannot exceed bytes.
fn pad_packed_rows<const UTF8: bool>(
    source: &vec_eval::ReadArgument<'_>,
    width: &vec_eval::ReadArgument<'_>,
    pattern: &vec_eval::ReadArgument<'_>,
    rows: usize,
    limit: u64,
    left: bool,
    result: &mut Column,
) -> bool {
    let (source, source_sel) = source.source();
    let (width, width_sel) = width.source();
    let (pattern, pattern_sel) = pattern.source();
    if rows == 0
        || source_sel.is_some()
        || width_sel.is_some()
        || pattern_sel.is_some()
        || source.rows() != rows
        || width.rows() != rows
        || pattern.rows() != rows
    {
        return false;
    }
    let width_cells = width.raw_cells();
    let widths = width_cells.fixed_cells::<8>();
    let Some(first) = (0..rows).find(|&row| !width_cells.is_null(row)) else {
        return false;
    };
    let target = i64::from_ne_bytes(widths[first]) as u64;
    if target > limit {
        return false;
    }
    let target = target as usize;
    let source_cells = source.raw_cells();
    let stride = source_cells.get(0).len();
    if (target > stride && !source_cells.is_null(0) && pattern.raw_cells().data_len() != 0)
        || (UTF8 && target != 0 && target <= stride)
    {
        return false;
    }
    if !widths
        .iter()
        .enumerate()
        .all(|(row, value)| *value == widths[first] || width_cells.is_null(row))
    {
        return false;
    }
    if width.null_count() != 0 || source.null_count() != 0 || pattern.null_count() != 0 {
        if UTF8 {
            return false;
        }
        pad_nullable_binary_rows(
            source_cells,
            width_cells,
            pattern.raw_cells(),
            target,
            rows,
            left,
            result,
        );
        return true;
    }
    if target != 0 {
        let Some(bytes) = source_cells.packed_byte_rows(stride) else {
            return false;
        };
        if target == stride {
            source.copy_construct_to(result);
            return true;
        }
        if target < stride {
            result.reserve_string(rows);
            result.append_byte_rows(
                bytes
                    .chunks_exact(stride)
                    .map(|row| Some((target, std::iter::once(&row[..target])))),
            );
            return true;
        }
    }
    result.reserve_string(rows);
    result.append_byte_rows(std::iter::repeat_n(Some((0, std::iter::empty())), rows));
    true
}

fn pad_nullable_binary_rows(
    source: RawCells<'_>,
    width: RawCells<'_>,
    pattern: RawCells<'_>,
    target: usize,
    rows: usize,
    left: bool,
    result: &mut Column,
) {
    let (source_bitmap, width_bitmap, pattern_bitmap) = (
        source.not_null_bitmap(),
        width.not_null_bitmap(),
        pattern.not_null_bitmap(),
    );
    let present = |row: usize| {
        let index = row >> 3;
        let bits = source_bitmap[index] & width_bitmap[index] & pattern_bitmap[index];
        bits & (1 << (row & 7)) != 0
    };
    result.reserve_string(rows);
    if target == 0 {
        result
            .append_byte_rows((0..rows).map(|row| present(row).then_some((0, std::iter::empty()))));
        return;
    }
    result
        .try_append_byte_rows::<std::convert::Infallible>(rows, |row, output| {
            if !present(row) {
                return Ok(false);
            }
            let value = source.get(row);
            if target <= value.len() {
                output.reserve(target);
                output.append(&value[..target]);
            } else {
                // Dense target widths can still be paired with shorter source rows.
                pad_byte_string(value, target, pattern.get(row), left, output);
            }
            Ok(true)
        })
        .unwrap();
}

#[inline]
fn pad_byte_string(
    source: &[u8],
    width: usize,
    pattern: &[u8],
    left: bool,
    output: &mut ByteRow<'_>,
) {
    if width <= source.len() {
        output.reserve(width);
        output.append(&source[..width]);
    } else if !pattern.is_empty() {
        write_padded(output, source, pattern, width - source.len(), left);
    }
}

fn pad_text(
    source: &[u8],
    width: usize,
    pattern: &[u8],
    left: bool,
    output: &mut ByteRow<'_>,
) -> Result<(), EvalError> {
    if source.is_ascii() && (width <= source.len() || pattern.is_ascii()) {
        pad_byte_string(source, width, pattern, left, output);
        return Ok(());
    }
    if let Ok(text) = simdutf8::basic::from_utf8(source) {
        if let Some((end, _)) = text.char_indices().nth(width) {
            output.reserve(end);
            output.append(&source[..end]);
            return Ok(());
        }
        let units = text.chars().count();
        if units == width {
            output.reserve(source.len());
            output.append(source);
            return Ok(());
        }
        if pattern.is_empty() {
            return Ok(());
        }
        if let Ok(pad) = simdutf8::basic::from_utf8(pattern) {
            let needed = width - units;
            let pad_units = pad.chars().count();
            let tail = text_prefix(pad, needed % pad_units);
            write_padded(
                output,
                source,
                pattern,
                needed / pad_units * pad.len() + tail.len(),
                left,
            );
            return Ok(());
        }
    }
    pad_malformed_text(source, width, pattern, left, output)
}

fn text_prefix(text: &str, units: usize) -> &[u8] {
    let end = text
        .char_indices()
        .nth(units)
        .map_or(text.len(), |(end, _)| end);
    &text.as_bytes()[..end]
}

/// Preserve Go's decoding after repetition/concatenation. Malformed bytes
/// at those seams can combine into a valid rune.
#[cold]
fn pad_malformed_text(
    source: &[u8],
    width: usize,
    pattern: &[u8],
    left: bool,
    output: &mut ByteRow<'_>,
) -> Result<(), EvalError> {
    use crate::{build::go_utf8_rune_count, string_signature::go_utf8_text};
    let units = go_utf8_rune_count(source);
    if width <= units {
        let text = go_utf8_text(source);
        let prefix = text_prefix(&text, width);
        output.reserve(prefix.len());
        output.append(prefix);
    } else if !pattern.is_empty() {
        let needed = width - units;
        let copies = needed / go_utf8_rune_count(pattern) + 1;
        let pattern = pattern.repeat(copies);
        if left {
            let pattern = go_utf8_text(&pattern);
            let (prefix, zeros) = go_rune_slice_prefix(&pattern, needed)?;
            let source = go_utf8_text(source);
            output.reserve(prefix.len() + zeros + source.len());
            output.append(prefix);
            output.append_pattern(&[0], zeros);
            output.append(source.as_bytes());
        } else {
            let combined = [source, &pattern].concat();
            let combined = go_utf8_text(&combined);
            let (prefix, zeros) = go_rune_slice_prefix(&combined, width)?;
            output.reserve(prefix.len() + zeros);
            output.append(prefix);
            output.append_pattern(&[0], zeros);
        }
    }
    Ok(())
}

/// Go's nonescaping `[]rune(string)[:width]` can use the zeroed part of
/// the conversion buffer past the decoded length, up to its capacity.
fn go_rune_slice_prefix(text: &str, width: usize) -> Result<(&[u8], usize), EvalError> {
    if let Some((end, _)) = text.char_indices().nth(width) {
        return Ok((&text.as_bytes()[..end], 0));
    }
    let runes = text.chars().count();
    if width > runes {
        use tidb_goruntime::go_runtime::{
            go_64_next_slice_capacity_for_element, GoSliceElementLayout,
        };
        let capacity = if runes <= 32 {
            32
        } else {
            go_64_next_slice_capacity_for_element(runes, 0, 4, GoSliceElementLayout::NoPointers)
        };
        if width > capacity {
            return Err(EvalError::Unsupported("LPAD/RPAD rune slice exceeds capacity"));
        }
    }
    Ok((text.as_bytes(), width - runes))
}

/// Copy a whole pattern once and double from initialized bytes, stopping at
/// the exact byte width of the padding.
#[inline]
fn write_padded(output: &mut ByteRow<'_>, source: &[u8], pattern: &[u8], fill: usize, left: bool) {
    output.reserve(source.len() + fill);
    if !left {
        output.append(source);
    }
    output.append_pattern(pattern, fill);
    if left {
        output.append(source);
    }
}

fn space(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    static SPACES: [u8; 128] = [b' '; 128];
    let rows = input.num_rows();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Int,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let widths = cells.fixed_cells::<8>();
            result.reserve_string(rows);
            let direct_limit = ctx.max_allowed_packet().min(SPACES.len() as u64) as i64;
            if widths.iter().all(|&width| i64::from_ne_bytes(width) <= direct_limit) {
                result.append_byte_rows((0..rows).map(|row| {
                    let row = selection.map_or(row, |selection| selection[row]);
                    (!cells.is_null(row)).then(|| {
                        let width = i64::from_ne_bytes(widths[row]).max(0) as usize;
                        (width, std::iter::once(&SPACES[..width]))
                    })
                }));
                return Ok(());
            }
            let mut buffer = Vec::new();
            for row in 0..rows {
                let row = selection.map_or(row, |selection| selection[row]);
                if cells.is_null(row) {
                    result.append_null();
                    continue;
                }
                let width = i64::from_ne_bytes(widths[row]);
                match crate::string_packet::space_output_len(width, ctx)? {
                    None => result.append_null(),
                    Some(width) if width <= SPACES.len() => result.append_bytes(&SPACES[..width]),
                    Some(width) => {
                        if width > buffer.len() {
                            buffer.resize(width, b' ');
                        }
                        result.append_bytes(&buffer[..width]);
                    }
                }
            }
            Ok(())
        },
    )?
}

fn quote(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let rows = input.num_rows();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let mut buffer = String::new();
            result.reserve_string(rows);
            for row in 0..rows {
                let row = selection.map_or(row, |selection| selection[row]);
                if cells.is_null(row) {
                    result.append_bytes(b"NULL");
                } else {
                    buffer.clear();
                    crate::string_fn::append_quoted(&mut buffer, cells.get(row));
                    result.append_bytes(buffer.as_bytes());
                }
            }
        },
    )
}
