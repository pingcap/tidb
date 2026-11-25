// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_json_vec.go`: typed document, path, and value columns.

use super::{vec_eval, ScalarFunction};
use crate::builtin_ext::json::{binary_json_error, contains_path_mode, json_unquote_quoted_content, literal_root_path, literal_string, parse_json_path};
use crate::{Columns, EvalError, JsonError};
use std::collections::{hash_map::Entry, HashMap};
use tidb_chunk::{
    chunk::Chunk,
    column::{ByteRow, Column},
};
use tidb_datatype::{BinaryJSON, EncodedJSONLayout, EncodedJSONSelection, EvalType, JSONKeyArray, JSONPathExpression};
use tidb_proto::tipb::ScalarFuncSig;

/// Go JSON signature `vectorized` methods (JSON_VALID/SCHEMA_VALID are scalar).
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        JsonTypeSig
            | JsonUnquoteSig
            | JsonLengthSig
            | JsonObjectSig
            | JsonArraySig
            | JsonMergeSig
            | JsonSetSig
            | JsonInsertSig
            | JsonRemoveSig
            | JsonContainsSig
            | JsonMemberOfSig
            | JsonReplaceSig
            | JsonArrayAppendSig
            | JsonMergePatchSig
            | JsonExtractSig
            | JsonMergePreserveSig
            | JsonArrayInsertSig
            | JsonContainsPathSig
            | JsonDepthSig
            | JsonKeysSig
            | JsonKeys2ArgsSig
            | JsonPrettySig
            | JsonQuoteSig
            | JsonSearchSig
            | JsonStorageFreeSig
            | JsonStorageSizeSig
    ) || matches!(
        function.func_name.lowercase(),
        "json_overlaps" | "json_sum_crc32"
    )
}

/// Go `builtinJSON*Sig.vecEval*`, including `vecJSONModify`'s path-before-value order.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    match function.pb_code() {
        JsonUnquoteSig => return unquote(function, ctx, input, result),
        JsonObjectSig => return object(function, ctx, input, result),
        JsonTypeSig => return type_name(function, ctx, input, result),
        JsonStorageSizeSig | JsonStorageFreeSig => return storage(function, ctx, input, result),
        JsonLengthSig => return length(function, ctx, input, result),
        JsonKeys2ArgsSig | JsonExtractSig => return query(function, ctx, input, result),
        JsonContainsPathSig => return contains_path(function, ctx, input, result),
        _ => {}
    }
    if matches!(function.pb_code(), JsonDepthSig | JsonKeysSig)
        && function.args[0]
            .static_type()
            .is_some_and(|field| field.eval_type() == EvalType::Json)
    {
        return metadata(function, ctx, input, result);
    }
    let order: Vec<_> = match function.pb_code() {
        JsonSetSig | JsonInsertSig | JsonReplaceSig => std::iter::once(0)
            .chain((1..function.args.len()).step_by(2))
            .chain((2..function.args.len()).step_by(2))
            .collect(),
        _ => (0..function.args.len()).collect(),
    };
    vec_eval::map_arguments_in_order(function, ctx, input, result, None, &order, |values| {
        function.pb_builtin.eval_values(function, values, ctx)
    })
}

/// Go validates double-quoted values and copies unquoted strings verbatim.
fn unquote(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let quoted = |bytes: &[u8]| matches!(bytes, [b'"', .., b'"']);
    let rows = input.num_rows();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |source, selection| {
            let cells = source.raw_cells();
            let bytes_at = cells.variable_cells();
            result.reserve_string(rows);
            if selection.is_none()
                && source.null_count() == 0
                && cells.data_len() <= result.get_data_cap()
                && (0..rows).all(|row| !quoted(bytes_at(row)))
            {
                *result = source.copy_reconstruct(None, Some(std::mem::take(result)));
                return Ok(());
            }
            let mut decoded = Vec::new();
            result.try_append_byte_rows(rows, |row, output| {
                let row = selection.map_or(row, |selection| selection[row]);
                if cells.is_null(row) {
                    return Ok(false);
                }
                let bytes = bytes_at(row);
                if !quoted(bytes) {
                    output.append(bytes);
                    return Ok(true);
                }
                let inner = json_unquote_quoted_content(bytes)?;
                if memchr::memchr(b'\\', inner).is_some() {
                    decoded.clear();
                    tidb_datatype::unquote_json_string_bytes_to(inner, &mut decoded)
                        .map_err(binary_json_error)?;
                    output.append(&decoded);
                } else {
                    output.append(inner);
                }
                Ok(true)
            })
        },
    )?
}

/// Go JSON_TYPE reads only the tag and the payload metadata for opaque values.
fn type_name(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let rows = input.num_rows();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Json,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let bytes_at = cells.variable_cells();
            let present = cells.not_null_bitmap();
            let has_nulls = column.null_count() != 0;
            result.try_write_short_string_rows(rows, |row| {
                let row = selection.map_or(row, |selection| selection[row]);
                if has_nulls && present[row >> 3] & (1 << (row & 7)) == 0 {
                    return Ok(None);
                }
                BinaryJSON::encoded_type_name(bytes_at(row))
                    .map(Some)
                    .map_err(binary_json_error)
            })
        },
    )?
}

/// Go STORAGE_SIZE counts the bytes including the type tag. STORAGE_FREE
/// returns zero for each non-NULL row after evaluating the JSON argument.
fn storage(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let rows = input.num_rows();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Json,
        ctx,
        input,
        |column, selection| {
            result.resize_int64(rows, false);
            merge_selected_nulls(column, selection, result);
            if function.pb_code() == ScalarFuncSig::JsonStorageSizeSig {
                let cells = column.raw_cells();
                result.with_fixed_cells_mut(|values, bitmap| {
                    if let Some(selection) = selection {
                        let bytes_at = cells.variable_cells();
                        for (row, (value, &physical)) in values.iter_mut().zip(selection).enumerate() {
                            if bitmap[row >> 3] & (1 << (row & 7)) != 0 {
                                *value = (bytes_at(physical).len() as i64).to_ne_bytes();
                            }
                        }
                    } else {
                        cells.write_variable_lengths::<1>(values, bitmap);
                    }
                });
            }
        },
    )
}

fn merge_selected_nulls(column: &Column, selection: Option<&[usize]>, result: &mut Column) {
    if let Some(selection) = selection {
        result.with_fixed_cells_mut::<8, _>(|_, bitmap| {
            for (row, &physical) in selection.iter().enumerate() {
                if column.is_null(physical) {
                    bitmap[row >> 3] &= !(1 << (row & 7));
                }
            }
        });
    } else {
        result.merge_nulls(&[column]);
    }
}

fn length(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    result.resize_int64(input.num_rows(), false);
    let path = function.args.get(1).filter(|path| !literal_root_path(path));
    let Some(path) = path else {
        return vec_eval::with_argument_column(
            &function.args[0],
            EvalType::Json,
            ctx,
            input,
            |column, selection| {
                merge_selected_nulls(column, selection, result);
                let document = column.raw_cells();
                let bytes_at = document.variable_cells();
                result.with_fixed_cells_mut::<8, _>(|values, bitmap| {
                    for (row, out) in values.iter_mut().enumerate() {
                        if bitmap[row >> 3] & (1 << (row & 7)) != 0 {
                            let physical = selection.map_or(row, |selection| selection[row]);
                            let count = BinaryJSON::encoded_root_count(bytes_at(physical))
                                .map_err(binary_json_error)?;
                            *out = (count as i64).to_ne_bytes();
                        }
                    }
                    Ok(())
                })
            },
        )?;
    };
    let document = vec_eval::read_argument(&function.args[0], EvalType::Json, ctx, input)?;
    let (column, selection) = document.source();
    merge_selected_nulls(column, selection, result);
    let document = column.raw_cells();
    let bytes_at = document.variable_cells();
    let path = vec_eval::read_argument(path, EvalType::String, ctx, input)?;
    let (path_column, path_selection) = path.source();
    merge_selected_nulls(path_column, path_selection, result);
    let paths = path_column.raw_cells();
    let path_bytes_at = paths.variable_cells();
    let mut parsed = HashMap::<&[u8], JSONPathExpression>::with_capacity(input.num_rows().min(8));
    let mut previous_path = None;
    result.with_fixed_cells_mut::<8, _>(|values, bitmap| {
        for (row, out) in values.iter_mut().enumerate() {
            let bit = 1 << (row & 7);
            if bitmap[row >> 3] & bit == 0 {
                continue;
            }
            let physical = selection.map_or(row, |selection| selection[row]);
            let bytes = path_bytes_at(path_selection.map_or(row, |selection| selection[row]));
            let path = cached_previous_path(&mut parsed, &mut previous_path, bytes, true)?;
            if let Some(value) = BinaryJSON::get_encoded_length(bytes_at(physical), Some(path))
                .map_err(binary_json_error)?
            {
                *out = (value as i64).to_ne_bytes();
            } else {
                bitmap[row >> 3] &= !bit;
            }
        }
        Ok(())
    })
}

fn cached_path<'cache, 'input>(
    parsed: &'cache mut HashMap<&'input [u8], JSONPathExpression>,
    bytes: &'input [u8],
) -> Result<&'cache JSONPathExpression, EvalError> {
    // Bound storage when every row has a different path.
    if parsed.len() == 32 {
        parsed.clear();
    }
    Ok(match parsed.entry(bytes) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => entry.insert(parse_json_path(&String::from_utf8_lossy(bytes))?),
    })
}

fn check_exact_path(path: &JSONPathExpression) -> Result<(), EvalError> {
    if path.could_match_multiple_values() {
        return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
    }
    Ok(())
}

fn cached_previous_path<'state, 'input>(
    parsed: &mut HashMap<&'input [u8], JSONPathExpression>,
    previous: &'state mut Option<(&'input [u8], JSONPathExpression)>,
    bytes: &'input [u8],
    exact: bool,
) -> Result<&'state JSONPathExpression, EvalError> {
    if !previous.as_ref().is_some_and(|(previous, _)| *previous == bytes) {
        let path = cached_path(parsed, bytes)?;
        if exact {
            check_exact_path(path)?;
        }
        *previous = Some((bytes, path.clone()));
    }
    Ok(&previous.as_ref().expect("parsed JSON path").1)
}

fn query(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let keys = function.pb_code() == ScalarFuncSig::JsonKeys2ArgsSig;
    if !keys && function.args.len() == 2 && literal_root_path(&function.args[1]) {
        return vec_eval::eval_into(&function.args[0], EvalType::Json, ctx, input, result);
    }
    let source = vec_eval::read_argument(&function.args[0], EvalType::Json, ctx, input)?;
    let first = vec_eval::read_argument(&function.args[1], EvalType::String, ctx, input)?;
    let args = function.args[2..]
        .iter()
        .map(|arg| vec_eval::read_argument(arg, EvalType::String, ctx, input))
        .collect::<Result<Vec<_>, _>>()?;
    let paths = args.iter().map(|arg| arg.cells()).collect::<Vec<_>>();
    let first = first.cells();
    let (column, selection) = source.source();
    let document = column.raw_cells();
    let bytes_at = document.variable_cells();
    let rows = input.num_rows();
    let mut parsed = HashMap::with_capacity(rows.min(8));
    let mut previous_path = None;
    let mut row_paths = Vec::new();
    let mut layouts = Vec::new();
    result.reserve_json(rows);
    let outcome = result.try_append_byte_rows(rows, |row, output| {
        let physical = selection.map_or(row, |selection| selection[row]);
        if document.is_null(physical) {
            return Ok(false);
        }
        let cell = bytes_at(physical);
        let Some(bytes) = first.bytes(row) else {
            return Ok(false);
        };
        if keys {
            let path = cached_previous_path(&mut parsed, &mut previous_path, bytes, true)?;
            let Some(array) = BinaryJSON::get_encoded_key_array(cell, Some(path))
                .map_err(binary_json_error)?
            else {
                return Ok(false);
            };
            append_key_array(output, row, array, &mut layouts);
            return Ok(true);
        }
        let row_paths = if paths.is_empty() {
            let path = cached_previous_path(&mut parsed, &mut previous_path, bytes, false)?;
            std::slice::from_ref(path)
        } else {
            let path = cached_path(&mut parsed, bytes)?;
            row_paths.clear();
            row_paths.push(path.clone());
            for arg in &paths {
                let Some(bytes) = arg.bytes(row) else {
                    return Ok(false);
                };
                row_paths.push(cached_path(&mut parsed, bytes)?.clone());
            }
            row_paths.as_slice()
        };
        let Some(selected) = BinaryJSON::extract_encoded_for_sql(cell, row_paths).map_err(binary_json_error)? else {
            return Ok(false);
        };
        let (tag, bytes) = match &selected {
            EncodedJSONSelection::Borrowed { type_code, value, .. } => (*type_code, *value),
            EncodedJSONSelection::Owned(value) => (value.type_code(), value.value()),
        };
        if bytes.len() >= Column::MIN_RETAINED_CELL_BYTES - 1 {
            if let Some(layout) = selected.layout(document.known_json_cell_layout(physical)) {
                layouts.push((row, layout));
            }
        }
        output.reserve(1 + bytes.len());
        output.append(&[tag]);
        output.append(bytes);
        Ok(true)
    });
    for (row, layout) in layouts {
        result.remember_json_layout(row, layout);
    }
    outcome
}

#[inline]
fn append_key_array(
    output: &mut ByteRow<'_>,
    row: usize,
    array: JSONKeyArray<'_>,
    layouts: &mut Vec<(usize, EncodedJSONLayout)>,
) {
    let size = array.encoded_len();
    if size >= Column::MIN_RETAINED_CELL_BYTES {
        layouts.push((row, array.layout()));
    }
    output.append_sized(size, |cell| array.write_cell(cell));
}

enum PredicateString<'a> {
    Literal(Option<&'a [u8]>),
    Column(vec_eval::ReadArgument<'a>),
}

impl<'a> PredicateString<'a> {
    fn read(
        arg: &'a crate::expression::Expression,
        ctx: &dyn Columns,
        input: &'a Chunk,
    ) -> Result<Self, EvalError> {
        if let Some(literal) = literal_string(arg) {
            return Ok(Self::Literal(literal));
        }
        vec_eval::read_argument(arg, EvalType::String, ctx, input).map(Self::Column)
    }

    fn cells(&self) -> PredicateStringCells<'_> {
        match self {
            Self::Literal(value) => PredicateStringCells::Literal(*value),
            Self::Column(column) => PredicateStringCells::Column(column.cells()),
        }
    }
}

enum PredicateStringCells<'a> {
    Literal(Option<&'a [u8]>),
    Column(vec_eval::ReadArgumentCells<'a>),
}

impl PredicateStringCells<'_> {
    #[inline]
    fn bytes(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Literal(value) => *value,
            Self::Column(column) => column.bytes(row),
        }
    }
}

fn contains_path(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let source = vec_eval::read_argument(&function.args[0], EvalType::Json, ctx, input)?;
    let mode = PredicateString::read(&function.args[1], ctx, input)?;
    let args = function.args[2..]
        .iter()
        .map(|arg| PredicateString::read(arg, ctx, input))
        .collect::<Result<Vec<_>, _>>()?;
    let paths = args.iter().map(PredicateString::cells).collect::<Vec<_>>();
    let mut constant_paths = vec![None; paths.len()];
    let (column, selection) = source.source();
    let document = column.raw_cells();
    let bytes_at = document.variable_cells();
    let modes = mode.cells();
    let rows = input.num_rows();
    let mut parsed = HashMap::with_capacity(rows.min(8));
    let mut previous = None;
    result.resize_int64(rows, false);
    merge_selected_nulls(column, selection, result);
    let literal_mode = match &mode {
        PredicateString::Literal(None) => {
            result.resize_int64(rows, true);
            return Ok(());
        }
        PredicateString::Literal(Some(mode)) => Some(contains_path_mode(mode)),
        PredicateString::Column(mode) => {
            let (column, selection) = mode.source();
            merge_selected_nulls(column, selection, result);
            None
        }
    };
    result.with_fixed_cells_mut::<8, _>(|values, bitmap| {
        for (row, output) in values.iter_mut().enumerate() {
            let bit = 1 << (row & 7);
            if bitmap[row >> 3] & bit == 0 {
                continue;
            }
            let one = if let Some(mode) = &literal_mode {
                mode.clone()?
            } else {
                contains_path_mode(modes.bytes(row).expect("merged mode is present"))?
            };
            let physical = selection.map_or(row, |selection| selection[row]);
            let cell = bytes_at(physical);
            let mut contains = true;
            for (arg, constant_path) in paths.iter().zip(&mut constant_paths) {
                let Some(bytes) = arg.bytes(row) else {
                    bitmap[row >> 3] &= !bit;
                    break;
                };
                let path = match arg {
                    PredicateStringCells::Literal(_) => {
                        if constant_path.is_none() {
                            *constant_path = Some(parse_json_path(&String::from_utf8_lossy(bytes))?);
                        }
                        constant_path.as_ref().expect("parsed literal path")
                    }
                    PredicateStringCells::Column(_) => {
                        cached_previous_path(&mut parsed, &mut previous, bytes, false)?
                    }
                };
                contains = BinaryJSON::encoded_path_exists_for_sql(cell, path)
                    .map_err(binary_json_error)?;
                if contains == one {
                    break;
                }
            }
            if bitmap[row >> 3] & bit != 0 {
                *output = i64::from(contains).to_ne_bytes();
            }
        }
        Ok(())
    })
}

fn metadata(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let n = input.num_rows();
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Json,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let rows = (0..n).map(|row| selection.map_or(row, |selection| selection[row]));
            if function.pb_code() == ScalarFuncSig::JsonDepthSig {
                result.resize_int64(n, false);
                result.with_fixed_cells_mut::<8, _>(|output, bitmap| {
                    for (i, (cell, row)) in output.iter_mut().zip(rows).enumerate() {
                        if cells.is_null(row) {
                            bitmap[i / 8] &= !(1 << (i % 8));
                        } else {
                            *cell = (cells.json_depth_for_sql(row).map_err(binary_json_error)? as i64)
                                .to_ne_bytes();
                        }
                    }
                    Ok(())
                })
            } else {
                result.reserve_json(n);
                let bytes_at = cells.variable_cells();
                let mut layouts = Vec::new();
                let outcome = result.try_append_byte_rows(n, |index, output| {
                    let row = selection.map_or(index, |selection| selection[index]);
                    if cells.is_null(row) {
                        return Ok(false);
                    }
                    let Some(array) = BinaryJSON::get_encoded_key_array(bytes_at(row), None)
                        .map_err(binary_json_error)?
                    else {
                        return Ok(false);
                    };
                    append_key_array(output, index, array, &mut layouts);
                    Ok(true)
                });
                for (row, layout) in layouts {
                    result.remember_json_layout(row, layout);
                }
                outcome
            }
        },
    )?
}

/// Go `builtinJSONObjectSig.vecEvalJSON`: check all key columns before
/// constructing any result document, with the first key's NULL error first.
fn object(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let mut buffers = Vec::with_capacity(function.args.len());
    for (i, arg) in function.args.iter().enumerate() {
        buffers.push(vec_eval::argument(
            arg,
            if i % 2 == 0 {
                EvalType::String
            } else {
                EvalType::Json
            },
            ctx,
            input,
        )?);
    }
    result.reserve_json(input.num_rows());
    for i in (0..function.args.len()).step_by(2) {
        for row in 0..input.num_rows() {
            if buffers[i].is_null(row) {
                return Err(EvalError::Conversion(
                    tidb_datatype::types_errors::ERR_JSON_DOCUMENT_NULL_KEY.clone(),
                ));
            }
        }
    }
    for row in 0..input.num_rows() {
        let values = buffers
            .iter()
            .enumerate()
            .map(|(i, buffer)| {
                vec_eval::value(
                    buffer,
                    row,
                    if i % 2 == 0 {
                        EvalType::String
                    } else {
                        EvalType::Json
                    },
                    function.args[i].static_type(),
                )
            })
            .collect();
        let value = function.pb_builtin.eval_values(function, values, ctx)?;
        vec_eval::put(result, row, EvalType::Json, &value)?;
    }
    Ok(())
}
