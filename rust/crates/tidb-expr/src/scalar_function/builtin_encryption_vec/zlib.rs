// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{ScalarFunction, vec_eval};
use crate::{Columns, EvalError, builtin_ext::crypto};
use tidb_chunk::{
    chunk::Chunk,
    column::{ByteRow, Column},
};
use tidb_datatype::EvalType;

pub(super) fn compress(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let mut encoder = None;
    write_rows(function, ctx, input, result, |bytes, output| {
        if !bytes.is_empty() {
            let encoder = encoder.get_or_insert_with(crate::go_flate::ZlibEncoder::new);
            output.append(crypto::compress_bytes(bytes, encoder));
        }
        Ok(true)
    })
}

pub(super) fn uncompress(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let mut inflater = None;
    write_rows(function, ctx, input, result, |bytes, output| {
        let inflater = inflater.get_or_insert_with(crypto::ZlibInflater::new);
        Ok(match inflater.decode(bytes, ctx)? {
            Some(decoded) => {
                output.append(decoded);
                true
            }
            None => false,
        })
    })
}

fn write_rows(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    mut write: impl FnMut(&[u8], &mut ByteRow<'_>) -> Result<bool, EvalError>,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            if cells.data_len() == 0 {
                // Every present input is empty and maps to itself.
                if let Some(selection) = selection {
                    result.reserve_string_with_size_hint(selection.len(), 0);
                    return result.try_append_byte_rows(selection.len(), |row, _| {
                        Ok(!cells.is_null(selection[row]))
                    });
                }
                drop(cells);
                column.copy_construct_to(result);
                return Ok(());
            }
            let get = cells.variable_cells();
            let rows = input.num_rows();
            result.reserve_string(rows);
            result.try_append_byte_rows(rows, |row, output| {
                let row = selection.map_or(row, |selection| selection[row]);
                Ok(!cells.is_null(row) && write(get(row), output)?)
            })
        },
    )?
}

pub(super) fn length(
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
            if selection.is_none() && column.raw_cells().data_len() == 0 {
                result.resize_int64(column.rows(), false);
                result.merge_nulls(&[column]);
            } else {
                vec_eval::map_variable_int64(column, selection, result, |bytes| {
                    crypto::uncompressed_length_bytes(bytes, ctx)
                });
            }
        },
    )
}
