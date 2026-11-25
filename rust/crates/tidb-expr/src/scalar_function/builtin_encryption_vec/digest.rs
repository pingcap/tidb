// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{ScalarFunction, vec_eval};
use crate::{Columns, EvalError, builtin_ext::crypto};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::EvalType;
use tidb_goruntime::go_hex;

pub(super) fn unary<const N: usize>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    digest: impl Fn(&[u8]) -> [u8; N],
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let get = cells.variable_cells();
            write_rows(result, input.num_rows(), N * 2, |row, scratch| {
                let row = selection.map_or(row, |selection| selection[row]);
                if cells.is_null(row) {
                    return None;
                }
                Some(go_hex::encode(scratch, &digest(get(row))))
            })
        },
    )?
}

pub(super) fn sha2(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let bytes = vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?;
    let lengths = vec_eval::read_argument(&function.args[1], EvalType::Int, ctx, input)?;
    let bytes = bytes.cells();
    let lengths = lengths.cells();
    let bits = lengths.fixed::<8>();
    write_rows(result, input.num_rows(), 64, |row, scratch| {
        crypto::sha2_hex(bytes.bytes(row)?, i64::from_ne_bytes(*bits(row)?), scratch)
    })
}

fn write_rows(
    result: &mut Column,
    rows: usize,
    size_hint: usize,
    mut hex: impl FnMut(usize, &mut [u8; 128]) -> Option<usize>,
) -> Result<(), EvalError> {
    result.reserve_string_with_size_hint(rows, size_hint);
    let mut scratch = [0; 128];
    result.try_append_byte_rows(rows, |row, output| {
        if let Some(len) = hex(row, &mut scratch) {
            output.append(&scratch[..len]);
            Ok(true)
        } else {
            Ok(false)
        }
    })
}
