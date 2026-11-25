// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{ScalarFunction, vec_eval};
use crate::{Columns, EvalError, builtin_ext::misc};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{EvalType, MYDECIMAL_STRUCT_SIZE, MyDecimal};

pub(super) fn version(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    map_uuid(
        function,
        ctx,
        input,
        result,
        Column::resize_int64,
        |bytes| misc::uuid_version_bytes(bytes).map(|version| Some(version.to_ne_bytes())),
    )
}

pub(super) fn timestamp(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    map_uuid(
        function,
        ctx,
        input,
        result,
        Column::resize_decimal,
        |bytes| Ok(misc::uuid_timestamp_micros(bytes)?.map(decimal_micros)),
    )
}

fn decimal_micros(micros: i64) -> [u8; MYDECIMAL_STRUCT_SIZE] {
    if micros.unsigned_abs() >= 1_000_000 {
        return MyDecimal::from_scaled_i128(i128::from(micros), 6, 6)
            .expect("a UUID timestamp fits a MyDecimal")
            .to_raw_bytes();
    }
    // Go uses zero integer digits here. Its first base-1e9 word holds the
    // six fraction digits padded on the right with three zeroes.
    let mut packed = [0; MYDECIMAL_STRUCT_SIZE];
    packed[..4].copy_from_slice(&[0, 6, 6, u8::from(micros < 0)]);
    packed[4..8].copy_from_slice(&((micros.unsigned_abs() as u32) * 1_000).to_ne_bytes());
    packed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoColumns;
    use crate::column::Column as ExpressionColumn;
    use crate::evaluator::EvaluatorSuite;
    use crate::expression::Expression;
    use crate::new_function::new_function_base;
    use tidb_datatype::{FieldType, FieldTypeCode};

    #[test]
    fn uuid_timestamp_packs_the_epoch_like_go() {
        let input_type = FieldType::new(FieldTypeCode::VarString);
        let output_type = FieldType::new(FieldTypeCode::NewDecimal);
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&input_type), 2);
        input.append_bytes(0, b"00000000-0000-7000-8000-000000000000");
        input.append_bytes(0, b"\xff00000000-0000-7000-8000-000000000000\xfe");
        let mut argument = ExpressionColumn::new(1, input_type);
        argument.index = 0;
        let function = new_function_base(
            &NoColumns,
            "uuid_timestamp",
            output_type.clone(),
            vec![Expression::Column(argument)],
        )
        .unwrap();
        let suite = EvaluatorSuite::new(vec![function], false);
        let mut output = Chunk::new_chunk_with_capacity(&[output_type], 2);
        suite.run(&NoColumns, &mut input, &mut output).unwrap();
        let values = output.column(0);
        let mut expected = [0; MYDECIMAL_STRUCT_SIZE];
        expected[..4].copy_from_slice(&[0, 6, 6, 0]);
        assert_eq!(values.get_raw(0), expected.as_slice());
        assert_eq!(values.get_raw(1), expected.as_slice());
    }
}

fn map_uuid<const N: usize>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    resize: fn(&mut Column, usize, bool),
    map: impl Fn(&[u8]) -> Result<Option<[u8; N]>, EvalError>,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let get = cells.variable_cells();
            let rows = selection.map_or(column.rows(), <[usize]>::len);
            resize(result, rows, false);
            result.with_fixed_cells_mut(|output, bitmap| {
                for (row, value) in output.iter_mut().enumerate() {
                    let source = selection.map_or(row, |selection| selection[row]);
                    if !cells.is_null(source) {
                        if let Some(cell) = map(get(source))? {
                            *value = cell;
                            continue;
                        }
                    }
                    bitmap[row / 8] &= !(1 << (row % 8));
                }
                Ok(())
            })
        },
    )?
}
