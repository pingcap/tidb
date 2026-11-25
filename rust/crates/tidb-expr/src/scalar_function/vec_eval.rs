// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `Expression.VecEval*`: typed argument columns and signature kernels.

use super::{Kernel, ScalarFunction};
use crate::expression::Expression;
use crate::{Columns, EvalError};
use std::borrow::Cow;
use tidb_chunk::{
    chunk::Chunk,
    column::{Column, RawCells},
    row::Row,
    ColumnRef,
};
use tidb_datatype::{Datum, Decimal, EvalType, FieldType, FieldTypeCode};

/// Go `ScalarFunction.VecEval*`: whether this signature has a migrated kernel.
pub(crate) fn has_kernel(function: &ScalarFunction) -> bool {
    if super::builtin_vec_vec::supported(function) {
        return true;
    }
    if function
        .get_static_type()
        .is_some_and(|f| f.eval_type() == EvalType::VectorFloat32)
        || function.args.iter().any(|a| {
            a.static_type()
                .is_some_and(|f| f.eval_type() == EvalType::VectorFloat32)
        })
    {
        return false;
    }
    if matches!(function.pb_builtin.kernel, Kernel::Json) {
        return super::builtin_json_vec::supported(function);
    }
    matches!(
        function.pb_builtin.kernel,
        Kernel::Binary(..)
            | Kernel::IntegerMultiply { .. }
            | Kernel::IntegerMod { .. }
            | Kernel::Compare(..)
            | Kernel::In(_)
            | Kernel::Case(_)
            | Kernel::Coalesce(_)
            | Kernel::If(_)
            | Kernel::IfNull(_)
            | Kernel::Cast { .. }
            | Kernel::Logic(_)
            | Kernel::Unary(_)
            | Kernel::IsNull(_)
            | Kernel::Truth { .. }
    ) || super::builtin_math_vec::supported(function.pb_code())
        || super::builtin_string_vec::supported(function)
        || super::builtin_other_vec::supported(function)
        || super::builtin_miscellaneous_vec::supported(function)
        || super::builtin_time_vec::supported(function)
        || super::builtin_encryption_vec::supported(function)
        || super::builtin_like_vec::supported(function)
        || super::builtin_info_vec::supported(function)
        || super::builtin_json_vec::supported(function)
        || super::builtin_compare_vec::supported(function.pb_code())
}

/// Go `evalOneVec`: evaluate directly into the projection's owned result column.
/// The projection program has already checked this expression's vector support.
pub(crate) fn eval_one(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
    output: &mut Chunk,
    index: usize,
) -> Result<(), EvalError> {
    let field = expression.static_type().expect("vector expression type");
    let target = field.eval_type();
    {
        let mut result = output.column_mut(index);
        match expression {
            Expression::ScalarFunction(function) => {
                eval_function_into(function, target, ctx, input, &mut result)?;
            }
            _ => eval_into(expression, target, ctx, input, &mut result)?,
        }
    }
    if matches!(
        field.code(),
        FieldTypeCode::Bit | FieldTypeCode::Float | FieldTypeCode::Enum | FieldTypeCode::Set
    ) {
        let source = std::mem::take(&mut *output.column_mut(index));
        let mut destination = Column::new_column(field, input.num_rows());
        match field.code() {
            FieldTypeCode::Float => {
                destination.resize_float32(input.num_rows(), false);
                destination.merge_nulls(&[&source]);
                destination.with_float32s_mut(|values| {
                    for (i, value) in values.iter_mut().enumerate() {
                        *value = source.get_float64(i) as f32;
                    }
                });
            }
            FieldTypeCode::Bit => {
                destination.reserve_bytes(input.num_rows());
                let width = u8::try_from((field.flen() + 7) >> 3)
                    .ok()
                    .and_then(|width| tidb_datatype::BinaryLiteralWidth::try_from(width).ok());
                for i in 0..input.num_rows() {
                    if source.is_null(i) {
                        destination.append_null();
                    } else {
                        destination.append_bytes(
                            tidb_datatype::BinaryLiteral::from_uint(source.get_uint64(i), width)
                                .as_bytes(),
                        );
                    }
                }
            }
            FieldTypeCode::Enum | FieldTypeCode::Set => {
                let elements = field.elems_snapshot();
                let collation = tidb_datatype::get_collator(field.collation_name());
                if field.code() == FieldTypeCode::Enum {
                    destination.reserve_enum(input.num_rows());
                } else {
                    destination.reserve_set(input.num_rows());
                }
                for i in 0..input.num_rows() {
                    if source.is_null(i) {
                        destination.append_null();
                        continue;
                    }
                    let bytes = source.get_bytes(i);
                    if field.code() == FieldTypeCode::Enum {
                        destination.append_enum(
                            &tidb_datatype::parse_enum_name(
                                &elements,
                                bytes.as_ref(),
                                collation.clone(),
                            )
                            .unwrap_or_else(|error| error.returned_value()),
                        );
                    } else {
                        destination.append_set(
                            &tidb_datatype::parse_set_name(
                                &elements,
                                bytes.as_ref(),
                                collation.clone(),
                            )
                            .unwrap_or_default(),
                        );
                    }
                }
            }
            _ => unreachable!(),
        }
        *output.column_mut(index) = destination;
    }
    Ok(())
}

/// Go `Expression.VecEval*`, including `Constant` broadcast and `Column.CopyReconstruct`.
pub(crate) fn eval_into(
    expression: &Expression,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    match expression {
        Expression::Constant(constant) => {
            if let Some(deferred) = constant.deferred_expr.as_deref() {
                return eval_into(deferred, target, ctx, input, result);
            }
            if input.num_rows() == 0 {
                result.reset_for_eval_type(target);
                return Ok(());
            }
            let variable = matches!(
                target,
                EvalType::String | EvalType::Json | EvalType::VectorFloat32
            );
            if variable {
                crate::chunk_executor::allocate(target, input.num_rows(), result);
            }
            let value = constant.eval_typed_as_on_row(ctx, Row::empty(), target)?;
            if !variable {
                crate::chunk_executor::allocate(target, input.num_rows(), result);
            }
            for i in 0..input.num_rows() {
                put(result, i, target, &value)?;
            }
            return Ok(());
        }
        Expression::CorrelatedColumn(column) => {
            if input.num_rows() == 0 {
                result.reset_for_eval_type(target);
                return Ok(());
            }
            let variable = matches!(
                target,
                EvalType::String | EvalType::Json | EvalType::VectorFloat32
            );
            if variable {
                crate::chunk_executor::allocate(target, input.num_rows(), result);
            }
            let value = column.eval_typed(ctx, target)?;
            if !variable {
                crate::chunk_executor::allocate(target, input.num_rows(), result);
            }
            for i in 0..input.num_rows() {
                put(result, i, target, &value)?;
            }
            return Ok(());
        }
        Expression::Column(column) => {
            let field = column.get_static_type().expect("typed column");
            let index = usize::try_from(column.index)
                .ok()
                .filter(|&i| i < input.num_cols())
                .ok_or(EvalError::Unsupported(
                    "column index is outside the input row",
                ))?;
            let time = |ty| matches!(ty, EvalType::Datetime | EvalType::Timestamp);
            if !field.is_hybrid()
                && field.code() != FieldTypeCode::Float
                && (field.eval_type() == target || (time(field.eval_type()) && time(target)))
            {
                let source = input.column(index);
                if input.sel().is_none() {
                    source.copy_construct_to(result);
                } else {
                    *result = source.copy_reconstruct(input.sel(), Some(std::mem::take(result)));
                }
                return Ok(());
            }
        }
        Expression::ScalarFunction(function) if has_kernel(function) => {
            return eval_function_into(function, target, ctx, input, result);
        }
        Expression::ScalarSubQuery(_) => return Err(crate::scalar_subquery::vec_eval_error()),
        _ => {}
    }
    // Go `Column.VecEval*`: hybrid columns and FLOAT widening read typed
    // cells; ordinary columns have already returned through CopyReconstruct.
    if !matches!(expression, Expression::Column(_)) {
        return Err(EvalError::Unsupported(
            "signature has no vectorized implementation",
        ));
    }
    if target == EvalType::Int
        && expression
            .static_type()
            .is_some_and(|field| field.is_hybrid())
    {
        result.resize_int64(0, false);
        for row in 0..input.num_rows() {
            let value = super::eval_numeric_row(expression, ctx, input.get_row(row), target)?;
            match crate::arg_eval_type::eval_int(&value)? {
                Some(value) => result.append_int64(value),
                None => result.append_null(),
            }
        }
        return Ok(());
    }
    crate::chunk_executor::allocate(target, input.num_rows(), result);
    for i in 0..input.num_rows() {
        let value = super::eval_numeric_row(expression, ctx, input.get_row(i), target)?;
        put(result, i, target, &value)?;
    }
    Ok(())
}

fn eval_function_into(
    function: &ScalarFunction,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    match function.pb_builtin.kernel {
        Kernel::Json => return super::builtin_json_vec::eval(function, ctx, input, result),
        Kernel::Logic(_) => return super::builtin_op_vec::logic(function, ctx, input, result),
        Kernel::IsNull(_) | Kernel::Truth { .. } => {
            return super::builtin_op_vec::predicate(function, ctx, input, result);
        }
        Kernel::Unary(_) => return super::builtin_op_vec::unary(function, ctx, input, result),
        _ => (),
    }
    use tidb_proto::tipb::ScalarFuncSig;
    if matches!(function.pb_builtin.kernel, Kernel::String { .. })
        || matches!(function.pb_code(), ScalarFuncSig::Ascii | ScalarFuncSig::BitLength)
    {
        return super::builtin_string_vec::eval(function, ctx, input, result);
    }
    if function.pb_code() == ScalarFuncSig::DayName
        && matches!(target, EvalType::Int | EvalType::Real)
    {
        return super::builtin_time_vec::dayname_index(
            function, target, ctx, input, result,
        );
    }
    if super::builtin_vec_vec::supported(function) {
        return super::builtin_vec_vec::eval(function, ctx, input, result);
    }
    if super::builtin_other_vec::supported(function) {
        return super::builtin_other_vec::eval(function, ctx, input, result);
    }
    if super::builtin_miscellaneous_vec::supported(function) {
        return super::builtin_miscellaneous_vec::eval(function, ctx, input, result);
    }
    if super::builtin_time_vec::supported(function) {
        return super::builtin_time_vec::eval(function, ctx, input, result);
    }
    if super::builtin_encryption_vec::supported(function) {
        return super::builtin_encryption_vec::eval(function, ctx, input, result);
    }
    if super::builtin_like_vec::supported(function) {
        return super::builtin_like_vec::eval(function, ctx, input, result);
    }
    if super::builtin_math_vec::supported(function.pb_code()) {
        return super::builtin_math_vec::eval(function, ctx, input, result);
    }
    if super::builtin_string_vec::supported(function) {
        return super::builtin_string_vec::eval(function, ctx, input, result);
    }
    if super::builtin_info_vec::supported(function) {
        return super::builtin_info_vec::eval(function, ctx, input, result);
    }
    if super::builtin_json_vec::supported(function) {
        return super::builtin_json_vec::eval(function, ctx, input, result);
    }
    if super::builtin_compare_vec::supported(function.pb_code()) {
        return super::builtin_compare_vec::extremum(function, ctx, input, result);
    }
    match function.pb_builtin.kernel {
        Kernel::Compare(..) => {
            super::builtin_compare_vec::compare(function, ctx, input, result)
        }
        Kernel::In(_) => super::builtin_compare_vec::in_list(function, ctx, input, result),
        Kernel::If(_) | Kernel::IfNull(_) | Kernel::Case(_) | Kernel::Coalesce(_) => {
            super::builtin_control_vec::eval(function, ctx, input, result)
        }
        Kernel::Cast { .. } => super::builtin_cast_vec::eval(function, ctx, input, result),
        _ => super::builtin_arithmetic_vec::eval(function, ctx, input, result),
    }
}

/// Go's fixed-arity vector loops: evaluate complete typed argument columns,
/// reuse the designated result column, then apply the signature's cell operation.
pub(super) fn map_arguments(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    reuse: Option<usize>,
    operation: impl FnMut(Vec<Datum>) -> Result<Datum, EvalError>,
) -> Result<(), EvalError> {
    map_arguments_in_order(
        function,
        ctx,
        input,
        result,
        reuse,
        &(0..function.args.len()).collect::<Vec<_>>(),
        operation,
    )
}

/// Go argument-column order for signatures such as `builtinFormatWithLocaleSig`.
pub(super) fn map_arguments_in_order(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    reuse: Option<usize>,
    order: &[usize],
    mut operation: impl FnMut(Vec<Datum>) -> Result<Datum, EvalError>,
) -> Result<(), EvalError> {
    let domain = function
        .get_static_type()
        .expect("builtin return type")
        .eval_type();
    let mut buffers = vec![Column::default(); function.args.len()];
    for &i in order {
        let arg = &function.args[i];
        let target = arg.static_type().expect("cast argument type").eval_type();
        let mut buffer = Column::default();
        eval_into(
            arg,
            target,
            ctx,
            input,
            if reuse == Some(i) {
                &mut *result
            } else {
                &mut buffer
            },
        )?;
        buffers[i] = buffer;
    }
    if reuse.is_none() {
        crate::chunk_executor::allocate(domain, input.num_rows(), result);
    }
    for i in 0..input.num_rows() {
        let values = function
            .args
            .iter()
            .enumerate()
            .map(|(j, arg)| {
                let field = arg.static_type().expect("cast argument type");
                value(
                    if reuse == Some(j) {
                        &*result
                    } else {
                        &buffers[j]
                    },
                    i,
                    field.eval_type(),
                    Some(field),
                )
            })
            .collect();
        put(result, i, domain, &operation(values)?)?;
    }
    Ok(())
}

/// Go `bufAllocator.get` followed by an argument's typed `VecEval*`.
pub(super) fn argument(
    expression: &Expression,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
) -> Result<Column, EvalError> {
    let mut column = Column::default();
    eval_into(expression, target, ctx, input, &mut column)?;
    Ok(column)
}

pub(super) struct ReadArgument<'a> {
    column: Cow<'a, Column>,
    selection: Option<&'a [usize]>,
}

impl<'a> ReadArgument<'a> {
    /// Returns the evaluated column and its logical-to-physical row mapping.
    pub(super) fn source(&self) -> (&Column, Option<&[usize]>) {
        (&self.column, self.selection)
    }

    pub(super) fn cells(&self) -> ReadArgumentCells<'_> {
        ReadArgumentCells {
            cells: self.column.raw_cells(),
            selection: self.selection,
        }
    }

    /// Expose typed rows in evaluation order for a packed column kernel.
    pub(super) fn into_contiguous(self) -> Cow<'a, Column> {
        match self.selection {
            Some(selection) => Cow::Owned(self.column.copy_reconstruct(Some(selection), None)),
            None => self.column,
        }
    }
}

/// Borrow a pair without copying a shared left column when the right
/// argument is a stable typed column. Otherwise evaluate in argument order.
pub(super) fn with_argument_pair<T>(
    left: &Expression,
    right: &Expression,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    consume: impl FnOnce(ReadArgument<'_>, ReadArgument<'_>) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    let stable_right = direct_argument_column(right, target, input)
        .ok()
        .flatten()
        .and_then(|source| source.try_borrow_stable());
    if let Some(column) = stable_right {
        let right = ReadArgument {
            column: Cow::Borrowed(column),
            selection: column.copy_reconstruct_selection(input.sel()),
        };
        return with_argument_column(left, target, ctx, input, |column, selection| {
            let left = ReadArgument {
                column: Cow::Borrowed(column),
                selection,
            };
            consume(left, right)
        })?;
    }
    let left = read_argument(left, target, ctx, input)?;
    let right = read_argument(right, target, ctx, input)?;
    consume(left, right)
}

/// Map two eight-byte argument columns in logical row order.
pub(super) fn write_fixed_pair(
    left: ReadArgument<'_>,
    right: ReadArgument<'_>,
    result: &mut Column,
    rows: usize,
    map: impl Fn(i64, i64) -> Result<Option<[u8; 8]>, EvalError>,
) -> Result<(), EvalError> {
    result.resize_int64(rows, false);
    if let ((left, None), (right, None)) = (left.source(), right.source()) {
        let left = left.raw_cells();
        let right = right.raw_cells();
        let first = left.fixed_cells::<8>();
        let second = right.fixed_cells::<8>();
        return result.with_fixed_cells_mut(|output, bitmap| {
            for (group, (output, mask)) in output.chunks_mut(8).zip(bitmap).enumerate() {
                let present =
                    *mask & left.not_null_bitmap()[group] & right.not_null_bitmap()[group];
                *mask = present;
                let start = group * 8;
                let inputs = first[start..].iter().zip(&second[start..]);
                for (i, (output, (&a, &b))) in output.iter_mut().zip(inputs).enumerate() {
                    if present == u8::MAX || present & (1 << i) != 0 {
                        if let Some(value) =
                            map(i64::from_ne_bytes(a), i64::from_ne_bytes(b))?
                        {
                            *output = value;
                        } else {
                            *mask &= !(1 << i);
                        }
                    }
                }
            }
            Ok(())
        });
    }
    let left = left.cells();
    let right = right.cells();
    let first = left.fixed::<8>();
    let second = right.fixed::<8>();
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, output) in output.iter_mut().enumerate() {
            if let (Some(a), Some(b)) = (first(row), second(row)) {
                if let Some(value) = map(i64::from_ne_bytes(*a), i64::from_ne_bytes(*b))? {
                    *output = value;
                    continue;
                }
            }
            bitmap[row / 8] &= !(1 << (row % 8));
        }
        Ok(())
    })
}

pub(super) struct ReadArgumentCells<'a> {
    cells: RawCells<'a>,
    selection: Option<&'a [usize]>,
}

impl ReadArgumentCells<'_> {
    #[inline]
    pub(super) fn is_null(&self, row: usize) -> bool {
        let row = self.selection.map_or(row, |selection| selection[row]);
        self.cells.is_null(row)
    }

    /// Holds one packed view for a loop that knows its cell width.
    pub(super) fn fixed<'a, const N: usize>(
        &'a self,
    ) -> impl Fn(usize) -> Option<&'a [u8; N]> + 'a {
        let values = self.cells.fixed_cells::<N>();
        move |row| {
            let row = self.selection.map_or(row, |selection| selection[row]);
            (!self.cells.is_null(row)).then(|| &values[row])
        }
    }

    #[inline]
    pub(super) fn bytes(&self, row: usize) -> Option<&[u8]> {
        let row = self.selection.map_or(row, |selection| selection[row]);
        (!self.cells.is_null(row)).then(|| self.cells.get(row))
    }

    #[inline]
    pub(super) fn int64(&self, row: usize) -> Option<i64> {
        self.bytes(row)
            .map(|bytes| i64::from_ne_bytes(bytes.try_into().expect("Int64 cell width")))
    }
}

/// Read-only equivalent of Go's typed argument buffers and CopyReconstruct.
/// Keep stable column borrows or snapshots while later arguments evaluate.
pub(super) fn read_argument<'a>(
    expression: &Expression,
    target: EvalType,
    ctx: &dyn Columns,
    input: &'a Chunk,
) -> Result<ReadArgument<'a>, EvalError> {
    if let Some(source) = direct_argument_column(expression, target, input)? {
        let column = source
            .try_borrow_stable()
            .map(Cow::Borrowed)
            .or_else(|| source.read().try_read_snapshot().map(Cow::Owned));
        if let Some(column) = column {
            let selection = column.copy_reconstruct_selection(input.sel());
            return Ok(ReadArgument { column, selection });
        }
    }
    Ok(ReadArgument {
        column: Cow::Owned(argument(expression, target, ctx, input)?),
        selection: None,
    })
}

/// Read a unary function's input directly for the duration of its kernel.
/// The consumer performs only cell reads and result writes while the source
/// is locked. Casts and nested expressions evaluate before the consumer runs.
pub(super) fn with_argument_column<T>(
    expression: &Expression,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    consume: impl FnOnce(&Column, Option<&[usize]>) -> T,
) -> Result<T, EvalError> {
    if let Some(source) = direct_argument_column(expression, target, input)? {
        let column = source.read();
        if !column.has_mutably_shared_bytes() {
            return Ok(consume(&column, column.copy_reconstruct_selection(input.sel())));
        }
    }
    let column = argument(expression, target, ctx, input)?;
    Ok(consume(&column, None))
}

/// Maps present variable-width cells directly to packed Int64 results.
pub(super) fn map_variable_int64(
    column: &Column,
    selection: Option<&[usize]>,
    result: &mut Column,
    mut map: impl FnMut(&[u8]) -> i64,
) {
    let cells = column.raw_cells();
    let get = cells.variable_cells();
    if let Some(selection) = selection {
        result.write_int64_rows(
            selection
                .iter()
                .map(|&row| (!cells.is_null(row)).then(|| map(get(row)))),
        );
        return;
    }
    result.resize_int64(column.rows(), false);
    result.with_fixed_cells_mut(|output, bitmap| {
        let presence = bitmap.iter_mut().zip(cells.not_null_bitmap());
        let mut row = 0;
        for (group, (output_present, &present)) in output.chunks_mut(8).zip(presence) {
            if present == u8::MAX {
                for cell in group {
                    *cell = map(get(row)).to_ne_bytes();
                    row += 1;
                }
            } else {
                *output_present &= present;
                for (bit, cell) in group.iter_mut().enumerate() {
                    if present & (1 << bit) != 0 {
                        *cell = map(get(row)).to_ne_bytes();
                    }
                    row += 1;
                }
            }
        }
    });
}

pub(super) fn direct_argument_column<'a>(
    expression: &Expression,
    target: EvalType,
    input: &'a Chunk,
) -> Result<Option<ColumnRef<'a>>, EvalError> {
    let Expression::Column(column) = expression else {
        return Ok(None);
    };
    let field = column.get_static_type().expect("typed column");
    if field.is_hybrid() || field.code() == FieldTypeCode::Float || field.eval_type() != target {
        return Ok(None);
    }
    let index = usize::try_from(column.index)
        .ok()
        .filter(|&i| i < input.num_cols())
        .ok_or(EvalError::Unsupported("column index is outside the input row"))?;
    Ok(Some(input.column_ref(index)))
}

/// Go typed column access (`Int64s`, `Float64s`, `Decimals`, `GetString`, ...).
pub(crate) fn value(
    column: &Column,
    index: usize,
    target: EvalType,
    field: Option<&FieldType>,
) -> Datum {
    if column.is_null(index) {
        return Datum::Null;
    }
    match target {
        EvalType::Int if field.is_some_and(FieldType::is_unsigned) => {
            Datum::UInt(column.get_uint64(index))
        }
        EvalType::Int => Datum::Int(column.get_int64(index)),
        EvalType::Real => Datum::Real(column.get_float64(index)),
        EvalType::Decimal => {
            Datum::Decimal(Decimal::from_my_decimal(&column.get_my_decimal(index)))
        }
        EvalType::Datetime | EvalType::Timestamp => Datum::Time(column.get_time(index)),
        EvalType::Duration => {
            Datum::Duration(column.get_duration(index, field.map_or(0, FieldType::decimal)))
        }
        EvalType::String if field.is_some_and(FieldType::is_binary_string) => {
            Datum::Bytes(column.get_bytes(index).as_ref().to_vec())
        }
        EvalType::String => Datum::new_string_datum(column.get_bytes(index).as_ref().to_vec()),
        EvalType::Json => Datum::Json(column.get_json(index)),
        EvalType::VectorFloat32 => Datum::VectorFloat32(column.get_vector_float32(index)),
    }
}

/// Go typed result writes: fixed cells are updated in place, variable cells appended.
pub(super) fn put(
    column: &mut Column,
    index: usize,
    target: EvalType,
    value: &Datum,
) -> Result<(), EvalError> {
    if matches!(
        target,
        EvalType::String | EvalType::Json | EvalType::VectorFloat32
    ) {
        match value {
            Datum::Null => column.append_null(),
            value if target == EvalType::String => column
                .append_bytes(&crate::arg_eval_type::eval_string(value)?.expect("non-NULL string")),
            Datum::Json(value) => column.append_json(value),
            Datum::VectorFloat32(value) => column.append_vector_float32(value),
            _ => {
                return Err(EvalError::Unsupported(
                    "vector result has wrong variable type",
                ))
            }
        }
        return Ok(());
    }
    column.set_null(index, value.is_null());
    match value {
        Datum::Null => {}
        Datum::Int(value) => column.set_cell_bytes(index, &value.to_ne_bytes()),
        Datum::UInt(value) => column.set_cell_bytes(index, &value.to_ne_bytes()),
        Datum::Real(value) | Datum::Float32(value) => {
            column.set_cell_bytes(index, &value.to_ne_bytes())
        }
        Datum::Decimal(value) => {
            column.set_cell_bytes(index, &value.to_chunk_my_decimal_lossy().to_raw_bytes())
        }
        Datum::Time(value) => column.set_cell_bytes(index, &value.go_raw().to_ne_bytes()),
        Datum::Duration(value) => column.set_cell_bytes(index, &value.nanoseconds().to_ne_bytes()),
        _ => return Err(EvalError::Unsupported("vector result has wrong fixed type")),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
