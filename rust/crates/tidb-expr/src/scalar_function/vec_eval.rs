// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `Expression.VecEval*`: typed argument columns and signature kernels.

use super::{Kernel, ScalarFunction};
use crate::expression::Expression;
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column, row::Row};
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
pub(crate) fn eval_one(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
    output: &mut Chunk,
    index: usize,
) -> Result<bool, EvalError> {
    if !crate::chunk_executor::vectorized(expression) {
        return Ok(false);
    }
    let field = expression.static_type().expect("vector expression type");
    eval_into(
        expression,
        field.eval_type(),
        ctx,
        input,
        &mut output.column_mut(index),
    )?;
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
    Ok(true)
}

/// Go `Expression.VecEval*`, including `Constant` broadcast and `Column.CopyReconstruct`.
pub(crate) fn eval_into(
    expression: &Expression,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let _scope = crate::chunk_executor::enter_vec_eval(true);
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
                *result = input
                    .column(index)
                    .copy_reconstruct(input.sel(), Some(std::mem::take(result)));
                return Ok(());
            }
        }
        Expression::ScalarFunction(function) if has_kernel(function) => {
            if function.pb_code() == tidb_proto::tipb::ScalarFuncSig::DayName
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
            return match function.pb_builtin.kernel {
                Kernel::Compare(..) => {
                    super::builtin_compare_vec::compare(function, ctx, input, result)
                }
                Kernel::In(_) => super::builtin_compare_vec::in_list(function, ctx, input, result),
                Kernel::If(_) | Kernel::IfNull(_) | Kernel::Case(_) | Kernel::Coalesce(_) => {
                    super::builtin_control_vec::eval(function, ctx, input, result)
                }
                Kernel::Cast { .. } => super::builtin_cast_vec::eval(function, ctx, input, result),
                Kernel::Logic(_) => super::builtin_op_vec::logic(function, ctx, input, result),
                Kernel::IsNull(_) | Kernel::Truth { .. } => {
                    super::builtin_op_vec::predicate(function, ctx, input, result)
                }
                Kernel::Unary(_) => super::builtin_op_vec::unary(function, ctx, input, result),
                _ => super::builtin_arithmetic_vec::eval(function, ctx, input, result),
            };
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
        Datum::Int(value) => {
            column.with_cell_bytes_mut(index, |cell| cell.copy_from_slice(&value.to_ne_bytes()))
        }
        Datum::UInt(value) => {
            column.with_cell_bytes_mut(index, |cell| cell.copy_from_slice(&value.to_ne_bytes()))
        }
        Datum::Real(value) | Datum::Float32(value) => {
            column.with_cell_bytes_mut(index, |cell| cell.copy_from_slice(&value.to_ne_bytes()))
        }
        Datum::Decimal(value) => column.with_cell_bytes_mut(index, |cell| {
            cell.copy_from_slice(&value.to_chunk_my_decimal_lossy().to_raw_bytes())
        }),
        Datum::Time(value) => column.with_cell_bytes_mut(index, |cell| {
            cell.copy_from_slice(&value.go_raw().to_ne_bytes())
        }),
        Datum::Duration(value) => column.with_cell_bytes_mut(index, |cell| {
            cell.copy_from_slice(&value.nanoseconds().to_ne_bytes())
        }),
        _ => return Err(EvalError::Unsupported("vector result has wrong fixed type")),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
